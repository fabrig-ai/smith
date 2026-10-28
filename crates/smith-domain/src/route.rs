//! Routing (programming-model.md, section 4.5; domain/run.md, sections 2, 3, 10 and 13;
//! domain/session.md, sections 3–6): each of the protocol's events to the child domain it is for,
//! each child domain's requests to the protocol layer or, translated, to the
//! other, and the hand-offs the ready list deferred.
//!
//! Every match is exhaustive, so a variant added to either side's vocabulary
//! breaks the build here.
//! The functions retain no independent state: they mutate the root's bounded
//! queues and child domains. They never know provider wire syntax, credential
//! secrets or the host's delivery policy. [`event`], [`deliver`] and
//! [`hand_off`] perform the boundary handoffs.

use alloc::boxed::Box;
use core::mem;

use skein_lib::{Duration, Env, Id, List, Queue, ReplyTo, Token};
use smith_domain_run::{self as run, Spend};
use smith_domain_session as session;

use crate::boundary::{AnsweredCall, Event, Operation, Request};
use crate::domain::{Credential, Domain, Due, Flight, Handoff, StartContext, TurnHandoff};
use crate::limits::{self, Limits};
use crate::peer::Peer;
use crate::translate;

/// What the run reads: this iteration's times, and its own limits.
pub(crate) const fn run_env(env: &Env<Limits>) -> Env<run::Limits> {
    Env { now: env.now, wall: env.wall, limits: env.limits.run }
}

/// What the session child domain reads.
pub(crate) const fn session_env(env: &Env<Limits>) -> Env<session::Limits> {
    Env { now: env.now, wall: env.wall, limits: env.limits.session }
}

/// Hands one of the protocol's events to the child domain it is for.
pub(crate) fn event(domain: &mut Domain, env: &Env<Limits>, event: Event) {
    let event = match event {
        Event::Start {
            reply_to,
            host_run,
            activation,
            identity,
            window,
            messages,
            charter,
            workspace,
            grants,
            transcript,
            answered,
        } => {
            if let Err(invalid) = models_declared(domain, &charter) {
                domain.notices.push(Request::Answer {
                    read: None,
                    to: reply_to,
                    answer: run::Answer::Refused(run::Refusal::Invalid(invalid)),
                    detail: Box::default(),
                });
                return;
            }
            if !takes_grants(domain, &charter, &grants, env.limits.accounts) {
                domain.notices.push(Request::Answer {
                    read: None,
                    to: reply_to,
                    answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Grants)),
                    detail: Box::default(),
                });
                return;
            }
            for grant in grants {
                granted(domain, env, grant);
            }
            return start(
                domain, env, reply_to, host_run, activation, identity, window, messages, charter, workspace,
                transcript, answered,
            );
        }
        Event::Acknowledge { run, turn } => return acknowledge(domain, env, run, turn),
        Event::Grant { grant } => return granted(domain, env, grant),
        Event::Message { finish_requested, run, name, label, text } => {
            run::Event::Message { finish_requested, run, name, label, text }
        }
        Event::Cancel { run } => run::Event::Cancel { run },
        Event::HostReturned { relay, reply } => run::Event::HostReturned { relay, reply },
        Event::Delivered { owner, delivery } => run::Event::Delivered { owner, delivery },
        Event::Read { owner, read } => run::Event::Read { owner, read },
        Event::Probed { owner, executable } => run::Event::Probed { owner, executable },
        Event::Checked { owner, ran } => run::Event::Checked { owner, ran },
        Event::Aborted { owner } => run::Event::Aborted { owner },
        Event::NativeVersion { owner, at, version } => {
            if domain.sessions.contains_key(&owner) {
                session_step(domain, env, session::Event::NativeVersion { session: owner, at, version });
            }
            return;
        }
        Event::BuiltinResult { owner, id, text, error } => {
            if domain.sessions.contains_key(&owner) {
                session_step(domain, env, session::Event::BuiltinResult { session: owner, id, text, error });
            }
            return;
        }
        Event::Completed { owner, completion } => return completed(domain, env, owner, completion),
        Event::Failed { owner, failure, evidence, detail, report } => {
            return failed(domain, env, owner, failure, evidence, detail, report);
        }
        Event::Cancelled { owner } => {
            crate::operation::completion_done(domain, owner);
            let id = *domain.sessions.get(&owner).expect("a session lives until its call has ended");
            let conversation = domain.peers.get(id).expect("a peer lives as its session").conversation;
            assert!(
                run::settle_reservation(&mut domain.run, conversation, owner, 0),
                "cancelled call held its reservation"
            );
            return session_step(domain, env, session::Event::Cancelled { owner });
        }
        Event::Done { owner, done } => {
            crate::operation::command_done(domain, owner);
            if room_for_content(domain, env, crate::facts::done_bytes(&done)) {
                domain.content.push(crate::Content::Tool { owner, done: done.clone() });
            }
            return session_step(domain, env, session::Event::Done { owner, done });
        }
    };
    run_step(domain, env, event);
}

/// Settle a failed completion's lease and zero charge before applying failure policy.
fn failed(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    failure: crate::llm::Failure,
    evidence: crate::llm::Evidence,
    detail: Box<[u8]>,
    report: crate::llm::Report,
) {
    let issued = crate::operation::completion_done(domain, owner);
    let grant = issued.grant;
    let retry_ok = issued.renewable
        || match grant {
            Some(name) => match domain.grants.get(&name.account) {
                Some(held) => held.name.generation > name.generation && held.expires > env.now,
                None => false,
            },
            None => false,
        };
    let id = *domain.sessions.get(&owner).expect("a session lives until its call has ended");
    let peer = domain.peers.get_mut(id).expect("a peer lives as its session");
    let conversation = peer.conversation;
    peer.report = report;
    assert!(run::settle_reservation(&mut domain.run, conversation, owner, 0), "failed call held its reservation");
    match failure {
        crate::llm::Failure::Unauthorized | crate::llm::Failure::SignedOut => {
            if let Some(grant) = grant
                && let Some(held) = domain.grants.get_mut(&grant.account)
                && !was_rejected(held, grant.generation)
            {
                held.rejected = Some(grant.generation);
                domain.notices.push(Request::Unauthorized { grant });
            }
        }
        crate::llm::Failure::Exhausted { retry_after } => {
            let peer = domain.peers.get_mut(id).expect("failed completion retains its peer");
            peer.retry_at = None;
            if let Some(reset) = retry_after {
                peer.retry_at = Some(env.now.saturating_add(reset));
            }
            if let Some(grant) = grant {
                domain.notices.push(Request::Limited { account: grant.account, reset: retry_after });
            }
        }
        crate::llm::Failure::Refused => {
            let code = domain.peers.get(id).expect("failed completion retains its peer").report.code.clone();
            if let Some(grant) = grant {
                domain.notices.push(Request::Refused { account: grant.account, code, reason: detail.clone() });
            }
        }
        crate::llm::Failure::Overloaded
        | crate::llm::Failure::RateLimited { .. }
        | crate::llm::Failure::Unavailable
        | crate::llm::Failure::TimedOut { .. }
        | crate::llm::Failure::ContextTooLong
        | crate::llm::Failure::Invalid
        | crate::llm::Failure::Program { .. }
        | crate::llm::Failure::SessionGone
        | crate::llm::Failure::Unsupported
        | crate::llm::Failure::Limit { .. }
        | crate::llm::Failure::ApplicationLimit
        | crate::llm::Failure::Protocol
        | crate::llm::Failure::Cancelled => {}
    }
    session_step(domain, env, session::Event::Failed { owner, failure, evidence, detail, retry_ok });
}

/// Resume the one unsent main completion when its host has kept enough turns.
/// Settle the provider lease and its budget reservation before the session
/// may record usage or run decoded calls.
fn completed(domain: &mut Domain, env: &Env<Limits>, owner: Token, completion: crate::llm::Completion) {
    let issued = crate::operation::completion_done(domain, owner);
    capture(domain, env, owner, issued.name, &completion);
    let id = *domain.sessions.get(&owner).expect("a session lives until its call has ended");
    let conversation = domain.peers.get(id).expect("a peer lives as its session").conversation;
    let preview = session::preview_completion(&domain.session, owner, completion.usage);
    let overflow = match &preview {
        Err(end) => Some(end.clone()),
        Ok(price) => priced_overflow(run::completion_overflow(
            &domain.run,
            conversation,
            *price,
            translate::spend(1, completion.usage),
        )),
    };
    let charge = preview.unwrap_or_default();
    let within_reservation = run::settle_reservation(&mut domain.run, conversation, owner, charge);
    if !within_reservation {
        return session_step(domain, env, session::Event::Overflowed { owner, end: session::End::PriceOverflow });
    }
    if let Some(end) = overflow {
        return session_step(domain, env, session::Event::Overflowed { owner, end });
    }
    let peer = domain.peers.get_mut(id).expect("a peer lives as its session");
    let before = peer.tickets();
    let completion = peer.completion(completion, env.limits.decoded_call_bytes.min(env.limits.session.history_bytes));
    domain.tickets = domain.tickets.saturating_add(peer.tickets()).saturating_sub(before);
    session_step(domain, env, session::Event::Completed { owner, completion });
}

fn acknowledge(domain: &mut Domain, env: &Env<Limits>, run: Token, turn: u32) {
    run_step(domain, env, run::Event::Acknowledge { run, turn });
    if let Some(pending) = domain.pending.remove(&run) {
        let mut emitted = Queue::with_capacity(2);
        complete(domain, env, pending, &mut emitted);
        for _ in 0_u32..2 {
            if let Some(request) = emitted.pop() {
                domain.notices.push(request);
            }
        }
    }
}

fn priced_overflow(failure: Option<run::Failure>) -> Option<session::End> {
    match failure {
        Some(run::Failure::Budget { exhausted: run::Exhausted::Overflow(run::Overflow::Spend), .. }) => {
            Some(session::End::PriceOverflow)
        }
        Some(run::Failure::Budget { exhausted: run::Exhausted::Overflow(run::Overflow::Usage), .. }) => {
            Some(session::End::UsageOverflow)
        }
        Some(_) | None => None,
    }
}

fn models_declared(domain: &Domain, charter: &run::Charter) -> Result<(), run::Invalid> {
    domain.config.check_model(&charter.llm)?;
    for model in &charter.models {
        domain.config.check_model(model)?;
    }
    Ok(())
}

/// Delivers a hand-off from the run that waited on the ready list.
pub(crate) fn deliver(domain: &mut Domain, env: &Env<Limits>, handoff: Handoff) {
    let event = match handoff {
        Handoff::Close { peer } => {
            let peer = domain.peers.get(peer).expect("a close is forgotten when its peer goes");
            let session = peer.session.expect("the run closes a conversation once it has started");
            session::Event::Close { session }
        }
        Handoff::Answer { owner } => {
            let flight = domain.flights.remove(&owner).expect("a call is in flight until it is answered");
            match flight.answer {
                Due::Answered { feedback, spent } => {
                    session::Event::Answered { owner, text: feedback.text, error: feedback.error, spent }
                }
                Due::Yielding { feedback, spent } => {
                    session::Event::AnsweredAndYield { owner, text: feedback.text, error: feedback.error, spent }
                }
                Due::Withheld => session::Event::AnswerWithheld { owner },
                Due::Cancelled { spent } => session::Event::AnswerCancelled { owner, spent },
                Due::Waiting => unreachable!("a call is on the ready list once the run has returned it"),
            }
        }
    };
    session_step(domain, env, event);
}

#[expect(
    clippy::too_many_arguments,
    reason = "Start keeps the host-issued activation with its concrete admission fields"
)]
fn start(
    domain: &mut Domain,
    env: &Env<Limits>,
    reply_to: ReplyTo,
    host_run: Token,
    activation: u64,
    identity: Option<run::Identity>,
    window: crate::Window,
    messages: Box<[run::Message]>,
    charter: run::Charter,
    workspace: Option<run::Workspace>,
    transcript: Option<session::record::Transcript>,
    answered: Box<[AnsweredCall]>,
) {
    let largest_turn = limits::max_turn_bytes(&env.limits).expect("representable turn cap");
    if window.turns == 0 || window.bytes < largest_turn {
        domain.notices.push(Request::Answer {
            read: None,
            to: reply_to,
            answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Window)),
            detail: Box::default(),
        });
        return;
    }
    if domain.starts.is_full() {
        domain.notices.push(Request::Answer {
            read: None,
            to: reply_to,
            answer: run::Answer::Refused(run::Refusal::Busy),
            detail: Box::default(),
        });
        return;
    }
    let compatible = match crate::feedback_worst_case(&env.limits.run) {
        Some(bytes) => bytes <= env.limits.session.delegated_result_bytes && env.limits.session.spend > 0,
        None => false,
    };
    if !compatible
        || env.limits.run.directories > env.limits.session.tools.repos
        || env.limits.run.directory_name_bytes > env.limits.session.tools.path_bytes
        || env.limits.run.host_path_bytes > env.limits.session.tools.host_path_bytes
    {
        domain.notices.push(Request::Answer {
            read: None,
            to: reply_to,
            answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Conversation)),
            detail: Box::default(),
        });
        return;
    }
    let (transcript, answered, refused) = if charter.resume {
        match crate::waking::render(answered, &env.limits) {
            Some(text) if history_fits(transcript.as_ref(), &text, &env.limits) => (transcript, text, None),
            Some(_) | None => (None, Box::default(), Some(run::TranscriptRefusal::TooLarge)),
        }
    } else {
        (None, Box::default(), None)
    };
    let resumed = transcript.is_some();
    // A resume without a host identity keeps the run tree under the recorded
    // main key (domain/run.md, section 3.2). The run only mints when both are absent.
    let identity = match identity {
        Some(identity) => Some(identity),
        None => match transcript.as_ref() {
            Some(history) => match history.turns.first() {
                Some(turn) => match &turn.kind {
                    session::record::Kind::First(opener) | session::record::Kind::Window(opener) => {
                        Some(run::Identity::new(opener.affinity.key))
                    }
                    session::record::Kind::Ordinary => None,
                },
                None => None,
            },
            None => None,
        },
    };
    let context = StartContext { reply_to: Some(reply_to), transcript, answered, refused };
    let id = domain.starts.insert(context).expect("reserved original parent right before work");
    run_step(
        domain,
        env,
        run::Event::Start {
            reply_to: ReplyTo::new(id.token()),
            host_run,
            activation,
            identity,
            messages,
            window: run::Window { turns: window.turns, bytes: window.bytes, largest_turn },
            charter,
            workspace,
            transcript: Some(id.token()),
            resumed,
        },
    );
}

fn history_fits(transcript: Option<&session::record::Transcript>, answered: &[u8], limits: &Limits) -> bool {
    let cap = u64::from(limits.session.history_messages);
    let mut messages = 0_u64;
    let mut history_bytes = 0_u64;
    if let Some(transcript) = transcript {
        if u64::try_from(transcript.turns.len()).expect("owned input count fits") > cap {
            return false;
        }
        for turn in &transcript.turns {
            messages = match messages.checked_add(u64::try_from(turn.messages.len()).expect("owned input count fits")) {
                Some(count) if count <= cap => count,
                Some(_) | None => return false,
            };
        }
        history_bytes = match transcript.owned_bytes() {
            Some(bytes) => bytes,
            None => return false,
        };
    }
    if messages > cap
        || u64::try_from(answered.len()).expect("owned length fits") > u64::from(limits.run.answered_bytes)
    {
        return false;
    }
    let total = history_bytes.checked_add(u64::try_from(answered.len()).expect("owned length fits"));
    match total {
        Some(bytes) => match limits::record_payload(limits) {
            Some(cap) => bytes <= cap && bytes <= limits.session.history_bytes,
            None => false,
        },
        None => false,
    }
}

/// Routes what the child domains emitted, and what that leads to, until both
/// have emitted all they will in this entry point.
pub(crate) fn hand_off(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    for _ in 0_u32..2 {
        if let Some(notice) = domain.notices.pop() {
            out.push(notice);
        }
    }
    let bound = limits::run_out(&env.limits).saturating_add(limits::session_out(&env.limits));
    for _ in 0..bound {
        // Finish the causal run translation of a child notice before taking
        // its next child request. In particular Session Turn precedes Complete;
        // its concrete root Turn must not be overtaken by that provider effect.
        if let Some(request) = domain.run_out.pop() {
            from_run(domain, env, request, out);
        } else if let Some(request) = domain.session_out.pop() {
            from_session(domain, env, request, out);
        } else {
            return;
        }
    }
    assert!(
        domain.session_out.is_empty() && domain.run_out.is_empty(),
        "an entry point's hand-offs end within its bound"
    );
}

fn run_step(domain: &mut Domain, env: &Env<Limits>, event: run::Event) {
    assert!(domain.run_out.room() >= run::MAX_OUT, "an entry point steps the run no more than its bound");
    run::step(&mut domain.run, &run_env(env), event, &mut domain.run_out);
    crate::domain::gather(domain, &env.limits);
}

fn session_step(domain: &mut Domain, env: &Env<Limits>, event: session::Event) {
    let room = session::max_out(&env.limits.session);
    assert!(domain.session_out.room() >= room, "an entry point steps the sessions no more than its bound");
    session::step(&mut domain.session, &session_env(env), event, &mut domain.session_out);
    crate::domain::gather(domain, &env.limits);
}

/// Release the settled peer and move its last diagnostic with the run terminal.
fn ended_session(
    domain: &mut Domain,
    opener: Token,
    end: session::End,
    turns: u32,
    reported: session::llm::Usage,
    detail: Box<[u8]>,
) -> run::Event {
    let run = run::owner(&domain.run, opener).expect("run outlives its conversation");
    let id = peer(domain, opener);
    let peer = domain.peers.get(id).expect("session retains peer");
    let model = peer.model;
    let account = peer.account;
    let retry_at = peer.retry_at;
    let report = peer.report.clone();
    if domain.peers.get(id).expect("session retains peer").is_main() {
        domain.pending.remove(&run);
    }
    free(domain, id);
    run::Event::Ended {
        conversation: opener,
        end: translate::end(end, model, account, retry_at, report),
        spend: translate::spend(turns, reported),
        detail,
    }
}

/// One of the sessions' requests: out to the protocol layer, or to the run.
fn from_session(domain: &mut Domain, env: &Env<Limits>, request: session::Request, out: &mut Queue<Request>) {
    let event = match request {
        session::Request::Priced { opener, spent, own_spent } => {
            run::Event::Priced { conversation: opener, own_spent, subtree_spent: spent }
        }
        session::Request::Turn { opener, turn } => {
            let id = peer(domain, opener);
            if !domain.peers.get(id).expect("main binding lives through Turn").is_main() {
                return;
            }
            let sequence = turn.sequence;
            let opens_window = turn.kind.opens_window();
            let record =
                domain.turns.insert(TurnHandoff { turn: Some(turn) }).expect("room for every queued session turn");
            run::Event::Turn { conversation: opener, record: record.token(), sequence, opens_window }
        }
        session::Request::Complete {
            owner,
            name,
            prompt,
            deadlines,
            max_completion_bytes,
            max_completion_blocks,
            max_failure_bytes,
        } => {
            let pending = PendingCompletion {
                owner,
                name,
                prompt,
                deadlines,
                max_completion_bytes,
                max_completion_blocks,
                max_failure_bytes,
            };
            return complete(domain, env, pending, out);
        }
        session::Request::Yield { owner } => return out.push(Request::Yield { owner }),
        session::Request::Cancel { owner } => {
            let id = *domain.sessions.get(&owner).expect("a call owner retains its session");
            let conversation = domain.peers.get(id).expect("session retains peer").conversation;
            let run = run::owner(&domain.run, conversation).expect("run outlives its session");
            let held = match domain.pending.get(&run) {
                Some(pending) => pending.owner == owner,
                None => false,
            };
            if held {
                domain.pending.remove(&run);
                return session_step(domain, env, session::Event::Cancelled { owner });
            }
            return out.push(Request::Cancel { owner });
        }
        session::Request::Io { owner, op, deadline } => {
            crate::operation::command(domain, owner, &op, deadline, out);
            return out.push(Request::Io { owner, op, deadline });
        }
        session::Request::Job { owner, act } => {
            crate::operation::job(domain, env, owner, act, out);
            return out.push(Request::Job { owner, act });
        }
        session::Request::CancelIo { owner } => return out.push(Request::CancelIo { owner }),
        session::Request::JobsCancelled { opener } => run::Event::JobsCancelled { conversation: opener },
        session::Request::Opened { opener, session } => {
            let id = peer(domain, opener);
            domain.peers.get_mut(id).expect("found above").session = Some(session);
            let fresh = domain.sessions.insert(session, id).expect("a peer for every session");
            assert!(fresh.is_none(), "a session opens once");
            run::Event::Started { conversation: opener, peer: session }
        }
        session::Request::Yielded { opener, stop, text } => {
            let id = peer(domain, opener);
            let peer = domain.peers.get_mut(id).expect("found above");
            let text = peer.yielded_text(text);
            let forgotten = peer.forget_asks(&env.limits.session);
            domain.tickets = domain.tickets.saturating_sub(forgotten);
            run::Event::Yielded { conversation: opener, stop: translate::stop(stop), text }
        }
        session::Request::Used { opener, usage } => {
            run::Event::Used { conversation: opener, spend: translate::spend(1, usage) }
        }
        session::Request::Ended { opener, end, turns, usage: _, reported, detail } => {
            ended_session(domain, opener, end, turns, reported, detail)
        }
        session::Request::Delegate { owner, opener, call, deadline, origin } => {
            delegated(domain, owner, opener, call, deadline, origin)
        }
        session::Request::Withdraw { owner } => {
            let flight = domain.flights.get_mut(&owner).expect("a call is withdrawn while it is in flight");
            flight.withdrawn = true;
            match &flight.answer {
                Due::Waiting => {}
                // The answer won the race: the run has returned it.
                Due::Answered { .. } | Due::Yielding { .. } | Due::Withheld | Due::Cancelled { .. } => return,
            }
            let conversation = domain.peers.get(flight.peer).expect("a peer outlives its calls").conversation;
            run::Event::Withdraw { conversation, call: owner }
        }
    };
    run_step(domain, env, event);
}

/// Transfer one live ask into a pending terminal right, retaining its complete
/// historical origin. The session's expiry bounds the call; only its /// terminal releases this flight.
fn delegated(
    domain: &mut Domain,
    owner: Token,
    opener: Token,
    call: Token,
    deadline: skein_lib::Time,
    origin: session::record::Origin,
) -> run::Event {
    let id = peer(domain, opener);
    let activation = domain.peers.get(id).expect("found above").activation;
    let ask = domain.peers.get_mut(id).expect("found above").take(call);
    domain.tickets = domain.tickets.saturating_sub(1);
    let flight = Flight { peer: id, withdrawn: false, answer: Due::Waiting };
    let fresh = domain.flights.insert(owner, flight).expect("room for a batch of each session");
    assert!(fresh.is_none(), "a session names its calls in flight apart");
    run::Event::Delegated {
        conversation: opener,
        call: owner,
        ask,
        deadline,
        name: run::CallName { activation, completion: origin.sequence, position: origin.position },
    }
}

/// One of the run's requests: out to the protocol layer, or to the sessions.
fn from_run(domain: &mut Domain, env: &Env<Limits>, request: run::Request, out: &mut Queue<Request>) {
    let event = match request {
        run::Request::CompletionReady { conversation } => {
            let run = run::owner(&domain.run, conversation).expect("main retains its run");
            if let Some(pending) = domain.pending.remove(&run) {
                complete(domain, env, pending, out);
            }
            return;
        }
        run::Request::HostCall { host_run, relay, name, tool, effect, input, deadline } => {
            return out.push(Request::HostCall { host_run, relay, name, tool, effect, input, deadline });
        }
        run::Request::WithdrawHost { relay } => return out.push(Request::WithdrawHost { relay }),
        run::Request::MessageRefused { host_run, name, reason } => {
            return out.push(Request::MessageRefused { host_run, name, reason });
        }
        run::Request::Admitted { host_run, run } => return out.push(Request::Admitted { host_run, run }),
        run::Request::Answer { to, answer, read, detail } => {
            let id = Id::<StartContext>::from_token(to.into_token());
            let context = domain.starts.get_mut(id).expect("root issued the start reply binding");
            let to = context.reply_to.take().expect("one actual terminal consumes the parent right");
            context.transcript = None;
            context.answered = Box::default();
            domain.starts.retire(id);
            return out.push(Request::Answer { to, answer, read, detail });
        }
        run::Request::Waiting { host_run, read } => return out.push(Request::Waiting { host_run, read }),
        run::Request::Turn { host_run, record, number, position, opens_window, read, spent } => {
            let id = Id::<TurnHandoff>::from_token(record);
            let handoff = domain.turns.get_mut(id).expect("root issued the concrete turn binding");
            let turn = handoff.turn.take().expect("one actual output takes the concrete body");
            assert_eq!(turn.sequence, position, "run and record name the same conversation turn");
            domain.turns.retire(id);
            return out.push(Request::Turn { host_run, number, position, opens_window, read, spent, turn });
        }
        run::Request::Checking { host_run, deadline } => {
            return out.push(Request::Long { host_run, operation: Operation::Checks, until: deadline });
        }
        run::Request::ChecksEnded { host_run } => return out.push(Request::LongDone { host_run }),
        run::Request::Deliver { host_run, owner, change, name, deadline } => {
            return out.push(Request::Deliver { host_run, owner, change, name, deadline });
        }
        run::Request::Read { owner, at, max, deadline } => return out.push(Request::Read { owner, at, max, deadline }),
        run::Request::Probe { owner, at, deadline } => return out.push(Request::Probe { owner, at, deadline }),
        run::Request::Check { owner, program, deadline, tail } => {
            return out.push(Request::Check { owner, program, deadline, tail });
        }
        run::Request::Abort { owner } => return out.push(Request::Abort { owner }),
        run::Request::Open { conversation, opening } => {
            return open(domain, env, conversation, opening);
        }
        run::Request::Say { peer, text } => session::Event::Continue { session: peer, content: text },
        run::Request::Yield { peer } => session::Event::Yield { session: peer },
        run::Request::CancelJobs { peer } => session::Event::CancelJobs { session: peer },
        run::Request::Close { peer } => {
            // A close of a session that has ended is stale.
            if let Some(id) = domain.sessions.get(&peer) {
                domain.ready.defer(Handoff::Close { peer: *id });
            }
            return;
        }
        run::Request::Return { call, result, spent } => {
            let flight = domain.flights.get_mut(&call).expect("the run returns a call in flight");
            if flight.withdrawn && result == run::Returned::Cancelled {
                flight.answer = Due::Cancelled { spent };
            } else if result == run::Returned::Withheld {
                flight.answer = Due::Withheld;
            } else {
                let yields = match &result {
                    run::Returned::Crossed { .. } | run::Returned::Waited { .. } => true,
                    run::Returned::Withheld
                    | run::Returned::HostAnswered(_)
                    | run::Returned::HostUnknown
                    | run::Returned::HostTooLarge { .. }
                    | run::Returned::HostReportedTooLarge
                    | run::Returned::HostRejected(_)
                    | run::Returned::Delivered(_)
                    | run::Returned::Nothing
                    | run::Returned::DeliveryRefused(_)
                    | run::Returned::Accepted
                    | run::Returned::Rejected { .. }
                    | run::Returned::ChecksFailed { .. }
                    | run::Returned::Stale
                    | run::Returned::DeliveryFailed { .. }
                    | run::Returned::Cancelled
                    | run::Returned::TimedOut
                    | run::Returned::Busy
                    | run::Returned::Answered { .. }
                    | run::Returned::Unanswered { .. }
                    | run::Returned::Refused { .. } => false,
                };
                let feedback = crate::feedback(result, env.limits.session.delegated_result_bytes)
                    .expect("compatible canonical receiving cap was checked before any effect");
                flight.answer =
                    if yields { Due::Yielding { feedback, spent } } else { Due::Answered { feedback, spent } };
            }
            return domain.ready.defer(Handoff::Answer { owner: call });
        }
    };
    session_step(domain, env, event);
}

/// Unsent session request moved into one concrete root admission cell. Its
/// source prompt already owns the receiving reservation; no extra copy is made
/// until the pure run gate accepts.
#[derive(Debug)]
pub(crate) struct PendingCompletion {
    owner: Token,
    name: crate::llm::CompletionName,
    prompt: session::llm::Prompt,
    deadlines: smith_domain_session::Deadlines,
    max_completion_bytes: u64,
    max_completion_blocks: u32,
    max_failure_bytes: u32,
}

fn denied_budget(exhausted: run::Exhausted) -> session::BudgetDenial {
    match exhausted {
        run::Exhausted::Turns => session::BudgetDenial::Turns,
        run::Exhausted::Spend => session::BudgetDenial::Spend,
        run::Exhausted::Time => session::BudgetDenial::Time,
        run::Exhausted::Tokens(_) | run::Exhausted::Overflow(_) => {
            unreachable!("receiving and overflow failures close the run")
        }
    }
}

fn completion_input(peer: &Peer, prompt: &session::llm::Prompt) -> Option<u64> {
    prompt.input_bytes()?.checked_add(peer.declaration_bytes()?)
}

fn pending_permit(
    domain: &mut Domain,
    conversation: Token,
    env: &Env<Limits>,
    pending: &PendingCompletion,
) -> run::CompletionPermit {
    if domain.config.program(run::charter::Endpoint(pending.prompt.endpoint.0)).is_some() {
        return run::completion_permit(&mut domain.run, conversation, env.now, run::Most::Unreserved);
    }
    match run::completion_permit(&mut domain.run, conversation, env.now, run::Most::Bounded(0)) {
        run::CompletionPermit::Allowed => {}
        permit @ (run::CompletionPermit::Final { .. }
        | run::CompletionPermit::Held
        | run::CompletionPermit::Denied(_)
        | run::CompletionPermit::Closing) => return permit,
    }
    let id = *domain.sessions.get(&pending.owner).expect("pending session");
    let peer = domain.peers.get(id).expect("session retains peer");
    let most = match completion_input(peer, &pending.prompt) {
        Some(bytes) => {
            session::preview_reservation(&domain.session, pending.owner, bytes, 0, pending.prompt.max_tokens)
        }
        None => None,
    };
    match most {
        Some(most) => run::completion_permit(&mut domain.run, conversation, env.now, run::Most::Bounded(most)),
        None => run::CompletionPermit::Denied(run::Exhausted::Spend),
    }
}

#[expect(clippy::manual_map, reason = "programming-model.md 10.1 keeps the reservation translation exhaustive")]
fn complete(domain: &mut Domain, env: &Env<Limits>, pending: PendingCompletion, out: &mut Queue<Request>) {
    let owner = pending.owner;
    let id = *domain.sessions.get(&owner).expect("a session asks for completions once it has opened");
    let conversation = domain.peers.get(id).expect("a peer lives as its session").conversation;
    // Admission precedes grant lookup, prompt cloning, lease retention
    // and every Client effect. This completion is still unsent.
    match pending_permit(domain, conversation, env, &pending) {
        run::CompletionPermit::Allowed | run::CompletionPermit::Final { note: None } => {}
        run::CompletionPermit::Final { note: Some(note) } => {
            drop(pending);
            let choice = domain.peers.get(id).expect("session retains peer").final_choice();
            let names = domain.peers.get(id).expect("session retains peer").retained_choice(choice.clone());
            return session_step(domain, env, session::Event::Narrowed { owner, note, choice, names });
        }
        run::CompletionPermit::Held => {
            let run = run::owner(&domain.run, conversation).expect("main remains live while held");
            let old = domain.pending.insert(run, pending).expect("one held completion per run");
            assert!(old.is_none(), "only one unsent main completion");
            return;
        }
        run::CompletionPermit::Denied(exhausted) => {
            let reason = denied_budget(exhausted);
            return session_step(domain, env, session::Event::BudgetDenied { owner, reason });
        }
        run::CompletionPermit::Closing => {
            return session_step(domain, env, session::Event::UnsentClosed { owner });
        }
    }
    let PendingCompletion {
        owner,
        name,
        prompt,
        deadlines,
        max_completion_bytes,
        max_completion_blocks,
        max_failure_bytes,
    } = pending;
    let peer = domain.peers.get_mut(id).expect("a peer lives as its session");
    // What the last completion asked and the session did not dispatch,
    // it never will.
    let forgotten = peer.forget_asks(&env.limits.session);
    domain.tickets = domain.tickets.saturating_sub(forgotten);
    let account = peer.account;
    let program = domain.config.program(run::charter::Endpoint(prompt.endpoint.0)).is_some();
    let input_bytes = if program { None } else { completion_input(peer, &prompt) };
    let prompt = peer.prompt(prompt);
    let reservation = if program {
        Some(run::Most::Unreserved)
    } else {
        match input_bytes {
            Some(bytes) => match session::reserve_completion(&mut domain.session, owner, bytes, 0, prompt.max_tokens) {
                Some(most) => Some(run::Most::Bounded(most)),
                None => None,
            },
            None => None,
        }
    };
    let Some(most) = reservation else {
        return session_step(domain, env, session::Event::BudgetDenied { owner, reason: session::BudgetDenial::Spend });
    };
    match run::reserve(&mut domain.run, conversation, owner, most) {
        Ok(()) => {}
        Err(exhausted) => {
            let reason = match exhausted {
                run::Exhausted::Turns => session::BudgetDenial::Turns,
                run::Exhausted::Time => session::BudgetDenial::Time,
                run::Exhausted::Spend | run::Exhausted::Tokens(_) | run::Exhausted::Overflow(_) => {
                    session::BudgetDenial::Spend
                }
            };
            return session_step(domain, env, session::Event::BudgetDenied { owner, reason });
        }
    }
    let mut renewable = true;
    let grant = match account {
        Some(account) => match domain.grants.get(&account) {
            Some(held) if held.expires > env.now => {
                renewable = held.renewable;
                Some(held.name)
            }
            Some(_) | None => return unsent_unauthorized(domain, env, conversation, owner),
        },
        None => None,
    };
    crate::operation::begin_completion(
        domain,
        owner,
        crate::domain::IssuedCompletion { renewable, grant, name },
        conversation,
        deadlines.whole,
        out,
    );
    out.push(Request::Complete {
        owner,
        name,
        grant,
        prompt,
        deadlines,
        max_completion_bytes,
        max_completion_blocks,
        max_failure_bytes,
        decoded_call_bytes: env.limits.decoded_call_bytes,
    });
}

/// Settle the reservation of a named account whose grant has lapsed.
fn unsent_unauthorized(domain: &mut Domain, env: &Env<Limits>, conversation: Token, owner: Token) {
    assert!(run::settle_reservation(&mut domain.run, conversation, owner, 0), "unsent call held its reservation");
    session_step(
        domain,
        env,
        session::Event::Failed {
            owner,
            retry_ok: true,
            failure: crate::llm::Failure::Unauthorized,
            evidence: crate::llm::Evidence::Unsent,
            detail: Box::default(),
        },
    );
}

/// Resolve the selected LLM through the charter's main-first numbering.
fn model_index(domain: &Domain, conversation: Token, model: &run::charter::Llm) -> u32 {
    let charter = run::conversation_charter(&domain.run, conversation).expect("run owns the conversation it opens");
    let children = u32::try_from(charter.models.len()).expect("admitted model count fits u32");
    for index in 0..=children {
        match charter.model(index) {
            Some(candidate) if candidate == model => return index,
            Some(_) | None => {}
        }
    }
    unreachable!("a conversation opens on one of its charter's LLMs")
}

/// Host rate/share translation and concrete restore admission precede effects.
/// Session alone prices, while token ceilings remain receiving limits.
fn open(domain: &mut Domain, env: &Env<Limits>, conversation: Token, opening: run::Opening) {
    let endpoint = opening.llm.endpoint;
    let model = model_index(domain, conversation, &opening.llm);
    let progress = domain.config.progress(&opening.llm);
    let allowance = domain.config.protocol_allowance(&opening.llm);
    let activation = opening.activation;
    let account = opening.llm.account;
    let dialect = opening.llm.dialect;
    let prices = opening.llm.prices;
    let budget = opening.budget.spend;
    let (transcript, answered) = match opening.transcript {
        Some(binding) => {
            let context = domain.starts.get_mut(Id::from_token(binding)).expect("root issued the restore binding");
            match context.refused.take() {
                Some(reason) => {
                    return run_step(
                        domain,
                        env,
                        run::Event::Ended {
                            conversation,
                            end: run::End::TranscriptRefused { reason },
                            spend: Spend::ZERO,
                            detail: Box::default(),
                        },
                    );
                }
                None => (context.transcript.take(), mem::replace(&mut context.answered, Box::new([]))),
            }
        }
        None => (None, Box::default()),
    };
    let window_llm = match resumed_model(&domain.config, &opening.llm, dialect, transcript.as_ref()) {
        Ok(llm) => llm,
        Err(reason) => {
            return run_step(
                domain,
                env,
                run::Event::Ended {
                    conversation,
                    end: run::End::TranscriptRefused { reason },
                    spend: Spend::ZERO,
                    detail: Box::default(),
                },
            );
        }
    };
    let Some((mut spec, offered)) =
        translate::spec(opening, env.limits.session.budget, progress, domain.config.program(endpoint))
    else {
        // Refused at the conversations' entrance, in the run's terms.
        let ended =
            run::Event::Ended { conversation, end: run::End::Invalid, spend: Spend::ZERO, detail: Box::default() };
        return run_step(domain, env, ended);
    };
    let Some(protocol_allowance) = spec.protocol_allowance.checked_add(allowance) else {
        return run_step(
            domain,
            env,
            run::Event::Ended { conversation, end: run::End::Invalid, spend: Spend::ZERO, detail: Box::default() },
        );
    };
    spec.protocol_allowance = protocol_allowance;
    if !answered.is_empty() {
        let Some(length) = answered.len().checked_add(spec.prompt.len()) else {
            return run_step(
                domain,
                env,
                run::Event::Ended {
                    conversation,
                    end: run::End::TranscriptRefused { reason: run::TranscriptRefusal::TooLarge },
                    spend: Spend::ZERO,
                    detail: Box::default(),
                },
            );
        };
        let mut prompt = List::with_capacity(u32::try_from(length).expect("admitted waking prompt length"));
        for byte in answered.iter().chain(spec.prompt.iter()) {
            prompt.push(*byte).expect("measured prompt length");
        }
        spec.prompt = prompt.into_boxed();
    }
    let peer = Peer::new(conversation, activation, account, model, offered, &env.limits.session);
    let id = domain.peers.insert(peer).expect("a peer for every conversation the run has");
    let fresh = domain.conversations.insert(conversation, id).expect("a peer for every conversation");
    assert!(fresh.is_none(), "the run names its conversations apart");
    let event = session::Event::Open {
        opener: conversation,
        opening: Box::new(session::record::Opening {
            spec,
            window_llm,
            dialect,
            prices: translate::prices(prices),
            budget,
            transcript,
            answered: Box::default(),
        }),
    };
    session_step(domain, env, event);
}

/// Select the configured model of a saved window only when main changes model.
fn resumed_model(
    config: &crate::Config,
    main: &run::charter::Llm,
    dialect: u32,
    transcript: Option<&session::record::Transcript>,
) -> Result<Option<session::record::Llm>, run::TranscriptRefusal> {
    let last = match transcript {
        Some(history) => {
            let Some(first) = history.turns.first() else { return Ok(None) };
            if history.endpoint != Some(session::llm::Endpoint(main.endpoint.0)) || history.dialect != dialect {
                return Ok(None);
            }
            match &first.kind {
                session::record::Kind::First(opener) | session::record::Kind::Window(opener) => {
                    if opener.account != main.account
                        || match &opener.backend {
                            session::record::Backend::Direct => config.program(main.endpoint).is_some(),
                            session::record::Backend::Program { name } => match config.program(main.endpoint) {
                                Some(program) => program.name != *name,
                                None => true,
                            },
                        }
                    {
                        return Ok(None);
                    }
                }
                session::record::Kind::Ordinary => return Ok(None),
            }
            history.turns.last()
        }
        None => None,
    };
    match last {
        Some(turn) if turn.model != main.model => match config.window_llm(main.endpoint, &turn.model) {
            Some(mut llm) => {
                llm.effort.clone_from(&turn.effort);
                Ok(Some(llm))
            }
            None => Err(run::TranscriptRefusal::Malformed),
        },
        Some(_) | None => Ok(None),
    }
}

/// The peer whose session's opener is `conversation`.
fn peer(domain: &Domain, conversation: Token) -> Id<Peer> {
    *domain.conversations.get(&conversation).expect("a session's opener is a conversation the run opened")
}

/// Frees a peer whose session has ended, and its tickets: its calls have all
/// been answered.
fn free(domain: &mut Domain, id: Id<Peer>) {
    let peer = domain.peers.get(id).expect("a peer lives as its session");
    domain.tickets = domain.tickets.checked_sub(peer.tickets()).expect("the peers' tickets are counted");
    let conversation = peer.conversation;
    if let Some(session) = peer.session {
        domain.sessions.remove(&session);
    }
    domain.conversations.remove(&conversation);
    domain.ready.forget(Handoff::Close { peer: id });
    domain.peers.retire(id);
}

fn granted(domain: &mut Domain, env: &Env<Limits>, grant: crate::Grant) {
    if let Some(old) = domain.grants.get(&grant.name.account)
        && old.name.generation >= grant.name.generation
    {
        return;
    }
    let valid = grant.valid.as_nanos().saturating_sub(env.limits.skew.as_nanos());
    let rejected = match domain.grants.get(&grant.name.account) {
        Some(old) => old.rejected,
        None => None,
    };
    let entry = Credential {
        name: grant.name,
        expires: env.now.saturating_add(Duration::from_nanos(valid)),
        rejected,
        renewable: grant.renewable,
    };
    if domain.grants.contains_key(&grant.name.account) || domain.grants.len() < env.limits.accounts {
        let inserted = domain.grants.insert(grant.name.account, entry);
        assert!(inserted.is_ok(), "known account or checked room");
    }
}

fn takes_grants(domain: &Domain, charter: &run::Charter, grants: &[crate::Grant], most: u32) -> bool {
    for llm in core::iter::once(&charter.llm).chain(charter.models.iter()) {
        if let Some(account) = llm.account {
            let mut present = domain.grants.contains_key(&account);
            for grant in grants {
                if grant.name.account == account {
                    present = true;
                }
            }
            if !present {
                return false;
            }
        }
    }
    if u32::try_from(grants.len()).unwrap_or(u32::MAX) > most {
        return false;
    }
    let mut adding = 0_u32;
    for (index, grant) in grants.iter().enumerate() {
        let mut seen = domain.grants.contains_key(&grant.name.account);
        for old in grants.get(..index).unwrap_or_default() {
            if old.name.account == grant.name.account {
                seen = true;
            }
        }
        if !seen {
            adding = adding.saturating_add(1);
        }
    }
    domain.grants.len().saturating_add(adding) <= most
}

fn was_rejected(credential: &Credential, generation: u64) -> bool {
    match credential.rejected {
        Some(reported) => reported >= generation,
        None => false,
    }
}

fn room_for_content(domain: &mut Domain, env: &Env<Limits>, bytes: u64) -> bool {
    if domain.content.room() == 0 || bytes > env.limits.session.history_bytes {
        domain.content_lost = domain.content_lost.saturating_add(1);
        false
    } else {
        true
    }
}

fn capture(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    name: crate::llm::CompletionName,
    completion: &crate::llm::Completion,
) {
    for said in &completion.content {
        match said {
            crate::llm::Said::Text { text, .. } | crate::llm::Said::Refusal { text, .. } => {
                if room_for_content(domain, env, crate::facts::bytes(text)) {
                    domain.content.push(crate::Content::Text { owner, completion: name, text: text.clone() });
                }
            }
            crate::llm::Said::ToolCall { id, name: tool, input, call: _, .. } => {
                let bytes = crate::facts::bytes(id)
                    .saturating_add(crate::facts::bytes(tool))
                    .saturating_add(crate::facts::bytes(input));
                if room_for_content(domain, env, bytes) {
                    domain.content.push(crate::Content::Call {
                        owner,
                        completion: name,
                        id: id.clone(),
                        name: tool.clone(),
                        input: input.clone(),
                    });
                }
            }
            crate::llm::Said::Opaque { .. } => {}
        }
    }
    if room_for_content(domain, env, 0) {
        domain.content.push(crate::Content::Usage { owner, completion: name, usage: completion.usage });
    }
}
