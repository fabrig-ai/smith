//! LLM sessions (domain/session.md, sections 3–7): a conversation with an LLM, driven turn by turn until the LLM
//! yields, a limit or the budget ends it, or its opener closes it.
//!
//! An `Open` opens a session, and its kit in the tools the session owns
//! (programming-model.md, section 4.5), with the spec's authority; then the session calls the LLM. While the
//! LLM asks for tools, the session runs them and sends their results back, in
//! call order, in another call. A call is to the session's own tools, which
//! answer it, at their entrance or once the operations they ask of io have
//! ended (the session passes those on as they are), or to one the opener
//! serves, delegated to it. Either kind runs in batches: adjacent calls that
//! read run together, up to `Limits::parallel_tools`, and a call that writes
//! runs alone. Each call carries its deadline, which whoever runs it races: a
//! call that runs out of time comes back as such, and goes to the LLM like any
//! other result. A call the protocol layer could not decode is answered with
//! its problem as it is reached, and nothing runs for it; a message with no
//! owned call goes straight back. When the LLM stops calling tools, the
//! session yields to its opener, which continues it with a new user message
//! or closes it; the message goes back after a result for each call the
//! yielded answer made but did not wait for.
//!
//! The session retains exact own and inclusive activation spend separately.
//! A charge that cannot fit ends the session before the completion's calls or
//! turn. An unsent root `BudgetDenied` releases the current
//! Calling reservation without inventing a provider lease or cancellation
//! (domain/session.md, section 6; domain/run.md, section 9).
//!
//! A session ends once, and only once nothing it asked for is in flight and
//! its kit has closed: closing cancels its call to the LLM, withdraws its
//! delegated calls and closes its kit, which cancels the tools' calls, and
//! waits for all of it to settle (programming-model.md, section 5.3).
//!
//! The transition table. Every other cell is unreachable by the boundary's
//! contract: one terminal event per request, a request only from the states
//! that wait for its terminal event, and a `Continue` only to a yielded
//! session.
//!
//! ```text
//! state     event or alarm             next      requests
//! (none)    open, admitted, kit opened Calling   opened, complete
//!           open, busy or invalid, or  (none)    ended: busy, invalid
//!             its kit refused
//! Calling   completed, tool use        Tooling   used, the first batch's runs
//!           ... all answered at once   Resting   used (on the ready list)
//!           ... and no calls after it  Calling   used, complete
//!           ... time up                Closing   used (its kit closes)
//!           completed, only invalid    Calling   used, complete
//!           completed, no calls        Yielded   used, yielded (malformed)
//!           completed, otherwise       Yielded   used, yielded
//!           completed, does not fit    Closing   used (its kit closes)
//!           failed, transient          Backoff
//!           failed, otherwise          Closing   (its kit closes)
//!           close, expiry              Closing   cancel
//! Backoff   retry                      Calling   complete
//!           close, expiry              Closing   (its kit closes)
//! Tooling   a run done, batch running  Tooling
//!           a run done, more calls     Tooling   the next batch's runs
//!           ... all answered at once   Resting   (on the ready list)
//!           ... and no calls after it  Calling   complete
//!           ... time up                Closing   (its kit closes)
//!           a run done, no more        Calling   complete
//!           a result that does not fit Closing   a withdraw per delegated run
//!           close, expiry              Closing   a withdraw per delegated run
//! Resting   resume                     as from a run done, more calls
//!           close, expiry              Closing   (its kit closes)
//! Yielded   continue                   Calling   complete
//!           continue, does not fit     Closing   (its kit closes)
//!           close, expiry              Closing   (its kit closes)
//! Closing   its kit closed, nothing    Closed    ended
//!             left to wait for
//!           completed                  Closing   used
//!           failed, cancelled          Closing
//!           a run ends                 Closing
//!           close                      Closing   (already closing)
//! Closed    continue, close            Closed    (dropped: the handle is stale)
//! ```
//!
//! "Time up" is the expiry the session checks itself before it starts a
//! batch ([`advance`]), for a completion or a run's end that comes in the
//! iteration its time runs out, before the alarm fires. "Does not fit" is a
//! message the transcript has no room for (see below).
//!
//! A run is done when the tools answer it or the opener does (answered); it
//! ends while closing when it is done all the same, or when the kit's cancel
//! or its withdraw wins (the tools answer it cancelled, answer cancelled).
//! Entering Closing closes the kit ([`settle`]): the tools cancel what they
//! run for it, and say it has closed once each call is answered.
//!
//! Answers within the step (the rule for any parent whose child may answer in
//! the step that gave it the work, as the tools do here and the run and the
//! sessions will at the top level). The tools answer some calls at their
//! entrance, without asking io anything (a path outside the checkout, a family
//! not granted, a file not read first). Were a parent to go on from such an
//! answer at once, starting the next piece of work, hand-offs would chain
//! within one step: what it emits would follow the length of the chain rather
//! than [`crate::max_out`], and what it holds for work ended in the iteration
//! (reclaimed only at the reclaim point) would outgrow its slabs. So a step
//! starts one batch at most, and a batch the tools answer entirely within the
//! step that started it rests (Resting): its results are kept, and the
//! session goes on the domain's ready list (programming-model.md, section 2),
//! held as its state rather than as a queued record. The loop drains the ready
//! list with [`crate::resume`] at the start of the domain's stage in a later
//! iteration, after the runs the batch ended have been reclaimed; a session
//! that rests while the list is drained waits for the next iteration, so the
//! chain never goes on before the reclaim point. A session therefore holds the
//! runs of two batches at most in an iteration (one ending and the next), and
//! each step, alarm and resume emits a bounded number of requests.
//!
//! Wherever the table calls the LLM (`complete`), the session first checks its
//! budget and its transcript, and ends instead: as out of budget, naming the
//! dimension, if its turns, input or output tokens are used up, if the last
//! completion took cache reads or writes past their budget, or if its time has
//! run out; and as transcript full if there is no room for the answer. So it
//! never starts a completion it may not pay for or could not keep, and the
//! turn that crossed a budget still runs its tools and keeps their results.
//! Only time does not wait: its expiry closes the session at once, and no
//! batch starts once it has passed. A completion, a result or a new message
//! that does not fit the byte limit ends the session as transcript full in
//! place of the transition it would have made, cancelling the rest of the
//! batch. A message is charged room for its calls' results with their ids
//! ([`held`]), and what a result holds besides as it arrives; so a refusal
//! at the tools' entrance, which holds nothing more, always fits, and a step
//! either starts a batch or cancels one, never both.
//!
//! The expiry alarm, set for when the time budget runs out, runs in every
//! state but Closing and Closed, and the retry alarm in Backoff; a session is
//! on the ready list while it is Resting. They follow from the state, in one
//! place ([`follow`]), which also retires a session once it is Closed.
//!
//! Every transition tells what happened as facts: the entry point tells of
//! the event it was given (a completion or a delegated call ending, a retry),
//! and [`tell`] of the requests the transition made, in one place; the tools
//! tell of their own calls, and the session passes their facts on.

#[path = "batching.rs"]
mod batching;
#[path = "builtin.rs"]
mod builtin;
#[path = "compaction.rs"]
mod compaction;
#[path = "history.rs"]
mod history;
#[path = "jobs.rs"]
mod jobs;

use alloc::boxed::Box;
use core::mem::{self, size_of};

use skein_lib::{Deadlines, Duration, Env, Id, List, Queue, ReplyTo, Rng, Set, Slab, Time, Token, Writer};
use smith_domain_tools::{self as tools, Call, Effect, Entry, Grants, Outcome};

use crate::boundary::{
    Budget, BudgetDenial, Deadlines as CallDeadlines, Dimension, End, Progress, Request, Spec, Yield,
};
use crate::domain::Domain;
use crate::facts::{FactKind, Facts};
use crate::limits::Limits;
use crate::llm::{
    Block, Completion, Decoded, Descriptor, Endpoint, Failure, Message, Problem, Prompt, Returned, Role, Stop,
    ToolChoice, Usage,
};

#[derive(Debug)]
pub(crate) struct Session {
    conversation: Conversation,
    state: State,
}

/// What a session holds in every state.
#[derive(Debug)]
struct Conversation {
    backend: crate::record::Backend,
    gone_window: Option<u32>,
    account: Option<u32>,
    protocol_allowance: u64,
    /// The opener's token, echoed on every record back to it.
    opener: Token,
    endpoint: Endpoint,
    affinity: crate::llm::Affinity,
    progress: Progress,
    model: Box<[u8]>,
    effort: Option<Box<[u8]>>,
    next_llm: Option<crate::record::Llm>,
    system: Box<[u8]>,
    /// The families of its own tools it offers the LLM, as its kit has them.
    tools: Grants,
    namespace: tools::Namespace,
    delegated: Box<[Descriptor]>,
    choice: ToolChoice,
    choice_names: Option<Box<[Box<[u8]>]>>,
    final_turn: crate::final_turn::Final,
    output: u32,
    model_window: u32,
    /// Run-derived line, in tokens, held across a model swap.
    line: u64,
    window: crate::window::Window,
    compaction: compaction::Compaction,
    task: Box<[u8]>,
    summary: Box<[u8]>,
    preamble: Box<[u8]>,
    window_turn: bool,
    /// The conversation so far, oldest first, starting with the spec's prompt.
    transcript: List<Message>,
    /// Bytes held, counted against `Limits::history_bytes`.
    bytes: u64,
    /// Logical space owed to provider and tool terminals.
    reserved: u64,
    provider_credit: Option<u64>,
    /// One-based attempt of the last issued completion, retained through close.
    completion_attempts: u32,
    /// What the session may spend, and what it has: completions received, and
    /// the tokens they used.
    budget: Budget,
    turns: u32,
    usage: Usage,
    /// Checked sums of supplied counts; each stays absent until reported once.
    /// Token budgets count these sums independently of missing reports.
    reported: Usage,
    /// When the time budget runs out.
    expires: Time,
    /// Its kit in the tools it owns, as the tools name it. A session is
    /// inserted Closed, holding nothing, and given its kit as it opens.
    kit: Token,
    recording: Recording,
    jobs: jobs::Jobs,
    builtin: builtin::Owed,
    closing_tools: Option<Tools>,
    /// Last failure text, bounded by `Limits::failure_bytes` and kept outside history.
    detail: Box<[u8]>,
}

#[derive(Debug)]
struct Recording {
    dialect: u32,
    prices: crate::record::Prices,
    budget: u64,
    spent: u64,
    own_spent: u64,
    sequence: u32,
    told: u32,
    pending: Option<Usage>,
    /// Maximum spend accepted by the parent before sending this attempt.
    reservation: Option<u64>,
    /// Actual charge of the most recently accepted completion, for its fact.
    last_charge: u64,
}

#[derive(Debug)]
enum State {
    /// A call is in flight, after `attempt` retries.
    Calling { attempt: u32 },
    /// The last call failed transiently: calling again at `until`.
    Backoff { attempt: u32, until: Time },
    /// Running the tool calls of the last assistant message.
    Tooling { tools: Tools },
    /// The tools answered every call of the last batch in the step that
    /// started it: the session is on the ready list, and the next batch
    /// starts once the runs it ended have been reclaimed.
    Resting { tools: Tools },
    /// The LLM stopped calling tools: waiting for the opener to continue or
    /// close the session.
    Yielded,
    /// Ending with `end`, once what it waits for has settled, its kit's close
    /// among it.
    Closing { end: End, waiting: Waiting },
    /// Terminal: holds nothing.
    Closed,
}

/// What a closing session waits for: the terminal event of its call, and of
/// each of its tool runs still `runs`, and its kit's close.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Waiting {
    call: bool,
    runs: u32,
    kit: Kit,
}

/// A closing session's kit: open until the session closes it as it settles
/// ([`settle`]), closing, its calls cancelled, or closed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kit {
    Open,
    Closing,
    Closed,
}

/// What a session waits for once nothing it asked for is in flight but its
/// kit, which it closes.
const KIT: Waiting = Waiting { call: false, runs: 0, kit: Kit::Open };

/// A closing session that waits for nothing: it has ended.
const SETTLED: Waiting = Waiting { call: false, runs: 0, kit: Kit::Closed };

/// The tool calls of the last assistant message, run in batches: adjacent
/// calls that read, up to `Limits::parallel_tools` of them, run together, and
/// one that writes runs alone.
#[derive(Debug)]
struct Tools {
    /// What follows the complete settled tool batch, requested by its opener.
    after: After,
    /// A slot for each call reached, in call order, with room for them all.
    slots: List<Slot>,
    /// The runs of the batch still in flight.
    running: u32,
    /// The block where the calls not reached yet begin.
    next: u32,
    /// The first failed call, counted from one among all calls.
    failed: Option<u16>,
    reached: u32,
}

#[derive(Debug)]
pub(crate) enum Slot {
    /// The call is running as `run`.
    Running { run: Id<Run> },
    /// The call has its result.
    Done { result: Block },
    /// An unstarted regular call, or a wait the opener parked without a historical result.
    Withheld,
}

/// A tool call running for a session, named by its own token: the call at
/// `block` of the session's last message, which fills `slot`, run `by` the
/// tools or the opener.
#[derive(Debug)]
pub(crate) struct Run {
    ended: bool,
    session: Id<Session>,
    slot: u32,
    block: u32,
    by: By,
    credit: u64,
    metadata: crate::ToolCall,
}

/// Who runs a call: the tools, or the opener, which serves a delegated one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum By {
    Tools,
    Opener,
    Program,
}

/// What runs the sessions' tool calls: the runs, the tools child domain, which
/// the session owns (programming-model.md, section 4.5), and room for what the tools emit in a step.
#[derive(Debug)]
pub(crate) struct Calls {
    pub(crate) runs: Slab<Run>,
    pub(crate) tools: tools::Domain,
    pub(crate) out: Queue<tools::Request>,
}

/// A session's timers, named by what they are for.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Alarm {
    Expiry { session: Id<Session> },
    Retry { session: Id<Session> },
}

/// The sessions that rest (programming-model.md, section 2), each at most once:
/// those that rested before the last reclaim point, which [`crate::resume`]
/// starts again, and those that rested since, which wait for the next.
#[derive(Debug)]
pub(crate) struct Ready {
    now: Set<Id<Session>>,
    next: Set<Id<Session>>,
}

impl Ready {
    pub(crate) const fn with_capacity(sessions: u32) -> Ready {
        Ready { now: Set::with_capacity(sessions), next: Set::with_capacity(sessions) }
    }

    /// The most heap the list takes for `sessions` sessions, or `None` past a
    /// `u64`.
    pub(crate) fn worst_case(sessions: u32) -> Option<u64> {
        Set::<Id<Session>>::worst_case(sessions)?.checked_mul(2)
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.now.is_empty()
    }

    /// Takes a session that may be started again now.
    pub(crate) fn pop(&mut self) -> Option<Id<Session>> {
        let id = *self.now.first()?;
        self.now.remove(&id);
        Some(id)
    }

    /// Keeps the session `id` on the list if it is `resting`, and off it if
    /// not.
    fn keep(&mut self, id: Id<Session>, resting: bool) {
        if !resting {
            self.now.remove(&id);
            self.next.remove(&id);
        } else if !self.now.contains(&id) {
            self.next.insert(id).expect("room on the ready list for every session");
        }
    }

    /// The reclaim point: what rested in this iteration may be started again
    /// in the next.
    pub(crate) fn promote(&mut self) {
        for _ in 0..self.next.capacity() {
            let Some(id) = self.next.first().copied() else {
                break;
            };
            self.next.remove(&id);
            self.now.insert(id).expect("room on the ready list for every session");
        }
    }
}

// Entry points, one per event or alarm: look the session up, take its state
// out, run the cell's handler, then settle, tell what happened and follow the
// new state ([`conclude`]).

fn open_admitted(
    domain: &mut Domain,
    env: &Env<Limits>,
    opener: Token,
    conversation: Conversation,
    authority: tools::Authority,
    out: &mut Queue<Request>,
    mark: u32,
) {
    // Closed until its kit opens and the first call is made, which a budget
    // just admitted pays for.
    let highest = conversation.jobs.highest;
    let session = Session { conversation, state: State::Closed };
    let id = domain.sessions.insert(session).expect("checked for room above");
    let heard =
        tools_step(&mut domain.calls, env, tools::Event::Open { session: id.token(), jobs: highest, authority }, out);
    let session = domain.sessions.get_mut(id).expect("inserted above");
    match heard.kit {
        Some(News::Opened { kit }) => {
            session.conversation.kit = kit;
            out.push(Request::Opened { opener, session: id.token() });
            session.state = call(&mut session.conversation, id, 0, env, out);
        }
        // Refused at the tools' entrance: the session never opened.
        Some(News::Refused { refusal }) => {
            let end = match refusal {
                tools::Refusal::Busy => End::Busy,
                tools::Refusal::Invalid => End::Invalid,
            };
            out.push(Request::Ended {
                opener,
                end,
                turns: 0,
                usage: Usage::ZERO,
                reported: Usage::NONE,
                detail: Box::default(),
            });
        }
        Some(News::Closed { .. }) | None => unreachable!("the tools answer an open with its kit, opened or refused"),
    }
    conclude(domain, env, id, out, mark);
}

pub(crate) fn continued(
    domain: &mut Domain,
    env: &Env<Limits>,
    session: Token,
    content: Box<[u8]>,
    out: &mut Queue<Request>,
) {
    let mark = out.len();
    let Some(id) = addressed(&domain.sessions, session) else {
        return;
    };
    let session = domain.sessions.get_mut(id).expect("addressed above");
    let state = mem::replace(&mut session.state, State::Closed);
    let conversation = &mut session.conversation;
    session.state = match state {
        State::Yielded => resumed(conversation, id, content, env, out),
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Closing { .. } => unreachable!("an opener continues a session only while it is yielded"),
        State::Closed => unreachable!("an addressed session has not ended"),
    };
    conclude(domain, env, id, out, mark);
}

pub(crate) use jobs::cancel as cancel_jobs;

/// Relay the run's one yield request only while this session owes a completion.
pub(crate) fn yield_completion(domain: &Domain, session: Token, out: &mut Queue<Request>) {
    let Some(id) = addressed(&domain.sessions, session) else {
        return;
    };
    match domain.sessions.get(id).expect("addressed session").state {
        State::Calling { .. } => out.push(Request::Yield { owner: id.token() }),
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => {}
    }
}

pub(crate) fn close(domain: &mut Domain, env: &Env<Limits>, session: Token, out: &mut Queue<Request>) {
    let mark = out.len();
    let Some(id) = addressed(&domain.sessions, session) else {
        return;
    };
    let session = domain.sessions.get_mut(id).expect("addressed above");
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Calling { attempt: _ } => cancel_call(id, End::Closed, out),
        State::Resting { tools } => {
            session.conversation.closing_tools = Some(tools);
            finish(End::Closed)
        }
        State::Backoff { .. } | State::Yielded => finish(End::Closed),
        State::Tooling { tools } => {
            cancel_tools(&mut session.conversation, &domain.calls.runs, tools, End::Closed, out)
        }
        // It is already ending, with the end it had first.
        State::Closing { end, waiting } => State::Closing { end, waiting },
        State::Closed => unreachable!("an addressed session has not ended"),
    };
    conclude(domain, env, id, out, mark);
}

pub(crate) fn completed(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    mut completion: Completion,
    out: &mut Queue<Request>,
) {
    let mark = out.len();
    let id = Id::from_token(owner);
    let session = domain.sessions.get_mut(id).expect("a session lives until its requests have ended");
    builtin::forget(&mut session.conversation);
    builtin::flush(&mut session.conversation, &mut domain.facts);
    if let Some(end) = tell_turn(&mut session.conversation, out) {
        session.state = finish(end);
    }
    let (opener, stop, blocks) = (session.conversation.opener, completion.stop, count(completion.content.len()));
    let (calls, invalid) = tally(&completion.content);
    let reasoning_dropped = mem::replace(&mut completion.reasoning_dropped, Box::new([]));
    assert!(
        reasoning_dropped.len() <= usize::try_from(env.limits.completion_blocks).expect("u32 fits usize"),
        "drop metadata obeys completion block receiving cap"
    );
    for bytes in &reasoning_dropped {
        domain.facts.push_response(
            FactKind::ReasoningDropped { opener, bytes: *bytes },
            next_response(&session.conversation),
            None,
        );
    }

    domain.facts.push_response(
        FactKind::CompletionAnswered { opener, stop, blocks, calls, invalid },
        next_response(&session.conversation),
        None,
    );
    let state = mem::replace(&mut session.state, State::Closed);
    let conversation = &mut session.conversation;
    session.state = match state {
        State::Calling { attempt: _ } => answered(conversation, id, &mut domain.calls, completion, env, out),
        State::Closing { end, waiting: waiting @ Waiting { call: true, .. } } => {
            answered_late(conversation, id, &mut domain.calls, end, waiting, completion, &env.limits, out)
        }
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { waiting: Waiting { call: false, .. }, .. }
        | State::Closed => unreachable!("a completion ends a call in flight"),
    };
    conclude(domain, env, id, out, mark);
}

/// Check one completion before the parent decodes any of its calls. The same
/// arithmetic is repeated when the session commits the completion.
pub fn preview_completion(domain: &Domain, owner: Token, usage: Usage) -> Result<u64, End> {
    let session = domain.sessions.get(Id::from_token(owner)).expect("completion retains its session");
    let conversation = &session.conversation;
    let price = completion_price(&conversation.recording, usage).ok_or(End::PriceOverflow)?;
    conversation.recording.own_spent.checked_add(price).ok_or(End::PriceOverflow)?;
    conversation.recording.spent.checked_add(price).ok_or(End::PriceOverflow)?;
    checked_usage(conversation.usage, usage)?;
    checked_reported(conversation.reported, usage).ok_or(End::UsageOverflow)?;
    conversation.turns.checked_add(1).ok_or(End::UsageOverflow)?;
    Ok(price)
}

/// Check the maximum completion charge against this session's spend allowance.
#[must_use]
pub fn preview_reservation(
    domain: &Domain,
    owner: Token,
    input_bytes: u64,
    allowance: u64,
    max_tokens: u32,
) -> Option<u64> {
    let session = domain.sessions.get(Id::from_token(owner)).expect("completion retains its session");
    reservation(&session.conversation, input_bytes, allowance, max_tokens)
}

/// Record the parent's accepted maximum before provider work. Missing input or
/// output later charges this entire reservation (domain/session.md, section 6).
pub fn reserve_completion(
    domain: &mut Domain,
    owner: Token,
    input_bytes: u64,
    allowance: u64,
    max_tokens: u32,
) -> Option<u64> {
    let session = domain.sessions.get_mut(Id::from_token(owner)).expect("completion retains its session");
    let most = reservation(&session.conversation, input_bytes, allowance, max_tokens)?;
    session.conversation.recording.reservation = Some(most);
    Some(most)
}

fn reservation(conversation: &Conversation, input_bytes: u64, allowance: u64, max_tokens: u32) -> Option<u64> {
    let input_tokens = input_bytes.checked_add(allowance)?;
    let prices = conversation.recording.prices;
    let prices = crate::record::Prices { input: prices.reserved_input(), ..prices };
    let most = prices.price(Usage {
        input_tokens: Some(input_tokens),
        output_tokens: Some(u64::from(max_tokens)),
        ..Usage::ZERO
    })?;
    let total = conversation.recording.spent.checked_add(most)?;
    if total > conversation.recording.budget {
        return None;
    }
    Some(most)
}

fn completion_price(recording: &Recording, usage: Usage) -> Option<u64> {
    if usage.input_tokens.is_none() || usage.output_tokens.is_none() {
        return recording.reservation;
    }
    recording.prices.price(usage)
}

/// Settle a provider completion rejected by the parent's run-wide arithmetic
/// check. Its calls and turn never enter the session.
pub(crate) fn overflowed(domain: &mut Domain, env: &Env<Limits>, owner: Token, end: End, out: &mut Queue<Request>) {
    match end {
        End::PriceOverflow | End::UsageOverflow => {}
        End::Busy
        | End::Invalid
        | End::Closed
        | End::TranscriptRefused { .. }
        | End::Failed { .. }
        | End::NoSummary { .. }
        | End::Budget { .. } => unreachable!("run-wide arithmetic rejects only overflow"),
    }
    let mark = out.len();
    let id = Id::from_token(owner);
    let session = domain.sessions.get_mut(id).expect("provider terminal retains its session");
    release_provider(&mut session.conversation);
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Calling { .. } => finish(end),
        State::Closing { waiting: Waiting { call: true, runs, kit }, .. } => {
            State::Closing { end, waiting: Waiting { call: false, runs, kit } }
        }
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { waiting: Waiting { call: false, .. }, .. }
        | State::Closed => {
            unreachable!("only an in-flight provider completion can overflow")
        }
    };
    conclude(domain, env, id, out, mark);
}

/// The root refused the current requested completion before any provider start.
/// Its reservation is released and only the kit closes. No accepted completion,
/// retry or external cancellation is invented; stale/duplicate denials are inert.
pub(crate) fn budget_denied(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    reason: BudgetDenial,
    out: &mut Queue<Request>,
) {
    let spent = match reason {
        BudgetDenial::Turns => Dimension::Turns,
        BudgetDenial::Spend => Dimension::Unit,
        BudgetDenial::Time => Dimension::Time,
    };
    unsent_finished(domain, env, owner, End::Budget { spent }, out);
}

/// The enclosing run closed before publishing this requested completion.
/// Only its current Calling reservation settles Closed; no provider lease,
/// cancel or completion evidence is fabricated. Closing calls still
/// require their terminal.
pub(crate) fn unsent_closed(domain: &mut Domain, env: &Env<Limits>, owner: Token, out: &mut Queue<Request>) {
    unsent_finished(domain, env, owner, End::Closed, out);
}

/// Replace a current unsent Complete with one carrying the final-turn note.
pub(crate) fn narrowed(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    note: Box<[u8]>,
    choice: ToolChoice,
    names: Option<Box<[Box<[u8]>]>>,
    out: &mut Queue<Request>,
) {
    let Some(id) = addressed(&domain.sessions, owner) else {
        return;
    };
    let session = domain.sessions.get_mut(id).expect("addressed above");
    match session.state {
        State::Calling { .. } => {}
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => return,
    }
    let mark = out.len();
    let conversation = &mut session.conversation;
    release_provider(conversation);
    if !valid_choice(&choice, &conversation.delegated) {
        session.state = finish(End::Invalid);
        conclude(domain, env, id, out, mark);
        return;
    }
    let old = ticket_bytes(&conversation.choice)
        .checked_add(choice_bytes(conversation.choice_names.as_deref()))
        .expect("retained choice");
    let new = ticket_bytes(&choice).checked_add(choice_bytes(names.as_deref())).expect("bounded names");
    conversation.bytes = conversation.bytes.checked_sub(old).expect("retained choice counted");
    conversation.choice = ToolChoice::Auto;
    conversation.choice_names = None;
    let cost = match new.checked_add(u64::try_from(size_of::<Block>()).expect("block cell fits")) {
        Some(cost) => cost.checked_add(u64::try_from(note.len()).expect("note length fits")),
        None => None,
    };
    session.state = if conversation.compaction.asking()
        || conversation.transcript.room() == 0
        || !charge(conversation, cost, &env.limits)
    {
        conversation.choice = choice;
        conversation.choice_names = names;
        match history::hold_note(conversation, note, &env.limits) {
            Some(end) => finish(end),
            None => call(conversation, id, 0, env, out),
        }
    } else {
        conversation.choice = choice;
        conversation.choice_names = names;
        let content = Box::new([Block::Text { text: note, replay: None }]);
        conversation.transcript.push(Message { role: Role::User, content }).expect("note slot checked");
        call(conversation, id, 0, env, out)
    };
    conclude(domain, env, id, out, mark);
}

fn valid_choice(choice: &ToolChoice, offered: &[Descriptor]) -> bool {
    match choice {
        ToolChoice::Auto | ToolChoice::None => true,
        ToolChoice::Only(tickets) => {
            if tickets.is_empty() || tickets.len() > offered.len() {
                return false;
            }
            for (index, ticket) in tickets.iter().enumerate() {
                let mut present = false;
                for descriptor in offered {
                    if descriptor.ticket == *ticket {
                        present = true;
                    }
                }
                if !present || tickets.get(..index).expect("enumerated prefix").contains(ticket) {
                    return false;
                }
            }
            true
        }
    }
}

fn ticket_bytes(choice: &ToolChoice) -> u64 {
    match choice {
        ToolChoice::Auto | ToolChoice::None => 0,
        ToolChoice::Only(tickets) => u64::try_from(tickets.len())
            .expect("bounded descriptors")
            .checked_mul(u64::try_from(size_of::<Token>()).expect("token cell fits"))
            .expect("admitted descriptor bound fits"),
    }
}

fn choice_bytes(choice: Option<&[Box<[u8]>]>) -> u64 {
    let mut bytes = 0_u64;
    if let Some(names) = choice {
        for name in names {
            let cell = u64::try_from(size_of::<Box<[u8]>>()).expect("name cell fits");
            bytes = bytes.checked_add(cell).expect("admitted names fit");
            bytes = bytes.checked_add(u64::try_from(name.len()).expect("bounded name")).expect("admitted names fit");
        }
    }
    bytes
}

/// Settles a current reserved but unpublished provider request. Other states
/// retain their terminal obligations and all stale identities are inert.
fn unsent_finished(domain: &mut Domain, env: &Env<Limits>, owner: Token, end: End, out: &mut Queue<Request>) {
    let Some(id) = addressed(&domain.sessions, owner) else {
        return;
    };
    let session = domain.sessions.get_mut(id).expect("addressed above");
    match &session.state {
        State::Calling { .. } => {}
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => return,
    }
    let mark = out.len();
    release_provider(&mut session.conversation);
    session.state = finish(end);
    conclude(domain, env, id, out, mark);
}

#[expect(clippy::too_many_arguments, reason = "the terminal carries its issued credential retry eligibility")]
pub(crate) fn failed(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    failure: Failure,
    evidence: crate::llm::Evidence,
    detail: Box<[u8]>,
    retry_ok: bool,
    out: &mut Queue<Request>,
) {
    let mark = out.len();
    let id = Id::from_token(owner);
    let session = domain.sessions.get_mut(id).expect("a session lives until its requests have ended");
    assert!(
        detail.len() <= usize::try_from(env.limits.failure_bytes).expect("receiving cap fits"),
        "actual adapter terminal obeys failure receiving cap"
    );
    builtin::forget(&mut session.conversation);
    builtin::flush(&mut session.conversation, &mut domain.facts);
    if let Some(end) = tell_turn(&mut session.conversation, out) {
        session.state = finish(end);
    }
    session.conversation.detail = detail;
    release_provider(&mut session.conversation);
    let opener = session.conversation.opener;
    domain.facts.push_response(
        FactKind::CompletionFailed { opener, failure, evidence },
        next_response(&session.conversation),
        None,
    );
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Calling { attempt } => {
            if failure == Failure::SessionGone {
                match compaction::session_gone(&mut session.conversation) {
                    Some(end) => finish(end),
                    None => call(&mut session.conversation, id, 0, env, out),
                }
            } else if compaction::context_failure(failure) {
                match compaction::start(
                    &mut session.conversation,
                    &env.limits,
                    evidence,
                    attempt.checked_add(1).expect("an actual completion has a one-based attempt"),
                ) {
                    Some(end) => finish(end),
                    None => call(&mut session.conversation, id, 0, env, out),
                }
            } else {
                call_failed(attempt, failure, evidence, retry_ok, &session.conversation, &mut domain.rng, env)
            }
        }
        State::Closing { end, waiting: waiting @ Waiting { call: true, .. } } => {
            State::Closing { end, waiting: Waiting { call: false, ..waiting } }
        }
        State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { waiting: Waiting { call: false, .. }, .. }
        | State::Closed => unreachable!("a failure ends a call in flight"),
    };
    // A retry is told with its wait: no request goes out until then.
    match &session.state {
        State::Backoff { attempt, until } => {
            let delay = until.saturating_since(env.now);
            domain.facts.push_response(
                FactKind::CompletionRetried { opener, attempt: *attempt, delay },
                next_response(&session.conversation),
                None,
            );
        }
        State::Calling { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => {}
    }
    conclude(domain, env, id, out, mark);
}

pub(crate) fn cancelled(domain: &mut Domain, env: &Env<Limits>, owner: Token, out: &mut Queue<Request>) {
    let mark = out.len();
    let id = Id::from_token(owner);
    let session = domain.sessions.get_mut(id).expect("a session lives until its requests have ended");
    release_provider(&mut session.conversation);
    domain.facts.push_response(
        FactKind::CompletionCancelled { opener: session.conversation.opener },
        next_response(&session.conversation),
        None,
    );
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Closing { end, waiting: waiting @ Waiting { call: true, .. } } => {
            State::Closing { end, waiting: Waiting { call: false, ..waiting } }
        }
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { waiting: Waiting { call: false, .. }, .. }
        | State::Closed => unreachable!("a cancellation answers the cancel of a call, sent on the way to Closing"),
    };
    conclude(domain, env, id, out, mark);
}

/// The parent opener whose live tools job issued this IO operation.
pub(crate) fn io_opener(domain: &Domain, owner: Token) -> Option<Token> {
    let session = domain.calls.tools.io_session(owner)?;
    Some(domain.sessions.get(Id::from_token(session))?.conversation.opener)
}

/// An operation the tools asked of io has ended: the tools take it, and it may
/// answer one of their calls, or end a closing kit with its last.
pub(crate) fn io_done(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    done: tools::Done,
    out: &mut Queue<Request>,
) {
    let mark = out.len();
    let heard = tools_step(&mut domain.calls, env, tools::Event::Done { owner, done }, out);
    let mut concerned = None;
    if let Some((run, result)) = heard.answer {
        concerned = Some(owned_answered(domain, env, run, result, out));
    }
    match heard.kit {
        Some(News::Closed { session }) => {
            kit_closed(&mut domain.sessions, session);
            assert!(concerned.is_none() || concerned == Some(session), "a step of the tools concerns one kit");
            concerned = Some(session);
        }
        None => {}
        Some(News::Opened { .. } | News::Refused { .. }) => unreachable!("an operation's end opens no kit"),
    }
    jobs::cancelled(domain, heard.cancelled, out);
    if let Some(id) = concerned {
        conclude(domain, env, id, out, mark);
    }
}

/// The tools answered the call of the run `run`: Tooling, the result goes in
/// its slot; Closing, one run fewer to wait for. Returns the run's session.
fn owned_answered(
    domain: &mut Domain,
    env: &Env<Limits>,
    run: Id<Run>,
    result: Returned,
    out: &mut Queue<Request>,
) -> Id<Session> {
    let Run { session: id, slot, block, by, credit, metadata, ended: _ } = ended_run(&mut domain.calls.runs, run);
    assert!(by == By::Tools, "the tools answer only the calls they were given");
    let session = domain.sessions.get_mut(id).expect("a session lives until its requests have ended");
    let state = mem::replace(&mut session.state, State::Closed);
    let conversation = &mut session.conversation;
    conversation.jobs.answered(&result, metadata);
    session.state = match state {
        State::Tooling { tools } => {
            tool_ran(conversation, id, &mut domain.calls, tools, slot, block, credit, result, env, out)
        }
        State::Closing { end, waiting } => {
            let end = keep_closing(conversation, slot, block, credit, result, &env.limits).unwrap_or(end);
            settled(end, waiting)
        }
        State::Calling { .. } | State::Backoff { .. } | State::Resting { .. } | State::Yielded | State::Closed => {
            unreachable!("an answer ends a tool run in flight")
        }
    };
    id
}

/// The kit of `id`, closing, has closed.
fn kit_closed(sessions: &mut Slab<Session>, id: Id<Session>) {
    let session = sessions.get_mut(id).expect("a session lives until its kit has closed");
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Closing { end, waiting: waiting @ Waiting { kit: Kit::Closing, .. } } => {
            State::Closing { end, waiting: Waiting { kit: Kit::Closed, ..waiting } }
        }
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { waiting: Waiting { kit: Kit::Open | Kit::Closed, .. }, .. }
        | State::Closed => unreachable!("a kit closes when its session closes it"),
    };
}

pub(crate) fn expire(domain: &mut Domain, env: &Env<Limits>, id: Id<Session>, out: &mut Queue<Request>) {
    let mark = out.len();
    let session = domain.sessions.get_mut(id).expect("an alarm is cancelled before its session closes");
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Calling { attempt: _ } => cancel_call(id, OUT_OF_TIME, out),
        State::Resting { tools } => {
            session.conversation.closing_tools = Some(tools);
            finish(OUT_OF_TIME)
        }
        State::Backoff { .. } | State::Yielded => finish(OUT_OF_TIME),
        State::Tooling { tools } => {
            cancel_tools(&mut session.conversation, &domain.calls.runs, tools, OUT_OF_TIME, out)
        }
        State::Closing { .. } | State::Closed => {
            unreachable!("the expiry alarm runs only in Calling, Backoff, Tooling, Resting and Yielded")
        }
    };
    conclude(domain, env, id, out, mark);
}

pub(crate) fn retry(domain: &mut Domain, env: &Env<Limits>, id: Id<Session>, out: &mut Queue<Request>) {
    let mark = out.len();
    let session = domain.sessions.get_mut(id).expect("an alarm is cancelled before its session closes");
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Backoff { attempt, until: _ } => call(&mut session.conversation, id, attempt, env, out),
        State::Calling { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => unreachable!("the retry alarm runs only in Backoff"),
    };
    conclude(domain, env, id, out, mark);
}

/// Off the ready list: the session rested after a batch the tools answered at
/// once, and starts the next.
pub(crate) fn rested(domain: &mut Domain, env: &Env<Limits>, id: Id<Session>, out: &mut Queue<Request>) {
    let mark = out.len();
    let session = domain.sessions.get_mut(id).expect("an alarm is cancelled before its session closes");
    let state = mem::replace(&mut session.state, State::Closed);
    let conversation = &mut session.conversation;
    session.state = match state {
        State::Resting { tools } => advance(conversation, id, &mut domain.calls, tools, env, out),
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Yielded
        | State::Closing { .. }
        | State::Closed => unreachable!("a session is ready only while it rests"),
    };
    conclude(domain, env, id, out, mark);
}

/// Passes the tools' facts on among the session's own, as the tools' parent,
/// each with the opener of the session whose kit it is of. Each pass drains
/// the tools' queue, which holds no more facts than it takes, so a fact goes
/// on at the end of the entry point that told it, while its session is there
/// still: a session is reclaimed at the reclaim point at the earliest.
pub(crate) fn pass_on_facts(domain: &mut Domain, env: &Env<Limits>) {
    for _ in 0..env.limits.tools.facts {
        let Some(fact) = domain.calls.tools.pop_fact() else {
            break;
        };
        let session = domain.sessions.get(Id::from_token(kit_session(fact.kind)));
        let opener = session.expect("a kit's session lives until the reclaim point").conversation.opener;
        if jobs::pass_on(domain, opener, &fact) {
            continue;
        }
        let kind = FactKind::Tools { opener, fact: fact.kind };
        match fact.call {
            Some(call) => {
                let run = domain
                    .calls
                    .runs
                    .get(Id::from_token(call.owner))
                    .expect("observed call lives through the reclaim point");
                domain.facts.push_call(fact.at, kind, &run.metadata);
            }
            None => domain.facts.push_at(fact.at, kind),
        }
    }
}

/// The session, as the tools name it, whose kit `fact` is of.
const fn kit_session(fact: tools::FactKind) -> Token {
    match fact {
        tools::FactKind::Opened { session }
        | tools::FactKind::Refused { session, .. }
        | tools::FactKind::Started { session, .. }
        | tools::FactKind::Answered { session, .. }
        | tools::FactKind::Closing { session, .. }
        | tools::FactKind::JobEnded { session, .. }
        | tools::FactKind::Closed { session } => session,
    }
}

/// The run `run` names, whose terminal event has come: retired, and copied
/// out.
fn ended_run(runs: &mut Slab<Run>, run: Id<Run>) -> Run {
    let found = runs.get_mut(run).expect("a run lives until its terminal event");
    assert!(!found.ended, "one terminal per run");
    found.ended = true;
    let ended = Run {
        session: found.session,
        slot: found.slot,
        block: found.block,
        by: found.by,
        credit: found.credit,
        ended: true,
        metadata: found.metadata.clone(),
    };
    runs.retire(run);
    ended
}

/// What a step of the tools gave back: the answer to a call, if one came, and
/// news of a kit.
struct Heard {
    answer: Option<(Id<Run>, Returned)>,
    kit: Option<News>,
    cancelled: Option<Token>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum News {
    Opened { kit: Token },
    Refused { refusal: tools::Refusal },
    Closed { session: Id<Session> },
}

/// Steps the tools the session owns with `event`. The operations they ask of
/// io go out as the session's own requests, as they are, and so do their
/// cancels; what is for the session comes back. One step of the tools answers
/// at most one call: an open, a close or an operation's end concerns one kit,
/// and a call is answered at the entrance or later.
fn tools_step(calls: &mut Calls, env: &Env<Limits>, event: tools::Event, out: &mut Queue<Request>) -> Heard {
    assert!(calls.tools.facts_room() >= tools::max_facts(&env.limits.tools), "session reserved child observations");
    let tools_env = Env { now: env.now, wall: env.wall, limits: env.limits.tools };
    tools::step(&mut calls.tools, &tools_env, event, &mut calls.out);
    let mut heard = Heard { answer: None, kit: None, cancelled: None };
    for _ in 0..tools::max_out(&env.limits.tools) {
        let Some(request) = calls.out.pop() else {
            break;
        };
        match request {
            tools::Request::Io { owner, op, deadline } => out.push(Request::Io { owner, op, deadline }),
            tools::Request::Job { owner, act } => out.push(Request::Job { owner, act }),
            tools::Request::CancelIo { owner } => out.push(Request::CancelIo { owner }),
            tools::Request::Answer { to, outcome } => {
                assert!(heard.answer.is_none(), "a step of the tools answers one call at most");
                heard.answer = Some((Id::from_token(to.into_token()), Returned::Owned { outcome }));
            }
            tools::Request::Invalid { to, problem } => {
                assert!(heard.answer.is_none(), "a step of the tools answers one call at most");
                let problem = match problem {
                    tools::JobProblem::Full { jobs } => Problem::JobsFull { jobs },
                    tools::JobProblem::Unknown { job } => Problem::UnknownJob { job: job.0 },
                };
                heard.answer = Some((Id::from_token(to.into_token()), Returned::Invalid { problem }));
            }
            tools::Request::Opened { session: _, kit } => heard.kit = Some(News::Opened { kit }),
            tools::Request::Refused { session: _, refusal } => heard.kit = Some(News::Refused { refusal }),
            tools::Request::Closed { session } => heard.kit = Some(News::Closed { session: Id::from_token(session) }),
            tools::Request::JobsCancelled { session } => heard.cancelled = Some(session),
        }
    }
    assert!(calls.out.is_empty(), "the tools emit no more than their max_out");
    heard
}

/// The session an opener's handle names, or `None` if it has ended: the handle
/// travelled down while the session's `Ended` travelled up, and is dropped
/// (5.2).
fn addressed(sessions: &Slab<Session>, session: Token) -> Option<Id<Session>> {
    let id = Id::from_token(session);
    match &sessions.get(id)?.state {
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closing { .. } => Some(id),
        State::Closed => None,
    }
}

/// Applied after every transition: a closing session's kit closes, and the
/// session ends once nothing is left ([`settle`]); then what the transition
/// tells, and what the new state implies. `mark` is where the requests the
/// transition made begin in `out`.
fn conclude(domain: &mut Domain, env: &Env<Limits>, id: Id<Session>, out: &mut Queue<Request>, mark: u32) {
    let session = domain.sessions.get_mut(id).expect("the transition's session lives");
    match session.state {
        State::Closing { .. } => builtin::forget(&mut session.conversation),
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closed => {}
    }
    builtin::flush(&mut session.conversation, &mut domain.facts);
    match session.state {
        State::Closing { waiting: Waiting { call: false, runs: 0, .. }, .. } | State::Yielded => {
            finish_closing(&mut session.conversation);
            if session.conversation.compaction.asking() {
                clear_pending(&mut session.conversation);
            }
            if let Some(end) = tell_turn(&mut session.conversation, out) {
                match &mut session.state {
                    State::Closing { end: current, .. } => *current = end,
                    State::Yielded => session.state = finish(end),
                    State::Calling { .. }
                    | State::Backoff { .. }
                    | State::Tooling { .. }
                    | State::Resting { .. }
                    | State::Closed => unreachable!("matched states above"),
                }
            }
        }
        State::Closing { .. }
        | State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Closed => {}
    }
    settle(domain, env, id, out);
    let session = domain.sessions.get_mut(id).expect("a session lives until it is retired");
    tell(&mut domain.facts, &domain.calls.runs, session, out, mark);
    follow(&mut domain.sessions, &mut domain.alarms, &mut domain.ready, id);
}

/// What Closing implies: the session's kit closes, which cancels the calls the
/// tools are running for it; and once the kit has closed and nothing the
/// session waits for is left, the opener is told it has ended.
fn settle(domain: &mut Domain, env: &Env<Limits>, id: Id<Session>, out: &mut Queue<Request>) {
    let session = domain.sessions.get_mut(id).expect("a session lives until it is retired");
    let (end, waiting) = match &session.state {
        State::Closing { end, waiting } => (end.clone(), *waiting),
        State::Calling { .. }
        | State::Backoff { .. }
        | State::Tooling { .. }
        | State::Resting { .. }
        | State::Yielded
        | State::Closed => return,
    };
    let kit = match waiting.kit {
        Kit::Open => {
            let close = tools::Event::Close { kit: session.conversation.kit };
            let heard = tools_step(&mut domain.calls, env, close, out);
            assert!(heard.answer.is_none(), "a kit's close answers its calls later, as their operations end");
            match heard.kit {
                Some(News::Closed { session: closed }) => {
                    assert!(closed == id, "a close is news of its own kit");
                    Kit::Closed
                }
                None => Kit::Closing,
                Some(News::Opened { .. } | News::Refused { .. }) => unreachable!("a close opens no kit"),
            }
        }
        Kit::Closing | Kit::Closed => waiting.kit,
    };
    let waiting = Waiting { kit, ..waiting };
    let session = domain.sessions.get_mut(id).expect("looked up above");
    if waiting == SETTLED {
        pass_on_facts(domain, env);
        let session = domain.sessions.get_mut(id).expect("session lives through its terminal");
        let Conversation { opener, turns, usage, reported, .. } = session.conversation;
        let detail = mem::replace(&mut session.conversation.detail, Box::new([]));
        let mut end = end;
        match &mut end {
            End::Failed { detail: retained, .. } => retained.clone_from(&detail),
            End::Busy
            | End::Invalid
            | End::Closed
            | End::Budget { .. }
            | End::NoSummary { .. }
            | End::TranscriptRefused { .. }
            | End::PriceOverflow
            | End::UsageOverflow => {}
        }
        out.push(Request::Ended { opener, end, turns, usage, reported, detail });
        session.state = State::Closed;
    } else {
        session.state = State::Closing { end, waiting };
    }
}

/// The logical tool selection without retaining descriptor tickets in a fact.
fn choice_kind(choice: &ToolChoice) -> crate::facts::Choice {
    match choice {
        ToolChoice::Auto => crate::facts::Choice::Auto,
        ToolChoice::None => crate::facts::Choice::None,
        ToolChoice::Only(_) => crate::facts::Choice::Only,
    }
}

/// What a transition tells, derived from the requests it made: a fact for
/// each but the cancels and the tools' operations, which the tools tell.
fn tell(facts: &mut Facts, runs: &Slab<Run>, session: &mut Session, out: &Queue<Request>, mark: u32) {
    let opener = session.conversation.opener;
    let made = usize::try_from(mark).expect("a u32 fits in a usize");
    for request in out.iter().skip(made) {
        compaction::tell(&mut session.conversation, facts, request);
        let fact = match request {
            Request::Opened { opener, session: _ } => FactKind::Opened { opener: *opener },
            Request::Yielded { opener, stop, text: _ } => FactKind::Yielded { opener: *opener, stop: *stop },
            Request::Used { opener, usage } => FactKind::Used { opener: *opener, usage: *usage },
            Request::Ended { opener, end, turns, usage, .. } => {
                FactKind::Ended { opener: *opener, end: end_fact(end), turns: *turns, usage: *usage }
            }
            Request::Complete {
                owner: _,
                name,
                prompt,
                deadlines: _,
                max_completion_bytes: _,
                max_completion_blocks: _,
                max_failure_bytes: _,
            } => {
                let (messages, max_tokens) = (count(prompt.messages.len()), prompt.max_tokens);
                FactKind::CompletionStarted {
                    opener,
                    window: session.conversation.window.number,
                    attempt: name.retry,
                    messages,
                    max_tokens,
                    choice: choice_kind(&prompt.choice),
                }
            }
            Request::Delegate { owner, opener: _, call: _, deadline: _, origin: _ } => {
                let run = runs.get(Id::from_token(*owner)).expect("a run lives while its call is asked for");
                FactKind::DelegateStarted { opener, block: run.block }
            }
            Request::JobsCancelled { .. }
            | Request::Turn { .. }
            | Request::Priced { .. }
            | Request::Cancel { .. }
            | Request::Yield { .. }
            | Request::Withdraw { .. }
            | Request::Io { .. }
            | Request::Job { .. }
            | Request::CancelIo { .. } => {
                continue;
            }
        };
        match request {
            Request::Delegate { owner, .. } => {
                let run = runs.get(Id::from_token(*owner)).expect("a delegated call lives through its start");
                facts.push_call(facts.now(), fact, &run.metadata);
            }
            Request::Complete { .. } => facts.push_response(fact, next_response(&session.conversation), None),
            Request::Used { .. } => {
                let spent = session.conversation.recording.last_charge;
                facts.push_response(fact, u64::from(session.conversation.turns), Some(spent));
            }
            Request::Opened { .. } | Request::Yielded { .. } | Request::Ended { .. } => facts.push(fact),
            Request::JobsCancelled { .. }
            | Request::Turn { .. }
            | Request::Priced { .. }
            | Request::Cancel { .. }
            | Request::Yield { .. }
            | Request::Withdraw { .. }
            | Request::Io { .. }
            | Request::Job { .. }
            | Request::CancelIo { .. } => unreachable!("requests without facts were skipped"),
        }
    }
}

/// What a session's state implies, applied after every transition: which
/// alarms run, whether it is on the ready list, and whether it is retired.
fn follow(sessions: &mut Slab<Session>, alarms: &mut Deadlines<Alarm>, ready: &mut Ready, id: Id<Session>) {
    let session = sessions.get(id).expect("a session lives until it is retired");
    let expires = session.conversation.expires;
    let (expiry, retry, resting, closed) = match &session.state {
        State::Calling { .. } | State::Tooling { .. } | State::Yielded => (Some(expires), None, false, false),
        State::Backoff { until, .. } => (Some(expires), Some(*until), false, false),
        State::Resting { .. } => (Some(expires), None, true, false),
        State::Closing { .. } => (None, None, false, false),
        State::Closed => (None, None, false, true),
    };
    set(alarms, Alarm::Expiry { session: id }, expiry);
    set(alarms, Alarm::Retry { session: id }, retry);
    ready.keep(id, resting);
    if closed {
        sessions.retire(id);
    }
}

fn set(alarms: &mut Deadlines<Alarm>, alarm: Alarm, at: Option<Time>) {
    if let Some(at) = at {
        alarms.arm(alarm, at).expect("the alarm table has room for two alarms per session");
    } else {
        alarms.cancel(alarm);
    }
}

// Cell handlers: each takes the source state's data by value and returns the
// target state.

/// Calling, completed: the LLM produced its next message.
fn answered(
    conversation: &mut Conversation,
    id: Id<Session>,
    calls: &mut Calls,
    completion: Completion,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    let origin_fits = {
        let Recording { sequence, .. } = conversation.recording;
        sequence.checked_add(1).is_some()
    } && u32::try_from(completion.content.len()).is_ok();
    if let Err(end) = used(conversation, completion.usage, out) {
        release_provider(conversation);
        return finish(end);
    }
    if conversation.compaction.asking() {
        return compaction::answered(conversation, id, completion, env, out);
    }
    if !origin_fits {
        release_provider(conversation);
        clear_pending(conversation);
        return finish(history::sequence_limit());
    }
    for block in &completion.content {
        match block {
            Block::ToolCall { call: Decoded::Historical, .. } => {
                release_provider(conversation);
                clear_pending(conversation);
                return finish(End::Invalid);
            }
            Block::ToolCall {
                call:
                    Decoded::Builtin { .. } | Decoded::Owned { .. } | Decoded::Delegated { .. } | Decoded::Invalid { .. },
                ..
            }
            | Block::Opaque { .. }
            | Block::Text { .. }
            | Block::Refusal { .. }
            | Block::ToolResult { .. } => {}
        }
    }
    if conversation.final_turn.answering() && tally(&completion.content).0 > 0 {
        return use_tools(conversation, id, calls, completion.content, env, out);
    }
    match completion.stop {
        Stop::ToolUse => use_tools(conversation, id, calls, completion.content, env, out),
        Stop::EndTurn => pause(conversation, Yield::Done, completion.content, &env.limits, out),
        Stop::MaxTokens => pause(conversation, Yield::Truncated, completion.content, &env.limits, out),
        Stop::Refusal => pause(conversation, Yield::Refused, completion.content, &env.limits, out),
    }
}

/// Closing, completed: the call won the race with its cancel. The session
/// ends as it was going to, but the provider counted the tokens, and so does
/// the session.
#[expect(clippy::too_many_arguments, reason = "late completion preserves its closing state and call issuer")]
fn answered_late(
    conversation: &mut Conversation,
    id: Id<Session>,
    issuer: &mut Calls,
    end: End,
    waiting: Waiting,
    completion: Completion,
    limits: &Limits,
    out: &mut Queue<Request>,
) -> State {
    if let Err(overflow) = used(conversation, completion.usage, out) {
        release_provider(conversation);
        return State::Closing { end: overflow, waiting: Waiting { call: false, ..waiting } };
    }
    if conversation.compaction.asking() {
        let end = compaction::accept(conversation, completion, limits, out).unwrap_or(end);
        return State::Closing { end, waiting: Waiting { call: false, ..waiting } };
    }
    assert!(conversation.transcript.room() > 0, "reserved assistant message");
    if !receive_completion(conversation, &completion.content, tally(&completion.content).0, limits) {
        clear_pending(conversation);
        return State::Closing {
            end: history::completion_limit(conversation, &completion.content, limits),
            waiting: Waiting { call: false, ..waiting },
        };
    }
    let calls = tally(&completion.content).0;
    conversation
        .transcript
        .push(Message { role: Role::Assistant, content: completion.content })
        .expect("reserved assistant slot");
    if calls > 0 {
        builtin::late(conversation, id, issuer, calls, limits);
    }

    State::Closing { end, waiting: Waiting { call: false, ..waiting } }
}

/// Calling, completed with tool use: record the message and go through its
/// calls.
fn use_tools(
    conversation: &mut Conversation,
    id: Id<Session>,
    calls: &mut Calls,
    content: Box<[Block]>,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    let (count, _) = tally(&content);
    if count == 0 {
        return pause(conversation, Yield::Malformed, content, &env.limits, out);
    }
    // The results go back in another message.
    assert!(conversation.transcript.room() >= 2, "reserved assistant and result messages");
    if !receive_completion(conversation, &content, count, &env.limits) {
        clear_pending(conversation);
        return finish(history::completion_limit(conversation, &content, &env.limits));
    }
    conversation.transcript.push(Message { role: Role::Assistant, content }).expect("checked for room above");
    let mut tools = Tools {
        after: After::Complete,
        slots: List::with_capacity(count),
        running: 0,
        next: 0,
        failed: None,
        reached: 0,
    };
    builtin::prepare(conversation, id, calls, &mut tools, &env.limits);
    advance(conversation, id, calls, tools, env, out)
}

/// With no run in flight, goes on through the tool calls of the last message:
/// answers each invalid one with its problem, and starts the next batch, the
/// adjacent calls that read, up to `Limits::parallel_tools`, or one that
/// writes. Past the last call, sends the results back.
///
/// A step starts one batch at most. The tools may answer a call at their
/// entrance, in this very step, without asking io anything (a path outside the
/// checkout, a family not granted); if they answer every call of the batch so,
/// the session rests on the ready list before it starts another, so that
/// the runs it ended are reclaimed first, and what one step emits and the runs
/// a session holds stay bounded ([`crate::limits`]).
fn advance(
    conversation: &mut Conversation,
    id: Id<Session>,
    calls: &mut Calls,
    mut tools: Tools,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    // Time does not wait: no batch starts once it is up.
    if env.now >= conversation.expires {
        conversation.closing_tools = Some(tools);
        return finish(OUT_OF_TIME);
    }
    let share = history::reserve_batch(conversation, tools.next, tools.failed.is_some(), &env.limits);
    let message = conversation.transcript.last().expect("the assistant message is last while tooling");
    let end = u32::try_from(message.content.len()).expect("a message whose calls were counted has its blocks counted");
    let begin = tools.next;
    let mut batch: Option<Effect> = None;
    let mut started: u32 = 0;
    let mut next = end;
    for index in tools.next..end {
        let block = current_block(conversation, index);
        if batching::answer_skipped(conversation, block, index, &mut tools) {
            continue;
        }
        match block {
            Block::ToolCall { id: provider_id, name, input, call: Decoded::Owned { call }, .. } => {
                let effect = tools::effect(call);
                if !joins(batch, effect, started, env.limits.parallel_tools) {
                    next = index;
                    break;
                }
                let bounded = history::tool_env(call, share, env);
                let credit = owned_credit(call, &bounded.limits).min(share);
                let call = call.clone();
                let deadline = env.now.saturating_add(env.limits.tools.tool_deadline).min(conversation.expires);
                let metadata = tool_metadata(provider_id, name, input, crate::ToolSource::Workspace, effect, deadline);
                let run = observed_run(calls, id, tools.reached, index, By::Tools, credit, metadata);
                let (kit, reply_to) = (conversation.kit, ReplyTo::new(run.token()));
                let observation = Some(tools::CallInfo {
                    owner: run.token(),
                    effect,
                    deadline,
                    input_bytes: u64::try_from(input.len()).expect("provider input byte count fits u64"),
                });
                let heard =
                    tools_step(calls, &bounded, tools::Event::Call { kit, reply_to, observation, call, deadline }, out);
                assert!(heard.kit.is_none(), "a call is news of no kit");
                started = started.saturating_add(1);
                batch = Some(effect);
                // Answered at the tools' entrance, or running.
                let Some((answered, result)) = heard.answer else {
                    batching::put_next(&mut tools, Slot::Running { run });
                    tools.running = tools.running.saturating_add(1);
                    continue;
                };
                assert!(answered == run, "the tools answer at once only the call they were given");
                let metadata = calls.runs.get(run).expect("answered run").metadata.clone();
                batching::answered_next(&mut tools, block, &result);
                conversation.jobs.answered(&result, metadata);
                calls.runs.retire(run);
                // Refused at the entrance: its block and its id were counted
                // when the message was recorded, and a refusal holds nothing
                // more, so it fits, and the batch it is in goes on.
                receive_result(conversation, credit, &result, &env.limits);
                let result = Block::ToolResult { id: call_id(conversation, index), result };
                batching::put_next(&mut tools, Slot::Done { result });
            }
            Block::ToolCall {
                id: provider_id,
                name,
                input,
                call: Decoded::Delegated { source, ticket, effect, descriptor: _ },
                ..
            } => {
                if !joins(batch, *effect, started, env.limits.parallel_tools) {
                    next = index;
                    break;
                }
                let credit = delegated_credit(&env.limits).min(share);
                let deadline = env.now.saturating_add(env.limits.tools.tool_deadline).min(conversation.expires);
                let metadata = tool_metadata(provider_id, name, input, *source, *effect, deadline);
                let run = observed_run(calls, id, tools.reached, index, By::Opener, credit, metadata);
                // The opener runs the race, and the session waits for it as
                // long as it lives.
                let (opener, call) = (conversation.opener, *ticket);
                let sequence = next_sequence(conversation);
                let origin = crate::record::Origin { sequence, position: index };
                out.push(Request::Delegate { owner: run.token(), opener, call, deadline, origin });
                batching::put_next(&mut tools, Slot::Running { run });
                tools.running = tools.running.saturating_add(1);
                started = started.saturating_add(1);
                batch = Some(*effect);
            }
            Block::ToolCall { call: Decoded::Builtin { .. }, .. } => batching::next_slot(&mut tools),
            Block::ToolCall { id: call, name: _, input: _, call: Decoded::Invalid { problem, .. }, .. } => {
                batching::invalid_slot(&mut tools, block, call, problem);
            }
            Block::ToolCall { call: Decoded::Historical, .. }
            | Block::Opaque { .. }
            | Block::Text { .. }
            | Block::Refusal { .. }
            | Block::ToolResult { .. } => {}
        }
    }
    tools.next = next;
    batching::end_waits(conversation, calls, &tools, env, out);
    if tools.running > 0 {
        return State::Tooling { tools };
    }
    if begin < end {
        return State::Resting { tools };
    }
    finish_tools(conversation, id, tools, env, out)
}

/// Assemble a fully settled batch in original call order, then issue the next
/// provider request only after its new receiving credit is available.
fn finish_tools(
    conversation: &mut Conversation,
    id: Id<Session>,
    tools: Tools,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    let final_text = if conversation.final_turn.answering() {
        Some(text_of(&conversation.transcript.last().expect("settled assistant message").content))
    } else {
        None
    };
    conversation.builtin.moved(conversation.transcript.len());
    let mut results = List::with_capacity(tools.slots.len());
    for slot in tools.slots.into_boxed() {
        match slot {
            Slot::Done { result } => results.push(result).expect("a result for every slot"),
            Slot::Running { .. } | Slot::Withheld => unreachable!("ordinary tooling has a result for every call"),
        }
    }
    let results = Message { role: Role::User, content: results.into_boxed() };
    conversation.transcript.push(results).expect("room was checked when the message was recorded");
    if let Some(text) = final_text {
        return final_answer(conversation, text, out);
    }
    match tools.after {
        After::Complete => call(conversation, id, 0, env, out),
        After::Yield => {
            if let Some(end) = tell_turn(conversation, out) {
                return finish(end);
            }
            out.push(Request::Yielded { opener: conversation.opener, stop: Yield::Done, text: Box::default() });
            State::Yielded
        }
    }
}

/// Whether a call with `effect` joins a batch of `batch` that has started
/// `started` runs: a read joins reads while there is room, and a write runs
/// alone.
const fn joins(batch: Option<Effect>, effect: Effect, started: u32, parallel: u32) -> bool {
    match batch {
        None => true,
        Some(Effect::Write) => false,
        Some(Effect::Read) => match effect {
            Effect::Read => started < parallel,
            Effect::Write => false,
        },
    }
}

/// Calling, completed without tools to run: record the message, and yield to
/// the opener with its text.
fn pause(
    conversation: &mut Conversation,
    stop: Yield,
    content: Box<[Block]>,
    limits: &Limits,
    out: &mut Queue<Request>,
) -> State {
    // A session continues from its transcript, so the message must fit there.
    assert!(conversation.transcript.room() > 0, "reserved assistant message");
    if !receive_completion(conversation, &content, tally(&content).0, limits) {
        clear_pending(conversation);
        return finish(history::completion_limit(conversation, &content, limits));
    }
    let text = text_of(&content);
    conversation.transcript.push(Message { role: Role::Assistant, content }).expect("checked for room above");
    if let Some(end) = tell_turn(conversation, out) {
        return finish(end);
    }
    let stop = if conversation.final_turn.answering() { Yield::Budget } else { stop };
    out.push(Request::Yielded { opener: conversation.opener, stop, text });
    State::Yielded
}

/// Yielded, continue: the opener's message goes to the LLM, after a result for
/// each tool call the yielded message made, as every call needs one.
fn resumed(
    conversation: &mut Conversation,
    id: Id<Session>,
    text: Box<[u8]>,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    if let Some(spent) = conversation.final_turn.spent() {
        return finish(End::Budget { spent });
    }
    // A budget the last answer took past its end comes before the room.
    if let Some(spent) = spent(conversation, env.now) {
        return finish(End::Budget { spent });
    }
    let cost = match len(&text) {
        Some(bytes) => bytes.checked_add(u64::try_from(size_of::<Block>()).expect("block size fits")),
        None => None,
    };
    let blocks = env.limits.completion_blocks.saturating_add(1);
    if conversation.transcript.room() == 0
        || !blocks_fit(conversation, blocks, &env.limits)
        || !charge(conversation, cost, &env.limits)
    {
        return history::continue_held(conversation, id, text, env, out);
    }
    let content = unrun(conversation, text, &mut [], false);
    conversation.transcript.push(Message { role: Role::User, content }).expect("checked for room above");
    call(conversation, id, 0, env, out)
}

/// The opener's message after one result per call of the saved assistant tail.
/// Unanswered calls become `NotRun`; host answers keep their original positions.
fn unrun(
    conversation: &Conversation,
    text: Box<[u8]>,
    answered: &mut [crate::record::Answered],
    waiting: bool,
) -> Box<[Block]> {
    let message = conversation.transcript.last().expect("a yielded session's transcript ends with its answer");
    let (calls, _) = tally(&message.content);
    let mut content = List::with_capacity(calls.saturating_add(1));
    let mut waking = Some(text);
    for (position, block) in message.content.iter().enumerate() {
        match block {
            Block::ToolCall { id, name, .. } => {
                let mut result = if waiting && name.as_ref() == b"wait" {
                    Returned::Text { text: waking.take().expect("one withheld wait"), error: false, replay: None }
                } else {
                    Returned::NotRun { why: crate::llm::NotRun::Stopped }
                };
                for answer in answered.iter_mut() {
                    if usize::try_from(answer.origin.position).expect("validated position") == position {
                        result =
                            mem::replace(&mut answer.result, Returned::NotRun { why: crate::llm::NotRun::Stopped });
                    }
                }
                let result = Block::ToolResult { id: id.clone(), result };
                content.push(result).expect("room for a result per call");
            }
            Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {}
        }
    }
    if let Some(text) = waking {
        content.push(Block::Text { text, replay: None }).expect("room for the message after the results");
    }
    content.into_boxed()
}

/// Tooling, a run done: keep the result of the call at `block` in its slot,
/// and once the batch is done, go on through the calls after it.
#[expect(clippy::too_many_arguments, reason = "a cell handler takes what its cell needs, and the calls to start more")]
fn tool_ran(
    conversation: &mut Conversation,
    id: Id<Session>,
    calls: &mut Calls,
    mut tools: Tools,
    slot: u32,
    block: u32,
    credit: u64,
    result: Returned,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    tools.running = tools.running.checked_sub(1).expect("the run that ended was counted");
    // The result's block and its id were counted when the message was
    // recorded.
    batching::answered(&mut tools, slot, current_block(conversation, block), &result);
    receive_result(conversation, credit, &result, &env.limits);
    let result = Block::ToolResult { id: call_id(conversation, block), result };
    *tools.slots.get_mut(slot).expect("a run fills its own slot") = Slot::Done { result };
    if !batching::poll(current_block(conversation, block)) {
        batching::end_waits(conversation, calls, &tools, env, out);
    }
    if tools.running > 0 {
        return State::Tooling { tools };
    }
    advance(conversation, id, calls, tools, env, out)
}

/// Closing, a tool run ended: one fewer to wait for.
fn settled(end: End, waiting: Waiting) -> State {
    let runs = waiting.runs.checked_sub(1).expect("the run that ended was waited for");
    State::Closing { end, waiting: Waiting { runs, ..waiting } }
}

/// Calling, failed: wait and call again if the failure is transient and
/// retries remain; end otherwise.
fn call_failed(
    attempt: u32,
    failure: Failure,
    evidence: crate::llm::Evidence,
    retry_ok: bool,
    conversation: &Conversation,
    rng: &mut Rng,
    env: &Env<Limits>,
) -> State {
    let failure = match failure {
        Failure::Limit { which: crate::llm::Limit::Path(crate::llm::Cap::Request), bound: _ } => {
            Failure::ContextTooLong
        }
        Failure::Limit { .. }
        | Failure::ApplicationLimit
        | Failure::Protocol
        | Failure::Cancelled
        | Failure::Overloaded
        | Failure::RateLimited { .. }
        | Failure::Unavailable
        | Failure::TimedOut { .. }
        | Failure::ContextTooLong
        | Failure::Refused
        | Failure::Program { .. }
        | Failure::SessionGone
        | Failure::SignedOut
        | Failure::Unsupported
        | Failure::Invalid
        | Failure::Unauthorized
        | Failure::Exhausted { .. } => failure,
    };
    let response = match evidence {
        crate::llm::Evidence::Response { .. } => true,
        crate::llm::Evidence::Unsent | crate::llm::Evidence::Unknown => false,
    };
    let decision = match failure {
        Failure::Unauthorized if conversation.account.is_none() || (!retry_ok && response) => Retry::Fail,
        Failure::Unauthorized
        | Failure::Overloaded
        | Failure::Unavailable
        | Failure::TimedOut { .. }
        | Failure::RateLimited { .. }
        | Failure::ContextTooLong
        | Failure::Refused
        | Failure::Program { .. }
        | Failure::SessionGone
        | Failure::SignedOut
        | Failure::Unsupported
        | Failure::Invalid
        | Failure::Exhausted { .. }
        | Failure::Limit { .. }
        | Failure::ApplicationLimit
        | Failure::Protocol
        | Failure::Cancelled => retry_decision(failure, attempt, env.now, conversation.expires, &env.limits, rng),
    };
    match decision {
        Retry::After(delay) => {
            State::Backoff { attempt: attempt.saturating_add(1), until: env.now.saturating_add(delay) }
        }
        Retry::Fail => finish(End::Failed {
            failure,
            evidence,
            detail: Box::default(),
            attempts: attempt.checked_add(1).expect("an admitted completion leaves room for its first attempt"),
        }),
        Retry::TimeUp => finish(OUT_OF_TIME),
    }
}

/// Ask for an ordinary answer or compact before the next answer crosses its
/// history or window bound. A fresh window names its limit; budgets end first.
fn call(
    conversation: &mut Conversation,
    id: Id<Session>,
    attempt: u32,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    if let Some(end) = tell_turn(conversation, out) {
        return finish(end);
    }
    if let Some(spent) = spent(conversation, env.now) {
        return finish(End::Budget { spent });
    }
    if let Some(end) = final_prompt(conversation, env) {
        return finish(end);
    }
    if !conversation.compaction.asking()
        && let Some(bound) = history::due(conversation, &env.limits)
    {
        if let Some(end) = builtin::keep_turn(conversation, out) {
            return finish(end);
        }
        if let Some(end) = history::start(conversation, &env.limits, bound) {
            return finish(end);
        }
    }
    if !conversation.compaction.asking() && compaction::due(conversation, &env.limits) {
        if let Some(end) = builtin::keep_turn(conversation, out) {
            return finish(end);
        }
        if let Some(end) = compaction::start(conversation, &env.limits, crate::llm::Evidence::Unsent, 0) {
            return finish(end);
        }
    }
    {
        let Recording { sequence, .. } = conversation.recording;

        if sequence.checked_add(1).is_none() {
            return finish(history::sequence_limit());
        }
    }
    assert!(reserve_provider(conversation, &env.limits), "admitted provider work has receiving room");
    conversation.completion_attempts = attempt.checked_add(1).expect("admitted one-based completion attempt");
    out.push(complete(id, conversation, attempt, &env.limits));
    State::Calling { attempt }
}

/// Move the already charged opener note into its one final user message.
fn final_prompt(conversation: &mut Conversation, env: &Env<Limits>) -> Option<End> {
    let turns = conversation.budget.turns.checked_sub(conversation.turns).expect("unspent turns checked");
    let time = conversation.expires.saturating_since(env.now);
    let due = history::due(conversation, &env.limits).is_some() || compaction::due(conversation, &env.limits);
    let note = conversation.final_turn.take_note(turns, time, due)?;
    let cost = u64::try_from(size_of::<Block>()).expect("one block cell fits");
    conversation.choice = ToolChoice::None;
    conversation.choice_names = None;
    if conversation.compaction.asking()
        || conversation.transcript.room() == 0
        || !charge(conversation, Some(cost), &env.limits)
    {
        return history::hold_note(conversation, note, &env.limits);
    }
    let content = Box::new([Block::Text { text: note, replay: None }]);
    conversation.transcript.push(Message { role: Role::User, content }).expect("final note slot checked");
    None
}

/// An actual final completion yields its text after every call has a not-run result.
fn final_answer(conversation: &mut Conversation, text: Box<[u8]>, out: &mut Queue<Request>) -> State {
    if let Some(end) = tell_turn(conversation, out) {
        return finish(end);
    }
    out.push(Request::Yielded { opener: conversation.opener, stop: Yield::Budget, text });
    State::Yielded
}

fn cancel_call(id: Id<Session>, end: End, out: &mut Queue<Request>) -> State {
    out.push(Request::Cancel { owner: id.token() });
    State::Closing { end, waiting: Waiting { call: true, ..KIT } }
}

/// Withdraws the delegated runs in flight, to end with `end` once every run
/// has settled. The tools' runs end as the kit's close cancels them.
fn cancel_tools(
    conversation: &mut Conversation,
    runs: &Slab<Run>,
    tools: Tools,
    end: End,
    out: &mut Queue<Request>,
) -> State {
    for slot in &tools.slots {
        match slot {
            Slot::Running { run } => match runs.get(*run).expect("a run in flight is in the slab").by {
                By::Tools => {}
                By::Program => unreachable!("program observation owners never run in slots"),
                By::Opener => out.push(Request::Withdraw { owner: run.token() }),
            },
            Slot::Done { .. } | Slot::Withheld => {}
        }
    }
    let running = tools.running;
    conversation.closing_tools = Some(tools);

    State::Closing { end, waiting: Waiting { runs: running, ..KIT } }
}

/// Ends with `end` at once, or once the runs in flight have settled.
fn abandon(
    conversation: &mut Conversation,
    runs: &Slab<Run>,
    tools: Tools,
    end: End,
    out: &mut Queue<Request>,
) -> State {
    if tools.running == 0 {
        return finish(end);
    }
    cancel_tools(conversation, runs, tools, end, out)
}

/// Counts a completion that came back, and tells the opener.
fn used(conversation: &mut Conversation, usage: Usage, out: &mut Queue<Request>) -> Result<(), End> {
    let price = completion_price(&conversation.recording, usage).ok_or(End::PriceOverflow)?;
    let own = conversation.recording.own_spent.checked_add(price).ok_or(End::PriceOverflow)?;
    let inclusive = conversation.recording.spent.checked_add(price).ok_or(End::PriceOverflow)?;
    let cumulative = checked_usage(conversation.usage, usage)?;
    let reported = checked_reported(conversation.reported, usage).ok_or(End::UsageOverflow)?;
    let next_turn = conversation.turns.checked_add(1).ok_or(End::UsageOverflow)?;
    conversation.turns = next_turn;
    conversation.usage = cumulative;
    conversation.reported = reported;
    conversation.recording.reservation = None;
    conversation.recording.last_charge = price;
    conversation.recording.own_spent = own;
    conversation.recording.spent = inclusive;
    conversation.recording.pending = Some(usage);
    out.push(Request::Priced { opener: conversation.opener, spent: inclusive, own_spent: own });
    out.push(Request::Used { opener: conversation.opener, usage });
    Ok(())
}

/// A cumulative raw count is known only if every accepted count was reported.
fn checked_usage(previous: Usage, received: Usage) -> Result<Usage, End> {
    Ok(Usage {
        input_tokens: sum_count(previous.input_tokens, received.input_tokens)?,
        output_tokens: sum_count(previous.output_tokens, received.output_tokens)?,
        cache_read_tokens: sum_count(previous.cache_read_tokens, received.cache_read_tokens)?,
        cache_write_tokens: sum_count(previous.cache_write_tokens, received.cache_write_tokens)?,
        reasoning_tokens: sum_count(previous.reasoning_tokens, received.reasoning_tokens)?,
    })
}

fn sum_count(previous: Option<u64>, received: Option<u64>) -> Result<Option<u64>, End> {
    match previous {
        Some(previous) => match received {
            Some(received) => Ok(Some(previous.checked_add(received).ok_or(End::UsageOverflow)?)),
            None => Ok(None),
        },
        None => Ok(None),
    }
}

/// Budget bookkeeping adds supplied counts across gaps, preserving absence.
fn checked_reported(previous: Usage, received: Usage) -> Option<Usage> {
    Some(Usage {
        input_tokens: add_reported_count(previous.input_tokens, received.input_tokens).ok()?,
        output_tokens: add_reported_count(previous.output_tokens, received.output_tokens).ok()?,
        cache_read_tokens: add_reported_count(previous.cache_read_tokens, received.cache_read_tokens).ok()?,
        cache_write_tokens: add_reported_count(previous.cache_write_tokens, received.cache_write_tokens).ok()?,
        reasoning_tokens: add_reported_count(previous.reasoning_tokens, received.reasoning_tokens).ok()?,
    })
}

fn add_reported_count(previous: Option<u64>, received: Option<u64>) -> Result<Option<u64>, End> {
    match previous {
        Some(previous) => match received {
            Some(received) => Ok(Some(previous.checked_add(received).ok_or(End::UsageOverflow)?)),
            None => Ok(Some(previous)),
        },
        None => Ok(received),
    }
}

/// Ends the session with `end`, which has nothing in flight but its kit: the
/// kit closes, and the opener is told once it has ([`settle`]).
const fn finish(end: End) -> State {
    State::Closing { end, waiting: KIT }
}

// Helpers.

/// How a session ends when its time budget runs out.
const OUT_OF_TIME: End = End::Budget { spent: Dimension::Time };

/// Refuses an open at the entrance: the session ends without having opened.
fn refuse(facts: &mut Facts, opener: Token, end: End, out: &mut Queue<Request>) {
    facts.push(FactKind::Ended { opener, end: end_fact(&end), turns: 0, usage: Usage::ZERO });
    out.push(Request::Ended {
        opener,
        end,
        turns: 0,
        usage: Usage::ZERO,
        reported: Usage::NONE,
        detail: Box::default(),
    });
}

/// The conversation for `spec`, and the authority its kit opens with; or
/// `None` if the spec does not fit the limits.
fn admit(
    opener: Token,
    spec: Spec,
    recording: Recording,
    limits: &Limits,
    now: Time,
) -> Option<(Conversation, tools::Authority)> {
    if spec.model.len() > 128
        || match &spec.effort {
            Some(effort) => effort.len(),
            None => 0,
        } > 32
        || spec.output == 0
        || spec.protocol_allowance > limits.protocol_allowance
        || spec.progress.head == Duration::ZERO
        || spec.progress.idle == Duration::ZERO
        || !affordable(&spec.budget, &limits.budget)
    {
        return None;
    }
    let task = spec.prompt.clone();
    let content: Box<[Block]> = Box::new([Block::Text { text: spec.prompt, replay: None }]);
    let final_turn = crate::final_turn::Final::new(spec.final_turn);
    let bytes = spec_cost(&spec.model, &spec.system, &spec.delegated, &content)?
        .checked_add(match &spec.backend {
            crate::record::Backend::Direct => 0,
            crate::record::Backend::Program { name } => len(name)?,
        })?
        .checked_add(spec.authority.namespace_bytes()?)?
        .checked_add(final_turn.note_bytes()?)?
        .checked_add(len(&task)?)?
        .checked_add(len(&spec.summary)?)?
        .checked_add(len(&spec.preamble)?)?
        .checked_add(match &spec.effort {
            Some(effort) => len(effort)?,
            None => 0,
        })?;
    if bytes > limits.history_bytes || limits.history_blocks == 0 {
        return None;
    }
    let mut transcript = List::with_capacity(limits.history_messages);
    transcript.push(Message { role: Role::User, content }).ok()?;
    let conversation = Conversation {
        backend: spec.backend,
        gone_window: None,
        account: spec.account,
        protocol_allowance: spec.protocol_allowance,
        opener,
        endpoint: spec.endpoint,
        affinity: spec.affinity,
        progress: spec.progress,
        model: spec.model,
        effort: spec.effort,
        next_llm: None,
        system: spec.system,
        tools: spec.authority.grants,
        namespace: spec.authority.namespace(),
        delegated: spec.delegated,
        choice: ToolChoice::Auto,
        choice_names: None,
        final_turn,
        output: spec.output,
        model_window: spec.window,
        line: spec.line,
        window: crate::window::Window::fresh(1, 0),
        compaction: compaction::Compaction::Idle,
        task,
        summary: spec.summary,
        preamble: spec.preamble,
        window_turn: false,
        transcript,
        bytes,
        reserved: 0,
        provider_credit: None,
        completion_attempts: 0,
        budget: spec.budget,
        turns: 0,
        usage: Usage::ZERO,
        reported: Usage::NONE,
        expires: now.saturating_add(spec.budget.time),
        // Named as the kit opens: until then, the session is Closed, which
        // holds nothing.
        kit: Token::new(0),
        recording,
        jobs: jobs::Jobs::new(limits.tools.jobs),
        builtin: builtin::Owed::with_capacity(limits.completion_blocks),
        closing_tools: None,
        detail: Box::default(),
    };
    Some((conversation, spec.authority))
}

/// Whether `budget` asks for no more than `most` in any dimension.
fn affordable(budget: &Budget, most: &Budget) -> bool {
    budget.turns <= most.turns
        && budget.input <= most.input
        && budget.output <= most.output
        && budget.cache_read <= most.cache_read
        && budget.cache_write <= most.cache_write
        && budget.time <= most.time
}

fn next_sequence(conversation: &Conversation) -> u32 {
    conversation.recording.sequence.checked_add(1).expect("checked before tool effects")
}

fn current_block(conversation: &Conversation, index: u32) -> &Block {
    let message = conversation.transcript.last().expect("the assistant message is last while tooling");
    message.content.get(usize::try_from(index).expect("u32 fits usize")).expect("within the assistant message")
}

fn chosen(choice: &ToolChoice, block: &Block) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Builtin { .. }, .. }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => true,
        Block::ToolCall { call, .. } => match choice {
            ToolChoice::Auto => true,
            ToolChoice::None => false,
            ToolChoice::Only(tickets) => match call {
                Decoded::Delegated { descriptor, .. } => tickets.contains(descriptor),
                Decoded::Invalid { descriptor, .. } => match descriptor {
                    Some(descriptor) => tickets.contains(descriptor),
                    None => false,
                },
                Decoded::Builtin { .. } | Decoded::Owned { .. } | Decoded::Historical => false,
            },
        },
    }
}

/// The first dimension of the budget that keeps the session from starting
/// another completion at `now`, if any: its turns, input or output tokens used
/// up, its cache reads or writes taken past their budget by a completion (whose
/// tokens are known only once it comes back), or its time run out.
fn spent(conversation: &Conversation, now: Time) -> Option<Dimension> {
    {
        let Recording { budget, spent, .. } = conversation.recording;

        if spent >= budget {
            return Some(Dimension::Unit);
        }
    }
    let Conversation { budget, turns, reported: usage, expires, .. } = conversation;
    if *turns >= budget.turns {
        return Some(Dimension::Turns);
    }
    if usage.input_tokens.unwrap_or(0) >= budget.input {
        return Some(Dimension::Input);
    }
    if usage.output_tokens.unwrap_or(0) >= budget.output {
        return Some(Dimension::Output);
    }
    if usage.cache_read_tokens.unwrap_or(0) > budget.cache_read {
        return Some(Dimension::CacheRead);
    }
    if usage.cache_write_tokens.unwrap_or(0) > budget.cache_write {
        return Some(Dimension::CacheWrite);
    }
    if now >= *expires {
        return Some(Dimension::Time);
    }
    None
}

/// The request for the next assistant message, its answer cut to the output
/// budget left. The conversation is copied: the session keeps it, and the
/// protocol layer holds the copy (copy at emission).
fn complete(id: Id<Session>, conversation: &Conversation, retry: u32, limits: &Limits) -> Request {
    let left = conversation.budget.output.saturating_sub(conversation.reported.output_tokens.unwrap_or(0));
    let max_tokens = u32::try_from(left).unwrap_or(u32::MAX).min(conversation.output);
    let purpose = if conversation.compaction.asking() {
        let mut history = compaction::prompt_bytes(conversation).unwrap_or(u64::MAX);
        for message in conversation.transcript.iter().rev().take(2) {
            for block in &message.content {
                history = history.saturating_sub(payload_cost(block).unwrap_or(0));
            }
        }
        let history = match conversation.window.last_prompt {
            Some(prompt) => prompt.0.saturating_add(conversation.window.last_output.0),
            None => history.saturating_add(conversation.protocol_allowance),
        };
        let window = u64::from(conversation.model_window);
        let room = window
            .saturating_sub(history)
            .saturating_sub(u64::try_from(compaction::instruction_bytes(conversation)).expect("bounded summary"));
        crate::llm::Purpose::Compaction { room }
    } else {
        match conversation.window.last_prompt {
            Some(prompt) => crate::llm::Purpose::Ordinary {
                guard: Some(crate::llm::Guard {
                    line: conversation.line,
                    last_prompt: prompt.0,
                    from: usize::try_from(conversation.window.following.checked_sub(1).expect("reported answer"))
                        .expect("bounded history"),
                }),
            },
            None => crate::llm::Purpose::Ordinary { guard: None },
        }
    };
    let prompt = Prompt {
        purpose,
        protocol_allowance: conversation.protocol_allowance,
        endpoint: conversation.endpoint,
        affinity: conversation.affinity,
        model: conversation.model.clone(),
        effort: conversation.effort.clone(),
        system: conversation.system.clone(),
        tools: conversation.tools,
        namespace: conversation.namespace.clone(),
        delegated: conversation.delegated.clone(),
        choice: if conversation.compaction.asking() { ToolChoice::None } else { conversation.choice.clone() },
        messages: conversation.transcript.to_boxed(),
        max_tokens,
    };
    Request::Complete {
        owner: id.token(),
        name: crate::llm::CompletionName {
            sequence: conversation.recording.sequence.checked_add(1).expect("call admitted the next sequence"),
            retry,
        },
        prompt,
        deadlines: CallDeadlines {
            head: conversation.progress.head,
            idle: conversation.progress.idle,
            whole: conversation.expires,
        },
        max_completion_bytes: limits.completion_bytes,
        max_completion_blocks: limits.completion_blocks,
        max_failure_bytes: limits.failure_bytes,
    }
}

/// The provider's name for the tool call at `block` of the last message, which
/// its result echoes.
fn call_id(conversation: &Conversation, block: u32) -> Box<[u8]> {
    let message = conversation.transcript.last().expect("the assistant message is last while tooling");
    let index = usize::try_from(block).expect("a u32 fits in a usize");
    match message.content.get(index).expect("the call in flight is a block of the message") {
        Block::ToolCall { id, .. } => id.clone(),
        Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {
            unreachable!("the call in flight is a tool call")
        }
    }
}

/// The text blocks of `content`, one after another, in a box of their own: the
/// transcript keeps the message, and the opener gets the copy (copy at
/// emission).
fn text_of(content: &[Block]) -> Box<[u8]> {
    let mut len: usize = 0;
    for block in content {
        match block {
            Block::Text { text, .. } | Block::Refusal { text, .. } => {
                len = len.checked_add(text.len()).expect("bytes held fit in a usize");
            }
            Block::Opaque { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => {}
        }
    }
    let mut text = Writer::new(len);
    for block in content {
        match block {
            Block::Text { text: part, .. } | Block::Refusal { text: part, .. } => {
                text.put(part).expect("the length was counted above");
            }
            Block::Opaque { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => {}
        }
    }
    text.finish()
}

/// How many tool calls `content` holds, and how many of them are invalid. A
/// message too long to count in a `u32` counts as having none, and yields.
fn tally(content: &[Block]) -> (u32, u32) {
    if u32::try_from(content.len()).is_err() {
        return (0, 0);
    }
    let (mut calls, mut invalid): (u32, u32) = (0, 0);
    for block in content {
        match block {
            Block::ToolCall {
                call: Decoded::Builtin { .. } | Decoded::Owned { .. } | Decoded::Delegated { .. } | Decoded::Historical,
                ..
            } => {
                calls = calls.saturating_add(1);
            }
            Block::ToolCall { call: Decoded::Invalid { .. }, .. } => {
                calls = calls.saturating_add(1);
                invalid = invalid.saturating_add(1);
            }
            Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {}
        }
    }
    (calls, invalid)
}

/// The session's policy decision after a failed attempt (domain/session.md, section 4).
enum Retry {
    /// A transient failure waits for this backoff before another attempt.
    After(Duration),
    /// A lasting failure or exhausted retry allowance ends the session.
    Fail,
    /// The whole call or the session time ended; no retry is armed.
    TimeUp,
}

fn retry_decision(failure: Failure, attempt: u32, now: Time, expiry: Time, limits: &Limits, rng: &mut Rng) -> Retry {
    if now >= expiry {
        return Retry::TimeUp;
    }
    match failure {
        Failure::TimedOut { phase } => match phase {
            crate::llm::Phase::Whole => return Retry::TimeUp,
            crate::llm::Phase::Connect
            | crate::llm::Phase::Handshake
            | crate::llm::Phase::Head
            | crate::llm::Phase::Idle => {}
        },
        Failure::Overloaded
        | Failure::Unavailable
        | Failure::Unauthorized
        | Failure::RateLimited { .. }
        | Failure::ContextTooLong
        | Failure::Refused
        | Failure::Program { .. }
        | Failure::SessionGone
        | Failure::SignedOut
        | Failure::Unsupported
        | Failure::Invalid
        | Failure::Exhausted { .. }
        | Failure::Limit { .. }
        | Failure::ApplicationLimit
        | Failure::Protocol
        | Failure::Cancelled => {}
    }
    match backoff(failure, attempt, limits, rng) {
        Some(delay) => Retry::After(delay),
        None => Retry::Fail,
    }
}

/// How long to wait before retrying a call that failed with `failure` after
/// `attempt` retries, or `None` if it is not to be retried.
fn backoff(failure: Failure, attempt: u32, limits: &Limits, rng: &mut Rng) -> Option<Duration> {
    let floor = match failure {
        Failure::Overloaded | Failure::Unavailable | Failure::TimedOut { .. } | Failure::Unauthorized => Duration::ZERO,
        Failure::RateLimited { retry_after } => retry_after,
        Failure::ContextTooLong
        | Failure::Refused
        | Failure::Program { .. }
        | Failure::SessionGone
        | Failure::SignedOut
        | Failure::Unsupported
        | Failure::Invalid
        | Failure::Exhausted { .. }
        | Failure::Limit { .. }
        | Failure::ApplicationLimit
        | Failure::Protocol
        | Failure::Cancelled => return None,
    };
    if attempt >= limits.retries {
        return None;
    }
    // Exponential and capped, with equal jitter: half fixed, half random.
    let factor = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let ceiling = limits.backoff_base.saturating_mul(factor).min(limits.backoff_max);
    let half = ceiling.as_nanos() / 2;
    let jittered = Duration::from_nanos(half.saturating_add(rng.below(half.saturating_add(1))));
    Some(jittered.max(floor))
}

/// Counts `cost` more bytes against the session's limit, or says they do not
/// fit.
#[must_use]
fn charge(conversation: &mut Conversation, cost: Option<u64>, limits: &Limits) -> bool {
    let Some(cost) = cost else {
        return false;
    };
    let Some(bytes) = conversation.bytes.checked_add(cost) else {
        return false;
    };
    match bytes.checked_add(conversation.reserved) {
        Some(total) if total <= limits.history_bytes => {}
        Some(_) | None => return false,
    }
    conversation.bytes = bytes;
    true
}

pub(crate) fn provider_reserve(limits: &Limits) -> Option<u64> {
    if limits.completion_bytes == 0 || limits.completion_blocks == 0 {
        return None;
    }
    // All copied result IDs and Invalid details already appear in C's content
    // payload, so their aggregate is <= C. Result wrappers require an additional
    // N*max(Block,Slot), irrespective of enum padding. Slot containers and
    // simultaneous result assembly are measured under scenario ceilings.
    let cell = size_of::<Block>().max(size_of::<Slot>());
    let wrappers = u64::try_from(cell).ok()?.checked_mul(u64::from(limits.completion_blocks))?;
    // Rules 1 and 2: copied IDs/details share one decoded answer total.
    let completion = limits.completion_bytes.checked_add(limits.answer_bytes)?.checked_add(wrappers)?;
    let terminal = u64::try_from(size_of::<Failure>())
        .ok()?
        .checked_add(u64::try_from(size_of::<crate::llm::Evidence>()).ok()?)?
        .checked_add(u64::try_from(size_of::<Box<[u8]>>()).ok()?)?
        .checked_add(u64::from(limits.failure_bytes))?;
    Some(completion.max(terminal))
}

fn blocks_fit(conversation: &Conversation, added: u32, limits: &Limits) -> bool {
    let mut held = added;
    for message in &conversation.transcript {
        let Ok(blocks) = u32::try_from(message.content.len()) else {
            return false;
        };
        held = match held.checked_add(blocks) {
            Some(held) => held,
            None => return false,
        };
        if held > limits.history_blocks {
            return false;
        }
    }
    held <= limits.history_blocks
}

fn receiving_reserve(conversation: &Conversation, limits: &Limits) -> Option<u64> {
    let choices =
        choice_bytes(conversation.choice_names.as_deref()).checked_mul(u64::from(limits.completion_blocks))?;
    provider_reserve(limits)?.checked_add(choices)
}

fn provider_room(conversation: &Conversation, limits: &Limits) -> bool {
    if conversation.compaction.asking() {
        return provider_reserve(limits).is_some();
    }
    let Some(pending) = limits.completion_blocks.checked_mul(2) else {
        return false;
    };
    if !blocks_fit(conversation, pending, limits) {
        return false;
    }
    let Some(reserve) = receiving_reserve(conversation, limits) else {
        return false;
    };
    if conversation.transcript.room() < 2 {
        return false;
    }
    let Some(held) = conversation.bytes.checked_add(conversation.reserved) else {
        return false;
    };
    match held.checked_add(reserve) {
        Some(total) => total <= limits.history_bytes,
        None => false,
    }
}

fn reserve_provider(conversation: &mut Conversation, limits: &Limits) -> bool {
    if !provider_room(conversation, limits) {
        return false;
    }
    // A summary replaces the old window; its input is covered by staging,
    // and no completion or result skeleton is appended to that history.
    let credit = if conversation.compaction.asking() {
        0
    } else {
        receiving_reserve(conversation, limits).expect("checked receiving bounds")
    };
    assert!(conversation.provider_credit.is_none(), "one provider right at a time");
    conversation.reserved = conversation.reserved.checked_add(credit).expect("checked receiving room");
    conversation.provider_credit = Some(credit);
    true
}

fn release_provider(conversation: &mut Conversation) {
    if let Some(credit) = conversation.provider_credit.take() {
        conversation.reserved = conversation.reserved.checked_sub(credit).expect("actual provider right owned credit");
    }
}

fn receive_completion(conversation: &mut Conversation, content: &[Block], calls: u32, limits: &Limits) -> bool {
    let cost = if calls == 0 {
        content_cost(content)
    } else {
        held(content, calls, &conversation.choice, conversation.choice_names.as_deref())
    };
    let credit = conversation.provider_credit.take().expect("actual completion owns receiving credit");
    conversation.reserved = conversation.reserved.checked_sub(credit).expect("provider credit was reserved");
    let Some(content_bytes) = content_cost(content) else {
        return false;
    };
    if content_bytes > limits.completion_bytes || count(content.len()) > limits.completion_blocks {
        return false;
    }
    let Some(cost) = cost else {
        return false;
    };
    assert!(cost <= credit, "complete content and result skeleton obey reserved caps");
    let fits = charge(conversation, Some(cost), limits);
    conversation.window.completed(
        conversation.recording.pending.expect("ordinary completion usage retained"),
        conversation.output,
        conversation.transcript.len().checked_add(1).expect("reserved assistant slot"),
        compaction::prompt_bytes(conversation).unwrap_or(u64::MAX),
    );
    assert!(fits, "valid actual completion consumes space reserved before provider work");
    true
}

fn owned_credit(call: &Call, limits: &Limits) -> u64 {
    tools::result_worst_case(call, &limits.tools).expect("admitted result bounds")
}

fn delegated_credit(limits: &Limits) -> u64 {
    limits.delegated_result_bytes
}

fn receive_result(conversation: &mut Conversation, credit: u64, result: &Returned, limits: &Limits) {
    let cost = returned_cost(result).expect("actual bounded result cost fits");
    assert!(cost <= credit, "actual lower terminal obeys its pre-effect result cap");
    conversation.reserved = conversation.reserved.checked_sub(credit).expect("actual call owned result credit");
    let fits = charge(conversation, Some(cost), limits);
    assert!(fits, "valid actual result consumes reserved space through close");
}

/// What a spec costs: its names, the tools its opener serves, and its first
/// message.
fn spec_cost(model: &[u8], system: &[u8], delegated: &[Descriptor], content: &[Block]) -> Option<u64> {
    let descriptor = u64::try_from(size_of::<Descriptor>()).ok()?;
    let delegated = descriptor.checked_mul(u64::try_from(delegated.len()).ok()?)?;
    len(model)?.checked_add(len(system)?)?.checked_add(delegated)?.checked_add(content_cost(content)?)
}

/// What an assistant message with `calls` tool calls costs while its tools run:
/// the message, a block for each call's result, the copied ID it echoes and
/// the answers to invalid calls, which are known already. Admission secured this whole
/// skeleton before provider work and separately reserves each batch's payloads
/// before effects. The terminal transfers its credit into held bytes.
/// Slot containers and their transient coexistence with assembled Block arrays
/// are measured under scenario ceilings; no Slot/Block size inequality is assumed.
fn held(content: &[Block], calls: u32, choice: &ToolChoice, names: Option<&[Box<[u8]>]>) -> Option<u64> {
    let slots = u64::try_from(size_of::<Block>()).ok()?.checked_mul(u64::from(calls))?;
    let mut cost = content_cost(content)?.checked_add(slots)?;
    for block in content {
        if !chosen(choice, block) {
            cost = cost.checked_add(choice_bytes(names))?;
        }
        match block {
            Block::ToolCall { id, name: _, input: _, call: Decoded::Invalid { problem, .. }, .. } => {
                cost = cost.checked_add(len(id)?)?.checked_add(problem_cost(problem)?)?;
            }
            Block::ToolCall {
                id,
                name: _,
                input: _,
                call: Decoded::Builtin { .. } | Decoded::Owned { .. } | Decoded::Delegated { .. } | Decoded::Historical,
                ..
            } => {
                cost = cost.checked_add(len(id)?)?;
            }
            Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {}
        }
    }
    Some(cost)
}

pub(crate) fn content_cost(content: &[Block]) -> Option<u64> {
    let mut cost: u64 = 0;
    for block in content {
        cost = cost.checked_add(block_cost(block)?)?;
    }
    Some(cost)
}

/// A block's fixed size plus its payload.
fn block_cost(block: &Block) -> Option<u64> {
    u64::try_from(size_of::<Block>()).ok()?.checked_add(payload_cost(block)?)
}

/// The bytes a block holds beyond its fixed size: its own, and those of the
/// call or outcome it carries.
fn payload_cost(block: &Block) -> Option<u64> {
    match block {
        Block::Text { text, replay } | Block::Refusal { text, replay } => {
            len(text)?.checked_add(replay_cost(replay.as_ref())?)
        }
        Block::Opaque { bytes } => len(bytes),
        Block::ToolCall { id, name, input, call, replay } => {
            // A delegated call is the opener's to hold.
            let decoded = match call {
                Decoded::Owned { call } => call_cost(call)?,
                Decoded::Builtin { .. } | Decoded::Delegated { .. } | Decoded::Historical => 0,
                Decoded::Invalid { problem, .. } => problem_cost(problem)?,
            };
            len(id)?
                .checked_add(len(name)?)?
                .checked_add(len(input)?)?
                .checked_add(decoded)?
                .checked_add(replay_cost(replay.as_ref())?)
        }
        Block::ToolResult { id, result } => len(id)?.checked_add(returned_cost(result)?),
    }
}

/// The replay wrapper is inline in Block; its owned envelope is additional.
fn replay_cost(replay: Option<&crate::llm::Replay>) -> Option<u64> {
    match replay {
        Some(replay) => len(&replay.bytes),
        None => Some(0),
    }
}

/// The bytes a tool call's result holds beyond its block and its id.
fn returned_cost(result: &Returned) -> Option<u64> {
    match result {
        Returned::Owned { outcome } => outcome_cost(outcome),
        Returned::Invalid { problem } => problem_cost(problem),
        Returned::NotRun { why } => match why {
            crate::llm::NotRun::Stopped | crate::llm::NotRun::AfterFailedWrite { .. } => Some(0),
            crate::llm::NotRun::OutsideChoice { choice } => Some(choice_bytes(choice.as_deref())),
        },
        Returned::Withdrawn => Some(0),
        Returned::Text { text, replay, .. } => len(text)?.checked_add(replay_cost(replay.as_ref())?),
    }
}

fn call_cost(call: &Call) -> Option<u64> {
    call.owned_bytes()?.checked_sub(u64::try_from(size_of::<Call>()).ok()?)
}

fn outcome_cost(outcome: &Outcome) -> Option<u64> {
    match outcome {
        Outcome::NotFound { path }
        | Outcome::NotDirectory { path }
        | Outcome::Outside { path }
        | Outcome::ReadOnly { path } => path.owned_bytes(),
        Outcome::Read { content, .. } => len(content),
        Outcome::Listed { entries, more: _ } => {
            let mut cost = u64::try_from(size_of::<Entry>()).ok()?.checked_mul(u64::try_from(entries.len()).ok()?)?;
            for entry in entries {
                cost = cost.checked_add(len(entry.name.as_ref())?)?;
            }
            Some(cost)
        }
        Outcome::Found { hits, more: _, timed_out: _ } => {
            let mut cost = u64::try_from(size_of::<tools::Hit>()).ok()?.checked_mul(u64::try_from(hits.len()).ok()?)?;
            for hit in hits {
                cost = cost.checked_add(len(&hit.path)?)?.checked_add(len(&hit.text)?)?;
            }
            Some(cost)
        }
        Outcome::Exited { head, tail, .. } | Outcome::Running { head, tail, .. } | Outcome::Job { head, tail, .. } => {
            len(head)?.checked_add(len(tail)?)
        }
        Outcome::EditFailed { edit: _, why } => match why {
            tools::Mismatch::Ambiguous { count: _, lines } => {
                u64::try_from(size_of::<u32>()).ok()?.checked_mul(u64::try_from(lines.len()).ok()?)
            }
            tools::Mismatch::NoMatch | tools::Mismatch::Unchanged => Some(0),
        },
        Outcome::Written { .. }
        | Outcome::Edited { .. }
        | Outcome::NotGranted
        | Outcome::TooLong
        | Outcome::NotFile
        | Outcome::Linked
        | Outcome::Protected
        | Outcome::TooLarge { .. }
        | Outcome::NotRead
        | Outcome::Stale
        | Outcome::Failed { .. }
        | Outcome::TimedOut
        | Outcome::Cancelled
        | Outcome::Busy
        | Outcome::NulByte => Some(0),
    }
}

fn problem_cost(problem: &Problem) -> Option<u64> {
    match problem {
        Problem::UnknownTool
        | Problem::Nesting
        | Problem::NotAnObject
        | Problem::TooLarge
        | Problem::Oversize { .. }
        | Problem::CutOff { .. }
        | Problem::JobsFull { .. }
        | Problem::UnknownJob { .. } => Some(0),
        Problem::Missing { field } | Problem::WrongType { field } | Problem::BadValue { field } => len(field),
    }
}

fn len(bytes: &[u8]) -> Option<u64> {
    u64::try_from(bytes.len()).ok()
}

/// A count for a fact, which saturates rather than fails: facts decide
/// nothing.
fn count(items: usize) -> u32 {
    u32::try_from(items).unwrap_or(u32::MAX)
}

/// Admit concrete history before acquiring a tools kit or making a completion.
pub(crate) fn open(
    domain: &mut Domain,
    env: &Env<Limits>,
    opener: Token,
    opening: crate::record::Opening,
    out: &mut Queue<Request>,
) {
    let mark = out.len();
    if domain.sessions.is_full() {
        refuse(&mut domain.facts, opener, End::Busy, out);
        return;
    }
    let crate::record::Opening { spec, window_llm, dialect, prices, budget, transcript, answered } = opening;
    if prices.unit == 0 || budget > env.limits.spend {
        refuse(&mut domain.facts, opener, End::Invalid, out);
        return;
    }
    let recording = Recording {
        dialect,
        prices,
        budget,
        spent: 0,
        own_spent: 0,
        sequence: 0,
        told: 0,
        pending: None,
        reservation: None,
        last_charge: 0,
    };
    let Some((mut conversation, authority)) = admit(opener, spec, recording, &env.limits, env.now) else {
        refuse(&mut domain.facts, opener, End::Invalid, out);
        return;
    };
    let mut sequence = 0;
    let mut told = 0;
    match transcript {
        None => {
            if !answered.is_empty() {
                refuse(
                    &mut domain.facts,
                    opener,
                    End::TranscriptRefused { reason: crate::record::Refusal::Malformed },
                    out,
                );
                return;
            }
        }
        Some(transcript) => match restore(&mut conversation, transcript, answered, &env.limits) {
            Ok(previous) => {
                sequence = previous;
                told = conversation.transcript.len().saturating_sub(1);
            }
            Err(reason) => {
                refuse(&mut domain.facts, opener, End::TranscriptRefused { reason }, out);
                return;
            }
        },
    }
    if let Some(llm) = window_llm
        && let Some(end) = compaction::change_model(&mut conversation, llm, &env.limits)
    {
        refuse(&mut domain.facts, opener, end, out);
        return;
    }
    conversation.recording.sequence = sequence;
    conversation.recording.told = told;
    open_admitted(domain, env, opener, conversation, authority, out, mark);
}

/// The restored opening is either fresh user text or the result of the last own wait.
#[derive(Debug)]
enum Resumed {
    Settled,
    WaitWithheld { id: Box<[u8]> },
}

fn recorded_sequence(turn: &crate::record::Turn, opening: bool, previous: u32) -> Result<u32, crate::record::Refusal> {
    use crate::record::{Kind, Refusal};
    match turn.kind {
        Kind::First(_) => {
            if !opening || turn.sequence != 1 {
                return Err(Refusal::Malformed);
            }
            Ok(previous)
        }
        Kind::Window(_) => {
            if !opening {
                return Err(Refusal::Malformed);
            }
            turn.sequence.checked_sub(1).ok_or(Refusal::Malformed)
        }
        Kind::Ordinary => {
            if opening {
                return Err(Refusal::Malformed);
            }
            Ok(previous)
        }
    }
}

fn recorded_turn_shape(turn: &crate::record::Turn, assistants: u32) -> Result<(), crate::record::Refusal> {
    use crate::record::Refusal;
    match turn.kind {
        crate::record::Kind::First(_) | crate::record::Kind::Ordinary => {
            if assistants != 1 {
                return Err(Refusal::Malformed);
            }
        }
        crate::record::Kind::Window(_) => {
            if turn.messages.len() != 1 || assistants != 0 {
                return Err(Refusal::Malformed);
            }
            let message = turn.messages.first().expect("validated one window message");
            for block in &message.content {
                match block {
                    Block::Text { .. } => {}
                    Block::Refusal { .. }
                    | Block::Opaque { .. }
                    | Block::ToolCall { .. }
                    | Block::ToolResult { .. } => {
                        return Err(Refusal::Malformed);
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_transcript(
    conversation: &Conversation,
    transcript: &crate::record::Transcript,
    answered: &[crate::record::Answered],
    limits: &Limits,
) -> Result<(u32, u64, Resumed), crate::record::Refusal> {
    use crate::record::{Refusal, VERSION};
    if transcript.version != VERSION {
        return Err(Refusal::Version);
    }
    // Check sizes before cloning or allocating history into the bounded list.
    if transcript.turns.len() > usize::try_from(limits.history_messages).expect("u32 fits") {
        return Err(Refusal::TooLarge);
    }
    let mut count = 1_u32;
    let mut blocks = 1_u32;
    let mut bytes = conversation.bytes;
    let mut sequence = 0_u32;
    let mut opening = true;
    let mut previous: Option<&Message> = None;
    let mut withheld: Option<&[u8]> = None;
    for turn in &transcript.turns {
        if turn.version != VERSION {
            return Err(Refusal::Version);
        }
        if turn.model != transcript.turns.first().expect("a turn implies nonempty history").model {
            return Err(Refusal::Malformed);
        }
        sequence = recorded_sequence(turn, opening, sequence)?;
        opening = false;
        sequence = sequence.checked_add(1).ok_or(Refusal::Malformed)?;
        if turn.sequence != sequence || turn.messages.is_empty() {
            return Err(Refusal::Malformed);
        }
        if turn.messages.len() > usize::try_from(limits.history_messages).expect("u32 fits") {
            return Err(Refusal::TooLarge);
        }
        let mut assistants = 0_u32;
        for message in &turn.messages {
            blocks = recorded_blocks(blocks, message, limits)?;
            count = count.checked_add(1).ok_or(Refusal::TooLarge)?;
            if count >= limits.history_messages {
                return Err(Refusal::TooLarge);
            }
            recorded_shape(message, limits)?;
            validate_recorded(message)?;
            match withheld {
                Some(id) => {
                    validate_wait_result(message, id)?;
                    withheld = None;
                }
                None => {
                    let missing = validate_link(previous, message)?;
                    if let Some(position) = missing {
                        let assistant = previous.expect("a missing result follows its assistant call");
                        withheld = match assistant
                            .content
                            .get(usize::try_from(position).expect("u32 fits"))
                            .expect("validated call position")
                        {
                            Block::ToolCall { id, .. } => Some(id),
                            Block::Text { .. }
                            | Block::Opaque { .. }
                            | Block::Refusal { .. }
                            | Block::ToolResult { .. } => {
                                unreachable!("validation returns a call position")
                            }
                        };
                    }
                }
            }
            previous = Some(message);
            if message.role == Role::Assistant {
                assistants = assistants.saturating_add(1);
            }
            bytes =
                bytes.checked_add(content_cost(&message.content).ok_or(Refusal::TooLarge)?).ok_or(Refusal::TooLarge)?;
            if bytes > limits.history_bytes {
                return Err(Refusal::TooLarge);
            }
        }
        recorded_turn_shape(turn, assistants)?;
    }
    if sequence == 0 {
        return Err(Refusal::Malformed);
    }
    let tail = previous.expect("validated nonempty turn");
    validate_answers(tail, answered, sequence)?;
    if count.checked_add(3).ok_or(Refusal::TooLarge)? > limits.history_messages || bytes > limits.history_bytes {
        return Err(Refusal::TooLarge);
    }
    let resumed = resumed_tail(tail, withheld);
    Ok((sequence, bytes, resumed))
}

/// Every recorded block leaves receiving credit for an assistant and its results.
fn recorded_blocks(blocks: u32, message: &Message, limits: &Limits) -> Result<u32, crate::record::Refusal> {
    use crate::record::Refusal;
    let blocks = blocks
        .checked_add(u32::try_from(message.content.len()).or(Err(Refusal::TooLarge))?)
        .ok_or(Refusal::TooLarge)?;
    let reserved = limits.completion_blocks.checked_mul(2).ok_or(Refusal::TooLarge)?;
    if blocks.checked_add(reserved).ok_or(Refusal::TooLarge)? > limits.history_blocks {
        return Err(Refusal::TooLarge);
    }
    Ok(blocks)
}

fn validate_answers(
    tail: &Message,
    answered: &[crate::record::Answered],
    sequence: u32,
) -> Result<(), crate::record::Refusal> {
    use crate::record::Refusal;
    for (index, answer) in answered.iter().enumerate() {
        if tail.role != Role::Assistant || answer.origin.sequence != sequence {
            return Err(Refusal::Malformed);
        }
        let Ok(position) = usize::try_from(answer.origin.position) else {
            return Err(Refusal::Malformed);
        };
        match tail.content.get(position) {
            Some(Block::ToolCall { .. }) => {}
            Some(Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. })
            | None => {
                return Err(Refusal::Malformed);
            }
        }
        for earlier in answered.iter().take(index) {
            if earlier.origin == answer.origin {
                return Err(Refusal::Malformed);
            }
        }
        match answer.result {
            Returned::Text { .. } => {}
            Returned::Owned { .. } | Returned::Invalid { .. } | Returned::NotRun { .. } | Returned::Withdrawn => {
                return Err(Refusal::Malformed);
            }
        }
    }
    Ok(())
}

fn resumed_tail(tail: &Message, withheld: Option<&[u8]>) -> Resumed {
    match withheld {
        Some(id) => Resumed::WaitWithheld { id: skein_lib::bytes::copy_of(id) },
        None => match tail.role {
            Role::User => Resumed::Settled,
            Role::Assistant => {
                let mut wait = None;
                for block in &tail.content {
                    match block {
                        Block::ToolCall { id, name, .. } if name.as_ref() == b"wait" => {
                            if wait.is_some() {
                                // A generic assistant tail may hold several unstarted calls.
                                // Only the single own wait tail identifies a held wait.
                                return Resumed::Settled;
                            }
                            wait = Some(id);
                        }
                        Block::ToolCall { .. }
                        | Block::Text { .. }
                        | Block::Opaque { .. }
                        | Block::Refusal { .. }
                        | Block::ToolResult { .. } => {}
                    }
                }
                match wait {
                    Some(id) => Resumed::WaitWithheld { id: id.clone() },
                    None => Resumed::Settled,
                }
            }
        },
    }
}

fn restore(
    conversation: &mut Conversation,
    mut transcript: crate::record::Transcript,
    mut answered: Box<[crate::record::Answered]>,
    limits: &Limits,
) -> Result<u32, crate::record::Refusal> {
    use crate::record::Refusal;
    let (sequence, mut bytes, resumed) = validate_transcript(conversation, &transcript, &answered, limits)?;
    conversation.jobs.highest = transcript.turns.last().expect("validated nonempty history").jobs;
    compaction::restore(conversation, &transcript, &mut bytes)?;
    bytes = bytes.checked_sub(compaction::carry_over(conversation, &mut transcript)?).ok_or(Refusal::TooLarge)?;
    conversation.affinity = match &transcript.turns.first().expect("validated nonempty history").kind {
        crate::record::Kind::First(opener) | crate::record::Kind::Window(opener) => opener.affinity,
        crate::record::Kind::Ordinary => unreachable!("validated opening kind"),
    };
    let prompt = conversation.transcript.get(0).expect("admission held the initial prompt");
    let prompt_charge = content_cost(&prompt.content).ok_or(Refusal::TooLarge)?;
    let last =
        transcript.turns.last().expect("validated nonempty history").messages.last().expect("validated nonempty turn");
    let waiting = match &resumed {
        Resumed::Settled => false,
        Resumed::WaitWithheld { .. } => true,
    };
    let added = match last.role {
        Role::Assistant => {
            unrun_cost(&last.content, initial_text(prompt), &answered, waiting).ok_or(Refusal::TooLarge)?
        }
        Role::User => match &resumed {
            Resumed::Settled => prompt_charge,
            Resumed::WaitWithheld { id } => {
                prompt_charge.checked_add(len(id).ok_or(Refusal::TooLarge)?).ok_or(Refusal::TooLarge)?
            }
        },
    };
    // The yielded tail needs one concrete result per call and copied ids.
    // Account all of them before moving history or allocating waking content.
    bytes = bytes.checked_sub(prompt_charge).ok_or(Refusal::TooLarge)?;
    bytes = bytes.checked_add(added).ok_or(Refusal::TooLarge)?;
    let reserve = provider_reserve(limits).ok_or(Refusal::TooLarge)?;
    if bytes.checked_add(reserve).ok_or(Refusal::TooLarge)? > limits.history_bytes {
        return Err(Refusal::TooLarge);
    }
    // Move the waking prompt: cloning its potentially large text before
    // clearing the initial transcript would also exceed the bounded staging.
    let initial = mem::replace(&mut conversation.transcript, List::with_capacity(limits.history_messages));
    let mut initial = initial.into_boxed().into_iter();
    let prompt = initial.next().expect("admission held the initial prompt");
    assert!(initial.next().is_none(), "admission holds exactly one user message");
    drop(initial);
    for turn in transcript.turns {
        for message in turn.messages {
            conversation.transcript.push(message).expect("history counted before allocation");
        }
    }
    conversation.window.last_prompt_bytes = compaction::prompt_prefix_bytes(
        conversation,
        usize::try_from(conversation.window.following.saturating_sub(1)).expect("bounded history"),
    )
    .ok_or(Refusal::TooLarge)?;
    let last = conversation.transcript.last().expect("nonempty history");
    let content = match last.role {
        Role::Assistant => {
            let mut blocks = prompt.content.into_iter();
            let text = match blocks.next().expect("admission's initial text") {
                Block::Text { text, .. } => text,
                Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => {
                    unreachable!("admission constructs one text block")
                }
            };
            unrun(conversation, text, &mut answered, waiting)
        }
        Role::User => match resumed {
            Resumed::Settled => prompt.content,
            Resumed::WaitWithheld { id } => {
                let mut parts = prompt.content.into_iter();
                let text = match parts.next().expect("admission's initial text") {
                    Block::Text { text, .. } => text,
                    Block::Refusal { .. }
                    | Block::Opaque { .. }
                    | Block::ToolCall { .. }
                    | Block::ToolResult { .. } => {
                        unreachable!("admission constructs one text block")
                    }
                };
                Box::new([Block::ToolResult { id, result: Returned::Text { text, error: false, replay: None } }])
            }
        },
    };
    conversation.transcript.push(Message { role: Role::User, content }).expect("one prompt reserved");
    conversation.bytes = bytes;
    Ok(sequence)
}

fn initial_text(prompt: &Message) -> &[u8] {
    match prompt.content.first().expect("admission's initial text") {
        Block::Text { text, .. } => text,
        Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => {
            unreachable!("admission constructs one text block")
        }
    }
}

/// The waking user message plus every saved call's result or `NotRun`.
/// Counts the eventual concrete blocks and copied provider ids without owning
/// any of them, so a refusal allocates no tail-sized scratch.
fn unrun_cost(content: &[Block], text: &[u8], answered: &[crate::record::Answered], waiting: bool) -> Option<u64> {
    let block = u64::try_from(size_of::<Block>()).ok()?;
    let mut cost = len(text)?;
    if !waiting {
        cost = cost.checked_add(block)?;
    }
    for (position, part) in content.iter().enumerate() {
        match part {
            Block::ToolCall { id, .. } => {
                cost = cost.checked_add(block)?.checked_add(len(id)?)?;
                for answer in answered {
                    if usize::try_from(answer.origin.position).ok()? == position {
                        match &answer.result {
                            Returned::Text { text, .. } => cost = cost.checked_add(len(text)?)?,
                            Returned::Owned { .. }
                            | Returned::Invalid { .. }
                            | Returned::NotRun { .. }
                            | Returned::Withdrawn => {
                                return None;
                            }
                        }
                    }
                }
            }
            Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {}
        }
    }
    Some(cost)
}

fn validate_recorded(message: &Message) -> Result<(), crate::record::Refusal> {
    use crate::record::Refusal;
    for block in &message.content {
        match block {
            Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } => {}
            Block::ToolCall { call, .. } => {
                if message.role != Role::Assistant {
                    return Err(Refusal::Malformed);
                }
                match call {
                    Decoded::Historical => {}
                    Decoded::Builtin { .. }
                    | Decoded::Owned { .. }
                    | Decoded::Delegated { .. }
                    | Decoded::Invalid { .. } => {
                        return Err(Refusal::Unresolved);
                    }
                }
            }
            Block::ToolResult { .. } => {
                if message.role != Role::User {
                    return Err(Refusal::Malformed);
                }
            }
        }
    }
    Ok(())
}

fn tell_turn(conversation: &mut Conversation, out: &mut Queue<Request>) -> Option<End> {
    if !conversation.builtin.is_empty() {
        return None;
    }
    let (dialect, sequence, told, usage, spent) = {
        let Recording { dialect, sequence, told, pending, spent, .. } = &mut conversation.recording;

        let usage = pending.take()?;
        (*dialect, sequence, told, usage, *spent)
    };
    let Some(next) = sequence.checked_add(1) else {
        return Some(history::sequence_limit());
    };
    let mut messages = List::with_capacity(conversation.transcript.len().saturating_sub(*told));
    for message in conversation.transcript.iter().skip(usize::try_from(*told).expect("u32 fits")) {
        let mut blocks = List::with_capacity(count(message.content.len()));
        for block in &message.content {
            let block = match block {
                Block::ToolCall { id, name, input, replay, .. } => Block::ToolCall {
                    id: id.clone(),
                    name: name.clone(),
                    input: input.clone(),
                    call: Decoded::Historical,
                    replay: replay.clone(),
                },
                Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {
                    block.clone()
                }
            };
            blocks.push(block).expect("one copied block per original");
        }
        messages
            .push(Message { role: message.role, content: blocks.into_boxed() })
            .expect("one copied message per original");
    }
    *sequence = next;
    *told = conversation.transcript.len();
    let turn = crate::record::Turn {
        version: crate::record::VERSION,
        kind: if conversation.window_turn {
            conversation.window_turn = false;
            crate::record::Kind::Window(Box::new(crate::record::Opener {
                affinity: conversation.affinity,
                account: conversation.account,
                backend: conversation.backend.clone(),
            }))
        } else if next == 1 {
            crate::record::Kind::First(Box::new(crate::record::Opener {
                affinity: conversation.affinity,
                account: conversation.account,
                backend: conversation.backend.clone(),
            }))
        } else {
            crate::record::Kind::Ordinary
        },
        endpoint: Some(conversation.endpoint),
        model: conversation.model.clone(),
        effort: conversation.effort.clone(),
        dialect,
        sequence: next,
        usage,
        spent,
        jobs: conversation.jobs.highest,
        messages: messages.into_boxed(),
    };
    out.push(Request::Turn { opener: conversation.opener, turn });
    None
}

/// Opener-requested continuation after its actual result and tool turn settle.
#[derive(Clone, Copy, Debug)]
pub(crate) enum After {
    /// Request the next completion directly.
    Complete,
    /// Return control to the opener before requesting the next completion.
    Yield,
}

/// A child's exact cumulative inclusive bill. This fixed handoff carries no
/// payload and never changes the receiver's own price.
#[derive(Debug)]
pub(crate) struct Bill {
    /// The opener's choice after the settled batch; closing always settles without continuation.
    pub(crate) after: After,
    /// Exact child bill, added once after the identity guard.
    pub(crate) spent: u64,
}

/// An addressed delegated terminal charges its inclusive bill once before
/// settling its result. Retired, stale, wrong-owner and duplicate bills
/// change neither price prefix; the own prefix never includes a child bill.
pub(crate) fn delegate_ended(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Token,
    result: Returned,
    bill: Bill,
    out: &mut Queue<Request>,
) {
    // This value may be redelivered by its parent. A retired or stale
    // identity cannot charge a child twice, including before reclaim.
    let Some(run) = domain.calls.runs.get(Id::from_token(owner)) else {
        return;
    };
    if run.ended || run.by != By::Opener {
        return;
    }
    let mark = out.len();
    let Run { session: id, slot, block, by, credit, metadata, ended: _ } =
        ended_run(&mut domain.calls.runs, Id::from_token(owner));
    assert!(by == By::Opener, "the opener answers its delegated call once");
    let session = domain.sessions.get_mut(id).expect("a call keeps its session alive");
    let conversation = &mut session.conversation;
    let added = conversation.recording.spent.checked_add(bill.spent);
    if let Some(total) = added {
        conversation.recording.spent = total;
        out.push(Request::Priced {
            opener: conversation.opener,
            spent: total,
            own_spent: conversation.recording.own_spent,
        });
    }
    let result = history::cut_result(result, credit);
    let fact = match &result {
        Returned::Text { text, error, replay: _ } => {
            FactKind::DelegateAnswered { opener: conversation.opener, bytes: count(text.len()).into(), error: *error }
        }
        Returned::Withdrawn => FactKind::DelegateCancelled { opener: conversation.opener },
        Returned::Owned { .. } | Returned::Invalid { .. } | Returned::NotRun { .. } => {
            unreachable!("terminals carry a concrete answer or withdrawal")
        }
    };
    domain.facts.push_call(env.now, fact, &metadata);
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Tooling { mut tools } => {
            match bill.after {
                After::Complete => {}
                After::Yield => tools.after = After::Yield,
            }
            match added {
                Some(_) => tool_ran(conversation, id, &mut domain.calls, tools, slot, block, credit, result, env, out),
                None => {
                    tools.running = tools.running.checked_sub(1).expect("settled delegated call");
                    receive_result(conversation, credit, &result, &env.limits);
                    let result = Block::ToolResult { id: call_id(conversation, block), result };
                    *tools.slots.get_mut(slot).expect("settled slot") = Slot::Done { result };
                    clear_pending(conversation);
                    abandon(conversation, &domain.calls.runs, tools, End::PriceOverflow, out)
                }
            }
        }
        State::Closing { end, waiting } => {
            let end = match added {
                Some(_) => keep_closing(conversation, slot, block, credit, result, &env.limits).unwrap_or(end),
                None => {
                    let _discarded: Option<End> = keep_closing(conversation, slot, block, credit, result, &env.limits);
                    clear_pending(conversation);
                    End::PriceOverflow
                }
            };
            settled(end, waiting)
        }
        State::Calling { .. } | State::Backoff { .. } | State::Resting { .. } | State::Yielded | State::Closed => {
            unreachable!("an answer ends an in-flight delegated call")
        }
    };
    conclude(domain, env, id, out, mark);
}

/// Release the live delegated right while retaining its unanswered historical call.
/// It also settles while closing; no provider request or yield is emitted.
pub(crate) fn delegate_withheld(domain: &mut Domain, env: &Env<Limits>, owner: Token, out: &mut Queue<Request>) {
    let Some(run) = domain.calls.runs.get(Id::from_token(owner)) else {
        return;
    };
    if run.ended || run.by != By::Opener {
        return;
    }
    let mark = out.len();
    let Run { session: id, slot, credit, metadata, .. } = ended_run(&mut domain.calls.runs, Id::from_token(owner));
    let session = domain.sessions.get_mut(id).expect("a delegated call retains its session");
    let conversation = &mut session.conversation;
    conversation.reserved = conversation.reserved.checked_sub(credit).expect("withheld call owns its result credit");
    domain.facts.push_call(env.now, FactKind::DelegateWithheld { opener: conversation.opener }, &metadata);
    let state = mem::replace(&mut session.state, State::Closed);
    session.state = match state {
        State::Tooling { mut tools } => {
            tools.running = tools.running.checked_sub(1).expect("withheld call was running");
            assert!(tools.running == 0, "the run's wait is a serialized writer");
            *tools.slots.get_mut(slot).expect("withheld call's slot") = Slot::Withheld;
            conversation.closing_tools = Some(tools);
            State::Yielded
        }
        State::Closing { end, waiting } => {
            if let Some(tools) = conversation.closing_tools.as_mut() {
                *tools.slots.get_mut(slot).expect("withheld call's closing slot") = Slot::Withheld;
            }
            settled(end, waiting)
        }
        State::Calling { .. } | State::Backoff { .. } | State::Resting { .. } | State::Yielded | State::Closed => {
            unreachable!("withheld terminal settles an in-flight opener call")
        }
    };
    conclude(domain, env, id, out, mark);
}

fn clear_pending(conversation: &mut Conversation) {
    conversation.recording.pending = None;
}

fn keep_closing(
    conversation: &mut Conversation,
    slot: u32,
    block: u32,
    credit: u64,
    result: Returned,
    limits: &Limits,
) -> Option<End> {
    conversation.closing_tools.as_ref()?;
    receive_result(conversation, credit, &result, limits);
    let result = Block::ToolResult { id: call_id(conversation, block), result };
    let tools = conversation.closing_tools.as_mut().expect("present above");
    *tools.slots.get_mut(slot).expect("the terminal fills its call's slot") = Slot::Done { result };
    None
}

fn finish_closing(conversation: &mut Conversation) {
    builtin::finish_closing(conversation);
}

pub(crate) use builtin::result as builtin_result;

pub(crate) fn builtin_worst_case(limits: &Limits) -> Option<u64> {
    builtin::Owed::worst_case(limits)
}

/// Result ids must resolve to the preceding assistant's calls, in call order.
/// A yielded assistant may be the tail; resuming supplies its `NotRun` results.
fn validate_link(previous: Option<&Message>, message: &Message) -> Result<Option<u32>, crate::record::Refusal> {
    use crate::record::Refusal;
    let Some(previous) = previous else {
        if message.role != Role::User {
            return Err(Refusal::Malformed);
        }
        for block in &message.content {
            match block {
                Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } => {}
                Block::ToolCall { .. } | Block::ToolResult { .. } => return Err(Refusal::Malformed),
            }
        }
        return Ok(None);
    };
    match message.role {
        Role::Assistant => {
            if previous.role != Role::User {
                return Err(Refusal::Malformed);
            }
            // Call ids must be unique within a provider completion.
            for (index, block) in message.content.iter().enumerate() {
                match block {
                    Block::ToolCall { id, .. } => {
                        for earlier in message.content.iter().take(index) {
                            match earlier {
                                Block::ToolCall { id: other, .. } => {
                                    if id == other {
                                        return Err(Refusal::Malformed);
                                    }
                                }
                                Block::Text { .. }
                                | Block::Refusal { .. }
                                | Block::Opaque { .. }
                                | Block::ToolResult { .. } => {}
                            }
                        }
                    }
                    Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
                }
            }
        }
        Role::User => return validate_results(previous, message),
    }
    Ok(None)
}

/// A resumed withheld result must appear before the next assistant turn.
fn validate_wait_result(message: &Message, wait: &[u8]) -> Result<(), crate::record::Refusal> {
    if message.role != Role::User {
        return Err(crate::record::Refusal::Malformed);
    }
    let mut found = false;
    for block in &message.content {
        match block {
            Block::ToolResult { id, .. } if id.as_ref() == wait && !found => found = true,
            Block::ToolResult { .. } | Block::ToolCall { .. } => return Err(crate::record::Refusal::Malformed),
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } => {}
        }
    }
    if !found {
        return Err(crate::record::Refusal::Malformed);
    }
    Ok(())
}

fn validate_results(previous: &Message, message: &Message) -> Result<Option<u32>, crate::record::Refusal> {
    use crate::record::Refusal;
    let mut next = 0_u32;
    let mut actual = next_result(&message.content, &mut next)?;
    let mut withheld = None;
    if previous.role == Role::Assistant {
        for (position, block) in previous.content.iter().enumerate() {
            match block {
                Block::ToolCall { id, name, .. } => {
                    let matches = match actual {
                        Some(position) => {
                            result_id(
                                message
                                    .content
                                    .get(usize::try_from(position).expect("u32 fits"))
                                    .expect("validated result position"),
                            ) == id.as_ref()
                        }
                        None => false,
                    };
                    if matches {
                        actual = next_result(&message.content, &mut next)?;
                    } else if name.as_ref() == b"wait" && withheld.is_none() {
                        withheld = Some(u32::try_from(position).or(Err(Refusal::TooLarge))?);
                    } else {
                        return Err(Refusal::Malformed);
                    }
                }
                Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
            }
        }
    }
    if actual.is_some() {
        return Err(Refusal::Malformed);
    }
    Ok(withheld)
}

fn result_id(block: &Block) -> &[u8] {
    match block {
        Block::ToolResult { id, .. } => id,
        Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolCall { .. } => {
            unreachable!("next_result returns a result position")
        }
    }
}

fn next_result(blocks: &[Block], next: &mut u32) -> Result<Option<u32>, crate::record::Refusal> {
    let begin = *next;
    let end = u32::try_from(blocks.len()).or(Err(crate::record::Refusal::TooLarge))?;
    for position in begin..end {
        *next = position.checked_add(1).ok_or(crate::record::Refusal::TooLarge)?;
        match blocks.get(usize::try_from(position).expect("u32 fits")).expect("within blocks") {
            Block::ToolResult { .. } => return Ok(Some(position)),
            Block::ToolCall { .. } => return Err(crate::record::Refusal::Malformed),
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } => {}
        }
    }
    Ok(None)
}

fn recorded_shape(message: &Message, limits: &Limits) -> Result<(), crate::record::Refusal> {
    let fixed = match u64::try_from(size_of::<Block>()).ok() {
        Some(block) => match u64::try_from(message.content.len()).ok() {
            Some(count) => block.checked_mul(count),
            None => None,
        },
        None => None,
    };
    match fixed {
        Some(fixed) if fixed <= limits.history_bytes => Ok(()),
        Some(_) | None => Err(crate::record::Refusal::TooLarge),
    }
}

/// Fault injection for the unit test of the integer naming fence. A
/// bounded transcript cannot hold this many turns; this tests the fence before
/// delegated or owned effects, while retaining the received provider usage.
#[cfg(test)]
pub(crate) fn exhaust_origin_for_test(domain: &mut Domain, owner: Token) {
    let session = domain.sessions.get_mut(Id::from_token(owner)).expect("live test session");
    session.conversation.recording.sequence = u32::MAX;
}

/// Reserved provider rights for focused settlement controls. This
/// observes bounded retained state, never supplies a completion or price.
#[cfg(test)]
pub(crate) fn provider_credit_for_test(domain: &Domain, owner: Token) -> (u64, Option<u64>) {
    let session = domain.sessions.get(Id::from_token(owner)).expect("admitted test session before reclaim");
    (session.conversation.reserved, session.conversation.provider_credit)
}

fn next_response(conversation: &Conversation) -> u64 {
    u64::from(conversation.turns).checked_add(1).expect("a u32 completion count fits in u64")
}

fn tool_metadata(
    id: &[u8],
    name: &[u8],
    input: &[u8],
    source: crate::ToolSource,
    effect: Effect,
    deadline: Time,
) -> crate::ToolCall {
    crate::ToolCall {
        owner: Token::new(0),
        id: skein_lib::bytes::copy_of(id),
        name: skein_lib::bytes::copy_of(name),
        source,
        effect,
        deadline,
        input_bytes: u64::try_from(input.len()).expect("provider input byte count fits u64"),
    }
}

/// Set the slab-issued observation identity before any request or fact can expose it.
fn observed_run(
    calls: &mut Calls,
    session: Id<Session>,
    slot: u32,
    block: u32,
    by: By,
    credit: u64,
    metadata: crate::ToolCall,
) -> Id<Run> {
    let run = Run { session, slot, block, by, credit, ended: false, metadata };
    let id = calls.runs.insert(run).expect("the run slab has room for regular batches and program observations");
    calls.runs.get_mut(id).expect("inserted above").metadata.owner = id.token();
    id
}

/// Facts carry terminal classification only; diagnostics go to the opener's terminal.
fn end_fact(end: &End) -> End {
    match end {
        End::Failed { failure, evidence, attempts, .. } => {
            End::Failed { failure: *failure, evidence: *evidence, attempts: *attempts, detail: Box::default() }
        }
        End::Busy
        | End::Invalid
        | End::Closed
        | End::Budget { .. }
        | End::NoSummary { .. }
        | End::TranscriptRefused { .. }
        | End::PriceOverflow
        | End::UsageOverflow => end.clone(),
    }
}
