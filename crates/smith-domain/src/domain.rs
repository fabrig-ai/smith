//! The domain's state and its entry points (programming-model.md, sections 4.5 and 6.3). Each hands what it is
//! given to the child domain it is for, then completes the hand-offs between
//! the child domains (programming-model.md, section 4.5) before it returns, routing what is for the protocol
//! layer out.
//! It retains the run and session children, conversation and credential
//! bindings and issued renewal policies, pending starts, turn handoffs, unsent completions waiting for
//! acknowledgements, bounded notices and uncapped facts awaiting the owner. [`step`],
//! [`fire`] and [`resume`] are its entries (domain/run.md, sections 2, 3, 10,
//! 13; domain/session.md, sections 3–5). It never knows provider
//! wire syntax, encoded turn size, credential secrets, forge state or host delivery policy.
//!
//! Hand-offs between the run and the sessions go both ways, so a chain of them
//! could go on within one step: a session's delegated call that the run
//! answers at once, an answer that lets the session start its next batch, a
//! close that withdraws a call whose sub-agent the run closes in turn. Two of
//! the run's records therefore reach a session only from the ready list
//! (programming-model.md, section 2): a `Return`, which the peer's delegated
//! call holds as its answer, and a `Close`, which the peer holds. The loop
//! drains the list with [`resume`] at the start of the domain's stage in a
//! later iteration, after the reclaim point, so a session starts at most one
//! batch of delegated calls an iteration, and a close cascades down the
//! sub-agent tree one level an iteration. A session takes either in any state
//! it can be in when it comes (it waits for each delegated call's answer, and
//! takes a close in any state, dropping it once it has ended). The run's `Open`
//! and `Say` go at once: they emit no delegated call or yield. Their unsent
//! completion may be denied by the root gate, adding one synchronous session
//! terminal and its run notice. `Return` and `Close` remain deferred, so this
//! cannot recurse through ancestors. A `Say` must reach the session while it
//! is still yielded, before its time can run out.
//!
//! An entry point's initial hand-offs have a finite bounded unsent settlement
//! pair in addition to the child domain it is
//! for (a session's step, say), the run's steps for what that emitted, the
//! sessions' steps for what the run opened or said at once, and the run's for
//! what those emitted; [`max_out`] follows from the child domains' along that
//! chain.

use alloc::boxed::Box;
use skein_lib::{Env, Id, Map, Queue, ReplyTo, Rng, Set, Slab, Time, Token};
use smith_domain_run as run;
use smith_domain_session::{self as session};

use crate::boundary::{Event, GrantName, Request};
use crate::config::Config;
use crate::facts::Fact;
use crate::limits::{self, Limits};
use crate::peer::Peer;
use crate::route;

/// The most requests an entry point emits per call under `limits`: what the
/// child domains emit in the most steps it takes of each (see the module), as
/// each of their requests is one of ours or a hand-off. The loop reserves this
/// much room in `out` before calling it.
#[must_use]
pub const fn max_out(limits: &Limits) -> u32 {
    // A session step issues at most one completion and one writer command;
    // commands run alone (domain/session.md, section 6). Each adds one `Long`.
    // Two notices cover the actual terminal or held completion handoff.
    limits::run_out(limits)
        .saturating_add(limits::session_out(limits))
        .saturating_add(limits::session_steps(limits).saturating_mul(2))
        .saturating_add(2)
}

/// The child observations along every bounded causal handoff of one entrance.
#[must_use]
pub const fn max_facts(limits: &Limits) -> u32 {
    limits::run_steps(limits)
        .saturating_mul(run::max_facts(&limits.run))
        .saturating_add(limits::session_steps(limits).saturating_mul(session::max_facts(&limits.session)))
}

/// The agent domain's state: its child domains', what it keeps of each
/// conversation between them, and facts awaiting the owner's drain.
#[derive(Debug)]
pub struct Domain {
    pub(crate) config: Config,
    pub(crate) run: run::Domain,
    pub(crate) session: session::Domain,
    /// The conversations the run opened, until their sessions end.
    pub(crate) peers: Slab<Peer>,
    pub(crate) starts: Slab<StartContext>,
    pub(crate) turns: Slab<TurnHandoff>,
    /// Peers by the run's token for their conversation, which is their
    /// session's opener, and by their session's own.
    pub(crate) conversations: Map<Token, Id<Peer>>,
    pub(crate) sessions: Map<Token, Id<Peer>>,
    /// Delegated calls between a session's `Delegate` and its answer, by the
    /// session's token for them.
    pub(crate) flights: Map<Token, Flight>,
    pub(crate) grants: Map<u32, Credential>,
    pub(crate) completions: Map<Token, IssuedCompletion>,
    /// One unsent main completion per run awaiting acknowledgement credit.
    pub(crate) pending: Map<Token, route::PendingCompletion>,
    pub(crate) notices: Queue<Request>,
    /// Issued command IO tokens retain their logical run until the actual terminal.
    pub(crate) commands: Map<Token, Token>,
    pub(crate) ready: Ready,
    /// Tickets the peers hold.
    pub(crate) tickets: u32,
    /// What each child domain emits in a step, until it is routed. Empty
    /// between steps.
    pub(crate) run_out: Queue<run::Request>,
    pub(crate) session_out: Queue<session::Request>,
    facts: Queue<Fact>,
    pub(crate) content: Queue<crate::Content>,
    pub(crate) content_lost: u64,
}

/// Original parent right and optional history survive run admission/preparation.
/// The main takes history once; the terminal alone consumes the right.
#[derive(Debug)]
pub(crate) struct StartContext {
    pub(crate) reply_to: Option<ReplyTo>,
    pub(crate) transcript: Option<session::record::Transcript>,
    pub(crate) answered: Box<[u8]>,
    pub(crate) refused: Option<run::TranscriptRefusal>,
}

/// Single-use ownership while a main turn crosses the sibling seam. Both
/// queues can coexist; capacity and complete envelope/payload costs are priced.
#[derive(Debug)]
pub(crate) struct TurnHandoff {
    pub(crate) turn: Option<session::record::Turn>,
}

/// Issued provider identity survives session closing and is consumed by its terminal.
#[derive(Debug)]
pub(crate) struct IssuedCompletion {
    pub(crate) renewable: bool,
    pub(crate) grant: Option<GrantName>,
    pub(crate) name: crate::llm::CompletionName,
}

#[derive(Debug)]
pub(crate) struct Credential {
    pub(crate) renewable: bool,
    pub(crate) name: GrantName,
    pub(crate) expires: Time,
    pub(crate) rejected: Option<u64>,
}

/// A delegated call in flight: the run serves it, and the peer's session
/// waits for its answer.
#[derive(Debug)]
pub(crate) struct Flight {
    pub(crate) peer: Id<Peer>,
    /// The session withdrew it.
    pub(crate) withdrawn: bool,
    pub(crate) answer: Due,
}

/// What a delegated call's session is answered.
#[derive(Debug)]
pub(crate) enum Due {
    /// The run has not returned it.
    Waiting,
    /// The run's answer, kept under a ticket, waiting on the ready list.
    Answered { feedback: crate::Feedback, spent: u64 },
    /// The settled result yields so the run can append queued work before the next request.
    Yielding { feedback: crate::Feedback, spent: u64 },
    /// A parked own wait ends its live call while leaving the historical result withheld.
    Withheld,
    /// The run returned it cancelled, after the session withdrew it.
    Cancelled { spent: u64 },
}

/// A hand-off from the run to a session, held by what it is about.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Handoff {
    /// The run closed the peer's conversation.
    Close { peer: Id<Peer> },
    /// The run answered the delegated call the session names `owner`.
    Answer { owner: Token },
}

/// The hand-offs waiting for a later iteration, each at most once: those made
/// before the last reclaim point, which [`resume`] delivers, and those made
/// since, which wait for the next.
#[derive(Debug)]
pub(crate) struct Ready {
    now: Set<Handoff>,
    next: Set<Handoff>,
}

impl Ready {
    const fn with_capacity(capacity: u32) -> Ready {
        Ready { now: Set::with_capacity(capacity), next: Set::with_capacity(capacity) }
    }

    fn is_ready(&self) -> bool {
        !self.now.is_empty()
    }

    fn pop(&mut self) -> Option<Handoff> {
        let handoff = *self.now.first()?;
        self.now.remove(&handoff);
        Some(handoff)
    }

    /// Holds `handoff` until the next iteration.
    pub(crate) fn defer(&mut self, handoff: Handoff) {
        if !self.now.contains(&handoff) {
            self.next.insert(handoff).expect("room on the ready list for every hand-off");
        }
    }

    /// Forgets `handoff`, which is moot.
    pub(crate) fn forget(&mut self, handoff: Handoff) {
        self.now.remove(&handoff);
        self.next.remove(&handoff);
    }

    /// The reclaim point: what was deferred in this iteration may be delivered
    /// in the next.
    fn promote(&mut self) {
        for _ in 0..self.next.capacity() {
            let Some(handoff) = self.next.first().copied() else {
                break;
            };
            self.next.remove(&handoff);
            self.now.insert(handoff).expect("room on the ready list for every hand-off");
        }
    }
}

impl Domain {
    /// A domain with room for `limits`, which [`crate::valid`] accepts,
    /// drawing randomness from `seed`: each child domain's seed is drawn from
    /// it.
    #[must_use]
    pub fn new(limits: &Limits, config: Config, seed: u64) -> Domain {
        assert!(config.valid(limits), "configured endpoints and models fit their limits and are unique");
        let mut rng = Rng::new(seed);
        let session_seed = rng.next_u64();
        let run_seed = rng.next_u64();
        let peers = limits.run.conversations;
        let flights = limits::flights(limits).expect("valid limits");
        let handoffs = limits::handoffs(limits).expect("valid limits");
        Domain {
            config,
            run: run::Domain::new(&limits.run, run_seed),
            session: session::Domain::new(&limits.session, session_seed),
            peers: Slab::with_capacity(peers),
            starts: Slab::with_capacity(limits.run.runs),
            turns: Slab::with_capacity(limits::session_out(limits)),
            conversations: Map::with_capacity(peers),
            sessions: Map::with_capacity(peers),
            flights: Map::with_capacity(flights),
            grants: Map::with_capacity(limits.accounts),
            completions: Map::with_capacity(limits.run.conversations),
            pending: Map::with_capacity(limits.run.runs),
            notices: Queue::with_capacity(2),
            commands: Map::with_capacity(limits::commands(limits).expect("valid limits")),
            ready: Ready::with_capacity(handoffs),
            tickets: 0,
            run_out: Queue::with_capacity(limits::run_out(limits)),
            session_out: Queue::with_capacity(limits::session_out(limits)),
            facts: Queue::uncapped(),
            content: Queue::with_capacity(limits.session.facts),
            content_lost: 0,
        }
    }

    /// The run child domain, for a world to look at.
    #[must_use]
    pub const fn run(&self) -> &run::Domain {
        &self.run
    }

    /// The session child domain, for a world to look at.
    #[must_use]
    pub const fn session(&self) -> &session::Domain {
        &self.session
    }

    /// Conversations the run opened whose sessions have not ended, ended ones
    /// included until they are reclaimed.
    #[must_use]
    pub const fn peers(&self) -> u32 {
        self.peers.len()
    }

    /// Delegated calls whose sessions have not been answered.
    #[must_use]
    pub fn flights(&self) -> u32 {
        self.flights.len()
    }

    /// Tickets held for the sessions: the asks of calls not dispatched yet,
    /// and the run's answers.
    #[must_use]
    pub const fn tickets(&self) -> u32 {
        self.tickets
    }

    /// When the earliest alarm falls due.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let session = self.session.next_deadline();
        match self.run.next_deadline() {
            Some(run) => match session {
                Some(session) => Some(run.min(session)),
                None => Some(run),
            },
            None => session,
        }
    }

    /// Whether an alarm is due at `now`. The loop calls [`fire`] while one is.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        self.run.is_due(now) || self.session.is_due(now)
    }

    /// Whether something waits to go on: a session after a batch its tools
    /// answered at once, or a hand-off from the run. While one does, the loop
    /// calls [`resume`] at the start of the domain's stage, before its input
    /// events.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.run.is_ready() || self.session.is_ready() || self.ready.is_ready()
    }

    /// The oldest fact not drained yet, the run's or the sessions' (which the
    /// tools' are among).
    pub fn pop_fact(&mut self) -> Option<Fact> {
        self.facts.pop()
    }

    /// Whether an emitted observation still waits for the parent's drain.
    #[must_use]
    pub fn has_facts(&self) -> bool {
        !self.facts.is_empty()
    }

    /// Facts held for the owner to drain (skein's draft dynamic-memory.md
    /// 4.1): the owner counts them in its backlog.
    #[must_use]
    pub fn backlog(&self) -> u32 {
        self.facts.len()
    }

    /// Channel facts with content; capture policy remains the engine's.
    pub fn pop_content(&mut self) -> Option<crate::Content> {
        self.content.pop()
    }

    /// Always ample observation room on the uncapped queue; kept for existing owners.
    #[must_use]
    pub fn facts_room(&self) -> u32 {
        self.facts.room()
    }

    /// Optional content observations omitted by the current capture buffer.
    #[must_use]
    pub fn content_lost(&self) -> u64 {
        self.content_lost
    }

    /// The reclaim point: frees what closed in this iteration.
    pub fn reclaim(&mut self) {
        self.run.reclaim();
        self.session.reclaim();
        self.peers.reclaim();
        self.starts.reclaim();
        self.turns.reclaim();
        self.ready.promote();
    }
}

/// Handles one event, emitting at most [`max_out`] requests.
pub fn step(domain: &mut Domain, env: &Env<Limits>, event: Event, out: &mut Queue<Request>) {
    route::event(domain, env, event);
    settle(domain, env, out);
}

/// Fires the earliest alarm due at `env.now`, the run's or a session's, if
/// there is one, emitting at most [`max_out`] requests. On a tie, the run's
/// goes first. A stage fires its alarms after its input events, so progress
/// that arrived in the same iteration wins over a deadline that passed while
/// the loop waited.
pub fn fire(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    let run_due = domain.run.is_due(env.now);
    let session_due = domain.session.is_due(env.now);
    if run_due && (!session_due || domain.run.next_deadline() <= domain.session.next_deadline()) {
        run::fire(&mut domain.run, &route::run_env(env), &mut domain.run_out);
    } else if session_due {
        session::fire(&mut domain.session, &route::session_env(env), &mut domain.session_out);
    }
    gather(domain, &env.limits);
    settle(domain, env, out);
}

/// Goes on with one thing that waits, if there is one, emitting at most
/// [`max_out`] requests: a session after a batch its tools answered within
/// the step that started it, or a hand-off from the run deferred in an earlier
/// iteration.
pub fn resume(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    if domain.run.is_ready() {
        run::resume(&mut domain.run, &route::run_env(env), &mut domain.run_out);
        gather(domain, &env.limits);
    } else if domain.session.is_ready() {
        session::resume(&mut domain.session, &route::session_env(env), &mut domain.session_out);
        gather(domain, &env.limits);
    } else if let Some(handoff) = domain.ready.pop() {
        route::deliver(domain, env, handoff);
    }
    settle(domain, env, out);
}

/// Completes the hand-offs, then gathers the facts.
fn settle(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>) {
    route::hand_off(domain, env, out);
    gather(domain, &env.limits);
}

/// Moves each child entrance's observations into the owner's uncapped queue.
pub(crate) fn gather(domain: &mut Domain, limits: &Limits) {
    for _ in 0..limits.run.facts {
        let Some(fact) = domain.run.pop_fact() else {
            break;
        };
        let opening = opening(domain, &fact);
        keep(domain, Fact::Run { fact, opening });
    }
    for _ in 0..limits.session.facts {
        let Some(fact) = domain.session.pop_fact() else {
            break;
        };
        keep(domain, Fact::Session { fact });
    }
}

fn keep(domain: &mut Domain, fact: Fact) {
    domain.facts.push(fact);
}

/// Gather precedes hand-off: the actual Open is still owned by the child's output queue.
fn opening(domain: &Domain, fact: &run::facts::Fact) -> Option<crate::Opening> {
    let conversation = match fact.kind {
        run::facts::FactKind::Opened { conversation, .. } => conversation,
        run::facts::FactKind::MessageRead { .. }
        | run::facts::FactKind::MessageFence { .. }
        | run::facts::FactKind::MessageUnread { .. }
        | run::facts::FactKind::MessageReceived { .. }
        | run::facts::FactKind::MessageRefused { .. }
        | run::facts::FactKind::Admitted { .. }
        | run::facts::FactKind::Prepared { .. }
        | run::facts::FactKind::Budget { .. }
        | run::facts::FactKind::Ended { .. }
        | run::facts::FactKind::Called { .. }
        | run::facts::FactKind::Returned { .. }
        | run::facts::FactKind::CheckStarted { .. }
        | run::facts::FactKind::CheckFinished { .. }
        | run::facts::FactKind::Delivered { .. }
        | run::facts::FactKind::Answered { .. } => return None,
    };
    for request in &domain.run_out {
        match request {
            run::Request::Open { conversation: opened, opening } => {
                if *opened == conversation {
                    return Some(crate::Opening {
                        activation: opening.activation,
                        ordinal: opening.ordinal,
                        llm: opening.llm.clone(),
                    });
                }
            }
            run::Request::CompletionReady { .. }
            | run::Request::MessageRefused { .. }
            | run::Request::Waiting { .. }
            | run::Request::Turn { .. }
            | run::Request::HostCall { .. }
            | run::Request::WithdrawHost { .. }
            | run::Request::Admitted { .. }
            | run::Request::Answer { .. }
            | run::Request::Say { .. }
            | run::Request::CancelJobs { .. }
            | run::Request::Yield { .. }
            | run::Request::Close { .. }
            | run::Request::Read { .. }
            | run::Request::Probe { .. }
            | run::Request::Check { .. }
            | run::Request::Abort { .. }
            | run::Request::Checking { .. }
            | run::Request::ChecksEnded { .. }
            | run::Request::Deliver { .. }
            | run::Request::Return { .. } => {}
        }
    }
    unreachable!("each opened fact accompanies its actual Open before hand-off")
}
