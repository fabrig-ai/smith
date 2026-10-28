//! Run admission, main and child lifetimes, shared spend and terminal rights
//! (domain/run.md, sections 3, 6–10). State retains the charter, prepared
//! guides/checks, run identity, never-reused child ordinals, conversation bindings,
//! call ownership, the wind-down's budget or host cause, and one pending ending.
//! Entrances below act only through bounded queues, slabs and injected time.
//! The run knows no host policy, forge, receipt encoding, authentication or
//! provider syntax. Generic final forms are judged before effects; delivery is
//! one exclusive checked snapshot. Before submission checks may abort; after
//! submission the bounded host terminal remains owed. Final Change
//! landing wins shutdown; a mid-run delivery settles before the pending
//! ending answers. Sessions alone price own usage; the run reserves each
//! completion's maximum across all conversations before the root sends it and
//! returns unused credit on its terminal. Global accounting sums monotonic own
//! deltas, while inclusive subtree totals transfer only as child bills. A charge that cannot fit ends the
//! session before it is added (domain/run.md, sections 9 and 10).
//!
//! The run and its conversations move through these retained phases. Each row
//! names the event that changes the phase; all issued operations still owe their
//! typed terminal while the run winds down (domain/run.md, sections 5.2, 6,
//! 8.2 and 10).
//!
//! ```text
//! run phase    event                              next phase   outward action
//! (none)       admitted Start                     Preparing   Admitted, read/probe
//! Preparing    prepared with work                Working     Open main
//! Preparing    prepared without work             Awaiting    Waiting without fence
//! Awaiting     first message                      Working     Open main
//! Awaiting     cancel, idle or wall deadline      Closed      Answer without session
//! Preparing    cancel or deadline                Stopping    settle read/probe
//! Stopping     read/probe terminal                Winding     close main
//! Working      main wait, empty inbox            Waiting     Waiting
//! Working      main wait, zero wait              Winding     Waiting, Withheld, close main
//! Working      Open text end, empty inbox         Listening   Waiting without a call
//! Listening    labelled message                  Working     Say user message
//! Listening    idle or ordinary wall deadline     Winding     close main, park
//! Waiting      labelled message                  Working     Waited; yield then Say
//! Waiting      idle alarm                         Winding     Withheld, close main
//! Working      finish or other ending             Winding     close main
//! Working      ordinary turns/time spent           Working     close children one per resume
//! Working      last child terminal                 Working     final permit, note once, mode tool choice
//! Working      final allowance spent               Winding     fail budget after current turn
//! Waiting      ordinary time spent                 Winding     Withheld, close main, park
//! Awaiting     ordinary time spent                 Closed      park without session
//! Winding      main and pending calls settled     Closed      Answer
//! ```
//!
//! ```text
//! conversation  event                         next       outward action
//! Pending       run prepared                  Opening    Open
//! Opening       Started                       Running    -
//! Opening       close before Started          Unwanted   await Started or Ended
//! Unwanted      Started                       Closing    Close
//! Running       close                         Closing    Close
//! Opening, Unwanted, Running, Closing
//!               Ended, after its calls return Closed     release bill; maybe Answer
//! ```
//!
//! The two calls with independent lower terminals keep their own phases
//! (domain/run.md, sections 5.2 and 8.2):
//!
//! ```text
//! operation  phase       event                          next        outward action
//! relay      Sending     timeout or withdrawal          Withdrawing WithdrawHost
//! relay      Sending     Busy or Unanswered terminal    Backoff/Closed retry if permitted
//! relay      Withdrawing HostReturned                   Backoff/Closed retry if permitted
//! relay      Backoff     retry alarm                    Sending     HostCall
//! relay      any live    Answered or final terminal     Closed      Return
//! delivery   Checking    check passed, more remain      Checking    Check
//! delivery   Checking    all checks passed             Delivering  Deliver
//! delivery   Checking    required check failed         Closed      Return feedback
//! delivery   Checking    withdrawal                    Aborting    Abort
//! delivery   Aborting    check/abort terminal           Closed      Return stopped
//! delivery   Delivering  Delivered terminal             Closed      Return result
//! ```
//!
//! A submitted delivery remains in Delivering through caller expiry, withdrawal
//! and shutdown; its typed terminal is still owed. The run answers only after
//! the operations it started have settled.

use alloc::boxed::Box;

mod jobs;
use core::mem;
pub(crate) use jobs::cancelled as jobs_cancelled;

use skein_lib::bytes::copy_of;
use skein_lib::{Deadlines, Duration, Env, Id, Queue, ReplyTo, Slab, Time, Token};

use crate::agent::{self, Child, Means};
use crate::boundary::{
    Answer, Ask, AskRefusal, End, Failure, Fault, Invalid, Opening, Policy, Ran, Read, Refusal, Request, Returned,
    Stop, Window,
};
use crate::budget::{Exhausted, Spend};
use crate::call::{self, Call, Calls, Withdrawal, Work};
use crate::charter::{self, Charter, Families, Mode, count};
use crate::delivery::{CallName, Delivery};
use crate::domain::Domain;
use crate::facts::{Asked, FactKind, Facts};
use crate::host::{Relay as HostRelay, Stage as HostStage};
use crate::inbox::{self, Entrance, Inbox};
use crate::land::{self, Settled};
use crate::limits::Limits;
use crate::outcome::{self, Change, Declared};
use crate::prepare::{self, Found, Step};
use crate::wind_down::Cause;
use crate::workspace;
use crate::{CompletionPermit, HostEffect, HostInput, HostProblem, HostReply, RelayName, Share, Workspace};
use crate::{opening, prompt};

#[derive(Debug)]
pub(crate) struct Run {
    /// The budget phase stays known through an accepted terminal.
    wind_down: Option<crate::wind_down::WindDown>,
    /// Exact retained children; closures are emitted one per resume.
    children: skein_lib::Set<Id<Conversation>>,
    identity: crate::Identity,
    /// Next never-reused child ordinal; main is zero.
    next_ordinal: u32,
    /// Injected activation admission time, the basis of budget wall usage.
    started_at: Time,
    pub(crate) lease: crate::lease::Lease,
    pub(crate) charter: Charter,
    pub(crate) workspace: Option<Workspace>,
    /// What it found in its checkout as it prepared.
    pub(crate) found: Found,
    /// Each directory's guide bound after the charter's host bytes are measured.
    guide_bytes: u32,
    /// The host's name for it.
    pub(crate) host_name: Token,
    /// Host-supplied number that distinguishes this activation's call names.
    pub(crate) activation: u64,
    /// Per-activation acknowledgement credit, reserved at the largest turn size.
    window: Window,
    /// Highest durably acknowledged activation turn.
    acknowledged: u32,
    /// What its conversations have spent.
    spent: Spend,
    /// Maximum charges of completions sent but not yet settled, across all conversations.
    reserved: u64,
    /// Sent completions whose usage has not yet settled.
    reserved_turns: u32,
    /// Nudges given.
    nudges: u32,
    /// Outcomes `finish` refused: rejected, or not landed.
    rejected: u32,
    /// Its conversations, main included, opened and not ended.
    conversations: u32,
    /// When its budget's time runs out.
    deadline: Time,
    state: State,
    /// Opaque restored history binding consumed when main opens.
    transcript: Option<Token>,
    resumed: bool,
    /// Admitted messages and the outstanding offered/read fence.
    inbox: Inbox,
    /// Activation-local number of main turns told to the host.
    turns: u32,
    /// Last historical session turn sequence, including a restored prefix.
    sequence: Option<u32>,
}

#[derive(Debug)]
enum State {
    /// Looking in its checkout: the look `step` is in flight. Its main
    /// conversation has its slot, and is not opened yet.
    Preparing { reply_to: ReplyTo, main: Id<Conversation>, step: Step },
    /// Prepared without work; main remains unopened while wall and idle time run.
    Awaiting { reply_to: ReplyTo, main: Id<Conversation>, until: Time },
    /// It fails with `failure` once the look in flight has ended.
    Stopping { reply_to: ReplyTo, main: Id<Conversation>, failure: Failure },
    /// Its main conversation is at work.
    Working { reply_to: ReplyTo, main: Id<Conversation> },
    /// Main wait remains unanswered; wall and idle timers remain distinct.
    Waiting { reply_to: ReplyTo, main: Id<Conversation>, call: Token, until: Time },
    /// Open main replied with text; idle listening retains no tool call.
    Listening { reply_to: ReplyTo, main: Id<Conversation>, until: Time },
    /// It ends with `ending` once its main conversation has ended.
    Winding { reply_to: ReplyTo, ending: Ending },
    /// Terminal: holds nothing.
    Closed,
}

/// How a winding run ends.
#[derive(Debug)]
enum Ending {
    Parked,
    Accepted(Declared),
    Failed(Failure),
}

/// A conversation a run opened: main, or a sub-agent.
#[derive(Debug)]
pub(crate) struct Conversation {
    pub(crate) run: Id<Run>,
    kind: crate::facts::Kind,
    opened_at: Time,
    /// The sub-agent call it serves, if it is a sub-agent.
    asker: Option<Id<Call>>,
    /// Its families of tools.
    families: Families,
    /// What it has spent, by its `Used` so far.
    spent: Spend,
    subtree_spent: u64,
    /// The one provider completion this conversation has reserved with the run.
    reservation: Option<(Token, Most)>,
    /// A budget close keeps its cause through the actual session terminal.
    budget_close: Option<Exhausted>,
    /// Its calls to the run in flight.
    calls: u32,
    phase: Phase,
}

#[derive(Debug)]
enum Phase {
    /// Not opened yet: its run is preparing or awaiting its first message.
    Pending,
    /// Opened, and not started yet.
    Opening,
    /// Opened, not started yet, and no longer wanted: closed once it starts.
    Unwanted,
    /// Started, and addressed as `peer`.
    Running { peer: Token },
    /// Closed by its run, and not ended yet.
    Closing,
    /// Terminal: holds nothing.
    Closed,
}

/// The timers of runs and their calls, named by what they are for.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Alarm {
    /// The budget's time of the run `run` runs out.
    Deadline { run: Id<Run> },
    /// Idle expiry of a waiting main.
    Park { run: Id<Run> },
    /// The deadline of the call `call` passes.
    Call { call: Id<Call> },
    /// Per-relay timeout or retry backoff, independent of caller expiry.
    Host { call: Id<Call> },
}

// Entry points, one per event or alarm: look the conversation or run up, take
// its state out, run the cell's handler, follow the run's new state.

/// Owned Start fields moved directly from the boundary event into admission.
/// This transient bundle retains no additional copy or separate allocation.
#[derive(Debug)]
pub(crate) struct Start {
    /// Affine right to the run's one final answer.
    pub(crate) reply_to: ReplyTo,

    /// Host-selected logical run identity.
    pub(crate) host_run: Token,
    /// Host-supplied positive activation number.
    pub(crate) activation: u64,
    pub(crate) identity: Option<crate::Identity>,
    pub(crate) window: Window,
    pub(crate) messages: Box<[crate::Message]>,

    /// Immutable requested contract and budget.
    pub(crate) charter: Charter,

    /// Optional host-owned mount metadata.
    pub(crate) workspace: Option<Workspace>,

    /// Optional opaque history handle.
    pub(crate) transcript: Option<Token>,
    /// Concrete history selected by the parent, independent of the opaque binding.
    pub(crate) resumed: bool,
}

/// A refused entrance has no conversation diagnostic or read fence.
fn refuse_start(reply_to: ReplyTo, refusal: Refusal, out: &mut Queue<Request>) {
    out.push(Request::Answer { to: reply_to, answer: Answer::Refused(refusal), read: None, detail: Box::default() });
}

pub(crate) fn start(domain: &mut Domain, env: &Env<Limits>, start: Start, out: &mut Queue<Request>) {
    let Start { reply_to, host_run, activation, identity, window, messages, charter, workspace, transcript, resumed } =
        start;
    let Domain { rng, runs, conversations, calls: _, alarms, facts, ready: budget_ready } = domain;
    // A charter that can never fit is invalid, room or not: busy invites a
    // retry.
    if activation == 0 {
        refuse_start(reply_to, Refusal::Invalid(Invalid::Activation), out);
        return;
    }
    if window.turns == 0 || window.largest_turn == 0 || window.bytes < window.largest_turn {
        refuse_start(reply_to, Refusal::Invalid(Invalid::Window), out);
        return;
    }
    let guide_bytes = match charter::check(&charter, workspace.as_ref(), &env.limits) {
        Ok(guide_bytes) => guide_bytes,
        Err(invalid) => {
            refuse_start(reply_to, Refusal::Invalid(invalid), out);
            return;
        }
    };
    if !inbox::messages_valid(&messages, &env.limits) {
        refuse_start(reply_to, Refusal::Invalid(Invalid::Messages), out);
        return;
    }
    if runs.is_full() || conversations.is_full() {
        refuse_start(reply_to, Refusal::Busy, out);
        return;
    }
    let identity = match identity {
        Some(identity) => identity,
        None => crate::Identity::draw(rng),
    };
    let deadline = env.now.saturating_add(charter.budget.time);
    let found = Found::with_capacity(count(workspace::directories(workspace.as_ref()).len()));
    // A run is stored before its main conversation, which names it, and starts
    // once main has a name too: main's slot is the run's from its admission.
    let run = Run {
        wind_down: None,
        children: skein_lib::Set::with_capacity(env.limits.run_conversations.saturating_sub(1)),
        identity,
        next_ordinal: 1,
        started_at: env.now,
        lease: crate::lease::Lease::Free,
        charter,
        workspace,
        found,
        guide_bytes,
        host_name: host_run,
        activation,
        window,
        acknowledged: 0,
        spent: Spend::ZERO,
        reserved: 0,
        reserved_turns: 0,
        nudges: 0,
        rejected: 0,
        conversations: 1,
        deadline,
        state: State::Closed,
        transcript,
        resumed,
        inbox: Inbox::new(&env.limits),
        turns: 0,
        sequence: None,
    };
    let id = runs.insert(run).expect("checked for room above");
    facts.about(id.token());
    let admitted = runs.get(id).expect("inserted above");
    let families = workspace::families(admitted.workspace.as_ref(), Families::of(&admitted.charter.grants));
    let conversation = Conversation {
        run: id,
        kind: crate::facts::Kind::Main,
        opened_at: env.now,
        asker: None,
        families,
        spent: Spend::ZERO,
        subtree_spent: 0,
        reservation: None,
        budget_close: None,
        calls: 0,
        phase: Phase::Pending,
    };
    let main = conversations.insert(conversation).expect("checked for room above");
    out.push(Request::Admitted { host_run, run: id.token() });
    let run = runs.get_mut(id).expect("inserted above");
    let finish_requested = run.inbox.carry(facts, id.token(), messages);
    if finish_requested {
        match run.charter.mode {
            Mode::Open => {
                if begin_wind_down(run, budget_ready, id, Cause::Host) {
                    facts.push(FactKind::Budget { run: id.token(), standing: standing(run, env.now), call: None });
                }
            }
            Mode::Finite => {}
        }
    }
    run.state = match prepare::next(run.workspace.as_ref(), None) {
        Some(step) => look(run, id, reply_to, main, step, env, out),
        None => ready(run, conversations, reply_to, main, env, facts, out),
    };
    follow(runs, alarms, facts, budget_ready, id);
}

/// Admit a named, labelled host message or emit its typed refusal.
/// Input precedes idle alarms; once the answer is decided every message ends refused.
pub(crate) fn message(
    domain: &mut Domain,
    env: &Env<Limits>,
    token: Token,
    message: crate::Message,
    out: &mut Queue<Request>,
) {
    let crate::Message { name, label, text, finish_requested } = message;
    let id = Id::<Run>::from_token(token);
    let Some(run) = domain.runs.get(id) else {
        return;
    };
    let entrance = match run.state {
        State::Stopping { .. } | State::Winding { .. } | State::Closed => Entrance::Ending,
        State::Working { .. } if run.wind_down.is_some() || run.charter.budget.exhausted(run.spent).is_some() => {
            Entrance::Ending
        }
        State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Working { .. }
        | State::Listening { .. }
        | State::Waiting { .. } => Entrance::Open,
    };
    let host_run = run.host_name;
    let bytes = inbox::rendered_bytes(&label, &text).unwrap_or(u64::MAX);
    let run = domain.runs.get_mut(id).expect("checked live run above");
    domain.facts.about(token);
    match run.inbox.admit(name, label, text, entrance, &env.limits) {
        Ok(bytes) => domain.facts.push(FactKind::MessageReceived { run: token, name, bytes }),
        Err(reason) => {
            domain.facts.push(FactKind::MessageRefused { run: token, name, bytes, reason });
            out.push(Request::MessageRefused { host_run, name, reason });
            return;
        }
    }
    if finish_requested {
        match run.charter.mode {
            Mode::Open => {
                if begin_wind_down(run, &mut domain.ready, id, Cause::Host) {
                    domain.facts.push(FactKind::Budget { run: token, standing: standing(run, env.now), call: None });
                }
            }
            Mode::Finite => {}
        }
    }
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Awaiting { reply_to, main, until: _ } => open(run, &mut domain.conversations, reply_to, main, env, out),
        State::Waiting { reply_to, main, call, until: _ } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Waited { text: prompt::waited() } });
            State::Working { reply_to, main }
        }
        State::Listening { reply_to, main, until: _ } => {
            let peer = match domain.conversations.get(main).expect("listening main lives").phase {
                Phase::Running { peer } => peer,
                Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closing | Phase::Closed => {
                    unreachable!("listening retains a yielded main")
                }
            };
            continue_messages(run, reply_to, main, peer, &env.limits, out)
        }
        state @ (State::Preparing { .. } | State::Working { .. }) => state,
        State::Stopping { .. } | State::Winding { .. } | State::Closed => {
            unreachable!("checked active run")
        }
    };
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, id);
}

fn continue_messages(
    run: &mut Run,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    peer: Token,
    limits: &Limits,
    out: &mut Queue<Request>,
) -> State {
    let text = run.inbox.offer(limits).expect("an admitted message fits an offer");
    say(reply_to, main, peer, text, out)
}

/// Root calls this only for main; child bodies are discarded at its routing seam.
/// Turns may still arrive during shutdown, before the terminal.
pub(crate) fn turn(
    domain: &mut Domain,
    conversation: Token,
    record: Token,
    sequence: u32,
    opens_window: bool,
    out: &mut Queue<Request>,
) {
    let conversation = domain.conversations.get(Id::from_token(conversation)).expect("turn precedes session end");
    assert!(conversation.asker.is_none(), "only main enters the host transcript");
    let run = domain.runs.get_mut(conversation.run).expect("run outlives its actual turns");
    match run.sequence {
        Some(previous) => assert!(previous.checked_add(1) == Some(sequence), "exact historical turn prefix"),
        None => assert!(sequence > 0, "historical sequence is one-based"),
    }
    run.sequence = Some(sequence);
    run.turns = run.turns.checked_add(1).expect("activation count is bounded by historical sequence");
    domain.facts.about(conversation.run.token());
    run.inbox.told(conversation.run.token(), run.turns, &mut domain.facts);
    out.push(Request::Turn {
        host_run: run.host_name,
        record,
        number: run.turns,
        position: sequence,
        opens_window,
        read: run.inbox.fence(),
        spent: run.spent,
    });
}

/// Release every outstanding turn through the host's durable prefix.
pub(crate) fn acknowledge(domain: &mut Domain, run: Token, turn: u32) {
    let Some(run) = domain.runs.get_mut(Id::from_token(run)) else {
        return;
    };
    if turn > run.acknowledged && turn <= run.turns {
        run.acknowledged = turn;
    }
}

pub(crate) fn park(domain: &mut Domain, id: Id<Run>, out: &mut Queue<Request>) {
    let run = domain.runs.get_mut(id).expect("idle alarm is cancelled before retirement");
    domain.facts.about(id.token());
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Awaiting { reply_to, main, until: _ } => unopened(
            run,
            &mut domain.conversations,
            reply_to,
            main,
            finished(Ending::Parked, run.spent, run.turns),
            out,
        ),
        State::Listening { reply_to, main, until: _ } => {
            assert!(!run.inbox.has_queued(), "an admitted wake cancels idle expiry");
            wind_down(&mut domain.conversations, reply_to, main, Ending::Parked, out)
        }
        State::Waiting { reply_to, main, call, until: _ } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Withheld });
            assert!(!run.inbox.has_queued(), "an admitted wake cancels idle expiry");
            wind_down(&mut domain.conversations, reply_to, main, Ending::Parked, out)
        }
        State::Preparing { .. }
        | State::Stopping { .. }
        | State::Working { .. }
        | State::Winding { .. }
        | State::Closed => {
            unreachable!("idle alarm belongs only to a retained wait or unopened awaiting main")
        }
    };
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, id);
}

pub(crate) fn read(domain: &mut Domain, env: &Env<Limits>, owner: Token, read: Read, out: &mut Queue<Request>) {
    let Domain { rng: _, runs, conversations, calls: _, alarms, facts, ready } = domain;
    let id = Id::<Run>::from_token(owner);
    facts.about(owner);
    let run = runs.get_mut(id).expect("a run lives until its look in flight has ended");
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Preparing { reply_to, main, step } => {
            prepare::guide(&mut run.found, step, read, run.guide_bytes);
            prepared(run, conversations, id, reply_to, main, step, env, facts, out)
        }
        State::Stopping { reply_to, main, failure } => stop(run, conversations, reply_to, main, failure, out),
        State::Awaiting { .. }
        | State::Working { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => {
            unreachable!("a run reads only while it prepares")
        }
    };
    follow(runs, alarms, facts, ready, id);
}

pub(crate) fn probed(domain: &mut Domain, env: &Env<Limits>, owner: Token, executable: bool, out: &mut Queue<Request>) {
    let Domain { rng: _, runs, conversations, calls: _, alarms, facts, ready } = domain;
    let id = Id::<Run>::from_token(owner);
    facts.about(owner);
    let run = runs.get_mut(id).expect("a run lives until its look in flight has ended");
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Preparing { reply_to, main, step } => {
            prepare::checks(&mut run.found, step, executable);
            prepared(run, conversations, id, reply_to, main, step, env, facts, out)
        }
        State::Stopping { reply_to, main, failure } => stop(run, conversations, reply_to, main, failure, out),
        State::Awaiting { .. }
        | State::Working { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => {
            unreachable!("a run probes only while it prepares")
        }
    };
    follow(runs, alarms, facts, ready, id);
}

pub(crate) fn cancel(domain: &mut Domain, run: Token, out: &mut Queue<Request>) {
    let Domain { rng: _, runs, conversations, calls: _, alarms, facts, ready } = domain;
    let id = Id::<Run>::from_token(run);
    // A cancel travels down, so it may name a run that has answered and gone.
    let Some(run) = runs.get_mut(id) else {
        return;
    };
    facts.about(id.token());
    let cancelled = Ending::Failed(Failure::Cancelled);
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Awaiting { reply_to, main, until: _ } => {
            stop(run, conversations, reply_to, main, Failure::Cancelled, out)
        }
        State::Preparing { reply_to, main, step: _ } => State::Stopping { reply_to, main, failure: Failure::Cancelled },
        State::Waiting { reply_to, main, call, until: _ } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Cancelled });
            wind_down(conversations, reply_to, main, cancelled, out)
        }
        State::Working { reply_to, main } | State::Listening { reply_to, main, until: _ } => {
            wind_down(conversations, reply_to, main, cancelled, out)
        }
        // How the run ends is decided already.
        State::Stopping { reply_to, main, failure } => State::Stopping { reply_to, main, failure },
        State::Winding { reply_to, ending } => State::Winding { reply_to, ending },
        // It answered in this iteration, and is retired already.
        State::Closed => return,
    };
    follow(runs, alarms, facts, ready, id);
}

pub(crate) fn started(domain: &mut Domain, conversation: Token, peer: Token, out: &mut Queue<Request>) {
    let id = Id::<Conversation>::from_token(conversation);
    let conversation = domain.conversations.get_mut(id).expect("a conversation lives until it has ended");
    domain.facts.about(conversation.run.token());
    let phase = mem::replace(&mut conversation.phase, Phase::Closed);
    conversation.phase = match phase {
        Phase::Opening => Phase::Running { peer },
        Phase::Unwanted => closing(peer, out),
        Phase::Pending | Phase::Running { .. } | Phase::Closing | Phase::Closed => {
            unreachable!("a conversation starts once it is opened, before anything else")
        }
    };
}

pub(crate) fn yielded(
    domain: &mut Domain,
    env: &Env<Limits>,
    conversation: Token,
    stop: Stop,
    text: &[u8],
    out: &mut Queue<Request>,
) {
    let Domain { rng: _, runs, conversations, calls, alarms, facts, ready } = domain;
    let id = Id::<Conversation>::from_token(conversation);
    let conversation = conversations.get(id).expect("a conversation lives until it has ended");
    facts.about(conversation.run.token());
    let peer = match &conversation.phase {
        Phase::Running { peer } => *peer,
        // The yield crossed the run's close: there is nothing left to decide.
        Phase::Closing => return,
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closed => {
            unreachable!("a conversation yields only between starting and ending")
        }
    };
    // A sub-agent that yields is done: its last message is its answer.
    if let Some(asker) = conversation.asker {
        let call = calls.get_mut(asker).expect("a sub-agent's call lives until it has ended");
        let close_child = match &mut call.work {
            Work::Child(child) => agent::yielded(child, text, stop, &env.limits),
            Work::Landing(_) | Work::Host(_) => unreachable!("a sub-agent serves a sub-agent's call"),
        };
        if let Some(child) = close_child {
            close(conversations, child, out);
        }
        return;
    }
    let run_id = conversation.run;
    let run = runs.get_mut(run_id).expect("a run lives until its conversations have ended");
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Working { reply_to, main } => {
            assert!(main == id, "a conversation with no asker is main");
            assert!(conversation.calls == 0, "a main yield follows every actual call terminal");
            let replied = match run.charter.mode {
                Mode::Finite => false,
                Mode::Open => match stop {
                    Stop::EndTurn => !text.is_empty(),
                    Stop::Budget | Stop::MaxTokens | Stop::Refusal | Stop::NoCalls => false,
                },
            };
            let final_reply = match &run.wind_down {
                Some(crate::wind_down::WindDown::Finishing { noted: true, .. }) => true,
                Some(
                    crate::wind_down::WindDown::Finishing { noted: false, .. }
                    | crate::wind_down::WindDown::Settling { .. },
                )
                | None => false,
            };
            if replied && final_reply {
                out.push(Request::Waiting { host_run: run.host_name, read: run.inbox.fence() });
                wind_down(conversations, reply_to, main, Ending::Parked, out)
            } else if run.wind_down.is_some() {
                match final_remaining(run, env.now) {
                    Ok(()) if run.inbox.has_queued() => continue_messages(run, reply_to, main, peer, &env.limits, out),
                    Ok(()) => {
                        let note = match run.charter.mode {
                            Mode::Finite => prompt::finish_now(),
                            Mode::Open => prompt::last_reply(),
                        };
                        say(reply_to, main, peer, note, out)
                    }
                    Err(exhausted) => wind_down(
                        conversations,
                        reply_to,
                        main,
                        Ending::Failed(Failure::Budget { exhausted, used: None, bound: None }),
                        out,
                    ),
                }
            } else if run.inbox.has_queued() {
                continue_messages(run, reply_to, main, peer, &env.limits, out)
            } else if replied {
                out.push(Request::Waiting { host_run: run.host_name, read: run.inbox.fence() });
                if run.charter.waiting == Duration::ZERO {
                    wind_down(conversations, reply_to, main, Ending::Parked, out)
                } else {
                    State::Listening { reply_to, main, until: env.now.saturating_add(run.charter.waiting) }
                }
            } else {
                match nudge(run, stop, &env.limits) {
                    Ok(()) => say(
                        reply_to,
                        main,
                        peer,
                        prompt::nudge(run.charter.mode, stop, run.nudges, env.limits.nudges),
                        out,
                    ),
                    Err(failure) => wind_down(conversations, reply_to, main, Ending::Failed(failure), out),
                }
            }
        }

        State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => {
            unreachable!("main yields only while its run works: it is closed when the run winds down")
        }
    };
    follow(runs, alarms, facts, ready, run_id);
}

/// Session own prices are monotonic activation totals. Stale names are inert;
/// subtree updates do not add global spend.
#[must_use]
pub fn completion_overflow(domain: &Domain, conversation: Token, price: u64, usage: Spend) -> Option<Failure> {
    let conversation =
        domain.conversations.get(Id::from_token(conversation)).expect("provider owner has a live run conversation");
    let run = domain.runs.get(conversation.run).expect("conversation retains its run");
    if run.spent.units.checked_add(price).is_none() {
        return Some(Failure::Budget {
            exhausted: Exhausted::Overflow(crate::Overflow::Spend),
            used: None,
            bound: None,
        });
    }
    let raw_total = Spend { units: 0, ..run.spent };
    let raw_increment = Spend { units: 0, ..usage };
    if raw_total.accumulate(raw_increment).is_none() {
        return Some(Failure::Budget {
            exhausted: Exhausted::Overflow(crate::Overflow::Usage),
            used: None,
            bound: None,
        });
    }
    None
}

/// Session own prices are monotonic activation totals. Stale names are inert;
/// subtree updates do not add global spend.
pub(crate) fn priced(domain: &mut Domain, conversation: Token, own: u64, subtree: u64, out: &mut Queue<Request>) {
    let id = Id::<Conversation>::from_token(conversation);
    let Some(conversation) = domain.conversations.get_mut(id) else {
        return;
    };
    match conversation.phase {
        Phase::Running { .. } | Phase::Closing => {}
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closed => return,
    }
    let run_id = conversation.run;
    if own < conversation.spent.units || subtree < conversation.subtree_spent {
        return;
    }
    let delta = own.checked_sub(conversation.spent.units).expect("checked monotonic own total");
    let run = domain.runs.get_mut(run_id).expect("conversation retains run");
    let Some(total) = run.spent.accumulate(Spend { units: delta, ..Spend::ZERO }) else {
        let state = mem::replace(&mut run.state, State::Closed);
        run.state = hard_stop(
            state,
            &mut domain.conversations,
            Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Spend), used: None, bound: None },
            out,
        );
        follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
        return;
    };
    conversation.spent.units = own;
    conversation.subtree_spent = subtree;
    run.spent = total;
    if account_state(run, &mut domain.ready, run_id, domain.facts.now()) {
        domain.facts.about(run_id.token());
        domain.facts.push(FactKind::Budget {
            run: run_id.token(),
            standing: standing(run, domain.facts.now()),
            call: None,
        });
    }
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
}

pub(crate) fn used(domain: &mut Domain, conversation: Token, spend: Spend, out: &mut Queue<Request>) {
    let id = Id::<Conversation>::from_token(conversation);
    let Some(conversation) = domain.conversations.get_mut(id) else {
        return;
    };
    match conversation.phase {
        Phase::Running { .. } | Phase::Closing => {}
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closed => return,
    }
    let run_id = conversation.run;
    let raw = Spend { units: 0, ..spend };
    let run = domain.runs.get_mut(run_id).expect("conversation retains run");
    let added = conversation.spent.accumulate(raw).zip(run.spent.accumulate(raw));
    let Some((conversation_total, run_total)) = added else {
        let state = mem::replace(&mut run.state, State::Closed);
        run.state = hard_stop(
            state,
            &mut domain.conversations,
            Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Usage), used: None, bound: None },
            out,
        );
        follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
        return;
    };
    conversation.spent = conversation_total;
    run.spent = run_total;
    account_state(run, &mut domain.ready, run_id, domain.facts.now());
    domain.facts.about(run_id.token());
    domain.facts.push(FactKind::Budget {
        run: run_id.token(),
        standing: standing(run, domain.facts.now()),
        call: None,
    });
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
}

fn account_state(run: &mut Run, ready: &mut crate::wind_down::Ready, id: Id<Run>, now: Time) -> bool {
    match run.state {
        State::Working { .. } => {}
        State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => return false,
    }
    let ordinary = ordinary_left(run, now);
    let exhausted = if ordinary.turns == 0 {
        Some(Exhausted::Turns)
    } else if ordinary.spend == 0 {
        Some(Exhausted::Spend)
    } else if ordinary.time == Duration::ZERO {
        Some(Exhausted::Time)
    } else {
        None
    };
    match exhausted {
        Some(exhausted) => begin_wind_down(run, ready, id, Cause::Budget(exhausted)),
        None => false,
    }
}

fn ordinary_left(run: &Run, now: Time) -> crate::Budget {
    let reserved_spend =
        crate::budget::reserve_spend(run.charter.reserve, &run.charter.llm).expect("admission checked reserve spend");
    crate::budget::ordinary(
        &run.charter.budget,
        run.charter.reserve,
        reserved_spend,
        run.spent,
        run.deadline.saturating_since(now),
    )
}

fn begin_wind_down(run: &mut Run, ready: &mut crate::wind_down::Ready, id: Id<Run>, cause: Cause) -> bool {
    match run.wind_down {
        Some(_) => false,
        None => {
            run.wind_down = Some(crate::wind_down::WindDown::new(run.children.len(), cause));
            match run.charter.mode {
                Mode::Open => ready.keep(id),
                Mode::Finite => {
                    if !run.children.is_empty() {
                        ready.keep(id);
                    }
                }
            }
            true
        }
    }
}

fn hard_stop(
    state: State,
    conversations: &mut Slab<Conversation>,
    failure: Failure,
    out: &mut Queue<Request>,
) -> State {
    match state {
        State::Working { reply_to, main } | State::Listening { reply_to, main, until: _ } => {
            wind_down(conversations, reply_to, main, Ending::Failed(failure), out)
        }
        State::Waiting { reply_to, main, call, .. } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Cancelled });
            wind_down(conversations, reply_to, main, Ending::Failed(failure), out)
        }
        State::Winding { reply_to, ending } => {
            let ending = hard_ending(ending, failure);
            State::Winding { reply_to, ending }
        }
        State::Awaiting { .. } | State::Preparing { .. } | State::Stopping { .. } | State::Closed => {
            unreachable!("actual usage is reported only by live admitted conversations")
        }
    }
}

fn hard_ending(ending: Ending, failure: Failure) -> Ending {
    match failure {
        Failure::Budget { exhausted: Exhausted::Overflow(_), .. } => Ending::Failed(failure),
        Failure::Transcript(_)
        | Failure::Model(_)
        | Failure::Budget { exhausted: _, .. }
        | Failure::Policy(_)
        | Failure::Cancelled
        | Failure::Stale => match ending {
            ending @ Ending::Accepted(Declared::Change(_)) => ending,
            Ending::Accepted(_) | Ending::Parked | Ending::Failed(_) => Ending::Failed(failure),
        },
    }
}

/// The completion's spend bound, supplied by the root (protocol/programs.md 5.7).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Most {
    /// The direct backend's priced worst case, released at settlement.
    Bounded(u64),
    /// A program's unknown cost, charged in full at settlement.
    Unreserved,
}

impl Most {
    fn reserved(self) -> u64 {
        match self {
            Self::Bounded(amount) => amount,
            Self::Unreserved => 0,
        }
    }
}

/// Admit unsent work, starting the finishing phase before any provider effect.
pub(crate) fn completion_permit(domain: &mut Domain, conversation: Token, now: Time, most: Most) -> CompletionPermit {
    let id = Id::<Conversation>::from_token(conversation);
    let Some(conversation) = domain.conversations.get(id) else {
        return CompletionPermit::Closing;
    };
    match conversation.phase {
        Phase::Running { .. } => {}
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closing | Phase::Closed => {
            return CompletionPermit::Closing;
        }
    }
    let run_id = conversation.run;
    let child = conversation.asker.is_some();
    let run = domain.runs.get_mut(run_id).expect("conversation retains run");
    match run.state {
        State::Working { .. } => {}
        State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Closed => return CompletionPermit::Closing,
    }
    let ordinary = ordinary_left(run, now);
    let spend_left = ordinary.spend.saturating_sub(run.reserved);
    let spend_exhausted = match most {
        Most::Bounded(amount) => amount > spend_left,
        Most::Unreserved => spend_left == 0,
    };
    let changed = if ordinary.turns <= run.reserved_turns {
        begin_wind_down(run, &mut domain.ready, run_id, Cause::Budget(Exhausted::Turns))
    } else if spend_exhausted {
        begin_wind_down(run, &mut domain.ready, run_id, Cause::Budget(Exhausted::Spend))
    } else if ordinary.time == Duration::ZERO {
        begin_wind_down(run, &mut domain.ready, run_id, Cause::Budget(Exhausted::Time))
    } else {
        false
    };
    if changed {
        domain.facts.begin(now);
        domain.facts.about(run_id.token());
        domain.facts.push(FactKind::Budget { run: run_id.token(), standing: standing(run, now), call: None });
    }
    let permit = working_permit(run, child, now);
    if child && permit == CompletionPermit::Closing {
        let exhausted = run.wind_down.as_ref().expect("closing child is winding down").cause().exhausted();
        domain.conversations.get_mut(id).expect("live child").budget_close = exhausted;
    }
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
    permit
}

fn working_permit(run: &mut Run, child: bool, now: Time) -> CompletionPermit {
    if child && run.wind_down.is_some() {
        return CompletionPermit::Closing;
    }
    if now >= run.deadline {
        return CompletionPermit::Denied(Exhausted::Time);
    }
    if child && ordinary_left(run, now).spend <= run.reserved {
        return CompletionPermit::Denied(Exhausted::Spend);
    }
    if let Some(exhausted) = run.charter.budget.exhausted(run.spent) {
        return CompletionPermit::Denied(exhausted);
    }
    if !child && completion_held(run) {
        return CompletionPermit::Held;
    }
    match &mut run.wind_down {
        Some(crate::wind_down::WindDown::Settling { .. }) => CompletionPermit::Held,
        Some(crate::wind_down::WindDown::Finishing { used, noted, cause }) => {
            if *used >= run.charter.reserve.turns {
                let reason = if run.charter.reserve.turns == 0 && *cause == Cause::Budget(Exhausted::Spend) {
                    Exhausted::Spend
                } else {
                    Exhausted::Turns
                };
                return CompletionPermit::Denied(reason);
            }
            let note = if *noted {
                None
            } else {
                *noted = true;
                Some(prompt::wind_down_note(
                    run.charter.mode,
                    *cause,
                    charter::Reserve {
                        time: run.charter.reserve.time.min(run.deadline.saturating_since(now)),
                        ..run.charter.reserve
                    },
                ))
            };
            CompletionPermit::Final { note }
        }
        None => CompletionPermit::Allowed,
    }
}

fn final_remaining(run: &Run, now: Time) -> Result<(), Exhausted> {
    if let Some(exhausted) = run.charter.budget.exhausted(run.spent) {
        return Err(exhausted);
    }
    if now >= run.deadline {
        return Err(Exhausted::Time);
    }
    match &run.wind_down {
        Some(crate::wind_down::WindDown::Finishing { used, cause, .. }) if *used >= run.charter.reserve.turns => {
            Err(if run.charter.reserve.turns == 0 && *cause == Cause::Budget(Exhausted::Spend) {
                Exhausted::Spend
            } else {
                Exhausted::Turns
            })
        }
        Some(crate::wind_down::WindDown::Finishing { .. } | crate::wind_down::WindDown::Settling { .. }) | None => {
            Ok(())
        }
    }
}

fn completion_held(run: &Run) -> bool {
    let pending = run.turns.checked_sub(run.acknowledged).expect("acknowledged prefix never exceeds turns");
    let next_bytes = match u64::from(pending).checked_add(1) {
        Some(count) => count.checked_mul(run.window.largest_turn),
        None => None,
    };
    pending >= run.window.turns
        || match next_bytes {
            Some(bytes) => bytes > run.window.bytes,
            None => true,
        }
}

/// Reserve one completion against the run's shared unspent ceiling before the
/// parent publishes it. Every live conversation can hold at most one.
pub(crate) fn reserve(domain: &mut Domain, conversation: Token, owner: Token, most: Most) -> Result<(), Exhausted> {
    let id = Id::<Conversation>::from_token(conversation);
    let conversation = domain.conversations.get_mut(id).expect("a provider request retains its conversation");
    assert!(conversation.reservation.is_none(), "a conversation sends one completion at a time");
    let run = domain.runs.get_mut(conversation.run).expect("conversation retains run");
    let committed = run.spent.units.checked_add(run.reserved).ok_or(Exhausted::Overflow(crate::Overflow::Spend))?;
    let total = committed.checked_add(most.reserved()).ok_or(Exhausted::Overflow(crate::Overflow::Spend))?;
    let final_turn = match &run.wind_down {
        Some(crate::wind_down::WindDown::Finishing { used, .. }) if conversation.asker.is_none() => {
            if *used >= run.charter.reserve.turns {
                return Err(Exhausted::Turns);
            }
            true
        }
        Some(_) => return Err(Exhausted::Turns),
        None => false,
    };
    if !final_turn {
        let turn_ceiling = run.charter.budget.turns.checked_sub(run.charter.reserve.turns).expect("admitted reserve");
        let next_turn = match run.spent.turns.checked_add(run.reserved_turns) {
            Some(turns) => turns.checked_add(1),
            None => None,
        }
        .ok_or(Exhausted::Overflow(crate::Overflow::Usage))?;
        if next_turn > turn_ceiling {
            return Err(Exhausted::Turns);
        }
    }
    let spend_ceiling = if final_turn {
        run.charter.budget.spend
    } else {
        let reserved = crate::budget::reserve_spend(run.charter.reserve, &run.charter.llm).expect("admitted reserve");
        run.charter.budget.spend.checked_sub(reserved).expect("admitted reserve")
    };
    let exhausted = match most {
        Most::Bounded(_) => total > spend_ceiling,
        Most::Unreserved => committed >= spend_ceiling,
    };
    if exhausted {
        return Err(Exhausted::Spend);
    }
    if final_turn {
        match &mut run.wind_down {
            Some(crate::wind_down::WindDown::Finishing { used, .. }) => {
                *used = used.checked_add(1).expect("bounded final completions");
            }
            Some(crate::wind_down::WindDown::Settling { .. }) | None => unreachable!("final turn admitted"),
        }
    }
    run.reserved = run.reserved.checked_add(most.reserved()).expect("checked with total above");
    run.reserved_turns = run.reserved_turns.checked_add(1).expect("one bounded reservation per conversation");
    conversation.reservation = Some((owner, most));
    Ok(())
}

/// Return unused credit after the provider's terminal. An overbound charge is
/// rejected by the caller before it can enter the session's spend record.
pub(crate) fn settle_reservation(domain: &mut Domain, conversation: Token, owner: Token, charge: u64) -> bool {
    let id = Id::<Conversation>::from_token(conversation);
    let conversation = domain.conversations.get_mut(id).expect("a provider terminal retains its conversation");
    let (reserved_owner, most) = conversation.reservation.take().expect("every sent completion was reserved");
    assert!(reserved_owner == owner, "provider terminal settles its own reservation");
    let run = domain.runs.get_mut(conversation.run).expect("conversation retains run");
    run.reserved = run.reserved.checked_sub(most.reserved()).expect("reservation belongs to this run");
    run.reserved_turns = run.reserved_turns.checked_sub(1).expect("reservation belongs to this run");
    match most {
        Most::Bounded(amount) => charge <= amount,
        Most::Unreserved => true,
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "the cell receives concrete origin independently of live callback identity"
)]
pub(crate) fn delegated(
    domain: &mut Domain,
    env: &Env<Limits>,
    conversation: Token,
    call: Token,
    name: CallName,
    ask: Ask,
    deadline: Time,
    out: &mut Queue<Request>,
) {
    let Domain { rng: _, runs, conversations, calls: _, alarms: _, facts, ready: _ } = domain;
    let id = Id::<Conversation>::from_token(conversation);
    let conversation = conversations.get(id).expect("a conversation lives until it has ended");
    let closing = match &conversation.phase {
        Phase::Running { .. } => false,
        Phase::Closing => true,
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closed => {
            unreachable!("a conversation calls only between starting and ending")
        }
    };
    let run_id = conversation.run;
    facts.about(run_id.token());
    let asked = match &ask {
        Ask::Wait => Asked::Wait,
        Ask::Host { .. } => Asked::Host,
        Ask::Deliver { .. } => Asked::Deliver,
        Ask::Finish { .. } => Asked::Finish,
        Ask::SubAgent { .. } => Asked::SubAgent,
    };
    facts.push(FactKind::Called { run: run_id.token(), conversation: id.token(), call, ask: asked });
    // The call crossed its conversation's close: it would be withdrawn at once.
    if closing {
        out.push(Request::Return { spent: 0, call, result: Returned::Cancelled });
        return;
    }
    let run = runs.get_mut(run_id).expect("a run lives until its conversations have ended");
    let state = mem::replace(&mut run.state, State::Closed);
    let made = Asking { name, call, deadline };
    let next = match state {
        State::Winding { reply_to, ending } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Cancelled });
            State::Winding { reply_to, ending }
        }
        State::Working { reply_to, main } => {
            let serving = Serving { reply_to, main, conversation: id, made };
            serve(domain, env, run_id, serving, ask, out)
        }

        State::Waiting { reply_to, main, call, until } => {
            assert!(id != main, "a withheld wait blocks main's next call");
            let serving = Serving { reply_to, main, conversation: id, made };
            match serve(domain, env, run_id, serving, ask, out) {
                State::Working { reply_to, main } => State::Waiting { reply_to, main, call, until },
                State::Preparing { .. }
                | State::Awaiting { .. }
                | State::Stopping { .. }
                | State::Listening { .. }
                | State::Waiting { .. }
                | State::Winding { .. }
                | State::Closed => {
                    unreachable!("a child's ordinary call cannot finish or park main")
                }
            }
        }
        State::Listening { .. }
        | State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Closed => {
            unreachable!("a run's conversation calls only while the run works or winds down")
        }
    };
    domain.runs.get_mut(run_id).expect("the served call retains its run").state = next;
    follow(&mut domain.runs, &mut domain.alarms, &mut domain.facts, &mut domain.ready, run_id);
}

/// Current completion-local call context; scalar crossing keeps these calls
/// stable while preventing the next completion.
struct Serving {
    reply_to: ReplyTo,
    main: Id<Conversation>,
    conversation: Id<Conversation>,
    made: Asking,
}

fn serve(
    domain: &mut Domain,
    env: &Env<Limits>,
    run_id: Id<Run>,
    serving: Serving,
    ask: Ask,
    out: &mut Queue<Request>,
) -> State {
    let Domain { rng: _, runs, conversations, calls, alarms, facts: _, ready: _ } = domain;
    let run = runs.get_mut(run_id).expect("the served call retains its run");
    let Serving { reply_to, main, conversation, made } = serving;
    match ask {
        Ask::Wait => {
            let result = if main != conversation || !run.charter.grants.wait {
                Returned::Refused { refusal: AskRefusal::NotGranted }
            } else if run.charter.budget.exhausted(run.spent).is_some() {
                Returned::Refused { refusal: AskRefusal::Over }
            } else if run.inbox.has_queued() {
                Returned::Waited { text: prompt::waited() }
            } else {
                out.push(Request::Waiting { host_run: run.host_name, read: run.inbox.fence() });
                if run.charter.waiting == Duration::ZERO || run.wind_down.is_some() {
                    out.push(Request::Return { spent: 0, call: made.call, result: Returned::Withheld });
                    return wind_down(conversations, reply_to, main, Ending::Parked, out);
                }
                return State::Waiting {
                    reply_to,
                    main,
                    call: made.call,
                    until: env.now.saturating_add(run.charter.waiting),
                };
            };
            out.push(Request::Return { spent: 0, call: made.call, result });
        }
        Ask::Host { tool, effect, input } => {
            host_call(run, run_id, conversations, calls, alarms, conversation, made, tool, effect, input, env, out);
        }
        Ask::Deliver { change } => {
            assert!(main == conversation, "only main is offered delivery");
            deliver(run, run_id, conversations, calls, alarms, main, made, change, env, out);
        }
        Ask::Finish { outcome } => {
            assert!(main == conversation, "only main is offered finish");
            return finish(run, run_id, conversations, calls, alarms, reply_to, main, made, outcome, env, out);
        }
        Ask::SubAgent { brief, families, llm, share } => {
            let wanted = Wanted { brief, families, llm, share };
            sub_agent(run, run_id, conversations, calls, alarms, conversation, made, wanted, env, out);
        }
    }
    State::Working { reply_to, main }
}

pub(crate) fn withdraw(domain: &mut Domain, conversation: Token, call: Token, out: &mut Queue<Request>) {
    let conversation = Id::<Conversation>::from_token(conversation);
    if let Some(peer) = domain.conversations.get(conversation) {
        let id = peer.run;
        let run = domain.runs.get_mut(id).expect("conversation retains run");
        let waiting = match run.state {
            State::Waiting { call: held, .. } => held == call,
            State::Preparing { .. }
            | State::Awaiting { .. }
            | State::Stopping { .. }
            | State::Working { .. }
            | State::Listening { .. }
            | State::Winding { .. }
            | State::Closed => false,
        };
        if waiting {
            park(domain, id, out);
            return;
        }
    }
    // A withdraw of a call that has returned is stale.
    let Some(id) = domain.calls.find(conversation, call) else {
        return;
    };
    // The call is settling now: its deadline no longer matters.
    domain.alarms.cancel(Alarm::Call { call: id });
    stop_call(domain, id, Withdrawal::Withdrawn, out);
}

/// The deadline of the call `id` passed before it returned: stop what it is
/// doing.
pub(crate) fn expired(domain: &mut Domain, id: Id<Call>, out: &mut Queue<Request>) {
    stop_call(domain, id, Withdrawal::Expired, out);
}

pub(crate) fn checked(domain: &mut Domain, env: &Env<Limits>, owner: Token, ran: Ran, out: &mut Queue<Request>) {
    let id = Id::<Call>::from_token(owner);
    let call = domain.calls.get_mut(id).expect("a call lives until it returns");
    let run = domain.runs.get(call.run).expect("a run lives until its calls have returned");
    domain.facts.about(call.run.token());
    domain.facts.push(FactKind::CheckFinished { run: call.run.token(), exit: ran.exit });
    out.push(Request::ChecksEnded { host_run: run.host_name });
    let settled = match &mut call.work {
        Work::Landing(landing) => land::checked(landing, id, call.owner, run, may_finish(&run.state), ran, env, out),
        Work::Child(_) | Work::Host(_) => unreachable!("io and the host answer only a landing's requests"),
    };
    settle(domain, id, settled, out);
}

pub(crate) fn aborted(domain: &mut Domain, owner: Token, out: &mut Queue<Request>) {
    let id = Id::<Call>::from_token(owner);
    let call = domain.calls.get_mut(id).expect("a call lives until it returns");
    let run = domain.runs.get(call.run).expect("a run outlives its checks");
    domain.facts.about(call.run.token());
    out.push(Request::ChecksEnded { host_run: run.host_name });
    let exit = match &mut call.work {
        Work::Landing(landing) => land::aborted(landing, call.owner, out),
        Work::Child(_) | Work::Host(_) => unreachable!("io and the host answer only a landing's requests"),
    };
    domain.facts.push(FactKind::CheckFinished { run: call.run.token(), exit });
    settle(domain, id, Settled::Cancelled, out);
}

pub(crate) fn delivered(domain: &mut Domain, owner: Token, delivery: Delivery, out: &mut Queue<Request>) {
    let id = Id::<Call>::from_token(owner);
    let Some(call) = domain.calls.get(id) else {
        return;
    };
    if domain.calls.find(call.conversation, call.owner) != Some(id) {
        return;
    }
    let call = domain.calls.get_mut(id).expect("the named call is live");
    let run = domain.runs.get(call.run).expect("run waits for the terminal");
    domain.facts.about(call.run.token());
    domain.facts.push(FactKind::Delivered { run: call.run.token(), status: delivery.status() });
    let settled = match &mut call.work {
        Work::Landing(landing) => land::delivered(landing, call.owner, delivery, run, out),
        Work::Child(_) | Work::Host(_) => unreachable!("host answers a delivery call"),
    };
    settle(domain, id, settled, out);
}

fn child_ready(run: &Run, out: &mut Queue<Request>) {
    match run.state {
        State::Working { main, .. } => out.push(Request::CompletionReady { conversation: main.token() }),
        State::Preparing { .. }
        | State::Awaiting { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => {}
    }
}

fn terminal_failure(end: &End) -> Option<Failure> {
    match end {
        End::PriceOverflow => {
            Some(Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Spend), used: None, bound: None })
        }
        End::UsageOverflow => {
            Some(Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Usage), used: None, bound: None })
        }
        End::Receiving(_)
        | End::TranscriptRefused { .. }
        | End::Closed
        | End::Busy
        | End::Invalid
        | End::Fault(_)
        | End::Budget(_) => None,
    }
}

pub(crate) fn ended(
    domain: &mut Domain,
    conversation: Token,
    end: End,
    spend: Spend,
    detail: Box<[u8]>,
    out: &mut Queue<Request>,
) {
    let Domain { rng: _, runs, conversations, calls, alarms, facts, ready } = domain;
    let id = Id::<Conversation>::from_token(conversation);
    let Some(conversation) = conversations.get_mut(id) else {
        return;
    };
    match conversation.phase {
        Phase::Closed => return,
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Running { .. } | Phase::Closing => {}
    }
    assert!(conversation.calls == 0, "a conversation ends once its calls have returned");
    assert!(conversation.reservation.is_none(), "a conversation settles its provider before ending");
    let phase = mem::replace(&mut conversation.phase, Phase::Closed);
    match phase {
        Phase::Opening | Phase::Unwanted | Phase::Running { .. } | Phase::Closing => {}
        Phase::Pending | Phase::Closed => unreachable!("a conversation ends once, once opened"),
    }
    // Its turns were counted as they were used; whatever its end counts beyond
    // them is counted now.
    let unaccounted = spend.unreported(conversation.spent);
    let writing = conversation.families.tools.modify || conversation.families.tools.shell;
    let (run_id, asker) = (conversation.run, conversation.asker);
    let bill = conversation.subtree_spent;
    let budget_close = conversation.budget_close;
    let used = crate::facts::Used {
        turns: spend.turns.max(conversation.spent.turns),
        time: facts.now().saturating_since(conversation.opened_at),
        spend: conversation.spent.units,
    };
    facts.about(run_id.token());
    facts.push(FactKind::Ended { run: run_id.token(), conversation: id.token(), end: end.kind() });
    conversations.retire(id);
    let run = runs.get_mut(run_id).expect("a run lives until its conversations have ended");
    run.spent = run.spent.accumulate(unaccounted).expect("terminal usage was checked before it was reported");
    let changed = account_state(run, ready, run_id, facts.now());
    let snapshot = standing(run, facts.now());
    let failure = terminal_failure(&end);
    if let (Some(failure), Some(_)) = (failure.clone(), asker) {
        let state = mem::replace(&mut run.state, State::Closed);
        run.state = hard_stop(state, conversations, failure, out);
    }
    run.conversations = run.conversations.checked_sub(1).expect("a run counts its conversations");
    // A sub-agent's end is its call's return.
    if let Some(asker) = asker {
        assert!(run.children.remove(&id), "a child is retained until its terminal");
        let finishing = match &mut run.wind_down {
            Some(phase) => phase.child_ended(),
            None => false,
        };
        if writing {
            run.lease.child_returned(asker);
        }
        let left = crate::budget::left(&snapshot);
        let call = calls.get_mut(asker).expect("a sub-agent's call lives until it has ended");
        let result = match &mut call.work {
            Work::Child(child) => {
                let end = match budget_close {
                    Some(exhausted) if end == End::Closed => End::Budget(exhausted),
                    Some(_) | None => end,
                };
                agent::ended(child, end, used, left)
            }
            Work::Landing(_) | Work::Host(_) => unreachable!("a sub-agent serves a sub-agent's call"),
        };
        let told = tells_budget(&result);
        if told || unaccounted.turns > 0 || changed {
            let call = if told { Some(call.owner) } else { None };
            facts.push(FactKind::Budget { run: run_id.token(), standing: snapshot, call });
        }
        crate::facts::child_returned(facts, run_id.token(), call.owner, &result, used);
        out.push(Request::Return { spent: bill, call: call.owner, result });
        retire_call(calls, alarms, conversations, asker);
        if finishing {
            ready.keep(run_id);
            child_ready(run, out);
        }
        return;
    }
    if unaccounted.turns > 0 || changed {
        facts.push(FactKind::Budget { run: run_id.token(), standing: snapshot, call: None });
    }
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Working { reply_to, main }
        | State::Listening { reply_to, main, until: _ }
        | State::Waiting { reply_to, main, call: _, until: _ } => {
            assert!(main == id, "a conversation with no asker is main");
            let completed = budget_answer(ending(end, run.spent, run.turns), run, facts.now());
            answer(reply_to, completed, run.inbox.fence(), detail, out)
        }
        State::Winding { reply_to, ending } => {
            let ending = match failure {
                Some(failure) => Ending::Failed(failure),
                None => ending,
            };
            let completed = budget_answer(finished(ending, run.spent, run.turns), run, facts.now());
            answer(reply_to, completed, run.inbox.fence(), detail, out)
        }
        State::Awaiting { .. } | State::Preparing { .. } | State::Stopping { .. } | State::Closed => {
            unreachable!("a run's conversation ends only once the run has opened it")
        }
    };
    follow(runs, alarms, facts, ready, run_id);
}

pub(crate) fn deadline(domain: &mut Domain, id: Id<Run>, out: &mut Queue<Request>) {
    let Domain { rng: _, runs, conversations, calls: _, alarms, facts, ready } = domain;
    let run = runs.get_mut(id).expect("an alarm is cancelled before its run closes");
    facts.about(id.token());
    let failure = Failure::Budget {
        exhausted: run.charter.budget.exhausted(run.spent).unwrap_or(Exhausted::Time),
        used: None,
        bound: None,
    };
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match state {
        State::Awaiting { reply_to, main, until: _ } => {
            unopened(run, conversations, reply_to, main, finished(Ending::Parked, run.spent, run.turns), out)
        }
        State::Listening { reply_to, main, until: _ } => {
            assert!(!run.inbox.has_queued(), "an admitted wake cancels idle expiry");
            wind_down(conversations, reply_to, main, Ending::Parked, out)
        }
        State::Waiting { reply_to, main, call, until: _ } => {
            out.push(Request::Return { spent: 0, call, result: Returned::Withheld });
            wind_down(conversations, reply_to, main, Ending::Parked, out)
        }
        State::Preparing { reply_to, main, step: _ } => State::Stopping { reply_to, main, failure },
        State::Working { reply_to, main } => match run.wind_down {
            None if facts.now() < run.deadline => {
                begin_wind_down(run, ready, id, Cause::Budget(Exhausted::Time));
                facts.push(FactKind::Budget { run: id.token(), standing: standing(run, facts.now()), call: None });
                State::Working { reply_to, main }
            }
            None | Some(_) => wind_down(conversations, reply_to, main, Ending::Failed(failure), out),
        },
        // It was past its budget first.
        State::Stopping { .. } | State::Winding { .. } | State::Closed => {
            unreachable!("the deadline alarm runs only while a run prepares or works")
        }
    };
    follow(runs, alarms, facts, ready, id);
}

/// Child parentage copied from its owned call for the actual opening fact.
pub(crate) fn opened(conversations: &Slab<Conversation>, conversation: Token) -> crate::facts::Kind {
    conversations.get(Id::from_token(conversation)).expect("a conversation is told of as it opens").kind
}

fn tells_budget(result: &Returned) -> bool {
    match result {
        Returned::Answered { .. } | Returned::Unanswered { .. } => true,
        Returned::Waited { .. }
        | Returned::Withheld
        | Returned::Crossed { .. }
        | Returned::HostAnswered(_)
        | Returned::HostTooLarge { .. }
        | Returned::HostReportedTooLarge
        | Returned::HostUnknown
        | Returned::HostRejected(_)
        | Returned::Delivered(_)
        | Returned::Nothing
        | Returned::DeliveryRefused(_)
        | Returned::Accepted
        | Returned::Rejected { .. }
        | Returned::ChecksFailed { .. }
        | Returned::Stale
        | Returned::DeliveryFailed { .. }
        | Returned::Cancelled
        | Returned::TimedOut
        | Returned::Busy
        | Returned::Refused { .. } => false,
    }
}

fn standing(run: &Run, now: Time) -> crate::facts::Standing {
    let nanos = now.saturating_since(run.started_at).as_nanos();
    let millis = (nanos / 1_000_000)
        .checked_add(u64::from(!nanos.is_multiple_of(1_000_000)))
        .expect("a rounded millisecond count fits the nanosecond clock");
    crate::facts::Standing {
        stage: match run.wind_down {
            Some(_) => crate::Stage::WindDown,
            None => crate::Stage::Ordinary,
        },
        turns: run.spent.turns,
        turns_limit: run.charter.budget.turns,
        spend: run.spent.units,
        spend_limit: run.charter.budget.spend,
        time: Duration::from_millis(millis),
        time_limit: run.charter.budget.time,
        reserve: run.charter.reserve,
        reserve_spend: crate::budget::reserve_spend(run.charter.reserve, &run.charter.llm)
            .expect("admission checked reserve spend"),
    }
}

fn ordinary_deadline(run: &Run) -> Time {
    Time::from_nanos(run.deadline.as_nanos().saturating_sub(run.charter.reserve.time.as_nanos()))
}

/// What a run's state implies, applied after every transition: whether its
/// deadline alarm runs, and whether it is retired.
fn follow(
    runs: &mut Slab<Run>,
    alarms: &mut Deadlines<Alarm>,
    facts: &mut Facts,
    ready: &mut crate::wind_down::Ready,
    id: Id<Run>,
) {
    let run = runs.get_mut(id).expect("a run lives until it is retired");
    let (deadline, closed) = match &run.state {
        State::Awaiting { .. } | State::Working { .. } | State::Listening { .. } | State::Waiting { .. } => {
            let at = match run.wind_down {
                Some(_) => run.deadline,
                None => ordinary_deadline(run),
            };
            (Some(at), false)
        }
        State::Preparing { .. } => (Some(run.deadline), false),
        State::Stopping { .. } | State::Winding { .. } => (None, false),
        State::Closed => (None, true),
    };
    if closed {
        run.lease.assert_free();
        run.inbox.unread(id.token(), facts);
    }
    let alarm = Alarm::Deadline { run: id };
    if let Some(at) = deadline {
        alarms.arm(alarm, at).expect("the alarm table has room for an alarm per run and per call");
    } else {
        alarms.cancel(alarm);
    }
    let park = Alarm::Park { run: id };
    match run.state {
        State::Awaiting { until, .. } | State::Listening { until, .. } | State::Waiting { until, .. } => {
            alarms.arm(park, until).expect("one independent idle alarm per run");
        }
        State::Preparing { .. }
        | State::Stopping { .. }
        | State::Working { .. }
        | State::Winding { .. }
        | State::Closed => {
            alarms.cancel(park);
        }
    }
    if closed {
        ready.remove(id);
        runs.retire(id);
    }
}

/// Whether a working run may still finish, including its final completion
/// with main's turn in flight.
fn may_finish(state: &State) -> bool {
    match state {
        State::Working { .. } => true,
        State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => false,
    }
}

/// The landing of the call `id` has settled, as `settled` says: the run goes
/// on, or finishes.
fn settle(domain: &mut Domain, id: Id<Call>, settled: Settled, out: &mut Queue<Request>) {
    let Domain { rng: _, runs, conversations, calls, alarms, facts, ready } = domain;
    if settled == Settled::Going {
        return;
    }
    let run_id = retire_call(calls, alarms, conversations, id);
    let run = runs.get_mut(run_id).expect("a run lives until its calls have returned");
    run.lease.workspace_returned(id.token());
    let state = mem::replace(&mut run.state, State::Closed);
    run.state = match settled {
        Settled::MidDelivered => match state {
            State::Working { reply_to, main } => working_again(run, conversations, reply_to, main, facts.now(), out),

            State::Winding { reply_to, ending } => State::Winding { reply_to, ending },
            State::Awaiting { .. }
            | State::Preparing { .. }
            | State::Stopping { .. }
            | State::Listening { .. }
            | State::Waiting { .. }
            | State::Closed => {
                unreachable!("delivery starts after main")
            }
        },
        Settled::Finished(change) => match state {
            State::Working { reply_to, main } => {
                wind_down(conversations, reply_to, main, Ending::Accepted(Declared::Change(change)), out)
            }
            // The host landed the checked state, whatever the run was winding down
            // for: main is closing already.
            State::Winding { reply_to, ending: _ } => {
                State::Winding { reply_to, ending: Ending::Accepted(Declared::Change(change)) }
            }
            State::Awaiting { .. }
            | State::Preparing { .. }
            | State::Stopping { .. }
            | State::Listening { .. }
            | State::Waiting { .. }
            | State::Closed => {
                unreachable!("a run lands a change only once it has opened main, and before it answers")
            }
        },
        Settled::Refused => {
            run.rejected = run.rejected.saturating_add(1);
            match state {
                State::Working { reply_to, main } => {
                    working_again(run, conversations, reply_to, main, facts.now(), out)
                }
                state @ State::Winding { .. } => state,
                State::Awaiting { .. }
                | State::Preparing { .. }
                | State::Stopping { .. }
                | State::Listening { .. }
                | State::Waiting { .. }
                | State::Closed => {
                    unreachable!("a run lands a change only once it has opened main, and before it answers")
                }
            }
        }
        Settled::Stale => match state {
            State::Working { reply_to, main } => {
                wind_down(conversations, reply_to, main, Ending::Failed(Failure::Stale), out)
            }
            state @ State::Winding { .. } => state,
            State::Awaiting { .. }
            | State::Preparing { .. }
            | State::Stopping { .. }
            | State::Listening { .. }
            | State::Waiting { .. }
            | State::Closed => {
                unreachable!("a run lands a change only once it has opened main, and before it answers")
            }
        },
        Settled::Going | Settled::Cancelled => state,
    };
    follow(runs, alarms, facts, ready, run_id);
}

// Cell handlers: each takes the source state's data by value and returns the
// target state.

/// Asks for the look `step`, preparing.
fn look(
    run: &Run,
    id: Id<Run>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    step: Step,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    out.push(prepare::request(
        &run.charter,
        run.workspace.as_ref(),
        step,
        id.token(),
        env.now,
        &env.limits,
        run.guide_bytes,
    ));
    State::Preparing { reply_to, main, step }
}

/// Preparing, the look `step` ended: the next one, or open main.
#[expect(clippy::too_many_arguments, reason = "a cell handler takes the fields it touches")]
fn prepared(
    run: &mut Run,
    conversations: &mut Slab<Conversation>,
    id: Id<Run>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    step: Step,
    env: &Env<Limits>,
    facts: &mut Facts,
    out: &mut Queue<Request>,
) -> State {
    match prepare::next(run.workspace.as_ref(), Some(step)) {
        Some(next) => look(run, id, reply_to, main, next, env, out),
        None => ready(run, conversations, reply_to, main, env, facts, out),
    }
}

/// Preparation chooses one concrete cell: awaiting without a session, or main opening.
fn ready(
    run: &mut Run,
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    env: &Env<Limits>,
    facts: &mut Facts,
    out: &mut Queue<Request>,
) -> State {
    let id = conversations.get(main).expect("pending main retains its run").run;
    facts.push(FactKind::Prepared { run: id.token(), guides: run.found.guides.len(), checks: run.found.checks.len() });
    if prompt::has_brief(&run.charter) || run.inbox.has_queued() || !run.charter.grants.wait {
        return open(run, conversations, reply_to, main, env, out);
    }
    out.push(Request::Waiting { host_run: run.host_name, read: None });
    if run.charter.waiting == Duration::ZERO || run.wind_down.is_some() {
        return unopened(run, conversations, reply_to, main, finished(Ending::Parked, run.spent, run.turns), out);
    }
    State::Awaiting { reply_to, main, until: env.now.saturating_add(run.charter.waiting) }
}

/// Prepared: open main with what the run found, and the whole budget.
fn open(
    run: &mut Run,
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    let conversation = conversations.get_mut(main).expect("main lives while its run prepares");
    let phase = mem::replace(&mut conversation.phase, Phase::Closed);
    conversation.phase = match phase {
        Phase::Pending => Phase::Opening,
        Phase::Opening | Phase::Unwanted | Phase::Running { .. } | Phase::Closing | Phase::Closed => {
            unreachable!("main is opened once, when its run has prepared")
        }
    };
    let mut opening = opening::main(
        &run.charter,
        run.resumed,
        run.activation,
        run.identity,
        run.workspace.as_ref(),
        &run.found,
        run.spent,
        run.deadline.saturating_since(env.now),
        &env.limits,
    );
    let offered = run.inbox.offer(&env.limits);
    opening.prompt = prompt::opening(&run.charter, run.resumed, offered);
    opening.transcript = run.transcript.take();
    out.push(Request::Open { conversation: main.token(), opening });
    State::Working { reply_to, main }
}

/// Stopping, the look in flight ended: main was never opened, and the run
/// answers.
fn stop(
    run: &Run,
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    failure: Failure,
    out: &mut Queue<Request>,
) -> State {
    unopened(run, conversations, reply_to, main, Answer::Failed { failure, spent: run.spent, turns: run.turns }, out)
}

/// Retire unopened main and answer without inventing any session terminal.
fn unopened(
    run: &Run,
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    terminal: Answer,
    out: &mut Queue<Request>,
) -> State {
    let conversation = conversations.get_mut(main).expect("main lives while its run prepares");
    let phase = mem::replace(&mut conversation.phase, Phase::Closed);
    match phase {
        Phase::Pending => {}
        Phase::Opening | Phase::Unwanted | Phase::Running { .. } | Phase::Closing | Phase::Closed => {
            unreachable!("main is not opened while its run prepares")
        }
    }
    conversations.retire(main);
    answer(reply_to, terminal, run.inbox.fence(), Box::default(), out)
}

/// Working in the ordinary or finishing phase: main called `finish` as
/// `made`. A refused outcome is returned at once; an accepted verdict ends the
/// run; a change lands, by the call's deadline, and the run goes on meanwhile.
#[expect(clippy::too_many_arguments, reason = "a cell handler takes the fields it touches")]
fn finish(
    run: &mut Run,
    run_id: Id<Run>,
    conversations: &mut Slab<Conversation>,
    calls: &mut Calls,
    alarms: &mut Deadlines<Alarm>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    made: Asking,
    declared: Declared,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    let Asking { name, call, deadline } = made;
    let conversation = conversations.get(main).expect("main lives while its run works");
    assert!(conversation.calls == 0, "a finish is a write, which a conversation runs alone");
    if run.charter.grants.wait && run.inbox.has_queued() {
        out.push(Request::Return { spent: 0, call, result: Returned::Crossed { text: prompt::crossed() } });
        return State::Working { reply_to, main };
    }
    let max = env.limits.outcome_bytes;
    let judged = match outcome::owned_bytes(&declared) {
        Some(cost) if cost <= max => outcome::judge(&run.charter.outcome, &declared),
        Some(_) | None => Err(outcome::too_large(max)),
    };
    if let Err(problems) = judged {
        out.push(Request::Return { spent: 0, call, result: Returned::Rejected { problems } });
        run.rejected = run.rejected.saturating_add(1);
        return working_again(run, conversations, reply_to, main, env.now, out);
    }
    match declared {
        completed @ (Declared::Verdict(_) | Declared::Report(_) | Declared::Failure(_)) => {
            out.push(Request::Return { spent: 0, call, result: Returned::Accepted });
            wind_down(conversations, reply_to, main, Ending::Accepted(completed), out)
        }
        Declared::Change(change) => {
            if name.activation != run.activation || name.completion == 0 {
                out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::Name } });
                return State::Working { reply_to, main };
            }
            if deadline <= env.now {
                out.push(Request::Return { spent: 0, call, result: Returned::TimedOut });
                return State::Working { reply_to, main };
            }
            if calls.is_full() {
                out.push(Request::Return { spent: 0, call, result: Returned::Busy });
                return State::Working { reply_to, main };
            }
            let work = Work::Landing(land::landing(change, name, deadline, true));
            let id = begin_call(calls, alarms, Call { run: run_id, conversation: main, owner: call, work }, deadline);
            run.lease.workspace(id.token());
            match &mut calls.get_mut(id).expect("inserted above").work {
                Work::Landing(landing) => land::cancel_jobs(landing, jobs::peer(conversations, main), out),
                Work::Child(_) | Work::Host(_) => unreachable!("inserted as a landing"),
            }
            let conversation = conversations.get_mut(main).expect("main lives while its run works");
            conversation.calls = conversation.calls.saturating_add(1);
            State::Working { reply_to, main }
        }
    }
}

#[expect(clippy::too_many_arguments, reason = "exclusive main delivery handler takes its owned bindings")]
fn deliver(
    run: &mut Run,
    run_id: Id<Run>,
    conversations: &mut Slab<Conversation>,
    calls: &mut Calls,
    alarms: &mut Deadlines<Alarm>,
    main: Id<Conversation>,
    made: Asking,
    change: Change,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) {
    let Asking { name, call, deadline } = made;
    assert!(conversations.get(main).expect("main lives").calls == 0, "delivery is an exclusive write");
    let Some(spec) = &run.charter.grants.deliver else {
        out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::NotGranted } });
        return;
    };
    let judged = match outcome::change_bytes(&change) {
        Some(bytes) if bytes <= env.limits.outcome_bytes => outcome::judge_change(spec, &change),
        Some(_) | None => Err(outcome::too_large(env.limits.outcome_bytes)),
    };
    if let Err(problems) = judged {
        run.rejected = run.rejected.saturating_add(1);
        out.push(Request::Return { spent: 0, call, result: Returned::Rejected { problems } });
        return;
    }
    if name.activation != run.activation || name.completion == 0 {
        out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::Name } });
        return;
    }
    if deadline <= env.now {
        out.push(Request::Return { spent: 0, call, result: Returned::TimedOut });
        return;
    }
    if calls.is_full() {
        out.push(Request::Return { spent: 0, call, result: Returned::Busy });
        return;
    }
    let work = Work::Landing(land::landing(change, name, deadline, false));
    let id = begin_call(calls, alarms, Call { run: run_id, conversation: main, owner: call, work }, deadline);
    run.lease.workspace(id.token());
    match &mut calls.get_mut(id).expect("inserted above").work {
        Work::Landing(landing) => land::cancel_jobs(landing, jobs::peer(conversations, main), out),
        Work::Child(_) | Work::Host(_) => unreachable!("inserted as delivery"),
    }
    let main = conversations.get_mut(main).expect("main lives");
    main.calls = main.calls.checked_add(1).expect("bounded exclusive call");
}

/// A call a conversation made: its token for it, and its deadline.
struct Asking {
    name: CallName,
    call: Token,
    deadline: Time,
}

/// What a conversation asks for when it asks for a sub-agent.
struct Wanted {
    brief: Box<[u8]>,
    families: Families,
    llm: Option<Box<[u8]>>,
    share: Share,
}

/// Working: the conversation `asker` asked for a sub-agent as `made`. Open
/// it, with no more time than to the call's deadline, or return why not.
#[expect(clippy::too_many_arguments, reason = "a cell handler takes the fields it touches")]
fn sub_agent(
    run: &mut Run,
    run_id: Id<Run>,
    conversations: &mut Slab<Conversation>,
    calls: &mut Calls,
    alarms: &mut Deadlines<Alarm>,
    asker: Id<Conversation>,
    made: Asking,
    wanted: Wanted,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) {
    let Asking { call, deadline, name: _ } = made;
    if u64::try_from(wanted.brief.len()).unwrap_or(u64::MAX) > env.limits.host_part {
        out.push(Request::Return {
            spent: 0,
            call,
            result: Returned::Refused { refusal: AskRefusal::TooLarge { bound: env.limits.host_part } },
        });
        return;
    }
    let asking = conversations.get(asker).expect("a conversation lives until it has ended");
    let left = run.deadline.saturating_since(env.now);
    let means = Means {
        spent: run.spent,
        conversations: run.conversations,
        left,
        call_left: deadline.saturating_since(env.now),
    };
    if run.charter.budget.exhausted(run.spent).is_some() {
        out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::Over } });
        return;
    }
    let planned = agent::plan(
        &run.charter,
        means,
        asking.families,
        wanted.families,
        wanted.llm.as_deref(),
        wanted.share,
        &env.limits,
    );
    let plan = match planned {
        Ok(plan) => plan,
        Err(refusal) => {
            out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal } });
            return;
        }
    };
    assert!(
        plan.budget == crate::Budget { turns: plan.granted.turns, time: plan.granted.time, spend: plan.granted.spend },
        "a child's opening budget is exactly its effective grant"
    );
    match prompt::child_host_bytes(&run.charter, run.workspace.as_ref(), &run.found, &wanted.brief, plan.families) {
        Some(bytes) if bytes <= env.limits.host_part => {}
        Some(_) | None => {
            out.push(Request::Return {
                spent: 0,
                call,
                result: Returned::Refused { refusal: AskRefusal::TooLarge { bound: env.limits.host_part } },
            });
            return;
        }
    }
    if conversations.is_full() || calls.is_full() {
        out.push(Request::Return { spent: 0, call, result: Returned::Busy });
        return;
    }
    let ordinal = run.next_ordinal;
    let Some(next_ordinal) = ordinal.checked_add(1) else {
        out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::TooMany } });
        return;
    };
    // A call is stored before its child, which names it, and holds the child
    // once the child has a name too.
    let unnamed = Call { run: run_id, conversation: asker, owner: call, work: Work::Child(Child::Closed) };
    let id = begin_call(calls, alarms, unnamed, deadline);
    if plan.families.tools.modify || plan.families.tools.shell {
        run.lease.child(id);
    } else {
        run.lease.assert_free();
    }
    let child = Conversation {
        run: run_id,
        kind: crate::facts::Kind::Child { parent: asker.token(), call, asked: wanted.share, granted: plan.granted },
        opened_at: env.now,
        asker: Some(id),
        families: plan.families,
        spent: Spend::ZERO,
        subtree_spent: 0,
        reservation: None,
        budget_close: None,
        calls: 0,
        phase: Phase::Opening,
    };
    let child = conversations.insert(child).expect("checked for room above");
    run.children.insert(child).expect("admitted child count fits run slots");
    calls.get_mut(id).expect("inserted above").work = Work::Child(agent::working(child));
    let asking = conversations.get_mut(asker).expect("a conversation lives until it has ended");
    asking.calls = asking.calls.saturating_add(1);
    run.conversations = run.conversations.saturating_add(1);
    run.next_ordinal = next_ordinal;
    let opening = child_opening(run, &wanted.brief, plan, ordinal, &env.limits);
    if plan_writing(conversations, child) {
        calls.get_mut(id).expect("admitted child call").work =
            Work::Child(Child::Cancelling { child, opening: Box::new(opening), deadline, why: None });
        out.push(Request::CancelJobs { peer: jobs::peer(conversations, asker) });
    } else {
        out.push(Request::Open { conversation: child.token(), opening });
    }
}

fn plan_writing(conversations: &Slab<Conversation>, child: Id<Conversation>) -> bool {
    let families = conversations.get(child).expect("admitted child").families;
    families.tools.modify || families.tools.shell
}

/// Render an admitted child's immutable opening from its effective grant.
fn child_opening(run: &Run, brief: &[u8], plan: agent::Plan, ordinal: u32, limits: &Limits) -> Opening {
    Opening {
        final_turn: Some(crate::FinalTurn {
            note: prompt::child_final_note(),
            allowance: agent::final_allowance(plan.budget.time, limits.wrap_up),
        }),
        generated_schema_bytes: 0,
        identity: run.identity,
        ordinal,
        activation: run.activation,
        transcript: None,
        wait: false,
        host_tools: Box::new([]),
        deliver: false,
        system: prompt::child(
            &run.charter,
            run.workspace.as_ref(),
            &run.found,
            brief,
            plan.families,
            plan.granted,
            limits.answer_bytes,
        ),
        prompt: copy_of(prompt::BEGIN),
        summary: copy_of(prompt::SUMMARY),
        preamble: copy_of(prompt::PREAMBLE),
        tools: plan.families.tools,
        workspace: run.workspace.clone(),
        budget: plan.budget,
        finish: false,
        families: plan.families,
        line: charter::line(&plan.llm, limits.compaction_material),
        llm: plan.llm,
    }
}

/// Stores `call`, there being room, and arms its deadline.
fn begin_call(calls: &mut Calls, alarms: &mut Deadlines<Alarm>, call: Call, deadline: Time) -> Id<Call> {
    let id = calls.insert(call);
    alarms.arm(Alarm::Call { call: id }, deadline).expect("the alarm table has room for an alarm per call");
    id
}

/// Stops what the call `id` is doing, for `why`; it returns once that has
/// settled.
fn stop_call(domain: &mut Domain, id: Id<Call>, why: Withdrawal, out: &mut Queue<Request>) {
    let host = match &domain.calls.get(id).expect("live call").work {
        Work::Host(_) => true,
        Work::Child(_) | Work::Landing(_) => false,
    };
    if host {
        host_stop(domain, id, why, out);
        return;
    }
    let call = domain.calls.get_mut(id).expect("a call lives until it returns, and its alarm with it");
    domain.facts.about(call.run.token());
    match &mut call.work {
        Work::Host(_) => unreachable!("host stop dispatched above"),
        Work::Landing(landing) => land::withdraw(landing, id, why, out),
        Work::Child(child) => {
            if let Some(child) = agent::withdraw(child, why) {
                close(&mut domain.conversations, child, out);
            }
        }
    }
}

/// Retires the call `id`, which has returned: its alarm is cancelled, and its
/// conversation has a call fewer in flight. The run it is of.
fn retire_call(
    calls: &mut Calls,
    alarms: &mut Deadlines<Alarm>,
    conversations: &mut Slab<Conversation>,
    id: Id<Call>,
) -> Id<Run> {
    let call = calls.get(id).expect("a call lives until it returns");
    let (run, conversation) = (call.run, call.conversation);
    calls.retire(id);
    alarms.cancel(Alarm::Call { call: id });
    alarms.cancel(Alarm::Host { call: id });
    let conversation = conversations.get_mut(conversation).expect("a conversation outlives its calls");
    conversation.calls = conversation.calls.checked_sub(1).expect("a conversation counts its calls");
    run
}

fn working_again(
    run: &Run,
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    now: Time,
    out: &mut Queue<Request>,
) -> State {
    match final_remaining(run, now) {
        Ok(()) => State::Working { reply_to, main },
        Err(exhausted) => wind_down(
            conversations,
            reply_to,
            main,
            Ending::Failed(Failure::Budget { exhausted, used: None, bound: None }),
            out,
        ),
    }
}

/// Working, an ending decided: close main, and wait for it to end.
fn wind_down(
    conversations: &mut Slab<Conversation>,
    reply_to: ReplyTo,
    main: Id<Conversation>,
    ending: Ending,
    out: &mut Queue<Request>,
) -> State {
    close(conversations, main, out);
    State::Winding { reply_to, ending }
}

/// Closes the conversation `id` once, retaining an already closing phase: at once, or
/// once it starts. A closing conversation withdraws its own calls.
fn close(conversations: &mut Slab<Conversation>, id: Id<Conversation>, out: &mut Queue<Request>) {
    let conversation = conversations.get_mut(id).expect("a conversation lives until it has ended");
    let phase = mem::replace(&mut conversation.phase, Phase::Closed);
    conversation.phase = match phase {
        Phase::Running { peer } => closing(peer, out),
        // It closes once it starts. A budget close and withdrawal may cross.
        Phase::Opening | Phase::Unwanted => Phase::Unwanted,
        Phase::Closing => Phase::Closing,
        Phase::Pending | Phase::Closed => {
            unreachable!("only an opened live conversation is closed")
        }
    };
}

/// Working, main yielded and may be nudged: tell it to carry on.
fn say(reply_to: ReplyTo, main: Id<Conversation>, peer: Token, text: Box<[u8]>, out: &mut Queue<Request>) -> State {
    out.push(Request::Say { peer, text });
    State::Working { reply_to, main }
}

fn closing(peer: Token, out: &mut Queue<Request>) -> Phase {
    out.push(Request::Close { peer });
    Phase::Closing
}

/// Answers the host: the run is done.
fn answer(
    reply_to: ReplyTo,
    answer: Answer,
    read: Option<Token>,
    detail: Box<[u8]>,
    out: &mut Queue<Request>,
) -> State {
    out.push(Request::Answer { to: reply_to, answer, read, detail });
    State::Closed
}

// Helpers.

fn budget_answer(answer: Answer, run: &Run, now: Time) -> Answer {
    match answer {
        Answer::Failed { failure: Failure::Budget { exhausted, .. }, spent, turns } => {
            let fields = match exhausted {
                Exhausted::Turns => Some((u64::from(run.spent.turns), u64::from(run.charter.budget.turns))),
                Exhausted::Spend => Some((run.spent.units, run.charter.budget.spend)),
                Exhausted::Time => Some((
                    now.saturating_since(run.started_at).as_nanos() / 1_000_000,
                    run.charter.budget.time.as_nanos() / 1_000_000,
                )),
                Exhausted::Tokens(_) | Exhausted::Overflow(_) => None,
            };
            let (used, bound) = match fields {
                Some((used, bound)) => (Some(used), Some(bound)),
                None => (None, None),
            };
            Answer::Failed { failure: Failure::Budget { exhausted, used, bound }, spent, turns }
        }
        other @ (Answer::Failed { .. } | Answer::Accepted { .. } | Answer::Parked { .. } | Answer::Refused(_)) => other,
    }
}

/// The answer of a run whose main conversation ended on its own, the run
/// having spent `spent`.
fn ending(end: End, spent: Spend, turns: u32) -> Answer {
    match end {
        End::PriceOverflow => Answer::Failed {
            failure: Failure::Budget {
                exhausted: Exhausted::Overflow(crate::Overflow::Spend),
                used: None,
                bound: None,
            },
            spent,
            turns,
        },
        End::UsageOverflow => Answer::Failed {
            failure: Failure::Budget {
                exhausted: Exhausted::Overflow(crate::Overflow::Usage),
                used: None,
                bound: None,
            },
            spent,
            turns,
        },
        End::Receiving(limit) => Answer::Failed {
            failure: Failure::Budget { exhausted: Exhausted::Tokens(limit), used: None, bound: None },
            spent,
            turns,
        },
        End::TranscriptRefused { reason } => Answer::Failed { failure: Failure::Transcript(reason), spent, turns },
        End::Busy => Answer::Refused(Refusal::Busy),
        End::Invalid => Answer::Refused(Refusal::Invalid(Invalid::Conversation)),
        End::Fault(fault) => Answer::Failed { failure: Failure::Model(fault), spent, turns },
        End::Budget(exhausted) => {
            Answer::Failed { failure: Failure::Budget { exhausted, used: None, bound: None }, spent, turns }
        }
        End::Closed => unreachable!("a conversation ends closed only once its run has closed it, winding down"),
    }
}

/// The answer of a run that wound down to `ending`, having spent `spent`.
fn finished(ending: Ending, spent: Spend, turns: u32) -> Answer {
    match ending {
        Ending::Parked => Answer::Parked { spent, turns },
        Ending::Accepted(outcome) => Answer::Accepted { outcome, spent, turns },
        Ending::Failed(failure) => Answer::Failed { failure, spent, turns },
    }
}

/// Counts a nudge for a run whose LLM stopped without finishing for `stop`,
/// or says how the run fails instead: when its nudges are used up, or no turn
/// is left in its budget for the LLM to carry on with.
fn nudge(run: &mut Run, stop: Stop, limits: &Limits) -> Result<(), Failure> {
    if run.nudges >= limits.nudges {
        return Err(unfinished(stop, run.nudges, run.rejected));
    }
    // Room to start a turn, as a conversation's ceilings have it: some
    // turns, input and output left.
    let spent = run.spent;
    let budget = &run.charter.budget;
    if spent.turns >= budget.turns {
        return Err(Failure::Budget { exhausted: Exhausted::Turns, used: None, bound: None });
    }
    if spent.units >= budget.spend {
        return Err(Failure::Budget { exhausted: Exhausted::Spend, used: None, bound: None });
    }
    run.nudges = run.nudges.saturating_add(1);
    Ok(())
}

/// How a run fails when its LLM stops without finishing, through `nudges`
/// nudges and `rejected` refused outcomes: as unfinished, or with the fault
/// its last stop shows.
fn unfinished(stop: Stop, nudges: u32, rejected: u32) -> Failure {
    match stop {
        Stop::Budget => unreachable!("main has no session-owned final turn"),
        Stop::EndTurn => Failure::Policy(Policy::Unfinished { nudges, rejected }),
        Stop::MaxTokens => Failure::Model(Fault::Truncated),
        Stop::Refusal => Failure::Model(Fault::Declined),
        Stop::NoCalls => Failure::Model(Fault::Malformed),
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "host admission checks the owning conversation and immutable call fields"
)]
fn host_call(
    run: &Run,
    run_id: Id<Run>,
    conversations: &mut Slab<Conversation>,
    calls: &mut Calls,
    alarms: &mut Deadlines<Alarm>,
    conversation: Id<Conversation>,
    made: Asking,
    tool: Box<[u8]>,
    effect: HostEffect,
    input: HostInput,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) {
    let Asking { name, call, deadline } = made;
    if conversations.get(conversation).expect("caller lives").asker.is_some() {
        out.push(Request::Return { spent: 0, call, result: Returned::HostRejected(HostProblem::Undeclared) });
        return;
    }
    let mut timeout = None;
    for declaration in &run.charter.grants.host_tools {
        if declaration.name == tool {
            if declaration.effect != effect {
                out.push(Request::Return { spent: 0, call, result: Returned::HostRejected(HostProblem::Effect) });
                return;
            }
            timeout = Some(declaration.timeout.min(env.limits.host_timeout_max));
        }
    }
    let Some(timeout) = timeout else {
        out.push(Request::Return { spent: 0, call, result: Returned::HostRejected(HostProblem::Undeclared) });
        return;
    };
    if input.bytes().len() > usize::try_from(env.limits.host_input_bytes).expect("byte cap fits") {
        out.push(Request::Return { spent: 0, call, result: Returned::HostRejected(HostProblem::TooLarge) });
        return;
    }
    if name.activation != run.activation || name.completion == 0 {
        out.push(Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::Name } });
        return;
    }
    if env.now >= deadline || env.now >= run.deadline {
        out.push(Request::Return { spent: 0, call, result: Returned::TimedOut });
        return;
    }
    if calls.is_full() {
        out.push(Request::Return { spent: 0, call, result: Returned::Busy });
        return;
    }
    let relay = HostRelay {
        name,
        tool,
        effect,
        input,
        timeout,
        caller_deadline: deadline.min(run.deadline),
        stopped: None,
        unknown: false,
        stage: HostStage::Closed,
    };
    let work = Work::Host(relay);
    let id = begin_call(calls, alarms, Call { run: run_id, conversation, owner: call, work }, deadline);
    match &mut calls.get_mut(id).expect("inserted above").work {
        Work::Host(relay) => host_send(relay, id, run.host_name, 1, env.now, out),
        Work::Child(_) | Work::Landing(_) => unreachable!("inserted as host relay"),
    }
    let caller = conversations.get_mut(conversation).expect("caller lives");
    caller.calls = caller.calls.checked_add(1).expect("bounded calls");
    host_follow(calls, alarms, id);
}

fn host_send(relay: &mut HostRelay, id: Id<Call>, host_run: Token, attempt: u32, now: Time, out: &mut Queue<Request>) {
    let deadline = now.saturating_add(relay.timeout).min(relay.caller_deadline);
    assert!(now < deadline, "admission and recovery refuse expired relays");
    out.push(Request::HostCall {
        host_run,
        relay: RelayName { owner: id.token(), attempt },
        name: relay.name,
        tool: relay.tool.clone(),
        effect: relay.effect,
        input: relay.input.clone(),
        deadline,
    });
    relay.stage = HostStage::Sending { attempt, deadline };
}

fn host_follow(calls: &Calls, alarms: &mut Deadlines<Alarm>, id: Id<Call>) {
    let relay = match &calls.get(id).expect("host call lives").work {
        Work::Host(relay) => relay,
        Work::Child(_) | Work::Landing(_) => unreachable!("host handler owns its call"),
    };
    let at = match relay.stage {
        HostStage::Sending { deadline, .. } => Some(deadline),
        HostStage::Backoff { at, .. } => Some(at),
        HostStage::Withdrawing { .. } | HostStage::Closed => None,
    };
    if let Some(at) = at {
        alarms.arm(Alarm::Host { call: id }, at).expect("two alarm slots per call");
    } else {
        alarms.cancel(Alarm::Host { call: id });
    }
}

fn host_return(domain: &mut Domain, id: Id<Call>, result: Returned, out: &mut Queue<Request>) {
    let call = domain.calls.get(id).expect("host call lives");
    domain.facts.about(call.run.token());
    let owner = call.owner;
    out.push(Request::Return { spent: 0, call: owner, result });
    retire_call(&mut domain.calls, &mut domain.alarms, &mut domain.conversations, id);
}

fn host_stop(domain: &mut Domain, id: Id<Call>, why: Withdrawal, out: &mut Queue<Request>) {
    let call = domain.calls.get_mut(id).expect("caller lives until relay settles");
    let relay = match &mut call.work {
        Work::Host(relay) => relay,
        Work::Child(_) | Work::Landing(_) => unreachable!("host stop dispatched by work kind"),
    };
    relay.stopped = relay.stopped.or(Some(why));
    let stage = mem::replace(&mut relay.stage, HostStage::Closed);
    let result = match stage {
        HostStage::Sending { attempt, .. } => {
            out.push(Request::WithdrawHost { relay: RelayName { owner: id.token(), attempt } });
            relay.stage = HostStage::Withdrawing { attempt };
            None
        }
        HostStage::Withdrawing { attempt } => {
            relay.stage = HostStage::Withdrawing { attempt };
            None
        }
        HostStage::Backoff { .. } => Some(if relay.unknown { Returned::HostUnknown } else { call::stopped(why) }),
        HostStage::Closed => unreachable!("only a live call stops"),
    };
    if let Some(result) = result {
        host_return(domain, id, result, out);
    } else {
        host_follow(&domain.calls, &mut domain.alarms, id);
    }
}

pub(crate) fn host_alarm(domain: &mut Domain, env: &Env<Limits>, id: Id<Call>, out: &mut Queue<Request>) {
    let call = domain.calls.get_mut(id).expect("alarm belongs to live host call");
    let run = domain.runs.get(call.run).expect("run waits for calls");
    let active = host_active(&run.state);
    let relay = match &mut call.work {
        Work::Host(relay) => relay,
        Work::Child(_) | Work::Landing(_) => unreachable!("host alarm belongs to host work"),
    };
    let stage = mem::replace(&mut relay.stage, HostStage::Closed);
    let result = match stage {
        HostStage::Sending { attempt, .. } => {
            out.push(Request::WithdrawHost { relay: RelayName { owner: id.token(), attempt } });
            relay.stage = HostStage::Withdrawing { attempt };
            None
        }
        HostStage::Backoff { attempt, .. } => {
            if active && relay.stopped.is_none() && env.now < relay.caller_deadline {
                host_send(relay, id, run.host_name, attempt, env.now, out);
                None
            } else {
                Some(if relay.unknown { Returned::HostUnknown } else { Returned::Busy })
            }
        }
        HostStage::Withdrawing { .. } | HostStage::Closed => unreachable!("non-timed host stage"),
    };
    if let Some(result) = result {
        host_return(domain, id, result, out);
    } else {
        host_follow(&domain.calls, &mut domain.alarms, id);
    }
}

fn host_active(state: &State) -> bool {
    match state {
        State::Working { .. } => true,
        State::Awaiting { .. }
        | State::Preparing { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => false,
    }
}

pub(crate) fn host_returned(
    domain: &mut Domain,
    env: &Env<Limits>,
    name: RelayName,
    reply: HostReply,
    out: &mut Queue<Request>,
) {
    let id = Id::<Call>::from_token(name.owner);
    let Some(call) = domain.calls.get(id) else {
        return;
    };
    if domain.calls.find(call.conversation, call.owner) != Some(id) {
        return;
    }
    let relay = match &call.work {
        Work::Host(relay) => relay,
        Work::Child(_) | Work::Landing(_) => return,
    };
    let expected = match relay.stage {
        HostStage::Sending { attempt, .. } | HostStage::Withdrawing { attempt } => Some(attempt),
        HostStage::Backoff { .. } | HostStage::Closed => None,
    };
    if expected != Some(name.attempt) {
        return;
    }
    let call = domain.calls.get_mut(id).expect("named call is live");
    let run = domain.runs.get(call.run).expect("run waits for relay terminal");
    let active = host_active(&run.state);
    let relay = match &mut call.work {
        Work::Host(relay) => relay,
        Work::Child(_) | Work::Landing(_) => unreachable!("verified host work"),
    };
    relay.stage = HostStage::Closed;
    let result = match reply {
        HostReply::Answered(answer) => {
            Some(if answer.text().len() <= usize::try_from(env.limits.host_reply_bytes).expect("cap fits") {
                Returned::HostAnswered(answer)
            } else {
                Returned::HostTooLarge {
                    bytes: u32::try_from(answer.text().len()).expect("constructor-bounded host answer"),
                    max: env.limits.host_reply_bytes,
                }
            })
        }
        HostReply::Busy => {
            let exhausted = if relay.unknown { Returned::HostUnknown } else { Returned::Busy };
            host_recover(relay, name.attempt, active, env, exhausted)
        }
        HostReply::Withdrawn | HostReply::Unanswered(_) => {
            relay.unknown = true;
            host_recover(relay, name.attempt, active, env, Returned::HostUnknown)
        }
        HostReply::TooLarge => Some(Returned::HostReportedTooLarge),
    };
    if let Some(result) = result {
        host_return(domain, id, result, out);
    } else {
        host_follow(&domain.calls, &mut domain.alarms, id);
    }
}

fn host_recover(
    relay: &mut HostRelay,
    attempt: u32,
    active: bool,
    env: &Env<Limits>,
    exhausted: Returned,
) -> Option<Returned> {
    let at = env.now.saturating_add(env.limits.host_backoff);
    if !active || relay.stopped.is_some() || at >= relay.caller_deadline {
        return Some(exhausted);
    }
    let Some(next) = attempt.checked_add(1) else {
        return Some(exhausted);
    };
    relay.stage = HostStage::Backoff { attempt: next, at };
    None
}

/// Drain one wind-down effect: a child close or main's one yield request.
pub(crate) fn resume(domain: &mut Domain, out: &mut Queue<Request>) {
    let Some(id) = domain.ready.pop() else {
        return;
    };
    let run = domain.runs.get(id).expect("ready run is retained until its answer");
    let exhausted = match &run.wind_down {
        Some(crate::wind_down::WindDown::Settling { cause, .. }) => cause.exhausted(),
        Some(crate::wind_down::WindDown::Finishing { noted: false, .. }) => {
            yield_main(run, &domain.conversations, out);
            return;
        }
        Some(crate::wind_down::WindDown::Finishing { noted: true, .. }) | None => return,
    };
    match run.state {
        State::Working { .. } => {}
        State::Preparing { .. }
        | State::Awaiting { .. }
        | State::Stopping { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Winding { .. }
        | State::Closed => return,
    }
    domain.facts.about(id.token());
    for child in &run.children {
        let conversation = domain.conversations.get_mut(*child).expect("retained child");
        match conversation.phase {
            Phase::Opening | Phase::Running { .. } => {
                conversation.budget_close = exhausted;
                close(&mut domain.conversations, *child, out);
                domain.ready.keep(id);
                return;
            }
            Phase::Unwanted | Phase::Closing | Phase::Closed => {}
            Phase::Pending => unreachable!("a child is opened at admission"),
        }
    }
}

/// History presence is kept after its handle moves to the main opening.
pub(crate) fn resumed(runs: &Slab<Run>, run: Token) -> bool {
    runs.get(Id::from_token(run)).expect("an admitted run lives through its facts").resumed
}

/// One deferred request when the wind-down reaches main, after child settlement.
fn yield_main(run: &Run, conversations: &Slab<Conversation>, out: &mut Queue<Request>) {
    match run.charter.mode {
        Mode::Finite => return,
        Mode::Open => {}
    }
    let main = match run.state {
        State::Working { main, .. } => main,
        State::Preparing { .. }
        | State::Awaiting { .. }
        | State::Listening { .. }
        | State::Waiting { .. }
        | State::Stopping { .. }
        | State::Winding { .. }
        | State::Closed => return,
    };
    let main = conversations.get(main).expect("working main is retained");
    if main.reservation.is_none() {
        return;
    }
    match main.phase {
        Phase::Running { peer } => out.push(Request::Yield { peer }),
        Phase::Pending | Phase::Opening | Phase::Unwanted | Phase::Closing | Phase::Closed => {}
    }
}
