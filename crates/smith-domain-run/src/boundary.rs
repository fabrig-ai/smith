//! The records that cross the boundary with the run's parent, the top-level
//! domain (programming-model.md, section 4.5). The run defines them; its parent depends on it.
//! Contracts: domain/run.md, sections 3, 5, 6, 8, 9 and 10;
//! protocol/channel.md, sections 4, 5 and 7; protocol/transcript.md, section 2.
//!
//! All three of the run's faces cross here:
//!
//! - The host's, which the parent routes to and from the protocol layer. A
//!   [`Event::Start`] is a call, answered by exactly one [`Request::Answer`].
//!   An admitted run is named by [`Request::Admitted`] first, so that a
//!   [`Event::Cancel`] can name it. A run's host call, [`Request::Deliver`], is
//!   ended by exactly one [`Event::Delivered`]. After submission the
//!   host operation is never abandoned; its deadline bounds the host terminal.
//!   Duplicate or stale callback owners are inert (domain/run.md, sections 8 and 10).
//! - io's, for what the run itself does in its checkout, which the parent
//!   routes to and from the protocol layer. A [`Request::Read`] is ended by
//!   exactly one [`Event::Read`], a [`Request::Probe`] by one
//!   [`Event::Probed`], and a [`Request::Check`] by one [`Event::Checked`],
//!   or after a [`Request::Abort`] by [`Event::Aborted`] if the abort won.
//!   Each carries its deadline, and io runs the race (programming-model.md, section 5.3).
//! - The conversations', which the parent translates to and from the session
//!   child domain's vocabulary. A [`Request::Open`] is ended by exactly one
//!   [`Event::Ended`], after a [`Event::Started`] unless the conversation was
//!   refused at its entrance. Every event about a conversation carries the
//!   run's token for it, `conversation`; the run addresses a conversation by
//!   `peer`, the token it gave back when it started (programming-model.md, section 4.2). A conversation's
//!   [`Event::Delegated`] call is ended by exactly one [`Request::Return`],
//!   after an [`Event::Withdraw`] too; the call is named by the
//!   conversation's own token for it, `call`. It carries its deadline, and
//!   the run runs the race: past it, the run stops work before submission and
//!   returns it as timed out once that settles. A submitted delivery always
//!   owes its host terminal, including after withdrawal or expiry;
//!   caller-only expiry does not decide the run's shutdown (domain/run.md, 8.2 and 10).
//!
//! A request's `owner` is the run's token for what asked: the run itself for
//! a read or a probe, or a delivery call for a check and its abort or host
//! submission. Submitted host delivery has no cancellation request.
//! It is echoed on the terminal.

use alloc::boxed::Box;

use crate::delivery::{CallName, Delivered, Delivery, DeliveryFailure, DeliveryRefusal};

use skein_lib::{Duration, ReplyTo, Time, Token};

use crate::budget::{Budget, Exhausted, Spend};
use crate::charter::{Charter, Families, Llm, Tools};
use crate::outcome::{Change, Declared, Problems};

/// Host acknowledgement credit for one activation; the agent reserves the largest turn before another completion
/// (protocol/channel.md, sections 3 and 7).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Window {
    /// Turns that may be told without acknowledgement.
    pub turns: u32,
    /// Bytes that may be told without acknowledgement.
    pub bytes: u64,
    /// Largest turn this agent may tell, established from its receiving limits.
    pub largest_turn: u64,
}

/// Named, labelled message carried by a host Start; it ends with that start or its run.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Message {
    /// Admitted Open messages request a final reply; Finite treats them as ordinary words.
    pub finish_requested: bool,
    /// Opaque parent name, unique among the start's messages.
    pub name: Token,
    pub label: Box<[u8]>,
    pub text: Box<[u8]>,
}

/// parent -> run
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// Actual terminal of one host relay; old attempts and callback generations are inert.
    HostReturned {
        /// Live attempt identity, distinct from durable `CallName`.
        relay: crate::RelayName,
        /// Bounded answer or settled retry classification.
        reply: crate::HostReply,
    },
    /// From the host, a call: start a run on `charter`, and answer once it
    /// has ended. `host_run` is the host's name for the run, echoed on
    /// `Admitted`.
    Start {
        /// Single-use right to answer this call; exactly one terminal consumes it.
        reply_to: ReplyTo,
        /// Parent-supplied stable logical host-run scope, preserved across relay
        /// recovery and restarted activations. Distinct from the returned live run
        /// token and every callback slab generation; echoed without interpretation.
        host_run: Token,
        /// Host-supplied positive activation number, unique within `host_run` across starts.
        activation: u64,
        /// Host identity kept across activations, or absent to draw one at admission.
        identity: Option<crate::Identity>,
        /// Bounded acknowledgement credit for this activation.
        window: Window,
        /// Triggering messages admitted together, before later relayed messages.
        messages: Box<[Message]>,
        /// Host-supplied admission policy, validated before the run starts.
        charter: Charter,
        /// Optional immutable host mounts and initial conflicts, admitted before effects.
        workspace: Option<crate::Workspace>,
        /// Root-owned restore binding, consumed by main once; never decoded here.
        transcript: Option<Token>,
        /// The root selected a concrete transcript for main, independently of its binding.
        resumed: bool,
    },

    /// Parent-labelled live message; names, including zero, are opaque and
    /// unique for the active run. Admission yields a typed terminal on refusal.
    Message {
        /// Admitted Open messages request a final reply; Finite ignores this bit.
        finish_requested: bool,
        /// Admitted live run handle.
        run: Token,

        /// Parent-issued active-run unique name.
        name: Token,

        /// Sender label rendered by the run before admission.
        label: Box<[u8]>,

        /// Attested UTF-8 text; the run checks its rendered bound at ingress.
        text: Box<[u8]>,
    },

    /// The host durably kept this turn and every preceding turn of the run.
    Acknowledge { run: Token, turn: u32 },

    /// Actual concrete session turn; the root owns the body behind record.
    Turn {
        /// Run-issued conversation binding.
        conversation: Token,

        /// Single-use root handoff name.
        record: Token,

        /// Historical transcript sequence, independent of activation numbering.
        sequence: u32,
        /// This turn starts a provider-history window, declared by the root.
        opens_window: bool,
    },

    /// From the host: end the run `run` as cancelled. A run that has already
    /// answered, or decided how it ends, ignores it.
    Cancel { run: Token },
    /// The conversation was admitted, and `peer` names it from now on.
    Started {
        conversation: Token,
        /// Conversation-issued opaque handle used after admission.
        peer: Token,
    },
    /// The addressed session has settled every job cancelled for a writer.
    JobsCancelled { conversation: Token },
    /// The LLM stopped calling tools, `text` being its last message. The
    /// conversation waits for `Say` or `Close`, and its time keeps running.
    Yielded { conversation: Token, stop: Stop, text: Box<[u8]> },
    /// Session-priced cumulative own and inclusive subtree units. Own deltas
    /// are counted globally once; subtree totals are only terminal child bills.
    /// Unknown/stale conversations are inert.
    Priced {
        /// Live run conversation.
        conversation: Token,

        /// Cumulative own completion units.
        own_spent: u64,

        /// Inclusive subtree units.
        subtree_spent: u64,
    },

    /// The Session completed a turn with exact raw counters in `spend`.
    /// Units are supplied separately by Priced; this event cannot reprice them.
    Used {
        conversation: Token,
        /// Session's exact single-completion raw counters and one turn increment.
        /// Units are ignored: `Priced` alone charges them.
        spend: Spend,
    },
    /// Terminal for `Open`: the conversation ended, having spent `spend` in
    /// all, once nothing it started was in flight.
    Ended {
        conversation: Token,
        /// Terminal classification after everything started beneath this entity has settled.
        end: End,
        /// Session's exact cumulative raw usage after all terminals settled.
        /// Units remain supplied separately by `Priced`.
        spend: Spend,
        /// Session diagnostic kept outside turns, facts and the channel.
        detail: Box<[u8]>,
    },
    /// Terminal for `Read`.
    Read {
        owner: Token,
        /// IO's one terminal for the run's pending text read.
        read: Read,
    },
    /// Terminal for `Probe`: whether an executable file is at the place. A
    /// failure or a deadline passed reads as not.
    Probed {
        owner: Token,
        /// Whether IO found an executable within the named root before its deadline.
        executable: bool,
    },
    /// A call the conversation's LLM made of the run, which the run answers
    /// with one `Return`. `deadline` is the conversation's own expiry: past
    /// it, pre-submission checks abort and return `TimedOut` once settled.
    /// After submission the host terminal remains owed through caller
    /// expiry or withdrawal, including while the conversation closes. A caller-only
    /// stop does not decide the run's independent shutdown outcome.
    Delegated {
        /// Parent-translated concrete transcript origin, fixed-size and distinct
        /// from `call`'s live callback. Zero completion is refused before effects.
        name: CallName,
        conversation: Token,
        /// Root-issued live call token, echoed on the one finish, delivery or sub-agent terminal.
        call: Token,
        /// Typed run tool ask, validated against the asker's authority and charter.
        ask: Ask,
        deadline: Time,
    },
    /// The conversation abandons its call `call`: it is closing. Its `Return`
    /// still comes, once what the call was doing has settled.
    Withdraw {
        conversation: Token,
        /// Root-issued live call token, echoed on the one finish, delivery or sub-agent terminal.
        call: Token,
    },
    /// Terminal for `Check`: what the checks' process did.
    Checked {
        owner: Token,
        /// Checks' process terminal, including its bounded diagnostic tail.
        ran: Ran,
    },
    /// Terminal for `Check`, after `Abort`: the checks were stopped.
    Aborted { owner: Token },
    /// Actual host terminal for `Deliver`, including while shutdown settles.
    /// Constructor-sealed owned evidence is revalidated against admitted writable
    /// mounts; stale callback generations are inert.
    Delivered { owner: Token, delivery: Delivery },
}

/// Why the run refuses a host message at its entrance (domain/run.md, section 6).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum MessageRefusal {
    /// The label, separator and text exceed the rendered-byte bound.
    TooLarge,
    /// The bounded unread inbox has no room.
    Full,
    /// An outstanding message or the latest read fence uses this name.
    NameInUse,
    /// The run has decided its answer and takes no more messages.
    Ending,
}

/// run -> parent
#[derive(PartialEq, Eq, Debug)]
#[expect(clippy::large_enum_variant, reason = "bounded diagnostics move inline with their terminal")]
pub enum Request {
    /// Internal notice after the last child settles; the root may release a held main completion.
    CompletionReady { conversation: Token },
    /// Ask main's session to yield its outstanding completion for the wind-down note; no separate terminal.
    Yield { peer: Token },
    /// A host message refused at ingress; this is its one terminal.
    MessageRefused { host_run: Token, name: Token, reason: MessageRefusal },

    /// Main yielded after a settled wait with an empty inbox. No terminal is owed.
    Waiting {
        /// Stable parent logical run scope.
        host_run: Token,

        /// Latest message consumed by a told turn.
        read: Option<Token>,
    },

    /// One settled main turn, emitted before the final answer; root moves its body.
    Turn {
        /// Stable parent logical run scope.
        host_run: Token,

        /// Single-use root-owned concrete body binding.
        record: Token,

        /// One-based activation-local output number.
        number: u32,

        /// One-based place in the conversation, including restored turns.
        position: u32,

        /// First historical turn or a later context window declared by the root.
        opens_window: bool,

        /// Latest message actually consumed by this turn.
        read: Option<Token>,

        /// Actual cumulative run token usage.
        spent: Spend,
    },

    /// Relay an admitted main host-tool call without interpreting its bytes. Each
    /// attempt gets exactly one terminal; a durable name is decided once.
    HostCall {
        /// Host logical run identity, preserved across restart.
        host_run: Token,
        /// Live relay attempt, never the durable operation name.
        relay: crate::RelayName,
        /// Immutable transcript-derived name, identical on every recovery attempt.
        name: CallName,
        /// Exact declared tool name.
        tool: Box<[u8]>,
        /// Declared scheduling effect, identical across attempts.
        effect: crate::HostEffect,
        /// Complete immutable protocol-attested JSON object, including whitespace.
        input: crate::HostInput,
        /// This relay's bounded deadline; a timeout requests withdrawal and waits for the terminal.
        deadline: Time,
    },
    /// Request settlement of a host relay, retaining its terminal right.
    /// This is transport withdrawal, never cancellation of submitted Delivery.
    WithdrawHost {
        /// The one live attempt to withdraw, answered by `HostReturned` exactly once.
        relay: crate::RelayName,
    },
    /// To the host: the run it names `host_run` was admitted, and is `run` to
    /// the run child domain from now on.
    Admitted { host_run: Token, run: Token },
    /// To the host, the answer to a `Start`: exactly one per start.
    Answer {
        /// Single-use reply right returned to the layer that issued it.
        to: ReplyTo,
        /// Single terminal value returned to the caller.
        answer: Answer,
        /// Main conversation's last diagnostic, for the operator.
        detail: Box<[u8]>,
        /// Last message covered by an actual told main turn; none before any read.
        read: Option<Token>,
    },
    /// Open a conversation, which every event about it names `conversation`.
    Open {
        conversation: Token,
        /// Opener-supplied conversation policy, checked at admission.
        opening: Opening,
    },
    /// A new user message for `peer`, a conversation that has yielded. One that
    /// has ended meanwhile drops it.
    Say {
        /// Conversation-issued opaque handle used after admission.
        peer: Token,
        text: Box<[u8]>,
    },
    /// Cancel the addressed session's jobs; `JobsCancelled` follows settlement.
    CancelJobs { peer: Token },
    /// Close `peer`, in any state: it stops what is in flight, then ends.
    Close {
        /// Conversation-issued opaque handle used after admission.
        peer: Token,
    },
    /// Read the first `max` bytes of the regular file at `at`, following
    /// symbolic links within its root, giving up at `deadline`.
    Read {
        owner: Token,
        at: Place,
        /// Inclusive maximum count allowed by this contract.
        max: u32,
        deadline: Time,
    },
    /// Find out whether an executable file is at `at`, giving up at
    /// `deadline`.
    Probe { owner: Token, at: Place, deadline: Time },
    /// Run the executable at `program`, in its repository's root, as a
    /// contained process, stopping it at `deadline`; keep the last `tail`
    /// bytes of what it writes.
    Check {
        owner: Token,
        /// IO-confined check executable, run as a contained process tree.
        program: Place,
        deadline: Time,
        /// Last output bytes retained under the caller's cap.
        tail: u32,
    },
    /// Stop the `Check` in flight for `owner`. Its terminal still comes:
    /// `Aborted`, or `Checked` if the checks ended first.
    Abort { owner: Token },
    /// To the host: checks of the run it names `host_run` are running until
    /// `deadline` at the latest, so its watchdog waits that long. A notice,
    /// with no terminal. It is a request, not only a fact (`CheckStarted`),
    /// because the watchdog decides on it, and nothing may depend on whether
    /// a fact is kept.
    Checking { host_run: Token, deadline: Time },
    /// To the host: the previously announced check has ended, including after an abort race.
    ChecksEnded { host_run: Token },
    /// To the host, deliver the exact checked writable-directory state under
    /// the logical run named `host_run`. The host interprets the unchanged
    /// generic fields and returns its bounded host terminal. Final Change
    /// and separately granted main delivery share the same exclusive checks;
    /// the terminal remains owed through shutdown.
    Deliver {
        /// Durable transcript-derived host call name, scoped by the logical host run.
        /// Callback `owner` is separate; retries of this operation reuse this name.
        name: CallName,
        /// Bounded host-operation deadline; the host supplies exactly one
        /// terminal even during shutdown. The run never abandons submission.
        deadline: Time,
        host_run: Token,
        owner: Token,
        /// Declared checkout change with validated host-named fields and bounded aggregate ownership, forwarded unchanged.
        change: Change,
    },

    /// The one terminal for the conversation's call `call`.
    Return {
        /// Root-issued live call token, echoed on the one finish, delivery or sub-agent terminal.
        call: Token,
        /// The one terminal value for the enclosing call; ownership passes to its receiver.
        result: Returned,

        /// Inclusive child bill; zero for every non-child call terminal.
        spent: u64,
    },
}

/// What a conversation's LLM asks of the run.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Ask {
    /// Main-only exclusive request to wait after this turn settles and yields.
    Wait,

    /// Invoke a host-declared tool. Main-only declaration and effect are checked
    /// before effects; provider-written JSON bytes are relayed unchanged.
    Host {
        /// Exact declaration name, not interpreted as host policy.
        tool: Box<[u8]>,
        /// Protocol-selected effect; must equal the admitted declaration.
        effect: crate::HostEffect,
        /// Complete protocol-attested bounded object input.
        input: crate::HostInput,
    },
    /// Main-only, separately granted mid-run delivery. Fields use that grant's
    /// required-name caps; every extra value counts toward `Limits::outcome_bytes`.
    /// It takes the same exclusive checked snapshot as finishing Change, then
    /// returns the terminal and continues unless shutdown was pending.
    Deliver {
        /// Opaque host-named metadata, passed unchanged after checked aggregate
        /// ownership and required field validation. No title/body interpretation.
        change: Change,
    },
    /// Finish the run with `outcome`.
    Finish {
        /// Declared or accepted result, validated against the host's contract.
        outcome: Declared,
    },
    /// Open a sub-agent: a conversation of its own on `brief`, with families
    /// of tools no wider than the asker's, on the LLM named `llm` among the
    /// charter's (the main conversation's if none is named), and a share of
    /// the ordinary pool, clamped by `share` when a part is asked for.
    /// Its last message is the call's result.
    SubAgent {
        /// Text supplied for the sub-agent, bounded by the receiving session's byte cap.
        brief: Box<[u8]>,
        /// Explicit sub-agent tool authority, no wider than the asker's.
        families: Families,
        /// Configured sub-agent model or main model when no override is requested.
        llm: Option<Box<[u8]>>,
        share: crate::Share,
    },
}

/// The run's answer to a delegated call.
#[derive(PartialEq, Eq, Hash, Debug)]
#[expect(clippy::large_enum_variant, reason = "bounded diagnostics move inline with their terminal")]
pub enum Returned {
    /// Main finish crossed queued messages; the run offers them before another completion.
    Crossed { text: Box<[u8]> },

    /// Main's retained wait is answered; its opener offers queued messages before the next request.
    Waited { text: Box<[u8]> },

    /// Main parks with its own wait unanswered; the session records the call without a result.
    Withheld,

    /// Actual host result/error text; the host's error is forwarded, never retried.
    HostAnswered(
        /// Exact constructor-bounded host text and error bit, at most receiving reply cap.
        crate::HostAnswer,
    ),
    /// The host decided the call, but its answer exceeds the receiving text cap.
    HostTooLarge { bytes: u32, max: u32 },
    /// Host reports an oversized answer without revealing its size; it is not retried.
    HostReportedTooLarge,
    /// No permissible recovery remains after the earlier relay settled. This
    /// means outcome unknown, never evidence of failure or permission to decide twice.
    HostUnknown,
    /// Host-tool declaration or owned input was refused before any relay.
    HostRejected(
        /// Typed pre-relay semantic admission failure.
        crate::HostProblem,
    ),
    /// Actual host landing evidence. A mid-run call continues normally; a finish
    /// call ends with its admitted Change. Receipt constructors cap each copy.
    Delivered(
        /// Constructor-bounded host receipts, at most 64 unique writable mounts.
        Delivered,
    ),
    /// Host found no changed writable directory; correctable LLM feedback.
    Nothing,
    /// Host named correctable feedback, bounded by `DeliveryRefusal`. Any marker
    /// ordinal is revalidated against admitted writable mounts before forwarding.
    DeliveryRefused(
        /// Named bounded explanation and optional relative marker location.
        DeliveryRefusal,
    ),
    /// A non-Change outcome is accepted: the run finishes with it.
    Accepted,
    /// The outcome does not fit what the run may finish with.
    Rejected {
        /// Bounded validation problems explaining why the declared outcome was rejected.
        problems: Problems,
    },
    /// The checks of the repository `repository` failed, as `ran` says.
    ChecksFailed {
        /// First failed repository in workspace order, or none for a workspace-wide failure.
        repository: Box<[u8]>,
        /// Checks' process terminal, including its bounded diagnostic tail.
        ran: Ran,
    },
    /// Delivery cannot land later because the host context moved, and
    /// the run ends.
    Stale,
    /// The host operation failed. Its fixed generic reason and sealed
    /// 512-byte diagnostic/drop count return as feedback; no landing is claimed.
    DeliveryFailed { failure: DeliveryFailure },
    /// Nothing was decided: the call was withdrawn, or the run is ending
    /// otherwise.
    Cancelled,
    /// Before submission, the call's deadline passed and its sub-agent or
    /// checks settled. A submitted delivery instead owes its settled host terminal.
    TimedOut,
    /// The run has no room for another call now; it may have later.
    Busy,
    /// The sub-agent's last message: at most the run's limit of its first
    /// bytes, with the `cut` bytes past them dropped, and why it stopped.
    Answered {
        text: Box<[u8]>,
        /// Bytes omitted outside the retained output window.
        cut: u64,
        stop: Stop,
        used: crate::facts::Used,
        /// The run's ordinary pool remaining above its finishing reserve.
        left: Budget,
    },
    /// The sub-agent ended without an answer: refused at its entrance, its
    /// LLM failed, or its share of the budget ran out.
    Unanswered {
        /// Terminal classification after everything started beneath this entity has settled.
        end: End,
        used: crate::facts::Used,
        /// The run's ordinary pool remaining above its finishing reserve.
        left: Budget,
    },
    /// The run refuses the requested authority or origin before an effect.
    Refused {
        /// Typed admission refusal; no work runs for a refused entrance.
        refusal: AskRefusal,
    },
}

/// Why a delegated run ask is refused before an effect.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum AskRefusal {
    /// A sub-agent brief exceeds the host's window part before effects.
    TooLarge { bound: u64 },
    /// Parent supplied a zero transcript completion. No check or host effect
    /// begins; a valid session origin is one-based and bounded before dispatch.
    Name,
    /// The asker lacks this mid-run delivery or sub-agent grant, or asked for families of tools
    /// it does not have itself.
    NotGranted,
    /// The run reached its live-conversation ceiling or exhausted its ordinals.
    TooMany,
    /// The charter has no LLM of that name for sub-agents.
    UnknownLlm,
    /// The share asked for, or what the run has left, leaves no room for a
    /// turn.
    Unworkable,
    /// The run has spent past its budget.
    Over,
}

/// What a check's process did: how it ended, and the tail of what it wrote,
/// with the `cut` bytes before the tail dropped.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Ran {
    /// Terminal process classification; only a zero exit code passes checks.
    pub exit: Exit,
    /// Owned output bytes retained within the enclosing output cap.
    pub output: Box<[u8]>,
    /// Bytes omitted outside the retained output window.
    pub cut: u64,
}

/// How workspace checks ended, reported by IO or the run after their tree stopped.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Exit {
    /// It exited with `code`; zero passes.
    Code {
        /// Typed or byte-valued terminal classification in the enclosing boundary.
        code: u8,
    },
    /// IO reports the number of the signal that killed the checks.
    Signalled { signal: u32 },
    /// Its deadline passed, and io stopped it.
    TimedOut,
    /// It could not be started.
    Unstarted,
    /// The run withdrew the checks, and their tree has stopped.
    Cancelled,
}

/// Where a file is, for the run's own io: a repository's root, as io names
/// it, and the path beneath it, names joined by `/`. io resolves it beneath
/// the root.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Place {
    /// IO-issued repository root token, stored and echoed without interpreting it.
    pub root: Token,
    pub path: Box<[u8]>,
}

/// What a `Read` found. A file is read as text: io checks that it is UTF-8,
/// so the domain never parses, and cuts it at a character boundary, so what
/// the run passes on to an LLM is text too.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Read {
    /// The file's first characters, in at most as many bytes as asked for,
    /// cut where a character ends; `whole` when they are all of it.
    Text {
        text: Box<[u8]>,
        /// Whether the retained text includes the entire file.
        whole: bool,
    },
    /// Nothing is there, or not a regular file.
    Missing,
    /// The file is not UTF-8 text.
    NotText,
    /// io failed, or the deadline passed first.
    Failed,
}

/// What a conversation is opened with.
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Opening {
    /// Session-owned final answer for a child; main follows its charter lifecycle in the run.
    pub final_turn: Option<FinalTurn>,
    /// The run identity shared by all its conversations.
    pub identity: crate::Identity,
    /// Unique conversation ordinal within this activation; main is zero.
    pub ordinal: u32,
    /// Host-supplied activation shared by this run's main and child sessions.
    pub activation: u64,

    /// Root-owned restore binding for main, None for every child.
    pub transcript: Option<Token>,

    /// Main-only wait descriptor; children cannot acquire this authority.
    pub wait: bool,

    /// Main's bounded host declarations, copied whole; every child receives none.
    pub host_tools: Box<[crate::HostTool]>,
    /// Whether the parent offers main the separately granted delivery tool.
    /// It is false for every child; this descriptor grants no final outcome form.
    pub deliver: bool,
    /// Contract-generated schema bytes, counted exactly from startup renderer costs.
    pub generated_schema_bytes: u64,
    /// The LLM it talks to.
    pub llm: Llm,
    /// Largest ordinary prompt, in tokens, leaving compaction material above it.
    pub line: u64,
    /// System instructions supplied by the opener, retained within the enclosing byte cap.
    pub system: Box<[u8]>,
    /// The first user message.
    pub prompt: Box<[u8]>,
    /// The run's instruction for summarising this conversation.
    pub summary: Box<[u8]>,
    /// The run's text introducing the summary in a new window.
    pub preamble: Box<[u8]>,
    /// The tools the LLM may run on the checkout.
    pub tools: Tools,
    /// Immutable accepted mounts, git kinds and initial conflicts, copied to the
    /// receiving root; None supplies no workspace tool authority.
    pub workspace: Option<crate::Workspace>,
    /// Its share of the run's budget: what the run has left when it opens, and
    /// the time to the run's deadline. The conversation keeps to it.
    pub budget: Budget,
    /// Whether its LLM may call `finish`, which the conversation runs as a
    /// write: alone, never beside another call. Only main may.
    pub finish: bool,
    /// The families it has: its tools, and whether its LLM may ask for
    /// sub-agents, a call whose effect follows the families asked for (a
    /// sub-agent that may modify or run commands is a write).
    pub families: Families,
}

/// The run's child final note and allowance (domain/run.md, section 5.3).
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct FinalTurn {
    /// Appended by the session before its final answer.
    pub note: Box<[u8]>,
    pub allowance: Duration,
}

/// Why a conversation's LLM stopped calling tools.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Stop {
    /// The child's share ended and its final turn answered.
    Budget,
    /// It finished its turn.
    EndTurn,
    /// It ran out of tokens mid-answer.
    MaxTokens,
    /// It declined to answer.
    Refusal,
    /// It asked for tools and named none.
    NoCalls,
}

/// How a conversation ended.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum End {
    /// Session price cannot be added; prior charges remain exact.
    PriceOverflow,

    /// Session usage cannot be added; prior charges remain exact.
    UsageOverflow,

    /// Session per-kind receiving cap, independent of scalar run exhaustion.
    Receiving(
        /// Exact receiving dimension.
        crate::ReceivingLimit,
    ),

    /// Exact V2 history refusal before provider or tool effects.
    TranscriptRefused {
        /// Small lossless sibling-independent admission reason.
        reason: TranscriptRefusal,
    },

    /// The run closed it.
    Closed,
    /// Refused at its entrance: no room for another conversation.
    Busy,
    /// Refused at its entrance: the opening does not fit the conversations'
    /// limits.
    Invalid,
    /// Its LLM could not go on.
    Fault(Fault),
    /// Its share of the budget ran out.
    Budget(Exhausted),
}

/// A native LLM path cap, sent by the protocol in a terminal failure.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Cap {
    /// The configured bound on encoded request body.
    Request,
    /// The configured bound on encoded request head.
    RequestHead,
    /// The configured bound on response head.
    ResponseHead,
    /// The configured bound on HTTP header field count.
    ResponseFields,
    /// Scanned wire bytes of one event.
    Skip,
    /// The configured bound on an SSE event type or identifier.
    Field,
    /// The configured bound on retained decoded text.
    Retained,
    /// The configured bound on retained JSON tokens.
    Tokens,
    /// The configured bound on JSON nesting depth.
    Depth,
    /// The configured bound on retained string.
    Strings,
    /// The fixed tokenizer bound on a numeral.
    Number,
    /// The configured bound on provider reasoning replay.
    Reasoning,
    /// The configured bound on completion items.
    OutputItems,
    /// The configured bound on other replay metadata.
    Metadata,
    /// The configured bound on offered tools.
    Tools,
    /// The configured bound on history messages and blocks.
    HistoryItems,
    /// The endpoint model's declared completion ceiling in tokens.
    Output,
    /// The configured bound on completion answer.
    Answer,
    /// The configured bound on provider error body.
    ErrorBody,
    /// The configured bound on access token.
    AccessToken,
    /// The configured bound on account identifier.
    AccountId,
}

/// The owner of a bounded value, sent in a terminal limit failure.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Limit {
    /// The requested reservation exceeds the entire native connection pool.
    Pool { reservation: u64 },
    /// The native provider path refused its named cap.
    Path(Cap),
    /// The session's retained window refused its history bound.
    History,
    /// The usable model window could not make its first ordinary completion.
    Window,
    /// One received tool call refused its argument bound.
    Call,
    /// One injected message refused its receiving bound.
    Message,
}

/// The protocol's elapsed call phase, carried losslessly in timeout terminals
/// (protocol/llm.md, sections 6 and 10).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Phase {
    /// The connection's socket admission deadline.
    Connect,
    /// The connection's TLS handshake deadline.
    Handshake,
    /// The completion's first response-byte deadline.
    Head,
    /// The completion's deadline between response events.
    Idle,
    /// The completion's absolute whole-call deadline.
    Whole,
}

/// Exact neutral completion failure after retries or nonretryable refusal.
/// Diagnostic bytes were consumed by session policy; this record remains content-free.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CompletionFailure {
    /// Shared-client receiving allowance exceeded.
    Limit { which: Limit, bound: u64 },

    /// The application aggregate receiving allowance was exceeded.
    ApplicationLimit,

    /// Shared protocol response contract violated.
    Protocol,

    /// Unsolicited lower cancellation.
    Cancelled,

    /// Provider capacity refused the request.
    Overloaded,

    /// Provider could not be reached or failed.
    Unavailable,

    /// The completion deadline elapsed.
    TimedOut { phase: Phase },

    /// Provider context allowance was exceeded.
    ContextTooLong,

    /// Provider rejected the request shape.
    Invalid,

    /// The provider refused the account.
    Refused,
    /// The provider will not take a request field.
    Unsupported,
    /// Provider rejected the credential.
    Unauthorized,

    /// Provider rate allowance requires the retained cooldown.
    RateLimited {
        /// Exact lower cooldown, without scheduling or recovery policy.
        retry_after: Duration,
    },

    /// Provider account allowance requires the retained cooldown.
    Exhausted {
        /// Exact lower cooldown, without scheduling or recovery policy.
        retry_after: Option<Duration>,
    },
}

/// Actual transport evidence retained alongside a neutral completion failure.
/// No domain infers this from an error label or from requested cancellation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum CompletionEvidence {
    /// The lower proves no request bytes were sent.
    Unsent,

    /// The request may have reached the peer; outcome is unknown.
    Unknown,

    /// A peer response, including a refusal, was received.
    Response { status: Option<u16> },
}

/// Bounded provider evidence, kept outside history and content-free facts (domain/run.md, section 10).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Report {
    pub code: Box<[u8]>,
    pub request: Box<[u8]>,
    pub field: Box<[u8]>,
}

impl Report {
    /// Three provider fields, each at the vocabulary's 128-byte bound (protocol/limits.md, 2.4).
    pub const BYTES: u64 = 3 * 128;

    /// No provider evidence was supplied.
    #[must_use]
    pub fn none() -> Self {
        Self { code: Box::default(), request: Box::default(), field: Box::default() }
    }
}

/// What kept an LLM from going on.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Fault {
    /// Full neutral failure and evidence after session retry policy.
    /// This is distinct from local model stopping rules and requested run Cancel.
    Completion {
        /// Exact content-free shared-client classification and cooldown.
        failure: CompletionFailure,

        /// Exact transport evidence, including across interrupted Delivery.
        evidence: CompletionEvidence,
        /// Charter LLM index, resolved by `Charter::model`.
        model: u32,
        attempts: u32,
        report: Report,
    },

    /// The program source lost its login, ending the run for its account.
    Unauthorized { account: u32 },
    /// The backend program failed, with its bounded standard error tail.
    Program { name: Box<[u8]>, exit: Option<u32>, detail: Box<[u8]> },
    /// The account spent its provider allowance, ending the run.
    Limited { account: u32, reset_at: Option<Time> },
    /// The provider refused the account, ending the run with its bounded code.
    Refused { account: u32, code: Box<[u8]> },
    /// Its provider failed for good: unreachable, overloaded past the
    /// retries, or refusing the call or its credentials.
    Provider,
    /// It kept declining to answer.
    Declined,
    /// It kept running out of tokens mid-answer.
    Truncated,
    /// It kept asking for tools and naming none.
    Malformed,
}

/// The answer to a `Start`.
#[derive(PartialEq, Eq, Hash, Debug)]
pub enum Answer {
    /// Main waited or replied in Open and this activation parked; all terminal rights have closed.
    Parked {
        /// Actual cumulative run token usage.
        spent: Spend,
        /// Number of main turns emitted in this activation.
        turns: u32,
    },
    /// Refused at the entrance: nothing was done.
    ///
    /// A run is refused at its own entrance, before `Admitted`; or after
    /// `Admitted`, when its main conversation is refused at the
    /// conversations' entrance. The run then did nothing but look in its
    /// checkout: `Busy` says the agent had no room for the conversation, and
    /// a later retry may find some; `Invalid(Conversation)` that its opening
    /// does not fit the conversations' limits, which a retry will not change.
    Refused(Refusal),
    /// The run finished with `outcome`, having spent `spent`. A change has
    /// been checked and delivered by the host.
    Accepted {
        /// Declared or accepted result, validated against the host's contract.
        outcome: Declared,
        /// Accepted cumulative usage across the enclosing run or session.
        spent: Spend,
        /// Actual main turns emitted in this activation before this terminal.
        turns: u32,
    },
    /// The run ended without an outcome, having spent `spent`.
    Failed {
        failure: Failure,
        /// Accepted cumulative usage across the enclosing run or session.
        spent: Spend,
        /// Actual main turns emitted in this activation before this terminal.
        turns: u32,
    },
}

/// Why a run was refused.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// No room for another run, or for its conversation.
    Busy,
    /// The charter does not fit the limits.
    Invalid(Invalid),
}

/// What about a charter does not fit the limits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Invalid {
    /// Rendered charter text, generated schemas and guides exceed this host part.
    HostPart { bound: u64 },
    /// A charter model leaves less room than startup derived.
    Room { bound: u64 },
    /// A start's messages exceed their bounds or repeat a name.
    Messages,
    /// The agent does not read this charter version; the protocol refuses it before domain admission.
    CharterVersion,
    /// The charter does not decode in a version the agent reads; protocol refuses before domain admission.
    MalformedCharter,
    /// The charter names a main or sub-agent endpoint absent from agent configuration.
    Endpoint,
    /// A host Start with activation zero is refused before the run opens.
    Activation,
    /// The acknowledgement window cannot hold one largest permitted turn.
    Window,

    /// A host convention path is empty, oversized, absolute or has unsafe
    /// components/bytes. The Start is refused before admission or any effects.
    Conventions,

    /// It holds more bytes than a run may.
    TooLarge,
    /// Present workspace is empty, exceeds directory/name/conflict/path caps,
    /// repeats a name/root/path, uses unsafe names or relative paths, or supplies
    /// conflicts for a plain directory. Delivery-capable starts also bound directory
    /// ordinals and require a positive receiving delivery timeout.
    Workspace,
    /// The host declarations exceed count/storage bounds, repeat/reserve a name,
    /// have empty opaque fields or a zero deadline, or the receiving retry limits are invalid,
    /// or a grant this run cannot use (protocol/programs.md, section 2).
    Grants,
    /// The result contract permits no form, exceeds the verdict count cap,
    /// declares invalid or duplicate names/caps/ranges, or its smallest
    /// accepted value for an allowed form cannot fit aggregate ownership.
    Outcome,
    /// The host's budget was refused before effects, naming its part and bound.
    Budget(crate::BudgetRefusal),
    /// A model is unpriced at this rate, refused before effects (domain/run.md, section 9).
    Price(crate::Rate),
    /// A model is undeclared, its effective window/output are zero or above its
    /// declaration, its spare is negative, or the charter repeats a model or exceeds its model count.
    Llm,
    /// The requested effort is outside the model’s declared values.
    Effort,
    /// The main conversation was refused: its opening does not fit the
    /// conversations' limits.
    Conversation,
}

/// Why a run ended without an outcome: what the host acts on.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    /// Exact transient history refusal; never silently starts fresh.
    Transcript(
        /// Session admission classification, translated exhaustively by root.
        TranscriptRefusal,
    ),

    /// The LLM could not do the work.
    Model(Fault),
    /// The run's budget ran out.
    Budget {
        exhausted: Exhausted,
        /// Turns, host spend units, or milliseconds of run wall time; absent for receiving/overflow failures.
        used: Option<u64>,
        bound: Option<u64>,
    },
    /// The LLM did not keep to the run's rules.
    Policy(Policy),
    /// The host cancelled the run.
    Cancelled,
    /// The host's delivery context moved: no later delivery from this run can
    /// land. The host decides what context a new run receives.
    Stale,
}

/// The run's rules, as the LLM broke them.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Policy {
    /// It kept stopping without finishing, through `nudges` nudges, having
    /// called `finish` with `rejected` outcomes that were refused.
    Unfinished {
        /// Number of run nudges already given after unfinished turns.
        nudges: u32,
        /// Number of declared outcomes rejected by the charter contract.
        rejected: u32,
    },
}

/// Exact concrete-history entrance classification, independent of the sibling type.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum TranscriptRefusal {
    /// Unsupported record version.
    Version,

    /// Invalid concrete record structure.
    Malformed,

    /// History retains a live ticket.
    Unresolved,

    /// Receiving ownership/count cap is incompatible.
    TooLarge,
}
