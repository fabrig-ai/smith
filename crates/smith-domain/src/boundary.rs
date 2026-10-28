//! The records that cross the boundary with the protocol layer (programming-model.md, section 4.4;
//! domain/run.md, sections 2, 3, 5, 6, 8, 9, 10 and 13;
//! domain/session.md, sections 3–6). The
//! domain defines them; the protocol crate depends on it.
//!
//! Three peers are behind it, and each record names which by its variant:
//!
//! - The host, the run child domain's face. A [`Event::Start`] is a call,
//!   answered by exactly one [`Request::Answer`], after a
//!   [`Request::Admitted`] that names the run if it was admitted, so that a
//!   [`Event::Cancel`] can name it. A run's host call, [`Request::Deliver`], is
//!   ended by exactly one [`Event::Delivered`]. After submission the
//!   host operation is never abandoned; its deadline bounds the host terminal.
//!   Duplicate or stale callback owners are inert (domain/run.md, sections 8 and 10).
//!   [`Request::Long`] and [`Request::LongDone`] bracket actual checks,
//!   provider completions and commands, from issuance to their terminal.
//! - LLM providers, for the sessions: a [`Request::Complete`] is ended by one
//!   of [`Event::Completed`], [`Event::Failed`] or, after a
//!   [`Request::Cancel`] that won its race, [`Event::Cancelled`]. Its prompt
//!   and its completion are in the conversation vocabulary ([`crate::llm`]):
//!   a prompt's messages hold text, tool calls sent back as the LLM wrote
//!   them, and their results (the tools' outcome, the run's answer to a tool
//!   it serves, the problem of a call that is none, or not run); a
//!   completion holds text and tool calls, each decoded into a call to the
//!   session's own tools, an ask of a tool the run serves, or a problem.
//! - io, in two families of records for now: the file and process operations
//!   of the sessions' tools, as the tools define them (a [`Request::Io`] is
//!   ended by [`Event::Done`], with `Cancelled` if a [`Request::CancelIo`]
//!   won its race), and the run's own looks in its checkout and
//!   its checks (a [`Request::Read`] by [`Event::Read`], a [`Request::Probe`]
//!   by [`Event::Probed`], a [`Request::Check`] by [`Event::Checked`], or
//!   after a [`Request::Abort`] by [`Event::Aborted`] if the abort won). They
//!   share a shape (a command run as a contained process, in a root, with a
//!   deadline, is a check for the run and a shell call for the tools), and
//!   become one vocabulary once the io layer is designed. Every io request
//!   carries its deadline, and io runs the race (programming-model.md, section 5.3).
//!
//! A request's `owner` is the token of whoever asked, echoed on its terminal
//! event. Tokens of different families may be equal: the variant routes a
//! terminal to the child domain that asked.

use skein_lib::{Duration, ReplyTo, Time, Token};
use smith_domain_run as run;
use smith_domain_tools as tools;

use crate::llm::{Completion, Failure, Prompt};
use alloc::boxed::Box;

/// Host credit for unacknowledged turns in one activation (protocol/channel.md, sections 3 and 7).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Window {
    /// Maximum turns awaiting acknowledgement.
    pub turns: u32,
    /// Maximum owned concrete message and payload bytes of those turns.
    pub bytes: u64,
}

/// One durable host decision made after the last saved turn.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct AnsweredCall {
    /// The call's stable name across activations.
    pub name: run::CallName,
    /// The host-declared tool the earlier call named.
    pub tool: Box<[u8]>,
    /// The host's answer, in the same vocabulary as a live terminal.
    pub answer: Answered,
}

/// A host decision to describe to the resumed run.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Answered {
    /// The host answered a tool with result or error text.
    Host(run::HostAnswer),
    /// The host answered a delivery in smith's delivery vocabulary.
    Delivery(Box<run::Delivery>),
    /// The host decided the call, but its answer exceeded the receiving bound.
    TooLarge,
}

/// A credential name, never its value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct GrantName {
    /// Configured credential account, compared as an opaque numeric name.
    pub account: u32,
    /// Credential generation, echoed unchanged so stale refreshes cannot replace newer credentials.
    pub generation: u64,
}

/// Credential metadata supplied by the host; the domain keeps its name, lifetime and renewal policy.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Grant {
    /// Host-supplied numeric credential account and generation; no secret credential bytes.
    pub name: GrantName,
    /// Remaining monotonic validity of the granted credential.
    pub valid: Duration,
    /// False for a key, which its host never renews (domain/session.md, section 4).
    pub renewable: bool,
}

/// Typed host, provider and IO inputs routed to the owned children. Start yields
/// one host answer; each lower terminal echoes the owner of its pending request.
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// The program bridge supplies the file version at a native allowance or write result.
    NativeVersion { owner: Token, at: tools::Place, version: Option<tools::Version> },
    /// Actual terminal for one host relay attempt; old callbacks are inert.
    HostReturned {
        /// Live attempt identity, distinct from durable operation scope.
        relay: run::RelayName,
        /// Bounded host text or settled retry classification.
        reply: run::HostReply,
    },
    /// From the host, a call: start a run on `charter`, and answer once it
    /// has ended. `host_run` is the host's name for the run, echoed on
    /// `Admitted`.
    Start {
        /// Single-use right to answer this call; exactly one terminal consumes it.
        reply_to: ReplyTo,
        /// Parent-supplied stable logical host-run scope, preserved across relay
        /// recovery and restarted activations; distinct from live run/callback tokens.
        host_run: Token,
        /// Host-supplied positive number, unique for each activation of `host_run`.
        activation: u64,
        /// Host identity across activations, or absent to draw one at admission.
        identity: Option<run::Identity>,
        /// Credit for turns awaiting durable host acknowledgement.
        window: Window,
        /// Triggering messages admitted before any later live input.
        messages: Box<[run::Message]>,
        /// Host-supplied admission policy, validated before the run starts.
        charter: run::Charter,
        /// Optional immutable host mounts and initial conflicts, moved to run admission.
        workspace: Option<run::Workspace>,
        /// Typed V2 turns, kept without assembling provider messages.
        /// False charter.resume ignores it; semantic refusal never starts fresh.
        /// Receiving record/count caps are checked before root retention.
        transcript: Option<smith_domain_session::record::Transcript>,
        /// Host answers the saved transcript does not hold.
        answered: Box<[AnsweredCall]>,
        /// Host credential-name and remaining-validity notices, bounded by `Limits.accounts`.
        /// These select a usable credential generation; they grant no checkout or tool authority.
        grants: Box<[Grant]>,
    },

    /// Parent-labelled live message, FIFO and bounded before retention by
    /// `Limits.run.messages` and `Limits.run.message_bytes`. Accepted messages
    /// emit no separate admission terminal; the read fence reports consumption.
    Message {
        /// Admitted Open messages request a final reply; Finite ignores this bit.
        finish_requested: bool,
        /// Admitted live run handle.
        run: Token,

        /// Active-run unique opaque name, including zero.
        name: Token,

        /// Sender label rendered by the run before admission.
        label: Box<[u8]>,

        /// Attested UTF-8 text; label, separator and text share the rendered-byte cap.
        text: Box<[u8]>,
    },

    /// The host durably kept this turn and every preceding turn of the run.
    Acknowledge { run: Token, turn: u32 },

    /// A refreshed credential, pushed by the engine through the host.
    Grant {
        /// Credential name or refresh notice; its secret value stays below the domain.
        grant: Grant,
    },
    /// From the host: end the run `run` as cancelled. A run that has already
    /// answered, or decided how it ends, ignores it.
    Cancel {
        /// Root-issued admitted run token, supplied in `Request::Admitted` and echoed by cancellation.
        run: Token,
    },
    /// Actual host terminal for `Deliver`, including while shutdown settles.
    /// Constructor-sealed owned evidence is revalidated against admitted writable
    /// mounts; stale callback generations are inert.
    Delivered { owner: Token, delivery: run::Delivery },

    /// The program reports a built-in call's result after its completion (protocol/programs.md, 5.5).
    BuiltinResult { owner: Token, id: Box<[u8]>, text: Box<[u8]>, error: bool },
    /// Terminal for `Complete`: the LLM produced its next message.
    Completed {
        owner: Token,
        /// Provider completion, validated and charged once before its tools run.
        completion: Completion,
    },
    /// Terminal for `Complete`: the call produced no message.
    Failed {
        owner: Token,
        failure: Failure,
        report: crate::llm::Report,

        /// Exact content-free transport evidence carried by the terminal.
        evidence: crate::llm::Evidence,

        /// Bounded exact shared-client diagnostic, consumed and dropped by policy.
        /// It never controls text-based retry decisions or enters saved history.
        detail: Box<[u8]>,
    },
    /// Terminal for `Complete`, after `Cancel`: the call was abandoned.
    Cancelled { owner: Token },
    /// Terminal for `Io`.
    Done { owner: Token, done: tools::Done },
    /// Terminal for `Read`.
    Read {
        owner: Token,
        /// IO's one terminal for the run's pending text read.
        read: run::Read,
    },
    /// Terminal for `Probe`: whether an executable file is at the place. A
    /// failure or a deadline passed reads as not.
    Probed {
        owner: Token,
        /// Whether IO found an executable within the named root before its deadline.
        executable: bool,
    },
    /// Terminal for `Check`: what the checks' process did.
    Checked {
        owner: Token,
        /// Checks' process terminal, including its bounded diagnostic tail.
        ran: run::Ran,
    },
    /// Terminal for `Check`, after `Abort`: the checks were stopped.
    Aborted { owner: Token },
}

/// Owned host notices and calls, provider calls and IO operations emitted by
/// the root. Calls require the typed terminal documented on their variant.
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    /// A host message refused at ingress; this is its one terminal.
    MessageRefused { host_run: Token, name: Token, reason: run::MessageRefusal },

    /// Settled main wait and yield with empty inbox; wall time keeps running.
    /// This observation to the parent owes no terminal; Start's reply right
    /// remains pending until the run's final Answer.
    Waiting {
        /// Stable parent run scope.
        host_run: Token,

        /// Latest name consumed by a turn.
        read: Option<Token>,
    },

    /// Actual main turn; caller owns durable payload and host ACK metadata has
    /// its separate existing owner. Emitted before the final Answer.
    Turn {
        /// Stable parent run scope.
        host_run: Token,

        /// One-based output number within this activation.
        number: u32,

        /// One-based place in the conversation, including restored turns.
        position: u32,

        /// First historical turn or a later context window; hosts keep this with the body.
        opens_window: bool,

        /// Latest message consumed by this turn.
        read: Option<Token>,

        /// Cumulative token usage, independently enforced from record prices.
        spent: run::Spend,

        /// Full concrete record including replay and terminal results.
        turn: smith_domain_session::record::Turn,
    },

    /// Opaque declared main host tool, forwarded without interpreting its input.
    /// One terminal is owed per attempt; durable decisions replay by name.
    HostCall {
        /// Stable logical run scope supplied by Start.
        host_run: Token,
        /// One live relay attempt.
        relay: run::RelayName,
        /// Immutable accepted-transcript operation name.
        name: run::CallName,
        /// Exact declared tool name.
        tool: Box<[u8]>,
        /// Checked declaration effect, used for session scheduling.
        effect: run::HostEffect,
        /// Complete bounded attested object bytes, unchanged.
        input: run::HostInput,
        /// Per-relay bounded deadline; withdrawal still owes its terminal.
        deadline: Time,
    },
    /// Ask the host to settle its live relay, retaining the original terminal.
    /// Submitted delivery has no such cancellation operation.
    WithdrawHost {
        /// The live relay attempt, echoed by its `HostReturned` terminal.
        relay: run::RelayName,
    },
    /// To the host: the run it names `host_run` was admitted, and is `run`
    /// from now on.
    Admitted {
        host_run: Token,
        /// Root-issued admitted run token, supplied in `Request::Admitted` and echoed by cancellation.
        run: Token,
    },
    /// To the host, the answer to a `Start`: exactly one per start.
    Answer {
        /// Single-use reply right returned to the layer that issued it.
        to: ReplyTo,
        /// Single terminal value returned to the caller.
        answer: run::Answer,
        /// Main conversation's last diagnostic, for the operator.
        detail: Box<[u8]>,
        /// Last message covered by an actual told main turn; none before any read.
        read: Option<Token>,
    },
    /// To the host: bounded work of `host_run` began; one `LongDone` ends it.
    Long { host_run: Token, operation: Operation, until: Time },
    /// To the host: one previously announced operation has actually ended.
    LongDone { host_run: Token },
    /// To the host, deliver the exact checked writable-directory state under
    /// the logical run named `host_run`. The host interprets the unchanged
    /// generic fields and returns its bounded host terminal. Final Change
    /// and separately granted main delivery share the same exclusive checks;
    /// the terminal remains owed through shutdown.
    Deliver {
        /// Durable transcript-derived host call name, scoped by the logical host run.
        /// Callback `owner` is separate; retries of this operation reuse this name.
        name: run::CallName,
        /// Bounded host-operation deadline; the host supplies exactly one
        /// terminal even during shutdown. The run never abandons submission.
        deadline: Time,
        host_run: Token,
        owner: Token,
        /// Declared checkout change with validated host-named fields and bounded aggregate ownership, forwarded unchanged.
        change: run::outcome::Change,
    },

    /// Ask an LLM for its next message, ended by one completion or call terminal.
    Complete {
        /// Session's opaque token, shared with its provider terminals and captured content.
        owner: Token,
        /// Session-owned completion identity; emitted even when no content follows.
        name: crate::llm::CompletionName,
        /// Credential name or refresh notice; its secret value stays below the domain.
        grant: Option<GrantName>,
        /// Complete provider-neutral conversation snapshot from the root, bounded by session ownership limits.
        prompt: Prompt,
        /// Model progress deadlines and the enclosing session's expiry.
        deadlines: crate::session::Deadlines,

        /// Maximum owned translated completion bytes, including block cells,
        /// replay envelopes and decoded calls. The adapter verifies its configured
        /// bound before preparing the provider request; terminals obey it.
        max_completion_bytes: u64,

        /// Maximum translated completion blocks, reserved with result skeletons
        /// before this request. One terminal remains owed after Cancel.
        max_completion_blocks: u32,

        /// Maximum exact shared-client failure diagnostic bytes. The adapter
        /// verifies compatibility before prepare; policy consumes the         /// terminal and drops detail without retaining text in facts/history.
        max_failure_bytes: u32,

        /// Aggregate decoded application-call ownership permitted in this
        /// completion, included by the adapter in its full translated byte bound.
        decoded_call_bytes: u64,
    },
    /// Provider authentication rejected this exact credential generation; a notice
    /// to the host, with no terminal owed. Repeated rejection of that generation is suppressed.
    Unauthorized {
        /// Credential name or refresh notice; its secret value stays below the domain.
        grant: GrantName,
    },
    /// The provider reports its account allowance spent.
    Limited {
        /// Configured credential account, compared as an opaque numeric name.
        account: u32,
        /// The reset, absent when the provider did not say.
        reset: Option<Duration>,
    },
    /// The provider refused an account; its diagnostic reason is not conversation content.
    Refused { account: u32, code: Box<[u8]>, reason: Box<[u8]> },
    /// Abandon the `Complete` in flight for `owner`. Its terminal event still
    /// comes: `Cancelled`, or whichever outcome won the race.
    Cancel { owner: Token },
    /// End the owner's outstanding completion at the backend's next boundary,
    /// delivering what it holds as that completion. A backend that cannot end
    /// it early lets it complete normally. No terminal of its own is owed;
    /// an owner with no outstanding completion is ignored.
    Yield { owner: Token },
    /// Ask io for `op` for a session's tools, giving up at `deadline`.
    Io {
        owner: Token,
        /// Typed IO operation carrying only the authority and bounds required for it.
        op: tools::Op,
        deadline: Time,
    },
    /// Abandon the `Io` in flight for `owner`. Its terminal event still comes:
    /// `Done` with `Cancelled`, or whichever outcome won the race.
    CancelIo { owner: Token },
    /// Read or stop a machine-held command; ends in one `Done`.
    Job { owner: Token, act: tools::JobAct },
    /// Read the first `max` bytes of the regular file at `at`, following
    /// symbolic links within its root, giving up at `deadline`.
    Read {
        owner: Token,
        at: run::Place,
        /// Requested retained UTF-8 file-byte cap; the IO terminal reports whether the whole file fits.
        max: u32,
        deadline: Time,
    },
    /// Find out whether an executable file is at `at`, giving up at
    /// `deadline`.
    Probe { owner: Token, at: run::Place, deadline: Time },
    /// Run the executable at `program`, in its repository's root, as a
    /// contained process, stopping it at `deadline`; keep the last `tail`
    /// bytes of what it writes.
    Check {
        owner: Token,
        /// IO-confined check executable, run as a contained process tree.
        program: run::Place,
        deadline: Time,
        /// Last output bytes retained under the caller's cap.
        tail: u32,
    },
    /// Stop the `Check` in flight for `owner`. Its terminal still comes:
    /// `Aborted`, or `Checked` if the checks ended first.
    Abort { owner: Token },
}

/// Work below the run, reported to its host with its own deadline
/// (domain/host.md, section 4; protocol/channel.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Operation {
    /// The run reports its checks before a delivery; their terminal ends this span.
    Checks,
    /// The root reports an issued provider completion; its actual terminal ends this span.
    Completion,
    /// The root reports an issued session command; its actual IO terminal ends this span.
    Command,
}
