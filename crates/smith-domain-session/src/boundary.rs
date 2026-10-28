//! The records that cross the boundary with the session's parent, the
//! root domain (programming-model.md, section 4.5), which routes them to and from the protocol layer and
//! the session's opener. The session defines them; its parent depends on it.
//! Contracts: domain/session.md, sections 3–7.
//! This module retains no runtime state and knows no provider wire format,
//! host policy or checkout contents; [`Event`] and [`Request`] are the typed
//! entrances and exits for the stateful session module.
//!
//! Two shapes cross it. A session's lifecycle, with its opener: an
//! [`Event::Open`] is answered by exactly one [`Request::Ended`], after an
//! [`Request::Opened`] that names the session if it was admitted, and any
//! number of [`Request::Yielded`] and [`Request::Used`] in between; the opener
//! addresses the session by that name, and every record back carries the
//! opener's token (programming-model.md, section 4.2). Each request has one
//! terminal event: a [`Request::Complete`] is ended by one of [`Event::Completed`],
//! [`Event::Failed`] or [`Event::Cancelled`], or by [`Event::BudgetDenied`]
//! when the root withholds it before any provider lease, or [`Event::UnsentClosed`]
//! when the run closes before publication; a [`Request::Delegate`], to the
//! opener, by [`Event::Answered`], [`Event::AnsweredAndYield`],
//! [`Event::AnswerWithheld`] or [`Event::AnswerCancelled`]; a
//! [`Request::Io`], the tools' file and process operations passed on as they
//! are, by [`Event::Done`]. A request's `owner` is echoed on its terminal
//! event: the session's token for a call to the LLM, a tool run's own for a
//! delegated call, and the tools' own for an operation.

use alloc::boxed::Box;

use skein_lib::{Duration, Time, Token};
use smith_domain_tools as tools;

use crate::llm::{Completion, Descriptor, Endpoint, Failure, Prompt, Usage};

/// parent -> session
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// A native program snapshot belongs to this session; it changes no conversation text.
    NativeVersion { session: Token, at: tools::Place, version: Option<tools::Version> },
    /// Parent admission of bounded concrete history, including a fresh session.
    /// Ends exactly once after all provider, tools and delegated work settles.
    Open {
        opener: Token,
        /// Parent-owned spec, prices and optional concrete history. Admission
        /// checks receiving caps, transcript identity and provider/result space
        /// before any effect; refusal returns one `Ended` without `Opened`.
        /// Exactly one fixed `Opening` node is owned in transit, in addition
        /// to its bounded payload/envelopes. The entry consumes its ownership
        /// as admission validates and moves content; `worst_case` separately
        /// counts the node while it may coexist with admission state.
        opening: Box<crate::record::Opening>,
    },
    /// Parent terminal for Delegate, possibly winning a Withdraw race. Its
    /// concrete payload consumes the result space reserved before that call.
    /// The current live identity charges the child bill once; stale repeats
    /// neither retain bytes nor change own or inclusive pricing.
    Answered {
        owner: Token,
        /// Parent-owned result bytes, at most `Limits::delegated_result_bytes`.
        /// Moved into concrete history even while closing; no answer ticket remains.
        text: Box<[u8]>,
        /// Whether the returned tool result represents a failure.
        error: bool,
        /// Child activation's inclusive cumulative bill, in deployment units;
        /// historical spend is excluded. Only the inclusive parent prefix changes.
        spent: u64,
    },
    /// Parent terminal for Delegate that yields after the settled tool turn, so its opener can append work.
    AnsweredAndYield {
        owner: Token,
        /// Complete result bytes within the previously reserved delegated result cap.
        text: Box<[u8]>,
        error: bool,
        /// Inclusive child activation bill, charged once by the live identity.
        spent: u64,
    },
    /// Parent terminal for its own parked wait; records the real call without supplying a result.
    AnswerWithheld { owner: Token },
    /// Parent terminal acknowledging Withdraw. Its current delegated identity
    /// settles exactly once and releases its reserved result space. A withdrawn
    /// child still reports its activation bill; stale repeats remain inert.
    AnswerCancelled {
        owner: Token,
        /// Child activation's inclusive cumulative bill, in deployment units;
        /// historical spend is excluded. Only the inclusive parent prefix changes.
        spent: u64,
    },
    /// A new user message for a yielded session, which calls the LLM again.
    /// Sent only while the session is yielded; a `session` that has ended
    /// meanwhile is dropped.
    Continue {
        session: Token,
        /// Owned content blocks or file bytes, bounded by the enclosing record's byte cap.
        content: Box<[u8]>,
    },
    /// End the session, whatever its state: it cancels what is in flight, and
    /// ends once that has settled. A `session` that has ended is dropped.
    Close { session: Token },
    /// Cancel the session's jobs; `JobsCancelled` follows their settlement.
    CancelJobs { session: Token },
    /// The run asks this session to yield its outstanding Complete; its terminal remains owed.
    Yield { session: Token },
    /// The program reports a built-in call's result after its completion (protocol/programs.md, 5.5).
    BuiltinResult { session: Token, id: Box<[u8]>, text: Box<[u8]>, error: bool },
    /// Terminal for `Complete`: the LLM produced its next message.
    Completed {
        owner: Token,
        /// Provider completion, validated and charged once before its tools run.
        completion: Completion,
    },
    /// The parent received a completion whose checked run-wide charge or usage
    /// cannot be added. Its content and calls are discarded before any effect.
    Overflowed {
        /// The pending provider request whose terminal was received.
        owner: Token,
        /// Exact typed arithmetic failure, either price or usage overflow.
        end: End,
    },
    /// The root denied this requested completion before publishing it to a
    /// provider. Only its current Calling owner releases credit and ends the
    /// session, after kit settlement. Stale or duplicate denials are inert;
    /// no cancellation, retry, usage or Turn is produced.
    BudgetDenied {
        /// Session-issued pending completion owner, echoed without a provider lease.
        owner: Token,
        /// Exact exhausted root allowance, mapped to the settled Budget terminal.
        reason: BudgetDenial,
    },
    /// The root closed the run before publishing this requested completion.
    /// Only the current Calling owner releases its reservation and settles
    /// Closed after the kit closes. Stale, duplicate and already-closing
    /// events are inert; a provider terminal remains owed once started.
    /// No cancellation, retry, usage or Turn is invented.
    UnsentClosed {
        /// Session-issued pending completion owner, without a provider lease.
        owner: Token,
    },

    /// Settles an unsent Complete, appends the note and asks with this choice from now on.
    Narrowed {
        owner: Token,
        note: Box<[u8]>,
        choice: crate::llm::ToolChoice,
        /// Concrete names retained in not-run results; none when no call is allowed.
        names: Option<Box<[Box<[u8]>]>>,
    },
    /// Terminal for `Complete`: the call produced no message.
    Failed {
        owner: Token,
        /// Whether an actual unauthorized call may recover; unsent calls remain recoverable.
        retry_ok: bool,
        failure: Failure,

        /// Exact content-free transport evidence carried by the terminal.
        evidence: crate::llm::Evidence,

        /// Bounded exact shared-client diagnostic, consumed and dropped by policy.
        /// It never controls text-based retry decisions or enters saved history.
        detail: Box<[u8]>,
    },
    /// Terminal for `Complete`, after `Cancel`: the call was abandoned.
    Cancelled { owner: Token },
    /// Terminal for `Io`, for the tools.
    Done { owner: Token, done: tools::Done },
}

/// session -> parent
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    /// The session for `opener` was admitted, and `session` names it from now
    /// on.
    Opened { opener: Token, session: Token },
    /// The opener's requested cancellation has settled every running job.
    JobsCancelled { opener: Token },
    /// A settled turn, copied at emission. Its values contain no session tickets.
    Turn {
        opener: Token,
        /// Settled concrete transcript turn handed to the opener.
        turn: crate::record::Turn,
    },
    /// The cumulative deployment-unit spend, including child calls.
    Priced {
        opener: Token,
        /// This activation's exact inclusive subtree charge. Historical turns
        /// do not seed it; own completions and delegated terminal bills do.
        spent: u64,
        /// Cumulative price of this activation's own completions only. The
        /// root counts its checked deltas; delegated bills never change it.
        own_spent: u64,
    },
    /// The LLM stopped calling tools, saying `text` (its message's text blocks,
    /// one after another). The session waits for `Continue` or `Close`, and
    /// its time budget keeps running.
    Yielded { opener: Token, stop: Yield, text: Box<[u8]> },
    /// A completion came back: one turn, and `usage` as the provider counts
    /// it. One per completion, the ones that win a race with a cancel
    /// included.
    Used { opener: Token, usage: Usage },
    /// The session for `opener` has ended, after `turns` completions that used
    /// `usage`, preserving unavailable cumulative counts: exactly one
    /// per `Open`, once nothing the session asked for is in flight.
    Ended {
        opener: Token,
        /// Terminal classification after everything started beneath this entity has settled.
        end: End,
        /// Completion count, bounded across the enclosing run or session.
        turns: u32,
        /// Exact cumulative raw usage of charged completions.
        usage: Usage,
        /// Sums of supplied counts for the parent's bookkeeping, absent until
        /// reported at least once. This is never a raw provider observation.
        reported: Usage,
        /// Last bounded provider diagnostic; never part of the model history.
        detail: Box<[u8]>,
    },
    /// Ask an LLM for its next message, ended by one completion or call terminal.
    Complete {
        owner: Token,
        /// Session-owned completion identity; emitted even when no content follows.
        name: crate::llm::CompletionName,
        /// First user message supplied by the opener.
        prompt: Prompt,
        /// Model progress deadlines and the enclosing session's expiry.
        deadlines: Deadlines,

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
    },
    /// Abandon the `Complete` in flight for `owner`. Its terminal event still
    /// comes: `Cancelled`, or whichever outcome won the race.
    Cancel { owner: Token },
    /// End the owner's outstanding completion at the backend's next boundary,
    /// delivering what it holds as that completion. A backend that cannot end
    /// it early lets it complete normally. No terminal of its own is owed;
    /// an owner with no outstanding completion is ignored.
    Yield { owner: Token },
    /// Ask io for `op` for the tools, giving up at `deadline`: io runs the
    /// race (programming-model.md, section 5.3).
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
    /// Ask the opener to serve the delegated call `call`, a ticket, and to
    /// answer it by `deadline`, when the session's time runs out: the opener
    /// runs the race, within limits of its own if it has shorter ones, and
    /// answers a call that loses as a failure.
    Delegate {
        /// Concrete transcript origin supplied by the session, preserved by the opener.
        /// It is fixed-size; its sequence is checked before any delegated effect.
        origin: crate::record::Origin,
        owner: Token,
        opener: Token,
        /// Typed checkout-tool call decoded below the domain.
        call: Token,
        deadline: Time,
    },
    /// Abandon the `Delegate` in flight for `owner`, as the session closes.
    /// Its terminal event still comes: `AnswerCancelled`, or `Answered` if
    /// the answer won the race.
    Withdraw { owner: Token },
}

/// What a session is opened for.
#[derive(PartialEq, Eq, Debug)]
pub struct Spec {
    /// Transport named in this conversation's opening turns (session.md 8.2).
    pub backend: crate::record::Backend,
    /// Account named by the charter, absent for uncredentialed calls.
    pub account: Option<u32>,
    /// A child's final answer; main's run supplies its own finishing prompts.
    pub final_turn: Option<FinalTurn>,
    pub endpoint: Endpoint,
    /// The opener's affinity; a resumed session keeps its transcript's instead.
    pub affinity: crate::llm::Affinity,
    /// Measured fixed/provider and generated-contract bytes absent from domain text.
    pub protocol_allowance: u64,
    pub progress: Progress,
    /// The provider's name for the model.
    pub model: Box<[u8]>,
    /// Reasoning effort requested by this activation, absent for the provider default.
    pub effort: Option<Box<[u8]>>,
    /// System instructions supplied by the opener, retained within the enclosing byte cap.
    pub system: Box<[u8]>,
    /// What the session's own tools may do and where (the families the LLM
    /// may call among them, the checkout's repositories, what commands run
    /// with), copied into its kit; and the tools its opener serves.
    pub authority: tools::Authority,
    /// Opener-owned tool descriptors; tickets are resolved only by the opener.
    pub delegated: Box<[Descriptor]>,
    /// The first user message.
    pub prompt: Box<[u8]>,
    /// The model's usable context window in tokens.
    pub window: u32,
    /// Run-derived largest ordinary prompt in tokens.
    pub line: u64,
    /// Run-owned instruction supplied to each compaction completion.
    pub summary: Box<[u8]>,
    /// The run's text introducing the summary in a new window.
    pub preamble: Box<[u8]>,
    /// The most tokens each answer may take, and fewer once the output budget
    /// has less left.
    pub output: u32,
    /// Allowance supplied at admission, checked before starting more work.
    pub budget: Budget,
}

/// Opener-supplied child wrap-up note and allowance (domain/session.md, section 7).
#[derive(PartialEq, Eq, Debug)]
pub struct FinalTurn {
    /// Appended once after the latest settled tool results.
    pub note: Box<[u8]>,
    pub allowance: Duration,
}

impl FinalTurn {
    /// Whether the final sequence is due, counting a compaction when its window needs one.
    #[must_use]
    pub fn due(&self, turns: u32, time: Duration, compaction: bool) -> bool {
        let completions = if compaction { 2 } else { 1 };
        let time_needed = if compaction { self.allowance.saturating_mul(2) } else { self.allowance };
        turns <= completions || time <= time_needed
    }
}

/// A model's progress deadlines, supplied by the root on session admission.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Progress {
    /// From request sent to the first response byte.
    pub head: Duration,
    /// Largest gap between response events.
    pub idle: Duration,
}

/// One call's progress bounds and the session's absolute expiry.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Deadlines {
    pub head: Duration,
    pub idle: Duration,
    /// Earlier of the session's own and its run's expiry.
    pub whole: Time,
}

/// What a session may spend, from the moment it opens. Every dimension is
/// within the session's `Limits`, or the spec is refused.
///
/// A session starts a completion only while it has turns, input and output
/// tokens left (what it spent is below the budget), its cache reads and writes
/// are within their budget, and its `time` has not run out; when it needs one
/// it may not start, it ends. A completion's tokens are known only once it
/// comes back, so it may take input and cache tokens past their budget: its
/// turn still runs the tools it asked for, and the session ends where it
/// would have started the next. The output budget it cannot pass, as the
/// answer's output cap is cut to what is left. A zero cache budget
/// therefore ends a session only once a completion touches the cache. Time
/// does not wait for the turn: when it runs out, the session closes at once.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Budget {
    /// Completion count, bounded across the enclosing run or session.
    pub turns: u32,
    /// Fresh input-token allowance or accepted count, as the provider reports it.
    pub input: u64,
    /// Output-token allowance or accepted count, as the provider reports it.
    pub output: u64,
    /// Cache-read token count or allowance as the provider reports it.
    pub cache_read: u64,
    /// Cache-write token count or allowance as the provider reports it.
    pub cache_write: u64,
    /// Monotonic wall-time allowance from admission.
    pub time: Duration,
}

/// A dimension of a [`Budget`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Dimension {
    /// The completion-count allowance is exhausted.
    Turns,
    /// The fresh input-token allowance is exhausted.
    Input,
    /// The output-token allowance is exhausted.
    Output,
    /// The cache-read token allowance is exhausted.
    CacheRead,
    /// The cache-write token allowance is exhausted.
    CacheWrite,
    /// The injected monotonic deadline is reached.
    Time,
    /// The checked integer-spend allowance is exhausted.
    Unit,
}

/// Why a session yielded: how the LLM stopped calling tools.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Yield {
    /// The child's final answer, forced by its share ending.
    Budget,
    /// The LLM finished its turn.
    Done,
    /// The LLM ran out of tokens mid-answer.
    Truncated,
    /// The LLM declined to answer.
    Refused,
    /// The LLM asked for tools and named none.
    Malformed,
}

/// Why a compaction answer cannot open a new window (domain/session.md, 8.2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum NoSummary {
    /// The provider refused the summary, with or without accompanying text.
    Refused,
    /// The output limit was reached before any summary text was produced.
    Truncated,
    /// The answer contained no non-whitespace summary text.
    Malformed,
}

/// How a session ended.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum End {
    /// Refused at the entrance: every session slot is taken.
    Busy,
    /// Refused at the entrance: the spec does not fit the limits.
    Invalid,
    /// Its opener closed it.
    Closed,
    /// A call failed, for good or after its retries ran out.
    Failed {
        failure: Failure,

        /// Exact content-free transport evidence carried by the terminal.
        evidence: crate::llm::Evidence,
        attempts: u32,
        /// Last failed attempt’s bounded diagnostic, carried to the opener.
        detail: Box<[u8]>,
    },
    /// The session's budget ran out in the `spent` dimension: it needed a
    /// completion the budget does not leave room for, or its time is up.
    Budget {
        /// Exhausted receiving or deployment-unit dimension, reported to the
        /// parent only after all started provider, tool and child work settles.
        spent: Dimension,
    },
    /// The compaction produced no usable summary; no turn was told.
    NoSummary { fault: NoSummary },
    /// A transcript cannot be resumed. The next attempt must start fresh;
    /// this is a transient run failure.
    TranscriptRefused {
        /// Typed terminal reason supplied by the lower layer.
        reason: crate::record::Refusal,
    },
    /// A checked price or cumulative spend did not fit the deployment counter.
    PriceOverflow,
    /// Cumulative raw usage did not fit; the rejected completion is not
    /// charged or told. `PriceOverflow` takes precedence when both fail.
    UsageOverflow,
}

/// A root allowance exhausted before publishing a requested completion.
/// This carries no provider policy and allocates no payload.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BudgetDenial {
    /// Ordinary wall time is spent; ends as `Dimension::Time` after kit close.
    Time,
    /// No global completion remains; ends as `Dimension::Turns` after kit close.
    Turns,
    /// No global deployment-unit allowance remains; ends as `Dimension::Unit`.
    Spend,
}
