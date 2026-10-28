//! Admission limits and checked capacities. This module retains no runtime state.
//! `valid` checks compatibility and constructor capacities, without knowing live ownership.
//!
//! Contract: domain/run.md, section 13; programming-model.md, sections 4.4, 4.5 and 6.3.

use alloc::boxed::Box;

use smith_domain_run as run;
use smith_domain_session as session;

/// The agent domain's limits (programming-model.md, sections 4.5 and 6.3), handed to every step read-only: its
/// child domains', each handed down to the one it bounds. The session's include
/// its tools'.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Maximum configured credential accounts retained by the root.
    pub accounts: u32,
    /// Maximum endpoint names in the agent configuration.
    pub endpoints: u32,
    /// Owned configured model cells and names, derived from startup declarations.
    pub configured_model_bytes: u64,

    /// Aggregate decoded application-call bytes admitted from one completion.
    /// The adapter includes these owning call fields in its translated completion
    /// bound before provider work; oversized classifications become `TooLarge`.
    /// This includes one Decoded cell for every possible completion block,
    /// including classifications with no payload after a refusal.
    pub decoded_call_bytes: u64,

    /// Safety margin subtracted from credential validity before use.
    pub skew: skein_lib::Duration,
    /// Immutable run-child ownership and admission limits.
    pub run: run::Limits,
    /// Immutable session-child ownership and admission limits.
    pub session: session::Limits,
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let run_limits = &limits.run;
    let session_limits = &limits.session;
    let budget = run_limits.budget;
    let ceiling = session_limits.budget;
    let fits = budget.turns <= ceiling.turns && budget.spend <= session_limits.spend && budget.time <= ceiling.time;
    if !fits
        || run_limits.conversations > session_limits.sessions
        || run_limits.directories > session_limits.tools.repos
        || run_limits.directory_name_bytes > session_limits.tools.path_bytes
        || run_limits.host_path_bytes > session_limits.tools.host_path_bytes
        || u64::from(run_limits.answered_bytes) > session_limits.history_bytes
    {
        return false;
    }
    let Some(feedback) = crate::feedback_worst_case(run_limits) else {
        return false;
    };
    if session_limits.spend == 0 || feedback > session_limits.delegated_result_bytes {
        return false;
    }
    let Some(decoded_cells) = u64::from(session_limits.completion_blocks)
        .checked_mul(u64::try_from(size_of::<crate::llm::Decoded>()).expect("decoded cell size fits u64"))
    else {
        return false;
    };
    if decoded_cells > limits.decoded_call_bytes.min(session_limits.history_bytes) {
        return false;
    }
    flights(limits).is_some()
        && handoffs(limits).is_some()
        && commands(limits).is_some()
        && run::valid(run_limits)
        && session::valid(session_limits)
}

/// Deferred concrete payload outside session while one complete delegate batch
/// owns its pre-effect receiving credit. Actual queued bytes are separately
/// allocated and priced; no post-effect history-full result is discarded.
#[cfg(test)]
pub(crate) fn uncharged(limits: &Limits) -> Option<u64> {
    u64::from(limits.session.parallel_tools).checked_mul(limits.session.delegated_result_bytes)
}

/// Delegated calls in flight at once: a batch of each session.
pub(crate) fn flights(limits: &Limits) -> Option<u32> {
    limits.run.conversations.checked_mul(limits.session.parallel_tools)
}

/// Hand-offs on the ready list at once: a close of each conversation, and the
/// answer to each delegated call.
pub(crate) fn handoffs(limits: &Limits) -> Option<u32> {
    limits.run.conversations.checked_add(flights(limits)?)
}

/// Room for what the run emits in an entry point: its most, for each step it
/// takes (see the domain module).
pub(crate) const fn run_out(limits: &Limits) -> u32 {
    run_steps(limits).saturating_mul(run::MAX_OUT)
}

/// Room for what the session child domain emits in an entry point.
pub(crate) const fn session_out(limits: &Limits) -> u32 {
    session_steps(limits).saturating_mul(session::max_out(&limits.session))
}

/// The most steps an entry point takes of the session child domain: its own, if
/// it is for the sessions, and one for each hand-off the run makes at once in
/// answer to what that step sent it (an `Open` or a `Say`: as many as the run
/// emits requests in each of its steps). An entry point for the run takes no
/// more: one hand-off for each request its step emits. Each such step may
/// request one completion denied before publication, adding one synchronous
/// unsent settlement. Doubling covers that extra step; its Return/Close stays
/// on Ready and cannot recurse through the subtree.
pub(crate) const fn session_steps(limits: &Limits) -> u32 {
    let sent = session::max_to_opener(&limits.session);
    1_u32.saturating_add(sent.saturating_mul(run::MAX_OUT)).saturating_mul(2)
}

/// The most steps an entry point takes of the run: one for each request the
/// session's first step sends it, and, for each hand-off the run makes at once
/// in answer, one for each request the session it hands off to sends it, or a
/// single one if the hand-off is refused in the run's terms without a
/// session. An unsent completion denial adds one synchronous session terminal
/// and its run notice. Child Return and Close stay deferred on Ready, so this
/// adds at most one extra pair, rather than recursing through the subtree.
/// Doubling accounts for that bounded pair. A session just opened or continued
/// calls nothing and does not yield. An entry point for the run takes fewer: its
/// own step, and those for what the sessions it hands off to send it.
///
/// It counts what a session step sends its opener, never what it sends its
/// tools' io (a kit's close may cancel as many operations as the tools run),
/// which goes out to the protocol layer and leads nowhere else.
pub(crate) const fn run_steps(limits: &Limits) -> u32 {
    let sent = session::max_to_opener(&limits.session);
    let at_once = sent.saturating_mul(run::MAX_OUT);
    sent.saturating_add(at_once.saturating_mul(sent)).saturating_mul(2)
}

/// History payload and separate allocated record/message envelopes. The source
/// session byte cap prices Block cells/payloads, never these arrays.
pub(crate) fn record_payload(limits: &Limits) -> Option<u64> {
    let messages = u64::from(limits.session.history_messages);
    limits
        .session
        .history_bytes
        // Each turn copies its transcript model (128 bytes) and optional effort (32).
        .checked_add(messages.checked_mul(160)?)?
        // One window opener is retained or handed off; ordinary turns have no box.
        .checked_add(u64::try_from(size_of::<session::record::Opener>()).ok()?)?
        .checked_add(messages.checked_mul(u64::try_from(size_of::<session::record::Turn>()).ok()?)?)?
        .checked_add(messages.checked_mul(u64::try_from(size_of::<session::llm::Message>()).ok()?)?)
}

/// Largest owned concrete turn the domain may tell, including allocated record envelopes.
/// A host's acknowledgement window must reserve this much for its next turn.
#[must_use]
pub fn max_turn_bytes(limits: &Limits) -> Option<u64> {
    record_payload(limits)
}

/// Rewritten root prompt envelopes can be larger than session Block cells:
/// the legacy root result union retains a sealed inline diagnostic. The payload
/// cap prices all source Block/payload bytes, while this separate conservative
/// bound prices every allocated root Block/Message/served descriptor and the
/// copied host declarations. Child queue and caller-output ownership coexist.
#[expect(dead_code, reason = "programming-model.md 6.3: preserve the independent edge workspace bound")]
pub(crate) fn prompt_payload(limits: &Limits) -> Option<u64> {
    let session_block = u64::try_from(size_of::<session::llm::Block>()).ok()?;
    let blocks = limits.session.history_bytes.checked_div(session_block)?;
    let root_block = u64::try_from(size_of::<crate::llm::Block>()).ok()?;
    // The source byte total already includes each session Block cell. Root
    // translation adds only the larger cell's difference, not that cell again.
    let extra_block = root_block.max(session_block).checked_sub(session_block)?;
    let messages = u64::from(limits.session.history_messages)
        .checked_mul(u64::try_from(size_of::<crate::llm::Message>()).ok()?)?;
    let descriptors = u64::from(limits.run.host_tools)
        .checked_add(4)?
        .checked_mul(u64::try_from(size_of::<crate::llm::Served>()).ok()?)?;
    // Every retained result has its call beside it; a wind-down names finish and deliver only.
    let names = u64::try_from(size_of::<Box<[u8]>>()).ok()?.checked_mul(2)?.checked_add(13)?;
    let served = u64::try_from(size_of::<crate::llm::Served>()).ok()?;
    let choices = blocks.checked_div(2)?.checked_mul(names)?.checked_add(served.checked_mul(2)?)?;
    limits
        .session
        .history_bytes
        .checked_add(blocks.checked_mul(extra_block)?)?
        .checked_add(messages)?
        .checked_add(descriptors)?
        .checked_add(choices)?
        .checked_add(limits.run.run_bytes)
}

/// One command per live tools call; retired job slots own no IO rights.
pub(crate) fn commands(limits: &Limits) -> Option<u32> {
    limits.session.tools.kits.checked_mul(limits.session.tools.calls)
}
