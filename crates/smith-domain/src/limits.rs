//! Ownership caps and checked worst-case arithmetic. This module retains no runtime state.
//! `valid` checks compatibility and constructor capacities, without knowing live ownership.
//! `worst_case` retains the transitional container and payload sum for callers.
//!
//! Contract: domain/run.md, section 13; programming-model.md, sections 4.4, 4.5 and 6.3.

use alloc::boxed::Box;

use skein_lib::{Id, Map, Queue, Set, Slab, Token};
use smith_domain_run::{self as run, Ask};
use smith_domain_session as session;

use crate::domain::{Credential, Flight, Handoff, IssuedCompletion, StartContext, TurnHandoff};
use crate::facts::Fact;
use crate::peer::{self, Peer};

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

/// The most memory the domain holds under `limits`, in bytes (programming-model.md, section 6.3), or `None`
/// if it does not fit a `u64` or the limits cannot be honoured: the
/// child domains' own, or limits under which a session would refuse what the
/// run asks of it within its own limits whatever the charter (fewer sessions
/// than the run's conversations, a smaller budget, fewer
/// repositories). How many bytes an opening holds is the charter's, and for a
/// sub-agent its brief's, which only its asker's session bounds: a session
/// refuses one larger than its byte limit at its entrance, which refuses a run
/// as `Invalid(Conversation)` for main, and answers a sub-agent's call as
/// unanswered.
///
/// Child domains are counted independently from root peer descriptors, bounded
/// decoded asks, deferred concrete answers, causal child queues, Start histories
/// and concrete Turn handoffs. Message/Turn arrays are additional to the
/// session's Block/payload cap. Rewritten root prompt Block/Message envelopes,
/// semantic-result plus escaped text construction, failure diagnostic transit
/// and separately retained observations are counted before caller ownership
/// transfer. V2 has no persistent answer-ticket map; valid results own
/// pre-effect session credit until received and recorded through close.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let run_limits = &limits.run;
    let session_limits = &limits.session;
    let children = run::worst_case(run_limits)?.checked_add(session::worst_case(session_limits)?)?;
    let peers = run_limits.conversations;
    let tickets = run_limits
        .run_bytes
        .checked_add(Map::<u64, Ask>::worst_case(peer::asks(session_limits))?)?
        .checked_add(limits.decoded_call_bytes)?
        .checked_add(crate::llm::Report::BYTES)?;
    let held = Slab::<Peer>::worst_case(peers)?.checked_add(u64::from(peers).checked_mul(tickets)?)?;
    let found = Map::<Token, Id<Peer>>::worst_case(peers)?.checked_mul(2)?;
    let flights = Map::<Token, Flight>::worst_case(flights(limits)?)?
        .checked_add(u64::from(flights(limits)?).checked_mul(session_limits.delegated_result_bytes)?)?
        .checked_add(Map::<u32, Credential>::worst_case(limits.accounts)?)?
        .checked_add(Map::<Token, IssuedCompletion>::worst_case(run_limits.conversations)?)?
        .checked_add(Queue::<crate::Request>::worst_case(2)?)?
        .checked_add(Map::<Token, Token>::worst_case(commands(limits)?)?)?;
    let ready = Set::<Handoff>::worst_case(handoffs(limits)?)?.checked_mul(2)?;
    // Each queued run output can independently own a receipt copy, fields
    // or returned feedback. Inline
    // request storage is counted by Queue; this is its owned payload.
    let payload = peer::payload(run_limits)?
        .max(u64::from(session_limits.failure_bytes))
        .max(run_limits.outcome_bytes)
        .max(run::Delivered::worst_case())
        .max(run_limits.run_bytes.checked_add(u64::from(run_limits.host_input_bytes))?);
    // An Open renders each selected guide/check path once per mount. These
    // owning prompt copies can coexist with the retained Charter and queued
    // sibling/caller handoffs; inline wrappers remain in their containers.
    let convention_paths = run_limits
        .run_bytes
        .min(u64::try_from(run::Conventions::PATH_CAPACITY).ok()?.checked_mul(2)?)
        .checked_mul(u64::from(run_limits.directories))?;
    // Kind/conflict labels and quoted delimiters are additional to the owning
    // workspace copy. Each guide and mount renders its name once; each conflict
    // payload renders once. The retained workspace already owns Box wrappers.
    let directories = u64::from(run_limits.directories);
    let conflicts = directories.checked_mul(u64::from(run_limits.conflicts))?;
    let names = directories.checked_mul(u64::from(run_limits.directory_name_bytes))?.min(run_limits.run_bytes);
    let paths = conflicts.checked_mul(u64::from(run_limits.conflict_path_bytes))?.min(run_limits.run_bytes);
    let workspace_render = directories
        .checked_mul(52)?
        .checked_add(conflicts.checked_mul(3)?)?
        .checked_add(paths)?
        .checked_add(names.checked_mul(2)?)?;
    // Moving directory names into tools authority overlaps its receiving Repo
    // cells and one allocated Name mount per directory with the source array;
    // cwd additionally clones the first Name and its payload.
    let host_names = run_limits.host_path_bytes.checked_add(1)? / 2;
    let host_cells = directories
        .checked_mul(u64::from(host_names))?
        .checked_mul(u64::try_from(size_of::<smith_domain_tools::Name>()).ok()?)?;
    let host_copy = directories.checked_mul(u64::from(run_limits.host_path_bytes))?.min(run_limits.run_bytes);
    let authority_cells = skein_lib::List::<smith_domain_tools::Repo>::worst_case(run_limits.directories)?
        .checked_add(host_cells)?
        .checked_add(host_copy)?
        .checked_add(
            directories.checked_add(1)?.checked_mul(u64::try_from(size_of::<smith_domain_tools::Name>()).ok()?)?,
        )?
        .checked_add(u64::from(run_limits.directory_name_bytes).min(run_limits.run_bytes))?;
    // A rendered main prefix adds at most two instruction LFs and seven
    // delimiter/paragraph bytes per supplied Section. Charter ownership prices
    // its cells and payloads separately; every queued/caller Open may coexist.
    let brief_delimiters = u64::from(run_limits.brief_sections).checked_mul(7)?.checked_add(2)?;
    let payload = payload
        .checked_add(brief_delimiters)?
        .checked_add(convention_paths)?
        .checked_add(workspace_render)?
        .checked_add(authority_cells)?;
    // Rule 5: queue cells cover the composed step reserve; payload is one
    // handoff per conversation before its next external completion.
    let run_out = Queue::<run::Request>::worst_case(run_out(limits))?
        .checked_add(u64::from(run_limits.conversations).checked_mul(payload)?)?;
    // Every copied turn/prompt in the child queue and every separate handoff
    // can own record envelopes plus capped block payload concurrently. Start
    // history remains independent while main has not consumed its binding.
    let record = record_payload(limits)?;
    let queued_session = Queue::<session::Request>::worst_case(session_out(limits))?.checked_add(
        u64::from(run_limits.conversations)
            .checked_mul(record.max(prompt_payload(limits)?).max(u64::from(session_limits.failure_bytes)))?,
    )?;
    let starts = Slab::<StartContext>::worst_case(run_limits.runs)?.checked_add(
        u64::from(run_limits.runs).checked_mul(record.checked_add(u64::from(run_limits.answered_bytes))?)?,
    )?;
    let pending = Map::<Token, crate::route::PendingCompletion>::worst_case(run_limits.runs)?
        .checked_add(u64::from(run_limits.runs).checked_mul(prompt_payload(limits)?)?)?
        .checked_add(Queue::<crate::Request>::worst_case(2)?)?;
    let turns = Slab::<TurnHandoff>::worst_case(session_out(limits))?
        .checked_add(u64::from(run_limits.conversations).checked_mul(record)?)?;
    // Canonical rendering can temporarily retain the complete semantic result
    // beside the allocated text, before it moves into one flight.
    let answers = u64::from(run_limits.answered_calls)
        .checked_mul(u64::try_from(size_of::<crate::AnsweredCall>()).ok()?)?
        .checked_add(u64::from(run_limits.answered_bytes).checked_mul(2)?)?;
    let rendering = payload.checked_add(session_limits.delegated_result_bytes)?.checked_add(answers)?;
    // Estimate at one composed entrance plus child buffering slack. Facts own only
    // provider IDs/names from one answer per producer, never whole completions.
    let facts = Queue::<Fact>::worst_case(facts(limits)?)?
        .checked_add(u64::from(run_limits.conversations).checked_mul(limits.session.answer_bytes)?)?
        // Each queued opening copies a model name already checked against Config.
        .checked_add(u64::from(facts(limits)?).checked_mul(limits.configured_model_bytes)?)?;
    let content = Queue::<crate::Content>::worst_case(limits.session.facts)?
        .checked_add(u64::from(run_limits.conversations).checked_add(1)?.checked_mul(limits.session.history_bytes)?)?;
    children
        .checked_add(
            u64::from(limits.endpoints).checked_mul(u64::try_from(size_of::<run::charter::Endpoint>()).ok()?)?,
        )?
        .checked_add(limits.configured_model_bytes)?
        .checked_add(held)?
        .checked_add(found)?
        .checked_add(flights)?
        .checked_add(ready)?
        .checked_add(run_out)?
        .checked_add(queued_session)?
        .checked_add(starts)?
        .checked_add(pending)?
        .checked_add(turns)?
        .checked_add(rendering)?
        .checked_add(facts)?
        .checked_add(content)
}

/// The root's footprint for an owner that drains optional content after every
/// step, resume and fire. The owner admits each completion within
/// `completion_bytes`; one input can copy that completion or one tool terminal,
/// never both. Queue cells remain allocated at the domain's ordinary capacity.
#[must_use]
pub fn worst_case_drained_content(limits: &Limits, completion_bytes: u64) -> Option<u64> {
    let retained = u64::from(limits.run.conversations).checked_add(1)?.checked_mul(limits.session.history_bytes)?;
    worst_case(limits)?.checked_sub(retained)?.checked_add(limits.session.history_bytes.max(completion_bytes))
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

/// Estimate for uncapped facts: one composed entrance plus the children's buffering slack.
pub(crate) fn facts(limits: &Limits) -> Option<u32> {
    let reserve = run_steps(limits)
        .checked_mul(run::max_facts(&limits.run))?
        .checked_add(session_steps(limits).checked_mul(session::max_facts(&limits.session))?)?;
    reserve.checked_add(limits.run.facts)?.checked_add(limits.session.facts)
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
