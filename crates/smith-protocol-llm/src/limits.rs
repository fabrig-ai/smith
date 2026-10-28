//! Static receiving compatibility and simultaneous translation ownership.
//! All prices use checked arithmetic before preparation or final allocation.

use core::mem::size_of;

use skein_llm::client;
use smith_domain::{llm, session};

/// Immutable protocol bounds for application translation and the shared Client.
/// The caller supplies these at startup, independently from application scheduling limits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Actual shared Client document, HTTP/SSE and receiving bounds.
    pub client: client::Limits,

    /// Aggregate retained served/application declaration wrappers and all owned fields.
    pub tool_bytes: u64,

    /// Aggregate source history payload admitted by the parent session.
    pub history_bytes: u64,

    /// Maximum rendered tool result, including its cut marker, in bytes.
    pub rendered_result: u32,

    /// Effective numbers supplied by the service for immutable tool descriptions.
    pub stated: crate::Stated,
}

/// Actual root Complete receiving contract, checked before preparing the Client.
/// No receiving field is inferred from an application's token budget.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Receiving {
    /// Maximum complete translated content ownership, including all block cells and replay envelopes.
    pub max_completion_bytes: u64,

    /// Maximum actual assistant blocks, including text, refusal and opaque blocks.
    pub max_completion_blocks: u32,

    /// Aggregate decoded-call ownership across this whole completion, including wrappers and fields.
    pub decoded_call_bytes: u64,

    /// Exact diagnostic bytes accepted on the actual Failed boundary.
    pub max_failure_bytes: u32,
}

/// A caller-owned JSON value is admitted by its encoded bytes and the native depth.
/// Event string and token receiving bounds do not restrict outgoing schemas.
pub(crate) const fn document_limits(client: &client::Limits, bytes: u32) -> skein_llm::DocumentLimits {
    skein_llm::DocumentLimits { bytes, strings: bytes, tokens: bytes, depth: client.depth }
}

/// One reasoning block may hold the completion's whole share; metadata
/// envelopes use the same entrance. Strings, tokens and depth stay per block.
pub(crate) fn replay_limits(client: &client::Limits) -> Option<skein_llm::DocumentLimits> {
    Some(skein_llm::DocumentLimits {
        bytes: client.output_items.checked_mul(client.reasoning)?.max(client.metadata),
        strings: client.strings,
        tokens: client.tokens,
        depth: client.depth,
    })
}

/// Complete translated ownership for every completion admitted by the shared Client.
/// Original text/IDs/arguments are under `answer_bytes`; reasoning shares one
/// aggregate allowance, beside each block's metadata and durable header.
/// Decoded ownership is one aggregate allowance, never multiplied per block.
/// It must first cover one full fallback `Decoded` cell per possible output part.
/// Returns None on overflow or an invalid shared-client configuration.
#[must_use]
pub fn completion_worst_case(client_limits: &client::Limits, decoded_call_bytes: u64) -> Option<u64> {
    client::worst_case(client_limits)?;
    let parts = u64::from(client_limits.output_items);
    let decoded_cells = parts.checked_mul(u64::try_from(size_of::<llm::Decoded>()).ok()?)?;
    if decoded_call_bytes < decoded_cells {
        return None;
    }
    skein_llm::replay_bytes(&replay_limits(client_limits)?)?;
    let reasoning = parts.checked_mul(u64::from(client_limits.reasoning))?;
    let envelopes =
        u64::from(client_limits.metadata).checked_add(u64::from(skein_llm::REPLAY_HEADER_BYTES))?.checked_mul(parts)?;
    let replay = reasoning.checked_add(envelopes)?;
    let cell = size_of::<llm::Said>().max(size_of::<session::llm::Block>());
    let wrappers = u64::try_from(cell).ok()?.checked_mul(parts)?;
    u64::from(client_limits.answer)
        .checked_add(replay)?
        .checked_add(wrappers)?
        .checked_add(parts.checked_mul(u64::try_from(size_of::<u64>()).ok()?)?)?
        .checked_add(decoded_call_bytes)
}

/// Whether an adapter can serve under these receiving bounds: compatibility
/// and representable edge workspaces (programming-model.md, section 6.3).
#[must_use]
pub fn valid(limits: &Limits, receiving: &Receiving) -> bool {
    if limits.rendered_result < crate::CUT_MARKER_BYTES {
        return false;
    }
    let Some(completion) = completion_worst_case(&limits.client, receiving.decoded_call_bytes) else {
        return false;
    };
    if completion > receiving.max_completion_bytes
        || limits.client.output_items > receiving.max_completion_blocks
        || limits.client.detail_bytes > receiving.max_failure_bytes
    {
        return false;
    }
    let Ok(document_bytes) = u32::try_from(limits.tool_bytes.max(u64::from(limits.client.input))) else {
        return false;
    };
    let Some(replay) = replay_limits(&limits.client) else {
        return false;
    };
    skein_llm::replay_worst_case(&document_limits(&limits.client, document_bytes)).is_some()
        && skein_llm::replay_worst_case(&replay).is_some()
}

/// Derive one result's rendering from the workspace caps and host/delegated text.
/// Every invalid byte expands to four visible escape bytes; a read's every
/// byte may be a line, with a ten-digit number, colon and space. Fixed room
/// holds counts, separators, status words and the explicit omitted-byte marker.
/// Contract: protocol/limits.md, section 3.4; protocol/llm.md, section 5.
#[must_use]
pub fn render_worst_case(tools: &smith_domain::tools::Limits, host_answer: u32, delegated_answer: u64) -> Option<u32> {
    let read = u64::from(tools.tool_payload).checked_mul(16)?.checked_add(192)?;
    let listed = tools
        .list_bytes
        .checked_mul(4)?
        .checked_add(u64::from(tools.list_entries).checked_mul(11)?)?
        .checked_add(64)?;
    let search = u64::from(tools.search_bytes)
        .checked_mul(4)?
        .checked_add(u64::from(tools.search_hits).checked_mul(14)?)?
        .checked_add(64)?;
    let shell =
        u64::from(tools.shell_head).checked_add(u64::from(tools.shell_tail))?.checked_mul(4)?.checked_add(288)?;
    let ambiguity = u64::from(tools.match_lines).checked_mul(12)?.checked_add(96)?;
    // A working name and normalized path may each reach the path ceiling;
    // invalid bytes expand fourfold, with separators and failure wording.
    let failure_path = u64::from(tools.path_bytes).checked_mul(8)?.checked_add(96)?;
    let host = u64::from(host_answer).checked_add(7)?;
    let largest = read
        .max(listed)
        .max(search)
        .max(shell)
        .max(ambiguity)
        .max(host)
        .max(delegated_answer)
        .max(failure_path)
        .max(crate::render::size_refusal_worst_case()?)
        .max(crate::failure::job_problem_worst_case()?)
        .max(128);
    u32::try_from(largest.checked_add(u64::from(crate::render::CUT_MARKER_BYTES))?).ok()
}
