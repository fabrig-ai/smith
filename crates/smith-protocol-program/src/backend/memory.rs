//! Program constructor capacities and JSON workspace edges (protocol/programs.md 5.1–5.8).
//! Keeps no live state and knows no retained heap total; `backend_valid` checks settings.

use super::stream;
use super::{Settings, measures};

/// Whether backend capacities and JSON edges fit (programming-model.md, 6.3).
#[must_use]
pub fn backend_valid(settings: &Settings) -> bool {
    backend_edges(settings).is_some()
}

fn backend_edges(settings: &Settings) -> Option<()> {
    settings.path_bytes.checked_mul(2)?.checked_add(1)?;
    settings.receiving.max_completion_blocks.checked_mul(5)?;
    measures::capacity(settings)?;
    settings.adapter.client.tools.checked_add(u32::try_from(super::policy::BUILTINS.len()).ok()?.checked_add(1)?)?;
    super::redaction::capacity(settings)?;
    let input = settings.adapter.stated.call_input_bytes;
    // Probe the individual document/collector/writer edges, without their
    // enclosing retained and transient heap accounting.
    document_bound(settings.stream_tokens, settings.line_bytes)?;
    document_bound(input, settings.mcp.message_bytes)?;
    document_bound(input, input)?;
    stream_edges(input, settings.adapter.client.depth, input)?;
    stream_edges(settings.mcp.message_bytes, settings.adapter.client.depth, input)?;
    stream_edges(settings.line_bytes, settings.adapter.client.depth, settings.stream_tokens)?;
    writer_bound(8, settings.input_bytes)?;
    for bound in [settings.mcp.message_bytes, settings.mcp.schema_bytes] {
        bound.checked_add(2)?;
        json_edges(skein_json::collector::Limits {
            tokenizer: skein_json::tokenizer::Limits {
                depth: bound,
                string: bound,
                number: bound,
                chunk: bound,
                length: bound,
            },
            tokens: bound.checked_add(2)?,
            text: bound,
            skip: u64::from(bound),
        })?;
    }
    Some(())
}

fn stream_edges(bound: u32, depth: u32, tokens: u32) -> Option<()> {
    json_edges(stream::limits(bound, depth, tokens))
}

fn json_edges(limits: skein_json::collector::Limits) -> Option<()> {
    skein_json::collector::worst_case(
        &limits,
        &[],
        &skein_json::collector::Filter { root: skein_json::collector::Keep::Value },
    )?;
    writer_bound(limits.tokenizer.depth, limits.tokenizer.length)?;
    Some(())
}

fn document_bound(tokens: u32, text: u32) -> Option<u64> {
    skein_json::document::worst_case(&skein_json::document::Limits { tokens, text })
}

fn writer_bound(depth: u32, length: u32) -> Option<u64> {
    skein_json::writer::worst_case(&skein_json::writer::Limits { depth, length })
}
