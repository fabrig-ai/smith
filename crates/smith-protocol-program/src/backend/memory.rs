//! Complete retained and transient program ownership (protocol/programs.md 5.1–5.8).
//! Stopped main storage is reused by the compaction invocation.

use super::stream;
use super::{Backend, Below, Command, Settings, calls, mcp, measures};
use alloc::boxed::Box;
use skein_io as io;
use skein_lib::{List, Queue};
use smith_domain::llm;

/// Complete additional protocol ownership, including transient copies (5.1–5.6).
/// Native I/O tables and the shared file driver are priced by the service.
#[must_use]
pub fn backend_worst_case(settings: &Settings) -> Option<u64> {
    if !backend_valid(settings) {
        return None;
    }
    let mut paths = u64::try_from(settings.program.directory.len().max(4096)).ok()?;
    paths = paths
        .checked_add(u64::try_from(settings.program.claude.len()).ok()?)?
        .checked_add(u64::try_from(settings.relay.len().max(4096)).ok()?)?;
    let mut environment = 0_u64;
    for entry in &settings.environment {
        environment = environment
            .checked_add(u64::try_from(entry.len()).ok()?)?
            .checked_add(u64::try_from(size_of::<Box<[u8]>>()).ok()?)?;
    }
    // These workspaces run in separate step phases. A line's collected
    // document survives decoding/correlation, whereas its collector has gone.
    // Include ignored fields: stream_tokens comes from the enforced line bytes.
    let document = document_bound(settings.stream_tokens, settings.line_bytes)?;
    let left = document_bound(settings.adapter.stated.call_input_bytes, settings.mcp.message_bytes)?;
    let input = settings.adapter.stated.call_input_bytes;
    let decode_collect = stream::worst_case(input, settings.adapter.client.depth, input)?;
    // The typed decoder builds fields/items after its collector has gone.
    // A call admits at most one JSON record per argument byte;
    // an item-array capacity can overlap its nested field-array capacities.
    let decode_build = document_bound(input, input)?
        .checked_add(
            u64::from(input).checked_mul(
                u64::try_from(
                    size_of::<smith_domain::run::outcome::Field>()
                        .checked_add(size_of::<smith_domain::run::outcome::Item>())?,
                )
                .ok()?,
            )?,
        )?
        .checked_add(u64::from(input))?;
    let decode = decode_collect.max(decode_build);
    let compare = stream::worst_case(settings.mcp.message_bytes, settings.adapter.client.depth, input)?
        .checked_add(left)?
        .checked_add(List::<(u32, u32)>::worst_case(input)?)?;
    // Before the compaction child spawns, the idle writer alternative also
    // covers json::quote's escaped window and depth-1 encoder; no stream,
    // collector or document is live in that phase.
    let workspace = stream::worst_case(settings.line_bytes, settings.adapter.client.depth, settings.stream_tokens)?
        .max(document.checked_add(decode.max(compare))?)
        .max(writer_bound(8, settings.input_bytes)?)
        // Tail passes borrow text. Consumed served results replace prompt
        // payloads; at most one input ceiling plus one temporary result
        // coexists with the encoded line during this separate workspace.
        .max(u64::from(settings.input_bytes).checked_add(u64::from(settings.adapter.rendered_result))?);
    u64::try_from(size_of::<Backend>())
        .ok()?
        // Launch peak: settings paths; executable; instructions and MCP path
        // temporaries; their two argv copies; relative cwd; source directory.
        .checked_add(paths.checked_mul(8)?)?
        // Settings, timeout derivation and the spawn each own command entries. The
        // service prices its machine environment independently. Fixed room
        // covers the synthesized config-directory entry, secret and argv cells.
        .checked_add(environment.checked_mul(3)?.checked_add(32_768)?)?
        .checked_add(
            crate::command_worst_case(settings.receiving.max_failure_bytes)?
                .checked_sub(u64::try_from(size_of::<Command>()).ok()?)?,
        )?
        .checked_add(mcp::mcp_worst_case(&settings.mcp)?.checked_sub(u64::try_from(size_of::<mcp::Server>()).ok()?)?)?
        // Correlation moves payloads, with old/new MCP event slot arrays
        // briefly coexisting while the held queue is rebuilt.
        .checked_add(List::<mcp::Up>::worst_case(u32::from(settings.mcp.calls))?.checked_mul(2)?)?
        .checked_add(u64::from(settings.mcp.message_bytes).checked_mul(u64::from(settings.mcp.calls))?)?
        // Held neutral prompt and the instructions Store's system-text copy.
        // Tool/schema inventories are already in the MCP bound, user encoding
        // in the line terms; neither is a third neutral prompt allocation.
        .checked_add(settings.prompt_bytes.checked_mul(2)?)?
        // Held outcome/delivery contracts, finish_schema's bytes and their
        // inline argv copy coexist during launch. MCP's schemas/list/escaping
        // are in mcp_worst_case, rather than another thirteen contract copies.
        .checked_add(u64::from(settings.contract_bytes).checked_mul(3)?)?
        // Four queued completions plus the message currently being assembled.
        .checked_add(settings.receiving.max_completion_bytes.checked_mul(5)?)?
        // Stderr ring, chronological tail, and sanitized failure terminal.
        // The sanitizer's List becomes that terminal Box without a fourth copy;
        // the status command's capture/scratch is in command_worst_case.
        .checked_add(u64::from(settings.receiving.max_failure_bytes).checked_mul(3)?)?
        // Permission policy uses MCP's collector after Server::from_io returns;
        // its byte-derived depth and scratch are already in mcp_worst_case.
        // Verdict validation runs only after judge has dropped its document.
        .checked_add(workspace)?
        // Relative resolution retains cwd, its joining slash and the original path.
        .checked_add(u64::from(settings.path_bytes.checked_mul(2)?.checked_add(1)?))?
        // The old stdout accumulator and its replacement coexist at a line
        // boundary. Collection/intake/encoding are in stream::worst_case.
        // Input has its independent ceiling: the encoder body and newline
        // copy coexist, then one prepared line remains during chunked writes.
        // Compaction uses this input-line term after window_request clears
        // outgoing; json::join may overshoot by 53 before its length check,
        // covered by the idle stdout accumulator terms before spawn.
        // Native writer retention belongs to the service's two writer terms.
        .checked_add(u64::from(settings.line_bytes).checked_mul(2)?)?
        // The finishing note is encoded only after the interrupted result,
        // reusing the old user line's term; its neutral prompt is already priced.
        .checked_add(u64::from(settings.input_bytes))?
        // Control line retained while waiting for input room, its encoding
        // copy, and the largest "smith-" + u64 id during correlation.
        .checked_add(stream::INTERRUPT_BYTES.checked_mul(2)?.checked_add(26)?)?
        // MCP correlation retains at most five messages' provider fields and
        // one rendered reply per ledger slot. Rebuilding moves payloads but
        // briefly owns both old and new slot allocations.
        .checked_add(
            List::<calls::Call>::worst_case(settings.receiving.max_completion_blocks.checked_mul(5)?)?
                .checked_mul(2)?,
        )?
        .checked_add(u64::from(settings.adapter.client.answer).checked_mul(5)?)?
        .checked_add(
            u64::from(settings.adapter.rendered_result)
                .checked_mul(u64::from(settings.receiving.max_completion_blocks).checked_mul(5)?)?,
        )?
        // Joining user text temporarily owns the tail and a replacement reply.
        .checked_add(u64::from(settings.adapter.rendered_result).checked_mul(2)?)?
        .checked_add(Queue::<llm::Completion>::worst_case(4)?)?
        .checked_add(measures::worst_case(settings)?)?
        // Compaction before spawn reuses settled storage, with I = input_bytes:
        // Held prompt: first prompt_bytes term; the old request was replaced.
        // render's I-capacity output: measures' prompt payload; prior measures
        // drained before Complete, begin_prompt cleared prompt_pending, and
        // prompt_text_measure moves this text into that idle payload slot.
        // Temporary rendered result: workspace's input + rendered_result
        // alternative; stream collection/decoding has not started.
        // Escaped quote and depth-1 encoder: workspace's writer(depth 8, I)
        // alternative; no stream, collector or document is live before spawn.
        // Joined line (at most I + 53): ordinary input-line term (overshoot
        // in idle stdout terms); window_request cleared the old outgoing.
        // Later offer_prompt system capture: second prompt_bytes term; the
        // instructions Store's copy was dropped before Stored enabled launch.
        .checked_add(4096)? // deferred stdout intake
        .checked_add(1024)? // retained API message ID
        .checked_add(4096)? // a failed Store's bounded temporary leaf
        .checked_add(Queue::<io::Request>::worst_case(32)?)?
        .checked_add(Queue::<Below>::worst_case(32)?)
}

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
