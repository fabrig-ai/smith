//! Replies to held MCP calls and the run's timeout (programs.md 5.3, 5.7).
//! Keeps no state; every user text in a reply pass belongs to its last held reply.

use super::{Backend, calls};
use crate::mcp;
use alloc::boxed::Box;
use skein_lib::{Decimal, Duration, List};
use smith_domain::llm;

const TIMEOUT_NAME: &[u8] = b"MCP_TOOL_TIMEOUT=";
// The largest u64 has 20 decimal digits.
const U64_DIGITS: usize = 20;

fn joined(left: &[u8], right: &[u8], bound: u32) -> Option<Box<[u8]>> {
    let separator = !left.is_empty() && !right.is_empty();
    let length = left.len().checked_add(usize::from(separator))?.checked_add(right.len())?;
    let length = u32::try_from(length).ok()?;
    if length > bound {
        return None;
    }
    let mut text = List::with_capacity(length);
    for byte in left {
        text.push(*byte).ok()?;
    }
    if separator {
        text.push(b'\n').ok()?;
    }
    for byte in right {
        text.push(*byte).ok()?;
    }
    Some(text.into_boxed())
}

/// A reply pass may leave a user line for a turn with no held call.
pub(super) struct Resumed {
    pub line: Option<Box<[u8]>>,
    pub observed: Option<Box<[u8]>>,
}

pub(super) fn resume(
    backend: &mut Backend,
    messages: Box<[llm::Message]>,
    prompt: &mut llm::Prompt,
    capture: bool,
) -> Option<Resumed> {
    let bound = backend.settings.adapter.rendered_result;
    let mut last = None;
    let mut nudge: Option<Box<[u8]>> = None;
    let mut synthetic = None;
    // A Complete contains history. Only the tail after the last assistant
    // belongs to this continuation (protocol/programs.md, section 5.9).
    let mut from = 0;
    for (index, message) in messages.iter().enumerate() {
        if message.role == llm::Role::Assistant {
            from = index.checked_add(1)?;
        }
    }
    let mut text = false;
    let mut result = false;
    for message in messages.iter().skip(from) {
        for block in &message.content {
            match block {
                llm::Block::ToolResult { .. } => result = true,
                llm::Block::Text { .. } => text = true,
                llm::Block::Opaque { .. } | llm::Block::Refusal { .. } | llm::Block::ToolCall { .. } => {}
            }
        }
    }
    if !result {
        if !text {
            return Some(Resumed { line: None, observed: None });
        }
        // Messages and nudges share the exact tail encoder. Its independent
        // input ceiling admits host offers larger than a tool reply, without
        // retaining an extra unescaped copy (programs.md 5.9).
        prompt.messages = messages;
        let rendered = super::tail::render_observed(
            prompt,
            &backend.ledger,
            &backend.settings.adapter,
            backend.settings.input_bytes,
            b"",
            capture,
        )?;
        return Some(Resumed { line: Some(rendered.line), observed: rendered.text });
    }
    for message in messages.into_iter().skip(from) {
        let user = match message.role {
            llm::Role::User => true,
            llm::Role::Assistant => false,
        };
        for block in message.content {
            match block {
                llm::Block::ToolResult { id, result } => {
                    if id.as_ref() != b"smith-structured-finish" && !backend.awaits_answer(&id) {
                        continue;
                    }
                    let mut answer = Some(calls::render(result, prompt, &backend.settings.adapter)?);
                    for index in 0..backend.ledger.len() {
                        let call = backend.ledger.get_mut(index)?;
                        if call.id == id && !call.answered {
                            call.answer = answer.take();
                            last = Some(index);
                            break;
                        }
                    }
                    if id.as_ref() == b"smith-structured-finish" {
                        synthetic = match answer {
                            Some(mcp::Answer::Text(text) | mcp::Answer::Error(text)) => Some(text),
                            None => synthetic,
                        };
                    }
                }
                llm::Block::Text { text, .. } => {
                    if user {
                        nudge = Some(joined(nudge.as_deref().unwrap_or(b""), &text, bound)?);
                    }
                }
                llm::Block::Opaque { .. } | llm::Block::Refusal { .. } | llm::Block::ToolCall { .. } => {}
            }
        }
    }
    if let Some(index) = last {
        let mut observed = None;
        if let Some(text) = nudge {
            let call = backend.ledger.get_mut(index)?;
            call.answer = Some(match call.answer.take()? {
                mcp::Answer::Text(answer) => mcp::Answer::Text(joined(&answer, &text, bound)?),
                mcp::Answer::Error(answer) => mcp::Answer::Error(joined(&answer, &text, bound)?),
            });
            observed = match call.answer.as_ref()? {
                mcp::Answer::Text(text) | mcp::Answer::Error(text) => Some(text.clone()),
            };
        }
        Some(Resumed { line: None, observed })
    } else {
        let text = nudge.or(synthetic);
        let line = match &text {
            Some(text) => Some(super::stream::text_message(text, backend.settings.input_bytes)?),
            None => None,
        };
        Some(Resumed { line, observed: text })
    }
}

pub(super) fn environment(base: &[Box<[u8]>], configuration: &[u8], run_time: Duration) -> Option<Box<[Box<[u8]>]>> {
    let mut entries = List::with_capacity(crate::ENVIRONMENT_ENTRIES);
    // Source filtering and its aggregate cap also cover the derived timeout.
    for entry in base {
        if !entry.starts_with(TIMEOUT_NAME) {
            entries.push(entry.clone()).ok()?;
        }
    }
    let milliseconds = Decimal::of(run_time.as_nanos().checked_div(1_000_000)?);
    let room = u32::try_from(TIMEOUT_NAME.len().checked_add(U64_DIGITS)?).ok()?;
    let mut timeout = List::with_capacity(room);
    for byte in TIMEOUT_NAME.iter().chain(milliseconds.as_bytes()) {
        timeout.push(*byte).ok()?;
    }
    entries.push(timeout.into_boxed()).ok()?;
    crate::environment(&entries.into_boxed(), configuration).ok()
}
