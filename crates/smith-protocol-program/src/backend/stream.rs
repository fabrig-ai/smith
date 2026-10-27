//! Startup stream admission (programs.md, sections 5.1 and 5.5).
//! Retains no stream state; checks complete init lines and encodes user and control input.

use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_json::{Kind, collector, tokenizer, writer::Encoder};
use skein_lib::{Decimal, Writer};
use smith_domain::llm;

pub(super) fn limits(bound: u32, depth: u32, tokens: u32) -> collector::Limits {
    collector::Limits {
        tokenizer: tokenizer::Limits {
            depth,
            string: bound,
            number: 32.min(bound),
            chunk: 4096.min(bound),
            length: bound,
        },
        tokens,
        text: bound,
        skip: u64::from(bound),
    }
}

pub(super) fn worst_case(bound: u32, depth: u32, tokens: u32) -> Option<u64> {
    json::worst_case_limited(limits(bound, depth, tokens))
}

pub(super) fn collect(bytes: &[u8], depth: u32, tokens: u32) -> Option<skein_json::Document> {
    json::collect_limited(bytes, limits(u32::try_from(bytes.len()).ok()?, depth, tokens))
}

pub(super) fn text_message(text: &[u8], maximum: u32) -> Option<Box<[u8]>> {
    let quoted = json::quote(text, maximum)?;
    let bytes = json::join(&[b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":", &quoted, b"}}\n"]);
    if bytes.len() > usize::try_from(maximum).ok()? {
        return None;
    }
    Some(bytes)
}

#[cfg(test)]
pub(super) fn init(bytes: &[u8], tools: &[mcp::Tool], depth: u32, tokens: u32) -> bool {
    super::builtins::init_error(
        bytes,
        smith_domain::tools::Grants { inspect: false, modify: false, shell: false },
        depth,
        tokens,
    )
    .is_none()
        && checked_init(bytes, tools, depth, tokens) == Some(())
}

/// Claude Code's system/init (`protocol/programs.md`, 5.5) hides the
/// permission-prompt tool even though it remains callable.
/// Accept it absent or listed once, alongside every declared server tool.
pub(super) fn checked_init(bytes: &[u8], tools: &[mcp::Tool], depth: u32, tokens: u32) -> Option<()> {
    let document = json::collect_limited(bytes, limits(u32::try_from(bytes.len()).ok()?, depth, tokens))?;
    if json::text_in(&document, json::field(&document, 0, b"type"))? != b"system"
        || json::text_in(&document, json::field(&document, 0, b"subtype"))? != b"init"
    {
        return None;
    }
    let servers = json::field(&document, 0, b"mcp_servers")?;
    if document.token(servers)?.kind != Kind::ArrayStart {
        return None;
    }
    let server = servers.checked_add(1)?;
    if json::text_in(&document, json::field(&document, server, b"name"))? != mcp::SERVER
        || json::text_in(&document, json::field(&document, server, b"status"))? != b"connected"
        || document.token(json::after(&document, server)?)?.kind != Kind::ArrayEnd
    {
        return None;
    }
    let listed = json::field(&document, 0, b"tools")?;
    if document.token(listed)?.kind != Kind::ArrayStart {
        return None;
    }
    let mut index = listed.checked_add(1)?;
    let mut count = 0_usize;
    let mut prompt_listed = false;
    for _ in 0..document.len() {
        if document.token(index)?.kind == Kind::ArrayEnd {
            break;
        }
        let name = json::text(&document, index)?;
        let suffix = match name.strip_prefix(b"mcp__smith__") {
            Some(suffix) => suffix,
            None if !name.starts_with(b"mcp__") => {
                index = index.checked_add(1)?;
                continue;
            }
            None => return None,
        };
        let mut known = suffix == mcp::PROMPT_TOOL;
        if known {
            prompt_listed = true;
        }
        for tool in tools {
            if suffix == tool.name.as_ref() {
                known = true;
            }
        }
        if !known {
            return None;
        }
        // A repeated name cannot satisfy a missing declared tool.
        for earlier in listed.checked_add(1)?..index {
            if json::text(&document, earlier) == Some(name) {
                return None;
            }
        }
        index = index.checked_add(1)?;
        count = count.checked_add(1)?;
    }
    if count != tools.len().checked_add(usize::from(prompt_listed))? {
        return None;
    }
    Some(())
}

#[cfg(test)]
#[path = "stream_tests.rs"]
pub(super) mod tests;

pub(super) fn user_message(prompt: &llm::Prompt, maximum: u32) -> Option<Box<[u8]>> {
    let limits = skein_json::writer::Limits { depth: 8, length: maximum.checked_sub(1)? };
    let mut measure = Encoder::measure(&limits);
    user(&mut measure, prompt);
    let length = measure.measured().ok()?;
    let mut write = Encoder::write(length, &limits);
    user(&mut write, prompt);
    Some(json::join(&[&write.finish(), b"\n"]))
}

fn user(json: &mut Encoder, prompt: &llm::Prompt) {
    json.object_start();
    json.key(b"type");
    json.string(b"user");
    json.key(b"message");
    json.object_start();
    json.key(b"role");
    json.string(b"user");
    json.key(b"content");
    json.array_start();
    for message in &prompt.messages {
        for block in &message.content {
            match block {
                llm::Block::Text { text, .. } => {
                    json.object_start();
                    json.key(b"type");
                    json.string(b"text");
                    json.key(b"text");
                    json.string(text);
                    json.object_end();
                }
                llm::Block::Opaque { .. }
                | llm::Block::Refusal { .. }
                | llm::Block::ToolCall { .. }
                | llm::Block::ToolResult { .. } => {}
            }
        }
    }
    json.array_end();
    json.object_end();
    json.object_end();
}

// The compact request with "smith-" and twenty digits, including its newline.
pub(super) const INTERRUPT_BYTES: u64 = 103;

// "smith-" and the largest u64's twenty decimal digits.
pub(super) fn interrupt_id(sequence: u64) -> Box<[u8]> {
    let digits = Decimal::of(sequence);
    let mut writer = Writer::new(6_usize.checked_add(digits.as_bytes().len()).expect("interrupt id"));
    writer.put(b"smith-").expect("id prefix");
    writer.put(digits.as_bytes()).expect("id digits");
    writer.finish()
}

pub(super) fn interrupt_request(sequence: u64, maximum: u32) -> Option<Box<[u8]>> {
    let id = interrupt_id(sequence);
    let limits = skein_json::writer::Limits { depth: 8, length: maximum.checked_sub(1)? };
    let mut measure = Encoder::measure(&limits);
    interrupt(&mut measure, &id);
    let length = measure.measured().ok()?;
    let mut write = Encoder::write(length, &limits);
    interrupt(&mut write, &id);
    Some(json::join(&[&write.finish(), b"\n"]))
}

fn interrupt(json: &mut Encoder, id: &[u8]) {
    json.object_start();
    json.key(b"type");
    json.string(b"control_request");
    json.key(b"request_id");
    json.string(id);
    json.key(b"request");
    json.object_start();
    json.key(b"subtype");
    json.string(b"interrupt");
    json.object_end();
    json.object_end();
}
