//! One user message for a program's tail (protocol/programs.md 5.7–5.8).
//! Borrows messages after the last assistant, measuring escaped bytes before
//! allocating the exact JSON line, including an optional agent pause reason
//! (protocol/programs.md 5.9). No unescaped tail or quoted copy is kept.
//! An opted-in capture is the queued Prompt's separately priced owned payload;
//! it moves into the measure without another text copy.

use super::calls;
use alloc::boxed::Box;
use skein_lib::List;
use smith_domain::llm;
use smith_protocol_llm as protocol;

const OPEN: &[u8] = b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"";
const CLOSE: &[u8] = b"\"}}\n";

pub(super) struct Rendered {
    pub line: Box<[u8]>,
    pub text: Option<Box<[u8]>>,
}

#[cfg(test)]
pub(super) fn render(
    prompt: &mut llm::Prompt,
    ledger: &List<calls::Call>,
    limits: &protocol::Limits,
    maximum: u32,
) -> Option<Box<[u8]>> {
    let Rendered { line, .. } = render_observed(prompt, ledger, limits, maximum, b"", false)?;
    Some(line)
}

#[expect(clippy::manual_map, reason = "the protocol subset matches Option without callbacks")]
pub(super) fn render_observed(
    prompt: &mut llm::Prompt,
    ledger: &List<calls::Call>,
    limits: &protocol::Limits,
    maximum: u32,
    prefix: &[u8],
    capture: bool,
) -> Option<Rendered> {
    let mut from = 0;
    for (index, message) in prompt.messages.iter().enumerate() {
        if message.role == llm::Role::Assistant {
            from = index.checked_add(1)?;
        }
    }
    let mut messages = core::mem::replace(&mut prompt.messages, Box::new([]));
    let mut rendered = 0_u32;
    for message in messages.iter_mut().skip(from) {
        for block in &mut message.content {
            match block {
                llm::Block::ToolResult { result, .. } => match result {
                    llm::Returned::Served { .. } => {
                        let result_value = core::mem::replace(result, llm::Returned::Withdrawn);
                        let (text, error) = match calls::render(result_value, prompt, limits)? {
                            crate::mcp::Answer::Text(text) => (text, false),
                            crate::mcp::Answer::Error(text) => (text, true),
                        };
                        rendered = rendered.checked_add(u32::try_from(text.len()).ok()?)?;
                        if rendered > maximum {
                            return None;
                        }
                        *result = llm::Returned::Text { text, error, replay: None };
                    }
                    llm::Returned::Text { .. }
                    | llm::Returned::Owned { .. }
                    | llm::Returned::Withdrawn
                    | llm::Returned::Invalid { .. }
                    | llm::Returned::NotRun { .. } => {}
                },
                llm::Block::Text { .. }
                | llm::Block::ToolCall { .. }
                | llm::Block::Opaque { .. }
                | llm::Block::Refusal { .. } => {}
            }
        }
    }
    let mut measured = Output { length: 0, maximum, bytes: None, text_length: 0, capture: None };
    if message(&mut measured, &messages, prompt, from, ledger, limits, prefix).is_none() {
        prompt.messages = messages;
        return None;
    }
    let mut output = Output {
        length: 0,
        maximum: measured.length,
        bytes: Some(List::with_capacity(measured.length)),
        text_length: 0,
        capture: if capture { Some(List::with_capacity(measured.text_length)) } else { None },
    };
    message(&mut output, &messages, prompt, from, ledger, limits, prefix)?;
    prompt.messages = Box::new([]);
    let text = match output.capture {
        Some(text) => Some(text.into_boxed()),
        None => None,
    };
    Some(Rendered { line: output.bytes?.into_boxed(), text })
}

fn message(
    output: &mut Output,
    messages: &[llm::Message],
    prompt: &llm::Prompt,
    from: usize,
    ledger: &List<calls::Call>,
    limits: &protocol::Limits,
    prefix: &[u8],
) -> Option<()> {
    output.put(OPEN)?;
    output.text(prefix)?;
    for message in messages.iter().skip(from) {
        for block in &message.content {
            match block {
                llm::Block::ToolResult { id, result } => {
                    let mut name = id.as_ref();
                    for call in ledger {
                        if call.id == *id {
                            name = &call.name;
                        }
                    }
                    output.text(b"result of ")?;
                    output.text(name)?;
                    output.text(b": ")?;
                    match result {
                        llm::Returned::Text { text, .. } => {
                            if text.len() > usize::try_from(limits.rendered_result).ok()? {
                                return None;
                            }
                            output.text(text)?;
                        }
                        llm::Returned::Owned { .. }
                        | llm::Returned::Served { .. }
                        | llm::Returned::Withdrawn
                        | llm::Returned::Invalid { .. }
                        | llm::Returned::NotRun { .. } => match calls::render_unserved(result, prompt, limits)? {
                            crate::mcp::Answer::Text(text) | crate::mcp::Answer::Error(text) => output.text(&text)?,
                        },
                    }
                    output.text(b"\n")?;
                }
                llm::Block::Text { text, .. } => {
                    output.text(text)?;
                    output.text(b"\n")?;
                }
                llm::Block::ToolCall { .. } | llm::Block::Opaque { .. } | llm::Block::Refusal { .. } => {}
            }
        }
    }
    output.put(CLOSE)
}

struct Output {
    length: u32,
    maximum: u32,
    bytes: Option<List<u8>>,
    text_length: u32,
    capture: Option<List<u8>>,
}

impl Output {
    fn put(&mut self, bytes: &[u8]) -> Option<()> {
        self.length = self.length.checked_add(u32::try_from(bytes.len()).ok()?)?;
        if self.length > self.maximum {
            return None;
        }
        if let Some(output) = &mut self.bytes {
            for byte in bytes {
                output.push(*byte).ok()?;
            }
        }
        Some(())
    }

    // Encoder::string_parts requires all pieces together. Result renderings
    // are temporary, so escape each borrowed piece directly (RFC 8259, §7).
    #[expect(
        clippy::disallowed_methods,
        reason = "protocol/programs.md 5.7: the JSON string encoder checks UTF-8 before output"
    )]
    fn text(&mut self, bytes: &[u8]) -> Option<()> {
        core::str::from_utf8(bytes).ok()?;
        self.text_length = self.text_length.checked_add(u32::try_from(bytes.len()).ok()?)?;
        if let Some(capture) = &mut self.capture {
            for byte in bytes {
                capture.push(*byte).ok()?;
            }
        }
        for byte in bytes {
            match byte {
                b'"' => self.put(br#"\""#)?,
                b'\\' => self.put(br"\\")?,
                b'\n' => self.put(br"\n")?,
                b'\r' => self.put(br"\r")?,
                b'\t' => self.put(br"\t")?,
                8 => self.put(br"\b")?,
                12 => self.put(br"\f")?,
                0..=31 => {
                    let hex = b"0123456789abcdef";
                    self.put(&[
                        b'\\',
                        b'u',
                        b'0',
                        b'0',
                        *hex.get(usize::from(byte.checked_div(16)?))?,
                        *hex.get(usize::from(byte.checked_rem(16)?))?,
                    ])?;
                }
                _ => self.put(&[*byte])?,
            }
        }
        Some(())
    }
}
