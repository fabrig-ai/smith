//! API message assembly (protocol/programs.md, section 5.5). Keeps one
//! message and four waiting completions; never derives usage from a result.
//! `StructuredOutput` waits for its outcome and becomes finish in the same slot;
//! it is never a workspace built-in (protocol/programs.md, section 5.5).

use crate::mcp::json;
use alloc::boxed::Box;
use skein_json::{Document, Kind};
use skein_lib::{List, Queue};
use smith_domain::llm;

// Marks adapter-owned finish feedback, which is never an MCP reply.
pub(super) const STRUCTURED_FINISH: &[u8] = b"smith-structured-finish";

pub(super) struct Message {
    id: Box<[u8]>,
    framed: bool,
    stopped: bool,
    expected: u32,
    received: u32,
    blocks: List<llm::Said>,
    usage: llm::Usage,
    bytes: u64,
    raw: u32,
}

pub(super) struct Turns {
    pub message: Option<Message>,
    pub ready: Queue<llm::Completion>,
    pub result: bool,
    pub seen: bool,
    pub assistant_events: u64,
    pub closed: u64,
    pub finish: bool,
    pub blocks: u32,
    pub maximum: u64,
    pub maximum_raw: u32,
}

impl Turns {
    pub fn open_blocks(&self) -> Option<&[llm::Said]> {
        match &self.message {
            Some(message) => Some(message.blocks.as_slice()),
            None => None,
        }
    }

    pub fn new(bounds: &smith_protocol_llm::Receiving, maximum_raw: u32) -> Self {
        Self {
            message: None,
            ready: Queue::with_capacity(4),
            result: false,
            seen: false,
            assistant_events: 0,
            closed: 0,
            finish: false,
            blocks: bounds.max_completion_blocks,
            maximum: bounds.max_completion_bytes,
            maximum_raw,
        }
    }

    fn begin(&mut self, id: &[u8], framed: bool) -> Option<()> {
        if id.len() > 1024 || self.message.is_some() {
            return None;
        }
        if self.result {
            self.finish = false;
        }
        self.result = false;
        self.seen = true;
        self.message = Some(Message {
            id: id.into(),
            framed,
            stopped: false,
            expected: 0,
            received: 0,
            blocks: List::with_capacity(self.blocks),
            usage: llm::Usage::NONE,
            bytes: 0,
            raw: 0,
        });
        Some(())
    }

    pub fn event(&mut self, document: &Document, event: u32) -> Option<()> {
        match json::text_in(document, json::field(document, event, b"type"))? {
            b"message_start" => {
                self.close(true)?;
                let id =
                    json::text_in(document, json::field_in(document, json::field(document, event, b"message"), b"id"))?;
                self.begin(id, true)
            }
            b"content_block_start" => {
                let index = number(document, json::field(document, event, b"index"))?;
                let expected = u32::try_from(index).ok()?.checked_add(1)?;
                if expected > self.blocks {
                    return None;
                }
                let message = self.message.as_mut()?;
                message.expected = message.expected.max(expected);
                Some(())
            }
            b"message_stop" => {
                self.message.as_mut()?.stopped = true;
                self.close(false)
            }
            b"message_delta" => {
                // The block records repeat the initial usage; the final delta
                // supplies the message's total, including its thinking tokens.
                if let Some(tokens) = json::field(document, event, b"usage") {
                    self.message.as_mut()?.usage = usage(document, Some(tokens));
                }
                Some(())
            }
            b"content_block_delta" | b"content_block_stop" => Some(()),
            _ => None,
        }
    }

    pub fn assistant(&mut self, document: &Document, maximum: u32) -> Option<u32> {
        self.assistant_events = self.assistant_events.checked_add(1)?;
        let value = json::field(document, 0, b"message")?;
        let id = json::text_in(document, json::field(document, value, b"id"))?;
        // programs.md 5.5: an assistant ID without message_start is an
        // atomic program-authored message. Framed IDs still wait for stop
        // and every block, including when the full content precedes stop.
        if let Some(message) = &self.message
            && message.id.as_ref() != id
        {
            self.close(true)?;
        }
        if self.message.is_none() {
            self.begin(id, false)?;
        }
        let message = self.message.as_mut()?;
        if message.id.as_ref() != id {
            return None;
        }
        let content = json::field(document, value, b"content")?;
        if document.token(content)?.kind != Kind::ArrayStart {
            return None;
        }
        let mut index = content.checked_add(1)?;
        let mut text_bytes = 0_u32;
        for _ in 0..document.len() {
            if document.token(index)?.kind == Kind::ArrayEnd {
                break;
            }
            message.received = message.received.checked_add(1)?;
            if message.received > self.blocks {
                return None;
            }
            let kind = json::text_in(document, json::field(document, index, b"type"))?;
            let block = match kind {
                b"text" => {
                    let text = json::text_in(document, json::field(document, index, b"text"))?;
                    text_bytes = text_bytes.checked_add(u32::try_from(text.len()).ok()?)?;
                    llm::Said::Text { text: text.into(), replay: None }
                }
                b"tool_use" => {
                    let name = json::text_in(document, json::field(document, index, b"name"))?;
                    let builtin = !name.starts_with(b"mcp__");
                    let name = name.strip_prefix(b"mcp__smith__").unwrap_or(name);
                    let input = json::raw(document, json::field(document, index, b"input")?, maximum)?;
                    if !builtin && name == b"finish" {
                        self.finish = true;
                    }
                    let provider_id = json::text_in(document, json::field(document, index, b"id"))?;
                    llm::Said::ToolCall {
                        id: if builtin && name == b"StructuredOutput" {
                            STRUCTURED_FINISH.into()
                        } else {
                            provider_id.into()
                        },
                        name: name.into(),
                        input,
                        call: if builtin && name != b"StructuredOutput" {
                            llm::Decoded::Builtin {
                                effect: match super::policy::Builtin::named(name) {
                                    Some(tool) => tool.effect(),
                                    None => smith_domain::tools::Effect::Write,
                                },
                            }
                        } else {
                            llm::Decoded::Invalid { problem: llm::Problem::UnknownTool }
                        },
                        replay: None,
                    }
                }
                b"thinking" | b"redacted_thinking" => {
                    index = json::after(document, index)?;
                    continue;
                }
                _ => return None,
            };
            let payload = match &block {
                llm::Said::Text { text, .. } | llm::Said::Refusal { text, .. } => text.len(),
                llm::Said::ToolCall { id, name, input, .. } => {
                    id.len().checked_add(name.len())?.checked_add(input.len())?
                }
                llm::Said::Opaque { bytes } => bytes.len(),
            };
            message.raw = message.raw.checked_add(u32::try_from(payload).ok()?)?;
            if message.raw > self.maximum_raw {
                return None;
            }
            message.bytes =
                message.bytes.checked_add(u64::try_from(size_of::<llm::Said>().checked_add(payload)?).ok()?)?;
            if message.bytes > self.maximum {
                return None;
            }
            message.blocks.push(block).ok()?;
            index = json::after(document, index)?;
        }
        message.usage = usage(document, json::field(document, value, b"usage"));
        if !message.framed {
            message.stopped = true;
            message.expected = message.received;
        }
        self.close(false)?;
        Some(text_bytes)
    }

    pub fn close(&mut self, boundary: bool) -> Option<()> {
        let Some(message) = &self.message else {
            return Some(());
        };
        if !message.stopped || message.received < message.expected {
            return if boundary { None } else { Some(()) };
        }
        let mut calls = false;
        let mut structured = false;
        for block in &message.blocks {
            match block {
                llm::Said::ToolCall { id, name, .. } => {
                    calls = true;
                    structured |= id.as_ref() == STRUCTURED_FINISH && name.as_ref() == b"StructuredOutput";
                }
                llm::Said::Text { .. } | llm::Said::Refusal { .. } | llm::Said::Opaque { .. } => {}
            }
        }
        // StructuredOutput's result supplies the finish input for this same
        // model completion (programs.md 5.5), never an extra budgeted turn.
        if structured {
            return if boundary { None } else { Some(()) };
        }
        if !calls && !boundary {
            return Some(());
        }
        if self.ready.room() == 0 {
            return None;
        }
        let message = self.message.take()?;
        self.closed = self.closed.checked_add(1)?;
        self.ready.push(llm::Completion {
            reasoning_dropped: Box::default(),
            content: message.blocks.into_boxed(),
            stop: if calls { llm::Stop::ToolUse } else { llm::Stop::EndTurn },
            usage: message.usage,
        });
        Some(())
    }

    // programs.md 5.5: a successful result without an assistant still hands
    // the no-tool turn to the domain, which owns its nudge allowance.
    pub fn end_empty(&mut self) -> Option<()> {
        if self.assistant_events == 0 && self.ready.is_empty() {
            if self.ready.room() == 0 {
                return None;
            }
            self.closed = self.closed.checked_add(1)?;
            self.ready.push(llm::Completion {
                reasoning_dropped: Box::default(),
                content: Box::default(),
                stop: llm::Stop::EndTurn,
                usage: llm::Usage::ZERO,
            });
        }
        Some(())
    }

    pub fn interrupted(&mut self) {
        if self.close(true).is_none() {
            self.message = None;
        }
        if self.ready.is_empty() {
            self.assistant_events = 0;
            self.end_empty().expect("one interrupt terminal has completion room");
        }
        self.result = true;
    }

    pub fn accepting_calls(&self) -> bool {
        match &self.message {
            Some(message) => !message.stopped || message.received < message.expected,
            None => !self.seen && !self.result,
        }
    }

    pub fn matching_floor(&self) -> Option<u64> {
        if self.message.is_some() && !self.accepting_calls() { self.closed.checked_add(1) } else { Some(self.closed) }
    }

    pub fn retry(&mut self) {
        self.message = None;
        self.seen = false;
        self.restart();
    }

    pub fn restart(&mut self) {
        self.result = false;
        self.finish = false;
        self.assistant_events = 0;
    }

    pub fn structured(&mut self, input: Box<[u8]>) -> Option<()> {
        if self.finish {
            return self.close(true);
        }
        let block = llm::Said::ToolCall {
            id: STRUCTURED_FINISH.into(),
            name: b"finish".as_slice().into(),
            input,
            call: llm::Decoded::Invalid { problem: llm::Problem::UnknownTool },
            replay: None,
        };
        if let Some(message) = &mut self.message {
            let mut position = None;
            for (index, held) in message.blocks.iter().enumerate() {
                match held {
                    llm::Said::ToolCall { id, name, .. }
                        if id.as_ref() == STRUCTURED_FINISH && name.as_ref() == b"StructuredOutput" =>
                    {
                        if position.is_some() {
                            return None;
                        }
                        position = Some(u32::try_from(index).ok()?);
                    }
                    llm::Said::ToolCall { .. }
                    | llm::Said::Text { .. }
                    | llm::Said::Refusal { .. }
                    | llm::Said::Opaque { .. } => {}
                }
            }
            match position {
                Some(position) => *message.blocks.get_mut(position)? = block,
                None => message.blocks.push(block).ok()?,
            }
            self.close(true)?;
        } else {
            if self.ready.room() == 0 {
                return None;
            }
            self.closed = self.closed.checked_add(1)?;
            self.ready.push(llm::Completion {
                reasoning_dropped: Box::default(),
                content: Box::new([block]),
                stop: llm::Stop::ToolUse,
                usage: llm::Usage::ZERO,
            });
        }
        self.finish = true;
        Some(())
    }
}

pub(super) fn number(document: &Document, index: Option<u32>) -> Option<u64> {
    let record = document.token(index?)?;
    if record.kind != Kind::Number {
        return None;
    }
    let mut value = 0_u64;
    for byte in document.text(record)? {
        if !byte.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(u64::from(byte.checked_sub(b'0')?))?;
    }
    Some(value)
}

fn usage(document: &Document, index: Option<u32>) -> llm::Usage {
    llm::Usage {
        input_tokens: number(document, json::field_in(document, index, b"input_tokens")),
        output_tokens: number(document, json::field_in(document, index, b"output_tokens")),
        cache_read_tokens: number(document, json::field_in(document, index, b"cache_read_input_tokens")),
        cache_write_tokens: number(document, json::field_in(document, index, b"cache_creation_input_tokens")),
        reasoning_tokens: None,
    }
}
