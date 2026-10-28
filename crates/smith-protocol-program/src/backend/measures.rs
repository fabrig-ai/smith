//! Program content observations (protocol/events.md 3.5, 3.10–3.11).
//! Holds one hand-off's new user text or one stream line's measures. No native
//! transport timings are inferred. The service drains through `pop_measure`.

use super::{Backend, policy, turns};
use crate::mcp::json;
use alloc::boxed::Box;
use skein_json::Document;
use skein_lib::{List, Time, Token};
use smith_domain::llm;
use smith_events::{Block, Completion, Message, Prompt, Role};
use smith_protocol_llm::{self as protocol, FromDomain, Measure};

pub(super) const MEASURES: u32 = 16;
const PROGRAM_TOOLS: usize = policy::BUILTINS.len() + 1;

pub(super) fn capacity(settings: &super::Settings) -> Option<u32> {
    MEASURES.checked_add(settings.receiving.max_completion_blocks)
}

impl Backend {
    /// Drain one bounded observation before consuming the corresponding domain terminal.
    pub fn pop_measure(&mut self) -> Option<Measure> {
        self.measures.pop()
    }

    pub(super) fn begin_prompt(&mut self, request: &FromDomain) {
        self.prompt_pending = None;
        self.prompt_due = match request {
            FromDomain::Complete { observation, .. } => observation.is_some(),
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => false,
        };
    }

    pub(super) fn first_prompt_measure(&mut self, _at: Time) {
        let prompt = match &self.request {
            Some(FromDomain::Complete { prompt, observation: Some(_), .. }) => prompt,
            Some(
                FromDomain::Complete { observation: None, .. } | FromDomain::Cancel { .. } | FromDomain::Yield { .. },
            )
            | None => return,
        };
        let mut count = 0_u32;
        for message in &prompt.messages {
            for block in &message.content {
                match block {
                    llm::Block::Text { .. } => count = count.checked_add(1).expect("admitted prompt blocks"),
                    llm::Block::Opaque { .. }
                    | llm::Block::Refusal { .. }
                    | llm::Block::ToolCall { .. }
                    | llm::Block::ToolResult { .. } => {}
                }
            }
        }
        let mut blocks = List::with_capacity(count);
        for message in &prompt.messages {
            for block in &message.content {
                match block {
                    llm::Block::Text { text, .. } => {
                        blocks.push(Block::Text { text: text.clone() }).expect("counted text");
                    }
                    llm::Block::Opaque { .. }
                    | llm::Block::Refusal { .. }
                    | llm::Block::ToolCall { .. }
                    | llm::Block::ToolResult { .. } => {}
                }
            }
        }
        self.prompt_pending = Some(blocks);
    }

    pub(super) fn prompt_measure(&mut self, at: Time, text: Option<&[u8]>) {
        if !self.prompt_due {
            return;
        }
        match text {
            None => {}
            Some(text) => self.prompt_text_measure(at, text.into()),
        }
    }

    pub(super) fn prompt_text_measure(&mut self, _at: Time, text: Box<[u8]>) {
        if !self.prompt_due {
            return;
        }
        let mut blocks = List::with_capacity(1);
        blocks.push(Block::Text { text }).expect("one user text");
        self.prompt_pending = Some(blocks);
    }

    pub(super) fn flush_prompt_measure(&mut self, at: Time) {
        let blocks = self.prompt_pending.take();
        self.offer_prompt(at, blocks);
    }

    fn offer_prompt(&mut self, at: Time, blocks: Option<List<Block>>) {
        if !core::mem::replace(&mut self.prompt_due, false) {
            return;
        }
        let (owner, prompt, observation) = match &self.request {
            Some(FromDomain::Complete { owner, prompt, observation: Some(observation), .. }) => {
                (*owner, prompt, observation)
            }
            Some(
                FromDomain::Complete { observation: None, .. } | FromDomain::Cancel { .. } | FromDomain::Yield { .. },
            )
            | None => return,
        };
        let (system, tools) = if self.compaction() {
            (Some(prompt.system.clone()), Some(List::with_capacity(0)))
        } else if observation.first {
            let server = self.tools();
            let mut names = List::with_capacity(
                u32::try_from(server.len())
                    .expect("bounded names")
                    .checked_add(u32::try_from(PROGRAM_TOOLS).expect("six built-ins plus permission tool"))
                    .expect("built-ins and permission tool"),
            );
            for tool in server {
                names.push(tool.name).expect("server inventory");
            }
            names.push(crate::mcp::PROMPT_TOOL.into()).expect("permission tool");
            for tool in policy::BUILTINS {
                if tool.granted(prompt.tools) {
                    names.push(tool.name().into()).expect("six built-ins");
                }
            }
            (Some(prompt.system.clone()), Some(names))
        } else {
            (None, None)
        };
        let mut messages = List::with_capacity(1);
        if let Some(blocks) = blocks {
            messages.push(Message { role: Role::User, blocks }).expect("one sent user line");
        }
        self.measures.push(Measure::Prompt { at, owner, prompt: Prompt { system, tools, messages } });
    }

    pub(super) fn terminal_measure(&mut self, at: Time, owner: Token) {
        self.flush_deltas(at);
        self.flush_prompt_measure(at);
        self.measures.push(Measure::Terminal { owner, measures: protocol::Measures::none() });
    }

    pub(super) fn completion_measure(&mut self, at: Time, owner: Token, completion: &llm::Completion) {
        self.flush_prompt_measure(at);
        match &self.request {
            Some(FromDomain::Complete { observation: Some(_), .. }) => {}
            Some(
                FromDomain::Complete { observation: None, .. } | FromDomain::Cancel { .. } | FromDomain::Yield { .. },
            )
            | None => return,
        }
        let mut blocks = List::with_capacity(u32::try_from(completion.content.len()).expect("bounded blocks"));
        for block in &completion.content {
            let block = match block {
                llm::Said::Text { text, .. } => Block::Text { text: text.clone() },
                llm::Said::Refusal { text, .. } => Block::Refusal { text: text.clone() },
                llm::Said::ToolCall { id, name, input, .. } => {
                    Block::Call { call: id.clone(), tool: name.clone(), input: input.clone() }
                }
                llm::Said::Opaque { bytes } => {
                    Block::Opaque { bytes: u64::try_from(bytes.len()).expect("bounded opaque") }
                }
            };
            blocks.push(block).expect("one capture per block");
        }
        self.measures.push(Measure::Completion { at, owner, completion: Completion { blocks } });
    }

    pub(super) fn partial_measure(&mut self, at: Time, document: &Document, event: u32) -> Option<()> {
        let owner = match &self.request {
            Some(FromDomain::Complete { owner, deltas: protocol::Deltas { text: true, .. }, .. }) => *owner,
            Some(
                FromDomain::Complete { deltas: protocol::Deltas { text: false, .. }, .. }
                | FromDomain::Cancel { .. }
                | FromDomain::Yield { .. },
            )
            | None => return Some(()),
        };
        if self.owner != Some(owner) {
            return Some(());
        }
        match json::text_in(document, json::field(document, event, b"type")) {
            Some(b"content_block_stop") => {
                let block = u32::try_from(turns::number(document, json::field(document, event, b"index"))?).ok()?;
                self.flush_delta(at, block);
                return Some(());
            }
            Some(b"message_stop") => {
                self.flush_deltas(at);
                return Some(());
            }
            Some(b"content_block_delta") => {}
            Some(_) | None => return Some(()),
        }
        let delta = json::field(document, event, b"delta");
        if json::text_in(document, json::field_in(document, delta, b"type")) != Some(b"text_delta") {
            return Some(());
        }
        let text = json::text_in(document, json::field_in(document, delta, b"text"))?;
        let block = u32::try_from(turns::number(document, json::field(document, event, b"index"))?).ok()?;
        if text.len() > usize::try_from(self.settings.adapter.client.answer).ok()?
            || block >= self.settings.receiving.max_completion_blocks
        {
            return None;
        }
        self.text_delta(at, owner, block, text)
    }

    pub(super) fn retry_measure(&mut self, at: Time, document: &Document) {
        self.flush_prompt_measure(at);
        self.flush_deltas(at);
        if let Some(owner) = self.owner {
            let status = match turns::number(document, json::field(document, 0, b"error_status")) {
                Some(status) => u16::try_from(status).ok(),
                None => None,
            };
            let retry_ms = turns::number(document, json::field(document, 0, b"retry_delay_ms"));
            self.measures.push(Measure::Retried { at, owner, status, retry_ms });
        }
    }
}
