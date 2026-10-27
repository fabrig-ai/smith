//! Built-in stream correlation (protocol/programs.md 5.4–5.5).
//! Uses the bounded call ledger: a result waits for its call's Completed owner.

use super::{Backend, Below, Phase, policy, stream};
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_json::{Document, Kind};
use skein_lib::{Env, List, Queue, Token};
use smith_domain::{llm, tools::Grants};
use smith_protocol_llm::{self as protocol, FromDomain, ToDomain};

impl Backend {
    pub(super) fn grants(&self) -> Grants {
        match &self.request {
            Some(FromDomain::Complete { prompt, .. }) => prompt.tools,
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => {
                Grants { inspect: false, modify: false, shell: false }
            }
        }
    }

    pub(super) fn admit_init(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        bytes: &[u8],
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if let Some(detail) = self.init_problem(bytes) {
            self.fail(env, llm::Failure::Program { exit: None }, &detail, up, below);
            return;
        }
        self.phase = Phase::Running;
        if !self.compaction() {
            self.remember_session();
        }
        self.completion_deadline(env, true);
    }

    pub(super) fn later_init(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        bytes: &[u8],
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if let Some(detail) = self.init_problem(bytes) {
            self.fail(env, llm::Failure::Program { exit: None }, &detail, up, below);
        }
    }

    fn init_problem(&self, bytes: &[u8]) -> Option<Box<[u8]>> {
        let depth = self.settings.adapter.client.depth;
        let tokens = self.settings.stream_tokens;
        if self.compaction() {
            if super::compact::checked_init(bytes, depth, tokens).is_some() {
                return None;
            }
            return Some(b"smith: summary program offered tools".as_slice().into());
        }
        let error = init_error(bytes, self.grants(), depth, tokens);
        if error.is_none() && stream::checked_init(bytes, &self.tools(), depth, tokens).is_some() {
            None
        } else {
            Some(match error {
                Some(detail) => detail,
                None => b"smith: the program started without smith's tools".as_slice().into(),
            })
        }
    }

    pub(super) fn awaits_answer(&self, id: &[u8]) -> bool {
        for call in &self.ledger {
            if !call.builtin && !call.answered && call.id.as_ref() == id {
                return true;
            }
        }
        false
    }

    pub(super) fn builtin_owners(&mut self, owner: Token, completion: &llm::Completion) {
        for block in &completion.content {
            match block {
                llm::Said::ToolCall { id, .. } => {
                    for slot in 0..self.ledger.len() {
                        let call = self.ledger.get_mut(slot).expect("ledger slot");
                        if call.builtin && call.id == *id {
                            call.owner = Some(owner);
                        }
                    }
                }
                llm::Said::Text { .. } | llm::Said::Opaque { .. } | llm::Said::Refusal { .. } => {}
            }
        }
    }

    pub(super) fn flush_builtins(&mut self, up: &mut Queue<ToDomain>) {
        for slot in 0..self.ledger.len() {
            let call = self.ledger.get_mut(slot).expect("ledger slot");
            if call.builtin
                && let Some(owner) = call.owner
            {
                let (text, error) = match call.answer.take() {
                    Some(mcp::Answer::Text(text)) => (text, false),
                    Some(mcp::Answer::Error(text)) => (text, true),
                    None => continue,
                };
                call.answered = true;
                up.push(ToDomain::BuiltinResult { owner, id: call.id.clone(), text, error });
            }
        }
    }

    pub(super) fn builtin_results(&mut self, document: &Document, up: &mut Queue<ToDomain>) -> Option<()> {
        let message = json::field(document, 0, b"message")?;
        let content = json::field(document, message, b"content")?;
        if document.token(content)?.kind != Kind::ArrayStart {
            return Some(());
        }
        let mut index = content.checked_add(1)?;
        for _ in 0..document.len() {
            if document.token(index)?.kind == Kind::ArrayEnd {
                break;
            }
            if json::text_in(document, json::field(document, index, b"type")) == Some(b"tool_result") {
                let id = json::text_in(document, json::field(document, index, b"tool_use_id"))?;
                for slot in 0..self.ledger.len() {
                    let call = self.ledger.get(slot)?;
                    if call.builtin && call.id.as_ref() == id && !call.answered && call.answer.is_none() {
                        let text =
                            result_text(document, json::field(document, index, b"content")?, self.settings.line_bytes)?;
                        let error = match json::field(document, index, b"is_error") {
                            Some(value) => document.token(value)?.kind == Kind::True,
                            None => false,
                        };
                        if !self.reply_room(u64::try_from(text.len()).ok()?) {
                            return None;
                        }
                        self.ledger.get_mut(slot)?.answer =
                            Some(if error { mcp::Answer::Error(text) } else { mcp::Answer::Text(text) });
                    }
                }
            }
            index = json::after(document, index)?;
        }
        self.flush_builtins(up);
        Some(())
    }

    // Share the retained-reply budget. Reserve each MCP slot's full cap so
    // its later answer cannot consume bytes already held by a built-in.
    pub(super) fn reply_room(&self, extra: u64) -> bool {
        let maximum = u64::from(self.settings.adapter.rendered_result)
            .checked_mul(u64::from(self.settings.receiving.max_completion_blocks).checked_mul(5).expect("call slots"))
            .expect("reply budget");
        let mut retained = extra;
        for call in &self.ledger {
            let bytes = if call.builtin {
                match &call.answer {
                    Some(mcp::Answer::Text(text) | mcp::Answer::Error(text)) => {
                        u64::try_from(text.len()).expect("bounded result")
                    }
                    None => 0,
                }
            } else {
                u64::from(self.settings.adapter.rendered_result)
            };
            retained = match retained.checked_add(bytes) {
                Some(bytes) => bytes,
                None => return false,
            };
        }
        retained <= maximum
    }
}

pub(super) fn init_error(bytes: &[u8], grants: Grants, depth: u32, tokens: u32) -> Option<Box<[u8]>> {
    let document = stream::collect(bytes, depth, tokens)?;
    let listed = json::field(&document, 0, b"tools")?;
    let mut seen = [false; 6];
    let mut index = listed.checked_add(1)?;
    for _ in 0..document.len() {
        if document.token(index)?.kind == Kind::ArrayEnd {
            break;
        }
        let name = json::text(&document, index)?;
        if !name.starts_with(b"mcp__") {
            let mut granted = false;
            for (slot, tool) in policy::BUILTINS.iter().enumerate() {
                if tool.name() == name && tool.granted(grants) && !seen.get(slot).expect("six built-ins") {
                    *seen.get_mut(slot).expect("six built-ins") = true;
                    granted = true;
                }
            }
            if !granted {
                return Some(json::join(&[b"smith: the program offers a built-in tool the run did not grant: ", name]));
            }
        }
        index = json::after(&document, index)?;
    }
    for (slot, tool) in policy::BUILTINS.iter().enumerate() {
        if tool.granted(grants) && !seen.get(slot).expect("six built-ins") {
            return Some(json::join(&[b"smith: the program lacks a granted built-in tool: ", tool.name()]));
        }
    }
    None
}

fn result_text(document: &Document, content: u32, maximum: u32) -> Option<Box<[u8]>> {
    if document.token(content)?.kind == Kind::String {
        return Some(json::text(document, content)?.into());
    }
    if document.token(content)?.kind != Kind::ArrayStart {
        return None;
    }
    let mut text = List::with_capacity(maximum);
    let mut index = content.checked_add(1)?;
    let mut first = true;
    for _ in 0..document.len() {
        if document.token(index)?.kind == Kind::ArrayEnd {
            break;
        }
        if json::text_in(document, json::field(document, index, b"type")) == Some(b"text") {
            if !first {
                text.push(b'\n').ok()?;
            }
            first = false;
            for byte in json::text_in(document, json::field(document, index, b"text"))? {
                text.push(*byte).ok()?;
            }
        }
        index = json::after(document, index)?;
    }
    Some(text.into_boxed())
}
