//! Conversation boundary driving (protocol/programs.md, sections 5.5–5.7).
//! Keeps correlation separately from delivered domain blocks; the domain alone
//! accepts outcomes; cancellation fails unanswered calls before settling the child.

use super::{Backend, Below, Phase, calls, stream, turns};
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_json::Kind;
use skein_lib::{Env, List, Queue};
use smith_domain::{llm, run};
use smith_protocol_llm::{self as protocol, FromDomain, ToDomain};

impl Backend {
    pub(super) fn resume(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        request: FromDomain,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let owner = match &request {
            FromDomain::Complete { owner, .. } => *owner,
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => unreachable!("completion route"),
        };
        self.begin_prompt(&request);
        self.owner = Some(owner);
        if self.exit.is_some() || self.child.is_none() || self.pipes[0].is_none() {
            self.fail(
                env,
                llm::Failure::Program { exit: self.exit },
                b"smith: the program is no longer accepting input",
                up,
                below,
            );
            return;
        }
        self.permitted = self
            .turns
            .closed
            .checked_sub(u64::from(self.turns.ready.len()))
            .expect("delivered prefix")
            .checked_add(1)
            .expect("permitted message");
        let Some(mut request) = self.wind_down(env, request, up, below) else {
            return;
        };
        let (prompt, capture) = match &mut request {
            FromDomain::Complete { prompt, observation, .. } => (prompt, observation.is_some()),
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => unreachable!("completion route"),
        };
        let messages = core::mem::replace(&mut prompt.messages, Box::new([]));
        let Some(resumed) = super::replies::resume(self, messages, prompt, capture) else {
            self.fail(
                env,
                llm::Failure::Invalid,
                b"smith: a program tool result exceeds its receiving bound",
                up,
                below,
            );
            return;
        };
        self.request = Some(request);
        let mut sent = resumed.observed;
        self.completion_deadline(env, true);
        self.answer_calls();
        self.match_calls();
        // A later API message may already be in flight. Only a completed
        // program turn needs the domain's new text (message or nudge) as a user line.
        if let Some(line) = resumed.line {
            if self.turns.result && self.turns.ready.is_empty() && self.turns.message.is_none() {
                self.outgoing = Some(line);
                self.turns.restart();
                self.send_input();
            } else {
                sent = None;
            }
        }
        self.prompt_measure(env.now, sent.as_deref());
        self.deliver(env, up, below);
        self.continue_ended(env, up, below);
    }

    #[expect(clippy::too_many_lines, reason = "one exhaustive stream grammar keeps terminal routing in one entrance")]
    pub(super) fn line(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        bytes: &[u8],
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let Some(document) = stream::collect(bytes, self.settings.adapter.client.depth, self.settings.stream_tokens)
        else {
            self.fail(
                env,
                llm::Failure::Program { exit: self.exit },
                b"smith: the program stream does not decode",
                up,
                below,
            );
            return;
        };
        let kind = json::text_in(&document, json::field(&document, 0, b"type"));
        if kind == Some(b"control_response") {
            if !self.control_response(&document) {
                self.fail(env, llm::Failure::Invalid, b"smith: the program refused the interrupt", up, below);
            }
            return;
        }
        if self.stop != super::stop::Stop::None {
            if kind == Some(b"result") {
                // Note rendering reuses the parser's workspace term.
                drop(document);
                self.stopped_result(env, up, below);
                self.deliver(env, up, below);
            }
            return;
        }
        if self.switching {
            return;
        }
        if kind == Some(b"stream_event") {
            self.flush_prompt_measure(env.now);
        }
        let result = match kind {
            Some(b"stream_event") => match json::field(&document, 0, b"event") {
                Some(event) => match self.partial_measure(env.now, &document, event) {
                    Some(()) => self.turns.event(&document, event),
                    None => None,
                },
                None => None,
            },
            Some(b"assistant") => match self.turns.assistant(&document, self.settings.adapter.client.answer) {
                Some(_) => Some(()),
                None => None,
            },
            Some(b"result") => {
                if let Some(failure) = result_failure(&document, env.wall) {
                    self.turns.result = true;
                    self.fail(env, failure, b"smith: the program reported a failed result", up, below);
                    return;
                }
                if self.turns.result {
                    self.fail(
                        env,
                        llm::Failure::Program { exit: self.exit },
                        b"smith: a program turn repeated its result",
                        up,
                        below,
                    );
                    return;
                }
                self.turns.result = true;
                let structured = match &self.request {
                    Some(FromDomain::Complete { prompt, .. }) => prompt.served.contains(&llm::Served::Finish),
                    Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => false,
                };
                let completed = match json::field(&document, 0, b"structured_output") {
                    Some(index) if structured && token_kind(&document, Some(index)) != Some(Kind::Null) => {
                        match json::raw(&document, index, self.settings.adapter.client.answer) {
                            Some(input) => self.turns.structured(input),
                            None => None,
                        }
                    }
                    Some(_) | None => self.turns.close(true),
                };
                match completed {
                    Some(()) => self.turns.end_empty(),
                    None => None,
                }
            }
            Some(b"rate_limit_event") => {
                let info = json::field(&document, 0, b"rate_limit_info");
                if json::text_in(&document, json::field_in(&document, info, b"status")) == Some(b"rejected".as_slice())
                {
                    let retry_after = retry_after(&document, env.wall);
                    self.fail(env, llm::Failure::Exhausted { retry_after }, b"smith: program usage limit", up, below);
                    return;
                }
                Some(())
            }
            Some(b"system") => match json::text_in(&document, json::field(&document, 0, b"subtype")) {
                Some(b"status") => return,
                Some(b"init") => {
                    // Admission collects the line itself; reuse the one-document
                    // workspace rather than retaining this dispatch document too.
                    drop(document);
                    self.later_init(env, bytes, up, below);
                    return;
                }
                Some(b"compact_boundary" | b"compaction") => Some(()),
                Some(b"api_retry") => {
                    self.retry_measure(env.now, &document);
                    self.turns.retry();
                    let old = core::mem::replace(
                        &mut self.ledger,
                        List::with_capacity(
                            self.settings.receiving.max_completion_blocks.checked_mul(5).expect("call slots"),
                        ),
                    );
                    for call in old.into_boxed() {
                        if call.message <= self.turns.closed {
                            assert!(self.ledger.push(call).is_ok(), "retained delivered calls");
                        }
                    }
                    Some(())
                }
                Some(_) | None => None,
            },
            Some(b"user") => self.builtin_results(&document, up),
            Some(_) | None => None,
        };
        if result.is_none() {
            let failure = if self.turns.ready.room() == 0 {
                llm::Failure::Invalid
            } else {
                llm::Failure::Program { exit: self.exit }
            };
            self.fail(env, failure, b"smith: invalid program message or completion backlog", up, below);
            return;
        }
        if self.register_calls().is_none() {
            self.fail(env, llm::Failure::Invalid, b"smith: program call ledger is full", up, below);
            return;
        }
        self.match_calls();
        self.completion_deadline(env, false);
        self.deliver(env, up, below);
        self.continue_ended(env, up, below);
    }

    fn continue_ended(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if self.owner.is_some() && self.turns.result && self.turns.ready.is_empty() && self.turns.message.is_none() {
            if self.exit.is_some() || self.child.is_none() || self.pipes[0].is_none() {
                self.fail(
                    env,
                    llm::Failure::Program { exit: self.exit },
                    b"smith: the program is no longer accepting input",
                    up,
                    below,
                );
                return;
            }
            // Complete already asks for the next API message. The preceding
            // tool message was delivered; resume the program without telling
            // the domain another message or spending a no-tool nudge.
            self.outgoing = stream::text_message(b"Continue with the tool results.", self.settings.input_bytes);
            if self.outgoing.is_none() {
                self.fail(env, llm::Failure::Invalid, b"smith: program input exceeds its bound", up, below);
                return;
            }
            self.prompt_measure(env.now, Some(b"Continue with the tool results."));
            self.turns.restart();
            self.completion_deadline(env, true);
            self.send_input();
        }
    }

    fn register_calls(&mut self) -> Option<()> {
        let mut message = self.turns.closed.checked_sub(u64::from(self.turns.ready.len()))?.checked_add(1)?;
        for _ in 0..self.turns.ready.len() {
            let completion = self.turns.ready.pop()?;
            let result = register_blocks(&mut self.ledger, &completion.content, message);
            self.turns.ready.push(completion);
            result?;
            message = message.checked_add(1)?;
        }
        if let Some(blocks) = self.turns.open_blocks() {
            register_blocks(&mut self.ledger, blocks, self.turns.closed.checked_add(1)?)?;
        }
        let floor = self.turns.matching_floor()?;
        for index in 0..self.ledger.len() {
            let call = self.ledger.get_mut(index)?;
            call.eligible = call.message >= floor;
        }
        if !self.reply_room(0) {
            return None;
        }
        Some(())
    }

    pub(super) fn deliver(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if self.switching {
            return;
        }
        let Some(owner) = self.owner else {
            return;
        };
        if !self.turns.ready.is_empty() {
            self.flush_deltas(env.now);
            self.flush_prompt_measure(env.now);
            if !self.measures.is_empty() {
                self.intake_due = Some(env.now);
                return;
            }
        }
        let Some(mut completion) = self.turns.ready.pop() else {
            return;
        };
        let (prompt, outcome, deliver) = match &self.request {
            Some(FromDomain::Complete { prompt, outcome, deliver, .. }) => (prompt, outcome, deliver),
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => {
                unreachable!("active completion contract")
            }
        };
        // The previous message's replies have been consumed before a new
        // Complete. Keep unbound answered records until their MCP call arrives.
        let old = core::mem::replace(
            &mut self.ledger,
            List::with_capacity(self.settings.receiving.max_completion_blocks.checked_mul(5).expect("call slots")),
        );
        let delivered = self.turns.closed.checked_sub(u64::from(self.turns.ready.len())).expect("delivered message");
        for call in old.into_boxed() {
            if !call.answered && (call.eligible || call.rpc.is_some() || (call.builtin && call.message >= delivered)) {
                assert!(self.ledger.push(call).is_ok(), "bounded outstanding calls");
            }
        }
        let mut decoded = 0_u64;
        for block in &mut completion.content {
            match block {
                llm::Said::ToolCall { name, input, call, .. } => {
                    match call {
                        llm::Decoded::Builtin { .. } => {}
                        llm::Decoded::Owned { .. } | llm::Decoded::Served { .. } | llm::Decoded::Invalid { .. } => {
                            *call = decode(name, input, prompt, outcome, deliver.as_ref(), &self.settings.adapter);
                        }
                    }
                    decoded = match call.owned_bytes() {
                        Some(bytes) => decoded.checked_add(bytes),
                        None => None,
                    }
                    .unwrap_or(u64::MAX);
                    if decoded > self.settings.receiving.decoded_call_bytes {
                        *call = llm::Decoded::Invalid { problem: llm::Problem::TooLarge };
                    }
                }
                llm::Said::Text { text, .. } | llm::Said::Refusal { text, .. } => self.redact_private(text),
                llm::Said::Opaque { .. } => {}
            }
        }
        let mut held = 0_u64;
        let mut text = 0_u32;
        for block in &completion.content {
            let payload = match block {
                llm::Said::ToolCall { id, name, input, call, .. } => {
                    let extra = match call.owned_bytes() {
                        Some(bytes) => bytes
                            .checked_sub(u64::try_from(size_of::<llm::Decoded>()).expect("decoded cell"))
                            .unwrap_or(u64::MAX),
                        None => u64::MAX,
                    };
                    u64::try_from(id.len().saturating_add(name.len()).saturating_add(input.len()))
                        .unwrap_or(u64::MAX)
                        .saturating_add(extra)
                }
                llm::Said::Text { text: bytes, .. } | llm::Said::Refusal { text: bytes, .. } => {
                    text = text.saturating_add(u32::try_from(bytes.len()).unwrap_or(u32::MAX));
                    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
                }
                llm::Said::Opaque { bytes } => u64::try_from(bytes.len()).unwrap_or(u64::MAX),
            };
            held =
                held.saturating_add(u64::try_from(size_of::<llm::Said>()).expect("block cell")).saturating_add(payload);
        }
        if held > self.settings.receiving.max_completion_bytes {
            self.fail(env, llm::Failure::Invalid, b"smith: program completion exceeds its receiving bound", up, below);
            return;
        }
        self.match_calls();
        self.owner = None;
        self.deadline = None;
        if text > 0 {
            up.push(ToDomain::Text { owner, bytes: text });
        }
        self.completion_measure(env.now, owner, &completion);
        self.terminal_measure(env.now, owner);
        self.builtin_owners(owner, &completion);
        up.push(ToDomain::Completed { owner, completion });
        self.flush_builtins(up);
        if self.compaction() {
            self.switching = true;
            self.stop(env, false, false, up, below);
        }
    }

    pub(super) fn match_calls(&mut self) {
        let held = core::mem::replace(&mut self.calls, List::with_capacity(u32::from(self.settings.mcp.calls)));
        for call in held.into_boxed() {
            match call {
                mcp::Up::Call { call, tool, input } => {
                    if self.phase == Phase::Ending || self.stop != super::stop::Stop::None {
                        self.server.as_mut().expect("stopped MCP call").answer(
                            call,
                            mcp::Answer::Error(super::stop::INTERRUPTED.into()),
                            &mut self.io,
                        );
                        continue;
                    }
                    match calls::find(
                        &self.ledger,
                        &tool,
                        &input,
                        self.settings.adapter.client.depth,
                        self.settings.adapter.stated.call_input_bytes,
                    ) {
                        Some(index) => self.ledger.get_mut(index).expect("matched call").rpc = Some(call),
                        None if self.turns.accepting_calls() => {
                            self.calls.push(mcp::Up::Call { call, tool, input }).expect("held MCP calls");
                        }
                        None => self.server.as_mut().expect("MCP call").answer(
                            call,
                            mcp::Answer::Error(b"smith: this call was not in the model's message".as_slice().into()),
                            &mut self.io,
                        ),
                    }
                }
                prompt @ mcp::Up::Prompt { .. } => self.permission(prompt),
                mcp::Up::Listening { .. } | mcp::Up::Connected | mcp::Up::Ended(_) => {
                    unreachable!("call queue")
                }
            }
        }
        self.answer_calls();
    }

    pub(super) fn completion_deadline(&mut self, env: &Env<protocol::ComponentLimits>, head: bool) {
        if self.owner.is_none()
            || self.phase != Phase::Running
            || self.switching
            || self.stop != super::stop::Stop::None
        {
            return;
        }
        match &self.request {
            Some(FromDomain::Complete { deadlines, .. }) => {
                self.response_phase = if head { llm::Phase::Head } else { llm::Phase::Idle };
                self.deadline = Some(
                    env.now.saturating_add(if head { deadlines.head } else { deadlines.idle }).min(deadlines.whole),
                );
            }
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => {}
        }
    }

    pub(super) fn answer_calls(&mut self) {
        if self.phase == Phase::Ending || self.stop != super::stop::Stop::None {
            return;
        }
        for index in 0..self.ledger.len() {
            let call = self.ledger.get_mut(index).expect("ledger cell");
            if !call.builtin
                && let Some(rpc) = call.rpc
                && let Some(answer) = call.answer.take()
            {
                self.server.as_mut().expect("MCP reply").answer(rpc, answer, &mut self.io);
                call.answered = true;
            }
        }
    }
}

fn register_blocks(ledger: &mut List<calls::Call>, blocks: &[llm::Said], message: u64) -> Option<()> {
    for block in blocks {
        match block {
            llm::Said::ToolCall { id, name, input, call, .. } if id.as_ref() != b"smith-structured-finish" => {
                let mut known = false;
                for call in ledger.iter() {
                    if call.id == *id {
                        known = true;
                    }
                }
                if !known {
                    ledger
                        .push(calls::Call {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                            rpc: None,
                            answer: None,
                            answered: false,
                            prompted: false,
                            eligible: true,
                            message,
                            builtin: match call {
                                llm::Decoded::Builtin { .. } => true,
                                llm::Decoded::Owned { .. }
                                | llm::Decoded::Served { .. }
                                | llm::Decoded::Invalid { .. } => false,
                            },
                            owner: None,
                        })
                        .ok()?;
                }
            }
            llm::Said::ToolCall { .. }
            | llm::Said::Text { .. }
            | llm::Said::Refusal { .. }
            | llm::Said::Opaque { .. } => {}
        }
    }
    Some(())
}

fn decode(
    name: &[u8],
    input: &[u8],
    prompt: &llm::Prompt,
    outcome: &run::outcome::OutcomeSpec,
    deliver: Option<&run::outcome::ChangeSpec>,
    limits: &protocol::Limits,
) -> llm::Decoded {
    if input.len() > usize::try_from(limits.stated.call_input_bytes).expect("call input cap") {
        return llm::Decoded::Invalid { problem: llm::Problem::TooLarge };
    }
    if name == b"sub_agent" {
        return llm::Decoded::Invalid { problem: llm::Problem::UnknownTool };
    }
    for served in &prompt.served {
        match served {
            llm::Served::Finish if name == b"finish" => return protocol::decode_finish(input, outcome, limits),
            llm::Served::Deliver if name == b"deliver" => {
                return match deliver {
                    Some(change) => protocol::decode_deliver(input, change, limits),
                    None => llm::Decoded::Invalid { problem: llm::Problem::UnknownTool },
                };
            }
            llm::Served::Host(tool) if tool.name.as_ref() == name => {
                let document = stream::collect(input, limits.client.depth, limits.stated.call_input_bytes);
                let object = match document {
                    Some(document) => token_kind(&document, Some(0)) == Some(Kind::ObjectStart),
                    None => false,
                };
                if !object {
                    return llm::Decoded::Invalid { problem: llm::Problem::NotAnObject };
                }
                return match run::HostInput::attested(input.into()) {
                    Some(input) => llm::Decoded::Served {
                        ask: run::Ask::Host { tool: tool.name.clone(), effect: tool.effect, input },
                    },
                    None => llm::Decoded::Invalid { problem: llm::Problem::TooLarge },
                };
            }
            llm::Served::Finish
            | llm::Served::Deliver
            | llm::Served::Host(_)
            | llm::Served::Wait
            | llm::Served::SubAgent => {}
        }
    }
    protocol::decode(name, input, prompt.tools, &prompt.served, prompt.asker, limits)
}

fn token_kind(document: &skein_json::Document, index: Option<u32>) -> Option<Kind> {
    Some(document.token(index?)?.kind)
}

fn result_failure(document: &skein_json::Document, wall: skein_lib::Wall) -> Option<llm::Failure> {
    if token_kind(document, json::field(document, 0, b"is_error")) != Some(Kind::True) {
        return None;
    }
    let status = turns::number(document, json::field(document, 0, b"api_error_status"));
    let reason = json::text_in(document, json::field(document, 0, b"terminal_reason"));
    let retry_after = retry_after(document, wall);
    Some(match status {
        Some(401) => llm::Failure::SignedOut,
        Some(429) => llm::Failure::Exhausted { retry_after },
        Some(_) | None => match reason {
            Some(b"usage_limit" | b"rate_limit" | b"limited") => llm::Failure::Exhausted { retry_after },
            Some(_) | None => llm::Failure::Invalid,
        },
    })
}

fn retry_after(document: &skein_json::Document, wall: skein_lib::Wall) -> Option<skein_lib::Duration> {
    let info = json::field(document, 0, b"rate_limit_info");
    let seconds = turns::number(document, json::field_in(document, info, b"resetsAt"))?;
    Some(skein_lib::Duration::from_nanos(seconds.checked_mul(1_000_000_000)?.saturating_sub(wall.as_nanos())))
}
