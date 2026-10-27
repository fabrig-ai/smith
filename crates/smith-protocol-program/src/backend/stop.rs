//! One interrupt/result/input-close sequence (protocol/programs.md 5.7).
//! Keeps the process-local interrupt sequence, its outstanding response and
//! whether a result is owed; knows no run budget. Keeping stops write control
//! requests, cancel/close signal, and the existing grace escalates both.

use super::{Backend, Below, Phase, stream, tail, turns};
use crate::mcp;
use skein_io::{self as io, kernel};
use skein_lib::{Env, List, Queue};
use smith_domain::llm;
use smith_protocol_llm as protocol;
use smith_protocol_llm::{FromDomain, ToDomain};

pub(super) const INTERRUPTED: &[u8] = b"not run: the program turn was interrupted";

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stop {
    /// No backend stop is awaiting the program's result.
    None,
    /// Cancel or close signals the program, then closes input at its result.
    Closing,
    /// End a held wait and close input after its session has recorded the turn.
    Parking,
    /// A finishing Complete waits for the result before sending its note.
    WindDown,
    /// End an in-flight completion so the domain can issue its finishing note.
    Yielding,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum WindDown {
    Ordinary,
    /// A one-shot domain request waits for startup or a window switch to settle.
    YieldRequested,
    Finishing,
}

impl Backend {
    /// Whether the backend can act on a domain yield request without
    /// repeating an interrupt already waiting on its result (programs.md 5.9).
    #[must_use]
    fn can_yield_for_wind_down(&self) -> bool {
        self.phase == Phase::Running
            && self.owner.is_some()
            && self.stop == Stop::None
            && self.winding != WindDown::Finishing
            && !self.turns.result
    }

    /// Relay the domain's request to end main's in-flight program turn before
    /// its finishing note. The result remains this Complete's one terminal.
    pub(super) fn yield_for_wind_down(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if !self.can_yield_for_wind_down() {
            return;
        }
        self.winding = WindDown::Ordinary;
        if self.active_turn() {
            self.stop(env, true, true, up, below);
            if self.phase == Phase::Ending {
                return;
            }
            self.stop = Stop::Yielding;
        } else {
            self.deliver(env, up, below);
        }
        self.flush(below);
    }

    pub(super) fn active_turn(&self) -> bool {
        if self.turns.result {
            return false;
        }
        if self.request.is_some() || self.owner.is_some() || self.turns.message.is_some() || !self.calls.is_empty() {
            return true;
        }
        for call in &self.ledger {
            if !call.answered {
                return true;
            }
        }
        false
    }

    pub(super) fn wind_down(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        request: FromDomain,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) -> Option<FromDomain> {
        let choice = match &request {
            FromDomain::Complete { prompt, .. } => &prompt.choice,
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => unreachable!("completion route"),
        };
        match choice {
            llm::ToolChoice::Only(_) | llm::ToolChoice::None if self.winding == WindDown::Ordinary => {}
            llm::ToolChoice::Only(_) | llm::ToolChoice::Auto | llm::ToolChoice::None => return Some(request),
        }
        self.winding = WindDown::Finishing;
        let active = self.active_turn();
        self.request = Some(request);
        // Keep the neutral prompt in its existing ownership term until the
        // interrupted result. Encoding now would retain two input ceilings.
        if !active && !self.render_finishing_input(env, up, below) {
            return None;
        }
        self.stop(env, active, true, up, below);
        if self.stop == Stop::None {
            self.completion_deadline(env, true);
        }
        self.flush(below);
        None
    }

    fn render_finishing_input(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) -> bool {
        // A natural end has no control request. A yield keeps the sequence
        // after its acknowledgement and result, until this finishing input.
        let prefix = if self.interrupt_sequence == 0 {
            b"".as_slice()
        } else {
            b"(The agent paused your last turn to send the finishing note.)\n".as_slice()
        };
        let rendered = match &mut self.request {
            Some(FromDomain::Complete { prompt, observation, .. }) => tail::render_observed(
                prompt,
                &self.ledger,
                &self.settings.adapter,
                self.settings.input_bytes,
                prefix,
                observation.is_some(),
            ),
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => unreachable!("finishing Complete"),
        };
        match rendered {
            Some(rendered) => {
                self.outgoing = Some(rendered.line);
                if let Some(text) = rendered.text {
                    self.prompt_text_measure(env.now, text);
                }
                true
            }
            None => {
                self.fail(env, llm::Failure::Invalid, b"smith: wind-down input exceeds its bound", up, below);
                false
            }
        }
    }

    pub(super) fn parking_wait(&self) -> bool {
        if self.owner.is_some() || self.turns.result {
            return false;
        }
        for call in &self.ledger {
            if call.name.as_ref() == b"wait" && !call.answered {
                return true;
            }
        }
        false
    }

    pub(super) fn stop(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        active: bool,
        keeping: bool,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        self.flush_deltas(env.now);
        let waiting = self.stop != Stop::None;
        let was_keeping = match self.stop {
            Stop::WindDown | Stop::Yielding | Stop::Parking => true,
            Stop::None | Stop::Closing => false,
        };
        if keeping && active && !waiting && self.child.is_some() {
            match self.prepare_interrupt() {
                Some(()) => {
                    // Finish any user line already being written before this line.
                    if self.outgoing.is_none() {
                        self.send_interrupt();
                    }
                }
                None => {
                    self.fail(env, llm::Failure::Invalid, b"smith: interrupt input exceeds its bound", up, below);
                    if self.stop == Stop::None {
                        self.stop(env, active, false, up, below);
                    }
                    return;
                }
            }
        }
        self.stop = if keeping { Stop::WindDown } else { Stop::Closing };
        if active && let Some(child) = self.child {
            if !keeping && (!waiting || was_keeping) {
                self.io.push(io::Request::Signal {
                    child,
                    to: kernel::Target::Group,
                    signal: kernel::Signal::Interrupt,
                });
            }
        } else if !waiting {
            self.finish_stop();
        }
        if !waiting {
            self.deadline = if self.child.is_some() || self.spawning {
                Some(env.now.saturating_add(self.settings.grace))
            } else {
                None
            };
        }
    }

    fn prepare_interrupt(&mut self) -> Option<()> {
        let sequence = self.interrupt_sequence.checked_add(1)?;
        let line = stream::interrupt_request(sequence, self.settings.input_bytes)?;
        self.interrupt_sequence = sequence;
        self.interrupt_pending = Some(sequence);
        self.interrupt_line = Some(line);
        Some(())
    }

    pub(super) fn send_interrupt(&mut self) {
        if let Some(line) = &self.interrupt_line {
            self.io.push(io::Request::Output {
                stream: self.pipes[0].expect("interrupt input"),
                down: skein_lib::stream::OutputDown::Room {
                    right: skein_lib::Token::new(2),
                    bytes: u32::try_from(line.len()).expect("bounded interrupt line"),
                },
            });
        }
    }

    pub(super) fn control_response(&mut self, document: &skein_json::Document) -> bool {
        let response = mcp::json::field(document, 0, b"response");
        let subtype = mcp::json::text_in(document, mcp::json::field_in(document, response, b"subtype"));
        let id = mcp::json::text_in(document, mcp::json::field_in(document, response, b"request_id"));
        match self.interrupt_pending {
            Some(sequence) if subtype == Some(b"success") => {
                let expected = stream::interrupt_id(sequence);
                if id == Some(expected.as_ref()) {
                    self.interrupt_pending = None;
                    return true;
                }
            }
            Some(_) | None => {}
        }
        false
    }

    pub(super) fn stopped_result(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if self.stop == Stop::WindDown {
            self.outgoing = None;
            self.written = 0;
            if !self.render_finishing_input(env, up, below) {
                return;
            }
        }
        let yielding = self.stop == Stop::Yielding;
        if yielding {
            // A complete assistant message retains its text and usage. A partial
            // message has no completion to charge (programs.md 5.5, 5.9).
            self.turns.interrupted();
        }
        self.finish_stop();
        if self.phase == Phase::Running && !self.switching {
            self.completion_deadline(env, true);
        }
    }

    fn refuse_unanswered(&mut self) {
        let calls = core::mem::replace(&mut self.calls, List::with_capacity(u32::from(self.settings.mcp.calls)));
        for pending in calls.into_boxed() {
            match pending {
                mcp::Up::Call { call, .. } => {
                    self.server.as_mut().expect("held MCP call").answer(
                        call,
                        mcp::Answer::Error(INTERRUPTED.into()),
                        &mut self.io,
                    );
                }
                mcp::Up::Prompt { call, .. } => {
                    self.server.as_mut().expect("held permission prompt").verdict(
                        call,
                        mcp::Verdict::Deny { message: INTERRUPTED.into() },
                        &mut self.io,
                    );
                }
                mcp::Up::Listening { .. } | mcp::Up::Connected | mcp::Up::Ended(_) => unreachable!("held calls"),
            }
        }
        for index in 0..self.ledger.len() {
            let call = self.ledger.get_mut(index).expect("ledger cell");
            if !call.answered
                && let Some(rpc) = call.rpc.take()
            {
                self.server.as_mut().expect("correlated MCP call").answer(
                    rpc,
                    mcp::Answer::Error(INTERRUPTED.into()),
                    &mut self.io,
                );
            }
        }
    }

    fn finish_stop(&mut self) {
        self.refuse_unanswered();
        for index in 0..self.ledger.len() {
            let call = self.ledger.get_mut(index).expect("ledger cell");
            call.rpc = None;
            call.answer = None;
            call.answered = true;
        }
        match self.stop {
            Stop::Closing | Stop::Parking => {
                if let Some(input) = self.pipes[0] {
                    self.io.push(io::Request::Close { entity: input });
                }
            }
            Stop::WindDown => {
                let closed = self.turns.closed;
                self.turns = turns::Turns::new(&self.settings.receiving, self.settings.adapter.client.answer);
                self.turns.closed = closed;
                self.send_input();
            }
            Stop::Yielding | Stop::None => {}
        }
        self.stop = Stop::None;
    }
}
