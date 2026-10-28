//! Lines, names and text shown to one terminal (domain/host.md, section 8).

use crate::chat::{Gone, Phase};
use crate::domain::AgentLife;
use crate::{Domain, ExitStatus, Fact, Reading, Request};
use alloc::boxed::Box;
use skein_lib::{List, Queue, Token};
use smith_domain::{self as agent, session, tools};
use smith_host_domain as host;

pub(crate) const LABEL: &[u8] = b"person";

/// Lines received and retained until read or shown not delivered.
#[derive(Debug)]
pub(crate) struct Lines {
    held: Queue<Line>,
    inbox: u32,
}

/// One owned line, including its durable naming and relay stage.
#[derive(Debug)]
pub(crate) struct Line {
    text: Box<[u8]>,
    stage: Stage,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Stage {
    Received,
    Naming { name: Token },
    Named { name: Token },
    Relayed { name: Token },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnreadPolicy {
    Carry,
    Drop,
}

pub(crate) fn policy(result: Option<&host::RunResult>, cancel_asked: bool) -> UnreadPolicy {
    if cancel_asked {
        return UnreadPolicy::Drop;
    }
    match result {
        Some(host::RunResult::Accepted { .. } | host::RunResult::Parked) => UnreadPolicy::Carry,
        Some(host::RunResult::Refused { .. } | host::RunResult::Failed { .. }) | None => UnreadPolicy::Drop,
    }
}

impl Lines {
    pub(crate) fn new(inbox: u32) -> Self {
        Self { held: Queue::with_capacity(inbox.checked_add(1).expect("validated inbox")), inbox }
    }

    pub(crate) fn receive(&mut self, text: Box<[u8]>) -> Result<(), Box<[u8]>> {
        match self.held.try_push(Line { text, stage: Stage::Received }) {
            Ok(()) => Ok(()),
            Err(line) => Err(line.text),
        }
    }

    pub(crate) fn named_count(&self) -> u64 {
        let mut count = 0_u64;
        for line in &self.held {
            match line.stage {
                Stage::Received => {}
                Stage::Naming { .. } | Stage::Named { .. } | Stage::Relayed { .. } => {
                    count = count.checked_add(1).expect("bounded line count");
                }
            }
        }
        count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    pub(crate) fn received(&self) -> bool {
        for line in &self.held {
            match line.stage {
                Stage::Received => return true,
                Stage::Naming { .. } | Stage::Named { .. } | Stage::Relayed { .. } => {}
            }
        }
        false
    }

    pub(crate) fn name(&mut self, name: Token) {
        let mut named = false;
        for _ in 0..self.held.len() {
            let mut line = self.held.pop().expect("bounded line count");
            match line.stage {
                Stage::Received if !named => {
                    line.stage = Stage::Naming { name };
                    named = true;
                }
                Stage::Received | Stage::Naming { .. } | Stage::Named { .. } | Stage::Relayed { .. } => {}
            }
            self.held.push(line);
        }
    }

    pub(crate) fn saved(&mut self) {
        for _ in 0..self.held.len() {
            let mut line = self.held.pop().expect("bounded line count");
            match line.stage {
                Stage::Naming { name } => line.stage = Stage::Named { name },
                Stage::Received | Stage::Named { .. } | Stage::Relayed { .. } => {}
            }
            self.held.push(line);
        }
    }

    pub(crate) fn fence(&mut self, read: Option<Token>) -> u32 {
        let mut count = 0_u32;
        for _ in 0..self.held.len() {
            let line = self.held.pop().expect("bounded line count");
            let covered = match line.stage {
                Stage::Relayed { name } => match read {
                    Some(read) => name.raw() <= read.raw(),
                    None => false,
                },
                Stage::Received | Stage::Naming { .. } | Stage::Named { .. } => false,
            };
            if covered {
                count = count.checked_add(1).expect("bounded lines");
            } else {
                self.held.push(line);
            }
        }
        count
    }

    pub(crate) fn refused(&mut self, name: Token) -> bool {
        let mut removed = false;
        for _ in 0..self.held.len() {
            let line = self.held.pop().expect("bounded line count");
            let same = match line.stage {
                Stage::Naming { name: held } | Stage::Named { name: held } | Stage::Relayed { name: held } => {
                    held == name
                }
                Stage::Received => false,
            };
            if same {
                removed = true;
            } else {
                self.held.push(line);
            }
        }
        removed
    }

    pub(crate) fn all_read(&self) -> bool {
        for line in &self.held {
            match line.stage {
                Stage::Relayed { .. } => return false,
                Stage::Received | Stage::Naming { .. } | Stage::Named { .. } => {}
            }
        }
        true
    }

    pub(crate) fn reading(&self) -> Reading {
        if self.held.len() >= self.inbox { Reading::Held } else { Reading::Open }
    }

    pub(crate) fn relay(&mut self, carried: bool) -> Box<[host::Message]> {
        let mut messages = List::with_capacity(self.held.capacity());
        for _ in 0..self.held.len() {
            let mut line = self.held.pop().expect("bounded line count");
            let name = match line.stage {
                Stage::Named { name } => Some(name),
                Stage::Relayed { name } if carried => Some(name),
                Stage::Received | Stage::Naming { .. } | Stage::Relayed { .. } => None,
            };
            if let Some(name) = name {
                messages
                    .push(host::Message {
                        finish_requested: false,
                        name,
                        label: Box::from(LABEL),
                        text: line.text.clone(),
                    })
                    .expect("one message per held line");
                line.stage = Stage::Relayed { name };
            }
            self.held.push(line);
        }
        messages.into_boxed()
    }

    pub(crate) fn carry(&mut self) -> Box<[host::Message]> {
        let messages = self.relay(true);
        assert!(messages.is_empty() || !self.all_read(), "carried lines await their first fence");
        messages
    }

    pub(crate) fn drop_unread(&mut self) -> Box<[Option<Token>]> {
        let mut names = List::with_capacity(self.held.capacity());
        for _ in 0..self.held.len() {
            let line = self.held.pop().expect("bounded line count");
            let name = match line.stage {
                Stage::Received => None,
                Stage::Naming { name } | Stage::Named { name } | Stage::Relayed { name } => Some(name),
            };
            names.push(name).expect("one name per held line");
        }
        names.into_boxed()
    }
}

/// Render assistant text blocks up to a UTF-8 boundary under the display cap.
pub(crate) fn turn_text(turn: &agent::Turn, cap: u32) -> Box<[u8]> {
    let mut shown = List::with_capacity(cap);
    for message in &turn.messages {
        match message.role {
            session::llm::Role::User => {}
            session::llm::Role::Assistant => {
                for block in &message.content {
                    match block {
                        session::llm::Block::Text { text, replay: _ }
                        | session::llm::Block::Refusal { text, replay: _ } => append_text(&mut shown, text),
                        session::llm::Block::ToolCall { name, .. } => {
                            append_text(&mut shown, b"\nTool: ");
                            append_text(&mut shown, name);
                            append_text(&mut shown, b"\n");
                        }
                        session::llm::Block::ToolResult { result, .. } => {
                            append_text(&mut shown, tool_end(result));
                        }
                        session::llm::Block::Opaque { .. } => {}
                    }
                }
            }
        }
    }
    shown.into_boxed()
}

pub(crate) fn tool_end(result: &session::llm::Returned) -> &'static [u8] {
    match result {
        session::llm::Returned::Owned { outcome } => match outcome {
            tools::Outcome::Running { .. }
            | tools::Outcome::Job {
                end: tools::JobEnd::Running | tools::JobEnd::Ended(tools::Exit::Code { code: 0 }),
                ..
            }
            | tools::Outcome::Read { .. }
            | tools::Outcome::Listed { .. }
            | tools::Outcome::Found { timed_out: false, .. }
            | tools::Outcome::Removed
            | tools::Outcome::Written { .. }
            | tools::Outcome::Edited { .. }
            | tools::Outcome::Exited { exit: tools::Exit::Code { code: 0 }, .. } => b"Tool finished\n",
            tools::Outcome::Job { .. }
            | tools::Outcome::Found { timed_out: true, .. }
            | tools::Outcome::Exited { .. }
            | tools::Outcome::NotGranted
            | tools::Outcome::Outside { .. }
            | tools::Outcome::ReadOnly { .. }
            | tools::Outcome::TooLong
            | tools::Outcome::NotFound { .. }
            | tools::Outcome::NotEmpty
            | tools::Outcome::NotFile
            | tools::Outcome::Linked
            | tools::Outcome::Protected
            | tools::Outcome::NotDirectory { .. }
            | tools::Outcome::TooLarge { .. }
            | tools::Outcome::NotRead
            | tools::Outcome::Stale
            | tools::Outcome::EditFailed { .. }
            | tools::Outcome::Failed { .. }
            | tools::Outcome::TimedOut
            | tools::Outcome::Cancelled
            | tools::Outcome::Busy
            | tools::Outcome::NulByte => b"Tool failed\n",
        },
        session::llm::Returned::Text { error: false, .. } => b"Tool finished\n",
        session::llm::Returned::Text { error: true, .. }
        | session::llm::Returned::Invalid { .. }
        | session::llm::Returned::NotRun { .. }
        | session::llm::Returned::Withdrawn => b"Tool failed\n",
    }
}

/// Copy attested UTF-8 text within the person's display cap.
pub(crate) fn bounded_text(text: &[u8], cap: u32) -> Box<[u8]> {
    let mut shown = List::with_capacity(cap);
    append_text(&mut shown, text);
    shown.into_boxed()
}

fn append_text(shown: &mut List<u8>, text: &[u8]) {
    let mut skip = 0_u8;
    for (index, byte) in text.iter().enumerate() {
        if skip > 0 {
            skip = skip.checked_sub(1).expect("continuation count is positive");
            continue;
        }
        let width = if *byte < 0x80 {
            1_usize
        } else if *byte < 0xe0 {
            2_usize
        } else if *byte < 0xf0 {
            3_usize
        } else {
            4_usize
        };
        let Some(bytes) = text.get(index..index.saturating_add(width)) else { return };
        if shown.room() < u32::try_from(width).expect("UTF-8 character width fits u32") {
            return;
        }
        for value in bytes {
            shown.push(*value).expect("character room checked before writing");
        }
        skip = u8::try_from(width.saturating_sub(1)).expect("UTF-8 character width fits u8");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_turn_shows_the_tool_name_and_its_terminal() {
        let turn = agent::Turn {
            model: b"model".as_slice().into(),
            effort: None,
            kind: session::record::Kind::First(Box::new(session::record::Opener {
                affinity: session::llm::Affinity { key: [0; 16], thread: 0 },
                account: None,
                backend: session::record::Backend::Direct,
            })),
            version: 1,
            endpoint: Some(session::llm::Endpoint(1)),
            dialect: 1,
            sequence: 1,
            usage: session::llm::Usage::ZERO,
            spent: 0,
            jobs: 0,
            messages: Box::new([session::llm::Message {
                role: session::llm::Role::Assistant,
                content: Box::new([
                    session::llm::Block::Text { text: Box::from(&b"Working"[..]), replay: None },
                    session::llm::Block::ToolCall {
                        id: Box::from(&b"call-1"[..]),
                        name: Box::from(&b"inspect"[..]),
                        input: Box::from(&b"{}"[..]),
                        call: session::llm::Decoded::Historical,
                        replay: None,
                    },
                    session::llm::Block::ToolResult {
                        id: Box::from(&b"call-1"[..]),
                        result: session::llm::Returned::Text {
                            text: Box::from(&b"done"[..]),
                            error: false,
                            replay: None,
                        },
                    },
                ]),
            }]),
        };
        assert_eq!(turn_text(&turn, 128).as_ref(), b"Working\nTool: inspect\nTool finished\n");
    }
}

pub(crate) fn show_dropped(lines: &mut Lines, out: &mut Queue<Request>) {
    for name in lines.drop_unread() {
        out.push(Request::Notice {
            notice: crate::Notice::NotDelivered { line: name, why: crate::Undelivered::Dropped },
        });
    }
}

pub(crate) fn finish_answer(domain: &mut Domain, out: &mut Queue<Request>) {
    if domain.agent_life != AgentLife::Gone || domain.chat.state_pending {
        return;
    }
    let Some(answer) = domain.turns.answer.take() else { return };
    if domain.stop_failed {
        domain.run = None;
        domain.chat.phase = Phase::Done;
        out.push(Request::Exit { status: crate::end::exit_status(domain.end_cause) });
        return;
    }
    crate::domain::observe(domain, Fact::Shown { activation: domain.chat.state.activation });
    let status = match domain.config.mode {
        crate::Mode::Headless { .. } => {
            crate::end::answer_status(&answer, domain.config.models.first().expect("configured main").account)
        }
        crate::Mode::Chat { .. } => ExitStatus::Accepted,
    };
    let detail = core::mem::replace(&mut domain.turns.detail, Box::new([]));
    match &answer.result {
        host::RunResult::Failed { failure } => {
            out.push(Request::Notice {
                notice: crate::Notice::Failure(crate::Failure::Run { failure: failure.clone(), detail }),
            });
        }
        host::RunResult::Refused { refusal } => {
            out.push(Request::Notice { notice: crate::Notice::Failure(crate::Failure::Refused(*refusal)) });
        }
        host::RunResult::Accepted { .. } | host::RunResult::Parked => {
            let text = crate::agent::answer_text(&answer);
            out.push(Request::Show { text: bounded_text(&text, domain.show_bytes) });
        }
    }
    out.push(Request::Answered { answer });
    domain.run = None;
    domain.wall_deadline = None;

    let headless = match domain.config.mode {
        crate::Mode::Headless { .. } => true,
        crate::Mode::Chat { .. } => false,
    };
    let drained = match domain.closing_last {
        Some(last) => domain.chat.state.activation >= last,
        None => false,
    };
    if domain.chat.closed && drained {
        show_dropped(&mut domain.lines, out);
    }
    if (domain.chat.closed && domain.lines.is_empty()) || headless {
        domain.chat.phase = Phase::Done;
        out.push(Request::Exit { status });
    } else {
        domain.chat.phase = Phase::Loading;
        domain.chat.load_requested = true;
        out.push(Request::Load);
    }
}

#[cfg(test)]
mod line_tests {
    use super::*;
    fn named() -> Lines {
        let mut lines = Lines::new(2);
        lines.receive(Box::from(&b"one"[..])).expect("room");
        lines.name(Token::new(1));
        lines.saved();
        lines
    }
    #[test]
    fn a_received_line_waits_for_its_name_and_store_terminal() {
        let mut lines = Lines::new(2);
        lines.receive(Box::from(&b"one"[..])).expect("room");
        assert!(lines.carry().is_empty());
        lines.name(Token::new(1));
        assert!(lines.carry().is_empty());
        lines.saved();
        assert_eq!(lines.carry()[0].name, Token::new(1));
    }
    #[test]
    fn a_relayed_line_is_not_sent_twice_before_the_answer() {
        let mut lines = named();
        assert_eq!(lines.relay(false).len(), 1);
        assert!(lines.relay(false).is_empty());
        assert_eq!(lines.carry()[0].text.as_ref(), b"one");
    }
    #[test]
    fn a_fence_pops_only_relayed_lines_it_covers() {
        let mut lines = named();
        assert_eq!(lines.fence(Some(Token::new(1))), 0);
        lines.carry();
        assert!(!lines.all_read());
        assert_eq!(lines.fence(None), 0);
        assert_eq!(lines.fence(Some(Token::new(1))), 1);
        assert!(lines.all_read());
        assert!(lines.is_empty());
    }
    #[test]
    fn a_refused_named_line_releases_its_cell_once() {
        let mut lines = named();
        lines.carry();
        assert!(lines.refused(Token::new(1)));
        assert!(!lines.refused(Token::new(1)));
        assert!(lines.is_empty());
    }
    #[test]
    fn input_holds_at_the_inbox_with_one_completed_line_cell() {
        let mut lines = Lines::new(1);
        lines.receive(Box::from(&b"one"[..])).expect("inbox");
        assert_eq!(lines.reading(), Reading::Held);
        lines.receive(Box::from(&b"two"[..])).expect("completed line");
        assert!(lines.receive(Box::from(&b"three"[..])).is_err());
        assert_eq!(lines.drop_unread().as_ref(), &[None, None]);
        assert_eq!(lines.reading(), Reading::Open);
    }
    #[test]
    fn dropping_retained_lines_keeps_names_in_order() {
        let mut lines = named();
        lines.carry();
        lines.receive(Box::from(&b"two"[..])).expect("room");
        lines.name(Token::new(2));
        assert_eq!(lines.drop_unread().as_ref(), &[Some(Token::new(1)), Some(Token::new(2))]);
        assert!(lines.is_empty());
    }
    #[test]
    fn a_cancel_asked_drops_unread_lines_after_a_parked_answer() {
        assert_eq!(policy(Some(&host::RunResult::Parked), true), UnreadPolicy::Drop);
    }
    #[test]
    fn a_parked_answer_without_cancel_carries_unread_lines() {
        assert_eq!(policy(Some(&host::RunResult::Parked), false), UnreadPolicy::Carry);
    }
    #[test]
    fn an_end_without_an_answer_drops_unread_lines() {
        assert_eq!(policy(None, false), UnreadPolicy::Drop);
    }
    #[test]
    fn a_failed_answer_drops_unread_lines() {
        let result = host::RunResult::Failed { failure: host::RunFailure::Cancelled };
        assert_eq!(policy(Some(&result), false), UnreadPolicy::Drop);
    }
}

pub(crate) fn waiting(lines: &mut Lines, read: Option<Token>, out: &mut Queue<Request>) {
    lines.fence(read);
    if lines.all_read() {
        out.push(Request::Notice { notice: crate::Notice::Waiting });
    }
}

pub(crate) fn answered(domain: &mut Domain, answer: host::Answer, detail: Box<[u8]>, out: &mut Queue<Request>) {
    domain.lines.fence(answer.read);
    domain.chat.state.read = answer.read;
    crate::domain::observe(domain, Fact::Answered { activation: domain.chat.state.activation, read: answer.read });
    domain.chat.phase =
        Phase::Answered { gone: if domain.agent_life == AgentLife::Gone { Gone::Heard } else { Gone::Owed } };
    match policy(Some(&answer.result), domain.cancel_requested.is_some()) {
        UnreadPolicy::Carry => {}
        UnreadPolicy::Drop => show_dropped(&mut domain.lines, out),
    }
    domain.turns.answer = Some(answer);
    domain.turns.detail = detail;
    if domain.turns.unsaved.is_empty() {
        finish_answer(domain, out);
    }
}
