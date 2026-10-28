//! One headless run's result and operator status (shell.md, sections 4.2–4.3).
//! It keeps the last told text and answer's counters; credentials and event
//! records are never known here. Failure words remain the shared notice renderer's.
use skein_lib::Duration;
use smith_local_domain::ExitStatus;
use std::io::Write;

mod input;
pub(crate) use input::read_prompt;

/// The product's exit class, taken from the local host's terminal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitCode {
    /// The run accepted its result.
    Accepted,
    /// The run or host failed.
    RunFailed,
    /// Usage, settings or an invalid start refused.
    Usage,
    /// A needed credential is unusable.
    Credentials,
    /// The run exhausted its budget.
    Budget,
    /// The answer parked for person input.
    NeedsInput,
    /// The run was cancelled or dropped.
    Interrupted,
}

impl ExitCode {
    /// Map every local terminal class to the product's code.
    #[must_use]
    pub const fn from_status(status: ExitStatus) -> Self {
        match status {
            ExitStatus::Accepted => Self::Accepted,
            ExitStatus::RunFailed => Self::RunFailed,
            ExitStatus::Refused => Self::Usage,
            ExitStatus::Credentials { .. } => Self::Credentials,
            ExitStatus::Budget => Self::Budget,
            ExitStatus::NeedsInput => Self::NeedsInput,
            ExitStatus::Interrupted => Self::Interrupted,
        }
    }

    /// The shipped process's status number.
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Accepted => 0,
            Self::RunFailed => 1,
            Self::Usage => 2,
            Self::Credentials => 3,
            Self::Budget => 4,
            Self::NeedsInput => 5,
            Self::Interrupted => 130,
        }
    }
}

/// The answer's result and optional last line, transferred to the shell writers.
pub struct Ending {
    pub text: Box<[u8]>,
    pub line: Option<Box<[u8]>>,
}

/// Rendering choices and the last told assistant text for a parked answer.
pub struct Headless {
    pub json: bool,
    pub output_kind: skein_shell::OutputKind,
    pub input_closed: bool,
    pub(crate) result_file: bool,
    text: skein_lib::List<u8>,
}

impl Headless {
    /// Prepare a bounded last response before resource adoption.
    #[must_use]
    pub fn new(json: bool, output_kind: skein_shell::OutputKind, cap: u32) -> Self {
        Self { json, output_kind, input_closed: false, result_file: false, text: skein_lib::List::with_capacity(cap) }
    }

    /// Show tool status and keep the latest told text, with names clipped for the operator.
    pub fn told(&mut self, turn: &smith_domain::Turn, errors: &mut dyn Write) {
        use smith_domain::session::llm::{Block, Role};
        let mut replaced = false;
        for message in &turn.messages {
            if message.role != Role::Assistant {
                continue;
            }
            for block in &message.content {
                match block {
                    Block::Text { text, .. } | Block::Refusal { text, .. } => {
                        if !replaced {
                            self.text.clear();
                            replaced = true;
                        }
                        for byte in text {
                            if self.text.push(*byte).is_err() {
                                break;
                            }
                        }
                    }
                    Block::ToolCall { name, .. } => {
                        drop(writeln!(errors, "smith: tool {}", String::from_utf8_lossy(smith_events::clip(name))));
                    }
                    Block::ToolResult { result, .. } => {
                        drop(write!(
                            errors,
                            "smith: {}",
                            String::from_utf8_lossy(smith_local_domain::Domain::tool_verdict(result))
                        ));
                    }
                    Block::Opaque { .. } => {}
                }
            }
        }
    }

    /// Render accepted and parked answers; typed failure notices supply the other last lines.
    #[must_use]
    pub fn answered(&mut self, answer: &smith_host_domain::Answer, chat: &[u8], elapsed: Duration) -> Ending {
        use smith_host_domain::RunResult;
        match &answer.result {
            RunResult::Accepted { .. } => Ending {
                text: smith_local_domain::Domain::result_text(answer),
                line: Some(
                    format!(
                        "accepted in {} turns, {} notional, {} ms\n",
                        answer.turns,
                        answer.spent,
                        elapsed.as_nanos() / 1_000_000
                    )
                    .into_bytes()
                    .into(),
                ),
            },
            RunResult::Parked => Ending {
                text: self.text.as_slice().into(),
                line: Some(
                    format!("needs input: answer with `smith exec --chat {}`\n", String::from_utf8_lossy(chat))
                        .into_bytes()
                        .into(),
                ),
            },
            RunResult::Failed { .. } | RunResult::Refused { .. } => Ending { text: Box::new([]), line: None },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(content: Box<[smith_domain::session::llm::Block]>) -> smith_domain::Turn {
        use smith_domain::session::{llm, record::Kind};
        smith_domain::Turn {
            model: b"model".as_slice().into(),
            effort: None,
            version: smith_domain::session::record::VERSION,
            kind: Kind::Ordinary,
            endpoint: Some(llm::Endpoint(0)),
            dialect: 0,
            sequence: 1,
            usage: llm::Usage::ZERO,
            spent: 0,
            jobs: 0,
            messages: Box::new([llm::Message { role: llm::Role::Assistant, content }]),
        }
    }

    #[test]
    fn a_headless_result_keeps_its_last_text_across_tool_only_turns() {
        use smith_domain::session::llm::{Block, Decoded};
        let mut front = Headless::new(false, skein_shell::OutputKind::Pipe, 32);
        let mut errors = Vec::new();
        front.told(
            &turn(Box::new([Block::Text { text: b"Please answer".as_slice().into(), replay: None }])),
            &mut errors,
        );
        front.told(
            &turn(Box::new([Block::ToolCall {
                id: b"call".as_slice().into(),
                name: b"wait".as_slice().into(),
                input: b"{}".as_slice().into(),
                call: Decoded::Historical,
                replay: None,
            }])),
            &mut errors,
        );
        let answer =
            smith_host_domain::Answer { read: None, turns: 2, spent: 17, result: smith_host_domain::RunResult::Parked };
        let ending = front.answered(&answer, b"plan", Duration::from_millis(5));
        assert_eq!(ending.text.as_ref(), b"Please answer");
        assert_eq!(ending.line.unwrap().as_ref(), b"needs input: answer with `smith exec --chat plan`\n");
        assert_eq!(errors, b"smith: tool wait\n");
    }

    #[test]
    fn a_new_headless_text_replaces_the_previous_turn_within_its_cap() {
        use smith_domain::session::llm::Block;
        let mut front = Headless::new(false, skein_shell::OutputKind::Pipe, 5);
        let mut errors = Vec::new();
        front.told(&turn(Box::new([Block::Text { text: b"old".as_slice().into(), replay: None }])), &mut errors);
        front.told(
            &turn(Box::new([
                Block::Text { text: b"new".as_slice().into(), replay: None },
                Block::Refusal { text: b" text".as_slice().into(), replay: None },
            ])),
            &mut errors,
        );
        assert_eq!(front.text.as_slice(), b"new t");
    }

    #[test]
    fn each_local_terminal_has_its_product_code() {
        for (status, code) in [
            (ExitStatus::Accepted, 0),
            (ExitStatus::RunFailed, 1),
            (ExitStatus::Refused, 2),
            (ExitStatus::Credentials { account: 0 }, 3),
            (ExitStatus::Budget, 4),
            (ExitStatus::NeedsInput, 5),
            (ExitStatus::Interrupted, 130),
        ] {
            assert_eq!(ExitCode::from_status(status).code(), code);
        }
    }
}
