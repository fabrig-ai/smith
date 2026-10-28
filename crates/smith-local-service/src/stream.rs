//! The local writer's terminal from the parent face's answer, for either kind.
//! Only accepted results carry content, under the configured capture policy.
//! Contract: protocol/events.md, sections 3.3 and 4.

#![expect(clippy::manual_map, reason = "the strict subset uses exhaustive matches without closures")]

use smith_agent_service::{Answered, sinks::Capture};
use smith_events::{AnswerClass, AnswerFailure, CompletionFailure, Evidence, FailureClass, Status};
use smith_host_domain as host;

/// Parts of `run.completed` from the exact answer the local host meets.
#[must_use]
pub fn answered(answer: &host::Answer, capture: Capture) -> Answered {
    let (status, failure, result) = match &answer.result {
        host::RunResult::Parked => (Status::Parked, None, None),
        host::RunResult::Refused { refusal } => (Status::Refused, Some(refused(*refusal)), None),
        host::RunResult::Failed { failure } => (Status::Failed, Some(failed(failure.clone())), None),
        host::RunResult::Accepted { outcome } => {
            (Status::Accepted, None, Some(smith_agent_service::accepted(outcome.value(), capture)))
        }
    };
    Answered {
        status,
        failure,
        result,
        turns: u64::from(answer.turns),
        spent: answer.spent,
        read: match answer.read {
            Some(read) => Some(read.raw()),
            None => None,
        },
    }
}

/// Parts of `run.completed` for a run that went without answering (events.md, section 3.3).
#[must_use]
pub fn unanswered(
    cause: smith_local_domain::EndCause,
    turns: u32,
    spent: u64,
    read: Option<skein_lib::Token>,
) -> Answered {
    let failure = match cause {
        smith_local_domain::EndCause::Cancelled => {
            let mut failure = failure(AnswerClass::Cancelled, None);
            failure.dropped = Some(true);
            failure
        }
        smith_local_domain::EndCause::Host { turn, reason: _ } => {
            let mut failure = failure(AnswerClass::Host, None);
            failure.reason = Some(b"store".as_slice().into());
            failure.turn = match turn {
                Some(turn) => Some(u64::from(turn)),
                None => None,
            };
            failure
        }
        smith_local_domain::EndCause::Agent { ended } => {
            let mut failure = failure(AnswerClass::Agent, None);
            failure.ended = ended;
            failure
        }
    };
    Answered {
        status: Status::Failed,
        failure: Some(failure),
        result: None,
        turns: u64::from(turns),
        spent,
        read: match read {
            Some(read) => Some(read.raw()),
            None => None,
        },
    }
}

fn failure(class: AnswerClass, which: Option<&[u8]>) -> AnswerFailure {
    AnswerFailure {
        state: None,
        code: None,
        program: None,
        exit: None,
        detail: None,
        reset_ms: None,
        used: None,
        bound: None,
        dropped: None,
        ended: None,
        turn: None,
        endpoint: None,
        model: None,
        attempts: None,
        class,
        completion: None,
        account: None,
        which: match which {
            Some(which) => Some(which.into()),
            None => None,
        },
        reason: None,
    }
}

fn refused(refusal: host::Refusal) -> AnswerFailure {
    match refusal {
        host::Refusal::Busy => failure(AnswerClass::Busy, None),
        host::Refusal::Invalid(invalid) => failure(AnswerClass::Invalid, Some(invalid_name(invalid))),
    }
}

fn invalid_name(invalid: host::RunInvalid) -> &'static [u8] {
    match invalid {
        host::RunInvalid::HostPart { .. } => b"host_part",
        host::RunInvalid::Room { .. } => b"room",
        host::RunInvalid::Messages => b"messages",
        host::RunInvalid::CharterVersion => b"charter_version",
        host::RunInvalid::MalformedCharter => b"malformed_charter",
        host::RunInvalid::Endpoint => b"endpoint",
        host::RunInvalid::Activation => b"activation",
        host::RunInvalid::Window => b"window",
        host::RunInvalid::Conventions => b"conventions",
        host::RunInvalid::TooLarge => b"too_large",
        host::RunInvalid::Workspace => b"workspace",
        host::RunInvalid::Grants => b"grants",
        host::RunInvalid::Outcome => b"outcome",
        host::RunInvalid::Budget(why) => match why {
            host::BudgetRefusal::Reserve(part) => match part {
                host::Part::Turns => b"budget_reserve_turns",
                host::Part::Spend => b"budget_reserve_spend",
                host::Part::Time => b"budget_reserve_time",
            },
            host::BudgetRefusal::Ceiling(_)
            | host::BudgetRefusal::Empty(_)
            | host::BudgetRefusal::Checks { .. }
            | host::BudgetRefusal::Completion { .. } => b"budget",
        },
        host::RunInvalid::Price(_) => b"price",
        host::RunInvalid::Llm => b"llm",
        host::RunInvalid::Effort => b"effort",
        host::RunInvalid::Conversation => b"conversation",
    }
}

fn failed(why: host::RunFailure) -> AnswerFailure {
    match why {
        host::RunFailure::Model(fault) => match fault {
            host::ModelFault::Completion { failure: why, evidence, model: _, attempts: _, report: _ } => {
                let mut result = failure(AnswerClass::Model, None);
                result.completion = Some(completion(why, evidence));
                result
            }
            host::ModelFault::Limited { account, reset } => {
                let mut result = failure(AnswerClass::Model, None);
                result.account = Some(account);
                result.state = Some(smith_events::AccountState::Limited);
                result.reset_ms = match reset {
                    Some(reset) => reset.as_nanos().checked_div(1_000_000),
                    None => None,
                };
                result
            }
            host::ModelFault::Provider => failure(AnswerClass::Model, Some(b"provider")),
            host::ModelFault::Program { name, exit, detail } => {
                let mut result = failure(AnswerClass::Model, None);
                result.program = Some(name);
                result.exit = match exit {
                    Some(code) => Some(smith_events::AgentExit::Code(i64::from(code))),
                    None => None,
                };
                result.detail = Some(detail);
                result
            }
            host::ModelFault::Refused { account, code } => {
                let mut result = failure(AnswerClass::Model, None);
                result.account = Some(account);
                result.state = Some(smith_events::AccountState::Refused);
                result.code = Some(code.clone());
                result
            }
            host::ModelFault::Declined => failure(AnswerClass::Model, Some(b"refused")),
            host::ModelFault::Unauthorized { account } => {
                let mut result = failure(AnswerClass::Model, None);
                result.account = Some(account);
                result.state = Some(smith_events::AccountState::Unauthorized);
                result
            }
            host::ModelFault::Truncated => failure(AnswerClass::Model, Some(b"truncated")),
            host::ModelFault::Malformed => failure(AnswerClass::Model, Some(b"malformed")),
        },
        host::RunFailure::Budget { exhausted, used, bound } => {
            let which: &[u8] = match exhausted {
                host::Exhausted::Turns => b"turns",
                host::Exhausted::Spend => b"spend",
                host::Exhausted::Time => b"time",
                host::Exhausted::Tokens(receiving) => match receiving {
                    host::ReceivingLimit::Input => b"input_tokens",
                    host::ReceivingLimit::Output => b"output_tokens",
                    host::ReceivingLimit::CacheRead => b"cache_read_tokens",
                    host::ReceivingLimit::CacheWrite => b"cache_write_tokens",
                },
                host::Exhausted::Overflow(overflow) => match overflow {
                    host::Overflow::Spend => b"spend_overflow",
                    host::Overflow::Usage => b"usage_overflow",
                },
            };
            let mut result = failure(AnswerClass::Budget, Some(which));
            result.used = used;
            result.bound = bound;
            result
        }
        host::RunFailure::Policy(host::Policy::Unfinished { .. }) => failure(AnswerClass::Policy, None),
        host::RunFailure::Cancelled => {
            let mut answer = failure(AnswerClass::Cancelled, None);
            answer.dropped = Some(false);
            answer
        }
        host::RunFailure::Stale => failure(AnswerClass::Stale, None),
        host::RunFailure::Transcript(refusal) => {
            let which: &[u8] = match refusal {
                host::TranscriptRefusal::Version => b"version",
                host::TranscriptRefusal::Malformed => b"malformed",
                host::TranscriptRefusal::Unresolved => b"unresolved",
                host::TranscriptRefusal::TooLarge => b"too_large",
            };
            let mut result = failure(AnswerClass::Transcript, None);
            result.reason = Some(which.into());
            result
        }
    }
}

fn completion(why: host::CompletionFailure, evidence: host::CompletionEvidence) -> CompletionFailure {
    let (class, retry_after_ms) = match why {
        host::CompletionFailure::Limit { .. } | host::CompletionFailure::ApplicationLimit => {
            (FailureClass::Limit, None)
        }
        host::CompletionFailure::Protocol => (FailureClass::Protocol, None),
        host::CompletionFailure::Cancelled => (FailureClass::Cancelled, None),
        host::CompletionFailure::Overloaded => (FailureClass::Overloaded, None),
        host::CompletionFailure::Unavailable => (FailureClass::Unavailable, None),
        host::CompletionFailure::TimedOut { .. } => (FailureClass::TimedOut, None),
        host::CompletionFailure::ContextTooLong => (FailureClass::ContextTooLong, None),
        // smith 05.2 gives the new classes their event names.
        host::CompletionFailure::Refused => (FailureClass::Unknown(b"refused".as_slice().into()), None),
        host::CompletionFailure::Unsupported | host::CompletionFailure::Invalid => (FailureClass::Invalid, None),
        host::CompletionFailure::Unauthorized => (FailureClass::Unauthorized, None),
        host::CompletionFailure::RateLimited { retry_after } => {
            (FailureClass::RateLimited, Some(retry_after.as_nanos() / 1_000_000))
        }
        host::CompletionFailure::Exhausted { retry_after } => (
            FailureClass::Exhausted,
            match retry_after {
                Some(reset) => reset.as_nanos().checked_div(1_000_000),
                None => None,
            },
        ),
    };
    let evidence = match evidence {
        host::CompletionEvidence::Unsent => Evidence::Unsent,
        host::CompletionEvidence::Unknown => Evidence::MaybeSent,
        host::CompletionEvidence::Response { .. } => Evidence::Response,
    };
    CompletionFailure {
        field: None,
        code: None,
        request_id: None,
        class,
        evidence,
        retry_after_ms,
        phase: None,
        limit: None,
        status: None,
        detail: None,
    }
}

const USAGE: skein_lib::Token = skein_lib::Token::new(u64::MAX - 1);

/// The spawned product's producer takes parent answers, never channel facts.
#[derive(Debug)]
pub(crate) struct Spawned {
    pub(crate) producer: smith_agent_service::events::Producer,
    capture: Capture,
    /// Bounds each write to the adopted trace (protocol/events.md, section 5.2).
    pub(crate) write_deadline: skein_lib::Duration,
    start: skein_lib::Time,
    pub(crate) exit: Option<u8>,
    measuring: bool,
    answered: bool,
    close: smith_agent_service::Close,
}

impl Spawned {
    pub(crate) fn configured(config: &smith_agent_service::Config) -> Option<Self> {
        let producer = smith_agent_service::events::Producer::configured(config)?;
        let events = config.limits.events?;
        Some(Self {
            producer,
            capture: events.capture,
            write_deadline: events.write_deadline,
            start: skein_lib::Time::ZERO,
            exit: None,
            measuring: false,
            answered: true,
            close: smith_agent_service::Close::Graceful,
        })
    }

    pub(crate) fn starting(
        &mut self,
        now: skein_lib::Time,
        activation: u64,
        messages: u64,
        charter: &smith_domain::run::Charter,
    ) {
        self.start = now;
        self.answered = false;
        self.producer.starting(activation, messages, charter);
    }

    pub(crate) fn answered(&mut self, now: skein_lib::Time, answer: &host::Answer) {
        self.producer.answered(now, self.start, answered(answer, self.capture));
        self.answered = true;
    }

    pub(crate) fn unanswered(&mut self, now: skein_lib::Time, answer: Answered) {
        self.producer.answered(now, self.start, answer);
        self.answered = true;
    }

    pub(crate) fn forced_exit(&mut self, now: skein_lib::Time, exit: Option<host::Exit>, forced: host::Forced) {
        self.producer.notice(
            now,
            smith_events::Notice {
                code: None,
                level: smith_events::Level::Warning,
                kind: smith_events::NoticeKind::ForcedExit,
                run: None,
                account: None,
                reset_ms: None,
                bytes: None,
                exit: match exit {
                    Some(host::Exit::Code(code)) => Some(smith_events::AgentExit::Code(i64::from(code))),
                    Some(host::Exit::Signal(signal)) => Some(smith_events::AgentExit::Signal(u64::from(signal))),
                    None => None,
                },
                forced: Some(match forced {
                    host::Forced::No => smith_events::Forced::No,
                    host::Forced::Terminated => smith_events::Forced::Terminated,
                    host::Forced::Killed => smith_events::Forced::Killed,
                }),
            },
        );
    }

    pub(crate) fn abort(&mut self, now: skein_lib::Time) {
        if self.close == smith_agent_service::Close::Graceful && (!self.measuring || self.exit.is_some()) {
            self.close = smith_agent_service::Close::Aborted;
            self.producer.notice(
                now,
                smith_events::Notice {
                    code: None,
                    level: smith_events::Level::Warning,
                    kind: smith_events::NoticeKind::TeardownAborted,
                    run: None,
                    account: None,
                    reset_ms: None,
                    bytes: None,
                    exit: None,
                    forced: None,
                },
            );
        }
    }

    pub(crate) fn owns(&self, owner: skein_lib::Token) -> bool {
        owner == USAGE || self.producer.owns(owner)
    }

    pub(crate) fn needs_measure(&self) -> bool {
        self.exit.is_some() && !self.measuring
    }

    pub(crate) fn measure(&mut self, requests: &mut skein_lib::Queue<skein_io::Request>) {
        if self.exit.is_some() && !self.measuring {
            requests.push(skein_io::Request::Usage { owner: USAGE });
            self.measuring = true;
        }
    }

    pub(crate) fn up(&mut self, now: skein_lib::Time, event: skein_io::Event) {
        match event {
            skein_io::Event::Usage { owner, usage } if owner == USAGE => {
                let origin = self.producer.origin();
                let teardown = match self.producer.answer_at() {
                    Some(at) => Some(now.saturating_since(at)),
                    None => None,
                };
                self.producer.sinks().ended(
                    now,
                    origin,
                    &smith_agent_service::sinks::Ended {
                        exit: self.exit.take().expect("product exited before usage"),
                        answered: self.answered,
                        aborted: self.close == smith_agent_service::Close::Aborted,
                        teardown,
                        usage,
                        channel: None,
                    },
                );
            }
            other @ (skein_io::Event::Listening { .. }
            | skein_io::Event::Accepted { .. }
            | skein_io::Event::Connecting { .. }
            | skein_io::Event::Connected { .. }
            | skein_io::Event::Stream { .. }
            | skein_io::Event::Output { .. }
            | skein_io::Event::Spawned { .. }
            | skein_io::Event::Exited { .. }
            | skein_io::Event::Usage { .. }
            | skein_io::Event::Failed { .. }
            | skein_io::Event::Closed { .. }
            | skein_io::Event::Resized
            | skein_io::Event::Window { .. }
            | skein_io::Event::Shutdown { .. }) => self.producer.sinks().up(now, other),
        }
    }
}

pub(crate) fn exit(status: smith_local_domain::ExitStatus) -> u8 {
    match status {
        smith_local_domain::ExitStatus::Accepted => 0,
        smith_local_domain::ExitStatus::RunFailed => 1,
        smith_local_domain::ExitStatus::Refused => 2,
        smith_local_domain::ExitStatus::Credentials { .. } => 3,
        smith_local_domain::ExitStatus::Budget => 4,
        smith_local_domain::ExitStatus::NeedsInput => 5,
        smith_local_domain::ExitStatus::Interrupted => 130,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_failures_keep_their_terminal_metadata() {
        for (exit, expected) in [(None, None), (Some(7), Some(smith_events::AgentExit::Code(7)))] {
            let fault = host::RunFailure::Model(host::ModelFault::Program {
                name: b"claude_code".as_slice().into(),
                exit,
                detail: b"program detail".as_slice().into(),
            });
            let value = failed(fault);
            assert_eq!(value.class, AnswerClass::Model);
            assert_eq!(value.program.as_deref(), Some(b"claude_code".as_slice()));
            assert_eq!(value.exit, expected);
            assert_eq!(value.detail.as_deref(), Some(b"program detail".as_slice()));
        }
    }
}
