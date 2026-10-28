//! Bounded JSON-line decoding with unknown fields and records skipped.
//! The accumulator keeps one partial line; the tokenizer keeps no stream state.
//! Receiving ceilings come from the writer's joint aggregate, independently of
//! each field's semantic cap. Contracts: protocol/events.md, section 6;
//! protocol/limits.md, section 3, rule 1.

use crate::records::{
    AccountState, Agent, AgentExit, AnswerClass, AnswerFailure, Backend, Block, Budget, BudgetStage, BudgetStanding,
    Capture, CheckCompleted, CheckExit, CheckStarted, ChildCompleted, ChildStop, Clamp, CommandExit, Completion,
    CompletionFailure, Containment, ConversationClosed, ConversationEnd, ConversationKind, ConversationOpened, Count,
    Cpu, Delivered, Delivery, Effect, EffectiveLimit, Event, Evidence, FailureClass, FailureLimit, FailurePhase,
    Family, Field, Forced, Form, JobEnded, Level, LimitValue, Loss, Message, MessageRead, MessageReceived,
    MessageRefusal, MessageUnread, Mode, Model, Notice, NoticeKind, Outcome, PeakRss, Prompt, Quantity, Record,
    Requested, Reserve, ResponseCompleted, ResponseStarted, Role, RunCompleted, RunResult, RunStarted, SessionEnded,
    SessionStarted, Share, ShareBound, SharePart, Source, Status, Stop, TextDelta, ToolChoice, ToolCompleted,
    ToolStarted, Tools, Usage, Verdict, Versions,
};
use crate::{Error, Limits};

/// A bounded line accumulator supplied chunks by a stream consumer.
#[derive(Debug)]
pub struct Reader {
    limits: Limits,
    encoded_line: u32,
    line: List<u8>,
    session_ended_aborted: bool,
}

/// One read's consumed bytes and optional record; unknown records still complete a line.
#[derive(Debug)]
pub struct Line {
    pub consumed: usize,
    pub complete: bool,
    pub record: Option<Record>,
}

impl Reader {
    /// Reserve the independent-maxima bound for callers without a joint writer bound.
    pub fn new(limits: &Limits) -> Result<Reader, Error> {
        let capacity = crate::largest_record(limits).ok_or(Error::Limits)?;
        Self::with_ceiling(limits, capacity)
    }

    /// Reserve the writer's encoded-line ceiling, including its final newline.
    /// Field and list caps remain independent semantic checks; zero refuses.
    pub fn with_ceiling(limits: &Limits, encoded_line: u32) -> Result<Reader, Error> {
        Self::with_session_ended_aborted(limits, encoded_line, true)
    }

    /// Select whether this writer's session terminal carries the `aborted` field.
    /// Only historical benchmark faces select its absence; current readers require it.
    pub fn with_session_ended_aborted(
        limits: &Limits,
        encoded_line: u32,
        session_ended_aborted: bool,
    ) -> Result<Reader, Error> {
        let capacity = encoded_line.checked_sub(1).ok_or(Error::Limits)?;
        Ok(Reader { limits: *limits, encoded_line, line: List::with_capacity(capacity), session_ended_aborted })
    }

    /// Consume through at most one newline, preserving a partial line between calls.
    pub fn feed(&mut self, input: &[u8]) -> Result<Line, Error> {
        let mut consumed = 0_usize;
        for byte in input {
            consumed = consumed.checked_add(1).ok_or(Error::Limits)?;
            if *byte == b'\n' {
                let result = read_for_session_ended(
                    self.line.as_slice(),
                    &self.limits,
                    self.encoded_line,
                    self.session_ended_aborted,
                );
                self.line.clear();
                return Ok(Line { consumed, complete: true, record: result? });
            }
            if self.line.push(*byte).is_err() {
                self.line.clear();
                return Err(Error::Limits);
            }
        }
        Ok(Line { consumed, complete: false, record: None })
    }

    /// End a stream only between newline-terminated records.
    pub fn finish(&self) -> Result<(), Error> {
        if self.line.is_empty() { Ok(()) } else { Err(Error::Malformed) }
    }
}
use alloc::boxed::Box;
use skein_json::{Token, tokenizer};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, List, Queue, Time, Wall, bytes};

/// Read one JSON line. Unknown record types return `None`; every version is checked.
pub fn read(line: &[u8], limits: &Limits) -> Result<Option<Record>, Error> {
    let encoded_line = crate::largest_record(limits).ok_or(Error::Limits)?;
    read_with_ceiling(line, limits, encoded_line)
}

/// Decode within a writer-derived encoded byte ceiling and the supplied semantic caps.
/// The byte ceiling includes a newline when the supplied line has one; zero refuses.
pub fn read_with_ceiling(line: &[u8], limits: &Limits, encoded_line: u32) -> Result<Option<Record>, Error> {
    read_for_session_ended(line, limits, encoded_line, true)
}

fn read_for_session_ended(
    line: &[u8],
    limits: &Limits,
    encoded_line: u32,
    session_ended_aborted: bool,
) -> Result<Option<Record>, Error> {
    let tokens = tokenize(line, limits, encoded_line)?;
    let tokens = tokens.as_slice();
    object(tokens)?;
    let version = unsigned(required(tokens, b"v")?)?;
    if version != crate::VERSION {
        return Err(Error::Version(version));
    }
    let kind = text(required(tokens, b"type")?, limits.string)?;
    let t_ms = unsigned(required(tokens, b"t_ms")?)?;
    let event = match kind.as_ref() {
        b"budget" => Event::Budget(read_budget_standing(tokens, limits)?),
        b"session.started" => Event::SessionStarted(read_session_started(tokens, limits)?),
        b"session.ended" => Event::SessionEnded(read_session_ended(tokens, limits, session_ended_aborted)?),
        b"run.started" => Event::RunStarted(read_run_started(tokens, limits)?),
        b"run.completed" => Event::RunCompleted(read_run_completed(tokens, limits)?),
        b"conversation.opened" => Event::ConversationOpened(read_conversation_opened(tokens, limits)?),
        b"conversation.closed" => Event::ConversationClosed(read_conversation_closed(tokens, limits)?),
        b"response.started" => Event::ResponseStarted(read_response_started(tokens, limits)?),
        b"response.completed" => Event::ResponseCompleted(read_response_completed(tokens, limits)?),
        b"text.delta" => Event::TextDelta(read_text_delta(tokens, limits)?),
        b"tool.started" => Event::ToolStarted(read_tool_started(tokens, limits)?),
        b"job.ended" => Event::JobEnded(read_job_ended(tokens, limits)?),
        b"tool.completed" => Event::ToolCompleted(read_tool_completed(tokens, limits)?),
        b"check.started" => Event::CheckStarted(read_check_started(tokens, limits)?),
        b"check.completed" => Event::CheckCompleted(read_check_completed(tokens, limits)?),
        b"message.received" => Event::MessageReceived(read_message_received(tokens, limits)?),
        b"message.read" => Event::MessageRead(read_message_read(tokens)?),
        b"message.unread" => Event::MessageUnread(read_message_unread(tokens)?),
        b"notice" => Event::Notice(read_notice(tokens, limits)?),
        _ => return Ok(None),
    };
    Ok(Some(Record { t_ms, event }))
}

fn tokenize(input: &[u8], limits: &Limits, encoded_line: u32) -> Result<List<Token>, Error> {
    if encoded_line == 0 || input.len() > usize::try_from(encoded_line).expect("u32 fits") {
        return Err(Error::Limits);
    }
    let env = Env {
        now: Time::ZERO,
        wall: Wall::EPOCH,
        limits: tokenizer::Limits {
            depth: 16,
            string: limits.string.max(limits.content).max(64),
            number: 20,
            chunk: 256,
            length: encoded_line,
        },
    };
    let mut machine = tokenizer::Tokenizer::new(&env.limits);
    let mut above = Queue::with_capacity(1);
    let mut below = Queue::with_capacity(1);
    let Ok(token_cap) = u32::try_from(input.len()) else {
        return Err(Error::Limits);
    };
    let mut tokens = List::with_capacity(token_cap);
    let mut at: usize = 0;
    let ticks = input.len().checked_mul(4).ok_or(Error::Limits)?.checked_add(16).ok_or(Error::Limits)?;
    for _tick in 0..ticks {
        match machine.waiting() {
            tokenizer::Waiting::Next => {
                tokenizer::down(&mut machine, &env, tokenizer::Request::Next, &mut above, &mut below);
            }
            tokenizer::Waiting::Bytes => {
                let demand = below.pop().ok_or(Error::Malformed)?;
                let read = match demand {
                    Down::Demand { read, .. } => read,
                    Down::Send(_) | Down::Finish => return Err(Error::Malformed),
                };
                let remaining = input.get(at..).ok_or(Error::Malformed)?;
                let count = match read {
                    Read::Fill(count) => usize::try_from(count).expect("u32 fits"),
                    Read::Scan { until, max } => {
                        let max = usize::try_from(max).expect("u32 fits");
                        let scanned = remaining.get(..remaining.len().min(max)).ok_or(Error::Malformed)?;
                        match bytes::find(scanned, until.as_bytes()) {
                            Some(position) => position.checked_add(until.as_bytes().len()).ok_or(Error::Limits)?,
                            None => max,
                        }
                    }
                    Read::Nothing | Read::Line { .. } | Read::Arrived { .. } => return Err(Error::Malformed),
                };
                if count <= remaining.len() {
                    let delivery = bytes::copy_of(remaining.get(..count).ok_or(Error::Malformed)?);
                    at = at.checked_add(count).ok_or(Error::Limits)?;
                    tokenizer::up(&mut machine, &env, Up::Bytes(delivery), &mut above, &mut below);
                } else {
                    tokenizer::up(&mut machine, &env, Up::End, &mut above, &mut below);
                }
            }
            tokenizer::Waiting::Close | tokenizer::Waiting::Nothing => return Err(Error::Malformed),
        }
        if let Some(event) = above.pop() {
            match event {
                tokenizer::Event::Token(token) => match tokens.push(token) {
                    Ok(()) => {}
                    Err(_) => return Err(Error::Limits),
                },
                tokenizer::Event::Long(_) | tokenizer::Event::Skipped(_) => {
                    unreachable!("the event reader requests only Next tokens")
                }
                tokenizer::Event::Done => return Ok(tokens),
                tokenizer::Event::Failed(_) | tokenizer::Event::Closed => return Err(Error::Malformed),
            }
        }
    }
    Err(Error::Malformed)
}

fn object(tokens: &[Token]) -> Result<(), Error> {
    if tokens.first() != Some(&Token::ObjectStart) || tokens.last() != Some(&Token::ObjectEnd) {
        return Err(Error::Shape);
    }
    Ok(())
}

fn value_end(tokens: &[Token], start: usize) -> Result<usize, Error> {
    let mut depth = 0_u32;
    for position in start..tokens.len() {
        match tokens.get(position).ok_or(Error::Shape)? {
            Token::ObjectStart | Token::ArrayStart => depth = depth.checked_add(1).ok_or(Error::Limits)?,
            Token::ObjectEnd | Token::ArrayEnd => {
                depth = depth.checked_sub(1).ok_or(Error::Shape)?;
                if depth == 0 {
                    return position.checked_add(1).ok_or(Error::Limits);
                }
            }
            Token::Key(_) => {}
            Token::String(_) | Token::Number(_) | Token::True | Token::False | Token::Null => {
                if depth == 0 {
                    return position.checked_add(1).ok_or(Error::Limits);
                }
            }
        }
    }
    Err(Error::Shape)
}

fn field<'a>(tokens: &'a [Token], name: &[u8]) -> Result<Option<&'a [Token]>, Error> {
    object(tokens)?;
    let mut at = 1_usize;
    let mut found = None;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let key = match tokens.get(at).ok_or(Error::Shape)? {
            Token::Key(key) => key,
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => return Err(Error::Shape),
        };
        let start = at.checked_add(1).ok_or(Error::Limits)?;
        at = value_end(tokens, start)?;
        if key.as_ref() == name {
            if found.is_some() {
                return Err(Error::Shape);
            }
            found = Some(tokens.get(start..at).ok_or(Error::Shape)?);
        }
    }
    Ok(found)
}

fn required<'a>(tokens: &'a [Token], name: &[u8]) -> Result<&'a [Token], Error> {
    field(tokens, name)?.ok_or(Error::Shape)
}

fn unsigned(tokens: &[Token]) -> Result<u64, Error> {
    match tokens {
        [Token::Number(value)] => digits(value),
        _ => Err(Error::Shape),
    }
}

fn digits(value: &[u8]) -> Result<u64, Error> {
    if value.is_empty() {
        return Err(Error::Shape);
    }
    let mut number = 0_u64;
    for byte in value {
        if !byte.is_ascii_digit() {
            return Err(Error::Shape);
        }
        number = number
            .checked_mul(10)
            .ok_or(Error::Shape)?
            .checked_add(u64::from(byte.saturating_sub(b'0')))
            .ok_or(Error::Shape)?;
    }
    Ok(number)
}

fn signed(tokens: &[Token]) -> Result<i64, Error> {
    match tokens {
        [Token::Number(value)] => {
            let negative = value.first() == Some(&b'-');
            let magnitude = if negative { digits(value.get(1..).ok_or(Error::Shape)?)? } else { digits(value)? };
            if negative && magnitude == i64::MIN.unsigned_abs() {
                return Ok(i64::MIN);
            }
            let Ok(number) = i64::try_from(magnitude) else {
                return Err(Error::Shape);
            };
            if negative { number.checked_neg().ok_or(Error::Shape) } else { Ok(number) }
        }
        _ => Err(Error::Shape),
    }
}

fn boolean(tokens: &[Token]) -> Result<bool, Error> {
    match tokens {
        [Token::True] => Ok(true),
        [Token::False] => Ok(false),
        _ => Err(Error::Shape),
    }
}

fn text(tokens: &[Token], capacity: u32) -> Result<Box<[u8]>, Error> {
    match tokens {
        [Token::String(value)] if value.len() <= usize::try_from(capacity).expect("u32 fits") => Ok(value.clone()),
        _ => Err(Error::Shape),
    }
}
fn read_capture(tokens: &[Token], limits: &Limits) -> Result<Capture, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"none" => Capture::None,
        b"calls" => Capture::Calls,
        b"everything" => Capture::Everything,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Capture::Unknown(value)
        }
    })
}

fn read_delivery(tokens: &[Token], limits: &Limits) -> Result<Delivery, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"complete" => Delivery::Complete,
        b"best_effort" => Delivery::BestEffort,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Delivery::Unknown(value)
        }
    })
}

fn read_mode(tokens: &[Token], limits: &Limits) -> Result<Mode, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"agent" => Mode::Agent,
        b"exec" => Mode::Exec,
        b"chat" => Mode::Chat,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Mode::Unknown(value)
        }
    })
}

fn read_containment(tokens: &[Token], limits: &Limits) -> Result<Containment, Error> {
    let value = text(tokens, limits.string.max(6))?;
    Ok(match value.as_ref() {
        b"groups" => Containment::Groups,
        b"trees" => Containment::Trees,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Containment::Unknown(value)
        }
    })
}

fn read_family(tokens: &[Token], limits: &Limits) -> Result<Family, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"inspect" => Family::Inspect,
        b"modify" => Family::Modify,
        b"shell" => Family::Shell,
        b"sub_agents" => Family::SubAgents,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Family::Unknown(value)
        }
    })
}

fn read_form(tokens: &[Token], limits: &Limits) -> Result<Form, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"report" => Form::Report,
        b"verdict" => Form::Verdict,
        b"change" => Form::Change,
        b"failure" => Form::Failure,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Form::Unknown(value)
        }
    })
}

fn read_status(tokens: &[Token], limits: &Limits) -> Result<Status, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"accepted" => Status::Accepted,
        b"parked" => Status::Parked,
        b"failed" => Status::Failed,
        b"refused" => Status::Refused,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Status::Unknown(value)
        }
    })
}

fn read_conversation_kind(tokens: &[Token], limits: &Limits) -> Result<ConversationKind, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"main" => ConversationKind::Main,
        b"child" => ConversationKind::Child,
        b"compaction" => ConversationKind::Compaction,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            ConversationKind::Unknown(value)
        }
    })
}

fn read_conversation_end(tokens: &[Token], limits: &Limits) -> Result<ConversationEnd, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"closed" => ConversationEnd::Closed,
        b"budget" => ConversationEnd::Budget,
        b"failed" => ConversationEnd::Failed,
        b"refused" => ConversationEnd::Refused,
        b"transcript" => ConversationEnd::Transcript,
        b"overflow" => ConversationEnd::Overflow,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            ConversationEnd::Unknown(value)
        }
    })
}

fn read_tool_choice(tokens: &[Token], limits: &Limits) -> Result<ToolChoice, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"auto" => ToolChoice::Auto,
        b"none" => ToolChoice::None,
        b"only" => ToolChoice::Only,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            ToolChoice::Unknown(value)
        }
    })
}

fn read_outcome(tokens: &[Token], limits: &Limits) -> Result<Outcome, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"completed" => Outcome::Completed,
        b"failed" => Outcome::Failed,
        b"cancelled" => Outcome::Cancelled,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Outcome::Unknown(value)
        }
    })
}

fn read_stop(tokens: &[Token], limits: &Limits) -> Result<Stop, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"end" => Stop::End,
        b"tools" => Stop::Tools,
        b"max_tokens" => Stop::MaxTokens,
        b"refusal" => Stop::Refusal,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Stop::Unknown(value)
        }
    })
}

fn read_source(tokens: &[Token], limits: &Limits) -> Result<Source, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"workspace" => Source::Workspace,
        b"run" => Source::Run,
        b"host" => Source::Host,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Source::Unknown(value)
        }
    })
}

fn read_effect(tokens: &[Token], limits: &Limits) -> Result<Effect, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"read" => Effect::Read,
        b"write" => Effect::Write,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Effect::Unknown(value)
        }
    })
}

fn read_verdict(tokens: &[Token], limits: &Limits) -> Result<Verdict, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"read" => Verdict::Read,
        b"listed" => Verdict::Listed,
        b"found" => Verdict::Found,
        b"removed" => Verdict::Removed,
        b"not_empty" => Verdict::NotEmpty,
        b"written" => Verdict::Written,
        b"edited" => Verdict::Edited,
        b"exited" => Verdict::Exited,
        b"conflict" => Verdict::Conflict,
        b"missing" => Verdict::Missing,
        b"too_large" => Verdict::TooLarge,
        b"timed_out" => Verdict::TimedOut,
        b"running" => Verdict::Running,
        b"stopped" => Verdict::Stopped,
        b"failed" => Verdict::Failed,
        b"cancelled" => Verdict::Cancelled,
        b"ambiguous" => Verdict::Ambiguous,
        b"result" => Verdict::Result,
        b"error" => Verdict::Error,
        b"invalid" => Verdict::Invalid,
        b"not_run" => Verdict::NotRun,
        b"withdrawn" => Verdict::Withdrawn,
        b"not_granted" => Verdict::NotGranted,
        b"outside" => Verdict::Outside,
        b"read_only" => Verdict::ReadOnly,
        b"too_long" => Verdict::TooLong,
        b"not_found" => Verdict::NotFound,
        b"not_file" => Verdict::NotFile,
        b"linked" => Verdict::Linked,
        b"protected" => Verdict::Protected,
        b"not_directory" => Verdict::NotDirectory,
        b"not_read" => Verdict::NotRead,
        b"stale" => Verdict::Stale,
        b"no_match" => Verdict::NoMatch,
        b"unchanged" => Verdict::Unchanged,
        b"busy" => Verdict::Busy,
        b"nul_byte" => Verdict::NulByte,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Verdict::Unknown(value)
        }
    })
}

fn read_delivered(tokens: &[Token], limits: &Limits) -> Result<Delivered, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"delivered" => Delivered::Delivered,
        b"nothing" => Delivered::Nothing,
        b"refused" => Delivered::Refused,
        b"failed" => Delivered::Failed,
        b"stale" => Delivered::Stale,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Delivered::Unknown(value)
        }
    })
}

fn read_failure_class(tokens: &[Token], limits: &Limits) -> Result<FailureClass, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"overloaded" => FailureClass::Overloaded,
        b"rate_limited" => FailureClass::RateLimited,
        b"exhausted" => FailureClass::Exhausted,
        b"refused" => FailureClass::Refused,
        b"unsupported" => FailureClass::Unsupported,
        b"unavailable" => FailureClass::Unavailable,
        b"timed_out" => FailureClass::TimedOut,
        b"context_too_long" => FailureClass::ContextTooLong,
        b"invalid" => FailureClass::Invalid,
        b"unauthorized" => FailureClass::Unauthorized,
        b"limit" => FailureClass::Limit,
        b"protocol" => FailureClass::Protocol,
        b"cancelled" => FailureClass::Cancelled,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            FailureClass::Unknown(value)
        }
    })
}

fn read_evidence(tokens: &[Token], limits: &Limits) -> Result<Evidence, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"unsent" => Evidence::Unsent,
        b"maybe_sent" => Evidence::MaybeSent,
        b"response" => Evidence::Response,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Evidence::Unknown(value)
        }
    })
}

fn read_answer_class(tokens: &[Token], limits: &Limits) -> Result<AnswerClass, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"model" => AnswerClass::Model,
        b"budget" => AnswerClass::Budget,
        b"policy" => AnswerClass::Policy,
        b"cancelled" => AnswerClass::Cancelled,
        b"agent" => AnswerClass::Agent,
        b"host" => AnswerClass::Host,
        b"stale" => AnswerClass::Stale,
        b"transcript" => AnswerClass::Transcript,
        b"busy" => AnswerClass::Busy,
        b"invalid" => AnswerClass::Invalid,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            AnswerClass::Unknown(value)
        }
    })
}

fn read_level(tokens: &[Token], limits: &Limits) -> Result<Level, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"info" => Level::Info,
        b"warning" => Level::Warning,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Level::Unknown(value)
        }
    })
}

fn read_notice_kind(tokens: &[Token], limits: &Limits) -> Result<NoticeKind, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"reasoning_dropped" => NoticeKind::ReasoningDropped,
        b"credential_unauthorized" => NoticeKind::CredentialUnauthorized,
        b"account_limited" => NoticeKind::AccountLimited,
        b"account_refused" => NoticeKind::AccountRefused,
        b"teardown_aborted" => NoticeKind::TeardownAborted,
        b"forced_exit" => NoticeKind::ForcedExit,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            NoticeKind::Unknown(value)
        }
    })
}

fn read_role(tokens: &[Token], limits: &Limits) -> Result<Role, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"user" => Role::User,
        b"assistant" => Role::Assistant,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Role::Unknown(value)
        }
    })
}

fn read_agent(tokens: &[Token], limits: &Limits) -> Result<Agent, Error> {
    object(tokens)?;
    Ok(Agent {
        name: text(required(tokens, b"name")?, limits.string)?,
        version: text(required(tokens, b"version")?, limits.string)?,
    })
}

fn read_versions(tokens: &[Token], _limits: &Limits) -> Result<Versions, Error> {
    object(tokens)?;
    Ok(Versions {
        events: unsigned(required(tokens, b"events")?)?,
        channel: unsigned(required(tokens, b"channel")?)?,
        charter: unsigned(required(tokens, b"charter")?)?,
        transcript: unsigned(required(tokens, b"transcript")?)?,
        chat_store: match field(tokens, b"chat_store")? {
            Some(value) => Some(unsigned(value)?),
            None => None,
        },
    })
}

#[cfg(test)]
#[test]
fn the_product_announces_its_chat_store_version() {
    let wire = b"{\"v\":4,\"type\":\"session.started\",\"t_ms\":0,\"wall_ms\":0,\"agent\":{\"name\":\"smith\",\"version\":\"0\"},\"mode\":\"exec\",\"chat\":\"plan\",\"containment\":\"groups\",\"pid\":1,\"profile\":\"standard\",\"capture\":\"none\",\"delivery\":\"complete\",\"versions\":{\"events\":4,\"channel\":2,\"charter\":2,\"transcript\":5,\"chat_store\":1},\"limits\":{}}\n";
    let limits = Limits { string: 32, content: 1024, items: 16 };
    let record = read(wire, &limits).unwrap().unwrap();
    let Event::SessionStarted(started) = &record.event else { panic!("startup record") };
    assert_eq!(started.versions.chat_store, Some(1));
    assert_eq!(crate::write(&record, &Capture::None, &limits).unwrap().unwrap().as_ref(), wire);
}

fn read_loss(tokens: &[Token], _limits: &Limits) -> Result<Loss, Error> {
    object(tokens)?;
    Ok(Loss {
        events: unsigned(required(tokens, b"events")?)?,
        channel: match required(tokens, b"channel")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
    })
}

fn read_cpu(tokens: &[Token], _limits: &Limits) -> Result<Cpu, Error> {
    object(tokens)?;
    Ok(Cpu {
        user: unsigned(required(tokens, b"user")?)?,
        system: unsigned(required(tokens, b"system")?)?,
        children_user: unsigned(required(tokens, b"children_user")?)?,
        children_system: unsigned(required(tokens, b"children_system")?)?,
    })
}

fn read_peak_rss(tokens: &[Token], _limits: &Limits) -> Result<PeakRss, Error> {
    object(tokens)?;
    Ok(PeakRss {
        self_bytes: unsigned(required(tokens, b"self")?)?,
        children: unsigned(required(tokens, b"children")?)?,
    })
}

fn read_model(tokens: &[Token], limits: &Limits) -> Result<Model, Error> {
    object(tokens)?;
    Ok(Model {
        account: match required(tokens, b"account")? {
            [Token::Null] => None,
            item => Some(match u32::try_from(unsigned(item)?) {
                Ok(account) => account,
                Err(_) => return Err(Error::Shape),
            }),
        },
        backend: {
            let name = text(required(tokens, b"backend")?, limits.string)?;
            match name.as_ref() {
                b"direct" => Backend::Direct,
                b"claude_code" => Backend::ClaudeCode,
                _ => Backend::Unknown(name),
            }
        },
        endpoint: text(required(tokens, b"endpoint")?, limits.string)?,
        model: text(required(tokens, b"model")?, limits.string)?,
        window_tokens: unsigned(required(tokens, b"window_tokens")?)?,
        output_tokens: unsigned(required(tokens, b"output_tokens")?)?,
        effort: match required(tokens, b"effort")? {
            [Token::Null] => None,
            item => Some(text(item, limits.string)?),
        },
    })
}

fn read_usage(tokens: &[Token], _limits: &Limits) -> Result<Usage, Error> {
    object(tokens)?;
    Ok(Usage {
        input_tokens: match required(tokens, b"input_tokens")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        cache_read_tokens: match required(tokens, b"cache_read_tokens")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        cache_write_tokens: match required(tokens, b"cache_write_tokens")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        output_tokens: match required(tokens, b"output_tokens")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        reasoning_tokens: match required(tokens, b"reasoning_tokens")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
    })
}

fn read_budget(tokens: &[Token], _limits: &Limits) -> Result<Budget, Error> {
    object(tokens)?;
    Ok(Budget {
        turns: unsigned(required(tokens, b"turns")?)?,
        spend: unsigned(required(tokens, b"spend")?)?,
        time_ms: unsigned(required(tokens, b"time_ms")?)?,
        reserve: read_reserve(required(tokens, b"reserve")?)?,
    })
}

fn read_tools(tokens: &[Token], limits: &Limits) -> Result<Tools, Error> {
    object(tokens)?;
    Ok(Tools {
        withheld: read_list_family(required(tokens, b"withheld")?, limits)?,
        families: read_list_family(required(tokens, b"families")?, limits)?,
        wait: boolean(required(tokens, b"wait")?)?,
        deliver: boolean(required(tokens, b"deliver")?)?,
        host: read_list_text(required(tokens, b"host")?, limits)?,
    })
}

fn read_failure_phase(tokens: &[Token], limits: &Limits) -> Result<FailurePhase, Error> {
    let value = text(tokens, limits.string)?;
    Ok(match value.as_ref() {
        b"connect" => FailurePhase::Connect,
        b"handshake" => FailurePhase::Handshake,
        b"head" => FailurePhase::Head,
        b"idle" => FailurePhase::Idle,
        b"whole" => FailurePhase::Whole,
        _ => FailurePhase::Unknown(value),
    })
}

fn read_completion_failure(tokens: &[Token], limits: &Limits) -> Result<CompletionFailure, Error> {
    object(tokens)?;
    Ok(CompletionFailure {
        request_id: match field(tokens, b"request_id")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        code: match field(tokens, b"code")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        field: match field(tokens, b"field")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        class: read_failure_class(required(tokens, b"class")?, limits)?,
        evidence: read_evidence(required(tokens, b"evidence")?, limits)?,
        phase: match field(tokens, b"phase")? {
            Some(item) => Some(read_failure_phase(item, limits)?),
            None => None,
        },
        limit: match field(tokens, b"limit")? {
            Some(item) => Some(read_failure_limit(item, limits)?),
            None => None,
        },
        status: match field(tokens, b"status")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        detail: match field(tokens, b"detail")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
        retry_after_ms: match field(tokens, b"retry_after_ms")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
    })
}

fn read_failure_limit(tokens: &[Token], limits: &Limits) -> Result<FailureLimit, Error> {
    object(tokens)?;
    Ok(FailureLimit {
        which: text(required(tokens, b"which")?, limits.string)?,
        bound: unsigned(required(tokens, b"bound")?)?,
    })
}

fn read_agent_exit(tokens: &[Token]) -> Result<AgentExit, Error> {
    object(tokens)?;
    match field(tokens, b"code")? {
        Some(code) => {
            if field(tokens, b"signal")?.is_some() {
                return Err(Error::Shape);
            }
            Ok(AgentExit::Code(signed(code)?))
        }
        None => Ok(AgentExit::Signal(unsigned(required(tokens, b"signal")?)?)),
    }
}

fn read_answer_failure(tokens: &[Token], limits: &Limits) -> Result<AnswerFailure, Error> {
    object(tokens)?;
    Ok(AnswerFailure {
        state: match field(tokens, b"state")? {
            Some(item) => Some(read_account_state(item, limits)?),
            None => None,
        },
        code: match field(tokens, b"code")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        program: match field(tokens, b"program")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        exit: match field(tokens, b"exit")? {
            Some(item) => Some(read_agent_exit(item)?),
            None => None,
        },
        detail: match field(tokens, b"detail")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
        reset_ms: match field(tokens, b"reset_ms")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        used: match field(tokens, b"used")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        bound: match field(tokens, b"bound")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        dropped: match field(tokens, b"dropped")? {
            Some(item) => Some(boolean(item)?),
            None => None,
        },
        ended: match field(tokens, b"ended")? {
            Some(item) => Some(read_agent_exit(item)?),
            None => None,
        },
        turn: match field(tokens, b"turn")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        class: read_answer_class(required(tokens, b"class")?, limits)?,
        endpoint: match field(tokens, b"endpoint")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        model: match field(tokens, b"model")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        attempts: match field(tokens, b"attempts")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        completion: match field(tokens, b"completion")? {
            Some(item) => Some(read_completion_failure(item, limits)?),
            None => None,
        },
        account: match field(tokens, b"account")? {
            Some(item) => Some(match u32::try_from(unsigned(item)?) {
                Ok(value) => value,
                Err(_) => return Err(Error::Shape),
            }),
            None => None,
        },
        which: match field(tokens, b"which")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        reason: match field(tokens, b"reason")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
    })
}

fn read_field(tokens: &[Token], limits: &Limits) -> Result<Field, Error> {
    object(tokens)?;
    Ok(Field {
        name: text(required(tokens, b"name")?, limits.string)?,
        text: text(required(tokens, b"text")?, limits.content)?,
    })
}

fn read_run_result(tokens: &[Token], limits: &Limits) -> Result<RunResult, Error> {
    object(tokens)?;
    Ok(RunResult {
        form: read_form(required(tokens, b"form")?, limits)?,
        label: match field(tokens, b"label")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        text: match field(tokens, b"text")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
        fields: match field(tokens, b"fields")? {
            Some(item) => Some(read_list_field(item, limits)?),
            None => None,
        },
    })
}

fn read_message(tokens: &[Token], limits: &Limits) -> Result<Message, Error> {
    object(tokens)?;
    Ok(Message {
        role: read_role(required(tokens, b"role")?, limits)?,
        blocks: read_list_block(required(tokens, b"blocks")?, limits)?,
    })
}

fn read_prompt(tokens: &[Token], limits: &Limits) -> Result<Prompt, Error> {
    object(tokens)?;
    Ok(Prompt {
        system: match field(tokens, b"system")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
        tools: match field(tokens, b"tools")? {
            Some(item) => Some(read_list_text(item, limits)?),
            None => None,
        },
        messages: read_list_message(required(tokens, b"messages")?, limits)?,
    })
}

fn read_completion(tokens: &[Token], limits: &Limits) -> Result<Completion, Error> {
    object(tokens)?;
    Ok(Completion { blocks: read_list_block(required(tokens, b"blocks")?, limits)? })
}

fn read_command_exit(tokens: &[Token], _limits: &Limits) -> Result<CommandExit, Error> {
    object(tokens)?;
    match field(tokens, b"code")? {
        Some(code) => {
            if field(tokens, b"signal")?.is_some() {
                return Err(Error::Shape);
            }
            Ok(CommandExit::Code { code: signed(code)? })
        }
        None => Ok(CommandExit::Signal { signal: unsigned(required(tokens, b"signal")?)? }),
    }
}

fn read_check_exit(tokens: &[Token], limits: &Limits) -> Result<CheckExit, Error> {
    object(tokens)?;
    let kind = text(required(tokens, b"kind")?, limits.string.max(9))?;
    match kind.as_ref() {
        b"code" => Ok(CheckExit::Code { code: signed(required(tokens, b"value")?)? }),
        b"signal" => Ok(CheckExit::Signal { signal: unsigned(required(tokens, b"value")?)? }),
        b"timed_out" => Ok(CheckExit::TimedOut),
        b"unstarted" => Ok(CheckExit::Unstarted),
        b"cancelled" => Ok(CheckExit::Cancelled),
        _ => {
            if kind.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Ok(CheckExit::Unknown(kind))
        }
    }
}

fn read_session_started(tokens: &[Token], limits: &Limits) -> Result<SessionStarted, Error> {
    object(tokens)?;
    let mode = read_mode(required(tokens, b"mode")?, limits)?;
    let chat = match &mode {
        Mode::Agent => None,
        Mode::Exec | Mode::Chat | Mode::Unknown(_) => match field(tokens, b"chat")? {
            Some([Token::Null]) | None => None,
            Some(value) => Some(text(value, limits.string)?),
        },
    };
    Ok(SessionStarted {
        chat,
        wall_ms: unsigned(required(tokens, b"wall_ms")?)?,
        agent: read_agent(required(tokens, b"agent")?, limits)?,
        mode,
        containment: read_containment(required(tokens, b"containment")?, limits)?,
        pid: unsigned(required(tokens, b"pid")?)?,
        profile: text(required(tokens, b"profile")?, limits.string)?,
        capture: read_capture(required(tokens, b"capture")?, limits)?,
        delivery: read_delivery(required(tokens, b"delivery")?, limits)?,
        versions: read_versions(required(tokens, b"versions")?, limits)?,
        limits: read_effective_limits(required(tokens, b"limits")?, limits)?,
    })
}

fn read_effective_limits(tokens: &[Token], limits: &Limits) -> Result<List<EffectiveLimit>, Error> {
    object(tokens)?;
    let mut values: List<EffectiveLimit> = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let name = match tokens.get(at).ok_or(Error::Shape)? {
            Token::Key(name) => {
                if name.is_empty() {
                    return Err(Error::Shape);
                }
                if name.len() > usize::try_from(limits.string).expect("u32 fits usize") {
                    return Err(Error::Limits);
                }
                bytes::copy_of(name)
            }
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => {
                return Err(Error::Shape);
            }
        };
        for previous in &values {
            if previous.name == name {
                return Err(Error::Shape);
            }
        }
        let start = at.checked_add(1).ok_or(Error::Limits)?;
        at = value_end(tokens, start)?;
        let value = tokens.get(start..at).ok_or(Error::Shape)?;
        let value = match value.first().ok_or(Error::Shape)? {
            Token::Number(_) => LimitValue::Integer(unsigned(value)?),
            Token::ObjectStart => {
                let numerator = unsigned(required(value, b"numerator")?)?;
                let denominator = unsigned(required(value, b"denominator")?)?;
                if denominator == 0 {
                    return Err(Error::Shape);
                }
                LimitValue::Fraction { numerator, denominator }
            }
            Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::Key(_)
            | Token::String(_)
            | Token::True
            | Token::False
            | Token::Null => return Err(Error::Shape),
        };
        values.push(EffectiveLimit { name, value }).or(Err(Error::Limits))?;
    }
    Ok(values)
}

fn read_session_ended(tokens: &[Token], limits: &Limits, session_ended_aborted: bool) -> Result<SessionEnded, Error> {
    object(tokens)?;
    Ok(SessionEnded {
        exit: signed(required(tokens, b"exit")?)?,
        answered: boolean(required(tokens, b"answered")?)?,
        aborted: if session_ended_aborted { Some(boolean(required(tokens, b"aborted")?)?) } else { None },
        teardown_ms: match required(tokens, b"teardown_ms")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        emitted: read_list_count(required(tokens, b"emitted")?, limits)?,
        loss: read_loss(required(tokens, b"loss")?, limits)?,
        cpu_ms: match required(tokens, b"cpu_ms")? {
            [Token::Null] => None,
            item => Some(read_cpu(item, limits)?),
        },
        peak_rss_bytes: match required(tokens, b"peak_rss_bytes")? {
            [Token::Null] => None,
            item => Some(read_peak_rss(item, limits)?),
        },
    })
}

fn read_run_started(tokens: &[Token], limits: &Limits) -> Result<RunStarted, Error> {
    object(tokens)?;
    Ok(RunStarted {
        run: unsigned(required(tokens, b"run")?)?,
        resumed: boolean(required(tokens, b"resumed")?)?,
        messages: unsigned(required(tokens, b"messages")?)?,
        main: read_model(required(tokens, b"main")?, limits)?,
        budget: read_budget(required(tokens, b"budget")?, limits)?,
        tools: read_tools(required(tokens, b"tools")?, limits)?,
        contract: read_list_form(required(tokens, b"contract")?, limits)?,
    })
}

fn read_run_completed(tokens: &[Token], limits: &Limits) -> Result<RunCompleted, Error> {
    object(tokens)?;
    Ok(RunCompleted {
        wind_down: boolean(required(tokens, b"wind_down")?)?,
        run: unsigned(required(tokens, b"run")?)?,
        status: read_status(required(tokens, b"status")?, limits)?,
        failure: match field(tokens, b"failure")? {
            Some(item) => Some(read_answer_failure(item, limits)?),
            None => None,
        },
        result: match field(tokens, b"result")? {
            Some(item) => Some(read_run_result(item, limits)?),
            None => None,
        },
        turns: unsigned(required(tokens, b"turns")?)?,
        spent: unsigned(required(tokens, b"spent")?)?,
        usage: read_usage(required(tokens, b"usage")?, limits)?,
        read: match required(tokens, b"read")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        unread: unsigned(required(tokens, b"unread")?)?,
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
    })
}

fn read_conversation_opened(tokens: &[Token], limits: &Limits) -> Result<ConversationOpened, Error> {
    object(tokens)?;
    Ok(ConversationOpened {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        kind: read_conversation_kind(required(tokens, b"kind")?, limits)?,
        parent: match field(tokens, b"parent")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        call: match field(tokens, b"call")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        thread: unsigned(required(tokens, b"thread")?)?,
        model: read_model(required(tokens, b"model")?, limits)?,
        requested: match field(tokens, b"requested")? {
            Some(item) => Some(read_requested(item)?),
            None => None,
        },
        share: match field(tokens, b"share")? {
            Some(item) => Some(read_share(item)?),
            None => None,
        },
        clamped: match field(tokens, b"clamped")? {
            Some(item) => read_list_clamp(item, limits)?,
            None => List::with_capacity(0),
        },
    })
}

fn read_conversation_closed(tokens: &[Token], limits: &Limits) -> Result<ConversationClosed, Error> {
    object(tokens)?;
    Ok(ConversationClosed {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        end: read_conversation_end(required(tokens, b"end")?, limits)?,
        which: match field(tokens, b"which")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        failure: match field(tokens, b"failure")? {
            Some(item) => Some(read_completion_failure(item, limits)?),
            None => None,
        },
        turns: unsigned(required(tokens, b"turns")?)?,
        usage: read_usage(required(tokens, b"usage")?, limits)?,
        spent: unsigned(required(tokens, b"spent")?)?,
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
    })
}

fn read_response_started(tokens: &[Token], limits: &Limits) -> Result<ResponseStarted, Error> {
    object(tokens)?;
    Ok(ResponseStarted {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        response: unsigned(required(tokens, b"response")?)?,
        turn: unsigned(required(tokens, b"turn")?)?,
        attempt: unsigned(required(tokens, b"attempt")?)?,
        window: unsigned(required(tokens, b"window")?)?,
        model: read_model(required(tokens, b"model")?, limits)?,
        messages: unsigned(required(tokens, b"messages")?)?,
        output_tokens: unsigned(required(tokens, b"output_tokens")?)?,
        tool_choice: read_tool_choice(required(tokens, b"tool_choice")?, limits)?,
        prompt: match field(tokens, b"prompt")? {
            Some(item) => Some(read_prompt(item, limits)?),
            None => None,
        },
    })
}

fn read_response_completed(tokens: &[Token], limits: &Limits) -> Result<ResponseCompleted, Error> {
    object(tokens)?;
    Ok(ResponseCompleted {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        response: unsigned(required(tokens, b"response")?)?,
        outcome: read_outcome(required(tokens, b"outcome")?, limits)?,
        stop: match field(tokens, b"stop")? {
            Some(item) => Some(read_stop(item, limits)?),
            None => None,
        },
        blocks: unsigned(required(tokens, b"blocks")?)?,
        calls: unsigned(required(tokens, b"calls")?)?,
        invalid: unsigned(required(tokens, b"invalid")?)?,
        oversized: unsigned(required(tokens, b"oversized")?)?,
        cut: unsigned(required(tokens, b"cut")?)?,
        usage: read_usage(required(tokens, b"usage")?, limits)?,
        spent: unsigned(required(tokens, b"spent")?)?,
        request_bytes: match required(tokens, b"request_bytes")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        first_byte_ms: match required(tokens, b"first_byte_ms")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        largest_gap_ms: match required(tokens, b"largest_gap_ms")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
        failure: match field(tokens, b"failure")? {
            Some(item) => Some(read_completion_failure(item, limits)?),
            None => None,
        },
        retry_ms: match field(tokens, b"retry_ms")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        completion: match field(tokens, b"completion")? {
            Some(item) => Some(read_completion(item, limits)?),
            None => None,
        },
    })
}

fn read_text_delta(tokens: &[Token], limits: &Limits) -> Result<TextDelta, Error> {
    object(tokens)?;
    Ok(TextDelta {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        response: unsigned(required(tokens, b"response")?)?,
        block: unsigned(required(tokens, b"block")?)?,
        text: text(required(tokens, b"text")?, limits.content)?,
    })
}

fn read_tool_started(tokens: &[Token], limits: &Limits) -> Result<ToolStarted, Error> {
    object(tokens)?;
    Ok(ToolStarted {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        call: text(required(tokens, b"call")?, limits.string)?,
        tool: text(required(tokens, b"tool")?, limits.string)?,
        source: read_source(required(tokens, b"source")?, limits)?,
        effect: read_effect(required(tokens, b"effect")?, limits)?,
        deadline_ms: match required(tokens, b"deadline_ms")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
        input_bytes: unsigned(required(tokens, b"input_bytes")?)?,
        input: match field(tokens, b"input")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
    })
}

fn read_child_completed(tokens: &[Token], limits: &Limits) -> Result<ChildCompleted, Error> {
    object(tokens)?;
    let value = text(required(tokens, b"stop")?, limits.string)?;
    let stop = match value.as_ref() {
        b"end" => ChildStop::End,
        b"budget" => ChildStop::Budget,
        b"failed" => ChildStop::Failed,
        b"closed" => ChildStop::Closed,
        _ => ChildStop::Unknown(value),
    };
    Ok(ChildCompleted {
        conversation: unsigned(required(tokens, b"conversation")?)?,
        stop,
        turns: unsigned(required(tokens, b"turns")?)?,
        time_ms: unsigned(required(tokens, b"time_ms")?)?,
        spent: unsigned(required(tokens, b"spent")?)?,
        usage: read_usage(required(tokens, b"usage")?, limits)?,
    })
}

fn read_tool_completed(tokens: &[Token], limits: &Limits) -> Result<ToolCompleted, Error> {
    object(tokens)?;
    Ok(ToolCompleted {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        call: text(required(tokens, b"call")?, limits.string)?,
        tool: text(required(tokens, b"tool")?, limits.string)?,
        verdict: read_verdict(required(tokens, b"verdict")?, limits)?,
        exit: match field(tokens, b"exit")? {
            Some(item) => Some(read_command_exit(item, limits)?),
            None => None,
        },
        job: match field(tokens, b"job")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        bytes: unsigned(required(tokens, b"bytes")?)?,
        omitted_bytes: unsigned(required(tokens, b"omitted_bytes")?)?,
        clamped: boolean(required(tokens, b"clamped")?)?,
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
        child: match field(tokens, b"child")? {
            Some(item) => Some(read_child_completed(item, limits)?),
            None => None,
        },
        delivery: match field(tokens, b"delivery")? {
            Some(item) => Some(read_delivered(item, limits)?),
            None => None,
        },
    })
}

fn read_job_ended(tokens: &[Token], limits: &Limits) -> Result<JobEnded, Error> {
    object(tokens)?;
    Ok(JobEnded {
        run: unsigned(required(tokens, b"run")?)?,
        conversation: unsigned(required(tokens, b"conversation")?)?,
        job: unsigned(required(tokens, b"job")?)?,
        call: text(required(tokens, b"call")?, limits.string)?,
        verdict: read_verdict(required(tokens, b"verdict")?, limits)?,
        exit: match field(tokens, b"exit")? {
            Some(item) => Some(read_command_exit(item, limits)?),
            None => None,
        },
        unread_bytes: unsigned(required(tokens, b"unread_bytes")?)?,
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
    })
}

fn read_check_started(tokens: &[Token], _limits: &Limits) -> Result<CheckStarted, Error> {
    object(tokens)?;
    Ok(CheckStarted {
        run: unsigned(required(tokens, b"run")?)?,
        deadline_ms: match required(tokens, b"deadline_ms")? {
            [Token::Null] => None,
            item => Some(unsigned(item)?),
        },
    })
}

fn read_check_completed(tokens: &[Token], limits: &Limits) -> Result<CheckCompleted, Error> {
    object(tokens)?;
    Ok(CheckCompleted {
        run: unsigned(required(tokens, b"run")?)?,
        exit: read_check_exit(required(tokens, b"exit")?, limits)?,
        passed: boolean(required(tokens, b"passed")?)?,
        duration_ms: unsigned(required(tokens, b"duration_ms")?)?,
    })
}

fn read_message_received(tokens: &[Token], limits: &Limits) -> Result<MessageReceived, Error> {
    object(tokens)?;
    Ok(MessageReceived {
        run: unsigned(required(tokens, b"run")?)?,
        name: unsigned(required(tokens, b"name")?)?,
        bytes: unsigned(required(tokens, b"bytes")?)?,
        refused: match field(tokens, b"refused")? {
            Some(item) => Some(read_message_refusal(item, limits)?),
            None => None,
        },
        label: match field(tokens, b"label")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        text: match field(tokens, b"text")? {
            Some(item) => Some(text(item, limits.content)?),
            None => None,
        },
    })
}

fn read_message_refusal(tokens: &[Token], limits: &Limits) -> Result<MessageRefusal, Error> {
    let value = text(tokens, limits.string.max(12))?;
    Ok(match value.as_ref() {
        b"too_large" => MessageRefusal::TooLarge,
        b"inbox_full" => MessageRefusal::InboxFull,
        b"name_in_use" => MessageRefusal::NameInUse,
        b"ending" => MessageRefusal::Ending,
        b"start_refused" => MessageRefusal::StartRefused,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            MessageRefusal::Unknown(value)
        }
    })
}

fn read_message_read(tokens: &[Token]) -> Result<MessageRead, Error> {
    object(tokens)?;
    Ok(MessageRead {
        run: unsigned(required(tokens, b"run")?)?,
        name: unsigned(required(tokens, b"name")?)?,
        turn: unsigned(required(tokens, b"turn")?)?,
    })
}

fn read_message_unread(tokens: &[Token]) -> Result<MessageUnread, Error> {
    object(tokens)?;
    Ok(MessageUnread { run: unsigned(required(tokens, b"run")?)?, name: unsigned(required(tokens, b"name")?)? })
}

fn read_forced(tokens: &[Token], limits: &Limits) -> Result<Forced, Error> {
    let value = text(tokens, limits.string)?;
    Ok(match value.as_ref() {
        b"no" => Forced::No,
        b"terminated" => Forced::Terminated,
        b"killed" => Forced::Killed,
        _ => Forced::Unknown(value),
    })
}

fn read_notice(tokens: &[Token], limits: &Limits) -> Result<Notice, Error> {
    object(tokens)?;
    let notice = Notice {
        code: match field(tokens, b"code")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
        exit: match field(tokens, b"exit")? {
            Some([Token::Null]) | None => None,
            Some(item) => Some(read_agent_exit(item)?),
        },
        forced: match field(tokens, b"forced")? {
            Some(item) => Some(read_forced(item, limits)?),
            None => None,
        },
        level: read_level(required(tokens, b"level")?, limits)?,
        kind: read_notice_kind(required(tokens, b"kind")?, limits)?,
        run: match field(tokens, b"run")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        account: match field(tokens, b"account")? {
            Some(item) => Some(match u32::try_from(unsigned(item)?) {
                Ok(value) => value,
                Err(_) => return Err(Error::Shape),
            }),
            None => None,
        },
        bytes: match field(tokens, b"bytes")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        reset_ms: match field(tokens, b"reset_ms")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
    };
    match notice.kind {
        NoticeKind::ForcedExit if notice.forced.is_none() || field(tokens, b"exit")?.is_none() => Err(Error::Shape),
        NoticeKind::ReasoningDropped if notice.bytes.is_none() => Err(Error::Shape),
        NoticeKind::AccountRefused if notice.code.is_none() => Err(Error::Shape),
        NoticeKind::CredentialUnauthorized | NoticeKind::AccountLimited if notice.account.is_none() => {
            Err(Error::Shape)
        }
        NoticeKind::ReasoningDropped
        | NoticeKind::CredentialUnauthorized
        | NoticeKind::AccountLimited
        | NoticeKind::AccountRefused
        | NoticeKind::TeardownAborted
        | NoticeKind::ForcedExit
        | NoticeKind::Unknown(_) => Ok(notice),
    }
}

fn read_block(tokens: &[Token], limits: &Limits) -> Result<Block, Error> {
    object(tokens)?;
    let kind = text(required(tokens, b"type")?, limits.string.max(64))?;
    Ok(match kind.as_ref() {
        b"text" => Block::Text { text: text(required(tokens, b"text")?, limits.content)? },
        b"refusal" => Block::Refusal { text: text(required(tokens, b"text")?, limits.content)? },
        b"call" => Block::Call {
            call: text(required(tokens, b"call")?, limits.string)?,
            tool: text(required(tokens, b"tool")?, limits.string)?,
            input: text(required(tokens, b"input")?, limits.content)?,
        },
        b"oversized" => Block::Oversized {
            call: text(required(tokens, b"call")?, limits.string)?,
            tool: text(required(tokens, b"tool")?, limits.string)?,
            bytes: unsigned(required(tokens, b"bytes")?)?,
        },
        b"cut" => Block::Cut {
            call: text(required(tokens, b"call")?, limits.string)?,
            tool: text(required(tokens, b"tool")?, limits.string)?,
            input: text(required(tokens, b"input")?, limits.content)?,
        },
        b"result" => Block::Result {
            call: text(required(tokens, b"call")?, limits.string)?,
            error: boolean(required(tokens, b"error")?)?,
            text: text(required(tokens, b"text")?, limits.content)?,
        },
        b"opaque" => Block::Opaque { bytes: unsigned(required(tokens, b"bytes")?)? },
        _ => {
            if kind.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            Block::Unknown { kind }
        }
    })
}

fn read_list_block(tokens: &[Token], limits: &Limits) -> Result<List<Block>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = read_block(item, limits)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_list_count(tokens: &[Token], limits: &Limits) -> Result<List<Count>, Error> {
    object(tokens)?;
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let key = match tokens.get(at).ok_or(Error::Shape)? {
            Token::Key(key) => key,
            Token::ObjectStart
            | Token::ObjectEnd
            | Token::ArrayStart
            | Token::ArrayEnd
            | Token::String(_)
            | Token::Number(_)
            | Token::True
            | Token::False
            | Token::Null => return Err(Error::Shape),
        };
        if key.len() > usize::try_from(limits.string).expect("u32 fits") {
            return Err(Error::Limits);
        }
        for value in &values {
            let value: &Count = value;
            if value.record == *key {
                return Err(Error::Shape);
            }
        }
        let start = at.checked_add(1).ok_or(Error::Limits)?;
        at = value_end(tokens, start)?;
        let count = unsigned(tokens.get(start..at).ok_or(Error::Shape)?)?;
        if values.push(Count { record: key.clone(), count }).is_err() {
            return Err(Error::Limits);
        }
    }
    Ok(values)
}

fn read_list_family(tokens: &[Token], limits: &Limits) -> Result<List<Family>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = read_family(item, limits)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_list_field(tokens: &[Token], limits: &Limits) -> Result<List<Field>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = read_field(item, limits)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_list_form(tokens: &[Token], limits: &Limits) -> Result<List<Form>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = read_form(item, limits)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_list_message(tokens: &[Token], limits: &Limits) -> Result<List<Message>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = read_message(item, limits)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_list_text(tokens: &[Token], limits: &Limits) -> Result<List<Box<[u8]>>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        let value = text(item, limits.string)?;
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_budget_stage(tokens: &[Token], limits: &Limits) -> Result<BudgetStage, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"ordinary" => BudgetStage::Ordinary,
        b"wind_down" => BudgetStage::WindDown,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            BudgetStage::Unknown(value)
        }
    })
}

fn read_share_part(tokens: &[Token], limits: &Limits) -> Result<SharePart, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"turns" => SharePart::Turns,
        b"time" => SharePart::Time,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            SharePart::Unknown(value)
        }
    })
}

fn read_share_bound(tokens: &[Token], limits: &Limits) -> Result<ShareBound, Error> {
    let value = text(tokens, limits.string.max(64))?;
    Ok(match value.as_ref() {
        b"reserve" => ShareBound::Reserve,
        b"deadline" => ShareBound::Deadline,
        _ => {
            if value.len() > usize::try_from(limits.string).expect("u32 fits") {
                return Err(Error::Limits);
            }
            ShareBound::Unknown(value)
        }
    })
}

fn read_requested(tokens: &[Token]) -> Result<Requested, Error> {
    object(tokens)?;
    Ok(Requested {
        turns: match field(tokens, b"turns")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
        seconds: match field(tokens, b"seconds")? {
            Some(item) => Some(unsigned(item)?),
            None => None,
        },
    })
}

fn read_share(tokens: &[Token]) -> Result<Share, Error> {
    object(tokens)?;
    Ok(Share {
        turns: unsigned(required(tokens, b"turns")?)?,
        time_ms: unsigned(required(tokens, b"time_ms")?)?,
        spend: unsigned(required(tokens, b"spend")?)?,
    })
}

fn read_quantity(tokens: &[Token]) -> Result<Quantity, Error> {
    object(tokens)?;
    Ok(Quantity { used: unsigned(required(tokens, b"used")?)?, limit: unsigned(required(tokens, b"limit")?)? })
}

fn read_reserve(tokens: &[Token]) -> Result<Reserve, Error> {
    object(tokens)?;
    Ok(Reserve {
        turns: unsigned(required(tokens, b"turns")?)?,
        time_ms: unsigned(required(tokens, b"time_ms")?)?,
        spend: unsigned(required(tokens, b"spend")?)?,
    })
}

fn read_budget_standing(tokens: &[Token], limits: &Limits) -> Result<BudgetStanding, Error> {
    object(tokens)?;
    Ok(BudgetStanding {
        run: unsigned(required(tokens, b"run")?)?,
        stage: read_budget_stage(required(tokens, b"stage")?, limits)?,
        turns: read_quantity(required(tokens, b"turns")?)?,
        spend: read_quantity(required(tokens, b"spend")?)?,
        time_ms: read_quantity(required(tokens, b"time_ms")?)?,
        reserve: read_reserve(required(tokens, b"reserve")?)?,
        call: match field(tokens, b"call")? {
            Some(item) => Some(text(item, limits.string)?),
            None => None,
        },
    })
}

fn read_list_clamp(tokens: &[Token], limits: &Limits) -> Result<List<Clamp>, Error> {
    if tokens.first() != Some(&Token::ArrayStart) || tokens.last() != Some(&Token::ArrayEnd) {
        return Err(Error::Shape);
    }
    let mut values = List::with_capacity(limits.items);
    let mut at = 1_usize;
    for _ in 0..tokens.len() {
        if at >= tokens.len().saturating_sub(1) {
            break;
        }
        let end = value_end(tokens, at)?;
        let item = tokens.get(at..end).ok_or(Error::Shape)?;
        object(item)?;
        let value = Clamp {
            part: read_share_part(required(item, b"part")?, limits)?,
            by: read_share_bound(required(item, b"by")?, limits)?,
        };
        if values.push(value).is_err() {
            return Err(Error::Limits);
        }
        at = end;
    }
    Ok(values)
}

fn read_account_state(tokens: &[Token], limits: &Limits) -> Result<AccountState, Error> {
    let value = text(tokens, limits.string)?;
    Ok(match value.as_ref() {
        b"limited" => AccountState::Limited,
        b"refused" => AccountState::Refused,
        b"unauthorized" => AccountState::Unauthorized,
        _ => AccountState::Unknown(value),
    })
}
