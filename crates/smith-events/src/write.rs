//! Sized deterministic encoding; capture removes content fields.
//! Contract: protocol/events.md, sections 5.3 and 6.

use crate::records::{
    AccountState, Agent, AgentExit, AnswerClass, AnswerFailure, Backend, Block, Budget, BudgetStage, BudgetStanding,
    Capture, CheckCompleted, CheckExit, CheckStarted, ChildCompleted, ChildStop, CommandExit, Completion,
    CompletionFailure, Containment, ConversationClosed, ConversationEnd, ConversationKind, ConversationOpened, Count,
    Cpu, Delivered, Delivery, Effect, EffectiveLimit, Event, Evidence, FailureClass, FailurePhase, Family, Field,
    Forced, Form, JobEnded, Level, LimitValue, Loss, Message, MessageRead, MessageReceived, MessageRefusal,
    MessageUnread, Mode, Model, Notice, NoticeKind, Outcome, PeakRss, Prompt, Quantity, Record, Requested, Reserve,
    ResponseCompleted, ResponseStarted, Role, RunCompleted, RunResult, RunStarted, SessionEnded, SessionStarted, Share,
    ShareBound, SharePart, Source, Status, Stop, TextDelta, ToolChoice, ToolCompleted, ToolStarted, Tools, Usage,
    Verdict, Versions,
};
use crate::{Error, Limits};
use alloc::boxed::Box;
use skein_json::writer::Encoder;
use skein_lib::{List, Writer};

/// Encode one line, or omit live text when capture does not request it.
pub fn write(record: &Record, capture: &Capture, limits: &Limits) -> Result<Option<Box<[u8]>>, Error> {
    let capacity = crate::limits::largest_size(&record.event, capture, limits).ok_or(Error::Limits)?;
    write_with_ceiling(record, capture, limits, capacity)
}

/// Encode within the owner's joint line ceiling, including the newline;
/// independent string, content and list caps still validate the whole record.
pub fn write_with_ceiling(
    record: &Record,
    capture: &Capture,
    limits: &Limits,
    encoded_line: u32,
) -> Result<Option<Box<[u8]>>, Error> {
    let bounded = skein_json::writer::Limits { depth: 16, length: encoded_line.checked_sub(1).ok_or(Error::Limits)? };
    match &record.event {
        Event::TextDelta(_) if *capture != Capture::Everything => return Ok(None),
        Event::Budget(_)
        | Event::SessionStarted(_)
        | Event::SessionEnded(_)
        | Event::RunStarted(_)
        | Event::RunCompleted(_)
        | Event::ConversationOpened(_)
        | Event::ConversationClosed(_)
        | Event::ResponseStarted(_)
        | Event::ResponseCompleted(_)
        | Event::TextDelta(_)
        | Event::ToolStarted(_)
        | Event::JobEnded(_)
        | Event::ToolCompleted(_)
        | Event::CheckStarted(_)
        | Event::CheckCompleted(_)
        | Event::MessageReceived(_)
        | Event::MessageRead(_)
        | Event::MessageUnread(_)
        | Event::Notice(_) => {}
    }
    validate_record(record, limits)?;
    let mut measure = Encoder::measure(&bounded);
    encode(&mut measure, record, capture);
    let length = match measure.measured() {
        Ok(length) => length,
        Err(refusal) => return Err(map_refusal(refusal)),
    };
    let mut encoder = Encoder::write(length, &bounded);
    encode(&mut encoder, record, capture);
    let body = encoder.finish();
    let mut line = Writer::new(body.len().checked_add(1).ok_or(Error::Limits)?);
    line.put(&body).expect("measured line room");
    line.put(b"\n").expect("newline room reserved");
    Ok(Some(line.finish()))
}

fn map_refusal(refusal: skein_json::writer::Refusal) -> Error {
    match refusal {
        skein_json::writer::Refusal::Text => Error::Text,
        skein_json::writer::Refusal::TooLong
        | skein_json::writer::Refusal::TooDeep
        | skein_json::writer::Refusal::Number => Error::Limits,
    }
}

fn encode(json: &mut Encoder, record: &Record, capture: &Capture) {
    json.object_start();
    json.key(b"v");
    json.unsigned(crate::VERSION);
    json.key(b"type");
    match &record.event {
        Event::Budget(_) => json.string(b"budget"),
        Event::SessionStarted(_) => json.string(b"session.started"),
        Event::SessionEnded(_) => json.string(b"session.ended"),
        Event::RunStarted(_) => json.string(b"run.started"),
        Event::RunCompleted(_) => json.string(b"run.completed"),
        Event::ConversationOpened(_) => json.string(b"conversation.opened"),
        Event::ConversationClosed(_) => json.string(b"conversation.closed"),
        Event::ResponseStarted(_) => json.string(b"response.started"),
        Event::ResponseCompleted(_) => json.string(b"response.completed"),
        Event::TextDelta(_) => json.string(b"text.delta"),
        Event::ToolStarted(_) => json.string(b"tool.started"),
        Event::JobEnded(_) => json.string(b"job.ended"),
        Event::ToolCompleted(_) => json.string(b"tool.completed"),
        Event::CheckStarted(_) => json.string(b"check.started"),
        Event::CheckCompleted(_) => json.string(b"check.completed"),
        Event::MessageReceived(_) => json.string(b"message.received"),
        Event::MessageRead(_) => json.string(b"message.read"),
        Event::MessageUnread(_) => json.string(b"message.unread"),
        Event::Notice(_) => json.string(b"notice"),
    }
    json.key(b"t_ms");
    json.unsigned(record.t_ms);
    match &record.event {
        Event::Budget(value) => encode_budget_standing(json, value, capture),
        Event::SessionStarted(value) => encode_session_started(json, value, capture),
        Event::SessionEnded(value) => encode_session_ended(json, value, capture),
        Event::RunStarted(value) => encode_run_started(json, value, capture),
        Event::RunCompleted(value) => encode_run_completed(json, value, capture),
        Event::ConversationOpened(value) => encode_conversation_opened(json, value, capture),
        Event::ConversationClosed(value) => encode_conversation_closed(json, value, capture),
        Event::ResponseStarted(value) => encode_response_started(json, value, capture),
        Event::ResponseCompleted(value) => encode_response_completed(json, value, capture),
        Event::TextDelta(value) => encode_text_delta(json, value, capture),
        Event::ToolStarted(value) => encode_tool_started(json, value, capture),
        Event::JobEnded(value) => encode_job_ended(json, value, capture),
        Event::ToolCompleted(value) => encode_tool_completed(json, value, capture),
        Event::CheckStarted(value) => encode_check_started(json, value, capture),
        Event::CheckCompleted(value) => encode_check_completed(json, value, capture),
        Event::MessageReceived(value) => encode_message_received(json, value, capture),
        Event::MessageRead(value) => encode_message_read(json, value),
        Event::MessageUnread(value) => encode_message_unread(json, value),
        Event::Notice(value) => encode_notice(json, value, capture),
    }
    json.object_end();
}

fn write_capture(json: &mut Encoder, value: &Capture, _capture: &Capture) {
    match value {
        Capture::None => json.string(b"none"),
        Capture::Calls => json.string(b"calls"),
        Capture::Everything => json.string(b"everything"),
        Capture::Unknown(text) => json.string(text),
    }
}

fn write_delivery(json: &mut Encoder, value: &Delivery, _capture: &Capture) {
    match value {
        Delivery::Complete => json.string(b"complete"),
        Delivery::BestEffort => json.string(b"best_effort"),
        Delivery::Unknown(text) => json.string(text),
    }
}

fn write_mode(json: &mut Encoder, value: &Mode, _capture: &Capture) {
    match value {
        Mode::Agent => json.string(b"agent"),
        Mode::Exec => json.string(b"exec"),
        Mode::Chat => json.string(b"chat"),
        Mode::Unknown(text) => json.string(text),
    }
}

fn write_containment(json: &mut Encoder, value: &Containment, _capture: &Capture) {
    match value {
        Containment::Groups => json.string(b"groups"),
        Containment::Trees => json.string(b"trees"),
        Containment::Unknown(text) => json.string(text),
    }
}

fn write_family(json: &mut Encoder, value: &Family, _capture: &Capture) {
    match value {
        Family::Inspect => json.string(b"inspect"),
        Family::Modify => json.string(b"modify"),
        Family::Shell => json.string(b"shell"),
        Family::SubAgents => json.string(b"sub_agents"),
        Family::Unknown(text) => json.string(text),
    }
}

fn write_form(json: &mut Encoder, value: &Form, _capture: &Capture) {
    match value {
        Form::Report => json.string(b"report"),
        Form::Verdict => json.string(b"verdict"),
        Form::Change => json.string(b"change"),
        Form::Failure => json.string(b"failure"),
        Form::Unknown(text) => json.string(text),
    }
}

fn write_status(json: &mut Encoder, value: &Status, _capture: &Capture) {
    match value {
        Status::Accepted => json.string(b"accepted"),
        Status::Parked => json.string(b"parked"),
        Status::Failed => json.string(b"failed"),
        Status::Refused => json.string(b"refused"),
        Status::Unknown(text) => json.string(text),
    }
}

fn write_conversation_kind(json: &mut Encoder, value: &ConversationKind, _capture: &Capture) {
    match value {
        ConversationKind::Main => json.string(b"main"),
        ConversationKind::Child => json.string(b"child"),
        ConversationKind::Compaction => json.string(b"compaction"),
        ConversationKind::Unknown(text) => json.string(text),
    }
}

fn write_conversation_end(json: &mut Encoder, value: &ConversationEnd, _capture: &Capture) {
    match value {
        ConversationEnd::Closed => json.string(b"closed"),
        ConversationEnd::Budget => json.string(b"budget"),
        ConversationEnd::Failed => json.string(b"failed"),
        ConversationEnd::Refused => json.string(b"refused"),
        ConversationEnd::Transcript => json.string(b"transcript"),
        ConversationEnd::Overflow => json.string(b"overflow"),
        ConversationEnd::Unknown(text) => json.string(text),
    }
}

fn write_tool_choice(json: &mut Encoder, value: &ToolChoice) {
    match value {
        ToolChoice::Auto => json.string(b"auto"),
        ToolChoice::None => json.string(b"none"),
        ToolChoice::Only => json.string(b"only"),
        ToolChoice::Unknown(text) => json.string(text),
    }
}

fn write_outcome(json: &mut Encoder, value: &Outcome, _capture: &Capture) {
    match value {
        Outcome::Completed => json.string(b"completed"),
        Outcome::Failed => json.string(b"failed"),
        Outcome::Cancelled => json.string(b"cancelled"),
        Outcome::Unknown(text) => json.string(text),
    }
}

fn write_stop(json: &mut Encoder, value: &Stop, _capture: &Capture) {
    match value {
        Stop::End => json.string(b"end"),
        Stop::Tools => json.string(b"tools"),
        Stop::MaxTokens => json.string(b"max_tokens"),
        Stop::Refusal => json.string(b"refusal"),
        Stop::Unknown(text) => json.string(text),
    }
}

fn write_source(json: &mut Encoder, value: &Source, _capture: &Capture) {
    match value {
        Source::Workspace => json.string(b"workspace"),
        Source::Run => json.string(b"run"),
        Source::Host => json.string(b"host"),
        Source::Unknown(text) => json.string(text),
    }
}

fn write_effect(json: &mut Encoder, value: &Effect, _capture: &Capture) {
    match value {
        Effect::Read => json.string(b"read"),
        Effect::Write => json.string(b"write"),
        Effect::Unknown(text) => json.string(text),
    }
}

fn write_verdict(json: &mut Encoder, value: &Verdict, _capture: &Capture) {
    match value {
        Verdict::Read => json.string(b"read"),
        Verdict::Listed => json.string(b"listed"),
        Verdict::Found => json.string(b"found"),
        Verdict::Removed => json.string(b"removed"),
        Verdict::NotEmpty => json.string(b"not_empty"),
        Verdict::Written => json.string(b"written"),
        Verdict::Edited => json.string(b"edited"),
        Verdict::Exited => json.string(b"exited"),
        Verdict::Conflict => json.string(b"conflict"),
        Verdict::Missing => json.string(b"missing"),
        Verdict::TooLarge => json.string(b"too_large"),
        Verdict::TimedOut => json.string(b"timed_out"),
        Verdict::Running => json.string(b"running"),
        Verdict::Stopped => json.string(b"stopped"),
        Verdict::Failed => json.string(b"failed"),
        Verdict::Cancelled => json.string(b"cancelled"),
        Verdict::Ambiguous => json.string(b"ambiguous"),
        Verdict::Result => json.string(b"result"),
        Verdict::Error => json.string(b"error"),
        Verdict::Invalid => json.string(b"invalid"),
        Verdict::NotRun => json.string(b"not_run"),
        Verdict::Withdrawn => json.string(b"withdrawn"),
        Verdict::NotGranted => json.string(b"not_granted"),
        Verdict::Outside => json.string(b"outside"),
        Verdict::ReadOnly => json.string(b"read_only"),
        Verdict::TooLong => json.string(b"too_long"),
        Verdict::NotFound => json.string(b"not_found"),
        Verdict::NotFile => json.string(b"not_file"),
        Verdict::Linked => json.string(b"linked"),
        Verdict::Protected => json.string(b"protected"),
        Verdict::NotDirectory => json.string(b"not_directory"),
        Verdict::NotRead => json.string(b"not_read"),
        Verdict::Stale => json.string(b"stale"),
        Verdict::NoMatch => json.string(b"no_match"),
        Verdict::Unchanged => json.string(b"unchanged"),
        Verdict::Busy => json.string(b"busy"),
        Verdict::NulByte => json.string(b"nul_byte"),
        Verdict::Unknown(text) => json.string(text),
    }
}

fn write_delivered(json: &mut Encoder, value: &Delivered, _capture: &Capture) {
    match value {
        Delivered::Delivered => json.string(b"delivered"),
        Delivered::Nothing => json.string(b"nothing"),
        Delivered::Refused => json.string(b"refused"),
        Delivered::Failed => json.string(b"failed"),
        Delivered::Stale => json.string(b"stale"),
        Delivered::Unknown(text) => json.string(text),
    }
}

fn write_failure_class(json: &mut Encoder, value: &FailureClass, _capture: &Capture) {
    match value {
        FailureClass::Overloaded => json.string(b"overloaded"),
        FailureClass::RateLimited => json.string(b"rate_limited"),
        FailureClass::Exhausted => json.string(b"exhausted"),
        FailureClass::Refused => json.string(b"refused"),
        FailureClass::Unsupported => json.string(b"unsupported"),
        FailureClass::Unavailable => json.string(b"unavailable"),
        FailureClass::TimedOut => json.string(b"timed_out"),
        FailureClass::ContextTooLong => json.string(b"context_too_long"),
        FailureClass::Invalid => json.string(b"invalid"),
        FailureClass::Unauthorized => json.string(b"unauthorized"),
        FailureClass::Limit => json.string(b"limit"),
        FailureClass::Protocol => json.string(b"protocol"),
        FailureClass::Cancelled => json.string(b"cancelled"),
        FailureClass::Unknown(text) => json.string(text),
    }
}

fn write_evidence(json: &mut Encoder, value: &Evidence, _capture: &Capture) {
    match value {
        Evidence::Unsent => json.string(b"unsent"),
        Evidence::MaybeSent => json.string(b"maybe_sent"),
        Evidence::Response => json.string(b"response"),
        Evidence::Unknown(text) => json.string(text),
    }
}

fn write_answer_class(json: &mut Encoder, value: &AnswerClass, _capture: &Capture) {
    match value {
        AnswerClass::Model => json.string(b"model"),
        AnswerClass::Budget => json.string(b"budget"),
        AnswerClass::Policy => json.string(b"policy"),
        AnswerClass::Cancelled => json.string(b"cancelled"),
        AnswerClass::Agent => json.string(b"agent"),
        AnswerClass::Host => json.string(b"host"),
        AnswerClass::Stale => json.string(b"stale"),
        AnswerClass::Transcript => json.string(b"transcript"),
        AnswerClass::Busy => json.string(b"busy"),
        AnswerClass::Invalid => json.string(b"invalid"),
        AnswerClass::Unknown(text) => json.string(text),
    }
}

fn write_level(json: &mut Encoder, value: &Level, _capture: &Capture) {
    match value {
        Level::Info => json.string(b"info"),
        Level::Warning => json.string(b"warning"),
        Level::Unknown(text) => json.string(text),
    }
}

fn write_notice_kind(json: &mut Encoder, value: &NoticeKind, _capture: &Capture) {
    match value {
        NoticeKind::ReasoningDropped => json.string(b"reasoning_dropped"),
        NoticeKind::CredentialUnauthorized => json.string(b"credential_unauthorized"),
        NoticeKind::AccountLimited => json.string(b"account_limited"),
        NoticeKind::AccountRefused => json.string(b"account_refused"),
        NoticeKind::TeardownAborted => json.string(b"teardown_aborted"),
        NoticeKind::ForcedExit => json.string(b"forced_exit"),
        NoticeKind::Unknown(text) => json.string(text),
    }
}

fn write_role(json: &mut Encoder, value: &Role, _capture: &Capture) {
    match value {
        Role::User => json.string(b"user"),
        Role::Assistant => json.string(b"assistant"),
        Role::Unknown(text) => json.string(text),
    }
}

fn write_agent(json: &mut Encoder, value: &Agent, _capture: &Capture) {
    json.object_start();
    json.key(b"name");
    json.string(&value.name);
    json.key(b"version");
    json.string(&value.version);
    json.object_end();
}

fn write_versions(json: &mut Encoder, value: &Versions, _capture: &Capture) {
    json.object_start();
    json.key(b"events");
    json.unsigned(value.events);
    json.key(b"channel");
    json.unsigned(value.channel);
    json.key(b"charter");
    json.unsigned(value.charter);
    json.key(b"transcript");
    json.unsigned(value.transcript);
    if let Some(version) = value.chat_store {
        json.key(b"chat_store");
        json.unsigned(version);
    }
    json.object_end();
}

fn write_loss(json: &mut Encoder, value: &Loss, _capture: &Capture) {
    json.object_start();
    json.key(b"events");
    json.unsigned(value.events);
    json.key(b"channel");
    match &value.channel {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.object_end();
}

fn write_cpu(json: &mut Encoder, value: &Cpu, _capture: &Capture) {
    json.object_start();
    json.key(b"user");
    json.unsigned(value.user);
    json.key(b"system");
    json.unsigned(value.system);
    json.key(b"children_user");
    json.unsigned(value.children_user);
    json.key(b"children_system");
    json.unsigned(value.children_system);
    json.object_end();
}

fn write_peak_rss(json: &mut Encoder, value: &PeakRss, _capture: &Capture) {
    json.object_start();
    json.key(b"self");
    json.unsigned(value.self_bytes);
    json.key(b"children");
    json.unsigned(value.children);
    json.object_end();
}

fn write_model(json: &mut Encoder, value: &Model, _capture: &Capture) {
    json.object_start();
    json.key(b"endpoint");
    json.string(&value.endpoint);
    json.key(b"model");
    json.string(&value.model);
    json.key(b"window_tokens");
    json.unsigned(value.window_tokens);
    json.key(b"output_tokens");
    json.unsigned(value.output_tokens);
    json.key(b"effort");
    match &value.effort {
        Some(item) => {
            json.string(item);
        }
        None => json.null(),
    }
    json.key(b"account");
    match value.account {
        Some(account) => json.unsigned(u64::from(account)),
        None => json.null(),
    }
    json.key(b"backend");
    json.string(match &value.backend {
        Backend::Direct => b"direct",
        Backend::ClaudeCode => b"claude_code",
        Backend::Unknown(name) => name,
    });
    json.object_end();
}

fn write_usage(json: &mut Encoder, value: &Usage, _capture: &Capture) {
    json.object_start();
    json.key(b"input_tokens");
    match &value.input_tokens {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"cache_read_tokens");
    match &value.cache_read_tokens {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"cache_write_tokens");
    match &value.cache_write_tokens {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"output_tokens");
    match &value.output_tokens {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"reasoning_tokens");
    match &value.reasoning_tokens {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.object_end();
}

fn write_budget(json: &mut Encoder, value: &Budget, _capture: &Capture) {
    json.object_start();
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"spend");
    json.unsigned(value.spend);
    json.key(b"time_ms");
    json.unsigned(value.time_ms);
    json.key(b"reserve");
    write_reserve(json, &value.reserve);
    json.object_end();
}

fn write_tools(json: &mut Encoder, value: &Tools, capture: &Capture) {
    json.object_start();
    json.key(b"families");
    write_list_family(json, &value.families, capture);
    json.key(b"withheld");
    write_list_family(json, &value.withheld, capture);
    json.key(b"wait");
    json.boolean(value.wait);
    json.key(b"deliver");
    json.boolean(value.deliver);
    json.key(b"host");
    write_list_text(json, &value.host, capture);
    json.object_end();
}

fn write_failure_phase(json: &mut Encoder, value: &FailurePhase) {
    json.string(match value {
        FailurePhase::Connect => b"connect",
        FailurePhase::Handshake => b"handshake",
        FailurePhase::Head => b"head",
        FailurePhase::Idle => b"idle",
        FailurePhase::Whole => b"whole",
        FailurePhase::Unknown(value) => value,
    });
}

fn write_completion_failure(json: &mut Encoder, value: &CompletionFailure, capture: &Capture) {
    json.object_start();
    json.key(b"class");
    write_failure_class(json, &value.class, capture);
    json.key(b"evidence");
    write_evidence(json, &value.evidence, capture);
    if let Some(item) = &value.phase {
        json.key(b"phase");
        write_failure_phase(json, item);
    }
    if let Some(item) = &value.limit {
        json.key(b"limit");
        json.object_start();
        json.key(b"which");
        json.string(&item.which);
        json.key(b"bound");
        json.unsigned(item.bound);
        json.object_end();
    }
    if let Some(item) = value.status {
        json.key(b"status");
        json.unsigned(item);
    }
    if let Some(item) = &value.field {
        json.key(b"field");
        json.string(item);
    }
    if let Some(item) = &value.code {
        json.key(b"code");
        json.string(item);
    }
    if let Some(item) = &value.request_id {
        json.key(b"request_id");
        json.string(item);
    }
    match capture {
        Capture::None | Capture::Unknown(_) => {}
        Capture::Calls | Capture::Everything => {
            if let Some(item) = &value.detail {
                json.key(b"detail");
                json.string(item);
            }
        }
    }
    if let Some(item) = &value.retry_after_ms {
        json.key(b"retry_after_ms");
        json.unsigned(*item);
    }
    json.object_end();
}

fn write_answer_failure(json: &mut Encoder, value: &AnswerFailure, capture: &Capture) {
    json.object_start();
    if let Some(state) = &value.state {
        json.key(b"state");
        json.string(match state {
            AccountState::Limited => b"limited",
            AccountState::Refused => b"refused",
            AccountState::Unauthorized => b"unauthorized",
            AccountState::Unknown(value) => value,
        });
    }
    if let Some(code) = &value.code {
        json.key(b"code");
        json.string(code);
    }
    if let Some(program) = &value.program {
        json.key(b"program");
        json.string(program);
    }
    if let Some(exit) = value.exit {
        json.key(b"exit");
        write_agent_exit(json, exit);
    }
    match capture {
        Capture::None | Capture::Unknown(_) => {}
        Capture::Calls | Capture::Everything => {
            if let Some(detail) = &value.detail {
                json.key(b"detail");
                json.string(detail);
            }
        }
    }
    if let Some(item) = value.reset_ms {
        json.key(b"reset_ms");
        json.unsigned(item);
    }
    if let Some(item) = value.used {
        json.key(b"used");
        json.unsigned(item);
    }
    if let Some(item) = value.bound {
        json.key(b"bound");
        json.unsigned(item);
    }
    json.key(b"class");
    write_answer_class(json, &value.class, capture);
    if let Some(item) = &value.endpoint {
        json.key(b"endpoint");
        json.string(item);
    }
    if let Some(item) = &value.model {
        json.key(b"model");
        json.string(item);
    }
    if let Some(item) = value.attempts {
        json.key(b"attempts");
        json.unsigned(item);
    }
    if let Some(item) = &value.completion {
        json.key(b"completion");
        write_completion_failure(json, item, capture);
    }
    if let Some(item) = &value.account {
        json.key(b"account");
        json.unsigned(u64::from(*item));
    }
    if let Some(item) = &value.which {
        json.key(b"which");
        json.string(item);
    }
    if let Some(item) = &value.reason {
        json.key(b"reason");
        json.string(item);
    }
    if let Some(item) = value.dropped {
        json.key(b"dropped");
        json.boolean(item);
    }
    if let Some(item) = &value.ended {
        json.key(b"ended");
        write_agent_exit(json, *item);
    }
    if let Some(item) = value.turn {
        json.key(b"turn");
        json.unsigned(item);
    }
    json.object_end();
}

fn write_field(json: &mut Encoder, value: &Field, _capture: &Capture) {
    json.object_start();
    json.key(b"name");
    json.string(&value.name);
    json.key(b"text");
    json.string(&value.text);
    json.object_end();
}

fn write_run_result(json: &mut Encoder, value: &RunResult, capture: &Capture) {
    json.object_start();
    json.key(b"form");
    write_form(json, &value.form, capture);
    if let Some(item) = &value.label {
        json.key(b"label");
        json.string(item);
    }
    if *capture == Capture::Everything
        && let Some(item) = &value.text
    {
        json.key(b"text");
        json.string(item);
    }
    if *capture == Capture::Everything
        && let Some(item) = &value.fields
    {
        json.key(b"fields");
        write_list_field(json, item, capture);
    }
    json.object_end();
}

fn write_message(json: &mut Encoder, value: &Message, capture: &Capture) {
    json.object_start();
    json.key(b"role");
    write_role(json, &value.role, capture);
    json.key(b"blocks");
    write_list_block(json, &value.blocks, capture);
    json.object_end();
}

fn write_prompt(json: &mut Encoder, value: &Prompt, capture: &Capture) {
    json.object_start();
    if let Some(item) = &value.system {
        json.key(b"system");
        json.string(item);
    }
    if let Some(item) = &value.tools {
        json.key(b"tools");
        write_list_text(json, item, capture);
    }
    json.key(b"messages");
    write_list_message(json, &value.messages, capture);
    json.object_end();
}

fn write_completion(json: &mut Encoder, value: &Completion, capture: &Capture) {
    json.object_start();
    json.key(b"blocks");
    write_list_block(json, &value.blocks, capture);
    json.object_end();
}

fn write_command_exit(json: &mut Encoder, value: &CommandExit, _capture: &Capture) {
    json.object_start();
    match value {
        CommandExit::Code { code } => {
            json.key(b"code");
            json.signed(*code);
        }
        CommandExit::Signal { signal } => {
            json.key(b"signal");
            json.unsigned(*signal);
        }
    }
    json.object_end();
}

fn write_check_exit(json: &mut Encoder, value: &CheckExit) {
    json.object_start();
    json.key(b"kind");
    match value {
        CheckExit::Code { code } => {
            json.string(b"code");
            json.key(b"value");
            json.signed(*code);
        }
        CheckExit::Signal { signal } => {
            json.string(b"signal");
            json.key(b"value");
            json.unsigned(*signal);
        }
        CheckExit::TimedOut => json.string(b"timed_out"),
        CheckExit::Unstarted => json.string(b"unstarted"),
        CheckExit::Cancelled => json.string(b"cancelled"),
        CheckExit::Unknown(kind) => json.string(kind),
    }
    json.object_end();
}

fn write_block(json: &mut Encoder, value: &Block, _capture: &Capture) {
    json.object_start();
    json.key(b"type");
    match value {
        Block::Unknown { kind } => json.string(kind),
        Block::Text { text } => {
            json.string(b"text");
            json.key(b"text");
            json.string(text);
        }
        Block::Refusal { text } => {
            json.string(b"refusal");
            json.key(b"text");
            json.string(text);
        }
        Block::Call { call, tool, input } => {
            json.string(b"call");
            json.key(b"call");
            json.string(call);
            json.key(b"tool");
            json.string(tool);
            json.key(b"input");
            json.string(input);
        }
        Block::Oversized { call, tool, bytes } => {
            json.string(b"oversized");
            json.key(b"call");
            json.string(call);
            json.key(b"tool");
            json.string(tool);
            json.key(b"bytes");
            json.unsigned(*bytes);
        }
        Block::Cut { call, tool, input } => {
            json.string(b"cut");
            json.key(b"call");
            json.string(call);
            json.key(b"tool");
            json.string(tool);
            json.key(b"input");
            json.string(input);
        }
        Block::Result { call, error, text } => {
            json.string(b"result");
            json.key(b"call");
            json.string(call);
            json.key(b"error");
            json.boolean(*error);
            json.key(b"text");
            json.string(text);
        }
        Block::Opaque { bytes } => {
            json.string(b"opaque");
            json.key(b"bytes");
            json.unsigned(*bytes);
        }
    }
    json.object_end();
}

fn write_list_block(json: &mut Encoder, values: &List<Block>, capture: &Capture) {
    json.array_start();
    for value in values {
        write_block(json, value, capture);
    }
    json.array_end();
}

fn write_list_count(json: &mut Encoder, values: &List<Count>, _capture: &Capture) {
    json.object_start();
    for value in values {
        json.key(&value.record);
        json.unsigned(value.count);
    }
    json.object_end();
}

fn write_list_family(json: &mut Encoder, values: &List<Family>, capture: &Capture) {
    json.array_start();
    for value in values {
        write_family(json, value, capture);
    }
    json.array_end();
}

fn write_list_field(json: &mut Encoder, values: &List<Field>, capture: &Capture) {
    json.array_start();
    for value in values {
        write_field(json, value, capture);
    }
    json.array_end();
}

fn write_list_form(json: &mut Encoder, values: &List<Form>, capture: &Capture) {
    json.array_start();
    for value in values {
        write_form(json, value, capture);
    }
    json.array_end();
}

fn write_list_message(json: &mut Encoder, values: &List<Message>, capture: &Capture) {
    json.array_start();
    for value in values {
        write_message(json, value, capture);
    }
    json.array_end();
}

fn write_list_text(json: &mut Encoder, values: &List<Box<[u8]>>, _capture: &Capture) {
    json.array_start();
    for value in values {
        json.string(value);
    }
    json.array_end();
}

fn check_text(text: &[u8], capacity: u32) -> Result<(), Error> {
    if text.len() > usize::try_from(capacity).expect("u32 fits") {
        return Err(Error::Limits);
    }
    Ok(())
}

fn validate_record(record: &Record, limits: &Limits) -> Result<(), Error> {
    match &record.event {
        Event::Budget(value) => {
            validate_budget_stage(&value.stage, limits)?;
            if let Some(call) = &value.call {
                check_text(call, limits.string)?;
            }
            Ok(())
        }
        Event::SessionStarted(value) => validate_session_started(value, limits),
        Event::SessionEnded(value) => validate_session_ended(value, limits),
        Event::RunStarted(value) => validate_run_started(value, limits),
        Event::RunCompleted(value) => validate_run_completed(value, limits),
        Event::ConversationOpened(value) => validate_conversation_opened(value, limits),
        Event::ConversationClosed(value) => validate_conversation_closed(value, limits),
        Event::ResponseStarted(value) => validate_response_started(value, limits),
        Event::ResponseCompleted(value) => validate_response_completed(value, limits),
        Event::TextDelta(value) => validate_text_delta(value, limits),
        Event::ToolStarted(value) => validate_tool_started(value, limits),
        Event::JobEnded(value) => {
            check_text(&value.call, limits.string)?;
            validate_verdict(&value.verdict, limits)
        }
        Event::ToolCompleted(value) => validate_tool_completed(value, limits),
        Event::CheckCompleted(value) => validate_check_exit(&value.exit, limits),
        Event::CheckStarted(_) | Event::MessageRead(_) | Event::MessageUnread(_) => Ok(()),
        Event::MessageReceived(value) => validate_message_received(value, limits),
        Event::Notice(value) => validate_notice(value, limits),
    }
}

fn validate_check_exit(value: &CheckExit, limits: &Limits) -> Result<(), Error> {
    match value {
        CheckExit::Code { .. }
        | CheckExit::Signal { .. }
        | CheckExit::TimedOut
        | CheckExit::Unstarted
        | CheckExit::Cancelled => Ok(()),
        CheckExit::Unknown(kind) => check_text(kind, limits.string),
    }
}

fn validate_capture(value: &Capture, limits: &Limits) -> Result<(), Error> {
    match value {
        Capture::None | Capture::Calls | Capture::Everything => Ok(()),
        Capture::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_delivery(value: &Delivery, limits: &Limits) -> Result<(), Error> {
    match value {
        Delivery::Complete | Delivery::BestEffort => Ok(()),
        Delivery::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_mode(value: &Mode, limits: &Limits) -> Result<(), Error> {
    match value {
        Mode::Agent | Mode::Exec | Mode::Chat => Ok(()),
        Mode::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_containment(value: &Containment, limits: &Limits) -> Result<(), Error> {
    match value {
        Containment::Groups | Containment::Trees => Ok(()),
        Containment::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_family(value: &Family, limits: &Limits) -> Result<(), Error> {
    match value {
        Family::Inspect | Family::Modify | Family::Shell | Family::SubAgents => Ok(()),
        Family::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_form(value: &Form, limits: &Limits) -> Result<(), Error> {
    match value {
        Form::Report | Form::Verdict | Form::Change | Form::Failure => Ok(()),
        Form::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_status(value: &Status, limits: &Limits) -> Result<(), Error> {
    match value {
        Status::Accepted | Status::Parked | Status::Failed | Status::Refused => Ok(()),
        Status::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_conversation_kind(value: &ConversationKind, limits: &Limits) -> Result<(), Error> {
    match value {
        ConversationKind::Main | ConversationKind::Child | ConversationKind::Compaction => Ok(()),
        ConversationKind::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_conversation_end(value: &ConversationEnd, limits: &Limits) -> Result<(), Error> {
    match value {
        ConversationEnd::Closed
        | ConversationEnd::Budget
        | ConversationEnd::Failed
        | ConversationEnd::Refused
        | ConversationEnd::Transcript
        | ConversationEnd::Overflow => Ok(()),
        ConversationEnd::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_outcome(value: &Outcome, limits: &Limits) -> Result<(), Error> {
    match value {
        Outcome::Completed | Outcome::Failed | Outcome::Cancelled => Ok(()),
        Outcome::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_stop(value: &Stop, limits: &Limits) -> Result<(), Error> {
    match value {
        Stop::End | Stop::Tools | Stop::MaxTokens | Stop::Refusal => Ok(()),
        Stop::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_source(value: &Source, limits: &Limits) -> Result<(), Error> {
    match value {
        Source::Workspace | Source::Run | Source::Host => Ok(()),
        Source::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_effect(value: &Effect, limits: &Limits) -> Result<(), Error> {
    match value {
        Effect::Read | Effect::Write => Ok(()),
        Effect::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_verdict(value: &Verdict, limits: &Limits) -> Result<(), Error> {
    match value {
        Verdict::Removed
        | Verdict::NotEmpty
        | Verdict::Read
        | Verdict::Listed
        | Verdict::Found
        | Verdict::Written
        | Verdict::Edited
        | Verdict::Exited
        | Verdict::Conflict
        | Verdict::Missing
        | Verdict::TooLarge
        | Verdict::Running
        | Verdict::Stopped
        | Verdict::TimedOut
        | Verdict::Failed
        | Verdict::Cancelled
        | Verdict::Ambiguous
        | Verdict::Result
        | Verdict::Error
        | Verdict::Invalid
        | Verdict::NotRun
        | Verdict::Withdrawn
        | Verdict::NotGranted
        | Verdict::Outside
        | Verdict::ReadOnly
        | Verdict::TooLong
        | Verdict::NotFound
        | Verdict::NotFile
        | Verdict::Linked
        | Verdict::Protected
        | Verdict::NotDirectory
        | Verdict::NotRead
        | Verdict::Stale
        | Verdict::NoMatch
        | Verdict::Unchanged
        | Verdict::Busy
        | Verdict::NulByte => Ok(()),
        Verdict::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_delivered(value: &Delivered, limits: &Limits) -> Result<(), Error> {
    match value {
        Delivered::Delivered | Delivered::Nothing | Delivered::Refused | Delivered::Failed | Delivered::Stale => Ok(()),
        Delivered::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_failure_class(value: &FailureClass, limits: &Limits) -> Result<(), Error> {
    match value {
        FailureClass::Overloaded
        | FailureClass::RateLimited
        | FailureClass::Exhausted
        | FailureClass::Refused
        | FailureClass::Unsupported
        | FailureClass::Unavailable
        | FailureClass::TimedOut
        | FailureClass::ContextTooLong
        | FailureClass::Invalid
        | FailureClass::Unauthorized
        | FailureClass::Limit
        | FailureClass::Protocol
        | FailureClass::Cancelled => Ok(()),
        FailureClass::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_evidence(value: &Evidence, limits: &Limits) -> Result<(), Error> {
    match value {
        Evidence::Unsent | Evidence::MaybeSent | Evidence::Response => Ok(()),
        Evidence::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_answer_class(value: &AnswerClass, limits: &Limits) -> Result<(), Error> {
    match value {
        AnswerClass::Model
        | AnswerClass::Budget
        | AnswerClass::Policy
        | AnswerClass::Cancelled
        | AnswerClass::Agent
        | AnswerClass::Host
        | AnswerClass::Stale
        | AnswerClass::Transcript
        | AnswerClass::Busy
        | AnswerClass::Invalid => Ok(()),
        AnswerClass::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_level(value: &Level, limits: &Limits) -> Result<(), Error> {
    match value {
        Level::Info | Level::Warning => Ok(()),
        Level::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_notice_kind(value: &NoticeKind, limits: &Limits) -> Result<(), Error> {
    match value {
        NoticeKind::ReasoningDropped
        | NoticeKind::CredentialUnauthorized
        | NoticeKind::AccountLimited
        | NoticeKind::AccountRefused
        | NoticeKind::TeardownAborted
        | NoticeKind::ForcedExit => Ok(()),
        NoticeKind::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_role(value: &Role, limits: &Limits) -> Result<(), Error> {
    match value {
        Role::User | Role::Assistant => Ok(()),
        Role::Unknown(text) => check_text(text, limits.string),
    }
}

fn validate_agent(value: &Agent, limits: &Limits) -> Result<(), Error> {
    check_text(&value.name, limits.string)?;
    check_text(&value.version, limits.string)?;
    Ok(())
}

fn validate_count(value: &Count, limits: &Limits) -> Result<(), Error> {
    check_text(&value.record, limits.string)?;

    Ok(())
}

fn validate_model(value: &Model, limits: &Limits) -> Result<(), Error> {
    check_text(&value.endpoint, limits.string)?;
    check_text(&value.model, limits.string)?;
    match &value.backend {
        Backend::Direct | Backend::ClaudeCode => {}
        Backend::Unknown(name) => check_text(name, limits.string)?,
    }

    if let Some(item) = &value.effort {
        check_text(item, limits.string)?;
    }
    Ok(())
}

fn validate_tools(value: &Tools, limits: &Limits) -> Result<(), Error> {
    validate_list_family(&value.families, limits)?;
    validate_list_family(&value.withheld, limits)?;

    validate_list_text(&value.host, limits)?;
    Ok(())
}

fn validate_completion_failure(value: &CompletionFailure, limits: &Limits) -> Result<(), Error> {
    validate_failure_class(&value.class, limits)?;
    for item in [&value.field, &value.code, &value.request_id].into_iter().flatten() {
        check_text(item, limits.string)?;
    }
    validate_evidence(&value.evidence, limits)?;
    if let Some(phase) = &value.phase {
        match phase {
            FailurePhase::Connect
            | FailurePhase::Handshake
            | FailurePhase::Head
            | FailurePhase::Idle
            | FailurePhase::Whole => {}
            FailurePhase::Unknown(text) => check_text(text, limits.string)?,
        }
    }
    if let Some(limit) = &value.limit {
        check_text(&limit.which, limits.string)?;
    }
    if let Some(detail) = &value.detail {
        check_text(detail, limits.content)?;
    }
    Ok(())
}

fn validate_answer_failure(value: &AnswerFailure, limits: &Limits) -> Result<(), Error> {
    validate_answer_class(&value.class, limits)?;
    if let Some(endpoint) = &value.endpoint {
        check_text(endpoint, limits.string)?;
    }
    if let Some(model) = &value.model {
        check_text(model, limits.string)?;
    }
    if let Some(item) = &value.completion {
        validate_completion_failure(item, limits)?;
    }
    if let Some(item) = &value.which {
        check_text(item, limits.string)?;
    }
    if let Some(item) = &value.reason {
        check_text(item, limits.string)?;
    }
    match &value.state {
        Some(AccountState::Unknown(value)) => check_text(value, limits.string)?,
        Some(AccountState::Limited | AccountState::Refused | AccountState::Unauthorized) | None => {}
    }
    if let Some(code) = &value.code {
        check_text(code, limits.string)?;
    }
    if let Some(program) = &value.program {
        check_text(program, limits.string)?;
    }
    if let Some(detail) = &value.detail {
        check_text(detail, limits.content)?;
    }
    Ok(())
}

fn validate_field(value: &Field, limits: &Limits) -> Result<(), Error> {
    check_text(&value.name, limits.string)?;
    check_text(&value.text, limits.content)?;
    Ok(())
}

fn validate_run_result(value: &RunResult, limits: &Limits) -> Result<(), Error> {
    validate_form(&value.form, limits)?;
    if let Some(item) = &value.label {
        check_text(item, limits.string)?;
    }
    if let Some(item) = &value.text {
        check_text(item, limits.content)?;
    }
    if let Some(item) = &value.fields {
        validate_list_field(item, limits)?;
    }
    Ok(())
}

fn validate_message(value: &Message, limits: &Limits) -> Result<(), Error> {
    validate_role(&value.role, limits)?;
    validate_list_block(&value.blocks, limits)?;
    Ok(())
}

fn validate_prompt(value: &Prompt, limits: &Limits) -> Result<(), Error> {
    if let Some(item) = &value.system {
        check_text(item, limits.content)?;
    }
    if let Some(item) = &value.tools {
        validate_list_text(item, limits)?;
    }
    validate_list_message(&value.messages, limits)?;
    Ok(())
}

fn validate_completion(value: &Completion, limits: &Limits) -> Result<(), Error> {
    validate_list_block(&value.blocks, limits)?;
    Ok(())
}

fn validate_session_started(value: &SessionStarted, limits: &Limits) -> Result<(), Error> {
    validate_agent(&value.agent, limits)?;
    validate_mode(&value.mode, limits)?;
    match &value.mode {
        Mode::Agent if value.chat.is_some() => return Err(Error::Shape),
        Mode::Agent | Mode::Exec | Mode::Chat | Mode::Unknown(_) => {}
    }
    if let Some(chat) = &value.chat {
        check_text(chat, limits.string)?;
    }
    validate_containment(&value.containment, limits)?;

    check_text(&value.profile, limits.string)?;
    validate_capture(&value.capture, limits)?;
    validate_delivery(&value.delivery, limits)?;
    validate_effective_limits(&value.limits, limits)?;
    Ok(())
}

fn validate_effective_limits(values: &List<EffectiveLimit>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for (index, value) in values.as_slice().iter().enumerate() {
        if value.name.is_empty() {
            return Err(Error::Shape);
        }
        check_text(&value.name, limits.string)?;
        for previous in values.as_slice().get(..index).ok_or(Error::Shape)? {
            if previous.name == value.name {
                return Err(Error::Shape);
            }
        }
        match value.value {
            LimitValue::Integer(_) => {}
            LimitValue::Fraction { denominator, .. } => {
                if denominator == 0 {
                    return Err(Error::Shape);
                }
            }
        }
    }
    Ok(())
}

fn validate_session_ended(value: &SessionEnded, limits: &Limits) -> Result<(), Error> {
    value.aborted.ok_or(Error::Shape)?;
    validate_list_count(&value.emitted, limits)?;
    Ok(())
}

fn validate_run_started(value: &RunStarted, limits: &Limits) -> Result<(), Error> {
    validate_model(&value.main, limits)?;
    validate_tools(&value.tools, limits)?;
    validate_list_form(&value.contract, limits)?;
    Ok(())
}

fn validate_run_completed(value: &RunCompleted, limits: &Limits) -> Result<(), Error> {
    validate_status(&value.status, limits)?;
    if let Some(item) = &value.failure {
        validate_answer_failure(item, limits)?;
    }
    if let Some(item) = &value.result {
        validate_run_result(item, limits)?;
    }

    Ok(())
}

fn validate_conversation_opened(value: &ConversationOpened, limits: &Limits) -> Result<(), Error> {
    validate_conversation_kind(&value.kind, limits)?;
    if let Some(item) = &value.call {
        check_text(item, limits.string)?;
    }
    validate_model(&value.model, limits)?;
    if value.clamped.len() > limits.items {
        return Err(Error::Limits);
    }
    for clamp in &value.clamped {
        validate_share_part(&clamp.part, limits)?;
        validate_share_bound(&clamp.by, limits)?;
    }
    Ok(())
}

fn validate_conversation_closed(value: &ConversationClosed, limits: &Limits) -> Result<(), Error> {
    validate_conversation_end(&value.end, limits)?;
    if let Some(item) = &value.which {
        check_text(item, limits.string)?;
    }
    if let Some(item) = &value.failure {
        validate_completion_failure(item, limits)?;
    }

    Ok(())
}

fn validate_response_started(value: &ResponseStarted, limits: &Limits) -> Result<(), Error> {
    validate_model(&value.model, limits)?;
    match &value.tool_choice {
        ToolChoice::Auto | ToolChoice::None | ToolChoice::Only => {}
        ToolChoice::Unknown(text) => check_text(text, limits.string)?,
    }

    if let Some(item) = &value.prompt {
        validate_prompt(item, limits)?;
    }
    Ok(())
}

fn validate_response_completed(value: &ResponseCompleted, limits: &Limits) -> Result<(), Error> {
    validate_outcome(&value.outcome, limits)?;
    if let Some(item) = &value.stop {
        validate_stop(item, limits)?;
    }

    if let Some(item) = &value.failure {
        validate_completion_failure(item, limits)?;
    }
    if let Some(item) = &value.completion {
        validate_completion(item, limits)?;
    }
    Ok(())
}

fn validate_text_delta(value: &TextDelta, limits: &Limits) -> Result<(), Error> {
    check_text(&value.text, limits.content)?;
    Ok(())
}

fn validate_tool_started(value: &ToolStarted, limits: &Limits) -> Result<(), Error> {
    check_text(&value.call, limits.string)?;
    check_text(&value.tool, limits.string)?;
    validate_source(&value.source, limits)?;
    validate_effect(&value.effect, limits)?;

    if let Some(item) = &value.input {
        check_text(item, limits.content)?;
    }
    Ok(())
}

fn validate_tool_completed(value: &ToolCompleted, limits: &Limits) -> Result<(), Error> {
    check_text(&value.call, limits.string)?;
    check_text(&value.tool, limits.string)?;
    validate_verdict(&value.verdict, limits)?;

    if let Some(item) = &value.child {
        match &item.stop {
            ChildStop::End | ChildStop::Budget | ChildStop::Failed | ChildStop::Closed => {}
            ChildStop::Unknown(text) => check_text(text, limits.string)?,
        }
    }
    if let Some(item) = &value.delivery {
        validate_delivered(item, limits)?;
    }
    Ok(())
}

fn validate_message_received(value: &MessageReceived, limits: &Limits) -> Result<(), Error> {
    if let Some(refusal) = &value.refused {
        match refusal {
            MessageRefusal::TooLarge
            | MessageRefusal::InboxFull
            | MessageRefusal::NameInUse
            | MessageRefusal::Ending
            | MessageRefusal::StartRefused => {}
            MessageRefusal::Unknown(text) => check_text(text, limits.string)?,
        }
    }
    if let Some(label) = &value.label {
        check_text(label, limits.string)?;
    }
    if let Some(text) = &value.text {
        check_text(text, limits.content)?;
    }
    Ok(())
}

fn validate_notice(value: &Notice, limits: &Limits) -> Result<(), Error> {
    if let Some(code) = &value.code {
        check_text(code, limits.string)?;
    }
    validate_level(&value.level, limits)?;
    validate_notice_kind(&value.kind, limits)?;
    match &value.forced {
        Some(Forced::Unknown(value)) => check_text(value, limits.string)?,
        Some(Forced::No | Forced::Terminated | Forced::Killed) | None => {}
    }
    match value.kind {
        NoticeKind::ForcedExit if value.forced.is_none() => Err(Error::Shape),
        NoticeKind::AccountRefused if value.code.is_none() => Err(Error::Shape),
        NoticeKind::ReasoningDropped if value.bytes.is_none() => Err(Error::Shape),
        NoticeKind::CredentialUnauthorized | NoticeKind::AccountLimited if value.account.is_none() => Err(Error::Shape),
        NoticeKind::ReasoningDropped
        | NoticeKind::CredentialUnauthorized
        | NoticeKind::AccountLimited
        | NoticeKind::AccountRefused
        | NoticeKind::TeardownAborted
        | NoticeKind::ForcedExit
        | NoticeKind::Unknown(_) => Ok(()),
    }
}

fn validate_block(value: &Block, limits: &Limits) -> Result<(), Error> {
    match value {
        Block::Unknown { kind } => check_text(kind, limits.string),
        Block::Text { text } | Block::Refusal { text } => {
            check_text(text, limits.content)?;
            Ok(())
        }
        Block::Oversized { call, tool, bytes: _ } => {
            check_text(call, limits.string)?;
            check_text(tool, limits.string)?;

            Ok(())
        }
        Block::Call { call, tool, input } | Block::Cut { call, tool, input } => {
            check_text(call, limits.string)?;
            check_text(tool, limits.string)?;
            check_text(input, limits.content)?;
            Ok(())
        }
        Block::Result { call, error: _, text } => {
            check_text(call, limits.string)?;

            check_text(text, limits.content)?;
            Ok(())
        }
        Block::Opaque { bytes: _ } => Ok(()),
    }
}

fn validate_list_block(values: &List<Block>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_block(value, limits)?;
    }
    Ok(())
}

fn validate_list_count(values: &List<Count>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_count(value, limits)?;
    }
    for first in 0..values.len() {
        let first_value = values.get(first).expect("bounded counter list");
        for second in first.saturating_add(1)..values.len() {
            let second_value = values.get(second).expect("bounded counter list");
            if first_value.record == second_value.record {
                return Err(Error::Shape);
            }
        }
    }
    Ok(())
}

fn validate_list_family(values: &List<Family>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_family(value, limits)?;
    }
    Ok(())
}

fn validate_list_field(values: &List<Field>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_field(value, limits)?;
    }
    Ok(())
}

fn validate_list_form(values: &List<Form>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_form(value, limits)?;
    }
    Ok(())
}

fn validate_list_message(values: &List<Message>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        validate_message(value, limits)?;
    }
    Ok(())
}

fn validate_list_text(values: &List<Box<[u8]>>, limits: &Limits) -> Result<(), Error> {
    if values.len() > limits.items {
        return Err(Error::Limits);
    }
    for value in values {
        check_text(value, limits.string)?;
    }
    Ok(())
}

fn encode_session_started(json: &mut Encoder, value: &SessionStarted, capture: &Capture) {
    json.key(b"wall_ms");
    json.unsigned(value.wall_ms);
    json.key(b"agent");
    write_agent(json, &value.agent, capture);
    json.key(b"mode");
    write_mode(json, &value.mode, capture);
    if let Some(chat) = &value.chat {
        json.key(b"chat");
        json.string(chat);
    }
    json.key(b"containment");
    write_containment(json, &value.containment, capture);
    json.key(b"pid");
    json.unsigned(value.pid);
    json.key(b"profile");
    json.string(&value.profile);
    json.key(b"capture");
    write_capture(json, &value.capture, capture);
    json.key(b"delivery");
    write_delivery(json, &value.delivery, capture);
    json.key(b"versions");
    write_versions(json, &value.versions, capture);
    json.key(b"limits");
    json.object_start();
    for limit in &value.limits {
        json.key(&limit.name);
        match limit.value {
            LimitValue::Integer(value) => json.unsigned(value),
            LimitValue::Fraction { numerator, denominator } => {
                json.object_start();
                json.key(b"numerator");
                json.unsigned(numerator);
                json.key(b"denominator");
                json.unsigned(denominator);
                json.object_end();
            }
        }
    }
    json.object_end();
}

fn encode_session_ended(json: &mut Encoder, value: &SessionEnded, capture: &Capture) {
    json.key(b"exit");
    json.signed(value.exit);
    json.key(b"answered");
    json.boolean(value.answered);
    json.key(b"aborted");
    json.boolean(value.aborted.expect("current terminal has its aborted field"));
    json.key(b"teardown_ms");
    match &value.teardown_ms {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"emitted");
    write_list_count(json, &value.emitted, capture);
    json.key(b"loss");
    write_loss(json, &value.loss, capture);
    json.key(b"cpu_ms");
    match &value.cpu_ms {
        Some(cpu) => write_cpu(json, cpu, capture),
        None => json.null(),
    }
    json.key(b"peak_rss_bytes");
    match &value.peak_rss_bytes {
        Some(peak_rss) => write_peak_rss(json, peak_rss, capture),
        None => json.null(),
    }
}

fn encode_run_started(json: &mut Encoder, value: &RunStarted, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"resumed");
    json.boolean(value.resumed);
    json.key(b"messages");
    json.unsigned(value.messages);
    json.key(b"main");
    write_model(json, &value.main, capture);
    json.key(b"budget");
    write_budget(json, &value.budget, capture);
    json.key(b"tools");
    write_tools(json, &value.tools, capture);
    json.key(b"contract");
    write_list_form(json, &value.contract, capture);
}

fn encode_run_completed(json: &mut Encoder, value: &RunCompleted, capture: &Capture) {
    json.key(b"wind_down");
    json.boolean(value.wind_down);
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"status");
    write_status(json, &value.status, capture);
    if let Some(item) = &value.failure {
        json.key(b"failure");
        write_answer_failure(json, item, capture);
    }
    if let Some(item) = &value.result {
        json.key(b"result");
        write_run_result(json, item, capture);
    }
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"spent");
    json.unsigned(value.spent);
    json.key(b"usage");
    write_usage(json, &value.usage, capture);
    json.key(b"read");
    match value.read {
        Some(name) => json.unsigned(name),
        None => json.null(),
    }
    json.key(b"unread");
    json.unsigned(value.unread);
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
}

fn encode_conversation_opened(json: &mut Encoder, value: &ConversationOpened, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"kind");
    write_conversation_kind(json, &value.kind, capture);
    if let Some(item) = &value.parent {
        json.key(b"parent");
        json.unsigned(*item);
    }
    if let Some(item) = &value.call {
        json.key(b"call");
        json.string(item);
    }
    json.key(b"thread");
    json.unsigned(value.thread);
    json.key(b"model");
    write_model(json, &value.model, capture);
    if let Some(requested) = &value.requested {
        json.key(b"requested");
        write_requested(json, requested);
    }
    if let Some(share) = &value.share {
        json.key(b"share");
        write_share(json, share);
    }
    if !value.clamped.is_empty() {
        json.key(b"clamped");
        json.array_start();
        for clamp in &value.clamped {
            json.object_start();
            json.key(b"part");
            write_share_part(json, &clamp.part);
            json.key(b"by");
            write_share_bound(json, &clamp.by);
            json.object_end();
        }
        json.array_end();
    }
}

fn encode_conversation_closed(json: &mut Encoder, value: &ConversationClosed, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"end");
    write_conversation_end(json, &value.end, capture);
    if let Some(item) = &value.which {
        json.key(b"which");
        json.string(item);
    }
    if let Some(item) = &value.failure {
        json.key(b"failure");
        write_completion_failure(json, item, capture);
    }
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"usage");
    write_usage(json, &value.usage, capture);
    json.key(b"spent");
    json.unsigned(value.spent);
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
}

fn encode_response_started(json: &mut Encoder, value: &ResponseStarted, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"response");
    json.unsigned(value.response);
    json.key(b"turn");
    json.unsigned(value.turn);
    json.key(b"attempt");
    json.unsigned(value.attempt);
    json.key(b"window");
    json.unsigned(value.window);
    json.key(b"model");
    write_model(json, &value.model, capture);
    json.key(b"messages");
    json.unsigned(value.messages);
    json.key(b"output_tokens");
    json.unsigned(value.output_tokens);
    json.key(b"tool_choice");
    write_tool_choice(json, &value.tool_choice);
    if *capture == Capture::Everything
        && let Some(item) = &value.prompt
    {
        json.key(b"prompt");
        write_prompt(json, item, capture);
    }
}

fn encode_response_completed(json: &mut Encoder, value: &ResponseCompleted, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"response");
    json.unsigned(value.response);
    json.key(b"outcome");
    write_outcome(json, &value.outcome, capture);
    if let Some(item) = &value.stop {
        json.key(b"stop");
        write_stop(json, item, capture);
    }
    json.key(b"blocks");
    json.unsigned(value.blocks);
    json.key(b"calls");
    json.unsigned(value.calls);
    json.key(b"invalid");
    json.unsigned(value.invalid);
    json.key(b"oversized");
    json.unsigned(value.oversized);
    json.key(b"cut");
    json.unsigned(value.cut);
    json.key(b"usage");
    write_usage(json, &value.usage, capture);
    json.key(b"spent");
    json.unsigned(value.spent);
    json.key(b"request_bytes");
    match &value.request_bytes {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"first_byte_ms");
    match &value.first_byte_ms {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"largest_gap_ms");
    match &value.largest_gap_ms {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
    if let Some(item) = &value.failure {
        json.key(b"failure");
        write_completion_failure(json, item, capture);
    }
    if let Some(item) = &value.retry_ms {
        json.key(b"retry_ms");
        json.unsigned(*item);
    }
    if *capture == Capture::Everything
        && let Some(item) = &value.completion
    {
        json.key(b"completion");
        write_completion(json, item, capture);
    }
}

fn encode_text_delta(json: &mut Encoder, value: &TextDelta, _capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"response");
    json.unsigned(value.response);
    json.key(b"block");
    json.unsigned(value.block);
    json.key(b"text");
    json.string(&value.text);
}

fn encode_tool_started(json: &mut Encoder, value: &ToolStarted, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"call");
    json.string(&value.call);
    json.key(b"tool");
    json.string(&value.tool);
    json.key(b"source");
    write_source(json, &value.source, capture);
    json.key(b"effect");
    write_effect(json, &value.effect, capture);
    json.key(b"deadline_ms");
    match &value.deadline_ms {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
    json.key(b"input_bytes");
    json.unsigned(value.input_bytes);
    if *capture != Capture::None
        && let Some(item) = &value.input
    {
        json.key(b"input");
        json.string(item);
    }
}

fn write_child_completed(json: &mut Encoder, value: &ChildCompleted, capture: &Capture) {
    json.object_start();
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"stop");
    match &value.stop {
        ChildStop::End => json.string(b"end"),
        ChildStop::Budget => json.string(b"budget"),
        ChildStop::Failed => json.string(b"failed"),
        ChildStop::Closed => json.string(b"closed"),
        ChildStop::Unknown(text) => json.string(text),
    }
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"time_ms");
    json.unsigned(value.time_ms);
    json.key(b"spent");
    json.unsigned(value.spent);
    json.key(b"usage");
    write_usage(json, &value.usage, capture);
    json.object_end();
}

fn encode_tool_completed(json: &mut Encoder, value: &ToolCompleted, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"call");
    json.string(&value.call);
    json.key(b"tool");
    json.string(&value.tool);
    json.key(b"verdict");
    write_verdict(json, &value.verdict, capture);
    if let Some(item) = &value.exit {
        json.key(b"exit");
        write_command_exit(json, item, capture);
    }
    if let Some(job) = value.job {
        json.key(b"job");
        json.unsigned(job);
    }
    json.key(b"bytes");
    json.unsigned(value.bytes);
    json.key(b"omitted_bytes");
    json.unsigned(value.omitted_bytes);
    json.key(b"clamped");
    json.boolean(value.clamped);
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
    if let Some(item) = &value.child {
        json.key(b"child");
        write_child_completed(json, item, capture);
    }
    if let Some(item) = &value.delivery {
        json.key(b"delivery");
        write_delivered(json, item, capture);
    }
}

fn encode_job_ended(json: &mut Encoder, value: &JobEnded, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"conversation");
    json.unsigned(value.conversation);
    json.key(b"job");
    json.unsigned(value.job);
    json.key(b"call");
    json.string(&value.call);
    json.key(b"verdict");
    write_verdict(json, &value.verdict, capture);
    if let Some(exit) = &value.exit {
        json.key(b"exit");
        write_command_exit(json, exit, capture);
    }
    json.key(b"unread_bytes");
    json.unsigned(value.unread_bytes);
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
}

fn encode_check_started(json: &mut Encoder, value: &CheckStarted, _capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"deadline_ms");
    match &value.deadline_ms {
        Some(item) => {
            json.unsigned(*item);
        }
        None => json.null(),
    }
}

fn encode_check_completed(json: &mut Encoder, value: &CheckCompleted, _capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"exit");
    write_check_exit(json, &value.exit);
    json.key(b"passed");
    json.boolean(value.passed);
    json.key(b"duration_ms");
    json.unsigned(value.duration_ms);
}

fn encode_message_received(json: &mut Encoder, value: &MessageReceived, capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"name");
    json.unsigned(value.name);
    json.key(b"bytes");
    json.unsigned(value.bytes);
    if let Some(refusal) = &value.refused {
        json.key(b"refused");
        match refusal {
            MessageRefusal::TooLarge => json.string(b"too_large"),
            MessageRefusal::InboxFull => json.string(b"inbox_full"),
            MessageRefusal::NameInUse => json.string(b"name_in_use"),
            MessageRefusal::Ending => json.string(b"ending"),
            MessageRefusal::StartRefused => json.string(b"start_refused"),
            MessageRefusal::Unknown(text) => json.string(text),
        }
    }
    if *capture == Capture::Everything {
        if let Some(label) = &value.label {
            json.key(b"label");
            json.string(label);
        }
        if let Some(text) = &value.text {
            json.key(b"text");
            json.string(text);
        }
    }
}

fn encode_message_read(json: &mut Encoder, value: &MessageRead) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"name");
    json.unsigned(value.name);
    json.key(b"turn");
    json.unsigned(value.turn);
}

fn encode_message_unread(json: &mut Encoder, value: &MessageUnread) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"name");
    json.unsigned(value.name);
}

fn write_agent_exit(json: &mut Encoder, exit: AgentExit) {
    json.object_start();
    match exit {
        AgentExit::Code(code) => {
            json.key(b"code");
            json.signed(code);
        }
        AgentExit::Signal(signal) => {
            json.key(b"signal");
            json.unsigned(signal);
        }
    }
    json.object_end();
}

fn write_forced(json: &mut Encoder, forced: &Forced) {
    match forced {
        Forced::No => json.string(b"no"),
        Forced::Terminated => json.string(b"terminated"),
        Forced::Killed => json.string(b"killed"),
        Forced::Unknown(value) => json.string(value),
    }
}

fn encode_notice(json: &mut Encoder, value: &Notice, capture: &Capture) {
    json.key(b"level");
    write_level(json, &value.level, capture);
    json.key(b"kind");
    write_notice_kind(json, &value.kind, capture);
    match value.exit {
        Some(exit) => {
            json.key(b"exit");
            write_agent_exit(json, exit);
        }
        None => match value.kind {
            NoticeKind::ForcedExit => {
                json.key(b"exit");
                json.null();
            }
            NoticeKind::ReasoningDropped
            | NoticeKind::CredentialUnauthorized
            | NoticeKind::AccountLimited
            | NoticeKind::AccountRefused
            | NoticeKind::TeardownAborted
            | NoticeKind::Unknown(_) => {}
        },
    }
    if let Some(forced) = &value.forced {
        json.key(b"forced");
        write_forced(json, forced);
    }
    if let Some(item) = &value.run {
        json.key(b"run");
        json.unsigned(*item);
    }
    if let Some(account) = value.account {
        json.key(b"account");
        json.unsigned(u64::from(account));
    }
    if let Some(bytes) = value.bytes {
        json.key(b"bytes");
        json.unsigned(bytes);
    }
    if let Some(item) = &value.reset_ms {
        json.key(b"reset_ms");
        json.unsigned(*item);
    }
    if let Some(code) = &value.code {
        json.key(b"code");
        json.string(code);
    }
}

fn write_budget_stage(json: &mut Encoder, value: &BudgetStage) {
    match value {
        BudgetStage::Ordinary => json.string(b"ordinary"),
        BudgetStage::WindDown => json.string(b"wind_down"),
        BudgetStage::Unknown(value) => json.string(value),
    }
}

fn validate_budget_stage(value: &BudgetStage, limits: &Limits) -> Result<(), Error> {
    match value {
        BudgetStage::Ordinary | BudgetStage::WindDown => Ok(()),
        BudgetStage::Unknown(value) => check_text(value, limits.string),
    }
}

fn write_share_part(json: &mut Encoder, value: &SharePart) {
    match value {
        SharePart::Turns => json.string(b"turns"),
        SharePart::Time => json.string(b"time"),
        SharePart::Unknown(value) => json.string(value),
    }
}

fn validate_share_part(value: &SharePart, limits: &Limits) -> Result<(), Error> {
    match value {
        SharePart::Turns | SharePart::Time => Ok(()),
        SharePart::Unknown(value) => check_text(value, limits.string),
    }
}

fn write_share_bound(json: &mut Encoder, value: &ShareBound) {
    match value {
        ShareBound::Reserve => json.string(b"reserve"),
        ShareBound::Deadline => json.string(b"deadline"),
        ShareBound::Unknown(value) => json.string(value),
    }
}

fn validate_share_bound(value: &ShareBound, limits: &Limits) -> Result<(), Error> {
    match value {
        ShareBound::Reserve | ShareBound::Deadline => Ok(()),
        ShareBound::Unknown(value) => check_text(value, limits.string),
    }
}

fn write_requested(json: &mut Encoder, value: &Requested) {
    json.object_start();
    if let Some(turns) = value.turns {
        json.key(b"turns");
        json.unsigned(turns);
    }
    if let Some(seconds) = value.seconds {
        json.key(b"seconds");
        json.unsigned(seconds);
    }
    json.object_end();
}

fn write_share(json: &mut Encoder, value: &Share) {
    json.object_start();
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"time_ms");
    json.unsigned(value.time_ms);
    json.key(b"spend");
    json.unsigned(value.spend);
    json.object_end();
}

fn write_quantity(json: &mut Encoder, value: &Quantity) {
    json.object_start();
    json.key(b"used");
    json.unsigned(value.used);
    json.key(b"limit");
    json.unsigned(value.limit);
    json.object_end();
}

fn write_reserve(json: &mut Encoder, value: &Reserve) {
    json.object_start();
    json.key(b"turns");
    json.unsigned(value.turns);
    json.key(b"time_ms");
    json.unsigned(value.time_ms);
    json.key(b"spend");
    json.unsigned(value.spend);
    json.object_end();
}

fn encode_budget_standing(json: &mut Encoder, value: &BudgetStanding, _capture: &Capture) {
    json.key(b"run");
    json.unsigned(value.run);
    json.key(b"stage");
    write_budget_stage(json, &value.stage);
    json.key(b"turns");
    write_quantity(json, &value.turns);
    json.key(b"spend");
    write_quantity(json, &value.spend);
    json.key(b"time_ms");
    write_quantity(json, &value.time_ms);
    json.key(b"reserve");
    write_reserve(json, &value.reserve);
    if let Some(call) = &value.call {
        json.key(b"call");
        json.string(call);
    }
}
