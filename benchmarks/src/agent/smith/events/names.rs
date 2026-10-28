//! Exact enum spellings keep task predicates in the event vocabulary.

use smith_events::{AnswerClass, ConversationKind, FailureClass, Status, Stop, Verdict};

pub(super) fn status(value: &Status) -> &[u8] {
    match value {
        Status::Accepted => b"accepted",
        Status::Parked => b"parked",
        Status::Failed => b"failed",
        Status::Refused => b"refused",
        Status::Unknown(value) => value,
    }
}

pub(super) fn answer(value: &AnswerClass) -> &[u8] {
    match value {
        AnswerClass::Model => b"model",
        AnswerClass::Budget => b"budget",
        AnswerClass::Policy => b"policy",
        AnswerClass::Cancelled => b"cancelled",
        AnswerClass::Agent => b"agent",
        AnswerClass::Host => b"host",
        AnswerClass::Stale => b"stale",
        AnswerClass::Transcript => b"transcript",
        AnswerClass::Busy => b"busy",
        AnswerClass::Invalid => b"invalid",
        AnswerClass::Unknown(value) => value,
    }
}

pub(super) fn kind(value: &ConversationKind) -> &[u8] {
    match value {
        ConversationKind::Main => b"main",
        ConversationKind::Child => b"child",
        ConversationKind::Compaction => b"compaction",
        ConversationKind::Unknown(value) => value,
    }
}

pub(super) fn stop(value: &Stop) -> &[u8] {
    match value {
        Stop::End => b"end",
        Stop::Tools => b"tools",
        Stop::MaxTokens => b"max_tokens",
        Stop::Refusal => b"refusal",
        Stop::Unknown(value) => value,
    }
}

pub(super) fn failure(value: &FailureClass) -> &[u8] {
    match value {
        FailureClass::Overloaded => b"overloaded",
        FailureClass::RateLimited => b"rate_limited",
        FailureClass::Exhausted => b"exhausted",
        FailureClass::Refused => b"refused",
        FailureClass::Unsupported => b"unsupported",
        FailureClass::Unavailable => b"unavailable",
        FailureClass::TimedOut => b"timed_out",
        FailureClass::ContextTooLong => b"context_too_long",
        FailureClass::Invalid => b"invalid",
        FailureClass::Unauthorized => b"unauthorized",
        FailureClass::Limit => b"limit",
        FailureClass::Protocol => b"protocol",
        FailureClass::Cancelled => b"cancelled",
        FailureClass::Unknown(value) => value,
    }
}

pub(super) fn verdict(value: &Verdict) -> &[u8] {
    match value {
        Verdict::Read => b"read",
        Verdict::Listed => b"listed",
        Verdict::Found => b"found",
        Verdict::Removed => b"removed",
        Verdict::NotEmpty => b"not_empty",
        Verdict::Written => b"written",
        Verdict::Edited => b"edited",
        Verdict::Exited => b"exited",
        Verdict::Conflict => b"conflict",
        Verdict::Missing => b"missing",
        Verdict::TooLarge => b"too_large",
        Verdict::TimedOut => b"timed_out",
        Verdict::Running => b"running",
        Verdict::Stopped => b"stopped",
        Verdict::Failed => b"failed",
        Verdict::Cancelled => b"cancelled",
        Verdict::Ambiguous => b"ambiguous",
        Verdict::Result => b"result",
        Verdict::Error => b"error",
        Verdict::Invalid => b"invalid",
        Verdict::NotRun => b"not_run",
        Verdict::Withdrawn => b"withdrawn",
        Verdict::NotGranted => b"not_granted",
        Verdict::Outside => b"outside",
        Verdict::ReadOnly => b"read_only",
        Verdict::TooLong => b"too_long",
        Verdict::NotFound => b"not_found",
        Verdict::NotFile => b"not_file",
        Verdict::Linked => b"linked",
        Verdict::Protected => b"protected",
        Verdict::NotDirectory => b"not_directory",
        Verdict::NotRead => b"not_read",
        Verdict::Stale => b"stale",
        Verdict::NoMatch => b"no_match",
        Verdict::Unchanged => b"unchanged",
        Verdict::Busy => b"busy",
        Verdict::NulByte => b"nul_byte",
        Verdict::Unknown(value) => value,
    }
}

/// The codec's record-type spelling, used by emitted cardinalities.
pub(super) fn event(value: &smith_events::Event) -> &'static [u8] {
    use smith_events::Event;
    match value {
        Event::SessionStarted(_) => b"session.started",
        Event::SessionEnded(_) => b"session.ended",
        Event::RunStarted(_) => b"run.started",
        Event::RunCompleted(_) => b"run.completed",
        Event::ConversationOpened(_) => b"conversation.opened",
        Event::ConversationClosed(_) => b"conversation.closed",
        Event::ResponseStarted(_) => b"response.started",
        Event::ResponseCompleted(_) => b"response.completed",
        Event::TextDelta(_) => b"text.delta",
        Event::ToolStarted(_) => b"tool.started",
        Event::JobEnded(_) => b"job.ended",
        Event::ToolCompleted(_) => b"tool.completed",
        Event::CheckStarted(_) => b"check.started",
        Event::CheckCompleted(_) => b"check.completed",
        Event::MessageReceived(_) => b"message.received",
        Event::MessageRead(_) => b"message.read",
        Event::MessageUnread(_) => b"message.unread",
        Event::Budget(_) => b"budget",
        Event::Notice(_) => b"notice",
    }
}
