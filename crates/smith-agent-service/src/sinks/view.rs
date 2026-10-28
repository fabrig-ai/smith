//! The sink's typed live view (protocol/events.md, section 5.2).
//! Only displayed fields are copied; prompts, completions and tool inputs never
//! enter its queue. Destinations continue to receive the original record.
use alloc::boxed::Box;
use smith_events::{
    AnswerFailure, CompletionFailure, Event, Model, Notice, Quantity, Record, TextDelta, Usage, Verdict,
};

/// Displayed event fields sent by the one sink to its best-effort front queue.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum View {
    /// A new activation clears the screen's model and budget joins.
    RunStarted,
    /// A conversation or response supplies the model for its next status line.
    Model { conversation: u64, model: Model },
    /// A conversation terminal retires its model join.
    ConversationClosed { conversation: u64 },
    /// A provider terminal supplies measured usage, spend, duration and failure.
    ResponseCompleted {
        conversation: u64,
        usage: Usage,
        spent: u64,
        duration_ms: u64,
        failure: Option<Box<CompletionFailure>>,
    },
    /// A streamed text piece is moved to the terminal by the front end.
    TextDelta { t_ms: u64, delta: TextDelta },
    /// A tool's name is shown when it starts, without its input.
    ToolStarted { tool: Box<[u8]> },
    /// A tool's terminal supplies its verdict and elapsed duration.
    ToolCompleted { tool: Box<[u8]>, verdict: Verdict, duration_ms: u64 },
    /// An operator notice carries the same classification as its destination record.
    Notice(Notice),
    /// A failed answer carries its typed failure without an accepted result.
    Failure(Box<AnswerFailure>),
    /// The latest budget quantities are joined to the next response status.
    Budget { turns: Quantity, spend: Quantity, time_ms: Quantity },
}

impl View {
    /// Project only the fields the live front displays, without copying content bodies.
    #[must_use]
    pub fn project(record: &Record) -> Option<Self> {
        Some(match &record.event {
            Event::RunStarted(_) => Self::RunStarted,
            Event::ConversationOpened(opened) => {
                Self::Model { conversation: opened.conversation, model: model(&opened.model) }
            }
            Event::ResponseStarted(started) => {
                Self::Model { conversation: started.conversation, model: model(&started.model) }
            }
            Event::ConversationClosed(closed) => Self::ConversationClosed { conversation: closed.conversation },
            Event::ResponseCompleted(completed) => Self::ResponseCompleted {
                conversation: completed.conversation,
                usage: completed.usage.clone(),
                spent: completed.spent,
                duration_ms: completed.duration_ms,
                failure: {
                    let mut observed = None;
                    if let Some(failure) = &completed.failure {
                        observed = Some(Box::new(completion_failure(failure)));
                    }
                    observed
                },
            },
            Event::TextDelta(delta) => Self::TextDelta { t_ms: record.t_ms, delta: delta.clone() },
            Event::ToolStarted(started) => Self::ToolStarted { tool: smith_events::clip(&started.tool).into() },
            Event::ToolCompleted(completed) => Self::ToolCompleted {
                tool: smith_events::clip(&completed.tool).into(),
                verdict: verdict(&completed.verdict),
                duration_ms: completed.duration_ms,
            },
            Event::Notice(value) => Self::Notice(notice(value)),
            Event::RunCompleted(completed) => match &completed.failure {
                Some(failure) => Self::Failure(Box::new(answer_failure(failure))),
                None => return None,
            },
            Event::Budget(budget) => Self::Budget {
                turns: budget.turns.clone(),
                spend: budget.spend.clone(),
                time_ms: budget.time_ms.clone(),
            },
            Event::JobEnded(_)
            | Event::SessionStarted(_)
            | Event::SessionEnded(_)
            | Event::CheckStarted(_)
            | Event::CheckCompleted(_)
            | Event::MessageReceived(_)
            | Event::MessageRead(_)
            | Event::MessageUnread(_) => return None,
        })
    }

    pub(super) fn shows(record: &Record) -> bool {
        match &record.event {
            Event::RunCompleted(completed) => completed.failure.is_some(),
            Event::RunStarted(_)
            | Event::ConversationOpened(_)
            | Event::ResponseStarted(_)
            | Event::ConversationClosed(_)
            | Event::ResponseCompleted(_)
            | Event::TextDelta(_)
            | Event::ToolStarted(_)
            | Event::ToolCompleted(_)
            | Event::Notice(_)
            | Event::Budget(_) => true,
            Event::JobEnded(_)
            | Event::SessionStarted(_)
            | Event::SessionEnded(_)
            | Event::CheckStarted(_)
            | Event::CheckCompleted(_)
            | Event::MessageReceived(_)
            | Event::MessageRead(_)
            | Event::MessageUnread(_) => false,
        }
    }

    /// One queued projection, including its cell and the largest owned payload.
    #[must_use]
    pub fn worst_case(text_bytes: u32) -> Option<u64> {
        let string = u64::try_from(smith_events::DETAIL_BYTES).ok()?;
        let model = string.checked_mul(4)?;
        let tool = string.checked_mul(2)?;
        let notice = string.checked_mul(4)?;
        // Answer class, endpoint, model, completion class/evidence/phase/limit,
        // answer which/reason, provider field/code/request, and account state/code/program/detail
        // are sixteen independently bounded strings.
        let failure = string
            .checked_mul(16)?
            .checked_add(string)?
            .checked_add(u64::try_from(size_of::<AnswerFailure>()).ok()?)?;
        let payload = u64::from(text_bytes).max(model).max(tool).max(notice).max(failure);
        u64::try_from(size_of::<Self>()).ok()?.checked_add(payload)
    }
}

fn optional(value: Option<&[u8]>) -> Option<Box<[u8]>> {
    let mut shown = None;
    if let Some(value) = value {
        shown = Some(smith_events::clip(value).into());
    }
    shown
}

fn model(value: &Model) -> Model {
    Model {
        account: value.account,
        backend: match &value.backend {
            smith_events::Backend::Direct => smith_events::Backend::Direct,
            smith_events::Backend::ClaudeCode => smith_events::Backend::ClaudeCode,
            smith_events::Backend::Unknown(name) => smith_events::Backend::Unknown(smith_events::clip(name).into()),
        },
        endpoint: smith_events::clip(&value.endpoint).into(),
        model: smith_events::clip(&value.model).into(),
        window_tokens: value.window_tokens,
        output_tokens: value.output_tokens,
        effort: optional(value.effort.as_deref()),
    }
}

fn completion_failure(value: &CompletionFailure) -> CompletionFailure {
    let mut phase = None;
    if let Some(value) = &value.phase {
        phase = Some(match value {
            smith_events::FailurePhase::Unknown(text) => {
                smith_events::FailurePhase::Unknown(smith_events::clip(text).into())
            }
            smith_events::FailurePhase::Connect
            | smith_events::FailurePhase::Handshake
            | smith_events::FailurePhase::Head
            | smith_events::FailurePhase::Idle
            | smith_events::FailurePhase::Whole => value.clone(),
        });
    }
    let mut limit = None;
    if let Some(value) = &value.limit {
        limit = Some(smith_events::FailureLimit { which: smith_events::clip(&value.which).into(), bound: value.bound });
    }
    CompletionFailure {
        field: optional(value.field.as_deref()),
        code: optional(value.code.as_deref()),
        request_id: optional(value.request_id.as_deref()),
        class: match &value.class {
            smith_events::FailureClass::Unknown(text) => {
                smith_events::FailureClass::Unknown(smith_events::clip(text).into())
            }
            smith_events::FailureClass::Overloaded
            | smith_events::FailureClass::RateLimited
            | smith_events::FailureClass::Exhausted
            | smith_events::FailureClass::Refused
            | smith_events::FailureClass::Unsupported
            | smith_events::FailureClass::Unavailable
            | smith_events::FailureClass::TimedOut
            | smith_events::FailureClass::ContextTooLong
            | smith_events::FailureClass::Invalid
            | smith_events::FailureClass::Unauthorized
            | smith_events::FailureClass::Limit
            | smith_events::FailureClass::Protocol
            | smith_events::FailureClass::Cancelled => value.class.clone(),
        },
        evidence: match &value.evidence {
            smith_events::Evidence::Unknown(text) => smith_events::Evidence::Unknown(smith_events::clip(text).into()),
            smith_events::Evidence::Unsent | smith_events::Evidence::MaybeSent | smith_events::Evidence::Response => {
                value.evidence.clone()
            }
        },
        retry_after_ms: value.retry_after_ms,
        phase,
        limit,
        status: value.status,
        detail: optional(value.detail.as_deref()),
    }
}

fn answer_failure(value: &AnswerFailure) -> AnswerFailure {
    let mut completion = None;
    if let Some(value) = &value.completion {
        completion = Some(completion_failure(value));
    }
    AnswerFailure {
        state: match &value.state {
            Some(smith_events::AccountState::Unknown(text)) => {
                Some(smith_events::AccountState::Unknown(smith_events::clip(text).into()))
            }
            Some(
                smith_events::AccountState::Limited
                | smith_events::AccountState::Refused
                | smith_events::AccountState::Unauthorized,
            )
            | None => value.state.clone(),
        },
        code: optional(value.code.as_deref()),
        program: optional(value.program.as_deref()),
        exit: value.exit,
        detail: optional(value.detail.as_deref()),
        class: match &value.class {
            smith_events::AnswerClass::Unknown(text) => {
                smith_events::AnswerClass::Unknown(smith_events::clip(text).into())
            }
            smith_events::AnswerClass::Model
            | smith_events::AnswerClass::Budget
            | smith_events::AnswerClass::Policy
            | smith_events::AnswerClass::Cancelled
            | smith_events::AnswerClass::Agent
            | smith_events::AnswerClass::Host
            | smith_events::AnswerClass::Stale
            | smith_events::AnswerClass::Transcript
            | smith_events::AnswerClass::Busy
            | smith_events::AnswerClass::Invalid => value.class.clone(),
        },
        endpoint: optional(value.endpoint.as_deref()),
        model: optional(value.model.as_deref()),
        attempts: value.attempts,
        completion,
        account: value.account,
        reset_ms: value.reset_ms,
        used: value.used,
        bound: value.bound,
        which: optional(value.which.as_deref()),
        reason: optional(value.reason.as_deref()),
        dropped: value.dropped,
        ended: value.ended,
        turn: value.turn,
    }
}

fn notice(value: &Notice) -> Notice {
    let mut forced = None;
    if let Some(value) = &value.forced {
        forced = Some(match value {
            smith_events::Forced::Unknown(text) => smith_events::Forced::Unknown(smith_events::clip(text).into()),
            smith_events::Forced::No | smith_events::Forced::Terminated | smith_events::Forced::Killed => value.clone(),
        });
    }
    Notice {
        code: optional(value.code.as_deref()),
        level: match &value.level {
            smith_events::Level::Unknown(text) => smith_events::Level::Unknown(smith_events::clip(text).into()),
            smith_events::Level::Info | smith_events::Level::Warning => value.level.clone(),
        },
        kind: match &value.kind {
            smith_events::NoticeKind::Unknown(text) => {
                smith_events::NoticeKind::Unknown(smith_events::clip(text).into())
            }
            smith_events::NoticeKind::ReasoningDropped
            | smith_events::NoticeKind::CredentialUnauthorized
            | smith_events::NoticeKind::AccountLimited
            | smith_events::NoticeKind::AccountRefused
            | smith_events::NoticeKind::ForcedExit
            | smith_events::NoticeKind::TeardownAborted => value.kind.clone(),
        },
        run: value.run,
        account: value.account,
        reset_ms: value.reset_ms,
        bytes: value.bytes,
        exit: value.exit,
        forced,
    }
}

fn verdict(value: &Verdict) -> Verdict {
    match value {
        Verdict::Unknown(text) => Verdict::Unknown(smith_events::clip(text).into()),
        Verdict::Read
        | Verdict::Listed
        | Verdict::Found
        | Verdict::Removed
        | Verdict::NotEmpty
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
        | Verdict::NulByte => value.clone(),
    }
}
