//! Failed writes and job waits within a completion (domain/session.md, section 5).
//! No state beyond the batch's first failed call is retained here.
use super::{Block, Conversation, Decoded, Effect, Outcome, Returned, Slot, Tools, call_id, chosen};
use crate::llm::NotRun;
use smith_domain_tools::{self as tools, Call};

pub(super) fn suppressed(block: &Block) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Owned { call }, .. } => tools::effect(call) == Effect::Write,
        Block::ToolCall { call: Decoded::Delegated { effect, .. }, .. } => *effect == Effect::Write,
        Block::ToolCall { call: Decoded::Builtin { .. } | Decoded::Invalid { .. } | Decoded::Historical, .. }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => false,
    }
}

pub(super) fn answer_skipped(conversation: &Conversation, block: &Block, index: u32, tools: &mut Tools) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Builtin { .. }, .. } => return false,
        Block::ToolCall { .. }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => {}
    }
    let why = if chosen(&conversation.choice, block) {
        match tools.failed {
            Some(call) if suppressed(block) => NotRun::AfterFailedWrite { call },
            Some(_) | None => return false,
        }
    } else {
        NotRun::OutsideChoice { choice: conversation.choice_names.clone() }
    };
    let result = Block::ToolResult { id: call_id(conversation, index), result: Returned::NotRun { why } };
    put_next(tools, Slot::Done { result });
    true
}

pub(super) fn answered_next(tools: &mut Tools, block: &Block, result: &Returned) {
    answered(tools, tools.reached, block, result);
}

pub(super) fn answered(tools: &mut Tools, slot: u32, block: &Block, result: &Returned) {
    let failed = match result {
        Returned::Invalid { .. } => true,
        Returned::Owned { outcome } => {
            file_change(block)
                && match outcome {
                    Outcome::Removed | Outcome::Written { .. } | Outcome::Edited { .. } => false,
                    Outcome::Read { .. }
                    | Outcome::Listed { .. }
                    | Outcome::Found { .. }
                    | Outcome::Exited { .. }
                    | Outcome::Running { .. }
                    | Outcome::Job { .. }
                    | Outcome::NotGranted
                    | Outcome::Outside { .. }
                    | Outcome::ReadOnly { .. }
                    | Outcome::TooLong
                    | Outcome::NotFound { .. }
                    | Outcome::NotEmpty
                    | Outcome::NotFile
                    | Outcome::Linked
                    | Outcome::Protected
                    | Outcome::NotDirectory { .. }
                    | Outcome::TooLarge { .. }
                    | Outcome::NotRead
                    | Outcome::Stale
                    | Outcome::EditFailed { .. }
                    | Outcome::Failed { .. }
                    | Outcome::TimedOut
                    | Outcome::Cancelled
                    | Outcome::Busy
                    | Outcome::NulByte => true,
                }
        }
        Returned::Text { .. } | Returned::Withdrawn | Returned::NotRun { .. } => false,
    };
    if failed {
        let call = u16::try_from(slot.checked_add(1).expect("bounded call count")).expect("bounded completion calls");
        tools.failed = Some(tools.failed.unwrap_or(call).min(call));
    }
}

/// Forecast invalid calls and skip the same writes as advancement does.
pub(super) fn scheduled(block: &Block, failed: &mut bool) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Invalid { .. }, .. } => {
            *failed = true;
            false
        }
        Block::ToolCall { call: Decoded::Owned { .. } | Decoded::Delegated { .. }, .. } => {
            !*failed || !suppressed(block)
        }
        Block::ToolCall { call: Decoded::Builtin { .. } | Decoded::Historical, .. }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => false,
    }
}

fn file_change(block: &Block) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Owned { call }, .. } => match call {
            Call::Write { .. } | Call::Edit { .. } | Call::Remove { .. } => true,
            Call::Read { .. } | Call::List { .. } | Call::Search { .. } | Call::Shell { .. } | Call::Job { .. } => {
                false
            }
        },
        Block::ToolCall {
            call: Decoded::Delegated { .. } | Decoded::Builtin { .. } | Decoded::Invalid { .. } | Decoded::Historical,
            ..
        }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => false,
    }
}

/// Store an invalid call's already reserved answer without starting a run.
pub(super) fn invalid_slot(tools: &mut Tools, block: &Block, call: &[u8], problem: &crate::llm::Problem) {
    let answer = Returned::Invalid { problem: problem.clone() };
    answered_next(tools, block, &answer);
    let result = Block::ToolResult { id: call.into(), result: answer };
    put_next(tools, Slot::Done { result });
}

/// End remaining polls only after the batch's other work has answered.
pub(super) fn end_waits(
    conversation: &Conversation,
    calls: &mut super::Calls,
    tools: &Tools,
    env: &skein_lib::Env<super::Limits>,
    out: &mut skein_lib::Queue<super::Request>,
) {
    if !other_call(conversation) {
        return;
    }
    for slot in &tools.slots {
        match slot {
            Slot::Running { run } => {
                let run = calls.runs.get(*run).expect("an active slot owns its run");
                if !poll(super::current_block(conversation, run.block)) {
                    return;
                }
            }
            Slot::Done { .. } | Slot::Withheld => {}
        }
    }
    for slot in &tools.slots {
        match slot {
            Slot::Running { run } => {
                let event = tools::Event::EndWait { kit: conversation.kit, call: run.token() };
                let heard = super::tools_step(calls, env, event, out);
                assert!(heard.kit.is_none() && heard.answer.is_none(), "ending a wait owns no answer right");
            }
            Slot::Done { .. } | Slot::Withheld => {}
        }
    }
}

pub(super) fn poll(block: &Block) -> bool {
    match block {
        Block::ToolCall { call: Decoded::Owned { call: Call::Job { stop: false, .. } }, .. } => true,
        Block::ToolCall { .. }
        | Block::Text { .. }
        | Block::Refusal { .. }
        | Block::Opaque { .. }
        | Block::ToolResult { .. } => false,
    }
}

fn other_call(conversation: &Conversation) -> bool {
    for block in &conversation.transcript.last().expect("the completion precedes its results").content {
        match block {
            Block::ToolCall { call: Decoded::Owned { call: Call::Job { .. } }, .. }
            | Block::Text { .. }
            | Block::Refusal { .. }
            | Block::Opaque { .. }
            | Block::ToolResult { .. } => {}
            Block::ToolCall {
                name,
                call: Decoded::Builtin { .. } | Decoded::Invalid { .. } | Decoded::Historical,
                ..
            } => {
                if name.as_ref() != b"job" {
                    return true;
                }
            }
            Block::ToolCall { .. } => return true,
        }
    }
    false
}

pub(super) fn put_next(tools: &mut Tools, slot: Slot) {
    *tools.slots.get_mut(tools.reached).expect("one prepared slot per call") = slot;
    next_slot(tools);
}

pub(super) fn next_slot(tools: &mut Tools) {
    tools.reached = tools.reached.checked_add(1).expect("bounded call slots");
}
