//! History admission and bounded result batches (domain/session.md, section 8.1).
//! This retains no state: it forecasts space before provider work and shares
//! remaining payload room before tool work. Window replacement owns held text.

use super::{Conversation, End, Limits, Request, Session, State, compaction};
use crate::llm::{Block, Cap, Decoded, Evidence, Failure, Limit, Message, NotRun, Returned, Role};
use crate::window::Stage;
use alloc::boxed::Box;
use core::mem::size_of;
use skein_lib::{Decimal, Env, Id, List, Queue, Writer};
use smith_domain_tools::{self as tools, Call};

pub(super) fn limit(bound: u64) -> End {
    End::Failed {
        failure: Failure::Limit { which: Limit::History, bound },
        evidence: Evidence::Unsent,
        attempts: 0,
        detail: Box::default(),
    }
}

pub(super) fn sequence_limit() -> End {
    End::Failed { failure: Failure::ApplicationLimit, evidence: Evidence::Unsent, attempts: 0, detail: Box::default() }
}

pub(super) fn completion_limit(conversation: &Conversation, content: &[Block], limits: &Limits) -> End {
    let (which, bound) = if super::count(content.len()) > limits.completion_blocks {
        (Cap::OutputItems, u64::from(limits.completion_blocks))
    } else {
        (Cap::Answer, limits.completion_bytes)
    };
    End::Failed {
        detail: Box::default(),
        failure: Failure::Limit { which: Limit::Path(which), bound },
        evidence: Evidence::Response { status: None },
        attempts: conversation.completion_attempts,
    }
}

fn summary_cost() -> Option<u64> {
    u64::try_from(size_of::<Block>()).ok()
}

pub(super) fn due(conversation: &Conversation, limits: &Limits) -> Option<u64> {
    let summary = !conversation.final_turn.answering();
    let messages = if summary { 3 } else { 2 };
    if conversation.transcript.room() < messages {
        return Some(u64::from(limits.history_messages));
    }
    let Some(pending) = pending_blocks(limits, summary) else {
        return Some(u64::from(limits.history_blocks));
    };
    if !super::blocks_fit(conversation, pending, limits) {
        return Some(u64::from(limits.history_blocks));
    }
    let wanted = wanted_bytes(conversation, limits, summary);
    match wanted {
        Some(bytes) if bytes <= limits.history_bytes => None,
        Some(_) | None => Some(limits.history_bytes),
    }
}

fn pending_blocks(limits: &Limits, summary: bool) -> Option<u32> {
    limits.completion_blocks.checked_mul(2)?.checked_add(u32::from(summary))
}

fn wanted_bytes(conversation: &Conversation, limits: &Limits, summary: bool) -> Option<u64> {
    let future = if summary { minimum(limits).checked_mul(u64::from(limits.completion_blocks))? } else { 0 };
    let choices =
        super::choice_bytes(conversation.choice_names.as_deref()).checked_mul(u64::from(limits.completion_blocks))?;
    let instruction = if summary { summary_cost()? } else { 0 };
    // A call retains its rejecting choice or runs with result credit; those payloads never coexist.
    conversation
        .bytes
        .checked_add(conversation.reserved)?
        .checked_add(super::provider_reserve(limits)?)?
        .checked_add(instruction)?
        .checked_add(future.max(choices))
}

pub(super) fn start(conversation: &mut Conversation, limits: &Limits, bound: u64) -> Option<End> {
    match conversation.window.stage {
        Stage::Fresh => Some(limit(bound)),
        Stage::Used if conversation.compaction.asking() => Some(limit(bound)),
        Stage::Used => compaction::start(conversation, limits, Evidence::Unsent, 0),
    }
}

pub(super) fn hold_note(conversation: &mut Conversation, text: Box<[u8]>, limits: &Limits) -> Option<End> {
    if !conversation.compaction.asking()
        && let Some(end) = start(conversation, limits, limits.history_bytes)
    {
        return Some(end);
    }
    match &mut conversation.compaction {
        compaction::Compaction::Asking { held, .. } => held.push(text).expect("one held offer beyond old history"),
        compaction::Compaction::Idle | compaction::Compaction::CarryOver => {
            unreachable!("successful history trigger asks for a summary")
        }
    }
    None
}

pub(super) fn continue_held(
    conversation: &mut Conversation,
    id: Id<Session>,
    text: Box<[u8]>,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> State {
    owe_stopped_calls(conversation);
    match hold_note(conversation, text, &env.limits) {
        Some(end) => super::finish(end),
        None => super::call(conversation, id, 0, env, out),
    }
}

/// A yielded answer's result skeleton was charged when the completion arrived.
fn owe_stopped_calls(conversation: &mut Conversation) {
    let message = conversation.transcript.last().expect("yielded assistant");
    let (calls, _) = super::tally(&message.content);
    if calls == 0 {
        return;
    }
    let mut results = List::with_capacity(calls);
    for block in &message.content {
        match block {
            Block::ToolCall { id, .. } => results
                .push(Block::ToolResult { id: id.clone(), result: Returned::NotRun { why: NotRun::Stopped } })
                .expect("one result for each stopped call"),
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
        }
    }
    conversation
        .transcript
        .push(Message { role: Role::User, content: results.into_boxed() })
        .expect("provider admission reserved a result message");
}

pub(super) fn minimum(limits: &Limits) -> u64 {
    // A path refusal, an ambiguous edit and the longest text marker still fit.
    u64::from(limits.tools.path_bytes)
        .max(u64::from(limits.tools.match_lines).checked_mul(4).expect("u32 diagnostic count"))
        .max(37)
}

/// Give future calls their smallest answer before sharing this batch's room.
pub(super) fn reserve_batch(conversation: &mut Conversation, next: u32, failed: bool, limits: &Limits) -> u64 {
    let message = conversation.transcript.last().expect("assistant awaiting results");
    let mut batch = None;
    let mut count = 0_u32;
    let mut following = 0_u32;
    let mut wanted = 0_u64;
    let mut ended = false;
    let mut blocked = failed;
    for block in message.content.iter().skip(usize::try_from(next).expect("u32 fits")) {
        if !super::chosen(&conversation.choice, block) || !super::batching::scheduled(block, &mut blocked) {
            continue;
        }
        let (effect, credit) = match block {
            Block::ToolCall { call: Decoded::Owned { call }, .. } => {
                (tools::effect(call), super::owned_credit(call, limits))
            }
            Block::ToolCall { call: Decoded::Delegated { effect, .. }, .. } => {
                (*effect, super::delegated_credit(limits))
            }
            Block::ToolCall {
                call: Decoded::Builtin { .. } | Decoded::Invalid { .. } | Decoded::Historical, ..
            }
            | Block::Text { .. }
            | Block::Refusal { .. }
            | Block::Opaque { .. }
            | Block::ToolResult { .. } => continue,
        };
        if ended || !super::joins(batch, effect, count, limits.parallel_tools) {
            ended = true;
            following = following.checked_add(1).expect("bounded calls");
        } else {
            wanted = wanted.checked_add(credit).expect("bounded batch credit");
            count = count.checked_add(1).expect("bounded batch width");
            batch = Some(effect);
        }
    }
    if count == 0 {
        return u64::MAX;
    }
    let held = conversation.bytes.checked_add(conversation.reserved).expect("bounded retained bytes");
    let left = limits.history_bytes.checked_sub(held).expect("retained history fits");
    let future = minimum(limits).checked_mul(u64::from(following)).expect("bounded following results");
    let history_room = left
        .checked_sub(future)
        .expect("provider admission left each call an answer")
        .checked_sub(summary_cost().expect("bounded summary"))
        .expect("summary room reserved");
    let minima = minimum(limits).checked_mul(u64::from(count)).expect("bounded batch minima");
    let available = match line_room(conversation, future) {
        Some(room) if room >= minima => room.min(history_room),
        Some(_) | None => history_room,
    };
    let share = if wanted <= available {
        u64::MAX
    } else {
        available.checked_div(u64::from(count)).expect("positive batch width")
    };
    assert!(share >= minimum(limits), "each scheduled result has a minimum answer");
    let mut reserved = 0_u64;
    let mut reached = 0_u32;
    let mut blocked = failed;
    for block in message.content.iter().skip(usize::try_from(next).expect("u32 fits")) {
        if reached == count
            || !super::chosen(&conversation.choice, block)
            || !super::batching::scheduled(block, &mut blocked)
        {
            continue;
        }
        let credit = match block {
            Block::ToolCall { call: Decoded::Owned { call }, .. } => {
                super::owned_credit(call, &tool_limits(call, share, limits)).min(share)
            }
            Block::ToolCall { call: Decoded::Delegated { .. }, .. } => super::delegated_credit(limits).min(share),
            Block::ToolCall {
                call: Decoded::Builtin { .. } | Decoded::Invalid { .. } | Decoded::Historical, ..
            }
            | Block::Text { .. }
            | Block::Refusal { .. }
            | Block::Opaque { .. }
            | Block::ToolResult { .. } => continue,
        };
        reserved = reserved.checked_add(credit).expect("batch credit within room");
        reached = reached.checked_add(1).expect("bounded width");
    }
    conversation.reserved = conversation.reserved.checked_add(reserved).expect("reserved batch fits history");
    share
}

/// Estimate the guard's room, retaining overhead beyond the last prompt's payload baseline.
fn line_room(conversation: &Conversation, future: u64) -> Option<u64> {
    let prompt = conversation.window.last_prompt?;
    let left = conversation.line.checked_sub(prompt.0)?;
    let decoded =
        left.checked_mul(u64::from(crate::ENCODED_BYTES[1]))?.checked_div(u64::from(crate::ENCODED_BYTES[0]))?;
    let appended = conversation.bytes.checked_sub(conversation.window.last_prompt_bytes)?;
    decoded.checked_sub(appended)?.checked_sub(conversation.reserved)?.checked_sub(future)
}

/// Select result caps for this call without changing its input or other calls.
pub(super) fn tool_env(call: &Call, share: u64, env: &Env<Limits>) -> Env<Limits> {
    let limits = tool_limits(call, share, &env.limits);
    Env { now: env.now, wall: env.wall, limits }
}

fn tool_limits(call: &Call, share: u64, declared: &Limits) -> Limits {
    let mut limits = *declared;
    let cap = u32::try_from(share).unwrap_or(u32::MAX);
    match call {
        Call::Read { .. } => limits.tools.tool_payload = limits.tools.tool_payload.min(cap),
        Call::Shell { .. } | Call::Job { .. } => {
            let full = limits.tools.shell_head.checked_add(limits.tools.shell_tail).expect("declared capture");
            if cap < full {
                let head = u64::from(cap)
                    .checked_mul(u64::from(limits.tools.shell_head))
                    .expect("u32 product")
                    .checked_div(u64::from(full))
                    .expect("positive capture");
                limits.tools.shell_head = u32::try_from(head).expect("within cap");
                limits.tools.shell_tail = cap.checked_sub(limits.tools.shell_head).expect("within cap");
            }
        }
        Call::List { .. } => limits.tools.list_bytes = limits.tools.list_bytes.min(share),
        Call::Search { .. } => {
            let hit_cell = u64::try_from(size_of::<tools::Hit>()).expect("cell fits");
            let hits = share.checked_div(hit_cell.checked_add(1).expect("cell fits")).expect("positive cell");
            limits.tools.search_hits = limits.tools.search_hits.min(u32::try_from(hits).unwrap_or(u32::MAX));
            let cells = hit_cell.checked_mul(u64::from(limits.tools.search_hits)).expect("bounded cells");
            limits.tools.search_bytes = limits
                .tools
                .search_bytes
                .min(u32::try_from(share.checked_sub(cells).expect("cells within share")).unwrap_or(u32::MAX));
        }
        Call::Write { .. } | Call::Edit { .. } | Call::Remove { .. } => {}
    }
    limits
}

pub(super) fn cut_result(result: Returned, credit: u64) -> Returned {
    match result {
        Returned::Text { text, error, replay } => {
            let cap = credit
                .checked_sub(super::replay_cost(replay.as_ref()).expect("bounded replay"))
                .expect("concrete result replay within its credit");
            Returned::Text { text: cut_text(text, cap), error, replay }
        }
        result
        @ (Returned::Owned { .. } | Returned::Invalid { .. } | Returned::NotRun { .. } | Returned::Withdrawn) => result,
    }
}

fn cut_text(text: Box<[u8]>, cap: u64) -> Box<[u8]> {
    if u64::try_from(text.len()).expect("owned bytes fit") <= cap {
        return text;
    }
    let mut kept = usize::try_from(cap.checked_sub(37).expect("marker room reserved")).expect("bounded cap");
    for _ in 0_u32..3 {
        let continuation = match text.get(kept) {
            Some(byte) => byte & 0xc0 == 0x80,
            None => false,
        };
        if continuation && kept > 0 {
            kept = kept.checked_sub(1).expect("positive prefix");
        }
    }
    let omitted = u64::try_from(text.len().checked_sub(kept).expect("prefix within text")).expect("bounded omission");
    let decimal = Decimal::of(omitted);
    let length = kept.checked_add(decimal.as_bytes().len()).expect("marker count").checked_add(17).expect("marker");
    let mut writer = Writer::new(length);
    writer.put(text.get(..kept).expect("prefix within text")).expect("prefix room");
    writer.put(b"\n[").expect("marker room");
    writer.put(decimal.as_bytes()).expect("count room");
    writer.put(b" bytes omitted]").expect("marker room");
    writer.finish()
}
