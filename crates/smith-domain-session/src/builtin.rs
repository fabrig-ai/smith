//! Owed program results (protocol/programs.md, sections 5.4–5.5; domain/tools.md, section 10).
//! A bounded table retains provider identity, result credit and its current place.
//! It knows no paths or program policy. Preparation makes stand-ins immediately;
//! result patches consume their credit, and forgetting settles the remaining stand-ins.

use super::{By, Calls, Conversation, Session, Slot, State, Tools, addressed, history};
use crate::facts::{FactKind, Facts, ToolCall, ToolSource};
use crate::llm::{Block, Decoded, Returned};
use crate::{Domain, Limits, Request};
use alloc::boxed::Box;
use core::mem::size_of;
use skein_lib::{Env, Id, List, Map, Queue, Token};

const STAND_IN: &[u8] = b"smith: the program wrote no result for this call";

#[derive(Clone, Copy, Debug)]
pub(super) enum Place {
    Slot { index: u32 },
    Result { message: u32, block: u32 },
}

#[derive(Debug)]
struct Claim {
    place: Place,
    // The map key owns the id; metadata.id stays empty until an observation is emitted.
    metadata: ToolCall,
    block: u32,
    credit: u64,
    kept_bytes: u64,
    announced: bool,
    kept: bool,
}

#[derive(Debug)]
pub(super) struct Owed {
    entries: Map<Box<[u8]>, Claim>,
}

impl Owed {
    pub(super) fn with_capacity(calls: u32) -> Owed {
        Owed { entries: Map::with_capacity(calls) }
    }

    pub(super) fn is_empty(&self) -> bool {
        for (_, claim) in &self.entries {
            if !claim.kept {
                return false;
            }
        }
        true
    }

    pub(super) fn moved(&mut self, message: u32) {
        for index in 0..self.entries.len() {
            let key = self
                .entries
                .iter()
                .nth(usize::try_from(index).expect("bounded entry"))
                .expect("entry exists")
                .0
                .clone();
            let claim = self.entries.get_mut(&key).expect("entry exists");
            claim.place = match claim.place {
                Place::Slot { index } => Place::Result { message, block: index },
                place @ Place::Result { .. } => place,
            };
        }
    }
}

pub(super) fn prepare(
    conversation: &mut Conversation,
    session: Id<Session>,
    calls: &mut Calls,
    tools: &mut Tools,
    limits: &Limits,
) {
    let message = conversation.transcript.last().expect("assistant precedes its results");
    let mut builtins = 0_u32;
    let mut others = 0_u32;
    for block in &message.content {
        match block {
            Block::ToolCall { call: Decoded::Builtin { .. }, .. } => {
                builtins = builtins.checked_add(1).expect("bounded calls");
            }
            Block::ToolCall { .. } => others = others.checked_add(1).expect("bounded calls"),
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
        }
    }
    let credit = if builtins == 0 {
        0
    } else {
        let available = limits
            .history_bytes
            .checked_sub(conversation.bytes)
            .expect("history fits")
            .checked_sub(conversation.reserved)
            .expect("reservations fit")
            .checked_sub(history::minimum(limits).checked_mul(u64::from(others)).expect("bounded future results"))
            .expect("provider left future calls an answer")
            .checked_sub(u64::try_from(size_of::<Block>()).expect("block fits"))
            .expect("summary room");
        super::delegated_credit(limits).min(available.checked_div(u64::from(builtins)).expect("positive calls"))
    };
    for (position, block) in message.content.iter().enumerate() {
        match block {
            Block::ToolCall { id, name, input, call: Decoded::Builtin { effect }, .. } => {
                let metadata = ToolCall {
                    owner: Token::new(0),
                    id: id.clone(),
                    name: name.clone(),
                    source: ToolSource::Workspace,
                    effect: *effect,
                    deadline: conversation.expires,
                    input_bytes: u64::try_from(input.len()).expect("bounded input"),
                };
                let position = u32::try_from(position).expect("bounded block");
                let run =
                    super::observed_run(calls, session, tools.slots.len(), position, By::Program, credit, metadata);
                let mut metadata = calls.runs.get(run).expect("issued program observation").metadata.clone();
                metadata.id = Box::default();
                calls.runs.retire(run);
                let result =
                    history::cut_result(Returned::Text { text: STAND_IN.into(), error: true, replay: None }, credit);
                let kept_bytes = super::returned_cost(&result).expect("bounded stand-in");
                let claim = Claim {
                    place: Place::Slot { index: tools.slots.len() },
                    metadata,
                    block: position,
                    credit,
                    kept_bytes,
                    announced: false,
                    kept: false,
                };
                let previous =
                    conversation.builtin.entries.insert(id.clone(), claim).expect("one entry per completion call");
                assert!(previous.is_none(), "provider call ids are unique within a completion");
                tools
                    .slots
                    .push(Slot::Done { result: Block::ToolResult { id: id.clone(), result } })
                    .expect("one slot per call");
                conversation.reserved = conversation
                    .reserved
                    .checked_add(credit.checked_sub(kept_bytes).expect("stand-in fits credit"))
                    .expect("reserved result room");
                conversation.bytes = conversation.bytes.checked_add(kept_bytes).expect("stand-in fits history");
            }
            Block::ToolCall { .. } => tools.slots.push(Slot::Withheld).expect("one slot per call"),
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
        }
    }
}

/// Prepare late built-ins through the same credit and observation path as live calls.
pub(super) fn late(
    conversation: &mut Conversation,
    session: Id<Session>,
    calls: &mut Calls,
    count: u32,
    limits: &Limits,
) {
    let mut tools = Tools {
        after: super::After::Complete,
        slots: List::with_capacity(count),
        running: 0,
        next: 0,
        failed: None,
        reached: 0,
    };
    prepare(conversation, session, calls, &mut tools, limits);
    conversation.closing_tools = Some(tools);
}

/// Mark stand-ins settled before a completion, close, failure or window replacement.
pub(super) fn forget(conversation: &mut Conversation) {
    for index in 0..conversation.builtin.entries.len() {
        let key = conversation
            .builtin
            .entries
            .iter()
            .nth(usize::try_from(index).expect("bounded entry"))
            .expect("entry exists")
            .0
            .clone();
        let claim = conversation.builtin.entries.get_mut(&key).expect("entry exists");
        if !claim.kept {
            conversation.reserved = conversation
                .reserved
                .checked_sub(claim.credit.checked_sub(claim.kept_bytes).expect("stand-in credit"))
                .expect("owed result reserve");
            claim.kept = true;
        }
    }
}

/// Emit starts before any terminal, and release only identities already settled.
pub(super) fn flush(conversation: &mut Conversation, facts: &mut Facts) {
    for index in 0..conversation.builtin.entries.len() {
        let id = conversation
            .builtin
            .entries
            .iter()
            .nth(usize::try_from(index).expect("bounded entry"))
            .expect("entry exists")
            .0
            .clone();
        let claim = conversation.builtin.entries.get_mut(&id).expect("entry exists");
        let mut metadata = claim.metadata.clone();
        metadata.id.clone_from(&id);
        if !claim.announced {
            facts.push_call(
                facts.now(),
                FactKind::BuiltinStarted { opener: conversation.opener, block: claim.block },
                &metadata,
            );
            claim.announced = true;
        }
        if claim.kept {
            facts.push_call(
                facts.now(),
                FactKind::BuiltinAnswered { opener: conversation.opener, bytes: claim.kept_bytes, error: true },
                &metadata,
            );
        }
    }
    for _ in 0..conversation.builtin.entries.capacity() {
        let mut settled = None;
        for (id, claim) in &conversation.builtin.entries {
            if claim.kept {
                settled = Some(id.clone());
                break;
            }
        }
        match settled {
            Some(id) => {
                conversation.builtin.entries.remove(&id);
            }
            None => break,
        }
    }
}

pub(crate) fn result(
    domain: &mut Domain,
    env: &Env<Limits>,
    session: Token,
    id: Box<[u8]>,
    text: Box<[u8]>,
    error: bool,
    out: &mut Queue<Request>,
) {
    let Some(session_id) = addressed(&domain.sessions, session) else {
        return;
    };
    let current = domain.sessions.get_mut(session_id).expect("addressed session");
    let conversation = &mut current.conversation;
    let claim = match conversation.builtin.entries.remove(&id) {
        Some(claim) if !claim.kept => claim,
        Some(_) | None => return,
    };
    let answer = history::cut_result(Returned::Text { text, error, replay: None }, claim.credit);
    let bytes = super::returned_cost(&answer).expect("bounded program result");
    conversation.bytes = conversation.bytes.checked_sub(claim.kept_bytes).expect("stand-in was counted");
    conversation.reserved = conversation.reserved.checked_add(claim.kept_bytes).expect("restore full credit");
    super::receive_result(conversation, claim.credit, &answer, &env.limits);
    let block = match claim.place {
        Place::Slot { index } => {
            let tools = match &mut current.state {
                State::Tooling { tools } | State::Resting { tools } => tools,
                State::Closing { .. } => conversation.closing_tools.as_mut().expect("closing slots retained"),
                State::Calling { .. } | State::Backoff { .. } | State::Yielded | State::Closed => {
                    unreachable!("owed slot lives with tools")
                }
            };
            match tools.slots.get_mut(index).expect("owed slot") {
                Slot::Done { result } => result,
                Slot::Running { .. } | Slot::Withheld => unreachable!("built-in is done immediately"),
            }
        }
        Place::Result { message, block } => conversation
            .transcript
            .get_mut(message)
            .expect("owed message")
            .content
            .get_mut(usize::try_from(block).expect("bounded block"))
            .expect("owed block"),
    };
    match block {
        Block::ToolResult { result, .. } => *result = answer,
        Block::ToolCall { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } => {
            unreachable!("owed result place")
        }
    }
    let mut metadata = claim.metadata;
    metadata.id = id;
    domain.facts.push_call(env.now, FactKind::BuiltinAnswered { opener: conversation.opener, bytes, error }, &metadata);
    let in_history = match claim.place {
        Place::Result { .. } => true,
        Place::Slot { .. } => false,
    };
    if conversation.builtin.is_empty() && in_history {
        let mark = out.len();
        if let Some(end) = super::tell_turn(conversation, out) {
            current.state = super::finish(end);
        }
        super::conclude(domain, env, session_id, out, mark);
    }
}

pub(super) fn finish_closing(conversation: &mut Conversation) {
    let Some(tools) = conversation.closing_tools.take() else {
        return;
    };
    conversation.builtin.moved(conversation.transcript.len());
    let message = conversation.transcript.last().expect("assistant recorded before tooling");
    let mut results = List::with_capacity(tools.slots.len());
    let mut slots = tools.slots.into_boxed().into_iter();
    for (position, block) in message.content.iter().enumerate() {
        match block {
            Block::ToolCall { id, .. } => match slots.next().expect("one prepared slot per call") {
                Slot::Done { result } => results.push(result).expect("result slot"),
                Slot::Withheld => {
                    if position >= usize::try_from(tools.next).expect("bounded position") {
                        results
                            .push(Block::ToolResult {
                                id: id.clone(),
                                result: Returned::NotRun { why: crate::llm::NotRun::Stopped },
                            })
                            .expect("stopped result slot");
                    }
                }
                Slot::Running { .. } => unreachable!("closing waits for active calls"),
            },
            Block::Text { .. } | Block::Refusal { .. } | Block::Opaque { .. } | Block::ToolResult { .. } => {}
        }
    }
    if !results.is_empty() {
        conversation
            .transcript
            .push(crate::llm::Message { role: crate::llm::Role::User, content: results.into_boxed() })
            .expect("reserved results message");
    }
}

/// A window replacement takes the old turn with its stand-ins before adding its instruction.
pub(super) fn keep_turn(conversation: &mut Conversation, out: &mut Queue<Request>) -> Option<crate::End> {
    forget(conversation);
    super::tell_turn(conversation, out)
}
