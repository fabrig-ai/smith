//! Actual requests and compaction turns authorise whole-window replacement.
//! This referee knows the opener's window and accepted usage, never domain state.

use super::{Block, Prompt, Session, agent};

/// Check two actual requests of one window (domain/session.md, sections 3 and 10).
pub fn assert_prompt_extends(previous: &Prompt, current: &Prompt) {
    assert_prompt_boundary(previous, current, None);
}

/// A replacement needs the exact opening observed in a compaction turn.
pub fn assert_prompt_boundary(previous: &Prompt, current: &Prompt, opening: Option<&[agent::llm::Message]>) {
    assert_eq!(current.affinity, previous.affinity, "a window keeps its affinity");
    assert_eq!(current.system, previous.system, "a window keeps its system text");
    assert_eq!(current.tools, previous.tools, "a window keeps its tool grants");
    assert_eq!(current.delegated, previous.delegated, "a window keeps its tool descriptors");
    if let Some(opening) = opening {
        assert!(current.messages.starts_with(opening), "a window begins with its recorded opening");
    } else {
        assert!(current.messages.starts_with(&previous.messages), "a window extends its history");
    }
}

/// No actual request may exceed the supplied model window.
pub fn assert_prompt_bound(bound: u64, window: u32) {
    assert!(bound <= u64::from(window), "no prompt whose bound exceeds its window");
}

pub(super) fn prompt(session: &mut Session, prompt: &Prompt, limits: &agent::Limits) {
    if let Some(previous) = &session.previous_prompt {
        if session.compacting.is_some() {
            assert_compaction_prompt(previous, prompt, session);
        } else if session.opening.is_some() {
            assert_prompt_boundary(previous, prompt, session.opening.as_deref());
        } else {
            assert_prompt_extends(previous, prompt);
        }
    }
    session.opening = None;
    if session.compacting.is_none() {
        let bytes = payload_bytes(prompt) + prompt.protocol_allowance;
        let input = match session.ordinary {
            Some((usage, following)) if usage.input_tokens.is_some() => {
                let previous = session.previous_prompt.as_ref().expect("last ordinary request");
                let previous_bytes = payload_bytes(previous);
                let tokens = usage.input_tokens.unwrap_or(0)
                    + usage.cache_read_tokens.unwrap_or(0)
                    + usage.cache_write_tokens.unwrap_or(0);
                let appended: u64 =
                    prompt.messages[following..].iter().flat_map(|message| &message.content).map(block_bytes).sum();
                let appended = (appended * tokens)
                    .div_ceil(previous_bytes.max(1))
                    .clamp(appended.div_ceil(u64::from(limits.token_bytes)), appended);
                tokens + usage.output_tokens.unwrap_or(u64::from(session.max_tokens)) + appended
            }
            _ => session.summary.map_or(bytes, |(summary_bytes, tokens)| bytes - summary_bytes + tokens),
        };
        let threshold = u64::from(session.window) * u64::from(limits.threshold[0]) / u64::from(limits.threshold[1]);
        assert!(input <= threshold, "ordinary completion's check stays within its threshold: {input} > {threshold}");
    }

    session.previous_prompt = Some(prompt.clone());
}

pub(super) fn recorded_window(session: &mut Session, turn: &agent::record::Turn, index: usize) {
    assert!(session.compacting.is_some(), "a window boundary follows one compaction");
    assert_eq!(turn.messages.len(), 1);
    assert_eq!(turn.messages[0].role, agent::llm::Role::User);
    let text = session.accepted_content[index]
        .iter()
        .filter_map(|block| match block {
            Block::Text { text, .. } => Some(text.as_ref()),
            Block::Opaque { .. } | Block::Refusal { .. } | Block::ToolCall { .. } | Block::ToolResult { .. } => None,
        })
        .collect::<Vec<&[u8]>>()
        .join(b"\n\n".as_slice());
    assert_eq!(turn.messages[0].content[2], Block::Text { text: text.clone().into(), replay: None });
    session.compacting = None;
    session.opening = Some(turn.messages.clone());
    session.ordinary = None;
    session.summary = Some((
        u64::try_from(text.len()).expect("small summary"),
        turn.usage.output_tokens.unwrap_or(u64::from(session.max_tokens)),
    ));
}

fn assert_compaction_prompt(previous: &Prompt, current: &Prompt, session: &Session) {
    let following = session.ordinary.map_or(0, |(_, following)| following);
    let mut preserved = previous.clone();
    preserved.messages = previous
        .messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| {
            let mut message = message.clone();
            if index >= following && message.role != agent::llm::Role::Assistant {
                message.content =
                    message.content.into_iter().filter(|block| !matches!(block, Block::Text { .. })).collect();
            }
            (!message.content.is_empty()).then_some(message)
        })
        .collect();
    assert_prompt_extends(&preserved, current);
    assert_eq!(
        current.messages.last().expect("summary request").content,
        Box::new([Block::Text { text: session.summary_instruction.clone(), replay: None }]) as Box<[Block]>
    );
}

/// Check the provider's independently drawn size only when a request is sent.
pub(super) fn measured(session: &Session, prompt: &crate::provider::MeasuredPrompt) {
    if prompt.sent {
        assert!(prompt.input <= u64::from(session.window), "the provider reports no prompt past its window");
        if prompt.ordinary {
            assert!(prompt.input <= session.line, "no ordinary request sent past its line");
        }
    }
}

fn payload_bytes(prompt: &Prompt) -> u64 {
    u64::try_from(prompt.model.len() + prompt.system.len() + prompt.effort.as_ref().map_or(0, |v| v.len()))
        .expect("small fixed bytes")
        + prompt.namespace.owned_bytes().expect("small namespace")
        + prompt.messages.iter().flat_map(|message| &message.content).map(block_bytes).sum::<u64>()
}

fn block_bytes(block: &Block) -> u64 {
    use agent::llm::{Decoded, Returned};
    let replay = |replay: &Option<agent::llm::Replay>| replay.as_ref().map_or(0, |value| value.bytes.len());
    let size = match block {
        Block::Text { text, replay: saved } | Block::Refusal { text, replay: saved } => text.len() + replay(saved),
        Block::Opaque { bytes } => bytes.len(),
        Block::ToolCall { id, name, input, call, replay: saved } => {
            let decoded = match call {
                Decoded::Owned { call } => {
                    usize::try_from(call.owned_bytes().expect("small call")).expect("small call")
                        - std::mem::size_of::<smith_domain_tools::Call>()
                }
                Decoded::Invalid { problem, .. } => problem_bytes(problem),
                Decoded::Builtin { .. } | Decoded::Delegated { .. } | Decoded::Historical => 0,
            };
            id.len() + name.len() + input.len() + decoded + replay(saved)
        }
        Block::ToolResult { id, result } => {
            id.len()
                + match result {
                    Returned::Owned { outcome } => outcome_bytes(outcome),
                    Returned::Text { text, replay: saved, .. } => text.len() + replay(saved),
                    Returned::Invalid { problem } => problem_bytes(problem),
                    Returned::NotRun { why: agent::llm::NotRun::OutsideChoice { choice } } => match choice {
                        Some(names) => names.iter().map(|name| std::mem::size_of::<Box<[u8]>>() + name.len()).sum(),
                        None => 0,
                    },
                    Returned::NotRun {
                        why: agent::llm::NotRun::Stopped | agent::llm::NotRun::AfterFailedWrite { .. },
                    }
                    | Returned::Withdrawn => 0,
                }
        }
    };
    u64::try_from(size).expect("bounded payload")
}

fn problem_bytes(problem: &agent::llm::Problem) -> usize {
    match problem {
        agent::llm::Problem::Missing { field }
        | agent::llm::Problem::WrongType { field }
        | agent::llm::Problem::BadValue { field } => field.len(),
        agent::llm::Problem::UnknownTool
        | agent::llm::Problem::Nesting
        | agent::llm::Problem::NotAnObject
        | agent::llm::Problem::TooLarge
        | agent::llm::Problem::Oversize { .. }
        | agent::llm::Problem::CutOff { .. }
        | agent::llm::Problem::JobsFull { .. }
        | agent::llm::Problem::UnknownJob { .. } => 0,
    }
}

fn outcome_bytes(outcome: &smith_domain_tools::Outcome) -> usize {
    use smith_domain_tools::{Mismatch, Outcome};
    match outcome {
        Outcome::NotFound { path }
        | Outcome::NotDirectory { path }
        | Outcome::Outside { path }
        | Outcome::ReadOnly { path } => usize::try_from(path.owned_bytes().expect("small path")).expect("small path"),
        Outcome::Read { content, .. } => content.len(),
        Outcome::Listed { entries, .. } => {
            entries.len() * std::mem::size_of::<smith_domain_tools::Entry>()
                + entries.iter().map(|entry| entry.name.as_ref().len()).sum::<usize>()
        }
        Outcome::Found { hits, .. } => {
            hits.len() * std::mem::size_of::<smith_domain_tools::Hit>()
                + hits.iter().map(|hit| hit.path.len() + hit.text.len()).sum::<usize>()
        }
        Outcome::Exited { head, tail, .. } | Outcome::Running { head, tail, .. } | Outcome::Job { head, tail, .. } => {
            head.len() + tail.len()
        }
        Outcome::EditFailed { why: Mismatch::Ambiguous { lines, .. }, .. } => lines.len() * std::mem::size_of::<u32>(),
        Outcome::EditFailed { why: Mismatch::NoMatch | Mismatch::Unchanged, .. }
        | Outcome::Removed
        | Outcome::NotEmpty
        | Outcome::Written { .. }
        | Outcome::Edited { .. }
        | Outcome::NotGranted
        | Outcome::TooLong
        | Outcome::NotFile
        | Outcome::Linked
        | Outcome::Protected
        | Outcome::TooLarge { .. }
        | Outcome::NotRead
        | Outcome::Stale
        | Outcome::Failed { .. }
        | Outcome::TimedOut
        | Outcome::Cancelled
        | Outcome::Busy
        | Outcome::NulByte => 0,
    }
}
