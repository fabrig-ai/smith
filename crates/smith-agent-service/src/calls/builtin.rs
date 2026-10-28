//! Projection of program call facts (protocol/events.md, section 3.6).
//! Only start times are held; terminal facts carry the session's same identity.
//! The common session issuer names these calls without retaining result text.

use crate::{inputs, sinks::Sinks};
use skein_lib::{Map, Time, Token};
use smith_domain::session;
use smith_events::{Event, Record, ToolCompleted, ToolStarted};

#[derive(Debug)]
pub(super) struct Book {
    started: Map<Token, Time>,
}

/// Whether every owed program call can be named (programming-model.md, 6.3).
#[must_use]
pub(super) fn valid(limits: &session::Limits) -> bool {
    limits.sessions.checked_mul(limits.completion_blocks).is_some()
}

impl Book {
    pub(super) fn new(limits: &session::Limits) -> Book {
        Book {
            started: Map::with_capacity(
                limits.sessions.checked_mul(limits.completion_blocks).expect("bounded owed program calls"),
            ),
        }
    }

    pub(super) fn worst_case(limits: &session::Limits) -> Option<u64> {
        if !valid(limits) {
            return None;
        }
        Map::<Token, Time>::worst_case(limits.sessions.checked_mul(limits.completion_blocks)?)
    }

    pub(super) fn fact(
        &mut self,
        origin: Time,
        fact: &session::Fact,
        inputs: &mut inputs::Book,
        sinks: &mut Sinks,
    ) -> bool {
        let (opener, answer) = match fact.kind {
            session::FactKind::BuiltinStarted { opener, .. } => (opener, None),
            session::FactKind::BuiltinAnswered { opener, bytes, error } => (opener, Some((bytes, error))),
            session::FactKind::Opened { .. }
            | session::FactKind::CompactionStarted { .. }
            | session::FactKind::CompactionEnded { .. }
            | session::FactKind::CompletionStarted { .. }
            | session::FactKind::CompletionAnswered { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::CompletionFailed { .. }
            | session::FactKind::CompletionCancelled { .. }
            | session::FactKind::CompletionRetried { .. }
            | session::FactKind::Tools { .. }
            | session::FactKind::DelegateStarted { .. }
            | session::FactKind::DelegateAnswered { .. }
            | session::FactKind::DelegateCancelled { .. }
            | session::FactKind::DelegateWithheld { .. }
            | session::FactKind::JobEnded { .. }
            | session::FactKind::Yielded { .. }
            | session::FactKind::Used { .. }
            | session::FactKind::Ended { .. } => return false,
        };
        let metadata = fact.call.as_ref().expect("program call facts carry provider identity");
        let scope = sinks.scope(opener).expect("conversation precedes program calls");
        let event = match answer {
            None => {
                let previous =
                    self.started.insert(metadata.owner, fact.at).expect("one start time per owed program call");
                assert!(previous.is_none(), "one start per program call");
                let effect = match metadata.effect {
                    smith_domain::tools::Effect::Read => smith_events::Effect::Read,
                    smith_domain::tools::Effect::Write => smith_events::Effect::Write,
                };
                Event::ToolStarted(ToolStarted {
                    run: scope.run,
                    conversation: scope.conversation,
                    call: metadata.id.clone(),
                    tool: metadata.name.clone(),
                    source: smith_events::Source::Workspace,
                    effect,
                    deadline_ms: Some(super::millis(metadata.deadline, fact.at)),
                    input_bytes: metadata.input_bytes,
                    input: inputs.take(opener, metadata),
                })
            }
            Some((bytes, error)) => {
                let start = self.started.remove(&metadata.owner).expect("program terminal follows start");
                Event::ToolCompleted(ToolCompleted {
                    run: scope.run,
                    conversation: scope.conversation,
                    call: metadata.id.clone(),
                    tool: metadata.name.clone(),
                    verdict: if error { smith_events::Verdict::Error } else { smith_events::Verdict::Result },
                    exit: None,
                    job: None,
                    bytes,
                    omitted_bytes: 0,
                    clamped: false,
                    duration_ms: super::millis(fact.at, start),
                    child: None,
                    delivery: None,
                })
            }
        };
        sinks.record(&Record { t_ms: super::millis(fact.at, origin), event });
        true
    }
}

