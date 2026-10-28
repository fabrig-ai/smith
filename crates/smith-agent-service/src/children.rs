//! Scalar child joins from actual opening, raw provider usage and settled root facts.
//! Each retained entry belongs to one live provider call, until its enclosing
//! terminal. It keeps no child text, feedback or prompts, and never derives a
//! child answer from wording. Contracts: protocol/events.md, sections3.6/3.10;
//! domain/run.md, section5.3.

#![expect(clippy::manual_let_else, reason = "bounded production joins keep explicit Option terminals")]

use skein_lib::{Map, Time, Token};
use smith_domain::{llm, run, session};

#[derive(Debug)]
struct Child {
    call: Token,
    number: u64,
    opened: Time,
    budget: bool,
    stop: smith_events::ChildStop,
    turns: u32,
    usage: crate::observations::Reported,
    spent: u64,
    time_ms: u64,
    ended: bool,
}

/// Fixed child observations, bounded by the enclosing calls still owing terminals.
#[derive(Debug)]
pub(crate) struct Book {
    children: Map<Token, Child>,
}

impl Book {
    pub(crate) fn new(calls: u32) -> Book {
        Book { children: Map::with_capacity(calls) }
    }

    pub(crate) fn opened(&mut self, owner: Token, number: u64, call: Token, at: Time) {
        let previous = self
            .children
            .insert(
                owner,
                Child {
                    call,
                    number,
                    opened: at,
                    budget: false,
                    stop: smith_events::ChildStop::Closed,
                    turns: 0,
                    usage: crate::observations::Reported::NONE,
                    spent: 0,
                    time_ms: 0,
                    ended: false,
                },
            )
            .expect("one child per provider call still owing a terminal");
        assert!(previous.is_none(), "an actual child opens once");
    }

    pub(crate) fn received(&mut self, owner: Token, usage: llm::Usage) {
        if let Some(child) = self.children.get_mut(&owner) {
            child.usage.add(usage);
        }
    }

    pub(crate) fn session(&mut self, fact: &session::Fact) {
        let opener = match fact.kind {
            session::FactKind::Yielded { opener, .. }
            | session::FactKind::Used { opener, .. }
            | session::FactKind::Ended { opener, .. } => opener,
            session::FactKind::CompactionStarted { .. }
            | session::FactKind::CompactionEnded { .. }
            | session::FactKind::Opened { .. }
            | session::FactKind::CompletionStarted { .. }
            | session::FactKind::CompletionAnswered { .. }
            | session::FactKind::CompletionFailed { .. }
            | session::FactKind::CompletionCancelled { .. }
            | session::FactKind::CompletionRetried { .. }
            | session::FactKind::JobEnded { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::Tools { .. }
            | session::FactKind::BuiltinStarted { .. }
            | session::FactKind::BuiltinAnswered { .. }
            | session::FactKind::DelegateStarted { .. }
            | session::FactKind::DelegateAnswered { .. }
            | session::FactKind::DelegateCancelled { .. }
            | session::FactKind::DelegateWithheld { .. } => return,
        };
        let child = match self.children.get_mut(&opener) {
            Some(child) => child,
            None => return,
        };
        match fact.kind {
            session::FactKind::Yielded { stop, .. } => {
                child.budget = match stop {
                    session::Yield::Budget => true,
                    session::Yield::Done
                    | session::Yield::Truncated
                    | session::Yield::Refused
                    | session::Yield::Malformed => false,
                };
            }
            session::FactKind::Used { .. } => {
                let spent = fact
                    .response
                    .expect("actual provider charge accompanies Used")
                    .spent
                    .expect("accepted Used carries its actual charge");
                child.spent = child.spent.checked_add(spent).expect("source child bill fits");
            }
            session::FactKind::Ended { turns, .. } => child.turns = turns,
            session::FactKind::CompactionStarted { .. }
            | session::FactKind::CompactionEnded { .. }
            | session::FactKind::Opened { .. }
            | session::FactKind::CompletionStarted { .. }
            | session::FactKind::CompletionAnswered { .. }
            | session::FactKind::CompletionFailed { .. }
            | session::FactKind::CompletionCancelled { .. }
            | session::FactKind::CompletionRetried { .. }
            | session::FactKind::JobEnded { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::Tools { .. }
            | session::FactKind::BuiltinStarted { .. }
            | session::FactKind::BuiltinAnswered { .. }
            | session::FactKind::DelegateStarted { .. }
            | session::FactKind::DelegateAnswered { .. }
            | session::FactKind::DelegateCancelled { .. }
            | session::FactKind::DelegateWithheld { .. } => unreachable!("selected scalar child facts"),
        }
    }

    pub(crate) fn ended(&mut self, owner: Token, end: run::facts::EndKind, at: Time) {
        if let Some(child) = self.children.get_mut(&owner) {
            assert!(!child.ended, "one actual settled root terminal");
            child.ended = true;
            child.time_ms = at.saturating_since(child.opened).as_nanos() / 1_000_000;
            child.stop = match end {
                run::facts::EndKind::Closed => smith_events::ChildStop::Closed,
                run::facts::EndKind::PriceOverflow
                | run::facts::EndKind::UsageOverflow
                | run::facts::EndKind::Receiving(_)
                | run::facts::EndKind::TranscriptRefused { .. }
                | run::facts::EndKind::Busy
                | run::facts::EndKind::Invalid
                | run::facts::EndKind::Fault
                | run::facts::EndKind::Budget(_) => smith_events::ChildStop::Failed,
            };
        }
    }

    pub(crate) fn returned(&mut self, call: Token, result: run::facts::Return) {
        let owner = match self.owner(call) {
            Some(owner) => owner,
            None => return,
        };
        let child = self.children.get_mut(&owner).expect("selected child");
        assert!(child.ended, "the root returns only after the actual child settled");
        let used = match result {
            run::facts::Return::Answered { used, .. } => {
                child.stop = if child.budget { smith_events::ChildStop::Budget } else { smith_events::ChildStop::End };
                Some(used)
            }
            run::facts::Return::Unanswered { used } => Some(used),
            run::facts::Return::Cancelled | run::facts::Return::TimedOut => None,
            run::facts::Return::Crossed
            | run::facts::Return::Waited
            | run::facts::Return::Withheld
            | run::facts::Return::HostAnswered
            | run::facts::Return::HostTooLarge
            | run::facts::Return::HostUnknown
            | run::facts::Return::HostRejected
            | run::facts::Return::Delivered
            | run::facts::Return::Nothing
            | run::facts::Return::DeliveryRefused
            | run::facts::Return::Accepted
            | run::facts::Return::Rejected
            | run::facts::Return::ChecksFailed
            | run::facts::Return::Stale
            | run::facts::Return::DeliveryFailed
            | run::facts::Return::Busy
            | run::facts::Return::Refused => unreachable!("an actual child has a child terminal"),
        };
        if let Some(used) = used {
            assert_eq!(used.turns, child.turns, "actual session totals join settled root usage");
            assert_eq!(used.spend, child.spent, "child own charges join root own spend");
            assert_eq!(
                used.time.as_nanos() / 1_000_000,
                child.time_ms,
                "the actual root interval matches the opening and settled end"
            );
        }
    }

    pub(crate) fn take(&mut self, call: Token) -> Option<smith_events::ChildCompleted> {
        let owner = self.owner(call)?;
        let child = self.children.remove(&owner).expect("selected child");
        assert!(child.ended, "an enclosing terminal waits for the actual child terminal");
        Some(smith_events::ChildCompleted {
            conversation: child.number,
            stop: child.stop,
            turns: u64::from(child.turns),
            time_ms: child.time_ms,
            spent: child.spent,
            usage: child.usage.usage(),
        })
    }

    fn owner(&self, call: Token) -> Option<Token> {
        for (owner, child) in &self.children {
            if child.call == call {
                return Some(*owner);
            }
        }
        None
    }
}
