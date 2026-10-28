//! The run's finishing phase and deferred wind-down effects (domain/run.md,
//! sections 6 and 9.1). This keeps the cause, final requests made,
//! and bounded ready runs. It never knows provider syntax or child results.

use skein_lib::{Id, Set};

use crate::Exhausted;
use crate::run::Run;

/// Why a run begins its existing finishing allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    /// Ordinary budget admission exhausted this part.
    Budget(Exhausted),
    /// An admitted host message asked an Open main to reply and park.
    Host,
}

impl Cause {
    pub(crate) const fn exhausted(self) -> Option<Exhausted> {
        match self {
            Cause::Budget(exhausted) => Some(exhausted),
            Cause::Host => None,
        }
    }
}

/// A run finishing after ordinary exhaustion or an admitted host request.
#[derive(Debug)]
pub(crate) enum WindDown {
    /// Child terminals settle before main's final completion.
    Settling { children: u32, cause: Cause },
    /// Main requests at most the reserve's turns; its first gate carries a note.
    Finishing { used: u32, noted: bool, cause: Cause },
}

impl WindDown {
    pub(crate) const fn new(children: u32, cause: Cause) -> WindDown {
        if children == 0 {
            WindDown::Finishing { used: 0, noted: false, cause }
        } else {
            WindDown::Settling { children, cause }
        }
    }

    pub(crate) const fn cause(&self) -> Cause {
        match self {
            WindDown::Settling { cause, .. } | WindDown::Finishing { cause, .. } => *cause,
        }
    }

    pub(crate) fn child_ended(&mut self) -> bool {
        match self {
            WindDown::Settling { children, cause } => {
                *children = children.checked_sub(1).expect("one terminal per retained child");
                if *children == 0 {
                    *self = WindDown::Finishing { used: 0, noted: false, cause: *cause };
                    true
                } else {
                    false
                }
            }
            WindDown::Finishing { .. } => false,
        }
    }
}

/// Runs whose child closures or main yield wait until the next reclaim point.
#[derive(Debug)]
pub(crate) struct Ready {
    now: Set<Id<Run>>,
    next: Set<Id<Run>>,
}

impl Ready {
    pub(crate) const fn with_capacity(runs: u32) -> Ready {
        Ready { now: Set::with_capacity(runs), next: Set::with_capacity(runs) }
    }

    pub(crate) fn is_ready(&self) -> bool {
        !self.now.is_empty()
    }

    pub(crate) fn pop(&mut self) -> Option<Id<Run>> {
        self.now.pop_first()
    }

    pub(crate) fn keep(&mut self, run: Id<Run>) {
        self.next.insert(run).expect("one ready entry per run");
    }

    pub(crate) fn remove(&mut self, run: Id<Run>) {
        self.now.remove(&run);
        self.next.remove(&run);
    }

    pub(crate) fn promote(&mut self) {
        for _ in 0..self.next.capacity() {
            let Some(run) = self.next.pop_first() else {
                break;
            };
            self.now.insert(run).expect("one ready entry per run");
        }
    }
}
