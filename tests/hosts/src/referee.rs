//! Independent host-process obligations from boundary observations
//! (protocol/hosts.md, section 7; domain/host.md, section 4).
//! Skein owns the judgment, deadlines and replay machinery.

extern crate alloc;

use alloc::collections::BTreeMap;

use skein_lib::{Duration, Time, Token};
use skein_world::domain::{Expectations, Judge, Referee};

use crate::Observation;

/// One pending host-process obligation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Obligation {
    /// The run eventually releases its slot.
    Gone,
    /// One observed host call gets one terminal.
    Call(Token),
}

/// The host's answer, call and child-lifetime rules.
#[derive(Debug, Default)]
#[expect(clippy::struct_excessive_bools, reason = "each independent observation must be checked before slot release")]
pub struct Meeting {
    started: bool,
    answered: bool,
    exited: bool,
    reaped: bool,
    unspawned: bool,
    gone: bool,
    calls: BTreeMap<Token, bool>,
}

impl Expectations for Meeting {
    type Seen = Observation;
    type Name = Obligation;
    type Stimulus = ();

    fn observe(&mut self, seen: Observation, judge: &mut Judge<Self::Name, Self::Stimulus>) {
        match seen {
            Observation::Started => {
                judge.check(!self.started && !self.unspawned, "one successful Start per run");
                self.started = true;
                judge.expect(Obligation::Gone, Duration::from_secs(60));
            }
            Observation::Answered => {
                judge.check(self.started && !self.answered && !self.gone, "at most one answer per started run");
                self.answered = true;
            }
            Observation::Called(call) => {
                judge.check(self.started && !self.gone, "calls belong to a live started run");
                judge.check(self.calls.insert(call, false).is_none(), "one callback per host call");
                judge.expect(Obligation::Call(call), Duration::from_secs(60));
            }
            Observation::CallAnswered(call) => {
                let live = self.calls.get_mut(&call);
                judge.check(
                    live.as_ref().is_some_and(|answered| !**answered),
                    "each host call is answered exactly once",
                );
                if let Some(answered) = live {
                    *answered = true;
                    let met = judge.meet(&Obligation::Call(call));
                    judge.check(met, "the call terminal meets its obligation");
                }
            }
            Observation::Exited => {
                judge.check(!self.exited && !self.unspawned, "one child exit");
                self.exited = true;
            }
            Observation::Reaped => {
                judge.check(self.exited && !self.reaped, "reap follows child exit once");
                self.reaped = true;
            }
            Observation::Unspawned => {
                judge.check(!self.started && !self.unspawned, "an opening fails only before Start");
                self.unspawned = true;
                judge.expect(Obligation::Gone, Duration::from_secs(60));
            }
            Observation::Gone => {
                judge.check(
                    !self.gone && (self.unspawned || (self.exited && self.reaped)),
                    "slot release follows child teardown",
                );
                judge.check(
                    self.calls.values().all(|answered| *answered),
                    "every host call has one answer before release",
                );
                self.gone = true;
                let met = judge.meet(&Obligation::Gone);
                judge.check(met, "slot release meets its obligation");
            }
        }
    }
}

/// Replay the sample host's boundary events through Skein's referee.
#[must_use]
pub fn review(observations: &[Observation]) -> Referee<Meeting> {
    let mut referee = Referee::new(Meeting::default());
    let mut stimuli = Vec::new();
    for observation in observations {
        referee.observe(Time::ZERO, *observation, &mut stimuli);
    }
    referee
}

/// Compare the host's actual Gone tail with the shipped agent's diagnostic
/// writer (testing.md, section 2.1).
pub fn review_agent_errors(errors: &[u8], tail: &[u8], answered: bool) {
    assert_eq!(tail, errors, "Gone retains exactly the agent's written diagnostic bytes");
    let text = core::str::from_utf8(errors).expect("agent diagnostic UTF-8");
    assert!(
        text.starts_with("smith: commands run in process groups and are not contained\nsmith: agent started\n"),
        "the shipped adapter supplied startup"
    );
    if answered {
        assert!(!text.contains("could not answer"), "the answer and observed writer agree on success");
    }
    assert!(
        text.lines().all(|line| line == "smith: commands run in process groups and are not contained"
            || line == "smith: agent started"
            || line.starts_with("smith: the run could not answer: ")
            || line.starts_with("smith: run failed: ")
            || line.ends_with(" observations were dropped")),
        "only the shipped agent's words are observed"
    );
}
