//! Deferred stdout bytes (protocol/events.md 3.5; programs.md 5.5).
//! Keeps at most one 4096-byte read. Parsing pauses for the service's measure
//! drain and the domain's next Complete, so content retains its response owner.

use super::{Backend, Below, Phase, stop};
use skein_lib::{Env, List, Queue};
use smith_protocol_llm::{self as protocol, ToDomain};

impl Backend {
    pub(super) fn consume_stdout(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        self.intake_due = None;
        if self.measures.is_empty() {
            self.deliver(env, up, below);
        }
        let before = self.intake_at;
        for _ in 0..self.intake.len() {
            if self.native.is_some() || !self.measures.is_empty() {
                break;
            }
            if self.phase == Phase::Running && self.owner.is_none() && self.stop == stop::Stop::None {
                break;
            }
            let Some(byte) = self.intake.get(self.intake_at).copied() else { break };
            self.intake_at = self.intake_at.checked_add(1).expect("4096-byte read");
            match self.phase {
                Phase::Starting | Phase::Running | Phase::Ending => {
                    if self.phase == Phase::Ending && self.stop == stop::Stop::None {
                        continue;
                    }
                    if byte == b'\n' {
                        let line = core::mem::replace(&mut self.line, List::with_capacity(self.settings.line_bytes));
                        if self.phase == Phase::Starting {
                            self.admit_init(env, line.as_slice(), up, below);
                        } else {
                            self.line(env, line.as_slice(), up, below);
                        }
                    } else if self.line.push(byte).is_err() {
                        self.fail(
                            env,
                            smith_domain::llm::Failure::Program { exit: self.exit },
                            b"smith: the program stream exceeds its line bound",
                            up,
                            below,
                        );
                    }
                }
                Phase::Idle | Phase::Checking | Phase::Ended => {}
            }
        }
        if self.intake_at > before && !self.line.is_empty() {
            self.completion_deadline(env, false);
        }
        if self.intake_at == self.intake.len() {
            self.intake = List::with_capacity(4096);
            self.intake_at = 0;
        } else if self.native.is_none()
            && (self.owner.is_some() || self.stop != stop::Stop::None || self.phase == Phase::Ending)
        {
            self.intake_due = Some(env.now);
        }
        if self.native.is_none() && self.owner.is_some() && !self.turns.ready.is_empty() {
            self.intake_due = Some(env.now);
        }
        self.read_stdout();
    }
}
