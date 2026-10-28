//! Route and settle one check (protocol/hosts.md, section 2).
use super::{Checks, Queue, SourceAnswer, Time, Token, failure, io, local};

impl Checks {
    pub(crate) fn owns(&self, owner: Token) -> bool {
        match &self.active {
            Some(active) => active.source.owns(owner),
            None => false,
        }
    }

    pub(crate) fn receive(&mut self, event: io::Event, below: &mut Queue<io::Request>) {
        self.active.as_mut().expect("check owns event").source.from_io(event, below);
    }

    pub(crate) fn cancel(&mut self, below: &mut Queue<io::Request>) {
        self.interrupted = self.waiting.len();
        if let Some(active) = &mut self.active {
            active.cancelled = true;
            active.source.cancel(below);
        }
    }

    pub(crate) fn drain_interrupted(&mut self, events: &mut Queue<local::Event>, notify: bool) -> bool {
        for _ in 0..self.interrupted {
            let account = self.waiting.pop().expect("cancelled waiting prefix");
            self.interrupted = self.interrupted.checked_sub(1).expect("cancelled prefix remains");
            if notify {
                events
                    .push(local::Event::NoCredential { account, reason: local::CredentialFailure::ProgramInterrupted });
            }
        }
        true
    }

    pub(crate) fn advance(
        &mut self,
        now: Time,
        below: &mut Queue<io::Request>,
        events: &mut Queue<local::Event>,
        shell: &mut Queue<local::Request>,
        notify: bool,
    ) {
        if !self.drain_interrupted(events, notify) {
            return;
        }
        if shell.room() == 0 || below.room() < 4 {
            return;
        }
        if let Some(active) = &mut self.active {
            active.source.fire(now, below);
            match active.source.answer() {
                Some(SourceAnswer::Check(answer)) => {
                    let reason = if active.cancelled {
                        Some(local::CredentialFailure::ProgramInterrupted)
                    } else {
                        failure(answer)
                    };
                    if notify {
                        match reason {
                            Some(reason) => events.push(local::Event::NoCredential { account: active.account, reason }),
                            None => shell.push(local::Request::Credential { account: active.account }),
                        }
                    }
                    self.active = None;
                }
                Some(SourceAnswer::SignOut(_)) => unreachable!("a check answers Check"),
                None => {}
            }
        }
        for _ in 0..self.waiting.capacity() {
            if self.active.is_some() || shell.room() == 0 || below.room() == 0 {
                break;
            }
            match self.waiting.pop() {
                Some(account) => self.start(account, now, below, shell),
                None => break,
            }
        }
    }
}
