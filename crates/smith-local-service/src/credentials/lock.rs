//! Record directory locks for shared writes (shell.md, 7.3; oauth.md, 6.4).
//! Each successful lock answer owns one root until Unlock closes it.
use super::Accounts;
use alloc::boxed::Box;
use skein_io::{file, file_layer};
use skein_lib::{Duration, Time, Token, Writer};

mod events;

// Retry contention without occupying the loop between attempts.
const RETRY: Duration = Duration::from_millis(10);

pub(super) const LOCK_FILE: u64 = 1 << 62;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Idle,
    Open,
    Opening,
    Take,
    Taking,
    Waiting { retry: Time },
    Held,
    Close,
    Closing,
}

pub(super) struct Lock {
    number: u32,
    base: Token,
    path: Box<[u8]>,
    root: Option<Token>,
    state: State,
    again: bool,
    until: Option<Time>,
}

impl Lock {
    pub(super) fn new(number: u32, base: Token, directory: &[u8], file: &[u8]) -> Self {
        let length = directory.len().checked_add(file.len()).expect("bounded lock path");
        let length = length.checked_add(6).expect("bounded lock suffix");
        let mut path = Writer::new(length);
        for bytes in [directory, b"/", file, b".lock"] {
            path.put(bytes).expect("measured lock path");
        }
        Self { number, base, path: path.finish(), root: None, state: State::Idle, again: false, until: None }
    }
}

impl Accounts {
    pub(super) fn lock_deadline(&self) -> Option<Time> {
        let mut deadline = None;
        for lock in &self.locks {
            match lock.state {
                State::Waiting { retry } => {
                    deadline = Some(match deadline {
                        Some(prior) => retry.min(prior),
                        None => retry,
                    });
                }
                State::Idle
                | State::Open
                | State::Opening
                | State::Take
                | State::Taking
                | State::Held
                | State::Close
                | State::Closing => {}
            }
        }
        deadline
    }

    pub(super) fn lock_held(&self, number: u32) -> bool {
        for lock in &self.locks {
            if lock.number == number && lock.state == State::Held {
                return true;
            }
        }
        false
    }

    pub(super) fn take_lock(&mut self, index: u32, now: Time) {
        let number = self.entries.get(index).expect("account index").number;
        for index in 0..self.locks.len() {
            let lock = self.locks.get_mut(index).expect("lock index");
            if lock.number == number {
                match lock.state {
                    State::Idle => {
                        lock.until = Some(now.saturating_add(self.limits.lock_wait));
                        lock.state = State::Open;
                    }
                    State::Close | State::Closing => {
                        lock.again = true;
                        lock.until = Some(now.saturating_add(self.limits.lock_wait));
                    }
                    State::Open
                    | State::Opening
                    | State::Take
                    | State::Taking
                    | State::Waiting { .. }
                    | State::Held => {
                        unreachable!("one lock per account write")
                    }
                }
            }
        }
    }

    pub(super) fn release_lock(&mut self, index: u32) {
        let number = self.entries.get(index).expect("account index").number;
        for index in 0..self.locks.len() {
            let lock = self.locks.get_mut(index).expect("lock index");
            if lock.number == number && lock.root.is_some() {
                lock.until = None;
                lock.state = State::Close;
            }
        }
    }

    pub(super) fn lock_pending(&self, now: Time) -> bool {
        for lock in &self.locks {
            match lock.state {
                State::Open | State::Take | State::Close => return true,
                State::Waiting { retry } if retry <= now => return true,
                State::Waiting { .. } | State::Idle | State::Opening | State::Taking | State::Held | State::Closing => {
                }
            }
        }
        false
    }

    pub(super) fn lock_progress(&mut self, now: Time) {
        if !self.files.takes() || self.events.room() < 3 || self.submissions.room() < 2 {
            return;
        }
        for index in 0..self.locks.len() {
            let lock = self.locks.get_mut(index).expect("lock index");
            if self.closed && lock.root.is_some() && lock.state == State::Held {
                lock.state = State::Close;
            }
            if self.closed && lock.state == State::Open {
                lock.state = State::Idle;
            }
            match lock.state {
                State::Waiting { retry } if retry <= now => lock.state = State::Take,
                State::Idle
                | State::Open
                | State::Opening
                | State::Take
                | State::Taking
                | State::Waiting { .. }
                | State::Held
                | State::Close
                | State::Closing => {}
            }
            let owner = Token::new(LOCK_FILE | u64::from(index));
            let request = match lock.state {
                State::Open => {
                    lock.state = State::Opening;
                    file::Request::OpenPrivate { owner, root: lock.base, path: lock.path.clone() }
                }
                State::Take => {
                    lock.state = State::Taking;
                    file::Request::Lock { owner, root: lock.root.expect("opened lock root") }
                }
                State::Close => {
                    lock.state = State::Closing;
                    file::Request::Close { owner, file: lock.root.take().expect("lock root closes once") }
                }
                State::Idle | State::Opening | State::Taking | State::Waiting { .. } | State::Held | State::Closing => {
                    continue;
                }
            };
            file_layer::down(&mut self.files, now, request, &mut self.events, &mut self.submissions);
            return;
        }
    }
}
