//! The account driver's socket pass and operation namespace.
//! File operations keep the low namespace; socket cancellations retain theirs.
use super::{Accounts, oauth};
use skein_io::{self as io, kernel};
use skein_lib::{Env, Time, Token, Wall};

const SOCKET_OPERATION: u64 = 1 << 61;

impl Accounts {
    pub(super) fn socket_completion(&mut self, complete: kernel::Complete) -> Option<kernel::Complete> {
        if complete.op.raw() & SOCKET_OPERATION == 0 {
            return Some(complete);
        }
        self.socket_completions.push(kernel::Complete {
            op: Token::new(complete.op.raw() & !SOCKET_OPERATION),
            kind: complete.kind,
            result: complete.result,
        });
        None
    }

    pub(super) fn socket_up(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits.io };
        if self.socket_events.room() >= io::MAX_OUT_RESUME.events
            && self.socket_submissions.room() >= io::MAX_OUT_RESUME.submissions
            && self.sockets.is_ready()
        {
            io::resume(&mut self.sockets, &env, &mut self.socket_events, &mut self.socket_submissions);
        }
        if self.socket_events.room() >= io::MAX_OUT_UP.events
            && self.socket_submissions.room() >= io::MAX_OUT_UP.submissions
            && let Some(complete) = self.socket_completions.pop()
        {
            io::up(&mut self.sockets, &env, complete, &mut self.socket_events, &mut self.socket_submissions);
        }
        if self.socket_events.room() >= io::MAX_OUT_FIRE.events
            && self.socket_submissions.room() >= io::MAX_OUT_FIRE.submissions
            && self.sockets.is_due(now)
        {
            io::fire(&mut self.sockets, &env, &mut self.socket_events, &mut self.socket_submissions);
        }
        if self.out.room() >= oauth::MAX_OUT_UP.above
            && self.socket_requests.room() >= oauth::MAX_OUT_UP.io
            && let Some(event) = self.socket_events.pop()
        {
            let env = Env { now, wall, limits: self.limits };
            self.driver.up(&env, event, &mut self.out, &mut self.socket_requests, &mut self.file_requests);
        }
    }

    pub(super) fn socket_down(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits.io };
        if self.sockets.takes()
            && self.socket_submissions.room() >= io::MAX_OUT_DOWN.submissions
            && let Some(request) = self.socket_requests.pop()
        {
            io::down(&mut self.sockets, &env, request, &mut self.socket_submissions);
        }
        for _ in 0..self.socket_submissions.capacity() {
            if self.submissions.room() == 0 {
                break;
            }
            let Some(submit) = self.socket_submissions.pop() else { break };
            self.submissions.push(socket_submit(submit));
        }
        self.sockets.reclaim();
        self.driver.reclaim();
    }

    pub(super) fn socket_pending(&self, now: Time) -> bool {
        self.sockets.is_ready()
            || self.sockets.is_due(now)
            || !self.socket_completions.is_empty()
            || !self.socket_submissions.is_empty()
            || !self.socket_events.is_empty()
            || !self.socket_requests.is_empty()
    }

    pub(super) fn socket_empty(&self) -> bool {
        self.sockets.is_empty()
            && self.socket_completions.is_empty()
            && self.socket_submissions.is_empty()
            && self.socket_events.is_empty()
            && self.socket_requests.is_empty()
    }
}

#[expect(clippy::wildcard_enum_match_arm, reason = "only an external kernel Cancel names another operation")]
fn socket_submit(submit: kernel::Submit) -> kernel::Submit {
    let kind = match submit.kind {
        kernel::Op::Cancel { target } if target.raw() != u64::MAX => {
            kernel::Op::Cancel { target: Token::new(target.raw() | SOCKET_OPERATION) }
        }
        other => other,
    };
    assert!(submit.op.raw() & SOCKET_OPERATION == 0, "account socket operation space");
    kernel::Submit { op: Token::new(submit.op.raw() | SOCKET_OPERATION), kind }
}
