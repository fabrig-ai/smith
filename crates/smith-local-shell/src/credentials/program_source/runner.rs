//! I/O routing for one source request (protocol/hosts.md, section 2).
use super::ROOT_CLOSE;
use skein_io::{self as io, kernel};
use skein_lib::{Env, Queue, Time, Wall};
use skein_shell::Host;
use smith_host_protocol::{ProgramSource, SourceAnswer};

pub(super) struct Runner {
    pub io: io::Io,
    pub limits: io::Limits,
    pub source: ProgramSource,
    pub answer: Option<SourceAnswer>,
    pub root: kernel::Fd,
    pub closing: bool,
    pub closed: bool,
    pub events: Queue<io::Event>,
    pub requests: Queue<io::Request>,
    pub completions: Queue<kernel::Complete>,
    pub submissions: Queue<kernel::Submit>,
    pub operations: u32,
}

impl Host for Runner {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits };
        while let Some(complete) = self.completions.pop() {
            if complete.op == ROOT_CLOSE {
                self.closed = true;
            } else {
                io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
            }
        }
        while self.io.is_ready() {
            io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while self.io.is_due(now) {
            io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(event) = self.events.pop() {
            let owner = match &event {
                io::Event::Spawned { owner, .. }
                | io::Event::Exited { owner, .. }
                | io::Event::Failed { owner, .. }
                | io::Event::Closed { owner }
                | io::Event::Stream { owner, .. } => *owner,
                io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Connecting { .. }
                | io::Event::Connected { .. }
                | io::Event::Output { .. }
                | io::Event::Usage { .. }
                | io::Event::Resized
                | io::Event::Window { .. }
                | io::Event::Shutdown { .. } => unreachable!("source events"),
            };
            assert!(self.source.owns(owner), "source owns routed event");
            self.source.from_io(event, &mut self.requests);
        }
        self.source.fire(now, &mut self.requests);
        if self.answer.is_none() {
            self.answer = self.source.answer();
        }
        self.io.reclaim();
        if self.answer.is_some() && !self.closing {
            self.closing = true;
            self.submissions.push(kernel::Submit { op: ROOT_CLOSE, kind: kernel::Op::Close { fd: self.root } });
        }
        while let Some(request) = self.requests.pop() {
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.completions.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || (self.answer.is_some() && !self.closing)
            || self.source.next_deadline().is_some_and(|at| at <= now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline().into_iter().chain(self.source.next_deadline()).min()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.source.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.closed
            && self.answer.is_some()
            && self.io.is_empty()
            && self.requests.is_empty()
            && self.events.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn operations(&self) -> u32 {
        self.operations
    }
}
