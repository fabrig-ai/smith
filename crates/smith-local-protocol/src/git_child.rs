//! A local git operation carried by host-owned children (protocol/hosts.md,
//! section 5.5). This adapter keeps git's typed command sequence, two bounded
//! output captures and child/pipe tokens until IO proves them closed. It
//! never decides delivery policy. `new`, `from_io` and `fire` translate one
//! git request into one local Git terminal. Plain children remain the
//! containment limit decided in the plan's QUESTIONS.md, item 1.
//!
//! | Phase | Input | Effect |
//! |---|---|---|
//! | Spawning | Spawned | demand stdout and stderr |
//! | Running | bytes | retain bounded output, demand more |
//! | Running | exit or pipe end | wait for child and pipe closures |
//! | Active | deadline | kill child and abort pipes, await closure |
//! | Closed | next git action | spawn next command or emit terminal |

use alloc::boxed::Box;
use skein_io::{self as io, kernel};
use skein_lib::{List, Queue, Time, Token, stream};
use smith_local_domain as local;

use crate::{Git, GitAction, GitCompletion, GitLimits};

const GIT_READ: u32 = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Life {
    Active,
    Closed,
    Done,
}

/// One host-owned git operation and its current child resources.
#[derive(Debug)]
pub struct GitChild {
    owner: Token,
    io_owner: Token,
    root: kernel::Fd,
    environment: Box<[Box<[u8]>]>,
    deadline: Time,
    limits: GitLimits,
    git: Git,
    child: Option<Token>,
    pipes: [Option<Token>; 2],
    stdout: List<u8>,
    stderr: List<u8>,
    tail_next: u32,
    stderr_total: u64,
    exit: Option<kernel::Exit>,
    life: Life,
    timed_out: bool,
    failed: bool,
}

impl GitChild {
    /// Begin one git command sequence; markers are handled by the file adapter.
    #[expect(clippy::too_many_arguments, reason = "the owned git request and launch binding cross together")]
    pub fn new(
        owner: Token,
        io_owner: Token,
        root: kernel::Fd,
        environment: Box<[Box<[u8]>]>,
        op: local::GitOp,
        deadline: Time,
        limits: GitLimits,
        below: &mut Queue<io::Request>,
    ) -> Option<Self> {
        if environment.len() > 64 {
            return None;
        }
        let mut environment_bytes = 0_usize;
        for entry in &environment {
            environment_bytes = environment_bytes.checked_add(entry.len())?;
            if !entry.contains(&b'=') || entry.contains(&0) || environment_bytes > 4096 {
                return None;
            }
        }
        let (git, action) = Git::new(op, limits)?;
        let mut child = Self {
            owner,
            io_owner,
            root,
            environment,
            deadline,
            limits,
            git,
            child: None,
            pipes: [None, None],
            stdout: List::with_capacity(limits.output_bytes.checked_add(1)?),
            stderr: List::with_capacity(limits.detail_bytes),
            tail_next: 0,
            stderr_total: 0,
            exit: None,
            life: Life::Active,
            timed_out: false,
            failed: false,
        };
        match action {
            GitAction::Command { args } => child.spawn(args, below),
            GitAction::Markers { .. } | GitAction::Done(_) => return None,
        }
        Some(child)
    }

    /// Whether this IO owner or output pipe belongs to the current command.
    #[must_use]
    pub fn owns(&self, owner: Token) -> bool {
        owner == self.io_owner || self.pipes.contains(&Some(owner))
    }

    /// Retain child progress, and emit a domain terminal only after all resources close.
    pub fn from_io(&mut self, event: io::Event, above: &mut Queue<local::Event>, below: &mut Queue<io::Request>) {
        match event {
            io::Event::Spawned { child, pipes, .. } => {
                self.child = Some(child);
                assert_eq!(pipes.len(), 2, "git asks for stdout and stderr");
                for (index, pipe) in pipes.iter().enumerate() {
                    *self.pipes.get_mut(index).expect("two pipe slots") = Some(*pipe);
                    below.push(io::Request::Stream {
                        stream: *pipe,
                        down: stream::Down::Demand { read: stream::Read::Arrived { max: GIT_READ }, room: 0 },
                    });
                }
                if self.timed_out {
                    self.stop(below);
                }
            }
            io::Event::Stream { owner, up } => match up {
                stream::Up::Bytes(bytes) => {
                    if self.pipes.first().copied().expect("stdout slot") == Some(owner) {
                        for byte in bytes {
                            if self.stdout.room() > 0 {
                                self.stdout.push(byte).expect("stdout room");
                            }
                        }
                    } else {
                        for byte in bytes {
                            self.stderr_total = self.stderr_total.saturating_add(1);
                            if self.stderr.room() > 0 {
                                self.stderr.push(byte).expect("stderr room");
                            } else if self.stderr.capacity() > 0 {
                                *self.stderr.get_mut(self.tail_next).expect("tail slot") = byte;
                                self.tail_next = self
                                    .tail_next
                                    .checked_add(1)
                                    .expect("tail index")
                                    .checked_rem(self.stderr.capacity())
                                    .expect("nonzero tail capacity");
                            }
                        }
                    }
                    below.push(io::Request::Stream {
                        stream: owner,
                        down: stream::Down::Demand { read: stream::Read::Arrived { max: GIT_READ }, room: 0 },
                    });
                }
                stream::Up::End => below.push(io::Request::Close { entity: owner }),
                stream::Up::Failed(_) => {
                    self.failed = true;
                    below.push(io::Request::Abort { entity: owner });
                }
                stream::Up::Room => unreachable!("git's output pipes ask for no write room"),
            },
            io::Event::Exited { exit, .. } => self.exit = Some(exit),
            io::Event::Failed { .. } => {
                self.failed = true;
                self.stop(below);
            }
            io::Event::Closed { owner } => {
                if owner == self.io_owner {
                    self.life = Life::Closed;
                } else {
                    for pipe in &mut self.pipes {
                        if *pipe == Some(owner) {
                            *pipe = None;
                        }
                    }
                }
            }
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Output { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. }
            | io::Event::Usage { .. } => unreachable!("only child and pipe events route to git"),
        }
        self.advance(above, below);
    }

    /// Stop an overdue command; its output pipes and child must still settle.
    pub fn fire(&mut self, now: Time, below: &mut Queue<io::Request>) {
        if self.life != Life::Done && !self.timed_out && now >= self.deadline {
            self.timed_out = true;
            self.stop(below);
        }
    }

    #[must_use]
    pub fn done(&self) -> bool {
        self.life == Life::Done
    }

    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        if self.life == Life::Done || self.timed_out { None } else { Some(self.deadline) }
    }

    fn spawn(&mut self, args: Box<[Box<[u8]>]>, below: &mut Queue<io::Request>) {
        below.push(io::Request::Spawn {
            owner: self.io_owner,
            spawn: kernel::Spawn {
                program: Box::from(&b"/usr/bin/git"[..]),
                args,
                env: self.environment.clone(),
                root: self.root,
                dir: Box::from(&b"."[..]),
                pipes: Box::new([
                    kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None },
                    kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None },
                ]),
            },
        });
    }

    fn stop(&self, below: &mut Queue<io::Request>) {
        if let Some(child) = self.child {
            below.push(io::Request::Signal { child, to: kernel::Target::Child, signal: kernel::Signal::Kill });
        }
        for pipe in self.pipes.into_iter().flatten() {
            below.push(io::Request::Abort { entity: pipe });
        }
    }

    fn advance(&mut self, above: &mut Queue<local::Event>, below: &mut Queue<io::Request>) {
        if self.life != Life::Closed {
            return;
        }
        for pipe in &self.pipes {
            if pipe.is_some() {
                return;
            }
        }
        let stdout = core::mem::replace(
            &mut self.stdout,
            List::with_capacity(self.limits.output_bytes.checked_add(1).expect("stdout bound")),
        )
        .into_boxed();
        let mut stderr = List::with_capacity(self.stderr.len());
        for index in 0..self.stderr.len() {
            let at = if self.stderr.len() == self.stderr.capacity() {
                self.tail_next
                    .checked_add(index)
                    .expect("tail index")
                    .checked_rem(self.stderr.len())
                    .expect("nonzero tail length")
            } else {
                index
            };
            stderr.push(*self.stderr.get(at).expect("tail slot")).expect("tail room");
        }
        let stderr_cut = self.stderr_total.saturating_sub(u64::from(self.stderr.len()));
        self.stderr.clear();
        self.tail_next = 0;
        self.stderr_total = 0;
        let code = if self.failed {
            None
        } else {
            match self.exit {
                Some(kernel::Exit::Code(code)) => Some(code),
                Some(kernel::Exit::Signal(_)) | None => None,
            }
        };
        self.child = None;
        self.life = Life::Active;
        self.exit = None;
        match self.git.complete(GitCompletion::Command {
            code,
            stdout,
            stderr: stderr.into_boxed(),
            timed_out: self.timed_out,
        }) {
            GitAction::Command { args } => self.spawn(args, below),
            GitAction::Done(result) => {
                self.life = Life::Done;
                above.push(local::Event::Git { owner: self.owner, result: diagnostic_cut(result, stderr_cut) });
            }
            GitAction::Markers { .. } => unreachable!("marker operations never spawn git"),
        }
    }
}

fn diagnostic_cut(result: local::GitResult, cut: u64) -> local::GitResult {
    match result {
        local::GitResult::Failed { reason, diagnostic } => local::GitResult::Failed {
            reason,
            diagnostic: Box::new(smith_domain::run::Diagnostic::new(
                diagnostic.output(),
                diagnostic.cut().saturating_add(cut),
            )),
        },
        local::GitResult::NoEffect { reason, diagnostic } => local::GitResult::NoEffect {
            reason,
            diagnostic: Box::new(smith_domain::run::Diagnostic::new(
                diagnostic.output(),
                diagnostic.cut().saturating_add(cut),
            )),
        },
        local::GitResult::Uncertain { reason, diagnostic } => local::GitResult::Uncertain {
            reason,
            diagnostic: Box::new(smith_domain::run::Diagnostic::new(
                diagnostic.output(),
                diagnostic.cut().saturating_add(cut),
            )),
        },
        result @ (local::GitResult::Head { .. }
        | local::GitResult::Status { .. }
        | local::GitResult::Inspected { .. }
        | local::GitResult::Markers { .. }
        | local::GitResult::Committed { .. }
        | local::GitResult::Pushed
        | local::GitResult::Stale) => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OWNER: Token = Token::new(1);
    const IO_OWNER: Token = Token::new(1 << 62);
    const CHILD: Token = Token::new(7);
    const OUT: Token = Token::new(8);
    const ERR: Token = Token::new(9);
    const HASH: &[u8] = b"0123456789abcdef0123456789abcdef01234567\n";

    fn limits() -> GitLimits {
        GitLimits { argument_bytes: 4096, output_bytes: 64, conflicts: 2, path_bytes: 64, detail_bytes: 8 }
    }

    fn make(op: local::GitOp) -> (GitChild, Queue<io::Request>, Queue<local::Event>) {
        let mut below = Queue::with_capacity(32);
        let child = GitChild::new(
            OWNER,
            IO_OWNER,
            kernel::Fd::new(3),
            Box::new([]),
            op,
            Time::from_nanos(100),
            limits(),
            &mut below,
        )
        .expect("bounded git child");
        (child, below, Queue::with_capacity(8))
    }

    fn finish(
        child: &mut GitChild,
        stdout: &[u8],
        stderr: &[u8],
        code: u8,
        above: &mut Queue<local::Event>,
        below: &mut Queue<io::Request>,
    ) {
        child.from_io(io::Event::Spawned { owner: IO_OWNER, child: CHILD, pipes: Box::new([OUT, ERR]) }, above, below);
        if !stdout.is_empty() {
            child.from_io(io::Event::Stream { owner: OUT, up: stream::Up::Bytes(stdout.into()) }, above, below);
        }
        if !stderr.is_empty() {
            child.from_io(io::Event::Stream { owner: ERR, up: stream::Up::Bytes(stderr.into()) }, above, below);
        }
        child.from_io(io::Event::Exited { owner: IO_OWNER, exit: kernel::Exit::Code(code) }, above, below);
        assert!(above.is_empty(), "exit is not resource settlement");
        for pipe in [OUT, ERR] {
            child.from_io(io::Event::Stream { owner: pipe, up: stream::Up::End }, above, below);
            child.from_io(io::Event::Closed { owner: pipe }, above, below);
        }
        assert!(above.is_empty(), "pipe closure is not child closure");
        child.from_io(io::Event::Closed { owner: IO_OWNER }, above, below);
    }

    #[test]
    fn a_git_commit_advances_only_after_each_child_and_both_pipes_close() {
        let (mut child, mut below, mut above) = make(local::GitOp::Commit { message: b"Title".as_slice().into() });
        let Some(io::Request::Spawn { spawn, .. }) = below.pop() else { panic!("add child") };
        assert_eq!(spawn.args[0].as_ref(), b"add");
        finish(&mut child, b"", b"", 0, &mut above, &mut below);
        let mut commit = false;
        while let Some(request) = below.pop() {
            if let io::Request::Spawn { spawn, .. } = request {
                commit = spawn.args[2].as_ref() == b"commit";
            }
        }
        assert!(commit);
        finish(&mut child, b"", b"", 0, &mut above, &mut below);
        while below.pop().is_some() {}
        finish(&mut child, HASH, b"", 0, &mut above, &mut below);
        let Some(local::Event::Git { owner, result: local::GitResult::Committed { head, .. } }) = above.pop() else {
            panic!("committed terminal")
        };
        assert_eq!(owner, OWNER);
        assert_eq!(head.as_ref(), &HASH[..HASH.len() - 1]);
        assert!(child.done());
    }

    #[test]
    fn git_output_past_its_bound_is_failed_after_resource_settlement() {
        let (mut child, mut below, mut above) = make(local::GitOp::Head);
        below.pop().expect("spawn");
        finish(&mut child, &[b'x'; 128], b"0123456789ABCDEF", 0, &mut above, &mut below);
        let Some(local::Event::Git { result: local::GitResult::Failed { reason, diagnostic }, .. }) = above.pop()
        else {
            panic!("oversized terminal")
        };
        assert_eq!(reason, smith_domain::run::DeliveryReason::TooLarge);
        assert_eq!(diagnostic.output(), b"89ABCDEF");
        assert_eq!(diagnostic.cut(), 8);
    }

    #[test]
    fn a_git_deadline_kills_the_child_and_waits_for_every_closed_event() {
        let (mut child, mut below, mut above) = make(local::GitOp::Head);
        below.pop().expect("spawn");
        child.from_io(
            io::Event::Spawned { owner: IO_OWNER, child: CHILD, pipes: Box::new([OUT, ERR]) },
            &mut above,
            &mut below,
        );
        while below.pop().is_some() {}
        child.fire(Time::from_nanos(100), &mut below);
        let Some(io::Request::Signal { child: CHILD, to: kernel::Target::Child, signal: kernel::Signal::Kill }) =
            below.pop()
        else {
            panic!("deadline signals git child");
        };
        assert!(above.is_empty());
        for owner in [OUT, ERR, IO_OWNER] {
            child.from_io(io::Event::Closed { owner }, &mut above, &mut below);
        }
        let Some(local::Event::Git { result: local::GitResult::Failed { reason, .. }, .. }) = above.pop() else {
            panic!("timeout terminal")
        };
        assert_eq!(reason, smith_domain::run::DeliveryReason::TimedOut);
        assert!(child.done());
    }
}
