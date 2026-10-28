//! Workspace authority, file and process ownership (protocol/agent.md,
//! sections 2 and 3; domain/tools.md, sections 4–6).
//! The component keeps root tokens, in-flight domain owners and io pipe
//! bindings until their `Closed` events (programming-model.md, sections 4.2
//! and 5.2). `from_below` distinguishes child owners from pipe tokens by
//! event variant, never by their numeric values. It holds a
//! create's new content while it makes the file's directories. It never
//! knows descriptors, file content before io reports it, or the domain's
//! state.

use alloc::boxed::Box;

use skein_io as io;
use skein_lib::{Env, List, Map, Queue, Time, Token};
use smith_domain::{run, tools};

use crate::boundary::{Below, BelowEvent, FromDomain, ToDomain};
use crate::guide::{Guide, Step};
use crate::limits::Limits;
use crate::process::Process;

/// One host-admitted root and its immutable write authority.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Root {
    pub(crate) token: Token,
    pub(crate) writable: bool,
}

/// Which terminal the in-flight file request owes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// A bounded whole-file load.
    Load,
    /// A conditional synced file replacement.
    Store,
}

/// One file operation kept until its last io terminal, even after
/// cancellation.
#[derive(Debug)]
pub(crate) enum Pending {
    /// One request, whose terminal answers the operation.
    One { kind: Kind },
    /// A create still making its directories (domain/tools.md, section 3).
    Create(crate::files::Create),
    /// A removal retaining its confined parent until close.
    Remove(crate::removing::Removing),
}

impl Pending {
    /// The domain cancelled: a create sends no further request.
    fn cancel(&mut self) -> bool {
        match self {
            Pending::One { .. } => true,
            Pending::Create(create) => {
                create.cancel();
                true
            }
            Pending::Remove(removing) => removing.cancel(),
        }
    }
}

/// Workspace authority and in-flight requests; io keeps the filesystem.
#[derive(Debug)]
pub struct Component {
    roots: List<Root>,
    pending: Map<Token, Pending>,
    guides: Map<Token, Guide>,
    processes: Map<Token, Process>,
    routes: Map<Token, Token>,
    environment: crate::Environment,
    closing: bool,
}

impl Component {
    /// Construct with fixed capacity before any request is admitted.
    #[must_use]
    pub fn new(limits: &Limits, environment: crate::Environment) -> Component {
        assert!(environment.fits(limits), "configured environment fits its machine byte cap");
        Component {
            roots: List::with_capacity(limits.roots),
            pending: Map::with_capacity(limits.operations),
            guides: Map::with_capacity(limits.operations),
            processes: Map::with_capacity(limits.processes),
            routes: Map::with_capacity(limits.processes.checked_mul(2).expect("pipe route count fits")),
            environment,
            closing: false,
        }
    }

    /// Install the run's already-opened root tokens and write authority.
    /// A service adopts their descriptors into `FileIo` before calling this.
    pub fn workspace(&mut self, workspace: &run::Workspace) {
        assert!(
            self.pending.is_empty() && self.guides.is_empty() && self.processes.is_empty(),
            "workspace changes only between runs"
        );
        self.roots = List::with_capacity(self.roots.capacity());
        for directory in &workspace.directories {
            self.roots.push(Root { token: directory.root, writable: directory.writable }).expect("admitted root count");
        }
    }

    /// Translate one domain request; the caller reserves one cell per output.
    pub fn from_domain(
        &mut self,
        env: &Env<Limits>,
        request: FromDomain,
        to_domain: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        assert!(!self.closed(), "a closed machine accepts no further requests");
        if self.closing && starts(&request) {
            refuse(request, to_domain);
            return;
        }
        match request {
            FromDomain::Op { owner, op, deadline } => {
                self.admit_tool(env, owner, op, deadline, to_domain, below);
            }
            FromDomain::Cancel { owner } => {
                if let Some(pending) = self.pending.get_mut(&owner) {
                    if pending.cancel() {
                        below.push(Below::CancelFile { owner });
                    }
                } else if let Some(process) = self.processes.get_mut(&owner) {
                    process.cancel_job();
                    process.stop(env.now, env.limits.group_stop, crate::process::Why::Cancel, below);
                    self.advance(owner, env.now, to_domain, below);
                } else if let Some(guide) = self.guides.get(&owner)
                    && guide.cancellable()
                {
                    below.push(Below::CancelFile { owner });
                }
            }
            FromDomain::Job { owner, act } => {
                let (head, tail) = match act {
                    tools::JobAct::Read { head, tail, .. } | tools::JobAct::Stop { head, tail } => (head, tail),
                    tools::JobAct::Hasten => (0, 0),
                };
                if head > env.limits.output_bytes || tail > env.limits.output_bytes {
                    to_domain.push(ToDomain::Done { owner, done: other() });
                    return;
                }
                if let Some(process) = self.processes.get_mut(&owner) {
                    process.job(env.now, env.limits.group_stop, act, below);
                    self.advance(owner, env.now, to_domain, below);
                }
            }
            FromDomain::Read { owner, at, max, deadline } => {
                self.vacant(owner);
                if env.now >= deadline
                    || max > env.limits.file_bytes
                    || crate::files::check(&self.roots, &env.limits, at.root, &at.path, false).is_err()
                {
                    to_domain.push(ToDomain::Read { owner, read: run::Read::Failed });
                } else if self.guides.insert(owner, Guide::OpeningRead { max, deadline }).is_ok() {
                    let path = crate::files::io_path(at.path);
                    below.push(Below::OpenRead { owner, root: at.root, path, deadline });
                } else {
                    to_domain.push(ToDomain::Read { owner, read: run::Read::Failed });
                }
            }
            FromDomain::Probe { owner, at, deadline } => {
                self.vacant(owner);
                if env.now >= deadline
                    || crate::files::check(&self.roots, &env.limits, at.root, &at.path, false).is_err()
                {
                    to_domain.push(ToDomain::Probed { owner, executable: false });
                } else if self.guides.insert(owner, Guide::OpeningProbe { deadline }).is_ok() {
                    let path = crate::files::io_path(at.path);
                    below.push(Below::OpenRead { owner, root: at.root, path, deadline });
                } else {
                    to_domain.push(ToDomain::Probed { owner, executable: false });
                }
            }
            FromDomain::Check { owner, program, deadline, tail } => {
                self.vacant(owner);
                if env.now >= deadline {
                    to_domain.push(ToDomain::Checked {
                        owner,
                        ran: run::Ran { exit: run::Exit::TimedOut, output: Box::new([]), cut: 0 },
                    });
                    return;
                }
                let admissible = crate::files::check(&self.roots, &env.limits, program.root, &program.path, false);
                match admissible {
                    Ok(()) => {
                        match crate::process::check(owner, program, &self.environment, tail, deadline, &env.limits) {
                            Some((process, request)) => self.start_process(owner, process, request, below, to_domain),
                            None => failed_check(owner, to_domain),
                        }
                    }
                    Err(_) => failed_check(owner, to_domain),
                }
            }
            FromDomain::Abort { owner } => {
                if let Some(process) = self.processes.get_mut(&owner) {
                    process.stop(env.now, env.limits.group_stop, crate::process::Why::Cancel, below);
                }
            }
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the admitted tool vocabulary is exhaustively translated at its entrance"
    )]
    fn admit_tool(
        &mut self,
        env: &Env<Limits>,
        owner: Token,
        op: tools::Op,
        deadline: Time,
        to_domain: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        self.vacant(owner);
        if env.now >= deadline {
            to_domain.push(ToDomain::Done { owner, done: tools::Done::TimedOut });
            return;
        }
        match op {
            tools::Op::Load { .. } | tools::Op::Store { .. } => {
                match crate::files::request(&self.roots, &env.limits, owner, op, deadline) {
                    Ok((pending, request)) => match self.pending.insert(owner, pending) {
                        Ok(None) => below.push(Below::File { request, deadline }),
                        Ok(Some(_)) => unreachable!("owner checked"),
                        Err(_) => to_domain.push(ToDomain::Done { owner, done: other() }),
                    },
                    Err(done) => to_domain.push(ToDomain::Done { owner, done }),
                }
            }
            tools::Op::Remove { at, expect } => {
                if self.pending.len() == self.pending.capacity() {
                    to_domain.push(ToDomain::Done { owner, done: other() });
                    return;
                }
                if let Err(done) = crate::files::check(&self.roots, &env.limits, at.root, &at.path, true) {
                    to_domain.push(ToDomain::Done { owner, done });
                    return;
                }
                let (removing, request) =
                    crate::removing::Removing::start(owner, at, expect, env.limits.file_bytes, deadline);
                self.pending.insert(owner, Pending::Remove(removing)).expect("checked room");
                below.push(request);
            }
            tools::Op::List { at, depth, glob, max, max_bytes } => {
                if self.processes.len() == self.processes.capacity() {
                    to_domain.push(ToDomain::Done { owner, done: other() });
                    return;
                }
                let admissible = crate::files::check(&self.roots, &env.limits, at.root, &at.path, false);
                self.start_tool(
                    owner,
                    admissible,
                    crate::process::list(
                        owner,
                        at,
                        depth,
                        glob,
                        max,
                        max_bytes,
                        deadline,
                        &self.environment,
                        &env.limits,
                    ),
                    to_domain,
                    below,
                );
            }
            tools::Op::Spawn { cwd, command, head, tail, after } => {
                let admissible = crate::files::check(&self.roots, &env.limits, cwd.root, &cwd.path, false);
                self.start_tool(
                    owner,
                    admissible,
                    yielding(
                        crate::process::shell(
                            owner,
                            cwd,
                            command,
                            &self.environment,
                            head,
                            tail,
                            deadline,
                            &env.limits,
                        ),
                        env.now,
                        after,
                        head,
                        tail,
                        env.limits.output_bytes,
                    ),
                    to_domain,
                    below,
                );
            }
            tools::Op::Search { at, pattern, glob, hits, bytes } => {
                let admissible = crate::files::check(&self.roots, &env.limits, at.root, &at.path, false);
                self.start_tool(
                    owner,
                    admissible,
                    crate::process::search(
                        owner,
                        at,
                        pattern,
                        glob,
                        hits,
                        bytes,
                        deadline,
                        &self.environment,
                        &env.limits,
                    ),
                    to_domain,
                    below,
                );
            }
        }
    }

    fn vacant(&self, owner: Token) {
        assert!(
            !self.pending.contains_key(&owner)
                && !self.guides.contains_key(&owner)
                && !self.processes.contains_key(&owner),
            "one in-flight request per owner"
        );
    }

    fn start_tool(
        &mut self,
        owner: Token,
        admissible: Result<(), tools::Done>,
        request: Option<(Process, Below)>,
        to_domain: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        match admissible {
            Ok(()) => match request {
                Some((process, request)) => self.start_process(owner, process, request, below, to_domain),
                None => to_domain.push(ToDomain::Done { owner, done: other() }),
            },
            Err(done) => to_domain.push(ToDomain::Done { owner, done }),
        }
    }

    fn start_process(
        &mut self,
        owner: Token,
        mut process: Process,
        request: Below,
        below: &mut Queue<Below>,
        to_domain: &mut Queue<ToDomain>,
    ) {
        let purpose = process.purpose;
        let request = match purpose {
            crate::process::Purpose::List => match request {
                Below::Spawn { owner, root, spawn } => process.open_directory(owner, root, spawn),
                Below::File { .. }
                | Below::Remove { .. }
                | Below::OpenRead { .. }
                | Below::OpenDirectory { .. }
                | Below::CancelFile { .. }
                | Below::Process(_) => unreachable!("listing prepares one spawn"),
            },
            crate::process::Purpose::Shell | crate::process::Purpose::Check | crate::process::Purpose::Search => {
                request
            }
        };
        match self.processes.insert(owner, process) {
            Ok(None) => below.push(request),
            Ok(Some(_)) => unreachable!("owner checked"),
            Err(_) => match request {
                Below::Spawn { .. } | Below::OpenDirectory { .. } => match purpose {
                    crate::process::Purpose::Check => failed_check(owner, to_domain),
                    crate::process::Purpose::Shell
                    | crate::process::Purpose::Search
                    | crate::process::Purpose::List => {
                        to_domain.push(ToDomain::Done { owner, done: other() });
                    }
                },
                Below::File { .. }
                | Below::CancelFile { .. }
                | Below::Remove { .. }
                | Below::OpenRead { .. }
                | Below::Process(_) => {
                    unreachable!("only spawn start")
                }
            },
        }
    }

    /// Consume one file, child or pipe event and release its owner on completion.
    pub fn from_below(
        &mut self,
        env: &Env<Limits>,
        event: BelowEvent,
        to_domain: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        match event {
            BelowEvent::File(event) => {
                let owner = event.owner();
                if let Some(guide) = self.guides.remove(&owner) {
                    match guide.event(owner, event, below) {
                        Step::Continue(guide) => {
                            self.guides.insert(owner, guide).expect("guide slot returned");
                        }
                        Step::Done(terminal) => to_domain.push(terminal),
                    }
                } else if let Some(process) = self.processes.get_mut(&owner) {
                    process.directory_event(owner, event, below);
                    self.advance(owner, env.now, to_domain, below);
                } else {
                    let done = match self.pending.remove(&owner).expect("file terminal has admitted owner") {
                        Pending::One { kind } => crate::files::terminal(kind, event),
                        Pending::Remove(removing) => match removing.event(owner, env.now, event, below) {
                            crate::removing::Next::Wait(removing) => {
                                self.pending.insert(owner, Pending::Remove(removing)).expect("returned pending slot");
                                return;
                            }
                            crate::removing::Next::Done(done) => done,
                        },
                        Pending::Create(create) => match create.event(owner, env.now, event) {
                            crate::files::Next::Request { pending, request, deadline } => {
                                self.pending.insert(owner, pending).expect("pending slot returned");
                                below.push(Below::File { request, deadline });
                                return;
                            }
                            crate::files::Next::Done(done) => done,
                        },
                    };
                    to_domain.push(ToDomain::Done { owner, done });
                }
            }
            BelowEvent::Process(event) => self.process_event(env.now, event, to_domain, below),
            BelowEvent::Pipe(event) => self.pipe_event(env.now, event, to_domain, below),
        }
    }

    fn process_event(
        &mut self,
        now: Time,
        event: io::Event,
        to_domain: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let owner = match &event {
            io::Event::Spawned { owner, .. }
            | io::Event::Exited { owner, .. }
            | io::Event::Failed { owner, .. }
            | io::Event::Closed { owner } => *owner,
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Stream { .. }
            | io::Event::Output { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. } => unreachable!("child progress carries a domain owner"),
        };
        let process = self.processes.get_mut(&owner).expect("process event has admitted owner");
        match event {
            io::Event::Spawned { child, pipes, .. } => {
                process.spawned(child, pipes, &mut self.routes, owner, below);
            }
            io::Event::Exited { exit, .. } => process.exited(exit, below),
            io::Event::Failed { error, .. } => process.failed(error),
            io::Event::Closed { .. } => process.child_closed = true,
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Stream { .. }
            | io::Event::Output { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. } => unreachable!("child progress carries a domain owner"),
        }
        self.advance(owner, now, to_domain, below);
    }

    fn pipe_event(&mut self, now: Time, event: io::Event, to_domain: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
        let pipe = match &event {
            io::Event::Stream { owner, .. } | io::Event::Closed { owner } => *owner,
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Failed { .. }
            | io::Event::Shutdown { .. } => unreachable!("a read pipe emits only stream progress and Closed"),
        };
        let owner = *self.routes.get(&pipe).expect("pipe event has a live route");
        let process = self.processes.get_mut(&owner).expect("pipe route has admitted process");
        match event {
            io::Event::Stream { up, .. } => crate::process::stream(process, pipe, up, below),
            io::Event::Closed { .. } => {
                process.pipe_closed(pipe);
                self.routes.remove(&pipe).expect("closed pipe has a live route");
            }
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Failed { .. }
            | io::Event::Shutdown { .. } => unreachable!("a read pipe emits only stream progress and Closed"),
        }
        self.advance(owner, now, to_domain, below);
    }

    /// Advance the group deadline, termination, kill and close stages.
    pub fn fire(&mut self, env: &Env<Limits>, to_domain: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
        let mut owners = List::with_capacity(self.processes.capacity());
        for (owner, _) in &self.processes {
            owners.push(*owner).expect("one key per process");
        }
        for owner in &owners {
            let process = self.processes.get_mut(owner).expect("retained process");
            process.fire(env.now, env.limits.group_stop, below);
            self.advance(*owner, env.now, to_domain, below);
        }
    }

    fn advance(&mut self, owner: Token, now: Time, above: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
        let process = self.processes.get_mut(&owner).expect("retained process");
        match process.advance(now, owner) {
            crate::process::Advance::Waiting => {}
            crate::process::Advance::Answer { terminal, retire } => {
                above.push(terminal);
                if retire {
                    drop(self.processes.remove(&owner));
                } else if self.closing {
                    self.processes.get_mut(&owner).expect("held process").close_job(below);
                }
            }
            crate::process::Advance::Finished => {
                let process = self.processes.remove(&owner).expect("finished process");
                above.push(process.answer(owner));
            }
            crate::process::Advance::Closed => {
                drop(self.processes.remove(&owner));
            }
        }
    }

    /// Close held jobs through io's child close; in-flight calls still drain.
    pub fn close(&mut self, below: &mut Queue<Below>) {
        self.closing = true;
        let mut owners = List::with_capacity(self.processes.capacity());
        for (owner, _) in &self.processes {
            owners.push(*owner).expect("one owner per process");
        }
        for owner in &owners {
            let process = self.processes.get_mut(owner).expect("retained process");
            process.close_job(below);
            if process.closing_ready() {
                drop(self.processes.remove(owner));
            }
        }
    }

    /// Whether every file and child retained at close has settled.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.closing && self.pending.is_empty() && self.guides.is_empty() && self.processes.is_empty()
    }

    /// First armed process deadline; file deadlines belong to `FileIo`.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut first = None;
        for (_, process) in &self.processes {
            if let Some(deadline) = process.next_deadline() {
                first = Some(match first {
                    Some(before) if before < deadline => before,
                    Some(_) | None => deadline,
                });
            }
        }
        first
    }
}

fn other() -> tools::Done {
    tools::Done::Failed { fault: tools::Fault::Other }
}

fn failed_check(owner: Token, to_domain: &mut Queue<ToDomain>) {
    to_domain
        .push(ToDomain::Checked { owner, ran: run::Ran { exit: run::Exit::Unstarted, output: Box::new([]), cut: 0 } });
}

fn refuse(request: FromDomain, above: &mut Queue<ToDomain>) {
    match request {
        FromDomain::Op { owner, .. } | FromDomain::Job { owner, .. } => {
            above.push(ToDomain::Done { owner, done: other() });
        }
        FromDomain::Read { owner, .. } => above.push(ToDomain::Read { owner, read: run::Read::Failed }),
        FromDomain::Probe { owner, .. } => above.push(ToDomain::Probed { owner, executable: false }),
        FromDomain::Check { owner, .. } => failed_check(owner, above),
        FromDomain::Cancel { .. } | FromDomain::Abort { .. } => {}
    }
}

fn yielding(
    request: Option<(Process, Below)>,
    now: Time,
    after: Option<skein_lib::Duration>,
    head: u32,
    tail: u32,
    kept: u32,
) -> Option<(Process, Below)> {
    match request {
        Some((mut process, below)) => {
            process.yielding(now, after, head, tail, kept);
            Some((process, below))
        }
        None => None,
    }
}

fn starts(request: &FromDomain) -> bool {
    match request {
        FromDomain::Cancel { .. } | FromDomain::Abort { .. } => false,
        FromDomain::Op { .. }
        | FromDomain::Job { .. }
        | FromDomain::Read { .. }
        | FromDomain::Probe { .. }
        | FromDomain::Check { .. } => true,
    }
}
