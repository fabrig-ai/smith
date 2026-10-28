//! Machine scenarios on Skein's harness (protocol/agent.md, sections 2 and 8;
//! testing.md, sections 2.1 and 5). Each settled request owns one application
//! process; the scenario retains only its fake disk, absolute clock and trace.
//! The component never knows fake filesystem state. Requests enter through
//! `World::request`; IO completion routing remains the application's pass.

mod jobs;

use std::cell::RefCell;
use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;

use skein_fake_machine::{How, Item, Machine, Opened, step as machine_step};
use skein_io::file;
use skein_io::file_layer::{self, FileIo};
use skein_io::kernel::{Complete, Fd, Submit};
use skein_io::{self as io, Io};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_sim::{Answer, Ask, Config, Handle, Program, Reply};
use skein_world::{Expectation, Expectations, Host, Memory, Referee};
use smith_domain::{run, tools};
use smith_protocol_machine::{Below, BelowEvent, Component, FromDomain, Limits, ToDomain};

const FLOOD_MOST: usize = 1 << 20;

fn machine_limits() -> Limits {
    Limits {
        operations: 8,
        roots: 1,
        path_bytes: 128,
        file_bytes: 128,
        tool_payload: 128,
        entries: 16,
        entry_bytes: 2048,
        processes: 2,
        output_bytes: 32,
        search_hits: 8,
        search_bytes: 128,
        search_line_bytes: 512,
        env_bytes: 256,
        group_stop: Duration::from_millis(1),
    }
}

fn io_limits() -> io::Limits {
    io::Limits {
        sockets: 8,
        names: 0,
        refusals: 4,
        intake: 32,
        receive: 16,
        output: 32,
        sends: 2,
        accepts: 1,
        backlog: 2,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(1),
    }
}

/// The authoritative fake checkout and successive, independently settled runs.
pub struct World {
    machine: Rc<RefCell<Machine>>,
    root_handle: Opened,
    seed: u64,
    writable: bool,
    limits: Limits,
    now: Time,
    memory: Memory,
    trace: Vec<String>,
    /// The child-output deliveries in the last settled run.
    pub deliveries: usize,
    heap: Vec<(u64, u64)>,
    environment: Vec<smith_protocol_machine::Variable>,
    spawns: Rc<RefCell<Vec<Spawned>>>,
}

/// Independent machine-seam observation of the complete child environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Spawned {
    pub program: Box<[u8]>,
    pub environment: Box<[Box<[u8]>]>,
}

impl World {
    /// Configure the machine's immutable command variables for each settled run.
    #[must_use]
    pub fn configured(seed: u64, items: &[Item], variables: &[smith_protocol_machine::Variable]) -> Self {
        let mut world = Self::new(seed, items);
        world.environment = variables.to_vec();
        world
    }

    /// Observe kernel spawns without looking inside the machine component.
    #[must_use]
    pub fn spawns(&self) -> Vec<Spawned> {
        self.spawns.borrow().clone()
    }
    /// Lay one writable workspace, outside the application's heap.
    #[must_use]
    pub fn new(seed: u64, items: &[Item]) -> Self {
        Self::with_grant(seed, items, true)
    }

    /// Lay one read-only workspace, for the write-authority referee.
    #[must_use]
    pub fn read_only(seed: u64, items: &[Item]) -> Self {
        Self::with_grant(seed, items, false)
    }

    /// Use caller-compatible receiving bounds with the existing native fake driver.
    #[must_use]
    pub fn bounded(seed: u64, items: &[Item], limits: Limits) -> Self {
        let mut world = Self::with_grant(seed, items, true);
        assert!(smith_protocol_machine::worst_case(&limits).is_some());
        world.limits = limits;
        world
    }

    fn with_grant(seed: u64, items: &[Item], writable: bool) -> Self {
        let mut machine = Machine::new();
        let root_handle = machine.lay(items);
        Self {
            machine: Rc::new(RefCell::new(machine)),
            root_handle,
            seed,
            writable,
            limits: machine_limits(),
            now: Time::ZERO,
            memory: Memory::Unchecked,
            trace: Vec::new(),
            deliveries: 0,
            heap: Vec::new(),
            environment: Vec::new(),
            spawns: Rc::default(),
        }
    }

    /// The first adopted root token in each independent application run.
    #[must_use]
    pub fn root(&self) -> Token {
        Token::new(1)
    }

    /// Enable the shared harness's construction, iteration and drop checks.
    pub fn check_memory(&mut self) {
        self.memory = Memory::Checked;
    }

    /// Complete kernel traces for every request, in submission order.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        &self.trace
    }

    /// The absolute scenario clock after settled kernel work.
    #[must_use]
    pub fn now(&self) -> Time {
        self.now
    }

    /// Each checked run's measured process peak and bound.
    #[must_use]
    pub fn heap(&self) -> &[(u64, u64)] {
        &self.heap
    }

    /// An external writer changes the authoritative fake checkout.
    pub fn change_behind_session(&mut self, path: &[u8], bytes: &[u8]) {
        let mut machine = self.machine.borrow_mut();
        let opened = machine.open(self.root_handle, path, How::Read).expect("external file exists");
        machine.write(opened, 0, bytes).expect("external writer can change the file");
        machine.close(opened);
    }

    /// Read only the fake checkout, for the tools' write referee.
    #[must_use]
    pub fn content(&mut self, path: &[u8]) -> Vec<u8> {
        let mut machine = self.machine.borrow_mut();
        let opened = machine.open(self.root_handle, path, How::Read).expect("file exists");
        let bytes = machine.read(opened, 0, 4096).expect("file readable");
        machine.close(opened);
        bytes
    }

    /// Run one admitted request through its terminal and descriptor settlement.
    pub fn request(&mut self, request: FromDomain) -> ToDomain {
        self.run(&Job::Request(request, false))
    }

    /// Stop the issued native process after it has acquired its output pipes.
    pub fn cancelled_request(&mut self, request: FromDomain) -> ToDomain {
        self.run(&Job::Request(request, true))
    }

    /// Abort a hanging check at the actual pre-spawn or post-spawn component boundary.
    pub fn aborted_check(&mut self, owner: Token, after_spawn: bool) -> Token {
        let before = self.spawns().len();
        let terminal = self.run(&Job::AbortedCheck { owner, after_spawn });
        let spawned = self.spawns().len().checked_sub(before).expect("spawn history grows");
        assert!(spawned <= 1, "one admitted check process");
        if after_spawn {
            assert_eq!(spawned, 1, "post-spawn abort observed the native process");
        }
        match terminal {
            ToDomain::Aborted { owner: ended } => {
                assert_eq!(ended, owner);
                ended
            }
            other @ (ToDomain::Done { .. }
            | ToDomain::Read { .. }
            | ToDomain::Probed { .. }
            | ToDomain::Checked { .. }) => {
                panic!("checks owe their abort terminal: {other:?}")
            }
        }
    }

    /// Preserve the bounded child-event flood fixture at the component seam.
    pub fn flood(&mut self, owner: Token, bytes: &[u8], head: u32, tail: u32) -> ToDomain {
        assert!(bytes.len() <= FLOOD_MOST, "bounded flood fixture");
        self.run(&Job::Flood { owner, bytes: bytes.into(), head, tail })
    }

    /// Deliver a finite rg-selected path script through the machine's existing child-event fixture.
    pub fn listing(
        &mut self,
        owner: Token,
        selected: &[u8],
        depth: tools::Depth,
        maximum: u32,
        bytes: u64,
    ) -> ToDomain {
        assert!(selected.len() <= 4096, "bounded selected-file fixture");
        self.run(&Job::Listing { owner, selected: selected.into(), depth, maximum, bytes })
    }

    fn run(&mut self, job: &Job) -> ToDomain {
        let root = self.machine.borrow_mut().open(self.root_handle, b".", How::Directory).expect("fresh run root");
        let mut config = Config::calm();
        config.wall = Wall::from_nanos(self.now.as_nanos());
        let referee = Expectations::new(
            self.seed,
            vec![Terminal { owner: job.owner(), expected: 1, deadline: Time::from_nanos(2_000_000_000) }],
        );
        let mut world = skein_world::World::new(self.seed, config, referee, self.memory)
            .with_machine(Adapter { files: Rc::clone(&self.machine), spawns: Rc::clone(&self.spawns) });
        world.spawn_root(Handle::new(root.raw()), |root| {
            Application::new(self.seed, self.writable, self.now, root, job.copy(), self.limits, &self.environment)
        });
        let outcome = world.run();
        let terminal = copy_terminal(outcome.procs[0].terminals.first().expect("one terminal"));
        self.deliveries = outcome.procs[0].deliveries;
        self.trace.extend(outcome.trace.iter().map(|entry| format!("{entry:?}")));
        self.now = Time::from_nanos(self.now.as_nanos().checked_add(outcome.end.as_nanos()).expect("scenario clock"));
        if let Some(heap) = &outcome.heap {
            self.heap.extend_from_slice(heap);
        }
        drop(outcome); // The original terminal remains in its metered process until drop.
        self.settle();
        terminal
    }

    /// Every run closes its own root; only the scenario's inspection root remains.
    pub fn settle(&mut self) {
        assert_eq!(self.machine.borrow().open_handles(), 1, "all run descriptors settled");
    }
}

impl Drop for World {
    fn drop(&mut self) {
        self.machine.borrow_mut().close(self.root_handle);
    }
}

fn job_terminals(requests: &[FromDomain]) -> usize {
    requests.iter().filter(|request| !matches!(request, FromDomain::Job { act: tools::JobAct::Hasten, .. })).count()
}

enum Job {
    Sequence { requests: Vec<FromDomain>, delays: Vec<Duration> },
    Request(FromDomain, bool),
    AbortedCheck { owner: Token, after_spawn: bool },
    Flood { owner: Token, bytes: Box<[u8]>, head: u32, tail: u32 },
    Listing { owner: Token, selected: Box<[u8]>, depth: tools::Depth, maximum: u32, bytes: u64 },
}

impl Job {
    fn owner(&self) -> Token {
        match self {
            Self::Sequence { requests, .. } => {
                Self::Request(requests.first().expect("one spawn").clone(), false).owner()
            }
            Self::Request(request, _) => match request {
                FromDomain::Op { owner, .. }
                | FromDomain::Job { owner, .. }
                | FromDomain::Cancel { owner }
                | FromDomain::Read { owner, .. }
                | FromDomain::Probe { owner, .. }
                | FromDomain::Check { owner, .. }
                | FromDomain::Abort { owner } => *owner,
            },
            Self::AbortedCheck { owner, .. } | Self::Flood { owner, .. } | Self::Listing { owner, .. } => *owner,
        }
    }
    fn copy(&self) -> Self {
        match self {
            Self::Sequence { requests, delays } => {
                Self::Sequence { requests: requests.clone(), delays: delays.clone() }
            }
            Self::Request(request, cancel) => Self::Request(
                match request {
                    FromDomain::Op { owner, op, deadline } => {
                        FromDomain::Op { owner: *owner, op: op.clone(), deadline: *deadline }
                    }
                    FromDomain::Job { owner, act } => FromDomain::Job { owner: *owner, act: *act },
                    FromDomain::Cancel { owner } => FromDomain::Cancel { owner: *owner },
                    FromDomain::Read { owner, at, max, deadline } => {
                        FromDomain::Read { owner: *owner, at: at.clone(), max: *max, deadline: *deadline }
                    }
                    FromDomain::Probe { owner, at, deadline } => {
                        FromDomain::Probe { owner: *owner, at: at.clone(), deadline: *deadline }
                    }
                    FromDomain::Check { owner, program, deadline, tail } => {
                        FromDomain::Check { owner: *owner, program: program.clone(), deadline: *deadline, tail: *tail }
                    }
                    FromDomain::Abort { owner } => FromDomain::Abort { owner: *owner },
                },
                *cancel,
            ),
            Self::AbortedCheck { owner, after_spawn } => {
                Self::AbortedCheck { owner: *owner, after_spawn: *after_spawn }
            }
            Self::Listing { owner, selected, depth, maximum, bytes } => Self::Listing {
                owner: *owner,
                selected: selected.clone(),
                depth: *depth,
                maximum: *maximum,
                bytes: *bytes,
            },
            Self::Flood { owner, bytes, head, tail } => {
                Self::Flood { owner: *owner, bytes: bytes.clone(), head: *head, tail: *tail }
            }
        }
    }
}

fn copy_terminal(terminal: &ToDomain) -> ToDomain {
    match terminal {
        ToDomain::Done { owner, done } => ToDomain::Done { owner: *owner, done: done.clone() },
        ToDomain::Read { owner, read } => ToDomain::Read { owner: *owner, read: read.clone() },
        ToDomain::Probed { owner, executable } => ToDomain::Probed { owner: *owner, executable: *executable },
        ToDomain::Checked { owner, ran } => ToDomain::Checked {
            owner: *owner,
            ran: run::Ran { exit: ran.exit, output: ran.output.clone(), cut: ran.cut },
        },
        ToDomain::Aborted { owner } => ToDomain::Aborted { owner: *owner },
    }
}

#[derive(Debug)]
struct Terminal {
    owner: Token,
    expected: usize,
    deadline: Time,
}

impl Expectation<Application> for Terminal {
    fn check(&self, _now: Time, procs: &[Application]) -> Result<bool, String> {
        let terminals = &procs[0].terminals;
        if terminals.len() > self.expected {
            return Err("duplicate domain terminal".into());
        }
        if let Some(terminal) = terminals.first() {
            let owner = match terminal {
                ToDomain::Done { owner, .. }
                | ToDomain::Read { owner, .. }
                | ToDomain::Probed { owner, .. }
                | ToDomain::Checked { owner, .. }
                | ToDomain::Aborted { owner } => *owner,
            };
            if owner != self.owner {
                return Err("terminal changed its owner".into());
            }
            return Ok(terminals.len() == self.expected);
        }
        Ok(false)
    }
    fn deadline(&self) -> Time {
        self.deadline
    }
}

impl Referee<Application> for Expectations<Application, Terminal> {
    fn act(&mut self, _now: Time, _procs: &mut [Application]) {}
    fn observe(&mut self, now: Time, procs: &[Application]) {
        assert!(procs[0].terminals.len() <= procs[0].expected, "one terminal per act throughout settlement");
        Expectations::observe(self, now, procs);
    }
    fn next_deadline(&self) -> Option<Time> {
        Expectations::next_deadline(self)
    }
    fn overdue(&self, now: Time) -> Option<String> {
        Expectations::overdue(self, now)
    }
    fn passed(&self) -> bool {
        Expectations::passed(self)
    }
}

struct Adapter {
    files: Rc<RefCell<Machine>>,
    spawns: Rc<RefCell<Vec<Spawned>>>,
}

impl skein_world::Machine for Adapter {
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<Answer>) {
        if let Ask::Spawn { program, env, .. } = &call.ask {
            self.spawns.borrow_mut().push(Spawned { program: program.clone(), environment: env.clone() });
        }
        match &call.ask {
            Ask::Spawn { program, args, .. } if program.as_ref() == b"/bin/sh" => {
                let command = args.get(1).expect("shell -c command");
                let fixture = if command.as_ref() == b"fork" {
                    Program::Fork { exit_leader: false }
                } else if command.as_ref() == b"fork-exit" {
                    Program::Fork { exit_leader: true }
                } else if command.as_ref() == b"hang" {
                    Program::Never
                } else if command.as_ref() == b"fail" {
                    Program::Exit(1)
                } else {
                    Program::Exit(0)
                };
                answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(fixture)) });
            }
            Ask::Spawn { program, .. } if program.as_ref() == b"/usr/bin/rg" => {
                answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Exit(1))) });
            }
            Ask::Spawn { program, .. } if program.as_ref() == b"check-ok" => {
                answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Exit(0))) });
            }
            Ask::Spawn { program, .. } if program.as_ref() == b"check-fail" => {
                answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Exit(1))) });
            }
            Ask::Spawn { program, .. } if program.as_ref() == b"check-hang" => {
                answers.push(Answer { ticket: call.ticket, result: Ok(Reply::Program(Program::Never)) });
            }
            Ask::Spawn { .. }
            | Ask::Lock { .. }
            | Ask::Open { .. }
            | Ask::Read { .. }
            | Ask::Write { .. }
            | Ask::Append { .. }
            | Ask::Truncate { .. }
            | Ask::Sync { .. }
            | Ask::Stat { .. }
            | Ask::Rename { .. }
            | Ask::Remove { .. }
            | Ask::MakeDirectory { .. }
            | Ask::List { .. }
            | Ask::Close { .. } => {
                machine_step(&mut self.files.borrow_mut(), call, answers);
            }
        }
    }
}

struct Application {
    file_io: FileIo,
    process_io: Io,
    component: Component,
    root: Token,
    root_closing: bool,
    root_closed: bool,
    offset: Time,
    job: Option<Job>,
    cancel_after_spawn: Option<Token>,
    abort_after_spawn: Option<Token>,
    machine_env: Env<Limits>,
    process_env: Env<io::Limits>,
    below: Queue<Below>,
    to_domain: Queue<ToDomain>,
    file_events: Queue<file::Event>,
    process_events: Queue<io::Event>,
    process_pipes: BTreeSet<Token>,
    file_subs: Queue<Submit>,
    process_subs: Queue<Submit>,
    submissions: Queue<Submit>,
    completions: Queue<Complete>,
    file_flights: BTreeSet<Token>,
    terminals: Vec<ToDomain>,
    following: VecDeque<FromDomain>,
    delays: VecDeque<Duration>,
    following_at: Option<Time>,
    expected: usize,
    deliveries: usize,
}

impl Application {
    fn new(
        seed: u64,
        writable: bool,
        offset: Time,
        root_fd: Fd,
        job: Job,
        limits: Limits,
        variables: &[smith_protocol_machine::Variable],
    ) -> Self {
        let mut file_io = FileIo::with_whole_limit(16, 128, 32, 128, Duration::from_secs(1));
        file_io.seed_randomness(seed);
        let root = file_io.adopt_root(root_fd).expect("one root slot");
        assert_eq!(root, Token::new(1));
        let mut component = Component::new(
            &limits,
            smith_protocol_machine::Environment::encode(variables, &limits).expect("configured environment fits"),
        );
        component.workspace(&run::Workspace {
            directories: Box::new([run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"repo".as_slice().into(),
                root,
                writable,
                git: true,
                conflicts: Box::new([]),
            }]),
        });
        Self {
            file_io,
            process_io: Io::new(&io_limits()),
            component,
            root,
            root_closing: false,
            root_closed: false,
            offset,
            job: Some(job),
            cancel_after_spawn: None,
            abort_after_spawn: None,
            machine_env: Env { now: offset, wall: Wall::EPOCH, limits },
            process_env: Env { now: offset, wall: Wall::EPOCH, limits: io_limits() },
            below: Queue::with_capacity(64),
            to_domain: Queue::with_capacity(64),
            file_events: Queue::with_capacity(64),
            process_events: Queue::with_capacity(64),
            process_pipes: BTreeSet::new(),
            file_subs: Queue::with_capacity(64),
            process_subs: Queue::with_capacity(64),
            submissions: Queue::with_capacity(128),
            completions: Queue::with_capacity(128),
            file_flights: BTreeSet::new(),
            terminals: Vec::with_capacity(8),
            following: VecDeque::new(),
            delays: VecDeque::new(),
            following_at: None,
            expected: 1,
            deliveries: 0,
        }
    }

    fn flood(&mut self, owner: Token, bytes: &[u8], head: u32, tail: u32) -> ToDomain {
        let op = tools::Op::Spawn {
            cwd: tools::Place { root: self.root, path: Box::new([]) },
            command: Box::from(b"flood".as_slice()),
            head,
            tail,
            after: None,
        };
        self.child_output(owner, op, bytes)
    }

    fn listing(&mut self, owner: Token, selected: &[u8], depth: tools::Depth, maximum: u32, bytes: u64) -> ToDomain {
        let op = tools::Op::List {
            at: tools::Place { root: self.root, path: Box::new([]) },
            depth,
            glob: None,
            max: maximum,
            max_bytes: bytes,
        };
        self.child_output(owner, op, selected)
    }

    fn child_output(&mut self, owner: Token, op: tools::Op, bytes: &[u8]) -> ToDomain {
        let deadline = Time::from_nanos(100_000_000);
        self.component.from_domain(
            &self.machine_env,
            FromDomain::Op { owner, op, deadline },
            &mut self.to_domain,
            &mut self.below,
        );
        if let Some(Below::OpenDirectory { .. }) = self.below.iter().next() {
            drop(self.below.pop());
            self.component.from_below(
                &self.machine_env,
                BelowEvent::File(file::Event::Opened { owner, file: Token::new(7999), len: 0 }),
                &mut self.to_domain,
                &mut self.below,
            );
            assert!(
                matches!(self.below.pop(), Some(Below::File { request: file::Request::Close { .. }, .. })),
                "listing directory is closed before spawn"
            );
            self.component.from_below(
                &self.machine_env,
                BelowEvent::File(file::Event::Closed { owner }),
                &mut self.to_domain,
                &mut self.below,
            );
        }
        match self.below.pop() {
            Some(Below::Spawn { .. }) => {}
            other => panic!("expected a flood spawn: {other:?}"),
        }
        let stdout = Token::new(8001);
        let stderr = Token::new(8002);
        let pipes = Box::new([stdout, stderr]);
        let event = io::Event::Spawned { owner, child: Token::new(8000), pipes };
        self.component.from_below(&self.machine_env, BelowEvent::Process(event), &mut self.to_domain, &mut self.below);
        let mut demand = self.below.pop().expect("stdout demand");
        assert!(self.below.pop().is_some(), "stderr demand");
        let mut at = 0;
        while at < bytes.len() {
            let count = match demand {
                Below::Process(io::Request::Stream {
                    down: Down::Demand { read: Read::Arrived { max }, .. }, ..
                }) => (max as usize).min(bytes.len() - at),
                Below::Process(io::Request::Stream { down: Down::Demand { read: Read::Fill(n), .. }, .. }) => {
                    n as usize
                }
                other @ (Below::File { .. }
                | Below::CancelFile { .. }
                | Below::Remove { .. }
                | Below::OpenRead { .. }
                | Below::OpenDirectory { .. }
                | Below::Spawn { .. }
                | Below::Process(_)) => panic!("expected output read: {other:?}"),
            };
            let up = Up::Bytes(Box::from(&bytes[at..at + count]));
            self.component.from_below(
                &self.machine_env,
                BelowEvent::Pipe(io::Event::Stream { owner: stdout, up }),
                &mut self.to_domain,
                &mut self.below,
            );
            at += count;
            self.deliveries += 1;
            demand = self.below.pop().expect("demand after each delivery");
        }
        self.component.from_below(
            &self.machine_env,
            BelowEvent::Process(io::Event::Exited { owner, exit: io::kernel::Exit::Code(0) }),
            &mut self.to_domain,
            &mut self.below,
        );
        assert_eq!(self.below.pop(), Some(Below::Process(io::Request::Close { entity: Token::new(8000) })));
        for pipe in [stdout, stderr] {
            self.component.from_below(
                &self.machine_env,
                BelowEvent::Pipe(io::Event::Closed { owner: pipe }),
                &mut self.to_domain,
                &mut self.below,
            );
        }
        assert!(self.to_domain.is_empty(), "child closure is still owed");
        self.component.from_below(
            &self.machine_env,
            BelowEvent::Process(io::Event::Closed { owner }),
            &mut self.to_domain,
            &mut self.below,
        );
        let terminal = self.to_domain.pop().expect("one flood terminal");
        assert!(self.to_domain.is_empty());
        terminal
    }

    /// Advance one IO pass and all fake-machine calls.
    #[expect(clippy::too_many_lines, reason = "the application loop keeps ordered IO stages in one place")]
    fn pass(&mut self) {
        if self.file_io.is_due(self.machine_env.now) {
            file_layer::expire(&mut self.file_io, self.machine_env.now, &mut self.file_subs);
        }
        if self.process_io.is_due(self.machine_env.now) {
            io::fire(&mut self.process_io, &self.process_env, &mut self.process_events, &mut self.process_subs);
        }
        if self.component.next_deadline().is_some_and(|at| at <= self.machine_env.now) {
            self.component.fire(&self.machine_env, &mut self.to_domain, &mut self.below);
        }
        while self.process_io.is_ready() {
            io::resume(&mut self.process_io, &self.process_env, &mut self.process_events, &mut self.process_subs);
        }
        while let Some(request) = self.below.pop() {
            match request {
                Below::File { request, deadline } => {
                    assert!(self.file_io.takes(), "serial file layer");
                    file_layer::down_until(
                        &mut self.file_io,
                        deadline,
                        request,
                        &mut self.file_events,
                        &mut self.file_subs,
                    );
                }
                Below::CancelFile { owner } => file_layer::cancel(&mut self.file_io, owner, &mut self.file_subs),
                Below::OpenRead { owner, root, path, deadline } => {
                    let descriptor = self.file_io.descriptor(root).expect("adopted root");
                    file_layer::down_until(
                        &mut self.file_io,
                        deadline,
                        file::Request::OpenRead { owner, root: descriptor, name: path },
                        &mut self.file_events,
                        &mut self.file_subs,
                    );
                }
                Below::Remove { owner, parent, name, directory, deadline } => {
                    let descriptor = self.file_io.descriptor(parent).expect("removal parent");
                    file_layer::down_until(
                        &mut self.file_io,
                        deadline,
                        file::Request::Remove { owner, root: descriptor, name, directory },
                        &mut self.file_events,
                        &mut self.file_subs,
                    );
                }
                Below::OpenDirectory { owner, root, path, no_follow, deadline } => {
                    let descriptor = self.file_io.descriptor(root).expect("adopted root");
                    file_layer::down_until(
                        &mut self.file_io,
                        deadline,
                        file::Request::OpenDirectory { owner, root: descriptor, name: path, no_follow },
                        &mut self.file_events,
                        &mut self.file_subs,
                    );
                }
                Below::Spawn { owner, root, spawn } => {
                    let descriptor = self.file_io.descriptor(root).expect("adopted root");
                    io::down(
                        &mut self.process_io,
                        &self.process_env,
                        io::Request::Spawn {
                            owner,
                            spawn: io::kernel::Spawn {
                                program: spawn.program,
                                args: spawn.args,
                                env: spawn.env,
                                root: descriptor,
                                dir: spawn.dir,
                                pipes: spawn.pipes,
                            },
                        },
                        &mut self.process_subs,
                    );
                }
                Below::Process(request) => {
                    io::down(&mut self.process_io, &self.process_env, request, &mut self.process_subs);
                }
            }
        }
        while let Some(complete) = self.completions.pop() {
            if self.file_flights.remove(&complete.op) {
                file_layer::up(&mut self.file_io, complete, &mut self.file_events, &mut self.file_subs);
            } else {
                io::up(
                    &mut self.process_io,
                    &self.process_env,
                    complete,
                    &mut self.process_events,
                    &mut self.process_subs,
                );
            }
        }
        while let Some(event) = self.file_events.pop() {
            if matches!(&event, file::Event::Closed { owner } if *owner == Token::new(u64::MAX)) {
                self.root_closed = true;
                continue;
            }
            self.component.from_below(&self.machine_env, BelowEvent::File(event), &mut self.to_domain, &mut self.below);
        }
        while let Some(event) = self.process_events.pop() {
            let spawned = if let io::Event::Spawned { owner, .. } = &event { Some(*owner) } else { None };
            let event = match &event {
                io::Event::Spawned { pipes, .. } => {
                    self.process_pipes.extend(pipes.iter().copied());
                    BelowEvent::Process(event)
                }
                io::Event::Stream { .. } => BelowEvent::Pipe(event),
                io::Event::Closed { owner } if self.process_pipes.remove(owner) => BelowEvent::Pipe(event),
                io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Connecting { .. }
                | io::Event::Connected { .. }
                | io::Event::Output { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Resized
                | io::Event::Window { .. }
                | io::Event::Failed { .. }
                | io::Event::Closed { .. }
                | io::Event::Shutdown { .. } => BelowEvent::Process(event),
            };
            self.component.from_below(&self.machine_env, event, &mut self.to_domain, &mut self.below);
            if spawned.is_some() && spawned == self.abort_after_spawn {
                let owner = self.abort_after_spawn.take().expect("issued abort owner");
                self.component.from_domain(
                    &self.machine_env,
                    FromDomain::Abort { owner },
                    &mut self.to_domain,
                    &mut self.below,
                );
            }
            if spawned.is_some() && spawned == self.cancel_after_spawn {
                let owner = self.cancel_after_spawn.take().expect("issued stop owner");
                self.component.from_domain(
                    &self.machine_env,
                    FromDomain::Cancel { owner },
                    &mut self.to_domain,
                    &mut self.below,
                );
            }
        }
        while let Some(event) = self.to_domain.pop() {
            self.terminals.push(event);
            if !self.following.is_empty() {
                let delay = self.delays.pop_front().unwrap_or(Duration::ZERO);
                if delay == Duration::ZERO {
                    let request = self.following.pop_front().expect("next act");
                    self.component.from_domain(&self.machine_env, request, &mut self.to_domain, &mut self.below);
                } else {
                    self.following_at = Some(self.machine_env.now.saturating_add(delay));
                }
            }
        }
        if self.following_at.is_some_and(|at| at <= self.machine_env.now) {
            self.following_at = None;
            let request = self.following.pop_front().expect("scheduled act");
            self.component.from_domain(&self.machine_env, request, &mut self.to_domain, &mut self.below);
        }
        if matches!(self.following.front(), Some(FromDomain::Job { act: tools::JobAct::Hasten, .. }))
            && self.following_at.is_none()
        {
            let request = self.following.pop_front().expect("a terminal-free act follows its read");
            self.component.from_domain(&self.machine_env, request, &mut self.to_domain, &mut self.below);
        }
        if self.terminals.len() == self.expected {
            self.component.close(&mut self.below);
        }
        self.process_io.reclaim();
        if self.terminals.len() == self.expected
            && !self.root_closing
            && self.below.is_empty()
            && self.file_flights.is_empty()
            && self.file_subs.is_empty()
            && self.file_io.takes()
            && self.process_io.is_empty()
        {
            assert_eq!(self.file_io.open_files(), 1, "only run root remains");
            self.root_closing = true;
            file_layer::down_until(
                &mut self.file_io,
                Time::from_nanos(self.machine_env.now.as_nanos().checked_add(1_000_000_000).expect("close deadline")),
                file::Request::Close { owner: Token::new(u64::MAX), file: self.root },
                &mut self.file_events,
                &mut self.file_subs,
            );
        }
        while let Some(submit) = self.file_subs.pop() {
            assert!(self.file_flights.insert(submit.op), "fresh file operation");
            self.submissions.push(submit);
        }
        while let Some(submit) = self.process_subs.pop() {
            self.submissions.push(submit);
        }
    }
}

impl Host for Application {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.machine_env.now =
            Time::from_nanos(self.offset.as_nanos().checked_add(now.as_nanos()).expect("absolute clock"));
        self.machine_env.wall = wall;
        self.process_env.now = self.machine_env.now;
        self.process_env.wall = wall;
        if let Some(job) = self.job.take() {
            match job {
                Job::Sequence { requests, delays } => {
                    self.expected = job_terminals(&requests);
                    self.following = requests.into();
                    self.delays = delays.into();
                    let first = self.following.pop_front().expect("sequence starts with one spawn");
                    self.component.from_domain(&self.machine_env, first, &mut self.to_domain, &mut self.below);
                }
                Job::Request(request, cancel) => {
                    if cancel {
                        self.cancel_after_spawn = Some(match &request {
                            FromDomain::Op { owner, .. }
                            | FromDomain::Job { owner, .. }
                            | FromDomain::Cancel { owner }
                            | FromDomain::Read { owner, .. }
                            | FromDomain::Probe { owner, .. }
                            | FromDomain::Check { owner, .. }
                            | FromDomain::Abort { owner } => *owner,
                        });
                    }
                    self.component.from_domain(&self.machine_env, request, &mut self.to_domain, &mut self.below);
                }
                Job::AbortedCheck { owner, after_spawn } => {
                    if after_spawn {
                        self.abort_after_spawn = Some(owner);
                    }
                    self.component.from_domain(
                        &self.machine_env,
                        FromDomain::Check {
                            owner,
                            program: run::Place { root: self.root, path: Box::from(b"check-hang".as_slice()) },
                            deadline: self.machine_env.now.saturating_add(Duration::from_secs(1)),
                            tail: 8,
                        },
                        &mut self.to_domain,
                        &mut self.below,
                    );
                    if !after_spawn {
                        assert!(
                            self.process_io.is_empty(),
                            "abort is issued before native spawn; a queued spawn may still race it"
                        );
                        self.component.from_domain(
                            &self.machine_env,
                            FromDomain::Abort { owner },
                            &mut self.to_domain,
                            &mut self.below,
                        );
                    }
                }
                Job::Listing { owner, selected, depth, maximum, bytes } => {
                    let terminal = self.listing(owner, &selected, depth, maximum, bytes);
                    self.terminals.push(terminal);
                }
                Job::Flood { owner, bytes, head, tail } => {
                    let terminal = self.flood(owner, &bytes, head, tail);
                    self.terminals.push(terminal);
                }
            }
        }
        self.pass();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.job.is_some()
            || !self.completions.is_empty()
            || !self.below.is_empty()
            || !self.to_domain.is_empty()
            || !self.file_events.is_empty()
            || !self.process_events.is_empty()
            || !self.file_subs.is_empty()
            || !self.process_subs.is_empty()
            || self.process_io.is_ready()
            || self.next_deadline().is_some_and(|deadline| deadline <= now)
    }
    fn next_deadline(&self) -> Option<Time> {
        [
            self.file_io.next_deadline(),
            self.process_io.next_deadline(),
            self.component.next_deadline(),
            self.following_at,
        ]
        .into_iter()
        .flatten()
        .min()
        .map(|deadline| Time::from_nanos(deadline.as_nanos().saturating_sub(self.offset.as_nanos())))
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        [self.component.next_deadline(), self.following_at]
            .into_iter()
            .flatten()
            .min()
            .map(|deadline| Time::from_nanos(deadline.as_nanos().saturating_sub(self.offset.as_nanos())))
    }
    fn is_empty(&self) -> bool {
        self.terminals.len() == self.expected
            && self.root_closed
            && self.file_io.open_files() == 0
            && self.process_io.is_empty()
            && self.file_flights.is_empty()
            && !self
                .work_pending(Time::from_nanos(self.machine_env.now.as_nanos().saturating_sub(self.offset.as_nanos())))
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        smith_protocol_machine::worst_case(&self.machine_env.limits)
            .and_then(|bound| bound.checked_add(FileIo::worst_case(16, 128, 32, 128)?))
            .and_then(|bound| bound.checked_add(io::worst_case(&self.process_env.limits)?))
            // Finite queues, one maximal request/terminal and the bounded flood.
            .and_then(|bound| bound.checked_add(2_097_152))
            .expect("application heap bound")
    }
    fn operations(&self) -> u32 {
        io::operations(&self.process_env.limits).and_then(|ops| ops.checked_add(16)).expect("application operations")
    }
}
