//! Scripted parent and observation referee beside a hosted agent. The parent
//! knows channel bytes and kernel terminals, never agent state; the referee
//! reads those bytes and independent provider observations, including facts
//! decoded from the host's channel.
//! Scheduling, replay and process heaps belong to Skein (testing.md, 2–5).
use crate::fake;
use skein_io::kernel::{self, Done, Fd, Op, Pipe, Spawn, Way};
use skein_lib::{Duration, Queue, Time, Token, Wall, bytes};
use skein_shell::Host;
use skein_world::{HostedProgram, Inherited, Memory, Outcome};

#[expect(clippy::struct_excessive_bools, reason = "independent kernel operations each own an in-flight bit")]
pub(crate) struct Parent {
    pub(crate) peer: skein_fake_channel::ScriptedPeer,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    input: Option<Fd>,
    output: Option<Fd>,
    error: Option<Fd>,
    pidfd: Option<Fd>,
    reading: bool,
    error_reading: bool,
    writing: bool,
    pending: Option<(Box<[u8]>, u32)>,
    pub(crate) status: Option<kernel::Exit>,
    signal: u32,
    signalled: u32,
    hang_up: bool,
    hold_channel: bool,
    pub(crate) errors: Vec<u8>,
    closing: u32,
    next: u64,
    acknowledge_turns: bool,
    acknowledged: u32,
    frames_at_release: usize,
    refreshing: Option<smith_channel::GrantValue>,
    pub(crate) cancel_pending: bool,
    pub(crate) program_host_answers: Option<usize>,
    pub(crate) program_message: bool,
    pub(crate) program_messages: std::collections::VecDeque<skein_channel::Frame>,
}

impl Parent {
    #[expect(clippy::too_many_arguments, reason = "ordinary scenario harness supplies independent startup inputs")]
    pub(crate) fn new(
        root: Fd,
        charter: &[u8],
        hang_up: bool,
        tracing: Option<(TraceCase, u32)>,
        window: u32,
        environment: bool,
        message: Option<&smith_channel::Message>,
        capture: smith_agent_service::sinks::Capture,
        provider: skein_llm::Provider,
        value: &smith_channel::GrantValue,
        live: Option<smith_channel::LiveAsk>,
        output: u32,
    ) -> Self {
        let mut peer = if matches!(tracing, Some((TraceCase::StoppedReasoning, _))) {
            crate::host_with_limits(crate::reasoning_configuration().limits.channel)
        } else if output > 0 {
            crate::host_with_limits(
                crate::configuration_output(provider == skein_llm::Provider::Anthropic, output).limits.channel,
            )
        } else {
            crate::host()
        };
        if !hang_up {
            peer.play(crate::start_workspace_message(
                charter,
                window,
                environment || matches!(tracing, Some((TraceCase::StoppedReasoning, _))),
                message,
                value,
                live,
            ));
        }
        let mut parent = Self {
            peer,
            completions: Queue::with_capacity(16),
            submissions: Queue::with_capacity(16),
            input: None,
            output: None,
            error: None,
            pidfd: None,
            reading: false,
            error_reading: false,
            writing: false,
            pending: None,
            status: None,
            signal: 0,
            signalled: 0,
            hang_up,
            hold_channel: false,
            errors: Vec::new(),
            closing: 0,
            next: 0,
            acknowledge_turns: matches!(tracing, Some((TraceCase::StoppedReasoning, _))),
            acknowledged: 0,
            frames_at_release: 0,
            refreshing: None,
            cancel_pending: false,
            program_host_answers: None,
            program_message: false,
            program_messages: std::collections::VecDeque::new(),
        };
        parent.submit(Op::Spawn {
            spawn: Box::new(Spawn {
                program: b"smith".as_slice().into(),
                args: if provider == skein_llm::Provider::OpenAi {
                    Box::new([Box::from(&b"openai"[..])])
                } else if environment {
                    Box::new([Box::from(b"environment".as_slice())])
                } else {
                    tracing.map_or_else(
                        || Box::new([]) as Box<[Box<[u8]>]>,
                        |(case, frames)| {
                            Box::new([
                                Box::new([b'0' + case.number()]) as Box<[u8]>,
                                frames.to_string().into_bytes().into_boxed_slice(),
                                match capture {
                                    smith_agent_service::sinks::Capture::None => Box::from(b"none".as_slice()),
                                    smith_agent_service::sinks::Capture::Calls => Box::from(b"calls".as_slice()),
                                    smith_agent_service::sinks::Capture::Everything => {
                                        Box::from(b"everything".as_slice())
                                    }
                                },
                                Box::from(if provider == skein_llm::Provider::Anthropic {
                                    b"anthropic".as_slice()
                                } else {
                                    b"codex".as_slice()
                                }),
                                output.to_string().into_bytes().into_boxed_slice(),
                            ])
                        },
                    )
                },
                env: Box::new([]),
                root,
                dir: Box::new([]),
                pipes: Box::new([
                    Pipe { child: 0, way: Way::In, parent: None },
                    Pipe { child: 1, way: Way::Out, parent: None },
                    Pipe { child: 2, way: Way::Out, parent: None },
                ]),
            }),
        });
        parent
    }

    pub(crate) fn program_arguments(
        &mut self,
        arguments: Box<[Box<[u8]>]>,
        refresh: Option<smith_channel::GrantValue>,
    ) {
        let mut startup = self.submissions.pop().expect("parent's initial spawn");
        let Op::Spawn { spawn } = &mut startup.kind else { panic!("initial operation is Spawn") };
        spawn.args = arguments;
        self.submissions.push(startup);
        self.refreshing = refresh;
    }

    fn submit(&mut self, kind: Op) {
        self.next = self.next.checked_add(1).expect("finite parent tokens");
        self.submissions.push(kernel::Submit { op: Token::new(self.next), kind });
    }

    fn close(&mut self, fd: Fd) {
        self.closing = self.closing.checked_add(1).expect("five parent descriptors");
        self.submit(Op::Close { fd });
    }

    fn acknowledge(&mut self) {
        if self.admitted()
            && let Some(value) = self.refreshing.take()
        {
            self.peer.play(crate::grants::refresh(value));
        }
        if self.acknowledge_turns {
            let turns: Vec<_> = self
                .peer
                .observed()
                .iter()
                .filter(|frame| frame.kind == 0x0109)
                .map(|frame| {
                    smith_channel::Turn::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&frame.body))
                        .expect("told turn")
                        .number()
                })
                .filter(|number| *number > self.acknowledged)
                .collect();
            for number in turns {
                self.peer.play(crate::acknowledge(number));
                self.acknowledged = number;
            }
        }
    }

    fn answered(&self) -> bool {
        self.peer.observed().iter().any(|frame| frame.kind == 0x0110)
    }
    fn admitted(&self) -> bool {
        self.peer.observed().iter().any(|frame| frame.kind == 0x0106)
    }

    fn completed(&mut self, complete: kernel::Complete) {
        match complete.kind {
            Op::Spawn { spawn } => {
                let Ok(Done::Spawned { pidfd, .. }) = complete.result else { panic!("hosted startup failed") };
                self.close(spawn.root);
                self.pidfd = Some(pidfd);
                self.input = spawn.pipes[0].parent;
                if self.hang_up {
                    let input = self.input.take().expect("child input");
                    self.close(input);
                }
                self.output = spawn.pipes[1].parent;
                self.error = spawn.pipes[2].parent;
                self.submit(Op::Wait { pidfd, reap: false });
            }
            Op::Wait { pidfd, reap } => {
                let Ok(Done::Exit(status)) = complete.result else { panic!("agent wait failed") };
                if reap {
                    assert_eq!(self.status, Some(status), "reap preserves observed exit");
                    let pidfd = self.pidfd.take().expect("live child pidfd");
                    self.close(pidfd);
                } else {
                    self.status = Some(status);
                    self.submit(Op::Wait { pidfd, reap: true });
                }
            }
            Op::PipeRead { fd, buf } => {
                let error = self.error == Some(fd);
                if error {
                    self.error_reading = false;
                } else {
                    self.reading = false;
                }
                let Ok(Done::Count(count)) = complete.result else { panic!("agent read failed") };
                if count == 0 {
                    if error {
                        self.error = None;
                    } else {
                        self.output = None;
                    }
                    self.close(fd);
                } else {
                    let bytes = &buf[..usize::try_from(count).expect("read count")];
                    if error {
                        self.errors.extend_from_slice(bytes);
                    } else {
                        self.peer.feed(bytes).expect("valid channel");
                    }
                }
            }
            Op::PipeWrite { bytes, from, .. } => {
                self.writing = false;
                let count = match complete.result {
                    Ok(Done::Count(count)) => count,
                    Err(kernel::Error::BrokenPipe) if self.acknowledge_turns && bytes.get(..2) == Some(&[1, 3]) => {
                        // The final turn's ACK can meet input close before the answer is read.
                        if let Some(fd) = self.input.take() {
                            self.close(fd);
                        }
                        return;
                    }
                    _ => panic!("agent write failed"),
                };
                let offset = from.checked_add(count).expect("bounded frame offset");
                self.pending =
                    if usize::try_from(offset).expect("offset") == bytes.len() { None } else { Some((bytes, offset)) };
            }
            Op::Close { .. } => {
                assert!(complete.result.is_ok(), "parent close succeeds");
                self.closing = self.closing.checked_sub(1).expect("submitted close");
            }
            Op::Signal { .. } => {
                assert!(complete.result.is_ok(), "live child signal succeeds");
            }
            Op::Socket { .. }
            | Op::Bind { .. }
            | Op::Listen { .. }
            | Op::Accept { .. }
            | Op::Connect { .. }
            | Op::Recv { .. }
            | Op::Send { .. }
            | Op::Shutdown { .. }
            | Op::Open { .. }
            | Op::Lock { .. }
            | Op::Append { .. }
            | Op::Read { .. }
            | Op::Write { .. }
            | Op::Truncate { .. }
            | Op::Sync { .. }
            | Op::Stat { .. }
            | Op::Rename { .. }
            | Op::Remove { .. }
            | Op::MakeDirectory { .. }
            | Op::List { .. }
            | Op::ReadSignal { .. }
            | Op::Window { .. }
            | Op::Usage
            | Op::Cancel { .. } => unreachable!("scripted parent owns only process pipes"),
        }
    }
}

impl Host for Parent {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        if self.program_message {
            self.program_message = false;
            let message = self.program_messages.pop_front().unwrap_or_else(crate::program_world::message);
            self.peer.play(message);
        }
        if self.cancel_pending {
            self.cancel_pending = false;
            self.peer.play(crate::program_world::cancel());
        }
        while let Some(complete) = self.completions.pop() {
            self.completed(complete);
        }
        crate::program_world::answer_host(self);
        self.acknowledge();
        for _ in self.signalled..self.signal {
            self.signalled = self.signalled.checked_add(1).expect("finite requested signals");
            self.submit(Op::Signal {
                pidfd: self.pidfd.expect("admitted child"),
                to: kernel::Target::Child,
                signal: kernel::Signal::Terminate,
            });
        }
        if !self.reading
            && let Some(fd) = self.output
        {
            self.reading = true;
            self.submit(Op::PipeRead { fd, buf: bytes::zeroed(4096) });
        }
        if !self.error_reading
            && let Some(fd) = self.error
        {
            self.error_reading = true;
            self.submit(Op::PipeRead { fd, buf: bytes::zeroed(256) });
        }
        if self.input.is_some() && self.pending.is_none() && !self.writing {
            self.pending = self.peer.pop_output().map(|bytes| (bytes, 0));
        }
        if !self.writing
            && let Some((bytes, from)) = self.pending.take()
        {
            self.writing = true;
            self.submit(Op::PipeWrite { fd: self.input.expect("child stdin"), bytes, from });
        }
        if (self.status.is_some() || (self.answered() && !self.hold_channel))
            && !self.writing
            && self.pending.is_none()
            && let Some(fd) = self.input.take()
        {
            self.close(fd);
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty()
            || !self.submissions.is_empty()
            || self.program_message
            || self.cancel_pending
            || (self.signal > self.signalled)
            || (self.output.is_some() && !self.reading)
            || (self.error.is_some() && !self.error_reading)
            || (self.pending.is_some() && !self.writing)
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.status.is_some()
            && self.input.is_none()
            && self.output.is_none()
            && self.error.is_none()
            && self.pidfd.is_none()
            && self.closing == 0
            && self.submissions.is_empty()
            && self.completions.is_empty()
    }
    fn operations(&self) -> u32 {
        16
    }
}

#[derive(Clone, Copy)]
struct ReleaseName {
    bytes: [u8; 64],
    length: usize,
}

impl ReleaseName {
    fn new(name: &[u8]) -> Self {
        let mut bytes = [0; 64];
        bytes.get_mut(..name.len()).expect("hold-name bound").copy_from_slice(name);
        Self { bytes, length: name.len() }
    }
}

enum Proc {
    Parent(Box<Parent>),
    Agent(Box<skein_world::PipeHost<smith_agent_shell::Agent>>, crate::AgentErrors),
    Peer(Box<skein_fake_peers::llm::Peer>, Option<ReleaseName>),
    Tracing(Box<crate::tracing::Tracing>),
}
impl Proc {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Parent(p) => p.as_ref(),
            Self::Agent(p, _) => p.as_ref(),
            Self::Peer(p, _) => p.as_ref(),
            Self::Tracing(p) => p.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Parent(p) => p.as_mut(),
            Self::Agent(p, _) => p.as_mut(),
            Self::Peer(p, _) => p.as_mut(),
            Self::Tracing(p) => p.as_mut(),
        }
    }
}
impl Host for Proc {
    fn drain(&mut self) {
        self.host_mut().drain();
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        if let Self::Peer(peer, pending) = self
            && let Some(name) = pending.take()
        {
            peer.release(&name.bytes[..name.length]).expect("named release fits the peer");
        }
        self.host_mut().iterate(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.host_mut().submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        matches!(self, Self::Peer(_, Some(_))) || self.host().work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.host().next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.host().next_policy_deadline()
    }
    fn is_empty(&self) -> bool {
        self.host().is_empty()
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.host().exit()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}
fn make(spawn: &Spawn, inherited: &Inherited) -> Proc {
    let fd = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1;
    let (errors, capture) = crate::AgentErrors::pipe(fd);
    let mut configuration = if spawn.args.first().is_some_and(|arg| arg.as_ref() == b"openai") {
        crate::configuration_for(skein_llm::Provider::OpenAi)
    } else {
        crate::configuration()
    };
    if spawn.args.first().is_some_and(|arg| arg.as_ref() == b"environment") {
        configuration.limits.machine.env_bytes = 128;
        configuration.environment = smith_protocol_machine::Environment::encode(
            &[
                smith_protocol_machine::Variable {
                    name: b"BUILD_FLAG".as_slice().into(),
                    value: b"configured=value".as_slice().into(),
                },
                smith_protocol_machine::Variable { name: b"EMPTY".as_slice().into(), value: Box::new([]) },
            ],
            &configuration.limits.machine,
        )
        .expect("configured command environment");
    }
    if spawn.args.first().is_some_and(|argument| argument.as_ref() == b"roots") {
        configuration.limits.machine.file_bytes = configuration.limits.domain.session.tools.file_bytes;
    }
    let agent = crate::configured(configuration, 7, inherited, errors.clone());
    Proc::Agent(Box::new(capture.host(agent)), errors)
}

fn make_tracing(spawn: &Spawn, inherited: &Inherited) -> Proc {
    let output = std::str::from_utf8(&spawn.args[4]).expect("output argument").parse().expect("output tokens");
    let mut configuration = if output > 0 {
        crate::configuration_output(spawn.args[3].as_ref() == b"anthropic", output)
    } else if spawn.args[0].as_ref() == b"8" {
        crate::reasoning_configuration()
    } else {
        crate::configuration_for(if spawn.args[3].as_ref() == b"anthropic" {
            skein_llm::Provider::Anthropic
        } else {
            skein_llm::Provider::OpenAiCodex
        })
    };
    if spawn.args[0].as_ref() == b"6" {
        configuration.limits.llm.connection.connections = 3;
        configuration.limits.llm.connection.calls = 3;
        configuration.limits.llm.connection.per_endpoint = 3;
    }
    configuration.limits.channel.channel.output_frames =
        std::str::from_utf8(&spawn.args[1]).expect("frame argument").parse().expect("frame allowance");
    let capture = match spawn.args[2].as_ref() {
        b"none" => smith_agent_service::sinks::Capture::None,
        b"calls" => smith_agent_service::sinks::Capture::Calls,
        b"everything" => smith_agent_service::sinks::Capture::Everything,
        _ => panic!("selected native capture policy"),
    };
    Proc::Tracing(Box::new(crate::tracing::Tracing::new(configuration, inherited, capture)))
}

fn unused_roots(spawn: &Spawn) -> Vec<skein_world::StartupRoot> {
    let mut roots: Vec<_> = [b"unused-a", b"unused-b"]
        .into_iter()
        .map(|name| skein_world::StartupRoot {
            name: name.as_slice().into(),
            path: [b"/", name.as_slice()].concat().into(),
        })
        .collect();
    if spawn.args.len() >= 4 {
        roots.push(skein_world::StartupRoot {
            name: b"event-trace".as_slice().into(),
            path: b"event-trace".as_slice().into(),
        });
    }
    roots
}

struct Spawned {
    program: Box<[u8]>,
    environment: Box<[Box<[u8]>]>,
}

struct Machine {
    files: skein_fake_machine::Machine,
    spawns: Vec<Spawned>,
    next_root: u32,
    trace_root: Option<u32>,
    problem_file: bool,
}
impl skein_world::Machine for Machine {
    fn open_root(&mut self, path: &[u8]) -> Result<skein_sim::Handle, kernel::Error> {
        assert!(matches!(path, b"/unused-a" | b"/unused-b" | b"event-trace"), "scenario startup roots");
        if path == b"event-trace" {
            self.trace_root = Some(self.next_root);
        }
        self.next_root += 1;
        let items = if self.problem_file && path == b"/unused-a" {
            vec![skein_fake_machine::Item::file(b"problem.txt", b"problem")]
        } else {
            Vec::new()
        };
        Ok(skein_sim::Handle::new(self.files.lay(&items).raw()))
    }

    fn close_root(&mut self, root: skein_sim::Handle) {
        self.files.close(skein_fake_machine::Opened::new(root.raw()));
    }

    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        if let skein_sim::Ask::Spawn { program, env, .. } = &call.ask {
            self.spawns.push(Spawned { program: program.clone(), environment: env.clone() });
            if program.as_ref() == b"/bin/sh" {
                answers.push(skein_sim::Answer {
                    ticket: call.ticket,
                    result: Ok(skein_sim::Reply::Program(skein_sim::Program::Exit(0))),
                });
                return;
            }
        }
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

// Reads the actual closed append file; no service or simulator decision is consulted.
fn read_records(machine: &mut Machine) -> (Vec<smith_events::Record>, Box<[u8]>) {
    let mut records = Vec::new();
    let mut actual = Box::new([]) as Box<[u8]>;
    if let Some(index) = machine.trace_root {
        let root = machine.files.reopen_root(index);
        let file = machine
            .files
            .open(root, b"events.jsonl", skein_fake_machine::How::ReadNoFollow)
            .expect("settled trace exists");
        let size = u32::try_from(machine.files.stat(file).size).expect("finite trace bytes");
        let bytes = machine.files.read(file, 0, size).expect("read actual trace file");
        machine.files.close(file);
        machine.files.close(root);
        let events = crate::tracing::event_limits(&crate::limits());
        records = crate::events::read(&bytes, &events);
        actual = bytes.into();
    }
    (records, actual)
}

#[derive(Debug)]
struct Terminal {
    hang_up: bool,
}
impl skein_world::Expectation<Proc> for Terminal {
    fn check(&self, _now: Time, procs: &[Proc]) -> Result<bool, String> {
        let parent = procs.iter().find_map(|p| if let Proc::Parent(p) = p { Some(p) } else { None }).expect("parent");
        let answers = parent.peer.observed().iter().filter(|frame| frame.kind == 0x0110).count();
        if answers > 1 {
            return Err("more than one Answer".into());
        }
        Ok(if self.hang_up {
            answers == 0 && parent.status == Some(kernel::Exit::Code(1))
        } else {
            answers == 1 && parent.status == Some(kernel::Exit::Code(0))
        })
    }
    fn deadline(&self) -> Time {
        Time::from_nanos(Duration::from_secs(120).as_nanos())
    }
}
enum HangUp {
    Never,
    BeforeStart,
    MidCall,
}

#[derive(Clone)]
struct Release {
    name: Box<[u8]>,
    queries: usize,
    after: Duration,
    at: Option<Time>,
    released: bool,
}

#[derive(Clone, Copy)]
enum CredentialStory {
    Ordinary,
    Supplied,
    Refresh,
}

struct Referee {
    release: Option<Release>,
    cancel: bool,
    provider_rejected: bool,
    after_answer: u32,
    hang_up: HangUp,
    reviewed: usize,
    admitted_facts: usize,
    answer_facts: usize,
    full_completion: bool,
    tracing: Option<(TraceCase, u32)>,
    maximum_queries: usize,
    credential_story: CredentialStory,
    workspace_refusal: Option<smith_channel::InvalidStart>,
    expected: skein_world::Expectations<Proc, Terminal>,
}
impl skein_world::Referee<Proc> for Referee {
    fn act(&mut self, now: Time, procs: &mut [Proc]) {
        let mut release = None;
        if let Some(waiting) = &mut self.release {
            let queries = procs
                .iter()
                .find_map(|proc| match proc {
                    Proc::Peer(peer, _) => Some(fake::queries(peer).count()),
                    Proc::Parent(_) | Proc::Agent(..) | Proc::Tracing(_) => None,
                })
                .unwrap_or(0);
            if queries >= waiting.queries && waiting.at.is_none() {
                waiting.at = Some(now.saturating_add(waiting.after));
            }
            if !waiting.released && waiting.at.is_some_and(|at| at <= now) {
                waiting.released = true;
                release = Some(waiting.name.as_ref());
            }
        }
        let admitted = procs.iter().any(|p| matches!(p, Proc::Parent(parent) if parent.admitted()));
        let exited = procs.iter().any(|p| matches!(p, Proc::Parent(parent) if parent.status.is_some()));
        let calling = procs.iter().any(|p| matches!(p, Proc::Peer(peer, _) if fake::queries(peer).count() > 0));
        for proc in procs {
            match proc {
                Proc::Parent(parent) => {
                    if release.is_some() {
                        parent.frames_at_release = parent.peer.observed().len();
                    }
                    if matches!(self.hang_up, HangUp::MidCall)
                        && calling
                        && let Some(input) = parent.input.take()
                    {
                        parent.close(input);
                    }
                    if self.cancel && admitted {
                        parent.signal = parent.signal.max(1);
                    }
                    if parent.answered() && parent.status.is_none() {
                        parent.signal = parent.signal.max(self.after_answer);
                    }
                }
                Proc::Peer(peer, pending) => {
                    if let Some(name) = release {
                        *pending = Some(ReleaseName::new(name));
                    }
                    if exited {
                        peer.shutdown();
                    }
                }
                Proc::Agent(_, _) | Proc::Tracing(_) => {}
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Proc]) {
        let parent = procs.iter().find_map(|p| if let Proc::Parent(p) = p { Some(p) } else { None }).expect("parent");
        let answers = parent.peer.observed().iter().filter(|frame| frame.kind == 0x0110).count();
        assert!(answers <= 1, "one Answer for one Start");
        assert!(
            parent.peer.observed().iter().filter(|frame| frame.kind == 0x0106).count() <= 1,
            "one admission for one Start"
        );
        let peer = procs.iter().find_map(|p| if let Proc::Peer(p, _) = p { Some(p) } else { None }).expect("peer");
        assert!(fake::queries(peer).count() <= self.maximum_queries, "bounded completion scenario");
        let entrance_refused = self.workspace_refusal.is_some()
            || (matches!(self.credential_story, CredentialStory::Supplied) && !parent.admitted())
            || matches!(self.tracing, Some((TraceCase::Refused, _)));
        if matches!(self.hang_up, HangUp::Never) && parent.status.is_some() {
            if entrance_refused {
                assert!(!parent.admitted(), "an entrance refusal has no admission");
            } else {
                assert!(parent.admitted(), "the host observed admission; answer={:?}", parent.peer.observed());
            }
            let peer = procs.iter().find_map(|p| if let Proc::Peer(p, _) = p { Some(p) } else { None }).expect("peer");
            if matches!(self.tracing, Some((TraceCase::Parked, _))) {
                assert_eq!(fake::queries(peer).count(), 0, "empty zero-waiting work parks without a provider request");
            } else if self.provider_rejected || matches!(self.tracing, Some((TraceCase::ProviderRejected, _))) {
                assert!(fake::context_rejected(peer), "the independent provider rejected the actual request");
            } else if !self.cancel && !matches!(self.credential_story, CredentialStory::Refresh) && !entrance_refused {
                assert!(fake::replied(peer), "provider answered before agent exit");
            }
        }
        for frame in parent.peer.observed().get(self.reviewed..).expect("channel observations only grow") {
            if frame.kind == 0x010f {
                let fact =
                    smith_channel::Fact::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&frame.body))
                        .expect("host's validated Fact");
                match fact.kind() {
                    smith_channel::FactKind::Admitted => self.admitted_facts += 1,
                    smith_channel::FactKind::Ended => self.answer_facts += 1,
                    smith_channel::FactKind::LlmFinished => {
                        if self.full_completion {
                            assert_eq!(
                                fact.count(),
                                u64::from(crate::limits().llm.adapter.client.output_items),
                                "all maximum provider parts decoded"
                            );
                        }
                    }
                    smith_channel::FactKind::LlmStarted
                    | smith_channel::FactKind::LlmRetried
                    | smith_channel::FactKind::ToolStarted
                    | smith_channel::FactKind::ToolFinished
                    | smith_channel::FactKind::CheckStarted
                    | smith_channel::FactKind::CheckFinished
                    | smith_channel::FactKind::TextArrived => {}
                }
            }
        }
        if parent.is_empty() {
            for proc in procs {
                if let Proc::Agent(agent, errors) = proc {
                    assert!(agent.failure().is_none(), "diagnostic pipe succeeds");
                    assert_eq!(parent.errors, errors.bytes(), "the parent receives exactly the shipped writer's bytes");
                }
            }
        }
        self.reviewed = parent.peer.observed().len();
        assert!(self.admitted_facts <= 1 && self.answer_facts <= 1, "one fact terminal per activation");
        if matches!(self.hang_up, HangUp::Never)
            && parent.status.is_some()
            && self.tracing.is_none()
            && !entrance_refused
        {
            assert_eq!(
                parent.peer.observed().iter().filter(|frame| frame.kind == 0x0106).count(),
                1,
                "the reliable admission frame agrees with the host; its fact projection is best effort"
            );
        }
        self.expected.observe(now, procs);
    }
    fn next_deadline(&self) -> Option<Time> {
        let release = self.release.as_ref().filter(|release| !release.released).and_then(|release| release.at);
        self.expected.next_deadline().into_iter().chain(release).min()
    }
    fn overdue(&self, now: Time) -> Option<String> {
        self.expected.overdue(now)
    }
    fn passed(&self) -> bool {
        self.expected.passed()
    }
}

/// Scenario configuration and a settled shared-harness outcome.
/// The actual run terminal selected by the process scenario's inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceCase {
    /// The independent provider calls the granted finish tool.
    Accepted,
    /// The one-turn budget ends after a plaintext response.
    Failed,
    /// A charter model absent from configured declarations is refused before admission.
    Refused,
    /// A settled wait parks immediately under its admitted idle policy.
    Parked,
    /// The parent signals after observing admission.
    Cancelled,
    /// A real delegated child answers before main's accepted finish.
    ChildAnswered,
    /// One native completion calls three children together, each of which answers.
    ThreeChildren,
    /// The actual independent provider sends a typed HTTP context rejection.
    ProviderRejected,
    /// A read is told, then reasoning stops without a finish and needs a nudge.
    StoppedReasoning,
}

impl TraceCase {
    fn number(self) -> u8 {
        match self {
            Self::Accepted => 0,
            Self::Failed => 1,
            Self::Refused => 2,
            Self::Parked => 3,
            Self::Cancelled => 4,
            Self::ChildAnswered => 5,
            Self::ThreeChildren => 6,
            Self::ProviderRejected => 7,
            Self::StoppedReasoning => 8,
        }
    }

    fn peer(self, anthropic: bool, credential: skein_llm::Credential) -> skein_fake_peers::llm::Peer {
        use skein_fake_llm_domain::api;
        if self == Self::ChildAnswered || self == Self::ThreeChildren {
            return child_peer(anthropic, self == Self::ThreeChildren, credential);
        }
        if self == Self::StoppedReasoning {
            return stopped_reasoning_peer();
        }
        let turns = match self {
            Self::Accepted => vec![api::Turn {
                lines: Box::new([api::Line::Call {
                    name: b"finish".as_slice().into(),
                    arguments: br#"{"form":"report","text":"done","fields":{}}"#.as_slice().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            }],
            Self::Parked => vec![
                api::Turn {
                    lines: Box::new([api::Line::Call {
                        name: b"wait".as_slice().into(),
                        arguments: b"{}".as_slice().into(),
                    }]),
                    finish: api::Finish::ToolCalls,
                    tokens: 1,
                },
                api::Turn {
                    lines: Box::new([api::Line::Text { text: b"Waiting".as_slice().into() }]),
                    finish: api::Finish::Stop,
                    tokens: 1,
                },
            ],
            Self::Failed | Self::Refused | Self::Cancelled => return fake::peer_for(anthropic),
            Self::ProviderRejected => return fake::rejected(),
            Self::ChildAnswered | Self::ThreeChildren | Self::StoppedReasoning => {
                unreachable!("selected actual child scripts above")
            }
        };
        fake::configured(
            Box::new([api::Script { cue: Box::new([]), turns: turns.into() }]),
            skein_llm::Credential {
                value: b"token".as_slice().into(),
                account_id: b"acc".as_slice().into(),
                scheme: skein_llm::Scheme::Bearer,
            },
            Duration::ZERO,
        )
    }
}

fn child_peer(anthropic: bool, three: bool, credential: skein_llm::Credential) -> skein_fake_peers::llm::Peer {
    use skein_fake_llm_domain::api;
    let count = if three { 3 } else { 1 };
    let mut scripts = Vec::new();
    let mut calls = Vec::new();
    for index in 0..count {
        let cue = if three { format!("@event-child-{index}") } else { "@event-child".to_owned() };
        scripts.push(api::Script {
            cue: cue.clone().into_bytes().into_boxed_slice(),
            turns: Box::new([api::Turn {
                lines: Box::new([api::Line::Text { text: b"actual child answer".as_slice().into() }]),
                finish: api::Finish::Stop,
                tokens: 1,
            }]),
        });
        calls.push(api::Line::Call {
            name: b"sub_agent".as_slice().into(),
            arguments: format!("{{\"brief\":\"{cue}\",\"tools\":[],\"max_turns\":2,\"max_seconds\":30}}")
                .into_bytes()
                .into_boxed_slice(),
        });
    }
    scripts.push(api::Script {
        cue: b"@event-main".as_slice().into(),
        turns: Box::new([
            api::Turn { lines: calls.into_boxed_slice(), finish: api::Finish::ToolCalls, tokens: 1 },
            api::Turn {
                lines: Box::new([api::Line::Call {
                    name: b"finish".as_slice().into(),
                    arguments: br#"{"form":"report","text":"done","fields":{}}"#.as_slice().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            },
        ]),
    });
    if anthropic {
        fake::configured_anthropic(scripts.into_boxed_slice(), credential)
    } else {
        fake::configured(scripts.into_boxed_slice(), credential, Duration::ZERO)
    }
}

fn child_charter(bytes: &[u8]) -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(bytes))
        .expect("source charter")
        .into_parts();
    parts.instructions = b"@event-main".as_slice().into();
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 100;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("child budget");
    let mut models = skein_lib::List::with_capacity(1);
    models.push(parts.main.clone()).expect("one permitted child model");
    parts.models = models;
    let mut tools = parts.tools.into_parts();
    let mut families = tools.families.into_parts();
    families.skein_bools[0] = true;
    families.skein_bools[3] = true;
    tools.families = smith_charter::Families::new(&smith_charter::CEILINGS, families).expect("child authority");
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("tools");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("child charter");
    let mut writer = skein_lib::Writer::new(usize::try_from(charter.measure()).expect("wire size"));
    charter.encode(&mut writer).expect("wire");
    writer.finish()
}

fn environment_peer() -> skein_fake_peers::llm::Peer {
    use skein_fake_llm_domain::api;
    fake::configured(
        Box::new([api::Script {
            cue: Box::new([]),
            turns: Box::new([
                api::Turn {
                    lines: Box::new([api::Line::Call {
                        name: b"shell".as_slice().into(),
                        arguments: br#"{"command":"true"}"#.as_slice().into(),
                    }]),
                    finish: api::Finish::ToolCalls,
                    tokens: 1,
                },
                api::Turn {
                    lines: Box::new([api::Line::Text { text: b"done".as_slice().into() }]),
                    finish: api::Finish::Stop,
                    tokens: 1,
                },
            ]),
        }]),
        skein_llm::Credential {
            value: b"token".as_slice().into(),
            account_id: b"acc".as_slice().into(),
            scheme: skein_llm::Scheme::Bearer,
        },
        Duration::ZERO,
    )
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "independent cancellation, hangup, completion and environment scenarios"
)]
pub struct World {
    seed: u64,
    provider_rejected: bool,
    charter: Box<[u8]>,
    cancel: bool,
    after_answer: u32,
    hang_up: bool,
    hold_channel: bool,
    hang_up_mid_call: bool,
    memory: Memory,
    full_completion: bool,
    outcome: Option<Outcome<Proc, Machine>>,
    window: u32,
    tracing: Option<(TraceCase, u32)>,
    environment: bool,
    records: Vec<smith_events::Record>,
    trace_bytes: Box<[u8]>,
    message: Option<smith_channel::Message>,
    capture: smith_agent_service::sinks::Capture,
    anthropic: bool,
    private_grant: bool,
    grant: Option<smith_channel::GrantValue>,
    refreshing: Option<smith_channel::GrantValue>,
    openai: bool,
    live: Option<smith_channel::LiveAsk>,
    script: Option<Box<[skein_fake_llm_domain::api::Turn]>>,
    output: u32,
    release: Option<Release>,
    saved_window: Option<Vec<Box<[u8]>>>,
    root_path: Option<Box<[u8]>>,
}
impl World {
    /// Prepare one Start; the harness owns startup and settlement.
    #[must_use]
    pub fn new(seed: u64, charter: &[u8]) -> Self {
        Self {
            seed,
            provider_rejected: false,
            charter: charter.into(),
            cancel: false,
            after_answer: 0,
            hang_up: false,
            hold_channel: false,
            hang_up_mid_call: false,
            memory: Memory::Unchecked,
            full_completion: false,
            outcome: None,
            window: 8,
            tracing: None,
            environment: false,
            records: Vec::new(),
            trace_bytes: Box::new([]),
            message: None,
            capture: smith_agent_service::sinks::Capture::Everything,
            anthropic: false,
            private_grant: false,
            grant: None,
            refreshing: None,
            openai: false,
            live: None,
            script: None,
            output: 0,
            release: None,
            saved_window: None,
            root_path: None,
        }
    }

    /// Derive the scripted model's output capacity through the shipped profile.
    pub fn output_tokens(&mut self, output: u32) {
        self.output = output;
    }

    /// Restore independently observed turns through the host's Start frame.
    pub fn restore(&mut self, turns: Vec<Box<[u8]>>) {
        self.saved_window = Some(turns);
    }

    /// Send this root path in Start while adopting the fixture's supplied descriptors.
    pub fn root_path(&mut self, path: &[u8]) {
        self.root_path = Some(path.into());
    }

    /// Supply the provider's turns in place of the scenario's default script.
    pub fn script(&mut self, turns: Box<[skein_fake_llm_domain::api::Turn]>) {
        self.script = Some(turns);
    }

    /// Release one named hold after the given query has waited in virtual time.
    pub fn release_after(&mut self, name: &[u8], queries: usize, after: Duration) {
        self.release = Some(Release { name: name.into(), queries, after, at: None, released: false });
    }

    /// Channel frames the host had read when the held reply was released.
    #[must_use]
    pub fn frames_at_release(&self) -> &[skein_fake_channel::Observed] {
        self.observed().get(..self.parent().frames_at_release).expect("the observed prefix at release remains")
    }

    /// Put the host's live-content ask in its actual Start.
    pub fn ask(&mut self, text: bool, reasoning: bool, input: bool) {
        self.live = Some(
            smith_channel::LiveAsk::new(
                &smith_channel::CEILINGS,
                smith_channel::LiveAskParts { text, reasoning, input },
            )
            .expect("live ask"),
        );
    }

    /// Decode the live records on the parent's channel.
    #[must_use]
    pub fn pieces(&self) -> Vec<smith_channel::Live> {
        Self::decode_pieces(self.observed())
    }

    /// Decode the pieces already read before releasing the reply.
    #[must_use]
    pub fn pieces_at_release(&self) -> Vec<smith_channel::Live> {
        Self::decode_pieces(self.frames_at_release())
    }

    fn decode_pieces(frames: &[skein_fake_channel::Observed]) -> Vec<smith_channel::Live> {
        frames
            .iter()
            .filter(|frame| frame.kind == 0x0112)
            .map(|frame| {
                smith_channel::Live::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&frame.body))
                    .expect("host's live record")
            })
            .collect()
    }

    /// Keep the host's input open until the agent's exit is observed.
    pub fn hold_channel(&mut self) {
        self.hold_channel = true;
    }

    /// Make the independent byte provider refuse the actual model request.
    pub fn reject_provider(&mut self) {
        self.provider_rejected = true;
    }

    /// Hang up only once the provider has decoded an active call.
    pub fn hang_up_mid_call(&mut self) {
        self.hang_up_mid_call = true;
    }

    /// Send this concrete message in the actual channel Start.
    pub fn initial_message(&mut self, name: u64, label: &[u8], text: &[u8]) {
        self.message = Some(
            smith_channel::Message::new(
                &smith_channel::CEILINGS,
                smith_channel::MessageParts { finish_requested: false, name, label: label.into(), text: text.into() },
            )
            .expect("selected host message fits the real channel schema"),
        );
    }

    /// Select the actual event destination's capture policy before spawning.
    pub fn capture(&mut self, capture: smith_agent_service::sinks::Capture) {
        self.capture = capture;
    }

    /// Run the same actual agent against the independent Anthropic byte peer.
    pub fn anthropic(&mut self) {
        self.anthropic = true;
    }

    /// Give the actual Start and byte peer distinctive secret values for leak checks.
    pub fn private_grant(&mut self) {
        self.private_grant = true;
    }

    /// Grant a shell over startup roots and ask the peer to call it once.
    pub fn configured_environment(&mut self) {
        self.environment = true;
        let source =
            smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&self.charter))
                .expect("charter");
        let mut parts = source.into_parts();
        let mut budget = parts.budget.into_parts();
        budget.turns = 2;
        parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("two turns");
        let mut tools = parts.tools.into_parts();
        tools.families = smith_charter::Families::new(
            &smith_charter::CEILINGS,
            smith_charter::FamiliesParts { skein_bools: [true, false, true, false] },
        )
        .expect("shell family");
        parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("tools");
        let record = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("configured charter");
        let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).expect("charter bytes"));
        record.encode(&mut writer).expect("charter encoding");
        self.charter = writer.finish();
    }

    /// Machine-seam observations of commands spawned by the actual agent.
    #[must_use]
    pub fn command_environments(&self) -> Vec<Box<[Box<[u8]>]>> {
        self.outcome
            .as_ref()
            .expect("settled world")
            .machine
            .spawns
            .iter()
            .filter(|spawn| spawn.program.as_ref() == b"/bin/sh")
            .map(|spawn| spawn.environment.clone())
            .collect()
    }

    /// Bound outstanding turns while the independent host withholds acknowledgements.
    pub fn acknowledgement_window(&mut self, turns: u32) {
        self.window = turns;
    }

    /// Observe every native trace fact while constraining channel output slots.
    pub fn observe_facts(&mut self, case: TraceCase, frames: u32) {
        self.tracing = Some((case, frames));
        self.cancel = case == TraceCase::Cancelled;
        if case == TraceCase::ChildAnswered || case == TraceCase::ThreeChildren {
            self.charter = child_charter(&self.charter);
        }
        if case == TraceCase::Parked || case == TraceCase::Refused {
            let mut parts =
                smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&self.charter))
                    .expect("charter")
                    .into_parts();
            if case == TraceCase::Refused {
                let mut main = parts.main.into_parts();
                main.model = b"undeclared-model".as_slice().into();
                parts.main = smith_charter::Llm::new(&smith_charter::CEILINGS, main).expect("undeclared model");
            }
            let mut budget = parts.budget.into_parts();
            budget.turns = if case == TraceCase::Parked { 4 } else { 1 };
            parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("park budget");
            let mut tools = parts.tools.into_parts();
            tools.wait = case == TraceCase::Parked;
            parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("wait authority");
            parts.waiting = Duration::ZERO;
            let record = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("park charter");
            let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).expect("wire size"));
            record.encode(&mut writer).expect("wire");
            self.charter = writer.finish();
        }
    }

    fn observations(&self) -> &crate::tracing::Tracing {
        self.outcome
            .as_ref()
            .expect("settled world")
            .procs
            .iter()
            .find_map(|proc| if let Proc::Tracing(trace) = proc { Some(trace.as_ref()) } else { None })
            .expect("native trace observer")
    }

    /// Typed records decoded from the settled append file, independently of service state.
    #[must_use]
    pub fn records(&self) -> &[smith_events::Record] {
        &self.records
    }

    /// Exact append-file bytes read after the actual writer has closed.
    #[must_use]
    pub fn trace_bytes(&self) -> &[u8] {
        &self.trace_bytes
    }

    /// Check record pairings, per-type counts and actual response totals from the file.
    pub fn assert_event_conformance(&self) {
        crate::events::check(&self.records);
    }

    /// Facts refused by the channel projection, as counted by that component.
    #[must_use]
    pub fn channel_loss(&self) -> u64 {
        self.observations().channel_loss
    }

    /// Live bytes omitted from the owner's channel by the actual service.
    #[must_use]
    pub fn lost_live_bytes(&self) -> u64 {
        self.observations().lost_live_bytes
    }

    /// Destination loss reported in the actual last stream record.
    #[must_use]
    pub fn trace_loss(&self) -> u64 {
        let Some(smith_events::Record { event: smith_events::Event::SessionEnded(end), .. }) = self.records.last()
        else {
            panic!("session end is last")
        };
        end.loss.events
    }

    /// Request an actual pidfd termination signal after observed admission.
    pub fn signal(&mut self) {
        self.cancel = true;
    }
    /// Request one more actual termination signal after the parent's channel answer.
    pub fn signal_after_answer(&mut self) {
        self.after_answer = self.after_answer.checked_add(1).expect("bounded signal scenario");
        self.tracing = Some((TraceCase::Failed, 8));
    }

    /// The parent's actual submitted signal count.
    #[must_use]
    pub fn signals(&self) -> u32 {
        self.parent().signalled
    }

    /// Close the channel before Start, without signalling the process.
    pub fn hang_up(&mut self) {
        self.hang_up = true;
    }

    /// Ask the provider for the full configured decoded completion payload.
    pub fn full_completion(&mut self) {
        self.full_completion = true;
    }

    /// Meter every process iteration and drop with the shared allocator.
    pub fn check_memory(&mut self) {
        self.memory = Memory::Checked;
    }

    fn hold_peer(&self, mut peer: skein_fake_peers::llm::Peer) -> skein_fake_peers::llm::Peer {
        if self.provider_rejected {
            peer = fake::rejected();
        }
        if self.after_answer > 0 {
            peer.ignore_half_close();
        }
        peer
    }

    fn maximum_queries(&self) -> usize {
        let scenario = if matches!(self.tracing, Some((TraceCase::ThreeChildren, _))) {
            5
        } else if matches!(self.tracing, Some((TraceCase::ChildAnswered | TraceCase::StoppedReasoning, _))) {
            3
        } else if self.environment || self.tracing.is_some() || self.saved_window.is_some() {
            2
        } else {
            1
        };
        scenario.max(self.release.as_ref().map_or(0, |release| release.queries))
    }
    fn hang_up_case(&self) -> HangUp {
        match (self.hang_up, self.hang_up_mid_call) {
            (true, _) => HangUp::BeforeStart,
            (false, true) => HangUp::MidCall,
            (false, false) => HangUp::Never,
        }
    }

    fn referee(&self) -> Referee {
        Referee {
            release: self.release.clone(),
            cancel: self.cancel,
            provider_rejected: self.provider_rejected,
            after_answer: self.after_answer,
            hang_up: self.hang_up_case(),
            reviewed: 0,
            admitted_facts: 0,
            answer_facts: 0,
            full_completion: self.full_completion,
            tracing: self.tracing,
            maximum_queries: self.maximum_queries(),
            workspace_refusal: self
                .root_path
                .as_ref()
                .filter(|path| path.first() != Some(&b'/'))
                .map(|_| smith_channel::InvalidStart::Workspace),
            credential_story: if self.refreshing.is_some() {
                CredentialStory::Refresh
            } else if self.grant.is_some() {
                CredentialStory::Supplied
            } else {
                CredentialStory::Ordinary
            },
            expected: skein_world::Expectations::new(
                self.seed,
                vec![Terminal { hang_up: self.hang_up || self.hang_up_mid_call || self.refreshing.is_some() }],
            ),
        }
    }

    /// Run until the one Answer and complete descriptor settlement.
    pub fn settle(&mut self) -> bool {
        let mut config = skein_sim::Config::calm();
        config.wall = skein_tls_world::pki::VALID;
        let mut machine = skein_fake_machine::Machine::new();
        let root = machine.lay(&[]);
        let referee = self.referee();
        let mut world = skein_world::World::new(self.seed, config, referee, self.memory).with_machine(Machine {
            files: machine,
            spawns: Vec::new(),
            next_root: 1,
            trace_root: None,
            problem_file: self.root_path.is_some() || matches!(self.tracing, Some((TraceCase::StoppedReasoning, _))),
        });
        world.host_roots(
            HostedProgram {
                program: b"smith".as_slice().into(),
                make: if self.tracing.is_some() { make_tracing } else { make },
                instances: 1,
                operations: crate::limits().routes.checked_add(8).expect("routes, startup trace and inherited closes"),
            },
            unused_roots,
        );
        world.spawn_root(skein_sim::Handle::new(root.raw()), |root| {
            let mut parent = Parent::new(
                root,
                &self.charter,
                self.hang_up,
                self.tracing,
                self.window,
                self.environment,
                self.message.as_ref(),
                self.capture,
                if self.openai {
                    skein_llm::Provider::OpenAi
                } else if self.anthropic {
                    skein_llm::Provider::Anthropic
                } else {
                    skein_llm::Provider::OpenAiCodex
                },
                &self.grant_value(),
                self.live.clone(),
                self.output,
            );
            if let Some(path) = &self.root_path {
                parent.peer = crate::host();
                parent.peer.play(crate::start_root(&self.charter, path));
                parent.program_arguments(Box::new([b"roots".as_slice().into()]), None);
            }
            if let Some(turns) = &self.saved_window {
                parent.peer = crate::host();
                parent.peer.play(crate::program_world::start_with_window(&self.charter, &self.grant_value(), turns));
            }
            parent.hold_channel = self.hold_channel;
            parent.refreshing.clone_from(&self.refreshing);
            Proc::Parent(Box::new(parent))
        });
        world.spawn(|| Proc::Peer(Box::new(self.hold_peer(self.make_peer())), None));

        let mut outcome = world.run();
        (self.records, self.trace_bytes) = read_records(&mut outcome.machine);
        self.outcome = Some(outcome);
        self.parent().status
            == Some(kernel::Exit::Code(u8::from(self.hang_up || self.hang_up_mid_call || self.refreshing.is_some())))
    }
    fn make_peer(&self) -> skein_fake_peers::llm::Peer {
        if let Some(turns) = &self.script {
            fake::scripted(turns.clone(), self.credential(), self.anthropic)
        } else if self.refreshing.is_some() {
            fake::configured(
                Box::new([skein_fake_llm_domain::api::Script {
                    cue: Box::new([]),
                    turns: Box::new([skein_fake_llm_domain::api::Turn {
                        lines: Box::new([skein_fake_llm_domain::api::Line::Text { text: Box::from(&b"Hello"[..]) }]),
                        finish: skein_fake_llm_domain::api::Finish::Stop,
                        tokens: 1,
                    }]),
                }]),
                crate::scenario_credential(false, false),
                Duration::from_secs(1),
            )
        } else if self.environment {
            environment_peer()
        } else if self.full_completion {
            fake::configured(
                Box::new([skein_fake_llm_domain::api::Script {
                    cue: Box::new([]),
                    turns: Box::new([skein_fake_llm_domain::api::Turn {
                        lines: (0..crate::limits().llm.adapter.client.output_items)
                            .map(|index| skein_fake_llm_domain::api::Line::Text {
                                // First response metadata is item_1_<index> and final_answer.
                                text: vec![
                                    b'x';
                                    usize::try_from(
                                        crate::limits().llm.adapter.client.answer
                                            / crate::limits().llm.adapter.client.output_items
                                            + u32::from(
                                                index
                                                    < crate::limits().llm.adapter.client.answer
                                                        % crate::limits().llm.adapter.client.output_items
                                            )
                                    )
                                    .expect("bounded text per part")
                                        - format!("item_1_{index}").len()
                                        - b"final_answer".len()
                                        - 9
                                ]
                                .into_boxed_slice(),
                            })
                            .collect(),
                        finish: skein_fake_llm_domain::api::Finish::Stop,
                        tokens: 1,
                    }]),
                }]),
                skein_llm::Credential {
                    value: b"token".as_slice().into(),
                    account_id: b"acc".as_slice().into(),
                    scheme: skein_llm::Scheme::Bearer,
                },
                Duration::ZERO,
            )
        } else if let Some((case, _)) = self.tracing {
            case.peer(self.anthropic, self.credential())
        } else {
            fake::peer()
        }
    }
    fn parent(&self) -> &Parent {
        self.outcome
            .as_ref()
            .expect("settled world")
            .procs
            .iter()
            .find_map(|p| if let Proc::Parent(p) = p { Some(p.as_ref()) } else { None })
            .expect("parent survives")
    }
    fn peer(&self) -> &skein_fake_peers::llm::Peer {
        self.outcome
            .as_ref()
            .expect("settled world")
            .procs
            .iter()
            .find_map(|p| if let Proc::Peer(p, _) = p { Some(p.as_ref()) } else { None })
            .expect("peer survives")
    }
    /// Validated frames seen by the scripted host.
    #[must_use]
    pub fn observed(&self) -> &[skein_fake_channel::Observed] {
        self.parent().peer.observed()
    }
    /// The host's terminal Answer, independently decoded.
    #[must_use]
    pub fn answer(&self) -> Option<smith_channel::Answer> {
        self.observed().iter().find(|frame| frame.kind == 0x0110).map(|frame| {
            smith_channel::Answer::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&frame.body))
                .expect("validated Answer")
        })
    }
    /// The independent provider's observations.
    #[must_use]
    pub fn peer_observations(&self) -> &[skein_fake_peers::llm::Observation] {
        self.peer().observations()
    }
    /// Whether the provider emitted a successful terminal.
    #[must_use]
    pub fn peer_replied(&self) -> bool {
        fake::replied(self.peer())
    }
    /// Independently decoded model requests.
    #[must_use]
    pub fn peer_queries(&self) -> Vec<&skein_fake_llm_domain::api::Query> {
        fake::queries(self.peer()).collect()
    }
    /// Decoded provider parts and aggregate text/replay-token bytes in the host Turn.
    #[must_use]
    pub fn provider_answer_shape(&self) -> (usize, usize) {
        let mut count = 0;
        let mut bytes = 0;
        for frame in self.observed().iter().filter(|frame| frame.kind == 0x0109) {
            let envelope =
                smith_channel::Turn::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&frame.body))
                    .expect("validated turn envelope");
            let turn = smith_transcript::Turn::decode(
                &smith_transcript::CEILINGS,
                &mut skein_lib::Reader::new(envelope.body()),
            )
            .expect("validated transcript");
            for message in turn.messages() {
                if *message.role() == smith_transcript::Role::Assistant {
                    for block in message.blocks() {
                        if let smith_transcript::Block::Text(text) = block {
                            count += 1;
                            bytes += text.text().len();
                            let metadata = skein_llm::Replay::from_bytes(
                                text.replay().as_ref().expect("text replay").bytes(),
                                &crate::limits().llm.adapter.client.native().document(),
                            )
                            .expect("provider metadata");
                            let document = metadata.value.document();
                            for index in 0..document.len() {
                                let token = document.token(index).expect("within provider metadata");
                                bytes += match token.kind {
                                    skein_json::Kind::Long => panic!("owned metadata does not skip a scalar"),
                                    skein_json::Kind::Key | skein_json::Kind::String | skein_json::Kind::Number => {
                                        document.text(token).expect("text-bearing metadata record").len()
                                    }
                                    skein_json::Kind::ObjectStart
                                    | skein_json::Kind::ObjectEnd
                                    | skein_json::Kind::ArrayStart
                                    | skein_json::Kind::ArrayEnd
                                    | skein_json::Kind::True
                                    | skein_json::Kind::False
                                    | skein_json::Kind::Null => 1,
                                };
                            }
                        }
                    }
                }
            }
        }
        (count, bytes)
    }

    /// Settlement already checked all descriptors and in-flight operations.
    pub fn assert_agent_clean(&self) {
        assert!(self.outcome.as_ref().expect("settled world").killed.is_empty());
    }
    /// The process status actually received by its parent.
    #[must_use]
    pub fn exit(&self) -> Option<kernel::Exit> {
        self.parent().status
    }

    /// Diagnostic bytes actually read from the child's standard-error pipe.
    #[must_use]
    pub fn errors(&self) -> Vec<u8> {
        self.parent().errors.clone()
    }

    /// Whether the scripted host sent a termination signal.
    #[must_use]
    pub fn signalled(&self) -> bool {
        self.parent().signalled > 0
    }

    /// Complete kernel trace for the generic replay assertion.
    #[must_use]
    pub fn trace(&self) -> Vec<String> {
        self.outcome.as_ref().expect("settled world").trace.iter().map(|entry| format!("{entry:?}")).collect()
    }
}

fn stopped_reasoning_peer() -> skein_fake_peers::llm::Peer {
    use skein_fake_llm_domain::api;
    fake::reasoning(Box::new([api::Script {
        cue: Box::new([]),
        turns: Box::new([
            api::Turn {
                lines: Box::new([api::Line::Call {
                    name: b"read".as_slice().into(),
                    arguments: br#"{"path":"problem.txt"}"#.as_slice().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            },
            api::Turn {
                lines: Box::new([
                    api::Line::Opaque {
                        bytes: format!(
                            r#"{{"id":"r2","type":"reasoning","encrypted_content":"{}","summary":[]}}"#,
                            "r".repeat(43_688)
                        )
                        .into_bytes()
                        .into(),
                    },
                    api::Line::Opaque {
                        bytes: format!(
                            r#"{{"id":"r3","type":"reasoning","encrypted_content":"{}","summary":[]}}"#,
                            "r".repeat(43_688)
                        )
                        .into_bytes()
                        .into(),
                    },
                    api::Line::Opaque {
                        bytes: format!(
                            r#"{{"id":"{}","type":"message","phase":"final_answer","role":"assistant","content":[{{"type":"output_text","text":"{}","annotations":[]}}]}}"#,
                            "m".repeat(54),
                            "t".repeat(903)
                        )
                        .into_bytes()
                        .into(),
                    },
                ]),
                finish: api::Finish::Stop,
                tokens: 1,
            },
            api::Turn {
                lines: Box::new([api::Line::Call {
                    name: b"finish".as_slice().into(),
                    arguments: br#"{"form":"report","text":"REASONING COMPLETE","fields":{}}"#.as_slice().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            },
        ]),
    }]))
}

mod grants;
