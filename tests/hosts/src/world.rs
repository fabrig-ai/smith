//! The sample host scenario on Skein's process harness (testing.md, sections
//! 2.1 and 5). It keeps only configuration and the settled outcome; scheduling,
//! hosted-child ownership and teardown belong to Skein. The referee watches
//! parent observations and requests shutdown or scripted child faults.
use skein_fake_llm_domain::api;
use skein_io::kernel;
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_shell::Host;
use skein_world::{HostedProgram, Inherited, Memory, Outcome};
use smith_agent_process_world as fixture;
use smith_agent_shell::Agent;
use std::os::unix::ffi::OsStrExt;

use crate::{HostService, Observation, Program, Seen, referee};

/// One scenario process, driven by either backend until its resources settle.
enum Proc {
    /// The scripted parent sends Start and watches the child until Gone.
    Parent(Box<HostService>),
    /// The actual agent service receives inherited streams until its exit.
    Agent { agent: Box<skein_world::PipeHost<Agent>>, errors: fixture::AgentErrors, paused: bool },
    /// The independent fake provider answers requests until shutdown.
    Peer(Box<skein_fake_peers::llm::Peer>),
    /// The failing startup writes stderr and waits to be killed.
    Error(Box<ErrorTail>),
}

impl Proc {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Parent(parent) => parent.as_ref(),
            Self::Agent { agent, .. } => agent.as_ref(),
            Self::Peer(peer) => peer.as_ref(),
            Self::Error(error) => error.as_ref(),
        }
    }

    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Parent(parent) => parent.as_mut(),
            Self::Agent { agent, .. } => agent.as_mut(),
            Self::Peer(peer) => peer.as_mut(),
            Self::Error(error) => error.as_mut(),
        }
    }
}

impl Host for Proc {
    fn drain(&mut self) {
        if !matches!(self, Self::Agent { paused: true, .. }) {
            self.host_mut().drain();
        }
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        if !matches!(self, Self::Agent { paused: true, .. }) {
            self.host_mut().iterate(now, wall);
        }
    }

    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.host_mut().completions()
    }

    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.host_mut().submissions()
    }

    fn work_pending(&self, now: Time) -> bool {
        !matches!(self, Self::Agent { paused: true, .. }) && self.host().work_pending(now)
    }

    fn next_deadline(&self) -> Option<Time> {
        if matches!(self, Self::Agent { paused: true, .. }) { None } else { self.host().next_deadline() }
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        if matches!(self, Self::Agent { paused: true, .. }) { None } else { self.host().next_policy_deadline() }
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

impl Host for HostService {
    fn iterate(&mut self, now: Time, wall: Wall) {
        HostService::iterate(self, now, wall);
    }

    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, now: Time) -> bool {
        HostService::work_pending(self, now)
    }

    fn next_deadline(&self) -> Option<Time> {
        HostService::next_deadline(self)
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        HostService::next_policy_deadline(self)
    }

    fn is_empty(&self) -> bool {
        self.seen.gone.is_some()
            && self.root_closed
            && self.io.is_empty()
            && !self.work_pending(self.io_env.now)
            && self.submissions.is_empty()
    }

    fn worst_case(&self) -> u64 {
        smith_host_domain::worst_case(&self.domain_env.limits)
            .and_then(|bound| bound.checked_add(skein_io::worst_case(&self.io_env.limits)?))
            .and_then(|bound| {
                bound.checked_add(smith_host_protocol::process_worst_case(
                    &self.process_limits,
                    self.domain_env.limits.detail_bytes,
                )?)
            })
            // Eight finite boundary queues may retain maximal charter/turn payloads.
            .and_then(|bound| bound.checked_add(134_217_728))
            .and_then(|bound| bound.checked_add(u64::try_from(size_of::<Self>()).ok()?))
            .and_then(|bound| {
                bound.checked_add(self.launch.as_ref().map_or(0, |launch| {
                    let strings = launch
                        .program
                        .len()
                        .checked_add(launch.directory.len())
                        .and_then(|bytes| {
                            launch
                                .arguments
                                .iter()
                                .chain(&launch.environment)
                                .try_fold(bytes, |bytes, argument| bytes.checked_add(argument.len()))
                        })
                        .expect("bounded launch strings");
                    let slots = launch
                        .arguments
                        .len()
                        .checked_add(launch.environment.len())
                        .and_then(|slots| slots.checked_mul(size_of::<Box<[u8]>>()))
                        .expect("bounded launch slots");
                    u64::try_from(strings.checked_add(slots).expect("bounded launch ownership")).expect("launch memory")
                }))
            })
            .expect("complete parent bound")
    }

    fn operations(&self) -> u32 {
        skein_io::operations(&self.io_env.limits)
            .and_then(|operations| operations.checked_add(1))
            .expect("parent operations")
    }
}

struct ErrorTail {
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    error: kernel::Fd,
    offset: u32,
    in_flight: bool,
}

impl Host for ErrorTail {
    fn iterate(&mut self, _now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            let Ok(kernel::Done::Count(bytes)) = complete.result else { unreachable!("stderr write completes") };
            self.offset = self.offset.checked_add(bytes).expect("bounded stderr offset");
            self.in_flight = false;
        }
        if self.offset < 26 && !self.in_flight {
            self.submissions.push(kernel::Submit {
                op: Token::new(1),
                kind: kernel::Op::PipeWrite {
                    fd: self.error,
                    bytes: b"agent configuration failed".as_slice().into(),
                    from: self.offset,
                },
            });
            // One write remains in flight until its completion returns.
            self.in_flight = true;
        }
    }

    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty() || (self.offset < 26 && !self.in_flight)
    }

    fn next_deadline(&self) -> Option<Time> {
        None
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }

    fn is_empty(&self) -> bool {
        false
    }

    fn worst_case(&self) -> u64 {
        4096
    }

    fn operations(&self) -> u32 {
        1
    }
}

fn make_agent(spawn: &kernel::Spawn, inherited: &Inherited) -> Proc {
    let fd = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1;
    let (errors, capture) = fixture::AgentErrors::pipe(fd);
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(
        spawn.args.first().expect("generated configuration argument"),
    ));
    let configuration = smith_agent_shell::config::read(path).expect("generated agent configuration");
    let agent = Agent::new(
        configuration,
        smith_agent_shell::Resources {
            programs: None,
            input: inherited.pipes.iter().find(|(child, _)| *child == 0).expect("stdin").1,
            output: inherited.pipes.iter().find(|(child, _)| *child == 1).expect("stdout").1,
            error: Some(inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1),
            signals: inherited.signal,
            seed: 7,
            pid: 101,
            trace: None,
            roots: Some(inherited.roots.iter().map(|(_, fd)| *fd).collect()),
        },
        Box::new(errors.clone()),
    )
    .expect("spawned agent");
    Proc::Agent { agent: Box::new(capture.host(agent)), errors, paused: false }
}

fn make_error(_spawn: &kernel::Spawn, inherited: &Inherited) -> Proc {
    let error = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1;
    Proc::Error(Box::new(ErrorTail {
        submissions: Queue::with_capacity(1),
        completions: Queue::with_capacity(1),
        error,
        offset: 0,
        in_flight: false,
    }))
}

struct Machine {
    files: skein_fake_machine::Machine,
    program: Program,
}

impl skein_world::Machine for Machine {
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        if matches!(&call.ask, skein_sim::Ask::Spawn { program, .. } if program.as_ref() == b"smith-agent") {
            let result = match self.program {
                Program::Refused => Err(kernel::Error::NotFound),
                Program::Silent => Ok(skein_sim::Reply::Program(skein_sim::Program::Never)),
                Program::Service | Program::ErrorTail => unreachable!("registered factory handles this spawn"),
            };
            answers.push(skein_sim::Answer { ticket: call.ticket, result });
        } else {
            skein_fake_machine::step(&mut self.files, call, answers);
        }
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    Plain,
    IgnoreCancel,
    Crash(u32),
}

#[derive(Clone)]
struct Release {
    name: Box<[u8]>,
    queries: usize,
    after: Duration,
    at: Option<Time>,
    released: bool,
}

struct Referee {
    release: Option<Release>,
    seed: u64,
    scenario: Scenario,
    iteration: u32,
    started: Option<u32>,
    injected: bool,
    reviewed: usize,
    meeting: skein_world::domain::Referee<referee::Meeting>,
}

impl skein_world::Referee<Proc> for Referee {
    fn act(&mut self, now: Time, procs: &mut [Proc]) {
        if let Some(release) = &mut self.release {
            for proc in &mut *procs {
                if let Proc::Peer(peer) = proc {
                    if fixture::fake::queries(peer).count() >= release.queries && release.at.is_none() {
                        release.at = Some(now.saturating_add(release.after));
                    }
                    if !release.released && release.at.is_some_and(|at| now >= at) {
                        peer.release(&release.name).expect("named hold fits the peer");
                        release.released = true;
                    }
                }
            }
        }
        self.iteration = self.iteration.checked_add(1).expect("finite scenario");
        let parent = procs
            .iter()
            .find_map(|proc| match proc {
                Proc::Parent(parent) => Some(parent),
                Proc::Agent { .. } | Proc::Peer(_) | Proc::Error(_) => None,
            })
            .expect("parent");
        let gone = parent.seen.gone.is_some();
        let admitted = parent.seen.admitted;
        if parent.seen.started && self.started.is_none() {
            self.started = Some(self.iteration);
        }
        let crash = match (self.scenario, self.started) {
            (Scenario::Crash(cut), Some(started)) => {
                !gone && !self.injected && self.iteration.checked_sub(started).expect("Start precedes cut") >= cut
            }
            (Scenario::Plain | Scenario::IgnoreCancel, _) | (Scenario::Crash(_), None) => false,
        };
        let pause = matches!(self.scenario, Scenario::IgnoreCancel) && admitted && !self.injected;
        for proc in procs {
            match proc {
                Proc::Parent(parent) => {
                    if crash {
                        parent.crash_requested = true;
                    }
                    if pause {
                        parent.stop_requested = true;
                    }
                }
                Proc::Agent { paused, .. } => {
                    *paused |= pause;
                }
                Proc::Peer(peer) => {
                    if gone {
                        peer.shutdown();
                    }
                }
                Proc::Error(_) => {}
            }
        }
        self.injected |= crash || pause;
    }

    fn observe(&mut self, now: Time, procs: &[Proc]) {
        let parent = procs
            .iter()
            .find_map(|proc| match proc {
                Proc::Parent(parent) => Some(parent),
                Proc::Agent { .. } | Proc::Peer(_) | Proc::Error(_) => None,
            })
            .expect("parent");
        let observations = parent.observations();
        for observation in observations.get(self.reviewed..).expect("observations only grow") {
            self.meeting.observe(now, *observation, &mut Vec::new());
        }
        self.reviewed = observations.len();
        if parent.seen.gone.is_some() {
            for proc in procs {
                if let Proc::Agent { errors, .. } = proc {
                    referee::review_agent_errors(
                        &errors.bytes(),
                        parent.seen.detail.as_deref().expect("Gone detail"),
                        parent.seen.answered.is_some(),
                    );
                }
            }
        }
        self.meeting.fire(now, &mut Vec::new());
        if matches!(self.meeting.verdict(), skein_world::domain::Verdict::Failed(_)) {
            self.meeting.assert_passed(self.seed);
        }
    }

    fn next_deadline(&self) -> Option<Time> {
        let release = self.release.as_ref().filter(|release| !release.released).and_then(|release| release.at);
        self.meeting.next_deadline().into_iter().chain(release).min()
    }

    fn overdue(&self, now: Time) -> Option<String> {
        if !self.passed() && now >= Time::from_nanos(Duration::from_secs(120).as_nanos()) {
            Some("host releases its slot".into())
        } else {
            None
        }
    }

    fn passed(&self) -> bool {
        self.reviewed > 0 && matches!(self.meeting.verdict(), skein_world::domain::Verdict::Passed)
    }
}

/// A configured scenario and its settled outcome; the harness owns its loop.
#[expect(missing_debug_implementations, reason = "the hosted outcome owns an independent non-Debug peer")]
pub struct World {
    seed: u64,
    program: Program,
    scenario: Scenario,
    memory: Memory,
    outcome: Option<Outcome<Proc, Machine>>,
    live: Option<smith_host_domain::channel::LiveAsk>,
    script: Option<Box<[api::Turn]>>,
    release: Option<Release>,
}

impl World {
    #[must_use]
    pub fn new(seed: u64, program: Program) -> Self {
        Self {
            seed,
            program,
            scenario: Scenario::Plain,
            memory: Memory::Unchecked,
            outcome: None,
            live: None,
            script: None,
            release: None,
        }
    }

    /// Put the owner's live-content request in the actual host Start.
    pub fn ask(&mut self, text: bool, reasoning: bool, input: bool) {
        self.live = Some(smith_host_domain::channel::LiveAsk { text, reasoning, input });
    }

    /// Give the independent peer the scenario's scripted reply.
    pub fn script(&mut self, turns: Box<[api::Turn]>) {
        self.script = Some(turns);
    }

    /// Release the named hold after the query has waited in virtual time.
    pub fn release_after(&mut self, name: &[u8], queries: usize, after: Duration) {
        self.release = Some(Release { name: name.into(), queries, after, at: None, released: false });
    }

    /// After admission, pause the agent and request Cancel from its parent.
    pub fn ignore_cancel(&mut self) {
        self.scenario = Scenario::IgnoreCancel;
    }

    /// Kill the child at this iteration offset from observed parent Start.
    pub fn crash_after(&mut self, cut: u32) {
        self.scenario = Scenario::Crash(cut);
    }

    /// Check each process at every iteration using the shared allocator.
    pub fn check_memory(&mut self) {
        self.memory = Memory::Checked;
    }

    /// Run the shared harness through complete process and descriptor settlement.
    pub fn settle(&mut self) {
        let scratch = smith_local_process_world::Files::new();
        let scenario = smith_local_process_world::world::World::new(
            self.seed,
            &scratch,
            smith_local_process_world::Placement::Spawned,
            b"First answer",
        )
        .scenario;
        let document = scratch.path().join("agent.json");
        std::fs::write(
            &document,
            serde_json::to_vec(&smith_local_process_world::process::agent_document(&scenario.launch))
                .expect("generated JSON"),
        )
        .expect("agent configuration");
        let kind = smith_local_shell::compose::AgentKind::Spawned { program: "smith-agent".into() };
        let mut config = skein_sim::Config::calm();
        config.wall = skein_tls_world::pki::VALID;
        let mut machine = skein_fake_machine::Machine::new();
        let root = machine.lay(&[]);
        let referee = Referee {
            release: self.release.clone(),
            seed: self.seed,
            scenario: self.scenario,
            iteration: 0,
            started: None,
            injected: false,
            reviewed: 0,
            meeting: referee::review(&[]),
        };
        let mut harness = skein_world::World::new(self.seed, config, referee, self.memory)
            .with_machine(Machine { files: machine, program: self.program });
        if self.program == Program::Service || self.program == Program::ErrorTail {
            harness.host(HostedProgram {
                program: b"smith-agent".as_slice().into(),
                make: if self.program == Program::Service { make_agent } else { make_error },
                instances: 1,
                operations: fixture::limits().routes.checked_add(2).expect("agent operations"),
            });
        }
        harness.spawn_root(skein_sim::Handle::new(root.raw()), |root| {
            Proc::Parent(Box::new(HostService::new(
                kind.launch(root, &document),
                Time::ZERO,
                config.wall,
                if self.program == Program::Service { 4096 } else { super::host_limits().detail_bytes },
                self.live,
            )))
        });
        harness.spawn(|| {
            let peer = match &self.script {
                Some(turns) => fixture::fake::scripted_reply(turns.clone(), false),
                None => fixture::fake::peer(),
            };
            Proc::Peer(Box::new(peer))
        });
        self.outcome = Some(harness.run());
    }

    fn parent(&self) -> &HostService {
        self.outcome
            .as_ref()
            .expect("settled world")
            .procs
            .iter()
            .find_map(|proc| match proc {
                Proc::Parent(parent) => Some(parent.as_ref()),
                Proc::Agent { .. } | Proc::Peer(_) | Proc::Error(_) => None,
            })
            .expect("parent survives")
    }
    #[must_use]
    pub fn seen(&self) -> &Seen {
        self.parent().seen()
    }
    #[must_use]
    pub fn observations(&self) -> &[Observation] {
        self.parent().observations()
    }
    #[must_use]
    pub fn peer_replied(&self) -> bool {
        self.outcome.as_ref().expect("settled world").procs.iter().any(|proc| match proc {
            Proc::Peer(peer) => fixture::fake::replied(peer),
            Proc::Parent(_) | Proc::Agent { .. } | Proc::Error(_) => false,
        })
    }
    #[must_use]
    pub fn crashed(&self) -> bool {
        self.parent().crashed
    }
    #[must_use]
    pub fn iterations(&self) -> u32 {
        self.outcome.as_ref().expect("settled world").iterations
    }
    #[must_use]
    pub fn trace(&self) -> Vec<String> {
        self.outcome.as_ref().expect("settled world").trace.iter().map(|entry| format!("{entry:?}")).collect()
    }
}
