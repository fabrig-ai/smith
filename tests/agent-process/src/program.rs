//! Hosted program starts beside the shipped agent and relay (programs.md, 3, 5).
//! The shared channel parent speaks only wire records; fixture files belong to
//! the simulated machine, never a person's home or configuration directory.

use skein_fake_machine::{How, Item, Machine as Files, Opened};
use skein_io::kernel::{self, Exit, Spawn};
use skein_lib::{Duration, Queue, Reader, Secret, Time, Wall};
use skein_shell::Host;
use skein_world::{HostedProgram, Inherited, Referee};
use smith_fake_claude::{Fake, Story};

use crate::world::Parent;

#[path = "program/waiting.rs"]
mod waiting;

#[path = "program/carry.rs"]
mod carry;
pub use carry::{from_frames, start_with_window};
pub(crate) use waiting::message;
pub use waiting::{HostMessage, When};

/// One startup scenario encoded in the hosted agent's arguments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Case {
    /// Start the signed-in fake with the approved program binding.
    #[default]
    Ordinary,
    /// Let a scripted conversation reach its domain terminal.
    Turns,
    /// Finish with competing host login variables in the configured environment.
    Credentials,
    /// Hold wait until the host brings a message.
    Waiting,
    /// Leave wait held until the idle policy parks the run.
    ParkWaiting,
    /// Run workspace built-ins with a writable cwd and a read-only library.
    Tools,
    /// Record a program conversation with full content capture.
    Events,
    /// Keep an Open conversation across replies and scripted host messages.
    Open,
    /// The main endpoint does not select the configured program.
    Unbound,
    /// The programs directory could not be opened by the shell.
    MissingRoot,
    /// The fake exits before emitting its init.
    EarlyExit,
    /// The relay exits before connecting the program to smith's server.
    MissingServer,
    /// The host brings prior turns to a program start.
    Transcript,
}

enum Proc {
    Parent(Box<Parent>),
    Agent(Box<skein_world::PipeHost<smith_agent_shell::Agent>>),
    Fake(Box<Fake>),
    Relay { process: Box<smith_agent_shell::relay::Process>, environment: Vec<String> },
}

impl Proc {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Parent(host) => host.as_ref(),
            Self::Agent(host) => host.as_ref(),
            Self::Fake(host) => host.as_ref(),
            Self::Relay { process, .. } => process.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Parent(host) => host.as_mut(),
            Self::Agent(host) => host.as_mut(),
            Self::Fake(host) => host.as_mut(),
            Self::Relay { process, .. } => process.as_mut(),
        }
    }
}

impl Host for Proc {
    fn drain(&mut self) {
        self.host_mut().drain();
    }
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.host_mut().submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.host().work_pending(now)
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
    fn exit(&self) -> Option<Exit> {
        self.host().exit()
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case().checked_add(4096).expect("wrapper memory")
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

fn configuration(case: Case) -> smith_agent_service::Config {
    let mut config = if case == Case::Events {
        let mut limits = crate::limits();
        limits.llm.adapter.client.request = 1_048_576;
        crate::configuration_with_limits(skein_llm::Provider::OpenAiCodex, &limits)
    } else {
        crate::configuration()
    };
    config.domain.programs = if case == Case::Unbound {
        Box::default()
    } else {
        Box::new([smith_domain::Program {
            endpoint: smith_domain::run::charter::Endpoint(0),
            name: b"claude_code".as_slice().into(),
        }])
    };
    config.limits.io.close_timeout = Duration::from_millis(10);
    config.program = Some(smith_agent_service::ProgramBackend {
        program: smith_protocol_program::Programs {
            claude: b"/programs/claude".as_slice().into(),
            directory: b"/programs".as_slice().into(),
        },
        endpoints: if case == Case::Unbound { Box::default() } else { Box::new([0]) },
        relay: b"/programs/smith-agent".as_slice().into(),
        entropy: Secret::new(Box::new([7; 32])),
        effective_user: 1000,
    });
    let mut variables = vec![
        smith_protocol_machine::Variable {
            name: b"WAIT_PROOF".as_slice().into(),
            value: if matches!(case, Case::Waiting | Case::ParkWaiting) {
                b"1".as_slice().into()
            } else {
                b"0".as_slice().into()
            },
        },
        smith_protocol_machine::Variable { name: b"HOME".as_slice().into(), value: b"/home".as_slice().into() },
        smith_protocol_machine::Variable {
            name: b"EARLY_EXIT".as_slice().into(),
            value: if case == Case::EarlyExit { b"1".as_slice().into() } else { b"0".as_slice().into() },
        },
    ];
    if case == Case::Credentials {
        variables.extend(
            [
                ("ANTHROPIC_API_KEY", "key-for-work"),
                ("ANTHROPIC_AUTH_TOKEN", "token-for-work"),
                ("CLAUDE_CODE_OAUTH_TOKEN", "token-for-work"),
                ("CLAUDE_CODE_USE_BEDROCK", "1"),
                ("AWS_BEARER_TOKEN_BEDROCK", "token-for-work"),
                ("CLAUDE_CONFIG_DIR", "/elsewhere"),
                ("TERM", "dumb"),
            ]
            .map(|(name, value)| smith_protocol_machine::Variable {
                name: name.as_bytes().into(),
                value: value.as_bytes().into(),
            }),
        );
    }
    // These worlds offer a 128-byte report or bounded Open text, without host tools.
    // Keep result receiving bounds small enough to exercise the program
    // alongside the unchanged two-gigabyte test deployment.
    config.limits.domain.run.run_bytes = config.limits.domain.run.run_bytes.min(4096);
    config.limits.domain.run.outcome_bytes = config.limits.domain.run.outcome_bytes.min(4096);
    config.limits.domain.run.answer_bytes = config.limits.domain.run.answer_bytes.min(4096);
    config.limits.domain.run.check_tail = config.limits.domain.run.check_tail.min(256);
    config.limits.domain.run.host_reply_bytes = config.limits.domain.run.host_reply_bytes.min(4096);
    config.limits.domain.run.message_bytes = config.limits.domain.run.message_bytes.min(4096);
    if matches!(case, Case::Tools | Case::Events) {
        config.limits.domain.run.outcome_bytes = 512;
        config.limits.domain.run.answer_bytes = 512;
        config.limits.domain.run.host_reply_bytes = 512;
        config.limits.domain.run.message_bytes = 512;
        config.limits.domain.run.check_tail = 64;
    }
    config.limits.domain.session.delegated_result_bytes =
        smith_domain::feedback_worst_case(&config.limits.domain.run).expect("complete bounded feedback");
    if matches!(case, Case::Tools | Case::Events) {
        config.limits.events = Some(
            smith_agent_service::EventLimits::derive(
                &config.limits,
                if case == Case::Events {
                    smith_agent_service::sinks::Capture::Everything
                } else {
                    smith_agent_service::sinks::Capture::None
                },
                1,
                0,
                &[],
                Duration::from_secs(1),
            )
            .expect("event bounds"),
        );
    }
    config.limits.machine.env_bytes = 4096;
    config.environment = smith_protocol_machine::Environment::encode(&variables, &config.limits.machine)
        .expect("private test environment");
    config
}

#[expect(clippy::zero_sized_map_values, reason = "the shell configuration uses a unit selector map")]
fn make_agent(spawn: &Spawn, inherited: &Inherited) -> Proc {
    let case: Case = serde_json::from_slice(&spawn.args[0]).expect("scenario arguments");
    let configuration = configuration(case);
    let fd = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("agent stderr").1;
    let (errors, capture) = crate::AgentErrors::pipe(fd);
    let pipe = |number| inherited.pipes.iter().find(|(child, _)| *child == number).expect("agent pipe").1;
    let agent = smith_agent_shell::Agent::new(
        smith_agent_shell::config::Configuration {
            memory: configuration.limits.memory,
            service: configuration,
            trace: if matches!(case, Case::Tools | Case::Events) {
                Some(smith_agent_shell::trace::TraceConfig {
                    path: "/home/events.jsonl".into(),
                    capture: if case == Case::Events {
                        smith_agent_service::sinks::Capture::Everything
                    } else {
                        smith_agent_service::sinks::Capture::None
                    },
                    delivery: smith_agent_service::sinks::Delivery::Complete,
                })
            } else {
                None
            },
            profile: crate::profile(),
            declarations: crate::declarations(4096),
            programs: std::collections::BTreeMap::new(),
            endpoint_programs: std::collections::BTreeMap::new(),
        },
        smith_agent_shell::Resources {
            input: pipe(0),
            output: pipe(1),
            error: Some(pipe(2)),
            signals: inherited.signal,
            seed: 7,
            pid: 101,
            trace: inherited.appends.first().map(|(_, fd)| *fd),
            roots: if matches!(case, Case::Tools | Case::Events) {
                Some(
                    inherited
                        .roots
                        .iter()
                        .filter(|(name, _)| name.as_ref() != b"programs")
                        .map(|(_, fd)| *fd)
                        .collect(),
                )
            } else {
                None
            },
            programs: Some(smith_agent_shell::ProgramResources {
                relay: "/programs/smith-agent".into(),
                entropy: Secret::new(Box::new([7; 32])),
                directory: inherited.roots.first().map(|(_, fd)| *fd),
            }),
        },
        Box::new(errors),
    )
    .expect("program agent starts");
    Proc::Agent(Box::new(capture.host(agent)))
}

fn make_fake(spawn: &Spawn, inherited: &Inherited) -> Proc {
    waiting::timeout(spawn);
    assert!(
        spawn.env.iter().any(|entry| entry.as_ref() == b"CLAUDE_CONFIG_DIR=/programs/source"),
        "each invocation uses the source's configuration directory"
    );
    let mut spawn = fake_launch(spawn);
    if spawn.args.first().is_some_and(|argument| argument.as_ref() == b"-p")
        && spawn.env.iter().any(|entry| entry.as_ref() == b"EARLY_EXIT=1")
    {
        spawn.args = Box::new([b"unknown-command".as_slice().into()]);
    }
    Proc::Fake(Box::new((smith_fake_claude::hosted(b"/programs/claude").make)(&spawn, inherited)))
}

fn fake_launch(spawn: &Spawn) -> Spawn {
    let mut launch = Spawn {
        program: spawn.program.clone(),
        args: spawn.args.clone(),
        env: spawn.env.clone(),
        root: spawn.root,
        dir: spawn.dir.clone(),
        pipes: spawn.pipes.clone(),
    };
    // Hosted startup paths use the scenario namespace. Program print launches
    // use either the admitted workspace descriptor (empty dir) or /programs
    // with a run-relative dir; resolve that descriptor-relative cwd for the fake.
    if launch.args.first().is_some_and(|argument| argument.as_ref() == b"-p") {
        launch.dir = if launch.dir.is_empty() {
            b"/work/repo".as_slice().into()
        } else {
            [b"/programs/", launch.dir.as_ref()].concat().into()
        };
    }
    launch
}

fn make_relay(spawn: &Spawn, inherited: &Inherited) -> Proc {
    relay(spawn, inherited, &spawn.args)
}

fn relay(spawn: &Spawn, inherited: &Inherited, arguments: &[Box<[u8]>]) -> Proc {
    let pipe = |number| inherited.pipes.iter().find(|(child, _)| *child == number).map(|(_, fd)| *fd);
    let environment = spawn
        .env
        .iter()
        .map(|entry| {
            String::from_utf8_lossy(entry.split(|byte| *byte == b'=').next().expect("entry name")).into_owned()
        })
        .collect();
    Proc::Relay {
        environment,
        process: Box::new(smith_agent_shell::relay::Process::new(
            arguments,
            &spawn.env,
            smith_agent_shell::relay::Faces {
                stdin: pipe(0).expect("relay input"),
                stdout: pipe(1).expect("relay output"),
                stderr: pipe(2),
                signal: inherited.signal,
            },
        )),
    }
}

fn make_failed_relay(spawn: &Spawn, inherited: &Inherited) -> Proc {
    // The relay refuses empty arguments, settling its pipes without a
    // handshake. Claude then emits an init with smith's server marked failed.
    relay(spawn, inherited, &[])
}

struct Machine {
    files: Files,
    base: Opened,
}

impl skein_world::Machine for Machine {
    fn open_root(&mut self, path: &[u8]) -> Result<skein_sim::Handle, kernel::Error> {
        self.files
            .open(self.base, path.strip_prefix(b"/").unwrap_or(path), How::Directory)
            .map(|opened| skein_sim::Handle::new(opened.raw()))
            .map_err(skein_fake_machine::kernel_error)
    }
    fn close_root(&mut self, root: skein_sim::Handle) {
        self.files.close(Opened::new(root.raw()));
    }
    fn open_append(
        &mut self,
        root: skein_sim::Handle,
        path: &[u8],
        mode: u32,
    ) -> Result<skein_sim::Handle, kernel::Error> {
        self.files
            .open(Opened::new(root.raw()), path, How::Append { mode })
            .map(|opened| skein_sim::Handle::new(opened.raw()))
            .map_err(skein_fake_machine::kernel_error)
    }
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        skein_fake_machine::step(&mut self.files, call, answers);
    }
}

struct Judge {
    cancel: Option<Time>,
    wait_for_exit: bool,
    ended: bool,
    waiting: waiting::Release,
}

impl Referee<Proc> for Judge {
    fn act(&mut self, now: Time, procs: &mut [Proc]) {
        self.waiting.act(now, procs);
        if self.cancel.is_some_and(|at| now >= at) {
            if let Proc::Parent(parent) = &mut procs[0]
                && !parent.peer.observed().iter().any(|frame| frame.kind == 0x0110)
                && parent.status.is_none()
            {
                parent.cancel_pending = true;
            }
            self.cancel = None;
        }
    }
    fn observe(&mut self, now: Time, procs: &[Proc]) {
        self.waiting.observe(now, procs);
        if let Proc::Parent(parent) = &procs[0] {
            self.ended = parent.peer.observed().iter().any(|frame| frame.kind == 0x0110);
        }
        if self.wait_for_exit
            && let Proc::Parent(parent) = &procs[0]
            && parent.peer.observed().iter().any(|frame| frame.kind == 0x0110)
        {
            for process in procs {
                if let Proc::Fake(program) = process {
                    assert!(program.exit().is_some(), "program exit precedes the host answer");
                }
            }
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        self.cancel.or(self.waiting.deadline())
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        self.cancel.is_none() && self.waiting.passed() && (!self.wait_for_exit || self.ended)
    }
    fn last_word(&self) -> skein_world::LastWord {
        skein_world::LastWord::LastExpectation
    }
}

/// An independently decoded channel and program-file observation after settlement.
pub struct Run {
    pub frames: Vec<skein_fake_channel::Observed>,
    pub invocations: Vec<smith_fake_claude::Observation>,
    pub seen: Vec<smith_fake_claude::Seen>,
    pub received: Vec<serde_json::Value>,
    pub exits: Vec<Exit>,
    pub errors: Vec<u8>,
    pub run_directories: Vec<Vec<u8>>,
    /// Names from each relay's spawn, with no environment values retained.
    pub relay_environments: Vec<Vec<String>>,
    pub events: Vec<smith_events::Record>,
    /// Final bytes of the writable fixture, read after the run settles.
    pub workspace_file: Option<Vec<u8>>,
    /// Final bytes of the fixture outside every admitted workspace directory.
    pub outside_file: Option<Vec<u8>>,
    /// Final bytes of the read-only library fixture.
    pub library_file: Option<Vec<u8>>,
}

impl Run {
    /// The host's terminal record, decoded independently of the agent.
    #[must_use]
    pub fn answer(&self) -> Option<smith_channel::Answer> {
        self.frames.iter().find(|frame| frame.kind == 0x0110).map(|frame| {
            smith_channel::Answer::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body))
                .expect("answer bytes")
        })
    }
}

fn fixture(story: &Story, login: Option<&[u8]>, case: Case) -> (Files, Opened) {
    let mut files = Files::new();
    let mut items = vec![
        Item::directory(b"home").mode(0o700),
        Item::directory(b"work").mode(0o700),
        Item::directory(b"work/repo").mode(0o700),
        Item::directory(b"library").mode(0o700),
        Item::directory(b"programs").mode(0o700),
        Item::directory(b"programs/source").mode(0o700),
        Item::file(b"programs/source/fake-claude.story", &story.encode()),
    ];
    if let Some(login) = login {
        items.push(Item::file(b"programs/source/fake-claude.login", login).mode(0o600));
    }
    if case == Case::Tools {
        items.extend([
            Item::file(b"work/repo/a", b"before\n"),
            Item::directory(b"work/repo/empty"),
            Item::file(b"library/a", b"library text"),
            Item::directory(b"outside"),
            Item::file(b"outside/a", b"outside text"),
        ]);
    }
    let base = files.lay(&items);
    (files, base)
}

/// Drive an ordinary scripted program on the deterministic machine.
#[must_use]
pub fn run(
    case: Case,
    story: &Story,
    login: Option<&[u8]>,
    charter: &[u8],
    value: &smith_channel::GrantValue,
    refresh: Option<&smith_channel::GrantValue>,
) -> Run {
    run_inner(case, story, login, charter, value, refresh, None, None)
}

/// Restore the host's recorded window through the shipped program composition.
#[must_use]
pub fn run_with_window(story: &Story, charter: &[u8], turns: &[Box<[u8]>]) -> Run {
    run_inner(Case::Transcript, story, Some(b"claude.ai"), charter, &crate::program(b"source"), None, Some(turns), None)
}

/// Drive Open replies and parent messages on one program input.
#[must_use]
pub fn open(story: &Story, charter: &[u8], releases: &[HostMessage]) -> Run {
    open_with_window(story, charter, releases, None)
}

/// Restore an Open window, or begin fresh, with the same parent release driver.
#[must_use]
pub fn open_with_window(story: &Story, charter: &[u8], releases: &[HostMessage], turns: Option<&[Box<[u8]>]>) -> Run {
    run_inner(Case::Open, story, Some(b"claude.ai"), charter, &crate::program(b"source"), None, turns, Some(releases))
}

/// Drive one hosted program scenario on the shared deterministic world and machine.
#[must_use]
#[expect(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "one hosted fixture connects the program, relay, roots and independent observations"
)]
fn run_inner(
    case: Case,
    story: &Story,
    login: Option<&[u8]>,
    charter: &[u8],
    value: &smith_channel::GrantValue,
    refresh: Option<&smith_channel::GrantValue>,
    history: Option<&[Box<[u8]>]>,
    releases: Option<&[HostMessage]>,
) -> Run {
    let mut story = story.clone();
    if !matches!(
        case,
        Case::Turns
            | Case::Credentials
            | Case::Tools
            | Case::Waiting
            | Case::ParkWaiting
            | Case::Events
            | Case::Transcript
            | Case::Open
    ) && story.turns.is_empty()
    {
        story.turns.push(smith_fake_claude::Turn {
            steps: vec![smith_fake_claude::Step::Hang],
            end: smith_fake_claude::End::default(),
        });
    }
    let (mut files, base) = fixture(&story, login, case);
    let root = files.open(base, b".", How::Directory).expect("parent root");
    let machine = Machine { files, base };
    let mut world =
        skein_world::World::new_controlled(7, skein_sim::Config::calm(), skein_world::Memory::Checked, |_| Judge {
            wait_for_exit: matches!(
                case,
                Case::Turns
                    | Case::Credentials
                    | Case::Tools
                    | Case::Waiting
                    | Case::ParkWaiting
                    | Case::Events
                    | Case::Transcript
                    | Case::Open
            ),
            ended: false,
            waiting: releases.map_or_else(|| waiting::Release::new(case == Case::Waiting), waiting::Release::open),
            cancel: if matches!(
                case,
                Case::Turns
                    | Case::Credentials
                    | Case::Tools
                    | Case::Waiting
                    | Case::ParkWaiting
                    | Case::Events
                    | Case::Transcript
                    | Case::Open
            ) {
                None
            } else {
                Some(Time::ZERO.saturating_add(Duration::from_millis(50)))
            },
        })
        .with_machine(machine);
    let operations = smith_agent_service::program_operations(&configuration(case))
        .expect("program operation bound")
        .checked_add(8)
        .expect("shell routes");
    world.host_startup(
        HostedProgram { program: b"smith".as_slice().into(), make: make_agent, instances: 1, operations },
        |spawn| {
            let case: Case = serde_json::from_slice(&spawn.args[0]).expect("case");
            if case == Case::MissingRoot {
                Vec::new()
            } else {
                let mut roots = vec![skein_world::StartupRoot {
                    name: b"programs".as_slice().into(),
                    path: b"/programs".as_slice().into(),
                }];
                if matches!(case, Case::Tools | Case::Events) {
                    roots.extend([
                        skein_world::StartupRoot {
                            name: b"cwd".as_slice().into(),
                            path: b"/work/repo".as_slice().into(),
                        },
                        skein_world::StartupRoot {
                            name: b"library".as_slice().into(),
                            path: b"/library".as_slice().into(),
                        },
                    ]);
                }
                roots
            }
        },
        |spawn| {
            let case: Case = serde_json::from_slice(&spawn.args[0]).expect("scenario");
            if matches!(case, Case::Tools | Case::Events) {
                vec![skein_world::StartupAppend {
                    name: b"trace".as_slice().into(),
                    root: b"/home".as_slice().into(),
                    path: b"events.jsonl".as_slice().into(),
                    mode: 0o600,
                }]
            } else {
                vec![]
            }
        },
    );

    let factory = smith_fake_claude::hosted(b"/programs/claude");
    world.host_startup(
        HostedProgram { program: factory.program, make: make_fake, instances: 2, operations: factory.operations },
        |spawn| {
            let launch = fake_launch(spawn);
            smith_fake_claude::config_root(&launch).into_iter().chain(smith_fake_claude::mcp_root(&launch)).collect()
        },
        |spawn| smith_fake_claude::observation_append(spawn).into_iter().collect(),
    );
    world.host(HostedProgram {
        program: b"/programs/smith-agent".as_slice().into(),
        make: if case == Case::MissingServer { make_failed_relay } else { make_relay },
        instances: 1,
        operations: smith_agent_shell::relay::operations(),
    });
    world.spawn_root(skein_sim::Handle::new(root.raw()), |root| {
        let initial = waiting::initial(matches!(case, Case::Waiting | Case::ParkWaiting | Case::Open));
        let mut parent = Parent::new(
            root,
            charter,
            false,
            None,
            8,
            false,
            initial.as_ref(),
            if case == Case::Events {
                smith_agent_service::sinks::Capture::Everything
            } else {
                smith_agent_service::sinks::Capture::None
            },
            skein_llm::Provider::OpenAiCodex,
            value,
            None,
            0,
        );
        parent.program_messages = waiting::messages(releases.unwrap_or(&[]));
        parent
            .program_arguments(Box::new([serde_json::to_vec(&case).expect("case arguments").into()]), refresh.cloned());
        if matches!(case, Case::Tools | Case::Events) {
            parent.peer = crate::host();
            parent.peer.play(tools_start(charter, value));
            parent.program_host_answers = Some(0);
        }
        if case == Case::Transcript || history.is_some() {
            parent.peer = crate::host();
            parent.peer.play(match history {
                Some(turns) => start_with_window(charter, value, turns),
                None => with_transcript(charter, value),
            });
        }
        Proc::Parent(Box::new(parent))
    });
    let mut outcome = world.run();
    assert!(outcome.procs.iter().all(Host::is_empty), "every hosted process settles");
    let Proc::Parent(parent) = &outcome.procs[0] else { panic!("parent first") };
    let bytes = match outcome.machine.files.open(base, b"programs/source/fake-claude.observations", How::Read) {
        Ok(file) => {
            let bytes = outcome.machine.files.read(file, 0, 1_000_000).expect("observations");
            outcome.machine.files.close(file);
            bytes
        }
        Err(_) => Vec::new(),
    };
    let run_directories = match outcome.machine.files.open(base, b"programs/.runs", How::Directory) {
        Ok(root) => {
            let entries = outcome.machine.files.list(root, 256, 65_536).expect("run directory entries");
            outcome.machine.files.close(root);
            entries.into_iter().map(|_| b"<private-run>".to_vec()).collect()
        }
        Err(_) => Vec::new(),
    };
    let run = Run {
        workspace_file: fixture_file(&mut outcome.machine.files, base, b"work/repo/a"),
        outside_file: fixture_file(&mut outcome.machine.files, base, b"outside/a"),
        library_file: fixture_file(&mut outcome.machine.files, base, b"library/a"),
        frames: parent.peer.observed().to_vec(),
        errors: parent.errors.clone(),
        invocations: redacted_invocations(&bytes),
        seen: smith_fake_claude::seen(&bytes),
        received: smith_fake_claude::received(&bytes),
        exits: outcome.procs.iter().filter_map(Host::exit).collect(),
        run_directories,
        relay_environments: outcome
            .procs
            .iter()
            .filter_map(|process| match process {
                Proc::Relay { environment, .. } => Some(environment.clone()),
                Proc::Parent(_) | Proc::Agent(_) | Proc::Fake(_) => None,
            })
            .collect(),
        events: if matches!(case, Case::Tools | Case::Events) {
            let file = outcome.machine.files.open(base, b"home/events.jsonl", How::Read).expect("trace file");
            let size = u32::try_from(outcome.machine.files.stat(file).size).expect("trace size");
            let bytes = outcome.machine.files.read(file, 0, size).expect("event stream");
            outcome.machine.files.close(file);
            crate::events::read(&bytes, configuration(case).limits.events.as_ref().expect("event limits"))
        } else {
            Vec::new()
        },
    };
    outcome.machine.files.close(base);
    assert_eq!(outcome.machine.files.open_handles(), 0, "fixture handles settle");
    run
}

fn fixture_file(files: &mut Files, base: Opened, path: &[u8]) -> Option<Vec<u8>> {
    let file = files.open(base, path, How::Read).ok()?;
    let bytes = files.read(file, 0, 65_536).expect("bounded fixture file");
    files.close(file);
    Some(bytes)
}

pub(crate) fn cancel() -> skein_channel::Frame {
    let record =
        smith_channel::Cancel::new(&smith_channel::CEILINGS, smith_channel::CancelParts {}).expect("cancel record");
    let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).expect("cancel bytes"));
    record.encode(&mut writer).expect("cancel record");
    let mut frame = skein_channel::frame_writer(0x0105, record.measure()).expect("cancel frame");
    frame.put(&writer.finish()).expect("cancel body");
    frame.finish().expect("cancel frame")
}

fn with_transcript(charter: &[u8], value: &smith_channel::GrantValue) -> skein_channel::Frame {
    let frame = crate::start_workspace_message(charter, 8, false, None, value, None);
    let mut start = smith_channel::Start::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.bytes()[8..]))
        .expect("start fixture")
        .into_parts();
    let source = include_bytes!("../../../crates/smith-transcript/golden/v6/record_turn_smallest.bin");
    let mut turn = smith_transcript::Turn::decode(&smith_transcript::CEILINGS, &mut Reader::new(source))
        .expect("turn fixture")
        .into_parts();
    turn.place = 1;
    turn.opening = Some(
        smith_transcript::Opening::decode(
            &smith_transcript::CEILINGS,
            &mut Reader::new(include_bytes!("../../../crates/smith-transcript/golden/v6/record_opening_smallest.bin")),
        )
        .expect("opening fixture"),
    );
    let turn = smith_transcript::Turn::new(&smith_transcript::CEILINGS, turn).expect("opening turn");
    let mut bytes = skein_lib::Writer::new(usize::try_from(turn.measure()).expect("turn length"));
    turn.encode(&mut bytes).expect("turn body");
    start.transcript = smith_protocol_channel::transcript_piece(&[bytes.finish()]).expect("transcript piece");
    let start = smith_channel::Start::new(&smith_channel::CEILINGS, start).expect("start with history");
    let mut bytes = skein_lib::Writer::new(usize::try_from(start.measure()).expect("start bytes"));
    start.encode(&mut bytes).expect("start body");
    let mut frame = skein_channel::frame_writer(0x0100, start.measure()).expect("start frame");
    frame.put(&bytes.finish()).expect("start body");
    frame.finish().expect("start frame")
}

fn redacted_invocations(bytes: &[u8]) -> Vec<smith_fake_claude::Observation> {
    let mut invocations = smith_fake_claude::observations(bytes);
    for invocation in &mut invocations {
        for argument in &mut invocation.arguments {
            if let Some(start) = argument.find("/.runs/") {
                let after = start + "/.runs/".len();
                if let Some(end) = argument[after..].find('/') {
                    argument.replace_range(after..after + end, "<private-run>");
                }
            }
        }
    }
    invocations
}

fn tools_start(charter: &[u8], value: &smith_channel::GrantValue) -> skein_channel::Frame {
    let frame = crate::start_workspace_message(charter, 8, false, None, value, None);
    let mut start = smith_channel::Start::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.bytes()[8..]))
        .expect("start")
        .into_parts();
    let mut directories = skein_lib::List::with_capacity(2);
    for (name, path, writable) in
        [(b"repo".as_slice(), b"/work/repo".as_slice(), true), (b"library".as_slice(), b"/library".as_slice(), false)]
    {
        directories
            .push(
                smith_channel::Directory::new(
                    &smith_channel::CEILINGS,
                    smith_channel::DirectoryParts {
                        name: name.into(),
                        path: path.into(),
                        writable,
                        git: false,
                        conflicts: skein_lib::List::with_capacity(0),
                    },
                )
                .expect("workspace directory"),
            )
            .expect("two roots");
    }
    start.workspace = Some(
        smith_channel::Workspace::new(&smith_channel::CEILINGS, smith_channel::WorkspaceParts { directories })
            .expect("workspace"),
    );
    let start = smith_channel::Start::new(&smith_channel::CEILINGS, start).expect("workspace start");
    let mut bytes = skein_lib::Writer::new(start.measure() as usize);
    start.encode(&mut bytes).expect("start body");
    let mut frame = skein_channel::frame_writer(0x0100, start.measure()).expect("start frame");
    frame.put(&bytes.finish()).expect("start bytes");
    frame.finish().expect("workspace start frame")
}

pub(crate) fn answer_host(parent: &mut Parent) {
    if let Some(answered) = parent.program_host_answers {
        let calls: Vec<_> =
            parent.peer.observed().iter().filter(|frame| frame.kind == 0x0107).skip(answered).cloned().collect();
        for frame in calls {
            let call = smith_channel::Call::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body))
                .expect("host call");
            let reply = smith_channel::HostReply::new(
                &smith_channel::CEILINGS,
                smith_channel::HostReplyParts { error: false, text: b"found".as_slice().into() },
            )
            .expect("host reply");
            let answer = smith_channel::HostAnswer::new(
                &smith_channel::CEILINGS,
                smith_channel::HostAnswerParts { name: call.name().clone(), reply: smith_channel::Reply::Host(reply) },
            )
            .expect("host answer");
            let mut body = skein_lib::Writer::new(answer.measure() as usize);
            answer.encode(&mut body).expect("answer body");
            let mut frame = skein_channel::frame_writer(0x0102, answer.measure()).expect("host frame");
            frame.put(&body.finish()).expect("host body");
            parent.peer.play(frame.finish().expect("host answer frame"));
            parent.program_host_answers = Some(parent.program_host_answers.expect("enabled host") + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_program_world_fits_its_declared_memory() {
        let config = super::configuration(super::Case::Tools);
        let program = smith_agent_service::program_worst_case(&config).expect("program bound");
        let service = smith_agent_service::worst_case(&config.limits, &config.llm_endpoints).expect("service bound");
        let complete = service.checked_add(program).expect("complete bound");
        let adapter = &config.limits.llm.adapter;
        let receiving = &config.limits.llm.receiving;
        // The service's existing line ceiling: six escapes per answer byte,
        // plus its fixed 65,536-byte stream framing allowance.
        let line_bytes =
            adapter.client.answer.checked_mul(6).expect("escaped answer").checked_add(65_536).expect("line framing");
        let tokens = smith_protocol_program::backend::Settings::stream_tokens(line_bytes);
        println!(
            "client_tokens={} stream_tokens={tokens} input={} blocks={} answer={}",
            adapter.client.tokens,
            adapter.stated.call_input_bytes,
            receiving.max_completion_blocks,
            adapter.client.answer
        );
        // Token records alone; unchanged line-text storage is outside this comparison.
        for count in [adapter.client.tokens, tokens] {
            let records = skein_json::document::worst_case(&skein_json::document::Limits { tokens: count, text: 0 })
                .expect("document token records");
            println!("token_records[{count}]={records}");
        }
        println!("program={program} service={service} complete={complete} memory={}", config.limits.memory);
        assert!(complete <= config.limits.memory, "program startup fits its declaration");
    }
}
