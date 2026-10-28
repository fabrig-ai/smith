//! The real run server and shipped relay around the fake (programs.md, 5.3, 9).

#![allow(dead_code, reason = "shared support is compiled by integration binaries that exercise other scenarios")]

use serde_json::{Value, json};
use skein_fake_machine::{Item, Machine as Files};
use skein_io::{
    self as io, Event, Io, Request,
    kernel::{Complete, Done, Exit, Fd, Op, Pipe, Spawn, Submit, Way},
};
use skein_lib::{
    Duration, Env, Queue, Time, Token, Wall,
    stream::{Down, Read, Up},
};
use skein_shell::Host;
use skein_world::{HostedProgram, Inherited, Machine, Referee};
use smith_fake_claude::{Fake, Story};
use smith_protocol_program::mcp::{self, Answer, Server, Tool, Verdict};

const CHILD: Token = Token::new(200);
const ROOT_CLOSE: Token = Token::new(u64::MAX);
const QUEUE: u32 = 64;
fn limits() -> io::Limits {
    io::Limits {
        sockets: 10,
        names: 0,
        refusals: 1,
        intake: 65_536,
        receive: 4096,
        output: 65_536,
        sends: 2,
        accepts: 1,
        backlog: 2,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    }
}

fn server_limits() -> mcp::Limits {
    mcp::Limits {
        message_bytes: 4096,
        answer_bytes: 4096,
        schema_bytes: 4096,
        calls: 2,
        unauthenticated: Time::from_nanos(1_000_000_000),
    }
}

pub struct Agent {
    io: Io,
    server: Server,
    seen: Vec<mcp::Up>,
    messages: Vec<Value>,
    root: Option<Fd>,
    root_closing: bool,
    child: Option<Token>,
    input: Option<Token>,
    input_bytes: Option<Box<[u8]>>,
    output: Option<Token>,
    error: Option<Token>,
    turns: usize,
    sent: usize,
    deny: bool,
    fake_done: bool,
    server_done: bool,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    requests: Queue<Request>,
    events: Queue<Event>,
}

impl Agent {
    fn new(root: Fd, turns: usize, deny: bool) -> Self {
        let mut requests = Queue::with_capacity(QUEUE);
        let server = Server::new(
            Token::new(10),
            47100,
            &[7; 32],
            Box::new([Tool {
                name: b"finish".as_slice().into(),
                description: b"Finish".as_slice().into(),
                input_schema: br#"{"type":"object"}"#.as_slice().into(),
            }]),
            server_limits(),
            &mut requests,
        )
        .expect("small tool list");
        Self {
            io: Io::new(&limits()),
            server,
            seen: Vec::new(),
            messages: Vec::new(),
            root: Some(root),
            root_closing: false,
            child: None,
            input: None,
            input_bytes: None,
            output: None,
            error: None,
            turns,
            sent: 0,
            deny,
            fake_done: false,
            server_done: false,
            completions: Queue::with_capacity(QUEUE),
            submissions: Queue::with_capacity(QUEUE),
            requests,
            events: Queue::with_capacity(QUEUE),
        }
    }
    fn start_fake(&mut self) {
        let mut arguments = super::print::arguments();
        let at = arguments.iter().position(|argument| argument == "--permission-prompt-tool").expect("prompt flag");
        *arguments.get_mut(at.checked_add(1).expect("flag value")).expect("prompt value") =
            "mcp__smith__permission_prompt".into();
        self.requests.push(Request::Spawn {
            owner: CHILD,
            spawn: Spawn {
                program: b"claude".as_slice().into(),
                args: arguments.iter().map(|argument| argument.as_bytes().into()).collect(),
                env: Box::new([b"HOME=home".as_slice().into(), b"CLAUDE_CONFIG_DIR=config".as_slice().into()]),
                root: self.root.expect("open fixture root"),
                dir: Box::new([]),
                pipes: Box::new([
                    Pipe { child: 0, way: Way::In, parent: None },
                    Pipe { child: 1, way: Way::Out, parent: None },
                    Pipe { child: 2, way: Way::Out, parent: None },
                ]),
            },
        });
    }
    fn notices(&mut self) {
        while let Some(up) = self.server.up() {
            match &up {
                mcp::Up::Listening { port } => {
                    assert_eq!(*port, 47100);
                    self.start_fake();
                }
                mcp::Up::Connected => {}
                mcp::Up::Call { call, .. } => {
                    self.server.answer(*call, Answer::Text(b"accepted".as_slice().into()), &mut self.requests)
                }
                mcp::Up::Prompt { call, input, .. } => {
                    let verdict = if self.deny {
                        Verdict::Deny { message: b"smith: no built-in tool is granted".as_slice().into() }
                    } else {
                        Verdict::Allow { updated_input: input.clone() }
                    };
                    self.server.verdict(*call, verdict, &mut self.requests);
                }
                mcp::Up::Ended(_) => self.server_done = true,
            }
            self.seen.push(up);
        }
    }
    fn read(&mut self, stream: Token) {
        self.requests
            .push(Request::Stream { stream, down: Down::Demand { read: Read::Line { max: 65_536 }, room: 0 } });
    }
    fn next_user(&mut self) {
        let input = self.input.expect("fake input pipe");
        if self.sent == self.turns {
            self.requests.push(Request::Close { entity: input });
        } else {
            self.sent = self.sent.checked_add(1).expect("few turns");
            let mut bytes = serde_json::to_vec(&json!({"type":"user","message":{"role":"user","content":"hello"}}))
                .expect("user JSON");
            bytes.push(b'\n');
            self.requests.push(Request::Stream {
                stream: input,
                down: Down::Demand {
                    read: Read::Nothing,
                    room: u32::try_from(bytes.len()).expect("short user message"),
                },
            });
            self.input_bytes = Some(bytes.into_boxed_slice());
        }
    }
    fn event(&mut self, now: Time, event: Event) {
        let owner = match &event {
            Event::Listening { owner, .. }
            | Event::Accepted { owner, .. }
            | Event::Stream { owner, .. }
            | Event::Output { owner, .. }
            | Event::Failed { owner, .. }
            | Event::Closed { owner } => Some(*owner),
            _ => None,
        };
        if owner.is_some_and(|owner| self.server.owns(owner)) {
            self.server.from_io(now, event, &mut self.requests);
            self.notices();
            return;
        }
        match event {
            Event::Spawned { child, pipes, .. } => {
                self.child = Some(child);
                self.input = Some(pipes[0]);
                self.output = Some(pipes[1]);
                self.error = Some(pipes[2]);
                self.read(pipes[1]);
                self.read(pipes[2]);
            }
            Event::Stream { owner, up } if Some(owner) == self.input => match up {
                Up::Room => self.requests.push(Request::Stream {
                    stream: owner,
                    down: Down::Send(self.input_bytes.take().expect("pending user message")),
                }),
                _ => panic!("fake stdin unexpected event"),
            },
            Event::Stream { owner, up } => match up {
                Up::Bytes(bytes) => {
                    if Some(owner) == self.output {
                        let message: Value = serde_json::from_slice(&bytes).expect("fake line JSON");
                        let advance = message["subtype"] == "init" || message["type"] == "result";
                        self.messages.push(message);
                        if advance {
                            self.next_user();
                        }
                    } else {
                        assert!(bytes.is_empty(), "fake stderr stays empty");
                    }
                    self.read(owner);
                }
                Up::End => self.requests.push(Request::Close { entity: owner }),
                _ => panic!("fake output stream failure"),
            },
            Event::Exited { exit, .. } => {
                assert_eq!(exit, Exit::Code(0));
                self.fake_done = true;
                self.requests.push(Request::Close { entity: self.child.expect("fake child") });
                self.server.close(&mut self.requests);
                self.notices();
            }
            Event::Closed { owner } => {
                if Some(owner) == self.input {
                    self.input = None;
                } else if Some(owner) == self.output {
                    self.output = None;
                } else if Some(owner) == self.error {
                    self.error = None;
                } else if owner == CHILD {
                    self.child = None;
                }
            }
            event => panic!("unexpected stand-in event {event:?}"),
        }
    }
}

impl Host for Agent {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: limits() };
        while self.io.is_ready() {
            io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            if complete.op == ROOT_CLOSE {
                assert_eq!(complete.result, Ok(Done::Nothing));
                self.root_closing = false;
            } else {
                io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
            }
        }
        while self.io.is_due(now) {
            io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(event) = self.events.pop() {
            self.event(now, event);
        }
        self.server.fire(now, &mut self.requests);
        self.notices();
        while let Some(request) = self.requests.pop() {
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
        if self.fake_done
            && self.io.is_empty()
            && let Some(fd) = self.root.take()
        {
            self.root_closing = true;
            self.submissions.push(Submit { op: ROOT_CLOSE, kind: Op::Close { fd } });
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        self.io.is_ready() || !self.completions.is_empty() || !self.events.is_empty() || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline().into_iter().chain(self.server.next_deadline()).min()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.server.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.fake_done
            && self.server_done
            && self.io.is_empty()
            && self.root.is_none()
            && !self.root_closing
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.requests.is_empty()
            && self.events.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        self.is_empty().then_some(Exit::Code(0))
    }
    fn operations(&self) -> u32 {
        io::operations(&limits()).expect("stand-in operations").checked_add(1).expect("root close")
    }
}

pub enum Process {
    Agent(Box<Agent>),
    Fake(Box<Fake>),
    Relay(Box<smith_agent_shell::relay::Process>),
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Self::Agent(p) => p.iterate(now, wall),
            Self::Fake(p) => p.iterate(now, wall),
            Self::Relay(p) => p.iterate(now, wall),
        }
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        match self {
            Self::Agent(p) => p.completions(),
            Self::Fake(p) => p.completions(),
            Self::Relay(p) => p.completions(),
        }
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        match self {
            Self::Agent(p) => p.submissions(),
            Self::Fake(p) => p.submissions(),
            Self::Relay(p) => p.submissions(),
        }
    }
    fn work_pending(&self, now: Time) -> bool {
        match self {
            Self::Agent(p) => p.work_pending(now),
            Self::Fake(p) => p.work_pending(now),
            Self::Relay(p) => p.work_pending(now),
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        match self {
            Self::Agent(p) => p.next_deadline(),
            Self::Fake(p) => p.next_deadline(),
            Self::Relay(p) => p.next_deadline(),
        }
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        match self {
            Self::Agent(p) => p.next_policy_deadline(),
            Self::Fake(p) => p.next_policy_deadline(),
            Self::Relay(p) => p.next_policy_deadline(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Self::Agent(p) => p.is_empty(),
            Self::Fake(p) => p.is_empty(),
            Self::Relay(p) => p.is_empty(),
        }
    }
    fn exit(&self) -> Option<Exit> {
        match self {
            Self::Agent(p) => p.exit(),
            Self::Fake(p) => p.exit(),
            Self::Relay(p) => p.exit(),
        }
    }
    fn operations(&self) -> u32 {
        match self {
            Self::Agent(p) => p.operations(),
            Self::Fake(p) => p.operations(),
            Self::Relay(p) => p.operations(),
        }
    }
}

struct Judge {
    wrong_secret: bool,
}

impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, _procs: &mut [Process]) {}
    fn observe(&mut self, _now: Time, procs: &[Process]) {
        if procs.get(1).is_some_and(|process| process.exit().is_some()) {
            assert_eq!(
                procs.get(2).and_then(Host::exit),
                Some(Exit::Code(u8::from(self.wrong_secret))),
                "relay settles before the fake"
            );
        }
        for (index, process) in procs.iter().enumerate() {
            if let Some(exit) = process.exit() {
                assert_eq!(exit, Exit::Code(if index == 2 { u8::from(self.wrong_secret) } else { 0 }));
            }
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        true
    }
    fn last_word(&self) -> skein_world::LastWord {
        skein_world::LastWord::LastExpectation
    }
}

fn make_fake(spawn: &Spawn, inherited: &Inherited) -> Process {
    Process::Fake(Box::new((smith_fake_claude::hosted(b"claude").make)(spawn, inherited)))
}

fn make_relay(spawn: &Spawn, inherited: &Inherited) -> Process {
    let pipe = |number| inherited.pipes.iter().find(|(child, _)| *child == number).map(|(_, fd)| *fd);
    Process::Relay(Box::new(smith_agent_shell::relay::Process::new(
        &spawn.args,
        &spawn.env,
        smith_agent_shell::relay::Faces {
            stdin: pipe(0).expect("relay input"),
            stdout: pipe(1).expect("relay output"),
            stderr: pipe(2),
            signal: inherited.signal,
        },
    )))
}

pub struct Run {
    pub seen: Vec<mcp::Up>,
    pub messages: Vec<Value>,
    pub relay_exit: Option<Exit>,
    pub relay_detail: Option<&'static [u8]>,
}

pub fn run(story: Story, deny: bool, wrong_secret: bool) -> Run {
    let entropy = if wrong_secret { [8; 32] } else { [7; 32] };
    let secret = mcp::secret_text(&entropy);
    let config = json!({"mcpServers":{"smith":{"type":"stdio","command":"smith-agent","args":[mcp::RELAY_FLAG,"127.0.0.1:47100"],"env":{"SMITH_MCP_SECRET":core::str::from_utf8(&secret).expect("hex")}}}});
    let settings = json!({"permissions":{"ask":["Read","Bash"],"allow":["mcp__smith__*"]}});
    let mut files = Files::new();
    let base = files.lay(&[
        Item::directory(b"home"),
        Item::directory(b"config"),
        Item::directory(b"run"),
        Item::file(b"config/fake-claude.story", &story.encode()),
        Item::file(b"config/fake-claude.login", b"claude.ai"),
        Item::file(b"config/settings.json", &serde_json::to_vec(&settings).expect("settings")),
        Item::file(b"run/mcp.json", &serde_json::to_vec(&config).expect("MCP config")),
    ]);
    let mut machine = super::MachineFiles { files, base };
    let root = machine.open_root(b".").expect("root");
    let mut world =
        skein_world::World::new_controlled(7, skein_sim::Config::calm(), skein_world::Memory::Checked, |_| Judge {
            wrong_secret,
        })
        .with_machine(machine);
    let fake = smith_fake_claude::hosted(b"claude");
    world.host_startup(
        HostedProgram {
            program: fake.program,
            make: make_fake,
            instances: fake.instances,
            operations: fake.operations,
        },
        |spawn| smith_fake_claude::config_root(spawn).into_iter().chain(smith_fake_claude::mcp_root(spawn)).collect(),
        |spawn| smith_fake_claude::observation_append(spawn).into_iter().collect(),
    );
    world.host(HostedProgram {
        program: b"smith-agent".as_slice().into(),
        make: make_relay,
        instances: 1,
        operations: smith_agent_shell::relay::operations(),
    });
    world.spawn_root(root, |fd| Process::Agent(Box::new(Agent::new(fd, story.turns.len(), deny))));
    let mut outcome = world.run();
    assert_eq!(outcome.procs.len(), 3, "agent, fake, relay");
    assert!(outcome.procs.iter().all(Host::is_empty));
    let relay_exit = outcome.procs[2].exit();
    let Process::Relay(relay) = &outcome.procs[2] else { panic!("relay third") };
    let relay_detail = relay.detail();
    outcome.machine.files.close(base);
    assert_eq!(outcome.machine.files.open_handles(), 0, "all fixture handles settle");
    let Process::Agent(agent) = &outcome.procs[0] else { panic!("agent first") };
    Run { seen: agent.seen.clone(), messages: agent.messages.clone(), relay_exit, relay_detail }
}
