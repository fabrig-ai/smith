//! Hosted sign-in stories over skein's fake filesystem (protocol/programs.md,
//! section 8; testing.md, section 4).

mod support;

use skein_io::kernel::Exit;
use smith_fake_claude::{Login, Logout, Observation, Status, Story, observations};
use support::{Invocation, run};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

fn status(record: &support::Record) -> serde_json::Value {
    assert!(record.stderr.is_empty());
    serde_json::from_slice(&record.stdout).expect("status JSON")
}

#[test]
fn a_status_without_a_login_is_not_signed_in() {
    let result = run(None, None, vec![Invocation::auth("status")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert_eq!(
        status(&result.records[0]),
        serde_json::json!({
            "loggedIn": false, "authMethod": "none", "apiProvider": "firstParty",
        })
    );
}

#[test]
fn a_login_then_a_status_is_signed_in_through_the_subscription() {
    let result = run(None, None, vec![Invocation::auth("login"), Invocation::auth("status")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(result.records[0].stdout, b"Signed in.\n");
    assert_eq!(result.records[1].exit, Some(Exit::Code(0)));
    assert_eq!(
        status(&result.records[1]),
        serde_json::json!({
            "loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty",
        })
    );
    assert_eq!(result.login.as_deref(), Some(b"claude.ai".as_slice()));
}

#[test]
fn a_login_as_another_method_is_signed_in_otherwise() {
    let method = "api\"key\\method";
    let result = run(
        Some(Story { login: Login::Immediate(method.into()), ..Story::default() }),
        Some(b"old-method-longer"),
        vec![Invocation::auth("login"), Invocation::auth("status")],
    );
    assert_eq!(result.records[1].exit, Some(Exit::Code(0)));
    assert_eq!(status(&result.records[1])["authMethod"], method);
    assert_eq!(result.login.as_deref(), Some(method.as_bytes()));
}

#[test]
fn a_login_by_code_reads_the_pasted_line() {
    let result = run(
        Some(Story { login: Login::Code("claude.ai".into()), ..Story::default() }),
        None,
        vec![Invocation::auth("login").input(b"pasted-code\n"), Invocation::auth("status")],
    );
    assert_eq!(result.records[0].stdout, b"Paste the code from the browser:\n");
    assert_eq!(result.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(status(&result.records[1])["authMethod"], "claude.ai");
    let bytes = result.observations.expect("observations");
    assert!(!String::from_utf8_lossy(&bytes).contains("pasted-code"));
}

#[test]
fn a_login_by_code_refuses_the_inputs_end() {
    let result = run(
        Some(Story { login: Login::Code("claude.ai".into()), ..Story::default() }),
        None,
        vec![Invocation::auth("login")],
    );
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert_eq!(result.login, None);
}

#[test]
fn a_login_by_code_refuses_an_unterminated_line() {
    let result = run(
        Some(Story { login: Login::Code("claude.ai".into()), ..Story::default() }),
        None,
        vec![Invocation::auth("login").input(b"unfinished-code")],
    );
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert_eq!(result.login, None);
}

#[test]
fn a_login_by_code_refuses_an_empty_line() {
    let result = run(
        Some(Story { login: Login::Code("claude.ai".into()), ..Story::default() }),
        Some(b"seed"),
        vec![Invocation::auth("login").input(b"\n")],
    );
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert_eq!(result.login.as_deref(), Some(b"seed".as_slice()));
}

#[test]
fn a_refused_login_keeps_the_login() {
    let result =
        run(Some(Story { login: Login::Refused, ..Story::default() }), Some(b"seed"), vec![Invocation::auth("login")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert!(!result.records[0].stderr.is_empty());
    assert_eq!(result.login.as_deref(), Some(b"seed".as_slice()));
}

#[test]
fn a_logout_removes_the_login() {
    let result = run(None, Some(b"claude.ai"), vec![Invocation::auth("logout"), Invocation::auth("status")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(result.records[0].stdout, b"Signed out.\n");
    assert_eq!(result.records[1].exit, Some(Exit::Code(1)));
    assert_eq!(status(&result.records[1])["loggedIn"], false);
    assert_eq!(result.login, None);
}

#[test]
fn a_logout_without_a_login_succeeds() {
    let result = run(None, None, vec![Invocation::auth("logout")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(result.records[0].stdout, b"Signed out.\n");
}

#[test]
fn a_failing_logout_keeps_the_login() {
    let result = run(
        Some(Story { logout: Logout::Fails, ..Story::default() }),
        Some(b"claude.ai"),
        vec![Invocation::auth("logout")],
    );
    assert_eq!(result.records[0].exit, Some(Exit::Code(1)));
    assert!(!result.records[0].stderr.is_empty());
    assert_eq!(result.login.as_deref(), Some(b"claude.ai".as_slice()));
}

#[test]
fn an_unreadable_status_is_not_json() {
    let result =
        run(Some(Story { status: Status::Unreadable, ..Story::default() }), None, vec![Invocation::auth("status")]);
    assert_eq!(result.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(result.records[0].stdout, b"not json\n");
    assert!(serde_json::from_slice::<serde_json::Value>(&result.records[0].stdout).is_err());
}

#[test]
fn a_silent_status_never_exits_until_signalled() {
    let result = run(
        Some(Story { status: Status::Silent, ..Story::default() }),
        None,
        vec![Invocation::auth("status").terminate()],
    );
    assert!(result.signalled);
    assert_eq!(result.records[0].exit, Some(Exit::Code(143)));
    assert!(result.records[0].stdout.is_empty());
    assert!(result.records[0].stderr.is_empty());
    assert_eq!(observations(&result.observations.expect("observations")).len(), 1);
}

#[test]
fn without_a_configuration_directory_it_refuses() {
    let result = run(
        None,
        Some(b"seed"),
        vec![
            Invocation::auth("status").environment(&["HOME=home"]),
            Invocation::auth("login").environment(&["HOME=home", "CLAUDE_CONFIG_DIR="]),
            Invocation::auth("logout").environment(&["HOME=home"]),
        ],
    );
    for record in &result.records {
        assert_eq!(record.exit, Some(Exit::Code(2)));
        assert!(record.stdout.is_empty());
        assert_eq!(record.stderr, b"fake claude: CLAUDE_CONFIG_DIR is not set\n");
    }
    assert_eq!(result.observations, None);
    assert_eq!(result.login.as_deref(), Some(b"seed".as_slice()));
}

#[test]
fn each_invocation_is_observed_by_its_environment_s_names() {
    let environment = ["HOME=home", "CLAUDE_CONFIG_DIR=config", "SECRET=never-record-this-value"];
    let result = run(
        None,
        None,
        vec![Invocation::auth("login").environment(&environment), Invocation::auth("status").environment(&environment)],
    );
    let bytes = result.observations.expect("observations");
    assert!(!String::from_utf8_lossy(&bytes).contains("never-record-this-value"));
    assert_eq!(
        observations(&bytes),
        vec![
            Observation {
                arguments: vec!["auth".into(), "login".into()],
                environment: vec!["HOME".into(), "CLAUDE_CONFIG_DIR".into(), "SECRET".into()]
            },
            Observation {
                arguments: vec!["auth".into(), "status".into()],
                environment: vec!["HOME".into(), "CLAUDE_CONFIG_DIR".into(), "SECRET".into()]
            },
        ]
    );
}

#[test]
fn unknown_commands_are_observed_then_refused() {
    let result = run(
        None,
        None,
        vec![
            Invocation::auth("status").arguments(&["auth", "status", "--json"]),
            Invocation::auth("status").arguments(&["-p"]),
        ],
    );
    for record in &result.records {
        assert_eq!(record.exit, Some(Exit::Code(2)));
        assert!(!record.stderr.is_empty());
    }
    assert_eq!(observations(&result.observations.expect("observations")).len(), 2);
}

// This parent asks for only stdout/stderr. The fake's ordinary fixture
// deliberately exercises all three pipes for interactive login.
mod no_stdin {
    use skein_fake_machine::{How, Item, Machine as Files, Opened};
    use skein_io::{self as io, kernel};
    use skein_lib::{Duration, Env, Queue, Time, Token, Wall, stream};
    use skein_shell::Host;
    use skein_world::{HostedProgram, Inherited, Machine, Referee};
    use smith_fake_claude::{Fake, Story};

    const OWNER: Token = Token::new(100);
    const CLOSE_ROOT: Token = Token::new(u64::MAX);

    struct Parent {
        io: io::Io,
        env: Env<io::Limits>,
        events: Queue<io::Event>,
        requests: Queue<io::Request>,
        completions: Queue<kernel::Complete>,
        submissions: Queue<kernel::Submit>,
        root: kernel::Fd,
        closed: u8,
        root_closed: bool,
        out: Option<Token>,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
        exit: Option<kernel::Exit>,
    }

    impl Parent {
        fn new(root: kernel::Fd, command: &[u8]) -> Self {
            let limits = io::Limits {
                sockets: 3,
                names: 0,
                refusals: 1,
                intake: 4096,
                receive: 4096,
                output: 1,
                sends: 1,
                accepts: 1,
                backlog: 1,
                close_timeout: Duration::from_millis(10),
                retry: Duration::from_millis(1),
            };
            let mut requests = Queue::with_capacity(16);
            requests.push(io::Request::Spawn {
                owner: OWNER,
                spawn: kernel::Spawn {
                    program: b"claude".as_slice().into(),
                    args: Box::new([b"auth".as_slice().into(), command.into()]),
                    env: Box::new([b"HOME=home".as_slice().into(), b"CLAUDE_CONFIG_DIR=config".as_slice().into()]),
                    root,
                    dir: b".".as_slice().into(),
                    pipes: Box::new([
                        kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None },
                        kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None },
                    ]),
                },
            });
            Self {
                io: io::Io::new(&limits),
                env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
                events: Queue::with_capacity(16),
                requests,
                completions: Queue::with_capacity(16),
                submissions: Queue::with_capacity(32),
                root,
                closed: 0,
                root_closed: false,
                out: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit: None,
            }
        }
    }

    impl Host for Parent {
        fn iterate(&mut self, now: Time, wall: Wall) {
            self.env.now = now;
            self.env.wall = wall;
            while let Some(complete) = self.completions.pop() {
                if complete.op == CLOSE_ROOT {
                    assert!(complete.result.is_ok());
                    self.root_closed = true;
                } else {
                    io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
                }
            }
            while self.io.is_ready() {
                io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
            while self.io.is_due(now) {
                io::fire(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
            while let Some(event) = self.events.pop() {
                match event {
                    io::Event::Spawned { pipes, .. } => {
                        self.out = pipes.first().copied();
                        for pipe in pipes {
                            self.requests.push(io::Request::Stream {
                                stream: pipe,
                                down: stream::Down::Demand { read: stream::Read::Arrived { max: 4096 }, room: 0 },
                            });
                        }
                    }
                    io::Event::Stream { owner, up: stream::Up::Bytes(bytes) } => {
                        if Some(owner) == self.out {
                            self.stdout.extend_from_slice(&bytes);
                        } else {
                            self.stderr.extend_from_slice(&bytes);
                        }
                        self.requests.push(io::Request::Stream {
                            stream: owner,
                            down: stream::Down::Demand { read: stream::Read::Arrived { max: 4096 }, room: 0 },
                        });
                    }
                    io::Event::Stream { owner, up: stream::Up::End } => {
                        self.requests.push(io::Request::Close { entity: owner })
                    }
                    io::Event::Exited { exit, .. } => self.exit = Some(exit),
                    io::Event::Closed { .. } => {
                        self.closed += 1;
                        if self.closed == 3 {
                            self.submissions
                                .push(kernel::Submit { op: CLOSE_ROOT, kind: kernel::Op::Close { fd: self.root } });
                        }
                    }
                    other => panic!("unexpected no-stdin event: {other:?}"),
                }
            }
            while let Some(request) = self.requests.pop() {
                io::down(&mut self.io, &self.env, request, &mut self.submissions);
            }
            self.io.reclaim();
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
        }
        fn next_deadline(&self) -> Option<Time> {
            self.io.next_deadline()
        }
        fn next_policy_deadline(&self) -> Option<Time> {
            None
        }
        fn is_empty(&self) -> bool {
            self.root_closed && self.io.is_empty() && self.completions.is_empty() && self.submissions.is_empty()
        }
        fn operations(&self) -> u32 {
            io::operations(&self.env.limits).expect("parent operations") + 1
        }
    }

    enum Process {
        Parent(Box<Parent>),
        Fake(Box<Fake>),
    }

    impl Process {
        fn host(&self) -> &dyn Host {
            match self {
                Self::Parent(parent) => parent.as_ref(),
                Self::Fake(fake) => fake.as_ref(),
            }
        }
        fn host_mut(&mut self) -> &mut dyn Host {
            match self {
                Self::Parent(parent) => parent.as_mut(),
                Self::Fake(fake) => fake.as_mut(),
            }
        }
    }

    impl Host for Process {
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
        fn exit(&self) -> Option<kernel::Exit> {
            self.host().exit()
        }
        fn operations(&self) -> u32 {
            self.host().operations()
        }
    }

    struct MachineFiles {
        files: Files,
        base: Opened,
    }

    impl Machine for MachineFiles {
        fn open_root(&mut self, path: &[u8]) -> Result<skein_sim::Handle, kernel::Error> {
            self.files
                .open(self.base, path, How::Directory)
                .map(|root| skein_sim::Handle::new(root.raw()))
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
                .map(|file| skein_sim::Handle::new(file.raw()))
                .map_err(skein_fake_machine::kernel_error)
        }
        fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
            skein_fake_machine::step(&mut self.files, call, answers);
        }
    }

    struct Judge {
        met: bool,
    }

    impl Referee<Process> for Judge {
        fn act(&mut self, _now: Time, _procs: &mut [Process]) {}
        fn observe(&mut self, _now: Time, procs: &[Process]) {
            let Process::Parent(parent) = &procs[0] else { panic!("parent first") };
            self.met = parent.exit.is_some();
        }
        fn next_deadline(&self) -> Option<Time> {
            None
        }
        fn overdue(&self, now: Time) -> Option<String> {
            if now > Time::ZERO.saturating_add(Duration::from_secs(1)) {
                Some("no-stdin command answers".into())
            } else {
                None
            }
        }
        fn passed(&self) -> bool {
            self.met
        }
    }

    fn make_fake(spawn: &kernel::Spawn, inherited: &Inherited) -> Process {
        assert_eq!(inherited.pipes.len(), 2);
        assert!(inherited.pipes.iter().all(|(number, _)| *number != 0));
        Process::Fake(Box::new((smith_fake_claude::hosted(b"claude").make)(spawn, inherited)))
    }

    pub fn invoke(
        command: &[u8],
        story: Option<Story>,
        login: Option<&[u8]>,
    ) -> (super::support::Record, Option<Vec<u8>>) {
        let mut items = vec![Item::directory(b"home"), Item::directory(b"config")];
        if let Some(story) = story {
            items.push(Item::file(b"config/fake-claude.story", &story.encode()));
        }
        if let Some(login) = login {
            items.push(Item::file(b"config/fake-claude.login", login));
        }
        let mut files = Files::new();
        let base = files.lay(&items);
        let mut machine = MachineFiles { files, base };
        let root = machine.open_root(b".").expect("parent root");
        let mut world =
            skein_world::World::new(7, skein_sim::Config::calm(), Judge { met: false }, skein_world::Memory::Checked)
                .with_machine(machine);
        let factory = smith_fake_claude::hosted(b"claude");
        world.host_startup(
            HostedProgram {
                program: factory.program,
                make: make_fake,
                instances: factory.instances,
                operations: factory.operations,
            },
            |spawn| smith_fake_claude::config_root(spawn).into_iter().collect(),
            |spawn| smith_fake_claude::observation_append(spawn).into_iter().collect(),
        );
        world.spawn_root(root, |fd| Process::Parent(Box::new(Parent::new(fd, command))));
        let mut outcome = world.run();
        let Process::Parent(parent) = &outcome.procs[0] else { panic!("parent first") };
        let record =
            super::support::Record { stdout: parent.stdout.clone(), stderr: parent.stderr.clone(), exit: parent.exit };
        let login = match outcome.machine.files.open(base, b"config/fake-claude.login", How::Read) {
            Ok(file) => {
                let bytes = outcome.machine.files.read(file, 0, 65_536).expect("login file");
                outcome.machine.files.close(file);
                Some(bytes)
            }
            Err(skein_fake_machine::Refusal::NotFound) => None,
            Err(error) => panic!("login read: {error:?}"),
        };
        outcome.machine.files.close(base);
        assert_eq!(outcome.machine.files.open_handles(), 0);
        (record, login)
    }
}

#[test]
fn a_status_without_stdin_answers() {
    let (record, _) = no_stdin::invoke(b"status", None, Some(b"claude.ai"));
    assert_eq!(record.exit, Some(Exit::Code(0)));
    assert_eq!(status(&record)["authMethod"], "claude.ai");
    let (record, _) = no_stdin::invoke(b"status", None, None);
    assert_eq!(record.exit, Some(Exit::Code(1)));
    assert_eq!(status(&record)["loggedIn"], false);
    let (record, login) = no_stdin::invoke(b"logout", None, Some(b"claude.ai"));
    assert_eq!(record.exit, Some(Exit::Code(0)));
    assert_eq!(record.stdout, b"Signed out.\n");
    assert!(record.stderr.is_empty());
    assert_eq!(login, None);
    let (record, login) =
        no_stdin::invoke(b"login", Some(Story { login: Login::Code("claude.ai".into()), ..Story::default() }), None);
    assert_eq!(record.exit, Some(Exit::Code(1)));
    assert_eq!(login, None, "no login written without the code input");
}
