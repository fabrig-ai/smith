//! Smith's shipped-binary stories: the shared person, browser, peers and scratch
//! over Skein's real loop (testing.md, sections 2.3 and 5). Only outside
//! observations are kept; no local or agent state is available.

use skein_io::kernel::{Complete, Exit, Submit};
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::{Clock, Host};
use skein_world::{
    Referee,
    end_to_end::{Binary, Command, Mode, Streams},
    real,
};
use smith_local_process_world::{
    process::{self, Proc},
    referee::{self, Seen},
    terminal::Terminal,
    world::Scenario,
};
use smith_real_world::{
    Scratch,
    settled::{Before, assert_no_children},
};
use std::{fmt::Write as _, path::Path};

pub enum Process {
    Binary(Box<Binary>),
    Script(Proc),
    Sample(Box<smith_hosts_world::HostService>),
}
impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Binary(p) => p.as_ref(),
            Self::Script(p) => p,
            Self::Sample(p) => p.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Binary(p) => p.as_mut(),
            Self::Script(p) => p,
            Self::Sample(p) => p.as_mut(),
        }
    }
}
impl Host for Process {
    fn drain(&mut self) {
        self.host_mut().drain();
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        self.host_mut().iterate(now, wall);
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        self.host_mut().completions()
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
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
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}
struct Judge {
    next_act: Option<Time>,
    sign_in: referee::SignIn,
    visiting: bool,
    shutdown: Option<Exit>,
    deadline: Time,
    saved: bool,
    tokens: std::path::PathBuf,
    checkout: smith_real_world::Checkout,
    ending: referee::Ending,
    seed: u64,
    reviewed: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        let exit = procs.iter().find_map(|p| if let Process::Binary(p) = p { p.exit_status() } else { None });
        self.next_act = None;
        let url = self.sign_in.url();
        self.shutdown = exit.or(self.shutdown);
        for proc in procs {
            match proc {
                Process::Script(Proc::Terminal(p)) => {
                    if let Some(exit) = exit {
                        p.finished(exit);
                    }
                }
                Process::Script(Proc::Peer(p)) if exit.is_some() => p.shutdown(),
                Process::Script(Proc::Issuer(p)) if exit.is_some() => p.shutdown(),
                Process::Script(Proc::Browser(p)) => {
                    if !self.visiting
                        && let Some(url) = url
                    {
                        p.visit(url);
                        self.visiting = true;
                    }
                    if exit.is_some() {
                        p.shutdown();
                    }
                }
                Process::Binary(_) | Process::Script(_) | Process::Sample(_) => {}
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Process]) {
        for proc in procs {
            if let Process::Script(Proc::Terminal(person)) = proc {
                self.sign_in.observe(person.shown());
            }
        }
        if (!self.visiting && self.sign_in.url().is_some())
            || (self.shutdown.is_none()
                && procs.iter().any(|proc| matches!(proc, Process::Binary(binary) if binary.exit_status().is_some())))
        {
            self.next_act = Some(now);
        }
        if now >= self.deadline {
            for proc in procs {
                match proc {
                    Process::Binary(p) => eprintln!(
                        "binary exit={:?} empty={} stderr={}",
                        p.exit_status(),
                        p.is_empty(),
                        String::from_utf8_lossy(p.stderr())
                    ),
                    Process::Script(Proc::Terminal(p)) => {
                        eprintln!("person empty={} shown={}", p.is_empty(), String::from_utf8_lossy(p.shown()));
                    }
                    Process::Script(Proc::Peer(p)) => {
                        eprintln!("queries={:?}", smith_local_process_world::llm::queries(p).count());
                    }
                    Process::Script(_) | Process::Sample(_) => {}
                }
            }
        }
        let authenticated = procs.iter().any(|p| matches!(p, Process::Script(Proc::Issuer(_))));
        if authenticated && !self.saved && procs.iter().any(|p| matches!(p, Process::Script(Proc::Peer(p)) if smith_local_process_world::llm::queries(p).next().is_some())) {
            let bytes = std::fs::read(&self.tokens).expect("saved before grant use");
            let record = skein_oauth::decode_record(&bytes, &smith_local_shell::local_host::token_limits()).expect("private token record");
            let directory = self.tokens.ancestors().nth(3).expect("scratch credential path");
            assert!(record.access_token == fixture_access(directory), "token persists before provider query");
            self.saved = true;
        }
        let shown = procs
            .iter()
            .find_map(|proc| match proc {
                Process::Script(Proc::Terminal(person)) => Some(person.shown()),
                Process::Binary(_) | Process::Script(_) | Process::Sample(_) => None,
            })
            .unwrap_or_default();
        for proc in procs {
            if let Process::Binary(p) = proc
                && let Some(exit) = p.exit_status()
            {
                assert_eq!(
                    exit,
                    Exit::Code(0),
                    "shipped local failed: {}\nterminal: {}",
                    String::from_utf8_lossy(p.stderr()),
                    String::from_utf8_lossy(shown)
                );
            }
        }
        if !self.reviewed && procs.iter().all(Host::is_empty) {
            let seen = observations(procs, self.saved);
            referee::review_binary(&seen, &self.checkout, self.ending.clone()).assert_passed(self.seed);
            self.reviewed = true;
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        self.next_act.into_iter().chain([self.deadline]).min()
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= self.deadline).then(|| "shipped Smith binary story did not settle".into())
    }
    fn passed(&self) -> bool {
        self.reviewed
    }
}

fn observations(procs: &[Process], saved: bool) -> Seen {
    let person = procs
        .iter()
        .find_map(|p| if let Process::Script(Proc::Terminal(p)) = p { Some(p) } else { None })
        .expect("person");
    let peer = procs
        .iter()
        .find_map(|p| if let Process::Script(Proc::Peer(p)) = p { Some(p) } else { None })
        .expect("provider");
    let browser = procs.iter().find_map(|p| if let Process::Script(Proc::Browser(p)) = p { Some(p) } else { None });
    let issuer = procs.iter().find_map(|p| if let Process::Script(Proc::Issuer(p)) = p { Some(p) } else { None });
    Seen {
        facts: vec![],
        shown: person.shown().to_vec(),
        errors: person.errors().to_vec(),
        exit: person.exit(),
        pushed: false,
        queries: smith_local_process_world::llm::queries(peer).cloned().collect(),
        oauth: issuer.map(|issuer| referee::OAuthSeen {
            posts: smith_local_process_world::oauth::posts(issuer.peer()),
            pages: browser.map_or(0, |b| b.pages()),
            browser_replied: browser.is_some_and(|b| b.replied()),
            saved_before_query: saved,
        }),
    }
}

#[must_use]
pub fn start(arguments: Vec<std::ffi::OsString>, directory: &Path, mode: Mode) -> Binary {
    start_program(&skein_scratch::paths::binary("smith"), arguments, directory, mode)
}

fn start_program(program: &Path, arguments: Vec<std::ffi::OsString>, directory: &Path, mode: Mode) -> Binary {
    Binary::start(
        Command {
            program: program.into(),
            arguments,
            environment: fixture_environment(
                directory,
                vec![
                    ("HOME".into(), directory.into()),
                    ("XDG_CONFIG_HOME".into(), directory.join("config").into()),
                    ("XDG_STATE_HOME".into(), directory.join("state").into()),
                    ("XDG_RUNTIME_DIR".into(), directory.join("runtime").into()),
                    ("PATH".into(), "/usr/bin:/bin".into()),
                ],
            ),
            directory: directory.into(),
        },
        mode,
        65_536,
    )
    .unwrap_or_else(|why| panic!("shipped Smith end to end requires usable io_uring and binary startup: {why:?}"))
}

fn fixture_environment(
    directory: &Path,
    mut environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(std::ffi::OsString, std::ffi::OsString)> {
    if directory.join("auth.json").exists() {
        use std::os::unix::ffi::OsStringExt;
        environment
            .push(("SMITH_FIXTURE_TOKEN".into(), std::ffi::OsString::from_vec(fixture_access(directory).into_vec())));
    }
    environment
}

fn fixture_access(directory: &Path) -> Box<[u8]> {
    let login: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.join("auth.json")).expect("synthetic login file"))
            .expect("synthetic login JSON");
    login
        .pointer("/tokens/access_token")
        .and_then(serde_json::Value::as_str)
        .expect("synthetic access token")
        .as_bytes()
        .into()
}

#[must_use]
pub fn settings(scratch: &Scratch, scenario: &Scenario) -> Box<[u8]> {
    let token = smith_local_process_world::world::login_at(Clock::new().now().wall, false);
    let login = serde_json::json!({"tokens":{"access_token":std::str::from_utf8(&token).expect("synthetic token text"),"account_id":"acc"}});
    std::fs::write(scratch.path().join("auth.json"), serde_json::to_vec(&login).expect("synthetic login JSON"))
        .expect("synthetic login file");
    let mut text = format!(
        "credentials='{}'\nmodel='fake/fake'\nchat='chat'\ninstructions='@local-shell Assist'\nwaiting_seconds=30\n\
        [budget]\nturns=512\nspend=25.0\nseconds=3600\n\
        [deployment]\nmemory=4294967296\nconversations=2\nllm_pool=134217728\ngroup_stop=1\n\
        [models.'fake/fake']\nwindow=2097152\noutput=65536\nreasoning_item=2048\nhead=60\nidle=30\n\
        input_price=1.25\ncached_price=0.125\noutput_price=10.0\nprice_unit=1000000\n\
        [endpoints.fake]\npreset='codex'\naccount='fake'\naddress='{}:34443'\nserver_name='skein.test'\n\
        identity='plain'\ntrust.system=false\ntrust.der=['{}']\n\
        [environment]\ninherit=[]\n[trace]\npath='{}'\ncapture='calls'\n",
        scratch.path().join("credentials").display(),
        smith_real_world::loopback(),
        scratch.path().join("root.der").display(),
        scratch.path().join("agent-trace.jsonl").display()
    );
    if scenario.launch.authenticated {
        write!(text, "[accounts.fake]\nsource='sign_in'\nclient_id='client'\nauthorization_url='http://{host}:34444/authorize'\n\
            token_url='http://{host}:34444/token'\nredirect_uri='http://127.0.0.1:{redirect}/callback'\nscope='read'\ntoken_format='form'\n", host = smith_real_world::loopback(), redirect = smith_real_world::redirect_port()).expect("fixture string");
    } else {
        text.push_str("[accounts.fake]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n");
    }
    if scenario.launch.change {
        write!(text, "[[workspace.directories]]\nname='repo'\npath='{}'\nwritable=true\ngit=true\n\
            [workspace.contract]\nform='change'\nchecks_must_pass={}\n\
            [[workspace.contract.fields]]\nname='title'\nmax=256\n[[workspace.contract.fields]]\nname='body'\nmax=1024\n",
            scratch.path().join("repo").display(),scenario.launch.tools).expect("fixture string");
    } else {
        text.push_str("[workspace.contract]\nform='report'\nmax=128\n");
    }
    if scenario.launch.tools {
        text.push_str("[workspace.conventions]\nguide='.smith-test/guide'\nchecks='.smith-test/check'\n");
    }
    let _path = user_layer(scratch, text.as_bytes());
    token
}

#[must_use]
pub fn run(scratch: &Scratch, scenario: Scenario) -> Seen {
    run_with_provider(scratch, scenario, None)
}

pub fn run_with_provider(
    scratch: &Scratch,
    mut scenario: Scenario,
    provider: Option<skein_fake_peers::llm::Peer>,
) -> Seen {
    assert_no_children();
    scenario.launch.tls = true;
    let token = if provider.is_some() { fixture_access(scratch.path()) } else { settings(scratch, &scenario) };
    let path = trace_path(scratch.path());
    let old_trace = std::fs::read(&path).unwrap_or_default();
    let before = Before::new(scratch.path());
    let clock = Clock::new();
    let now = clock.now().now;
    let mut binary = start(Vec::new(), scratch.path(), Mode::Terminal);
    let streams = binary.take_streams().expect("scripted terminal");
    let descriptors = streams.descriptors();
    let Streams::Terminal { stream } = streams else { unreachable!("person on terminal") };
    let mut world = real::World::new(Judge {
        next_act: None,
        sign_in: referee::SignIn::default(),
        visiting: false,
        shutdown: None,
        deadline: now.saturating_add(Duration::from_secs(10)),
        saved: false,
        tokens: scratch.path().join("credentials/tokens/fake"),
        checkout: if scenario.launch.change { scratch.checkout() } else { smith_real_world::Checkout::default() },
        ending: scenario.ending.clone(),
        seed: scenario.launch.seed,
        reviewed: false,
    });
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn_with_fds(descriptors, || {
        Process::Script(Proc::Terminal(Box::new(Terminal::attached(stream, scenario.commands.clone()))))
    });
    world.spawn(|| {
        Process::Script(Proc::Peer(Box::new(provider.unwrap_or_else(|| {
            scenario.provider_with(skein_llm::Credential {
                value: token.clone(),
                account_id: b"acc".as_slice().into(),
                scheme: skein_llm::Scheme::Bearer,
            })
        }))))
    });
    if scenario.launch.authenticated {
        world.spawn(|| {
            Process::Script(Proc::Issuer(Box::new(process::IssuerProcess::with_token(
                skein_fake_peers::Transport::Plaintext,
                34_444,
                token.clone(),
            ))))
        });
        world.spawn(|| Process::Script(Proc::Browser(Box::default())));
    }
    let outcome = world.run(&clock, Duration::from_secs(12));
    assert!(outcome.procs.iter().all(Host::is_empty), "binary and scripted processes drained");
    let seen = observations(&outcome.procs, scenario.launch.authenticated);
    let mut resolved = resolved_settings(scratch.path());
    before.assert_binary_expected(&scenario, &seen, &resolved);
    let records = read_trace_resolved(&mut resolved, &old_trace);
    assert!(
        records.iter().any(|record| matches!(&record.event,
            smith_events::Event::ToolStarted(started) if started.tool.as_ref() == b"finish")),
        "agent actually called finish"
    );
    let trace = std::fs::read(path).expect("agent trace");
    for credential in [b"access-new".as_slice(), b"refresh-new".as_slice()] {
        assert!(!trace.windows(credential.len()).any(|bytes| bytes == credential), "trace excludes credentials");
    }
    assert_no_children();
    seen
}

/// Decode actual append bytes within the producer's configured receiving bounds.
/// Each appended invocation is checked independently; a prior prefix stays untouched.
#[must_use]
pub fn read_trace(directory: &Path, prefix: &[u8]) -> Vec<smith_events::Record> {
    read_trace_resolved(&mut resolved_settings(directory), prefix)
}

fn resolved_settings(directory: &Path) -> smith_local_shell::settings::resolve::Resolved {
    let source = smith_local_shell::settings::Source {
        layer: smith_local_shell::settings::Layer::User,
        path: directory.join("config/smith/config.toml"),
        text: std::fs::read_to_string(directory.join("config/smith/config.toml")).expect("product settings"),
    };
    let settings = smith_local_shell::settings::from_sources(
        &smith_local_shell::command::Shared::default(),
        directory.into(),
        &[source],
    )
    .expect("merged product settings");
    smith_local_shell::settings::resolve::resolve_startup(&settings).unwrap_or_else(|why| panic!("{}", why.line())).0
}

fn read_trace_resolved(
    resolved: &mut smith_local_shell::settings::resolve::Resolved,
    prefix: &[u8],
) -> Vec<smith_events::Record> {
    let trace = resolved.agent.trace.as_ref().expect("configured agent destination").path.clone();
    let _limits = smith_local_shell::check::interactive_limits(resolved).expect("producer limits");
    let configuration = &resolved.agent;
    let events = configuration.service.limits.events.expect("configured event producer");
    let limits = events.sinks.records;
    let ceiling = events.sinks.line_ceiling(events.capture).expect("actual writer-derived line ceiling");
    let bytes = std::fs::read(trace).expect("actual append trace");
    assert!(bytes.starts_with(prefix), "durable trace prefix stays unchanged");
    let mut reader = smith_events::Reader::with_ceiling(&limits, ceiling)
        .unwrap_or_else(|why| panic!("configured receiving bounds {limits:?}: {why:?}"));
    let mut records = Vec::new();
    for chunk in bytes.get(prefix.len()..).expect("prefix retained").chunks(7) {
        let mut remaining = chunk;
        while !remaining.is_empty() {
            let line = reader.feed(remaining).expect("typed producer line");
            assert!(line.consumed > 0, "bounded reader makes progress");
            remaining = remaining.get(line.consumed..).expect("consumed input");
            if line.complete {
                records.push(line.record.expect("the writing version names a known record"));
            }
        }
    }
    reader.finish().expect("the append file ends between complete records");
    let mut beginning = 0_usize;
    for (index, record) in records.iter().enumerate() {
        if matches!(&record.event, smith_events::Event::SessionEnded(_)) {
            smith_agent_process_world::assert_event_conformance(
                records.get(beginning..=index).expect("one actual process stream"),
            );
            beginning = index.checked_add(1).expect("bounded trace inventory");
        }
    }
    assert_eq!(
        beginning,
        records.len(),
        "each native invocation has its authoritative end; record types {:?}",
        records.iter().map(|record| std::mem::discriminant(&record.event)).collect::<Vec<_>>()
    );
    assert!(!records.is_empty(), "the shipped process wrote its configured trace");
    records
}

struct Refusal {
    poll: Time,
    next_act: Option<Time>,
    terminal_sign_in: referee::SignIn,
    stderr_sign_in: referee::SignIn,
    visiting: bool,
    shutdown: bool,
    foreign: Option<std::path::PathBuf>,
    deadline: Time,
    trace: Option<(std::path::PathBuf, bool)>,
}
impl Refusal {
    fn new(now: Time, deadline: Time) -> Self {
        Self {
            poll: now,
            next_act: None,
            terminal_sign_in: referee::SignIn::default(),
            stderr_sign_in: referee::SignIn::default(),
            visiting: false,
            shutdown: false,
            foreign: None,
            deadline,
            trace: None,
        }
    }
}

impl Referee<Process> for Refusal {
    fn act(&mut self, now: Time, procs: &mut [Process]) {
        if now >= self.poll
            && let Some(runtime) = &self.foreign
        {
            for entry in std::fs::read_dir(runtime).into_iter().flatten() {
                let entry = entry.expect("private fixture entry");
                if entry.file_name().as_encoded_bytes().starts_with(b".smith-exec-") {
                    std::fs::write(entry.path().join("foreign.txt"), b"kept foreign content")
                        .expect("foreign fixture file");
                    self.foreign = None;
                    break;
                }
            }
        }
        let exit = procs.iter().find_map(|p| if let Process::Binary(p) = p { p.exit_status() } else { None });
        self.next_act = None;
        let url = self.terminal_sign_in.url().or_else(|| self.stderr_sign_in.url());
        let listening = procs.iter().any(|process| {
            matches!(process,
            Process::Script(Proc::Issuer(issuer)) if issuer.peer().address().is_some())
        });
        if let Some(exit) = exit {
            self.shutdown = true;
            for p in procs {
                match p {
                    Process::Script(Proc::Terminal(p)) => p.finished(exit),
                    Process::Script(Proc::Peer(p)) => p.shutdown(),
                    Process::Script(Proc::Issuer(p)) => p.shutdown(),
                    Process::Script(Proc::Browser(p)) => p.shutdown(),
                    Process::Binary(_) | Process::Script(_) | Process::Sample(_) => {}
                }
            }
        } else if listening && !self.visiting {
            for process in procs {
                if let Process::Script(Proc::Browser(browser)) = process
                    && browser.pages() == 0
                    && let Some(url) = url
                {
                    browser.visit(url);
                    self.visiting = true;
                }
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Process]) {
        for process in procs {
            match process {
                Process::Binary(binary) => self.stderr_sign_in.observe(binary.stderr()),
                Process::Script(Proc::Terminal(person)) => self.terminal_sign_in.observe(person.shown()),
                Process::Script(_) | Process::Sample(_) => {}
            }
        }
        let visit = !self.visiting
            && (self.terminal_sign_in.url().is_some() || self.stderr_sign_in.url().is_some())
            && procs.iter().any(
                |process| matches!(process, Process::Script(Proc::Issuer(issuer)) if issuer.peer().address().is_some()),
            );
        if visit
            || (!self.shutdown
                && procs.iter().any(|p| matches!(p, Process::Binary(binary) if binary.exit_status().is_some())))
        {
            self.next_act = Some(now);
        }

        if now >= self.poll
            && let Some((path, growing)) = &mut self.trace
            && !*growing
            && !procs.iter().any(|p| matches!(p, Process::Binary(binary) if binary.exit_status().is_some()))
        {
            let bytes = std::fs::read(path).unwrap_or_default();
            *growing |= bytes.contains(&b'\n');
        }
        if now >= self.poll {
            self.poll = now.saturating_add(Duration::from_millis(1));
        }
        if now >= self.deadline {
            for process in procs {
                match process {
                    Process::Binary(binary) => eprintln!(
                        "binary exit={:?} empty={} stderr={}",
                        binary.exit_status(),
                        binary.is_empty(),
                        String::from_utf8_lossy(binary.stderr())
                    ),
                    Process::Script(Proc::Terminal(person)) => eprintln!(
                        "terminal empty={} shown={}",
                        person.is_empty(),
                        String::from_utf8_lossy(person.shown())
                    ),
                    Process::Script(_) | Process::Sample(_) => {}
                }
            }
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        // Foreign runtime entries and trace appends have no ring completion here.
        // Binary/terminal observations and exit already wake the loop.
        let poll =
            (self.foreign.is_some() || self.trace.as_ref().is_some_and(|(_, growing)| !growing)).then_some(self.poll);
        self.next_act.into_iter().chain(poll).chain([self.deadline]).min()
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= self.deadline).then(|| "startup refusal did not exit and drain".into())
    }
    fn passed(&self) -> bool {
        self.foreign.is_none() && self.trace.as_ref().is_none_or(|(_, growing)| *growing)
    }
}

#[must_use]
pub fn refusal_program(program: &Path, scratch: &Scratch, arguments: Vec<std::ffi::OsString>) -> Vec<u8> {
    refusal_program_code(program, scratch, arguments, 1)
}

#[must_use]
pub fn refusal_code(scratch: &Scratch, arguments: Vec<std::ffi::OsString>, code: u8) -> Vec<u8> {
    refusal_program_code(&skein_scratch::paths::binary("smith"), scratch, arguments, code)
}

fn refusal_program_code(
    program: &Path,
    scratch: &Scratch,
    arguments: Vec<std::ffi::OsString>,
    refusal_code: u8,
) -> Vec<u8> {
    assert_no_children();
    let clock = Clock::new();
    let now = clock.now().now;
    let mut binary = start_program(program, arguments, scratch.path(), Mode::Terminal);
    let streams = binary.take_streams().expect("startup terminal");
    let descriptors = streams.descriptors();
    let Streams::Terminal { stream } = streams else { unreachable!("startup on terminal") };
    let mut world = real::World::new(Refusal::new(now, now.saturating_add(Duration::from_secs(2))));
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn_with_fds(descriptors, || {
        Process::Script(Proc::Terminal(Box::new(Terminal::attached(
            stream,
            vec![smith_local_process_world::terminal::Action::Eof],
        ))))
    });
    let outcome = world.run(&clock, Duration::from_secs(4));
    assert!(outcome.procs.iter().all(Host::is_empty), "startup refusal drains all processes");
    let Process::Binary(binary) = outcome.procs.first().expect("binary process") else {
        unreachable!("binary observer")
    };
    assert_eq!(binary.exit_status(), Some(Exit::Code(refusal_code)), "startup refusal keeps its command category");
    let Process::Script(Proc::Terminal(person)) = outcome.procs.get(1).expect("terminal process") else {
        unreachable!("person")
    };
    assert!(person.shown().is_empty(), "startup refusal opens no channel or chat");
    assert_no_children();
    binary.stderr().to_vec()
}

pub mod live_run;

/// The agent's expected destination from the story's selected placement.
#[must_use]
pub fn trace_path(directory: &Path) -> std::path::PathBuf {
    directory.join("agent-trace.jsonl")
}

/// Outside observations of one product invocation on pipes.
pub struct Ran {
    pub code: u8,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// A user layer confined to this story's scratch configuration directory.
#[must_use]
pub fn user_layer(scratch: &Scratch, bytes: &[u8]) -> std::path::PathBuf {
    let path = scratch.path().join("config/smith/config.toml");
    std::fs::create_dir_all(path.parent().expect("user layer parent")).expect("scratch user directory");
    std::fs::write(&path, bytes).expect("scratch user layer");
    path
}

/// Drive the shipped product with only the scratch HOME and XDG locations.
#[must_use]
pub fn smith(scratch: &Scratch, arguments: &[&str], input: &[u8]) -> Ran {
    smith_with_environment(scratch, arguments, input, Vec::new())
}

/// Run with caller-supplied synthetic variables beside the scratch locations.
pub fn smith_with_environment(
    scratch: &Scratch,
    arguments: &[&str],
    input: &[u8],
    additional: Vec<(std::ffi::OsString, std::ffi::OsString)>,
) -> Ran {
    assert_no_children();
    let environment = vec![
        ("HOME".into(), scratch.path().into()),
        ("XDG_CONFIG_HOME".into(), scratch.path().join("config").into()),
        ("XDG_STATE_HOME".into(), scratch.path().join("state").into()),
        ("XDG_RUNTIME_DIR".into(), scratch.path().join("runtime").into()),
        ("PATH".into(), "/usr/bin:/bin".into()),
    ];
    let mut environment = fixture_environment(scratch.path(), environment);
    environment.extend(additional);
    let mut binary = Binary::start(
        Command {
            program: skein_scratch::paths::binary("smith"),
            arguments: arguments.iter().map(std::ffi::OsString::from).collect(),
            environment,
            directory: scratch.path().into(),
        },
        Mode::Pipes,
        65_536,
    )
    .expect("shipped product pipes");
    let streams = binary.take_streams().expect("product streams");
    let descriptors = streams.descriptors();
    let Streams::Pipes { input: stdin, output } = streams else { unreachable!("product on pipes") };
    let clock = Clock::new();
    let now = clock.now().now;
    let mut world = real::World::new(Refusal::new(now, now.saturating_add(Duration::from_secs(2))));
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn_with_fds(descriptors, || {
        let mut commands = Vec::new();
        if !input.is_empty() {
            commands.push(smith_local_process_world::terminal::Action::Send(input.into()));
        }
        commands.push(smith_local_process_world::terminal::Action::Eof);
        Process::Script(Proc::Terminal(Box::new(Terminal::piped(stdin, output, commands))))
    });
    let outcome = world.run(&clock, Duration::from_secs(4));
    assert!(outcome.procs.iter().all(Host::is_empty), "product and person drain");
    let Process::Binary(binary) = outcome.procs.first().expect("binary observation") else { unreachable!("binary") };
    let Process::Script(Proc::Terminal(person)) = outcome.procs.get(1).expect("person observation") else {
        unreachable!("person")
    };
    let Some(Exit::Code(code)) = binary.exit_status() else { panic!("product exit code") };
    assert_no_children();
    Ran { code, stdout: person.shown().into(), stderr: binary.stderr().into() }
}

mod exec;
pub use exec::{
    Reply, chat, chat_modes, chat_resized, chat_signed_in, chat_traced, chat_waiting, exec_with_foreign_file,
    exec_with_provider, exec_with_turns, exec_without_head, fake_provider_settings, key_chat, program_chat,
    program_interrupt, run as run_product, run_scripted as run_product_scripted,
};

pub mod login;

pub mod sample_host;

pub mod actor_refusal;

mod window;

pub mod provider_login;

mod program_interrupt;
