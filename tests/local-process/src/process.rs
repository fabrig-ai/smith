//! Reusable local-host processes and hosted factories (testing.md, sections
//! 2.1, 2.2 and 5). Each process owns its IO and outside observations. Launch
//! arguments carry immutable scenario settings; no process knows a simulator,
//! fake checkout, another process's heap or the driving loop.
use crate::{
    llm::Peer,
    oauth::{Browser, Peer as Issuer},
    terminal::Terminal,
};
use serde::{Deserialize, Serialize};
use skein_fake_oauth as fake_oauth;
use skein_io::kernel;
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::Host;
use skein_world::{Inherited, StartupRoot};
use smith_agent_service as agent;
use smith_local_domain as local;
use smith_local_service as service;
use smith_local_shell::local_host::{Local, Resources, token_limits};
use std::{os::unix::ffi::OsStrExt, path::PathBuf};

mod configuration;

pub use configuration::{
    agent_document, configuration, configuration_with, configuration_with_tools, lower_configuration,
    lower_configuration_with, prepared_accounts,
};
use configuration::{configuration_for_launch, specialize};

/// Immutable configuration sent by the terminal and passed to a spawned agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Launch {
    pub seed: u64,
    pub tls: bool,
    pub trust_der: Option<PathBuf>,
    pub change: bool,
    /// Include the real-loop tool and mandatory-check story.
    #[serde(default)]
    pub tools: bool,
    /// Keep the workspace but accept a report after reads.
    #[serde(default)]
    pub read_report: bool,
    /// Start one unattended run from the fixture's retained prompt.
    #[serde(default)]
    pub headless: bool,
    /// Select the product's line-mode front on the same local composition.
    #[serde(default)]
    pub interactive: bool,
    /// Compose the alternate model and model-owned efforts for local-choice stories.
    #[serde(default)]
    pub choices: bool,
    /// Admit three tool turns, so the fourth would exceed the budget.
    #[serde(default)]
    pub budget_failure: bool,
    pub placement: crate::Placement,
    pub keep_facts: bool,
    #[serde(default)]
    pub traced: bool,
    pub authenticated: bool,
    #[serde(default)]
    pub lending: Option<crate::world::Lending>,
    pub state_directory: PathBuf,
    pub token_directory: PathBuf,
    pub root_path: PathBuf,
}
impl Launch {
    /// Exact arguments used unchanged by both kernel backends.
    #[must_use]
    pub fn arguments(&self) -> Box<[Box<[u8]>]> {
        Box::new([serde_json::to_vec(self).expect("scenario arguments").into_boxed_slice()])
    }
    fn read(spawn: &kernel::Spawn) -> Self {
        assert_eq!(spawn.args.len(), 1, "one bounded scenario document");
        serde_json::from_slice(&spawn.args[0]).expect("immutable scenario settings")
    }
}

fn agent_launch(spawn: &kernel::Spawn) -> Launch {
    let path = std::path::Path::new(std::ffi::OsStr::from_bytes(&spawn.args[0]));
    serde_json::from_slice(&std::fs::read(path.with_file_name("launch.json")).unwrap()).expect("fixture launch")
}

// Observe emitted trace bytes at the public kernel boundary, preserving every submission.
fn wait_in_trace(submissions: &mut Queue<kernel::Submit>) -> bool {
    let mut wait = false;
    for _ in 0..submissions.len() {
        let submit = submissions.pop().expect("bounded output snapshot");
        if let kernel::Op::Append { bytes, .. } = &submit.kind {
            wait |= bytes.windows(b"\"type\":\"tool.started\"".len()).any(|part| part == b"\"type\":\"tool.started\"")
                && bytes.windows(b"\"tool\":\"wait\"".len()).any(|part| part == b"\"tool\":\"wait\"");
        }
        submissions.push(submit);
    }
    wait
}

/// The scripted issuer and its one successful token rotation.
#[must_use]
pub fn issuer() -> Issuer {
    issuer_with(skein_fake_peers::Transport::Plaintext)
}

/// The identical issuer domain, with TLS selected in the real-loop tier.
#[must_use]
pub fn issuer_with(transport: skein_fake_peers::Transport) -> Issuer {
    let port = if transport == skein_fake_peers::Transport::Tls { 34_444 } else { 444 };
    issuer_at(transport, port)
}

/// The same fake issuer domain at an explicitly selected test port.
#[must_use]
pub fn issuer_at(transport: skein_fake_peers::Transport, port: u16) -> Issuer {
    issuer_with_token(transport, port, b"access-new".as_slice().into())
}

fn issuer_with_token(transport: skein_fake_peers::Transport, port: u16, access_token: Box<[u8]>) -> Issuer {
    let scheme = if transport == skein_fake_peers::Transport::Tls { "https" } else { "http" };
    let limits = fake_oauth::Limits {
        document: token_limits(),
        uri_bytes: 8192,
        request_bytes: 16_384,
        codes: 4,
        carried: 0,
        rotations: 4,
        plans: 4,
    };
    let response_bytes = skein_fake_peers::oauth::least_response(&limits).expect("issuer response floor fits u32");
    let host = smith_agent_process_world::fake::loopback();
    let redirect = smith_agent_process_world::fake::redirect_port();
    let mut issuer = Issuer::new(
        (host, port).into(),
        transport,
        smith_agent_process_world::fake::limits(),
        fake_oauth::Config {
            authorization_url: format!("{scheme}://{host}:{port}/authorize").into_bytes().into(),
            token_endpoint: format!("{scheme}://{host}:{port}/token").into_bytes().into(),
            client_id: b"client".as_slice().into(),
            client_secret: None,
            terms: None,
            redirect_uri: format!("http://127.0.0.1:{redirect}/callback").into_bytes().into(),
            refresh_token: b"refresh-old".as_slice().into(),
        },
        limits,
        skein_http::server::Limits {
            head: 8192,
            headers: 32,
            body: 16_384,
            read: 1024,
            response: response_bytes,
            send: 1024,
        },
    )
    .expect("issuer fixture");
    issuer
        .queue(fake_oauth::Plan {
            status: 200,
            body: fake_oauth::Body::Token(skein_oauth::TokenResponse {
                scope: None,
                id_token: None,
                earliest_refresh_at: None,
                access_token,
                refresh_token: Some(b"refresh-new".as_slice().into()),
                expires_in: 7200,
            }),
            delay: Duration::ZERO,
            retry_after: Duration::ZERO,
        })
        .expect("token plan");
    issuer
}

/// Local shell and retained outside facts, ending after the shared stderr bridge.
pub struct LocalProcess {
    local: skein_world::PipeHost<Local>,
    facts: Vec<local::Fact>,
    keep_facts: bool,
    wait_seen: bool,
}
impl LocalProcess {
    /// Wrap a configured shared shell inside its own metered factory call.
    #[must_use]
    pub fn new(local: skein_world::PipeHost<Local>, keep_facts: bool) -> Self {
        Self { local, facts: Vec::with_capacity(256), keep_facts, wait_seen: false }
    }
    /// Facts emitted at the public boundary, independent of internal state.
    #[must_use]
    pub fn facts(&self) -> &[local::Fact] {
        &self.facts
    }
    /// The first wait event observed in the process's outgoing trace records.
    #[must_use]
    pub fn wait_seen(&self) -> bool {
        self.wait_seen
    }

    /// The shared shell's public terminal outcome.
    #[must_use]
    pub fn result(&self) -> Option<Result<local::ExitStatus, &str>> {
        self.local.inner().result()
    }
}
impl Host for LocalProcess {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.local.iterate(now, wall);
        while let Some(fact) = self.local.inner_mut().pop_fact() {
            if self.keep_facts {
                assert!(self.facts.len() < 256);
                self.facts.push(fact);
            }
        }
    }
    fn drain(&mut self) {
        self.local.drain();
        self.wait_seen |= wait_in_trace(self.local.submissions());
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.local.completions()
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.local.submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.local.work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.local.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.local.next_policy_deadline()
    }
    fn is_empty(&self) -> bool {
        self.local.is_empty()
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.local.exit()
    }
    fn worst_case(&self) -> u64 {
        self.local.worst_case() + 1_048_576 + (size_of::<local::Fact>() * 256 + size_of::<Self>()) as u64
    }
    fn operations(&self) -> u32 {
        self.local.operations()
    }
}

/// Browser admitted before sign-in, receiving a bounded URL flag between turns.
pub struct BrowserProcess {
    browser: Option<Browser>,
    url: Box<[u8]>,
    length: usize,
    stopped: bool,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
}
impl Default for BrowserProcess {
    fn default() -> Self {
        Self::new()
    }
}
impl BrowserProcess {
    /// Prepare a browser without allocating outside its process later.
    #[must_use]
    pub fn new() -> Self {
        Self {
            browser: None,
            url: vec![0; 8192].into_boxed_slice(),
            length: 0,
            stopped: false,
            completions: Queue::with_capacity(64),
            submissions: Queue::with_capacity(64),
        }
    }
    /// The referee copies only observed URL bytes into already owned storage.
    pub fn visit(&mut self, url: &[u8]) {
        assert!(self.length == 0 && self.browser.is_none() && url.len() <= self.url.len());
        self.url[..url.len()].copy_from_slice(url);
        self.length = url.len();
    }
    /// Stop an unused browser once the terminal exits.
    pub fn shutdown(&mut self) {
        self.stopped = true;
    }
    /// Whether one authorization page was actually configured.
    #[must_use]
    pub fn pages(&self) -> u32 {
        u32::from(self.length > 0)
    }
    /// The browser's actual loopback callback response.
    #[must_use]
    pub fn replied(&self) -> bool {
        self.browser.as_ref().is_some_and(Browser::saw_reply)
    }
}
impl Host for BrowserProcess {
    fn iterate(&mut self, now: Time, wall: Wall) {
        if self.browser.is_none() && self.length > 0 {
            self.browser = Some(Browser::new(&self.url[..self.length]));
        }
        if let Some(browser) = &mut self.browser {
            while let Some(complete) = self.completions.pop() {
                browser.completions().push(complete);
            }
            browser.iterate(now, wall);
            while let Some(submit) = browser.submissions().pop() {
                self.submissions.push(submit);
            }
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        (self.browser.is_none() && self.length > 0)
            || self.browser.as_ref().is_some_and(|b| b.work_pending(now))
            || !self.completions.is_empty()
            || !self.submissions.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.browser.as_ref().and_then(Host::next_deadline)
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.browser.as_ref().and_then(Host::next_policy_deadline)
    }
    fn is_empty(&self) -> bool {
        self.browser.as_ref().map_or(self.stopped, Host::is_empty)
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        crate::oauth::browser_worst_case() + 131_072
    }
    fn operations(&self) -> u32 {
        128
    }
}

mod provider;

/// Issuer transport and the wall time of its first observed token POST.
pub struct IssuerProcess {
    peer: Issuer,
    first_post: Option<Wall>,
}
impl IssuerProcess {
    /// Build the scenario's independent issuer inside its metered process.
    #[must_use]
    pub fn new() -> Self {
        Self::configured(skein_fake_peers::Transport::Plaintext)
    }
    /// Construct the same issuer process with explicit tier transport.
    #[must_use]
    pub fn configured(transport: skein_fake_peers::Transport) -> Self {
        Self { peer: issuer_with(transport), first_post: None }
    }
    /// Prepare a new process story at an unprivileged port.
    #[must_use]
    pub fn at(transport: skein_fake_peers::Transport, port: u16) -> Self {
        Self { peer: issuer_at(transport, port), first_post: None }
    }
    /// Issue this synthetic token for the product's claim extraction story.
    pub fn with_token(transport: skein_fake_peers::Transport, port: u16, token: Box<[u8]>) -> Self {
        Self { peer: issuer_with_token(transport, port, token), first_post: None }
    }
    /// Script smith's own issued registration, identity and optional revocation.
    #[must_use]
    pub fn provider(port: u16, subject: &[u8], plan: bool, again: bool, revocation: Option<bool>) -> Self {
        Self { peer: provider::issuer(port, subject, plan, again, revocation), first_post: None }
    }
    /// Peer observations only.
    #[must_use]
    pub fn peer(&self) -> &Issuer {
        &self.peer
    }
    /// Observed token endpoint arrival time.
    #[must_use]
    pub fn first_post(&self) -> Option<Wall> {
        self.first_post
    }
    /// Flag normal shutdown after terminal settlement.
    pub fn shutdown(&mut self) {
        self.peer.shutdown();
    }
}
impl Default for IssuerProcess {
    fn default() -> Self {
        Self::new()
    }
}
impl Host for IssuerProcess {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.peer.iterate(now, wall);
        if self.first_post.is_none() && crate::oauth::posts(&self.peer) > 0 {
            self.first_post = Some(wall);
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.peer.completions()
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.peer.submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.peer.work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.peer.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.peer.next_policy_deadline()
    }
    fn is_empty(&self) -> bool {
        self.peer.is_empty()
    }
    fn worst_case(&self) -> u64 {
        self.peer.worst_case() + size_of::<Self>() as u64
    }
    fn operations(&self) -> u32 {
        self.peer.operations()
    }
}

/// The scenario's processes, driven unchanged by either shared kernel loop.
pub enum Proc {
    /// The scripted terminal, starting one local invocation and draining its exit.
    Terminal(Box<Terminal>),
    /// The actual shared local shell, ending after its public result settles.
    Local(Box<LocalProcess>),
    /// The spawned agent service, ending after its channel and descriptors settle.
    Agent(Box<skein_world::PipeHost<smith_agent_shell::Agent>>, smith_agent_process_world::AgentErrors, bool),
    /// A child that ends before admitting an opening.
    Ended(Box<crate::git::Child>),
    /// A child that answered and closed its descriptors but has not exited.
    LingeringAgent(Box<skein_world::PipeHost<smith_agent_shell::Agent>>),
    /// The independent fake provider, stopped after the terminal exits.
    Peer(Box<Peer>),
    /// The independent fake issuer and observed POST timing.
    Issuer(Box<IssuerProcess>),
    /// The scripted browser, following one authorization redirect to callback.
    Browser(Box<BrowserProcess>),
    /// The simulated fake-checkout child; real worlds execute git externally.
    Git(Box<crate::git::Prepared>),
}
impl Proc {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Terminal(p) => p.as_ref(),
            Self::Local(p) => p.as_ref(),
            Self::Agent(p, _, _) => p.as_ref(),
            Self::Ended(p) => p.as_ref(),
            Self::LingeringAgent(p) => p.as_ref(),
            Self::Peer(p) => p.as_ref(),
            Self::Issuer(p) => p.as_ref(),
            Self::Browser(p) => p.as_ref(),
            Self::Git(p) => p.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Terminal(p) => p.as_mut(),
            Self::Local(p) => p.as_mut(),
            Self::Agent(p, _, _) => p.as_mut(),
            Self::Ended(p) => p.as_mut(),
            Self::LingeringAgent(p) => p.as_mut(),
            Self::Peer(p) => p.as_mut(),
            Self::Issuer(p) => p.as_mut(),
            Self::Browser(p) => p.as_mut(),
            Self::Git(p) => p.as_mut(),
        }
    }
}
impl Host for Proc {
    fn drain(&mut self) {
        if let Self::Agent(agent, _, wait) = self {
            agent.drain();
            *wait |= wait_in_trace(agent.submissions());
        } else {
            self.host_mut().drain();
        }
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
        match self {
            Self::LingeringAgent(_) => false,
            _ => self.host().is_empty(),
        }
    }
    fn exit(&self) -> Option<kernel::Exit> {
        match self {
            Self::Ended(child) => child.is_empty().then_some(kernel::Exit::Code(child.code())),
            Self::LingeringAgent(_) => None,
            _ => self.host().exit(),
        }
    }
    fn worst_case(&self) -> u64 {
        self.host().worst_case() + size_of::<Self>() as u64 + 4096
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}
fn root(inherited: &Inherited, name: &[u8]) -> kernel::Fd {
    inherited.roots.iter().find(|(n, _)| n.as_ref() == name).expect("named child root").1
}

/// Fresh launch, delivery and colocated effect roots for the local shell.
#[must_use]
pub fn local_roots(spawn: &kernel::Spawn) -> Vec<StartupRoot> {
    let launch = Launch::read(spawn);
    let path = launch.root_path.as_os_str().as_encoded_bytes();
    let mut roots = vec![StartupRoot { name: b"launch".as_slice().into(), path: path.into() }];
    roots.push(StartupRoot {
        name: b"state".as_slice().into(),
        path: launch.state_directory.as_os_str().as_encoded_bytes().into(),
    });
    if launch.authenticated {
        roots.push(StartupRoot { name: b"credential".as_slice().into(), path: path.into() });
    }
    if launch.change {
        let path = launch.root_path.join("repo");
        roots.push(StartupRoot {
            name: b"delivery".as_slice().into(),
            path: path.as_os_str().as_encoded_bytes().into(),
        });
        if launch.placement == crate::Placement::InProcess {
            roots.push(StartupRoot {
                name: b"effect".as_slice().into(),
                path: path.as_os_str().as_encoded_bytes().into(),
            });
        }
    }
    roots
}
fn configure_choices(config: &mut service::Config, lower: &mut agent::Config) {
    let mut main = lower.domain.models[0].clone();
    main.efforts = Box::new([b"low".as_slice().into(), b"high".as_slice().into()]);
    let mut other = main.clone();
    other.model = b"alternate".as_slice().into();
    other.efforts = Box::new([b"low".as_slice().into()]);
    lower.domain.models = Box::new([main, other]);
    let mut names = skein_lib::List::with_capacity(1);
    names.push(smith_protocol_channel::Endpoint { name: b"main".as_slice().into(), number: 0, dialect: 1 }).unwrap();
    lower.channel_endpoints = smith_protocol_channel::Endpoints::new(names);
    config.endpoints = lower.channel_endpoints.clone();
    let main = config.local.models[0].clone();
    let mut other = main.clone();
    other.model = b"alternate".as_slice().into();
    config.local.models = Box::new([main, other]);
    config.local.efforts = lower.domain.models.iter().map(|model| model.efforts.clone()).collect();
    config.local.progress = Box::new([config.local.progress[0], config.local.progress[0]]);
    config.local.protocol_allowances =
        Box::new([config.local.protocol_allowances[0], config.local.protocol_allowances[0]]);
    config.limits.local.models = 2;
}

/// Adopt local startup resources without any backend-specific operations.
#[must_use]
pub fn make_local(spawn: &kernel::Spawn, inherited: &Inherited) -> Proc {
    let launch = Launch::read(spawn);
    let (mut config, mut lower) = configuration_for_launch(root(inherited, b"launch"), &launch);
    if launch.choices {
        configure_choices(&mut config, &mut lower);
    }
    if launch.headless {
        config.local.mode = local::Mode::Headless { prompt: b"hello".as_slice().into() };
    }
    if launch.budget_failure {
        config.local.budget.turns = 3;
    }
    if launch.read_report {
        config.local.contract =
            local::Contract::Report(smith_domain::run::outcome::TextSpec { max: 128, fields: Box::new([]) });
    }
    let kind = match launch.placement {
        crate::Placement::InProcess => smith_local_shell::compose::AgentKind::Inline,
        crate::Placement::Spawned => smith_local_shell::compose::AgentKind::Spawned { program: "smith-agent".into() },
    };
    let document = launch.token_directory.parent().unwrap().join("agent.json");
    if launch.placement == crate::Placement::Spawned {
        std::fs::write(&document, serde_json::to_vec(&agent_document(&launch)).unwrap()).unwrap();
        std::fs::write(document.with_file_name("launch.json"), serde_json::to_vec(&launch).unwrap()).unwrap();
    }
    config.launch = kind.launch(config.launch.root, &document);
    if launch.traced {
        lower.limits.events = Some(
            smith_agent_service::EventLimits::derive(
                &lower.limits,
                smith_agent_service::sinks::Capture::Everything,
                1,
                0,
                &[],
                Duration::from_secs(1),
            )
            .expect("trace limits"),
        );
    }
    let (lower, header) = if launch.traced || launch.interactive {
        let (lower, header) = product_header(&mut config, lower, &launch);
        (lower, Some(header))
    } else {
        (lower, None)
    };
    let fd = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1;
    let (writer, capture) = skein_world::PipeWriter::new(fd, 4096);
    let interactive = launch.interactive.then(|| {
        smith_local_shell::front::Front::Interactive(
            smith_local_shell::front::interactive::Interactive::new(true, &lower.limits).expect("bounded screen"),
        )
    });
    let (lower, spawned) =
        if launch.placement == crate::Placement::InProcess { (Some(lower), None) } else { (None, Some(lower)) };
    let handed = lending_accounts(&launch, spawn, config.limits.process.io).unwrap_or_else(|| {
        prepared_accounts(
            &launch.token_directory,
            launch.authenticated,
            launch.tls,
            launch.authenticated.then(|| root(inherited, b"credential")),
            config.limits.process.io,
        )
    });
    let mut local = Local::within_memory(
        config,
        lower,
        Resources {
            state_root: root(inherited, b"state"),
            input: inherited.pipes.iter().find(|(child, _)| *child == 0).expect("stdin").1,
            output: inherited.pipes.iter().find(|(child, _)| *child == 1).expect("stdout").1,
            signals: inherited.signal,
            delivery_roots: if launch.change { Box::new([root(inherited, b"delivery")]) } else { Box::new([]) },
            effect_roots: if launch.change && launch.placement == crate::Placement::InProcess {
                Box::new([root(inherited, b"effect")])
            } else {
                Box::new([])
            },
            delivery_environment: Box::new([]),
            seed: launch.seed,
            oauth_entropy: [31; 32],
        },
        Box::new(writer),
        Some(handed),
        interactive,
    )
    .expect("actual shared local shell");
    if let Some(configuration) = &spawned
        && launch.traced
    {
        local.prepare_trace(configuration).expect("priced trace");
    }
    if launch.traced {
        let fd = inherited.appends.iter().find(|(name, _)| name.as_ref() == b"product-trace").expect("trace fd").1;
        let delivery = smith_agent_service::sinks::Delivery::Complete;
        let header = header.expect("traced product header");
        local.adopt_trace(fd, None, Time::ZERO, header, delivery).expect("product trace adoption");
    } else if let Some(header) = header {
        local.start_session(Time::ZERO, header);
    }
    Proc::Local(Box::new(LocalProcess::new(capture.host(local), launch.keep_facts)))
}

fn product_header(
    config: &mut service::Config,
    lower: agent::Config,
    launch: &Launch,
) -> (agent::Config, smith_events::SessionStarted) {
    if launch.placement == crate::Placement::InProcess {
        let cancel = match config.limits.local.agent {
            local::AgentLimits::Inline(bounds) => bounds.cancel_grace,
            local::AgentLimits::Spawned(bounds) => bounds.cancel_grace,
        };
        config.limits.local.agent = local::AgentLimits::inline(
            config.limits.local.smith,
            config.limits.local.unsaved,
            config.local.budget.time,
            cancel,
        )
        .expect("inline startup bounds");
    }
    let mut configuration =
        smith_agent_shell::config::from_value(agent_document(launch)).expect("generated fixture JSON");
    configuration.service = lower;
    configuration.trace = None;
    let trace = smith_agent_shell::trace::TraceConfig {
        path: launch.root_path.join("events.jsonl"),
        capture: agent::sinks::Capture::Everything,
        delivery: agent::sinks::Delivery::Complete,
    };
    let header = Local::trace_header(
        &mut configuration,
        &mut config.limits,
        &trace,
        b"main".as_slice().into(),
        101,
        smith_local_shell::local_host::View { destinations: 1, front: launch.interactive },
        skein_lib::Duration::ZERO,
    )
    .expect("product effective header");
    (configuration.service, header)
}
/// The agent's declared workspace directories, each owned by this child.
#[must_use]
pub fn agent_roots(spawn: &kernel::Spawn) -> Vec<StartupRoot> {
    let launch = agent_launch(spawn);
    if launch.change {
        let path = launch.root_path.join("repo");
        vec![StartupRoot { name: b"repo".as_slice().into(), path: path.as_os_str().as_encoded_bytes().into() }]
    } else {
        vec![]
    }
}
/// Construct the actual agent service with the local scenario's settings.
#[must_use]
#[expect(
    clippy::zero_sized_map_values,
    reason = "The fixture retains adapter names even while Claude Code is the sole adapter."
)]
pub fn make_agent(spawn: &kernel::Spawn, inherited: &Inherited) -> Proc {
    let launch = agent_launch(spawn);
    let fd = inherited.pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1;
    let (errors, capture) = smith_agent_process_world::AgentErrors::pipe(fd);
    let document = std::path::Path::new(std::ffi::OsStr::from_bytes(&spawn.args[0]));
    let parsed = smith_agent_shell::config::read(document).expect("generated child JSON");
    let mut configuration = specialize(parsed.service);
    let trace = if launch.traced {
        configuration.limits.events = Some(
            agent::EventLimits::derive(
                &configuration.limits,
                agent::sinks::Capture::Everything,
                1,
                0,
                &[],
                Duration::from_secs(1),
            )
            .expect("trace limits"),
        );
        Some(smith_agent_shell::trace::TraceConfig {
            path: launch.root_path.join("events.jsonl.agent"),
            capture: agent::sinks::Capture::Everything,
            delivery: agent::sinks::Delivery::Complete,
        })
    } else {
        None
    };
    let agent = smith_agent_shell::Agent::new(
        smith_agent_shell::config::Configuration {
            programs: std::collections::BTreeMap::new(),
            endpoint_programs: std::collections::BTreeMap::new(),
            service: configuration,
            memory: parsed.memory,
            trace,
            profile: parsed.profile,
            declarations: parsed.declarations,
        },
        smith_agent_shell::Resources {
            programs: None,
            input: inherited.pipes.iter().find(|(child, _)| *child == 0).expect("stdin").1,
            output: inherited.pipes.iter().find(|(child, _)| *child == 1).expect("stdout").1,
            error: Some(fd),
            signals: inherited.signal,
            seed: launch.seed,
            pid: 102,
            trace: inherited.appends.first().map(|(_, fd)| *fd),
            roots: Some(inherited.roots.iter().map(|(_, fd)| *fd).collect()),
        },
        Box::new(errors.clone()),
    )
    .expect("configured agent trace");
    Proc::Agent(Box::new(capture.host(agent)), errors, false)
}

/// The product trace selected by the same launch document as its agent kind.
#[must_use]
pub fn local_appends(spawn: &kernel::Spawn) -> Vec<skein_world::StartupAppend> {
    let launch = Launch::read(spawn);
    if launch.traced {
        vec![skein_world::StartupAppend {
            name: b"product-trace".as_slice().into(),
            root: launch.root_path.as_os_str().as_encoded_bytes().into(),
            path: b"events.jsonl".as_slice().into(),
            mode: 0o600,
        }]
    } else {
        Vec::new()
    }
}

/// The spawned agent's own stream, distinct from the product's append.
#[must_use]
pub fn agent_appends(spawn: &kernel::Spawn) -> Vec<skein_world::StartupAppend> {
    let launch = agent_launch(spawn);
    let mut appends = if launch.traced {
        vec![skein_world::StartupAppend {
            name: b"product-trace".as_slice().into(),
            root: launch.root_path.as_os_str().as_encoded_bytes().into(),
            path: b"events.jsonl".as_slice().into(),
            mode: 0o600,
        }]
    } else {
        Vec::new()
    };
    if let Some(append) = appends.first_mut() {
        append.path = b"events.jsonl.agent".as_slice().into();
    }
    appends
}

/// Bind a child that opens its channel and exits without answering.
pub fn make_exited_agent(_: &kernel::Spawn, inherited: &Inherited) -> Proc {
    Proc::Ended(Box::new(crate::git::Child::unanswered(inherited)))
}

/// Reuse the real agent while withholding its process exit from the host kit.
#[must_use]
pub fn make_lingering_agent(spawn: &kernel::Spawn, inherited: &Inherited) -> Proc {
    let Proc::Agent(agent, _, _) = make_agent(spawn, inherited) else { panic!("agent fixture") };
    Proc::LingeringAgent(agent)
}

fn lending_accounts(
    launch: &Launch,
    spawn: &kernel::Spawn,
    io: skein_io::Limits,
) -> Option<smith_local_shell::credentials::Prepared> {
    use crate::world::Lending;
    use smith_local_shell::credentials::{Location, Prepared, Source};
    let lending = launch.lending?;
    let mut sources = skein_lib::List::with_capacity(1);
    let mut locations = skein_lib::List::with_capacity(1);
    let mut lendable = skein_lib::Map::with_capacity(1);
    let (handed, descriptor) = match lending {
        Lending::Key => {
            let file = launch.token_directory.parent().expect("scratch parent").join("key");
            let bytes = std::fs::read(&file).expect("scratch key at startup");
            lendable.insert(0, (0, smith_protocol_channel::GrantValue::Key(bytes.into()))).expect("one key value");
            locations
                .push(Location { account: 0, name: "codex".into(), source: Source::Key { file } })
                .expect("one key location");
            return Some(Prepared {
                sources,
                locations,
                lendable,
                limits: smith_local_shell::credentials::limits(
                    io,
                    1,
                    smith_agent_service::profile::standard().policy.refresh_lead,
                ),
            });
        }
        Lending::Env => {
            let variable = "SMITH_FIXTURE_TOKEN";
            let value = spawn
                .env
                .iter()
                .find_map(|entry| entry.strip_prefix(b"SMITH_FIXTURE_TOKEN="))
                .expect("the launch's named fixture variable");
            let borrowed = smith_local_shell::credentials::env_record(value, true, skein_tls_world::pki::VALID)
                .expect("synthetic variable value");
            (service::Source::Env { account: 0, borrowed }, Source::Env { variable: variable.into(), codex: true })
        }
    };
    assert!(sources.push(handed).is_ok(), "one selected source");
    locations.push(Location { account: 0, name: "codex".into(), source: descriptor }).expect("one selected location");
    Some(Prepared {
        sources,
        locations,
        lendable,
        limits: smith_local_shell::credentials::limits(
            io,
            1,
            smith_agent_service::profile::standard().policy.refresh_lead,
        ),
    })
}
