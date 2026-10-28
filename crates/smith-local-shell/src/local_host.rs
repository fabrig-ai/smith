//! Shared local shell pass (protocol/hosts.md, sections 5.3–5.6). It keeps
//! chat and token file owners, the terminal writer, and kernel routing. It
//! knows no simulator or checkout state. `Local::new` takes all directories
//! and terminal descriptors; `Host::iterate` performs the service and shell
//! passes. Every store acknowledgement follows file and directory sync.

use std::io::Write;

mod credentials;
mod header;
mod lend;
pub use header::View;
mod memory;
pub(crate) use memory::Admission;
mod words;

use skein_io::{self as io, kernel};
use skein_lib::stream::{OutputDown, OutputOutcome, OutputUp};
use skein_lib::{Env, Map, Queue, Time, Token, Wall};
use skein_shell::Host;
use smith_agent_service as agent;
use smith_local_domain as domain;
use smith_local_service as service;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    ResultFile(Token),
    Service(Token),
    Writer(Token),
    Launch,
    TraceRoot,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Terminal {
    Live,
    Closing(agent::Close),
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Announcement {
    BeforeStart,
    Running,
    Ended,
}

/// Paths and inherited descriptors owned by one invocation's shared shell.
pub struct Resources {
    pub state_root: kernel::Fd,
    pub input: kernel::Fd,
    pub output: kernel::Fd,
    pub signals: kernel::Fd,
    pub delivery_roots: Box<[kernel::Fd]>,
    pub effect_roots: Box<[kernel::Fd]>,
    pub delivery_environment: Box<[Box<[u8]>]>,
    pub seed: u64,
    /// Unpredictable bytes from the binary shell; worlds supply deterministic bytes.
    pub oauth_entropy: [u8; 32],
}

/// The local service and its shell effects, independent of the driving loop.
pub struct Local {
    service: service::Service,
    result_file: Option<Box<crate::compose::ResultFile>>,
    ephemeral_directory: Option<Box<[u8]>>,
    headless: Option<crate::front::headless::Headless>,
    interactive: Option<crate::front::interactive::Interactive>,
    credential_locations: skein_lib::List<crate::credentials::Location>,
    lent: lend::Lent,
    writer: Option<io::Io>,
    env: Env<io::Limits>,
    output: Option<Token>,
    launch_root: Option<kernel::Fd>,
    launch_closing: bool,
    routes: Map<Token, Route>,
    operation: u64,
    pending: Option<(Token, Box<[u8]>)>,
    right: u64,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    store_completions: Queue<kernel::Complete>,
    store_submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    exit: Option<domain::ExitStatus>,
    terminal: Terminal,
    error: Option<String>,
    errors: Box<dyn Write>,
    announcement: Announcement,
    started: Option<Time>,
    last_line: Option<Box<[u8]>>,
    worst: u64,
    operations: u32,
}

impl Local {
    /// Compose either agent kind with the same file and terminal shell pass.
    pub fn new(
        config: service::Config,
        lower: Option<agent::Config>,
        resources: Resources,
        errors: Box<dyn Write>,
    ) -> Result<Self, String> {
        Self::within_memory(config, lower, resources, errors, u64::MAX, None, None)
            .map_err(|failure| failure.detail().to_owned())
    }

    /// Refuse the complete shell footprint above the caller's memory before stream adoption.
    /// Contract: protocol/limits.md, section 4.
    pub fn within_memory(
        config: service::Config,
        lower: Option<agent::Config>,
        resources: Resources,
        errors: Box<dyn Write>,
        available_bytes: u64,
        handed: Option<crate::credentials::Prepared>,
        front: Option<crate::front::Front>,
    ) -> Result<Self, crate::startup::Failure> {
        Self::with_admission(
            config,
            lower,
            resources,
            errors,
            Admission { available_bytes, handed, front, producer: None, programs: None },
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "startup validates the shared store, terminal, credential and selected lower owners"
    )]
    pub(crate) fn with_admission(
        mut config: service::Config,
        lower: Option<agent::Config>,
        resources: Resources,
        errors: Box<dyn Write>,
        admission: Admission,
    ) -> Result<Self, crate::startup::Failure> {
        let Admission { available_bytes, handed, front, producer, programs } = admission;
        let (front, mut interactive) = match front {
            Some(crate::front::Front::Headless(front)) => (Some(front), None),
            Some(crate::front::Front::Interactive(front)) => (None, Some(front)),
            None => (None, None),
        };
        let headless = front.or_else(|| match &config.local.mode {
            domain::Mode::Headless { .. } => Some(crate::front::headless::Headless::new(
                false,
                skein_shell::OutputKind::Pipe,
                config.limits.local.show_bytes,
                config.limits.local.smith.run.outcome_bytes,
            )),
            domain::Mode::Chat { .. } => None,
        });
        let lower_routes = lower.as_ref().map_or(0, |configuration| configuration.limits.routes);
        if let Some(front) = &mut interactive {
            let model = config.local.models.first().expect("configured main");
            let endpoint = config.endpoints.name_for(model.endpoint.0).expect("configured endpoint");
            front.remote_config(endpoint, config.local.budget);
            if let Some(prepared) = &handed {
                for location in &prepared.locations {
                    let Some(account_model) =
                        config.local.models.iter().find(|candidate| candidate.account == Some(location.account))
                    else {
                        continue;
                    };
                    let endpoint = lower.as_ref().or(producer.as_ref()).and_then(|configuration| {
                        configuration.llm_endpoints.resolve(smith_domain::llm::Endpoint(account_model.endpoint.0))
                    });
                    let openai = endpoint.is_some_and(|endpoint| endpoint.provider == skein_llm::Provider::OpenAi);
                    front.account(
                        location.account,
                        crate::credentials::status_words(&location.source, openai),
                        Some(location.account) == model.account,
                    );
                }
            }
        }
        config.agent = match lower {
            Some(configuration) => {
                config.limits.local.agent = domain::AgentLimits::inline(
                    config.limits.local.smith,
                    config.limits.local.unsaved,
                    config.local.budget.time,
                    match config.limits.local.agent {
                        domain::AgentLimits::Inline(limits) => limits.cancel_grace,
                        domain::AgentLimits::Spawned(limits) => limits.cancel_grace,
                    },
                )
                .ok_or_else(|| configuration_failure(derivation_refusal("local_worst_case")))?;
                config.limits.inline = Some(
                    service::InlineLimits::new(&configuration)
                        .ok_or_else(|| configuration_failure(derivation_refusal("local_worst_case")))?,
                );
                service::AgentConfig::Inline { configuration, roots: resources.effect_roots }
            }
            None if resources.effect_roots.is_empty() => service::AgentConfig::Spawned,
            None => return Err(configuration_failure("spawned mode cannot adopt inline roots".into())),
        };
        config.local.live_text = interactive.as_ref().is_some_and(|front| !front.plain())
            && matches!(config.agent, service::AgentConfig::Spawned);
        let launch_root = config.launch.root;
        let queue = config.limits.queue;
        let outer_queue =
            queue.checked_mul(2).ok_or_else(|| configuration_failure(derivation_refusal("local_queue")))?;
        let io_limits = config.limits.process.io;
        let (worst, operations) = Self::startup_memory(
            &config.limits,
            lower_routes,
            handed.as_ref(),
            headless.as_ref(),
            interactive.as_ref(),
        )?;
        let worst = worst
            .checked_add(match &producer {
                Some(configuration) => Self::producer_memory(configuration)
                    .ok_or_else(|| configuration_failure(derivation_refusal("local_trace")))?,
                None => 0,
            })
            .ok_or_else(|| configuration_failure(derivation_refusal("local_trace")))?;
        smith_agent_service::profile::validate::memory(worst, available_bytes)
            .map_err(profile_refusal)
            .map_err(configuration_failure)?;
        if handed.as_ref().is_some_and(memory::has_programs) {
            config.limits.process.io.sockets = config
                .limits
                .process
                .io
                .sockets
                .checked_add(3)
                .ok_or_else(|| configuration_failure("program I/O count overflows".into()))?;
        }
        let mut service = service::Service::new(config, resources.seed)
            .map_err(|error| configuration_failure(format!("local service: {error:?}")))?;
        if let Some(configuration) = producer {
            service.prepare_trace(&configuration);
        }
        service.adopt_store(resources.state_root).map_err(|error| host_failure(format!("chat store: {error:?}")))?;
        if let Some(front) = &interactive {
            service.line_mode(front.plain());
            service.initial_chat();
        }
        let (credential_locations, lendable) = match handed {
            Some(prepared) => {
                if let Some(programs) = programs {
                    credentials::programs::hand(
                        &mut service,
                        programs,
                        &prepared.locations,
                        resources.delivery_environment.clone(),
                    )?;
                }
                service
                    .handed_accounts(prepared.sources, prepared.limits, resources.oauth_entropy)
                    .map_err(|error| configuration_failure(format!("account driver: {error:?}")))?;
                (prepared.locations, prepared.lendable)
            }
            None => (skein_lib::List::with_capacity(0), Map::with_capacity(0)),
        };
        let mut writer = if service.inline() { None } else { Some(io::Io::new(&io_limits)) };
        let output = if headless.as_ref().is_some_and(|front| front.json) {
            None
        } else {
            Some(
                match &mut writer {
                    Some(writer) => writer.adopt_write_pipe(resources.output, None),
                    None => service.adopt_writer(resources.output),
                }
                .map_err(|_| host_failure("terminal output cannot be adopted".into()))?,
            )
        };
        let requests = Queue::with_capacity(queue);
        let mut local = Self {
            service,
            result_file: None,
            ephemeral_directory: None,
            headless,
            interactive,
            lent: lend::Lent::new(lendable),
            credential_locations,
            writer,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: io_limits },
            output,
            launch_root: Some(launch_root),
            launch_closing: false,
            routes: Map::with_capacity(operations),
            operation: 1,
            pending: None,
            right: 1,
            events: Queue::with_capacity(queue),
            requests,
            store_completions: Queue::with_capacity(queue),
            store_submissions: Queue::with_capacity(queue),
            completions: Queue::with_capacity(outer_queue),
            submissions: Queue::with_capacity(outer_queue),
            exit: None,
            terminal: Terminal::Live,
            error: None,
            errors,
            announcement: Announcement::BeforeStart,
            started: None,
            last_line: None,
            worst,
            operations,
        };
        local
            .service
            .adopt_terminal(
                if local.headless.as_ref().is_some_and(|front| front.input_closed) {
                    None
                } else {
                    Some(resources.input)
                },
                resources.signals,
            )
            .map_err(|fd| host_failure(format!("local terminal {} cannot be adopted", fd.raw())))?;
        if local.interactive.as_ref().is_some_and(|front| !front.plain()) {
            local.service.read_window();
        }
        if let Some(front) = &local.headless
            && front.json
        {
            local
                .service
                .adopt_output(
                    resources.output,
                    front.output_kind == skein_shell::OutputKind::Append,
                    agent::sinks::Delivery::Complete,
                )
                .map_err(|_| host_failure("JSON output cannot be adopted".into()))?;
        }
        local
            .service
            .adopt_delivery_roots(resources.delivery_roots, resources.delivery_environment)
            .map_err(|error| host_failure(format!("delivery directories: {error:?}")))?;
        Ok(local)
    }

    /// Price and configure the spawned product's bounded trace producer.
    pub fn prepare_trace(
        &mut self,
        configuration: &agent::Config,
        available: u64,
    ) -> Result<(), crate::startup::Failure> {
        let bytes = Self::producer_memory(configuration)
            .ok_or_else(|| configuration_failure(derivation_refusal("local_trace")))?;
        let worst =
            self.worst.checked_add(bytes).ok_or_else(|| configuration_failure(derivation_refusal("local_trace")))?;
        agent::profile::validate::memory(worst, available).map_err(profile_refusal).map_err(configuration_failure)?;
        self.service.prepare_trace(configuration);
        self.worst = worst;
        Ok(())
    }

    /// Adopt an opened product trace before the first loop pass.
    pub fn adopt_trace(
        &mut self,
        fd: kernel::Fd,
        root: Option<kernel::Fd>,
        now: Time,
        header: smith_events::SessionStarted,
        delivery: agent::sinks::Delivery,
    ) -> Result<(), kernel::Fd> {
        self.service.adopt_trace(fd, now, header, delivery)?;
        if let Some(fd) = root {
            self.submit(kernel::Submit { op: Token::new(0), kind: kernel::Op::Close { fd } }, Route::TraceRoot);
        }
        Ok(())
    }

    /// The same checked operation bound used before opening the product's one kernel.
    #[must_use]
    pub fn kernel_operations(limits: &io::Limits, lower_routes: u32, handed: bool) -> Option<u32> {
        io::operations(limits)?.checked_mul(2)?.checked_add(64)?.checked_add(lower_routes)?.checked_add(if handed {
            8
        } else {
            0
        })
    }

    /// Emit the startup record once all configured destinations have been adopted.
    pub fn start_session(&mut self, now: Time, header: smith_events::SessionStarted) {
        self.service.start_session(now, header);
    }

    /// Content-free service facts available to the caller's independent referee.
    pub fn pop_fact(&mut self) -> Option<domain::Fact> {
        self.service.pop_fact()
    }

    /// Take one typed best-effort live-view record from the service's event sink.
    pub fn pop_front(&mut self) -> Option<agent::sinks::View> {
        self.service.pop_front()
    }

    /// Ask the domain to select a chat only when activation and store work are idle.
    pub fn select_chat(&mut self, name: Box<[u8]>, create: bool) {
        self.service.local_event(domain::Event::SelectChat { name, create });
    }

    /// Live-view records dropped by that sink, independent of destination loss.
    #[must_use]
    pub fn front_lost(&mut self) -> u64 {
        self.service.front_lost()
    }

    /// Attach the product result owner before its first pass.
    pub(crate) fn result_file(&mut self, file: Option<Box<crate::compose::ResultFile>>) {
        self.result_file = file;
    }

    /// The private store removes only its own names through this parent.
    pub(crate) fn ephemeral(&mut self, parent: kernel::Fd, path: &std::path::Path) -> Result<(), kernel::Fd> {
        let name = path.file_name().expect("private directory has a name").as_encoded_bytes().into();
        self.service.ephemeral(parent, name)?;
        self.ephemeral_directory = Some(path.as_os_str().as_encoded_bytes().into());
        Ok(())
    }

    /// The completed invocation's status after every lower close settles.
    #[must_use]
    pub fn result(&self) -> Option<Result<domain::ExitStatus, &str>> {
        if !self.is_empty() {
            return None;
        }
        self.exit.map(|status| match &self.error {
            Some(error) => Err(error.as_str()),
            None => Ok(status),
        })
    }

    fn files(&mut self) {
        if let Some(front) = &mut self.headless
            && let Some(turn) = self.service.take_told_turn()
        {
            front.told(&turn, self.errors.as_mut());
        }
        for _ in 0..self.service.shell_requests().capacity() {
            if !self.credential_locations.is_empty() && self.service.output().room() == 0 {
                break;
            }
            let Some(request) = self.service.shell_requests().pop() else { break };
            match request {
                domain::Request::Credential { account } => {
                    self.credential(account);
                }
                domain::Request::Exit { status } => {
                    self.exit = Some(status);
                }
                domain::Request::Answered { answer } => {
                    let Some(front) = &mut self.headless else { continue };
                    let elapsed = self.env.now.saturating_since(self.started.unwrap_or(self.env.now));
                    let ending = front.answered(&answer, self.service.chat_name(), elapsed);
                    if let Some(file) = &mut self.result_file {
                        file.begin(ending.text.clone());
                    }
                    if !front.json && !ending.text.is_empty() {
                        self.service.output().push(ending.text);
                    }
                    if let Some(line) = ending.line {
                        self.operator_line(line);
                    }
                }
                domain::Request::Notice { notice } => {
                    let text = if let Some(text) = self.credential_notice(&notice) {
                        self.service.output().push(text.clone());
                        text
                    } else if let Some(text) = self.run_notice(&notice) {
                        text
                    } else {
                        smith_local_protocol::notice_text(
                            &notice,
                            self.service.chat_name(),
                            self.service.models(),
                            self.service.show_bytes(),
                        )
                    };
                    match notice {
                        domain::Notice::Warning(_) => {
                            drop(write!(self.errors, "smith: {}", String::from_utf8_lossy(&text)));
                        }
                        domain::Notice::Failure(_) => self.operator_line(text),
                        domain::Notice::Waiting
                        | domain::Notice::Checking(_)
                        | domain::Notice::NotDelivered { .. }
                        | domain::Notice::ChatRefused { .. } => {
                            unreachable!("only operator notices reach the shell")
                        }
                    }
                }
                domain::Request::Load
                | domain::Request::SelectChat { .. }
                | domain::Request::SaveState { .. }
                | domain::Request::SaveTurn { .. }
                | domain::Request::SaveDelivery { .. }
                | domain::Request::Unanswered { .. }
                | domain::Request::Show { .. }
                | domain::Request::Git { .. }
                | domain::Request::PlainStatus { .. }
                | domain::Request::Lower { .. }
                | domain::Request::Process(_)
                | domain::Request::Input { .. } => unreachable!("service owns lower effects"),
            }
        }
    }

    fn operator_line(&mut self, text: Box<[u8]>) {
        if let Some(previous) = self.last_line.replace(text) {
            drop(write!(self.errors, "smith: {}", String::from_utf8_lossy(&previous)));
        }
    }

    fn credential(&mut self, account: u32) {
        match self.lent.answer(account) {
            lend::Answer::Lend(grant, value) => self.service.credential(grant, value),
            lend::Answer::Missing => self
                .service
                .local_event(domain::Event::NoCredential { account, reason: domain::CredentialFailure::Missing }),
        }
    }

    fn writer_up(&mut self) {
        while let Some(event) = self.service.writer_events().pop() {
            self.events.push(event);
        }
        if let Some(writer) = &mut self.writer {
            while writer.is_ready() {
                io::resume(writer, &self.env, &mut self.events, &mut self.store_submissions);
            }
            for _ in 0..self.store_completions.capacity() {
                let Some(complete) = self.store_completions.pop() else { break };
                io::up(writer, &self.env, complete, &mut self.events, &mut self.store_submissions);
            }
            for _ in 0..self.env.limits.sockets {
                if writer.is_ready() {
                    io::resume(writer, &self.env, &mut self.events, &mut self.store_submissions);
                }
                if writer.is_due(self.env.now) {
                    io::fire(writer, &self.env, &mut self.events, &mut self.store_submissions);
                }
            }
        }
        while let Some(event) = self.events.pop() {
            match event {
                io::Event::Resized => {
                    if self.interactive.as_ref().is_some_and(|front| !front.plain()) {
                        self.service.read_window();
                    }
                }
                io::Event::Window { window, .. } => match window {
                    io::Window::Size(size) => {
                        if let Some(front) = &mut self.interactive {
                            front.columns(size.columns);
                        }
                    }
                    io::Window::Gone => {}
                },
                io::Event::Output { owner, up: OutputUp::Settled { right, outcome } } if Some(owner) == self.output => {
                    let (wanted, text) = self.pending.take().expect("terminal has one output right");
                    assert_eq!(right, wanted, "terminal settled its sole output right");
                    match outcome {
                        OutputOutcome::Granted => self.requests.push(io::Request::Output {
                            stream: self.output.expect("terminal owns its output"),
                            down: OutputDown::Send { right, bytes: text },
                        }),
                        OutputOutcome::Cancelled | OutputOutcome::Failed(_) => self.write_failed(),
                    }
                }
                io::Event::Failed { .. } | io::Event::Stream { up: skein_lib::stream::Up::Failed(_), .. } => {
                    self.write_failed();
                }
                io::Event::Closed { owner } => {
                    if Some(owner) == self.output {
                        self.terminal = Terminal::Closed;
                    }
                }
                other @ (io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Connecting { .. }
                | io::Event::Connected { .. }
                | io::Event::Stream { .. }
                | io::Event::Output { .. }
                | io::Event::Spawned { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Shutdown { .. }) => panic!("unexpected terminal writer event: {other:?}"),
            }
        }
    }

    fn product_files(&mut self, now: Time, wall: Wall) {
        if let Some(error) = self.service.cleanup_warning() {
            let directory = self.ephemeral_directory.as_deref().expect("private store has its path");
            drop(writeln!(
                self.errors,
                "smith: warning: ephemeral directory {}: {error:?}",
                String::from_utf8_lossy(directory)
            ));
        }
        let Some(file) = &mut self.result_file else { return };
        if self.service.closing() && file.waiting() {
            file.begin(Box::new([]));
        }
        if self.service.aborted() {
            file.abort();
        }
        file.iterate(now, wall);
        if let Some(failure) = file.failure() {
            match failure {
                crate::compose::ResultFileFailure::Write(error) => {
                    if self.service.result_file_failed() {
                        let text = format!(
                            "run accepted, but its result could not be written to {}: {error:?}\n",
                            file.path().display()
                        );
                        self.operator_line(text.into_bytes().into());
                    } else {
                        drop(writeln!(
                            self.errors,
                            "smith: warning: its result could not be written to {}: {error:?}",
                            file.path().display()
                        ));
                    }
                }
                crate::compose::ResultFileFailure::Close(error) => {
                    drop(writeln!(
                        self.errors,
                        "smith: warning: result directory for {} could not close: {error:?}",
                        file.path().display()
                    ));
                }
            }
        }
    }

    fn write_failed(&mut self) {
        self.error = Some("terminal output failed".into());
        self.service.local_event(domain::Event::StoreFailed { turn: None, reason: domain::StoreFailure::Write });
    }

    fn writer_down(&mut self) {
        if self.pending.is_none()
            && self.terminal == Terminal::Live
            && let Some(text) = self.display_text()
        {
            let right = Token::new(self.right);
            self.right = self.right.checked_add(1).expect("terminal output names remain representable");
            let bytes = u32::try_from(text.len()).expect("bounded show text");
            if bytes > 0 {
                self.pending = Some((right, text));
                self.requests.push(io::Request::Output {
                    stream: self.output.expect("terminal owns its output"),
                    down: OutputDown::Room { right, bytes },
                });
            }
        }
        if self.service.closing()
            && self.terminal == Terminal::Live
            && self.pending.is_none()
            && self.service.output().is_empty()
            && !self.live_pending()
        {
            self.terminal = Terminal::Closing(agent::Close::Graceful);
            if let Some(output) = self.output {
                self.requests.push(io::Request::Close { entity: output });
            } else {
                self.terminal = Terminal::Closed;
            }
            let fd = self.launch_root.take().expect("one launch root");
            self.launch_closing = true;
            self.submit(kernel::Submit { op: Token::new(0), kind: kernel::Op::Close { fd } }, Route::Launch);
        }
        match self.terminal {
            Terminal::Closing(agent::Close::Graceful) if self.service.aborted() => {
                self.terminal = Terminal::Closing(agent::Close::Aborted);
                self.requests.push(io::Request::Abort { entity: self.output.expect("terminal owns its output") });
            }
            Terminal::Live | Terminal::Closing(_) | Terminal::Closed => {}
        }
        if let Some(writer) = &mut self.writer {
            while writer.takes() {
                let Some(request) = self.requests.pop() else { break };
                io::down(writer, &self.env, request, &mut self.store_submissions);
            }
            writer.reclaim();
        } else {
            while let Some(request) = self.requests.pop() {
                self.service.writer_request(request);
            }
        }
    }

    fn display_text(&mut self) -> Option<Box<[u8]>> {
        if let Some(text) = self.service.output().pop() {
            return Some(text);
        }
        let front = self.interactive.as_mut()?;
        front.remote_activation(self.service.remote_activation());
        if let Some(piece) = self.service.pop_live() {
            return front.piece(piece);
        }
        if let Some(turn) = self.service.take_remote_turn() {
            return Some(front.remote_turn(&turn));
        }
        front.record(self.service.pop_front()?)
    }

    fn live_pending(&self) -> bool {
        self.interactive.is_some() && self.service.front_pending()
    }
    fn submit(&mut self, submit: kernel::Submit, route: Route) {
        let kind = match submit.kind {
            kernel::Op::Cancel { target } => {
                let wanted = match route {
                    Route::Service(_) => Route::Service(target),
                    Route::Writer(_) => Route::Writer(target),
                    Route::ResultFile(_) => Route::ResultFile(target),
                    Route::Launch | Route::TraceRoot => unreachable!("startup directory only closes"),
                };
                let target = self
                    .routes
                    .iter()
                    .find_map(|(global, active)| (*active == wanted).then_some(*global))
                    .unwrap_or(Token::new(u64::MAX));
                kernel::Op::Cancel { target }
            }
            other @ (kernel::Op::Socket { .. }
            | kernel::Op::Bind { .. }
            | kernel::Op::Listen { .. }
            | kernel::Op::Accept { .. }
            | kernel::Op::Connect { .. }
            | kernel::Op::Recv { .. }
            | kernel::Op::Send { .. }
            | kernel::Op::Shutdown { .. }
            | kernel::Op::Close { .. }
            | kernel::Op::Open { .. }
            | kernel::Op::Lock { .. }
            | kernel::Op::Append { .. }
            | kernel::Op::Read { .. }
            | kernel::Op::Write { .. }
            | kernel::Op::Truncate { .. }
            | kernel::Op::Sync { .. }
            | kernel::Op::Stat { .. }
            | kernel::Op::Rename { .. }
            | kernel::Op::Remove { .. }
            | kernel::Op::MakeDirectory { .. }
            | kernel::Op::List { .. }
            | kernel::Op::Spawn { .. }
            | kernel::Op::Wait { .. }
            | kernel::Op::Signal { .. }
            | kernel::Op::ReadSignal { .. }
            | kernel::Op::PipeRead { .. }
            | kernel::Op::PipeWrite { .. }
            | kernel::Op::Window { .. }
            | kernel::Op::Usage) => other,
        };
        let op = Token::new(self.operation);
        self.operation = self.operation.checked_add(1).expect("shell operation names remain representable");
        self.routes.insert(op, route).expect("configured operation routes");
        self.submissions.push(kernel::Submit { op, kind });
    }
}

impl Host for Local {
    fn drain(&mut self) {
        if self.announcement == Announcement::BeforeStart {
            self.announcement = Announcement::Running;
            if self.headless.is_some() || self.interactive.is_some() {
                drop(writeln!(self.errors, "smith: chat {}", String::from_utf8_lossy(self.service.chat_name())));
            } else {
                drop(writeln!(self.errors, "smith: local host started; worst case {} bytes", self.worst));
            }
        }
        if self.is_empty() && self.announcement != Announcement::Ended {
            self.announcement = Announcement::Ended;
            if let Some(text) = self.last_line.take() {
                drop(write!(self.errors, "smith: {}", String::from_utf8_lossy(&text)));
            }
            if let Some(Err(why)) = self.result() {
                let why = why.to_owned();
                drop(writeln!(self.errors, "smith: {why}"));
            }
            if self.exit == Some(domain::ExitStatus::Interrupted)
                && self.service.end_cause() == Some(domain::EndCause::Cancelled)
            {
                drop(writeln!(self.errors, "smith: interrupted: the run was dropped before it answered"));
            }
        }
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        if self.started.is_none() {
            self.started = Some(now);
        }
        while let Some(complete) = self.completions.pop() {
            match self.routes.remove(&complete.op).expect("each operation has one lower owner") {
                Route::Service(op) => self.service.completions().push(kernel::Complete {
                    op,
                    kind: complete.kind,
                    result: complete.result,
                }),
                Route::Writer(op) => {
                    self.store_completions.push(kernel::Complete { op, kind: complete.kind, result: complete.result });
                }
                Route::ResultFile(op) => self
                    .result_file
                    .as_mut()
                    .expect("product file owns its operations")
                    .completions()
                    .push(kernel::Complete { op, kind: complete.kind, result: complete.result }),
                Route::TraceRoot => {
                    if complete.result.is_err() {
                        self.error = Some("trace directory close failed".into());
                    }
                }
                Route::Launch => {
                    self.launch_closing = false;
                    if complete.result.is_err() {
                        self.error = Some("launch directory close failed".into());
                    }
                }
            }
        }
        self.writer_up();
        service::iterate(&mut self.service, now, wall);
        // Each destination given up is named once, with why (protocol/events.md,
        // section 5.2); the agent shell prints the same line.
        while let Some(given_up) = self.service.pop_given_up() {
            drop(writeln!(self.errors, "smith: {}", smith_agent_shell::given_up_line(given_up)));
        }
        self.files();
        self.product_files(now, wall);
        self.writer_down();
        if self.service.closing()
            && self.terminal == Terminal::Closed
            && self.writer.as_ref().is_none_or(io::Io::is_empty)
            && self.result_file.as_ref().is_none_or(|file| file.is_empty())
            && self.launch_root.is_none()
            && !self.launch_closing
            && self.pending.is_none()
            && !self.live_pending()
            && self.store_submissions.is_empty()
            && self.requests.is_empty()
        {
            self.service.shell_closed();
        }
        while let Some(submit) = self.service.submissions().pop() {
            let op = submit.op;
            self.submit(submit, Route::Service(op));
        }
        if let Some(mut file) = self.result_file.take() {
            while let Some(submit) = file.submissions().pop() {
                let op = submit.op;
                self.submit(submit, Route::ResultFile(op));
            }
            self.result_file = Some(file);
        }
        while let Some(submit) = self.store_submissions.pop() {
            let op = submit.op;
            self.submit(submit, Route::Writer(op));
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.service.work_pending(now)
            || self.live_pending()
            || self.result_file.as_ref().is_some_and(|file| file.work_pending(now))
            || self.writer.as_ref().is_some_and(io::Io::is_ready)
            || self.writer.as_ref().is_some_and(|writer| writer.is_due(now))
            || !self.completions.is_empty()
            || !self.store_completions.is_empty()
            || !self.store_submissions.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.service.next_deadline(), self.writer.as_ref().and_then(io::Io::next_deadline)]
            .into_iter()
            .flatten()
            .chain(self.result_file.as_ref().and_then(|file| file.next_deadline()))
            .min()
    }

    /// The service's and the accounts' deadlines; the terminal writer's io
    /// deadline is no policy deadline.
    fn next_policy_deadline(&self) -> Option<Time> {
        self.service.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.exit.is_some()
            && self.terminal == Terminal::Closed
            && self.writer.as_ref().is_none_or(io::Io::is_empty)
            && self.pending.is_none()
            && self.result_file.as_ref().is_none_or(|file| file.is_empty())
            && self.routes.is_empty()
            && !self.launch_closing
            && self.launch_root.is_none()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.store_completions.is_empty()
            && self.store_submissions.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.result().map(|result| {
            kernel::Exit::Code(match result {
                Ok(status) => crate::front::headless::ExitCode::from_status(status).code(),
                Err(_) => 1,
            })
        })
    }

    fn worst_case(&self) -> u64 {
        self.worst
    }
    fn operations(&self) -> u32 {
        self.operations
    }
}

/// Bounded saved-token documents shared by both placements and the shell store.
#[must_use]
pub fn token_limits() -> skein_oauth::Limits {
    skein_oauth::Limits {
        issued_client_bytes: 0,
        granted_bytes: 1024,
        subject_bytes: 0,
        id_token_bytes: 8192,
        document_bytes: 16_384,
        string_bytes: 8192,
        token_bytes: 8192,
        client_bytes: 1024,
        detail_bytes: 1024,
        record_bytes: 32_768,
        depth: 16,
        tokens: 1024,
    }
}

fn profile_refusal(error: smith_agent_service::profile::Refusal) -> String {
    String::from_utf8(smith_agent_service::profile::words(&error).into_vec()).expect("refusal words are UTF-8")
}

fn derivation_refusal(field: &'static str) -> String {
    profile_refusal(smith_agent_service::profile::Refusal::Derived {
        limit: smith_agent_service::profile::LimitName::deployment(smith_agent_service::profile::Layer::Service, field),
    })
}

fn configuration_failure(detail: String) -> crate::startup::Failure {
    crate::startup::Failure::Configuration { detail }
}

fn host_failure(detail: String) -> crate::startup::Failure {
    crate::startup::Failure::Host { detail }
}
