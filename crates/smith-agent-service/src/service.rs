//! Bounded startup and staged routing for one agent process.
//! The retained state is one root domain, three protocol components, Skein IO,
//! one file driver, and queues for their requests and terminals. It never
//! chooses a domain outcome or interprets a bearer token.
//! Contract: protocol/agent.md, sections 4–6; protocol/README.md, section 6.

#![expect(clippy::manual_let_else, reason = "production steps use explicit bounded matches")]

use alloc::boxed::Box;

mod down;
mod effects;
mod memory;
mod request;
mod routing;
mod up;

#[cfg(test)]
#[expect(clippy::wildcard_enum_match_arm, reason = "test assertions describe unexpected events")]
pub(crate) mod tests;

pub use effects::Effects;

use down::{channel_takes, component_down, domain_down};
use routing::{component_owner, io_down, io_event_owner};
use up::{component_up, domain_up, io_up, lower_events};

#[path = "program.rs"]
mod program;

pub(crate) use program::bound;
pub use program::{AdoptError, ProgramBackend, program_operations, program_worst_case};
#[path = "teardown.rs"]
pub mod teardown;

pub use memory::effects_parts;
use memory::parts_worst_case;

use teardown::Teardown;

use skein_channel::{Lower as ChannelLower, StreamMode};
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::{Duration, Env, List, Map, Queue, ReplyTo, Slab, Time, Token, Wall};
use smith_domain::{self as domain, run};
use smith_protocol_channel as channel;
use smith_protocol_llm as llm;
use smith_protocol_machine as machine;

#[derive(Debug)]
struct PendingMessage {
    finish_requested: bool,
    name: Token,
    label: Box<[u8]>,
    text: Box<[u8]>,
}

const COMPONENT_OWNER_BIT: u64 = 1 << 63;

/// Fixed limits for one agent process and every stage it owns.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub domain: domain::Limits,
    pub channel: channel::Limits,
    pub llm: llm::ComponentLimits,
    pub machine: machine::Limits,
    pub io: io::Limits,
    pub file_slots: u32,
    pub file_read: u32,
    pub file_entries: u32,
    pub file_bytes: u32,
    pub file_timeout: Duration,
    /// Longest hold before projecting live bytes (protocol/limits.md 2.3).
    pub live_interval: Duration,
    /// Backlog watermark and maximum entries drained per iteration at each stage.
    pub queue: u32,
    /// Concurrent IO owner and kernel submission routes.
    pub routes: u32,
    /// Startup refuses a complete bound above this many heap bytes.
    pub memory: u64,
    /// Event ownership, absent for a host that has no process stream.
    pub events: Option<crate::EventLimits>,
}

/// All startup inputs except the inherited descriptors, which the shell
/// adopts after constructing the service (protocol/agent.md, section 4).
#[derive(Debug)]
pub struct Config {
    pub limits: Limits,
    pub domain: domain::Config,
    pub channel_endpoints: channel::Endpoints,
    pub llm_endpoints: llm::Endpoints,
    pub environment: machine::Environment,
    pub stream_mode: StreamMode,
    pub program: Option<ProgramBackend>,
}

/// Why startup cannot build a bounded agent service.
#[derive(Debug)]
pub enum ConfigError {
    /// IO cannot run under the requested capacities.
    Io,
    /// A stage cannot reserve its maximum output.
    Queue,
    /// One checked retained-memory calculation overflowed or is unusable.
    Memory,
    /// The complete bound exceeds the configured memory ceiling.
    MemoryCeiling,
    /// The host channel cannot open under these limits or endpoints.
    Channel(channel::Error),
    /// The LLM pool cannot retain these endpoints or limits.
    Llm(llm::ComponentError),
}

/// The first failure that prevented an answer from crossing the channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// The channel ended before the answer.
    ChannelEnded,
    /// The channel's output side failed.
    WriteFailed,
    /// A host grant could not be installed below the domain.
    Credential,
    /// A required host record could not be encoded or queued.
    Send,
}

/// A lower owner belongs to exactly one protocol component.
#[derive(Clone, Copy, Debug)]
enum Owner {
    Llm(Token),
    Machine(Token),
    Program(Token),
}

/// One submission's original token and the layer that issued it.
#[derive(Clone, Copy, Debug)]
enum Operation {
    Io(Token),
    Signals(Token),
    File(Token),
}

/// The running agent and every bounded queue between its layers.
#[derive(Debug)]
pub struct Service {
    limits: Limits,
    events: Option<crate::events::Producer>,
    teardown: Teardown,
    product_exit: Option<u8>,
    usage_unread: bool,
    domain: Option<domain::Domain>,
    channel: Option<channel::Component>,
    live: crate::live::Live,
    llm: llm::Component,
    machine: machine::Component,
    program: Option<Box<program::Program>>,
    io: Option<io::Io>,
    files: file_layer::FileIo,
    domain_env: Env<domain::Limits>,
    llm_env: Env<llm::ComponentLimits>,
    machine_env: Env<machine::Limits>,
    io_env: Env<io::Limits>,
    channel_events: Queue<channel::OpenEvent>,
    pending_messages: Queue<PendingMessage>,
    channel_below: Queue<ChannelLower>,
    llm_events: Queue<llm::ToDomain>,
    llm_below: Queue<io::Request>,
    machine_events: Queue<machine::ToDomain>,
    machine_below: Queue<machine::Below>,
    domain_events: Queue<domain::Event>,
    domain_requests: Queue<domain::Request>,
    io_events: Queue<io::Event>,
    io_requests: Queue<io::Request>,
    file_events: Queue<file::Event>,
    io_submissions: Queue<kernel::Submit>,
    file_submissions: Queue<kernel::Submit>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    owners: Slab<Owner>,
    operations: Map<Token, Operation>,
    next_operation: u64,
    next_send: u64,
    input: Option<Token>,
    output: Option<Token>,
    signals: Option<Token>,
    admitted: Option<Token>,
    run_started: Time,
    cancel_pending: bool,
    failure: Option<Failure>,
    pub(crate) final_answer_at: Time,
    pub(crate) final_failure: Option<run::Failure>,
    pub(crate) final_refusal: Option<run::Refusal>,
    pub(crate) final_detail: Box<[u8]>,
    outcome: Option<run::outcome::OutcomeSpec>,
    delivery: Option<run::outcome::ChangeSpec>,
    pending_start: Option<Box<channel::DecodedStart>>,
    directories: List<run::Directory>,
    root_index: u32,
    pipe_routes: Map<Token, Token>,
    roots: List<Token>,
    root_close_next: u32,
    root_close_pending: bool,
}

const USAGE_OWNER: Token = Token::new(u64::MAX - 1);

/// Construction peak and retained bound for the host-owned effects adapter
/// with its configured endpoint request heads.
#[must_use]
pub fn effects_worst_case(limits: &Limits, endpoints: &llm::Endpoints) -> Option<u64> {
    let mut total = 0_u64;
    for (_, part) in effects_parts(limits, endpoints)? {
        total = total.checked_add(part)?;
    }
    Some(total)
}

/// Checked maximum of every owned layer and inter-layer queue. The file
/// driver's transient buffers are counted by its own bound; configured
/// endpoints supply the request-head and credential bounds.
#[must_use]
pub fn worst_case(limits: &Limits, endpoints: &llm::Endpoints) -> Option<u64> {
    parts_worst_case(limits, endpoints, true)?
        .checked_add(domain::worst_case_drained_content(&limits.domain, limits.llm.receiving.max_completion_bytes)?)?
        .checked_add(channel::worst_case(&limits.channel)?)?
        .checked_add(crate::live::worst_case()?)?
        .checked_add(io_worst_case(limits)?)
}

// One physical LLM socket is already included by its connection owner. The
// service owns IO's tables once. Its other producers are read-only process
// pipes/input/signals and the single host-channel writer; only that writer
// can retain output payload outside the LLM sockets (limits.md, section 3).
/// The single native IO ownership, excluding the connection owners' socket buffers.
#[must_use]
pub fn io_worst_case(limits: &Limits) -> Option<u64> {
    let physical = limits.llm.connection.connections;
    let other = limits.io.sockets.checked_sub(physical)?;
    let child = limits.llm.connection.io;
    let writers = match limits.events {
        Some(events) => events.sinks.destinations.checked_add(1)?,
        None => 1,
    };
    if other < writers
        || child.intake != limits.io.intake
        || child.receive != limits.io.receive
        || child.output != limits.io.output
        || child.sends != limits.io.sends
    {
        return None;
    }
    let socket = skein_lib::Intake::worst_case(limits.io.intake)?
        .checked_add(u64::from(limits.io.receive))?
        .checked_add(u64::from(limits.io.output))?
        .checked_add(Queue::<Box<[u8]>>::worst_case(limits.io.sends)?)?;
    io::worst_case(&limits.io)?
        .checked_sub(u64::from(physical).checked_mul(socket)?)?
        .checked_sub(u64::from(other.checked_sub(writers)?).checked_mul(u64::from(limits.io.output))?)
}

impl Service {
    /// Prepare every child and queue before a channel byte is read.
    pub fn new(config: Config, seed: u64) -> Result<Service, ConfigError> {
        let limits = config.limits;
        if !limits.io.is_usable() || limits.io.sockets < 3 || limits.file_slots < limits.machine.roots {
            return Err(ConfigError::Io);
        }
        let lower_routes = match io::operations(&limits.io) {
            Some(operations) => operations.checked_add(2).ok_or(ConfigError::Queue)?,
            None => return Err(ConfigError::Queue),
        };
        if limits.routes < lower_routes {
            return Err(ConfigError::Queue);
        }
        let memory = worst_case(&limits, &config.llm_endpoints)
            .ok_or(ConfigError::Memory)?
            .checked_add(program_worst_case(&config).ok_or(ConfigError::Memory)?)
            .ok_or(ConfigError::Memory)?;
        if memory > limits.memory {
            return Err(ConfigError::MemoryCeiling);
        }
        Self::parts(config, Some(seed))
    }

    fn parts(config: Config, seed: Option<u64>) -> Result<Self, ConfigError> {
        let events = crate::events::Producer::configured(&config);
        let Config { mut limits, domain, channel_endpoints, llm_endpoints, environment, stream_mode, program } = config;
        let program::Parts { program, files } = program::parts(program, &mut limits, &environment)?;
        let (domain, channel, io, frames) = match seed {
            Some(seed) => (
                Some(domain::Domain::new(&limits.domain, domain, seed)),
                Some(match channel::Component::new(&limits.channel, stream_mode, channel_endpoints) {
                    Ok(channel) => channel,
                    Err(why) => return Err(ConfigError::Channel(why)),
                }),
                Some(io::Io::new(&limits.io)),
                limits.queue,
            ),
            None => (None, None, None, 0),
        };
        let llm = match llm::Component::new(&limits.llm, llm_endpoints) {
            Ok(llm) => llm,
            Err(error) => return Err(ConfigError::Llm(error)),
        };
        let machine = machine::Component::new(&limits.machine, environment);
        Ok(Service {
            limits,
            events,
            teardown: Teardown::Live,
            product_exit: None,
            usage_unread: false,
            domain,
            channel,
            live: crate::live::Live::new(None, limits.live_interval),
            llm,
            machine,
            program,
            io,
            files,
            domain_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.domain },
            llm_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.llm },
            machine_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.machine },
            io_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.io },
            channel_events: Queue::with_capacity(frames),
            pending_messages: Queue::with_capacity(frames),
            channel_below: Queue::with_capacity(frames),
            llm_events: Queue::uncapped(),
            llm_below: Queue::uncapped(),
            machine_events: Queue::uncapped(),
            machine_below: Queue::uncapped(),
            domain_events: Queue::uncapped(),
            domain_requests: Queue::uncapped(),
            io_events: Queue::uncapped(),
            io_requests: Queue::uncapped(),
            file_events: Queue::uncapped(),
            io_submissions: Queue::uncapped(),
            file_submissions: Queue::uncapped(),
            submissions: Queue::uncapped(),
            completions: Queue::uncapped(),
            owners: Slab::with_capacity(limits.routes),
            operations: Map::with_capacity(limits.routes),
            next_operation: 1,
            next_send: 1,
            input: None,
            output: None,
            signals: None,
            admitted: None,
            run_started: Time::ZERO,
            cancel_pending: false,
            failure: None,
            final_answer_at: Time::ZERO,
            final_failure: None,
            final_refusal: None,
            final_detail: Box::default(),
            outcome: None,
            delivery: None,
            pending_start: None,
            directories: List::with_capacity(limits.machine.roots),
            root_index: 0,
            pipe_routes: Map::with_capacity(limits.machine.processes.checked_mul(2).ok_or(ConfigError::Memory)?),
            roots: List::with_capacity(limits.machine.roots),
            root_close_next: 0,
            root_close_pending: false,
        })
    }

    /// Adopt the shell's opened append descriptor under its declared write deadline.
    pub fn adopt_destination(
        &mut self,
        fd: kernel::Fd,
        destination: crate::sinks::Destination,
        delivery: crate::sinks::Delivery,
        append: bool,
    ) -> Result<(), kernel::Fd> {
        let events = match &mut self.events {
            Some(events) => events,
            None => return Err(fd),
        };
        if !events.sinks.room_for_destination(destination) {
            return Err(fd);
        }
        let deadline = self.limits.events.expect("producer has configured ownership").write_deadline;
        let io = self.io.as_mut().expect("standalone IO");
        let token = if append { io.adopt_append(fd, deadline)? } else { io.adopt_write_pipe(fd, Some(deadline))? };
        events.sinks.add(destination, token, delivery).expect("destination room checked before descriptor adoption");
        Ok(())
    }

    /// Record the shell's actual startup before any service pass.
    pub fn start_session(&mut self, now: Time, header: smith_events::SessionStarted) {
        if let Some(events) = &mut self.events {
            events.started(now, header);
        }
    }

    /// One destination IO gave up, and why, for the shell's operator line
    /// (protocol/events.md, section 5.2).
    pub fn pop_given_up(&mut self) -> Option<crate::sinks::GivenUp> {
        match &mut self.events {
            Some(events) => events.sinks.pop_given_up(),
            None => None,
        }
    }

    /// A typed run accounting failure for the shell operator, drained once.
    pub fn pop_accounting_overflow(&mut self) -> Option<run::Overflow> {
        match &mut self.events {
            Some(events) => events.pop_overflow_notice(),
            None => None,
        }
    }

    /// An unread process usage terminal, reported once without changing the answer.
    pub fn pop_usage_unread(&mut self) -> bool {
        core::mem::replace(&mut self.usage_unread, false)
    }

    /// Kernel completions arriving since the last iteration.
    pub const fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }

    /// Submissions to hand to the kernel after an iteration.
    pub const fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    /// Number of projected host facts dropped at the channel's output reserve.
    #[must_use]
    pub fn lost_channel_facts(&self) -> u64 {
        self.channel.as_ref().expect("framed agent owns its channel").lost_facts()
    }

    /// Live bytes omitted from the host channel.
    #[must_use]
    pub fn lost_live_bytes(&self) -> u64 {
        self.channel.as_ref().expect("framed agent owns its channel").lost_live_bytes()
    }

    /// Give Skein IO the inherited channel and termination descriptors. The
    /// shell opens these before iterating; io closes them after the run.
    pub fn adopt_streams(
        &mut self,
        input: kernel::Fd,
        output: kernel::Fd,
        signals: kernel::Fd,
    ) -> Result<(), kernel::Fd> {
        let input = self.io.as_mut().expect("standalone IO").adopt_read_pipe(input)?;
        self.input = Some(input);
        let output = self.io.as_mut().expect("standalone IO").adopt_write_pipe(output, None)?;
        self.output = Some(output);
        let signals = self.io.as_mut().expect("standalone IO").adopt_signals(signals)?;
        self.signals = Some(signals);
        Ok(())
    }

    /// The next host-supplied mount path that the shell must open as a root.
    #[must_use]
    pub fn root_to_open(&self) -> Option<&[u8]> {
        let start = self.pending_start.as_ref()?;
        let mounts = start.mounts.as_ref()?;
        let index = usize::try_from(self.root_index).ok()?;
        match mounts.get(index) {
            Some(mount) => Some(&mount.path),
            None => None,
        }
    }

    /// Receiving ceiling for absolute host metadata at root attachment.
    #[must_use]
    pub fn host_path_bytes(&self) -> u32 {
        self.limits.domain.run.host_path_bytes
    }

    /// Attach one opened mount in Start order, or refuse the Start if the
    /// shell could not open it. Absolute host metadata is retained for aliases.
    pub fn root_opened(&mut self, root: Result<kernel::Fd, ()>, host: Box<[u8]>) {
        match root {
            Ok(fd) => match self.files.adopt_root(fd) {
                Some(token) => {
                    self.roots.push(token).expect("root count was checked before opening");
                    self.root_close_next = self.roots.len();
                    let start = self.pending_start.as_ref().expect("a root belongs to a pending Start");
                    let mounts = start.mounts.as_ref().expect("a requested root belongs to a workspace");
                    let index = usize::try_from(self.root_index).expect("root index fits usize");
                    let mount = mounts.get(index).expect("one mount requested at a time");
                    self.directories
                        .push(run::Directory {
                            host,
                            name: mount.name.clone(),
                            root: token,
                            writable: mount.writable,
                            git: mount.git,
                            conflicts: mount.conflicts.clone(),
                        })
                        .expect("root count was checked before opening");
                    self.root_index = self.root_index.checked_add(1).expect("bounded mount count");
                    if self.root_to_open().is_none() {
                        self.start_ready();
                    }
                }
                None => self.refuse_start(run::Invalid::Workspace),
            },
            Err(()) => self.refuse_start(run::Invalid::Workspace),
        }
    }

    fn next_send(&mut self) -> Token {
        let token = Token::new(self.next_send);
        self.next_send = self.next_send.checked_add(1).expect("one run does not exhaust send names");
        token
    }

    fn mark_failed(&mut self, reason: Failure) {
        if self.failure.is_none() {
            self.failure = Some(reason);
        }
        teardown::failed(self, self.failure.expect("kept first failure"));
    }

    fn refuse_start(&mut self, reason: run::Invalid) {
        self.pending_start = None;
        let token = self.next_send();
        let answer = run::Answer::Refused(run::Refusal::Invalid(reason));
        crate::answers::remember(self, &answer, Box::default(), self.domain_env.now);
        let read = None;
        if let Some(events) = &mut self.events {
            events.answer(self.domain_env.now, self.run_started, &answer, read);
        }
        if self
            .channel
            .as_mut()
            .expect("framed agent owns its channel")
            .send_answer(self.domain_env.now, answer, read, token, &mut self.channel_events, &mut self.channel_below)
            .is_err()
        {
            self.mark_failed(Failure::Send);
        }
        teardown::answered(self);
    }

    fn start_ready(&mut self) {
        let start = self.pending_start.take().expect("all roots opened for pending Start");
        let workspace = match start.mounts {
            Some(_) => Some(run::Workspace {
                directories: core::mem::replace(&mut self.directories, List::with_capacity(self.limits.machine.roots))
                    .into_boxed(),
            }),
            None => None,
        };
        if let Some(ref workspace) = workspace {
            self.machine.workspace(workspace);
        }
        self.outcome = Some(start.charter.outcome.clone());
        self.delivery.clone_from(&start.charter.outcome.change);
        if self.llm.bind_grants(&start.charter).is_err() {
            self.refuse_start(run::Invalid::Grants);
            return;
        }
        let mut grants = List::with_capacity(u32::try_from(start.grants.len()).expect("bounded grant count"));
        for grant in &start.grants {
            let value = match self.channel.as_ref().expect("framed agent owns its channel").grant_value(grant.name) {
                Some(value) => value.clone(),
                None => {
                    self.refuse_start(run::Invalid::Grants);
                    return;
                }
            };
            let lapses = self.domain_env.now.saturating_add(grant.valid);
            if program::install(self, &start, grant.name, &value, lapses, workspace.as_ref()).is_err() {
                self.refuse_start(run::Invalid::Grants);
                return;
            }
            grants
                .push(domain::Grant { name: grant.name, valid: grant.valid, renewable: grant.renewable })
                .expect("grant count was checked at Start");
        }
        self.run_started = self.domain_env.now;
        self.domain_events.push(domain::Event::Start {
            messages: start.messages,
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            activation: start.activation,
            identity: start.identity,
            window: start.window,
            charter: start.charter,
            workspace,
            transcript: start.transcript,
            answered: start.answered,
            grants: grants.into_boxed(),
        });
    }
}

/// Install only credential forms; accounts-sources supplies the program backend.
fn credential(value: &channel::GrantValue) -> Option<skein_llm::Credential> {
    let credential = match value {
        channel::GrantValue::Bearer(value) => skein_llm::Credential::bearer(value.clone()),
        channel::GrantValue::Key(value) => skein_llm::Credential::key(value.clone()),
        channel::GrantValue::Withdrawn { account_id, bearer } => {
            if account_id.is_empty() {
                skein_llm::Credential::bearer(bearer.clone())
            } else {
                skein_llm::Credential::codex(bearer.clone(), account_id.clone())
            }
        }
        channel::GrantValue::Program(_) => return None,
    };
    if credential.value.is_empty() {
        return None;
    }
    Some(credential)
}

/// Advance io, the components, and the domain once at injected clocks.
pub fn iterate(service: &mut Service, now: Time, wall: Wall) {
    clocks(service, now, wall);
    io_up(service);
    lower_events(service);
    component_up(service);
    domain_up(service);
    domain_down(service);
    component_down(service);
    teardown::advance(service);
    if let Some(events) = &mut service.events {
        events.sinks.down(service.io_env.now, &mut service.io_requests);
    }
    io_down(service);
    if let Some(io) = &mut service.io {
        io.reclaim();
    }
    service.llm.reclaim();
    service.domain.as_mut().expect("framed agent owns its domain").reclaim();
    service.owners.reclaim();
}

fn clocks(service: &mut Service, now: Time, wall: Wall) {
    service.domain_env.now = now;
    service.domain_env.wall = wall;
    service.llm_env.now = now;
    service.llm_env.wall = wall;
    service.machine_env.now = now;
    service.machine_env.wall = wall;
    service.io_env.now = now;
    service.io_env.wall = wall;
}

/// Earliest active deadline over all child machines.
#[must_use]
pub fn next_deadline(service: &Service) -> Option<Time> {
    let mut earliest = match &service.domain {
        Some(domain) => domain.next_deadline(),
        None => None,
    };
    for deadline in [
        service.llm.next_deadline(),
        program::deadline(service),
        service.machine.next_deadline(),
        match &service.io {
            Some(io) => io.next_deadline(),
            None => None,
        },
        service.files.next_deadline(),
        service.live.deadline(),
    ] {
        earliest = match deadline {
            Some(next) => match earliest {
                Some(old) => Some(old.min(next)),
                None => Some(next),
            },
            None => earliest,
        };
    }
    earliest
}

/// Earliest policy deadline: the domain's, the LLM's and the machine's,
/// excluding io's and the files' (skein's testing-strategy.md, section 6).
#[must_use]
pub fn next_policy_deadline(service: &Service) -> Option<Time> {
    let mut earliest = match &service.domain {
        Some(domain) => domain.next_deadline(),
        None => None,
    };
    for deadline in [
        service.llm.next_deadline(),
        program::policy_deadline(service),
        service.machine.next_deadline(),
        service.live.deadline(),
    ] {
        earliest = match deadline {
            Some(next) => match earliest {
                Some(old) => Some(old.min(next)),
                None => Some(next),
            },
            None => earliest,
        };
    }
    earliest
}

// Count internal handoffs, including the optional program's lower queue.
fn backlog(service: &Service) -> u64 {
    let mut total = u64::from(program::backlog(service));
    for entries in [
        service.llm_events.len(),
        service.llm_below.len(),
        service.machine_events.len(),
        service.machine_below.len(),
        service.domain_events.len(),
        service.domain_requests.len(),
        service.io_events.len(),
        service.io_requests.len(),
        service.file_events.len(),
        service.io_submissions.len(),
        service.file_submissions.len(),
        service.submissions.len(),
        service.completions.len(),
    ] {
        total = total.checked_add(u64::from(entries)).expect("bounded queue counts fit u64");
    }
    total
}

fn admits(service: &Service) -> bool {
    backlog(service) < u64::from(service.limits.queue)
}

/// Whether another iteration can progress without waiting on the kernel.
#[must_use]
pub fn work_pending(service: &Service, now: Time) -> bool {
    let takes = match &service.events {
        Some(events) => events.sinks.takes(),
        None => true,
    };
    let due = match next_deadline(service) {
        Some(deadline) => deadline <= now,
        None => false,
    };
    due || teardown::work_pending(service)
        || match &service.io {
            Some(io) => io.is_ready(),
            None => false,
        }
        || (takes && (service.llm.has_work() || !service.llm.measures_drained()))
        || match &service.events {
            Some(events) => events.sinks.has_work(),
            None => false,
        }
        || match &service.domain {
            Some(domain) => takes && (domain.has_facts() || domain.is_ready()),
            None => false,
        }
        || !service.completions.is_empty()
        || !service.io_events.is_empty()
        || !service.file_events.is_empty()
        || !service.channel_events.is_empty()
        || (!service.pending_messages.is_empty() && service.admitted.is_some())
        || (takes && !service.llm_events.is_empty())
        || !service.machine_events.is_empty()
        || (takes && !service.domain_events.is_empty())
        || (takes && !service.domain_requests.is_empty() && channel_takes(service) && program::requests_ready(service))
        || !service.channel_below.is_empty()
        || program::pending(service)
        || !service.llm_below.is_empty()
        || !service.machine_below.is_empty()
        || !service.io_requests.is_empty()
        || !service.io_submissions.is_empty()
        || !service.file_submissions.is_empty()
}

/// The channel has ended, every lower child has settled, and the final
/// answer crossed the channel.
#[must_use]
pub fn done(service: &Service) -> Option<bool> {
    match service.teardown {
        Teardown::Closed { answer, .. } => Some(answer.sent()),
        Teardown::Live | Teardown::Answered { .. } | Teardown::Closing { .. } => None,
    }
}

/// The first concrete failure, retained for the shell's final stderr line.
#[must_use]
pub const fn failure(service: &Service) -> Option<Failure> {
    service.failure
}
