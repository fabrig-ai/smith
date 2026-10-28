//! Local-to-host routing keeps uncapped internal handoffs and capped edges.
//! Local policy owns durability and delivery; the host kit owns child lifecycle and channel rights. The saved
//! conversation position continues across activation-local channel turn numbers
//! (protocol/channel.md, section 5; protocol/hosts.md, section 5.3).

use alloc::boxed::Box;
use core::mem::size_of;
use skein_io::kernel;
use skein_lib::{Env, List, Map, Queue, Time, Token, Wall};
use smith_agent_service as agent_service;
use smith_agent_service::{Close, Step};

mod accounts;
mod choices;
mod destinations;
mod ending;
pub(crate) mod program_check;
use ending::Ending;
use smith_domain as agent;
use smith_host_domain as host;
use smith_inline_agent as inline;
use smith_local_domain as local;
use smith_local_protocol as protocol;
use smith_protocol_channel as channel;

use crate::process::{Launch, ProcessAdapter, ProcessLimits};

/// Immutable bounds for one local chat and its host process slot.
#[derive(Clone, Debug)]
pub struct Limits {
    pub local: local::Limits,
    pub inline: Option<InlineLimits>,
    /// Maximum tagged grant payload bytes retained by the owner.
    pub envelope_bytes: u64,
    pub process: ProcessLimits,
    /// Edge capacity (kernel, terminal, shell) and the work drained per iteration.
    pub queue: u32,
    /// Backlog at which a person's new line or command waits (protocol/limits.md 3.8).
    pub watermark: u32,
}

/// Startup policy, configured endpoint names and spawned-agent launch choices.
#[derive(Debug)]
pub struct Config {
    pub local: local::Config,
    pub limits: Limits,
    pub endpoints: channel::Endpoints,
    pub paths: Box<[Box<[u8]>]>,
    pub launch: Launch,
    pub agent: AgentConfig,
}

/// Receiving bounds of the LLM and machine components for the inline kind.
#[derive(Clone, Debug)]
pub struct InlineLimits {
    /// Complete checked lower-component ownership, including endpoint inventory.
    pub memory: u64,
}

impl InlineLimits {
    /// Price the exact LLM and machine configuration before moving its endpoint inventory.
    #[must_use]
    pub fn new(configuration: &agent_service::Config) -> Option<Self> {
        Some(Self {
            memory: agent_service::effects_worst_case(&configuration.limits, &configuration.llm_endpoints)?
                .checked_add(agent_service::io_worst_case(&configuration.limits)?)?,
        })
    }
}

/// Lower configuration chosen with the local agent kind at construction.
#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "caller-owned startup configuration moves once without an extra heap allocation"
)]
pub enum AgentConfig {
    /// LLM and machine components with separately owned workspace descriptors.
    Inline { configuration: agent_service::Config, roots: Box<[kernel::Fd]> },
    /// The process adapter uses the common launch configuration.
    Spawned,
}

/// Components below the chosen child, plus common terminal and delivery IO.
#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "bounded component ownership is priced directly")]
enum Effects {
    /// The process adapter here owns only common terminal, git and plain-directory IO.
    Inline { agent: agent_service::Effects, local: ProcessAdapter },
    /// Common IO and the spawned child's process and pipes.
    Spawned(ProcessAdapter),
}

impl Effects {
    fn process(&self) -> &ProcessAdapter {
        match self {
            Self::Inline { local, .. } => local,
            Self::Spawned(process) => process,
        }
    }

    fn process_mut(&mut self) -> &mut ProcessAdapter {
        match self {
            Self::Inline { local, .. } => local,
            Self::Spawned(process) => process,
        }
    }

    fn inline(&self) -> Option<&agent_service::Effects> {
        match self {
            Self::Inline { agent, .. } => Some(agent),
            Self::Spawned(_) => None,
        }
    }

    fn inline_mut(&mut self) -> Option<&mut agent_service::Effects> {
        match self {
            Self::Inline { agent, .. } => Some(agent),
            Self::Spawned(_) => None,
        }
    }
}

/// Startup cannot allocate or represent the requested local composition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// Local policy refused the operator's configuration.
    Local(local::Invalid),
    /// Account adoption cannot reserve its shell and terminal output.
    Queue,
    /// The requested retained bound or directory table cannot be represented.
    Memory,
    /// The lower agent or process configuration cannot be composed.
    Process,
}

/// Checked memory bound for both domains and their routing queues.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let mut total = 0_u64;
    for (_, part) in memory_parts(limits)? {
        total = total.checked_add(part)?;
    }
    Some(total)
}

/// Local ownership by part, with the inline effects' single IO priced by their admission.
#[must_use]
pub fn memory_parts(limits: &Limits) -> Option<[(&'static str, u64); 6]> {
    let effects = match limits.local.agent {
        local::AgentLimits::Inline(_) => {
            let bounds = limits.inline.as_ref()?;
            bounds.memory
        }
        local::AgentLimits::Spawned(_) => {
            if limits.inline.is_some() {
                return None;
            }
            0
        }
    };

    let process = ProcessAdapter::memory_parts(
        &limits.process,
        match limits.local.agent {
            local::AgentLimits::Inline(_) => false,
            local::AgentLimits::Spawned(_) => true,
        },
    )?;
    let terminal = protocol::terminal_worst_case(&protocol::TerminalLimits {
        line_bytes: limits.local.line_bytes,
        show_bytes: limits.local.show_bytes,
    })?;
    // Internal queues are estimated at queue entries.
    let routing = Queue::<local::Event>::worst_case(limits.queue)?
        .checked_add(Queue::<local::Request>::worst_case(limits.queue)?.checked_mul(2)?)?
        .checked_add(Queue::<host::Input>::worst_case(limits.queue)?)?
        .checked_add(Queue::<host::process::Request>::worst_case(limits.queue)?)?
        .checked_add(Queue::<protocol::TerminalEvent>::worst_case(limits.queue)?)?
        .checked_add(u64::from(limits.queue).checked_mul(u64::try_from(size_of::<local::Event>()).ok()?)?)?
        .checked_add(Queue::<Box<[u8]>>::worst_case(limits.queue)?)?
        .checked_add(Map::<u32, smith_protocol_channel::GrantValue>::worst_case(limits.local.smith.accounts)?)?
        .checked_add(u64::from(limits.local.smith.accounts).checked_mul(limits.envelope_bytes)?)?
        .checked_add(
            u64::from(limits.local.smith.run.directories).checked_mul(u64::try_from(size_of::<kernel::Fd>()).ok()?)?,
        )?
        .checked_add(Queue::<kernel::Complete>::worst_case(limits.queue)?)?
        .checked_add(Queue::<kernel::Submit>::worst_case(limits.queue)?)?
        .checked_add(List::<u32>::worst_case(64)?)?
        .checked_add(agent::max_turn_bytes(&limits.local.smith)?)?
        .checked_add(crate::live::Timing::worst_case(&limits.local.agent)?)?
        .checked_add(u64::try_from(size_of::<Service>()).ok()?)?
        .checked_add(*process.get(2)?)?;
    Some([
        ("local_domain", local::worst_case(&limits.local)?),
        ("effects_and_io", effects),
        ("store", *process.first()?),
        ("terminal", terminal),
        ("delivery", *process.get(1)?),
        ("local_routing", routing),
    ])
}

#[derive(Debug)]
enum SnapshotState {
    Idle,
    Capturing { saved: bool },
    Failed,
}

/// One local chat, its host kit and bounded requests to process and shell.
#[expect(missing_debug_implementations, reason = "the handed-in account owner contains credentials")]
pub struct Service {
    accounts: Option<crate::Accounts>,
    told: Option<agent::Turn>,
    remote: bool,
    remote_timing: crate::live::Timing,
    limits: Limits,
    local: local::Domain,
    below: Effects,
    lower_agent: Option<Token>,
    capture_time: skein_lib::Duration,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    now: Time,
    wall: Wall,
    plain_directories: List<u32>,
    capture_activation: Option<u64>,
    snapshot_state: SnapshotState,
    ending: Ending,
    terminal: protocol::Terminal,
    line_mode: Option<bool>,
    terminal_events: Queue<protocol::TerminalEvent>,
    output: Queue<Box<[u8]>>,
    local_events: Queue<local::Event>,
    local_requests: Queue<local::Request>,
    interrupted_credentials: u32,
    host_events: Queue<host::Input>,
    shell: Queue<local::Request>,
    lower: Queue<host::process::Request>,
    values: Map<u32, smith_protocol_channel::GrantValue>,
    paths: Box<[Box<[u8]>]>,
    start_values: Option<StartValues>,
    failed: bool,
    stream: Option<crate::stream::Spawned>,
}

/// Ordered lower values paired with the host kit's opaque Start names.
#[derive(Debug)]
pub struct StartValues {
    pub paths: Box<[Box<[u8]>]>,
    pub credentials: Box<[smith_protocol_channel::GrantValue]>,
}

impl Service {
    /// Construct the child kind selected by the local limits, without performing IO.
    pub fn new(mut config: Config, seed: u64) -> Result<Self, Error> {
        worst_case(&config.limits).ok_or(Error::Memory)?;
        let native = match config.agent {
            AgentConfig::Inline { configuration, roots } => {
                match config.limits.local.agent {
                    local::AgentLimits::Inline(_) => {}
                    local::AgentLimits::Spawned(_) => return Err(Error::Process),
                }
                Some(inline_effects(configuration, roots, &mut config.local, &config.limits, seed)?)
            }
            AgentConfig::Spawned => {
                match config.limits.local.agent {
                    local::AgentLimits::Inline(_) => return Err(Error::Process),
                    local::AgentLimits::Spawned(_) => {}
                }
                None
            }
        };
        let mut plain_directories = List::with_capacity(64);
        if let Some(workspace) = &config.local.workspace {
            for (position, directory) in workspace.directories.iter().enumerate() {
                if directory.writable
                    && !directory.git
                    && plain_directories.push(u32::try_from(position).expect("admitted directory")).is_err()
                {
                    return Err(Error::Memory);
                }
            }
        }
        let remote = config.local.live_text;
        let capture_time = config.local.budget.time;
        let local = match local::Domain::new(config.local, &config.limits.local, seed) {
            Ok(local) => local,
            Err(error) => return Err(Error::Local(error)),
        };
        let process =
            ProcessAdapter::new(&config.limits.process, config.launch, config.endpoints, seed).ok_or(Error::Process)?;
        let terminal = protocol::Terminal::new(protocol::TerminalLimits {
            line_bytes: config.limits.local.line_bytes,
            show_bytes: config.limits.local.show_bytes,
        })
        .ok_or(Error::Memory)?;
        let queue = config.limits.queue;
        let accounts = config.limits.local.smith.accounts;
        let remote_timing = crate::live::Timing::new(&config.limits.local.agent);
        Ok(Self {
            accounts: None,
            limits: config.limits,
            local,
            below: match native {
                Some(agent) => Effects::Inline { agent, local: process },
                None => Effects::Spawned(process),
            },
            lower_agent: None,
            capture_time,
            completions: Queue::with_capacity(queue),
            submissions: Queue::with_capacity(queue),
            now: Time::ZERO,
            wall: Wall::EPOCH,
            plain_directories,
            capture_activation: None,
            snapshot_state: SnapshotState::Idle,
            ending: Ending::Live { shell_closed: true },
            terminal,
            line_mode: None,
            terminal_events: Queue::with_capacity(queue),
            output: Queue::with_capacity(queue),
            local_events: Queue::uncapped(),
            local_requests: Queue::uncapped(),
            interrupted_credentials: 0,
            host_events: Queue::uncapped(),
            shell: Queue::with_capacity(queue),
            lower: Queue::uncapped(),
            values: Map::with_capacity(accounts),
            paths: config.paths,
            start_values: None,
            failed: false,
            stream: None,
            told: None,
            remote,
            remote_timing,
        })
    }

    /// Configure the spawned product's producer before adopting its destination.
    pub fn prepare_trace(&mut self, configuration: &agent_service::Config) {
        assert!(self.below.inline().is_none(), "inline effects already own their producer");
        self.stream = crate::stream::Spawned::configured(configuration);
    }

    /// Adopt a product trace and record its startup before the first pass.
    pub fn adopt_trace(
        &mut self,
        fd: kernel::Fd,
        now: Time,
        header: smith_events::SessionStarted,
        delivery: agent_service::sinks::Delivery,
    ) -> Result<(), kernel::Fd> {
        if let Some(stream) = &mut self.stream {
            let token = self.below.process_mut().adopt_trace(fd, stream.write_deadline)?;
            stream
                .producer
                .sinks()
                .add(agent_service::Destination::Trace, token, delivery)
                .expect("one configured product destination");
            stream.producer.started(now, header);
            return Ok(());
        }
        match &mut self.below {
            Effects::Inline { agent, local } => {
                agent.adopt_destination(fd, agent_service::Destination::Trace, delivery, true, local.io())?;
                agent.start_session(now, header);
            }
            Effects::Spawned(_) => return Err(fd),
        }
        Ok(())
    }

    fn drain_run_facts(&mut self) -> bool {
        let Some(effects) = self.below.inline_mut() else {
            return match &self.stream {
                Some(stream) => stream.producer.takes(),
                None => true,
            };
        };
        for _ in 0..self.limits.queue {
            if !effects.takes() {
                return false;
            }
            let Some(message) = self.local.pop_run_message() else { break };
            effects.message(message.name, &message.label, &message.text);
        }
        for _ in 0..self.limits.queue {
            if !effects.takes() {
                return false;
            }
            let Some(fact) = self.local.pop_run_fact() else {
                effects.facts_drained();
                return effects.takes();
            };
            effects.fact(&fact);
        }
        false
    }

    /// Queue a terminal, store, git or OAuth outcome from the shell.
    pub fn local_event(&mut self, event: local::Event) {
        self.local_events.push(event);
    }

    /// Select the already prepared initial chat beneath the adopted chats root.
    pub fn initial_chat(&mut self) {
        let name = self.local.initial_chat();
        self.below.process_mut().store_request(protocol::StoreRequest::SelectChat { name, create: false });
    }

    /// Enable the product's line commands and terminal prompt before receiving input.
    pub fn line_mode(&mut self, plain: bool) {
        self.line_mode = Some(plain);
        self.local.line_mode();
    }

    /// First local cause of an unanswering run, kept through effects settlement.
    #[must_use]
    pub const fn end_cause(&self) -> Option<local::EndCause> {
        self.local.end_cause()
    }

    /// Bounded chat name for the final operator line.
    #[must_use]
    pub fn chat_name(&self) -> &[u8] {
        self.local.chat_name()
    }

    /// Words for an entrance refusal, from the sent charter and startup declarations.
    #[must_use]
    pub fn effort_notice(&self, notice: &local::Notice) -> Option<Box<[u8]>> {
        match notice {
            local::Notice::Failure(local::Failure::Refused(host::Refusal::Invalid(host::RunInvalid::Effort))) => {
                Some(self.local.effort_words())
            }
            local::Notice::Failure(_)
            | local::Notice::Warning(_)
            | local::Notice::Waiting
            | local::Notice::Checking(_)
            | local::Notice::NotDelivered { .. }
            | local::Notice::ChatRefused { .. } => None,
        }
    }

    /// Feed one standard-input byte to the bounded line translator.
    pub fn feed(&mut self, byte: u8) {
        self.terminal.feed(byte, &mut self.terminal_events);
    }

    /// Deliver a terminal interrupt to the run or stop its child on a repeat.
    pub fn interrupt(&mut self) {
        self.terminal.interrupt(&mut self.terminal_events);
    }

    /// Report standard-input closure to local policy.
    pub fn closed(&mut self) {
        self.terminal.closed(&mut self.terminal_events);
    }

    /// Receiving cap used by both renders of a notice.
    #[must_use]
    pub fn show_bytes(&self) -> u32 {
        self.limits.local.show_bytes
    }

    /// Model inventory borrowed by the operator notice renderer.
    #[must_use]
    pub fn models(&self) -> &[agent::run::charter::Llm] {
        self.local.models()
    }

    /// Text ready for the terminal writer.
    pub fn output(&mut self) -> &mut Queue<Box<[u8]>> {
        &mut self.output
    }

    /// Take a typed live-view record from the same sink that owns the trace.
    pub fn pop_front(&mut self) -> Option<agent_service::sinks::View> {
        match self.below.inline_mut() {
            Some(effects) => match effects.front_sinks() {
                Some(sinks) => sinks.pop_front(),
                None => None,
            },
            None => match &mut self.stream {
                Some(stream) => stream.producer.sinks().pop_front(),
                None => None,
            },
        }
    }

    /// Take text decoded beside the host domain; loss never holds the run.
    pub fn pop_live(&mut self) -> Option<crate::LivePiece> {
        let through = match &self.told {
            Some(_) if self.remote => Some(self.remote_timing.number),
            Some(_) | None => None,
        };
        self.below.process_mut().live.pop_through(through)
    }

    /// Live pieces omitted by the local front queue.
    #[must_use]
    pub fn live_lost(&self) -> u64 {
        self.below.process().live.lost()
    }

    /// The activation whose text and told turns the interactive front shows.
    #[must_use]
    pub const fn remote_activation(&self) -> Option<u64> {
        match self.capture_activation {
            Some(_) => Some(self.remote_timing.generation),
            None => None,
        }
    }

    /// Move the existing retained turn into the spawned chat's front.
    pub fn take_remote_turn(&mut self) -> Option<crate::RemoteTurn> {
        if !self.remote {
            return None;
        }
        Some(crate::RemoteTurn {
            number: self.remote_timing.number,
            elapsed: self.remote_timing.elapsed,
            since_start: self.remote_timing.previous.saturating_since(self.remote_timing.started),
            turn: self.told.take()?,
        })
    }

    /// Whether the product has a typed record to consume on its next pass.
    #[must_use]
    pub fn front_pending(&self) -> bool {
        if self.below.process().live.pending() || (self.remote && self.told.is_some()) {
            return true;
        }
        match self.below.inline() {
            Some(effects) => effects.front_pending(),
            None => match &self.stream {
                Some(stream) => stream.producer.front_pending(),
                None => false,
            },
        }
    }

    /// Loss in the typed view, kept apart from every destination's own loss.
    #[must_use]
    pub fn front_lost(&mut self) -> u64 {
        match self.below.inline_mut() {
            Some(effects) => match effects.front_sinks() {
                Some(sinks) => sinks.front_lost(),
                None => 0,
            },
            None => match &mut self.stream {
                Some(stream) => stream.producer.sinks().front_lost(),
                None => 0,
            },
        }
    }

    /// The next destination given up, with why, for the shell's one line on
    /// standard error (protocol/events.md, section 5.2).
    #[must_use]
    pub fn pop_given_up(&mut self) -> Option<agent_service::GivenUp> {
        match self.below.inline_mut() {
            Some(effects) => match effects.front_sinks() {
                Some(sinks) => sinks.pop_given_up(),
                None => None,
            },
            None => match &mut self.stream {
                Some(stream) => stream.producer.sinks().pop_given_up(),
                None => None,
            },
        }
    }

    /// Lend a credential value only beside the domain's grant name.
    pub fn credential(&mut self, grant: agent::Grant, value: smith_protocol_channel::GrantValue) {
        let Ok(form) = smith_host_protocol::grant_form(&value) else {
            self.local_events.push(local::Event::NoCredential {
                account: grant.name.account,
                reason: local::CredentialFailure::Missing,
            });
            return;
        };
        let bytes = match &value {
            channel::GrantValue::Bearer(bytes)
            | channel::GrantValue::Key(bytes)
            | channel::GrantValue::Program(bytes) => bytes.len(),
            channel::GrantValue::Withdrawn { account_id, bearer } => account_id.len().saturating_add(bearer.len()),
        };
        if u64::try_from(bytes).expect("grant payload bytes fit u64") > self.limits.envelope_bytes {
            self.local_events.push(local::Event::NoCredential {
                account: grant.name.account,
                reason: local::CredentialFailure::Missing,
            });
            return;
        }
        if let Some(effects) = self.below.inline_mut()
            && effects.grant(grant, &value, self.now).is_err()
        {
            self.local_events.push(local::Event::NoCredential {
                account: grant.name.account,
                reason: local::CredentialFailure::Missing,
            });
            return;
        }
        self.values.insert(grant.name.account, value).expect("configured account capacity");
        self.local_events.push(local::Event::Credential { grant, form });
    }

    #[cfg(test)]
    pub(crate) fn store_requests(&mut self) -> &mut Queue<protocol::StoreRequest> {
        self.below.process_mut().store_requests()
    }

    /// Shell work, including credential requests and the final exit.
    pub fn shell_requests(&mut self) -> &mut Queue<local::Request> {
        &mut self.shell
    }

    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut next: Option<Time> = None;
        for deadline in [
            self.local.next_deadline(),
            match &self.accounts {
                Some(accounts) => accounts.next_deadline(),
                None => None,
            },
            self.below.process().next_deadline(),
            match self.below.inline() {
                Some(effects) => effects.next_deadline(),
                None => None,
            },
        ]
        .into_iter()
        .flatten()
        {
            next = Some(match next {
                Some(current) => current.min(deadline),
                None => deadline,
            });
        }
        next
    }

    // Entries between the service's own children (protocol/limits.md 3.8).
    // Edges and each child's own internal queues never count.
    fn backlog(&self) -> u64 {
        let mut total = u64::from(self.below.process().backlog());
        for entries in [self.local_events.len(), self.local_requests.len(), self.host_events.len(), self.lower.len()] {
            total = total.checked_add(u64::from(entries)).expect("service backlog entries fit u64");
        }
        total
    }

    // New work waits at the watermark (dynamic-memory.md 4.2).
    fn admits(&self) -> bool {
        self.backlog() < u64::from(self.limits.watermark)
    }

    // O1: dispatch the previous entrance's requests before its successor's
    // facts. Interrupt and Drop pass to cancel undispatched credentials.
    fn local_requests_dispatched(&self) -> bool {
        self.local_requests.is_empty()
    }

    fn local_head_ready(&self) -> bool {
        match self.local_events.iter().next() {
            Some(local::Event::Interrupt | local::Event::Drop) => true,
            Some(
                local::Event::Line { .. }
                | local::Event::Slash(_)
                | local::Event::Closed
                | local::Event::SelectChat { .. }
                | local::Event::ChatSelected { .. }
                | local::Event::Loaded { .. }
                | local::Event::StateSaved
                | local::Event::TurnSaved { .. }
                | local::Event::DeliverySaved { .. }
                | local::Event::Git { .. }
                | local::Event::PlainStatus { .. }
                | local::Event::StoreFailed { .. }
                | local::Event::Credential { .. }
                | local::Event::NoCredential { .. }
                | local::Event::Below { .. }
                | local::Event::Process(_),
            ) => self.local_requests_dispatched(),
            None => false,
        }
    }

    fn local_request_ready(&self) -> bool {
        self.shell.room() > 0 && self.terminal_events.room() >= protocol::terminal_max_out()
    }

    // Admission and FIFO head readiness are shared by the drain and readiness.
    // Only a person's new line or command waits (dynamic-memory.md 4.2–4.3).
    fn terminal_head_ready(&self) -> bool {
        match self.terminal_events.iter().next() {
            Some(protocol::TerminalEvent::Domain(event)) => match event.as_ref() {
                local::Event::Line { .. } | local::Event::Slash(_) => self.admits(),
                local::Event::Interrupt
                | local::Event::Drop
                | local::Event::Closed
                | local::Event::SelectChat { .. }
                | local::Event::ChatSelected { .. }
                | local::Event::Loaded { .. }
                | local::Event::StateSaved
                | local::Event::TurnSaved { .. }
                | local::Event::DeliverySaved { .. }
                | local::Event::Git { .. }
                | local::Event::PlainStatus { .. }
                | local::Event::StoreFailed { .. }
                | local::Event::Credential { .. }
                | local::Event::NoCredential { .. }
                | local::Event::Below { .. }
                | local::Event::Process(_) => true,
            },
            Some(protocol::TerminalEvent::DropRun | protocol::TerminalEvent::Write(_)) => true,
            None => false,
        }
    }

    #[must_use]
    pub fn work_pending(&self, now: Time) -> bool {
        match &self.accounts {
            Some(accounts) if accounts.work_pending(now) => return true,
            Some(_) | None => {}
        }
        let takes = match self.below.inline() {
            Some(effects) => effects.takes(),
            None => match &self.stream {
                Some(stream) => stream.producer.takes(),
                None => true,
            },
        };
        self.end_work()
            || !self.completions.is_empty()
            || !self.submissions.is_empty()
            || (takes && self.local_requests_dispatched() && (self.local.is_ready() || self.local.is_due(now)))
            || match self.below.inline() {
                Some(effects) => effects.work_pending(now),
                None => false,
            }
            || (takes && self.local_head_ready())
            || (takes && !self.local_requests.is_empty() && self.local_request_ready() && self.told.is_none())
            || !self.host_events.is_empty()
            || (self.terminal_head_ready() && self.output.room() > 0)
            || self.below.process().work_pending(now)
            || match &self.stream {
                Some(stream) => {
                    stream.producer.has_work()
                        || (stream.needs_measure()
                            && self.below.process().settled(
                                stream
                                    .producer
                                    .streams()
                                    .checked_add(self.below.process().signal_streams())
                                    .expect("bounded retained streams"),
                            ))
                }
                None => false,
            }
    }

    #[must_use]
    pub fn failed(&self) -> bool {
        self.failed || self.below.process().failed()
    }

    /// Kernel completions for the child process and its three pipes.
    pub fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }

    /// Adopt the separately opened chat root into the store's IO owner.
    pub fn adopt_store(&mut self, root: kernel::Fd) -> Result<(), Error> {
        self.below.process_mut().adopt_store(root).ok_or(Error::Memory)
    }

    /// The private chat's known-name cleanup owns this parent directory.
    pub fn ephemeral(&mut self, parent: kernel::Fd, name: Box<[u8]>) -> Result<(), kernel::Fd> {
        self.below.process_mut().ephemeral(parent, name)
    }

    /// One warning from a private store cleanup that could not finish.
    pub fn cleanup_warning(&mut self) -> Option<kernel::Error> {
        self.below.process_mut().cleanup_warning()
    }

    /// Whether the local domain and effects live in this process.
    #[must_use]
    pub fn inline(&self) -> bool {
        match self.below {
            Effects::Inline { .. } => true,
            Effects::Spawned(_) => false,
        }
    }

    /// Adopt the shell writer into the inline product's single IO.
    pub fn adopt_writer(&mut self, fd: kernel::Fd) -> Result<Token, kernel::Fd> {
        self.below.process_mut().adopt_writer(fd)
    }

    /// Queue one request of the shell's adopted writer behind the service's own IO.
    pub fn writer_request(&mut self, request: skein_io::Request) {
        self.below.process_mut().io_requests().push(request);
    }

    /// Terminals of the shell's adopted output stream.
    pub fn writer_events(&mut self) -> &mut Queue<skein_io::Event> {
        self.below.process_mut().shell_events()
    }

    /// Read the adopted input's size after the shell hears a window change.
    pub fn read_window(&mut self) {
        self.below.process_mut().read_window();
    }

    /// Adopt terminal input and blocked termination signals into the IO pass.
    pub fn adopt_terminal(&mut self, input: Option<kernel::Fd>, signals: kernel::Fd) -> Result<(), kernel::Fd> {
        self.below.process_mut().adopt_terminal(input, signals, !self.local.headless())?;
        self.ending = Ending::Live { shell_closed: false };
        Ok(())
    }

    /// Adopt separate workspace descriptors for delivery IO and its explicit child environment.
    /// Colocated effects own their roots independently; these descriptors must not alias them.
    pub fn adopt_delivery_roots(
        &mut self,
        roots: Box<[kernel::Fd]>,
        environment: Box<[Box<[u8]>]>,
    ) -> Result<(), Error> {
        if roots.len() > 64
            || roots.len() > usize::try_from(self.limits.local.smith.run.directories).expect("directory bound fits")
            || environment.len() > 64
        {
            return Err(Error::Memory);
        }
        let mut bytes = 0_usize;
        for entry in &environment {
            bytes = bytes.checked_add(entry.len()).ok_or(Error::Memory)?;
            if bytes > 4096 || !entry.contains(&b'=') || entry.contains(&0) {
                return Err(Error::Memory);
            }
        }
        self.below.process_mut().adopt_delivery_roots(roots, environment);
        Ok(())
    }

    /// Kernel work submitted by the supervised child process.
    pub fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    /// Content-free local observations for the shell or an independent referee.
    pub fn pop_fact(&mut self) -> Option<local::Fact> {
        self.local.pop_fact()
    }

    /// The host wall clock at its last answer, before teardown and store settlement.
    #[must_use]
    pub fn answer_wall(&self) -> Option<Wall> {
        self.local.answer_wall()
    }

    /// Configured endpoint text for the operator, without provider addresses.
    #[must_use]
    pub fn endpoint_name(&self, number: u32) -> Option<&[u8]> {
        self.below.process().endpoint_name(number)
    }

    /// One headless told turn, retained until the shell renders its status.
    pub fn take_told_turn(&mut self) -> Option<agent::Turn> {
        self.told.take()
    }

    fn route_completions(&mut self) {
        for _ in 0..self.completions.capacity() {
            let Some(complete) = self.completions.pop() else { break };
            if complete.op.raw() & ACCOUNT_OPERATION_BIT != 0 {
                self.accounts.as_mut().expect("account operation owner").complete(kernel::Complete {
                    op: Token::new(complete.op.raw() & !ACCOUNT_OPERATION_BIT),
                    kind: complete.kind,
                    result: complete.result,
                });
            } else if complete.op.raw() & EFFECT_OPERATION_BIT == 0 {
                self.below.process_mut().completions().push(complete);
            } else {
                self.below.inline_mut().expect("effect namespace belongs to colocated mode").completions().push(
                    kernel::Complete {
                        op: Token::new(complete.op.raw() & !EFFECT_OPERATION_BIT),
                        kind: complete.kind,
                        result: complete.result,
                    },
                );
            }
        }
    }

    fn flush_submissions(&mut self) {
        if let Some(accounts) = &mut self.accounts {
            for _ in 0..self.limits.queue {
                if self.submissions.room() == 0 {
                    break;
                }
                let Some(submit) = accounts.submissions().pop() else { break };
                self.submissions.push(namespace_submit(submit, ACCOUNT_OPERATION_BIT));
            }
        }
        for _ in 0..self.limits.queue {
            if self.submissions.room() == 0 {
                break;
            }
            let Some(submit) = self.below.process_mut().submissions().pop() else { break };
            self.submissions.push(namespace_submit(submit, 0));
        }
        if let Some(effects) = self.below.inline_mut() {
            for _ in 0..self.limits.queue {
                if self.submissions.room() == 0 {
                    break;
                }
                let Some(submit) = effects.submissions().pop() else { break };
                self.submissions.push(namespace_submit(submit, EFFECT_OPERATION_BIT));
            }
        }
    }

    fn state_saved(&mut self) -> bool {
        match &mut self.snapshot_state {
            SnapshotState::Capturing { saved } => {
                *saved = true;
                false
            }
            SnapshotState::Idle => true,
            SnapshotState::Failed => false,
        }
    }

    fn route_local(&mut self, now: Time, request: local::Request) {
        match request {
            local::Request::Process(request) => {
                if self.remote || self.local.headless() {
                    self.remote_timing.sent(&request, now);
                }
                self.prepare_values(&request);
                self.lower.push(request);
            }
            local::Request::Git { owner, directory, op, deadline } => match op {
                local::GitOp::Markers { paths } => {
                    self.below.process_mut().start_markers(owner, directory, paths, deadline, &mut self.local_events);
                }
                op @ (local::GitOp::Head
                | local::GitOp::Status
                | local::GitOp::Inspect { .. }
                | local::GitOp::Commit { .. }
                | local::GitOp::Push { .. }) => {
                    self.below.process_mut().start_git(owner, directory, op, deadline, &mut self.local_events);
                }
            },
            local::Request::Lower { agent, request } => {
                match self.lower_agent {
                    Some(current) => assert_eq!(current, agent, "one inline slot owns every outstanding lower right"),
                    None => self.lower_agent = Some(agent),
                }
                self.below.inline_mut().expect("native IO belongs to inline mode").request(to_root_request(request));
            }
            local::Request::Input { reading } => self.below.process_mut().input(reading),
            local::Request::Notice { notice } => self.notice(now, notice),
            local::Request::Show { text } => {
                if !self.local.headless() {
                    self.terminal.show(text, &mut self.terminal_events);
                }
            }
            local::Request::Load => {
                self.lower_agent = None;
                self.capture_activation = None;
                self.terminal.answer_finished();
                self.below.process_mut().store_request(protocol::StoreRequest::Load);
            }
            local::Request::SelectChat { name, create } => {
                self.below.process_mut().store_request(protocol::StoreRequest::SelectChat { name, create });
            }
            local::Request::PlainStatus { owner, directory, deadline } => {
                self.below.process_mut().plain_status(owner, directory, deadline, &mut self.local_events);
            }
            local::Request::SaveState { state, fresh } => self.save_state(now, state, fresh),
            local::Request::Answered { answer } => {
                self.release_accounts(now);
                if let Some(effects) = self.below.inline_mut() {
                    effects.answered(now, crate::stream::answered(&answer, effects.capture()));
                }
                if let Some(stream) = &mut self.stream {
                    stream.answered(now, &answer);
                }
                if self.local.headless() {
                    self.shell.push(local::Request::Answered { answer });
                }
            }
            local::Request::Unanswered { cause, turns, spent, read } => {
                self.release_accounts(now);
                self.unanswered(now, cause, turns, spent, read);
            }
            local::Request::Exit { status } => {
                let shell_closed = match self.ending {
                    Ending::Live { shell_closed } => shell_closed,
                    Ending::Closing { .. } | Ending::Closed { .. } => unreachable!("one local last word"),
                };
                self.ending = Ending::Closing {
                    at: now,
                    exit: status,
                    close: Close::Graceful,
                    step: Step::Components,
                    shell_closed,
                };
                self.below.process_mut().close();
                if let Some(effects) = self.below.inline_mut() {
                    effects.close();
                }
            }
            local::Request::SaveTurn { number, read, opens_window, turn } => {
                self.save_turn(now, number, read, opens_window, turn);
            }
            local::Request::SaveDelivery { record } => {
                self.below.process_mut().store_request(protocol::StoreRequest::SaveDelivery { record });
            }
            local::Request::Credential { account } => self.account_request(account),
        }
    }

    fn save_state(&mut self, now: Time, state: local::ChatState, fresh: bool) {
        if self.capture_activation != Some(state.activation) {
            self.capture_activation = Some(state.activation);
            self.below.process_mut().live.clear();
            self.remote_timing.activate(now);
            let (charter, messages) = self.local.charter();
            if let Some(effects) = self.below.inline_mut() {
                effects.starting(state.activation, messages, &charter);
            }
            if let Some(stream) = &mut self.stream {
                stream.starting(now, state.activation, messages, &charter);
            }
            self.snapshot_state = SnapshotState::Capturing { saved: false };
            self.below
                .process_mut()
                .capture_plain(self.plain_directories.clone(), now.saturating_add(self.capture_time));
        }
        self.below.process_mut().store_request(protocol::StoreRequest::SaveState {
            state,
            history: if fresh { protocol::History::Fresh } else { protocol::History::Kept },
        });
    }

    fn save_turn(&mut self, now: Time, number: u32, read: Option<Token>, opens_window: bool, turn: agent::Turn) {
        if self.local.headless() || self.remote {
            self.remote_timing.told(number, read, now);
            assert!(self.told.is_none(), "one told turn waits for the front end");
            self.told = Some(turn.clone());
        }
        self.below.process_mut().store_request(protocol::StoreRequest::SaveTurn { number, read, opens_window, turn });
    }

    fn notice(&mut self, now: Time, notice: local::Notice) {
        if self.line_mode == Some(false) && notice == local::Notice::Waiting {
            self.terminal.show(b"> ".as_slice().into(), &mut self.terminal_events);
            return;
        }
        let handed = match &notice {
            local::Notice::Failure(local::Failure::Credential { account, reason }) => match reason {
                local::CredentialFailure::ProgramNotSignedIn
                | local::CredentialFailure::ProgramSignedInOtherwise { .. }
                | local::CredentialFailure::ProgramFailed(_)
                | local::CredentialFailure::ProgramInterrupted
                | local::CredentialFailure::BorrowedExpiring { .. } => true,
                local::CredentialFailure::Missing | local::CredentialFailure::Refresh => match &self.accounts {
                    Some(accounts) => accounts.account_id(*account).is_some(),
                    None => false,
                },
            },
            local::Notice::Failure(_)
            | local::Notice::Warning(_)
            | local::Notice::Waiting
            | local::Notice::Checking(_)
            | local::Notice::NotDelivered { .. }
            | local::Notice::ChatRefused { .. } => false,
        };
        if !handed && !self.local.headless() {
            match self.effort_notice(&notice) {
                Some(text) => self.terminal.show(text, &mut self.terminal_events),
                None => self.terminal.notice(
                    &notice,
                    self.local.chat_name(),
                    self.local.models(),
                    &mut self.terminal_events,
                ),
            }
        }
        match notice {
            local::Notice::Warning(local::Warning::ForcedExit { exit, forced }) => {
                self.forced_exit(now, exit, forced);
                self.shell.push(local::Request::Notice { notice });
            }
            local::Notice::Failure(_) => self.shell.push(local::Request::Notice { notice }),
            local::Notice::Waiting
            | local::Notice::Checking(_)
            | local::Notice::NotDelivered { .. }
            | local::Notice::ChatRefused { .. } => {}
        }
    }

    fn forced_exit(&mut self, now: Time, exit: Option<host::Exit>, forced: host::Forced) {
        if let Some(stream) = &mut self.stream {
            stream.forced_exit(now, exit, forced);
        }
    }

    fn unanswered(&mut self, now: Time, cause: local::EndCause, turns: u32, spent: u64, read: Option<Token>) {
        let answer = crate::stream::unanswered(cause, turns, spent, read);
        if let Some(effects) = self.below.inline_mut() {
            effects.answered(now, answer.clone());
        }
        if let Some(stream) = &mut self.stream {
            stream.unanswered(now, answer);
        }
    }

    fn drain_terminal(&mut self) {
        for _ in 0..self.terminal_events.capacity() {
            if !self.terminal_head_ready() || self.output.room() == 0 {
                break;
            }
            let Some(event) = self.terminal_events.pop() else { break };
            match event {
                protocol::TerminalEvent::Domain(event) => match *event {
                    local::Event::Line { text } if self.line_mode.is_some() => match protocol::entered(text) {
                        protocol::Entered::Message(text) => self.local_events.push(local::Event::Line { text }),
                        protocol::Entered::Slash(choice) => match choices::resolve(self, choice) {
                            Ok(choice) => self.local_events.push(local::Event::Slash(choice)),
                            Err(text) => self.terminal.show(text, &mut self.terminal_events),
                        },
                        protocol::Entered::Refused => self
                            .terminal
                            .show(b"Unknown or incomplete command".as_slice().into(), &mut self.terminal_events),
                    },
                    event @ (local::Event::Line { .. }
                    | local::Event::Slash(_)
                    | local::Event::Interrupt
                    | local::Event::Drop
                    | local::Event::Closed
                    | local::Event::SelectChat { .. }
                    | local::Event::ChatSelected { .. }
                    | local::Event::Loaded { .. }
                    | local::Event::StateSaved
                    | local::Event::TurnSaved { .. }
                    | local::Event::DeliverySaved { .. }
                    | local::Event::Git { .. }
                    | local::Event::PlainStatus { .. }
                    | local::Event::StoreFailed { .. }
                    | local::Event::Credential { .. }
                    | local::Event::NoCredential { .. }
                    | local::Event::Below { .. }
                    | local::Event::Process(_)) => self.local_events.push(event),
                },
                protocol::TerminalEvent::DropRun => self.local_events.push(local::Event::Drop),
                protocol::TerminalEvent::Write(text) => self.output.push(text),
            }
        }
    }

    fn start_captured(&mut self) {
        match self.below.process_mut().take_capture_done() {
            Some(true) => {
                let state = core::mem::replace(&mut self.snapshot_state, SnapshotState::Idle);
                match state {
                    SnapshotState::Capturing { saved: true } => self.local_events.push(local::Event::StateSaved),
                    SnapshotState::Capturing { saved: false } => {}
                    SnapshotState::Idle | SnapshotState::Failed => unreachable!("capture belongs to activation save"),
                }
            }
            Some(false) => {
                self.snapshot_state = SnapshotState::Failed;
                self.local_events.push(local::Event::StoreFailed { turn: None, reason: local::StoreFailure::Read });
            }
            None => {}
        }
    }

    fn prepare_values(&mut self, request: &host::process::Request) {
        match request {
            host::process::Request::Send { message, .. } => match message {
                host::Down::Start { start, .. } => {
                    let mut credentials =
                        List::with_capacity(u32::try_from(start.grants.len()).expect("bounded grant count"));
                    for grant in &start.grants {
                        credentials
                            .push(self.values.get(&grant.account).expect("credential supplied before Start").clone())
                            .expect("one value per named grant");
                    }
                    assert_eq!(
                        start.directories.len(),
                        self.paths.len(),
                        "typed mounted directories keep configured path order"
                    );
                    self.start_values =
                        Some(StartValues { paths: self.paths.clone(), credentials: credentials.into_boxed() });
                }
                host::Down::Message { .. }
                | host::Down::Acknowledge { .. }
                | host::Down::Answer { .. }
                | host::Down::Grant { .. }
                | host::Down::Cancel => {}
            },
            host::process::Request::Spawn { .. }
            | host::process::Request::Read { .. }
            | host::process::Request::Signal { .. }
            | host::process::Request::Wait { .. }
            | host::process::Request::Reap { .. } => {}
        }
    }

    fn process_terminals(&mut self) {
        for _ in 0..self.limits.queue {
            match self.host_events.pop() {
                Some(host::Input::Process(event)) => self.local_events.push(local::Event::Process(event)),
                Some(host::Input::Parent(_)) => unreachable!("the process adapter supplies process terminals only"),
                None => break,
            }
        }
    }
}

/// Build the selected inline components and adopt exactly the configured roots.
fn inline_effects(
    configuration: agent_service::Config,
    roots: Box<[kernel::Fd]>,
    local: &mut local::Config,
    limits: &Limits,
    seed: u64,
) -> Result<agent_service::Effects, Error> {
    if configuration.limits.domain != limits.local.smith || configuration.limits.io != limits.process.io {
        return Err(Error::Process);
    }
    for endpoint in &limits.local.endpoints {
        if !configuration.domain.endpoints.contains(endpoint) {
            return Err(Error::Process);
        }
    }
    let actual = InlineLimits::new(&configuration).ok_or(Error::Memory)?;
    let advertised = limits.inline.as_ref().ok_or(Error::Memory)?;
    if actual.memory > advertised.memory {
        return Err(Error::Memory);
    }
    let Ok(mut effects) = agent_service::Effects::new(configuration, seed) else { return Err(Error::Process) };
    let charter = local::charter(local);
    effects.bind_grants(&charter).or(Err(Error::Process))?;
    effects.contract(charter.outcome);
    local.workspace = match local.workspace.take() {
        Some(workspace) => match effects.workspace(workspace, roots) {
            Ok(workspace) => Some(workspace),
            Err(_) => return Err(Error::Process),
        },
        None if roots.is_empty() => None,
        None => return Err(Error::Memory),
    };
    Ok(effects)
}

/// One bounded up pass followed by one bounded down pass.
pub fn iterate(service: &mut Service, now: Time, wall: Wall) {
    service.now = now;
    service.wall = wall;
    service.route_completions();
    service.accounts_pass(now, wall);
    let process_env = service.below.process().at(now, wall);
    service.below.process_mut().io_up(&process_env);
    match &mut service.below {
        Effects::Inline { agent, local } => local.effects_up(agent),
        Effects::Spawned(_) => {}
    }
    if let Some(effects) = service.below.inline_mut() {
        effects.up(now, wall);
        for _ in 0..service.limits.queue {
            let Some(event) = effects.event() else { break };
            service.local_events.push(local::Event::Below {
                agent: service.lower_agent.expect("terminal belongs to an inline lower request"),
                terminal: to_below(event),
            });
        }
    }
    service.below.process_mut().up(
        &process_env,
        &mut service.host_events,
        &mut service.terminal,
        &mut service.terminal_events,
        &mut service.local_events,
        &mut service.stream,
    );
    if service.below.process_mut().take_shutdown() {
        service.shutdown();
    }
    service.drain_terminal();
    service.start_captured();
    service.process_terminals();
    local_pass(service, now, wall);
    let notify = !service.closing();
    service.below.process_mut().programs_pass(now, &mut service.local_events, &mut service.shell, notify);
    service.below.process_mut().down(
        &process_env,
        &mut service.lower,
        &mut service.start_values,
        &service.values,
        &mut service.host_events,
        &mut service.stream,
    );
    match &mut service.below {
        Effects::Inline { agent, local } => {
            agent.down();
            for _ in 0..service.limits.queue {
                let Some(request) = agent.io_requests().pop() else { break };
                local.io_requests().push(request);
            }
        }
        Effects::Spawned(_) => {}
    }
    service.flush_submissions();
    service.advance_end();
    service.local.reclaim();
}

const ACCOUNT_OPERATION_BIT: u64 = 1 << 62;
const EFFECT_OPERATION_BIT: u64 = 1 << 63;

fn local_pass(service: &mut Service, now: Time, wall: Wall) {
    let local_env = Env { now, wall, limits: service.limits.local.clone() };
    for _ in 0..service.limits.queue {
        if !service.drain_run_facts() {
            break;
        }
        if !service.local_head_ready() {
            break;
        }
        let Some(event) = service.local_events.pop() else { break };
        match &event {
            local::Event::Interrupt | local::Event::Drop => service.interrupt_program_requests(),
            local::Event::Process(event) => {
                if service.remote || service.local.headless() {
                    service.remote_timing.received(event, now);
                }
            }
            local::Event::StateSaved
            | local::Event::Line { .. }
            | local::Event::Slash(_)
            | local::Event::Closed
            | local::Event::ChatSelected { .. }
            | local::Event::SelectChat { .. }
            | local::Event::Loaded { .. }
            | local::Event::TurnSaved { .. }
            | local::Event::DeliverySaved { .. }
            | local::Event::Git { .. }
            | local::Event::PlainStatus { .. }
            | local::Event::StoreFailed { .. }
            | local::Event::Credential { .. }
            | local::Event::NoCredential { .. }
            | local::Event::Below { .. } => {}
        }
        match event {
            local::Event::StateSaved => {
                if !service.state_saved() {
                    continue;
                }
            }
            other @ (local::Event::Line { .. }
            | local::Event::Slash(_)
            | local::Event::Interrupt
            | local::Event::Drop
            | local::Event::Closed
            | local::Event::ChatSelected { .. }
            | local::Event::SelectChat { .. }
            | local::Event::Loaded { .. }
            | local::Event::TurnSaved { .. }
            | local::Event::DeliverySaved { .. }
            | local::Event::Git { .. }
            | local::Event::PlainStatus { .. }
            | local::Event::StoreFailed { .. }
            | local::Event::Credential { .. }
            | local::Event::NoCredential { .. }
            | local::Event::Below { .. }
            | local::Event::Process(_)) => {
                local::step(&mut service.local, &local_env, other, &mut service.local_requests);
                continue;
            }
        }
        local::step(&mut service.local, &local_env, local::Event::StateSaved, &mut service.local_requests);
    }
    if service.drain_run_facts() && service.local.is_due(now) && service.local_requests_dispatched() {
        local::fire(&mut service.local, &local_env, &mut service.local_requests);
    }
    if service.drain_run_facts() && service.local.is_ready() && service.local_requests_dispatched() {
        local::resume(&mut service.local, &local_env, &mut service.local_requests);
    }
    for _ in 0..service.limits.queue {
        if !service.drain_program_interruptions()
            || service.told.is_some()
            || !service.drain_run_facts()
            || !service.local_request_ready()
        {
            break;
        }
        let Some(request) = service.local_requests.pop() else { break };
        service.route_local(now, request);
    }
    service.drain_terminal();
    service.start_captured();
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "kernel operations are external; only Cancel names another operation"
)]
fn namespace_submit(submit: kernel::Submit, bit: u64) -> kernel::Submit {
    assert!(submit.op.raw() & (ACCOUNT_OPERATION_BIT | EFFECT_OPERATION_BIT) == 0, "bounded operation namespace");
    let kind = match submit.kind {
        kernel::Op::Cancel { target } if target.raw() != u64::MAX => {
            assert!(
                target.raw() & (ACCOUNT_OPERATION_BIT | EFFECT_OPERATION_BIT) == 0,
                "lower cancel uses its own namespace"
            );
            kernel::Op::Cancel { target: Token::new(target.raw() | bit) }
        }
        other => other,
    };
    kernel::Submit { op: Token::new(submit.op.raw() | bit), kind }
}

fn to_below(event: agent::Event) -> inline::Below {
    match event {
        agent::Event::Completed { owner, completion } => inline::Below::Completed { owner, completion },
        agent::Event::Failed { owner, failure, evidence, detail, report } => {
            inline::Below::Failed { owner, failure, evidence, detail, report }
        }
        agent::Event::Cancelled { owner } => inline::Below::Cancelled { owner },
        agent::Event::Done { owner, done } => inline::Below::Done { owner, done },
        agent::Event::Read { owner, read } => inline::Below::Read { owner, read },
        agent::Event::Probed { owner, executable } => inline::Below::Probed { owner, executable },
        agent::Event::Checked { owner, ran } => inline::Below::Checked { owner, ran },
        agent::Event::Aborted { owner } => inline::Below::Aborted { owner },
        agent::Event::Start { .. }
        | agent::Event::Message { .. }
        | agent::Event::Cancel { .. }
        | agent::Event::Acknowledge { .. }
        | agent::Event::Grant { .. }
        | agent::Event::HostReturned { .. }
        | agent::Event::BuiltinResult { .. }
        | agent::Event::Delivered { .. } => unreachable!("effects emit only typed IO terminals"),
    }
}

/// Convert only the closed lower face into the shared component owner's request vocabulary.
fn to_root_request(request: inline::Lower) -> agent::Request {
    match request {
        inline::Lower::Complete {
            owner,
            name,
            grant,
            prompt,
            deadlines,
            max_completion_bytes,
            max_completion_blocks,
            max_failure_bytes,
            decoded_call_bytes,
        } => agent::Request::Complete {
            owner,
            name,
            grant,
            prompt,
            deadlines,
            max_completion_bytes,
            max_completion_blocks,
            max_failure_bytes,
            decoded_call_bytes,
        },
        inline::Lower::Cancel { owner } => agent::Request::Cancel { owner },
        inline::Lower::Io { owner, op, deadline } => agent::Request::Io { owner, op, deadline },
        inline::Lower::Job { owner, act } => agent::Request::Job { owner, act },
        inline::Lower::CancelIo { owner } => agent::Request::CancelIo { owner },
        inline::Lower::Read { owner, at, max, deadline } => agent::Request::Read { owner, at, max, deadline },
        inline::Lower::Probe { owner, at, deadline } => agent::Request::Probe { owner, at, deadline },
        inline::Lower::Check { owner, program, deadline, tail } => {
            agent::Request::Check { owner, program, deadline, tail }
        }
        inline::Lower::Abort { owner } => agent::Request::Abort { owner },
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "tests.rs"]
pub(crate) mod entry_tests;

#[cfg(test)]
mod live_tests;
