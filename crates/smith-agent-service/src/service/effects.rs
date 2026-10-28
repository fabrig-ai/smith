//! Host-owned effects over the service's shared lower stages.
//! It retains the service and installed grant names, never a root domain or
//! channel. Contract: domain/host.md, section 9; protocol/hosts.md, section 5.6.

use super::down::{component_down, domain_down};
use super::routing::{io_down, io_event_owner};
use super::up::{component_up, io_up, lower_events};
use super::{
    COMPONENT_OWNER_BIT, Config, ConfigError, Failure, Service, USAGE_OWNER, clocks, credential, done,
    effects_worst_case, next_deadline, teardown, work_pending,
};
use alloc::boxed::Box;
use skein_io::{self as io, kernel};
use skein_lib::{Id, Map, Queue, Time, Token, Wall};
use smith_domain::{self as domain, run};
use smith_protocol_channel as channel;

/// Agent LLM and machine effects owned by a host that keeps the domain itself.
/// Typed requests have typed terminals, with no channel, frames or agent process
/// (domain/host.md, section 9; protocol/hosts.md, section 5.6).
#[derive(Debug)]
pub struct Effects {
    service: Service,
    installed: Map<u32, domain::GrantName>,
}

impl Effects {
    /// Build the shared lower stages, retaining neither a root domain nor a channel.
    pub fn new(config: Config, _seed: u64) -> Result<Self, ConfigError> {
        let accounts = config.limits.llm.accounts;
        if !config.limits.io.is_usable() || config.limits.file_slots < config.limits.machine.roots {
            return Err(ConfigError::Queue);
        }
        if effects_worst_case(&config.limits, &config.llm_endpoints).ok_or(ConfigError::Memory)? > config.limits.memory
        {
            return Err(ConfigError::MemoryCeiling);
        }
        let service = Service::parts(config, None)?;
        Ok(Self { service, installed: Map::with_capacity(accounts) })
    }

    /// Replace the account-to-endpoint bindings before a colocated host lends grants.
    pub fn bind_grants(&mut self, charter: &run::Charter) -> Result<(), Failure> {
        self.service.llm.bind_grants(charter).or(Err(Failure::Credential))
    }

    /// Configure the result shapes used by completion tool schemas.
    pub fn contract(&mut self, outcome: run::outcome::OutcomeSpec) {
        self.service.delivery.clone_from(&outcome.change);
        self.service.outcome = Some(outcome);
    }

    /// Adopt opened workspace roots and replace the host's opaque names with file tokens.
    pub fn workspace(
        &mut self,
        mut workspace: run::Workspace,
        roots: Box<[kernel::Fd]>,
    ) -> Result<run::Workspace, ConfigError> {
        if workspace.directories.len() != roots.len()
            || roots.len() > usize::try_from(self.service.limits.machine.roots).expect("root bound fits")
        {
            return Err(ConfigError::Io);
        }
        for (directory, fd) in workspace.directories.iter_mut().zip(&roots) {
            let root = self.service.files.adopt_root(*fd).ok_or(ConfigError::Io)?;
            self.service.roots.push(root).expect("admitted root count");
            directory.root = root;
        }
        self.service.root_close_next = self.service.roots.len();
        self.service.machine.workspace(&workspace);
        Ok(workspace)
    }

    /// Install a grant's tagged value before supplying the grant to the domain.
    pub fn grant(&mut self, grant: domain::Grant, value: &channel::GrantValue, now: Time) -> Result<(), Failure> {
        if self.installed.get(&grant.name.account) == Some(&grant.name) {
            return Ok(());
        }
        let credential = credential(value).ok_or(Failure::Credential)?;
        match self.service.llm.grant(grant.name, credential, now.saturating_add(grant.valid)) {
            Ok(()) => {
                self.installed.insert(grant.name.account, grant.name).expect("admitted account count");
                Ok(())
            }
            Err(_) => Err(Failure::Credential),
        }
    }

    /// Room in the uncapped typed-request queue for the owning host.
    #[must_use]
    pub fn request_room(&self) -> u32 {
        self.service.domain_requests.room()
    }

    /// Queue one effect emitted by the host's in-process agent domain.
    pub fn request(&mut self, request: domain::Request) {
        match request {
            request @ (domain::Request::Complete { .. }
            | domain::Request::Cancel { .. }
            | domain::Request::Yield { .. }
            | domain::Request::Io { .. }
            | domain::Request::Job { .. }
            | domain::Request::CancelIo { .. }
            | domain::Request::Read { .. }
            | domain::Request::Probe { .. }
            | domain::Request::Check { .. }
            | domain::Request::Abort { .. }) => self.service.domain_requests.push(request),
            domain::Request::MessageRefused { .. }
            | domain::Request::Waiting { .. }
            | domain::Request::Turn { .. }
            | domain::Request::HostCall { .. }
            | domain::Request::WithdrawHost { .. }
            | domain::Request::Admitted { .. }
            | domain::Request::Answer { .. }
            | domain::Request::Long { .. }
            | domain::Request::LongDone { .. }
            | domain::Request::Deliver { .. }
            | domain::Request::Unauthorized { .. }
            | domain::Request::Refused { .. }
            | domain::Request::Limited { .. } => unreachable!("local domain owns host requests"),
        }
    }

    /// Take one typed LLM or machine terminal for the host's domain.
    pub fn event(&mut self) -> Option<domain::Event> {
        self.service.domain_events.pop()
    }

    /// Advance the same lower stages used by a framed agent at injected clocks.
    pub fn iterate(&mut self, now: Time, wall: Wall) {
        self.up(now, wall);
        self.down();
    }

    /// Reap IO and translate its settled terminals before the host's domain pass.
    pub fn up(&mut self, now: Time, wall: Wall) {
        let service = &mut self.service;
        clocks(service, now, wall);
        io_up(service);
        lower_events(service);
        component_up(service);
    }

    /// Submit domain effects and reclaim lower entities after the host's down pass.
    pub fn down(&mut self) {
        let service = &mut self.service;
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
        service.owners.reclaim();
    }

    /// Completions supplied by the shell's kernel or a world's simulator.
    pub fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        self.service.completions()
    }

    /// Kernel work issued by the shared lower stages.
    pub fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.service.submissions()
    }

    /// The next deadline belonging to an LLM, machine or IO entity.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        next_deadline(&self.service)
    }

    /// Whether another lower pass can progress without waiting.
    #[must_use]
    pub fn work_pending(&self, now: Time) -> bool {
        work_pending(&self.service, now)
    }

    /// Entries in the effects' own handoffs for the colocated host's
    /// backlog (protocol/limits.md 3.8).
    #[must_use]
    pub fn backlog(&self) -> u64 {
        super::backlog(&self.service)
    }

    /// Adopt the product's append descriptor under its configured delivery policy.
    pub fn adopt_destination(
        &mut self,
        fd: kernel::Fd,
        destination: crate::Destination,
        delivery: crate::sinks::Delivery,
        append: bool,
        io: &mut io::Io,
    ) -> Result<(), kernel::Fd> {
        let service = &mut self.service;
        let Some(events) = &mut service.events else { return Err(fd) };
        if !events.sinks.room_for_destination(destination) {
            return Err(fd);
        }
        let deadline = service.limits.events.expect("configured producer").write_deadline;
        let token = if append { io.adopt_append(fd, deadline)? } else { io.adopt_write_pipe(fd, Some(deadline))? };
        events.sinks.add(destination, token, delivery).expect("destination room checked");
        Ok(())
    }

    /// Requests for the product's one IO owner.
    pub fn io_requests(&mut self) -> &mut Queue<io::Request> {
        &mut self.service.io_requests
    }

    /// Room for one terminal from the common IO owner.
    #[must_use]
    pub fn io_takes(&self) -> bool {
        self.service.io_events.room() > 0
    }

    /// A native IO terminal routed by the product to these effects.
    pub fn io_event(&mut self, event: io::Event) {
        self.service.io_events.push(event);
    }

    /// Whether an IO terminal names a live effect or destination.
    #[must_use]
    pub fn owns_io(&self, event: &io::Event) -> bool {
        let service = &self.service;
        let Some(owner) = io_event_owner(event) else { return false };
        if owner == USAGE_OWNER || service.pipe_routes.contains_key(&owner) {
            return true;
        }
        if let Some(events) = &service.events
            && events.sinks.owns(owner)
        {
            return true;
        }
        owner.raw() & COMPONENT_OWNER_BIT != 0
            && service.owners.get(Id::from_token(Token::new(owner.raw() & !COMPONENT_OWNER_BIT))).is_some()
    }

    /// Open destinations retained while the common owner closes its other entities.
    #[must_use]
    pub fn streams(&self) -> u32 {
        match &self.service.events {
            Some(events) => events.sinks.open_streams(),
            None => 0,
        }
    }

    /// Record the product's startup before its first pass.
    pub fn start_session(&mut self, now: Time, header: smith_events::SessionStarted) {
        self.service.start_session(now, header);
    }

    /// Retain the values supplied by the parent face's start.
    pub fn starting(&mut self, activation: u64, messages: u64, charter: &run::Charter) {
        self.service.llm.bind_grants(charter).expect("admitted charter fits its LLM binding bound");
        self.service.run_started = self.service.domain_env.now;
        if let Some(events) = &mut self.service.events {
            events.starting(activation, messages, charter);
        }
    }

    /// Capture selected by the product's event configuration.
    #[must_use]
    pub fn capture(&self) -> crate::sinks::Capture {
        match self.service.limits.events {
            Some(events) => events.capture,
            None => crate::sinks::Capture::None,
        }
    }

    /// The existing event sink's optional in-process view, with no destination credit.
    pub fn front_sinks(&mut self) -> Option<&mut crate::sinks::Sinks> {
        match &mut self.service.events {
            Some(events) => Some(&mut events.sinks),
            None => None,
        }
    }

    /// A live-view record awaits its product front end.
    #[must_use]
    pub fn front_pending(&self) -> bool {
        match &self.service.events {
            Some(events) => events.sinks.front_pending(),
            None => false,
        }
    }

    /// Whether the destinations can take another record this pass.
    #[must_use]
    pub fn takes(&self) -> bool {
        match &self.service.events {
            Some(events) => events.sinks.takes(),
            None => true,
        }
    }

    /// Observe the exact parent message before draining its run receipt.
    pub fn message(&mut self, name: Token, label: &[u8], text: &[u8]) {
        if let Some(events) = &mut self.service.events {
            events.message(name, label, text);
        }
    }

    /// Project one inline run fact in its native drain order.
    pub fn fact(&mut self, fact: &domain::Fact) {
        if let Some(events) = &mut self.service.events {
            events.fact(fact);
        }
    }

    /// Finish any opening whose root terminal followed its last session fact.
    pub fn facts_drained(&mut self) {
        if let Some(events) = &mut self.service.events {
            for _ in 0..self.service.limits.domain.run.conversations {
                if !events.sinks.takes() || !events.sinks.facts_drained(events.origin()) {
                    break;
                }
            }
        }
    }

    /// Record the answer where the product meets it, with its producer's totals.
    pub fn answered(&mut self, now: Time, answer: crate::Answered) {
        if let Some(events) = &mut self.service.events {
            events.answered(now, self.service.run_started, answer);
        }
    }

    /// End the stream with the product's exit after its children settle.
    pub fn end(&mut self, exit: u8) {
        self.service.product_exit = Some(exit);
        teardown::answered(&mut self.service);
    }

    /// Close reusable lower entities after the local domain has ended its run.
    pub fn close(&mut self) {
        teardown::answered(&mut self.service);
    }

    /// Lower components and roots have closed; the product terminal is still owed.
    #[must_use]
    pub fn components_closed(&self) -> bool {
        teardown::components_closed(&self.service)
    }

    /// Turn the remaining lower closes into aborts after the product's last word.
    pub fn abort(&mut self) {
        teardown::shutdown(&mut self.service);
    }

    /// Every lower child and adopted root has completed its close.
    #[must_use]
    pub fn closed(&self) -> bool {
        done(&self.service).is_some()
    }
}
