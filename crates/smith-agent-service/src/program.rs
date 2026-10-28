//! Program backend admission and lower routing (protocol/programs.md, 2, 3, 5).
//! Keeps the selected endpoints and pipe routes; never reads source logins.
//! The service calls these hooks at the same stages as its direct LLM component.

use super::{Config, ConfigError, Failure, Limits, Owner, Service, component_owner, credential};
use alloc::boxed::Box;
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::{Duration, Map, Queue, Secret, Token};
use smith_domain::{self as domain};
use smith_protocol_channel as channel;
use smith_protocol_llm as llm;
use smith_protocol_program::{Programs, backend};

const FIRST: Token = Token::new(u64::MAX - 1024);
const IO_SLOTS: u32 = 11;
const FILE_SLOTS: u32 = 3;

/// One configured program selected by the main account's source grant (section 3).
#[derive(Debug)]
pub struct ProgramBackend {
    pub program: Programs,
    pub endpoints: Box<[u32]>,
    pub relay: Box<[u8]>,
    pub entropy: Secret,
    /// Effective UID read by the shell before any private root is adopted.
    pub effective_user: u32,
}

/// The shell's programs root cannot be adopted by this service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdoptError {
    /// No endpoint names a program backend.
    Unconfigured,
    /// The checked file table has no free slot.
    Full,
}

#[derive(Debug)]
pub(super) struct Program {
    backend: backend::Backend,
    endpoints: Box<[u32]>,
    below: Queue<backend::Below>,
    pipes: Map<Token, Token>,
}

fn settings(
    config: &ProgramBackend,
    limits: &Limits,
    environment: &smith_protocol_machine::Environment,
) -> Option<backend::Settings> {
    let mut adapter = limits.llm.adapter;
    // Built-in results are skipped before rendering; only host text and
    // bounded root feedback use this MCP reply ceiling (programs.md 5.5).
    let result = limits
        .domain
        .session
        .delegated_result_bytes
        .max(u64::from(limits.domain.run.host_reply_bytes).checked_add(7)?)
        .max(u64::from(limits.domain.run.message_bytes))
        .max(4096)
        .checked_add(u64::from(llm::CUT_MARKER_BYTES))?;
    adapter.rendered_result = u32::try_from(result).ok()?;
    let contract_bytes = u32::try_from(limits.llm.contract_bytes).ok()?;
    // Host schemas count in run::charter::cost against run_bytes (domain/run.md, 5.2).
    // schemas_for_contract caps finish/deliver at contract_bytes; the fixed
    // inventory bounds every other generated schema (protocol/programs.md, 5.3).
    let schema_bytes = contract_bytes
        .max(u32::try_from(limits.domain.run.run_bytes).ok()?)
        .max(u32::try_from(llm::fixed_schema_bytes(&adapter.stated)?).ok()?);
    // A call's JSON input plus its RPC ID/name/envelope. This is a line
    // ceiling, not another allocation beside MCP's priced input buffers.
    let call_bytes = adapter.stated.call_input_bytes.checked_add(4096)?;
    // Each tool byte can become six JSON escape bytes in tools/list. Finish
    // and delivery schemas each have their own contract-sized encoded ceiling;
    // fixed room covers descriptions, names and the RPC/list envelope.
    let tool_bytes = u32::try_from(adapter.tool_bytes)
        .ok()?
        .checked_mul(6)?
        .checked_add(contract_bytes.checked_mul(2)?)?
        .checked_add(4096)?;
    // A result byte needs at most six JSON escape bytes. Tool-list and result
    // are alternative messages in the same MCP output slot, hence max, not sum.
    let answer_bytes = adapter.rendered_result.checked_mul(6)?.checked_add(4096)?.max(tool_bytes);
    // Stream JSON wraps semantic answer text/arguments (six-byte escapes),
    // plus message headers, IDs, usage and block envelopes.
    let line_bytes = adapter.client.answer.checked_mul(6)?.checked_add(65_536)?;
    Some(backend::Settings {
        program: Programs { claude: config.program.claude.clone(), directory: config.program.directory.clone() },
        relay: config.relay.clone(),
        directory: None,
        workspace: None,
        path_bytes: limits.domain.session.tools.host_path_bytes,
        environment: environment.command_entries(),
        mcp: smith_protocol_program::mcp::Limits {
            message_bytes: call_bytes,
            answer_bytes,
            schema_bytes,
            calls: u16::try_from(limits.domain.run.calls).ok()?,
            unauthenticated: skein_lib::Time::from_nanos(5_000_000_000),
        },
        adapter,
        receiving: limits.llm.receiving,
        stream_tokens: backend::Settings::stream_tokens(line_bytes),
        prompt_bytes: u64::from(adapter.client.request),
        contract_bytes,
        line_bytes,
        // client.request already bounds encoded JSON, including escaping and
        // all admitted history. This user envelope omits the model's schemas.
        input_bytes: adapter.client.request.checked_add(65_536)?,
        max_turns: 1,
        grace: limits.io.close_timeout,
        startup: Duration::from_secs(30),
    })
}

fn reserved(limits: &Limits, settings: &backend::Settings) -> Option<Limits> {
    let mut reserved = *limits;
    reserved.io.sockets = reserved.io.sockets.checked_add(IO_SLOTS)?;
    reserved.io.output = reserved.io.output.max(settings.mcp.answer_bytes);
    reserved.llm.connection.io.output = reserved.io.output;
    reserved.io.intake = reserved.io.intake.max(settings.mcp.message_bytes);
    reserved.llm.connection.io.intake = reserved.io.intake;
    reserved.file_slots = reserved.file_slots.checked_add(FILE_SLOTS)?;
    let operations = io::operations(&reserved.io)?;
    reserved.routes = reserved.routes.max(operations.checked_add(64)?);
    Some(reserved)
}

pub(crate) fn bound(config: Option<&ProgramBackend>) -> Option<&ProgramBackend> {
    match config {
        Some(program) if !program.endpoints.is_empty() => Some(program),
        Some(_) | None => None,
    }
}

/// Additional complete ownership charged only for a program an endpoint names.
/// Includes the protocol, queues, routes, file slots and native I/O growth (5.1).
#[must_use]
pub fn program_worst_case(config: &Config) -> Option<u64> {
    if !program_valid(config) {
        return None;
    }
    let program = match bound(config.program.as_ref()) {
        Some(program) => program,
        None => return Some(0),
    };
    let settings = settings(program, &config.limits, &config.environment)?;
    let reserved = reserved(&config.limits, &settings)?;
    let io_growth = super::worst_case(&reserved, &config.llm_endpoints)?
        .checked_sub(super::worst_case(&config.limits, &config.llm_endpoints)?)?;
    backend::backend_worst_case(&settings)?
        // Start directories are copied once; the policy borrows their host names.
        .checked_add(u64::from(config.limits.machine.roots).checked_mul(
            u64::try_from(size_of::<backend::Mount>()).ok()?.checked_add(u64::from(settings.path_bytes))?,
        )?)?
        .checked_add(io_growth)?
        // The MCP connection and program input are two additional writers.
        .checked_add(u64::from(reserved.io.output).checked_mul(2)?)?
        .checked_add(Map::<Token, Token>::worst_case(5)?)?
        .checked_add(u64::try_from(size_of::<Program>().checked_sub(size_of::<backend::Backend>())?).ok()?)?
        .checked_add(
            u64::try_from(size_of::<u32>())
                .ok()?
                .checked_mul(u64::try_from(program.endpoints.len()).ok()?)?
                .checked_mul(2)?,
        )
}

/// Whether a named program's settings and reserved service fit (programming-model.md, 6.3).
#[must_use]
pub fn program_valid(config: &Config) -> bool {
    let program = match bound(config.program.as_ref()) {
        Some(program) => program,
        None => return true,
    };
    let settings = match settings(program, &config.limits, &config.environment) {
        Some(settings) => settings,
        None => return false,
    };
    let reserved = match reserved(&config.limits, &settings) {
        Some(reserved) => reserved,
        None => return false,
    };
    backend::backend_valid(&settings)
        && super::valid(&config.limits, &config.llm_endpoints)
        && super::valid(&reserved, &config.llm_endpoints)
}

/// Native operation route capacity after reserving the configured program's I/O.
#[must_use]
pub fn program_operations(config: &Config) -> Option<u32> {
    match bound(config.program.as_ref()) {
        Some(program) => {
            Some(reserved(&config.limits, &settings(program, &config.limits, &config.environment)?)?.routes)
        }
        None => Some(config.limits.routes),
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "protocol/programs.md, section 5.1: the MCP command path must be JSON text"
)]
fn new(
    config: ProgramBackend,
    limits: &mut Limits,
    environment: &smith_protocol_machine::Environment,
) -> Result<Box<Program>, ConfigError> {
    let settings = settings(&config, limits, environment).ok_or(ConfigError::Limits)?;
    if config.entropy.len() != 32
        || !config.relay.starts_with(b"/")
        || config.relay.len() > 4096
        || config.relay.contains(&0)
        || core::str::from_utf8(&config.relay).is_err()
        || settings.line_bytes == 0
    {
        return Err(ConfigError::Io);
    }
    *limits = reserved(limits, &settings).ok_or(ConfigError::Limits)?;
    let process_slots = limits.machine.processes.checked_mul(3).ok_or(ConfigError::Limits)?;
    let minimum = limits
        .llm
        .connection
        .connections
        .checked_add(3)
        .ok_or(ConfigError::Limits)?
        .checked_add(process_slots)
        .ok_or(ConfigError::Limits)?
        .checked_add(IO_SLOTS)
        .ok_or(ConfigError::Limits)?;
    if limits.io.sockets < minimum
        || limits.file_slots < limits.machine.roots.checked_add(FILE_SLOTS).ok_or(ConfigError::Limits)?
    {
        return Err(ConfigError::Io);
    }
    Ok(Box::new(Program {
        backend: backend::Backend::new(FIRST, settings, &config.entropy),
        endpoints: config.endpoints,
        below: Queue::uncapped(),
        pipes: Map::with_capacity(5),
    }))
}

/// Native construction shared with `Service::parts`; private roots need the UID first.
#[derive(Debug)]
pub(super) struct Parts {
    pub program: Option<Box<Program>>,
    pub files: file_layer::FileIo,
}

/// Build and reserve only a program an endpoint names; private roots need its UID first.
pub(super) fn parts(
    config: Option<ProgramBackend>,
    limits: &mut Limits,
    environment: &smith_protocol_machine::Environment,
) -> Result<Parts, ConfigError> {
    let config = match bound(config.as_ref()) {
        Some(_) => config,
        None => None,
    };
    #[expect(clippy::manual_map, reason = "production steps use explicit matches instead of closures")]
    let effective_user = match &config {
        Some(program) => Some(program.effective_user),
        None => None,
    };
    let program = match config {
        Some(program) => Some(new(program, limits, environment)?),
        None => None,
    };
    let files = file_layer::FileIo::with_whole_limit(
        limits.file_slots,
        limits.file_read,
        limits.file_entries,
        limits.file_bytes,
        limits.file_timeout,
    );
    let files = match effective_user {
        Some(user) => files.with_effective_user(user),
        None => files,
    };
    Ok(Parts { program, files })
}

impl Service {
    /// Adopt the shell's private programs root, closing it with the backend (5.1).
    pub fn adopt_program_root(&mut self, descriptor: kernel::Fd) -> Result<(), AdoptError> {
        let program = self.program.as_mut().ok_or(AdoptError::Unconfigured)?;
        let file = self.files.adopt_root(descriptor).ok_or(AdoptError::Full)?;
        program.backend.directory(backend::Root { file, descriptor });
        Ok(())
    }
}

pub(super) fn install(
    service: &mut Service,
    start: &channel::DecodedStart,
    name: domain::GrantName,
    value: &channel::GrantValue,
    lapses: skein_lib::Time,
    workspace: Option<&domain::run::Workspace>,
) -> Result<(), ()> {
    match value {
        channel::GrantValue::Program(source) => {
            let program = service.program.as_mut().ok_or(())?;
            if start.charter.llm.account != Some(name.account)
                || !program.endpoints.contains(&start.charter.llm.endpoint.0)
            {
                return Err(());
            }
            if let Some(file) = service.roots.get(0) {
                let descriptor = service.files.descriptor(*file).ok_or(())?;
                program.backend.workspace(backend::Root { file: *file, descriptor });
            }
            let mut mounts = skein_lib::List::with_capacity(service.limits.machine.roots);
            if let Some(workspace) = workspace {
                for directory in &workspace.directories {
                    if directory.host.len()
                        > usize::try_from(service.limits.domain.session.tools.host_path_bytes).or(Err(()))?
                    {
                        return Err(());
                    }
                    mounts
                        .push(backend::Mount { host: directory.host.clone(), writable: directory.writable })
                        .or(Err(()))?;
                }
            }
            program.backend.mounts(mounts.into_boxed());
            program.backend.turn_limit(start.charter.budget.turns);
            program.backend.run_time(start.charter.budget.time);
            program.backend.grant(backend::Grant { name, source: source.clone(), lapses }).or(Err(()))
        }
        channel::GrantValue::Bearer(_) | channel::GrantValue::Key(_) | channel::GrantValue::Withdrawn { .. } => {
            if let Some(program) = &service.program {
                for model in core::iter::once(&start.charter.llm).chain(start.charter.models.iter()) {
                    if model.account == Some(name.account) && program.endpoints.contains(&model.endpoint.0) {
                        return Err(());
                    }
                }
            }
            service.llm.grant(name, credential(value).ok_or(())?, lapses).or(Err(()))
        }
    }
}

pub(super) fn renew(service: &mut Service, grant: domain::Grant, value: &channel::GrantValue) -> bool {
    let program = match &mut service.program {
        Some(program) if program.backend.holds(grant.name) => program,
        Some(_) | None => return false,
    };
    let accepted = match value {
        channel::GrantValue::Program(source) => program
            .backend
            .grant(backend::Grant {
                name: grant.name,
                source: source.clone(),
                lapses: service.domain_env.now.saturating_add(grant.valid),
            })
            .is_ok(),
        channel::GrantValue::Bearer(_) | channel::GrantValue::Key(_) | channel::GrantValue::Withdrawn { .. } => false,
    };
    if accepted {
        service.domain_events.push(domain::Event::Grant { grant });
    } else {
        service.mark_failed(Failure::Credential);
    }
    true
}

pub(super) fn complete(service: &mut Service, request: llm::FromDomain) {
    let use_program = match &request {
        llm::FromDomain::Complete { grant, prompt, .. } => {
            let main = match prompt.asker {
                domain::llm::Asker::Main => true,
                domain::llm::Asker::Child => false,
            };
            match grant {
                Some(name) if main => match &service.program {
                    Some(program) => program.backend.holds(*name),
                    None => false,
                },
                Some(_) | None => false,
            }
        }
        llm::FromDomain::Cancel { owner } | llm::FromDomain::Yield { owner } => match &service.program {
            Some(program) => program.backend.pending(*owner),
            None => false,
        },
    };
    if use_program {
        let program = service.program.as_mut().expect("installed program grant");
        program.backend.from_domain(&service.llm_env, request, &mut service.llm_events, &mut program.below);
    } else {
        service.llm.from_domain(&service.llm_env, request, &mut service.llm_events, &mut service.llm_below);
    }
}

pub(super) fn pop_measure(service: &mut Service) -> Option<llm::Measure> {
    match &mut service.program {
        Some(program) => program.backend.pop_measure(),
        None => None,
    }
}

pub(super) fn backlog(service: &Service) -> u32 {
    match &service.program {
        Some(program) => program.below.len(),
        None => 0,
    }
}

pub(super) fn fire(service: &mut Service) {
    if let Some(program) = &mut service.program {
        match service.teardown {
            super::Teardown::Live => {}
            super::Teardown::Answered { .. } | super::Teardown::Closing { .. } | super::Teardown::Closed { .. } => {
                program.backend.close(&service.llm_env, &mut service.llm_events, &mut program.below);
            }
        }
        program.backend.fire(&service.llm_env, &mut service.llm_events, &mut program.below);
    }
}

pub(super) fn deadline(service: &Service) -> Option<skein_lib::Time> {
    match &service.program {
        Some(program) => program.backend.next_deadline(),
        None => None,
    }
}

// After the domain's terminal, program deadlines only settle owned I/O
// (child close grace and MCP sockets); they do not await policy work.
pub(super) fn policy_deadline(service: &Service) -> Option<skein_lib::Time> {
    match service.teardown {
        super::Teardown::Live if answer_head(service) => None,
        super::Teardown::Live => deadline(service),
        super::Teardown::Answered { .. } | super::Teardown::Closing { .. } | super::Teardown::Closed { .. } => None,
    }
}

pub(super) fn closed(service: &Service) -> bool {
    match &service.program {
        Some(program) => program.backend.closed() && program.below.is_empty() && program.pipes.is_empty(),
        None => true,
    }
}

pub(super) fn pending(service: &Service) -> bool {
    match &service.program {
        Some(program) => !program.below.is_empty(),
        None => false,
    }
}

pub(super) fn file_event(service: &mut Service, event: file::Event) -> Option<file::Event> {
    let descriptor = match &event {
        file::Event::Opened { file, .. } => service.files.descriptor(*file),
        file::Event::Locked { .. }
        | file::Event::Busy { .. }
        | file::Event::Made { .. }
        | file::Event::Refused { .. }
        | file::Event::Loaded { .. }
        | file::Event::Scanned { .. }
        | file::Event::Stored { .. }
        | file::Event::Conflict { .. }
        | file::Event::TooLarge { .. }
        | file::Event::Stated { .. }
        | file::Event::Written { .. }
        | file::Event::Read { .. }
        | file::Event::Cut { .. }
        | file::Event::Synced { .. }
        | file::Event::Closed { .. }
        | file::Event::Renamed { .. }
        | file::Event::Removed { .. }
        | file::Event::Listed { .. }
        | file::Event::Failed { .. }
        | file::Event::Cancelled { .. } => None,
    };
    match &mut service.program {
        Some(program) if program.backend.owns(event.owner()) => {
            program.backend.from_io(
                &service.llm_env,
                backend::BelowEvent::File { event, descriptor },
                &mut service.llm_events,
                &mut program.below,
            );
            None
        }
        Some(_) | None => Some(event),
    }
}

pub(super) fn pipe_event(service: &mut Service, event: io::Event) -> Option<io::Event> {
    let owner = super::io_event_owner(&event)?;
    let program = match &mut service.program {
        Some(program) if program.pipes.contains_key(&owner) => program,
        Some(_) | None => return Some(event),
    };
    let closed = match &event {
        io::Event::Closed { .. } => true,
        io::Event::Listening { .. }
        | io::Event::Accepted { .. }
        | io::Event::Connecting { .. }
        | io::Event::Connected { .. }
        | io::Event::Stream { .. }
        | io::Event::Output { .. }
        | io::Event::Spawned { .. }
        | io::Event::Exited { .. }
        | io::Event::Usage { .. }
        | io::Event::Window { .. }
        | io::Event::Failed { .. }
        | io::Event::Resized
        | io::Event::Shutdown { .. } => false,
    };
    if closed {
        program.pipes.remove(&owner);
    }
    program.backend.from_io(
        &service.llm_env,
        backend::BelowEvent::Io(event),
        &mut service.llm_events,
        &mut program.below,
    );
    None
}

pub(super) fn io_event(service: &mut Service, event: io::Event) {
    let program = service.program.as_mut().expect("program route retains its backend");
    match &event {
        io::Event::Spawned { pipes, .. } => {
            for pipe in pipes {
                program.pipes.insert(*pipe, *pipe).expect("program's five pipe slots");
            }
        }
        io::Event::Listening { .. }
        | io::Event::Accepted { .. }
        | io::Event::Connecting { .. }
        | io::Event::Connected { .. }
        | io::Event::Stream { .. }
        | io::Event::Output { .. }
        | io::Event::Exited { .. }
        | io::Event::Usage { .. }
        | io::Event::Window { .. }
        | io::Event::Failed { .. }
        | io::Event::Closed { .. }
        | io::Event::Resized
        | io::Event::Shutdown { .. } => {}
    }
    program.backend.from_io(
        &service.llm_env,
        backend::BelowEvent::Io(event),
        &mut service.llm_events,
        &mut program.below,
    );
}

pub(super) fn down(service: &mut Service) {
    let program = match &mut service.program {
        Some(program) => program,
        None => return,
    };
    for _ in 0..service.limits.queue {
        let waits_for_file = match program.below.iter().next() {
            Some(backend::Below::File { .. }) => !service.files.takes(),
            Some(backend::Below::Io(_)) | None => false,
        };
        if waits_for_file {
            break;
        }
        let request = match program.below.pop() {
            Some(request) => request,
            None => break,
        };
        match request {
            backend::Below::File { request, deadline } => {
                service.files.seed_randomness(service.next_operation);
                file_layer::down_until(
                    &mut service.files,
                    deadline,
                    request,
                    &mut service.file_events,
                    &mut service.file_submissions,
                );
            }
            backend::Below::Io(request) => {
                let request = match request {
                    io::Request::Listen { owner, addr } => {
                        let route = service.owners.insert(Owner::Program(owner)).expect("program listener route");
                        io::Request::Listen { owner: component_owner(route.token()), addr }
                    }
                    io::Request::Bind { socket, owner } => {
                        let route = service.owners.insert(Owner::Program(owner)).expect("program MCP connection route");
                        io::Request::Bind { socket, owner: component_owner(route.token()) }
                    }
                    io::Request::Spawn { owner, spawn } => {
                        let route = service.owners.insert(Owner::Program(owner)).expect("program process route");
                        io::Request::Spawn { owner: component_owner(route.token()), spawn }
                    }
                    other @ (io::Request::Connect { .. }
                    | io::Request::Reject { .. }
                    | io::Request::Stream { .. }
                    | io::Request::Output { .. }
                    | io::Request::Signal { .. }
                    | io::Request::Window { .. }
                    | io::Request::Usage { .. }
                    | io::Request::Close { .. }
                    | io::Request::Abort { .. }) => other,
                };
                service.io_requests.push(request);
            }
        }
    }
}

// The queued Answer already owns and prices its payload. Hold that request
// until the program has acknowledged finish and settled its child/files.
fn answer_head(service: &Service) -> bool {
    match service.domain_requests.iter().next() {
        Some(domain::Request::Answer { .. }) => true,
        Some(
            domain::Request::Complete { .. }
            | domain::Request::Cancel { .. }
            | domain::Request::Yield { .. }
            | domain::Request::Io { .. }
            | domain::Request::Job { .. }
            | domain::Request::CancelIo { .. }
            | domain::Request::Read { .. }
            | domain::Request::Probe { .. }
            | domain::Request::Check { .. }
            | domain::Request::Abort { .. }
            | domain::Request::MessageRefused { .. }
            | domain::Request::Waiting { .. }
            | domain::Request::Turn { .. }
            | domain::Request::HostCall { .. }
            | domain::Request::WithdrawHost { .. }
            | domain::Request::Admitted { .. }
            | domain::Request::Long { .. }
            | domain::Request::LongDone { .. }
            | domain::Request::Deliver { .. }
            | domain::Request::Unauthorized { .. }
            | domain::Request::Refused { .. }
            | domain::Request::Limited { .. },
        )
        | None => false,
    }
}

pub(super) fn requests_ready(service: &Service) -> bool {
    if !answer_head(service) {
        return true;
    }
    match &service.program {
        Some(program) => program.backend.closed(),
        None => true,
    }
}

pub(super) fn settled(service: &mut Service, turn: &domain::Turn) {
    if let Some(program) = &mut service.program {
        program.backend.settled(&turn.messages, &mut program.below);
    }
}

pub(super) fn wait_for_close(service: &mut Service) -> bool {
    if !answer_head(service) {
        return false;
    }
    match &mut service.program {
        Some(program) if !program.backend.closed() => {
            program.backend.close(&service.llm_env, &mut service.llm_events, &mut program.below);
            !program.backend.closed()
        }
        Some(_) | None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unbound_program_keeps_the_native_limits() {
        let mut configuration = super::super::tests::configuration();
        let limits = configuration.limits;
        configuration.program = Some(ProgramBackend {
            program: Programs { claude: b"/claude".as_slice().into(), directory: b"/programs".as_slice().into() },
            endpoints: Box::default(),
            relay: Box::default(),
            entropy: Secret::new(Box::default()),
            effective_user: 1000,
        });
        let service = Service::new(configuration, 1).expect("unbound declaration needs no backend settings");
        assert!(service.program.is_none());
        assert_eq!(service.limits.routes, limits.routes);
        assert_eq!(service.limits.io, limits.io);
        assert_eq!(service.limits.file_slots, limits.file_slots);
    }

    #[test]
    fn program_close_reaches_io_with_pending_file_terminals() {
        let mut configuration = super::super::tests::configuration();
        configuration.program = Some(ProgramBackend {
            program: Programs { claude: b"/claude".as_slice().into(), directory: b"/programs".as_slice().into() },
            endpoints: Box::new([0]),
            relay: b"/agent".as_slice().into(),
            entropy: Secret::new(Box::new([7; 32])),
            effective_user: 1000,
        });
        let mut service = Service::new(configuration, 1).expect("bounded program service");
        let entity = Token::new(91);
        service
            .program
            .as_mut()
            .expect("configured program")
            .below
            .push(backend::Below::Io(io::Request::Close { entity }));
        service.file_events.push(file::Event::Closed { owner: Token::new(92) });
        assert_eq!(super::super::backlog(&service), 2, "program lower work counts toward admission");
        down(&mut service);
        let io::Request::Close { entity: received } = service.io_requests.pop().expect("program close progresses")
        else {
            unreachable!("only a close was queued")
        };
        assert_eq!(received, entity);
        assert!(service.program.as_ref().expect("configured program").below.is_empty());
        assert_eq!(service.file_events.len(), 1, "file terminals still await their consumer");
    }
}

#[cfg(test)]
mod validity_tests {
    use super::*;

    fn configuration() -> Config {
        let mut config = crate::service::tests::configuration();
        config.program = Some(ProgramBackend {
            program: Programs {
                claude: Box::from(b"/claude".as_slice()),
                directory: Box::from(b"/programs".as_slice()),
            },
            endpoints: Box::new([0]),
            relay: Box::from(b"/agent".as_slice()),
            entropy: Secret::new(Box::new([7; 32])),
            effective_user: 1000,
        });
        config
    }

    #[test]
    fn invalid_limits_are_refused() {
        assert!(program_valid(&crate::service::tests::configuration()));
        assert!(program_valid(&configuration()));
        for case in 0_u8..7 {
            let mut config = configuration();
            match case {
                0 => config.limits.llm.contract_bytes = u64::MAX,
                1 => config.limits.io.sockets = u32::MAX,
                2 => config.limits.file_slots = u32::MAX,
                3 => config.limits.domain.session.tools.host_path_bytes = u32::MAX,
                4 => config.limits.domain.run.conversations = u32::MAX,
                5 => config.limits.io.receive += 1,
                6 => config.limits.io.sockets = u32::MAX / 8 - 10,
                _ => unreachable!(),
            }
            assert!(!program_valid(&config), "invalid program case {case}");
        }
    }
}
