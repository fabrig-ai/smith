//! Shared agent startup and shell pass (protocol/agent.md, sections 4–6).
//! It keeps the service, startup header, inherited mount descriptors and error
//! output. It never keeps credentials outside the service or knows the driving
//! kernel, simulator or neighbours. `Agent::read` reads startup settings;
//! `Agent::new` adopts caller resources; `Host::iterate` runs the same pass in
//! the binary and worlds. `run` supplies the binary's kernel and clock.

use std::io::Write;
#[cfg(unix)]
use std::path::Path;

use service::profile::{Layer, LimitName, Refusal};
use skein_io::kernel::{Complete, Exit, Fd, Op, Submit};
use skein_lib::{List, Queue, Secret, Time, Token, Wall};
use skein_shell::Host;
#[cfg(unix)]
use skein_shell::{Clock, Config as KernelConfig, Kernel, drive};
use smith_agent_service as service;

use crate::config;

/// Program resources supplied by the binary or a hosted process world (programs.md, 5.1).
pub struct ProgramResources {
    pub relay: std::path::PathBuf,
    pub entropy: Secret,
    pub directory: Option<Fd>,
}

/// Channel and signal descriptors handed by the caller to one agent invocation.
pub struct Resources {
    pub input: Fd,
    pub output: Fd,
    pub signals: Fd,
    pub seed: u64,
    /// The process identity read by startup, or supplied by a simulated process.
    pub pid: u32,
    /// An already opened append descriptor for the configured trace, when the
    /// caller supplies startup effects. The invocation owns its close.
    pub trace: Option<Fd>,
    /// An inherited stderr descriptor whose close the invocation owns; the
    /// supplied writer owns the diagnostic bytes.
    pub error: Option<Fd>,
    /// When present, roots are adopted in Start order; unused roots close at
    /// the invocation's end instead of returning to the caller.
    pub roots: Option<Box<[Fd]>>,
    pub programs: Option<ProgramResources>,
}

/// The binary's agent and shell effects, hosted by a kernel or world until settlement.
pub struct Agent {
    service: service::Service,
    header: Option<smith_events::SessionStarted>,
    trace_root: Option<Fd>,
    roots: Option<std::vec::IntoIter<Fd>>,
    errors: Box<dyn Write>,
    error: Option<Fd>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    closing: u32,
    result: Option<Result<(), String>>,
    memory: u64,
    started: bool,
    reported: bool,
    worst: u64,
    operations: u32,
}

struct Prepared {
    service: service::Service,
    header: Option<smith_events::SessionStarted>,
    trace_root: Option<Fd>,
    unused_program_root: Option<Fd>,
    worst: u64,
    operations: u32,
    queue: u32,
}

impl Agent {
    /// Read the binary's configuration and build the same invocation worlds host.
    /// Every startup refusal is written once to the supplied error output.
    #[cfg(unix)]
    pub fn read(path: &Path, resources: Resources, mut errors: Box<dyn Write>) -> Result<Self, String> {
        let configuration = read_configuration(path, errors.as_mut())?;
        Self::new(configuration, resources, errors)
    }

    /// Build a configured service and adopt its streams; `drain` announces startup.
    /// Worlds inject their clock through `iterate` and deterministic resource seeds.
    pub fn new(
        configuration: config::Configuration,
        resources: Resources,
        mut errors: Box<dyn Write>,
    ) -> Result<Self, String> {
        let memory = configuration.memory;
        let prepared = match prepare(configuration, &resources) {
            Ok(prepared) => prepared,
            Err(why) => {
                diagnostic(errors.as_mut(), &why);
                return Err(why);
            }
        };
        let mut agent = Self {
            service: prepared.service,
            header: prepared.header,
            trace_root: prepared.trace_root,
            roots: resources.roots.map(|roots| roots.into_vec().into_iter()),
            errors,
            error: resources.error,
            completions: Queue::with_capacity(prepared.queue),
            submissions: Queue::with_capacity(prepared.queue),
            closing: 0,
            result: None,
            memory,
            started: false,
            reported: false,
            worst: prepared.worst,
            operations: prepared.operations,
        };
        if let Some(root) = prepared.unused_program_root {
            agent.close(root);
        }
        Ok(agent)
    }

    /// The settled invocation's success or failure, already logged to error output.
    #[must_use]
    pub fn result(&self) -> Option<Result<(), &str>> {
        if self.is_empty() {
            self.result.as_ref().map(|result| result.as_ref().copied().map_err(String::as_str))
        } else {
            None
        }
    }

    fn close(&mut self, fd: Fd) {
        self.closing = self.closing.checked_add(1).expect("bounded inherited closes");
        self.submissions.push(Submit {
            op: Token::new(u64::MAX.checked_sub(u64::from(self.closing)).expect("bounded close token")),
            kind: Op::Close { fd },
        });
    }

    fn finish(&mut self, answered: bool) {
        self.result = Some(if answered {
            Ok(())
        } else {
            let why = match service::failure(&self.service).expect("an unanswered agent keeps its first failure") {
                service::Failure::ChannelEnded => "the channel ended before the answer",
                service::Failure::WriteFailed => "the channel's output failed",
                service::Failure::Credential => "a host credential could not be installed",
                service::Failure::Send => "a required host record could not be sent",
            };
            Err(format!("the run could not answer: {why}"))
        });
    }
}

fn effective_header(configuration: &config::Configuration) -> Result<List<smith_events::EffectiveLimit>, String> {
    let named = service::profile::effective(
        &configuration.profile,
        &configuration.declarations,
        &configuration.service.limits,
        &configuration.service.llm_endpoints,
    )
    .ok_or_else(|| derivation_refusal("effective"))?;
    let header_limits =
        service::profile::header(&named, &configuration.service.channel_endpoints, &configuration.declarations)
            .ok_or_else(|| derivation_refusal("effective_names"))?;
    Ok(header_limits)
}

#[cfg(unix)]
fn prepare_program(configuration: &mut service::Config, resources: Option<&ProgramResources>) -> Result<(), String> {
    if let Some(program) = &mut configuration.program {
        let resources = resources.ok_or("a configured program requires its shell resources")?;
        program.relay = resources.relay.as_os_str().as_encoded_bytes().into();
        if program.relay.is_empty() || program.relay.len() > 4096 || program.relay.contains(&0) {
            return Err("the program relay must have a bounded executable path".into());
        }
        if resources.entropy.len() != 32 {
            return Err("program entropy must have 32 bytes".into());
        }
        program.entropy = Secret::new(resources.entropy.expose().into());
    } else if resources.is_some() {
        return Err("program resources require a configured program".into());
    }
    Ok(())
}

#[cfg(unix)]
fn adopt_program_root(service: &mut service::Service, resources: &Resources) -> Result<Option<Fd>, String> {
    if let Some(root) = resources.programs.as_ref().and_then(|programs| programs.directory) {
        match service.adopt_program_root(root) {
            Ok(()) => {}
            Err(service::AdoptError::Unconfigured) => return Ok(Some(root)),
            Err(error @ service::AdoptError::Full) => {
                return Err(format!("programs root cannot be adopted: {error:?}"));
            }
        }
    }
    Ok(None)
}

#[cfg(not(unix))]
fn validate_hosted(configuration: &config::Configuration, resources: &Resources) -> Result<(), String> {
    if resources.roots.is_none() {
        return Err("a hosted agent's roots are its host's to supply".into());
    }
    if let Some(trace) = &configuration.trace
        && resources.trace.is_none()
    {
        return Err(format!("the trace at {} is not supplied by the host", trace.path.display()));
    }
    if resources.programs.is_some() || !configuration.programs.is_empty() || configuration.service.program.is_some() {
        return Err("program claude_code requires a native shell".into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn prepare_program(configuration: &mut service::Config, resources: Option<&ProgramResources>) -> Result<(), String> {
    if configuration.program.is_some() || resources.is_some() {
        return Err("program claude_code requires a native shell".into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn adopt_program_root(_service: &mut service::Service, _resources: &Resources) -> Result<Option<Fd>, String> {
    Ok(None)
}

#[cfg(unix)]
fn open_trace(path: &Path) -> Result<(Fd, Fd), String> {
    let path = std::path::absolute(path).map_err(|error| format!("trace path: {error}"))?;
    let parent = path.parent().ok_or("trace path has no parent")?;
    let file = path.file_name().ok_or("trace path has no filename")?;
    let root = skein_shell::open_root(parent).map_err(|error| format!("trace root: {error:?}"))?;
    let fd = skein_shell::open_append(root, file.as_encoded_bytes(), 0o600)
        .map_err(|error| format!("trace append: {error:?}"))?;
    Ok((fd, root))
}

#[cfg(not(unix))]
fn open_trace(path: &std::path::Path) -> Result<(Fd, Fd), String> {
    Err(format!("the trace at {} is not supplied by the host", path.display()))
}

#[cfg(unix)]
fn open_root(path: &[u8], maximum: u32) -> (Result<Fd, ()>, Box<[u8]>) {
    let host = std::str::from_utf8(path)
        .ok()
        .and_then(|path| std::path::absolute(Path::new(path)).ok())
        .and_then(|path| smith_protocol_channel::host_path(path.as_os_str().as_encoded_bytes(), maximum));
    let Some(host) = host else {
        return (Err(()), Box::new([]));
    };
    let root = match std::str::from_utf8(path) {
        Ok(path) => skein_shell::open_root(Path::new(path)).map_err(|_| ()),
        Err(_) => Err(()),
    };
    (root, host)
}

#[cfg(not(unix))]
fn open_root(_path: &[u8], _maximum: u32) -> (Result<Fd, ()>, Box<[u8]>) {
    (Err(()), Box::new([]))
}

fn extra_operations(configuration: &config::Configuration, resources: &Resources, root_count: usize) -> Option<u32> {
    u32::try_from(root_count)
        .ok()?
        .checked_add(u32::from(resources.error.is_some()))
        .and_then(|count| count.checked_add(u32::from(configuration.trace.is_some() && resources.trace.is_none())))
        .and_then(|count| {
            // An unbound program leaves one inherited root for the shell to close.
            count.checked_add(u32::from(
                configuration.endpoint_programs.is_empty()
                    && resources.programs.as_ref().and_then(|programs| programs.directory).is_some(),
            ))
        })
}

fn prepare(mut configuration: config::Configuration, resources: &Resources) -> Result<Prepared, String> {
    #[cfg(not(unix))]
    validate_hosted(&configuration, resources)?;
    prepare_program(&mut configuration.service, resources.programs.as_ref())?;
    let root_count = resources.roots.as_ref().map_or(0, |roots| roots.len());
    if root_count > usize::try_from(configuration.service.limits.domain.run.directories).expect("directory bound") {
        return Err("inherited agent roots exceed the directory limit".into());
    }
    let extra_operations =
        extra_operations(&configuration, resources, root_count).ok_or_else(|| derivation_refusal("routes"))?;
    let queue =
        configuration.service.limits.queue.checked_add(extra_operations).ok_or_else(|| derivation_refusal("queue"))?;
    let operations = service::program_operations(&configuration.service)
        .ok_or_else(|| derivation_refusal("program_routes"))?
        .checked_add(extra_operations)
        .ok_or_else(|| derivation_refusal("routes"))?;
    let header_limits = effective_header(&configuration)?;
    let mut header_bytes = List::<smith_events::EffectiveLimit>::worst_case(header_limits.len())
        .ok_or_else(|| derivation_refusal("startup_header"))?;
    for entry in &header_limits {
        header_bytes = header_bytes
            .checked_add(u64::try_from(entry.name.len()).map_err(|_| derivation_refusal("startup_header"))?)
            .ok_or_else(|| derivation_refusal("startup_header"))?;
    }
    let worst = service::worst_case(&configuration.service.limits, &configuration.service.llm_endpoints)
        .and_then(|bytes| bytes.checked_add(service::program_worst_case(&configuration.service)?))
        .and_then(|bytes| {
            bytes.checked_add(if configuration.trace.is_some() {
                // The retained startup header's three strings; its structure is in Agent.
                u64::try_from(
                    b"smith".len().checked_add(env!("CARGO_PKG_VERSION").len())?.checked_add(b"standard".len())?,
                )
                .ok()?
                .checked_add(header_bytes)?
            } else {
                0
            })
        })
        .and_then(|bytes| bytes.checked_add(Queue::<Complete>::worst_case(queue)?))
        .and_then(|bytes| bytes.checked_add(Queue::<Submit>::worst_case(queue)?))
        .and_then(|bytes| bytes.checked_add(u64::try_from(size_of::<Agent>()).ok()?))
        .and_then(|bytes| bytes.checked_add(u64::try_from(root_count.checked_mul(size_of::<Fd>())?).ok()?))
        .ok_or_else(|| derivation_refusal("worst_case"))?;
    service::profile::validate::memory(worst, configuration.memory).map_err(config::profile_refusal)?;
    let profile = match configuration.profile.name {
        service::profile::Name::Standard => b"standard".as_slice(),
    };
    let header = configuration.trace.as_ref().map(|trace| smith_events::SessionStarted {
        chat: None,
        wall_ms: 0,
        agent: smith_events::Agent {
            name: b"smith".as_slice().into(),
            version: env!("CARGO_PKG_VERSION").as_bytes().into(),
        },
        mode: smith_events::Mode::Agent,
        containment: smith_events::Containment::Groups,
        pid: u64::from(resources.pid),
        profile: profile.into(),
        capture: trace.capture.encoded(),
        delivery: trace.delivery.encoded(),
        versions: smith_events::Versions {
            events: smith_events::VERSION,
            channel: 5,
            charter: 4,
            transcript: 6,
            chat_store: None,
        },
        limits: header_limits,
    });
    let mut service = service::Service::new(configuration.service, resources.seed)
        .map_err(|error| format!("agent configuration cannot start: {error:?}"))?;
    let unused_program_root = adopt_program_root(&mut service, resources)?;
    service
        .adopt_streams(resources.input, resources.output, resources.signals)
        .map_err(|fd| format!("agent stream {} cannot be adopted", fd.raw()))?;
    let mut trace_root = None;
    if let Some(trace) = configuration.trace {
        let fd = if let Some(fd) = resources.trace {
            fd
        } else {
            let (fd, root) = open_trace(&trace.path)?;
            trace_root = Some(root);
            fd
        };
        service
            .adopt_destination(fd, service::Destination::Trace, trace.delivery, true)
            .map_err(|fd| format!("trace descriptor {} cannot be adopted", fd.raw()))?;
    } else if resources.trace.is_some() {
        return Err("an inherited trace requires a configured destination".into());
    }
    Ok(Prepared { service, header, trace_root, unused_program_root, worst, operations, queue })
}

impl Host for Agent {
    fn drain(&mut self) {
        if !self.started {
            self.started = true;
            diagnostic(self.errors.as_mut(), "commands run in process groups and are not contained");
            diagnostic(
                self.errors.as_mut(),
                &format!("agent started; worst case {} of {} configured bytes", self.worst, self.memory),
            );
        }
        if self.result.is_some() && !self.reported {
            self.reported = true;
            if let Some(Err(why)) = &self.result {
                diagnostic(self.errors.as_mut(), why);
            }
            match service::last(&self.service) {
                Some(service::Last::Failed { failure, detail }) => {
                    let words = smith_events::words(&failure, None, None);
                    let mut line = format!(
                        "run failed: {}",
                        String::from_utf8(words.into_vec()).expect("operator words are UTF-8")
                    );
                    let detail = smith_events::clip(&detail);
                    if !detail.is_empty() {
                        line.push_str(": ");
                        line.push_str(core::str::from_utf8(detail).expect("clipped detail is UTF-8"));
                    }
                    diagnostic(self.errors.as_mut(), &line);
                }
                Some(service::Last::Quiet | service::Last::Unanswered(_)) | None => {}
            }
        }
    }

    fn iterate(&mut self, now: Time, wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            if complete.op.raw() >= u64::MAX.saturating_sub(u64::from(self.operations)) {
                assert!(complete.result.is_ok(), "inherited descriptor closes");
                self.closing = self.closing.checked_sub(1).expect("submitted inherited close");
            } else {
                self.service.completions().push(complete);
            }
        }
        if self.result.is_some() {
            return;
        }
        if let Some(mut header) = self.header.take() {
            header.wall_ms = wall.as_nanos() / 1_000_000;
            self.service.start_session(now, header);
        }
        if let Some(root) = self.trace_root.take() {
            self.close(root);
        }
        service::iterate(&mut self.service, now, wall);
        while let Some(given_up) = self.service.pop_given_up() {
            diagnostic(self.errors.as_mut(), &given_up_line(given_up));
        }
        if let Some(overflow) = self.service.pop_accounting_overflow() {
            let line = match overflow {
                smith_domain::run::Overflow::Spend => "run accounting failed: spend overflow",
                smith_domain::run::Overflow::Usage => {
                    "run accounting failed: usage overflow; overflowing reported totals are unavailable"
                }
            };
            diagnostic(self.errors.as_mut(), line);
        }
        if self.service.pop_usage_unread() {
            diagnostic(self.errors.as_mut(), "process resource usage is unavailable");
        }
        while let Some(path) = self.service.root_to_open() {
            let (root, host) = match &mut self.roots {
                Some(roots) => {
                    let Some(host) = smith_protocol_channel::host_path(path, self.service.host_path_bytes()) else {
                        self.service.root_opened(Err(()), Box::new([]));
                        continue;
                    };
                    (roots.next().ok_or(()), host)
                }
                None => open_root(path, self.service.host_path_bytes()),
            };
            self.service.root_opened(root, host);
        }
        if let Some(answered) = service::done(&self.service) {
            self.finish(answered);
            while let Some(fd) = self.roots.as_mut().and_then(Iterator::next) {
                self.close(fd);
            }
            if let Some(fd) = self.error.take() {
                self.close(fd);
            }
        }
        while let Some(submission) = self.service.submissions().pop() {
            self.submissions.push(submission);
        }
    }

    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }

    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }

    fn work_pending(&self, now: Time) -> bool {
        (self.result.is_none() && service::work_pending(&self.service, now))
            || !self.completions.is_empty()
            || !self.submissions.is_empty()
    }

    fn next_deadline(&self) -> Option<Time> {
        if self.result.is_none() { service::next_deadline(&self.service) } else { None }
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        if self.result.is_none() { service::next_policy_deadline(&self.service) } else { None }
    }

    fn is_empty(&self) -> bool {
        self.result.is_some() && self.closing == 0 && self.completions.is_empty() && self.submissions.is_empty()
    }

    fn exit(&self) -> Option<Exit> {
        self.result().map(|result| Exit::Code(u8::from(result.is_err())))
    }

    fn worst_case(&self) -> u64 {
        self.worst
    }

    fn operations(&self) -> u32 {
        self.operations
    }
}

#[cfg(unix)]
fn read_configuration(path: &Path, errors: &mut dyn Write) -> Result<config::Configuration, String> {
    config::read(path).inspect_err(|why| diagnostic(errors, why))
}

/// The operator's line for one destination IO gave up, naming it and why in
/// IO's terms; every shell prints it once per destination (protocol/events.md,
/// section 5.2).
#[must_use]
pub fn given_up_line(given_up: service::GivenUp) -> String {
    let name = match given_up.destination {
        service::Destination::Trace => "trace file",
        service::Destination::Output => "standard output events",
    };
    let why = match given_up.why {
        service::Why::Behind => "it stayed behind past the write deadline",
        service::Why::ReaderGone => "its reader went away",
        service::Why::WriteFailed => "a write failed",
    };
    format!("event destination {name} was given up: {why}; later records count as lost")
}

fn diagnostic(errors: &mut dyn Write, line: &str) {
    // Diagnostics are best effort, as trace records are; a broken operator
    // output cannot prevent the channel's answer or its lower settlement.
    drop(writeln!(errors, "smith: {line}"));
}

/// Run the shipped agent command; startup and final failures are already logged.
#[cfg(unix)]
pub fn run(path: &Path, mut errors: Box<dyn Write>) -> Result<Exit, String> {
    let configuration = read_configuration(path, errors.as_mut())?;
    let seed = skein_shell::seed()
        .map_err(|errno| format!("kernel seed failed (errno {errno})"))
        .inspect_err(|why| diagnostic(errors.as_mut(), why))?;
    let signals = skein_shell::open_termination_signals()
        .map_err(|errno| format!("termination signals cannot open (errno {errno})"))
        .inspect_err(|why| diagnostic(errors.as_mut(), why))?;
    let programs = match configuration.programs.get(&config::ProgramName::ClaudeCode) {
        Some(program) => Some(ProgramResources {
            relay: std::env::current_exe().map_err(|error| format!("program relay: {error}"))?,
            entropy: skein_shell::crypto_seed().map_err(|error| format!("program entropy: {error}"))?,
            directory: if crate::program::private_directory(&program.directory).map_err(|error| error.to_string())? {
                Some(skein_shell::open_root(&program.directory).map_err(|error| format!("programs root: {error:?}"))?)
            } else {
                None
            },
        }),
        None => None,
    };
    let mut agent = Agent::new(
        configuration,
        Resources {
            input: Fd::new(0),
            output: Fd::new(1),
            signals,
            seed,
            pid: skein_shell::process_id(),
            trace: None,
            error: None,
            roots: None,
            programs,
        },
        errors,
    )?;
    let mut kernel = Kernel::open(KernelConfig { operations: agent.operations() })
        .map_err(|error| format!("agent kernel cannot open: {error}"))
        .inspect_err(|why| diagnostic(agent.errors.as_mut(), why))?;
    Ok(drive(&mut kernel, &Clock::new(), &mut agent))
}

fn derivation_refusal(field: &'static str) -> String {
    config::profile_refusal(Refusal::Derived { limit: LimitName::deployment(Layer::Service, field) })
}

/// The shipped agent identity, shared with the product's event header.
#[must_use]
pub fn identity() -> smith_events::Agent {
    smith_events::Agent { name: b"smith".as_slice().into(), version: env!("CARGO_PKG_VERSION").as_bytes().into() }
}

#[cfg(test)]
mod tests {
    use super::given_up_line;
    use smith_agent_service::{Destination, GivenUp, Why};

    #[test]
    fn the_given_up_line_names_the_destination_and_why() {
        let cases = [
            (
                Destination::Trace,
                Why::WriteFailed,
                "event destination trace file was given up: a write failed; later records count as lost",
            ),
            (
                Destination::Trace,
                Why::Behind,
                "event destination trace file was given up: it stayed behind past the write deadline; \
                 later records count as lost",
            ),
            (
                Destination::Output,
                Why::ReaderGone,
                "event destination standard output events was given up: its reader went away; \
                 later records count as lost",
            ),
        ];
        for (destination, why, line) in cases {
            assert_eq!(given_up_line(GivenUp { destination, why }), line);
        }
    }
}
