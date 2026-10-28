//! Product startup and the selected local composition (shell.md, sections 4 and 9).
//! Resolution owns model/account selection; the shell opens named resources and
//! carries one kernel from the stdin phase into the local host's run.
use crate::{
    command::{Command, Exec, Prompt, Usage},
    credentials, settings, startup,
};
mod files;
mod placement;
mod spawned;
pub(crate) use files::{Failure as ResultFileFailure, Files, ResultFile};
pub(crate) use placement::{host_limits, program_account, service_limits};

use skein_lib::Wall;
use skein_shell::{Clock, Config, Kernel};
use std::path::{Path, PathBuf};

/// The agent placement chosen by the product or supplied by a library host.
#[derive(Clone)]
pub enum AgentKind {
    /// The local domain owns its agent and the native effects in this process.
    Inline,
    /// The host launches the named agent binary with generated configuration.
    Spawned { program: PathBuf },
}

impl AgentKind {
    /// Build a launch from the caller's root and the generated configuration's path.
    #[must_use]
    pub fn launch(&self, root: skein_io::kernel::Fd, document: &Path) -> smith_local_service::Launch {
        let (program, arguments) = match self {
            Self::Inline => (Box::<[u8]>::default(), Box::<[Box<[u8]>]>::default()),
            Self::Spawned { program } => (
                program.as_os_str().as_encoded_bytes().into(),
                Box::from([document.as_os_str().as_encoded_bytes().into()]),
            ),
        };
        smith_local_service::Launch {
            program,
            arguments,
            environment: Box::new([]),
            root,
            directory: b".".as_slice().into(),
        }
    }
}

/// A typed startup refusal, rendered once by the product binary.
pub enum Refused {
    /// The invocation cannot supply one bounded prompt.
    Usage(Usage),
    /// A settings value or relationship refused.
    Settings(settings::Refusal),
    /// A selected source has no usable credential.
    Credentials(credentials::Refusal),
    /// Resource opening or complete memory admission refused.
    Local(startup::Failure),
}

impl Refused {
    /// The product's code for this startup terminal.
    #[must_use]
    pub const fn code(&self) -> u8 {
        match self {
            Self::Usage(_) | Self::Settings(_) => 2,
            Self::Credentials(why) => why.exit_code(),
            Self::Local(why) => why.exit_code(),
        }
    }

    /// One clipped operator line, with settings and source origins preserved.
    #[must_use]
    pub fn line(&self) -> String {
        let line = match self {
            Self::Usage(why) => format!("smith: usage: {}", why.0),
            Self::Settings(why) => why.line(),
            Self::Credentials(why) => why.line(),
            Self::Local(why) => format!("smith: {}", why.detail()),
        };
        String::from_utf8_lossy(smith_events::clip(line.as_bytes())).into_owned()
    }
}

pub(crate) struct Opened {
    pub resolved: settings::resolve::Resolved,
    pub invocation: Exec,
    pub seed: u64,
    pub wall: Wall,
    pub input_closed: bool,
    pub interactive: Option<bool>,
    document: Option<Box<[u8]>>,
}

/// Read the product layers and one complete prompt before any credentials are inspected.
pub(crate) fn open(invocation: Exec, wall: Wall, clock: &Clock, kind: &AgentKind) -> Result<(Opened, Kernel), Refused> {
    open_mode(invocation, wall, clock, None, kind)
}

fn open_mode(
    invocation: Exec,
    wall: Wall,
    clock: &Clock,
    interactive: Option<bool>,
    kind: &AgentKind,
) -> Result<(Opened, Kernel), Refused> {
    let settings = settings::read(&invocation.shared).map_err(Refused::Settings)?;
    let selection = settings::resolve::select(&settings).map_err(Refused::Settings)?;
    open_settings(invocation, wall, clock, interactive, kind, selection)
}

fn open_settings(
    invocation: Exec,
    wall: Wall,
    clock: &Clock,
    interactive: Option<bool>,
    kind: &AgentKind,
    selection: settings::resolve::Selection,
) -> Result<(Opened, Kernel), Refused> {
    let spawned = matches!(kind, AgentKind::Spawned { .. });
    let (resolved, document) = settings::resolve::build(selection, spawned).map_err(Refused::Settings)?;
    let document = spawned.then_some(document);
    let mut invocation = invocation;
    let operations =
        crate::Local::kernel_operations(&resolved.agent.service.limits.io, resolved.agent.service.limits.routes, true)
            .ok_or_else(|| {
                Refused::Local(startup::Failure::Configuration { detail: "local operation bound overflows".into() })
            })?;
    let operations = operations
        .checked_add(if invocation.output.is_some() { 4 } else { 0 })
        .ok_or_else(|| Refused::Usage(Usage("the output operation bound overflows".into())))?;
    let mut kernel = Kernel::open(Config { operations })
        .map_err(|why| Refused::Local(startup::Failure::Host { detail: format!("local kernel: {why}") }))?;
    let maximum = resolved.limits.line_bytes;
    let input_closed = match &invocation.prompt {
        Prompt::Stdin => true,
        Prompt::Argument(_) => false,
    };
    let prompt = match invocation.prompt {
        Prompt::Argument(bytes) => bytes,
        Prompt::Stdin => {
            crate::front::headless::read_prompt(&mut kernel, clock, resolved.agent.service.limits.io, maximum)
                .map_err(Refused::Usage)?
        }
    };
    if prompt.len() > usize::try_from(maximum).expect("message bound fits usize") {
        return Err(Refused::Usage(Usage("the prompt exceeds the message bound".into())));
    }
    if interactive.is_none() && prompt.trim_ascii().is_empty() {
        return Err(Refused::Usage(Usage("the prompt is empty".into())));
    }
    if std::str::from_utf8(&prompt).is_err() {
        return Err(Refused::Usage(Usage("the prompt is not UTF-8".into())));
    }
    invocation.prompt = Prompt::Argument(prompt);
    let seed = skein_shell::seed()
        .map_err(|why| Refused::Local(startup::Failure::Host { detail: format!("local seed failed (errno {why})") }))?;
    Ok((Opened { resolved, invocation, seed, wall, input_closed, interactive, document }, kernel))
}

struct Composed {
    local: crate::Local,
}

/// Compose native roots and the account driver; a spawned child receives generated JSON.
fn compose(
    opened: Opened,
    kind: &AgentKind,
    now: skein_lib::Time,
    files: &mut Files,
    kernel: &mut Kernel,
    clock: &Clock,
) -> Result<Composed, Refused> {
    let Opened { mut resolved, invocation, seed, wall, input_closed, interactive, document } = opened;
    let interactive =
        interactive.map(|plain| plain || skein_shell::terminal_modes(skein_io::kernel::Fd::new(0)).is_err());
    files.prepare_output(seed)?;
    files.create_temporary()?;
    if interactive.is_none()
        && let Some(chat) = invocation_chat(&invocation.shared, seed)
    {
        resolved.local.chat = chat;
    }
    let prompt = match invocation.prompt {
        Prompt::Argument(prompt) => prompt,
        Prompt::Stdin => unreachable!("stdin phase ended"),
    };
    if interactive.is_none() {
        resolved.local.mode = smith_local_domain::Mode::Headless { prompt };
    } else {
        resolved.local.resume = true;
    }
    let programs = credentials::program_check::needed(&resolved, clock);
    let prepared =
        credentials::prepare(&resolved, wall, interactive.is_some(), &programs).map_err(Refused::Credentials)?;
    let (workspace, chat_path) = chat_paths(
        &resolved.settings.workspace,
        &resolved.local.chat,
        files.temporary.as_deref(),
        &resolved.settings.paths,
    )?;
    trace_admission(&resolved, &workspace)?;
    deliver_contract(&mut resolved.local, invocation.deliver);
    let WorkspaceRoots { paths, delivery_roots, effect_roots } = workspace_roots(&resolved.local, kind)?;
    let host_limits = host_limits(&resolved.agent, matches!(kind, AgentKind::Spawned { .. })).map_err(|why| {
        Refused::Local(startup::Failure::Configuration {
            detail: String::from_utf8_lossy(&smith_agent_service::profile::words(&why)).into_owned(),
        })
    })?;
    if matches!(kind, AgentKind::Spawned { .. }) {
        resolved.limits.agent = smith_local_domain::AgentLimits::Spawned(host_limits);
    }
    let mut limits = service_limits(resolved.limits, host_limits, &resolved.agent.service)
        .map_err(|detail| Refused::Local(startup::Failure::Configuration { detail }))?;
    let (trace, header) = files.prepare_stream(
        &mut resolved.agent,
        &resolved.local.chat,
        &mut limits,
        invocation.json,
        wall,
        interactive.is_some(),
    )?;
    let front = prepare_front(interactive, input_closed, invocation.json, &resolved.agent, &limits, files)?;
    let environment = resolved.agent.service.environment.command_entries();
    let launch = spawned::Startup {
        document,
        kind,
        chat: &chat_path,
        workspace: &workspace,
        trace: trace.as_ref(),
        exit_grace: host_limits.exit_grace,
        timeout: resolved.agent.service.limits.file_timeout,
        seed,
    }
    .launch(kernel, clock)?;
    let endpoints = resolved.agent.service.channel_endpoints.clone();
    let rechecks = recheck_programs(&prepared, &resolved.agent.service);
    let (lower, producer) = match kind {
        AgentKind::Inline => (Some(resolved.agent.service), None),
        AgentKind::Spawned { .. } => (None, Some(resolved.agent.service)),
    };
    let config = smith_local_service::Config {
        local: resolved.local,
        limits,
        endpoints,
        paths,
        launch,
        agent: smith_local_service::AgentConfig::Spawned,
    };
    let resources = startup_resources(files, &chat_path, interactive, delivery_roots, effect_roots, environment, seed)?;
    let mut local = crate::Local::with_admission(
        config,
        lower,
        resources,
        Box::new(std::io::stderr()),
        crate::local_host::Admission { handed: Some(prepared), front: Some(front), producer, programs: rechecks },
    )
    .map_err(Refused::Local)?;
    files.attach_private(&mut local)?;
    start_stream(&mut local, trace, header, now)?;
    local.result_file(files.output.take());
    files.composed();
    Ok(Composed { local })
}

fn recheck_programs(
    prepared: &credentials::Prepared,
    configuration: &smith_agent_service::Config,
) -> Option<smith_protocol_program::Programs> {
    if prepared.locations.iter().any(|location| matches!(location.source, credentials::Source::Program { .. })) {
        let binding = &configuration.program.as_ref().expect("program account backend").program;
        Some(smith_protocol_program::Programs { directory: binding.directory.clone(), claude: binding.claude.clone() })
    } else {
        None
    }
}

fn startup_resources(
    files: &mut Files,
    chat_path: &Path,
    interactive: Option<bool>,
    delivery_roots: Box<[skein_io::kernel::Fd]>,
    effect_roots: Box<[skein_io::kernel::Fd]>,
    environment: Box<[Box<[u8]>]>,
    seed: u64,
) -> Result<crate::Resources, Refused> {
    Ok(crate::Resources {
        state_root: files.store_root(if interactive.is_some() {
            chat_path.parent().expect("workspace chats parent")
        } else {
            chat_path
        })?,
        input: skein_io::kernel::Fd::new(0),
        output: skein_io::kernel::Fd::new(1),
        signals: skein_shell::open_signals(if interactive == Some(false) {
            skein_shell::Watch::TerminationAndWindow
        } else {
            skein_shell::Watch::Termination
        })
        .map_err(|errno| {
            Refused::Local(startup::Failure::Host { detail: format!("termination signals failed (errno {errno})") })
        })?,
        delivery_roots,
        effect_roots,
        delivery_environment: environment,
        seed,
        oauth_entropy: startup::oauth_entropy().map_err(|detail| Refused::Local(startup::Failure::Host { detail }))?,
    })
}

fn deliver_contract(local: &mut smith_local_domain::Config, deliver: bool) {
    if deliver && let smith_local_domain::Contract::Report(_) = local.contract {
        local.contract = smith_local_domain::Contract::Change(smith_domain::run::outcome::ChangeSpec {
            checks_must_pass: true,
            fields: Box::new([
                smith_domain::run::outcome::FieldRule { name: b"title".as_slice().into(), max: 256 },
                smith_domain::run::outcome::FieldRule { name: b"body".as_slice().into(), max: 1024 },
            ]),
        });
    }
}

fn prepare_front(
    interactive: Option<bool>,
    input_closed: bool,
    json: bool,
    agent: &smith_agent_shell::config::Configuration,
    limits: &smith_local_service::Limits,
    files: &Files,
) -> Result<crate::front::Front, Refused> {
    if let Some(plain) = interactive {
        let front = crate::front::interactive::Interactive::new(plain, &agent.service.limits)
            .ok_or_else(|| Refused::Usage(Usage("interactive view bound overflows".into())))?;
        return Ok(crate::front::Front::Interactive(front));
    }
    let mut front = crate::front::headless::Headless::new(
        json,
        skein_shell::prepare_output(skein_io::kernel::Fd::new(1))
            .map_err(|why| Refused::Local(startup::Failure::Host { detail: format!("standard output: {why:?}") }))?,
        limits.local.show_bytes,
    );
    front.input_closed = input_closed;
    front.result_file = files.has_output();
    Ok(crate::front::Front::Headless(front))
}

fn start_stream(
    local: &mut crate::Local,
    trace: Option<smith_agent_shell::trace::TraceConfig>,
    mut header: Option<smith_events::SessionStarted>,
    now: skein_lib::Time,
) -> Result<(), Refused> {
    if let Some(trace) = trace {
        let parent = trace.path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
        let name =
            trace.path.file_name().ok_or_else(|| Refused::Usage(Usage("the trace requires a file name".into())))?;
        let descriptor = root(parent)?;
        let fd = skein_shell::open_append(descriptor, name.as_encoded_bytes(), 0o600)
            .map_err(|why| Refused::Local(startup::Failure::Host { detail: format!("trace append: {why:?}") }))?;
        local
            .adopt_trace(fd, Some(descriptor), now, header.take().expect("configured header"), trace.delivery)
            .map_err(|why| {
                Refused::Local(startup::Failure::Host {
                    detail: format!("trace descriptor {} cannot be adopted", why.raw()),
                })
            })?;
    } else if let Some(header) = header {
        local.start_session(now, header);
    }
    Ok(())
}

struct WorkspaceRoots {
    paths: Box<[Box<[u8]>]>,
    delivery_roots: Box<[skein_io::kernel::Fd]>,
    effect_roots: Box<[skein_io::kernel::Fd]>,
}

fn workspace_roots(local: &smith_local_domain::Config, kind: &AgentKind) -> Result<WorkspaceRoots, Refused> {
    let directories = local.workspace.as_ref().expect("resolved workspace").directories.as_ref();
    let paths = directories.iter().map(|directory| directory.host.clone()).collect();
    let roots = || {
        directories
            .iter()
            .map(|directory| {
                skein_shell::open_root(Path::new(std::str::from_utf8(&directory.host).expect("resolved host path")))
                    .map_err(|errno| {
                        Refused::Local(startup::Failure::Host {
                            detail: format!("workspace root failed (errno {errno})"),
                        })
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    let delivery_roots = roots()?.into_boxed_slice();
    let effect_roots = match kind {
        AgentKind::Inline => roots()?.into_boxed_slice(),
        AgentKind::Spawned { .. } => Box::new([]),
    };
    Ok(WorkspaceRoots { paths, delivery_roots, effect_roots })
}

fn chat_paths(
    workspace: &Path,
    chat_name: &[u8],
    temporary: Option<&Path>,
    paths: &settings::Paths,
) -> Result<(PathBuf, PathBuf), Refused> {
    use std::os::unix::fs::DirBuilderExt;
    let chat = std::str::from_utf8(chat_name).expect("decoded chat");
    if chat == "." || chat == ".." || chat.contains(['/', '\\', '\0']) {
        return Err(Refused::Usage(Usage("the chat must be one directory name".into())));
    }
    let workspace = workspace.canonicalize().map_err(|why| host(&why))?;
    if let Some(path) = temporary {
        return Ok((workspace, path.into()));
    }
    let chat_path = chats_tree(&workspace, paths)?.join(chat);
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&chat_path).map_err(|why| host(&why))?;
    Ok((workspace, chat_path))
}

fn chats_tree(workspace: &Path, paths: &settings::Paths) -> Result<PathBuf, Refused> {
    use std::fmt::Write;
    let digest = skein_io::digest::digest(workspace.as_os_str().as_encoded_bytes());
    let mut key = String::new();
    for part in digest.0 {
        write!(&mut key, "{part:016x}").expect("writing a string");
    }
    let base = paths
        .state_home
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| paths.home.as_ref().map(|home| PathBuf::from(home).join(".local/state")))
        .ok_or_else(|| Refused::Usage(Usage("HOME or XDG_STATE_HOME is required for chats".into())))?;
    Ok(base.join("smith/chats").join(key))
}

fn trace_admission(resolved: &settings::resolve::Resolved, workspace: &Path) -> Result<(), Refused> {
    let Some(trace) = &resolved.agent.trace else {
        return Ok(());
    };
    let tree = chats_tree(workspace, &resolved.settings.paths)?;
    // canonicalize is the existing startup path resolver. No trace is opened here.
    let tree = match tree.canonicalize() {
        Ok(tree) => tree,
        Err(why) if why.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(why) => return Err(host(&why)),
    };
    trace_outside(&trace.path, &tree)
}

fn trace_outside(path: &Path, chats: &Path) -> Result<(), Refused> {
    let resolved = match path.canonicalize() {
        Ok(path) => path,
        Err(why) if why.kind() == std::io::ErrorKind::NotFound => {
            if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
                return Err(Refused::Usage(Usage("the trace's symbolic link has no resolved destination".into())));
            }
            let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
            let name =
                path.file_name().ok_or_else(|| Refused::Usage(Usage("the trace requires a file name".into())))?;
            parent.canonicalize().map_err(|why| host(&why))?.join(name)
        }
        Err(why) => return Err(host(&why)),
    };
    if resolved.starts_with(chats) {
        return Err(Refused::Usage(Usage("the trace destination is inside this workspace's chats tree".into())));
    }
    Ok(())
}

pub(crate) fn invocation_chat(shared: &crate::command::Shared, seed: u64) -> Option<Box<[u8]>> {
    shared.chat.is_none().then(|| format!("exec-{seed:016x}").into_bytes().into())
}

pub(crate) fn prepare_stream(
    agent: &mut smith_agent_shell::config::Configuration,
    chat: &[u8],
    limits: &mut smith_local_service::Limits,
    json: bool,
    wall: Wall,
    result_file: skein_lib::Duration,
    front: bool,
) -> Result<(Option<smith_agent_shell::trace::TraceConfig>, Option<smith_events::SessionStarted>), Refused> {
    use std::path::PathBuf;
    let trace = agent.trace.take();
    let capture = if front && !json {
        smith_agent_service::sinks::Capture::None
    } else {
        smith_agent_service::sinks::Capture::Everything
    };
    let specification = smith_agent_shell::trace::TraceConfig {
        path: trace.as_ref().map_or_else(|| PathBuf::from("stdout"), |trace| trace.path.clone()),
        capture: trace.as_ref().map_or(capture, |trace| trace.capture),
        delivery: trace.as_ref().map_or(smith_agent_service::sinks::Delivery::Complete, |trace| trace.delivery),
    };
    let destinations = u32::from(trace.is_some()).checked_add(u32::from(json)).expect("two destinations");
    let mut header = crate::Local::prepare_header(
        agent,
        limits,
        (destinations != 0 || front).then_some(&specification),
        chat,
        u64::from(std::process::id()),
        crate::local_host::View { destinations, front },
        result_file,
    )
    .map_err(Refused::Local)?;
    if let Some(header) = &mut header {
        header.mode = if front { smith_events::Mode::Chat } else { smith_events::Mode::Exec };
        header.wall_ms = wall.as_nanos() / 1_000_000;
    }
    Ok((trace, header))
}

fn host(why: &std::io::Error) -> Refused {
    Refused::Local(startup::Failure::Host { detail: why.to_string() })
}

fn root(path: &Path) -> Result<skein_io::kernel::Fd, Refused> {
    skein_shell::open_root(path).map_err(|errno| {
        Refused::Local(startup::Failure::Host {
            detail: format!("directory {} failed (errno {errno})", path.display()),
        })
    })
}

/// Run an exec invocation on its one kernel; leave other commands to their owners.
#[must_use]
pub fn start(arguments: &[std::ffi::OsString], at: skein_shell::Now, clock: &Clock) -> Option<u8> {
    let (invocation, interactive) = match crate::command::parse(arguments.iter().skip(1).cloned().collect()) {
        Ok(Command::Exec(invocation)) => (invocation, None),
        Ok(Command::Interactive { shared, plain }) => (
            Exec {
                shared,
                prompt: Prompt::Argument(Box::new([])),
                json: false,
                output: None,
                deliver: false,
                ephemeral: false,
            },
            Some(plain),
        ),
        Ok(Command::Check { .. } | Command::Logout { .. } | Command::Login { .. }) => {
            return None;
        }
        Err(why) => {
            eprintln!("{}", Refused::Usage(why).line());
            return Some(2);
        }
    };
    let result = settings::read(&invocation.shared).map_err(Refused::Settings).and_then(|settings| {
        let selection = settings::resolve::select(&settings).map_err(Refused::Settings)?;
        let kind =
            if program_account(&selection.accounts) { placement::beside_current_exe()? } else { AgentKind::Inline };
        let opened = open_settings(invocation, at.wall, clock, interactive, &kind, selection)?;
        run_opened(opened, at, clock, &kind)
    });
    Some(match result {
        Ok(code) => code,
        Err(why) => {
            eprintln!("{}", why.line());
            why.code()
        }
    })
}

/// Compose and drive a library invocation with the chosen child placement.
pub fn run(
    invocation: Exec,
    at: skein_shell::Now,
    clock: &Clock,
    interactive: Option<bool>,
    kind: &AgentKind,
) -> Result<u8, Refused> {
    let opened = if interactive.is_none() {
        open(invocation, at.wall, clock, kind)?
    } else {
        open_mode(invocation, at.wall, clock, interactive, kind)?
    };
    run_opened(opened, at, clock, kind)
}

fn run_opened(
    (opened, mut kernel): (Opened, Kernel),
    at: skein_shell::Now,
    clock: &Clock,
    kind: &AgentKind,
) -> Result<u8, Refused> {
    let mut files = Files::prepare(&opened.invocation, &opened.resolved, opened.seed)?;
    let result = compose(opened, kind, at.now, &mut files, &mut kernel, clock).map(|mut started| {
        match skein_shell::drive(&mut kernel, clock, &mut started.local) {
            skein_io::kernel::Exit::Code(code) => code,
            skein_io::kernel::Exit::Signal(_) => 130,
        }
    });
    files.close_startup(&mut kernel, clock);
    result
}

#[cfg(test)]
mod trace_admission_tests {
    use super::*;

    #[test]
    fn trace_admission_resolves_existing_files_absent_files_and_aliases_before_opening() {
        let scratch = smith_real_world::Scratch::new();
        let chats = scratch.path().join("chats");
        let chat = chats.join("old");
        std::fs::create_dir_all(&chat).unwrap();
        let state = chat.join("state");
        std::fs::write(&state, b"older bytes").unwrap();
        let outside = scratch.path().join("outside");
        trace_outside(&outside, &chats).unwrap_or_else(|why| panic!("{}", why.line()));
        assert!(!outside.exists(), "admission does not open an ordinary outside destination");
        let alias = scratch.path().join("alias");
        std::os::unix::fs::symlink(&chat, &alias).unwrap();
        let link = scratch.path().join("linked-file");
        std::os::unix::fs::symlink(&state, &link).unwrap();
        for path in [&state, &chat.join("trace"), &alias.join("state"), &alias.join("trace"), &link] {
            let Err(refused) = trace_outside(path, &chats) else {
                panic!("trace inside chats must refuse");
            };
            assert_eq!(refused.code(), 2);
            assert!(refused.line().contains("inside this workspace's chats tree"));
            assert_eq!(std::fs::read(&state).unwrap(), b"older bytes");
            assert!(!chat.join("trace").exists());
        }
    }
}

#[cfg(test)]
mod configured_trace_tests {
    use super::*;

    #[test]
    fn configured_local_and_generated_child_traces_are_admitted_before_any_open() {
        let scratch = smith_real_world::Scratch::new();
        let workspace = scratch.path().canonicalize().unwrap();
        let paths =
            settings::Paths { state_home: Some(scratch.path().join("state").into()), ..settings::Paths::default() };
        let tree = chats_tree(&workspace, &paths).unwrap_or_else(|why| panic!("{}", why.line()));
        let chat = tree.join("old");
        std::fs::create_dir_all(&chat).unwrap();
        std::fs::write(chat.join("state"), b"old state").unwrap();
        let alias = scratch.path().join("alias");
        std::os::unix::fs::symlink(&chat, &alias).unwrap();
        let certificate =
            skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
        for (path, allowed) in
            [(scratch.path().join("outside"), true), (chat.join("state"), false), (alias.join("trace"), false)]
        {
            let source = settings::Source {
                layer: settings::Layer::User,
                path: "trace-admission.toml".into(),
                text: format!(
                    "model='codex/gpt-6-luna'\ntrace.path='{}'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\ntrust.system=false\ntrust.der=['{certificate}']\n",
                    path.display()
                ),
            };
            let mut settings = settings::from_sources(&crate::command::Shared::default(), workspace.clone(), &[source])
                .unwrap_or_else(|why| panic!("{}", why.line()));
            settings.paths = paths.clone();
            let (resolved, document) =
                settings::resolve::resolve_startup(&settings).unwrap_or_else(|why| panic!("{}", why.line()));
            let document: serde_json::Value = serde_json::from_slice(&document).unwrap();
            assert_eq!(document["trace"]["path"].as_str(), path.to_str());
            assert_eq!(trace_admission(&resolved, &workspace).is_ok(), allowed);
            assert_eq!(std::fs::read(chat.join("state")).unwrap(), b"old state");
            assert!(!chat.join("trace").exists());
            assert!(!scratch.path().join("outside").exists());
        }
    }
}
