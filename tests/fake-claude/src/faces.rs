//! Startup adapters for the same command machine (protocol/programs.md,
//! sections 4.2, 5 and 8; testing.md, section 4). Only this module reads the
//! process's arguments/environment or opens kernel startup descriptors.

use std::{os::unix::ffi::OsStrExt, path::Path, process::ExitCode};

use skein_io::kernel::{Exit, Fd, Spawn};
use skein_shell::{Clock, Config, Host};
use skein_world::{HostedProgram, Inherited, StartupAppend, StartupRoot};

use crate::{CONFIG_DIR, Faces, Fake, OBSERVATIONS_FILE};

pub(crate) fn config_path(environment: &[Box<[u8]>]) -> Option<&[u8]> {
    environment.iter().find_map(|entry| entry.strip_prefix(b"CLAUDE_CONFIG_DIR=").filter(|path| !path.is_empty()))
}

/// Declares the configuration directory named by this launch, as `config`.
/// Worlds must make it before spawning (protocol/programs.md, section 4.2):
/// an absent directory fails hosted startup as any missing startup root does.
/// The binary separately admits an absent directory as `Faces::config: None`.
pub fn config_root(spawn: &Spawn) -> Option<StartupRoot> {
    config_path(&spawn.env).map(|path| StartupRoot { name: b"config".as_slice().into(), path: path.into() })
}

/// Declares the invocation file namespace as `mcp`, shared with allowed acts.
/// Paths are root-relative; the launch directory resolves relative user paths.
pub fn mcp_root(spawn: &Spawn) -> Option<StartupRoot> {
    file_arguments(&spawn.args, Path::new(std::ffi::OsStr::from_bytes(&spawn.dir)))
        .map(|files| StartupRoot { name: b"mcp".as_slice().into(), path: files.root.into() })
}

/// Startup's file namespace and flag paths relative to it.
#[derive(Debug)]
pub(crate) struct FileArguments {
    pub root: Vec<u8>,
    pub arguments: Vec<Box<[u8]>>,
}

// Startup and commands share a file root without adding a third root slot. Kernel
// file effects deliberately refuse absolute names and paths escaping a root.
pub(crate) fn file_arguments(arguments: &[Box<[u8]>], directory: &Path) -> Option<FileArguments> {
    let paths: Vec<_> = arguments
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| matches!(pair[0].as_ref(), b"--mcp-config" | b"--append-system-prompt-file"))
        .map(|(index, pair)| (index + 1, directory.join(std::ffi::OsStr::from_bytes(&pair[1]))))
        .collect();
    let rooted = paths.iter().any(|(_, path)| path.is_absolute());
    let mut relative = arguments.to_vec();
    for (index, path) in &paths {
        relative[*index] = path.strip_prefix("/").unwrap_or(path).as_os_str().as_bytes().into();
    }
    if paths.is_empty() {
        return None;
    }
    // `/.' also maps to the simulated machine's root through its startup hook.
    let root = if rooted { b"/.".to_vec() } else { b".".to_vec() };
    Some(FileArguments { root, arguments: relative })
}

/// Declares the observations append file for `World::host_startup`, alongside
/// [`config_root`]. Both selectors return no descriptor when the variable is unset.
pub fn observation_append(spawn: &Spawn) -> Option<StartupAppend> {
    config_path(&spawn.env).map(|path| StartupAppend {
        name: b"observations".as_slice().into(),
        root: path.into(),
        path: OBSERVATIONS_FILE.as_bytes().into(),
        mode: 0o600,
    })
}

/// A hosted factory; register it with [`config_root`] and [`observation_append`]
/// and [`mcp_root`] through `World::host_startup`. Its default limit is one simultaneous invocation.
/// Scenario adapters resolve a descriptor-relative `Spawn::dir` into the
/// scenario's filesystem namespace before selecting roots or calling the factory.
pub fn hosted(program: &[u8]) -> HostedProgram<Fake> {
    HostedProgram { program: program.into(), make, instances: 1, operations: crate::fake::operations() }
}

fn make(spawn: &Spawn, inherited: &Inherited) -> Fake {
    let pipe = |number| inherited.pipes.iter().find(|(child, _)| *child == number).expect("standard pipe").1;
    Fake::new_in(
        &spawn.args,
        &spawn.env,
        Faces {
            stdin: inherited.pipes.iter().find(|(child, _)| *child == 0).map(|(_, fd)| *fd),
            stdout: pipe(1),
            stderr: pipe(2),
            signal: inherited.signal,
            config: inherited.roots.iter().find(|(name, _)| name.as_ref() == b"config").map(|(_, fd)| *fd),
            mcp: inherited.roots.iter().find(|(name, _)| name.as_ref() == b"mcp").map(|(_, fd)| *fd),
            observations: inherited
                .appends
                .iter()
                .find(|(name, _)| name.as_ref() == b"observations")
                .map(|(_, fd)| *fd),
        },
        Path::new(std::ffi::OsStr::from_bytes(&spawn.dir)),
    )
}

/// Runs the fake on the kernel, returning its command's exit code. Downstream
/// packages need only call this from their own binary's `main`.
pub fn main() -> ExitCode {
    let arguments: Vec<Box<[u8]>> = std::env::args_os().skip(1).map(|argument| argument.as_bytes().into()).collect();
    let environment: Vec<Box<[u8]>> = std::env::vars_os()
        .map(|(name, value)| [name.as_bytes(), b"=", value.as_bytes()].concat().into_boxed_slice())
        .collect();
    let config = match config_path(&environment) {
        Some(path) => match skein_shell::open_root(Path::new(std::ffi::OsStr::from_bytes(path))) {
            Ok(root) => Some(root),
            Err(2) => None,
            Err(error) => {
                eprintln!("fake claude: cannot open {CONFIG_DIR}: errno {error}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    let observations = match config {
        Some(root) => match skein_shell::open_append(root, OBSERVATIONS_FILE.as_bytes(), 0o600) {
            Ok(file) => Some(file),
            Err(error) => {
                skein_shell::close_keeper_fd(root);
                eprintln!("fake claude: cannot open observations: {error:?}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };
    let signal = match skein_shell::open_termination_signals() {
        Ok(signal) => signal,
        Err(error) => {
            for fd in config.into_iter().chain(observations) {
                skein_shell::close_keeper_fd(fd);
            }
            eprintln!("fake claude: cannot open signals: errno {error}");
            return ExitCode::FAILURE;
        }
    };
    let directory = std::env::current_dir().expect("invocation working directory");
    let mcp = file_arguments(&arguments, &directory)
        .and_then(|files| skein_shell::open_root(Path::new(std::ffi::OsStr::from_bytes(&files.root))).ok());
    let mut fake = Fake::new_in(
        &arguments,
        &environment,
        Faces { stdin: Some(Fd::new(0)), stdout: Fd::new(1), stderr: Fd::new(2), signal, config, observations, mcp },
        &directory,
    );
    fake.record_process(skein_shell::process_id());
    let mut kernel = match skein_shell::Kernel::open(Config { operations: fake.operations() }) {
        Ok(kernel) => kernel,
        Err(error) => {
            eprintln!("fake claude: cannot open kernel: {error:?}");
            return ExitCode::FAILURE;
        }
    };
    match skein_shell::drive(&mut kernel, &Clock::new(), &mut fake) {
        Exit::Code(code) => ExitCode::from(code),
        Exit::Signal(signal) => ExitCode::from(u8::try_from(128_u32.saturating_add(signal)).unwrap_or(255)),
    }
}
