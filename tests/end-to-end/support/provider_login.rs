//! One shipped credential command against a listening provider fixture.
//! The referee keeps readiness and browser progress, never credential values;
//! entry points observe sign-in ownership (protocol/hosts.md, section 5.4)
//! through shipped commands (testing.md, sections 2.3 and 5).
use super::{Process, Ran, STARTUP_WORLD_PATIENCE};
use skein_lib::Time;
use skein_shell::{Clock, Host};
use skein_world::{
    Referee,
    end_to_end::{Binary, Command, Mode, Streams},
    real,
};
use smith_local_process_world::process::{BrowserProcess, IssuerProcess, Proc};
use smith_local_process_world::terminal::Terminal;
use smith_real_world::Scratch;

struct Judge {
    deadline: Time,
    visited: bool,
    started: bool,
    arriving: Option<(std::path::PathBuf, Vec<u8>)>,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        let binary = procs
            .iter()
            .find_map(|proc| match proc {
                Process::Binary(binary) => Some(binary),
                Process::Script(_) | Process::Sample(_) => None,
            })
            .expect("credential fixture");
        let exit = binary.exit_status();
        let listening = procs
            .iter()
            .any(|proc| matches!(proc, Process::Script(Proc::Issuer(issuer)) if issuer.peer().address().is_some()));
        let start = listening && !self.started;
        self.started |= listening;
        let url = if self.visited || !listening {
            None
        } else {
            binary.stderr().split_inclusive(|b| *b == b'\n').find_map(|line| {
                line.strip_suffix(b"\n").and_then(|line| line.strip_prefix(b"Sign in: ")).map(Vec::from)
            })
        };
        if url.is_some() {
            self.visited = true;
            if let Some((path, bytes)) = self.arriving.take() {
                keep_record(&path, &bytes).expect("racing record fixture");
            }
        }
        for proc in procs {
            match proc {
                Process::Script(Proc::Terminal(person)) => {
                    if start {
                        person.type_line(b"ready\n".as_slice().into());
                    }
                    if let Some(exit) = exit {
                        person.finished(exit);
                    }
                }
                Process::Script(Proc::Browser(browser)) => {
                    if let Some(url) = &url {
                        browser.visit(url);
                    }
                    if exit.is_some() {
                        browser.shutdown();
                    }
                }
                Process::Script(Proc::Issuer(issuer)) if exit.is_some() => issuer.shutdown(),
                Process::Binary(_) | Process::Script(_) | Process::Sample(_) => {}
            }
        }
    }
    fn observe(&mut self, _now: Time, _procs: &[Process]) {}
    fn next_deadline(&self) -> Option<Time> {
        Some(self.deadline)
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= self.deadline).then(|| "provider command did not settle".into())
    }
    fn passed(&self) -> bool {
        true
    }
}

#[must_use]
pub fn run(
    scratch: &Scratch,
    name: &str,
    logout: bool,
    subject: &[u8],
    plan: bool,
    again: bool,
    revocation: Option<bool>,
) -> (Ran, Vec<Vec<u8>>, usize) {
    run_arriving(scratch, name, logout, subject, plan, again, (revocation, None))
}

pub fn keep_record(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut record = std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
    record.write_all(bytes)
}

pub fn run_arriving(
    scratch: &Scratch,
    name: &str,
    logout: bool,
    subject: &[u8],
    plan: bool,
    again: bool,
    finish: (Option<bool>, Option<(std::path::PathBuf, Vec<u8>)>),
) -> (Ran, Vec<Vec<u8>>, usize) {
    let (revocation, arriving) = finish;
    let clock = Clock::new();
    let mut binary = start(scratch, name, logout, Mode::Pipes);
    let streams = binary.take_streams().expect("credential pipes");
    let descriptors = streams.descriptors();
    let Streams::Pipes { input, output } = streams else { unreachable!("credential pipes") };
    let mut world = real::World::new(Judge {
        deadline: clock.now().now.saturating_add(STARTUP_WORLD_PATIENCE),
        visited: false,
        started: false,
        arriving,
    });
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn(|| {
        Process::Script(Proc::Issuer(Box::new(IssuerProcess::provider(34_444, subject, plan, again, revocation))))
    });
    world.spawn(|| Process::Script(Proc::Browser(Box::new(BrowserProcess::new()))));
    world.spawn_with_fds(descriptors, || {
        Process::Script(Proc::Terminal(Box::new(Terminal::piped(input, output, Vec::new()))))
    });
    let outcome = world.run(&clock, STARTUP_WORLD_PATIENCE);
    assert!(outcome.procs.iter().all(Host::is_empty), "credential command drains");
    let Process::Binary(binary) = outcome.procs.first().expect("binary") else { unreachable!() };
    let Process::Script(Proc::Issuer(issuer)) = outcome.procs.get(1).expect("issuer") else { unreachable!() };
    let urls = issuer
        .peer()
        .observations()
        .iter()
        .filter_map(|observation| match observation {
            skein_fake_peers::oauth::Observation::Authorize { target, .. } => Some(target.to_vec()),
            skein_fake_peers::oauth::Observation::Accepted { .. }
            | skein_fake_peers::oauth::Observation::Post { .. }
            | skein_fake_peers::oauth::Observation::Document { .. }
            | skein_fake_peers::oauth::Observation::Revoke { .. }
            | skein_fake_peers::oauth::Observation::Redirect { .. }
            | skein_fake_peers::oauth::Observation::Answered { .. }
            | skein_fake_peers::oauth::Observation::Closed { .. } => None,
        })
        .collect();
    let revoked = issuer
        .peer()
        .observations()
        .iter()
        .filter(|observation| matches!(observation, skein_fake_peers::oauth::Observation::Revoke { .. }))
        .count();
    let Some(skein_io::kernel::Exit::Code(code)) = binary.exit_status() else {
        let lines = String::from_utf8_lossy(binary.stderr())
            .lines()
            .filter(|line| !line.starts_with("Sign in: "))
            .collect::<Vec<_>>()
            .join("\n");
        panic!("credential command exit: {lines}")
    };
    (Ran { code, stdout: Vec::new(), stderr: binary.stderr().into() }, urls, revoked)
}

fn start(scratch: &Scratch, name: &str, logout: bool, mode: Mode) -> Binary {
    let mut command = Command {
        program: skein_scratch::paths::binary("smith"),
        arguments: vec![if logout { "logout".into() } else { "login".into() }, name.into()],
        environment: vec![
            ("HOME".into(), scratch.path().into()),
            ("XDG_CONFIG_HOME".into(), scratch.path().join("config").into()),
            ("XDG_STATE_HOME".into(), scratch.path().join("state").into()),
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("SSH_CONNECTION".into(), "192.0.2.2 1234 192.0.2.1 22".into()),
        ],
        directory: scratch.path().into(),
    };
    match mode {
        Mode::Pipes => {
            // Wait for the issuer's Listening observation before exec: starting
            // logout sooner can correctly fail its connection before the fake
            // binds. The referee deadline bounds a missing readiness cue.
            let mut arguments = vec![
                "-c".into(),
                "read -r ready && exec \"$@\"".into(),
                "provider-ready".into(),
                command.program.into_os_string(),
            ];
            arguments.append(&mut command.arguments);
            command.program = "/bin/sh".into();
            command.arguments = arguments;
        }
        Mode::Terminal => {}
    }
    Binary::start(command, mode, 1_048_576).expect("credential fixture")
}

pub fn terminal_key(scratch: &Scratch, name: &str) -> Ran {
    let clock = Clock::new();
    let mut binary = start(scratch, name, false, Mode::Terminal);
    let streams = binary.take_streams().expect("credential fixture");
    let descriptors = streams.descriptors();
    let Streams::Terminal { stream } = streams else { unreachable!() };
    let mut world =
        real::World::new(super::Refusal::new(clock.now().now, clock.now().now.saturating_add(STARTUP_WORLD_PATIENCE)));
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn_with_fds(descriptors, || {
        Process::Script(Proc::Terminal(Box::new(Terminal::attached(stream, Vec::new()))))
    });
    let outcome = world.run(&clock, STARTUP_WORLD_PATIENCE);
    assert!(outcome.procs.iter().all(Host::is_empty), "credential command drained");
    let Process::Binary(binary) = outcome.procs.first().expect("binary") else { unreachable!() };
    let Some(skein_io::kernel::Exit::Code(code)) = binary.exit_status() else { panic!("terminal key exit") };
    Ran { code, stdout: Vec::new(), stderr: binary.stderr().into() }
}
