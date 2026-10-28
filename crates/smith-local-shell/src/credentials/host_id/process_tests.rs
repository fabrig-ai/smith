//! Two startup processes race an absent-file store using the shared real loop.
use super::*;
use crate::credentials::tests::Files;
use skein_io::kernel;
use skein_lib::{Duration, Queue, Time, Wall};
use skein_shell::{Clock, Host};
use skein_world::{
    Referee,
    end_to_end::{Binary, Command, Mode, Streams},
    real,
};
use smith_local_process_world::terminal::{Action, Terminal};

struct Judge;
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        for (binary, terminal) in [(0, 1), (2, 3)] {
            let exit = match &procs[binary] {
                Process::Binary(binary) => binary.exit_status(),
                Process::Terminal(_) => None,
            };
            if let Some(exit) = exit {
                match &mut procs[terminal] {
                    Process::Terminal(terminal) => terminal.finished(exit),
                    Process::Binary(_) => unreachable!(),
                }
            }
        }
    }
    fn observe(&mut self, _now: Time, _procs: &[Process]) {}
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _now: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        true
    }
}

enum Process {
    Binary(Box<Binary>),
    Terminal(Box<Terminal>),
}
impl Process {
    fn host(&self) -> &dyn Host {
        match self {
            Self::Binary(host) => host.as_ref(),
            Self::Terminal(host) => host.as_ref(),
        }
    }
    fn host_mut(&mut self) -> &mut dyn Host {
        match self {
            Self::Binary(host) => host.as_mut(),
            Self::Terminal(host) => host.as_mut(),
        }
    }
}
impl Host for Process {
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
        self.host().is_empty()
    }
    fn operations(&self) -> u32 {
        self.host().operations()
    }
}

#[test]
fn host_id_drawn_by_two_processes_is_one_value() {
    // This is the child process's edge: all inputs below are explicit arguments.
    if let Some(base) = std::env::var_os("SMITH_HOST_ID_TEST") {
        let directory = Directory::from_base(base.into()).unwrap();
        let serial = std::env::var("SMITH_HOST_ID_SERIAL").unwrap();
        let byte = serial.parse::<u8>().unwrap();
        let value = read_or_draw(&directory, &[byte; 32]).unwrap();
        std::fs::write(directory.host_id().with_file_name(format!("seen-{serial}")), value).unwrap();
        return;
    }
    let files = Files::new();
    let base = files.0.join("installation");
    let clock = Clock::new();
    let mut world = real::World::new(Judge);
    for serial in ["3", "4"] {
        let mut binary = Binary::start(
            Command {
                program: std::env::current_exe().unwrap(),
                arguments: vec![
                    "--exact".into(),
                    "credentials::host_id::process_tests::host_id_drawn_by_two_processes_is_one_value".into(),
                ],
                environment: vec![
                    ("HOME".into(), files.0.clone().into()),
                    ("XDG_CONFIG_HOME".into(), files.0.join("config").into()),
                    ("SMITH_HOST_ID_TEST".into(), base.clone().into()),
                    ("SMITH_HOST_ID_SERIAL".into(), serial.into()),
                ],
                directory: files.0.clone(),
            },
            Mode::Pipes,
            4096,
        )
        .unwrap();
        let streams = binary.take_streams().unwrap();
        let descriptors = streams.descriptors();
        let Streams::Pipes { input, output } = streams else { unreachable!() };
        world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
        world.spawn_with_fds(descriptors, || {
            Process::Terminal(Box::new(Terminal::piped(input, output, vec![Action::Eof])))
        });
    }
    let outcome = world.run(&clock, Duration::from_secs(1));
    for process in outcome.procs {
        if let Process::Binary(binary) = process {
            assert_eq!(binary.exit_status(), Some(kernel::Exit::Code(0)));
        }
    }
    let first = std::fs::read(base.join("seen-3")).unwrap();
    let second = std::fs::read(base.join("seen-4")).unwrap();
    assert_eq!(first, second);
    assert!(
        first == b"urn:uuid:03030303-0303-4303-8303-030303030303"
            || first == b"urn:uuid:04040404-0404-4404-8404-040404040404"
    );
}
