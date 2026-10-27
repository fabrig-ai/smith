//! Outside terminal observations on the existing shipped-binary world.
use super::{BINARY_WORLD_PATIENCE, Process, Refusal};
use skein_io::kernel::{Fd, Size};
use skein_lib::{Duration, Time};
use skein_shell::{Modes, resize_terminal, terminal_modes};
use skein_world::Referee;
use smith_local_process_world::process::Proc;

/// How long ordinary exec/chat worlds may take to settle without sign-in.
pub(super) const WORLD_PATIENCE: Duration = Duration::from_secs(2);

/// How long program shipped-binary worlds may take to settle.
pub(super) const PROGRAM_WORLD_PATIENCE: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
pub(super) enum Story {
    None,
    Modes,
    Resize,
    ProgramCheck,
    ProgramRun,
}

enum Resize {
    None,
    Response,
    Done,
}

struct Watch {
    master: Fd,
    before: Modes,
    during: Option<Modes>,
    after: Option<Modes>,
    resize: Resize,
}

pub(super) struct Judge {
    pub common: Refusal,
    watch: Option<Watch>,
    program: Option<super::program_interrupt::Watch>,
}

impl Judge {
    pub fn new(common: Refusal, story: Story, master: Option<Fd>, scratch: &std::path::Path) -> Self {
        let watch = match story {
            Story::None | Story::ProgramRun | Story::ProgramCheck => None,
            Story::Modes | Story::Resize => {
                let master = master.expect("terminal story has a master");
                Some(Watch {
                    master,
                    before: terminal_modes(master).expect("initial terminal modes"),
                    during: None,
                    after: None,
                    resize: match story {
                        Story::Resize => Resize::Response,
                        Story::None | Story::Modes | Story::ProgramRun | Story::ProgramCheck => Resize::None,
                    },
                })
            }
        };
        let program = matches!(story, Story::ProgramCheck).then(|| {
            super::program_interrupt::Watch::new(scratch.join("state/smith/programs/work/fake-claude.observations"))
        });
        Self { common, watch, program }
    }
}

impl Watch {
    fn inspect(&mut self, procs: &[Process]) {
        let binary = procs
            .iter()
            .find_map(|process| match process {
                Process::Binary(binary) => Some(binary),
                Process::Script(_) | Process::Sample(_) => None,
            })
            .expect("shipped binary");
        if binary.exit_status().is_some() {
            if self.after.is_none() {
                self.after = Some(terminal_modes(self.master).expect("terminal modes after exit"));
            }
            return;
        }
        let shown = procs
            .iter()
            .find_map(|process| match process {
                Process::Script(Proc::Terminal(person)) => Some(person.shown()),
                Process::Binary(_) | Process::Script(_) | Process::Sample(_) => None,
            })
            .expect("scripted person");
        let modes = terminal_modes(self.master).expect("running terminal modes");
        assert_eq!(modes, self.before, "smith leaves running terminal modes alone");
        self.during = Some(modes);
        match self.resize {
            Resize::Response
                if shown
                    .windows(b"codex/gpt-6-luna; API key: in ".len())
                    .any(|bytes| bytes == b"codex/gpt-6-luna; API key: in ") =>
            {
                resize_terminal(self.master, Size { rows: 24, columns: 20 }).expect("second window change");
                self.resize = Resize::Done;
            }
            Resize::None | Resize::Response | Resize::Done => {}
        }
    }

    fn passed(&self) -> bool {
        if self.after.is_none() {
            return false;
        }
        assert_eq!(self.during, Some(self.before), "running smith leaves terminal modes alone");
        assert_eq!(self.after, Some(self.before), "exited smith leaves terminal modes alone");
        matches!(self.resize, Resize::None | Resize::Done)
    }
}

impl Referee<Process> for Judge {
    fn act(&mut self, now: Time, procs: &mut [Process]) {
        if let Some(watch) = &mut self.watch {
            watch.inspect(procs);
        }
        if let Some(program) = &mut self.program {
            program.act(now, procs);
        }
        self.common.act(now, procs);
    }

    fn observe(&mut self, now: Time, procs: &[Process]) {
        self.common.observe(now, procs);
    }

    fn next_deadline(&self) -> Option<Time> {
        self.common
            .next_deadline()
            .into_iter()
            .chain(self.program.as_ref().and_then(super::program_interrupt::Watch::next_deadline))
            .min()
    }

    fn overdue(&self, now: Time) -> Option<String> {
        self.common.overdue(now)
    }

    fn passed(&self) -> bool {
        self.common.passed()
            && self.watch.as_ref().is_none_or(Watch::passed)
            && self.program.as_ref().is_none_or(super::program_interrupt::Watch::passed)
    }
}

impl Story {
    pub(super) fn budget(self, sign_in: bool) -> Duration {
        match self {
            // These stories judge outcomes, modes and widths, not durations.
            // Their referees share their world's patience (director 1294).
            Self::None | Self::Modes => {
                // Sign-in and commit delivery use the shipped-binary world,
                // not ordinary chat's two seconds; never shorten their former
                // five-second referee allowance to that smaller constant.
                if sign_in { BINARY_WORLD_PATIENCE } else { WORLD_PATIENCE }
            }
            Self::Resize => WORLD_PATIENCE,
            // The five-second interruption assertion stays in program_interrupt.
            Self::ProgramRun | Self::ProgramCheck => PROGRAM_WORLD_PATIENCE,
        }
    }
}

#[test]
fn non_program_referees_use_world_patience() {
    let now = Time::ZERO;
    for story in [Story::None, Story::Modes, Story::Resize] {
        let referee = Refusal::new(now, now.saturating_add(story.budget(false)));
        assert_eq!(referee.overdue(now.saturating_add(Duration::from_millis(1118))), None);
        let deadline = now.saturating_add(WORLD_PATIENCE);
        assert_eq!(referee.next_deadline(), Some(deadline));
        assert!(referee.overdue(deadline).is_some());
    }
}

#[test]
fn sign_in_referees_keep_their_allowance_and_use_binary_world_patience() {
    let now = Time::ZERO;
    for story in [Story::None, Story::Modes] {
        let budget = story.budget(true);
        assert!(budget >= Duration::from_secs(5), "sign-in never loses its former referee allowance");
        let referee = Refusal::new(now, now.saturating_add(budget));
        assert_eq!(referee.overdue(now.saturating_add(WORLD_PATIENCE)), None);
        assert_eq!(referee.overdue(now.saturating_add(Duration::from_secs(5))), None);
        let deadline = now.saturating_add(BINARY_WORLD_PATIENCE);
        assert_eq!(referee.next_deadline(), Some(deadline));
        assert!(referee.overdue(deadline).is_some());
    }
}

#[test]
fn program_stories_allow_thirty_seconds_total() {
    for story in [Story::ProgramRun, Story::ProgramCheck] {
        for sign_in in [false, true] {
            assert_eq!(story.budget(sign_in), Duration::from_secs(30));
        }
    }
}
