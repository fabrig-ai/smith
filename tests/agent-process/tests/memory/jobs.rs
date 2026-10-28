//! Saturate the machine's declared job captures beside the full LLM pool.
use skein_io::Event;
use skein_lib::stream::Up;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use smith_domain::{run, tools};
use smith_protocol_machine::{Below, BelowEvent, Component, Environment, FromDomain, Limits, ToDomain};

const ROOT: Token = Token::new(1);
const OWNER_BASE: u64 = 100;
const CHILD_BASE: u64 = 200;
const PIPE_BASE: u64 = 300;
const PIPE_COUNT: u64 = 2;
const PROCESS_DEADLINE: Duration = Duration::from_secs(60);
const QUEUE_SLOTS: u32 = 8;

pub(super) struct Jobs {
    component: Component,
    env: Env<Limits>,
    up: Queue<ToDomain>,
    down: Queue<Below>,
}

pub(super) fn fill() -> Jobs {
    let limits = smith_agent_process_world::limits();
    let mut jobs = Jobs {
        component: Component::new(&limits.machine, Environment::encode(&[], &limits.machine).expect("environment")),
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.machine },
        up: Queue::with_capacity(QUEUE_SLOTS),
        down: Queue::with_capacity(QUEUE_SLOTS),
    };
    let workspace = run::Workspace {
        directories: Box::new([run::Directory {
            host: b"/host".as_slice().into(),
            name: b"repo".as_slice().into(),
            root: ROOT,
            writable: true,
            git: false,
            conflicts: Box::new([]),
        }]),
    };
    jobs.component.workspace(&workspace);
    let capture = limits.domain.session.tools;
    let count = limits.domain.run.conversations * capture.jobs;
    assert!(count <= limits.machine.processes);
    for number in 0..count {
        jobs.start(u64::from(number), capture.shell_head, capture.shell_tail);
    }
    assert!(jobs.up.is_empty());
    jobs
}

impl Jobs {
    fn start(&mut self, number: u64, head: u32, tail: u32) {
        let owner = Token::new(OWNER_BASE + number);
        self.component.from_domain(
            &self.env,
            FromDomain::Op {
                owner,
                op: tools::Op::Spawn {
                    cwd: tools::Place { root: ROOT, path: Box::new([]) },
                    command: b"build".as_slice().into(),
                    head,
                    tail,
                    after: Some(Duration::ZERO),
                },
                deadline: Time::ZERO.saturating_add(PROCESS_DEADLINE),
            },
            &mut self.up,
            &mut self.down,
        );
        assert!(matches!(self.down.pop(), Some(Below::Spawn { .. })));
        let stdout = Token::new(PIPE_BASE + PIPE_COUNT * number);
        let stderr = Token::new(PIPE_BASE + PIPE_COUNT * number + 1);
        self.child(Event::Spawned { owner, child: Token::new(CHILD_BASE + number), pipes: Box::new([stdout, stderr]) });
        while self.down.pop().is_some() {}
        self.component.fire(&self.env, &mut self.up, &mut self.down);
        assert_eq!(
            self.up.pop(),
            Some(ToDomain::Done {
                owner,
                done: tools::Done::Held { head: Box::new([]), tail: Box::new([]), dropped: 0 }
            })
        );
        self.pipe(Event::Stream {
            owner: stdout,
            up: Up::Bytes(vec![b'x'; usize::try_from(head + tail).expect("kept output")].into()),
        });
        while self.down.pop().is_some() {}
    }

    fn child(&mut self, event: Event) {
        self.component.from_below(&self.env, BelowEvent::Process(event), &mut self.up, &mut self.down);
    }

    fn pipe(&mut self, event: Event) {
        self.component.from_below(&self.env, BelowEvent::Pipe(event), &mut self.up, &mut self.down);
    }

    pub(super) fn check_captures(&mut self) {
        let limits = smith_agent_process_world::limits();
        let capture = limits.domain.session.tools;
        let count = limits.domain.run.conversations * capture.jobs;
        for number in 0..count {
            let owner = Token::new(OWNER_BASE + u64::from(number));
            self.component.from_domain(
                &self.env,
                FromDomain::Job {
                    owner,
                    act: tools::JobAct::Read {
                        head: capture.shell_head,
                        tail: capture.shell_tail,
                        wait: Duration::ZERO,
                    },
                },
                &mut self.up,
                &mut self.down,
            );
            self.component.fire(&self.env, &mut self.up, &mut self.down);
            assert_eq!(
                self.up.pop(),
                Some(ToDomain::Done {
                    owner,
                    done: tools::Done::Job {
                        head: vec![b'x'; usize::try_from(capture.shell_head).expect("head")].into(),
                        tail: vec![b'x'; usize::try_from(capture.shell_tail).expect("tail")].into(),
                        dropped: 0,
                        end: tools::JobEnd::Running,
                    }
                })
            );
            while self.down.pop().is_some() {}
        }
    }
}
