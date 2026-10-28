//! Simulator-free child output for the fake checkout's git decisions. The
//! world answers command effects through skein-fake-checkout, then binds this
//! Host to the child's pipes. It knows neither Sim nor Machine.

use skein_io::kernel;
use skein_lib::{Queue, Time, Token, Wall};
use skein_world::Host;

/// A git child sending its bounded stdout before closing both output pipes.
pub struct Child {
    output: Option<kernel::Fd>,
    error: Option<kernel::Fd>,
    bytes: Box<[u8]>,
    written: u32,
    pending: bool,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    code: u8,
    extra: Vec<kernel::Fd>,
    opening: u32,
}

impl Child {
    #[must_use]
    pub fn new(pipes: &[(u32, kernel::Fd)], bytes: Box<[u8]>, code: u8) -> Self {
        assert!(bytes.len() <= 32_768);
        assert_eq!(pipes.len(), 2);
        Self {
            output: Some(pipes.iter().find(|(child, _)| *child == 1).expect("stdout").1),
            error: Some(pipes.iter().find(|(child, _)| *child == 2).expect("stderr").1),
            bytes,
            written: 0,
            pending: false,
            completions: Queue::with_capacity(32),
            submissions: Queue::with_capacity(32),
            code,
            extra: Vec::new(),
            opening: 0,
        }
    }
    /// A valid opening followed by an exit before any answer, with every inherited descriptor closed.
    #[must_use]
    pub fn unanswered(inherited: &skein_world::Inherited) -> Self {
        let configuration = crate::process::lower_configuration();
        let limits = configuration.limits.channel.channel;
        let schema = smith_protocol_channel::schema(&configuration.limits.channel).expect("agent channel schema");
        let version = schema.range().expect("one channel version").1;
        let mut entries = skein_lib::List::with_capacity(limits.kinds);
        for kind in &schema.version(version).expect("selected channel version").kinds {
            if kind.direction == skein_channel::Direction::FromInitiator {
                entries.push(skein_channel::Term { kind: kind.kind, largest: kind.largest }).expect("schema terms");
            }
        }
        let mut bytes = Vec::new();
        for control in
            [skein_channel::Control::Accept { version, features: 0 }, skein_channel::Control::Terms { entries }]
        {
            bytes.extend_from_slice(skein_channel::control_frame(&control, &limits).expect("opening frame").bytes());
        }
        let pipes: Vec<_> = inherited.pipes.iter().filter(|(number, _)| *number != 0).copied().collect();
        let mut child = Self::new(&pipes, bytes.into_boxed_slice(), 7);
        child.extra = vec![inherited.signal, inherited.pipes.iter().find(|(number, _)| *number == 0).expect("stdin").1];
        child.opening = 34
            + 6 * schema
                .version(version)
                .expect("selected version")
                .kinds
                .iter()
                .filter(|kind| kind.direction == skein_channel::Direction::FromResponder)
                .count() as u32;
        child
    }

    #[must_use]
    pub fn code(&self) -> u8 {
        self.code
    }
}

impl Host for Child {
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn iterate(&mut self, _: Time, _: Wall) {
        while let Some(complete) = self.completions.pop() {
            assert!(self.pending);
            self.pending = false;
            match complete.result.expect("fake git pipe outcome") {
                kernel::Done::Count(count) => {
                    if complete.op == Token::new(5) {
                        self.opening = self.opening.saturating_sub(count);
                    } else {
                        self.written = self.written.checked_add(count).expect("bounded stdout");
                    }
                }
                kernel::Done::Nothing => {
                    if complete.op == Token::new(2) {
                        self.output = None;
                    } else {
                        self.error = None;
                    }
                }
                done => panic!("unexpected git pipe completion {done:?}"),
            }
        }
        if self.pending {
            return;
        }
        let next = if let Some(output) = self.output {
            if usize::try_from(self.written).expect("small count") < self.bytes.len() {
                kernel::Submit {
                    op: Token::new(1),
                    kind: kernel::Op::PipeWrite { fd: output, bytes: self.bytes.clone(), from: self.written },
                }
            } else if self.opening > 0 {
                kernel::Submit {
                    op: Token::new(5),
                    kind: kernel::Op::PipeRead {
                        fd: *self.extra.last().expect("opening input"),
                        buf: vec![0; 32_768].into_boxed_slice(),
                    },
                }
            } else {
                kernel::Submit { op: Token::new(2), kind: kernel::Op::Close { fd: output } }
            }
        } else if let Some(error) = self.error {
            kernel::Submit { op: Token::new(3), kind: kernel::Op::Close { fd: error } }
        } else if let Some(fd) = self.extra.pop() {
            kernel::Submit { op: Token::new(4), kind: kernel::Op::Close { fd } }
        } else {
            return;
        };
        self.submissions.push(next);
        self.pending = true;
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn work_pending(&self, _: Time) -> bool {
        !self.pending && !self.is_empty()
    }
    fn is_empty(&self) -> bool {
        self.output.is_none() && self.error.is_none() && self.extra.is_empty() && !self.pending
    }
    fn operations(&self) -> u32 {
        1
    }
}

/// Child startup reading a bounded fake-checkout result through its own root.
/// The machine owns the checkout; this process owns only file and pipe IO.
pub struct Prepared {
    pipes: Vec<(u32, kernel::Fd)>,
    root: Option<kernel::Fd>,
    file: Option<kernel::Fd>,
    bytes: Vec<u8>,
    child: Option<Child>,
    reading: bool,
    closes: u32,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
}
impl Prepared {
    /// Adopt independently owned result, signal and output descriptors.
    #[must_use]
    pub fn new(inherited: &skein_world::Inherited) -> Self {
        let root = inherited.roots.iter().find(|(name, _)| name.as_ref() == b"result").expect("prepared git result").1;
        let mut submissions = Queue::with_capacity(32);
        submissions.push(kernel::Submit {
            op: Token::new(101),
            kind: kernel::Op::Open { root, path: b"result".as_slice().into(), how: kernel::OpenHow::Read },
        });
        submissions.push(kernel::Submit { op: Token::new(105), kind: kernel::Op::Close { fd: inherited.signal } });
        Self {
            pipes: inherited.pipes.clone(),
            root: Some(root),
            file: None,
            bytes: Vec::with_capacity(32_769),
            child: None,
            reading: true,
            closes: 1,
            completions: Queue::with_capacity(32),
            submissions,
        }
    }
    fn read(&mut self) {
        self.submissions.push(kernel::Submit {
            op: Token::new(102),
            kind: kernel::Op::Read {
                fd: self.file.expect("result file"),
                at: self.bytes.len() as u64,
                buf: vec![0; 32_769].into_boxed_slice(),
            },
        });
        self.reading = true;
    }
}
impl Host for Prepared {
    fn iterate(&mut self, now: Time, wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            match complete.op.raw() {
                101 => {
                    let kernel::Done::Fd(fd) = complete.result.expect("open fake result") else {
                        panic!("result is a file")
                    };
                    self.file = Some(fd);
                    self.submissions.push(kernel::Submit {
                        op: Token::new(103),
                        kind: kernel::Op::Close { fd: self.root.take().expect("one result root") },
                    });
                    self.closes += 1;
                    self.reading = false;
                }
                102 => {
                    let kernel::Done::Count(count) = complete.result.expect("read fake result") else {
                        panic!("result read count")
                    };
                    let kernel::Op::Read { buf, .. } = complete.kind else { unreachable!() };
                    self.reading = false;
                    if count == 0 {
                        let mut bytes = std::mem::take(&mut self.bytes);
                        assert!(!bytes.is_empty());
                        let code = bytes.remove(0);
                        self.child = Some(Child::new(&self.pipes, bytes.into_boxed_slice(), code));
                        self.submissions.push(kernel::Submit {
                            op: Token::new(104),
                            kind: kernel::Op::Close { fd: self.file.take().expect("one result file") },
                        });
                        self.closes += 1;
                    } else {
                        assert!(self.bytes.len() + count as usize <= 32_769, "bounded fake stdout");
                        self.bytes.extend_from_slice(&buf[..count as usize]);
                    }
                }
                103..=105 => {
                    assert!(complete.result.is_ok());
                    self.closes -= 1;
                }
                _ => self.child.as_mut().expect("child output").completions().push(complete),
            }
        }
        if self.file.is_some() && !self.reading {
            self.read();
        }
        if let Some(child) = &mut self.child {
            child.iterate(now, wall);
            while let Some(submit) = child.submissions().pop() {
                self.submissions.push(submit);
            }
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        (self.file.is_some() && !self.reading)
            || self.child.as_ref().is_some_and(|c| c.work_pending(now))
            || !self.completions.is_empty()
            || !self.submissions.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.child.as_ref().is_some_and(Host::is_empty)
            && self.root.is_none()
            && self.file.is_none()
            && self.closes == 0
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.is_empty().then(|| kernel::Exit::Code(self.child.as_ref().expect("settled child").code()))
    }
    fn operations(&self) -> u32 {
        4
    }
}

/// Exact git arguments select a private result directory in the fake namespace.
#[must_use]
pub fn roots(spawn: &kernel::Spawn) -> Vec<skein_world::StartupRoot> {
    let mut path = b".smith-test/git/".to_vec();
    path.extend_from_slice(&serde_json::to_vec(&spawn.args).expect("bounded git arguments"));
    vec![skein_world::StartupRoot { name: b"result".as_slice().into(), path: path.into_boxed_slice() }]
}

/// Only the simulated backend registers this adapter; the real loop runs git.
#[must_use]
pub fn make(_spawn: &kernel::Spawn, inherited: &skein_world::Inherited) -> crate::process::Proc {
    crate::process::Proc::Git(Box::new(Prepared::new(inherited)))
}
