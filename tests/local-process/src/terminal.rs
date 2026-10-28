//! A scripted person's terminal over real child pipes. It retains only IO,
//! bounded commands and observed output, never a simulator. The world binds
//! the local child; a real loop can spawn the binary with the same Host.

use std::collections::VecDeque;

use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_world::Host;

const OWNER: Token = Token::new(1);
const ROOT_CLOSE: Token = Token::new(1 << 63);
const OUTPUT_READ: u32 = 4096;

/// One person's bounded input, output wait, signal or end-of-input action.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Send these terminal bytes once output credit is granted.
    Send(Box<[u8]>),
    /// Continue after these bytes appeared on the terminal.
    Wait(Box<[u8]>),
    /// Request an interrupt through the child's blocked termination signal.
    Interrupt,
    /// Finish terminal input while continuing to drain its output.
    Eof,
}

/// Simulator-free terminal process implementing the shared Host boundary.
pub struct Terminal {
    terminal: Option<kernel::Fd>,
    terminal_reading: bool,
    terminal_writing: bool,
    terminal_ended: bool,
    terminal_closing: bool,
    io: io::Io,
    env: Env<io::Limits>,
    root: Option<kernel::Fd>,
    root_closed: bool,
    child: Option<Token>,
    input: Option<Token>,
    output: Option<Token>,
    error: Option<Token>,
    commands: VecDeque<Action>,
    sending: Option<Box<[u8]>>,
    shown: Vec<u8>,
    wait_searched: usize,
    errors: Vec<u8>,
    exit: Option<kernel::Exit>,
    right: u64,
    interrupt: bool,
    terminate_on_end: bool,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
}

impl Terminal {
    #[must_use]
    pub fn new(root: kernel::Fd, commands: Vec<Action>) -> Self {
        Self::configured(root, commands, Box::new([]))
    }

    /// Launch the same terminal story with immutable program arguments.
    #[must_use]
    pub fn configured(root: kernel::Fd, commands: Vec<Action>, arguments: Box<[Box<[u8]>]>) -> Self {
        Self::with_environment(root, commands, arguments, Box::new([]))
    }

    /// Launch with the story's named environment, isolated from the test process.
    #[must_use]
    pub fn with_environment(
        root: kernel::Fd,
        commands: Vec<Action>,
        arguments: Box<[Box<[u8]>]>,
        environment: Box<[Box<[u8]>]>,
    ) -> Self {
        assert!(commands.len() <= 16);
        assert!(
            commands
                .iter()
                .map(|command| match command {
                    Action::Send(bytes) | Action::Wait(bytes) => bytes.len(),
                    _ => 0,
                })
                .sum::<usize>()
                <= 32_768
        );
        let limits = io::Limits {
            sockets: 4,
            names: 0,
            refusals: 1,
            intake: 256,
            receive: 256,
            output: 8192,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        };
        let mut requests = Queue::with_capacity(128);
        requests.push(io::Request::Spawn {
            owner: OWNER,
            spawn: kernel::Spawn {
                program: b"smith-local".as_slice().into(),
                args: arguments,
                env: environment,
                root,
                dir: b".".as_slice().into(),
                pipes: Box::new([
                    kernel::Pipe { child: 0, way: kernel::Way::In, parent: None },
                    kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None },
                    kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None },
                ]),
            },
        });
        Self {
            terminal: None,
            terminal_reading: false,
            terminal_writing: false,
            terminal_ended: false,
            terminal_closing: false,
            io: io::Io::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            root: Some(root),
            root_closed: false,
            child: None,
            input: None,
            output: None,
            error: None,
            commands: commands.into(),
            sending: None,
            shown: Vec::new(),
            wait_searched: 0,
            errors: Vec::new(),
            exit: None,
            right: 1,
            interrupt: false,
            terminate_on_end: false,
            completions: Queue::with_capacity(128),
            submissions: Queue::with_capacity(128),
            events: Queue::with_capacity(128),
            requests,
        }
    }

    /// The same person's script on the controlling terminal of a shipped binary.
    #[must_use]
    pub fn attached(stream: kernel::Fd, commands: Vec<Action>) -> Self {
        let mut person = Self::new(stream, commands);
        while person.requests.pop().is_some() {}
        person.root = None;
        person.root_closed = true;
        person.terminal = Some(stream);
        person
    }

    /// The same scripted person on a shipped binary's separate input and output pipes.
    #[must_use]
    pub fn piped(input: kernel::Fd, output: kernel::Fd, commands: Vec<Action>) -> Self {
        let mut person = Self::new(input, commands);
        while person.requests.pop().is_some() {}
        person.root = None;
        person.root_closed = true;
        person.env.limits.intake = OUTPUT_READ;
        person.env.limits.receive = OUTPUT_READ;
        person.io = io::Io::new(&person.env.limits);
        person.input = Some(person.io.adopt_write_pipe(input, None).expect("person input pipe"));
        let output = person.io.adopt_read_pipe(output).expect("person output pipe");
        person.output = Some(output);
        person.read(output);
        person
    }

    /// Insert a line supplied by the scripted person's observed peer cue.
    pub fn type_line(&mut self, line: Box<[u8]>) {
        self.commands.push_front(Action::Send(line));
    }

    /// The binary observer supplies only the child's externally observed exit.
    pub fn finished(&mut self, exit: kernel::Exit) {
        self.exit = Some(exit);
        self.commands.clear();
        if self.sending.is_none()
            && let Some(input) = self.input.take()
        {
            self.requests.push(io::Request::Close { entity: input });
        }
    }

    fn terminal_iterate(&mut self) {
        while let Some(complete) = self.completions.pop() {
            match complete.kind {
                kernel::Op::PipeRead { buf, .. } => {
                    self.terminal_reading = false;
                    match complete.result {
                        Ok(kernel::Done::Count(0)) | Err(kernel::Error::Other(5)) => self.terminal_ended = true,
                        Ok(kernel::Done::Count(count)) => {
                            let count = usize::try_from(count).expect("terminal read size");
                            assert!(self.shown.len() + count <= 65_536, "bounded terminal observations");
                            self.shown.extend_from_slice(&buf[..count]);
                        }
                        result => panic!("terminal read: {result:?}"),
                    }
                }
                kernel::Op::PipeWrite { bytes: buf, .. } => {
                    self.terminal_writing = false;
                    let Ok(kernel::Done::Count(count)) = complete.result else {
                        panic!(
                            "terminal write failed: {:?}; exit: {:?}; terminal ended: {}",
                            complete.result, self.exit, self.terminal_ended
                        )
                    };
                    let count = usize::try_from(count).expect("terminal write size");
                    assert!(count > 0 && count <= buf.len());
                    if count < buf.len() {
                        self.sending = Some(buf[count..].into());
                    }
                }
                kernel::Op::Close { .. } => {
                    assert!(complete.result.is_ok());
                    self.terminal = None;
                    self.terminal_closing = false;
                }
                _ => unreachable!("person submits terminal read, write and close only"),
            }
        }
        let Some(stream) = self.terminal else { return };
        if self.terminal_ended {
            if self.exit.is_some() && !self.terminal_writing && !self.terminal_closing {
                self.terminal_closing = true;
                self.submissions.push(kernel::Submit { op: Token::new(3), kind: kernel::Op::Close { fd: stream } });
            }
            return;
        }
        if !self.terminal_reading {
            self.terminal_reading = true;
            self.submissions.push(kernel::Submit {
                op: Token::new(1),
                kind: kernel::Op::PipeRead { fd: stream, buf: vec![0; 4096].into() },
            });
        }
        if !self.terminal_writing && self.exit.is_none() {
            if self.sending.is_none() {
                for _ in 0..16 {
                    let Some(action) = self.commands.front() else { break };
                    if let Action::Wait(needle) = action
                        && !appeared(&self.shown, needle, &mut self.wait_searched)
                    {
                        break;
                    }
                    self.wait_searched = 0;
                    match self.commands.pop_front().expect("script action") {
                        Action::Send(bytes) => {
                            self.sending = Some(bytes);
                            break;
                        }
                        Action::Wait(_) => {}
                        Action::Eof => {
                            self.sending = Some(Box::new([4]));
                            break;
                        }
                        Action::Interrupt => panic!("terminal interrupts are outside this pass"),
                    }
                }
            }
            if let Some(bytes) = self.sending.take() {
                self.terminal_writing = true;
                self.submissions.push(kernel::Submit {
                    op: Token::new(2),
                    kind: kernel::Op::PipeWrite { fd: stream, bytes, from: 0 },
                });
            }
        }
    }

    #[must_use]
    pub fn shown(&self) -> &[u8] {
        &self.shown
    }

    #[must_use]
    pub fn errors(&self) -> &[u8] {
        &self.errors
    }

    #[must_use]
    pub fn exit(&self) -> Option<kernel::Exit> {
        self.exit
    }

    /// Signal the product after its terminal writer reports end of output.
    pub fn terminate_when_output_ends(&mut self) {
        self.terminate_on_end = true;
    }

    /// A story schedules the person's signal from a peer observation.
    pub fn interrupt(&mut self) {
        self.interrupt = true;
    }

    fn read(&mut self, stream: Token) {
        self.requests.push(io::Request::Stream {
            stream,
            down: Down::Demand { read: Read::Arrived { max: OUTPUT_READ }, room: 0 },
        });
    }

    fn event(&mut self, event: io::Event) {
        match event {
            io::Event::Spawned { child, pipes, .. } => {
                self.child = Some(child);
                self.input = Some(pipes[0]);
                self.output = Some(pipes[1]);
                self.error = Some(pipes[2]);
                self.read(pipes[1]);
                self.read(pipes[2]);
            }
            io::Event::Output { owner, up: OutputUp::Settled { right, outcome } } => {
                assert_eq!(Some(owner), self.input);
                let bytes = self.sending.take().expect("one terminal output right");
                match outcome {
                    OutputOutcome::Granted => self
                        .requests
                        .push(io::Request::Output { stream: owner, down: OutputDown::Send { right, bytes } }),
                    OutputOutcome::Cancelled | OutputOutcome::Failed(_) => {
                        assert!(self.exit.is_some(), "live terminal input refused")
                    }
                }
            }
            io::Event::Stream { owner, up: Up::Bytes(bytes) } => {
                let target = if Some(owner) == self.output {
                    &mut self.shown
                } else {
                    assert_eq!(Some(owner), self.error);
                    &mut self.errors
                };
                assert!(target.len() + bytes.len() <= 65_536, "bounded terminal observations");
                target.extend_from_slice(&bytes);
                self.read(owner);
            }
            io::Event::Stream { owner, up: Up::End | Up::Failed(_) } => {
                if Some(owner) == self.output && self.terminate_on_end {
                    self.interrupt = true;
                    self.terminate_on_end = false;
                }
                self.requests.push(io::Request::Close { entity: owner });
            }
            io::Event::Exited { exit, .. } => {
                self.exit = Some(exit);
                self.commands.clear();
                if let Some(input) = self.input.take() {
                    self.requests.push(io::Request::Close { entity: input });
                }
                self.requests.push(io::Request::Close { entity: self.child.expect("spawned child") });
            }
            io::Event::Closed { owner } if Some(owner) == self.output => self.output = None,
            io::Event::Closed { owner } if Some(owner) == self.error => self.error = None,
            io::Event::Closed { owner: OWNER } => self.child = None,
            io::Event::Failed { .. } => panic!("scripted local invocation failed to spawn"),
            _ => {}
        }
    }

    fn commands(&mut self) {
        if self.input.is_none() || self.sending.is_some() || self.exit.is_some() {
            return;
        }
        for _ in 0..16 {
            let Some(command) = self.commands.front() else { break };
            match command {
                Action::Wait(needle) if !appeared(&self.shown, needle, &mut self.wait_searched) => {
                    break;
                }
                _ => {}
            }
            self.wait_searched = 0;
            match self.commands.pop_front().expect("announced command") {
                Action::Send(bytes) => {
                    assert!(bytes.len() <= 8192 && !bytes.is_empty());
                    let right = Token::new(self.right);
                    self.right = self.right.checked_add(1).expect("bounded terminal script");
                    self.requests.push(io::Request::Output {
                        stream: self.input.expect("open input"),
                        down: OutputDown::Room { right, bytes: u32::try_from(bytes.len()).expect("bounded input") },
                    });
                    self.sending = Some(bytes);
                    break;
                }
                Action::Wait(_) => {}
                Action::Interrupt => self.interrupt(),
                Action::Eof => {
                    self.requests.push(io::Request::Close { entity: self.input.take().expect("open input") });
                    break;
                }
            }
        }
    }
}

impl Host for Terminal {
    fn iterate(&mut self, now: Time, wall: Wall) {
        if self.terminal.is_some() || self.terminal_ended {
            self.terminal_iterate();
            return;
        }
        self.env.now = now;
        self.env.wall = wall;
        while self.io.is_ready() {
            io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            if complete.op == ROOT_CLOSE {
                assert!(complete.result.is_ok());
                self.root_closed = true;
            } else {
                io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
            }
        }
        for _ in 0..16 {
            if self.io.is_ready() {
                io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
            if self.io.is_due(now) {
                io::fire(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
        }
        while let Some(event) = self.events.pop() {
            self.event(event);
        }
        if (self.child.is_some() || self.exit.is_some())
            && let Some(fd) = self.root.take()
        {
            self.submissions.push(kernel::Submit { op: ROOT_CLOSE, kind: kernel::Op::Close { fd } });
        }
        self.commands();
        if self.interrupt
            && let Some(child) = self.child
        {
            self.interrupt = false;
            self.requests.push(io::Request::Signal {
                child,
                to: kernel::Target::Child,
                signal: kernel::Signal::Terminate,
            });
        }
        while self.io.takes() {
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &self.env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        if self.terminal.is_some() || self.terminal_ended {
            return !self.completions.is_empty()
                || (self.terminal_ended && self.exit.is_some() && self.terminal.is_some() && !self.terminal_closing);
        }
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.completions.is_empty()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || self.interrupt
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        if self.terminal.is_some() || self.terminal_ended {
            return self.terminal.is_none()
                && self.exit.is_some()
                && self.submissions.is_empty()
                && self.completions.is_empty();
        }
        self.exit.is_some()
            && self.root_closed
            && self.io.is_empty()
            && self.requests.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn operations(&self) -> u32 {
        io::operations(&self.env.limits).expect("terminal operations") + 1
    }
}

fn appeared(shown: &[u8], needle: &[u8], searched: &mut usize) -> bool {
    assert!(!needle.is_empty(), "a terminal wait names output");
    if *searched == shown.len() {
        return false;
    }
    let from = searched.saturating_sub(needle.len() - 1);
    let found = shown[from..].windows(needle.len()).any(|part| part == needle);
    *searched = shown.len();
    found
}

#[cfg(test)]
mod tests {
    use super::{Action, Terminal, appeared};
    use skein_io::kernel::Fd;
    use skein_lib::Token;

    #[test]
    fn terminal_wait_finds_each_split_match() {
        let shown = b"before\xffanswer\0after";
        let needle = b"\xffanswer\0";
        for split in 0..=shown.len() {
            let mut searched = 0;
            let first = appeared(&shown[..split], needle, &mut searched);
            assert!(first || appeared(shown, needle, &mut searched), "split {split}");
        }
    }

    #[test]
    fn a_later_wait_can_match_previously_shown_output() {
        let mut person = Terminal::new(
            Fd::new(10),
            vec![Action::Wait(b"second answer".as_slice().into()), Action::Wait(b"first answer".as_slice().into())],
        );
        person.input = Some(Token::new(1));
        person.shown.extend_from_slice(b"first answer then second answer");
        person.commands();
        assert!(person.commands.is_empty(), "both waits can use the same output history");
    }
}
