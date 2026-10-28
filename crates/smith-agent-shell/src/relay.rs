//! Kernel and hosted glue for the relay (protocol/programs.md, sections 5.3, 9).
//! Keeps I/O queues, the relay and one fixed diagnostic; knows no MCP policy.
//! `Process::new` transfers startup descriptors and `main` drives it unchanged.

use skein_io::{
    self as io, Event, Io, Request,
    kernel::{Complete, Exit, Fd, Submit},
};
use skein_lib::{
    Duration, Env, Queue, Time, Token, Wall,
    stream::{Down, Read, Up},
};
use skein_shell::Host;
use smith_protocol_program::relay::{self, Relay};
use std::{os::unix::ffi::OsStrExt, process::ExitCode};

pub use smith_protocol_program::mcp::RELAY_FLAG as FLAG;

const QUEUE: u32 = 64;
const SOCKET: Token = Token::new(100);
const USAGE: &[u8] = b"smith-agent: usage: smith-agent --mcp-relay 127.0.0.1:PORT, its secret in SMITH_MCP_SECRET\n";

/// Owned descriptors transferred by kernel startup or the hosted factory (9).
#[derive(Debug)]
pub struct Faces {
    pub stdin: Fd,
    pub stdout: Fd,
    pub stderr: Option<Fd>,
    pub signal: Fd,
}

/// The shipped relay process, also hosted by simulated worlds (programs.md, 9).
pub struct Process {
    io: Io,
    relay: Option<Relay>,
    signal: Token,
    error: Option<Token>,
    diagnostic: Option<Box<[u8]>>,
    diagnostic_waiting: bool,
    diagnostic_done: bool,
    code: Option<u8>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    events: Queue<Event>,
    requests: Queue<Request>,
}

impl std::fmt::Debug for Process {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Process").field("relay", &self.relay).field("code", &self.code).finish_non_exhaustive()
    }
}

fn limits() -> io::Limits {
    io::Limits {
        sockets: 5,
        names: 0,
        refusals: 1,
        intake: 4096,
        receive: 4096,
        output: 4096,
        sends: 1,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    }
}

/// Kernel ring size for the relay's I/O limits (protocol/programs.md, section 9).
#[must_use]
pub fn operations() -> u32 {
    io::operations(&limits()).expect("relay operation bound")
}

impl Process {
    /// A usage error writes one stderr line and settles with exit 1 (programs.md, 9).
    #[must_use]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "startup transfers ownership of these descriptors to the process"
    )]
    pub fn new(arguments: &[Box<[u8]>], environment: &[Box<[u8]>], faces: Faces) -> Self {
        let mut io = Io::new(&limits());
        let input = io.adopt_read_pipe(faces.stdin).expect("relay input slot");
        let output = io.adopt_write_pipe(faces.stdout, None).expect("relay output slot");
        let error = faces.stderr.map(|fd| io.adopt_write_pipe(fd, None).expect("relay stderr slot"));
        let signal = io.adopt_signals(faces.signal).expect("relay signal slot");
        let mut requests = Queue::with_capacity(QUEUE);
        let (relay, diagnostic, code) = if let Ok(target) = relay::parse(arguments, environment) {
            (Some(Relay::new(target, input, output, SOCKET, &mut requests)), None, None)
        } else {
            requests.push(Request::Abort { entity: input });
            requests.push(Request::Abort { entity: output });
            requests.push(Request::Abort { entity: signal });
            (None, Some(Box::from(USAGE)), Some(1))
        };
        Self {
            io,
            relay,
            signal,
            error,
            diagnostic,
            diagnostic_waiting: false,
            diagnostic_done: false,
            code,
            completions: Queue::with_capacity(QUEUE),
            submissions: Queue::with_capacity(QUEUE),
            events: Queue::with_capacity(QUEUE),
            requests,
        }
    }

    /// The fixed line written on stderr for a refusal or failure (programs.md, 9).
    #[must_use]
    pub fn detail(&self) -> Option<&'static [u8]> {
        match &self.relay {
            Some(relay) => relay.detail(),
            None => Some(USAGE),
        }
    }

    fn event(&mut self, event: Event) {
        if self.relay.as_ref().is_some_and(|relay| owner(&event).is_some_and(|owner| relay.owns(owner))) {
            self.relay.as_mut().expect("relay owns event").from_io(event, &mut self.requests);
            return;
        }
        match event {
            Event::Shutdown { .. } => {
                if let Some(relay) = &mut self.relay {
                    relay.terminate(&mut self.requests);
                }
            }
            Event::Stream { owner, up } if Some(owner) == self.error => match up {
                Up::Room => {
                    self.diagnostic_waiting = false;
                    let bytes = self.diagnostic.take().expect("stderr line requested room");
                    self.requests.push(Request::Stream { stream: owner, down: Down::Send(bytes) });
                    self.requests.push(Request::Close { entity: owner });
                    self.diagnostic_done = true;
                }
                Up::End | Up::Failed(_) => {
                    self.diagnostic_done = true;
                    self.diagnostic = None;
                    self.requests.push(Request::Abort { entity: owner });
                }
                Up::Bytes(_) => unreachable!("stderr is write-only"),
            },
            Event::Closed { owner } if Some(owner) == self.error => self.error = None,
            Event::Listening { .. }
            | Event::Accepted { .. }
            | Event::Connecting { .. }
            | Event::Connected { .. }
            | Event::Stream { .. }
            | Event::Output { .. }
            | Event::Spawned { .. }
            | Event::Exited { .. }
            | Event::Usage { .. }
            | Event::Resized
            | Event::Window { .. }
            | Event::Failed { .. }
            | Event::Closed { .. } => {}
        }
    }

    fn progress(&mut self) {
        if let Some(relay) = &self.relay {
            if self.diagnostic.is_none() && !self.diagnostic_done {
                self.diagnostic = self.detail().map(Box::from);
            }
            if self.code.is_none()
                && let Some(exit) = relay.ended()
            {
                self.code = Some(exit.code());
                self.requests.push(Request::Abort { entity: self.signal });
            }
        }
        if !self.diagnostic_waiting {
            if let Some(error) = self.error {
                if let Some(bytes) = &self.diagnostic {
                    self.diagnostic_waiting = true;
                    self.requests.push(Request::Stream {
                        stream: error,
                        down: Down::Demand {
                            read: Read::Nothing,
                            room: u32::try_from(bytes.len()).expect("fixed diagnostic"),
                        },
                    });
                } else if self.code.is_some() && !self.diagnostic_done {
                    self.diagnostic_done = true;
                    self.requests.push(Request::Close { entity: error });
                }
            } else {
                self.diagnostic = None;
                self.diagnostic_done = true;
            }
        }
    }
}

fn owner(event: &Event) -> Option<Token> {
    match event {
        Event::Listening { owner, .. }
        | Event::Accepted { owner, .. }
        | Event::Connecting { owner, .. }
        | Event::Connected { owner }
        | Event::Stream { owner, .. }
        | Event::Output { owner, .. }
        | Event::Spawned { owner, .. }
        | Event::Exited { owner, .. }
        | Event::Failed { owner, .. }
        | Event::Closed { owner }
        | Event::Usage { owner, .. }
        | Event::Window { owner, .. } => Some(*owner),
        Event::Shutdown { .. } | Event::Resized => None,
    }
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: limits() };
        while self.io.is_ready() {
            io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
        }
        while self.io.is_due(now) {
            io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(event) = self.events.pop() {
            self.event(event);
        }
        self.progress();
        while let Some(request) = self.requests.pop() {
            io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        self.io.is_ready() || !self.completions.is_empty() || !self.events.is_empty() || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.code.is_some()
            && self.io.is_empty()
            && self.diagnostic.is_none()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
    }
    fn exit(&self) -> Option<Exit> {
        if self.is_empty() { self.code.map(Exit::Code) } else { None }
    }
    fn operations(&self) -> u32 {
        operations()
    }
}

/// Drive the shipped relay with arguments, environment, signals and the kernel (9).
#[must_use]
pub fn main() -> ExitCode {
    let arguments: Vec<Box<[u8]>> = std::env::args_os().skip(1).map(|argument| argument.as_bytes().into()).collect();
    let environment: Vec<Box<[u8]>> = std::env::vars_os()
        .map(|(name, value)| [name.as_bytes(), b"=", value.as_bytes()].concat().into_boxed_slice())
        .collect();
    let Ok(signal) = skein_shell::open_termination_signals() else {
        eprintln!("smith-agent: the relay's connection failed");
        return ExitCode::FAILURE;
    };
    let mut process = Process::new(
        &arguments,
        &environment,
        Faces { stdin: Fd::new(0), stdout: Fd::new(1), stderr: Some(Fd::new(2)), signal },
    );
    let Ok(mut kernel) = skein_shell::Kernel::open(skein_shell::Config { operations: operations() }) else {
        eprintln!("smith-agent: the relay's connection failed");
        return ExitCode::FAILURE;
    };
    match skein_shell::drive(&mut kernel, &skein_shell::Clock::new(), &mut process) {
        Exit::Code(code) => ExitCode::from(code),
        Exit::Signal(_) => ExitCode::FAILURE,
    }
}
