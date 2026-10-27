//! One invocation's I/O and settlement (protocol/programs.md, sections 5 and 8;
//! testing.md, section 4). Startup loads a story and appends names before
//! entering a command; terminal exits wait for every descriptor to close.

use skein_io::{
    Event, Io, Limits, Request, file,
    file_layer::{self, FileIo},
    kernel::{self, Complete, Fd, Submit},
};
use skein_lib::{
    Duration, Env, Queue, Time, Token, Wall,
    stream::{Down, Read, Up},
};
use skein_shell::Host;
use std::collections::VecDeque;

use crate::{
    Observation, STORY_FILE, Story,
    commands::{self, Command, Config, Effect, FILE_OWNER},
    output::Output,
};

pub(crate) const FILE_BYTES: u32 = 65_536;
pub(crate) const OUTPUT_BYTES: usize = 65_536;
const FILE_BIT: u64 = 1 << 63;
const CLOSE_ROOT: Token = Token::new(2);
const QUEUE: u32 = 64;
const FILE_TIMEOUT: Duration = Duration::from_secs(1);

/// Descriptors transferred by the kernel entry point or hosted factory.
/// `observations` appends beneath `config`; `mcp` roots invocation files and allowed workspace actions.
#[derive(Debug)]
pub struct Faces {
    pub stdin: Option<Fd>,
    pub stdout: Fd,
    pub stderr: Fd,
    pub signal: Fd,
    pub config: Option<Fd>,
    pub observations: Option<Fd>,
    pub mcp: Option<Fd>,
}

/// One fake invocation, driven unchanged by a world or the kernel loop.
/// Commands own no home-directory state (protocol/programs.md, section 8).
#[derive(Debug)]
pub struct Fake {
    io: Io,
    files: FileIo,
    limits: Limits,
    config: Config,
    input: Option<Token>,
    input_ended: bool,
    // At most one FILE_BYTES delivery, consumed only when the command asks.
    input_bytes: VecDeque<u8>,
    input_requested: bool,
    input_demanding: bool,
    signal: Token,
    stdout: Output,
    stderr: Output,
    observation: Output,
    arguments: Vec<String>,
    directory: std::path::PathBuf,
    story: Option<Story>,
    command: Option<Command>,
    code: Option<u8>,
    root_closing: bool,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    io_events: Queue<Event>,
    io_requests: Queue<Request>,
    file_events: Queue<file::Event>,
    file_submissions: Queue<Submit>,
    file_request: Option<file::Request>,
    memory: u64,
    environment: Vec<Box<[u8]>>,
    mcp: crate::mcp::Mcp,
    now: Time,
    delayed_init: Option<(Time, Vec<u8>)>,
}

pub(crate) fn limits() -> Limits {
    Limits {
        sockets: 8,
        names: 0,
        refusals: 1,
        intake: FILE_BYTES,
        receive: FILE_BYTES,
        output: u32::try_from(OUTPUT_BYTES).expect("output limit fits"),
        sends: 2,
        accepts: 1,
        backlog: 1,
        close_timeout: FILE_TIMEOUT,
        retry: Duration::from_millis(10),
    }
}

pub(crate) fn operations() -> u32 {
    skein_io::operations(&limits()).expect("small I/O limits").checked_add(2).expect("file operation and cancel")
}

impl Fake {
    /// The one-based turn currently held inside a call or scripted hang.
    #[must_use]
    pub fn active_turn(&self) -> Option<u32> {
        match &self.command {
            Some(Command::Print(print)) => print.active_turn(),
            Some(Command::Status(_) | Command::Login(_) | Command::Logout(_) | Command::Unknown) | None => None,
        }
    }

    /// The scripted call awaiting its MCP reply, without its input or credentials.
    #[must_use]
    pub fn pending_tool(&self) -> Option<&str> {
        match &self.command {
            Some(Command::Print(print)) => print.pending_tool(),
            Some(Command::Status(_) | Command::Login(_) | Command::Logout(_) | Command::Unknown) | None => None,
        }
    }

    /// Takes the invocation's arguments (without argv[0]), `NAME=value`
    /// environment entries and owned startup descriptors.
    pub fn new(arguments: &[Box<[u8]>], environment: &[Box<[u8]>], faces: Faces) -> Self {
        Self::new_in(arguments, environment, faces, std::path::Path::new("."))
    }

    pub(crate) fn new_in(
        arguments: &[Box<[u8]>],
        environment: &[Box<[u8]>],
        faces: Faces,
        directory: &std::path::Path,
    ) -> Self {
        let command_arguments = crate::faces::file_arguments(arguments, directory).map(|files| files.arguments);
        let limits = limits();
        let mut io = Io::new(&limits);
        let input = faces.stdin.map(|fd| io.adopt_read_pipe(fd).expect("input slot"));
        let stdout = Output::new(Some(io.adopt_write_pipe(faces.stdout, None).expect("stdout slot")));
        let stderr = Output::new(Some(io.adopt_write_pipe(faces.stderr, None).expect("stderr slot")));
        let signal = io.adopt_signals(faces.signal).expect("signal slot");
        let mut observation =
            Output::new(faces.observations.map(|fd| io.adopt_append(fd, FILE_TIMEOUT).expect("observation slot")));
        let mut files = FileIo::new(2, FILE_BYTES, 1, FILE_TIMEOUT);
        files.seed_randomness(7);
        let config = Config {
            root: faces.config.map(|fd| files.adopt_root(fd).expect("root slot")),
            directory: faces.config,
            mcp_directory: faces.mcp,
            mcp: faces.mcp.map(|fd| files.adopt_root(fd).expect("MCP root slot")),
        };
        let arguments: Vec<_> = arguments.iter().map(|bytes| String::from_utf8_lossy(bytes).into_owned()).collect();
        let observed = Observation {
            arguments: arguments.clone(),
            environment: environment
                .iter()
                .map(|entry| {
                    let name = entry.split(|byte| *byte == b'=').next().expect("an entry has a name");
                    String::from_utf8_lossy(name).into_owned()
                })
                .collect(),
        };
        // File content is bounded by FILE_BYTES. This envelope includes its
        // JSON expansion, command/output copies, queues and both I/O drivers.
        // Invocation text is supplied by the caller and counted separately.
        let invocation = observed
            .arguments
            .iter()
            .chain(&observed.environment)
            .map(String::len)
            .chain(environment.iter().map(|entry| entry.len()))
            .try_fold(0_u64, |size, length| {
                size.checked_add(u64::try_from(length).expect("text length fits"))?.checked_add(32)
            })
            .expect("invocation length fits");
        let memory = 4_194_304_u64
            .checked_add(invocation.checked_mul(16).expect("invocation copies fit"))
            .expect("fake memory fits");
        let configured = crate::faces::config_path(environment).is_some();
        if configured && config.root.is_some() {
            assert!(observation.token().is_some(), "a configuration root has an observations append descriptor");
            let mut bytes = serde_json::to_vec(&observed).expect("observation is JSON data");
            bytes.push(b'\n');
            observation.push(&bytes);
        }
        let file_request = if configured {
            config.root.map(|root| file::Request::Load {
                owner: FILE_OWNER,
                root,
                path: STORY_FILE.as_bytes().into(),
                max: FILE_BYTES,
                no_follow: true,
            })
        } else {
            None
        };
        let story = if file_request.is_none() { Some(Story::default()) } else { None };
        let arguments = match command_arguments {
            Some(arguments) => arguments.iter().map(|bytes| String::from_utf8_lossy(bytes).into_owned()).collect(),
            None => arguments,
        };
        let mut fake = Self {
            io,
            files,
            limits,
            config,
            input,
            input_ended: input.is_none(),
            input_bytes: VecDeque::new(),
            input_requested: false,
            input_demanding: false,
            signal,
            stdout,
            stderr,
            observation,
            arguments,
            directory: directory.to_path_buf(),
            story,
            command: None,
            code: None,
            root_closing: false,
            completions: Queue::with_capacity(QUEUE),
            submissions: Queue::with_capacity(QUEUE),
            io_events: Queue::with_capacity(QUEUE),
            io_requests: Queue::with_capacity(QUEUE),
            file_events: Queue::with_capacity(QUEUE),
            file_submissions: Queue::with_capacity(QUEUE),
            file_request,
            memory,
            environment: environment.to_vec(),
            mcp: crate::mcp::Mcp::new(),
            now: Time::ZERO,
            delayed_init: None,
        };
        if !configured {
            fake.effects(commands::error("CLAUDE_CONFIG_DIR is not set", 2));
        }
        fake
    }

    /// Add the binary's injected process number to its invocation observation.
    pub fn record_process(&mut self, process: u32) {
        self.observation.record_process(process);
    }

    fn effects(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Output(bytes) => self.stdout.push(&bytes),
                Effect::Error(bytes) => self.stderr.push(&bytes),
                Effect::File(request) => {
                    assert!(self.file_request.is_none(), "one file request at a time");
                    self.file_request = Some(request);
                }
                Effect::Received(message) => {
                    let mut bytes = serde_json::to_vec(&serde_json::json!({"received":message})).expect("input JSON");
                    bytes.push(b'\n');
                    if self.observation.token().is_some() {
                        self.observation.push(&bytes);
                    }
                }
                Effect::Seen(seen) => {
                    let value = match seen {
                        crate::Seen::Selected { .. } => serde_json::json!({"selection": seen}),
                        _ => serde_json::json!({"seen": seen}),
                    };
                    let mut bytes = serde_json::to_vec(&value).expect("seen JSON");
                    bytes.push(b'\n');
                    if self.observation.token().is_some() {
                        self.observation.push(&bytes);
                    }
                }
                Effect::DelayedInit { after_ms, bytes } => {
                    self.delayed_init = Some((self.now.saturating_add(Duration::from_millis(after_ms)), bytes));
                }
                Effect::ReadCode => self.input_requested = true,
                Effect::McpStart { configuration, directory } => self.mcp.start(
                    configuration,
                    self.config.mcp_directory.expect("MCP root"),
                    directory.as_bytes(),
                    &self.environment,
                    &mut self.io_requests,
                ),
                Effect::McpCall { tool, input } => self.mcp.call(tool, input),
                Effect::McpCancel => self.mcp.cancel(),
                Effect::End(code) => self.end(code),
            }
        }
    }

    fn end(&mut self, code: u8) {
        if self.code.is_none() {
            self.code = Some(code);
            self.delayed_init = None;
            let effects = self.command.as_mut().map(Command::stop).unwrap_or_default();
            self.effects(effects);
            self.mcp.stop();
            self.file_request = None;
            if let Some(input) = self.input {
                self.io_requests.push(Request::Abort { entity: input });
            }
            self.io_requests.push(Request::Abort { entity: self.signal });
            if !self.files.takes() {
                file_layer::cancel(&mut self.files, FILE_OWNER, &mut self.file_submissions);
            }
        }
    }

    fn file_event(&mut self, event: file::Event) {
        if event.owner() == CLOSE_ROOT {
            self.root_closing = false;
            return;
        }
        if self.code.is_some() {
            return;
        }
        if let Some(command) = &mut self.command {
            let effects = command.file(event);
            self.effects(effects);
        } else {
            match event {
                file::Event::Loaded { bytes, .. } => match serde_json::from_slice(&bytes) {
                    Ok(story) => self.story = Some(story),
                    Err(_) => self.effects(commands::error("cannot decode story", 1)),
                },
                file::Event::Failed { error: kernel::Error::NotFound, .. } => self.story = Some(Story::default()),
                _ => self.effects(commands::error("cannot read story", 1)),
            }
        }
    }

    fn io_event(&mut self, event: Event) {
        if self.mcp.owns(&event) {
            self.mcp.event(event, &mut self.io_requests);
            return;
        }
        match event {
            Event::Shutdown { signal: kernel::ServiceSignal::Interrupt, .. } => {
                if self.arguments.iter().any(|argument| argument == "-p") {
                    self.effects(vec![Effect::Seen(crate::Seen::Interrupted)]);
                }
                let effects = match &mut self.command {
                    Some(command) => command.interrupt(),
                    None if self.arguments.iter().any(|argument| argument == "-p") => Vec::new(),
                    None => vec![Effect::End(143)],
                };
                self.effects(effects);
            }
            Event::Shutdown { signal: kernel::ServiceSignal::Terminate, .. } => {
                if self.arguments.iter().any(|argument| argument == "-p") {
                    self.effects(vec![Effect::Seen(crate::Seen::Terminated)]);
                }
                self.end(143);
            }
            Event::Stream { owner, up } if Some(owner) == self.stdout.token() => {
                let failed = matches!(up, Up::Failed(_));
                self.stdout.event(up, &mut self.io_requests);
                if failed {
                    self.end(1);
                }
            }
            Event::Stream { owner, up } if Some(owner) == self.stderr.token() => {
                let failed = matches!(up, Up::Failed(_));
                self.stderr.event(up, &mut self.io_requests);
                if failed {
                    self.end(1);
                }
            }
            Event::Stream { owner, up } if Some(owner) == self.observation.token() => {
                let failed = matches!(up, Up::Failed(_));
                self.observation.event(up, &mut self.io_requests);
                if failed {
                    self.end(1);
                }
            }
            Event::Stream { owner, up } if Some(owner) == self.input && self.code.is_none() => {
                self.input_demanding = false;
                if matches!(up, Up::End | Up::Failed(_)) {
                    self.input_ended = true;
                }
                match up {
                    Up::Bytes(bytes) => {
                        assert!(self.input_bytes.is_empty(), "read only after the last delivery is consumed");
                        assert!(bytes.len() <= FILE_BYTES as usize, "stdin delivery fits its demand");
                        self.input_bytes = bytes.into_vec().into();
                        self.effects(vec![Effect::ReadCode]);
                    }
                    Up::End | Up::Failed(_) => self.effects(vec![Effect::ReadCode]),
                    Up::Room => {}
                }
            }
            Event::Closed { owner } if Some(owner) == self.stdout.token() => self.stdout.closed(),
            Event::Closed { owner } if Some(owner) == self.stderr.token() => self.stderr.closed(),
            Event::Closed { owner } if Some(owner) == self.observation.token() => self.observation.closed(),
            Event::Failed { .. } => self.end(1),
            _ => {}
        }
    }

    fn read_input(&mut self) {
        if self.code.is_some() || !self.input_requested || self.file_request.is_some() {
            return;
        }
        self.input_requested = false;
        if self.input_demanding {
            return;
        }
        if let Some(bytes) = take_input(&mut self.input_bytes) {
            let effects =
                self.command.as_mut().map(|command| command.input(Some(&bytes), self.config)).unwrap_or_default();
            self.effects(effects);
        } else if self.input_ended {
            let effects = self.command.as_mut().map(|command| command.input(None, self.config)).unwrap_or_default();
            self.effects(effects);
        } else {
            self.input_demanding = true;
            self.io_requests.push(Request::Stream {
                stream: self.input.expect("code input is present"),
                down: Down::Demand {
                    // Login accepts CR or LF; JSON framing is buffered above I/O.
                    read: match &self.command {
                        Some(Command::Login(_)) => Read::Line { max: FILE_BYTES },
                        _ => Read::Arrived { max: FILE_BYTES },
                    },
                    room: 0,
                },
            });
        }
    }

    fn progress(&mut self, now: Time) {
        if self.code.is_none()
            && self.command.is_none()
            && self.observation.drained()
            && let Some(story) = self.story.take()
        {
            let (command, effects) = Command::start(&self.arguments, story, self.config, &self.directory);
            self.command = Some(command);
            self.effects(effects);
        }
        if self.files.takes() {
            if let Some(request) = self.file_request.take() {
                file_layer::down(&mut self.files, now, request, &mut self.file_events, &mut self.file_submissions);
            } else if self.code.is_some()
                && let Some(root) = self.config.root.take().or_else(|| self.config.mcp.take())
            {
                self.root_closing = true;
                file_layer::down(
                    &mut self.files,
                    now,
                    file::Request::Close { owner: CLOSE_ROOT, file: root },
                    &mut self.file_events,
                    &mut self.file_submissions,
                );
            }
        }
        // Defer buffered reads until queued file effects have been submitted,
        // just as an I/O demand used to answer on a later loop iteration.
        self.read_input();
        self.mcp.progress(&mut self.io_requests);
        self.stdout.progress(self.code.is_some(), &mut self.io_requests);
        self.stderr.progress(self.code.is_some(), &mut self.io_requests);
        self.observation.progress(self.code.is_some(), &mut self.io_requests);
    }
}

/// File and stream drivers issue independent operation tokens. The file
/// driver's namespace bit also follows cancellation targets in both directions.
fn file_namespace(token: &mut Token, kind: &mut kernel::Op) {
    *token = Token::new(token.raw() ^ FILE_BIT);
    if let kernel::Op::Cancel { target } = kind {
        *target = Token::new(target.raw() ^ FILE_BIT);
    }
}

impl Host for Fake {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.now = now;
        if self.delayed_init.as_ref().is_some_and(|(at, _)| now >= *at) {
            let (_, bytes) = self.delayed_init.take().expect("due init");
            self.stdout.push(&bytes);
            self.effects(vec![Effect::ReadCode]);
        }
        let env = Env { now, wall, limits: self.limits };
        while self.io.is_ready() {
            skein_io::resume(&mut self.io, &env, &mut self.io_events, &mut self.submissions);
        }
        while let Some(mut complete) = self.completions.pop() {
            if complete.op.raw() & FILE_BIT != 0 {
                file_namespace(&mut complete.op, &mut complete.kind);
                file_layer::up(&mut self.files, complete, &mut self.file_events, &mut self.file_submissions);
            } else {
                skein_io::up(&mut self.io, &env, complete, &mut self.io_events, &mut self.submissions);
            }
        }
        while self.io.is_due(now) {
            skein_io::fire(&mut self.io, &env, &mut self.io_events, &mut self.submissions);
        }
        if self.files.is_due(now) {
            file_layer::expire(&mut self.files, now, &mut self.file_submissions);
        }
        while let Some(event) = self.io_events.pop() {
            self.io_event(event);
        }
        while let Some(event) = self.file_events.pop() {
            self.file_event(event);
        }
        while let Some(notice) = self.mcp.notices.pop_front() {
            if self.code.is_none() {
                let effects = self.command.as_mut().map(|command| command.mcp(notice)).unwrap_or_default();
                self.effects(effects);
            }
        }
        self.progress(now);
        while let Some(request) = self.io_requests.pop() {
            skein_io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        while let Some(mut submit) = self.file_submissions.pop() {
            file_namespace(&mut submit.op, &mut submit.kind);
            self.submissions.push(submit);
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
        self.io.is_ready()
            || !self.completions.is_empty()
            || !self.file_events.is_empty()
            || !self.io_events.is_empty()
            || self.file_request.is_some()
            || (self.input_requested && self.code.is_none())
            || (self.code.is_some() && (self.config.root.is_some() || self.config.mcp.is_some()) && self.files.takes())
    }

    fn next_deadline(&self) -> Option<Time> {
        self.io
            .next_deadline()
            .into_iter()
            .chain(self.files.next_deadline())
            .chain(self.delayed_init.as_ref().map(|(at, _)| *at))
            .min()
    }

    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }

    fn is_empty(&self) -> bool {
        self.code.is_some()
            && self.io.is_empty()
            && self.mcp.is_empty()
            && self.files.open_files() == 0
            && self.files.takes()
            && !self.root_closing
            && self.stdout.is_empty()
            && self.stderr.is_empty()
            && self.observation.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.io_events.is_empty()
            && self.file_events.is_empty()
            && self.io_requests.is_empty()
            && self.file_submissions.is_empty()
            && self.file_request.is_none()
    }

    fn exit(&self) -> Option<kernel::Exit> {
        if self.is_empty() { self.code.map(kernel::Exit::Code) } else { None }
    }

    fn worst_case(&self) -> u64 {
        self.memory
    }

    fn operations(&self) -> u32 {
        operations()
    }
}

// A delivery may hold several JSON lines. Keep the remainder until the next
// ReadCode so file stores, tool calls and results keep their original pacing.
fn take_input(bytes: &mut VecDeque<u8>) -> Option<Vec<u8>> {
    if bytes.is_empty() {
        return None;
    }
    let end = bytes.iter().position(|byte| *byte == b'\n').map_or(bytes.len(), |at| at + 1);
    Some(bytes.drain(..end).collect())
}

#[cfg(test)]
mod tests {
    use super::take_input;

    #[test]
    fn one_delivery_keeps_adjacent_json_lines_in_order() {
        let first = b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"first\"}}\n";
        let second = b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"second\"}}\n";
        let mut delivery = [first.as_slice(), second.as_slice()].concat().into();
        assert_eq!(take_input(&mut delivery).as_deref(), Some(first.as_slice()));
        assert_eq!(take_input(&mut delivery).as_deref(), Some(second.as_slice()));
        assert_eq!(take_input(&mut delivery), None);
    }
}
