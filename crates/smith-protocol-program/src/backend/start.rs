//! Lazy child startup and private entropy-derived names (programs.md, 3, 5.1).
//! Keeps no login; the existing status reader classifies subscription use.

use super::{Backend, Below, Phase};
use crate::{Command, Ended, Launch, STATUS, Status, configuration_directory, environment, mcp};
use alloc::boxed::Box;
use skein_io::{self as io, kernel};
use skein_lib::{Decimal, Env, List, Queue, Secret, Token, sha256, stream};
use smith_domain::llm;
use smith_protocol_llm::{self as protocol, FromDomain, ToDomain};

pub(super) struct Names {
    pub mcp: Secret,
    pub run: [u8; 32],
    pub session: [u8; 36],
}

impl Names {
    pub fn new(entropy: &Secret) -> Self {
        let mut mcp_hash = sha256::Sha256::new();
        mcp_hash.update(entropy.expose());
        mcp_hash.update(b"mcp");
        let mcp = Secret::new(Box::from(mcp_hash.finish().as_slice()));
        let mut run_hash = sha256::Sha256::new();
        run_hash.update(entropy.expose());
        run_hash.update(b"run");
        let run_hex = mcp::secret_text(&run_hash.finish());
        let mut run = [0; 32];
        for (target, source) in run.iter_mut().zip(run_hex) {
            *target = source;
        }
        Self { mcp, run, session: [0; 36] }
    }
}

impl Backend {
    pub(super) fn start(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        request: FromDomain,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let (owner, name) = match &request {
            FromDomain::Complete { owner, grant, .. } => (*owner, *grant),
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => unreachable!("start receives Complete"),
        };
        if self.phase != Phase::Idle {
            self.owner = Some(owner);
            self.fail(env, llm::Failure::Invalid, b"smith: a program conversation is already started", up, below);
            return;
        }
        self.begin_prompt(&request);
        self.owner = Some(owner);
        let valid = match &self.grant {
            Some(grant) => match name {
                Some(name) => {
                    name.account == grant.name.account
                        && name.generation <= grant.name.generation
                        && env.now < grant.lapses
                }
                None => false,
            },
            None => false,
        };
        if !valid {
            self.fail(env, llm::Failure::SignedOut, b"smith: the program source grant is unavailable", up, below);
            return;
        }
        if self.files.prepared() {
            self.request = Some(request);
            self.phase = Phase::Starting;
            self.prepare_invocation(env, up, below);
            return;
        }
        let root = match self.settings.workspace.or(self.settings.directory) {
            Some(root) if self.settings.directory.is_some() => root,
            Some(_) | None => {
                self.fail(env, llm::Failure::SignedOut, b"smith: the program source is not signed in", up, below);
                return;
            }
        };
        let Ok(configuration) =
            configuration_directory(&self.settings.program, &self.grant.as_ref().expect("validated grant").source)
        else {
            self.fail(env, llm::Failure::Invalid, b"smith: invalid program source", up, below);
            return;
        };
        let Ok(environment) = environment(&self.settings.environment, &configuration) else {
            self.fail(env, llm::Failure::Program { exit: None }, b"smith: invalid program environment", up, below);
            return;
        };
        self.request = Some(request);
        let mut arguments = List::with_capacity(2);
        for argument in STATUS {
            arguments.push(Box::from(argument)).expect("status arguments");
        }
        self.phase = Phase::Checking;
        self.check = Some(Command::new(
            self.token(3),
            Launch {
                program: self.settings.program.claude.clone(),
                arguments: arguments.into_boxed(),
                environment,
                root: root.descriptor,
                directory: Box::default(),
            },
            env.now.saturating_add(self.settings.startup),
            self.settings.receiving.max_failure_bytes,
            &mut self.io,
        ));
    }

    pub(super) fn progress(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        self.consume_stdout(env, up, below);
        self.checked(env, up, below);
        self.server_events(env, up, below);
        self.switch_progress(env, up, below);
        if self.winding == super::stop::WindDown::YieldRequested {
            self.yield_for_wind_down(env, up, below);
        }
        if self.phase == Phase::Ending
            && self.child.is_none()
            && !self.spawning
            && let Some(server) = &mut self.server
        {
            server.close(&mut self.io);
        }
        if self.phase == Phase::Ending
            && self.check.is_none()
            && !self.spawning
            && self.child.is_none()
            && self.pipes == [None; 3]
            && (self.server.is_none() || self.server_ended)
        {
            self.cleanup(env.now, below);
        }
    }

    fn checked(&mut self, env: &Env<protocol::ComponentLimits>, up: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
        let (status, exit, detail) = match match &self.check {
            Some(check) => check.ended(),
            None => None,
        } {
            Some(Ended::Exited { exit: kernel::Exit::Code(code), stdout, detail }) => (
                match stdout {
                    Some(bytes) => crate::status(code, bytes),
                    None => Status::Unreadable,
                },
                Some(u32::from(code)),
                Box::<[u8]>::from(detail),
            ),
            Some(Ended::Exited { exit: kernel::Exit::Signal(signal), detail, .. }) => {
                (Status::Unreadable, Some(128_u32.checked_add(signal).expect("signal code")), detail.into())
            }
            Some(Ended::Unspawned { detail } | Ended::Deadline { detail }) => (Status::Unreadable, None, detail.into()),
            None => return,
        };
        self.check = None;
        if self.phase == Phase::Ending {
            return;
        }
        match status {
            Status::Usable => {
                self.phase = Phase::Starting;
                self.open_runs(env.now, below);
            }
            Status::NotSignedIn | Status::Otherwise(_) => self.fail(
                env,
                llm::Failure::SignedOut,
                b"smith: the program source is not signed in through a subscription",
                up,
                below,
            ),
            Status::Unreadable => self.fail(env, llm::Failure::Program { exit }, &detail, up, below),
        }
    }

    pub(super) fn server_events(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        for _ in 0_u32..2 {
            let event = match &mut self.server {
                Some(server) => server.up(),
                None => None,
            };
            match event {
                Some(mcp::Up::Listening { port }) => {
                    if self.phase == Phase::Starting {
                        self.store_instructions(env.now, port, below);
                    }
                }
                Some(mcp::Up::Connected) => {}
                Some(prompt @ mcp::Up::Prompt { .. }) => self.permission(prompt),
                Some(call @ mcp::Up::Call { .. }) => {
                    self.calls.push(call).expect("server bounds admitted calls");
                    self.match_calls();
                }
                Some(mcp::Up::Ended(_)) => {
                    self.mcp_ended(env, up, below);
                }
                None => break,
            }
        }
    }

    pub(super) fn launch(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let Some(launch) = self.launch_spec() else {
            self.fail(
                env,
                llm::Failure::Program { exit: None },
                b"smith: the program command exceeds its bounds",
                up,
                below,
            );
            return;
        };
        if !self.compaction() && !self.resuming {
            self.first_prompt_measure(env.now);
        }
        self.io.push(io::Request::Spawn { owner: self.token(4), spawn: launch });
        self.spawning = true;
        self.deadline = Some(env.now.saturating_add(self.settings.startup));
    }

    pub(super) fn mcp_ended(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        self.server_ended = true;
        if self.switching {
            return;
        }
        if self.phase == Phase::Running && self.child.is_some() {
            // Relay EOF can precede waitpid. Let the child and stderr settle
            // before choosing its program fault (programs.md, section 5.6).
            let until = env.now.saturating_add(self.settings.grace);
            self.eof_deadline = Some(self.eof_deadline.unwrap_or(until).min(until));
        } else if self.phase != Phase::Ending && self.phase != Phase::Ended {
            self.fail(env, llm::Failure::Program { exit: self.exit }, b"smith: the MCP server ended", up, below);
        }
    }

    pub(super) fn launch_spec(&mut self) -> Option<kernel::Spawn> {
        let prompt = match self.request.as_ref()? {
            FromDomain::Complete { prompt, .. } => prompt,
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => return None,
        };
        let outcome = match self.request.as_ref()? {
            FromDomain::Complete { outcome, .. } => outcome,
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => return None,
        };
        if !super::compact::is_compaction(prompt) {
            match &prompt.choice {
                llm::ToolChoice::Only(_) | llm::ToolChoice::None => self.winding = super::stop::WindDown::Finishing,
                llm::ToolChoice::Auto => {}
            }
        }
        let schema = if self.result_contract() {
            let contract = protocol::finish_schema(outcome, self.settings.contract_bytes).ok()?;
            mcp::json::outcome_schema(&contract, self.settings.contract_bytes)?
        } else {
            Box::default()
        };
        let root = self.settings.workspace.or(self.settings.directory)?;
        let directory = if self.settings.workspace.is_some() { Box::default() } else { self.run_relative() };
        let configuration = configuration_directory(&self.settings.program, &self.grant.as_ref()?.source).ok()?;
        let instructions = self.run_path(b"instructions.md");
        let mcp_file = self.run_path(b"mcp.json");
        let mut arguments = if super::compact::is_compaction(prompt) {
            super::compact::arguments(prompt, &instructions, &self.names.session)?
        } else {
            arguments(
                &prompt.model,
                prompt.effort.as_deref(),
                &instructions,
                &mcp_file,
                &schema,
                self.settings.max_turns,
                &self.names.session,
                &super::policy::tools_flag(prompt.tools),
            )?
        };
        if self.resuming {
            for argument in &mut arguments {
                if argument.as_ref() == b"--session-id" {
                    *argument = b"--resume".as_slice().into();
                }
            }
            self.resume_input()?;
        } else if super::compact::is_compaction(prompt) {
            self.compaction_input()?;
        } else {
            self.outgoing = super::stream::user_message(prompt, self.settings.input_bytes);
        }
        self.outgoing.as_ref()?;
        Some(kernel::Spawn {
            program: self.settings.program.claude.clone(),
            args: arguments,
            env: super::replies::environment(&self.settings.environment, &configuration, self.run_time)?,
            root: root.descriptor,
            dir: directory,
            pipes: Box::new([
                kernel::Pipe { child: 0, way: kernel::Way::In, parent: None },
                kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None },
                kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None },
            ]),
        })
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive entry routes child and pipe terminals before the shared progress pass"
    )]
    pub(super) fn io_event(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        event: io::Event,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let owner = match &event {
            io::Event::Listening { owner, .. }
            | io::Event::Accepted { owner, .. }
            | io::Event::Connecting { owner, .. }
            | io::Event::Connected { owner }
            | io::Event::Stream { owner, .. }
            | io::Event::Output { owner, .. }
            | io::Event::Spawned { owner, .. }
            | io::Event::Exited { owner, .. }
            | io::Event::Usage { owner, .. }
            | io::Event::Window { owner, .. }
            | io::Event::Failed { owner, .. }
            | io::Event::Closed { owner } => *owner,
            io::Event::Resized | io::Event::Shutdown { .. } => unreachable!("backend has no signals"),
        };
        if let Some(check) = &mut self.check
            && check.owns(owner)
        {
            check.from_io(event, &mut self.io);
            return;
        }
        if let Some(server) = &mut self.server
            && server.owns(owner)
        {
            server.from_io(env.now, event, &mut self.io);
            return;
        }
        match event {
            io::Event::Spawned { child, pipes, .. } => {
                self.spawning = false;
                self.child = Some(child);
                self.interrupt_sequence = 0;
                for (slot, pipe) in self.pipes.iter_mut().zip(pipes) {
                    *slot = Some(pipe);
                }
                self.read_stdout();
                if let Some(pipe) = self.pipes[2] {
                    Self::demand(pipe, 4096, &mut self.io);
                }
                if self.phase == Phase::Ending {
                    self.io.push(io::Request::Close { entity: self.pipes[0].expect("stdin") });
                    if self.stopping > 0
                        || match self.deadline {
                            Some(deadline) => env.now >= deadline,
                            None => false,
                        }
                    {
                        self.stopping = 0;
                        self.terminate(env.now);
                    }
                } else {
                    self.send_input();
                }
            }
            io::Event::Exited { exit, .. } => {
                self.drain_deadline = Some(env.now.saturating_add(self.settings.grace));
                self.exit = Some(match exit {
                    kernel::Exit::Code(code) => u32::from(code),
                    kernel::Exit::Signal(signal) => 128_u32.checked_add(signal).expect("signal code"),
                });
                if let Some(child) = self.child {
                    self.io.push(io::Request::Close { entity: child });
                }
                if let Some(input) = self.pipes[0] {
                    self.io.push(io::Request::Close { entity: input });
                }
            }
            io::Event::Closed { owner } => {
                if owner == self.token(4) {
                    self.child = None;
                }
                for pipe in &mut self.pipes {
                    if *pipe == Some(owner) {
                        *pipe = None;
                    }
                }
                if self.child.is_none()
                    && !self.spawning
                    && self.pipes == [None; 3]
                    && self.phase != Phase::Ending
                    && !self.switching
                {
                    let detail = self.error_tail();
                    let failure = self.exit_failure();
                    self.fail(env, failure, &detail, up, below);
                }
            }
            io::Event::Failed { owner, .. } => {
                if owner == self.token(4) {
                    self.spawning = false;
                }
                self.fail(
                    env,
                    llm::Failure::Program { exit: self.exit },
                    b"smith: the program could not start",
                    up,
                    below,
                );
            }
            io::Event::Stream { owner, up: stream::Up::Bytes(bytes) } => {
                if self.pipes[2] == Some(owner) {
                    self.keep_error(&bytes);
                    Self::demand(owner, 4096, &mut self.io);
                } else {
                    self.stdout_demanding = false;
                    self.completion_deadline(env, false);
                    assert!(self.intake.is_empty(), "one outstanding stdout read");
                    for byte in bytes {
                        self.intake.push(byte).expect("stdout demand is 4096 bytes");
                    }
                    self.consume_stdout(env, up, below);
                }
            }
            io::Event::Stream { owner, up: stream::Up::End | stream::Up::Failed(_) } => {
                self.io.push(io::Request::Close { entity: owner });
            }
            io::Event::Stream { up: stream::Up::Room, .. } => {}
            io::Event::Output { up: stream::OutputUp::Settled { right, outcome }, .. } => {
                if self.phase != Phase::Ending || self.stop == super::stop::Stop::Parking {
                    match outcome {
                        stream::OutputOutcome::Granted => {
                            if self.exit.is_some() || self.pipes[0].is_none() {
                                self.fail(
                                    env,
                                    llm::Failure::Program { exit: self.exit },
                                    b"smith: the program input closed",
                                    up,
                                    below,
                                );
                                return;
                            }
                            if right == Token::new(2) {
                                if let Some(bytes) = self.interrupt_line.take() {
                                    self.io.push(io::Request::Output {
                                        stream: self.pipes[0].expect("interrupt write"),
                                        down: stream::OutputDown::Send { right, bytes },
                                    });
                                }
                            } else if let Some(bytes) = &self.outgoing {
                                let start = usize::try_from(self.written).expect("input cursor");
                                let end = start.checked_add(4096).expect("input chunk").min(bytes.len());
                                let chunk = Box::from(bytes.get(start..end).expect("input range"));
                                self.written = u32::try_from(end).expect("input ceiling");
                                let finished = end == bytes.len();
                                self.io.push(io::Request::Output {
                                    stream: self.pipes[0].expect("input write"),
                                    down: stream::OutputDown::Send { right, bytes: chunk },
                                });
                                if finished {
                                    self.outgoing = None;
                                    self.written = 0;
                                    self.remember_session();
                                    self.send_interrupt();
                                } else {
                                    self.send_input();
                                }
                            }
                        }
                        stream::OutputOutcome::Cancelled | stream::OutputOutcome::Failed(_) => self.fail(
                            env,
                            llm::Failure::Program { exit: self.exit },
                            b"smith: the program input failed",
                            up,
                            below,
                        ),
                    }
                }
            }
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Usage { .. }
            | io::Event::Window { .. }
            | io::Event::Resized
            | io::Event::Shutdown { .. } => unreachable!("program event route"),
        }
    }

    pub(super) fn demand(pipe: Token, max: u32, below: &mut Queue<io::Request>) {
        below.push(io::Request::Stream {
            stream: pipe,
            down: stream::Down::Demand { read: stream::Read::Arrived { max }, room: 0 },
        });
    }

    pub(super) fn read_stdout(&mut self) {
        if !self.stdout_demanding
            && self.intake.is_empty()
            && let Some(pipe) = self.pipes[1]
        {
            Self::demand(pipe, 4096, &mut self.io);
            self.stdout_demanding = true;
        }
    }

    pub(super) fn send_input(&mut self) {
        if let Some(bytes) = &self.outgoing {
            self.io.push(io::Request::Output {
                stream: self.pipes[0].expect("spawned input"),
                down: stream::OutputDown::Room {
                    right: Token::new(1),
                    bytes: u32::try_from(bytes.len())
                        .expect("bounded input")
                        .checked_sub(self.written)
                        .expect("remaining input")
                        .min(4096),
                },
            });
        }
    }

    fn keep_error(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if self.stderr.room() > 0 {
                self.stderr.push(*byte).expect("tail room");
            } else if self.stderr.capacity() > 0 {
                self.stderr_wrapped = true;
                *self.stderr.get_mut(self.tail).expect("tail cell") = *byte;
                self.tail = self
                    .tail
                    .checked_add(1)
                    .expect("tail index")
                    .checked_rem(self.stderr.capacity())
                    .expect("positive capacity");
            }
        }
    }

    fn error_tail(&self) -> Box<[u8]> {
        let mut bytes = List::with_capacity(self.stderr.len());
        for index in 0..self.stderr.len() {
            let at = self
                .tail
                .checked_add(index)
                .expect("tail offset")
                .checked_rem(self.stderr.len())
                .expect("nonempty tail");
            bytes.push(*self.stderr.get(at).expect("tail cell")).expect("sized tail");
        }
        bytes.into_boxed()
    }
}

/// The program's exact argv, keeping inline schema and empty built-ins as whole arguments.
#[expect(clippy::too_many_arguments, reason = "each independent argv value remains a whole argument")]
pub(super) fn arguments(
    model: &[u8],
    effort: Option<&[u8]>,
    instructions: &[u8],
    mcp_file: &[u8],
    schema: &[u8],
    max_turns: u32,
    session: &[u8],
    tools: &[u8],
) -> Option<Box<[Box<[u8]>]>> {
    let turns = Decimal::of(u64::from(max_turns));
    let mut arguments = List::with_capacity(36);
    for flag in [
        b"-p".as_slice(),
        b"--input-format",
        b"stream-json",
        b"--output-format",
        b"stream-json",
        b"--verbose",
        b"--include-partial-messages",
        b"--setting-sources",
        b"user",
        b"--model",
    ] {
        arguments.push(Box::from(flag)).ok()?;
    }
    arguments.push(Box::from(model)).ok()?;
    if let Some(effort) = effort {
        arguments.push(Box::from(b"--effort".as_slice())).ok()?;
        arguments.push(Box::from(effort)).ok()?;
    }
    for argument in [
        b"--append-system-prompt-file".as_slice(),
        instructions,
        b"--mcp-config",
        mcp_file,
        b"--strict-mcp-config",
        b"--permission-prompt-tool",
        b"mcp__smith__permission_prompt",
        b"--tools",
        tools,
        b"--max-turns",
        turns.as_bytes(),
        b"--session-id",
        session,
    ] {
        arguments.push(Box::from(argument)).ok()?;
    }
    if !schema.is_empty() {
        arguments.push(Box::from(b"--json-schema".as_slice())).ok()?;
        arguments.push(Box::from(schema)).ok()?;
    }
    Some(arguments.into_boxed())
}
