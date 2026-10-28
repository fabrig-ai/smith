//! A spawned agent's pipes and child lifetime (protocol/hosts.md, section 3;
//! domain/host.md, section 4). The component keeps a bounded standard-error
//! tail, the opening deadline, and the child's three pipe tokens. It never
//! knows host policy or credential values. `spawn`, `from_io`, `fire` and
//! `signal` translate process events; `read` demands one channel terminal,
//! `send_*` encodes output, and `next_channel_down`/`channel_down` route its IO.

use alloc::boxed::Box;
use skein_channel::{Lower, LowerEvent, StreamMode};
use skein_io::{Event as IoEvent, Request as IoRequest, kernel};
use skein_lib::{List, Queue, Time, Token, stream};
use smith_host_domain::channel;

use crate::{Component, Error, Limits, OpenEvent, Values};

const STDERR_READ: u32 = 4096;

/// The executable, arguments, credential-free environment and prepared view.
#[derive(Debug)]
pub struct Launch {
    pub program: Box<[u8]>,
    pub arguments: Box<[Box<[u8]>]>,
    pub environment: Box<[Box<[u8]>]>,
    pub root: kernel::Fd,
    pub directory: Box<[u8]>,
}

/// Process and channel observations for the host service.
#[derive(Debug, PartialEq, Eq)]
pub enum ProcessEvent {
    /// The child and its channel completed the opening.
    Spawned { agent: Token, process: Token },
    /// Spawn or opening failed and all owned process resources were released.
    Unspawned { agent: Token, detail: Box<[u8]> },
    /// The main child process exited; pipe contents may still be unread.
    Exited { agent: Token, exit: Option<kernel::Exit> },
    /// The child, its pipes and its bounded error tail settled.
    Reaped { agent: Token, detail: Box<[u8]> },
    /// One event from the opened channel, for the host-domain translation.
    Channel { agent: Token, event: Box<OpenEvent> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Spawning,
    Opening,
    Ready,
    Failing,
    Gone,
}

/// One agent process and its host-side channel.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "independent pipe closures and wait rights settle separately")]
pub struct Process {
    channel: Component,
    agent: Token,
    io_owner: Token,
    phase: Phase,
    deadline: Time,
    child: Option<Token>,
    input: Option<Token>,
    output: Option<Token>,
    error: Option<Token>,
    input_closed: bool,
    output_closed: bool,
    error_closed: bool,
    child_closed: bool,
    exited: bool,
    exit: Option<kernel::Exit>,
    channel_ended: bool,
    stderr_demanded: bool,
    tail: List<u8>,
    channel_events: Queue<OpenEvent>,
    channel_below: Queue<Lower>,
}

impl Process {
    /// Construct one process half, including its framed pipe channel.
    pub fn new(
        agent: Token,
        limits: &Limits,
        detail_bytes: u32,
        endpoints: smith_protocol_channel::Endpoints,
    ) -> Result<Process, Error> {
        assert!(agent.raw() < (1_u64 << 63_u32), "domain owner fits the process-owner namespace");
        Ok(Process {
            channel: Component::new(limits, StreamMode::Two, endpoints)?,
            agent,
            io_owner: Token::new(agent.raw() | (1_u64 << 63_u32)),
            phase: Phase::Idle,
            deadline: Time::ZERO,
            child: None,
            input: None,
            output: None,
            error: None,
            input_closed: false,
            output_closed: false,
            error_closed: false,
            child_closed: false,
            exited: false,
            exit: None,
            channel_ended: false,
            stderr_demanded: false,
            tail: List::with_capacity(detail_bytes),
            channel_events: Queue::with_capacity(8),
            channel_below: Queue::with_capacity(16),
        })
    }

    /// Ask io for a child with the three standard pipes.
    pub fn spawn(&mut self, launch: Launch, deadline: Time, below: &mut Queue<IoRequest>) {
        assert!(self.phase == Phase::Idle, "one launch per agent process");
        self.phase = Phase::Spawning;
        self.deadline = deadline;
        below.push(IoRequest::Spawn {
            owner: self.io_owner,
            spawn: kernel::Spawn {
                program: launch.program,
                args: launch.arguments,
                env: launch.environment,
                root: launch.root,
                dir: launch.directory,
                pipes: Box::new([
                    kernel::Pipe { child: 0, way: kernel::Way::In, parent: None },
                    kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None },
                    kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None },
                ]),
            },
        });
    }

    /// The separate Skein IO owner for child lifecycle events.
    #[must_use]
    pub const fn io_owner(&self) -> Token {
        self.io_owner
    }

    /// Demand one record, live piece or end from the opened child channel.
    pub fn read(&mut self, above: &mut Queue<ProcessEvent>, below: &mut Queue<IoRequest>) {
        assert!(self.phase == Phase::Ready, "application reads follow Spawned");
        assert!(!self.channel_ended, "no read after the delivered channel end");
        self.channel.read(&mut self.channel_events, &mut self.channel_below);
        self.drain_channel(above, below);
        self.finish(above);
    }

    /// Send the run's Start once the child channel is ready.
    pub fn send_start(
        &mut self,
        start: channel::Start,
        window: channel::Window,
        values: Values,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_start(start, window, values, token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Relay a named parent message to the admitted child.
    pub fn send_message(
        &mut self,
        message: channel::Message,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_message(message, token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Return one durable host-call answer to the child.
    pub fn send_reply(
        &mut self,
        call: Token,
        reply: channel::Reply,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_reply(call, reply, token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Confirm one durably kept turn.
    pub fn send_acknowledge(
        &mut self,
        turn: u32,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_acknowledge(turn, token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Refresh one credential without exposing its bytes to the host domain.
    pub fn send_grant(
        &mut self,
        grant: channel::Grant,
        credential: smith_protocol_channel::GrantValue,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_grant(grant, credential, token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Ask the admitted run to wind down before an eventual signal.
    pub fn send_cancel(
        &mut self,
        token: Token,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) -> Result<(), Error> {
        self.channel.send_cancel(token, &mut self.channel_events, &mut self.channel_below)?;
        self.drain_channel(above, below);
        Ok(())
    }

    /// Forward one channel lower request to its pipe.
    pub fn channel_down(&mut self, request: Lower, below: &mut Queue<IoRequest>) {
        let input = self.input.expect("spawned channel has standard input");
        let output = self.output.expect("spawned channel has standard output");
        match request {
            Lower::Read(down) => below.push(IoRequest::Stream { stream: output, down }),
            Lower::Write(down) => below.push(IoRequest::Output { stream: input, down }),
            Lower::FinishWrite => below.push(IoRequest::Stream { stream: input, down: stream::Down::Finish }),
        }
    }

    /// Take one pending lower request emitted by the channel.
    pub fn next_channel_down(&mut self) -> Option<Lower> {
        self.channel_below.pop()
    }

    /// Send a domain stop to io's child and acknowledge its accepted request.
    pub fn signal(&self, signal: kernel::Signal, below: &mut Queue<IoRequest>) {
        let child = self.child.expect("a signal names a spawned child");
        below.push(IoRequest::Signal { child, to: kernel::Target::Child, signal });
    }

    /// Request an immediate operator stop once the child handle exists.
    /// The host domain still waits for exit, pipe EOF and reap before Gone.
    pub fn kill_if_spawned(&self, below: &mut Queue<IoRequest>) -> bool {
        match self.child {
            Some(child) => {
                below.push(IoRequest::Signal { child, to: kernel::Target::Child, signal: kernel::Signal::Kill });
                true
            }
            None => false,
        }
    }

    /// Consume an io observation for this child or one of its pipes.
    #[expect(clippy::too_many_lines, reason = "one exhaustive owner routing table")]
    pub fn from_io(
        &mut self,
        now: Time,
        event: IoEvent,
        above: &mut Queue<ProcessEvent>,
        below: &mut Queue<IoRequest>,
    ) {
        self.channel.set_now(now);
        match event {
            IoEvent::Spawned { owner, child, pipes } if owner == self.io_owner => {
                assert!(
                    (self.phase == Phase::Spawning || self.phase == Phase::Failing) && pipes.len() == 3,
                    "three standard pipes"
                );
                self.child = Some(child);
                self.input = pipes.first().copied();
                self.output = pipes.get(1).copied();
                self.error = pipes.get(2).copied();
                if self.phase == Phase::Failing || now >= self.deadline {
                    self.phase = Phase::Spawning;
                    self.fail(below);
                } else {
                    self.phase = Phase::Opening;
                    self.channel.open(&mut self.channel_events, &mut self.channel_below);
                    self.drain_channel(above, below);
                    self.read_stderr(below);
                }
            }
            IoEvent::Failed { owner, .. } if owner == self.io_owner && self.child.is_none() => {
                self.phase = Phase::Failing;
            }
            IoEvent::Failed { owner, .. } if owner == self.io_owner => {
                if self.phase == Phase::Opening {
                    self.fail(below);
                }
                self.exited = true;
                if self.phase == Phase::Ready {
                    above.push(ProcessEvent::Exited { agent: self.agent, exit: None });
                }
                if let Some(input) = self.input {
                    below.push(IoRequest::Close { entity: input });
                }
                if let Some(child) = self.child {
                    below.push(IoRequest::Close { entity: child });
                }
            }
            IoEvent::Closed { owner } if owner == self.io_owner => {
                self.child_closed = true;
                self.finish(above);
            }
            IoEvent::Exited { owner, exit } if owner == self.io_owner => {
                self.exited = true;
                self.exit = Some(exit);
                if self.phase == Phase::Ready {
                    above.push(ProcessEvent::Exited { agent: self.agent, exit: self.exit });
                }
                if let Some(input) = self.input {
                    below.push(IoRequest::Close { entity: input });
                }
                if let Some(child) = self.child {
                    below.push(IoRequest::Close { entity: child });
                }
            }
            IoEvent::Stream { owner, up } if Some(owner) == self.output => {
                self.channel.from_below(LowerEvent::Read(up), &mut self.channel_events, &mut self.channel_below);
                self.drain_channel(above, below);
            }
            IoEvent::Output { owner, up } if Some(owner) == self.input => {
                self.channel.from_below(LowerEvent::Write(up), &mut self.channel_events, &mut self.channel_below);
                self.drain_channel(above, below);
            }
            IoEvent::Stream { owner, up } if Some(owner) == self.input => match up {
                stream::Up::Failed(fault) => {
                    self.channel.from_below(
                        LowerEvent::WriteFailed(fault),
                        &mut self.channel_events,
                        &mut self.channel_below,
                    );
                    self.drain_channel(above, below);
                }
                stream::Up::Bytes(_) | stream::Up::Room | stream::Up::End => {}
            },
            IoEvent::Stream { owner, up } if Some(owner) == self.error => match up {
                stream::Up::Bytes(bytes) => {
                    self.stderr_demanded = false;
                    self.keep_tail(&bytes);
                    self.read_stderr(below);
                }
                stream::Up::End | stream::Up::Failed(_) => {
                    self.stderr_demanded = false;
                    if let Some(error) = self.error {
                        below.push(IoRequest::Close { entity: error });
                    }
                }
                stream::Up::Room => {}
            },
            IoEvent::Closed { owner } if Some(owner) == self.input => self.input_closed = true,
            IoEvent::Closed { owner } if Some(owner) == self.output => {
                self.output_closed = true;
                if !self.channel_ended && (self.phase == Phase::Opening || self.phase == Phase::Ready) {
                    self.channel.from_below(
                        LowerEvent::Read(stream::Up::End),
                        &mut self.channel_events,
                        &mut self.channel_below,
                    );
                    self.drain_channel(above, below);
                }
            }
            IoEvent::Closed { owner } if Some(owner) == self.error => self.error_closed = true,
            IoEvent::Listening { .. }
            | IoEvent::Accepted { .. }
            | IoEvent::Connecting { .. }
            | IoEvent::Connected { .. }
            | IoEvent::Spawned { .. }
            | IoEvent::Exited { .. }
            | IoEvent::Usage { .. }
            | IoEvent::Resized
            | IoEvent::Window { .. }
            | IoEvent::Shutdown { .. }
            | IoEvent::Failed { .. }
            | IoEvent::Closed { .. }
            | IoEvent::Stream { .. }
            | IoEvent::Output { .. } => {}
        }
        self.finish(above);
    }

    /// Expire the opening deadline and progress the channel's output queues.
    pub fn fire(&mut self, now: Time, above: &mut Queue<ProcessEvent>, below: &mut Queue<IoRequest>) {
        self.channel.set_now(now);
        if (self.phase == Phase::Spawning || self.phase == Phase::Opening) && now >= self.deadline {
            self.fail(below);
        }
        if self.phase == Phase::Opening || self.phase == Phase::Ready {
            self.channel.fire(&mut self.channel_events, &mut self.channel_below);
            self.drain_channel(above, below);
        }
        self.finish(above);
    }

    /// The opening deadline, until the channel is ready or failed.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        match self.phase {
            Phase::Spawning | Phase::Opening => Some(self.deadline),
            Phase::Idle | Phase::Ready | Phase::Failing | Phase::Gone => None,
        }
    }

    fn drain_channel(&mut self, above: &mut Queue<ProcessEvent>, below: &mut Queue<IoRequest>) {
        for _ in 0..self.channel_events.capacity() {
            let Some(event) = self.channel_events.pop() else { break };
            if self.phase == Phase::Failing {
                continue;
            }
            match event {
                OpenEvent::Opened { .. } if self.phase == Phase::Opening => {
                    self.phase = Phase::Ready;
                    above.push(ProcessEvent::Spawned {
                        agent: self.agent,
                        process: self.child.expect("opening belongs to child"),
                    });
                    if self.exited {
                        above.push(ProcessEvent::Exited { agent: self.agent, exit: self.exit });
                    }
                }
                OpenEvent::Hangup { .. } if self.phase == Phase::Opening => {
                    self.channel_ended = true;
                    self.fail(below);
                }
                OpenEvent::Hangup { .. } if self.phase == Phase::Ready => {
                    self.channel_ended = true;
                    if let Some(output) = self.output {
                        below.push(IoRequest::Close { entity: output });
                    }
                    above.push(ProcessEvent::Channel { agent: self.agent, event: Box::new(event) });
                }
                event @ OpenEvent::Answer { .. } => {
                    if let Some(input) = self.input {
                        below.push(IoRequest::Stream { stream: input, down: stream::Down::Finish });
                    }
                    above.push(ProcessEvent::Channel { agent: self.agent, event: Box::new(event) });
                }
                event @ (OpenEvent::Opened { .. }
                | OpenEvent::Hangup { .. }
                | OpenEvent::Sent { .. }
                | OpenEvent::Unsent { .. }
                | OpenEvent::MessageRefused { .. }
                | OpenEvent::Admitted
                | OpenEvent::Waiting { .. }
                | OpenEvent::Long { .. }
                | OpenEvent::LongDone
                | OpenEvent::Turn { .. }
                | OpenEvent::Fact { .. }
                | OpenEvent::Live { .. }
                | OpenEvent::Unauthorized { .. }
                | OpenEvent::Limited { .. }
                | OpenEvent::Refused { .. }
                | OpenEvent::WriteFailed
                | OpenEvent::Call { .. }
                | OpenEvent::Withdraw { .. }) => {
                    above.push(ProcessEvent::Channel { agent: self.agent, event: Box::new(event) });
                }
            }
        }
    }

    fn read_stderr(&mut self, below: &mut Queue<IoRequest>) {
        if self.stderr_demanded || self.error_closed {
            return;
        }
        if let Some(error) = self.error {
            self.stderr_demanded = true;
            below.push(IoRequest::Stream {
                stream: error,
                down: stream::Down::Demand { read: stream::Read::Arrived { max: STDERR_READ }, room: 0 },
            });
        }
    }

    fn keep_tail(&mut self, bytes: &[u8]) {
        let capacity = self
            .tail
            .as_slice()
            .len()
            .checked_add(usize::try_from(self.tail.room()).expect("tail room fits"))
            .expect("bounded tail capacity");
        let old = self.tail.as_slice();
        let total = old.len().saturating_add(bytes.len());
        let skip = total.saturating_sub(capacity);
        let mut next = List::with_capacity(u32::try_from(capacity).expect("bounded tail capacity"));
        for (index, byte) in old.iter().chain(bytes.iter()).enumerate() {
            if index >= skip {
                next.push(*byte).expect("suffix fits tail cap");
            }
        }
        self.tail = next;
    }

    fn fail(&mut self, below: &mut Queue<IoRequest>) {
        if self.phase == Phase::Failing || self.phase == Phase::Gone {
            return;
        }
        self.phase = Phase::Failing;
        if let Some(child) = self.child {
            below.push(IoRequest::Abort { entity: child });
        }
        if let Some(input) = self.input {
            below.push(IoRequest::Abort { entity: input });
        }
        if let Some(output) = self.output {
            below.push(IoRequest::Abort { entity: output });
        }
        self.read_stderr(below);
    }

    fn finish(&mut self, above: &mut Queue<ProcessEvent>) {
        if self.phase == Phase::Gone || !self.child_closed {
            return;
        }
        if self.child.is_some() && (!self.input_closed || !self.output_closed || !self.error_closed) {
            return;
        }
        if self.phase == Phase::Ready && !self.channel_ended {
            return;
        }
        let detail = self.tail.to_boxed();
        match self.phase {
            Phase::Spawning | Phase::Opening | Phase::Failing => {
                above.push(ProcessEvent::Unspawned { agent: self.agent, detail });
            }
            Phase::Ready => above.push(ProcessEvent::Reaped { agent: self.agent, detail }),
            Phase::Idle | Phase::Gone => unreachable!("a live launch reaches its terminal once"),
        }
        self.phase = Phase::Gone;
    }
}
