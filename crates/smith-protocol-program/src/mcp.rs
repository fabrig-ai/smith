//! The run's MCP listener (protocol/programs.md, sections 5.3, 5.4 and 9).
//! Keeps one secret, two authentication slots, bounded calls and output lines;
//! knows no tool policy. `from_io`/`fire` tell calls through `up`, and
//! `answer`/`verdict` return the owner's decisions. Drain `up` after each
//! entry point and reserve eight downward records before calling one.

use alloc::boxed::Box;
use core::{
    mem::size_of,
    net::{Ipv4Addr, SocketAddr},
};
use skein_io::{self as io, Addr};
use skein_lib::{Duration, List, Queue, Secret, Time, Token, sha256, stream};

pub(crate) mod json;
#[cfg(test)]
mod tests;

/// MCP configuration name and tool prefix (protocol/programs.md, section 5.3).
pub const SERVER: &[u8] = b"smith";

/// Reserved permission tool (protocol/programs.md, sections 5.3 and 5.4).
pub const PROMPT_TOOL: &[u8] = b"permission_prompt";

/// Acceptance line telling the relay it was accepted (protocol/programs.md, section 9).
pub const ACK: &[u8] = b"smith-mcp ok\n";

/// Agent binary's relay mode (protocol/programs.md, section 9).
pub const RELAY_FLAG: &str = "--mcp-relay";

/// Private configuration environment entry (protocol/programs.md, section 9).
pub const SECRET_ENV: &str = "SMITH_MCP_SECRET";

/// Lowercase hexadecimal bytes for 256 bits of entropy (programs.md, section 9).
pub const SECRET_BYTES: usize = 64;

/// Negotiated MCP versions, newest first (protocol/programs.md, section 5.3).
pub const PROTOCOL_VERSIONS: [&str; 4] = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"];

/// An owner's tool exposed to Claude Code (protocol/programs.md, section 5.3).
#[derive(Debug)]
pub struct Tool {
    pub name: Box<[u8]>,
    pub description: Box<[u8]>,
    /// One complete JSON Schema object; MCP adds an absent object type.
    pub input_schema: Box<[u8]>,
}

/// Startup bounds supplied by the backend (protocol/programs.md, section 9).
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub message_bytes: u32,
    pub answer_bytes: u32,
    /// Maximum encoded bytes in each delegated tool's input schema.
    pub schema_bytes: u32,
    pub calls: u16,
    /// A span in monotonic nanoseconds, measured from acceptance.
    pub unauthenticated: Time,
}

/// One unanswered call, sent by the server to its owner (programs.md, 5.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallId(u32);

/// The server's events to its backend (protocol/programs.md, sections 5.3 and 9).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Up {
    /// The loopback listener is ready at its resolved port.
    Listening { port: u16 },
    /// The run's one client supplied its secret.
    Connected,
    /// A declared tool waits for `answer` from the backend.
    Call { call: CallId, tool: Box<[u8]>, input: Box<[u8]> },
    /// A built-in tool waits for `verdict` from the backend.
    Prompt { call: CallId, tool_name: Box<[u8]>, input: Box<[u8]>, tool_use_id: Option<Box<[u8]>> },
    /// Every listener and connection has settled.
    Ended(Ended),
}

/// The settled server's outcome (protocol/programs.md, section 9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ended {
    /// The peer or owner closed the server.
    Closed,
    /// The loopback listener could not start or continue.
    ListenFailed,
    /// The authenticated stream broke or exceeded its framing bound.
    Failed,
}

/// A backend's tool answer (protocol/programs.md, section 5.3).
#[derive(Debug)]
pub enum Answer {
    /// Successful tool text sent to the model.
    Text(Box<[u8]>),
    /// Failed tool text sent to the model.
    Error(Box<[u8]>),
}

/// A backend's built-in permission decision (protocol/programs.md, section 5.4).
#[derive(Debug)]
pub enum Verdict {
    /// Allow the supplied JSON input.
    Allow { updated_input: Box<[u8]> },
    /// Deny with a message for the model.
    Deny { message: Box<[u8]> },
}

/// Startup refusal returned to the backend before any listener (programs.md, 5.3).
#[derive(Debug)]
pub enum Refused {
    /// The tool inventory exceeds a configured schema or answer bound.
    TooLarge,
    /// The named tool cannot be served as an MCP object schema.
    Schema { tool: Box<[u8]> },
}

#[derive(Debug)]
struct Pending {
    owner: Token,
    socket: Option<Token>,
    deadline: Option<Time>,
}

#[derive(Debug)]
struct Call {
    call: CallId,
    id: Box<[u8]>,
}

/// Sans-io run server driven by its backend (protocol/programs.md, 5.3, 5.4, 9).
#[expect(
    clippy::struct_excessive_bools,
    reason = "listener, authentication, read, output and terminal states advance independently"
)]
pub struct Server {
    first: Token,
    listener: Option<Token>,
    listening: bool,
    pending: [Pending; 2],
    connected: Option<Token>,
    acknowledging: bool,
    secret: Secret,
    tools: Box<[Tool]>,
    tool_list: Box<[u8]>,
    limits: Limits,
    calls: List<Option<Call>>,
    sequence: u32,
    output: Queue<Box<[u8]>>,
    sending: Option<Box<[u8]>>,
    demanding: bool,
    writing: bool,
    write_sequence: u64,
    ending: Option<Ended>,
    told_end: bool,
    above: Queue<Up>,
}

impl core::fmt::Debug for Server {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Server")
            .field("first", &self.first)
            .field("secret_length", &self.secret.len())
            .field("connected", &self.connected)
            .field("ending", &self.ending)
            .finish_non_exhaustive()
    }
}

/// Hexadecimal text used only for authentication and the private MCP file (9).
#[must_use]
pub fn secret_text(entropy: &[u8; 32]) -> [u8; SECRET_BYTES] {
    let digits = b"0123456789abcdef";
    let mut text = [0; SECRET_BYTES];
    for (pair, byte) in text.chunks_exact_mut(2).zip(entropy) {
        *pair.first_mut().expect("hex pair") = *digits.get(usize::from(byte >> 4_u8)).expect("hex digit");
        *pair.last_mut().expect("hex pair") = *digits.get(usize::from(byte & 15)).expect("hex digit");
    }
    text
}

/// Held lines, call IDs, tools and JSON collection/encoding peaks (programs.md, 9).
#[must_use]
pub fn mcp_worst_case(limits: &Limits) -> Option<u64> {
    let calls = u32::from(limits.calls);
    // Reading pauses at calls + 1 queued lines. Answers for the calls
    // already admitted can still arrive, beside one line awaiting its grant.
    let queued = calls.checked_mul(2)?.checked_add(2)?;
    u64::try_from(size_of::<Server>())
        .ok()?
        .checked_add(64 + 130)?
        .checked_add(List::<Option<Call>>::worst_case(calls)?)?
        .checked_add(u64::from(calls).checked_mul(64)?)?
        .checked_add(Queue::<Box<[u8]>>::worst_case(queued)?)?
        .checked_add(u64::from(queued).checked_mul(u64::from(limits.answer_bytes))?)?
        // Tool records and their list, quoting, result/envelope copies and
        // the owner's output record overlap while one answer is encoded.
        .checked_add(u64::from(limits.answer_bytes).checked_mul(12)?)?
        .checked_add(u64::from(limits.message_bytes).checked_mul(12)?)?
        // tool_list drops each schema document before new returns; message drops its document on return.
        .checked_add(json::worst_case(limits.message_bytes)?.max(json::worst_case(limits.schema_bytes)?))?
        .checked_add(Queue::<Up>::worst_case(2)?)
}

impl Server {
    /// Listen on IPv4 loopback; tool-list refusal has no I/O effects (programs.md, 9).
    pub fn new(
        first: Token,
        port: u16,
        entropy: &[u8; 32],
        tools: Box<[Tool]>,
        limits: Limits,
        below: &mut Queue<io::Request>,
    ) -> Result<Self, Refused> {
        assert!(limits.calls > 0 && limits.message_bytes > 0, "positive MCP admission bounds");
        let tool_list = json::tool_list(&tools, limits.answer_bytes, limits.schema_bytes)?;
        let second = Token::new(first.raw().checked_add(1).expect("MCP token range"));
        let third = Token::new(first.raw().checked_add(2).expect("MCP token range"));
        let calls = u32::from(limits.calls);
        let mut slots = List::with_capacity(calls);
        for _ in 0..calls {
            slots.push(None).expect("call slot capacity");
        }
        below.push(io::Request::Listen { owner: first, addr: Addr::Ip(SocketAddr::from((Ipv4Addr::LOCALHOST, port))) });
        Ok(Self {
            first,
            listener: None,
            listening: true,
            pending: [
                Pending { owner: second, socket: None, deadline: None },
                Pending { owner: third, socket: None, deadline: None },
            ],
            connected: None,
            acknowledging: false,
            secret: Secret::new(Box::from(secret_text(entropy).as_slice())),
            tools,
            tool_list,
            limits,
            calls: slots,
            sequence: 0,
            output: Queue::with_capacity(
                calls.checked_mul(2).expect("u16 calls").checked_add(1).expect("output sentinel"),
            ),
            sending: None,
            demanding: false,
            writing: false,
            write_sequence: 0,
            ending: None,
            told_end: false,
            above: Queue::with_capacity(2),
        })
    }

    /// Whether an owner belongs to this listener or its two connection slots (9).
    #[must_use]
    pub fn owns(&self, owner: Token) -> bool {
        owner == self.first
            || self.pending.first().expect("pending slot").owner == owner
            || self.pending.last().expect("pending slot").owner == owner
    }

    /// Secret bytes for the private MCP configuration file alone (programs.md, 9).
    #[must_use]
    pub fn secret(&self) -> &[u8] {
        self.secret.expose()
    }

    /// Drain the upward records after each entry point (programs.md, 5.3).
    pub fn up(&mut self) -> Option<Up> {
        self.above.pop()
    }

    /// Consume a listener or stream event, with the caller's monotonic time (9).
    pub fn from_io(&mut self, now: Time, event: io::Event, below: &mut Queue<io::Request>) {
        match event {
            io::Event::Listening { listener, addr, .. } => {
                self.listener = Some(listener);
                if self.ending.is_some() {
                    below.push(io::Request::Close { entity: listener });
                } else {
                    self.above.push(Up::Listening { port: addr.socket().expect("IP loopback listener").port() });
                }
            }
            io::Event::Accepted { socket, .. } => self.accept(now, socket, below),
            io::Event::Stream { owner, up } => {
                if self.ending.is_none() {
                    if self.connected == Some(owner) {
                        self.message_event(up, below);
                    } else {
                        self.authenticate(owner, up, below);
                    }
                }
            }
            io::Event::Failed { owner, .. } => {
                if owner == self.first {
                    self.stop(Ended::ListenFailed, false, below);
                } else if self.connected == Some(owner) {
                    self.stop(Ended::Failed, true, below);
                } else {
                    self.abort_pending(owner, below);
                }
            }
            io::Event::Closed { owner } => {
                if owner == self.first {
                    self.listening = false;
                    self.listener = None;
                } else {
                    for pending in &mut self.pending {
                        if pending.owner == owner {
                            pending.socket = None;
                            pending.deadline = None;
                        }
                    }
                }
            }
            io::Event::Output { up: stream::OutputUp::Settled { right, outcome }, .. } => {
                self.writing = false;
                if self.ending.is_none() {
                    match outcome {
                        stream::OutputOutcome::Granted => {
                            let bytes = self.sending.take().expect("requested output line");
                            below.push(io::Request::Output {
                                stream: self.socket(),
                                down: stream::OutputDown::Send { right, bytes },
                            });
                            if self.acknowledging {
                                self.acknowledging = false;
                                self.above.push(Up::Connected);
                            }
                        }
                        stream::OutputOutcome::Cancelled | stream::OutputOutcome::Failed(_) => {
                            self.stop(Ended::Failed, true, below);
                        }
                    }
                }
            }
            io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. }
            | io::Event::Usage { .. } => unreachable!("only server-owned events route here"),
        }
        self.progress(below);
        self.settle();
    }

    fn accept(&mut self, now: Time, socket: Token, below: &mut Queue<io::Request>) {
        if self.ending.is_none() && self.connected.is_none() {
            for pending in &mut self.pending {
                if pending.socket.is_none() {
                    pending.socket = Some(socket);
                    pending.deadline =
                        Some(now.saturating_add(Duration::from_nanos(self.limits.unauthenticated.as_nanos())));
                    below.push(io::Request::Bind { socket, owner: pending.owner });
                    below.push(io::Request::Stream {
                        stream: socket,
                        down: stream::Down::Demand { read: stream::Read::Line { max: 65 }, room: 0 },
                    });
                    return;
                }
            }
        }
        below.push(io::Request::Reject { socket });
    }

    fn abort_pending(&mut self, owner: Token, below: &mut Queue<io::Request>) {
        for pending in &mut self.pending {
            if pending.owner == owner {
                pending.deadline = None;
                if let Some(socket) = pending.socket {
                    below.push(io::Request::Abort { entity: socket });
                }
            }
        }
    }

    fn authenticate(&mut self, owner: Token, up: stream::Up, below: &mut Queue<io::Request>) {
        let valid = match up {
            stream::Up::Bytes(bytes) => {
                bytes.len() == 65
                    && bytes.last() == Some(&b'\n')
                    && sha256::equal(bytes.get(..SECRET_BYTES).expect("64 authentication bytes"), self.secret.expose())
            }
            stream::Up::End | stream::Up::Failed(_) => false,
            stream::Up::Room => unreachable!("authentication asks for no output room"),
        };
        let mut active = false;
        for pending in &self.pending {
            if pending.owner == owner && pending.deadline.is_some() {
                active = true;
            }
        }
        if !valid || !active {
            self.abort_pending(owner, below);
            return;
        }
        self.connected = Some(owner);
        for pending in &mut self.pending {
            pending.deadline = None;
            if pending.owner != owner
                && let Some(socket) = pending.socket
            {
                below.push(io::Request::Abort { entity: socket });
            }
        }
        if let Some(listener) = self.listener {
            below.push(io::Request::Close { entity: listener });
        }
        self.acknowledging = true;
        self.output.push(Box::from(ACK));
    }

    fn message_event(&mut self, up: stream::Up, below: &mut Queue<io::Request>) {
        match up {
            stream::Up::Bytes(bytes) => {
                self.demanding = false;
                match bytes.last() {
                    Some(b'\n' | b'\r') => {
                        let line = bytes.get(..bytes.len().checked_sub(1).expect("line end")).expect("line prefix");
                        if !line.is_empty() {
                            self.message(line);
                        }
                    }
                    Some(_) | None => self.stop(Ended::Failed, true, below),
                }
            }
            stream::Up::Room => unreachable!("server output uses independent grants"),
            stream::Up::End => self.stop(Ended::Closed, false, below),
            stream::Up::Failed(_) => self.stop(Ended::Failed, true, below),
        }
    }

    fn socket(&self) -> Token {
        let owner = self.connected.expect("authenticated connection");
        for pending in &self.pending {
            if pending.owner == owner {
                return pending.socket.expect("connection is open");
            }
        }
        unreachable!("authenticated pending slot")
    }

    fn progress(&mut self, below: &mut Queue<io::Request>) {
        if self.ending.is_some() || self.connected.is_none() {
            return;
        }
        if !self.writing {
            if self.sending.is_none() {
                self.sending = self.output.pop();
            }
            if let Some(bytes) = &self.sending {
                self.write_sequence = self.write_sequence.checked_add(1).expect("output rights fit process lifetime");
                self.writing = true;
                below.push(io::Request::Output {
                    stream: self.socket(),
                    down: stream::OutputDown::Room {
                        right: Token::new(self.write_sequence),
                        bytes: u32::try_from(bytes.len()).expect("bounded answer"),
                    },
                });
            }
        }
        let mut count = 0_u32;
        for slot in &self.calls {
            if slot.is_some() {
                count = count.checked_add(1).expect("bounded calls");
            }
        }
        let output_count = self.output.len().checked_add(u32::from(self.sending.is_some())).expect("output count");
        if !self.acknowledging
            && !self.demanding
            && count < u32::from(self.limits.calls)
            && output_count <= u32::from(self.limits.calls)
        {
            self.demanding = true;
            below.push(io::Request::Stream {
                stream: self.socket(),
                down: stream::Down::Demand { read: stream::Read::Line { max: self.limits.message_bytes }, room: 0 },
            });
        }
    }

    /// Return text for a live call; duplicate and late answers are ignored (5.3).
    pub fn answer(&mut self, call: CallId, answer: Answer, below: &mut Queue<io::Request>) {
        let Some(id) = self.take_call(call) else {
            return;
        };
        let result = match answer {
            Answer::Text(text) => json::result(&text, false, self.limits.answer_bytes),
            Answer::Error(text) => json::result(&text, true, self.limits.answer_bytes),
        };
        self.send_result(&id, result);
        self.progress(below);
    }

    /// Return the backend's permission decision to a live prompt call (5.4).
    #[expect(clippy::manual_map, reason = "the protocol subset uses exhaustive matches without callbacks")]
    pub fn verdict(&mut self, call: CallId, verdict: Verdict, below: &mut Queue<io::Request>) {
        let Some(id) = self.take_call(call) else {
            return;
        };
        let bound = self.limits.answer_bytes;
        let text = match verdict {
            Verdict::Allow { updated_input } => {
                if updated_input.len() <= usize::try_from(bound).expect("u32 fits usize")
                    && json::collect(&updated_input).is_some()
                {
                    Some(json::join(&[b"{\"behavior\":\"allow\",\"updatedInput\":", &updated_input, b"}"]))
                } else {
                    None
                }
            }
            Verdict::Deny { message } => match json::quote(&message, bound) {
                Some(message) => Some(json::join(&[b"{\"behavior\":\"deny\",\"message\":", &message, b"}"])),
                None => None,
            },
        };
        let result = match text {
            Some(text) => json::result(&text, false, bound),
            None => None,
        };
        self.send_result(&id, result);
        self.progress(below);
    }

    fn take_call(&mut self, call: CallId) -> Option<Box<[u8]>> {
        if self.ending.is_some() {
            return None;
        }
        for index in 0..self.calls.len() {
            if let Some(pending) = self.calls.get(index).expect("call slot")
                && pending.call == call
            {
                return Some(self.calls.get_mut(index).expect("matching slot").take().expect("matching call").id);
            }
        }
        None
    }

    fn send_result(&mut self, id: &[u8], result: Option<Box<[u8]>>) {
        if let Some(result) = result {
            let line = json::reply(id, &result);
            if line.len() <= usize::try_from(self.limits.answer_bytes).expect("u32 fits usize") {
                self.output.push(line);
                return;
            }
        }
        let result = json::result(b"smith: the answer passed its bound", true, self.limits.answer_bytes)
            .expect("tool list admits fallback");
        self.output.push(json::reply(id, &result));
    }

    /// Abort unauthenticated connections whose accept-relative deadline expired (9).
    pub fn fire(&mut self, now: Time, below: &mut Queue<io::Request>) {
        for pending in &mut self.pending {
            if let Some(deadline) = pending.deadline
                && now >= deadline
            {
                pending.deadline = None;
                below.push(io::Request::Abort { entity: pending.socket.expect("authentication socket") });
            }
        }
        self.settle();
    }

    /// The earliest authentication deadline, if any (protocol/programs.md, 9).
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut next = None;
        for pending in &self.pending {
            if let Some(deadline) = pending.deadline {
                next = Some(next.unwrap_or(deadline).min(deadline));
            }
        }
        next
    }

    /// Close every owned entity; `Ended(Closed)` follows their settlement (9).
    pub fn close(&mut self, below: &mut Queue<io::Request>) {
        self.stop(Ended::Closed, false, below);
        self.settle();
    }

    fn stop(&mut self, ending: Ended, abort: bool, below: &mut Queue<io::Request>) {
        if self.ending.is_some() {
            return;
        }
        self.ending = Some(ending);
        if let Some(listener) = self.listener {
            below.push(io::Request::Close { entity: listener });
        }
        for pending in &mut self.pending {
            pending.deadline = None;
            if let Some(socket) = pending.socket {
                if abort {
                    below.push(io::Request::Abort { entity: socket });
                } else {
                    below.push(io::Request::Close { entity: socket });
                }
            }
        }
        self.sending = None;
        for _ in 0..self.output.len() {
            drop(self.output.pop());
        }
        for index in 0..self.calls.len() {
            *self.calls.get_mut(index).expect("call slot") = None;
        }
    }

    fn settle(&mut self) {
        if self.listening || self.told_end {
            return;
        }
        for pending in &self.pending {
            if pending.socket.is_some() {
                return;
            }
        }
        if let Some(ending) = self.ending {
            self.above.push(Up::Ended(ending));
            self.told_end = true;
        }
    }
}
