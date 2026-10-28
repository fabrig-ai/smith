//! Bounded event destinations, sharing records and retaining independent delivery state.
//! Each stream keeps its queued lines, byte credit, outstanding send time and, once
//! IO gives it up, why. No provider syntax or credential is known here. `record`
//! accepts typed records; `up` and `down` meet Skein IO at the owner's clock, and
//! `pop_given_up` supplies the shell diagnostic.
//! Contracts: protocol/events.md, sections 5.1–5.3; protocol/agent.md, sections 5–6.

#![expect(clippy::manual_let_else, reason = "bounded production entrances use explicit matches")]

use alloc::boxed::Box;

use skein_io as io;
use skein_lib::{Duration, List, Map, Queue, Time, Token, bytes, stream};
use smith_events::{Event, Record};

mod view;
pub use view::View;

/// An event destination, named by the shell if IO abandons it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Destination {
    /// The shell's append-only trace file.
    Trace,
    /// The product's standard output stream.
    Output,
}

/// Why IO gave a destination up, in IO's terms (protocol/events.md, section 5.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Why {
    /// It stayed behind past the write deadline, as the sink's own clock tells.
    Behind,
    /// Its reader went away: IO reported the stream reset.
    ReaderGone,
    /// A write failed for a reason IO does not name further.
    WriteFailed,
}

/// One destination IO gave up, and why, for the shell's line on standard error.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct GivenUp {
    /// The destination given up; it gets nothing more.
    pub destination: Destination,
    /// Why IO gave it up.
    pub why: Why,
}

/// One destination's delivery policy, supplied by its owner at adoption.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Delivery {
    /// A behind destination holds counted records until they can be delivered.
    Complete,
    /// A behind destination counts records it cannot retain as lost.
    BestEffort,
}

impl Delivery {
    /// The codec's total representation of this configured policy.
    #[must_use]
    pub const fn encoded(self) -> smith_events::Delivery {
        match self {
            Delivery::Complete => smith_events::Delivery::Complete,
            Delivery::BestEffort => smith_events::Delivery::BestEffort,
        }
    }
}

/// The owner's content policy; it grants observation, never execution authority.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Capture {
    /// Content-free names, classifications and measurements.
    None,
    /// Also call inputs and failure details.
    Calls,
    /// Also request deltas, completions, results and live text.
    Everything,
}

impl Capture {
    /// The codec's total representation of this configured policy.
    #[must_use]
    pub const fn encoded(self) -> smith_events::Capture {
        match self {
            Capture::None => smith_events::Capture::None,
            Capture::Calls => smith_events::Capture::Calls,
            Capture::Everything => smith_events::Capture::Everything,
        }
    }
}

/// Immutable sink ownership bounds, supplied before the first destination is adopted.
#[derive(Clone, Copy, Debug)]
pub struct SinkLimits {
    pub destinations: u32,
    /// Bytes retained per destination, including three authoritative records' reserved room.
    pub output: u32,
    pub records: smith_events::Limits,
    /// Aggregate owned bytes of one accepted outcome, before JSON escaping.
    pub answer_bytes: u32,
    /// One configured or fixed metadata string; provider call id/name use records.string.
    pub metadata_string: u32,
    /// Entries in authoritative metadata lists, independently of content blocks.
    pub metadata_items: u32,
    /// Largest accepted answer or native failure detail, excluding prompt payload.
    pub scalar_content: u32,
    /// Joint line bound for actual observed content, including its envelope and newline.
    pub observation_line: u32,
    /// Captured payload and block framing beyond two scalar records from one fact.
    pub fact_extra: u32,
    pub conversations: u32,
}

impl SinkLimits {
    /// The writer's joint encoded-line ceiling, including the terminating newline.
    /// Receivers carry this value alongside the independent semantic record caps.
    #[must_use]
    pub fn line_ceiling(&self, capture: Capture) -> Option<u32> {
        let metadata =
            smith_events::Limits { content: self.scalar_content, items: self.metadata_items, ..self.records };
        let scalar = largest_line(&metadata, capture, self.answer_bytes, self.metadata_string)?;
        match capture {
            Capture::None => Some(scalar),
            Capture::Calls | Capture::Everything => Some(scalar.max(self.observation_line)),
        }
    }
}

/// The settled process terminal supplied by the service after its last child closes.
/// Unread IO usage stays unknown; it never changes the answer or the exit.
#[derive(Clone, Copy, Debug)]
pub struct Ended {
    pub exit: u8,
    pub answered: bool,
    pub aborted: bool,
    pub teardown: Option<Duration>,
    pub usage: io::Measured,
    pub channel: Option<u64>,
}

#[derive(Debug)]
struct Line {
    bytes: Box<[u8]>,
    sent: u32,
}

#[derive(Clone, Copy, Debug)]
struct Pending {
    /// Whole record credit released only after its final chunk drains.
    bytes: u32,
}

/// The stream's physical lifecycle; Closed is its IO terminal.
#[derive(Clone, Copy, Debug)]
enum Stream {
    /// A queued line needs room, or its prior line is still draining.
    Waiting,
    /// IO has granted its full output credit for one Send.
    Ready,
    /// A full-credit demand is outstanding; its answer confirms the prior Send drained.
    Demanding,
    /// The owner ended the stream and its last bytes are draining.
    Closing,
    /// IO abandoned the destination and owes its Closed terminal.
    GivenUp,
    /// The owner aborted the stream at teardown; it owes Closed and gives nothing up.
    Aborted,
    /// IO's descriptor terminal was received.
    Closed,
}

#[derive(Debug)]
struct Writer {
    destination: Destination,
    token: Token,
    delivery: Delivery,
    stream: Stream,
    lines: Queue<Line>,
    active: Option<Line>,
    pending: Option<Pending>,
    /// When the outstanding Send left, from which IO's write deadline runs;
    /// none once Room proves it drained.
    sent: Option<Time>,
    held: u32,
    loss: u64,
    abandoned: bool,
    /// Why IO gave the destination up, until the shell takes its line.
    given_up: Option<Why>,
    ended: bool,
}

/// One producer's bounded destinations; each closes after its last queued record.
#[derive(Debug)]
pub struct Sinks {
    limits: SinkLimits,
    capture: Capture,
    responses: crate::responses::Book,
    pending: Map<Token, Record>,
    writers: List<Writer>,
    line: u32,
    reserve: u32,
    ordinary: u32,
    /// IO's actual output cap: a full-credit grant proves the previous chunk drained.
    io_output: u32,
    /// The write deadline IO keeps on each destination; past it a failed one was behind.
    write_deadline: Duration,
    counts: [u64; TYPES],
    ended: bool,
    front: Option<Queue<View>>,
    front_lost: u64,
}

pub(crate) const FRONT_RECORDS: u32 = 4;
const TYPES: usize = 19;
const AUTHORITATIVE: u32 = 4;
// The three authoritative records, teardown notice and two-record fact hand-off have separate slots.
const LINE_SLOTS: u32 = 6;

impl Sinks {
    /// Construct the destinations with a checked bound, the actual adopted IO output cap,
    /// and the write deadline IO keeps on each destination.
    #[must_use]
    pub fn new(limits: &SinkLimits, capture: Capture, io_output: u32, write_deadline: Duration) -> Sinks {
        assert!(sinks_worst_case(limits, capture).is_some(), "startup checked the sink's ownership bound");
        let line = limits.line_ceiling(capture).expect("startup checked the produced record bound");
        assert!(io_output > 0, "a destination can send at least one byte");
        Sinks {
            limits: *limits,
            capture,
            responses: crate::responses::Book::new(limits.conversations),
            pending: Map::with_capacity(limits.conversations),
            writers: List::with_capacity(limits.destinations),
            line,
            reserve: authoritative_credit(limits, capture).expect("startup checked authoritative credit"),
            ordinary: ordinary_credit(limits, capture).expect("startup checked one observation or two fact records"),
            io_output,
            write_deadline,
            counts: [0; TYPES],
            ended: false,
            front: None,
            front_lost: 0,
        }
    }

    pub(crate) fn with_front(
        limits: &SinkLimits,
        capture: Capture,
        io_output: u32,
        write_deadline: Duration,
        front: bool,
    ) -> Self {
        let mut sinks = Self::new(limits, capture, io_output, write_deadline);
        if front {
            sinks.front = Some(Queue::with_capacity(FRONT_RECORDS));
        }
        sinks
    }

    /// Take the next typed live-view record without changing a destination's credit.
    pub fn pop_front(&mut self) -> Option<View> {
        match &mut self.front {
            Some(front) => front.pop(),
            None => None,
        }
    }

    /// Records the live view could not hold; destinations keep their own loss counts.
    #[must_use]
    pub const fn front_lost(&self) -> u64 {
        self.front_lost
    }

    /// Whether a typed live-view record is waiting for the product.
    #[must_use]
    pub fn front_pending(&self) -> bool {
        match &self.front {
            Some(front) => !front.is_empty(),
            None => false,
        }
    }

    /// Adopt an IO stream exactly once, or return its destination to the caller.
    pub fn add(&mut self, destination: Destination, token: Token, delivery: Delivery) -> Result<(), Destination> {
        for writer in &self.writers {
            if writer.destination == destination || writer.token == token {
                return Err(destination);
            }
        }
        if self.writers.room() == 0 {
            return Err(destination);
        }
        self.writers
            .push(Writer {
                destination,
                token,
                delivery,
                stream: Stream::Waiting,
                lines: Queue::with_capacity(LINE_SLOTS),
                active: None,
                pending: None,
                sent: None,
                held: 0,
                loss: 0,
                abandoned: false,
                given_up: None,
                ended: false,
            })
            .expect("destination room checked");
        Ok(())
    }

    /// Whether startup can adopt another bounded destination.
    #[must_use]
    pub fn room_for_destination(&self, destination: Destination) -> bool {
        if self.writers.len() >= self.limits.destinations {
            return false;
        }
        for writer in &self.writers {
            if writer.destination == destination {
                return false;
            }
        }
        true
    }

    /// Append entities that still owe their physical IO terminal.
    #[must_use]
    pub fn open_streams(&self) -> u32 {
        let mut open = 0_u32;
        for writer in &self.writers {
            match writer.stream {
                Stream::Closed => {}
                Stream::Waiting
                | Stream::Ready
                | Stream::Demanding
                | Stream::Closing
                | Stream::GivenUp
                | Stream::Aborted => {
                    open = open.checked_add(1).expect("bounded destinations");
                }
            }
        }
        open
    }

    /// Whether a bounded down pass can send a demand, line or close.
    #[must_use]
    pub fn has_work(&self) -> bool {
        for writer in &self.writers {
            match writer.stream {
                Stream::Waiting | Stream::Ready
                    if !writer.lines.is_empty()
                        || writer.active.is_some()
                        || writer.pending.is_some()
                        || writer.ended =>
                {
                    return true;
                }
                Stream::Waiting
                | Stream::Ready
                | Stream::Demanding
                | Stream::Closing
                | Stream::GivenUp
                | Stream::Aborted
                | Stream::Closed => {}
            }
        }
        false
    }

    /// Whether every writing complete destination can retain the next counted record.
    #[must_use]
    pub fn takes(&self) -> bool {
        for writer in &self.writers {
            match writer.delivery {
                Delivery::Complete if !writer.abandoned && !writer.ended => {
                    if !room(writer, self.limits.output, self.reserve, self.ordinary)
                        || writer.lines.room() < AUTHORITATIVE.checked_add(2).expect("bounded fact reserve")
                    {
                        return false;
                    }
                }
                Delivery::Complete | Delivery::BestEffort => {}
            }
        }
        true
    }

    /// Retain the actual opening's identity and model before its first request.
    /// The owner projects this value from the run's opening and configured endpoint.
    pub fn conversation(&mut self, at: Time, origin: Time, owner: Token, opened: smith_events::ConversationOpened) {
        self.responses.opened(at, owner, &opened);
        self.record(&Record {
            t_ms: at.saturating_since(origin).as_nanos() / 1_000_000,
            event: Event::ConversationOpened(opened),
        });
    }

    pub(crate) fn next_conversation(&mut self, run: u64) -> u64 {
        self.responses.next_conversation(run)
    }

    pub(crate) fn scope(&self, owner: Token) -> Option<crate::responses::Scope> {
        self.responses.scope(owner)
    }

    /// Assign one response identity at the actual hand-off to the LLM component.
    /// Everything keeps its start until the component observes the translated prompt.
    pub fn request(
        &mut self,
        now: Time,
        origin: Time,
        owner: Token,
        prompt: &smith_domain::llm::Prompt,
    ) -> Option<smith_protocol_llm::Observation> {
        let record = self.responses.request(now, origin, owner, prompt);
        let observation = if self.front.is_some() { Capture::Everything } else { self.capture };
        match observation {
            Capture::None | Capture::Calls if !self.responses.program(owner) => {
                self.record(&record);
                None
            }
            Capture::None | Capture::Calls | Capture::Everything => {
                let previous = self.pending.insert(owner, record).expect("one pending start per admitted conversation");
                assert!(previous.is_none(), "one actual hand-off per owner");
                if observation == Capture::Everything {
                    Some(self.responses.observation(owner))
                } else {
                    Some(self.responses.timing_observation(owner))
                }
            }
        }
    }

    /// Drain an actual component observation before its domain terminal is consumed.
    pub(crate) fn measure(&mut self, origin: Time, measure: smith_protocol_llm::Measure) {
        match measure {
            smith_protocol_llm::Measure::Call { .. } | smith_protocol_llm::Measure::Arguments { .. } => {}
            smith_protocol_llm::Measure::Retried { at, owner, status, retry_ms } => {
                self.attempt_ready(owner);
                let records = self.responses.retried(at, origin, owner, status, retry_ms);
                for record in &records {
                    self.record(record);
                }
            }
            smith_protocol_llm::Measure::Terminal { owner, measures } => {
                self.responses.measured(owner, measures);
            }
            smith_protocol_llm::Measure::Prompt { at, owner, prompt } => {
                let mut record = self.pending.remove(&owner).expect("observed prompt follows its actual hand-off");
                record.t_ms = crate::responses::millis(at, origin);
                self.responses.prompt_started(at, owner);
                let messages = match &mut record.event {
                    Event::ResponseStarted(started) => {
                        started.prompt = if self.front.is_some() || self.capture == Capture::Everything {
                            Some(prompt)
                        } else {
                            None
                        };
                        u32::try_from(started.messages).expect("actual prompt count fits admitted history")
                    }
                    Event::SessionStarted(_)
                    | Event::SessionEnded(_)
                    | Event::RunStarted(_)
                    | Event::RunCompleted(_)
                    | Event::ConversationOpened(_)
                    | Event::ConversationClosed(_)
                    | Event::ResponseCompleted(_)
                    | Event::TextDelta(_)
                    | Event::ToolStarted(_)
                    | Event::JobEnded(_)
                    | Event::ToolCompleted(_)
                    | Event::CheckStarted(_)
                    | Event::CheckCompleted(_)
                    | Event::MessageReceived(_)
                    | Event::MessageRead(_)
                    | Event::MessageUnread(_)
                    | Event::Notice(_)
                    | Event::Budget(_) => unreachable!("pending entries are response starts"),
                };
                self.responses.prompt_recorded(owner, messages);
                self.record(&record);
            }
            smith_protocol_llm::Measure::Completion { at: _, owner, completion } => {
                if self.front.is_some() || self.capture == Capture::Everything {
                    self.responses.completion(owner, completion);
                }
            }
            smith_protocol_llm::Measure::Delta { at, owner, block, text } => {
                if self.responses.observed(owner) {
                    let record = self.responses.delta(at, origin, owner, block, text);
                    self.record(&record);
                }
            }
        }
    }

    /// A failure before prompt translation still tells its handed-off start exactly once.
    #[expect(clippy::single_match, reason = "bounded production entries keep explicit Option matches")]
    pub(crate) fn attempt_ready(&mut self, owner: Token) {
        match self.pending.remove(&owner) {
            Some(record) => self.record(&record),
            None => {}
        }
    }

    /// Retain only the typed completion's raw Usage and rejection counts before the domain consumes it.
    pub fn received(&mut self, owner: Token, completion: &smith_domain::llm::Completion) -> Token {
        self.responses.received(owner, completion)
    }

    pub(crate) fn failed(
        &mut self,
        owner: Token,
        failure: smith_domain::llm::Failure,
        evidence: smith_domain::llm::Evidence,
        detail: &[u8],
        report: &smith_domain::llm::Report,
    ) {
        assert!(
            detail.len() <= usize::try_from(self.limits.scalar_content).expect("u32 fits"),
            "the native diagnostic respects its configured source bound"
        );
        self.responses.failed(owner, failure, evidence, detail, self.capture, report);
    }

    pub(crate) fn answer_failure(&mut self, record: &mut smith_events::RunCompleted) {
        self.responses.answer_failure(record);
    }

    /// All raw reported counters across this invocation, independently of accepted charges.
    #[must_use]
    pub fn reported(&self) -> smith_events::Usage {
        self.responses.reported()
    }

    /// Begin another run after the previous conversations have settled.
    pub(crate) fn starting(&mut self) {
        self.responses.starting();
    }

    /// Keep an actual root terminal until the gathered session facts have drained.
    pub fn conversation_ended(&mut self, at: Time, owner: Token, end: smith_domain::run::facts::EndKind) {
        self.responses.root_ended(at, owner, end);
    }

    /// Close one opening refused before its session admitted, after all gathered facts drained.
    pub fn facts_drained(&mut self, origin: Time) -> bool {
        match self.responses.facts_drained(origin) {
            Some(record) => {
                self.record(&record);
                true
            }
            None => false,
        }
    }

    /// Join actual session terminals and their accepted Usage/charge to a handed-off response.
    /// One fact yields at most two records, reserved by takes before the caller drains it.
    pub fn session(&mut self, origin: Time, fact: &smith_domain::session::Fact) {
        let records = self.responses.fact(origin, fact);
        for record in &records {
            self.record(record);
        }
    }

    /// Encode one record to every destination, counting it once per type.
    /// The owner calls this only while takes holds, except for the three authoritative records.
    pub fn record(&mut self, record: &Record) {
        assert!(!self.ended, "no record follows the process terminal");
        if let Some(front) = &mut self.front {
            if front.room() == 0 {
                if View::shows(record) {
                    self.front_lost = self.front_lost.checked_add(1).expect("bounded process record count");
                }
            } else if let Some(view) = View::project(record) {
                front.push(view);
            }
        }
        let capture = match &record.event {
            Event::RunCompleted(answer) => {
                assert!(
                    answer_bytes(answer) <= u64::from(self.limits.answer_bytes),
                    "the run admitted aggregate result ownership"
                );
                self.capture.encoded()
            }
            Event::SessionStarted(_)
            | Event::SessionEnded(_)
            | Event::RunStarted(_)
            | Event::ConversationOpened(_)
            | Event::ConversationClosed(_)
            | Event::ResponseStarted(_)
            | Event::ResponseCompleted(_)
            | Event::TextDelta(_)
            | Event::ToolStarted(_)
            | Event::JobEnded(_)
            | Event::ToolCompleted(_)
            | Event::CheckStarted(_)
            | Event::CheckCompleted(_)
            | Event::MessageReceived(_)
            | Event::MessageRead(_)
            | Event::MessageUnread(_)
            | Event::Notice(_)
            | Event::Budget(_) => self.capture.encoded(),
        };
        let bytes = match smith_events::write_with_ceiling(record, &capture, &self.limits.records, self.line)
            .expect("typed sources fit startup's receiving bounds")
        {
            Some(bytes) => bytes,
            None => return,
        };
        assert!(
            u32::try_from(bytes.len()).expect("encoded line fits u32") <= self.line,
            "actual source projection fits its owned record bound"
        );
        let index = kind(&record.event);
        let count = self.counts.get_mut(index).expect("the event has a fixed vocabulary index");
        *count = count.checked_add(1).expect("one process cannot exhaust a record count");
        let authoritative = authoritative(&record.event);
        for index in 0..self.writers.len() {
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            offer(writer, bytes.clone(), authoritative, self.limits.output, self.reserve, self.line);
        }
    }

    /// Emit one authoritative end, with each destination's own loss, then close its stream.
    /// The owner supplies the process clock origin, independently of drain time.
    pub fn ended(&mut self, now: Time, started: Time, ended: &Ended) {
        assert!(!self.ended, "one authoritative end per process");
        self.ended = true;
        let count = self.counts.get_mut(1).expect("session.ended vocabulary index");
        *count = count.checked_add(1).expect("one process cannot exhaust a record count");
        for index in 0..self.writers.len() {
            let loss = self.writers.get(index).expect("bounded destinations").loss;
            let record = ended_record(now, started, ended, self.emitted(), loss);
            let bytes = smith_events::write_with_ceiling(
                &record,
                &smith_events::Capture::None,
                &self.limits.records,
                self.line,
            )
            .expect("the settled end fits its checked record bound")
            .expect("the process terminal survives every capture");
            assert!(
                u32::try_from(bytes.len()).expect("encoded end fits u32") <= self.line,
                "settled end fits the source bound"
            );
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            offer(writer, bytes, true, self.limits.output, self.reserve, self.line);
        }
        self.finish();
    }

    /// The emitted table, including authoritative types and the end record itself once made.
    #[must_use]
    pub fn emitted(&self) -> List<smith_events::Count> {
        let mut result = List::with_capacity(u32::try_from(TYPES).expect("fixed vocabulary fits u32"));
        for index in 0..TYPES {
            let count = *self.counts.get(index).expect("bounded vocabulary index");
            if count > 0 {
                result
                    .push(smith_events::Count { record: names(index).into(), count })
                    .expect("one entry per vocabulary type");
            }
        }
        result
    }

    /// Stop accepting records and close each destination after its queued last line drains.
    pub fn finish(&mut self) {
        for index in 0..self.writers.len() {
            self.writers.get_mut(index).expect("bounded destinations").ended = true;
        }
    }

    /// Abort each stream that still owes its terminal after the authoritative end.
    /// A teardown's abort is the end, not a failure: it gives no destination up
    /// (protocol/events.md, section 5.2; protocol/agent.md, section 6).
    pub fn abort(&mut self, out: &mut Queue<io::Request>) {
        for index in 0..self.writers.len() {
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            match writer.stream {
                Stream::Aborted | Stream::Closed => {}
                Stream::Waiting | Stream::Ready | Stream::Demanding | Stream::Closing | Stream::GivenUp => {
                    writer.abandoned = true;
                    discard(writer);
                    writer.stream = Stream::Aborted;
                    out.push(io::Request::Abort { entity: writer.token });
                }
            }
        }
    }

    /// Whether an IO token belongs to a destination retained until Closed.
    #[must_use]
    pub fn owns(&self, token: Token) -> bool {
        for writer in &self.writers {
            if writer.token == token {
                return true;
            }
        }
        false
    }

    /// The destination's IO event at the owner's clock, including the exact terminal after abandonment.
    pub fn up(&mut self, now: Time, event: io::Event) {
        let token = match &event {
            io::Event::Stream { owner, .. } | io::Event::Failed { owner, .. } | io::Event::Closed { owner } => *owner,
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. } => return,
        };
        for index in 0..self.writers.len() {
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            if writer.token == token {
                received(writer, event, now, self.write_deadline);
                return;
            }
        }
    }

    /// Per-destination demands, owning sends and closes for the caller's IO queue.
    /// Each send is stamped with the owner's clock, from which IO's write deadline runs.
    pub fn down(&mut self, now: Time, out: &mut Queue<io::Request>) {
        for index in 0..self.writers.len() {
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            match writer.stream {
                Stream::Waiting => {
                    if !writer.lines.is_empty() || writer.active.is_some() || writer.pending.is_some() {
                        out.push(io::Request::Stream {
                            stream: writer.token,
                            down: stream::Down::Demand { read: stream::Read::Nothing, room: self.io_output },
                        });
                        writer.stream = Stream::Demanding;
                    } else if writer.ended {
                        out.push(io::Request::Close { entity: writer.token });
                        writer.stream = Stream::Closing;
                    }
                }
                Stream::Ready => {
                    if writer.active.is_none() {
                        writer.active = writer.lines.pop();
                    }
                    match writer.active.take() {
                        Some(line) => send_chunk(writer, line, self.io_output, now, out),
                        None if writer.ended => {
                            out.push(io::Request::Close { entity: writer.token });
                            writer.stream = Stream::Closing;
                        }
                        None => {}
                    }
                }
                Stream::Demanding | Stream::Closing | Stream::GivenUp | Stream::Aborted | Stream::Closed => {}
            }
        }
    }

    /// One destination IO gave up, and why, for the shell's line, exactly once;
    /// a teardown's abort gives none up.
    pub fn pop_given_up(&mut self) -> Option<GivenUp> {
        for index in 0..self.writers.len() {
            let writer = self.writers.get_mut(index).expect("bounded destinations");
            if let Some(why) = writer.given_up.take() {
                return Some(GivenUp { destination: writer.destination, why });
            }
        }
        None
    }

    /// Every retained destination reached its physical IO terminal.
    #[must_use]
    pub fn closed(&self) -> bool {
        for writer in &self.writers {
            match writer.stream {
                Stream::Closed => {}
                Stream::Waiting
                | Stream::Ready
                | Stream::Demanding
                | Stream::Closing
                | Stream::GivenUp
                | Stream::Aborted => {
                    return false;
                }
            }
        }
        true
    }
}

#[expect(clippy::manual_map, reason = "the strict subset requires an explicit match instead of a closure")]
fn ended_record(now: Time, started: Time, ended: &Ended, emitted: List<smith_events::Count>, loss: u64) -> Record {
    let (cpu_ms, peak_rss_bytes) = match ended.usage {
        io::Measured::Read(usage) => (
            Some(smith_events::Cpu {
                user: usage.own.user.as_nanos() / 1_000_000,
                system: usage.own.system.as_nanos() / 1_000_000,
                children_user: usage.children.user.as_nanos() / 1_000_000,
                children_system: usage.children.system.as_nanos() / 1_000_000,
            }),
            Some(smith_events::PeakRss {
                self_bytes: usage.own.peak_rss_bytes,
                children: usage.children.peak_rss_bytes,
            }),
        ),
        io::Measured::Unread => (None, None),
    };
    Record {
        t_ms: now.saturating_since(started).as_nanos() / 1_000_000,
        event: Event::SessionEnded(smith_events::SessionEnded {
            exit: i64::from(ended.exit),
            answered: ended.answered,
            aborted: Some(ended.aborted),
            teardown_ms: match ended.teardown {
                Some(duration) => Some(duration.as_nanos() / 1_000_000),
                None => None,
            },
            emitted,
            loss: smith_events::Loss { events: loss, channel: ended.channel },
            cpu_ms,
            peak_rss_bytes,
        }),
    }
}

fn room(writer: &Writer, output: u32, reserve: u32, line: u32) -> bool {
    let available = output.checked_sub(writer.held).expect("writer stays within byte credit");
    available >= reserve.checked_add(line).expect("startup checked ordinary plus authoritative credit")
        && writer.lines.room() > AUTHORITATIVE
}

fn offer(writer: &mut Writer, bytes: Box<[u8]>, authoritative: bool, output: u32, reserve: u32, line: u32) {
    if writer.abandoned {
        lost(writer);
        return;
    }
    assert!(!writer.ended, "no record follows session.ended");
    let length = u32::try_from(bytes.len()).expect("a record fits its checked bound");
    assert!(length <= line, "the offered record fits the checked line ceiling");
    if !authoritative && !room(writer, output, reserve, length) {
        match writer.delivery {
            Delivery::Complete => unreachable!("the owner checks takes before draining a fact"),
            Delivery::BestEffort => {
                lost(writer);
                return;
            }
        }
    }
    writer.held = writer.held.checked_add(length).expect("startup checked retained byte credit");
    assert!(writer.held <= output, "authoritative room was reserved before the run");
    writer.lines.push(Line { bytes, sent: 0 });
}

fn send_chunk(writer: &mut Writer, mut line: Line, output: u32, now: Time, out: &mut Queue<io::Request>) {
    let length = u32::try_from(line.bytes.len()).expect("a line fits its checked byte bound");
    let remaining = length.checked_sub(line.sent).expect("sent offset stays within the record");
    let chunk = remaining.min(output);
    let end = line.sent.checked_add(chunk).expect("chunk ends within the record");
    let bytes = if line.sent == 0 && end == length {
        writer.pending = Some(Pending { bytes: length });
        line.bytes
    } else {
        let start = usize::try_from(line.sent).expect("u32 fits usize");
        let finish = usize::try_from(end).expect("u32 fits usize");
        let bytes = bytes::copy_of(line.bytes.get(start..finish).expect("chunk lies within the record"));
        line.sent = end;
        if end == length {
            writer.pending = Some(Pending { bytes: length });
        } else {
            writer.pending = Some(Pending { bytes: 0 });
            writer.active = Some(line);
        }
        bytes
    };
    out.push(io::Request::Stream { stream: writer.token, down: stream::Down::Send(bytes) });
    writer.sent = Some(now);
    writer.stream = Stream::Waiting;
}

fn received(writer: &mut Writer, event: io::Event, now: Time, deadline: Duration) {
    match event {
        io::Event::Stream { up, .. } => match up {
            stream::Up::Room => {
                match writer.stream {
                    Stream::Demanding => {}
                    Stream::Waiting
                    | Stream::Ready
                    | Stream::Closing
                    | Stream::GivenUp
                    | Stream::Aborted
                    | Stream::Closed => return,
                }
                if let Some(pending) = writer.pending.take() {
                    writer.held = writer.held.checked_sub(pending.bytes).expect("pending bytes retain credit");
                }
                writer.sent = None;
                writer.stream = Stream::Ready;
            }
            stream::Up::Failed(fault) => {
                let reset = match fault {
                    stream::Fault::Reset => true,
                    stream::Fault::Invalid | stream::Fault::Other => false,
                };
                let why = why(writer, now, deadline, reset);
                abandon(writer, why);
            }
            stream::Up::End => {}
            stream::Up::Bytes(_) => unreachable!("an append destination never requests input"),
        },
        io::Event::Failed { error, .. } => {
            let reset = match error {
                io::Error::Reset => true,
                io::Error::Refused
                | io::Error::Unreachable
                | io::Error::TimedOut
                | io::Error::Busy
                | io::Error::Other => false,
            };
            let why = why(writer, now, deadline, reset);
            abandon(writer, why);
        }
        io::Event::Closed { .. } => {
            match writer.stream {
                Stream::Closing | Stream::GivenUp | Stream::Aborted | Stream::Closed => {}
                Stream::Waiting | Stream::Ready | Stream::Demanding => {
                    let why = why(writer, now, deadline, false);
                    abandon(writer, why);
                }
            }
            writer.stream = Stream::Closed;
        }
        io::Event::Listening { .. }
        | io::Event::Accepted { .. }
        | io::Event::Connecting { .. }
        | io::Event::Connected { .. }
        | io::Event::Output { .. }
        | io::Event::Spawned { .. }
        | io::Event::Exited { .. }
        | io::Event::Usage { .. }
        | io::Event::Resized
        | io::Event::Window { .. }
        | io::Event::Shutdown { .. } => {}
    }
}

/// Why IO failed a destination: a reset is its reader gone; any other failure
/// at or past the outstanding send's write deadline, by this clock, is behind.
fn why(writer: &Writer, now: Time, deadline: Duration, reset: bool) -> Why {
    if reset {
        return Why::ReaderGone;
    }
    match writer.sent {
        Some(at) if now >= at.saturating_add(deadline) => Why::Behind,
        Some(_) | None => Why::WriteFailed,
    }
}

/// IO gave the destination up: it gets nothing more, and the shell names it once.
fn abandon(writer: &mut Writer, why: Why) {
    if writer.abandoned {
        return;
    }
    writer.abandoned = true;
    writer.given_up = Some(why);
    discard(writer);
    writer.stream = Stream::GivenUp;
}

/// Drop what the destination still holds, counting each record as lost.
fn discard(writer: &mut Writer) {
    if let Some(pending) = writer.pending.take()
        && pending.bytes > 0
    {
        lost(writer);
    }
    if writer.active.take().is_some() {
        lost(writer);
    }
    for _ in 0..writer.lines.capacity() {
        match writer.lines.pop() {
            Some(_) => lost(writer),
            None => break,
        }
    }
    writer.sent = None;
    writer.held = 0;
}

fn lost(writer: &mut Writer) {
    writer.loss = writer.loss.checked_add(1).expect("one process cannot exhaust a record count");
}

fn authoritative(event: &Event) -> bool {
    match event {
        Event::SessionStarted(_) | Event::RunCompleted(_) | Event::SessionEnded(_) => true,
        Event::Budget(_)
        | Event::RunStarted(_)
        | Event::ConversationOpened(_)
        | Event::ConversationClosed(_)
        | Event::ResponseStarted(_)
        | Event::ResponseCompleted(_)
        | Event::TextDelta(_)
        | Event::ToolStarted(_)
        | Event::JobEnded(_)
        | Event::ToolCompleted(_)
        | Event::CheckStarted(_)
        | Event::CheckCompleted(_)
        | Event::MessageReceived(_)
        | Event::MessageRead(_)
        | Event::MessageUnread(_) => false,
        Event::Notice(notice) => match notice.kind {
            smith_events::NoticeKind::TeardownAborted => true,
            smith_events::NoticeKind::ReasoningDropped
            | smith_events::NoticeKind::CredentialUnauthorized
            | smith_events::NoticeKind::AccountLimited
            | smith_events::NoticeKind::AccountRefused
            | smith_events::NoticeKind::ForcedExit
            | smith_events::NoticeKind::Unknown(_) => false,
        },
    }
}

fn kind(event: &Event) -> usize {
    match event {
        Event::SessionStarted(_) => 0,
        Event::SessionEnded(_) => 1,
        Event::RunStarted(_) => 2,
        Event::RunCompleted(_) => 3,
        Event::ConversationOpened(_) => 4,
        Event::ConversationClosed(_) => 5,
        Event::ResponseStarted(_) => 6,
        Event::ResponseCompleted(_) => 7,
        Event::TextDelta(_) => 8,
        Event::ToolStarted(_) => 9,
        Event::ToolCompleted(_) => 10,
        Event::CheckStarted(_) => 11,
        Event::CheckCompleted(_) => 12,
        Event::Notice(_) => 13,
        Event::Budget(_) => 14,
        Event::MessageReceived(_) => 15,
        Event::MessageRead(_) => 16,
        Event::MessageUnread(_) => 17,
        Event::JobEnded(_) => 18,
    }
}

fn names(index: usize) -> &'static [u8] {
    [
        b"session.started".as_slice(),
        b"session.ended",
        b"run.started",
        b"run.completed",
        b"conversation.opened",
        b"conversation.closed",
        b"response.started",
        b"response.completed",
        b"text.delta",
        b"tool.started",
        b"tool.completed",
        b"check.started",
        b"check.completed",
        b"notice",
        b"budget",
        b"message.received",
        b"message.read",
        b"message.unread",
        b"job.ended",
    ]
    .get(index)
    .copied()
    .expect("bounded vocabulary index")
}

fn answer_bytes(answer: &smith_events::RunCompleted) -> u64 {
    let result = match &answer.result {
        Some(result) => result,
        None => return 0,
    };
    let mut bytes = match &result.text {
        Some(text) => u64::try_from(text.len()).expect("accepted text length fits u64"),
        None => 0,
    };
    if let Some(fields) = &result.fields {
        for field in fields {
            bytes = bytes
                .checked_add(u64::try_from(field.text.len()).expect("accepted field length fits u64"))
                .expect("run admitted aggregate outcome ownership");
        }
    }
    bytes
}

pub(crate) fn largest_line(
    limits: &smith_events::Limits,
    capture: Capture,
    answer_bytes: u32,
    metadata_string: u32,
) -> Option<u32> {
    // Only a tool call id and name come from provider strings. Configured model,
    // charter, result field and fixed vocabulary names use the smaller source cap.
    // Extra provider room covers both strings even on records without a call.
    let metadata = smith_events::Limits { string: metadata_string, ..*limits };
    let provider = limits.string.checked_mul(12)?;
    let scalar_capture = match capture {
        Capture::None => smith_events::Capture::None,
        Capture::Calls | Capture::Everything => smith_events::Capture::Calls,
    };
    let scalar = smith_events::largest_record_for(&scalar_capture, &metadata)?.checked_add(provider)?;
    match capture {
        Capture::None | Capture::Calls => Some(scalar),
        Capture::Everything => {
            // The record kind controls the size formula; no answer content is allocated here.
            let answer = crate::answer_record(
                crate::AnswerMeta {
                    now: Time::ZERO,
                    run: 0,
                    wind_down: false,
                    read: None,
                    unread: 0,
                    elapsed: Duration::ZERO,
                },
                &smith_domain::run::Answer::Parked { spent: smith_domain::run::Spend::ZERO, turns: 0 },
                Capture::None,
            );
            let shape = smith_events::Limits { content: 0, ..metadata };
            let bound =
                smith_events::largest_size(&Event::RunCompleted(answer), &smith_events::Capture::Everything, &shape)?
                    .checked_add(answer_bytes.checked_mul(6)?)?;
            Some(scalar.max(bound))
        }
    }
}

/// Whether receiving vocabulary and output credit fit (programming-model.md, 6.3).
#[must_use]
pub fn valid(limits: &SinkLimits, capture: Capture) -> bool {
    receiving_valid(limits, capture).is_some()
}

fn receiving_valid(limits: &SinkLimits, capture: Capture) -> Option<()> {
    if limits.metadata_string > limits.records.string || limits.records.items < u32::try_from(TYPES).ok()? {
        return None;
    }
    for index in 0..TYPES {
        if u32::try_from(names(index).len()).ok()? > limits.records.string {
            return None;
        }
    }
    limits.line_ceiling(capture)?;
    if limits.output < output_bound(limits, capture)? {
        return None;
    }
    Some(())
}

/// Peak heap of the destination queues and their bounded output, including codec construction.
#[must_use]
pub fn sinks_worst_case(limits: &SinkLimits, capture: Capture) -> Option<u64> {
    if !valid(limits, capture) {
        return None;
    }
    let line = limits.line_ceiling(capture)?;
    let writer = Queue::<Line>::worst_case(LINE_SLOTS)?.checked_add(u64::from(limits.output))?;
    // The measured writer can retain its encoded body and final newline-owning line together.
    List::<Writer>::worst_case(limits.destinations)?
        .checked_add(u64::from(limits.destinations).checked_mul(writer)?)?
        .checked_add(u64::from(line).checked_mul(2)?)?
        .checked_add(List::<smith_events::Count>::worst_case(u32::try_from(TYPES).ok()?)?)?
        .checked_add(emitted_names_bytes()?)?
        .checked_add(Map::<Token, Record>::worst_case(limits.conversations)?)?
        .checked_add(u64::from(limits.conversations).checked_mul(u64::from(limits.metadata_string).checked_mul(3)?)?)?
        .checked_add(crate::responses::worst_case(limits.conversations, limits.metadata_string, limits.scalar_content)?)
}

fn emitted_names_bytes() -> Option<u64> {
    let mut bytes = 0_u64;
    for index in 0..TYPES {
        bytes = bytes.checked_add(u64::try_from(names(index).len()).ok()?)?;
    }
    Some(bytes)
}

fn authoritative_credit(limits: &SinkLimits, capture: Capture) -> Option<u32> {
    // The three authoritative records and teardown notice contain no provider call id/name or prompt.
    // The accepted answer is the only captured payload they can retain.
    let metadata = smith_events::Limits {
        string: limits.metadata_string,
        content: limits.answer_bytes,
        items: limits.metadata_items,
    };
    largest_line(&metadata, capture, limits.answer_bytes, limits.metadata_string)?.checked_mul(AUTHORITATIVE)
}

pub(crate) fn output_bound(limits: &SinkLimits, capture: Capture) -> Option<u32> {
    authoritative_credit(limits, capture)?.checked_add(ordinary_credit(limits, capture)?)
}

fn ordinary_credit(limits: &SinkLimits, capture: Capture) -> Option<u32> {
    // A prompt or live delta makes one record. A fact can make two records,
    // with one response completion's content, never two held prompts.
    let metadata =
        smith_events::Limits { content: limits.scalar_content, items: limits.metadata_items, ..limits.records };
    let scalar = largest_line(&metadata, capture, limits.answer_bytes, limits.metadata_string)?;
    let extra = match capture {
        Capture::None => 0,
        Capture::Calls | Capture::Everything => limits.fact_extra,
    };
    let pair = scalar.checked_mul(2)?.checked_add(extra)?;
    Some(pair.max(limits.line_ceiling(capture)?))
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods,
        reason = "programming-model.md section 10.2: deterministic harnesses use ordinary Rust"
    )]

    use super::*;

    mod program;
    mod view;

    /// The write deadline IO keeps on each test destination.
    pub(crate) const DEADLINE: Duration = Duration::from_secs(30);

    pub(crate) fn limits(destinations: u32) -> SinkLimits {
        let records = smith_events::Limits { string: 64, content: 1024, items: 19 };
        let line = largest_line(&records, Capture::None, records.content, records.string).unwrap();
        SinkLimits {
            destinations,
            output: line.checked_mul(6).unwrap(),
            records,
            answer_bytes: records.content,
            metadata_string: records.string,
            metadata_items: records.items,
            scalar_content: records.content,
            observation_line: 0,
            fact_extra: 0,
            conversations: 1,
        }
    }

    #[test]
    fn the_live_view_drops_without_holding_a_none_capture_destination() {
        let limits = limits(1);
        let token = Token::new(400);
        let mut sinks = Sinks::with_front(&limits, Capture::None, 7, DEADLINE, true);
        sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
        for number in 0..6 {
            sinks.record(&Record {
                t_ms: number,
                event: Event::TextDelta(smith_events::TextDelta {
                    run: 1,
                    conversation: 1,
                    response: 1,
                    block: 0,
                    text: b"live".as_slice().into(),
                }),
            });
        }
        assert!(sinks.takes(), "a full live view never holds the run");
        assert_eq!(sinks.front_lost(), 2);
        for number in 0..4 {
            let record = sinks.pop_front().expect("the bounded live view");
            let View::TextDelta { t_ms, delta } = record else { panic!("typed delta") };
            assert_eq!(t_ms, number);
            assert_eq!(delta.text.as_ref(), b"live");
        }
        assert!(sinks.pop_front().is_none());
        assert!(drain_ready(&mut sinks, token).is_empty(), "trace capture remains none");
    }

    #[test]
    fn authoritative_credit_uses_its_own_metadata_and_answer_bounds() {
        let mut limits = limits(2);
        limits.records.string = 524_288;
        let line = limits.line_ceiling(Capture::Everything).unwrap();
        let authoritative = largest_line(
            &smith_events::Limits { string: limits.metadata_string, ..limits.records },
            Capture::Everything,
            limits.answer_bytes,
            limits.metadata_string,
        )
        .unwrap();
        limits.output = authoritative.checked_mul(4).unwrap().checked_add(line.checked_mul(2).unwrap()).unwrap();
        assert!(limits.output < line.checked_mul(6).unwrap(), "ordinary identities do not enter authoritative records");
        assert!(sinks_worst_case(&limits, Capture::Everything).is_some());
        let mut sinks = Sinks::new(&limits, Capture::Everything, 1, DEADLINE);
        sinks.add(Destination::Trace, Token::new(1), Delivery::Complete).unwrap();
        assert!(sinks.takes(), "two ordinary records and every authoritative record have separate reserved credit");
    }

    #[test]
    fn only_everything_captures_the_exact_accepted_report_text_and_fields() {
        let actual = smith_domain::run::Answer::Accepted {
            outcome: smith_domain::run::outcome::Declared::Report(smith_domain::run::outcome::Report {
                text: b"Frozen task token: ALPHA.\n\"quoted\"".to_vec().into(),
                fields: Box::new([smith_domain::run::outcome::Field {
                    name: b"source".to_vec().into(),
                    value: b"actual\nreport".to_vec().into(),
                }]),
            }),
            spent: smith_domain::run::Spend {
                units: 17,
                input: Some(19),
                output: Some(23),
                ..smith_domain::run::Spend::ZERO
            },
            turns: 2,
        };
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            let mut limits = limits(1);
            limits.output =
                largest_line(&limits.records, capture, limits.answer_bytes, limits.metadata_string).unwrap() * 6;
            let token = Token::new(11);
            let mut sinks = Sinks::new(&limits, capture, 7, DEADLINE);
            sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
            let answer = crate::answer_record(
                crate::AnswerMeta {
                    now: Time::ZERO,
                    run: 7,
                    wind_down: true,
                    read: Some(31),
                    unread: 1,
                    elapsed: Duration::from_millis(29),
                },
                &actual,
                capture,
            );
            sinks.record(&Record { t_ms: 37, event: Event::RunCompleted(answer) });
            // The same policy grants the request's content in 02.6.
            let request =
                golden(include_bytes!("../../smith-events/golden/v4/response_started_everything.jsonl"), &limits);
            let Event::ResponseStarted(expected) = &request.event else { panic!("golden request") };
            let expected = expected.prompt.clone();
            sinks.record(&request);
            sinks.finish();
            let records = drain(&mut sinks, token);
            let Event::RunCompleted(answer) = &records[0].event else { panic!("typed answer") };
            assert_eq!(answer.status, smith_events::Status::Accepted);
            assert_eq!(
                (answer.run, answer.turns, answer.spent, answer.read, answer.unread, answer.duration_ms),
                (7, 2, 17, Some(31), 1, 29)
            );
            assert!(answer.wind_down);
            assert_eq!(answer.usage.input_tokens, Some(19));
            assert_eq!(answer.usage.output_tokens, Some(23));
            assert_eq!(answer.usage.reasoning_tokens, None);
            let result = answer.result.as_ref().unwrap();
            assert_eq!(result.form, smith_events::Form::Report);
            if capture == Capture::Everything {
                assert_eq!(result.text.as_deref(), Some(b"Frozen task token: ALPHA.\n\"quoted\"".as_slice()));
                assert_eq!(
                    result.fields.as_ref().unwrap().as_slice(),
                    &[smith_events::Field { name: b"source".to_vec().into(), text: b"actual\nreport".to_vec().into() }]
                );
            } else {
                assert_eq!(result.text, None);
                assert_eq!(result.fields, None);
            }
            let Event::ResponseStarted(request) = &records[1].event else { panic!("typed request") };
            assert_eq!(request.prompt, if capture == Capture::Everything { expected } else { None });
        }
    }

    #[test]
    fn the_complete_emitted_vocabulary_must_fit_the_receiving_bounds() {
        let mut bounds = limits(1);
        bounds.records.string = 18;
        bounds.metadata_string = 18;
        assert!(sinks_worst_case(&bounds, Capture::None).is_none());
        bounds.records.string = 19;
        bounds.metadata_string = 19;
        assert!(sinks_worst_case(&bounds, Capture::None).is_some());
        bounds.records.items = 17;
        assert!(sinks_worst_case(&bounds, Capture::None).is_none());
    }

    #[test]
    fn settled_end_preserves_read_or_unread_resources_and_the_actual_exit() {
        let measured = io::kernel::Usage {
            own: io::kernel::Resources {
                user: Duration::from_nanos(1_000_000),
                system: Duration::from_nanos(2_000_000),
                peak_rss_bytes: 5,
            },
            children: io::kernel::Resources {
                user: Duration::from_nanos(3_000_000),
                system: Duration::from_nanos(4_000_000),
                peak_rss_bytes: 6,
            },
        };
        for usage in [io::Measured::Read(measured), io::Measured::Unread] {
            for exit in [0, 1] {
                let limits = limits(1);
                let token = Token::new(11);
                let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
                sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
                sinks.record(&counted(1));
                let ended = Ended {
                    exit,
                    answered: exit == 0,
                    aborted: false,
                    teardown: if exit == 0 { Some(Duration::from_nanos(3_000_000)) } else { None },
                    usage,
                    channel: Some(7),
                };
                sinks.ended(Time::from_nanos(12_000_000), Time::from_nanos(2_000_000), &ended);
                let records = drain(&mut sinks, token);
                assert!(sinks.closed());
                assert_eq!(records.len(), 2);
                let last = records.last().unwrap();
                assert_eq!(last.t_ms, 10);
                let Event::SessionEnded(end) = &last.event else { panic!("one final process end") };
                assert_eq!(end.exit, i64::from(exit));
                assert_eq!(end.answered, exit == 0);
                assert_eq!(end.teardown_ms, if exit == 0 { Some(3) } else { None });
                assert_eq!(end.loss, smith_events::Loss { events: 0, channel: Some(7) });
                assert_eq!(end.emitted.iter().map(|entry| entry.count).sum::<u64>(), 2);
                assert_eq!(
                    end.emitted.iter().find(|entry| entry.record.as_ref() == b"session.ended").unwrap().count,
                    1
                );
                match usage {
                    io::Measured::Read(_) => {
                        assert_eq!(
                            end.cpu_ms,
                            Some(smith_events::Cpu { user: 1, system: 2, children_user: 3, children_system: 4 })
                        );
                        assert_eq!(end.peak_rss_bytes, Some(smith_events::PeakRss { self_bytes: 5, children: 6 }));
                    }
                    io::Measured::Unread => {
                        assert_eq!(end.cpu_ms, None);
                        assert_eq!(end.peak_rss_bytes, None);
                    }
                }
            }
        }
    }

    #[test]
    fn each_settled_end_reports_its_destination_loss_and_counts_itself_once() {
        let count = u64::from(LINE_SLOTS).next_power_of_two();
        let limits = limits(2);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::BestEffort).unwrap();
        sinks.add(Destination::Output, Token::new(12), Delivery::BestEffort).unwrap();
        sinks.up(Time::ZERO, io::Event::Failed { owner: Token::new(12), error: io::Error::Other });
        sinks.up(Time::ZERO, io::Event::Closed { owner: Token::new(12) });
        for name in 0..count {
            sinks.record(&counted(name));
        }
        let ended = Ended {
            exit: 0,
            answered: true,
            aborted: false,
            teardown: None,
            usage: io::Measured::Unread,
            channel: None,
        };
        sinks.ended(Time::ZERO, Time::ZERO, &ended);
        let records = drain(&mut sinks, token);
        let Event::SessionEnded(end) = &records.last().unwrap().event else { panic!("end is last") };
        assert_eq!(end.emitted.iter().map(|entry| entry.count).sum::<u64>(), count + 1);
        assert_eq!(u64::try_from(records.len()).unwrap().checked_add(end.loss.events).unwrap(), count + 1);
        assert_eq!(sinks.writers.get(1).unwrap().loss, count + 1);
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Output, why: Why::WriteFailed }));
        assert_eq!(sinks.pop_given_up(), None);
        assert!(sinks.closed());
    }

    fn settled() -> Ended {
        Ended { exit: 0, answered: true, aborted: false, teardown: None, usage: io::Measured::Unread, channel: None }
    }

    /// One trace destination with one counted record sent at `at` and not yet drained.
    fn sending(at: Time) -> (Sinks, Token, Queue<io::Request>) {
        let limits = limits(1);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
        let mut out = Queue::with_capacity(2);
        sinks.record(&counted(1));
        sinks.down(at, &mut out);
        let Some(io::Request::Stream { down: stream::Down::Demand { .. }, .. }) = out.pop() else {
            panic!("a full-credit demand")
        };
        grant(&mut sinks, token);
        sinks.down(at, &mut out);
        let Some(io::Request::Stream { down: stream::Down::Send(_), .. }) = out.pop() else { panic!("one send") };
        (sinks, token, out)
    }

    #[test]
    fn a_failed_write_mid_run_names_its_destination_and_why_once_and_later_records_count_lost() {
        let at = Time::from_nanos(5_000_000_000);
        let (mut sinks, token, mut out) = sending(at);
        sinks.record(&counted(2));
        sinks.up(at, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why: Why::WriteFailed }));
        assert_eq!(sinks.pop_given_up(), None, "the shell names a destination once");
        assert_eq!(sinks.writers.get(0).unwrap().loss, 2, "the record in flight and the one queued");
        sinks.record(&counted(3));
        sinks.ended(at, Time::ZERO, &settled());
        assert_eq!(sinks.writers.get(0).unwrap().loss, 4, "a given-up destination gets nothing more, its end too");
        sinks.down(at, &mut out);
        assert!(out.is_empty(), "nothing is tried there again");
        sinks.up(at, io::Event::Closed { owner: token });
        assert_eq!(sinks.pop_given_up(), None);
        assert!(sinks.closed());
    }

    #[test]
    fn a_destination_failed_at_its_write_deadline_was_behind_by_the_sinks_own_clock() {
        let at = Time::from_nanos(5_000_000_000);
        let due = at.saturating_add(DEADLINE);
        let before = Time::from_nanos(due.as_nanos().checked_sub(1).unwrap());
        for (now, why) in [(before, Why::WriteFailed), (due, Why::Behind)] {
            let (mut sinks, token, _) = sending(at);
            sinks.up(now, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
            assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why }));
        }
        let (mut sinks, token, mut out) = sending(at);
        sinks.down(at, &mut out);
        let Some(io::Request::Stream { down: stream::Down::Demand { .. }, .. }) = out.pop() else {
            panic!("the demand that proves the send drained")
        };
        grant(&mut sinks, token);
        sinks.down(due, &mut out);
        assert!(out.is_empty(), "the drained send leaves nothing in flight");
        sinks.up(due, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
        assert_eq!(
            sinks.pop_given_up(),
            Some(GivenUp { destination: Destination::Trace, why: Why::WriteFailed }),
            "with no write outstanding, the deadline cannot have passed"
        );
    }

    #[test]
    fn a_reset_stream_says_its_reader_went_away() {
        let at = Time::from_nanos(5_000_000_000);
        let (mut sinks, token, _) = sending(at);
        sinks.up(
            at.saturating_add(DEADLINE),
            io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Reset) },
        );
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why: Why::ReaderGone }));
        let (mut sinks, token, _) = sending(at);
        sinks.up(at, io::Event::Failed { owner: token, error: io::Error::Reset });
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why: Why::ReaderGone }));
    }

    #[test]
    fn a_teardowns_abort_gives_no_destination_up() {
        let at = Time::from_nanos(5_000_000_000);
        let (mut sinks, token, mut out) = sending(at);
        sinks.record(&counted(2));
        sinks.ended(at, Time::ZERO, &settled());
        sinks.abort(&mut out);
        let Some(io::Request::Abort { entity }) = out.pop() else { panic!("the abort") };
        assert_eq!(entity, token);
        assert!(out.is_empty());
        sinks.abort(&mut out);
        assert!(out.is_empty(), "a stream is aborted once");
        assert_eq!(sinks.pop_given_up(), None, "the abort is the end, not a failure");
        sinks.up(at, io::Event::Stream { owner: token, up: stream::Up::Room });
        sinks.up(at, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
        sinks.up(at, io::Event::Failed { owner: token, error: io::Error::Other });
        assert_eq!(sinks.pop_given_up(), None, "what follows the abort gives nothing up either");
        assert!(!sinks.closed(), "the aborted stream still owes Closed");
        sinks.down(at, &mut out);
        assert!(out.is_empty(), "an aborted destination is never written again");
        sinks.up(at, io::Event::Closed { owner: token });
        assert_eq!(sinks.pop_given_up(), None);
        assert!(sinks.closed());
        assert_eq!(sinks.writers.get(0).unwrap().loss, 3, "what the abort cut off counts as lost");
    }

    #[test]
    fn a_failure_already_named_before_the_abort_is_still_named_once() {
        let at = Time::from_nanos(5_000_000_000);
        let (mut sinks, token, mut out) = sending(at);
        sinks.up(at, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
        sinks.abort(&mut out);
        let Some(io::Request::Abort { .. }) = out.pop() else { panic!("the abort") };
        sinks.up(at, io::Event::Closed { owner: token });
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why: Why::WriteFailed }));
        assert_eq!(sinks.pop_given_up(), None);
        assert!(sinks.closed());
    }

    fn counted(name: u64) -> Record {
        Record { t_ms: name, event: Event::MessageUnread(smith_events::MessageUnread { run: 7, name }) }
    }

    fn golden(bytes: &[u8], limits: &SinkLimits) -> Record {
        smith_events::read(bytes, &limits.records).unwrap().unwrap()
    }

    pub(crate) fn grant(sinks: &mut Sinks, token: Token) {
        sinks.up(Time::ZERO, io::Event::Stream { owner: token, up: stream::Up::Room });
    }

    pub(crate) fn drain(sinks: &mut Sinks, token: Token) -> Vec<Record> {
        drain_until(sinks, token, true)
    }

    pub(crate) fn drain_ready(sinks: &mut Sinks, token: Token) -> Vec<Record> {
        drain_until(sinks, token, false)
    }

    fn drain_until(sinks: &mut Sinks, token: Token, ended: bool) -> Vec<Record> {
        let mut out = Queue::with_capacity(2);
        let mut read = Vec::new();
        let mut reader = smith_events::Reader::with_ceiling(&sinks.limits.records, sinks.line).unwrap();
        for _ in 0_u32..10_000 {
            sinks.down(Time::ZERO, &mut out);
            match out.pop() {
                Some(io::Request::Stream { down: stream::Down::Demand { room, .. }, .. }) => {
                    assert_eq!(room, sinks.io_output);
                    grant(sinks, token);
                }
                Some(io::Request::Stream { down: stream::Down::Send(bytes), .. }) => {
                    assert!(bytes.len() <= usize::try_from(sinks.io_output).unwrap());
                    let line = reader.feed(&bytes).unwrap();
                    assert_eq!(line.consumed, bytes.len());
                    if let Some(record) = line.record {
                        read.push(record);
                    }
                }
                Some(io::Request::Close { .. }) => {
                    reader.finish().unwrap();
                    sinks.up(Time::ZERO, io::Event::Closed { owner: token });
                    return read;
                }
                None => {
                    assert!(!ended, "writer failed to advance before close");
                    reader.finish().unwrap();
                    return read;
                }
                Some(other) => panic!("unexpected append request: {other:?}"),
            }
        }
        panic!("bounded writer did not close");
    }

    #[test]
    fn a_slow_complete_destination_preserves_order_and_reserves_authoritative_room() {
        let count = u64::from(LINE_SLOTS).next_power_of_two();
        let limits = limits(1);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
        let mut expected = Vec::new();
        let start = golden(include_bytes!("../../smith-events/golden/v4/session_started_none.jsonl"), &limits);
        sinks.record(&start);
        expected.push(start);
        for name in 0..count {
            if !sinks.takes() {
                break;
            }
            let record = counted(name);
            sinks.record(&record);
            expected.push(record);
        }
        assert!(!sinks.takes(), "the actual output cap holds the next fact");
        let completed = golden(include_bytes!("../../smith-events/golden/v4/run_completed_none.jsonl"), &limits);
        sinks.record(&completed);
        expected.push(completed);
        let end = golden(include_bytes!("../../smith-events/golden/v4/session_ended_none.jsonl"), &limits);
        sinks.record(&end);
        expected.push(end);
        sinks.finish();
        assert_eq!(drain(&mut sinks, token), expected);
        assert_eq!(sinks.writers.get(0).unwrap().loss, 0);
        assert!(sinks.closed());
        let emitted = sinks.emitted();
        assert_eq!(emitted.iter().map(|entry| entry.count).sum::<u64>(), u64::try_from(expected.len()).unwrap());
    }

    #[test]
    fn best_effort_loses_only_the_counted_records_that_find_it_behind() {
        let count = u64::from(LINE_SLOTS).next_power_of_two();
        let limits = limits(1);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::BestEffort).unwrap();
        for name in 0..count {
            sinks.record(&counted(name));
        }
        assert!(sinks.takes(), "best effort never holds its owner");
        let lost = sinks.writers.get(0).unwrap().loss;
        assert!(lost > 0);
        sinks.finish();
        let records = drain(&mut sinks, token);
        assert_eq!(u64::try_from(records.len()).unwrap().checked_add(lost).unwrap(), count);
        assert_eq!(sinks.emitted().get(0).unwrap().count, count);
    }

    #[test]
    fn a_failed_stream_counts_queued_inflight_and_later_records_and_notifies_once() {
        let limits = limits(1);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, 1, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
        sinks.record(&counted(1));
        let mut out = Queue::with_capacity(2);
        sinks.down(Time::ZERO, &mut out);
        let Some(io::Request::Stream { down: stream::Down::Demand { .. }, .. }) = out.pop() else {
            panic!("actual full-credit demand")
        };
        grant(&mut sinks, token);
        sinks.down(Time::ZERO, &mut out);
        let Some(io::Request::Stream { down: stream::Down::Send(_), .. }) = out.pop() else {
            panic!("one owning send")
        };
        assert!(sinks.writers.get(0).unwrap().active.is_some(), "only a prefix left the owner");
        assert_eq!(
            sinks.writers.get(0).unwrap().pending.as_ref().unwrap().bytes,
            0,
            "prefixes keep the whole record credit"
        );
        sinks.record(&counted(2));
        sinks.up(Time::ZERO, io::Event::Stream { owner: token, up: stream::Up::Failed(stream::Fault::Other) });
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Trace, why: Why::WriteFailed }));
        assert_eq!(sinks.pop_given_up(), None);
        sinks.record(&counted(3));
        assert!(sinks.takes());
        assert_eq!(sinks.writers.get(0).unwrap().loss, 3);
        sinks.up(Time::ZERO, io::Event::Failed { owner: token, error: io::Error::TimedOut });
        assert_eq!(sinks.pop_given_up(), None);
        assert!(!sinks.closed(), "the append descriptor still owes Closed");
        sinks.up(Time::ZERO, io::Event::Closed { owner: token });
        assert!(sinks.closed());
        sinks.record(&counted(4));
        assert_eq!(sinks.writers.get(0).unwrap().loss, 4);
        sinks.down(Time::ZERO, &mut out);
        assert!(out.is_empty(), "an abandoned destination is never written again");
    }

    #[test]
    fn one_destination_failing_does_not_abandon_the_other() {
        let limits = limits(2);
        let token = Token::new(11);
        let mut sinks = Sinks::new(&limits, Capture::None, limits.output, DEADLINE);
        sinks.add(Destination::Trace, token, Delivery::Complete).unwrap();
        sinks.add(Destination::Output, Token::new(12), Delivery::Complete).unwrap();
        sinks.record(&counted(1));
        sinks.up(Time::ZERO, io::Event::Failed { owner: Token::new(12), error: io::Error::Other });
        sinks.up(Time::ZERO, io::Event::Closed { owner: Token::new(12) });
        sinks.record(&counted(2));
        assert_eq!(sinks.pop_given_up(), Some(GivenUp { destination: Destination::Output, why: Why::WriteFailed }));
        sinks.finish();
        assert_eq!(drain(&mut sinks, token), [counted(1), counted(2)]);
        assert_eq!(sinks.writers.get(0).unwrap().loss, 0);
        assert_eq!(sinks.writers.get(1).unwrap().loss, 2);
        assert!(sinks.closed());
    }
}
