//! Best-effort text beside the host domain (domain/host.md, section 2;
//! protocol/channel.md, sections 4 and 8). The queue knows the shown run,
//! never its policy; `receive` keeps the domain's read terminal as Progress.
use alloc::boxed::Box;
use skein_lib::{Duration, Queue, Time, Token};
use smith_host_domain::{self as host, process};
use smith_host_protocol::OpenEvent;

/// The front's four lossy slots, independent of channel flow control.
pub const LIVE_SLOTS: u32 = 4;

/// Protocol/limits.md, section 3.6: maximum bytes in one live piece.
pub const LIVE_BYTES: u32 = smith_channel::CEILINGS.live_bytes;

/// One text piece, sent beside the domain to the shown activation's front.
#[derive(Debug)]
pub struct LivePiece {
    pub turn: u32,
    pub retry: u32,
    pub offset: u64,
    pub bytes: Box<[u8]>,
}

/// A told turn and local measurements, sent to the chat front before release.
#[derive(Debug)]
pub struct RemoteTurn {
    pub number: u32,
    pub elapsed: Duration,
    pub since_start: Duration,
    pub turn: smith_domain::Turn,
}

#[derive(Debug)]
pub(crate) struct Live {
    run: Option<Token>,
    pieces: Queue<LivePiece>,
    lost: u64,
}

impl Live {
    pub(crate) fn new() -> Self {
        Self { run: None, pieces: Queue::with_capacity(LIVE_SLOTS), lost: 0 }
    }

    pub(crate) fn clear(&mut self) {
        self.run = None;
        for _ in 0..self.pieces.len() {
            self.pieces.pop();
            self.lost = self.lost.saturating_add(1);
        }
    }

    pub(crate) fn start(&mut self, run: Token, asked: bool) {
        self.clear();
        self.run = if asked { Some(run) } else { None };
    }

    pub(crate) fn receive(&mut self, run: Token, event: OpenEvent) -> host::Input {
        match event {
            OpenEvent::Live { turn, retry, kind, offset, bytes, .. } => {
                match kind {
                    smith_channel::LiveKind::Text => {
                        if self.run == Some(run) && self.pieces.room() > 0 {
                            self.pieces.push(LivePiece { turn, retry, offset, bytes });
                        } else {
                            self.lost = self.lost.saturating_add(1);
                        }
                    }
                    smith_channel::LiveKind::Reasoning | smith_channel::LiveKind::Input => {}
                }
                host::Input::Process(process::Event::Progress { owner: run })
            }
            other @ (OpenEvent::Opened { .. }
            | OpenEvent::Hangup { .. }
            | OpenEvent::Sent { .. }
            | OpenEvent::Unsent { .. }
            | OpenEvent::WriteFailed
            | OpenEvent::Answer { .. }
            | OpenEvent::Admitted
            | OpenEvent::MessageRefused { .. }
            | OpenEvent::Waiting { .. }
            | OpenEvent::Long { .. }
            | OpenEvent::LongDone
            | OpenEvent::Turn { .. }
            | OpenEvent::Fact { .. }
            | OpenEvent::Unauthorized { .. }
            | OpenEvent::Limited { .. }
            | OpenEvent::Refused { .. }
            | OpenEvent::Call { .. }
            | OpenEvent::Withdraw { .. }) => crate::process::channel_event(run, other),
        }
    }

    pub(crate) fn pop_through(&mut self, through: Option<u32>) -> Option<LivePiece> {
        if let Some(number) = through
            && let Some(piece) = self.pieces.iter().next()
            && piece.turn > number
        {
            return None;
        }
        self.pieces.pop()
    }

    pub(crate) fn pending(&self) -> bool {
        !self.pieces.is_empty()
    }

    pub(crate) const fn lost(&self) -> u64 {
        self.lost
    }
}

// Sent-unread names share the domain inbox: messages + one completed line.
// Received turn measurements share the host unacknowledged-turn bound
// until SaveTurn accepts them. A Waiting
// fence can release inbox names before a turn, retaining only its last time.
pub(crate) struct Timing {
    pub(crate) generation: u64,
    pub(crate) started: Time,
    pub(crate) previous: Time,
    pub(crate) number: u32,
    pub(crate) elapsed: Duration,
    run: Option<Token>,
    sent: Queue<(Token, Time)>,
    read: Option<(Token, Time)>,
    measured: Time,
    turns: Queue<(u32, Option<Token>, Time, Duration)>,
}

impl Timing {
    pub(crate) fn new(limits: &smith_local_domain::AgentLimits) -> Self {
        let (messages, turns) = Self::bounds(limits);
        Self {
            generation: 0,
            started: Time::ZERO,
            previous: Time::ZERO,
            number: 0,
            elapsed: Duration::ZERO,
            run: None,
            sent: Queue::with_capacity(messages),
            read: None,
            measured: Time::ZERO,
            turns: Queue::with_capacity(turns),
        }
    }

    fn bounds(limits: &smith_local_domain::AgentLimits) -> (u32, u32) {
        match limits {
            smith_local_domain::AgentLimits::Spawned(host) => (host.messages.saturating_add(1), host.turns),
            smith_local_domain::AgentLimits::Inline(_) => (0, 0),
        }
    }

    pub(crate) fn activate(&mut self, now: Time) {
        self.generation = self.generation.checked_add(1).expect("front generations are never reused");
        self.started = now;
        self.previous = now;
        self.measured = now;
        self.run = None;
        self.read = None;
        for _ in 0..self.sent.len() {
            self.sent.pop();
        }
        for _ in 0..self.turns.len() {
            self.turns.pop();
        }
    }

    pub(crate) fn sent(&mut self, request: &process::Request, now: Time) {
        match request {
            process::Request::Send { owner, message: host::Down::Start { start, .. }, .. } => {
                self.run = Some(*owner);
                self.started = now;
                self.previous = now;
                self.measured = now;
                for message in &start.messages {
                    // Missing times fall back to the previous told turn.
                    let _timestamp: Result<(), (Token, Time)> = self.sent.try_push((message.name, now));
                }
            }
            process::Request::Send { message: host::Down::Message { name, .. }, .. } => {
                let _timestamp: Result<(), (Token, Time)> = self.sent.try_push((*name, now));
            }
            process::Request::Spawn { .. }
            | process::Request::Read { .. }
            | process::Request::Signal { .. }
            | process::Request::Wait { .. }
            | process::Request::Reap { .. }
            | process::Request::Send {
                message:
                    host::Down::Answer { .. }
                    | host::Down::Acknowledge { .. }
                    | host::Down::Grant { .. }
                    | host::Down::Cancel,
                ..
            } => {}
        }
    }

    // Only a matching sent token advances this observation. The domain still
    // validates each record; no measurement reaches the front before SaveTurn.
    pub(crate) fn received(&mut self, event: &process::Event, now: Time) {
        match event {
            process::Event::Received { owner, message } if self.run == Some(*owner) => match message {
                host::Up::Turn { turn } => self.measure(turn.number, turn.read, now),
                host::Up::Waiting { read } => {
                    self.fence(*read);
                }
                host::Up::MessageRefused { name, .. } => {
                    for _ in 0..self.sent.len() {
                        let entry = self.sent.pop().expect("bounded sent names");
                        if entry.0 != *name {
                            self.sent.push(entry);
                        }
                    }
                }
                host::Up::Admitted
                | host::Up::Call { .. }
                | host::Up::Withdraw { .. }
                | host::Up::Fact { .. }
                | host::Up::Long { .. }
                | host::Up::LongDone
                | host::Up::Unauthorized { .. }
                | host::Up::Limited { .. }
                | host::Up::Refused { .. }
                | host::Up::Answer { .. } => {}
            },
            process::Event::Received { .. }
            | process::Event::Spawned { .. }
            | process::Event::Unspawned { .. }
            | process::Event::Sent { .. }
            | process::Event::Unsent { .. }
            | process::Event::Progress { .. }
            | process::Event::Malformed { .. }
            | process::Event::Hangup { .. }
            | process::Event::Signalled { .. }
            | process::Event::Exited { .. }
            | process::Event::Reaped { .. } => {}
        }
    }

    fn fence(&mut self, read: Option<Token>) -> Option<Time> {
        let name = read?;
        let mut known = false;
        for entry in &self.sent {
            if entry.0 == name {
                known = true;
                break;
            }
        }
        if known {
            for _ in 0..self.sent.len() {
                let entry = self.sent.pop().expect("known sent prefix");
                self.read = Some(entry);
                if entry.0 == name {
                    break;
                }
            }
        }
        match self.read {
            Some((token, time)) if token == name => Some(time),
            Some(_) | None => None,
        }
    }

    fn measure(&mut self, number: u32, read: Option<Token>, now: Time) {
        let baseline = match self.fence(read) {
            Some(sent) => self.measured.max(sent),
            None => self.measured,
        };
        // A full queue can only follow invalid turn credit. Such records never
        // acquire a SaveTurn; the domain owns their fault and teardown.
        if self.turns.room() > 0 {
            self.turns.push((number, read, now, now.saturating_since(baseline)));
        }
        self.measured = now;
    }

    pub(crate) fn told(&mut self, number: u32, read: Option<Token>, now: Time) {
        self.number = number;
        match self.turns.pop() {
            Some((measured_number, measured_read, at, elapsed)) => {
                assert_eq!(number, measured_number, "SaveTurn accepts the received turn in order");
                assert_eq!(read, measured_read, "SaveTurn accepts the received read fence");
                self.elapsed = elapsed;
                self.previous = at;
            }
            None => {
                self.elapsed = now.saturating_since(self.previous);
                self.previous = now;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn piece() -> OpenEvent {
        OpenEvent::Live {
            turn: 1,
            retry: 0,
            kind: smith_channel::LiveKind::Text,
            offset: 0,
            bytes: b"hello".as_slice().into(),
            call: None,
        }
    }

    #[test]
    fn text_stays_beside_the_progress_terminal() {
        let mut live = Live::new();
        let run = Token::new(1);
        live.start(run, true);
        let host::Input::Process(process::Event::Progress { owner }) = live.receive(run, piece()) else {
            panic!("a live read reports Progress")
        };
        assert_eq!(owner, run);
        assert_eq!(live.pop_through(None).unwrap().bytes.as_ref(), b"hello");
    }

    #[test]
    fn full_slots_drop_without_holding_a_read() {
        let mut live = Live::new();
        let run = Token::new(1);
        live.start(run, true);
        for _ in 0..=LIVE_SLOTS {
            let host::Input::Process(process::Event::Progress { owner }) = live.receive(run, piece()) else {
                panic!("even a dropped piece reports Progress")
            };
            assert_eq!(owner, run);
        }
        assert_eq!(live.pieces.len(), LIVE_SLOTS);
        assert_eq!(live.lost(), 1);
    }

    #[test]
    fn another_run_cannot_reach_the_shown_activation() {
        let mut live = Live::new();
        live.start(Token::new(1), true);
        live.receive(Token::new(2), piece());
        assert!(!live.pending());
        assert_eq!(live.lost(), 1);
    }

    #[test]
    fn a_later_turn_waits_for_the_pending_told_turn() {
        let mut live = Live::new();
        let run = Token::new(1);
        live.start(run, true);
        let OpenEvent::Live { retry, kind, offset, bytes, call, .. } = piece() else { unreachable!() };
        live.receive(run, OpenEvent::Live { turn: 2, retry, kind, offset, bytes, call });
        assert!(live.pop_through(Some(1)).is_none());
        assert_eq!(live.pop_through(Some(2)).unwrap().turn, 2);
    }

    #[test]
    fn a_new_activation_drops_the_previous_runs_queued_text() {
        let mut live = Live::new();
        let run = Token::new(1);
        live.start(run, true);
        live.receive(run, piece());
        live.clear();
        live.receive(run, piece());
        assert!(!live.pending());
        assert_eq!(live.lost(), 2);
    }

    #[test]
    fn an_unasked_start_keeps_no_pieces() {
        let mut live = Live::new();
        let run = Token::new(1);
        live.start(run, false);
        live.receive(run, piece());
        assert!(!live.pending());
    }

    fn clock(milliseconds: u64) -> Time {
        Time::ZERO.saturating_add(Duration::from_millis(milliseconds))
    }

    fn timing() -> Timing {
        let mut timing = Timing::new(&crate::tests::config().limits.local.agent);
        timing.activate(Time::ZERO);
        timing.run = Some(Token::new(7));
        timing
    }

    fn send(timing: &mut Timing, name: u64, milliseconds: u64) {
        timing.sent(
            &process::Request::Send {
                owner: Token::new(7),
                process: Token::new(8),
                message: host::Down::Message {
                    finish_requested: false,
                    name: Token::new(name),
                    label: Box::new([]),
                    text: Box::new([]),
                },
            },
            clock(milliseconds),
        );
    }

    fn receive_turn(timing: &mut Timing, number: u32, name: u64, milliseconds: u64) {
        let value = smith_domain::Turn {
            version: 1,
            kind: smith_domain::session::record::Kind::Ordinary,
            endpoint: None,
            model: Box::new([]),
            effort: None,
            dialect: 1,
            sequence: number,
            usage: smith_domain::session::llm::Usage::NONE,
            spent: 0,
            jobs: 0,
            messages: Box::new([]),
        };
        timing.received(
            &process::Event::Received {
                owner: Token::new(7),
                message: host::Up::Turn {
                    turn: host::Turn {
                        opens_window: false,
                        number,
                        spent: 0,
                        read: Some(Token::new(name)),
                        body: host::TurnValue::new(value, 1024).unwrap(),
                    },
                },
            },
            clock(milliseconds),
        );
    }

    #[test]
    fn an_unread_message_preserves_the_running_turns_elapsed_time() {
        let mut timing = timing();
        send(&mut timing, 1, 0);
        send(&mut timing, 2, 900);
        receive_turn(&mut timing, 1, 1, 1000);
        timing.told(1, Some(Token::new(1)), clock(1000));
        assert_eq!(timing.elapsed, Duration::from_millis(1000));
        receive_turn(&mut timing, 2, 2, 1200);
        timing.told(2, Some(Token::new(2)), clock(1200));
        assert_eq!(timing.elapsed, Duration::from_millis(200));
        send(&mut timing, 3, 1900);
        receive_turn(&mut timing, 3, 3, 2200);
        timing.told(3, Some(Token::new(3)), clock(2200));
        assert_eq!(timing.elapsed, Duration::from_millis(300));
        receive_turn(&mut timing, 4, 3, 2300);
        timing.told(4, Some(Token::new(3)), clock(2300));
        assert_eq!(timing.elapsed, Duration::from_millis(100));
    }

    #[test]
    fn waiting_fences_release_timestamp_slots_before_a_turn_is_told() {
        let mut timing = timing();
        // More messages than the fixed inbox: Waiting reads each before the next.
        for name in 1..=timing.sent.capacity().checked_add(1).unwrap() {
            send(&mut timing, u64::from(name), 900);
            timing.received(
                &process::Event::Received {
                    owner: Token::new(7),
                    message: host::Up::Waiting { read: Some(Token::new(u64::from(name))) },
                },
                clock(950),
            );
            assert!(timing.sent.is_empty());
        }
        let last = u64::from(timing.sent.capacity().checked_add(1).unwrap());
        receive_turn(&mut timing, 1, last, 1000);
        timing.told(1, Some(Token::new(last)), clock(1000));
        assert_eq!(timing.elapsed, Duration::from_millis(100));
    }

    #[test]
    fn a_later_read_preserves_a_queued_turns_measurement() {
        let mut timing = timing();
        send(&mut timing, 1, 100);
        receive_turn(&mut timing, 1, 1, 1000);
        send(&mut timing, 2, 1500);
        receive_turn(&mut timing, 2, 2, 2000);
        timing.told(1, Some(Token::new(1)), clock(2100));
        assert_eq!(timing.elapsed, Duration::from_millis(900));
        assert_eq!(timing.previous, clock(1000));
        timing.told(2, Some(Token::new(2)), clock(2200));
        assert_eq!(timing.elapsed, Duration::from_millis(500));
        assert_eq!(timing.previous, clock(2000));
    }

    #[test]
    fn refused_messages_release_their_timestamp_slots() {
        let mut timing = timing();
        for name in 1..=timing.sent.capacity().checked_add(1).unwrap() {
            send(&mut timing, u64::from(name), 900);
            timing.received(
                &process::Event::Received {
                    owner: Token::new(7),
                    message: host::Up::MessageRefused {
                        name: Token::new(u64::from(name)),
                        reason: host::MessageRefusal::Full,
                    },
                },
                clock(950),
            );
            assert!(timing.sent.is_empty());
        }
    }

    fn start(timing: &mut Timing, names: u32, milliseconds: u64) {
        let configuration = crate::tests::config();
        let mut messages = skein_lib::List::with_capacity(names);
        for name in 1..=names {
            messages
                .push(host::Message {
                    finish_requested: false,
                    name: Token::new(u64::from(name)),
                    label: Box::new([]),
                    text: Box::new([]),
                })
                .unwrap();
        }
        timing.sent(
            &process::Request::Send {
                owner: Token::new(7),
                process: Token::new(8),
                message: host::Down::Start {
                    start: host::Start {
                        live: None,
                        identity: None,
                        messages: messages.into_boxed(),
                        logical_run: Token::new(9),
                        activation: 1,
                        workspace: None,
                        charter: host::Charter::new(smith_local_domain::charter(&configuration.local), u64::MAX)
                            .unwrap(),
                        transcript: None,
                        answered: Box::new([]),
                        directories: Box::new([]),
                        grants: Box::new([]),
                    },
                    window: host::Window { turns: 2, bytes: 1024 },
                },
            },
            clock(milliseconds),
        );
    }

    #[test]
    fn the_completed_line_past_the_inbox_keeps_its_send_time() {
        let smith_local_domain::AgentLimits::Spawned(limits) = crate::tests::config().limits.local.agent else {
            panic!("spawned fixture")
        };
        let names = limits.messages.saturating_add(1);
        for carried in [false, true] {
            let mut timing = timing();
            assert_eq!(timing.sent.capacity(), names);
            if carried {
                start(&mut timing, names, 900);
            } else {
                for name in 1..=names {
                    send(&mut timing, u64::from(name), 900);
                }
            }
            assert_eq!(timing.sent.len(), names, "every unread name retains its time");
            receive_turn(&mut timing, 1, u64::from(names), 1000);
            timing.told(1, Some(Token::new(u64::from(names))), clock(1000));
            assert_eq!(timing.elapsed, Duration::from_millis(100));
            assert!(timing.sent.is_empty(), "the last read name fences the entire inbox");
        }
    }

    #[test]
    fn a_full_timestamp_queue_uses_the_previous_turns_time() {
        for carried in [false, true] {
            let mut timing = timing();
            let names = timing.sent.capacity().checked_add(1).unwrap();
            if carried {
                start(&mut timing, names, 0);
            } else {
                for name in 1..=names {
                    send(&mut timing, u64::from(name), 900);
                }
            }
            assert_eq!(timing.sent.len(), timing.sent.capacity());
            receive_turn(&mut timing, 1, u64::from(names), 1000);
            timing.told(1, Some(Token::new(u64::from(names))), clock(1000));
            assert_eq!(timing.elapsed, Duration::from_millis(1000));
            assert!(timing.read.is_none(), "the dropped name has no timestamp");
        }
    }
}
