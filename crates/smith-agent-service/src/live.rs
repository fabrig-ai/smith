//! Best effort main-conversation text and tool input (protocol/channel.md 7; limits.md 3.6).
use skein_channel::Lower;
use skein_lib::{Duration, Map, Queue, Time, Token};
use smith_domain::llm;
use smith_protocol_channel::{self as channel, Piece};

/// Held payload bound, independent of provider delta size (limits.md 3.6).
const HELD: u32 = 16_384;
const PIECE: u32 = 4096;

pub(crate) trait Room {
    fn takes(&self, kind: channel::PieceKind<'_>, bytes: u32) -> bool;
    fn send(&mut self, piece: Piece<'_>);
    fn lose(&mut self, bytes: u64);
}

pub(crate) struct Connection<'a> {
    pub channel: &'a mut channel::Component,
    pub above: &'a mut Queue<channel::OpenEvent>,
    pub below: &'a mut Queue<Lower>,
    pub next: &'a mut u64,
    pub failed: bool,
    pub maximum: channel::MaxOut,
    pub ending: bool,
}

impl Room for Connection<'_> {
    fn takes(&self, kind: channel::PieceKind<'_>, bytes: u32) -> bool {
        let maximum = self.maximum;
        let entrances = if self.ending { 2 } else { 1 };
        self.above.room() >= maximum.to_domain.saturating_mul(entrances)
            && self.below.room() >= maximum.below.saturating_mul(entrances)
            && self.channel.takes_live(kind, bytes)
    }
    fn send(&mut self, piece: Piece<'_>) {
        let token = Token::new(*self.next);
        *self.next = self.next.checked_add(1).expect("one run does not exhaust send names");
        if self.channel.send_live(piece, token, self.above, self.below).is_err() {
            self.failed = true;
        }
    }
    fn lose(&mut self, bytes: u64) {
        self.channel.lose_live(bytes);
    }
}

pub(crate) enum Action<'a> {
    Open { owner: Token, retry: u32 },
    Delta { owner: Token, bytes: &'a [u8] },
    Call { owner: Token, index: u32, id: &'a [u8], tool: &'a [u8] },
    Arguments { owner: Token, index: u32, bytes: &'a [u8] },
    End { turn: u32 },
    Pump,
}

#[derive(Debug)]
struct Stream {
    owner: Token,
    turn: u32,
    retry: u32,
    ended: bool,
}

// At most one identity per held byte, plus the currently open empty call.
const IDENTITIES: u32 = HELD.checked_add(1).expect("held identities bound");

#[derive(Debug)]
struct Call {
    identity: smith_channel::LiveCall,
    held: u32,
}

#[derive(Debug)]
struct HeldByte {
    byte: u8,
    arrived: Time,
    call: Option<u32>,
    offset: u64,
}

#[derive(Debug)]
pub(crate) struct Live {
    ask: Option<smith_channel::LiveAsk>,
    last_turn: u32,
    stream: Option<Stream>,
    held: Queue<HeldByte>,
    interval: Duration,
    calls: Map<u32, Call>,
    current: Option<u32>,
    next: u64,
    input_next: u64,
}

pub(crate) fn wanted(ask: Option<&smith_channel::LiveAsk>, prompt: &llm::Prompt) -> bool {
    let selected = match ask {
        Some(ask) => ask.text() || ask.input(),
        None => false,
    };
    let ordinary = match prompt.purpose {
        llm::Purpose::Ordinary { .. } => true,
        llm::Purpose::Compaction { .. } => false,
    };
    selected && prompt.asker == llm::Asker::Main && ordinary
}

impl Live {
    pub(crate) fn new(ask: Option<smith_channel::LiveAsk>, live_interval: Duration) -> Self {
        Self {
            ask,
            last_turn: 0,
            stream: None,
            held: Queue::with_capacity(HELD),
            interval: live_interval,
            calls: Map::with_capacity(IDENTITIES),
            current: None,
            next: 0,
            input_next: 0,
        }
    }
    pub(crate) fn ask(&mut self, ask: Option<smith_channel::LiveAsk>) {
        self.ask = ask;
    }
    pub(crate) fn wants(&self, prompt: &llm::Prompt) -> bool {
        wanted(self.ask.as_ref(), prompt)
    }
    pub(crate) fn deltas(&self, prompt: &llm::Prompt, observed: bool) -> smith_protocol_llm::Deltas {
        let eligible = self.wants(prompt);
        let (text, input) = match &self.ask {
            Some(ask) => (ask.text(), ask.input()),
            None => (false, false),
        };
        smith_protocol_llm::Deltas { text: observed || (eligible && text), input: eligible && input }
    }
    pub(crate) fn act<R: Room>(&mut self, action: Action<'_>, now: Time, room: &mut R) {
        match action {
            Action::Open { owner, retry } => {
                self.open(owner, self.last_turn.checked_add(1).expect("bounded turns"), retry, room);
            }
            Action::Delta { owner, bytes } => self.delta(owner, bytes, now, room),
            Action::Call { owner, index, id, tool } => self.call(owner, index, id, tool),
            Action::Arguments { owner, index, bytes } => self.arguments(owner, index, bytes, now, room),
            Action::End { turn } => {
                self.end(room);
                self.last_turn = turn;
            }
            Action::Pump => self.pump(now, room),
        }
    }
    fn open<R: Room>(&mut self, owner: Token, turn: u32, retry: u32, room: &mut R) {
        self.end(room);
        self.stream = Some(Stream { owner, turn, retry, ended: false });
        self.next = 0;
        self.input_next = 0;
    }
    fn call(&mut self, owner: Token, index: u32, id: &[u8], tool: &[u8]) {
        match &self.stream {
            Some(stream) if stream.owner == owner && !stream.ended => {}
            Some(_) | None => return,
        }
        if let Some(previous) = self.current.take()
            && self.calls.get(&previous).expect("open call").held == 0
        {
            self.calls.remove(&previous);
        }
        self.input_next = 0;
        // Validate before allocating the bounded identity, including UTF-8.
        if id.len() > 256 || tool.len() > 64 {
            return;
        }
        let Ok(identity) = smith_channel::LiveCall::new(
            &smith_channel::CEILINGS,
            smith_channel::LiveCallParts { id: id.into(), tool: tool.into() },
        ) else {
            return;
        };
        self.calls.insert(index, Call { identity, held: 0 }).expect("one identity per held byte plus open call");
        self.current = Some(index);
    }
    fn arguments<R: Room>(&mut self, owner: Token, index: u32, bytes: &[u8], now: Time, room: &mut R) {
        match &self.stream {
            Some(stream) if stream.owner == owner => {}
            Some(_) | None => return,
        }
        if self.current != Some(index) {
            room.lose(u64::try_from(bytes.len()).expect("delta length"));
            return;
        }
        self.append(owner, Some(index), bytes, now, room);
    }
    fn delta<R: Room>(&mut self, owner: Token, bytes: &[u8], now: Time, room: &mut R) {
        match &self.ask {
            Some(ask) if ask.text() => self.append(owner, None, bytes, now, room),
            Some(_) | None => {}
        }
    }
    fn append<R: Room>(&mut self, owner: Token, call: Option<u32>, bytes: &[u8], now: Time, room: &mut R) {
        let Some(stream) = &self.stream else {
            return;
        };
        if stream.owner != owner {
            return;
        }
        let length = u64::try_from(bytes.len()).expect("provider delta length fits u64");
        if stream.ended {
            room.lose(length);
            return;
        }
        let next = match call {
            Some(_) => &mut self.input_next,
            None => &mut self.next,
        };
        let count = bytes.len().min(usize::try_from(self.held.room()).expect("held bound"));
        for (position, byte) in bytes.get(..count).expect("admitted prefix").iter().enumerate() {
            self.held.push(HeldByte {
                byte: *byte,
                arrived: now,
                call,
                offset: next.checked_add(u64::try_from(position).expect("held bound")).expect("stream bound"),
            });
        }
        if let Some(index) = call {
            let identity = self.calls.get_mut(&index).expect("open identity");
            identity.held = identity.held.checked_add(u32::try_from(count).expect("held bound")).expect("held bound");
        }
        *next = next.checked_add(length).expect("bounded stream bytes");
        room.lose(u64::try_from(bytes.len().saturating_sub(count)).expect("delta length"));
        self.pump(now, room);
    }
    pub(crate) fn deadline(&self) -> Option<Time> {
        Some(self.held.iter().next()?.arrived.saturating_add(self.interval))
    }
    fn pump<R: Room>(&mut self, now: Time, room: &mut R) {
        let Some(stream) = &self.stream else {
            return;
        };
        if stream.ended {
            return;
        }
        for _ in 0..HELD {
            let Some(first) = self.held.iter().next() else {
                break;
            };
            let offset = first.offset;
            let call = first.call;
            let due = now >= first.arrived.saturating_add(self.interval);
            let mut count: u32 = 0;
            for byte in &self.held {
                if count == PIECE
                    || byte.call != call
                    || byte.offset != offset.checked_add(u64::from(count)).expect("stream bound")
                {
                    break;
                }
                count = count.checked_add(1).expect("piece bound");
            }
            // A stream boundary flushes the preceding short piece without mixing streams.
            if count < PIECE && count == self.held.len() && !due {
                break;
            }
            let kind = match call {
                Some(index) => {
                    let identity = &self.calls.get(&index).expect("held identity").identity;
                    channel::PieceKind::Input { id: identity.id(), tool: identity.tool() }
                }
                None => channel::PieceKind::Text,
            };
            if !room.takes(kind, count) {
                break;
            }
            let mut bytes = [0; 4096];
            for byte in bytes.get_mut(..usize::try_from(count).expect("piece bound")).expect("piece bound") {
                *byte = self.held.pop().expect("held byte").byte;
            }
            room.send(Piece {
                kind,
                turn: stream.turn,
                retry: stream.retry,
                offset,
                bytes: bytes.get(..usize::try_from(count).expect("piece bound")).expect("piece bound"),
            });
            if let Some(index) = call {
                let identity = self.calls.get_mut(&index).expect("held identity");
                identity.held = identity.held.checked_sub(count).expect("held count");
                if identity.held == 0 && self.current != Some(index) {
                    self.calls.remove(&index);
                }
            }
        }
    }
    fn end<R: Room>(&mut self, room: &mut R) {
        self.pump(Time::from_nanos(u64::MAX), room);
        room.lose(u64::from(self.held.len()));
        for _ in 0..HELD {
            if self.held.pop().is_none() {
                break;
            }
        }
        for _ in 0..IDENTITIES {
            let index = match self.calls.iter().next() {
                Some((index, _)) => *index,
                None => break,
            };
            self.calls.remove(&index);
        }
        self.current = None;
        if let Some(stream) = &mut self.stream {
            stream.ended = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Live, Room as ChannelRoom, wanted};
    use crate::profile::standard;
    use alloc::boxed::Box;
    use skein_lib::{List, Time, Token};
    use smith_domain::llm;
    use smith_protocol_channel::Piece;

    const AT_ZERO: Time = Time::ZERO;
    const PIECE_BOUND: usize = 4096;
    const OWNER: Token = Token::new(7);
    const RETRY_OWNER: Token = Token::new(8);
    // limits.md 3.6: 16 KiB held, cut into four 4096-byte pieces.
    const HELD: usize = 16_384;
    const LARGE_DELTA: usize = 10_000;
    static AT_BOUND: [u8; HELD] = [b'a'; HELD];
    static PAST_BOUND: [u8; HELD + 1] = [b'a'; HELD + 1];
    #[derive(Debug)]
    struct Sent {
        turn: u32,
        retry: u32,
        offset: u64,
        bytes: Box<[u8]>,
        call: Option<smith_channel::LiveCall>,
    }
    struct Room {
        takes: bool,
        remaining: u32,
        lost: u64,
        sent: List<Sent>,
    }
    impl Room {
        fn new(takes: bool) -> Self {
            Self { takes, remaining: u32::MAX, lost: 0, sent: List::with_capacity(super::HELD) }
        }
        fn payloads(&self) -> List<&[u8]> {
            let mut pieces = List::with_capacity(self.sent.len());
            for piece in &self.sent {
                pieces.push(piece.bytes.as_ref()).expect("sent piece count");
            }
            pieces
        }
        fn offsets(&self) -> List<(u64, usize)> {
            let mut offsets = List::with_capacity(self.sent.len());
            for piece in &self.sent {
                offsets.push((piece.offset, piece.bytes.len())).expect("sent piece count");
            }
            offsets
        }
    }
    impl ChannelRoom for Room {
        fn takes(&self, _: smith_protocol_channel::PieceKind<'_>, _: u32) -> bool {
            self.takes && self.remaining > 0
        }
        fn send(&mut self, piece: Piece<'_>) {
            self.sent
                .push(Sent {
                    turn: piece.turn,
                    retry: piece.retry,
                    offset: piece.offset,
                    bytes: Box::from(piece.bytes),
                    call: match piece.kind {
                        smith_protocol_channel::PieceKind::Text => None,
                        smith_protocol_channel::PieceKind::Input { id, tool } => Some(
                            smith_channel::LiveCall::new(
                                &smith_channel::CEILINGS,
                                smith_channel::LiveCallParts { id: id.into(), tool: tool.into() },
                            )
                            .expect("valid input identity"),
                        ),
                    },
                })
                .expect("piece bound");
            self.remaining = self.remaining.saturating_sub(1);
        }
        fn lose(&mut self, bytes: u64) {
            self.lost = self.lost.saturating_add(bytes);
        }
    }
    fn ask(text: bool, reasoning: bool, input: bool) -> smith_channel::LiveAsk {
        smith_channel::LiveAsk::new(&smith_channel::CEILINGS, smith_channel::LiveAskParts { text, reasoning, input })
            .expect("ask")
    }
    fn stream(takes: bool) -> (Live, Room) {
        let mut live = Live::new(Some(ask(true, false, false)), standard().policy.live_interval);
        let mut room = Room::new(takes);
        live.open(OWNER, 1, 0, &mut room);
        (live, room)
    }
    #[test]
    fn a_stream_past_the_held_bytes_drops_the_newest_and_counts_them() {
        let (mut live, mut room) = stream(false);
        live.delta(OWNER, &[b'a'; LARGE_DELTA], AT_ZERO, &mut room);
        live.delta(OWNER, &[b'b'; LARGE_DELTA], AT_ZERO, &mut room);
        assert_eq!(room.lost, 3616, "20,000 provider bytes minus the held bound");
        room.takes = true;
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        assert_eq!(room.offsets().as_slice(), [(0, 4096), (4096, 4096), (8192, 4096), (12_288, 4096)]);
        live.delta(OWNER, b"c", AT_ZERO, &mut room);
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        assert_eq!(room.sent.last().expect("later piece").offset, 20_000, "the omitted suffix leaves a gap");
    }
    #[test]
    fn a_delta_past_the_piece_bound_goes_in_several_pieces() {
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, &[b'a'; LARGE_DELTA], AT_ZERO, &mut room);
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        assert_eq!(room.offsets().as_slice(), [(0, 4096), (4096, 4096), (8192, 1808)]);
        let mut joined = List::with_capacity(u32::try_from(LARGE_DELTA).expect("fixture bound"));
        for piece in &room.sent {
            for byte in &piece.bytes {
                joined.push(*byte).expect("fixture payload");
            }
        }
        assert_eq!(joined.as_slice(), &[b'a'; LARGE_DELTA]);
    }
    #[test]
    fn ending_a_turn_settles_the_held_bytes_first() {
        for (takes, pieces, lost) in [(true, 1, 0), (false, 0, 4)] {
            let (mut live, mut room) = stream(false);
            live.delta(OWNER, b"held", AT_ZERO, &mut room);
            room.takes = takes;
            live.end(&mut room);
            assert_eq!((room.sent.len(), room.lost), (pieces, lost), "room={takes}");
            room.takes = true;
            live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
            assert_eq!(room.sent.len(), pieces, "no piece follows the ended turn");
        }
    }
    #[test]
    fn a_delta_after_its_turn_is_dropped_and_counted() {
        let (mut live, mut room) = stream(true);
        live.end(&mut room);
        live.delta(OWNER, b"late", AT_ZERO, &mut room);
        assert_eq!((room.sent.len(), room.lost), (0, 4));
    }
    #[test]
    fn a_new_retry_starts_again_at_offset_zero() {
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, b"first", AT_ZERO, &mut room);
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        room.takes = false;
        live.delta(OWNER, b"held", AT_ZERO, &mut room);
        live.open(RETRY_OWNER, 1, 1, &mut room);
        room.takes = true;
        live.delta(RETRY_OWNER, b"again", AT_ZERO, &mut room);
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        let mut identities = List::with_capacity(room.sent.len());
        for piece in &room.sent {
            identities.push((piece.turn, piece.retry, piece.offset)).expect("sent identities");
        }
        assert_eq!(identities.as_slice(), [(1, 0, 0), (1, 1, 0)]);
        assert_eq!(room.lost, 4);
    }
    #[test]
    fn only_the_main_ordinary_completion_is_wanted() {
        let mut prompt = crate::responses::tests::prompt(0);
        for (text, reasoning, input, main_ordinary) in [
            (false, false, false, false),
            (true, false, false, true),
            (false, true, false, false),
            (false, false, true, true),
            (true, true, true, true),
        ] {
            let ask = ask(text, reasoning, input);
            for (asker, purpose, eligible) in [
                (llm::Asker::Main, llm::Purpose::Ordinary { guard: None }, true),
                (llm::Asker::Child, llm::Purpose::Ordinary { guard: None }, false),
                (llm::Asker::Main, llm::Purpose::Compaction { room: 1 }, false),
                (llm::Asker::Child, llm::Purpose::Compaction { room: 1 }, false),
            ] {
                prompt.asker = asker;
                prompt.purpose = purpose;
                assert_eq!(
                    wanted(Some(&ask), &prompt),
                    main_ordinary && eligible,
                    "{asker:?}, {purpose:?}, text={text}, reasoning={reasoning}, input={input}"
                );
                assert!(!wanted(None, &prompt), "an absent ask wants no completion");
            }
        }
    }
    #[test]
    fn the_held_bound_keeps_the_prefix_and_drops_only_the_excess() {
        for (input, lost) in [(AT_BOUND.as_slice(), 0), (PAST_BOUND.as_slice(), 1)] {
            let (mut live, mut room) = stream(false);
            live.delta(OWNER, input, AT_ZERO, &mut room);
            assert_eq!(room.lost, lost);
            room.takes = true;
            live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
            let mut joined = List::with_capacity(super::HELD);
            for piece in &room.sent {
                for byte in &piece.bytes {
                    joined.push(*byte).expect("held prefix");
                }
            }
            assert_eq!(joined.as_slice(), &[b'a'; HELD]);
        }
    }
    #[test]
    fn a_gap_survives_partial_draining_and_later_held_bytes() {
        let (mut live, mut room) = stream(false);
        live.delta(OWNER, &[b'a'; LARGE_DELTA], AT_ZERO, &mut room);
        live.delta(OWNER, &[b'b'; LARGE_DELTA], AT_ZERO, &mut room);
        room.takes = true;
        room.remaining = 1;
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        live.delta(OWNER, b"c", AT_ZERO, &mut room);
        room.remaining = u32::MAX;
        live.pump(AT_ZERO.saturating_add(standard().policy.live_interval), &mut room);
        assert_eq!(room.offsets().as_slice(), [(0, 4096), (4096, 4096), (8192, 4096), (12_288, 4096), (20_000, 1)]);
        assert_eq!(room.lost, 3616);
    }
    #[test]
    fn a_short_piece_goes_at_the_intervals_end() {
        let interval = standard().policy.live_interval;
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, b"Hel", AT_ZERO, &mut room);
        let end = AT_ZERO.saturating_add(interval);
        live.pump(Time::from_nanos(end.as_nanos() - 1), &mut room);
        assert_eq!(room.sent.len(), 0, "held before the interval ends");
        assert_eq!(live.deadline(), Some(end));
        live.pump(end, &mut room);
        assert_eq!(room.payloads().as_slice(), [b"Hel"]);
        assert_eq!(live.deadline(), None);
    }

    #[test]
    fn a_piece_reaching_the_bound_goes_before_the_interval() {
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, &[b'a'; PIECE_BOUND], AT_ZERO, &mut room);
        assert_eq!(room.payloads().as_slice(), [[b'a'; PIECE_BOUND].as_slice()]);
        assert_eq!(live.deadline(), None);
    }

    #[test]
    fn draining_a_piece_retains_the_next_bytes_arrival_time() {
        let interval = standard().policy.live_interval;
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, &[b'a'; PIECE_BOUND - 1], AT_ZERO, &mut room);
        let end = AT_ZERO.saturating_add(interval);
        let later = Time::from_nanos(end.as_nanos() - 1);
        live.delta(OWNER, b"bc", later, &mut room);
        assert_eq!(live.deadline(), Some(later.saturating_add(interval)));
    }
    #[test]
    fn unnamed_refused_and_earlier_calls_arguments_are_counted() {
        let (mut live, mut room) = stream(true);
        live.arguments(OWNER, 0, b"unnamed", AT_ZERO, &mut room);
        live.call(OWNER, 0, &[b'x'; 257], b"tool");
        live.arguments(OWNER, 0, b"refused", AT_ZERO, &mut room);
        live.call(OWNER, 1, b"first", b"tool");
        live.call(OWNER, 2, b"second", b"tool");
        live.arguments(OWNER, 1, b"earlier", AT_ZERO, &mut room);
        assert_eq!(room.lost, 21);
        assert!(room.sent.is_empty());
    }

    #[test]
    fn interleaved_text_and_input_keep_separate_offsets_and_identities() {
        let (mut live, mut room) = stream(true);
        live.delta(OWNER, b"ab", AT_ZERO, &mut room);
        live.call(OWNER, 0, b"first", b"tool");
        live.arguments(OWNER, 0, b"12", AT_ZERO, &mut room);
        live.delta(OWNER, b"cd", AT_ZERO, &mut room);
        live.arguments(OWNER, 0, b"34", AT_ZERO, &mut room);
        live.call(OWNER, 1, b"second", b"other");
        live.arguments(OWNER, 1, b"56", AT_ZERO, &mut room);
        live.end(&mut room);
        assert_eq!(room.offsets().as_slice(), [(0, 2), (0, 2), (2, 2), (2, 2), (0, 2)]);
        let mut calls = List::with_capacity(5);
        for piece in &room.sent {
            match &piece.call {
                Some(call) => calls.push(Some(call.id())).expect("five pieces"),
                None => calls.push(None).expect("five pieces"),
            }
        }
        assert_eq!(
            calls.as_slice(),
            [None, Some(b"first".as_slice()), None, Some(b"first".as_slice()), Some(b"second".as_slice())]
        );
        assert_eq!(room.lost, 0);
    }

    #[test]
    fn text_and_calls_share_the_held_bound() {
        let (mut live, mut room) = stream(false);
        live.delta(OWNER, &[b't'; LARGE_DELTA], AT_ZERO, &mut room);
        live.call(OWNER, 0, b"call", b"tool");
        live.arguments(OWNER, 0, &[b'i'; LARGE_DELTA], AT_ZERO, &mut room);
        assert_eq!(room.lost, 3616);
        room.takes = true;
        live.end(&mut room);
        assert_eq!(room.lost, 3616);
        let mut bytes: usize = 0;
        for piece in &room.sent {
            bytes = bytes.checked_add(piece.bytes.len()).expect("held bound");
        }
        assert_eq!(bytes, HELD);
    }
}
