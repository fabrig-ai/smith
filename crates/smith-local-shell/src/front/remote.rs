//! Spawned main's text and told-turn recovery (shell.md, section 4.1;
//! protocol/channel.md, section 4). The front retains only a capped byte
//! prefix for one retry, never a transcript or provider timing. `piece`
//! joins bytes before decoding; `told` finishes the prefix and its status.
use super::interactive::{Remaining, StatusLine};
use skein_lib::{Duration, List};
use smith_domain::{
    run::Budget,
    session::llm::{Block, Returned, Role},
};
use smith_events::{Backend, Model, Quantity, Usage};
use smith_local_service::{LivePiece, RemoteTurn};

/// The shipped front's told-text cap, also used before local configuration arrives.
pub(super) const TEXT_BYTES: u32 = 8192;

pub(super) struct Remote {
    activation: Option<u64>,
    turn: u32,
    retry: Option<u32>,
    prefix: List<u8>,
    shown: usize,
    gap: bool,
    told: u32,
    spent: u64,
}

impl Remote {
    pub(super) fn new(cap: u32) -> Self {
        Self {
            activation: None,
            turn: 0,
            retry: None,
            prefix: List::with_capacity(cap),
            shown: 0,
            gap: false,
            told: 0,
            spent: 0,
        }
    }

    pub(super) fn activation(&mut self, activation: Option<u64>) {
        if self.activation != activation {
            self.activation = activation;
            self.turn = 0;
            self.told = 0;
            self.spent = 0;
            self.reset();
        }
    }

    fn reset(&mut self) {
        self.retry = None;
        self.prefix.clear();
        self.shown = 0;
        self.gap = false;
    }

    pub(super) fn piece(&mut self, piece: LivePiece) -> Option<Box<[u8]>> {
        if piece.turn <= self.told || piece.turn < self.turn {
            return None;
        }
        let mut newline = false;
        if piece.turn != self.turn {
            self.turn = piece.turn;
            self.reset();
        }
        if self.retry.is_some_and(|retry| piece.retry < retry) {
            return None;
        }
        if self.retry != Some(piece.retry) {
            newline = self.shown > 0;
            self.reset();
            self.retry = Some(piece.retry);
        }
        if self.gap {
            return None;
        }
        if piece.offset != u64::from(self.prefix.len()) {
            self.gap = true;
            return newline.then(|| b"\n".as_slice().into());
        }
        for byte in piece.bytes.into_vec().into_iter().take(usize::try_from(self.prefix.room()).expect("front bound")) {
            self.prefix.push(byte).expect("checked prefix capacity");
        }
        let tail = self.prefix.as_slice().get(self.shown..).expect("shown prefix is retained");
        let valid = match std::str::from_utf8(tail) {
            Ok(_) => tail.len(),
            Err(why) => {
                if why.error_len().is_some() {
                    self.gap = true;
                }
                why.valid_up_to()
            }
        };
        let mut output = Vec::with_capacity(valid.saturating_add(usize::from(newline)));
        if newline {
            output.push(b'\n');
        }
        output.extend_from_slice(tail.get(..valid).expect("decoded prefix"));
        self.shown = self.shown.checked_add(valid).expect("bounded shown prefix");
        (!output.is_empty()).then(|| output.into_boxed_slice())
    }

    pub(super) fn told(&mut self, told: &RemoteTurn) -> Box<[u8]> {
        let cap = self.prefix.capacity();
        let mut text = List::with_capacity(cap);
        for message in &told.turn.messages {
            if message.role == Role::Assistant {
                for block in &message.content {
                    if let Block::Text { text: bytes, .. } | Block::Refusal { text: bytes, .. } = block {
                        append(&mut text, bytes);
                    }
                }
            }
        }
        let shown = if self.turn == told.number {
            self.prefix.as_slice().get(..self.shown).expect("shown prefix is retained")
        } else {
            &[]
        };
        let mut output = List::with_capacity(cap.saturating_add(1));
        if text.as_slice().starts_with(shown) {
            append(&mut output, text.as_slice().get(shown.len()..).expect("matched prefix"));
        } else {
            append(&mut output, b"\n");
            append(&mut output, text.as_slice());
        }
        // Text and tool summaries share the existing per-turn cap.
        for message in &told.turn.messages {
            if message.role != Role::Assistant {
                continue;
            }
            for block in &message.content {
                if let Block::ToolCall { id, name, .. } = block {
                    let result =
                        told.turn.messages.iter().flat_map(|message| message.content.iter()).find_map(|block| {
                            match block {
                                Block::ToolResult { id: result_id, result } if result_id == id => Some(result),
                                Block::ToolResult { .. }
                                | Block::Text { .. }
                                | Block::Refusal { .. }
                                | Block::ToolCall { .. }
                                | Block::Opaque { .. } => None,
                            }
                        });
                    let verdict = result.map_or("not run", returned_verdict);
                    let line = format!("\ntool {}: {verdict}\n", String::from_utf8_lossy(smith_events::clip(name)));
                    let remaining = cap.saturating_sub(text.len());
                    let bytes = line.as_bytes();
                    let end = usize::try_from(remaining).expect("bounded tool line").min(bytes.len());
                    let end = floor_boundary(&line, end);
                    let bytes = bytes.get(..end).expect("bounded tool line");
                    append(&mut output, bytes);
                    for byte in bytes {
                        text.push(*byte).expect("shared display cap");
                    }
                }
            }
        }
        self.told = told.number;
        self.reset();
        output.into_boxed()
    }

    pub(super) fn status(&mut self, told: &RemoteTurn, endpoint: &[u8], budget: Budget) -> StatusLine {
        let turn = &told.turn;
        let spent = turn.spent.saturating_sub(self.spent);
        self.spent = turn.spent;
        StatusLine {
            model: Model {
                endpoint: smith_events::clip(endpoint).into(),
                model: smith_events::clip(&turn.model).into(),
                window_tokens: 0,
                output_tokens: 0,
                effort: None,
                account: None,
                backend: Backend::ClaudeCode,
            },
            usage: Usage {
                input_tokens: turn.usage.input_tokens,
                cache_read_tokens: turn.usage.cache_read_tokens,
                cache_write_tokens: turn.usage.cache_write_tokens,
                output_tokens: turn.usage.output_tokens,
                reasoning_tokens: turn.usage.reasoning_tokens,
            },
            spent,
            elapsed: told.elapsed,
            remaining: Some(Remaining {
                turns: Quantity { used: u64::from(told.number), limit: u64::from(budget.turns) },
                spend: Quantity { used: turn.spent, limit: budget.spend },
                time_ms: Quantity { used: millis(told.since_start), limit: millis(budget.time) },
            }),
        }
    }

    pub(super) fn worst_case(cap: u32) -> Option<u64> {
        // Prefix, collected told text, output, a copied status and popped piece.
        u64::from(cap).checked_mul(3)?.checked_add(8192)?.checked_add(u64::try_from(size_of::<Self>()).ok()?)
    }
}

fn millis(duration: Duration) -> u64 {
    duration.as_nanos() / 1_000_000
}

fn append(output: &mut List<u8>, bytes: &[u8]) {
    let text = std::str::from_utf8(bytes).expect("attested turn text");
    let end = floor_boundary(text, usize::try_from(output.room()).expect("front bound").min(bytes.len()));
    for byte in bytes.get(..end).expect("bounded UTF-8 prefix") {
        output.push(*byte).expect("checked front room");
    }
}

fn floor_boundary(text: &str, mut end: usize) -> usize {
    while !text.is_char_boundary(end) {
        end = end.checked_sub(1).expect("zero boundary");
    }
    end
}

fn returned_verdict(result: &Returned) -> &'static str {
    use smith_domain::tools::{Exit, JobEnd, Mismatch, Outcome};
    match result {
        Returned::Text { error: false, .. } => "result",
        Returned::Text { error: true, .. } => "error",
        Returned::Invalid { .. } => "invalid",
        Returned::NotRun { .. } => "not run",
        Returned::Withdrawn => "withdrawn",
        Returned::Owned { outcome } => match outcome {
            Outcome::Read { .. } => "read",
            Outcome::Listed { .. } => "listed",
            Outcome::Found { .. } => "found",
            Outcome::Removed => "removed",
            Outcome::NotEmpty => "directory is not empty",
            Outcome::Written { .. } => "written",
            Outcome::Edited { .. } => "edited",
            Outcome::Exited { exit: Exit::TimedOut, .. } | Outcome::TimedOut => "timed out",
            Outcome::Exited { .. } => "exited",
            Outcome::Running { .. } => "running",
            Outcome::Job { end, .. } => match end {
                JobEnd::Running => "running",
                JobEnd::Ended(Exit::TimedOut) => "timed out",
                JobEnd::Ended(Exit::Code { .. } | Exit::Signal { .. }) => "exited",
                JobEnd::Stopped => "stopped",
                JobEnd::Cancelled => "cancelled",
            },
            Outcome::NotGranted => "not granted",
            Outcome::Outside { .. } => "outside",
            Outcome::ReadOnly { .. } => "read only",
            Outcome::TooLong => "too long",
            Outcome::NotFound { .. } => "not found",
            Outcome::NotFile => "not a file",
            Outcome::Linked => "linked",
            Outcome::Protected => "protected",
            Outcome::NotDirectory { .. } => "not a directory",
            Outcome::TooLarge { .. } => "too large",
            Outcome::NotRead => "not read",
            Outcome::Stale => "stale",
            Outcome::EditFailed { why, .. } => match why {
                Mismatch::NoMatch => "no match",
                Mismatch::Ambiguous { .. } => "ambiguous",
                Mismatch::Unchanged => "unchanged",
            },
            Outcome::Failed { .. } => "failed",
            Outcome::Cancelled => "cancelled",
            Outcome::Busy => "busy",
            Outcome::NulByte => "NUL byte",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smith_domain::session::{
        llm::{Message, Usage as TurnUsage},
        record::Kind,
    };

    fn piece(turn: u32, retry: u32, offset: u64, bytes: &[u8]) -> LivePiece {
        LivePiece { turn, retry, offset, bytes: bytes.into() }
    }

    fn told(number: u32, text: &[u8]) -> RemoteTurn {
        RemoteTurn {
            number,
            elapsed: Duration::from_millis(12),
            since_start: Duration::from_millis(30),
            turn: smith_domain::Turn {
                version: 1,
                kind: Kind::Ordinary,
                endpoint: None,
                model: b"small".as_slice().into(),
                effort: None,
                dialect: 1,
                sequence: number,
                usage: TurnUsage {
                    input_tokens: Some(10),
                    output_tokens: Some(3),
                    cache_read_tokens: Some(4),
                    ..TurnUsage::NONE
                },
                spent: 17,
                jobs: 0,
                messages: Box::new([Message {
                    role: Role::Assistant,
                    content: Box::new([Block::Text { text: text.into(), replay: None }]),
                }]),
            },
        }
    }

    #[test]
    fn a_split_character_waits_for_its_last_byte() {
        let mut front = Remote::new(32);
        assert!(front.piece(piece(1, 0, 0, &[0xe2, 0x82])).is_none());
        assert_eq!(front.piece(piece(1, 0, 2, &[0xac])).unwrap().as_ref(), "€".as_bytes());
        assert!(front.told(&told(1, "€".as_bytes())).is_empty());
    }

    #[test]
    fn a_gap_stops_the_retry_until_its_told_remainder() {
        let mut front = Remote::new(32);
        assert_eq!(front.piece(piece(1, 0, 0, b"a")).unwrap().as_ref(), b"a");
        assert!(front.piece(piece(1, 0, 2, b"c")).is_none());
        assert!(front.piece(piece(1, 0, 3, b"d")).is_none());
        assert_eq!(front.told(&told(1, b"abcd")).as_ref(), b"bcd");
    }

    #[test]
    fn a_retry_begins_a_new_line_without_erasing_text() {
        let mut front = Remote::new(32);
        assert_eq!(front.piece(piece(1, 0, 0, b"first")).unwrap().as_ref(), b"first");
        assert_eq!(front.piece(piece(1, 1, 0, b"sec")).unwrap().as_ref(), b"\nsec");
        assert_eq!(front.told(&told(1, b"second")).as_ref(), b"ond");
    }

    #[test]
    fn a_piece_after_the_told_turn_is_ignored() {
        let mut front = Remote::new(32);
        assert_eq!(front.told(&told(1, b"done")).as_ref(), b"done");
        assert!(front.piece(piece(1, 0, 0, b"done")).is_none());
    }

    #[test]
    fn a_different_told_text_starts_on_a_new_line() {
        let mut front = Remote::new(32);
        drop(front.piece(piece(1, 0, 0, b"attempt")));
        assert_eq!(front.told(&told(1, b"settled")).as_ref(), b"\nsettled");
    }

    #[test]
    fn a_told_turn_without_pieces_shows_all_its_text() {
        let mut front = Remote::new(32);
        assert_eq!(front.told(&told(1, b"settled")).as_ref(), b"settled");
    }

    #[test]
    fn a_status_uses_told_fields_with_local_time() {
        let mut front = Remote::new(32);
        let budget = Budget { turns: 5, spend: 100, time: Duration::from_millis(200) };
        let first = told(1, b"text");
        let status = front.status(&first, b"endpoint", budget);
        assert_eq!(status.with_account("through Claude Code").as_ref(), b"\nendpoint/small; through Claude Code: in 10, cached 4, out 3; 17 notional; 12 ms; 4 turns, 83 notional, 170 ms left\n");
        let mut second = told(2, b"text");
        second.turn.spent = 22;
        assert_eq!(front.status(&second, b"endpoint", budget).spent, 5);
    }

    #[test]
    fn an_activation_forgets_its_prefix() {
        let mut front = Remote::new(32);
        front.activation(Some(1));
        drop(front.piece(piece(7, 0, 0, b"first")));
        // The next chat also saved activation 1; the service sends generation 2.
        front.activation(Some(2));
        assert!(front.prefix.is_empty(), "activation releases the uncommitted prefix");
        assert_eq!(front.piece(piece(1, 0, 0, b"next")).unwrap().as_ref(), b"next");
        // A third chat repeats the saved activation and turn numbers again.
        front.activation(Some(3));
        assert!(front.prefix.is_empty(), "every generation releases its prefix");
        assert_eq!(front.piece(piece(1, 0, 0, b"fresh")).unwrap().as_ref(), b"fresh");
    }

    #[test]
    fn a_told_text_cap_ends_on_a_character_boundary() {
        let mut front = Remote::new(2);
        assert_eq!(front.told(&told(1, "a€".as_bytes())).as_ref(), b"a");
    }
}
