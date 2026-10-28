//! The line-mode live view (shell.md, section 4.1). Records come typed from
//! the one sink's best-effort queue. Outcomes still come from the local host.
use skein_lib::{Duration, Map, Token};
use smith_agent_service::sinks::View;
use smith_events::{Model, Quantity, Usage, Verdict};

/// The three reported budget quantities retained for the next response status.
#[derive(Clone)]
pub struct Remaining {
    pub turns: Quantity,
    pub spend: Quantity,
    pub time_ms: Quantity,
}

/// The measured response and its current budget, sent by the screen to the person.
pub struct StatusLine {
    pub model: Model,
    pub usage: Usage,
    pub spent: u64,
    pub elapsed: Duration,
    pub remaining: Option<Remaining>,
}

impl StatusLine {
    /// Render reported counts without turning an unknown count into zero.
    #[must_use]
    pub fn text(&self) -> Box<[u8]> {
        self.with_account("")
    }

    pub(super) fn with_account(&self, account: &str) -> Box<[u8]> {
        let remaining = self.remaining.as_ref().map_or_else(
            || "budget unavailable".into(),
            |budget| {
                format!(
                    "{} turns, {} notional, {} ms left",
                    budget.turns.limit.saturating_sub(budget.turns.used),
                    budget.spend.limit.saturating_sub(budget.spend.used),
                    budget.time_ms.limit.saturating_sub(budget.time_ms.used)
                )
            },
        );
        format!(
            "\n{}/{}{}: in {}, cached {}, out {}; {} notional; {} ms; {}\n",
            String::from_utf8_lossy(smith_events::clip(&self.model.endpoint)),
            String::from_utf8_lossy(smith_events::clip(&self.model.model)),
            if account.is_empty() { String::new() } else { format!("; {account}") },
            count(self.usage.input_tokens),
            count(self.usage.cache_read_tokens),
            count(self.usage.output_tokens),
            self.spent,
            self.elapsed.as_nanos() / 1_000_000,
            remaining
        )
        .into_bytes()
        .into()
    }
}

fn count(value: Option<u64>) -> String {
    value.map_or_else(|| "unknown".into(), |value| value.to_string())
}

/// The presentation selected at startup, with a terminal's current width.
pub enum Screen {
    /// Told text without the live response status.
    Plain,
    /// A terminal status fits the most recently observed window.
    Terminal { columns: u16 },
}

/// One bounded model join and the last budget snapshot, independent of any trace.
struct Live {
    screen: Screen,
    models: Map<Token, Model>,
    remaining: Option<Remaining>,
    account: &'static str,
    accounts: Map<u32, &'static str>,
}

impl Live {
    /// Reserve the live conversations before the product's startup admission.
    #[must_use]
    pub fn new(plain: bool, conversations: u32) -> Self {
        let screen = if plain { Screen::Plain } else { Screen::Terminal { columns: u16::MAX } };
        Self {
            screen,
            models: Map::with_capacity(conversations),
            remaining: None,
            account: "",
            accounts: Map::with_capacity(0),
        }
    }

    fn plain(&self) -> bool {
        matches!(self.screen, Screen::Plain)
    }

    /// Price the model names and one retained budget snapshot beside the typed queue.
    #[must_use]
    pub fn worst_case(conversations: u32, name_bytes: u32) -> Option<u64> {
        Map::<Token, Model>::worst_case(conversations)?
            .checked_add(u64::from(conversations).checked_mul(u64::from(name_bytes).checked_mul(3)?)?)?
            // The budget's labels, one copied model, and the output's names coexist.
            .checked_add(u64::from(name_bytes).checked_mul(8)?)?
            .checked_add(4096)?
            .checked_add(u64::try_from(size_of::<Self>()).ok()?)
    }

    /// Move streamed text into the terminal; status joins tolerate a dropped opening.
    pub fn record(&mut self, record: View) -> Option<Box<[u8]>> {
        match record {
            View::TextDelta { delta, .. } if !self.plain() => Some(delta.text),
            View::RunStarted => {
                for _ in 0..self.models.capacity() {
                    let Some((name, _)) = self.models.iter().next() else { break };
                    let name = *name;
                    self.models.remove(&name);
                }
                self.remaining = None;
                None
            }
            View::Model { conversation, model } => {
                drop(self.models.insert(Token::new(conversation), model));
                None
            }
            View::ConversationClosed { conversation } => {
                self.models.remove(&Token::new(conversation));
                None
            }
            View::ResponseCompleted { conversation, usage, spent, duration_ms, .. } if !self.plain() => {
                let model = self.models.get(&Token::new(conversation))?.clone();
                let account = model.account.and_then(|number| self.accounts.get(&number)).copied().unwrap_or("");
                Some(
                    self.status(
                        StatusLine {
                            model,
                            usage,
                            spent,
                            elapsed: Duration::from_nanos(duration_ms.saturating_mul(1_000_000)),
                            remaining: self.remaining.clone(),
                        }
                        .with_account(account),
                    ),
                )
            }
            View::Budget { turns, spend, time_ms } => {
                self.remaining = Some(Remaining { turns, spend, time_ms });
                None
            }
            View::ToolStarted { tool } => {
                Some(format!("\ntool {}\n", String::from_utf8_lossy(smith_events::clip(&tool))).into_bytes().into())
            }
            View::ToolCompleted { tool, verdict: terminal, duration_ms } if !self.plain() => Some(
                format!(
                    "tool {}: {} ({} ms)\n",
                    String::from_utf8_lossy(smith_events::clip(&tool)),
                    verdict(&terminal),
                    duration_ms
                )
                .into_bytes()
                .into(),
            ),
            View::Notice(notice) => notice_text(notice),
            View::Failure(_) | View::ResponseCompleted { .. } | View::ToolCompleted { .. } | View::TextDelta { .. } => {
                None
            }
        }
    }

    fn status(&self, text: Box<[u8]>) -> Box<[u8]> {
        match self.screen {
            Screen::Plain => text,
            Screen::Terminal { columns } => {
                let line = std::str::from_utf8(&text).expect("status is UTF-8").trim_matches('\n');
                let mut end = line.len().min(usize::from(columns));
                while !line.is_char_boundary(end) {
                    end = end.checked_sub(1).expect("zero is a character boundary");
                }
                format!("\n{}\n", &line[..end]).into_bytes().into()
            }
        }
    }
}

fn notice_text(notice: smith_events::Notice) -> Option<Box<[u8]>> {
    match notice.kind {
        smith_events::NoticeKind::ReasoningDropped => {
            Some(format!("reasoning item omitted ({} bytes)\n", count(notice.bytes)).into_bytes().into())
        }
        smith_events::NoticeKind::Unknown(kind) => {
            let text = if kind.as_ref() == b"not_contained" {
                "smith: commands are not contained\n".to_string()
            } else {
                format!("notice: {}\n", String::from_utf8_lossy(smith_events::clip(&kind)))
            };
            Some(text.into_bytes().into())
        }
        smith_events::NoticeKind::CredentialUnauthorized
        | smith_events::NoticeKind::AccountLimited
        | smith_events::NoticeKind::AccountRefused
        | smith_events::NoticeKind::ForcedExit
        | smith_events::NoticeKind::TeardownAborted => None,
    }
}

fn verdict(value: &Verdict) -> &str {
    match value {
        Verdict::Read => "read",
        Verdict::Listed => "listed",
        Verdict::Found => "found",
        Verdict::Removed => "removed",
        Verdict::NotEmpty => "directory is not empty",
        Verdict::Written => "written",
        Verdict::Edited => "edited",
        Verdict::Exited => "exited",
        Verdict::Conflict => "conflict",
        Verdict::Missing => "missing",
        Verdict::TooLarge => "too large",
        Verdict::TimedOut => "timed out",
        Verdict::Running => "running",
        Verdict::Stopped => "stopped",
        Verdict::Failed => "failed",
        Verdict::Cancelled => "cancelled",
        Verdict::Ambiguous => "ambiguous",
        Verdict::Result => "result",
        Verdict::Error => "error",
        Verdict::Invalid => "invalid",
        Verdict::NotRun => "not run",
        Verdict::Withdrawn => "withdrawn",
        Verdict::NotGranted => "not granted",
        Verdict::Outside => "outside",
        Verdict::ReadOnly => "read only",
        Verdict::TooLong => "too long",
        Verdict::NotFound => "not found",
        Verdict::NotFile => "not a file",
        Verdict::Linked => "linked",
        Verdict::Protected => "protected",
        Verdict::NotDirectory => "not a directory",
        Verdict::NotRead => "not read",
        Verdict::Stale => "stale",
        Verdict::NoMatch => "no match",
        Verdict::Unchanged => "unchanged",
        Verdict::Busy => "busy",
        Verdict::NulByte => "NUL byte",
        Verdict::Unknown(text) => std::str::from_utf8(smith_events::clip(text)).unwrap_or("unknown"),
    }
}

/// One product front end, holding its screen and its startup reservation.
pub struct Interactive {
    screen: Live,
    memory: u64,
    remote: super::remote::Remote,
    remote_config: Option<(Box<[u8]>, smith_domain::run::Budget)>,
}

impl Interactive {
    /// Whether told text, without live pieces or response status, is the selected presentation.
    #[must_use]
    pub fn plain(&self) -> bool {
        self.screen.plain()
    }

    /// Prepare the bounded screen before any resource is adopted.
    #[must_use]
    pub fn new(plain: bool, limits: &smith_agent_service::Limits) -> Option<Self> {
        let conversations = limits.domain.run.conversations;
        let name_bytes = u32::try_from(smith_events::DETAIL_BYTES).ok()?;
        let client = limits.llm.adapter.client;
        // The writer can retain one popped view while the sink fills its slots.
        let memory = Live::worst_case(conversations, name_bytes)?
            .checked_add(View::worst_case(client.answer.max(client.strings))?)?;
        let mut screen = Live::new(plain, conversations);
        screen.accounts = Map::with_capacity(limits.domain.accounts);
        let memory = memory.checked_add(Map::<u32, &'static str>::worst_case(limits.domain.accounts)?)?;
        Some(Self {
            screen,
            memory: memory.checked_add(super::remote::Remote::worst_case(super::remote::TEXT_BYTES)?)?,
            remote: super::remote::Remote::new(super::remote::TEXT_BYTES),
            remote_config: None,
        })
    }

    pub(crate) fn account(&mut self, number: u32, words: &'static str, main: bool) {
        assert!(
            self.screen.accounts.insert(number, words).is_ok(),
            "startup account sources fit the admitted inventory"
        );
        if main {
            self.screen.account = words;
        }
    }

    pub(crate) fn remote_activation(&mut self, activation: Option<u64>) {
        self.remote.activation(activation);
    }

    pub(crate) fn piece(&mut self, piece: smith_local_service::LivePiece) -> Option<Box<[u8]>> {
        self.remote.piece(piece)
    }

    pub(crate) fn remote_config(&mut self, endpoint: &[u8], budget: smith_domain::run::Budget) {
        self.remote_config = Some((smith_events::clip(endpoint).into(), budget));
    }

    pub(crate) fn remote_turn(&mut self, turn: &smith_local_service::RemoteTurn) -> Box<[u8]> {
        let (endpoint, budget) = self.remote_config.as_ref().expect("spawned chat configuration");
        let text = self.remote.told(turn);
        let status = self.remote.status(turn, endpoint, *budget).with_account(self.screen.account);
        let status = self.screen.status(status);
        let mut output = Vec::with_capacity(text.len().saturating_add(status.len()));
        output.extend_from_slice(&text);
        output.extend_from_slice(&status);
        output.into_boxed_slice()
    }

    /// Update the status width from startup or an io window terminal.
    pub(crate) fn columns(&mut self, columns: u16) {
        self.screen.screen = Screen::Terminal { columns: if columns == 0 { u16::MAX } else { columns } };
    }

    /// Render the next typed record from the one sink's live queue.
    pub(crate) fn record(&mut self, record: View) -> Option<Box<[u8]>> {
        self.screen.record(record)
    }

    /// The outer shell's retained live-view footprint.
    #[must_use]
    pub const fn worst_case(&self) -> u64 {
        self.memory
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use smith_events::{BudgetStanding, Event, Record};

    fn model() -> Model {
        Model {
            account: None,
            backend: smith_events::Backend::Direct,
            endpoint: b"fake".as_slice().into(),
            model: b"small".as_slice().into(),
            window_tokens: 1024,
            output_tokens: 128,
            effort: None,
        }
    }

    fn record(event: Event) -> View {
        View::project(&Record { t_ms: 0, event }).expect("shown projection")
    }

    fn usage() -> Usage {
        Usage {
            input_tokens: Some(10),
            cache_read_tokens: None,
            cache_write_tokens: None,
            output_tokens: Some(4),
            reasoning_tokens: None,
        }
    }

    fn completed() -> Event {
        Event::ResponseCompleted(smith_events::ResponseCompleted {
            run: 1,
            conversation: 2,
            response: 3,
            outcome: smith_events::Outcome::Completed,
            stop: None,
            blocks: 1,
            calls: 0,
            invalid: 0,
            oversized: 0,
            cut: 0,
            usage: usage(),
            spent: 30,
            request_bytes: None,
            first_byte_ms: None,
            largest_gap_ms: None,
            duration_ms: 12,
            failure: None,
            retry_ms: None,
            completion: None,
        })
    }

    fn opened() -> Event {
        Event::ConversationOpened(smith_events::ConversationOpened {
            run: 1,
            conversation: 2,
            kind: smith_events::ConversationKind::Main,
            parent: None,
            call: None,
            thread: 1,
            model: model(),
            requested: None,
            share: None,
            clamped: skein_lib::List::with_capacity(0),
        })
    }

    #[test]
    fn a_terminal_status_fits_its_observed_width() {
        let mut live = Live::new(false, 5);
        live.screen = Screen::Terminal { columns: 12 };
        assert!(live.record(record(opened())).is_none());
        assert_eq!(live.record(record(completed())).unwrap().as_ref(), b"\nfake/small: \n");
    }

    fn started() -> Event {
        Event::RunStarted(smith_events::RunStarted {
            run: 2,
            resumed: false,
            messages: 1,
            main: model(),
            budget: smith_events::Budget {
                turns: 8,
                spend: 100,
                time_ms: 1000,
                reserve: smith_events::Reserve { turns: 0, time_ms: 0, spend: 0 },
            },
            tools: smith_events::Tools {
                withheld: skein_lib::List::with_capacity(0),
                families: skein_lib::List::with_capacity(0),
                wait: false,
                deliver: false,
                host: skein_lib::List::with_capacity(0),
            },
            contract: skein_lib::List::with_capacity(0),
        })
    }

    #[test]
    fn a_response_start_also_supplies_the_status_model_when_its_opening_was_lost() {
        let mut screen = Live::new(false, 5);
        screen.record(record(Event::ResponseStarted(smith_events::ResponseStarted {
            run: 1,
            conversation: 2,
            response: 3,
            turn: 1,
            attempt: 1,
            window: 1,
            model: model(),
            messages: 1,
            output_tokens: 128,
            tool_choice: smith_events::ToolChoice::Auto,
            prompt: None,
        })));
        assert_eq!(
            screen.record(record(completed())).unwrap().as_ref(),
            b"\nfake/small: in 10, cached unknown, out 4; 30 notional; 12 ms; budget unavailable\n"
        );
    }

    #[test]
    fn a_conversation_close_retires_its_status_model() {
        let mut screen = Live::new(false, 5);
        screen.record(record(opened()));
        screen.record(record(Event::ConversationClosed(smith_events::ConversationClosed {
            run: 1,
            conversation: 2,
            end: smith_events::ConversationEnd::Closed,
            which: None,
            failure: None,
            turns: 1,
            usage: usage(),
            spent: 30,
            duration_ms: 12,
        })));
        assert!(screen.record(record(completed())).is_none());
    }

    #[test]
    fn the_next_run_forgets_old_status_models() {
        let mut screen = Live::new(false, 5);
        screen.record(record(opened()));
        screen.record(record(started()));
        assert!(screen.record(record(completed())).is_none());
    }

    #[test]
    fn a_plain_tool_opening_shows_only_the_tool_name() {
        let tool = smith_events::ToolStarted {
            run: 1,
            conversation: 2,
            call: b"call".as_slice().into(),
            tool: b"read_file".as_slice().into(),
            source: smith_events::Source::Workspace,
            effect: smith_events::Effect::Read,
            deadline_ms: None,
            input_bytes: 0,
            input: None,
        };
        assert_eq!(
            Live::new(true, 5).record(record(Event::ToolStarted(tool))).unwrap().as_ref(),
            b"\ntool read_file\n"
        );
    }

    fn notice(kind: smith_events::NoticeKind, bytes: Option<u64>) -> Event {
        Event::Notice(smith_events::Notice {
            code: None,
            level: smith_events::Level::Warning,
            kind,
            run: None,
            account: None,
            reset_ms: None,
            bytes,
            exit: None,
            forced: None,
        })
    }

    #[test]
    fn an_opening_and_budget_join_the_next_response_status() {
        let mut screen = Live::new(false, 5);
        assert!(screen.record(record(opened())).is_none());
        assert!(
            screen
                .record(record(Event::Budget(BudgetStanding {
                    run: 1,
                    stage: smith_events::BudgetStage::Ordinary,
                    turns: Quantity { used: 2, limit: 8 },
                    spend: Quantity { used: 30, limit: 100 },
                    time_ms: Quantity { used: 12, limit: 1000 },
                    reserve: smith_events::Reserve { turns: 0, time_ms: 0, spend: 0 },
                    call: None,
                })))
                .is_none()
        );
        assert_eq!(
            screen.record(record(completed())).unwrap().as_ref(),
            b"\nfake/small: in 10, cached unknown, out 4; 30 notional; 12 ms; 6 turns, 70 notional, 988 ms left\n"
        );
    }

    #[test]
    fn a_lost_opening_omits_a_status_instead_of_guessing_its_model() {
        assert!(Live::new(false, 5).record(record(completed())).is_none());
    }

    #[test]
    fn a_plain_screen_omits_live_deltas_and_response_status() {
        let mut screen = Live::new(true, 5);
        let delta = Event::TextDelta(smith_events::TextDelta {
            run: 1,
            conversation: 2,
            response: 3,
            block: 0,
            text: b"piece".as_slice().into(),
        });
        assert!(screen.record(record(delta)).is_none());
        screen.record(record(opened()));
        assert!(screen.record(record(completed())).is_none());
    }

    #[test]
    fn a_reasoning_notice_keeps_its_observed_byte_count() {
        assert_eq!(
            Live::new(false, 5)
                .record(record(notice(smith_events::NoticeKind::ReasoningDropped, Some(19))))
                .unwrap()
                .as_ref(),
            b"reasoning item omitted (19 bytes)\n"
        );
    }

    #[test]
    fn the_containment_notice_has_words_and_unknown_notice_names_are_clipped() {
        let mut screen = Live::new(false, 5);
        assert_eq!(
            screen
                .record(record(notice(smith_events::NoticeKind::Unknown(b"not_contained".as_slice().into()), None)))
                .unwrap()
                .as_ref(),
            b"smith: commands are not contained\n"
        );
        assert_eq!(
            screen
                .record(record(notice(smith_events::NoticeKind::Unknown(b"future\nother".as_slice().into()), None)))
                .unwrap()
                .as_ref(),
            b"notice: future\n"
        );
    }

    #[test]
    fn terminal_failure_notices_remain_with_the_local_hosts_words() {
        for kind in [
            smith_events::NoticeKind::CredentialUnauthorized,
            smith_events::NoticeKind::AccountLimited,
            smith_events::NoticeKind::ForcedExit,
            smith_events::NoticeKind::TeardownAborted,
        ] {
            assert!(Live::new(false, 5).record(record(notice(kind, None))).is_none());
        }
    }

    #[test]
    fn a_screen_shows_the_streamed_text_without_a_trace() {
        let mut screen = Live::new(false, 5);
        let record = Record {
            t_ms: 0,
            event: Event::TextDelta(smith_events::TextDelta {
                run: 1,
                conversation: 1,
                response: 1,
                block: 0,
                text: b"as it arrives".as_slice().into(),
            }),
        };
        assert_eq!(screen.record(View::project(&record).unwrap()).unwrap().as_ref(), b"as it arrives");
    }

    #[test]
    fn a_status_keeps_unknown_usage_and_the_counted_budget() {
        let status = StatusLine {
            model: model(),
            usage: Usage {
                input_tokens: Some(10),
                cache_read_tokens: None,
                cache_write_tokens: None,
                output_tokens: Some(4),
                reasoning_tokens: None,
            },
            spent: 30,
            elapsed: Duration::from_nanos(12_000_000),
            remaining: None,
        };
        assert_eq!(
            status.text().as_ref(),
            b"\nfake/small: in 10, cached unknown, out 4; 30 notional; 12 ms; budget unavailable\n"
        );
    }

    #[test]
    fn a_tool_terminal_has_its_verdict_and_duration_only_outside_plain_mode() {
        let record = Record {
            t_ms: 0,
            event: Event::ToolCompleted(smith_events::ToolCompleted {
                run: 1,
                conversation: 1,
                call: b"call".as_slice().into(),
                tool: b"read_file".as_slice().into(),
                verdict: Verdict::Read,
                exit: None,
                job: None,
                bytes: 0,
                omitted_bytes: 0,
                clamped: false,
                duration_ms: 19,
                child: None,
                delivery: None,
            }),
        };
        assert_eq!(
            Live::new(false, 5).record(View::project(&record).unwrap()).unwrap().as_ref(),
            b"tool read_file: read (19 ms)\n"
        );
        assert!(Live::new(true, 5).record(View::project(&record).unwrap()).is_none());
    }
}
