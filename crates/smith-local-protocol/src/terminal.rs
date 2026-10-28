//! Bounded line and signal translation for one person at a terminal
//! (protocol/hosts.md, section 5.1). Only completed lines reach the local
//! domain; a second interrupt asks the selected child to drop through local policy.

use alloc::boxed::Box;
use skein_lib::{List, Queue};
use smith_local_domain as local;

mod entered;
mod failure;
pub use entered::{Entered, Slash, entered};

/// Receiving and display bounds of one local terminal.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum bytes in one continued message.
    pub line_bytes: u32,
    /// Maximum bytes in one shown notice.
    pub show_bytes: u32,
}

/// One output to the local service.
#[derive(Debug)]
pub enum Event {
    /// A completed person line for the local domain.
    Domain(Box<local::Event>),
    /// A second interrupt before an answer drops the selected child; its terminals still settle.
    DropRun,
    /// A bounded notice to write on the terminal.
    Write(Box<[u8]>),
}

/// Maximum output cells emitted by one entry point.
#[must_use]
pub const fn max_out() -> u32 {
    1
}

/// Checked retained upper bound for the terminal.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if limits.line_bytes == 0 || limits.show_bytes == 0 {
        return None;
    }
    List::<u8>::worst_case(limits.line_bytes)?.checked_add(u64::from(limits.show_bytes))
}

/// One line accumulator and the live run's interrupt count.
#[derive(Debug)]
pub struct Terminal {
    limits: Limits,
    line: List<u8>,
    discarding: bool,
    cancelled: bool,
}

impl Terminal {
    /// Build the bounded terminal translator.
    #[must_use]
    pub fn new(limits: Limits) -> Option<Self> {
        if limits.line_bytes == 0 || limits.show_bytes == 0 {
            return None;
        }
        Some(Self { line: List::with_capacity(limits.line_bytes), limits, discarding: false, cancelled: false })
    }

    /// Feed one byte from standard input; only a complete line emits an event.
    pub fn feed(&mut self, byte: u8, out: &mut Queue<Event>) {
        if byte == b'\n' {
            self.finish_line(out);
            return;
        }
        if self.discarding {
            return;
        }
        if self.line.push(byte).is_err() {
            self.line = List::with_capacity(self.limits.line_bytes);
            self.discarding = true;
        }
    }

    /// Translate a terminal interrupt; the second one asks local policy to drop the child.
    pub fn interrupt(&mut self, out: &mut Queue<Event>) {
        if self.cancelled {
            out.push(Event::DropRun);
        } else {
            self.cancelled = true;
            out.push(Event::Domain(Box::new(local::Event::Interrupt)));
        }
    }

    /// A new run can be interrupted independently of the previous answer.
    pub fn answer_finished(&mut self) {
        self.cancelled = false;
    }

    /// Translate terminal closure after any complete line.
    pub fn closed(&mut self, out: &mut Queue<Event>) {
        out.push(Event::Domain(Box::new(local::Event::Closed)));
    }

    /// Forward bounded local-domain text to the terminal.
    pub fn show(&self, text: Box<[u8]>, out: &mut Queue<Event>) {
        let cap = usize::try_from(self.limits.show_bytes).expect("u32 fits usize");
        let kept = text.get(..cap).unwrap_or(text.as_ref());
        out.push(Event::Write(Box::from(kept)));
    }

    /// Render a typed host notice as bounded UTF-8 lines ending in newlines.
    pub fn notice(
        &self,
        notice: &local::Notice,
        chat: &[u8],
        models: &[smith_domain::run::charter::Llm],
        out: &mut Queue<Event>,
    ) {
        out.push(Event::Write(notice_text(notice, chat, models, self.limits.show_bytes)));
    }

    /// Render the same bounded notice for the terminal and operator's error output.
    #[must_use]
    pub fn notice_text(
        notice: &local::Notice,
        chat: &[u8],
        models: &[smith_domain::run::charter::Llm],
        cap: u32,
    ) -> Box<[u8]> {
        let mut text = Shown { bytes: List::with_capacity(cap), cut: false };
        match notice {
            local::Notice::ChatRefused { name, reason } => chat_refused(&mut text, name, *reason),
            local::Notice::Waiting => append(&mut text, b"Waiting for a message"),
            local::Notice::Checking(checks) => append(
                &mut text,
                match checks {
                    local::Checks::Started => b"Running checks",
                    local::Checks::Ended => b"Checks finished",
                },
            ),
            local::Notice::NotDelivered { line, why } => {
                if let Some(name) = line {
                    append(&mut text, b"Line ");
                    decimal(&mut text, name.raw());
                    append(&mut text, b" not delivered: ");
                }
                append(
                    &mut text,
                    match why {
                        local::Undelivered::TooLong => b"Line is too long",
                        local::Undelivered::Dropped => b"the run ended without reading it",
                        local::Undelivered::Refused(reason) => match reason {
                            smith_host_domain::MessageRefusal::TooLarge => b"it is too large",
                            smith_host_domain::MessageRefusal::Full => b"the inbox is full",
                            smith_host_domain::MessageRefusal::Ending => b"the run is ending",
                            smith_host_domain::MessageRefusal::NameInUse => b"its name is in use",
                        },
                    },
                );
            }
            local::Notice::Failure(failure) => match failure {
                local::Failure::NamesExhausted => append(&mut text, b"Chat names exhausted"),
                local::Failure::Account(account) => failure::account(&mut text, account),
                local::Failure::HeadUnavailable { directory } => {
                    append(&mut text, b"run refused: repository ");
                    append(&mut text, directory);
                    append(&mut text, b": its head cannot be read");
                }
                local::Failure::Run { failure, detail } => {
                    failure::run(&mut text, failure, detail, models, None, None, None);
                }
                local::Failure::Refused(refusal) => failure::refused(&mut text, *refusal),
                local::Failure::Unstarted(end) => failure::end(&mut text, *end),
                local::Failure::Credential { account, reason } => {
                    failure::credential(&mut text, *account, reason.clone());
                }
                local::Failure::Agent { failure: why, end, detail } => {
                    append(&mut text, b"run failed: the agent ");
                    append(
                        &mut text,
                        match why {
                            local::AgentFailure::NotStarted => b"could not start",
                            local::AgentFailure::Fault(fault) => match fault {
                                smith_host_domain::Fault::Exited => b"ended without answering",
                                smith_host_domain::Fault::Rules => b"broke the channel rules",
                                smith_host_domain::Fault::TooLarge => b"exceeded its receiving bound",
                                smith_host_domain::Fault::NoProgress => b"made no progress",
                                smith_host_domain::Fault::WallTime => b"reached its wall time bound",
                            },
                        },
                    );
                    failure::end(&mut text, *end);
                    failure::detail(&mut text, detail);
                }
                local::Failure::DeliveryUnsaved => {
                    append(&mut text, b"A delivery committed, but its answer could not be saved");
                }
                local::Failure::Store { turn, reason } => store_failure(&mut text, chat, *turn, *reason),
            },
            local::Notice::Warning(local::Warning::ForcedExit { exit, forced }) => {
                append(&mut text, b"warning: agent forced to exit after answering");
                failure::end(&mut text, smith_host_domain::End::Exited { exit: *exit, forced: *forced });
            }
        }
        newline(&mut text);
        text.bytes.into_boxed()
    }

    fn finish_line(&mut self, out: &mut Queue<Event>) {
        if self.discarding {
            self.discarding = false;
            out.push(Event::Write(Box::from(&b"Line is too long\n"[..])));
            return;
        }
        if self.line.as_slice().last() == Some(&b'\r') {
            self.pop_last();
        }
        if self.line.as_slice().last() == Some(&b'\\') {
            self.pop_last();
            return;
        }
        let completed = core::mem::replace(&mut self.line, List::with_capacity(self.limits.line_bytes));
        out.push(Event::Domain(Box::new(local::Event::Line { text: completed.into_boxed() })));
    }

    fn pop_last(&mut self) {
        let count = self.line.as_slice().len().saturating_sub(1);
        let mut kept = List::with_capacity(self.limits.line_bytes);
        for byte in self.line.as_slice().get(..count).unwrap_or_default() {
            kept.push(*byte).expect("prefix fits line capacity");
        }
        self.line = kept;
    }
}

/// Render the same bounded notice for the terminal and operator's error output.
#[must_use]
pub fn notice_text(
    notice: &local::Notice,
    chat: &[u8],
    models: &[smith_domain::run::charter::Llm],
    cap: u32,
) -> Box<[u8]> {
    Terminal::notice_text(notice, chat, models, cap)
}

/// Render a run failure with names and the host clock from its answer, within the notice cap.
#[must_use]
pub fn run_failure_text(
    failure: &smith_host_domain::RunFailure,
    diagnostic: &[u8],
    models: &[smith_domain::run::charter::Llm],
    cap: u32,
    account_name: Option<&[u8]>,
    endpoint_name: Option<&[u8]>,
    answered_at: Option<skein_lib::Wall>,
) -> Box<[u8]> {
    let mut text = Shown { bytes: List::with_capacity(cap), cut: false };
    failure::run(&mut text, failure, diagnostic, models, account_name, endpoint_name, answered_at);
    newline(&mut text);
    text.bytes.into_boxed()
}

fn store_failure(text: &mut Shown, chat: &[u8], turn: Option<u32>, reason: local::StoreFailure) {
    match reason {
        local::StoreFailure::OlderBuild { why } => {
            let words = why.words();
            append(text, words.first().expect("older prefix").as_bytes());
            append(text, chat);
            append(text, words.get(1).expect("older suffix").as_bytes());
        }
        local::StoreFailure::Read | local::StoreFailure::Write => {
            if let Some(number) = turn {
                append(text, b"run failed: chat ");
                append(text, chat);
                append(text, b": turn ");
                decimal(text, u64::from(number));
                append(
                    text,
                    match reason {
                        local::StoreFailure::Read => b" could not be kept: read failed",
                        local::StoreFailure::Write => b" could not be kept: write failed",
                        local::StoreFailure::OlderBuild { .. } => unreachable!("older words rendered above"),
                    },
                );
            } else {
                append(text, b"The chat could not be saved");
            }
        }
    }
}

struct Shown {
    bytes: List<u8>,
    cut: bool,
}

// Keep one byte for a newline and cut only at a complete character.
fn append(text: &mut Shown, bytes: &[u8]) {
    if text.cut {
        return;
    }
    let mut skip = 0_u8;
    for (index, byte) in bytes.iter().enumerate() {
        if skip > 0 {
            skip = skip.checked_sub(1).expect("continuation count is positive");
            continue;
        }
        let width = if *byte < 0x80 {
            1
        } else if *byte < 0xe0 {
            2
        } else if *byte < 0xf0 {
            3
        } else {
            4
        };
        if width >= text.bytes.room() {
            text.cut = true;
            return;
        }
        let end = index.checked_add(usize::try_from(width).expect("character width fits usize")).expect("bounded text");
        let Some(character) = bytes.get(index..end) else {
            text.cut = true;
            return;
        };
        for byte in character {
            text.bytes.push(*byte).expect("character and newline room checked");
        }
        skip = u8::try_from(width.checked_sub(1).expect("positive width")).expect("character width fits u8");
    }
}

fn newline(text: &mut Shown) {
    if text.bytes.room() > 0 {
        text.bytes.push(b'\n').expect("newline room checked");
    }
}

fn decimal(text: &mut Shown, mut number: u64) {
    let mut digits = [0; 20];
    let mut first = digits.len();
    for _ in 0..digits.len() {
        first = first.checked_sub(1).expect("u64 fits twenty decimal digits");
        *digits.get_mut(first).expect("bounded decimal position") =
            u8::try_from(number % 10).expect("decimal digit").checked_add(b'0').expect("ASCII digit");
        number /= 10;
        if number == 0 {
            break;
        }
    }
    append(text, digits.get(first..).expect("bounded decimal suffix"));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn terminal() -> Terminal {
        Terminal::new(Limits { line_bytes: 32, show_bytes: 32 }).expect("valid terminal")
    }

    fn rendered(notice: local::Notice) -> Box<[u8]> {
        let terminal = Terminal::new(Limits { line_bytes: 32, show_bytes: 128 }).expect("valid terminal");
        let mut out = Queue::with_capacity(max_out());
        terminal.notice(&notice, b"main", &[], &mut out);
        let Some(Event::Write(text)) = out.pop() else { panic!("one notice written") };
        assert!(out.is_empty());
        text
    }

    #[test]
    fn waiting_is_one_line_ending_in_a_newline() {
        assert_eq!(rendered(local::Notice::Waiting).as_ref(), b"Waiting for a message\n");
    }

    #[test]
    fn checking_spans_are_shown_as_whole_lines() {
        for (checks, expected) in [
            (local::Checks::Started, b"Running checks\n".as_slice()),
            (local::Checks::Ended, b"Checks finished\n".as_slice()),
        ] {
            assert_eq!(rendered(local::Notice::Checking(checks)).as_ref(), expected);
        }
    }

    #[test]
    fn a_refused_line_is_shown_by_name_and_reason_as_one_line() {
        for (reason, expected) in [
            (smith_host_domain::MessageRefusal::Full, b"Line 3 not delivered: the inbox is full\n".as_slice()),
            (smith_host_domain::MessageRefusal::TooLarge, b"Line 3 not delivered: it is too large\n".as_slice()),
            (smith_host_domain::MessageRefusal::Ending, b"Line 3 not delivered: the run is ending\n".as_slice()),
            (smith_host_domain::MessageRefusal::NameInUse, b"Line 3 not delivered: its name is in use\n".as_slice()),
        ] {
            let notice = local::Notice::NotDelivered {
                line: Some(skein_lib::Token::new(3)),
                why: local::Undelivered::Refused(reason),
            };
            assert_eq!(rendered(notice).as_ref(), expected);
        }
    }

    #[test]
    fn unnamed_lines_show_the_local_admission_reason() {
        for (why, expected) in [
            (local::Undelivered::TooLong, b"Line is too long\n".as_slice()),
            (local::Undelivered::Dropped, b"the run ended without reading it\n".as_slice()),
        ] {
            assert_eq!(rendered(local::Notice::NotDelivered { line: None, why }).as_ref(), expected);
        }
    }

    #[test]
    fn a_store_failure_names_the_chat_and_its_turn_on_two_lines() {
        let notice =
            local::Notice::Failure(local::Failure::Store { turn: Some(3), reason: local::StoreFailure::Write });
        assert_eq!(rendered(notice).as_ref(), b"run failed: chat main: turn 3 could not be kept: write failed\n");
    }

    #[test]
    fn a_store_failure_without_a_turn_is_one_line() {
        let notice = local::Notice::Failure(local::Failure::Store { turn: None, reason: local::StoreFailure::Read });
        assert_eq!(rendered(notice).as_ref(), b"The chat could not be saved\n");
    }

    #[test]
    fn local_failures_keep_their_own_words() {
        for (failure, expected) in [
            (local::Failure::NamesExhausted, b"Chat names exhausted\n".as_slice()),
            (
                local::Failure::HeadUnavailable { directory: b"workspace".as_slice().into() },
                b"run refused: repository workspace: its head cannot be read\n".as_slice(),
            ),
            (
                local::Failure::Unstarted(smith_host_domain::End::Busy),
                b"agent start refused: no agent slot\n".as_slice(),
            ),
            (local::Failure::DeliveryUnsaved, b"A delivery committed, but its answer could not be saved\n".as_slice()),
        ] {
            assert_eq!(rendered(local::Notice::Failure(failure)).as_ref(), expected);
        }
    }

    #[test]
    fn account_failures_keep_their_own_words() {
        for (account, expected) in account_failure_rows() {
            assert_eq!(
                rendered(local::Notice::Failure(local::Failure::Account(account))).as_ref(),
                expected,
                "{expected:?}"
            );
        }
    }

    fn account_failure_rows() -> [(local::AccountFailure, &'static [u8]); 7] {
        [
            (local::AccountFailure::Unavailable, b"A model account is unavailable\n".as_slice()),
            (local::AccountFailure::NotRefreshed, b"A model account could not refresh\n".as_slice()),
            (
                local::AccountFailure::Limited {
                    account: 7,
                    reset_at: Some(skein_lib::Wall::from_nanos(36_060_000_000_000)),
                },
                b"run failed: account 7 reached its usage limit, usable again at 10:01\n".as_slice(),
            ),
            (
                local::AccountFailure::Limited { account: 7, reset_at: None },
                b"run failed: account 7 reached its usage limit\n".as_slice(),
            ),
            (
                local::AccountFailure::Refused {
                    account: 7,
                    code: b"billing".as_slice().into(),
                    reason: b"contact support\nprivate".as_slice().into(),
                },
                b"run failed: account 7: the provider refused it: billing: contact support\n".as_slice(),
            ),
            (
                local::AccountFailure::Refused {
                    account: 7,
                    code: Box::default(),
                    reason: b"contact support".as_slice().into(),
                },
                b"run failed: account 7: the provider refused it: contact support\n".as_slice(),
            ),
            (
                local::AccountFailure::Refused {
                    account: 7,
                    code: b"billing".as_slice().into(),
                    reason: Box::default(),
                },
                b"run failed: account 7: the provider refused it: billing\n".as_slice(),
            ),
        ]
    }

    #[test]
    fn unsuccessful_agent_ends_keep_their_own_words() {
        let notice = local::Notice::Failure(local::Failure::Agent {
            failure: local::AgentFailure::Fault(smith_host_domain::Fault::Rules),
            end: smith_host_domain::End::Exited {
                exit: Some(smith_host_domain::Exit::Code(7)),
                forced: smith_host_domain::Forced::No,
            },
            detail: b"broken frame\nsecret next line".as_slice().into(),
        });
        assert_eq!(
            rendered(notice).as_ref(),
            b"run failed: the agent broke the channel rules (exit 7): broken frame\n"
        );
    }

    #[test]
    fn a_capped_store_notice_ends_on_a_character_boundary_with_a_newline() {
        let terminal = Terminal::new(Limits { line_bytes: 32, show_bytes: 37 }).expect("valid terminal");
        let mut out = Queue::with_capacity(max_out());
        terminal.notice(
            &local::Notice::Failure(local::Failure::Store { turn: Some(1), reason: local::StoreFailure::Write }),
            "éé".as_bytes(),
            &[],
            &mut out,
        );
        let Some(Event::Write(text)) = out.pop() else { panic!("one notice written") };
        assert_eq!(text.as_ref(), "run failed: chat éé: turn 1 could \n".as_bytes());
    }

    #[test]
    fn a_line_and_its_continuation_are_one_message() {
        let mut terminal = terminal();
        let mut out = Queue::with_capacity(max_out());
        for byte in b"hello\\\nworld\n" {
            terminal.feed(*byte, &mut out);
        }
        match out.pop().expect("one line") {
            Event::Domain(event) => {
                let local::Event::Line { text } = *event else { panic!("unexpected terminal event") };
                assert_eq!(text.as_ref(), b"helloworld");
            }
            Event::DropRun | Event::Write(_) => panic!("unexpected terminal event"),
        }
        assert!(out.is_empty());
    }

    #[test]
    fn a_second_interrupt_drops_the_selected_child() {
        let mut terminal = terminal();
        let mut out = Queue::with_capacity(max_out());
        terminal.interrupt(&mut out);
        match out.pop().expect("first interrupt") {
            Event::Domain(event) => {
                let local::Event::Interrupt = *event else { panic!("expected cancel") };
            }
            Event::DropRun | Event::Write(_) => panic!("expected cancel"),
        }
        terminal.interrupt(&mut out);
        match out.pop().expect("second interrupt") {
            Event::DropRun => {}
            Event::Domain(_) | Event::Write(_) => panic!("expected process stop"),
        }
        terminal.answer_finished();
        terminal.interrupt(&mut out);
        match out.pop().expect("new run interrupt") {
            Event::Domain(event) => {
                let local::Event::Interrupt = *event else { panic!("expected cancel") };
            }
            Event::DropRun | Event::Write(_) => panic!("expected cancel"),
        }
    }

    #[test]
    fn an_overlong_line_is_discarded_until_newline() {
        let mut terminal = Terminal::new(Limits { line_bytes: 3, show_bytes: 32 }).expect("valid terminal");
        let mut out = Queue::with_capacity(max_out());
        for byte in b"abcd\n" {
            terminal.feed(*byte, &mut out);
        }
        match out.pop().expect("too long notice") {
            Event::Write(_) => {}
            Event::Domain(_) | Event::DropRun => panic!("expected notice"),
        }
        for byte in b"ok\n" {
            terminal.feed(*byte, &mut out);
        }
        match out.pop().expect("following line") {
            Event::Domain(event) => {
                let local::Event::Line { text } = *event else { panic!("expected line") };
                assert_eq!(text.as_ref(), b"ok");
            }
            Event::DropRun | Event::Write(_) => panic!("expected line"),
        }
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use smith_host_domain as host;

    fn shown(failure: local::Failure) -> Box<[u8]> {
        notice_text(&local::Notice::Failure(failure), b"main", &[], 512)
    }

    fn named_failure(failure: host::RunFailure, detail: &[u8]) -> Box<[u8]> {
        let models = [smith_domain::run::charter::Llm {
            effort: None,
            prices: smith_domain::run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1 },
            dialect: 0,
            account: Some(7),
            endpoint: smith_domain::run::charter::Endpoint(3),
            model: b"small".as_slice().into(),
            window: 16,
            output: 8,
        }];
        run_failure_text(
            &failure,
            detail,
            &models,
            512,
            Some(b"work"),
            Some(b"codex"),
            Some(skein_lib::Wall::from_nanos(36_000_000_000_000)),
        )
    }

    #[test]
    fn a_model_failure_uses_its_endpoint_name_and_clipped_detail() {
        let failure = host::RunFailure::Model(host::ModelFault::Completion {
            failure: host::CompletionFailure::Unavailable,
            evidence: host::CompletionEvidence::Response { status: Some(503) },
            model: 0,
            attempts: 3,
            report: host::Report::none(),
        });
        assert_eq!(
            named_failure(failure, b"provider reason\nignored").as_ref(),
            b"run failed: codex/small unavailable (HTTP 503) after 3 attempts: provider reason\n"
        );
    }

    #[test]
    fn an_exhausted_failure_uses_its_answer_account_and_clock() {
        for (wait, expected) in [
            (
                Some(skein_lib::Duration::from_secs(60)),
                b"run failed: account work reached its usage limit, usable again at 10:01\n".as_slice(),
            ),
            (None, b"run failed: account work reached its usage limit\n".as_slice()),
        ] {
            let failure = host::RunFailure::Model(host::ModelFault::Limited { account: 7, reset: wait });
            assert_eq!(named_failure(failure, b"").as_ref(), expected);
        }
    }

    #[test]
    fn a_credential_failure_uses_its_answer_models_account_name() {
        let failure = host::RunFailure::Model(host::ModelFault::Completion {
            failure: host::CompletionFailure::Unauthorized,
            evidence: host::CompletionEvidence::Response { status: Some(401) },
            model: 0,
            attempts: 3,
            report: host::Report::none(),
        });
        assert_eq!(named_failure(failure, b"").as_ref(), b"account work unauthorized\n");
    }

    #[test]
    fn each_budget_last_line_uses_the_runs_counted_values() {
        for (exhausted, used, bound, expected) in [
            (host::Exhausted::Turns, 7, 8, b"run failed: out of turns: used 7 of 8\n".as_slice()),
            (host::Exhausted::Spend, 103, 101, b"run failed: out of spend: used 103 of 101\n".as_slice()),
            (host::Exhausted::Time, 1234, 1500, b"run failed: out of time: used 1234 of 1500\n".as_slice()),
        ] {
            let failure = host::RunFailure::Budget { exhausted, used: Some(used), bound: Some(bound) };
            assert_eq!(named_failure(failure, b"").as_ref(), expected);
        }
    }

    #[test]
    fn each_budget_failure_names_what_ran_out() {
        for (kind, expected) in [
            (host::Exhausted::Turns, b"run failed: out of turns\n".as_slice()),
            (host::Exhausted::Spend, b"run failed: out of spend\n".as_slice()),
            (host::Exhausted::Time, b"run failed: out of time\n".as_slice()),
        ] {
            assert_eq!(
                shown(local::Failure::Run {
                    failure: host::RunFailure::Budget { exhausted: kind, used: None, bound: None },
                    detail: Box::new([])
                })
                .as_ref(),
                expected
            );
        }
    }

    #[test]
    fn a_provider_limit_keeps_its_name_bound_status_and_detail() {
        let failure = host::RunFailure::Model(host::ModelFault::Completion {
            failure: host::CompletionFailure::Limit { which: host::Limit::Path(host::Cap::ErrorBody), bound: 19 },
            evidence: host::CompletionEvidence::Response { status: Some(400) },
            model: 0,
            attempts: 3,
            report: host::Report::none(),
        });
        assert_eq!(
            shown(local::Failure::Run { failure, detail: b"provider reason\nprivate".as_slice().into() }).as_ref(),
            b"run failed: limit: error_body reached its bound of 19 (HTTP 400) after 3 attempts: provider reason\n"
        );
    }

    #[test]
    fn a_provider_timeout_keeps_its_phase() {
        let failure = host::RunFailure::Model(host::ModelFault::Completion {
            failure: host::CompletionFailure::TimedOut { phase: host::Phase::Head },
            evidence: host::CompletionEvidence::Unsent,
            model: 0,
            attempts: 2,
            report: host::Report::none(),
        });
        assert_eq!(
            shown(local::Failure::Run { failure, detail: Box::new([]) }).as_ref(),
            b"run failed: timed out during response head after 2 attempts\n"
        );
    }

    #[test]
    fn a_refused_start_names_what_was_invalid() {
        assert_eq!(
            shown(local::Failure::Refused(host::Refusal::Invalid(host::RunInvalid::Workspace))).as_ref(),
            b"run refused: invalid: workspace\n"
        );
    }

    #[test]
    fn a_missing_credential_names_its_account_and_login() {
        assert_eq!(
            shown(local::Failure::Credential { account: 7, reason: local::CredentialFailure::Missing }).as_ref(),
            b"account 7 has no credential: run `smith login 7`\n"
        );
    }
    #[test]
    fn each_model_fault_has_a_last_line() {
        for (fault, expected) in last_line_faults() {
            assert_eq!(named_failure(host::RunFailure::Model(fault.clone()), b"").as_ref(), expected, "{fault:?}");
        }
    }
    fn last_line_faults() -> [(host::ModelFault, &'static [u8]); 9] {
        [
            (
                host::ModelFault::Limited { account: 7, reset: None },
                b"run failed: account work reached its usage limit\n".as_slice(),
            ),
            (
                host::ModelFault::Refused { account: 7, code: b"quota".as_slice().into() },
                b"run failed: account work: the provider refused it: quota\n".as_slice(),
            ),
            (host::ModelFault::Unauthorized { account: 7 }, b"run failed: account work unauthorized\n".as_slice()),
            (
                host::ModelFault::Program { name: b"login".as_slice().into(), exit: Some(2), detail: Box::default() },
                b"run failed: login exited before its answer: \n".as_slice(),
            ),
            (host::ModelFault::Declined, b"run failed: refused\n".as_slice()),
            (host::ModelFault::Provider, b"run failed: provider\n".as_slice()),
            (host::ModelFault::Truncated, b"run failed: truncated\n".as_slice()),
            (host::ModelFault::Malformed, b"run failed: malformed\n".as_slice()),
            (
                host::ModelFault::Completion {
                    failure: host::CompletionFailure::Unsupported,
                    evidence: host::CompletionEvidence::Unsent,
                    model: 0,
                    attempts: 1,
                    report: host::Report::none(),
                },
                b"run failed: codex/small invalid after 1 attempts\n".as_slice(),
            ),
        ]
    }
}

fn chat_refused(text: &mut Shown, name: &[u8], reason: local::ChatRefusal) {
    append(text, b"Chat ");
    append(text, name);
    append(
        text,
        match reason {
            local::ChatRefusal::Busy => b": the current chat is still busy",
            local::ChatRefusal::Name => b": invalid name",
            local::ChatRefusal::Missing => b": does not exist",
            local::ChatRefusal::Ephemeral => b": the ephemeral store cannot select another chat",
            local::ChatRefusal::Io => b": directory operation failed",
        },
    );
}

#[cfg(test)]
#[expect(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    reason = "verbatim operator-word tests are ordinary Rust (programming-model.md, section 10.2)"
)]
mod older_tests {
    use super::*;

    #[test]
    fn actual_store_failure_rendering_keeps_older_words_even_without_a_turn() {
        for why in [
            local::Older::Unrecorded,
            local::Older::Layout(local::StoreFamily::Store),
            local::Older::Layout(local::StoreFamily::Transcript),
        ] {
            for turn in [None, Some(3)] {
                let notice = local::Notice::Failure(local::Failure::Store {
                    turn,
                    reason: local::StoreFailure::OlderBuild { why },
                });
                let text = notice_text(&notice, b"older", &[], 2048);
                assert_eq!(text.as_ref(), format!("{}\n", why.words().join("older")).as_bytes());
            }
        }
    }
}
