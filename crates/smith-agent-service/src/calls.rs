//! Bounded provider call joins for event starts and terminals.
//! The book keeps the session-issued owner, actual provider identity, public
//! conversation scope and start time until the terminal fact. It never owns
//! result content or a rendered result. Selected provider arguments wait in a
//! bounded input book until their actual start. Workspace caps arrive from
//! the tool's terminal; delegated content remains outside this projection.
//! Contracts: protocol/events.md, sections 3.1, 3.6 and 5; domain/run.md, section 11.

#![expect(clippy::manual_let_else, reason = "bounded production entries keep explicit Option terminals")]

mod builtin;
mod jobs;

use alloc::boxed::Box;
use skein_lib::{List, Map, Time, Token};
use smith_domain::{session, tools};
use smith_events::{Event, Record, ToolCompleted, ToolStarted};

use crate::responses::Scope;
use crate::sinks::Sinks;

#[derive(Debug)]
struct Call {
    metadata: session::ToolCall,
    scope: Scope,
    at: Time,
    delivery: Option<smith_events::Delivered>,
    clamped: bool,
    omitted_bytes: u64,
}

/// Actual session batch capacities and aggregate provider metadata ownership.
#[derive(Clone, Copy)]
struct Bounds {
    slots: u32,
    bytes: u64,
}

impl Bounds {
    fn derive(limits: &session::Limits) -> Option<Bounds> {
        // The source session run slab holds two batches per session;
        // Provider identities are unchanged by application decoding and share
        // the raw answer's aggregate bytes. Decoded calls and replay envelopes
        // are never retained in this book.
        Some(Bounds {
            slots: limits.sessions.checked_mul(limits.parallel_tools)?.checked_mul(2)?,
            bytes: u64::from(limits.sessions).checked_mul(2)?.checked_mul(limits.answer_bytes)?,
        })
    }
}

/// One bounded set of actual provider calls which still owe a terminal.
#[derive(Debug)]
pub(crate) struct Book {
    calls: Map<Token, Call>,
    bytes: u64,
    bound: u64,
    children: crate::children::Book,
    landing: Option<Token>,
    inputs: crate::inputs::Book,
    builtin: builtin::Book,
}

impl Book {
    pub(crate) fn new(limits: &session::Limits, capture: crate::sinks::Capture) -> Book {
        let bounds = Bounds::derive(limits).expect("admitted session batch bounds fit");
        Book {
            calls: Map::with_capacity(bounds.slots),
            bytes: 0,
            bound: bounds.bytes,
            children: crate::children::Book::new(bounds.slots),
            landing: None,
            inputs: crate::inputs::Book::new(limits, capture),
            builtin: builtin::Book::new(limits),
        }
    }

    pub(crate) fn received(&mut self, owner: Token, completion: &smith_domain::llm::Completion) {
        self.inputs.received(owner, completion);
    }

    /// Join a validated root ask to its already observed actual provider call.
    pub(crate) fn called(&mut self, owner: Token, scope: Scope, ask: smith_domain::run::facts::Asked) {
        let call = self.calls.get(&owner).expect("session delegation precedes the causal root ask");
        assert_eq!(call.scope.run, scope.run, "the root call belongs to its provider activation");
        assert_eq!(call.scope.conversation, scope.conversation, "the root call belongs to its provider asker");
        match ask {
            smith_domain::run::facts::Asked::Deliver | smith_domain::run::facts::Asked::Finish => {
                assert!(self.landing.is_none(), "main's landing is an exclusive writer");
                self.landing = Some(owner);
            }
            smith_domain::run::facts::Asked::Wait
            | smith_domain::run::facts::Asked::Host
            | smith_domain::run::facts::Asked::SubAgent => {}
        }
    }

    /// Provider identity retained until this call's terminal drains.
    pub(crate) fn id(&self, owner: Token) -> Box<[u8]> {
        self.calls.get(&owner).expect("the returning child retains its provider call").metadata.id.clone()
    }

    /// Actual provider identity of the call which opened this child.
    pub(crate) fn child_opened(
        &mut self,
        owner: Token,
        number: u64,
        call_owner: Token,
        parent: Scope,
        granted: smith_domain::run::Granted,
        at: Time,
    ) -> Box<[u8]> {
        let call = self.calls.get_mut(&call_owner).expect("actual delegated sub-agent call precedes opening");
        assert_eq!(call.scope.run, parent.run, "the child and its call belong to the same activation");
        assert_eq!(call.scope.conversation, parent.conversation, "the child keeps its actual provider asker");
        call.clamped = lowered(granted.turns_set) || lowered(granted.time_set);
        self.children.opened(owner, number, call_owner, at);
        call.metadata.id.clone()
    }

    pub(crate) fn child_received(&mut self, owner: Token, usage: smith_domain::llm::Usage) {
        self.children.received(owner, usage);
    }

    pub(crate) fn child_ended(&mut self, owner: Token, end: smith_domain::run::facts::EndKind, at: Time) {
        self.children.ended(owner, end, at);
    }

    pub(crate) fn returned(&mut self, owner: Token, result: smith_domain::run::facts::Return) {
        self.children.returned(owner, result);
        match result {
            smith_domain::run::facts::Return::Answered { cut, .. } => {
                self.calls.get_mut(&owner).expect("actual child retains its enclosing provider call").omitted_bytes =
                    cut;
            }
            smith_domain::run::facts::Return::Crossed
            | smith_domain::run::facts::Return::Waited
            | smith_domain::run::facts::Return::Withheld
            | smith_domain::run::facts::Return::HostAnswered
            | smith_domain::run::facts::Return::HostTooLarge
            | smith_domain::run::facts::Return::HostUnknown
            | smith_domain::run::facts::Return::HostRejected
            | smith_domain::run::facts::Return::Delivered
            | smith_domain::run::facts::Return::Nothing
            | smith_domain::run::facts::Return::DeliveryRefused
            | smith_domain::run::facts::Return::Accepted
            | smith_domain::run::facts::Return::Rejected
            | smith_domain::run::facts::Return::ChecksFailed
            | smith_domain::run::facts::Return::Stale
            | smith_domain::run::facts::Return::DeliveryFailed
            | smith_domain::run::facts::Return::Cancelled
            | smith_domain::run::facts::Return::TimedOut
            | smith_domain::run::facts::Return::Busy
            | smith_domain::run::facts::Return::Unanswered { .. }
            | smith_domain::run::facts::Return::Refused => {}
        }
        if self.landing == Some(owner) {
            self.landing = None;
        }
        if let Some(delivery) = returned_delivery(result) {
            let call = self.calls.get_mut(&owner).expect("root terminal keeps its actual provider call");
            call.delivery = Some(delivery);
        }
    }

    /// Root deliveries are serialized main writes; no name or text parser joins them.
    pub(crate) fn delivered(&mut self, status: smith_domain::run::DeliveryStatus) {
        let owner = self.landing.expect("actual delivery belongs to the exclusive main landing");
        let call = self.calls.get_mut(&owner).expect("landing retains its provider call");
        call.delivery = Some(match status {
            smith_domain::run::DeliveryStatus::Delivered => smith_events::Delivered::Delivered,
            smith_domain::run::DeliveryStatus::Nothing => smith_events::Delivered::Nothing,
            smith_domain::run::DeliveryStatus::Refused => smith_events::Delivered::Refused,
            smith_domain::run::DeliveryStatus::Failed(_) => smith_events::Delivered::Failed,
            smith_domain::run::DeliveryStatus::Stale => smith_events::Delivered::Stale,
        });
    }

    /// Project only a fact with actual provider metadata; kit lifecycle is not a call.
    pub(crate) fn fact(&mut self, origin: Time, fact: &session::Fact, sinks: &mut Sinks) {
        self.children.session(fact);
        if self.builtin.fact(origin, fact, &mut self.inputs, sinks) {
            return;
        }
        let opener = match self.call_opener(fact) {
            Some(opener) => opener,
            None => return,
        };
        let metadata = match &fact.call {
            Some(metadata) => metadata,
            None => return,
        };
        let scope = sinks.scope(opener).expect("actual conversation opening precedes its provider call");
        match fact.kind {
            session::FactKind::Tools { fact: tools::FactKind::Started { .. }, .. }
            | session::FactKind::DelegateStarted { .. } => {
                sinks.record(&self.started(origin, fact.at, opener, scope, metadata));
            }
            session::FactKind::Tools {
                fact: tools::FactKind::Answered { verdict, bytes, job, omitted_bytes, clamped, .. },
                ..
            } => {
                // An entrance refusal settles the session's actual call in
                // the same step, without a lower work-start fact or IO.
                if !self.calls.contains_key(&metadata.owner) {
                    sinks.record(&self.started(origin, fact.at, opener, scope, metadata));
                }
                let terminal = Terminal {
                    verdict: verdict_kind(verdict),
                    job: jobs::number(job),
                    exit: command_exit(verdict),
                    bytes,
                    omitted_bytes,
                    clamped,
                };
                sinks.record(&self.completed(origin, fact.at, metadata, terminal));
            }
            session::FactKind::JobEnded { .. } => sinks.record(&jobs::ended(origin, fact, scope, metadata)),
            session::FactKind::DelegateAnswered { bytes, error, .. } => {
                let verdict = if error { smith_events::Verdict::Error } else { smith_events::Verdict::Result };
                sinks.record(&self.completed(
                    origin,
                    fact.at,
                    metadata,
                    Terminal { job: None, verdict, exit: None, bytes, omitted_bytes: 0, clamped: false },
                ));
            }
            session::FactKind::DelegateCancelled { .. } | session::FactKind::DelegateWithheld { .. } => {
                let verdict = smith_events::Verdict::Withdrawn;
                let terminal = Terminal { job: None, verdict, exit: None, bytes: 0, omitted_bytes: 0, clamped: false };
                sinks.record(&self.completed(origin, fact.at, metadata, terminal));
            }
            session::FactKind::BuiltinStarted { .. }
            | session::FactKind::BuiltinAnswered { .. }
            | session::FactKind::CompactionStarted { .. }
            | session::FactKind::CompactionEnded { .. }
            | session::FactKind::Opened { .. }
            | session::FactKind::CompletionStarted { .. }
            | session::FactKind::CompletionAnswered { .. }
            | session::FactKind::CompletionFailed { .. }
            | session::FactKind::CompletionCancelled { .. }
            | session::FactKind::CompletionRetried { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::Yielded { .. }
            | session::FactKind::Used { .. }
            | session::FactKind::Ended { .. }
            | session::FactKind::Tools {
                fact:
                    tools::FactKind::Opened { .. }
                    | tools::FactKind::Refused { .. }
                    | tools::FactKind::Closing { .. }
                    | tools::FactKind::JobEnded { .. }
                    | tools::FactKind::Closed { .. },
                ..
            } => unreachable!("only selected actual call facts reach projection"),
        }
    }

    fn call_opener(&mut self, fact: &session::Fact) -> Option<Token> {
        match fact.kind {
            session::FactKind::Tools {
                opener,
                fact: tools::FactKind::Started { .. } | tools::FactKind::Answered { .. },
            }
            | session::FactKind::DelegateStarted { opener, .. }
            | session::FactKind::DelegateAnswered { opener, .. }
            | session::FactKind::DelegateCancelled { opener }
            | session::FactKind::DelegateWithheld { opener }
            | session::FactKind::JobEnded { opener, .. } => Some(opener),
            session::FactKind::Ended { opener, .. } => {
                self.inputs.ended(opener);
                None
            }
            session::FactKind::BuiltinStarted { .. }
            | session::FactKind::BuiltinAnswered { .. }
            | session::FactKind::CompactionStarted { .. }
            | session::FactKind::CompactionEnded { .. }
            | session::FactKind::Opened { .. }
            | session::FactKind::CompletionStarted { .. }
            | session::FactKind::CompletionAnswered { .. }
            | session::FactKind::CompletionFailed { .. }
            | session::FactKind::CompletionCancelled { .. }
            | session::FactKind::CompletionRetried { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::Yielded { .. }
            | session::FactKind::Used { .. }
            | session::FactKind::Tools {
                fact:
                    tools::FactKind::Opened { .. }
                    | tools::FactKind::Refused { .. }
                    | tools::FactKind::Closing { .. }
                    | tools::FactKind::JobEnded { .. }
                    | tools::FactKind::Closed { .. },
                ..
            } => None,
        }
    }

    fn started(&mut self, origin: Time, at: Time, opener: Token, scope: Scope, metadata: &session::ToolCall) -> Record {
        let input = self.inputs.take(opener, metadata);
        let bytes = metadata_bytes(metadata);
        self.bytes = self.bytes.checked_add(bytes).expect("bounded call identity sum fits");
        assert!(self.bytes <= self.bound, "actual source batches bound copied provider identity");
        let old = self
            .calls
            .insert(
                metadata.owner,
                Call { metadata: metadata.clone(), scope, at, delivery: None, clamped: false, omitted_bytes: 0 },
            )
            .expect("source session run slab bounds active event calls");
        assert!(old.is_none(), "one provider start per actual session call owner");
        let source = match metadata.source {
            session::ToolSource::Workspace => smith_events::Source::Workspace,
            session::ToolSource::Run => smith_events::Source::Run,
            session::ToolSource::Host => smith_events::Source::Host,
        };
        let effect = match metadata.effect {
            tools::Effect::Read => smith_events::Effect::Read,
            tools::Effect::Write => smith_events::Effect::Write,
        };
        Record {
            t_ms: millis(at, origin),
            event: Event::ToolStarted(ToolStarted {
                run: scope.run,
                conversation: scope.conversation,
                call: metadata.id.clone(),
                tool: metadata.name.clone(),
                source,
                effect,
                deadline_ms: Some(millis(metadata.deadline, at)),
                input_bytes: metadata.input_bytes,
                input,
            }),
        }
    }

    fn completed(&mut self, origin: Time, at: Time, metadata: &session::ToolCall, terminal: Terminal) -> Record {
        let call = self.calls.remove(&metadata.owner).expect("actual terminal follows its one start");
        assert_eq!(call.metadata, *metadata, "terminal keeps its exact parent call metadata");
        self.bytes = self.bytes.checked_sub(metadata_bytes(&call.metadata)).expect("identity was retained at start");
        Record {
            t_ms: millis(at, origin),
            event: Event::ToolCompleted(ToolCompleted {
                run: call.scope.run,
                conversation: call.scope.conversation,
                call: call.metadata.id,
                tool: call.metadata.name,
                verdict: terminal.verdict,
                exit: terminal.exit,
                job: terminal.job,
                bytes: terminal.bytes,
                omitted_bytes: terminal
                    .omitted_bytes
                    .checked_add(call.omitted_bytes)
                    .expect("bounded actual tool omissions"),
                clamped: terminal.clamped || call.clamped,
                duration_ms: millis(at, call.at),
                child: self.children.take(metadata.owner),
                delivery: call.delivery,
            }),
        }
    }
}

struct Terminal {
    job: Option<u64>,
    verdict: smith_events::Verdict,
    exit: Option<smith_events::CommandExit>,
    bytes: u64,
    omitted_bytes: u64,
    clamped: bool,
}

fn lowered(set: smith_domain::run::Set) -> bool {
    match set {
        smith_domain::run::Set::Asked | smith_domain::run::Set::Pool => false,
        smith_domain::run::Set::Reserve | smith_domain::run::Set::Deadline => true,
    }
}

fn returned_delivery(result: smith_domain::run::facts::Return) -> Option<smith_events::Delivered> {
    match result {
        smith_domain::run::facts::Return::Delivered => Some(smith_events::Delivered::Delivered),
        smith_domain::run::facts::Return::Nothing => Some(smith_events::Delivered::Nothing),
        smith_domain::run::facts::Return::DeliveryRefused => Some(smith_events::Delivered::Refused),
        smith_domain::run::facts::Return::DeliveryFailed => Some(smith_events::Delivered::Failed),
        smith_domain::run::facts::Return::Stale => Some(smith_events::Delivered::Stale),
        smith_domain::run::facts::Return::Crossed
        | smith_domain::run::facts::Return::Waited
        | smith_domain::run::facts::Return::Withheld
        | smith_domain::run::facts::Return::HostAnswered
        | smith_domain::run::facts::Return::HostTooLarge
        | smith_domain::run::facts::Return::HostUnknown
        | smith_domain::run::facts::Return::HostRejected
        | smith_domain::run::facts::Return::Accepted
        | smith_domain::run::facts::Return::Rejected
        | smith_domain::run::facts::Return::ChecksFailed
        | smith_domain::run::facts::Return::Cancelled
        | smith_domain::run::facts::Return::TimedOut
        | smith_domain::run::facts::Return::Busy
        | smith_domain::run::facts::Return::Answered { .. }
        | smith_domain::run::facts::Return::Unanswered { .. }
        | smith_domain::run::facts::Return::Refused => None,
    }
}

fn millis(at: Time, since: Time) -> u64 {
    at.saturating_since(since).as_nanos() / 1_000_000
}

fn metadata_bytes(metadata: &session::ToolCall) -> u64 {
    let id = u64::try_from(metadata.id.len()).expect("bounded provider id fits u64");
    id.checked_add(u64::try_from(metadata.name.len()).expect("bounded tool name fits u64"))
        .expect("bounded provider identity fits u64")
}

fn command_exit(verdict: tools::Verdict) -> Option<smith_events::CommandExit> {
    match verdict {
        tools::Verdict::Exited { exit } => match exit {
            tools::Exit::Code { code } => Some(smith_events::CommandExit::Code { code: i64::from(code) }),
            tools::Exit::Signal { signal } => Some(smith_events::CommandExit::Signal { signal: u64::from(signal) }),
            tools::Exit::TimedOut => None,
        },
        tools::Verdict::Running
        | tools::Verdict::Stopped
        | tools::Verdict::Read
        | tools::Verdict::Listed
        | tools::Verdict::Found
        | tools::Verdict::Written { .. }
        | tools::Verdict::Edited { .. }
        | tools::Verdict::NotGranted
        | tools::Verdict::Outside
        | tools::Verdict::ReadOnly
        | tools::Verdict::TooLong
        | tools::Verdict::NotFound
        | tools::Verdict::NotFile
        | tools::Verdict::Linked
        | tools::Verdict::Protected
        | tools::Verdict::NotDirectory
        | tools::Verdict::TooLarge
        | tools::Verdict::NotRead
        | tools::Verdict::Stale
        | tools::Verdict::NoMatch { .. }
        | tools::Verdict::Ambiguous { .. }
        | tools::Verdict::Unchanged { .. }
        | tools::Verdict::Failed { .. }
        | tools::Verdict::TimedOut
        | tools::Verdict::Cancelled
        | tools::Verdict::Busy
        | tools::Verdict::NulByte => None,
    }
}

fn verdict_kind(verdict: tools::Verdict) -> smith_events::Verdict {
    match verdict {
        tools::Verdict::Running => smith_events::Verdict::Running,
        tools::Verdict::Stopped => smith_events::Verdict::Stopped,
        tools::Verdict::Read => smith_events::Verdict::Read,
        tools::Verdict::Listed => smith_events::Verdict::Listed,
        tools::Verdict::Found => smith_events::Verdict::Found,
        tools::Verdict::Written { .. } => smith_events::Verdict::Written,
        tools::Verdict::Edited { .. } => smith_events::Verdict::Edited,
        tools::Verdict::Exited { exit } => match exit {
            tools::Exit::Code { .. } | tools::Exit::Signal { .. } => smith_events::Verdict::Exited,
            tools::Exit::TimedOut => smith_events::Verdict::TimedOut,
        },
        tools::Verdict::NotGranted => smith_events::Verdict::NotGranted,
        tools::Verdict::Outside => smith_events::Verdict::Outside,
        tools::Verdict::ReadOnly => smith_events::Verdict::ReadOnly,
        tools::Verdict::TooLong => smith_events::Verdict::TooLong,
        tools::Verdict::NotFound => smith_events::Verdict::NotFound,
        tools::Verdict::NotFile => smith_events::Verdict::NotFile,
        tools::Verdict::Linked => smith_events::Verdict::Linked,
        tools::Verdict::Protected => smith_events::Verdict::Protected,
        tools::Verdict::NotDirectory => smith_events::Verdict::NotDirectory,
        tools::Verdict::TooLarge => smith_events::Verdict::TooLarge,
        tools::Verdict::NotRead => smith_events::Verdict::NotRead,
        tools::Verdict::Stale => smith_events::Verdict::Stale,
        tools::Verdict::NoMatch { .. } => smith_events::Verdict::NoMatch,
        tools::Verdict::Ambiguous { .. } => smith_events::Verdict::Ambiguous,
        tools::Verdict::Unchanged { .. } => smith_events::Verdict::Unchanged,
        tools::Verdict::Failed { .. } => smith_events::Verdict::Failed,
        tools::Verdict::TimedOut => smith_events::Verdict::TimedOut,
        tools::Verdict::Cancelled => smith_events::Verdict::Cancelled,
        tools::Verdict::Busy => smith_events::Verdict::Busy,
        tools::Verdict::NulByte => smith_events::Verdict::NulByte,
    }
}

pub(crate) fn worst_case(limits: &session::Limits, string: u32) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let bounds = Bounds::derive(limits)?;
    Map::<Token, Call>::worst_case(bounds.slots)?
        .checked_add(crate::children::worst_case(bounds.slots)?)?
        .checked_add(builtin::Book::worst_case(limits)?)?
        .checked_add(bounds.bytes)?
        .checked_add(List::<Record>::worst_case(2)?)?
        .checked_add(u64::from(string).checked_mul(4)?)
}

/// Whether call joins and their metadata counter fit (programming-model.md, 6.3).
#[must_use]
pub(crate) fn valid(limits: &session::Limits) -> bool {
    Bounds::derive(limits).is_some() && builtin::valid(limits)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "programming-model.md section10.2: deterministic test harnesses use ordinary Rust"
    )]

    use super::*;
    use crate::sinks::tests::{drain, drain_ready, grant};
    use crate::sinks::{Capture, Delivery, Destination, Ended};
    use skein_lib::Duration;

    #[test]
    fn invalid_limits_are_refused() {
        let mut limits = source();
        limits.sessions = 2;
        assert!(valid(&limits));
        for invalid in [
            session::Limits { sessions: u32::MAX, ..limits },
            session::Limits { parallel_tools: u32::MAX, ..limits },
            session::Limits { answer_bytes: u64::MAX, ..limits },
            session::Limits { completion_blocks: u32::MAX, ..limits },
        ] {
            assert!(!valid(&invalid));
        }
    }

    fn no_tool_result(record: &Record, capture: Capture) {
        let limits = crate::sinks::tests::limits(1).records;
        let line = smith_events::write(record, &capture.encoded(), &limits).unwrap().unwrap();
        assert!(!core::str::from_utf8(&line).unwrap().contains("\"result\":"));
    }

    fn source() -> session::Limits {
        let mut profile = crate::profile::standard();
        profile.declared.memory = 2_281_701_376;
        let mut source = crate::profile::derive(&profile, &crate::tests::declarations(0)).unwrap().domain.session;
        source.sessions = 1;
        source.parallel_tools = 2;
        source.completion_bytes = 1024;
        source
    }

    fn fixture(source: &session::Limits, capture: Capture) -> (Book, Sinks, Token, Token, session::ToolCall) {
        let book = Book::new(source, capture);
        let limits = crate::sinks::tests::limits(1);
        let mut sinks = Sinks::new(&limits, capture, 7, crate::sinks::tests::DEADLINE);
        let destination = Token::new(999);
        sinks.add(Destination::Trace, destination, Delivery::Complete).unwrap();
        grant(&mut sinks, destination);
        let opener = Token::new(91);
        sinks.conversation(
            Time::ZERO,
            Time::ZERO,
            opener,
            smith_events::ConversationOpened {
                run: 77,
                conversation: 0,
                kind: smith_events::ConversationKind::Main,
                parent: None,
                call: None,
                thread: 0,
                model: smith_events::Model {
                    account: None,
                    backend: smith_events::Backend::Direct,
                    endpoint: b"endpoint-7".as_slice().into(),
                    model: b"model-7".as_slice().into(),
                    window_tokens: 4096,
                    output_tokens: 32,
                    effort: None,
                },
                requested: None,
                share: None,
                clamped: List::with_capacity(0),
            },
        );
        let opening = drain_ready(&mut sinks, destination);
        assert_eq!(opening.len(), 1);
        let metadata = session::ToolCall {
            owner: Token::new(43),
            id: b"provider-\"id".as_slice().into(),
            name: b"actual-tool".as_slice().into(),
            source: session::ToolSource::Workspace,
            effect: tools::Effect::Read,
            deadline: Time::from_nanos(103_000_000),
            input_bytes: 1234,
        };
        (book, sinks, destination, opener, metadata)
    }

    fn timed_out_command_fact(opener: Token, metadata: session::ToolCall) -> session::Fact {
        session::Fact {
            at: metadata.deadline,
            kind: session::FactKind::Tools {
                opener,
                fact: tools::FactKind::Answered {
                    session: Token::new(88),
                    tool: tools::Tool::Shell,
                    verdict: tools::Verdict::Exited { exit: tools::Exit::TimedOut },
                    bytes: 0,
                    omitted_bytes: 0,
                    job: None,
                    clamped: true,
                },
            },
            call: Some(metadata),
            response: None,
        }
    }

    fn running_fact(opener: Token, metadata: session::ToolCall) -> session::Fact {
        session::Fact {
            at: Time::from_nanos(1_000_000_000),
            kind: session::FactKind::Tools {
                opener,
                fact: tools::FactKind::Answered {
                    session: Token::new(88),
                    tool: tools::Tool::Shell,
                    verdict: tools::Verdict::Running,
                    bytes: 0,
                    job: Some(tools::JobNumber(7)),
                    omitted_bytes: 0,
                    clamped: false,
                },
            },
            call: Some(metadata),
            response: None,
        }
    }

    #[test]
    fn a_running_shell_terminal_names_its_actual_job() {
        let source = source();
        let (mut book, mut sinks, destination, opener, metadata) = fixture(&source, Capture::None);
        book.fact(Time::ZERO, &running_fact(opener, metadata), &mut sinks);
        let records = drain_ready(&mut sinks, destination);
        let Event::ToolCompleted(completed) = &records[1].event else { panic!("shell terminal") };
        assert_eq!(completed.job, Some(7));
        assert_eq!(completed.verdict, smith_events::Verdict::Running);
    }

    #[test]
    fn an_unreported_job_end_keeps_its_shell_identity() {
        let source = source();
        let (mut book, mut sinks, destination, opener, metadata) = fixture(&source, Capture::None);
        book.fact(Time::ZERO, &running_fact(opener, metadata.clone()), &mut sinks);
        let mut records = drain_ready(&mut sinks, destination);
        book.fact(
            Time::ZERO,
            &session::Fact {
                at: Time::from_nanos(11_000_000_000),
                kind: session::FactKind::JobEnded {
                    opener,
                    job: tools::JobNumber(7),
                    end: tools::JobEnd::Cancelled,
                    unread_bytes: 13,
                    started: Time::from_nanos(1_000_000_000),
                },
                call: Some(metadata),
                response: None,
            },
            &mut sinks,
        );
        records.extend(drain_ready(&mut sinks, destination));
        let Event::JobEnded(ended) = &records[2].event else { panic!("unreported job terminal") };
        assert_eq!(ended.call.as_ref(), b"provider-\"id");
        assert_eq!(ended.job, 7);
        assert_eq!(ended.verdict, smith_events::Verdict::Cancelled);
        assert_eq!(ended.unread_bytes, 13);
        assert_eq!(ended.duration_ms, 10_000);
    }

    #[test]
    fn a_timed_out_command_is_projected_as_timed_out_with_its_bound() {
        let source = source();
        let (mut book, mut sinks, destination, opener, mut metadata) = fixture(&source, Capture::Everything);
        metadata.name = b"shell".as_slice().into();
        metadata.effect = tools::Effect::Write;
        let start = book.started(
            Time::ZERO,
            Time::from_nanos(3_000_000),
            opener,
            Scope { run: 77, conversation: 0 },
            &metadata,
        );
        sinks.record(&start);
        book.fact(Time::ZERO, &timed_out_command_fact(opener, metadata), &mut sinks);
        let records = drain_ready(&mut sinks, destination);
        let [started, completed] = records.as_slice() else { panic!("one command start and terminal") };
        let Event::ToolStarted(started) = &started.event else { panic!("command start") };
        let Event::ToolCompleted(completed) = &completed.event else { panic!("command terminal") };
        assert_eq!(started.deadline_ms, Some(100));
        assert_eq!(completed.verdict, smith_events::Verdict::TimedOut);
        assert_eq!(completed.exit, None);
        assert!(completed.clamped);
        assert_eq!(completed.duration_ms, 100);
        assert!(book.calls.is_empty());
    }

    fn workspace_fact(
        opener: Token,
        metadata: session::ToolCall,
        verdict: tools::Verdict,
        omitted: u64,
        clamped: bool,
    ) -> session::Fact {
        session::Fact {
            at: Time::from_nanos(14_000_000),
            kind: session::FactKind::Tools {
                opener,
                fact: tools::FactKind::Answered {
                    session: Token::new(88),
                    tool: tools::Tool::Read,
                    verdict,
                    bytes: 13,
                    omitted_bytes: omitted,
                    job: None,
                    clamped,
                },
            },
            call: Some(metadata),
            response: None,
        }
    }

    #[test]
    fn actual_workspace_terminals_keep_scalar_measurements_and_provider_identity_under_every_capture() {
        let source = source();
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            for (verdict, expected, exit, omitted, clamped) in [
                (tools::Verdict::Read, smith_events::Verdict::Read, None, 19, true),
                (tools::Verdict::Read, smith_events::Verdict::Read, None, 0, false),
                (
                    tools::Verdict::Exited { exit: tools::Exit::Code { code: 0 } },
                    smith_events::Verdict::Exited,
                    Some(smith_events::CommandExit::Code { code: 0 }),
                    21,
                    true,
                ),
                (
                    tools::Verdict::Exited { exit: tools::Exit::Signal { signal: 15 } },
                    smith_events::Verdict::Exited,
                    Some(smith_events::CommandExit::Signal { signal: 15 }),
                    0,
                    false,
                ),
                (
                    tools::Verdict::Exited { exit: tools::Exit::TimedOut },
                    smith_events::Verdict::TimedOut,
                    None,
                    9,
                    true,
                ),
                (tools::Verdict::TimedOut, smith_events::Verdict::TimedOut, None, 0, true),
                (tools::Verdict::Cancelled, smith_events::Verdict::Cancelled, None, 0, true),
            ] {
                let (mut book, mut sinks, destination, opener, metadata) = fixture(&source, capture);
                book.fact(
                    Time::ZERO,
                    &session::Fact {
                        at: Time::from_nanos(3_000_000),
                        kind: session::FactKind::Tools {
                            opener,
                            fact: tools::FactKind::Started { session: Token::new(88), tool: tools::Tool::Read },
                        },
                        call: Some(metadata.clone()),
                        response: None,
                    },
                    &mut sinks,
                );
                book.fact(Time::ZERO, &workspace_fact(opener, metadata.clone(), verdict, omitted, clamped), &mut sinks);
                assert!(book.calls.is_empty());
                assert_eq!(book.bytes, 0);
                sinks.ended(
                    Time::from_nanos(20_000_000),
                    Time::ZERO,
                    &Ended {
                        exit: 0,
                        answered: true,
                        aborted: false,
                        teardown: Some(Duration::from_millis(6)),
                        usage: skein_io::Measured::Unread,
                        channel: None,
                    },
                );
                let records = drain(&mut sinks, destination);
                let [started, completed, _] = records.as_slice() else {
                    panic!("one actual start and one completed record")
                };
                let Event::ToolStarted(started) = &started.event else { panic!("start") };
                let Event::ToolCompleted(completed) = &completed.event else { panic!("completed") };
                assert_eq!((started.run, started.conversation, completed.run, completed.conversation), (77, 0, 77, 0));
                assert_eq!(started.call, metadata.id);
                assert_eq!(completed.call, metadata.id);
                assert_eq!(started.tool, metadata.name);
                assert_eq!(completed.tool, metadata.name);
                assert_eq!(started.input_bytes, 1234);
                assert_eq!(started.deadline_ms, Some(100));
                assert_eq!(started.input, None);
                no_tool_result(records.get(1).expect("terminal"), capture);
                assert_eq!(completed.verdict, expected);
                assert_eq!(completed.exit, exit);
                assert_eq!(
                    (completed.bytes, completed.omitted_bytes, completed.clamped, completed.duration_ms),
                    (13, omitted, clamped, 11)
                );
            }
        }
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "one15-case child control keeps the actual three-layer typed source join together"
    )]
    fn actual_child_terminals_join_public_identity_reported_usage_and_omissions_under_every_capture() {
        use smith_domain::run;
        let source = source();
        let used = run::facts::Used { turns: 2, spend: 18, time: Duration::from_millis(13) };
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            for (result, yielded, end, expected, omitted) in [
                (
                    run::facts::Return::Answered { used, cut: 17 },
                    session::Yield::Done,
                    run::facts::EndKind::Closed,
                    smith_events::ChildStop::End,
                    17,
                ),
                (
                    run::facts::Return::Answered { used, cut: 17 },
                    session::Yield::Budget,
                    run::facts::EndKind::Closed,
                    smith_events::ChildStop::Budget,
                    17,
                ),
                (
                    run::facts::Return::Unanswered { used },
                    session::Yield::Done,
                    run::facts::EndKind::Budget(run::Exhausted::Turns),
                    smith_events::ChildStop::Failed,
                    0,
                ),
                (
                    run::facts::Return::Cancelled,
                    session::Yield::Done,
                    run::facts::EndKind::Closed,
                    smith_events::ChildStop::Closed,
                    0,
                ),
                (
                    run::facts::Return::TimedOut,
                    session::Yield::Done,
                    run::facts::EndKind::Closed,
                    smith_events::ChildStop::Closed,
                    0,
                ),
            ] {
                let (mut book, mut sinks, destination, opener, mut metadata) = fixture(&source, capture);
                metadata.source = session::ToolSource::Run;
                book.fact(
                    Time::ZERO,
                    &session::Fact {
                        at: Time::from_nanos(3_000_000),
                        kind: session::FactKind::DelegateStarted { opener, block: 0 },
                        call: Some(metadata.clone()),
                        response: None,
                    },
                    &mut sinks,
                );
                let scope = sinks.scope(opener).unwrap();
                book.called(metadata.owner, scope, run::facts::Asked::SubAgent);
                let child = Token::new(92);
                let actual_id = book.child_opened(
                    child,
                    19,
                    metadata.owner,
                    scope,
                    run::Granted {
                        turns: 2,
                        time: Duration::from_millis(30),
                        spend: 18,
                        turns_set: run::Set::Reserve,
                        time_set: run::Set::Asked,
                    },
                    Time::from_nanos(5_000_000),
                );
                assert_eq!(actual_id, metadata.id);
                for (number, charge, usage) in [
                    (
                        1,
                        7,
                        smith_domain::llm::Usage {
                            input_tokens: Some(3),
                            output_tokens: None,
                            cache_read_tokens: Some(0),
                            cache_write_tokens: None,
                            reasoning_tokens: None,
                        },
                    ),
                    (
                        2,
                        11,
                        smith_domain::llm::Usage {
                            input_tokens: None,
                            output_tokens: Some(5),
                            cache_read_tokens: None,
                            cache_write_tokens: Some(2),
                            reasoning_tokens: Some(1),
                        },
                    ),
                ] {
                    book.child_received(child, usage);
                    book.fact(
                        Time::ZERO,
                        &session::Fact {
                            at: Time::from_nanos(9_000_000),
                            kind: session::FactKind::Used { opener: child, usage },
                            call: None,
                            response: Some(session::ResponseInfo { number, spent: Some(charge) }),
                        },
                        &mut sinks,
                    );
                }
                let answered = result == run::facts::Return::Answered { used, cut: 17 };
                if answered {
                    book.fact(
                        Time::ZERO,
                        &session::Fact {
                            at: Time::from_nanos(17_000_000),
                            kind: session::FactKind::Yielded { opener: child, stop: yielded },
                            call: None,
                            response: None,
                        },
                        &mut sinks,
                    );
                }
                book.fact(
                    Time::ZERO,
                    &session::Fact {
                        at: Time::from_nanos(18_000_000),
                        kind: session::FactKind::Ended {
                            opener: child,
                            end: session::End::Closed,
                            turns: 2,
                            usage: smith_domain::llm::Usage::NONE,
                        },
                        call: None,
                        response: None,
                    },
                    &mut sinks,
                );
                book.child_ended(child, end, Time::from_nanos(18_000_000));
                book.returned(metadata.owner, result);
                let terminal = if result == run::facts::Return::Cancelled {
                    session::FactKind::DelegateCancelled { opener }
                } else {
                    session::FactKind::DelegateAnswered { opener, bytes: 23, error: !answered }
                };
                book.fact(
                    Time::ZERO,
                    &session::Fact {
                        at: Time::from_nanos(21_000_000),
                        kind: terminal,
                        call: Some(metadata.clone()),
                        response: None,
                    },
                    &mut sinks,
                );
                sinks.ended(
                    Time::from_nanos(23_000_000),
                    Time::ZERO,
                    &Ended {
                        exit: 0,
                        answered: true,
                        aborted: false,
                        teardown: Some(Duration::ZERO),
                        usage: skein_io::Measured::Unread,
                        channel: None,
                    },
                );
                let records = drain(&mut sinks, destination);
                let [_, terminal, _] = records.as_slice() else { panic!("paired call and end") };
                let Event::ToolCompleted(completed) = &terminal.event else { panic!("typed completion") };
                assert_eq!((completed.run, completed.conversation), (77, 0));
                assert_eq!(completed.call, metadata.id);
                assert_eq!(completed.omitted_bytes, omitted);
                assert!(completed.clamped);
                let child = completed.child.as_ref().unwrap();
                assert_eq!((child.conversation, child.turns, child.time_ms, child.spent), (19, 2, 13, 18));
                assert_eq!(child.stop, expected);
                assert_eq!(
                    child.usage,
                    smith_events::Usage {
                        input_tokens: Some(3),
                        output_tokens: Some(5),
                        cache_read_tokens: Some(0),
                        cache_write_tokens: Some(2),
                        reasoning_tokens: Some(1)
                    }
                );
                no_tool_result(terminal, capture);
                assert!(book.calls.is_empty());
            }
        }
    }
    #[test]
    fn actual_main_deliveries_keep_the_exclusive_provider_call_under_every_capture() {
        use smith_domain::run;
        let source = source();
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            for (status, returned, expected) in [
                (run::DeliveryStatus::Delivered, run::facts::Return::Delivered, smith_events::Delivered::Delivered),
                (run::DeliveryStatus::Nothing, run::facts::Return::Nothing, smith_events::Delivered::Nothing),
                (run::DeliveryStatus::Refused, run::facts::Return::DeliveryRefused, smith_events::Delivered::Refused),
                (
                    run::DeliveryStatus::Failed(run::DeliveryReason::Unknown),
                    run::facts::Return::DeliveryFailed,
                    smith_events::Delivered::Failed,
                ),
                (run::DeliveryStatus::Stale, run::facts::Return::Stale, smith_events::Delivered::Stale),
            ] {
                for ask in [run::facts::Asked::Finish, run::facts::Asked::Deliver] {
                    let (mut book, mut sinks, destination, opener, mut metadata) = fixture(&source, capture);
                    metadata.source = session::ToolSource::Run;
                    metadata.name = if ask == run::facts::Asked::Finish {
                        b"finish".as_slice().into()
                    } else {
                        b"deliver".as_slice().into()
                    };
                    book.fact(
                        Time::ZERO,
                        &session::Fact {
                            at: Time::from_nanos(3_000_000),
                            kind: session::FactKind::DelegateStarted { opener, block: 0 },
                            call: Some(metadata.clone()),
                            response: None,
                        },
                        &mut sinks,
                    );
                    book.called(metadata.owner, Scope { run: 77, conversation: 0 }, ask);
                    book.delivered(status);
                    book.returned(
                        metadata.owner,
                        if ask == run::facts::Asked::Finish { run::facts::Return::Accepted } else { returned },
                    );
                    book.fact(
                        Time::ZERO,
                        &session::Fact {
                            at: Time::from_nanos(9_000_000),
                            kind: session::FactKind::DelegateAnswered { opener, bytes: 23, error: false },
                            call: Some(metadata.clone()),
                            response: None,
                        },
                        &mut sinks,
                    );
                    sinks.ended(
                        Time::from_nanos(11_000_000),
                        Time::ZERO,
                        &Ended {
                            exit: 0,
                            answered: true,
                            aborted: false,
                            teardown: Some(Duration::ZERO),
                            usage: skein_io::Measured::Unread,
                            channel: None,
                        },
                    );
                    let records = drain(&mut sinks, destination);
                    let [_, terminal, _] = records.as_slice() else { panic!("paired provider call and process end") };
                    let Event::ToolCompleted(completed) = &terminal.event else { panic!("actual provider terminal") };
                    assert_eq!(completed.call, metadata.id);
                    assert_eq!(completed.tool, metadata.name);
                    assert_eq!(completed.delivery, Some(expected.clone()));
                    assert_eq!(completed.child, None);
                    assert_eq!((completed.omitted_bytes, completed.clamped), (0, false));
                    assert!(book.calls.is_empty() && book.landing.is_none());
                }
            }
        }
    }
}
