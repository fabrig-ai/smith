//! Keeps bounded scalar joins and endpoint names; never learns credentials.
//! `fact` precedes `request`; the service supplies its exact `answer`.
//! Contracts: protocol/events.md, sections 3–5; domain/run.md, section 11.

#![expect(clippy::manual_map, reason = "bounded production entries use explicit matches without closures")]

use alloc::boxed::Box;
use skein_lib::{Duration, List, Map, Time, Token};
use smith_domain::{self as domain, run};
use smith_events::{Event, Record};
use smith_protocol_channel as channel;
use smith_protocol_llm as llm;

use crate::sinks::{Capture, SinkLimits, Sinks};

/// Startup's bounded event configuration, supplied before any descriptor is adopted.
#[derive(Clone, Copy, Debug)]
pub struct EventLimits {
    pub sinks: SinkLimits,
    pub capture: Capture,
    /// The product asks for a best-effort typed live view, independent of capture.
    pub front: bool,
    /// IO's stall deadline for each adopted destination.
    pub write_deadline: Duration,
}

impl EventLimits {
    /// Derive receiving and destination ownership from the admitted source bounds.
    /// `effort_bytes` is the charter codec’s reasoning effort bound.
    /// `header` supplies the startup inventory's count and longest configured name.
    #[must_use]
    pub fn derive(
        limits: &crate::Limits,
        capture: Capture,
        destinations: u32,
        effort_bytes: u32,
        header: &[smith_events::EffectiveLimit],
        write_deadline: Duration,
    ) -> Option<EventLimits> {
        let charter = limits.channel.charter;
        let mut header_string = 0;
        for entry in header {
            header_string = header_string.max(u32::try_from(entry.name.len()).ok()?);
        }
        let metadata_string = charter
            .llm_endpoint
            .max(charter.llm_model)
            .max(charter.host_tool_name)
            .max(charter.field_name)
            .max(charter.run_result_label_some)
            .max(effort_bytes)
            .max(header_string)
            .max(46);
        let client = limits.llm.adapter.client;
        let string = metadata_string.max(client.strings).max(limits.channel.bodies.message_label);
        let answer_bytes = u32::try_from(limits.domain.run.outcome_bytes).ok()?;
        let metadata_items =
            charter.run_result_fields.max(limits.domain.run.host_tools).max(18).max(u32::try_from(header.len()).ok()?);
        let content = answer_bytes
            .max(client.answer)
            .max(client.request)
            .max(limits.channel.bodies.message_text)
            .max(client.detail_bytes);
        let items = metadata_items.max(client.output_items).max(client.history_items);
        let records = smith_events::Limits { string, content, items };
        let scalar_content = answer_bytes.max(client.detail_bytes);
        let metadata = smith_events::Limits { string, content: scalar_content, items: metadata_items };
        let scalar = crate::sinks::largest_line(&metadata, Capture::None, answer_bytes, metadata_string)?;
        // One completion's blocks share the Client's answer bytes. A message
        // receipt owns only that channel message's exact label and text.
        let observed = client
            .answer
            .checked_mul(6)?
            .checked_add(client.output_items.checked_mul(128)?)?
            .max(limits.channel.bodies.message_text.checked_mul(6)?);
        // The component validates this exact native body before copying a new
        // part. Its payload is a subset of that body. At most history_items
        // messages and blocks add 128 bytes of event framing per total block;
        // tool names add only bounded array framing. No replay payload is copied.
        let prompt = client
            .request
            .checked_add(client.history_items.checked_mul(128)?)?
            .checked_add(client.tools.checked_mul(128)?)?
            .checked_add(metadata_string.checked_mul(18)?)?
            .checked_add(2048)?;
        let observation_line = match capture {
            Capture::None | Capture::Calls => scalar.checked_add(observed)?,
            Capture::Everything => scalar.checked_add(observed)?.max(prompt),
        };
        let mut sinks = SinkLimits {
            destinations,
            output: 0,
            records,
            answer_bytes,
            metadata_string,
            metadata_items,
            scalar_content,
            observation_line,
            fact_extra: observed,
            conversations: limits.domain.run.conversations,
        };
        sinks.output = crate::sinks::output_bound(&sinks, capture)?;
        if !crate::sinks::valid(&sinks, capture) {
            return None;
        }
        Some(EventLimits { sinks, capture, front: false, write_deadline })
    }
}

#[derive(Debug)]
struct Start {
    messages: u64,
    llm: run::charter::Llm,
    budget: smith_events::Budget,
    tools: smith_events::Tools,
    contract: List<smith_events::Form>,
}

/// One invocation's typed records, supplied to its bounded destination drain.
#[derive(Debug)]
pub struct Producer {
    pub(crate) sinks: Sinks,
    names: channel::Endpoints,
    programs: Box<[u32]>,
    calls: crate::calls::Book,
    messages: crate::messages::Book,
    capture: Capture,
    origin: Option<Time>,
    start: Option<Start>,
    admitted: Option<u64>,
    admitted_at: Option<Time>,
    activation: Option<u64>,
    answer_at: Option<Time>,
    overflow_notice: Option<run::Overflow>,
    wind_down: bool,
    unread: u64,
    check_at: Option<Time>,
}

impl Producer {
    /// The bounded destinations driven by the owning process's IO.
    pub fn sinks(&mut self) -> &mut Sinks {
        &mut self.sinks
    }

    /// A typed live-view record awaits its product, independently of destination work.
    #[must_use]
    pub fn front_pending(&self) -> bool {
        self.sinks.front_pending()
    }

    /// Whether a complete destination can take another parent observation.
    #[must_use]
    pub fn takes(&self) -> bool {
        self.sinks.takes()
    }

    /// Live destination streams, for lower settlement excluding the trace itself.
    #[must_use]
    pub fn streams(&self) -> u32 {
        self.sinks.open_streams()
    }

    /// Whether a destination can send or close in the next bounded pass.
    #[must_use]
    pub fn has_work(&self) -> bool {
        self.sinks.has_work()
    }

    /// Whether this IO owner belongs to one of the destinations.
    #[must_use]
    pub fn owns(&self, owner: Token) -> bool {
        self.sinks.owns(owner)
    }

    #[must_use]
    pub fn configured(config: &crate::Config) -> Option<Producer> {
        match config.limits.events {
            Some(_) => {
                let mut producer = Producer::new(
                    &config.limits,
                    config.channel_endpoints.clone(),
                    &config.llm_endpoints,
                    &config.domain.endpoints,
                );
                producer.programs = match crate::service::bound(config.program.as_ref()) {
                    Some(program) => program.endpoints.clone(),
                    None => Box::default(),
                };
                Some(producer)
            }
            None => None,
        }
    }

    pub(crate) fn new(
        limits: &crate::Limits,
        names: channel::Endpoints,
        _endpoints: &llm::Endpoints,
        _declared: &[run::charter::Endpoint],
    ) -> Producer {
        let events = limits.events.expect("event configuration exists");
        Producer {
            sinks: Sinks::with_front(
                &events.sinks,
                events.capture,
                limits.io.output,
                events.write_deadline,
                events.front,
            ),
            names,
            programs: Box::default(),
            calls: crate::calls::Book::new(&limits.domain.session, events.capture),
            messages: crate::messages::Book::new(limits, events.capture),
            capture: events.capture,
            origin: None,
            start: None,
            admitted: None,
            admitted_at: None,
            activation: None,
            answer_at: None,
            overflow_notice: None,
            wind_down: false,
            unread: 0,
            check_at: None,
        }
    }

    pub fn started(&mut self, now: Time, header: smith_events::SessionStarted) {
        assert!(self.origin.is_none(), "one process startup before its first pass");
        self.origin = Some(now);
        self.sinks.record(&Record { t_ms: 0, event: Event::SessionStarted(header) });
        self.notice(
            now,
            smith_events::Notice {
                code: None,
                level: smith_events::Level::Warning,
                kind: smith_events::NoticeKind::Unknown(b"not_contained".as_slice().into()),
                run: None,
                account: None,
                reset_ms: None,
                bytes: None,
                exit: None,
                forced: None,
            },
        );
    }

    pub(crate) fn message(&mut self, name: Token, label: &[u8], text: &[u8]) {
        self.messages.received(name, label, text);
    }

    pub fn starting(&mut self, activation: u64, messages: u64, charter: &run::Charter) {
        assert!(self.activation.is_none() || self.answer_at.is_some(), "prior run has answered");
        self.sinks.starting();
        self.activation = Some(activation);
        self.admitted = None;
        self.admitted_at = None;
        self.answer_at = None;
        self.wind_down = false;
        self.unread = 0;
        self.check_at = None;
        let mut families = List::with_capacity(4);
        for (allowed, family) in [
            (charter.grants.tools.inspect, smith_events::Family::Inspect),
            (charter.grants.tools.modify, smith_events::Family::Modify),
            (charter.grants.tools.shell, smith_events::Family::Shell),
            (charter.grants.agents, smith_events::Family::SubAgents),
        ] {
            if allowed {
                families.push(family).expect("four tool families");
            }
        }
        let mut host =
            List::with_capacity(u32::try_from(charter.grants.host_tools.len()).expect("bounded host declarations"));
        for tool in &charter.grants.host_tools {
            host.push(tool.name.clone()).expect("admitted host names");
        }
        let mut withheld = List::with_capacity(1);
        if charter.grants.agents && self.programs.contains(&charter.llm.endpoint.0) {
            withheld.push(smith_events::Family::SubAgents).expect("one withheld family");
        }
        let mut contract = List::with_capacity(4);
        if charter.outcome.change.is_some() {
            contract.push(smith_events::Form::Change).expect("four forms");
        }
        if !charter.outcome.verdicts.is_empty() {
            contract.push(smith_events::Form::Verdict).expect("four forms");
        }
        if charter.outcome.report.is_some() {
            contract.push(smith_events::Form::Report).expect("four forms");
        }
        if charter.outcome.failure.is_some() {
            contract.push(smith_events::Form::Failure).expect("four forms");
        }
        self.start = Some(Start {
            messages,
            llm: charter.llm.clone(),
            budget: smith_events::Budget {
                turns: u64::from(charter.budget.turns),
                spend: charter.budget.spend,
                time_ms: charter.budget.time.as_nanos() / 1_000_000,
                reserve: smith_events::Reserve {
                    turns: u64::from(charter.reserve.turns),
                    time_ms: charter.reserve.time.as_nanos() / 1_000_000,
                    spend: run::reserve_spend(charter.reserve, &charter.llm).unwrap_or(0),
                },
            },
            tools: smith_events::Tools {
                withheld,
                families,
                wait: charter.grants.wait,
                deliver: charter.grants.deliver.is_some(),
                host,
            },
            contract,
        });
    }

    pub(crate) fn pop_overflow_notice(&mut self) -> Option<run::Overflow> {
        self.overflow_notice.take()
    }

    #[must_use]
    pub fn answer_at(&self) -> Option<Time> {
        self.answer_at
    }

    #[must_use]
    pub fn origin(&self) -> Time {
        self.origin.expect("session.started precedes the first service pass")
    }

    #[expect(clippy::too_many_lines, reason = "one exhaustive content-free root fact projection")]
    pub(crate) fn fact(&mut self, fact: &domain::Fact) {
        let origin = self.origin();
        let (fact, opening) = match fact {
            domain::Fact::Session { fact } => {
                self.calls.fact(origin, fact, &mut self.sinks);
                self.sinks.session(origin, fact);
                return;
            }
            domain::Fact::Run { fact, opening } => (fact, opening),
        };
        let event = match fact.kind {
            run::facts::FactKind::Admitted { run: _, resumed } => {
                self.admitted = Some(self.activation());
                self.admitted_at = Some(fact.at);
                let start = self.start.take().expect("actual Start retained before its admission");
                Event::RunStarted(smith_events::RunStarted {
                    run: self.activation(),
                    resumed,
                    messages: start.messages,
                    main: self.model(&start.llm),
                    budget: start.budget,
                    tools: start.tools,
                    contract: start.contract,
                })
            }
            run::facts::FactKind::Opened { run: _, conversation, kind } => {
                let opening = opening.as_ref().expect("actual Run::Open metadata accompanied the fact");
                assert_eq!(opening.activation, self.activation(), "opening carries this actual Start activation");
                let number = self.sinks.next_conversation(self.activation());
                let (kind, parent, call, requested, share, clamped) = match kind {
                    run::facts::Kind::Main => {
                        (smith_events::ConversationKind::Main, None, None, None, None, List::with_capacity(0))
                    }
                    run::facts::Kind::Child { parent, call, granted, .. } => {
                        let parent = self.sinks.scope(parent).expect("actual parent conversation precedes child");
                        let id = self.calls.child_opened(conversation, number, call, parent, granted, fact.at);
                        let share = crate::child_share(kind).expect("actual child share");
                        (
                            smith_events::ConversationKind::Child,
                            Some(parent.conversation),
                            Some(id),
                            Some(share.requested),
                            Some(share.share),
                            share.clamped,
                        )
                    }
                };
                self.sinks.conversation(
                    fact.at,
                    origin,
                    conversation,
                    smith_events::ConversationOpened {
                        run: self.activation(),
                        conversation: number,
                        kind,
                        parent,
                        call,
                        thread: u64::from(opening.ordinal),
                        model: self.model(&opening.llm),
                        requested,
                        share,
                        clamped,
                    },
                );
                return;
            }
            run::facts::FactKind::Ended { conversation, end, .. } => {
                self.calls.child_ended(conversation, end, fact.at);
                self.sinks.conversation_ended(fact.at, conversation, end);
                return;
            }
            run::facts::FactKind::Called { conversation, call, ask, .. } => {
                let scope = self.sinks.scope(conversation).expect("actual asker conversation remains alive");
                self.calls.called(call, scope, ask);
                return;
            }
            run::facts::FactKind::Returned { call, result, .. } => {
                self.calls.returned(call, result);
                return;
            }
            run::facts::FactKind::Delivered { status, .. } => {
                self.calls.delivered(status);
                return;
            }
            run::facts::FactKind::Budget { standing, call, .. } => {
                self.wind_down = match standing.stage {
                    run::Stage::Ordinary => false,
                    run::Stage::WindDown => true,
                };
                let call = match call {
                    Some(owner) => Some(self.calls.id(owner)),
                    None => None,
                };
                self.sinks
                    .record(&crate::budget_record(fact, origin, self.activation(), call).expect("matched budget fact"));
                return;
            }
            run::facts::FactKind::MessageReceived { run: _, name, bytes } => self.message_received(name, bytes, None),
            run::facts::FactKind::MessageRefused { run: _, name, bytes, reason } => {
                self.message_received(name, bytes, Some(message_refusal(reason)))
            }
            run::facts::FactKind::MessageRead { run: _, name, turn } => Event::MessageRead(smith_events::MessageRead {
                run: self.activation(),
                name: name.raw(),
                turn: u64::from(turn),
            }),
            run::facts::FactKind::MessageUnread { run: _, name } => {
                self.unread = self.unread.checked_add(1).expect("bounded unread messages");
                Event::MessageUnread(smith_events::MessageUnread { run: self.activation(), name: name.raw() })
            }
            run::facts::FactKind::CheckStarted { run: _, deadline } => {
                self.check_at = Some(fact.at);
                Event::CheckStarted(smith_events::CheckStarted {
                    run: self.activation(),
                    deadline_ms: Some(deadline.saturating_since(fact.at).as_nanos() / 1_000_000),
                })
            }
            run::facts::FactKind::CheckFinished { run: _, exit } => {
                let (exit, passed) = crate::check_exit(exit);
                Event::CheckCompleted(smith_events::CheckCompleted {
                    run: self.activation(),
                    passed,
                    exit,
                    duration_ms: fact.at.saturating_since(self.check_at.take().expect("one check start")).as_nanos()
                        / 1_000_000,
                })
            }
            run::facts::FactKind::MessageFence { .. }
            | run::facts::FactKind::Prepared { .. }
            | run::facts::FactKind::Answered { .. } => return,
        };
        self.sinks.record(&Record { t_ms: fact.at.saturating_since(origin).as_nanos() / 1_000_000, event });
    }

    fn activation(&self) -> u64 {
        self.activation.expect("actual decoded Start precedes run observations")
    }

    fn model(&self, model: &run::charter::Llm) -> smith_events::Model {
        let name =
            self.names.name_of(model.endpoint.0, model.dialect).expect("admitted endpoint has its configured name");
        let backend = if self.programs.contains(&model.endpoint.0) {
            smith_events::Backend::ClaudeCode
        } else {
            smith_events::Backend::Direct
        };
        crate::opening_model(model, name, model.effort.as_deref(), backend)
    }

    pub(crate) fn received(&mut self, owner: Token, completion: &domain::llm::Completion) {
        self.calls.child_received(owner, completion.usage);
        self.calls.received(owner, completion);
    }

    pub(crate) fn request(
        &mut self,
        now: Time,
        owner: Token,
        prompt: &domain::llm::Prompt,
    ) -> Option<smith_protocol_llm::Observation> {
        self.sinks.request(now, self.origin(), owner, prompt)
    }

    fn message_received(&mut self, name: Token, bytes: u64, refused: Option<smith_events::MessageRefusal>) -> Event {
        let (label, text) = match self.messages.take(name) {
            Some(message) => (Some(message.label), Some(message.text)),
            None => (None, None),
        };
        Event::MessageReceived(smith_events::MessageReceived {
            run: self.activation(),
            name: name.raw(),
            bytes,
            refused,
            label,
            text,
        })
    }

    pub(crate) fn answer(&mut self, now: Time, started: Time, answer: &run::Answer, read: Option<Token>) {
        self.messages.clear();
        assert!(self.answer_at.is_none(), "one actual answer per invocation");
        self.answer_at = Some(now);
        let mut record = crate::answer_record(
            crate::AnswerMeta {
                now,
                run: self.activation(),
                wind_down: self.wind_down,
                read: match read {
                    Some(read) => Some(read.raw()),
                    None => None,
                },
                unread: self.unread,
                elapsed: now.saturating_since(started),
            },
            answer,
            self.capture,
        );
        record.usage = self.sinks.reported();
        self.sinks.answer_failure(&mut record);
        self.overflow_notice = match answer {
            run::Answer::Failed { failure, .. } => match failure {
                run::Failure::Budget { exhausted, .. } => match exhausted {
                    run::Exhausted::Overflow(overflow) => Some(*overflow),
                    run::Exhausted::Turns
                    | run::Exhausted::Spend
                    | run::Exhausted::Time
                    | run::Exhausted::Tokens(_) => None,
                },
                run::Failure::Model(_)
                | run::Failure::Policy(_)
                | run::Failure::Cancelled
                | run::Failure::Stale
                | run::Failure::Transcript(_) => None,
            },
            run::Answer::Accepted { .. } | run::Answer::Parked { .. } | run::Answer::Refused(_) => None,
        };
        self.sinks.record(&Record {
            t_ms: now.saturating_since(self.origin()).as_nanos() / 1_000_000,
            event: Event::RunCompleted(record),
        });
    }

    pub fn answered(&mut self, now: Time, started: Time, answer: crate::Answered) {
        let unanswered = match &answer.failure {
            Some(failure) => {
                failure.dropped == Some(true)
                    || failure.class == smith_events::AnswerClass::Host
                    || failure.class == smith_events::AnswerClass::Agent
            }
            None => false,
        };
        self.messages.clear();
        assert!(self.answer_at.is_none(), "one actual answer per run");
        self.answer_at = Some(now);
        let mut record = smith_events::RunCompleted {
            run: self.activation(),
            wind_down: self.wind_down && !unanswered,
            status: answer.status,
            failure: answer.failure,
            result: answer.result,
            turns: answer.turns,
            spent: answer.spent,
            read: answer.read,
            unread: self.unread,
            usage: self.sinks.reported(),
            duration_ms: now.saturating_since(self.admitted_at.unwrap_or(started)).as_nanos() / 1_000_000,
        };
        self.sinks.answer_failure(&mut record);
        self.sinks.record(&Record {
            t_ms: now.saturating_since(self.origin()).as_nanos() / 1_000_000,
            event: Event::RunCompleted(record),
        });
    }

    /// Write an operator notice, attaching the run observed by this writer.
    pub fn notice(&mut self, now: Time, mut notice: smith_events::Notice) {
        notice.run = match notice.kind {
            smith_events::NoticeKind::ForcedExit => self.activation,
            smith_events::NoticeKind::ReasoningDropped
            | smith_events::NoticeKind::CredentialUnauthorized
            | smith_events::NoticeKind::AccountLimited
            | smith_events::NoticeKind::AccountRefused
            | smith_events::NoticeKind::TeardownAborted
            | smith_events::NoticeKind::Unknown(_) => self.admitted,
        };
        self.sinks.record(&Record {
            t_ms: now.saturating_since(self.origin()).as_nanos() / 1_000_000,
            event: Event::Notice(notice),
        });
    }
}

fn message_refusal(reason: run::MessageRefusal) -> smith_events::MessageRefusal {
    match reason {
        run::MessageRefusal::TooLarge => smith_events::MessageRefusal::TooLarge,
        run::MessageRefusal::Full => smith_events::MessageRefusal::InboxFull,
        run::MessageRefusal::NameInUse => smith_events::MessageRefusal::NameInUse,
        run::MessageRefusal::Ending => smith_events::MessageRefusal::Ending,
    }
}

#[must_use]
pub fn worst_case(limits: &crate::Limits) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let events = limits.events?;
    // The component moves each observed completion into the response join.
    // It stays there until the corresponding accounting/terminal fact, and
    // moves into the terminal record without a second payload copy.
    let observed = if events.front { Capture::Everything } else { events.capture };
    let completions = match observed {
        Capture::None | Capture::Calls => 0,
        Capture::Everything => u64::from(events.sinks.conversations).checked_mul(
            List::<smith_events::Block>::worst_case(limits.llm.adapter.client.output_items)?
                .checked_add(u64::from(limits.llm.adapter.client.answer))?,
        )?,
    };
    crate::sinks::sinks_worst_case(&events.sinks, events.capture)?
        .checked_add(if events.front { front_worst_case(limits)? } else { 0 })?
        .checked_add(completions)?
        .checked_add(crate::messages::worst_case(limits, events.capture)?)?
        .checked_add(crate::calls::worst_case(&limits.domain.session, events.sinks.records.string)?)?
        .checked_add(crate::inputs::worst_case(&limits.domain.session, events.capture)?)?
        .checked_add(channel::Endpoints::worst_case(limits.channel.endpoints, limits.channel.charter.llm_endpoint)?)?
        .checked_add(Map::<u32, Box<[u8]>>::worst_case(limits.channel.endpoints)?)?
        .checked_add(u64::from(limits.channel.endpoints).checked_mul(u64::from(events.sinks.metadata_string))?)?
        .checked_add(List::<smith_events::Family>::worst_case(4)?)?
        .checked_add(List::<Box<[u8]>>::worst_case(limits.domain.run.host_tools)?)?
        .checked_add(List::<smith_events::Form>::worst_case(4)?)?
        .checked_add(
            u64::from(limits.domain.run.host_tools.checked_add(1)?)
                .checked_mul(u64::from(events.sinks.metadata_string))?,
        )
}

/// Whether configured event sinks and joins fit (programming-model.md, 6.3).
#[must_use]
pub fn valid(limits: &crate::Limits) -> bool {
    match limits.events {
        Some(events) => {
            crate::sinks::valid(&events.sinks, events.capture)
                && crate::messages::valid(limits, events.capture)
                && crate::calls::valid(&limits.domain.session)
        }
        None => true,
    }
}

/// The live queue owns exactly its slots times one projection's bound.
#[must_use]
pub fn front_worst_case(limits: &crate::Limits) -> Option<u64> {
    limits.events?;
    let client = limits.llm.adapter.client;
    crate::sinks::View::worst_case(client.answer.max(client.strings))?
        .checked_mul(u64::from(crate::sinks::FRONT_RECORDS))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods,
        reason = "programming-model.md section10.2: bounded deterministic source fixtures use ordinary Rust"
    )]

    use super::*;
    use crate::responses::tests::{actual_completion, at, fact, opened, prompt};
    use crate::sinks::tests::{drain_ready, limits};
    use smith_domain::session;

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "two independent raw-accounting terminal controls exercise the complete producer and wire"
    )]
    fn refused_raw_totals_and_overflow_reach_the_wire_answer_and_operator_once() {
        for overflow in [false, true] {
            let bounds = limits(1);
            let destination = Token::new(400);
            let owner = Token::new(11);
            let mut producer = Producer {
                sinks: Sinks::new(&bounds, Capture::Calls, bounds.output, crate::sinks::tests::DEADLINE),
                names: channel::Endpoints::new(List::with_capacity(0)),
                programs: Box::default(),
                calls: crate::calls::Book::new(&smith_agent_world::LIMITS.session, Capture::Calls),
                messages: crate::messages::Book::disabled(),
                capture: Capture::Calls,
                origin: Some(Time::ZERO),
                start: None,
                admitted: Some(7),
                admitted_at: None,
                activation: Some(7),
                answer_at: None,
                overflow_notice: None,
                wind_down: false,
                unread: 0,
                check_at: None,
            };
            producer
                .sinks
                .add(crate::sinks::Destination::Trace, destination, crate::sinks::Delivery::Complete)
                .unwrap();
            producer.sinks.conversation(at(1), Time::ZERO, owner, opened(11, 0));
            drop(drain_ready(&mut producer.sinks, destination));
            for turn in 1..=if overflow { 2 } else { 1 } {
                let mut completion = actual_completion();
                if overflow {
                    completion.usage.input_tokens = Some(if turn == 1 { u64::MAX - 1 } else { 2 });
                }
                producer.sinks.session(
                    Time::ZERO,
                    &fact(
                        2,
                        session::FactKind::CompletionStarted {
                            window: 1,
                            opener: owner,
                            attempt: 0,
                            messages: 0,
                            max_tokens: 7,
                            choice: session::Choice::Only,
                        },
                        turn,
                        None,
                    ),
                );
                producer.sinks.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
                drop(drain_ready(&mut producer.sinks, destination));
                let actual_owner = producer.sinks.received(Token::new(99), &completion);
                producer.received(actual_owner, &completion);
                producer.sinks.session(
                    Time::ZERO,
                    &fact(
                        5,
                        session::FactKind::CompletionAnswered {
                            opener: owner,
                            stop: completion.stop,
                            blocks: 2,
                            calls: 2,
                            invalid: 2,
                        },
                        turn,
                        None,
                    ),
                );
                let end = if overflow && turn == 1 {
                    session::FactKind::Used { opener: owner, usage: completion.usage }
                } else {
                    session::FactKind::Ended {
                        opener: owner,
                        end: if overflow { session::End::UsageOverflow } else { session::End::PriceOverflow },
                        turns: u32::from(overflow),
                        usage: domain::llm::Usage::ZERO,
                    }
                };
                producer
                    .sinks
                    .session(Time::ZERO, &fact(5, end, turn, if overflow && turn == 1 { Some(0) } else { None }));
                let records = drain_ready(&mut producer.sinks, destination);
                if !(overflow && turn == 1) {
                    let Event::ConversationClosed(closed) = &records.last().unwrap().event else {
                        panic!("actual conversation terminal")
                    };
                    assert_eq!(closed.usage.input_tokens, if overflow { None } else { Some(17) });
                    assert_eq!(closed.usage.output_tokens, Some(if overflow { 38 } else { 19 }));
                }
            }
            let cause = if overflow { run::Overflow::Usage } else { run::Overflow::Spend };
            producer.answer(
                at(7),
                at(1),
                &run::Answer::Failed {
                    failure: run::Failure::Budget {
                        exhausted: run::Exhausted::Overflow(cause),
                        used: None,
                        bound: None,
                    },
                    spent: run::Spend::ZERO,
                    turns: u32::from(overflow),
                },
                None,
            );
            let records = drain_ready(&mut producer.sinks, destination);
            let Event::RunCompleted(answer) = &records[0].event else { panic!("actual wire answer") };
            assert_eq!(answer.usage.input_tokens, if overflow { None } else { Some(17) });
            assert_eq!(answer.usage.output_tokens, Some(if overflow { 38 } else { 19 }));
            assert_eq!(answer.usage.cache_read_tokens, None);
            assert_eq!(answer.usage.cache_write_tokens, Some(0));
            assert_eq!(answer.usage.reasoning_tokens, Some(if overflow { 6 } else { 3 }));
            assert_eq!(
                answer.failure.as_ref().unwrap().which.as_deref(),
                Some(if overflow { b"usage_overflow".as_slice() } else { b"spend_overflow".as_slice() })
            );
            assert_eq!(producer.pop_overflow_notice(), Some(cause));
            assert_eq!(producer.pop_overflow_notice(), None);
        }
    }
}

#[cfg(test)]
#[path = "tests/program.rs"]
mod program_tests;
