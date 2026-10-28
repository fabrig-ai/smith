//! Bounded joins between logical session facts and actual LLM hand-offs.
//! Each live conversation keeps its opening model, current intent and at most
//! one provider attempt and one temporary compaction scope. Actual hand-offs
//! retain their public conversation number until their terminal. Intents can be
//! revised before hand-off; only `request`
//! assigns a run-wide response number. Raw usage is counted before accounting;
//! rejected reports remain in nullable totals, with sticky per-field overflow.
//! A content cursor skips already recorded completions; original completion
//! blocks and observed transport measures wait for their terminal. Unknown
//! measures remain absent, and observed zero durations remain zero. No provider
//! replay bytes are retained. Typed diagnostics join the actual request terminal,
//! then its conversation and failed main answer; child diagnostics never supply
//! the main failure. Capture none keeps no provider diagnostic content.
//! Contracts: protocol/events.md, sections 3.4–3.5, 3.10–3.11 and 4.

#![expect(
    clippy::manual_let_else,
    clippy::manual_map,
    reason = "the strict subset keeps bounded entries explicit and uses no closures"
)]

mod retry;

use alloc::boxed::Box;

use skein_lib::{List, Map, Time, Token};
use smith_domain::{llm, run, session};
use smith_events::{
    CompletionFailure, ConversationClosed, ConversationEnd, ConversationOpened, Event, Model, Outcome, Record,
    ResponseCompleted, ResponseStarted, Stop, Usage,
};

/// Public stream identity of one actual internal conversation owner.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Scope {
    pub(crate) run: u64,
    pub(crate) conversation: u64,
}

/// A bounded response join table, owned by the event sink.
#[derive(Debug)]
pub(crate) struct Book {
    conversations: Map<Token, Conversation>,
    next: u64,
    reported: crate::observations::Reported,
    run: Option<u64>,
    next_conversation: u64,
    main_failure: Option<RunFailure>,
}

#[derive(Debug)]
struct RunFailure {
    run: u64,
    model: Model,
    attempts: u64,
    failure: CompletionFailure,
}

#[derive(Debug)]
struct Conversation {
    run: u64,
    number: u64,
    thread: u64,
    model: Model,
    opened: Time,
    intent: Option<Intent>,
    attempt: Attempt,
    spent: u64,
    reported: crate::observations::Reported,
    admitted: bool,
    root_end: Option<(Time, run::facts::EndKind)>,
    recorded: Option<u32>,
    main: bool,
    attempts: u64,
    last_failure: Option<CompletionFailure>,
    compaction: Option<Compaction>,
}

#[derive(Debug)]
struct Compaction {
    charter_model: Model,
    number: u64,
    at: Time,
    usage: crate::observations::Reported,
    spent: u64,
    turns: u32,
}

#[derive(Clone, Copy, Debug)]
struct Intent {
    turn: u64,
    retry: u32,
    window: u32,
}

#[derive(Debug)]
struct Started {
    opening: retry::Opening,
    number: u64,
    conversation: u64,
    at: Time,
    owner: Token,
    observed: bool,
    received: Option<Received>,
    completion: Option<smith_events::Completion>,
    measures: Option<smith_protocol_llm::Measures>,
    failure: Option<CompletionFailure>,
}

#[derive(Clone, Copy, Debug)]
struct Received {
    usage: llm::Usage,
    oversized: u64,
    cut: u64,
    blocks: u64,
}

/// The actual hand-off's one terminal, completed only after accepted accounting.
#[derive(Debug)]
enum Attempt {
    /// No provider request was handed off, or its terminal was already recorded.
    Idle,
    /// A request was handed to the LLM component and owes one terminal.
    Started(Started),
    /// The completion answered; its accepted Used or end fact still follows.
    Answered { started: Started, at: Time, stop: Stop, blocks: u64, calls: u64, invalid: u64 },
    /// The failure's retry decision or session end still follows.
    Failed { started: Started, at: Time, failure: CompletionFailure },
}

impl Book {
    pub(crate) fn new(conversations: u32) -> Book {
        Book {
            conversations: Map::with_capacity(conversations),
            next: 1,
            reported: crate::observations::Reported::NONE,
            main_failure: None,
            run: None,
            next_conversation: 0,
        }
    }

    pub(crate) fn next_conversation(&mut self, run: u64) -> u64 {
        if self.run != Some(run) {
            self.run = Some(run);
            self.next_conversation = 0;
        }
        let number = self.next_conversation;
        self.next_conversation = number.checked_add(1).expect("conversation names cannot exhaust");
        number
    }

    pub(crate) fn opened(&mut self, at: Time, owner: Token, opening: &ConversationOpened) {
        self.run = Some(opening.run);
        self.next_conversation =
            self.next_conversation.max(opening.conversation.checked_add(1).expect("bounded names"));
        let old = self
            .conversations
            .insert(
                owner,
                Conversation {
                    run: opening.run,
                    number: opening.conversation,
                    thread: opening.thread,
                    model: opening.model.clone(),
                    opened: at,
                    intent: None,
                    attempt: Attempt::Idle,
                    spent: 0,
                    reported: crate::observations::Reported::NONE,
                    admitted: false,
                    root_end: None,
                    recorded: None,
                    main: match opening.kind {
                        smith_events::ConversationKind::Main => true,
                        smith_events::ConversationKind::Child
                        | smith_events::ConversationKind::Compaction
                        | smith_events::ConversationKind::Unknown(_) => false,
                    },
                    attempts: 0,
                    last_failure: None,
                    compaction: None,
                },
            )
            .expect("one join row per bounded live conversation");
        assert!(old.is_none(), "each conversation opens once");
    }

    pub(crate) fn starting(&mut self) {
        assert!(self.conversations.is_empty(), "prior run's conversations settled");
        self.next = 1;
        self.run = None;
        self.next_conversation = 0;
        self.reported = crate::observations::Reported::NONE;
        self.main_failure = None;
    }

    pub(crate) fn scope(&self, owner: Token) -> Option<Scope> {
        let conversation = self.conversations.get(&owner)?;
        Some(Scope { run: conversation.run, conversation: conversation.number })
    }

    pub(crate) fn request(&mut self, now: Time, origin: Time, request_owner: Token, prompt: &llm::Prompt) -> Record {
        let mut owner = None;
        for (name, conversation) in &self.conversations {
            if conversation.thread == u64::from(prompt.affinity.thread) {
                assert!(owner.is_none(), "one live conversation per actual affinity ordinal");
                owner = Some(*name);
            }
        }
        let owner = owner.expect("the opening fact is drained before its actual request");
        let conversation = self.conversations.get_mut(&owner).expect("found above");
        assert_eq!(
            conversation.model.model.as_ref(),
            prompt.model.as_ref(),
            "the actual request uses its opening's model"
        );
        match &conversation.attempt {
            Attempt::Idle => {}
            Attempt::Started(_) | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("a conversation hands off one provider request at a time")
            }
        }
        let intent = conversation.intent.take().expect("each actual request has a surviving logical intent");
        let number = self.next;
        self.next = self.next.checked_add(1).expect("a process cannot exhaust response names");
        conversation.attempts = u64::from(intent.retry).checked_add(1).expect("u32 retry count plus one fits u64");
        conversation.last_failure = None;
        let response_conversation = match &conversation.compaction {
            Some(compaction) => compaction.number,
            None => conversation.number,
        };
        conversation.attempt = Attempt::Started(Started {
            opening: retry::Opening {
                intent,
                output_tokens: u64::from(prompt.max_tokens),
                tool_choice: crate::request_tool_choice(&prompt.choice),
            },
            number,
            conversation: response_conversation,
            at: now,
            owner: request_owner,
            observed: false,
            received: None,
            completion: None,
            measures: None,
            failure: None,
        });
        Record {
            t_ms: millis(now, origin),
            event: Event::ResponseStarted(ResponseStarted {
                run: conversation.run,
                conversation: response_conversation,
                response: number,
                turn: match &conversation.compaction {
                    Some(_) => 1,
                    None => intent.turn,
                },
                attempt: u64::from(intent.retry),
                window: u64::from(intent.window),
                model: conversation.model.clone(),
                messages: u64::try_from(prompt.messages.len()).expect("admitted actual prompt messages"),
                output_tokens: u64::from(prompt.max_tokens),
                tool_choice: crate::request_tool_choice(&prompt.choice),
                prompt: None,
            }),
        }
    }

    fn owner_for(&self, request_owner: Token) -> Option<Token> {
        for (owner, conversation) in &self.conversations {
            let started = match &conversation.attempt {
                Attempt::Idle => continue,
                Attempt::Started(started) | Attempt::Answered { started, .. } | Attempt::Failed { started, .. } => {
                    started
                }
            };
            if started.owner == request_owner {
                return Some(*owner);
            }
        }
        None
    }

    pub(crate) fn failed(
        &mut self,
        request_owner: Token,
        failure: llm::Failure,
        evidence: llm::Evidence,
        detail: &[u8],
        capture: crate::sinks::Capture,
        report: &llm::Report,
    ) {
        let owner = self.owner_for(request_owner).expect("failure belongs to an actual request");
        let conversation = self.conversations.get_mut(&owner).expect("found above");
        let mut observed = completion_failure(failure, evidence);
        observed.field = provider_field(&report.field);
        observed.code = provider_field(&report.code);
        observed.request_id = provider_field(&report.request);
        observed.detail = match capture {
            crate::sinks::Capture::None => None,
            crate::sinks::Capture::Calls | crate::sinks::Capture::Everything => Some(detail.into()),
        };
        match &mut conversation.attempt {
            Attempt::Started(started) => {
                assert!(started.failure.is_none(), "one actual failure observation per terminal");
                started.failure = Some(observed);
            }
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("actual failure precedes its domain fact")
            }
        }
    }

    pub(crate) fn answer_failure(&mut self, record: &mut smith_events::RunCompleted) {
        let observed = match self.main_failure.take() {
            Some(observed) => observed,
            None => return,
        };
        if observed.run != record.run {
            return;
        }
        let failure = match &mut record.failure {
            Some(failure) => failure,
            None => return,
        };
        match &failure.class {
            smith_events::AnswerClass::Model => {}
            smith_events::AnswerClass::Budget
            | smith_events::AnswerClass::Policy
            | smith_events::AnswerClass::Cancelled
            | smith_events::AnswerClass::Agent
            | smith_events::AnswerClass::Host
            | smith_events::AnswerClass::Stale
            | smith_events::AnswerClass::Transcript
            | smith_events::AnswerClass::Busy
            | smith_events::AnswerClass::Invalid
            | smith_events::AnswerClass::Unknown(_) => return,
        }
        let completion = match &failure.completion {
            Some(completion) => completion,
            None => return,
        };
        assert_eq!(completion.class, observed.failure.class, "the actual main terminal supplies the failed answer");
        assert_eq!(
            completion.evidence, observed.failure.evidence,
            "the main answer agrees with its actual transport evidence"
        );
        failure.endpoint = Some(observed.model.endpoint);
        failure.model = Some(observed.model.model);
        failure.attempts = Some(observed.attempts);
        failure.completion = Some(observed.failure);
    }

    pub(crate) fn observation(&mut self, request_owner: Token) -> smith_protocol_llm::Observation {
        let owner = self.owner_for(request_owner).expect("actual request retains its conversation");
        let conversation = self.conversations.get_mut(&owner).expect("found above");
        if let Attempt::Started(started) = &mut conversation.attempt {
            started.observed = true;
        }
        match conversation.recorded {
            Some(messages) => smith_protocol_llm::Observation { first: false, messages },
            None => smith_protocol_llm::Observation { first: true, messages: 0 },
        }
    }

    pub(crate) fn observed(&self, request_owner: Token) -> bool {
        let Some(owner) = self.owner_for(request_owner) else {
            return false;
        };
        match &self.conversations.get(&owner).expect("found above").attempt {
            Attempt::Started(started) => started.observed,
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => false,
        }
    }

    pub(crate) fn prompt_recorded(&mut self, request_owner: Token, messages: u32) {
        let owner = self.owner_for(request_owner).expect("prompt belongs to an actual request");
        self.conversations.get_mut(&owner).expect("found above").recorded = Some(messages);
    }

    pub(crate) fn completion(&mut self, request_owner: Token, completion: smith_events::Completion) {
        let owner = self.owner_for(request_owner).expect("completion belongs to an actual request");
        let conversation = self.conversations.get_mut(&owner).expect("found above");
        let started = match &mut conversation.attempt {
            Attempt::Started(started) => started,
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("capture precedes the domain terminal")
            }
        };
        assert!(started.completion.is_none(), "one observed completion per actual request");
        started.completion = Some(completion);
    }

    pub(crate) fn measured(&mut self, request_owner: Token, measures: smith_protocol_llm::Measures) {
        let owner = self.owner_for(request_owner).expect("transport measures belong to an actual request");
        let conversation = self.conversations.get_mut(&owner).expect("found above");
        let started = match &mut conversation.attempt {
            Attempt::Started(started) => started,
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("transport measures precede the domain terminal")
            }
        };
        assert!(started.measures.is_none(), "one connection terminal per actual request");
        started.measures = Some(measures);
    }

    pub(crate) fn delta(&self, at: Time, origin: Time, request_owner: Token, block: u32, text: Box<[u8]>) -> Record {
        let owner = self.owner_for(request_owner).expect("delta belongs to an actual request");
        let conversation = self.conversations.get(&owner).expect("found above");
        let (response, response_conversation) = match &conversation.attempt {
            Attempt::Started(started) => (started.number, started.conversation),
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("deltas precede the domain terminal")
            }
        };
        Record {
            t_ms: millis(at, origin),
            event: Event::TextDelta(smith_events::TextDelta {
                run: conversation.run,
                conversation: response_conversation,
                response,
                block: u64::from(block),
                text,
            }),
        }
    }

    pub(crate) fn received(&mut self, owner: Token, completion: &llm::Completion) -> Token {
        let mut found = None;
        for (name, conversation) in &self.conversations {
            match &conversation.attempt {
                Attempt::Started(started) if started.owner == owner => {
                    found = Some(*name);
                    break;
                }
                Attempt::Idle | Attempt::Started(_) | Attempt::Answered { .. } | Attempt::Failed { .. } => {}
            }
        }
        let conversation_owner = found.expect("typed answer retains its hand-off identity");
        let conversation = self.conversations.get_mut(&conversation_owner).expect("found above");
        conversation.reported.add(completion.usage);
        if let Some(compaction) = &mut conversation.compaction {
            compaction.usage.add(completion.usage);
        }
        self.reported.add(completion.usage);
        match &mut conversation.attempt {
            Attempt::Started(started) => {
                let (oversized, cut) = rejected_calls(completion);
                assert!(started.received.is_none(), "one typed component answer per actual hand-off");
                let blocks = u64::try_from(completion.content.len())
                    .expect("bounded blocks")
                    .checked_add(u64::try_from(completion.reasoning_dropped.len()).expect("bounded drops"))
                    .expect("bounded original completion blocks");
                started.received = Some(Received { usage: completion.usage, oversized, cut, blocks });
            }
            Attempt::Idle | Attempt::Answered { .. } | Attempt::Failed { .. } => {
                unreachable!("found active hand-off above")
            }
        }
        conversation.recorded = match conversation.recorded {
            Some(messages) => {
                Some(messages.checked_add(1).expect("the received assistant message fits admitted history"))
            }
            None => None,
        };
        conversation_owner
    }

    pub(crate) fn reported(&self) -> Usage {
        self.reported.usage()
    }

    pub(crate) fn root_ended(&mut self, at: Time, owner: Token, end: run::facts::EndKind) {
        // SessionEnded can have already closed a child in a previous drain.
        if let Some(conversation) = self.conversations.get_mut(&owner) {
            conversation.root_end = Some((at, end));
        }
    }

    /// Called only after the complete gathered fact batch has been drained.
    /// A root refusal before session admission still closes its opening once.
    pub(crate) fn facts_drained(&mut self, origin: Time) -> Option<Record> {
        let mut refused = None;
        for (owner, conversation) in &self.conversations {
            if !conversation.admitted && conversation.root_end.is_some() {
                refused = Some(*owner);
                break;
            }
        }
        let owner = refused?;
        let conversation = self.conversations.remove(&owner).expect("selected above");
        let (at, end) = conversation.root_end.expect("selected above");
        let end = match end {
            run::facts::EndKind::Busy | run::facts::EndKind::Invalid => ConversationEnd::Refused,
            run::facts::EndKind::TranscriptRefused { .. } => ConversationEnd::Transcript,
            run::facts::EndKind::PriceOverflow
            | run::facts::EndKind::UsageOverflow
            | run::facts::EndKind::Receiving(_)
            | run::facts::EndKind::Closed
            | run::facts::EndKind::Fault
            | run::facts::EndKind::Budget(_) => {
                unreachable!("other conversation ends follow a session admission or its Ended fact")
            }
        };
        Some(Record {
            t_ms: millis(at, origin),
            event: Event::ConversationClosed(ConversationClosed {
                run: conversation.run,
                conversation: conversation.number,
                end,
                which: None,
                failure: None,
                turns: 0,
                usage: conversation.reported.usage(),
                spent: 0,
                duration_ms: millis(at, conversation.opened),
            }),
        })
    }

    /// At most two records: an unanswered completion's terminal, then its conversation end.
    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive session-fact join; each entrance keeps at most two records"
    )]
    pub(crate) fn fact(&mut self, origin: Time, fact: &session::Fact) -> List<Record> {
        let mut records = List::with_capacity(2);
        match fact.kind.clone() {
            session::FactKind::CompactionStarted {
                opener, model, model_bytes, model_window, output, effort, ..
            } => {
                let run = self.conversations.get(&opener).expect("compaction follows opening").run;
                let number = self.next_conversation(run);
                let conversation = self.conversations.get_mut(&opener).expect("found above");
                if let Some(record) = terminal(conversation, origin, unknown_usage(), 0, None) {
                    records.push(record).expect("the prior failed response settles before the summary request");
                }
                assert!(conversation.compaction.is_none(), "one active compaction per conversation");
                let effort = match effort {
                    Some((bytes, length)) => Some(Box::from(
                        bytes.get(..usize::try_from(length).expect("bounded effort")).expect("admitted effort"),
                    )),
                    None => None,
                };
                let model = Model {
                    account: conversation.model.account,
                    backend: conversation.model.backend.clone(),
                    endpoint: conversation.model.endpoint.clone(),
                    model: Box::from(
                        model.get(..usize::try_from(model_bytes).expect("bounded model")).expect("admitted model"),
                    ),
                    window_tokens: u64::from(model_window),
                    output_tokens: u64::from(output),
                    effort,
                };
                let charter_model = core::mem::replace(&mut conversation.model, model);
                conversation.compaction = Some(Compaction {
                    charter_model,
                    number,
                    at: fact.at,
                    usage: crate::observations::Reported::NONE,
                    spent: 0,
                    turns: 0,
                });
                records
                    .push(Record {
                        t_ms: millis(fact.at, origin),
                        event: Event::ConversationOpened(ConversationOpened {
                            run,
                            conversation: number,
                            thread: conversation.thread,
                            model: conversation.model.clone(),
                            kind: smith_events::ConversationKind::Compaction,
                            parent: Some(conversation.number),
                            call: None,
                            requested: None,
                            share: None,
                            clamped: List::with_capacity(0),
                        }),
                    })
                    .expect("one compaction opening");
            }
            session::FactKind::CompactionEnded { opener, end, .. } => {
                let conversation = self.conversations.get_mut(&opener).expect("end follows opening");
                if let Some(record) = terminal(conversation, origin, unknown_usage(), 0, None) {
                    records.push(record).expect("the summary response settles before its conversation");
                }
                let compaction = conversation.compaction.take().expect("one compaction terminal");
                match end {
                    session::Compacted::Opened => {
                        conversation.recorded = None;
                        conversation.model = compaction.charter_model;
                    }
                    session::Compacted::Failed => {}
                }
                records
                    .push(Record {
                        t_ms: millis(fact.at, origin),
                        event: Event::ConversationClosed(ConversationClosed {
                            run: conversation.run,
                            conversation: compaction.number,
                            end: match end {
                                session::Compacted::Opened => ConversationEnd::Closed,
                                session::Compacted::Failed => ConversationEnd::Failed,
                            },
                            which: None,
                            failure: match end {
                                session::Compacted::Opened => None,
                                session::Compacted::Failed => conversation.last_failure.clone(),
                            },
                            turns: u64::from(compaction.turns),
                            usage: compaction.usage.usage(),
                            spent: compaction.spent,
                            duration_ms: millis(fact.at, compaction.at),
                        }),
                    })
                    .expect("one compaction terminal");
            }
            session::FactKind::Opened { opener } => {
                self.conversations.get_mut(&opener).expect("session admission follows actual opening").admitted = true;
            }
            session::FactKind::CompletionStarted { opener, attempt, window, .. } => {
                let turn = fact.response.expect("intent carries its actual session turn").number;
                let conversation = self.conversations.get_mut(&opener).expect("intent follows opening");
                conversation.intent = Some(Intent { turn, retry: attempt, window });
            }
            session::FactKind::CompletionAnswered { opener, stop, blocks, calls, invalid } => {
                let conversation = self.conversations.get_mut(&opener).expect("answer follows opening");
                let started = take_started(&mut conversation.attempt);
                match started {
                    Some(started) => {
                        conversation.attempt = Attempt::Answered {
                            started,
                            at: fact.at,
                            stop: stop_kind(stop),
                            blocks: u64::from(blocks),
                            calls: u64::from(calls),
                            invalid: u64::from(invalid),
                        }
                    }
                    None => unreachable!("a provider answer follows an actual hand-off"),
                }
            }
            session::FactKind::Used { opener, usage } => {
                let conversation = self.conversations.get_mut(&opener).expect("charge follows opening");
                let spent =
                    fact.response.expect("charge has response identity").spent.expect("Used has the accepted charge");
                conversation.spent = conversation
                    .spent
                    .checked_add(spent)
                    .expect("session accepted this representable cumulative spend");
                if let Some(compaction) = &mut conversation.compaction {
                    compaction.spent = spent;
                    compaction.turns = 1;
                }
                let record = terminal(conversation, origin, crate::reported_usage(usage), spent, None)
                    .expect("an accepted charge follows its pending answer");
                records.push(record).expect("one response terminal");
            }
            session::FactKind::CompletionFailed { opener, failure, evidence } => {
                let conversation = self.conversations.get_mut(&opener).expect("failure follows opening");
                let started = take_started(&mut conversation.attempt);
                match started {
                    Some(mut started) => {
                        let projected = completion_failure(failure, evidence);
                        let observed = match started.failure.take() {
                            Some(observed) => {
                                assert_eq!(
                                    observed.class, projected.class,
                                    "the domain fact preserves the actual failure class"
                                );
                                assert_eq!(
                                    observed.evidence, projected.evidence,
                                    "the domain fact preserves actual transport evidence"
                                );
                                observed
                            }
                            None => projected,
                        };
                        conversation.last_failure = Some(observed.clone());
                        conversation.attempt = Attempt::Failed { started, at: fact.at, failure: observed };
                    }
                    None => unreachable!("a component failure follows an actual hand-off"),
                }
            }
            session::FactKind::CompletionRetried { opener, delay, .. } => {
                let conversation = self.conversations.get_mut(&opener).expect("retry follows opening");
                let record = terminal(conversation, origin, unknown_usage(), 0, Some(delay.as_nanos() / 1_000_000))
                    .expect("a retry follows one pending actual failure");
                records.push(record).expect("one response terminal");
            }
            session::FactKind::CompletionCancelled { opener } => {
                let conversation = self.conversations.get_mut(&opener).expect("cancellation follows opening");
                let started = take_started(&mut conversation.attempt);
                // The domain can abandon an unsent logical intent; that is not a provider attempt.
                if let Some(started) = started {
                    records.push(cancelled(conversation, origin, started, fact.at)).expect("one cancellation");
                }
            }
            session::FactKind::Ended { opener, end, turns, usage: _ } => {
                let mut conversation = match self.conversations.remove(&opener) {
                    Some(conversation) => conversation,
                    None => return records,
                };
                if let Some(record) = terminal(&mut conversation, origin, unknown_usage(), 0, None) {
                    records.push(record).expect("one pending terminal before end");
                }
                if let session::End::Failed {
                    failure: llm::Failure::Limit { which: llm::Limit::History, .. },
                    attempts,
                    ..
                } = end
                {
                    conversation.attempts = u64::from(attempts);
                }
                let (end, which, mut failure) = conversation_end(end);
                if failure.is_some() {
                    if let Some(observed) = conversation.last_failure.take() {
                        failure = Some(end_failure(failure.expect("typed failed terminal"), observed));
                    }
                    if conversation.main {
                        self.main_failure = match &failure {
                            Some(failure) => Some(RunFailure {
                                run: conversation.run,
                                model: conversation.model.clone(),
                                attempts: conversation.attempts,
                                failure: failure.clone(),
                            }),
                            None => None,
                        };
                    }
                }
                records
                    .push(Record {
                        t_ms: millis(fact.at, origin),
                        event: Event::ConversationClosed(ConversationClosed {
                            run: conversation.run,
                            conversation: conversation.number,
                            end,
                            which: match which {
                                Some(which) => Some(which.into()),
                                None => None,
                            },
                            failure,
                            turns: u64::from(turns),
                            usage: conversation.reported.usage(),
                            spent: conversation.spent,
                            duration_ms: millis(fact.at, conversation.opened),
                        }),
                    })
                    .expect("one conversation terminal");
            }
            session::FactKind::JobEnded { .. }
            | session::FactKind::ReasoningDropped { .. }
            | session::FactKind::Tools { .. }
            | session::FactKind::BuiltinStarted { .. }
            | session::FactKind::BuiltinAnswered { .. }
            | session::FactKind::DelegateStarted { .. }
            | session::FactKind::DelegateAnswered { .. }
            | session::FactKind::DelegateCancelled { .. }
            | session::FactKind::DelegateWithheld { .. }
            | session::FactKind::Yielded { .. } => {}
        }
        records
    }
}

fn rejected_calls(completion: &llm::Completion) -> (u64, u64) {
    let mut oversized = 0_u64;
    let mut cut = 0_u64;
    for block in &completion.content {
        match block {
            llm::Said::ToolCall { call, .. } => match call {
                llm::Decoded::Invalid { problem } => match problem {
                    llm::Problem::Oversize { .. } => oversized = oversized.checked_add(1).expect("bounded blocks"),
                    llm::Problem::CutOff { .. } => cut = cut.checked_add(1).expect("bounded blocks"),
                    llm::Problem::Nesting
                    | llm::Problem::UnknownTool
                    | llm::Problem::NotAnObject
                    | llm::Problem::Missing { .. }
                    | llm::Problem::WrongType { .. }
                    | llm::Problem::BadValue { .. }
                    | llm::Problem::TooLarge
                    | llm::Problem::JobsFull { .. }
                    | llm::Problem::UnknownJob { .. } => {}
                },
                llm::Decoded::Builtin { .. } | llm::Decoded::Owned { .. } | llm::Decoded::Served { .. } => {}
            },
            llm::Said::Opaque { .. } | llm::Said::Refusal { .. } | llm::Said::Text { .. } => {}
        }
    }
    (oversized, cut)
}

fn take_started(attempt: &mut Attempt) -> Option<Started> {
    match core::mem::replace(attempt, Attempt::Idle) {
        Attempt::Started(started) => Some(started),
        Attempt::Idle => None,
        Attempt::Answered { .. } | Attempt::Failed { .. } => unreachable!("one terminal per actual provider attempt"),
    }
}

fn terminal(
    conversation: &mut Conversation,
    origin: Time,
    usage: Usage,
    spent: u64,
    retry_ms: Option<u64>,
) -> Option<Record> {
    let (started, at, outcome, stop, blocks, calls, invalid, failure) =
        match core::mem::replace(&mut conversation.attempt, Attempt::Idle) {
            Attempt::Idle => return None,
            Attempt::Started(_) => unreachable!("a session end follows its actual provider terminal"),
            Attempt::Answered { started, at, stop, blocks, calls, invalid } => {
                (started, at, Outcome::Completed, Some(stop), blocks, calls, invalid, None)
            }
            Attempt::Failed { started, at, failure } => (started, at, Outcome::Failed, None, 0, 0, 0, Some(failure)),
        };
    let (usage, oversized, cut, blocks) = match started.received {
        Some(received) => (crate::reported_usage(received.usage), received.oversized, received.cut, received.blocks),
        None => (usage, 0, 0, blocks),
    };
    let measures = transport(started.measures);
    Some(Record {
        t_ms: millis(at, origin),
        event: Event::ResponseCompleted(ResponseCompleted {
            run: conversation.run,
            conversation: started.conversation,
            response: started.number,
            outcome,
            stop,
            blocks,
            calls,
            invalid,
            oversized,
            cut,
            usage,
            spent,
            request_bytes: measures.request_bytes,
            first_byte_ms: duration_ms(measures.first_byte),
            largest_gap_ms: duration_ms(measures.largest_gap),
            duration_ms: millis(at, started.at),
            failure,
            retry_ms,
            completion: started.completion,
        }),
    })
}

fn cancelled(conversation: &Conversation, origin: Time, started: Started, at: Time) -> Record {
    let measures = transport(started.measures);
    Record {
        t_ms: millis(at, origin),
        event: Event::ResponseCompleted(ResponseCompleted {
            run: conversation.run,
            conversation: started.conversation,
            response: started.number,
            outcome: Outcome::Cancelled,
            stop: None,
            blocks: 0,
            calls: 0,
            invalid: 0,
            oversized: 0,
            cut: 0,
            usage: unknown_usage(),
            spent: 0,
            request_bytes: measures.request_bytes,
            first_byte_ms: duration_ms(measures.first_byte),
            largest_gap_ms: duration_ms(measures.largest_gap),
            duration_ms: millis(at, started.at),
            failure: None,
            retry_ms: None,
            completion: None,
        }),
    }
}

fn transport(measures: Option<smith_protocol_llm::Measures>) -> smith_protocol_llm::Measures {
    match measures {
        Some(measures) => measures,
        None => smith_protocol_llm::Measures::none(),
    }
}

fn duration_ms(duration: Option<skein_lib::Duration>) -> Option<u64> {
    match duration {
        Some(duration) => Some(duration.as_nanos() / 1_000_000),
        None => None,
    }
}

pub(crate) const fn unknown_usage() -> Usage {
    Usage {
        input_tokens: None,
        cache_read_tokens: None,
        cache_write_tokens: None,
        output_tokens: None,
        reasoning_tokens: None,
    }
}

pub(crate) fn millis(at: Time, origin: Time) -> u64 {
    at.saturating_since(origin).as_nanos() / 1_000_000
}

fn stop_kind(stop: llm::Stop) -> Stop {
    match stop {
        llm::Stop::EndTurn => Stop::End,
        llm::Stop::ToolUse => Stop::Tools,
        llm::Stop::MaxTokens => Stop::MaxTokens,
        llm::Stop::Refusal => Stop::Refusal,
    }
}

pub(crate) fn completion_failure(failure: llm::Failure, evidence: llm::Evidence) -> CompletionFailure {
    let (class, retry_after_ms) = match failure {
        llm::Failure::Limit { .. } | llm::Failure::ApplicationLimit => (smith_events::FailureClass::Limit, None),
        llm::Failure::Protocol => (smith_events::FailureClass::Protocol, None),
        llm::Failure::Cancelled => (smith_events::FailureClass::Cancelled, None),
        llm::Failure::Overloaded => (smith_events::FailureClass::Overloaded, None),
        llm::Failure::Unavailable => (smith_events::FailureClass::Unavailable, None),
        llm::Failure::TimedOut { .. } => (smith_events::FailureClass::TimedOut, None),
        llm::Failure::ContextTooLong => (smith_events::FailureClass::ContextTooLong, None),
        llm::Failure::Refused => (smith_events::FailureClass::Refused, None),
        llm::Failure::Unsupported => (smith_events::FailureClass::Unsupported, None),
        llm::Failure::Invalid | llm::Failure::Program { .. } | llm::Failure::SessionGone => {
            (smith_events::FailureClass::Invalid, None)
        }
        llm::Failure::SignedOut | llm::Failure::Unauthorized => (smith_events::FailureClass::Unauthorized, None),
        llm::Failure::RateLimited { retry_after } => {
            (smith_events::FailureClass::RateLimited, Some(retry_after.as_nanos() / 1_000_000))
        }
        llm::Failure::Exhausted { retry_after } => (
            smith_events::FailureClass::Exhausted,
            match retry_after {
                Some(reset) => Some(reset.as_nanos() / 1_000_000),
                None => None,
            },
        ),
    };
    let status = match evidence {
        llm::Evidence::Unsent | llm::Evidence::Unknown => None,
        llm::Evidence::Response { status } => match status {
            Some(status) => Some(u64::from(status)),
            None => None,
        },
    };
    let evidence = match evidence {
        llm::Evidence::Unsent => smith_events::Evidence::Unsent,
        llm::Evidence::Unknown => smith_events::Evidence::MaybeSent,
        llm::Evidence::Response { .. } => smith_events::Evidence::Response,
    };
    let phase = match failure {
        llm::Failure::TimedOut { phase } => Some(match phase {
            llm::Phase::Connect => smith_events::FailurePhase::Connect,
            llm::Phase::Handshake => smith_events::FailurePhase::Handshake,
            llm::Phase::Head => smith_events::FailurePhase::Head,
            llm::Phase::Idle => smith_events::FailurePhase::Idle,
            llm::Phase::Whole => smith_events::FailurePhase::Whole,
        }),
        llm::Failure::Limit { .. }
        | llm::Failure::ApplicationLimit
        | llm::Failure::Protocol
        | llm::Failure::Cancelled
        | llm::Failure::Overloaded
        | llm::Failure::Unavailable
        | llm::Failure::ContextTooLong
        | llm::Failure::Refused
        | llm::Failure::Program { .. }
        | llm::Failure::SessionGone
        | llm::Failure::SignedOut
        | llm::Failure::Unsupported
        | llm::Failure::Invalid
        | llm::Failure::Unauthorized
        | llm::Failure::RateLimited { .. }
        | llm::Failure::Exhausted { .. } => None,
    };
    let limit = match failure {
        llm::Failure::Limit { which, bound } => {
            Some(smith_events::FailureLimit { which: limit_name(which).into(), bound })
        }
        llm::Failure::ApplicationLimit
        | llm::Failure::Protocol
        | llm::Failure::Cancelled
        | llm::Failure::Overloaded
        | llm::Failure::Unavailable
        | llm::Failure::TimedOut { .. }
        | llm::Failure::ContextTooLong
        | llm::Failure::Refused
        | llm::Failure::Program { .. }
        | llm::Failure::SessionGone
        | llm::Failure::SignedOut
        | llm::Failure::Unsupported
        | llm::Failure::Invalid
        | llm::Failure::Unauthorized
        | llm::Failure::RateLimited { .. }
        | llm::Failure::Exhausted { .. } => None,
    };
    CompletionFailure {
        field: None,
        code: None,
        request_id: None,
        class,
        evidence,
        retry_after_ms,
        phase,
        limit,
        status,
        detail: None,
    }
}

fn limit_name(which: llm::Limit) -> &'static [u8] {
    match which {
        llm::Limit::Pool { .. } => b"pool",
        llm::Limit::History => b"history",
        llm::Limit::Window => b"window",
        llm::Limit::Call => b"call",
        llm::Limit::Message => b"message",
        llm::Limit::Path(which) => match which {
            llm::Cap::Request => b"request",
            llm::Cap::RequestHead => b"request_head",
            llm::Cap::ResponseHead => b"response_head",
            llm::Cap::ResponseFields => b"response_fields",
            llm::Cap::Skip => b"skip",
            llm::Cap::Field => b"field",
            llm::Cap::Retained => b"retained",
            llm::Cap::Tokens => b"tokens",
            llm::Cap::Depth => b"depth",
            llm::Cap::Strings => b"strings",
            llm::Cap::Number => b"number",
            llm::Cap::Reasoning => b"reasoning",
            llm::Cap::OutputItems => b"output_items",
            llm::Cap::Metadata => b"metadata",
            llm::Cap::Tools => b"tools",
            llm::Cap::HistoryItems => b"history_items",
            llm::Cap::Output => b"output",
            llm::Cap::Answer => b"answer",
            llm::Cap::ErrorBody => b"error_body",
            llm::Cap::AccessToken => b"access_token",
            llm::Cap::AccountId => b"account_id",
        },
    }
}

/// A derived window limit keeps the native failure that caused it (protocol/events.md, 3.10).
fn end_failure(mut projected: CompletionFailure, observed: CompletionFailure) -> CompletionFailure {
    match &projected.limit {
        Some(limit) if limit.which.as_ref() == b"window" => {
            let request_limit = match &observed.limit {
                Some(limit) => limit.which.as_ref() == b"request",
                None => false,
            };
            if projected.evidence == observed.evidence
                && (observed.class == smith_events::FailureClass::ContextTooLong
                    || observed.class == smith_events::FailureClass::Limit && request_limit)
            {
                projected.evidence = observed.evidence;
                projected.field = observed.field;
                projected.code = observed.code;
                projected.request_id = observed.request_id;
                projected.status = observed.status;
                projected.detail = observed.detail;
            }
            projected
        }
        Some(limit) if limit.which.as_ref() == b"history" => projected,
        Some(_) | None => observed,
    }
}

fn conversation_end(end: session::End) -> (ConversationEnd, Option<&'static [u8]>, Option<CompletionFailure>) {
    match end {
        session::End::Closed => (ConversationEnd::Closed, None, None),
        session::End::NoSummary { .. } => (ConversationEnd::Failed, None, None),
        session::End::Busy | session::End::Invalid => (ConversationEnd::Refused, None, None),
        session::End::Failed { failure, evidence, attempts: _, detail: _ } => {
            (ConversationEnd::Failed, None, Some(completion_failure(failure, evidence)))
        }
        session::End::Budget { spent } => {
            let which: &[u8] = match spent {
                session::Dimension::Turns => b"turns",
                session::Dimension::Input => b"input_tokens",
                session::Dimension::Output => b"output_tokens",
                session::Dimension::CacheRead => b"cache_read_tokens",
                session::Dimension::CacheWrite => b"cache_write_tokens",
                session::Dimension::Time => b"time",
                session::Dimension::Unit => b"spend",
            };
            (ConversationEnd::Budget, Some(which), None)
        }
        session::End::TranscriptRefused { .. } => (ConversationEnd::Transcript, None, None),
        session::End::PriceOverflow | session::End::UsageOverflow => (ConversationEnd::Overflow, None, None),
    }
}

fn provider_field(value: &[u8]) -> Option<Box<[u8]>> {
    if value.is_empty() { None } else { Some(value.into()) }
}

#[cfg(test)]
pub(crate) mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods,
        reason = "programming-model.md section 10.2: deterministic unit harnesses use ordinary Rust"
    )]

    use super::*;
    use alloc::boxed::Box;
    use skein_lib::Duration;

    pub(crate) fn at(millis: u64) -> Time {
        Time::ZERO.checked_add(Duration::from_millis(millis)).unwrap()
    }

    pub(crate) fn opened(owner: u64, thread: u64) -> ConversationOpened {
        ConversationOpened {
            run: 7,
            conversation: owner,
            thread,
            kind: smith_events::ConversationKind::Main,
            parent: None,
            call: None,
            requested: None,
            share: None,
            clamped: List::with_capacity(0),
            model: Model {
                account: None,
                backend: smith_events::Backend::Direct,
                endpoint: b"endpoint-7".to_vec().into(),
                model: b"model-7".to_vec().into(),
                window_tokens: 123,
                output_tokens: 7,
                effort: Some(b"medium".to_vec().into()),
            },
        }
    }

    pub(crate) fn prompt(thread: u32) -> llm::Prompt {
        llm::Prompt {
            effort: Some(b"medium".as_slice().into()),
            purpose: llm::Purpose::Ordinary { guard: None },
            endpoint: llm::Endpoint(0),
            affinity: llm::Affinity { key: [0; 16], thread },
            asker: llm::Asker::Main,
            model: b"model-7".to_vec().into(),
            system: Box::new([]),
            tools: smith_domain::tools::Grants { inspect: false, modify: false, shell: false },
            namespace: smith_domain::tools::Namespace::NoWorkspace,
            served: Box::new([]),
            choice: llm::ToolChoice::Only(Box::new([])),
            messages: Box::new([]),
            max_tokens: 7,
        }
    }

    pub(crate) fn fact(millis: u64, kind: session::FactKind, turn: u64, spent: Option<u64>) -> session::Fact {
        session::Fact {
            at: at(millis),
            kind,
            call: None,
            response: Some(session::ResponseInfo { number: turn, spent }),
        }
    }

    pub(crate) fn intent(book: &mut Book, owner: Token, choice: session::Choice, retry: u32) {
        let records = book.fact(
            Time::ZERO,
            &fact(
                2,
                session::FactKind::CompletionStarted {
                    window: 1,
                    opener: owner,
                    attempt: retry,
                    messages: 0,
                    max_tokens: 7,
                    choice,
                },
                1,
                None,
            ),
        );
        assert!(records.is_empty());
    }

    #[test]
    fn a_compaction_settles_the_prior_failed_response_before_its_request() {
        let mut book = Book::new(1);
        let owner = Token::new(11);
        book.opened(at(1), owner, &opened(11, 0));
        intent(&mut book, owner, session::Choice::Auto, 0);
        book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
        let failed = fact(
            5,
            session::FactKind::CompletionFailed {
                opener: owner,
                failure: llm::Failure::ContextTooLong,
                evidence: llm::Evidence::Response { status: None },
            },
            1,
            None,
        );
        assert!(book.fact(Time::ZERO, &failed).is_empty());
        let records = book.fact(
            Time::ZERO,
            &fact(6, session::FactKind::compaction_started(owner, 1, b"model-7", 123, 7, Some(b"medium")), 1, None),
        );
        let Event::ResponseCompleted(ended) = &records.get(0).expect("failure terminal").event else {
            panic!("response closes")
        };
        assert_eq!(ended.failure.as_ref().expect("failed request").class, smith_events::FailureClass::ContextTooLong);
        intent(&mut book, owner, session::Choice::Auto, 0);
        let Event::ResponseStarted(started) = book.request(at(7), Time::ZERO, Token::new(100), &prompt(0)).event else {
            panic!("summary request starts")
        };
        assert_eq!(started.response, 2);
    }

    #[test]
    fn an_unsent_history_limit_keeps_no_earlier_attempt_or_failure() {
        let mut book = Book::new(1);
        let owner = Token::new(11);
        book.opened(at(1), owner, &opened(11, 0));
        intent(&mut book, owner, session::Choice::Auto, 0);
        book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
        let failed = session::FactKind::CompletionFailed {
            opener: owner,
            failure: llm::Failure::Overloaded,
            evidence: llm::Evidence::Unknown,
        };
        book.fact(Time::ZERO, &fact(4, failed, 1, None));
        let limit = session::End::Failed {
            detail: Box::default(),
            failure: llm::Failure::Limit { which: llm::Limit::History, bound: 2 },
            evidence: llm::Evidence::Unsent,
            attempts: 0,
        };
        let ended = session::FactKind::Ended { opener: owner, end: limit, turns: 0, usage: llm::Usage::ZERO };
        book.fact(Time::ZERO, &fact(5, ended, 1, None));
        let ended = book.main_failure.expect("the main history limit reaches its run");
        assert_eq!(ended.attempts, 0);
        assert_eq!(
            ended.failure,
            completion_failure(llm::Failure::Limit { which: llm::Limit::History, bound: 2 }, llm::Evidence::Unsent,)
        );
    }

    #[test]
    fn revised_unsent_intent_and_retry_join_once_to_actual_hand_offs() {
        let mut book = Book::new(2);
        let owner = Token::new(11);
        book.opened(at(1), Token::new(11), &opened(11, 0));
        intent(&mut book, owner, session::Choice::Auto, 0);
        intent(&mut book, owner, session::Choice::Only, 0);
        let first = book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
        let Event::ResponseStarted(first) = first.event else {
            panic!("actual hand-off");
        };
        assert_eq!((first.response, first.turn, first.attempt), (1, 1, 0));
        assert_eq!(first.tool_choice, smith_events::ToolChoice::Only);
        assert!(first.prompt.is_none());
        let failed = fact(
            5,
            session::FactKind::CompletionFailed {
                opener: owner,
                failure: llm::Failure::RateLimited { retry_after: Duration::from_millis(29) },
                evidence: llm::Evidence::Response { status: None },
            },
            1,
            None,
        );
        assert!(book.fact(Time::ZERO, &failed).is_empty());
        let records = book.fact(
            Time::ZERO,
            &fact(
                6,
                session::FactKind::CompletionRetried { opener: owner, attempt: 1, delay: Duration::from_millis(31) },
                1,
                None,
            ),
        );
        let Event::ResponseCompleted(failed) = &records.get(0).unwrap().event else {
            panic!("one failure terminal");
        };
        assert_eq!((failed.response, failed.duration_ms, failed.retry_ms), (1, 2, Some(31)));
        assert_eq!(failed.failure.as_ref().unwrap().retry_after_ms, Some(29));
        assert_eq!(failed.usage, unknown_usage());
        intent(&mut book, owner, session::Choice::Only, 1);
        let second = book.request(at(37), Time::ZERO, Token::new(100), &prompt(0));
        let Event::ResponseStarted(second) = second.event else {
            panic!("actual retry hand-off");
        };
        assert_eq!((second.response, second.turn, second.attempt), (2, 1, 1));
        book.opened(at(38), Token::new(12), &opened(12, 1));
        intent(&mut book, Token::new(12), session::Choice::Only, 0);
        let child = book.request(at(39), Time::ZERO, Token::new(101), &prompt(1));
        let Event::ResponseStarted(child) = child.event else {
            panic!("actual second conversation");
        };
        assert_eq!(child.response, 3);
    }

    #[test]
    fn internal_owner_tokens_do_not_replace_public_activation_and_conversation_numbers() {
        let mut book = Book::new(1);
        let owner = Token::new(91);
        let mut opening = opened(0, 0);
        opening.run = 77;
        book.opened(at(1), owner, &opening);
        let scope = book.scope(owner).expect("actual owner joined");
        assert_eq!((scope.run, scope.conversation), (77, 0));
        assert!(book.scope(Token::new(0)).is_none(), "public ordinal is not the internal owner");
        intent(&mut book, owner, session::Choice::Auto, 0);
        let record = book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
        let Event::ResponseStarted(record) = record.event else { panic!("actual hand-off") };
        assert_eq!((record.run, record.conversation), (77, 0));
        let completion = actual_completion();
        book.received(Token::new(99), &completion);
        drop(book.fact(
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
                1,
                None,
            ),
        ));
        let records = book
            .fact(Time::ZERO, &fact(7, session::FactKind::Used { opener: owner, usage: completion.usage }, 1, Some(3)));
        let Event::ResponseCompleted(record) = &records.as_slice()[0].event else { panic!("actual terminal") };
        assert_eq!((record.run, record.conversation), (77, 0));
        let records = book.fact(
            Time::ZERO,
            &fact(
                11,
                session::FactKind::Ended {
                    opener: owner,
                    end: session::End::Closed,
                    turns: 1,
                    usage: completion.usage,
                },
                1,
                None,
            ),
        );
        let Event::ConversationClosed(record) = &records.as_slice()[0].event else { panic!("actual conversation end") };
        assert_eq!((record.run, record.conversation), (77, 0));
        assert!(book.scope(owner).is_none());
    }

    pub(crate) fn actual_completion() -> llm::Completion {
        llm::Completion {
            reasoning_dropped: Box::new([]),
            content: Box::new([
                llm::Said::ToolCall {
                    id: b"oversized-call".to_vec().into(),
                    name: b"tool".to_vec().into(),
                    input: b"{}".to_vec().into(),
                    call: llm::Decoded::Invalid { problem: llm::Problem::Oversize { bytes: 55, bound: 32 } },
                    replay: None,
                },
                llm::Said::ToolCall {
                    id: b"cut-call".to_vec().into(),
                    name: b"tool".to_vec().into(),
                    input: b"{}".to_vec().into(),
                    call: llm::Decoded::Invalid { problem: llm::Problem::CutOff { bytes: 13 } },
                    replay: None,
                },
            ]),
            stop: llm::Stop::ToolUse,
            usage: llm::Usage {
                input_tokens: Some(17),
                output_tokens: Some(19),
                cache_read_tokens: None,
                cache_write_tokens: Some(0),
                reasoning_tokens: Some(3),
            },
        }
    }

    #[test]
    fn raw_usage_and_rejection_counts_survive_accounting_refusal() {
        for accepted in [false, true] {
            let mut book = Book::new(1);
            let owner = Token::new(11);
            book.opened(at(1), Token::new(11), &opened(11, 0));
            book.fact(Time::ZERO, &fact(2, session::FactKind::Opened { opener: owner }, 1, None));
            intent(&mut book, owner, session::Choice::Only, 0);
            book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
            let completion = actual_completion();
            book.received(Token::new(99), &completion);
            book.fact(
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
                    1,
                    None,
                ),
            );
            let records = if accepted {
                book.fact(
                    Time::ZERO,
                    &fact(5, session::FactKind::Used { opener: owner, usage: completion.usage }, 1, Some(23)),
                )
            } else {
                book.fact(
                    Time::ZERO,
                    &fact(
                        5,
                        session::FactKind::Ended {
                            opener: owner,
                            end: session::End::PriceOverflow,
                            turns: 0,
                            usage: llm::Usage::ZERO,
                        },
                        1,
                        None,
                    ),
                )
            };
            let Event::ResponseCompleted(ended) = &records.get(0).unwrap().event else {
                panic!("one typed response terminal");
            };
            assert_eq!(ended.usage, crate::reported_usage(completion.usage));
            assert_eq!((ended.oversized, ended.cut, ended.invalid), (1, 1, 2));
            assert_eq!(ended.spent, if accepted { 23 } else { 0 });
            assert!(ended.completion.is_none());
            assert!(ended.request_bytes.is_none() && ended.first_byte_ms.is_none() && ended.largest_gap_ms.is_none());
            if !accepted {
                assert_eq!(records.len(), 2);
                let Event::ConversationClosed(closed) = &records.get(1).unwrap().event else {
                    panic!("then one conversation end");
                };
                assert_eq!((closed.turns, closed.spent), (0, 0));
                assert_eq!(closed.end, ConversationEnd::Overflow);
                assert_eq!(closed.usage, ended.usage, "a reported terminal belongs to its conversation total");
            }
        }
    }

    #[test]
    fn cancellations_and_pre_session_refusals_have_one_terminal_right() {
        for sent in [false, true] {
            let mut book = Book::new(1);
            let owner = Token::new(11);
            book.opened(at(1), Token::new(11), &opened(11, 0));
            intent(&mut book, owner, session::Choice::Only, 0);
            if sent {
                book.request(at(3), Time::ZERO, Token::new(99), &prompt(0));
            }
            let cancelled =
                book.fact(Time::ZERO, &fact(5, session::FactKind::CompletionCancelled { opener: owner }, 1, None));
            assert_eq!(cancelled.len(), u32::from(sent));
            if sent {
                let Event::ResponseCompleted(cancelled) = &cancelled.get(0).unwrap().event else {
                    panic!("one cancellation terminal");
                };
                assert_eq!(cancelled.outcome, Outcome::Cancelled);
                assert_eq!(cancelled.usage, unknown_usage());
            }
        }
        let mut book = Book::new(1);
        let owner = Token::new(11);
        book.opened(at(1), Token::new(11), &opened(11, 0));
        book.root_ended(at(5), owner, run::facts::EndKind::Invalid);
        let Event::ConversationClosed(refused) = book.facts_drained(Time::ZERO).unwrap().event else {
            panic!("refusal closes opening");
        };
        assert_eq!(refused.end, ConversationEnd::Refused);
        assert_eq!((refused.turns, refused.spent), (0, 0));
        assert!(book.facts_drained(Time::ZERO).is_none());
        assert!(
            book.fact(
                Time::ZERO,
                &fact(
                    5,
                    session::FactKind::Ended {
                        opener: owner,
                        end: session::End::Invalid,
                        turns: 0,
                        usage: llm::Usage::ZERO
                    },
                    1,
                    None
                )
            )
            .is_empty()
        );
        // The same capacity can now admit another refused opening.
        book.opened(at(6), Token::new(12), &opened(12, 1));
        book.root_ended(at(7), Token::new(12), run::facts::EndKind::Busy);
        assert!(book.facts_drained(Time::ZERO).is_some());
    }
    #[test]
    fn observed_transport_values_survive_each_terminal_and_absent_fields_stay_unknown() {
        let sources = [
            smith_protocol_llm::Measures::none(),
            smith_protocol_llm::Measures {
                request_bytes: Some(u64::from(u32::MAX) + 1),
                first_byte: Some(Duration::ZERO),
                largest_gap: Some(Duration::from_nanos(7_999_999)),
            },
            smith_protocol_llm::Measures { request_bytes: Some(91), first_byte: None, largest_gap: None },
        ];
        for outcome in [Outcome::Completed, Outcome::Failed, Outcome::Cancelled] {
            for measures in sources {
                let mut book = Book::new(1);
                let owner = Token::new(11);
                let request = Token::new(99);
                book.opened(at(1), owner, &opened(11, 0));
                intent(&mut book, owner, session::Choice::Only, 0);
                book.request(at(3), Time::ZERO, request, &prompt(0));
                book.measured(request, measures);
                let records = match outcome {
                    Outcome::Completed => {
                        let completion = actual_completion();
                        book.received(request, &completion);
                        book.fact(
                            Time::ZERO,
                            &fact(
                                4,
                                session::FactKind::CompletionAnswered {
                                    opener: owner,
                                    stop: completion.stop,
                                    blocks: 2,
                                    calls: 2,
                                    invalid: 2,
                                },
                                1,
                                None,
                            ),
                        );
                        book.fact(
                            Time::ZERO,
                            &fact(5, session::FactKind::Used { opener: owner, usage: completion.usage }, 1, Some(23)),
                        )
                    }
                    Outcome::Failed => {
                        book.fact(
                            Time::ZERO,
                            &fact(
                                4,
                                session::FactKind::CompletionFailed {
                                    opener: owner,
                                    failure: llm::Failure::Unavailable,
                                    evidence: llm::Evidence::Unknown,
                                },
                                1,
                                None,
                            ),
                        );
                        book.fact(
                            Time::ZERO,
                            &fact(
                                5,
                                session::FactKind::CompletionRetried {
                                    opener: owner,
                                    attempt: 1,
                                    delay: Duration::from_millis(9),
                                },
                                1,
                                None,
                            ),
                        )
                    }
                    Outcome::Cancelled => book
                        .fact(Time::ZERO, &fact(5, session::FactKind::CompletionCancelled { opener: owner }, 1, None)),
                    Outcome::Unknown(_) => unreachable!("the fixture names the three actual terminals"),
                };
                let Event::ResponseCompleted(response) = &records.as_slice()[0].event else {
                    panic!("one actual response terminal");
                };
                assert_eq!(records.len(), 1);
                assert_eq!(response.outcome, outcome);
                assert_eq!(response.request_bytes, measures.request_bytes);
                assert_eq!(response.first_byte_ms, measures.first_byte.map(|duration| duration.as_nanos() / 1_000_000));
                assert_eq!(
                    response.largest_gap_ms,
                    measures.largest_gap.map(|duration| duration.as_nanos() / 1_000_000)
                );
            }
        }
    }

    #[test]
    fn conversation_totals_keep_each_reported_counter_and_no_report_remains_unknown() {
        for reported in [false, true] {
            let mut book = Book::new(1);
            let owner = Token::new(11);
            book.opened(at(1), owner, &opened(11, 0));
            let sources = [
                llm::Usage {
                    input_tokens: Some(3),
                    output_tokens: None,
                    cache_read_tokens: Some(0),
                    cache_write_tokens: None,
                    reasoning_tokens: None,
                },
                llm::Usage {
                    input_tokens: None,
                    output_tokens: Some(5),
                    cache_read_tokens: None,
                    cache_write_tokens: Some(2),
                    reasoning_tokens: Some(1),
                },
            ];
            if reported {
                for (index, usage) in sources.into_iter().enumerate() {
                    let request = Token::new(u64::try_from(index).unwrap() + 99);
                    intent(&mut book, owner, session::Choice::Auto, 0);
                    book.request(at(3), Time::ZERO, request, &prompt(0));
                    let completion = llm::Completion {
                        content: Box::new([]),
                        stop: llm::Stop::EndTurn,
                        usage,
                        reasoning_dropped: Box::new([]),
                    };
                    book.received(request, &completion);
                    book.fact(
                        Time::ZERO,
                        &fact(
                            4,
                            session::FactKind::CompletionAnswered {
                                opener: owner,
                                stop: completion.stop,
                                blocks: 0,
                                calls: 0,
                                invalid: 0,
                            },
                            1,
                            None,
                        ),
                    );
                    let terminal =
                        book.fact(Time::ZERO, &fact(5, session::FactKind::Used { opener: owner, usage }, 1, Some(7)));
                    let Event::ResponseCompleted(actual) = &terminal.get(0).unwrap().event else {
                        panic!("actual provider report")
                    };
                    assert_eq!(actual.usage, crate::reported_usage(usage));
                }
            }
            let records = book.fact(
                Time::ZERO,
                &fact(
                    9,
                    session::FactKind::Ended {
                        opener: owner,
                        end: session::End::Closed,
                        turns: if reported { 2 } else { 0 },
                        usage: llm::Usage::NONE,
                    },
                    1,
                    None,
                ),
            );
            let Event::ConversationClosed(ended) = &records.get(0).unwrap().event else {
                panic!("one actual conversation end")
            };
            assert_eq!(ended.spent, if reported { 14 } else { 0 });
            assert_eq!(
                ended.usage,
                if reported {
                    Usage {
                        input_tokens: Some(3),
                        output_tokens: Some(5),
                        cache_read_tokens: Some(0),
                        cache_write_tokens: Some(2),
                        reasoning_tokens: Some(1),
                    }
                } else {
                    unknown_usage()
                }
            );
        }
    }

    fn fail_attempt(book: &mut Book, owner: Token, retry: u32, capture: crate::sinks::Capture) {
        let request = Token::new(u64::from(retry) + 99);
        let (status, detail) = if retry == 0 { (429, b"first".as_slice()) } else { (404, b"last".as_slice()) };

        intent(book, owner, session::Choice::Auto, retry);
        book.request(at(3), Time::ZERO, request, &prompt(0));
        let failure = if retry == 0 {
            llm::Failure::RateLimited { retry_after: Duration::from_millis(29) }
        } else {
            llm::Failure::Invalid
        };
        book.failed(
            request,
            failure,
            llm::Evidence::Response { status: Some(status) },
            detail,
            capture,
            &llm::Report::none(),
        );
        assert!(
            book.fact(
                Time::ZERO,
                &fact(
                    5,
                    session::FactKind::CompletionFailed {
                        opener: owner,
                        failure,
                        evidence: llm::Evidence::Response { status: None }
                    },
                    1,
                    None
                )
            )
            .is_empty()
        );
        if retry == 0 {
            let records = book.fact(
                Time::ZERO,
                &fact(
                    6,
                    session::FactKind::CompletionRetried {
                        opener: owner,
                        attempt: 1,
                        delay: Duration::from_millis(31),
                    },
                    1,
                    None,
                ),
            );
            let Event::ResponseCompleted(first) = &records.get(0).unwrap().event else { panic!("retry terminal") };
            let observed = first.failure.as_ref().unwrap();
            assert_eq!(observed.status, Some(429));
            assert_eq!(observed.retry_after_ms, Some(29));
            assert_eq!(first.retry_ms, Some(31));
        }
    }

    fn unlocated_failure(completion: CompletionFailure) -> smith_events::AnswerFailure {
        smith_events::AnswerFailure {
            state: None,
            code: None,
            program: None,
            exit: None,
            detail: None,
            reset_ms: None,
            used: None,
            bound: None,
            dropped: None,
            ended: None,
            turn: None,
            class: smith_events::AnswerClass::Model,
            endpoint: None,
            model: None,
            attempts: None,
            completion: Some(completion),
            account: None,
            which: None,
            reason: None,
        }
    }

    #[test]
    fn actual_failure_joins_keep_retry_identity_and_child_diagnostics_out_of_the_run() {
        for kind in [smith_events::ConversationKind::Main, smith_events::ConversationKind::Child] {
            for capture in
                [crate::sinks::Capture::None, crate::sinks::Capture::Calls, crate::sinks::Capture::Everything]
            {
                let mut book = Book::new(1);
                let owner = Token::new(11);
                let mut opening = opened(11, 0);
                opening.kind = kind.clone();
                book.opened(at(1), owner, &opening);
                for retry in [0, 1] {
                    fail_attempt(&mut book, owner, retry, capture);
                }
                let records = book.fact(
                    Time::ZERO,
                    &fact(
                        7,
                        session::FactKind::Ended {
                            opener: owner,
                            end: session::End::Failed {
                                detail: Box::default(),
                                failure: llm::Failure::Invalid,
                                evidence: llm::Evidence::Response { status: None },
                                attempts: 1,
                            },
                            turns: 0,
                            usage: llm::Usage::NONE,
                        },
                        1,
                        None,
                    ),
                );
                let Event::ResponseCompleted(last) = &records.get(0).unwrap().event else {
                    panic!("last actual response")
                };
                let Event::ConversationClosed(closed) = &records.get(1).unwrap().event else { panic!("conversation") };
                assert_eq!(last.failure, closed.failure);
                let observed = last.failure.as_ref().unwrap();
                assert_eq!(observed.status, Some(404));
                assert_eq!(observed.retry_after_ms, None);
                assert_eq!(
                    observed.detail.as_deref(),
                    if capture == crate::sinks::Capture::None { None } else { Some(b"last".as_slice()) }
                );
                let mut run = smith_events::RunCompleted {
                    run: 7,
                    wind_down: false,
                    status: smith_events::Status::Failed,
                    failure: Some(unlocated_failure(completion_failure(
                        llm::Failure::Invalid,
                        llm::Evidence::Response { status: None },
                    ))),
                    result: None,
                    turns: 0,
                    spent: 0,
                    usage: unknown_usage(),
                    read: None,
                    unread: 0,
                    duration_ms: 7,
                };
                book.answer_failure(&mut run);
                let answer = run.failure.as_ref().unwrap();
                if kind == smith_events::ConversationKind::Main {
                    assert_eq!(answer.completion, last.failure);
                    assert_eq!(answer.endpoint, Some(opening.model.endpoint.clone()));
                    assert_eq!(answer.model, Some(opening.model.model.clone()));
                    assert_eq!(answer.attempts, Some(2));
                } else {
                    assert_eq!(answer.completion.as_ref().unwrap().status, None);
                    assert_eq!(answer.endpoint, None);
                    assert_eq!(answer.attempts, None);
                }
            }
        }
        for phase in [llm::Phase::Connect, llm::Phase::Handshake, llm::Phase::Head, llm::Phase::Idle, llm::Phase::Whole]
        {
            let failure = completion_failure(llm::Failure::TimedOut { phase }, llm::Evidence::Unknown);
            assert!(failure.phase.is_some());
            assert_eq!(failure.status, None);
            assert_eq!(failure.detail, None);
        }
        let failure = completion_failure(
            llm::Failure::Limit { which: llm::Limit::Path(llm::Cap::ResponseHead), bound: 1935 },
            llm::Evidence::Unsent,
        );
        assert_eq!(
            failure.limit,
            Some(smith_events::FailureLimit { which: b"response_head".as_slice().into(), bound: 1935 })
        );
        assert_eq!(failure.status, None);
    }
}

#[cfg(test)]
mod window_failure_tests {
    use super::end_failure;
    use smith_events::{CompletionFailure, Evidence, FailureClass, FailureLimit};

    #[test]
    fn an_unsent_window_check_does_not_take_an_earlier_transient_failure_as_its_cause() {
        let projected = CompletionFailure {
            field: None,
            code: None,
            request_id: None,
            class: FailureClass::Limit,
            evidence: Evidence::Unsent,
            limit: Some(FailureLimit { which: b"window".as_slice().into(), bound: 6_144 }),
            status: None,
            detail: None,
            phase: None,
            retry_after_ms: None,
        };
        let observed = CompletionFailure {
            field: None,
            code: None,
            request_id: None,
            class: FailureClass::RateLimited,
            evidence: Evidence::Response,
            limit: None,
            status: Some(429),
            detail: Some(b"retry later".as_slice().into()),
            phase: None,
            retry_after_ms: Some(1),
        };
        assert_eq!(end_failure(projected.clone(), observed), projected);
    }
}

#[cfg(test)]
mod model_change_tests {
    use super::tests::{at, fact, intent, opened, prompt};
    use super::*;

    #[test]
    fn a_model_change_compactions_conversation_names_the_windows_model() {
        let owner = Token::new(11);
        let mut book = Book::new(1);
        book.opened(at(1), owner, &opened(11, 0));
        let kind = session::FactKind::compaction_started(owner, 1, b"old-model", 256, 7, Some(b"low"));
        let records = book.fact(Time::ZERO, &fact(2, kind, 1, None));
        let Event::ConversationOpened(opened) = &records.get(0).expect("opening").event else {
            panic!("compaction opening")
        };
        assert_eq!(opened.model.model.as_ref(), b"old-model");
        assert_eq!((opened.model.window_tokens, opened.model.output_tokens), (256, 7));
        assert_eq!(opened.model.effort.as_deref(), Some(b"low".as_slice()));
        intent(&mut book, owner, session::Choice::Auto, 0);
        let mut requested = prompt(0);
        requested.model = b"old-model".as_slice().into();
        requested.effort = Some(b"low".as_slice().into());
        let started = book.request(at(4), Time::ZERO, Token::new(99), &requested);
        let Event::ResponseStarted(response) = started.event else { panic!("compaction request") };
        assert_eq!(response.model, opened.model);
    }
}
