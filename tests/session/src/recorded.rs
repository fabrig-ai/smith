//! A scripted parent/provider world for concrete version-three sessions. The
//! referee observes only requests: provider bytes, turns and spend, never
//! session state. The harness separately checks boundary and settle contracts.

use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use smith_domain_session::{self as session, llm, record};
use smith_domain_tools::{Authority, Effect, Grants};

/// Integer prices used by the concrete transcript scenarios; pricing performs no live account lookup.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub const PRICES: record::Prices = record::Prices { input: 7, cached: 3, cache_write: None, output: 11, unit: 10 };

/// Fixed provider usage supplied by concrete transcript scenarios.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub const USAGE: llm::Usage = llm::Usage {
    input_tokens: Some(11),
    output_tokens: Some(4),
    cache_read_tokens: Some(5),
    cache_write_tokens: Some(3),
    reasoning_tokens: None,
};

/// Fixed opaque provider bytes whose exact replay preservation is asserted.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub const OPAQUE: &[u8] = b"\0provider reasoning\xffsignature";

/// Builds the concrete version-three opening with the supplied transcript and integer spend cap.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn opening(transcript: Option<record::Transcript>, budget: u64) -> record::Opening {
    record::Opening {
        window_llm: None,
        spec: session::Spec {
            backend: smith_domain_session::record::Backend::Direct,
            effort: None,
            final_turn: None,
            window: 65_536,
            line: 64_512,
            summary: b"Summarise the work so far.".as_slice().into(),
            preamble: b"Earlier window summary:".as_slice().into(),
            affinity: llm::Affinity { key: [0; 16], thread: 0 },
            protocol_allowance: 0,
            progress: smith_domain_session::Progress { head: Duration::from_secs(30), idle: Duration::from_secs(30) },
            account: Some(0),
            endpoint: llm::Endpoint(7),
            model: b"model".as_slice().into(),
            system: b"instructions".as_slice().into(),
            authority: Authority {
                cwd: Box::default(),
                repos: Box::default(),
                grants: Grants { inspect: false, modify: false, shell: false },
            },
            delegated: Box::new([llm::Descriptor { ticket: Token::new(19), effect: Effect::Write }]),
            prompt: b"wake".as_slice().into(),
            output: 128,
            budget: crate::BUDGET,
        },
        dialect: 23,
        prices: PRICES,
        budget,
        answered: Box::default(),
        transcript,
    }
}

/// Real domain plus scripted typed peers, pending terminals and external observations for this component story.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub struct World {
    /// Real session state under test, advanced only through its declared boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub domain: session::Domain,
    /// Injected monotonic and wall times together with immutable session limits.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub env: Env<session::Limits>,
    /// Bounded queue of owned session requests awaiting scripted delivery.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub out: Queue<session::Request>,
    /// Current admitted session identity, or none before admission or after its terminal.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub session: Option<Token>,
    /// Pending provider completion identity; its single terminal consumes it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completing: Option<Token>,
    /// Pending opener-served call identities, each requiring one terminal.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegated: Vec<Token>,
    /// Actual delegated transcript origins in emitted order, independent of live callbacks.
    /// Contract: domain/session.md, sections 3, 5 and 12; testing-strategy.md, section 7.
    pub origins: Vec<record::Origin>,
    /// Pending checkout-operation identities and their owned typed requests.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub operations: Vec<(Token, smith_domain_tools::Op)>,
    /// Identities for which the domain requested cancellation; their terminals remain owed.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub cancelled_operations: Vec<Token>,
    /// Commands retained by the machine after their shell call answered running.
    pub held: std::collections::BTreeSet<Token>,
    /// Outstanding acts retain the original machine owner until their answer.
    pub job_acts: Vec<(Token, smith_domain_tools::JobAct)>,
    /// Hastened waits keep their existing terminal obligation.
    pub hastened: Vec<Token>,
    /// Settlement acknowledgements returned to the scripted opener.
    pub jobs_cancelled: u32,
    jobs: std::collections::BTreeSet<u32>,
    /// Complete deterministic state snapshots at loop boundaries, separate
    /// from the referee's external observations.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub snapshots: Vec<String>,
    /// Concrete provider prompts in their observed request order.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub prompts: Vec<llm::Prompt>,
    /// Concrete transcript turns emitted by the session, in order.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub turns: Vec<record::Turn>,
    /// Accepted cumulative integer charges and their overflow flags, in request order.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub spend: Vec<u64>,
    /// Cumulative own-completion charges, excluding every delegated bill.
    /// Contract: domain/session.md, section 6; domain/run.md, section 9.
    pub own_spend: Vec<u64>,
    /// Exact completion usage and its cumulative overflow attestation.
    /// Contract: domain/session.md, section 6; domain/run.md, section 10.
    pub used: Vec<llm::Usage>,
    /// Actual opener-facing yields, retaining final answer text and its stop.
    pub yielded: Vec<(session::Yield, Box<[u8]>)>,
    /// Settled count, cumulative raw prefix and its overflow attestation.
    /// Contract: domain/session.md, section 6; domain/run.md, section 10.
    pub terminal_usage: Option<(u32, llm::Usage)>,
    /// Actual emitted facts, including cumulative overflow attestations.
    /// Contract: domain/session.md, sections 6 and 10; domain/run.md, section 10.
    pub facts: Vec<session::Fact>,
    /// The single settled session terminal, or none while work remains.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub end: Option<session::End>,
    /// Last provider diagnostic carried by the settled session terminal.
    pub detail: Box<[u8]>,
    /// Ordered deterministic boundary record renderings used for replay comparisons.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub trace: Vec<String>,
}

impl World {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn new(seed: u64, facts: u32) -> World {
        let mut limits = crate::Settings::calm(seed).agent;
        limits.sessions = 1;
        limits.spend = u64::MAX;
        limits.history_messages = 32;
        limits.facts = facts;
        World {
            domain: session::Domain::new(&limits, seed),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            out: Queue::with_capacity(session::max_out(&limits)),
            session: None,
            completing: None,
            delegated: vec![],
            origins: vec![],
            operations: vec![],
            cancelled_operations: vec![],
            held: std::collections::BTreeSet::new(),
            job_acts: vec![],
            hastened: vec![],
            jobs_cancelled: 0,
            jobs: std::collections::BTreeSet::new(),
            snapshots: vec![],
            prompts: vec![],
            turns: vec![],
            spend: vec![],
            own_spend: vec![],
            used: vec![],
            yielded: vec![],
            terminal_usage: None,
            facts: vec![],
            end: None,
            detail: Box::default(),
            trace: vec![],
        }
    }

    /// Delivers one typed terminal or entrance, drains the emitted requests and checks terminal ownership.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn step(&mut self, event: session::Event) {
        match &event {
            session::Event::Completed { owner, .. }
            | session::Event::Overflowed { owner, .. }
            | session::Event::BudgetDenied { owner, .. }
            | session::Event::UnsentClosed { owner }
            | session::Event::Narrowed { owner, .. }
            | session::Event::Cancelled { owner }
            | session::Event::Failed { owner, .. } => {
                assert_eq!(self.completing.take(), Some(*owner), "one terminal per completion");
            }
            session::Event::Answered { owner, .. }
            | session::Event::AnsweredAndYield { owner, .. }
            | session::Event::AnswerWithheld { owner }
            | session::Event::AnswerCancelled { owner, .. } => {
                let position =
                    self.delegated.iter().position(|pending| pending == owner).expect("one terminal per delegate");
                self.delegated.remove(position);
            }
            session::Event::Done { owner, done } => {
                if self.held.contains(owner) {
                    let position = self
                        .job_acts
                        .iter()
                        .position(|(pending, _)| pending == owner)
                        .expect("one terminal per job act");
                    self.job_acts.remove(position);
                    if !matches!(done, smith_domain_tools::Done::Job { end: smith_domain_tools::JobEnd::Running, .. }) {
                        assert!(self.held.remove(owner));
                    }
                } else {
                    let position = self
                        .operations
                        .iter()
                        .position(|(pending, _)| pending == owner)
                        .expect("one terminal per owned operation");
                    self.operations.remove(position);
                    if matches!(done, smith_domain_tools::Done::Held { .. }) {
                        assert!(self.held.insert(*owner));
                    }
                }
            }
            session::Event::NativeVersion { .. }
            | session::Event::BuiltinResult { .. }
            | session::Event::CancelJobs { .. }
            | session::Event::Yield { .. }
            | session::Event::Open { .. }
            | session::Event::Continue { .. }
            | session::Event::Close { .. } => {}
        }
        self.domain.reclaim();
        session::step(&mut self.domain, &self.env, event, &mut self.out);
        assert!(self.out.len() <= session::max_out(&self.env.limits));
        self.collect_outputs();
    }

    /// Resume one settled immediate tool batch before the next provider request.
    pub fn resume(&mut self) {
        self.domain.reclaim();
        session::resume(&mut self.domain, &self.env, &mut self.out);
        assert!(self.out.len() <= session::max_out(&self.env.limits));
        self.collect_outputs();
    }

    fn collect_outputs(&mut self) {
        while let Some(request) = self.out.pop() {
            self.trace.push(format!("{request:?}"));
            self.observe(request);
        }
        while let Some(fact) = self.domain.pop_fact() {
            if let session::FactKind::JobEnded { job, .. } = fact.kind {
                assert!(self.jobs.remove(&job.0), "one terminal per job");
            }
            self.facts.push(fact);
        }
        if self.end.is_some() {
            assert!(self.jobs.is_empty() && self.held.is_empty(), "no job outlives its session");
        }
        self.snapshots.push(format!("{:?}", self.domain));
    }

    /// Records actual boundary outputs and checks their single-owner obligations.
    /// Contract: domain/session.md, sections 3, 6 and 12; testing-strategy.md, section 2.2.
    fn observe(&mut self, request: session::Request) {
        match request {
            session::Request::JobsCancelled { .. } => self.jobs_cancelled += 1,
            session::Request::Opened { opener, session } => {
                assert_eq!(opener, Token::new(31));
                assert!(self.session.replace(session).is_none());
            }
            session::Request::Complete { owner, prompt, .. } => {
                assert!(self.completing.replace(owner).is_none());
                self.prompts.push(prompt);
            }
            session::Request::Delegate { owner, origin, .. } => {
                assert!(!self.delegated.contains(&owner));
                self.delegated.push(owner);
                self.origins.push(origin);
            }
            session::Request::Turn { opener, turn } => {
                assert_eq!(opener, Token::new(31));
                for block in turn.messages.iter().flat_map(|message| &message.content) {
                    match block {
                        llm::Block::ToolResult {
                            result: llm::Returned::Owned { outcome: smith_domain_tools::Outcome::Running { job, .. } },
                            ..
                        } => {
                            assert!(self.jobs.insert(job.0), "a shell answers a fresh job");
                        }
                        llm::Block::ToolResult {
                            result: llm::Returned::Owned { outcome: smith_domain_tools::Outcome::Job { job, end, .. } },
                            ..
                        } if *end != smith_domain_tools::JobEnd::Running => {
                            assert!(self.jobs.remove(&job.0), "one terminal per job");
                        }
                        llm::Block::Opaque { .. }
                        | llm::Block::Refusal { .. }
                        | llm::Block::Text { .. }
                        | llm::Block::ToolCall { .. }
                        | llm::Block::ToolResult { .. } => {}
                    }
                }
                self.turns.push(turn);
            }
            session::Request::Priced { opener, spent, own_spent } => {
                assert_eq!(opener, Token::new(31));
                self.spend.push(spent);
                self.own_spend.push(own_spent);
            }
            session::Request::Ended { opener, end, turns, usage, detail, .. } => {
                assert_eq!(opener, Token::new(31));
                assert!(self.end.replace(end).is_none());
                assert!(self.terminal_usage.replace((turns, usage)).is_none());
                self.detail = detail;
            }
            session::Request::Used { opener, usage } => {
                assert_eq!(opener, Token::new(31));
                self.used.push(usage);
            }
            session::Request::Yielded { stop, text, .. } => self.yielded.push((stop, text)),
            session::Request::Cancel { .. } | session::Request::Yield { .. } | session::Request::Withdraw { .. } => {}
            session::Request::Io { owner, op, .. } => {
                assert!(!self.operations.iter().any(|(pending, _)| pending == &owner));
                self.operations.push((owner, op));
            }
            session::Request::Job { owner, act: smith_domain_tools::JobAct::Hasten } => {
                assert!(self.held.contains(&owner));
                self.hastened.push(owner);
            }
            session::Request::Job { owner, act } => {
                assert!(self.held.contains(&owner));
                assert!(!self.job_acts.iter().any(|(pending, _)| *pending == owner));
                self.job_acts.push((owner, act));
            }
            session::Request::CancelIo { owner } => {
                assert!(self.operations.iter().any(|(pending, _)| pending == &owner) || self.held.contains(&owner));
                self.cancelled_operations.push(owner);
            }
        }
    }

    /// Delivers a new typed opening to the scripted peer or session and retains its pending terminal obligations.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn open(&mut self, opening: record::Opening) {
        self.step(session::Event::Open { opener: Token::new(31), opening: Box::new(opening) });
    }

    /// Advance the injected clock to the next declared deadline and drain its requests.
    pub fn fire(&mut self) {
        self.env.now = self.domain.next_deadline().expect("a scripted retry is armed");
        self.domain.reclaim();
        session::fire(&mut self.domain, &self.env, &mut self.out);
        self.collect_outputs();
    }

    /// Answers the pending provider completion with the supplied blocks and fixed usage.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn complete(&mut self, content: Box<[llm::Block]>, stop: llm::Stop, usage: llm::Usage) {
        self.step(session::Event::Completed {
            owner: self.completing.expect("a completion is in flight"),
            completion: llm::Completion { reasoning_dropped: Box::default(), content, stop, usage },
        });
    }

    /// Requests session closure; lower terminals must still be delivered before settlement.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn close(&mut self) {
        self.step(session::Event::Close { session: self.session.expect("admitted") });
        if let Some(owner) = self.completing {
            self.step(session::Event::Cancelled { owner });
        }
        self.domain.reclaim();
        assert!(self.end.is_some());
        assert_eq!(self.domain.sessions(), 0);
        assert_eq!(self.domain.runs(), 0);
        assert_eq!(self.domain.kits(), 0);
        assert!(
            self.completing.is_none()
                && self.delegated.is_empty()
                && self.operations.is_empty()
                && self.held.is_empty()
                && self.job_acts.is_empty()
        );
    }
}

/// Builds a concrete provider call block naming the opener-served fixture tool.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn called() -> Box<[llm::Block]> {
    Box::new([
        llm::Block::Opaque { bytes: OPAQUE.into() },
        llm::Block::ToolCall {
            id: b"provider-call".as_slice().into(),
            name: b"sub_agent".as_slice().into(),
            input: br#"{"task":"review"}"#.as_slice().into(),
            call: llm::Decoded::Delegated {
                descriptor: Token::new(991),
                source: smith_domain_session::ToolSource::Run,
                ticket: Token::new(991),
                effect: Effect::Write,
            },
            replay: None,
        },
    ])
}

/// Runs the fixed concrete transcript story and returns its external observations.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn scenario(seed: u64, facts: u32) -> World {
    let mut world = World::new(seed, facts);
    world.open(opening(None, 100));
    world.complete(called(), llm::Stop::ToolUse, USAGE);
    assert!(world.turns.is_empty(), "the turn waits for its tool results");
    let owner = world.delegated[0];
    world.step(session::Event::Answered { owner, text: b"child finished".as_slice().into(), error: false, spent: 9 });
    // Independent referee arithmetic: ceil((14*7 + 5*3 + 4*11)/10)=16,
    // then the child contributes 9. One terminal response charges it once.
    assert_eq!(world.spend, [16, 25]);
    assert_eq!(world.turns.len(), 1);
    assert_eq!(world.turns[0].spent, 25);
    assert_eq!(world.turns[0].messages.len(), 3);
    let blocks = &world.turns[0].messages[1].content;
    assert_eq!(blocks[0], llm::Block::Opaque { bytes: OPAQUE.into() });
    assert!(matches!(blocks[1], llm::Block::ToolCall { call: llm::Decoded::Historical, .. }));
    assert_eq!(
        world.turns[0].messages[2].content[0],
        llm::Block::ToolResult {
            id: b"provider-call".as_slice().into(),
            result: llm::Returned::Text { text: b"child finished".as_slice().into(), error: false, replay: None }
        }
    );
    world.complete(
        Box::new([llm::Block::Text { text: b"done".as_slice().into(), replay: None }]),
        llm::Stop::EndTurn,
        llm::Usage { output_tokens: Some(1), ..llm::Usage::ZERO },
    );
    assert_eq!(world.own_spend, [16, 16, 18]);
    assert_eq!(world.turns[1].spent, 27); // ceil(11/10)=2, per completion.
    world.close();
    world
}

/// Copies the settled concrete turns into a replayable version-three transcript.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn transcript(world: &World) -> record::Transcript {
    record::Transcript {
        version: record::VERSION,
        endpoint: Some(llm::Endpoint(7)),
        dialect: 23,
        turns: world.turns.clone().into(),
    }
}

/// Builds independent run-tool writes, which the session must run in separate batches.
/// Contract: domain/session.md, sections 5 and 8.1.
#[must_use]
pub fn written_calls(count: u32) -> Box<[llm::Block]> {
    (0..count)
        .map(|index| llm::Block::ToolCall {
            id: format!("call-{index}").into_bytes().into_boxed_slice(),
            name: b"finish".as_slice().into(),
            input: b"{}".as_slice().into(),
            call: llm::Decoded::Delegated {
                source: session::ToolSource::Run,
                ticket: Token::new(19),
                effect: Effect::Write,
                descriptor: Token::new(19),
            },
            replay: None,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice()
}
