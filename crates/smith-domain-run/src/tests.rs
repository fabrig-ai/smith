//! Feed the domain events, inspect the requests that come out.

mod guide_room;

mod program;

mod open;

mod opening;

use alloc::boxed::Box;

use skein_lib::{Duration, Env, List, Queue, ReplyTo, Time, Token, Wall};

use crate::charter::{Endpoint, Grants, HostTool, Llm, Tools};
use crate::facts::{Answered, Asked, FactKind as Fact, Return};
use crate::outcome::{
    Change, ChangeSpec, Declared, Field, FieldRule, Item, OutcomeSpec, Problem, Problems, Verdict, VerdictRule,
};
use crate::prepare::{Found, Guide};
use crate::{
    Answer, Ask, AskRefusal, Budget, Charter, Delivery, Domain, End, Event, Exhausted, Exit, Failure, Fault, Invalid,
    Limits, MAX_OUT, Opening, Place, Policy, Ran, Read, Refusal, Request, Returned, Spend, Stop, fire, step,
    worst_case,
};
use crate::{Directory, Workspace};

const BUDGET: Budget = Budget { turns: 10, spend: 10_000, time: Duration::from_secs(600) };

#[test]
fn an_unread_charter_version_has_its_own_start_refusal() {
    let unread = Answer::Refused(Refusal::Invalid(Invalid::CharterVersion));
    let malformed = Answer::Refused(Refusal::Invalid(Invalid::MalformedCharter));
    let oversized = Answer::Refused(Refusal::Invalid(Invalid::TooLarge));
    assert_ne!(unread, malformed);
    assert_ne!(malformed, oversized);
    assert_ne!(unread, oversized);
}

/// Fixed admission and ownership limits shared by focused run and result tests.
/// Contract: domain/run.md, sections 3.1, 7.1 and 13.
pub(crate) const LIMITS: Limits = Limits {
    runs: 2,
    conversations: 4,
    run_bytes: 4096,
    brief_sections: 4,
    host_part: 1 << 20,
    room: 0,
    compaction_threshold: [4, 5],
    compaction_material: 0,
    schema_costs: crate::SchemaCosts::NONE,
    directories: 2,
    directory_name_bytes: 256,
    host_path_bytes: 4096,
    conflicts: 64,
    conflict_path_bytes: 4096,
    host_tools: 2,
    host_input_bytes: 65_536,
    host_reply_bytes: 65_536,
    answered_calls: 16,
    answered_bytes: 4096,
    host_timeout: Duration::from_secs(60),
    host_timeout_max: Duration::from_secs(60),
    host_backoff: Duration::from_millis(50),
    verdicts: 2,
    calls: 2,
    budget: Budget { turns: 100, spend: 1_000_000, time: Duration::from_secs(3600) },
    models: 2,
    run_conversations: 3,
    answer_bytes: 16,
    nudges: 2,
    guide_bytes: 64,
    io_timeout: Duration::from_secs(10),
    outcome_bytes: 1024,
    delivery_timeout: Duration::from_secs(300),
    check_timeout: Duration::from_secs(300),
    wrap_up: Duration::from_secs(60),
    check_tail: 4096,
    facts: 64,
    messages: 8,
    message_bytes: 4096,
    offer_messages: 8,
    offer_bytes: 32_782,
    waiting: Duration::from_secs(300),
};

/// The domain, its environment, and room for one step's output.
struct Harness {
    observed: Queue<crate::facts::Fact>,
    domain: Domain,
    env: Env<Limits>,
    out: Queue<Request>,
    prices: alloc::collections::BTreeMap<Token, u64>,
    checks_ended: u32,
    peers: alloc::collections::BTreeMap<Token, Token>,
}

impl Harness {
    fn keep_facts(&mut self) {
        while let Some(fact) = self.domain.pop_fact() {
            self.observed.push(fact);
        }
    }

    fn pop_fact(&mut self) -> Option<crate::facts::Fact> {
        match self.observed.pop() {
            Some(fact) => Some(fact),
            None => self.domain.pop_fact(),
        }
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "programming-model.md, section 10.2: the harness receives a copied immutable profile"
    )]
    fn new(limits: Limits) -> Harness {
        Harness {
            observed: Queue::with_capacity(4096),
            domain: Domain::new(&limits, 0),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            out: Queue::with_capacity(MAX_OUT),
            prices: alloc::collections::BTreeMap::new(),
            checks_ended: 0,
            peers: alloc::collections::BTreeMap::new(),
        }
    }

    /// Steps `event`, returning what it emitted, oldest first.
    fn step(&mut self, event: Event) -> Box<[Request]> {
        if let Event::Started { conversation, peer } = &event {
            self.peers.insert(*peer, *conversation);
        }
        // Legacy controls supply own units alongside raw usage; the         // run receives the two independent session notices in protocol order.
        if let Event::Used { conversation, spend } = &event {
            let previous = self.prices.get(conversation).copied().unwrap_or(0);
            let own_spent = previous.checked_add(spend.units).expect("fixture own units");
            self.prices.insert(*conversation, own_spent);
            let priced = Event::Priced { conversation: *conversation, own_spent, subtree_spent: own_spent };
            self.keep_facts();
            step(&mut self.domain, &self.env, priced, &mut self.out);
        }
        if let Event::Priced { conversation, own_spent, .. } = &event {
            self.prices.insert(*conversation, *own_spent);
        }
        self.keep_facts();
        step(&mut self.domain, &self.env, event, &mut self.out);
        self.drain()
    }

    fn fire(&mut self) -> Box<[Request]> {
        assert!(self.domain.is_due(self.env.now), "an alarm is due");
        self.keep_facts();
        fire(&mut self.domain, &self.env, &mut self.out);
        self.drain()
    }

    fn resume(&mut self) -> Box<[Request]> {
        self.keep_facts();
        self.domain.reclaim();
        crate::resume(&mut self.domain, &self.env, &mut self.out);
        self.drain()
    }

    fn drain(&mut self) -> Box<[Request]> {
        let mut requests = List::with_capacity(MAX_OUT);
        for _ in 0..MAX_OUT.saturating_mul(2) {
            let Some(request) = self.out.pop() else { break };
            match request {
                Request::CancelJobs { peer } => {
                    let conversation = self.peers[&peer];
                    self.keep_facts();
                    step(&mut self.domain, &self.env, Event::JobsCancelled { conversation }, &mut self.out);
                }
                Request::ChecksEnded { .. } => {
                    self.checks_ended = self.checks_ended.checked_add(1).expect("bounded checks");
                }
                request @ (Request::CompletionReady { .. }
                | Request::MessageRefused { .. }
                | Request::Waiting { .. }
                | Request::Turn { .. }
                | Request::HostCall { .. }
                | Request::WithdrawHost { .. }
                | Request::Admitted { .. }
                | Request::Answer { .. }
                | Request::Open { .. }
                | Request::Say { .. }
                | Request::Yield { .. }
                | Request::Close { .. }
                | Request::Read { .. }
                | Request::Probe { .. }
                | Request::Check { .. }
                | Request::Abort { .. }
                | Request::Checking { .. }
                | Request::Deliver { .. }
                | Request::Return { .. }) => requests.push(request).expect("room for MAX_OUT"),
            }
        }
        assert!(self.out.is_empty(), "a step emits at most MAX_OUT");
        requests.into_boxed()
    }

    /// Starts a run of `charter` for call `call`: what it emitted.
    fn start(&mut self, call: u64, charter: Charter) -> Box<[Request]> {
        let mounted = if charter.outcome.change.is_some() {
            Some(coding_workspace())
        } else if charter.grants.deliver.is_some() {
            let mut mounted = Some(workspace());
            mounted.as_mut().unwrap().directories[0].writable = true;
            mounted
        } else {
            Some(workspace())
        };
        self.start_workspace(call, charter, mounted)
    }

    fn start_workspace(&mut self, call: u64, charter: Charter, workspace: Option<Workspace>) -> Box<[Request]> {
        let reply_to = ReplyTo::new(Token::new(call));
        self.step(Event::Start {
            messages: Box::default(),
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            reply_to,
            host_run: Token::new(call),
            identity: None,
            activation: 1,
            charter,
            workspace,
            transcript: None,
            resumed: false,
        })
    }

    /// Starts a run of the test charter for call `call`, which is admitted:
    /// the run's token, once it has looked for its one repository's guide.
    fn prepare(&mut self, call: u64) -> Token {
        let emitted = self.start(call, charter());
        let [Request::Admitted { host_run, run }, Request::Read { owner, .. }] = &*emitted else {
            panic!("expected an admitted run, got {emitted:?}");
        };
        assert_eq!((*host_run, owner), (Token::new(call), run));
        *run
    }

    /// Starts a run of the test charter for call `call`, which is admitted and
    /// finds no guide: the run's token and its main conversation's.
    fn admit(&mut self, call: u64) -> (Token, Token) {
        let run = self.prepare(call);
        let emitted = self.step(Event::Read { owner: run, read: Read::Missing });
        let [Request::Open { conversation, opening: _ }] = &*emitted else {
            panic!("expected main to open, got {emitted:?}");
        };
        (run, *conversation)
    }

    /// Admits a run for call `call` and starts its main conversation as
    /// `peer`: the run's token and main's.
    fn running(&mut self, call: u64, peer: u64) -> (Token, Token) {
        let (run, conversation) = self.admit(call);
        assert!(self.step(Event::Started { conversation, peer: Token::new(peer) }).is_empty(), "starting is quiet");
        (run, conversation)
    }

    fn after(&mut self, span: Duration) {
        self.env.now = self.env.now.checked_add(span).expect("the test stays in range");
    }
}

pub(crate) fn bytes(text: &[u8]) -> Box<[u8]> {
    Box::from(text)
}

pub(crate) fn rule(name: &[u8], min: u32, max: u32) -> VerdictRule {
    VerdictRule {
        name: bytes(name),
        text_max: 1024,
        fields: Box::new([]),
        items: crate::outcome::ItemSpec {
            min,
            max,
            kinds: Box::new([
                crate::outcome::ItemRule {
                    kind: bytes(b"blocking"),
                    fields: Box::new([
                        FieldRule { name: bytes(b"path"), max: 1024 },
                        FieldRule { name: bytes(b"body"), max: 1024 },
                    ]),
                },
                crate::outcome::ItemRule {
                    kind: bytes(b"nit"),
                    fields: Box::new([
                        FieldRule { name: bytes(b"path"), max: 1024 },
                        FieldRule { name: bytes(b"body"), max: 1024 },
                    ]),
                },
            ]),
        },
    }
}

pub(crate) fn workspace() -> Workspace {
    Workspace {
        directories: Box::new([Directory {
            host: Box::from(b"/host".as_slice()),
            name: bytes(b"temper"),
            root: Token::new(900),
            writable: false,
            git: true,
            conflicts: Box::new([]),
        }]),
    }
}

pub(crate) fn charter() -> Charter {
    Charter {
        mode: crate::Mode::Finite,
        reserve: crate::charter::Reserve { turns: 0, time: Duration::ZERO },
        instructions: Box::new([]),
        brief: crate::Brief {
            sections: Box::new([crate::Section {
                title: b"Task".as_slice().into(),
                text: bytes(b"Review the change."),
            }]),
        },

        grants: Grants {
            wait: true,
            deliver: None,
            tools: Tools { inspect: true, modify: false, shell: true },

            agents: false,
            host_tools: Box::new([HostTool {
                name: bytes(b"comment"),
                description: b"Host action".as_slice().into(),
                schema: b"{}".as_slice().into(),
                effect: crate::HostEffect::Read,
                timeout: Duration::from_secs(5),
            }]),
        },
        outcome: OutcomeSpec {
            change: None,
            verdicts: Box::new([rule(b"approve", 0, 0), rule(b"request", 1, 8)]),
            report: None,
            failure: None,
        },
        budget: BUDGET,
        llm: Llm {
            effort: None,
            prices: crate::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            account: Some(0),
            endpoint: Endpoint(1),
            model: bytes(b"model-a"),
            window: 8192,
            output: 1024,
            dialect: 1,
        },
        models: Box::new([]),
        conventions: Some(crate::Conventions {
            guide: b"AGENTS.md".as_slice().into(),
            checks: b".temper/pre-pr".as_slice().into(),
        }),
        resume: false,
        waiting: Duration::from_secs(30),
    }
}

fn spend(input: u64) -> Spend {
    Spend {
        units: input,
        turns: 1,
        input: Some(input),
        output: Some(10),
        cache_read: Some(0),
        cache_write: Some(0),
        reasoning: None,
    }
}

/// What a run answered, and to which call.
fn answered(emitted: Box<[Request]>) -> (u64, Answer) {
    let Ok(one) = Box::<[Request; 1]>::try_from(emitted) else {
        panic!("expected one request");
    };
    let [Request::Answer { to, answer, read: _, detail: _ }] = *one else {
        panic!("expected an answer");
    };
    (to.into_token().raw(), answer)
}

fn failed(failure: Failure, spent: Spend) -> Answer {
    Answer::Failed { failure, spent, turns: 0 }
}

#[test]
fn an_admitted_run_reads_its_checkout_then_opens_main_with_the_whole_budget() {
    let mut h = Harness::new(LIMITS);
    let emitted = h.start(7, charter());
    let [Request::Admitted { host_run, run }, Request::Read { owner, at, max, deadline }] = &*emitted else {
        panic!("expected an admitted run, got {emitted:?}");
    };
    assert_eq!((*host_run, owner), (Token::new(7), run));
    assert_eq!(at, &Place { root: Token::new(900), path: bytes(b"AGENTS.md") });
    assert_eq!((*max, *deadline), (LIMITS.guide_bytes, Time::ZERO.saturating_add(LIMITS.io_timeout)));
    assert_eq!((h.domain.runs(), h.domain.conversations()), (1, 1), "main has its slot from the start");

    let read = Read::Text { text: bytes(b"Run the tests."), whole: true };
    let emitted = h.step(Event::Read { owner: *run, read });
    let [Request::Open { conversation: _, opening }] = &*emitted else {
        panic!("expected main to open, got {emitted:?}");
    };
    let mut found = Found::with_capacity(1);
    found.guides.push(Guide { repository: 0, text: bytes(b"Run the tests."), whole: true }).expect("room");
    let expected = Opening {
        final_turn: None,
        generated_schema_bytes: LIMITS.schema_costs.finish(&charter().outcome).expect("schema count"),
        identity: opening.identity,
        ordinal: 0,
        activation: 1,
        host_tools: charter().grants.host_tools,
        deliver: false,
        llm: charter().llm,
        line: 7168,
        system: super::prompt::system(&charter(), Some(&workspace()), &found, BUDGET, false),
        prompt: bytes(super::prompt::BEGIN),
        summary: bytes(super::prompt::SUMMARY),
        preamble: bytes(super::prompt::PREAMBLE),
        tools: charter().grants.tools,
        workspace: Some(workspace()),
        budget: BUDGET,
        finish: true,
        families: crate::charter::Families { tools: charter().grants.tools, agents: false },
        transcript: None,
        wait: true,
    };
    assert_eq!(opening, &expected);
    assert_eq!(h.domain.next_deadline(), Some(Time::ZERO.saturating_add(BUDGET.time)));
}

#[test]
fn a_run_looks_for_checks_in_writable_repositories_when_a_change_must_pass_them() {
    let mut h = Harness::new(LIMITS);
    let checkout = Workspace {
        directories: Box::new([
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"temper"),
                root: Token::new(900),
                writable: true,
                git: true,
                conflicts: Box::new([]),
            },
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"docs"),
                root: Token::new(901),
                writable: false,
                git: true,
                conflicts: Box::new([]),
            },
        ]),
    };
    let outcome = OutcomeSpec {
        change: Some(ChangeSpec {
            checks_must_pass: true,
            fields: Box::new([
                FieldRule { name: b"title".as_slice().into(), max: 1024 },
                FieldRule { name: b"body".as_slice().into(), max: 1024 },
            ]),
        }),
        verdicts: Box::new([]),
        report: None,
        failure: None,
    };
    let emitted = h.start_workspace(
        1,
        Charter { outcome, reserve: crate::charter::Reserve { turns: 0, time: LIMITS.check_timeout }, ..charter() },
        Some(checkout),
    );
    let [Request::Admitted { run, .. }, Request::Read { at, .. }] = &*emitted else {
        panic!("expected a read, got {emitted:?}");
    };
    let run = *run;
    assert_eq!(at.root, Token::new(900));
    let emitted = h.step(Event::Read { owner: run, read: Read::Failed });
    let [Request::Probe { owner, at, deadline }] = &*emitted else {
        panic!("expected a probe, got {emitted:?}");
    };
    assert_eq!((owner, at), (&run, &Place { root: Token::new(900), path: bytes(b".temper/pre-pr") }));
    assert_eq!(*deadline, Time::ZERO.saturating_add(LIMITS.io_timeout));
    let emitted = h.step(Event::Probed { owner: run, executable: true });
    let [Request::Read { at, .. }] = &*emitted else {
        panic!("expected a read, got {emitted:?}");
    };
    assert_eq!(at.root, Token::new(901), "a read-only repository has no checks to look for");
    let emitted = h.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Open { opening, .. }] = &*emitted else {
        panic!("expected main to open, got {emitted:?}");
    };
    let system = &opening.system;
    let marked = b"- `temper`, which you may change, a git working tree, with checks (`.temper/pre-pr`)\n";
    assert!(skein_lib::bytes::find(system, marked).is_some(), "the checkout says which has checks");
}

#[test]
fn a_report_only_run_probes_checks_in_its_writable_directory() {
    let mut h = Harness::new(LIMITS);
    let mut mounted = workspace();
    mounted.directories[0].writable = true;
    let emitted = h.start_workspace(1, text_charter(false), Some(mounted));
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else {
        panic!("report run begins discovery: {emitted:?}");
    };
    let run = *run;
    let emitted = h.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Probe { at, .. }] = &*emitted else {
        panic!("writable report directory probes checks: {emitted:?}");
    };
    assert_eq!(at.root, Token::new(900));
    let emitted = h.step(Event::Probed { owner: run, executable: true });
    let [Request::Open { opening, .. }] = &*emitted else {
        panic!("report opens after the probe: {emitted:?}");
    };
    assert!(skein_lib::bytes::find(&opening.system, b"with checks (`.temper/pre-pr`)").is_some());
}

#[test]
fn a_run_with_nothing_to_look_for_opens_main_at_once() {
    let mut h = Harness::new(LIMITS);
    let emitted = h.start_workspace(1, charter(), None);
    let [Request::Admitted { .. }, Request::Open { .. }] = &*emitted else {
        panic!("expected an admitted run and main, got {emitted:?}");
    };
}

#[test]
fn starts_beyond_the_run_or_conversation_slots_are_refused_as_busy() {
    let mut h = Harness::new(Limits { runs: 1, ..LIMITS });
    let _: (Token, Token) = h.admit(1);
    assert_eq!(answered(h.start(2, charter())), (2, Answer::Refused(Refusal::Busy)));

    let mut h = Harness::new(Limits { conversations: 1, ..LIMITS });
    let _: (Token, Token) = h.admit(1);
    assert_eq!(answered(h.start(2, charter())), (2, Answer::Refused(Refusal::Busy)));
    assert_eq!(h.domain.runs(), 1);

    // A charter that can never fit is invalid, room or not.
    let never = Charter { budget: Budget { turns: 0, ..BUDGET }, ..charter() };
    assert_eq!(
        answered(h.start(3, never)),
        (3, Answer::Refused(Refusal::Invalid(Invalid::Budget(crate::BudgetRefusal::Empty(crate::Part::Turns)))))
    );
}

#[test]
fn drawn_budget_ceilings_admit_the_boundary_and_refuse_each_part_before_effects() {
    let mut random = skein_lib::Rng::new(701);
    for _ in 0_u32..128 {
        let ceiling = Budget {
            turns: u32::try_from(random.between(1, 4096)).expect("bounded turns"),
            spend: random.between(1, 1_000_000_000),
            time: Duration::from_secs(random.between(1, 86_400)),
        };
        let limits = Limits { budget: ceiling, ..LIMITS };
        let policy = Charter { budget: ceiling, ..charter() };
        let emitted = Harness::new(limits).start_workspace(7, policy, None);
        let [Request::Admitted { .. }, Request::Open { .. }] = &*emitted else {
            panic!("at ceiling is admitted: {emitted:?}")
        };
        for (budget, reason) in [
            (
                Budget { turns: ceiling.turns.checked_add(1).expect("bounded"), ..ceiling },
                crate::Ceiling::Turns(ceiling.turns),
            ),
            (
                Budget { spend: ceiling.spend.checked_add(1).expect("bounded"), ..ceiling },
                crate::Ceiling::Spend(ceiling.spend),
            ),
            (
                Budget { time: ceiling.time.checked_add(Duration::from_secs(1)).expect("bounded"), ..ceiling },
                crate::Ceiling::Time(ceiling.time),
            ),
        ] {
            let mut harness = Harness::new(limits);
            assert_eq!(
                answered(harness.start(7, Charter { budget, ..charter() })),
                (7, Answer::Refused(Refusal::Invalid(Invalid::Budget(crate::BudgetRefusal::Ceiling(reason)))))
            );
            assert_eq!(harness.domain.runs(), 0);
        }
    }
    for (budget, part) in [
        (Budget { turns: 0, ..BUDGET }, crate::Part::Turns),
        (Budget { spend: 0, ..BUDGET }, crate::Part::Spend),
        (Budget { time: Duration::ZERO, ..BUDGET }, crate::Part::Time),
    ] {
        assert_eq!(
            answered(Harness::new(LIMITS).start(7, Charter { budget, ..charter() })),
            (7, Answer::Refused(Refusal::Invalid(Invalid::Budget(crate::BudgetRefusal::Empty(part)))))
        );
    }
}

#[test]
fn every_main_and_child_model_rate_is_positive_before_any_effect() {
    let positive = crate::Prices { input: 1, cached: 1000, cache_write: Some(3), output: 1, unit: 1_000_000 };
    for (prices, rate) in [
        (crate::Prices { input: 0, ..positive }, crate::Rate::Input),
        (crate::Prices { cached: 0, ..positive }, crate::Rate::Cached),
        (crate::Prices { cache_write: Some(0), ..positive }, crate::Rate::CacheWrite),
        (crate::Prices { output: 0, ..positive }, crate::Rate::Output),
    ] {
        for main in [true, false] {
            let mut policy = agents();
            if main {
                policy.llm.prices = prices;
            } else {
                policy.models[0].prices = prices;
            }
            let mut harness = Harness::new(LIMITS);
            assert_eq!(
                answered(harness.start(7, policy)),
                (7, Answer::Refused(Refusal::Invalid(Invalid::Price(rate))))
            );
            assert_eq!(harness.domain.runs(), 0);
        }
    }
    let mut policy = agents();
    policy.llm.prices = positive;
    policy.models[0].prices = positive;
    let emitted = Harness::new(LIMITS).start_workspace(7, policy, None);
    let [Request::Admitted { .. }, Request::Open { .. }] = &*emitted else {
        panic!("positive model rates are admitted: {emitted:?}")
    };
}

#[test]
fn charters_beyond_the_limits_are_refused_as_invalid() {
    let three: Box<[Directory]> = Box::new([repository(b"a"), repository(b"b"), repository(b"c")]);
    let twins: Box<[Directory]> = Box::new([repository(b"a"), repository(b"a")]);
    let host_tools = Box::new([
        HostTool {
            name: bytes(b"reply"),
            description: b"Host action".as_slice().into(),
            schema: b"{}".as_slice().into(),
            effect: crate::HostEffect::Read,
            timeout: Duration::from_secs(5),
        },
        HostTool {
            name: bytes(b"reply"),
            description: b"Host action".as_slice().into(),
            schema: b"{}".as_slice().into(),
            effect: crate::HostEffect::Read,
            timeout: Duration::from_secs(5),
        },
    ]);
    let llm = Llm {
        effort: None,
        prices: crate::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
        account: Some(0),
        endpoint: Endpoint(2),
        model: bytes(b"model-b"),
        window: 8192,
        output: 512,
        dialect: 1,
    };
    let models = Box::new([llm.clone(), Llm { endpoint: Endpoint(3), ..llm }]);
    for directories in [three, twins] {
        let mut harness = Harness::new(LIMITS);
        assert_eq!(
            answered(harness.start_workspace(9, charter(), Some(Workspace { directories }))),
            (9, Answer::Refused(Refusal::Invalid(Invalid::Workspace)))
        );
    }
    let cases = [
        (Charter { llm: Llm { window: 8192, output: 0, ..charter().llm }, ..charter() }, Invalid::Llm),
        (
            Charter {
                llm: Llm { prices: crate::Prices { unit: 0, ..charter().llm.prices }, ..charter().llm },
                ..charter()
            },
            Invalid::Llm,
        ),
        (Charter { llm: Llm { window: 0, output: 512, ..charter().llm }, ..charter() }, Invalid::Llm),
        (Charter { models, ..charter() }, Invalid::Llm),
        (Charter { instructions: Box::from([b'x'; 4096].as_slice()), ..charter() }, Invalid::TooLarge),
        (Charter { grants: Grants { host_tools, ..charter().grants }, ..charter() }, Invalid::Grants),
        (Charter { outcome: spec(Box::new([])), ..charter() }, Invalid::Outcome),
        (Charter { outcome: spec(Box::new([rule(b"a", 0, 0), rule(b"a", 1, 1)])), ..charter() }, Invalid::Outcome),
        (
            Charter { outcome: spec(Box::new([rule(b"a", 0, 0), rule(b"b", 0, 0), rule(b"c", 0, 0)])), ..charter() },
            Invalid::Outcome,
        ),
        (Charter { outcome: spec(Box::new([rule(b"a", 3, 2)])), ..charter() }, Invalid::Outcome),
        // ItemSpec allowed, with no kind for them to be.
        (
            Charter {
                outcome: spec(Box::new([VerdictRule {
                    items: crate::outcome::ItemSpec { kinds: Box::new([]), ..(rule(b"a", 0, 1)).items },
                    ..rule(b"a", 0, 1)
                }])),
                ..charter()
            },
            Invalid::Outcome,
        ),
    ];
    for (call, (charter, invalid)) in (0_u64..).zip(cases) {
        let mut h = Harness::new(LIMITS);
        assert_eq!(answered(h.start(call, charter)), (call, Answer::Refused(Refusal::Invalid(invalid))));
        assert_eq!(h.domain.runs(), 0);
    }
    // A change alone is an outcome.
    let mut h = Harness::new(LIMITS);
    drop(h.start(
        1,
        Charter {
            reserve: crate::charter::Reserve { turns: 0, time: LIMITS.check_timeout },
            outcome: OutcomeSpec {
                change: Some(ChangeSpec {
                    checks_must_pass: true,
                    fields: Box::new([
                        FieldRule { name: b"title".as_slice().into(), max: 1024 },
                        FieldRule { name: b"body".as_slice().into(), max: 1024 },
                    ]),
                }),
                verdicts: Box::new([]),
                report: None,
                failure: None,
            },
            ..charter()
        },
    ));
    assert_eq!(h.domain.runs(), 1);
}

fn repository(name: &[u8]) -> Directory {
    Directory {
        host: Box::from(b"/host".as_slice()),
        name: bytes(name),
        root: Token::new(1),
        writable: true,
        git: true,
        conflicts: Box::new([]),
    }
}

fn spec(verdicts: Box<[VerdictRule]>) -> OutcomeSpec {
    OutcomeSpec { change: None, verdicts, report: None, failure: None }
}

#[test]
fn a_run_whose_main_conversation_fails_answers_with_its_fault_and_what_it_spent() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    assert!(h.step(Event::Used { conversation, spend: spend(100) }).is_empty(), "within the budget");
    let end = End::Fault(Fault::Provider);
    let emitted = h.step(Event::Ended { conversation, end, spend: spend(100), detail: Box::default() });
    assert_eq!(answered(emitted), (1, failed(Failure::Model(Fault::Provider), spend(100))));
    assert_eq!(h.domain.next_deadline(), None);
    h.domain.reclaim();
    assert_eq!((h.domain.runs(), h.domain.conversations()), (0, 0));
}

#[test]
fn a_run_whose_main_conversation_runs_out_of_budget_fails_for_budget() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    let end = End::Budget(Exhausted::Turns);
    let emitted = h.step(Event::Ended { conversation, end, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget { exhausted: Exhausted::Turns, used: Some(0), bound: Some(u64::from(BUDGET.turns)) },
                Spend::ZERO
            )
        )
    );
}

#[test]
fn a_main_conversation_refused_at_its_entrance_refuses_the_run() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.admit(1);
    let emitted = h.step(Event::Ended { conversation, end: End::Busy, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (1, Answer::Refused(Refusal::Busy)));
    let (_, conversation) = h.admit(2);
    let emitted = h.step(Event::Ended { conversation, end: End::Invalid, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (2, Answer::Refused(Refusal::Invalid(Invalid::Conversation))));
}

#[test]
fn zero_activation_has_its_own_refusal_before_admission() {
    let mut harness = Harness::new(LIMITS);
    let emitted = harness.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
        reply_to: ReplyTo::new(Token::new(81)),
        host_run: Token::new(81),
        identity: None,
        activation: 0,
        charter: charter(),
        workspace: Some(workspace()),
        transcript: None,
        resumed: false,
    });
    assert_eq!(answered(emitted), (81, Answer::Refused(Refusal::Invalid(Invalid::Activation))));
    assert_eq!((harness.domain.runs(), harness.domain.conversations()), (0, 0));
}

#[test]
fn an_llm_that_stops_without_finishing_is_nudged_until_its_nudges_run_out() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    let peer = Token::new(100);
    for nudge in 1..=LIMITS.nudges {
        assert!(h.step(Event::Used { conversation, spend: spend(5) }).is_empty(), "within the budget");
        let text = super::prompt::nudge(crate::Mode::Finite, Stop::EndTurn, nudge, LIMITS.nudges);
        assert_eq!(&*h.step(end_turn(conversation)), &[Request::Say { peer, text }]);
    }
    assert!(h.step(Event::Used { conversation, spend: spend(5) }).is_empty(), "within the budget");
    assert_eq!(&*h.step(end_turn(conversation)), &[Request::Close { peer }]);
    assert_eq!(h.domain.next_deadline(), None);
    // A turn that won the race with the close is spent all the same.
    assert!(h.step(Event::Used { conversation, spend: spend(5) }).is_empty(), "winding down");
    let total = Spend {
        units: 20,
        turns: 4,
        input: Some(20),
        output: Some(40),
        cache_read: Some(0),
        cache_write: Some(0),
        reasoning: None,
    };
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: total, detail: Box::default() });
    let unfinished = Failure::Policy(Policy::Unfinished { nudges: LIMITS.nudges, rejected: 0 });
    assert_eq!(answered(emitted), (1, failed(unfinished, total)));
}

fn end_turn(conversation: Token) -> Event {
    Event::Yielded { conversation, stop: Stop::EndTurn, text: bytes(b"done, I think") }
}

#[test]
fn an_llm_whose_last_stop_shows_a_fault_fails_the_run_with_it() {
    let faults =
        [(Stop::MaxTokens, Fault::Truncated), (Stop::Refusal, Fault::Declined), (Stop::NoCalls, Fault::Malformed)];
    for (stop, fault) in faults {
        let mut h = Harness::new(Limits { nudges: 0, ..LIMITS });
        let (_, conversation) = h.running(1, 100);
        drop(h.step(Event::Yielded { conversation, stop, text: bytes(b"") }));
        let emitted =
            h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
        assert_eq!(answered(emitted), (1, failed(Failure::Model(fault), Spend::ZERO)));
    }
}

#[test]
fn an_llm_with_no_input_or_output_left_is_not_nudged() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend) }));
    assert_eq!(&*h.step(end_turn(conversation)), &[Request::Close { peer: Token::new(100) }]);
    let emitted =
        h.step(Event::Ended { conversation, end: End::Closed, spend: spend(BUDGET.spend), detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget { exhausted: Exhausted::Spend, used: Some(BUDGET.spend), bound: Some(BUDGET.spend) },
                spend(BUDGET.spend)
            )
        )
    );
}

#[test]
fn an_llm_with_no_turn_left_is_not_nudged() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    let all = Spend { turns: BUDGET.turns, ..spend(5) };
    assert!(h.step(Event::Used { conversation, spend: all }).is_empty(), "at the budget");
    let yielded = Event::Yielded { conversation, stop: Stop::EndTurn, text: bytes(b"") };
    assert_eq!(&*h.step(yielded), &[Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: all, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget {
                    exhausted: Exhausted::Turns,
                    used: Some(u64::from(BUDGET.turns)),
                    bound: Some(u64::from(BUDGET.turns))
                },
                all
            )
        )
    );
}

#[test]
fn spending_past_the_budget_keeps_admitted_turns_stable_until_yield() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    assert!(h.step(Event::Used { conversation, spend: spend(BUDGET.spend) }).is_empty(), "at the budget");
    assert!(h.step(Event::Used { conversation, spend: spend(1) }).is_empty(), "past it: main keeps its turn");
    assert!(h.step(Event::Used { conversation, spend: spend(1) }).is_empty());
    assert_eq!(&*h.step(end_turn(conversation)), &[Request::Close { peer: Token::new(100) }]);
    let total = Spend {
        units: BUDGET.spend + 2,
        turns: 3,
        input: Some(BUDGET.spend + 2),
        output: Some(30),
        cache_read: Some(0),
        cache_write: Some(0),
        reasoning: None,
    };
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: total, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget {
                    exhausted: Exhausted::Spend,
                    used: Some(BUDGET.spend + 2),
                    bound: Some(BUDGET.spend)
                },
                total
            )
        )
    );

    // Or it yields, and is closed then.
    let (_, conversation) = h.running(2, 101);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    assert_eq!(&*h.step(end_turn(conversation)), &[Request::Close { peer: Token::new(101) }], "no nudge past it");
}

#[test]
fn what_an_end_counts_beyond_the_turns_used_is_charged() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(10) }));
    let end = End::Fault(Fault::Completion {
        failure: crate::CompletionFailure::Limit { which: crate::Limit::History, bound: 65_536 },
        evidence: crate::CompletionEvidence::Unsent,
        model: 0,
        attempts: 0,
        report: crate::Report::none(),
    });
    let emitted = h.step(Event::Ended { conversation, end, spend: spend(30), detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Model(Fault::Completion {
                    failure: crate::CompletionFailure::Limit { which: crate::Limit::History, bound: 65_536 },
                    evidence: crate::CompletionEvidence::Unsent,
                    model: 0,
                    attempts: 0,
                    report: crate::Report::none(),
                }),
                Spend { units: 10, ..spend(30) }
            )
        )
    );
}

#[test]
fn the_deadline_closes_main_and_fails_the_run_for_time() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    h.after(BUDGET.time);
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended {
        conversation,
        end: End::Budget(Exhausted::Time),
        spend: Spend::ZERO,
        detail: Box::default(),
    });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget { exhausted: Exhausted::Time, used: Some(600_000), bound: Some(600_000) },
                Spend::ZERO
            )
        )
    );
}

#[test]
fn the_worst_case_is_bounded_or_refused() {
    let bytes = worst_case(&LIMITS).expect("the test limits fit");
    assert!(bytes > 2 * LIMITS.run_bytes, "every run may hold its bytes");
    assert_eq!(worst_case(&Limits { runs: u32::MAX, run_bytes: u64::MAX, ..LIMITS }), None);
}

// Finishing.

/// When a conversation of a run started at zero expires: the run's deadline.
const EXPIRY: Time = Time::from_nanos(BUDGET.time.as_nanos());

/// What a run's main conversation asks when it finishes with `outcome`, as
/// its call `call`.
fn finish(conversation: Token, call: u64, outcome: Declared) -> Event {
    finish_by(conversation, call, outcome, EXPIRY)
}

/// The same, with the call due by `deadline`.
fn finish_by(conversation: Token, call: u64, outcome: Declared, deadline: Time) -> Event {
    Event::Delegated {
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        conversation,
        call: Token::new(call),
        ask: Ask::Finish { outcome },
        deadline,
    }
}

fn returned(call: u64, result: Returned) -> Request {
    Request::Return { spent: 0, call: Token::new(call), result }
}

fn verdict(name: &[u8], children: Box<[Item]>) -> Declared {
    Declared::Verdict(Verdict { name: bytes(name), text: bytes(b"Looks good."), items: children, fields: Box::new([]) })
}

fn comment() -> Item {
    let fields = Box::new([
        Field { name: bytes(b"path"), value: bytes(b"a.rs") },
        Field { name: bytes(b"body"), value: bytes(b"Nit.") },
    ]);
    Item { kind: bytes(b"nit"), fields }
}

fn change() -> Change {
    Change {
        fields: Box::new([
            Field { name: b"title".as_slice().into(), value: bytes(b"Fix the parser") },
            Field { name: b"body".as_slice().into(), value: bytes(b"It accepts tabs now.") },
        ]),
    }
}

/// The test charter, finishing with a change whose checks must pass, in two
/// writable repositories.
fn coding_workspace() -> Workspace {
    Workspace {
        directories: Box::new([
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"temper"),
                root: Token::new(900),
                writable: true,
                git: true,
                conflicts: Box::new([]),
            },
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"docs"),
                root: Token::new(901),
                writable: true,
                git: true,
                conflicts: Box::new([]),
            },
        ]),
    }
}

fn coding() -> Charter {
    Charter {
        reserve: crate::charter::Reserve { turns: 0, time: LIMITS.check_timeout },
        outcome: OutcomeSpec {
            change: Some(ChangeSpec {
                checks_must_pass: true,
                fields: Box::new([
                    FieldRule { name: b"title".as_slice().into(), max: 1024 },
                    FieldRule { name: b"body".as_slice().into(), max: 1024 },
                ]),
            }),
            verdicts: Box::new([]),
            report: None,
            failure: None,
        },
        ..charter()
    }
}

impl Harness {
    /// Starts a run of `coding()` for call `call` whose repositories both have
    /// checks, and its main conversation as `peer`: the run's token and main's.
    fn coding(&mut self, call: u64, peer: u64) -> (Token, Token) {
        self.coding_policy(call, peer, coding())
    }

    fn coding_policy(&mut self, call: u64, peer: u64, policy: Charter) -> (Token, Token) {
        let emitted = self.start(call, policy);
        let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else {
            panic!("expected a read, got {emitted:?}");
        };
        let run = *run;
        drop(self.step(Event::Read { owner: run, read: Read::Missing }));
        drop(self.step(Event::Probed { owner: run, executable: true }));
        drop(self.step(Event::Read { owner: run, read: Read::Missing }));
        let emitted = self.step(Event::Probed { owner: run, executable: true });
        let [Request::Open { conversation, .. }] = &*emitted else {
            panic!("expected main to open, got {emitted:?}");
        };
        let conversation = *conversation;
        drop(self.step(Event::Started { conversation, peer: Token::new(peer) }));
        (run, conversation)
    }

    /// Has main finish with `change()` as call `call`: the owner of the
    /// landing, whose first check is in flight.
    fn land(&mut self, conversation: Token, call: u64) -> Token {
        let emitted = self.step(finish(conversation, call, Declared::Change(change())));
        let [Request::Check { owner, program, deadline, tail }, Request::Checking { host_run: _, deadline: until }] =
            &*emitted
        else {
            panic!("expected the first check, got {emitted:?}");
        };
        assert_eq!(program, &Place { root: Token::new(900), path: bytes(b".temper/pre-pr") });
        assert_eq!((*tail, deadline), (LIMITS.check_tail, until));
        assert_eq!(*deadline, self.env.now.saturating_add(LIMITS.check_timeout));
        *owner
    }
}

fn ran(code: u8, output: &[u8]) -> Ran {
    Ran { exit: Exit::Code { code }, output: bytes(output), cut: 0 }
}

#[test]
fn an_outcome_that_does_not_fit_the_spec_is_rejected_and_the_run_goes_on() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    let emitted = h.step(finish(conversation, 7, Declared::Change(change())));
    let problems = Problems { listed: Box::new([Problem::ChangeNotAllowed]), more: 0 };
    assert_eq!(&*emitted, &[returned(7, Returned::Rejected { problems })]);
    let huge = verdict(b"approve", Box::new([]));
    let Declared::Verdict(mut huge) = huge else { unreachable!("a verdict") };
    huge.text = Box::from([b'x'; 2000].as_slice());
    let emitted = h.step(finish(conversation, 8, Declared::Verdict(huge)));
    let problems = Problems { listed: Box::new([Problem::TooLarge { max: LIMITS.outcome_bytes }]), more: 0 };
    assert_eq!(&*emitted, &[returned(8, Returned::Rejected { problems })]);
    // Nudged out, the run fails as unfinished, counting what it rejected.
    for _ in 0..LIMITS.nudges {
        drop(h.step(end_turn(conversation)));
    }
    assert_eq!(&*h.step(end_turn(conversation)), &[Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    let unfinished = Failure::Policy(Policy::Unfinished { nudges: LIMITS.nudges, rejected: 2 });
    assert_eq!(answered(emitted), (1, failed(unfinished, Spend::ZERO)));
}

#[test]
fn a_verdict_that_fits_is_accepted_and_the_run_finishes_with_it() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    let outcome = verdict(b"request", Box::new([comment()]));
    let emitted = h.step(finish(conversation, 7, outcome));
    assert_eq!(&*emitted, &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: spend(5), detail: Box::default() });
    let accepted = Answer::Accepted {
        outcome: verdict(b"request", Box::new([comment()])),
        spent: Spend { units: 0, ..spend(5) },
        turns: 0,
    };
    assert_eq!(answered(emitted), (1, accepted));
}

#[test]
fn a_change_runs_each_repositorys_checks_then_is_pushed_and_accepted() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    let held = crate::lease::Lease::Workspace { owner };
    assert_eq!(h.domain.runs.get(skein_lib::Id::from_token(run)).unwrap().lease, held);
    let emitted = h.step(Event::Checked { owner, ran: ran(0, b"ok") });
    let [Request::Check { owner: second, program, .. }, Request::Checking { .. }] = &*emitted else {
        panic!("expected the second check, got {emitted:?}");
    };
    assert_eq!((second, program.root), (&owner, Token::new(901)));
    assert_eq!(h.domain.runs.get(skein_lib::Id::from_token(run)).unwrap().lease, held);
    let emitted = h.step(Event::Checked { owner, ran: ran(0, b"ok") });
    assert_eq!(
        &*emitted,
        &[Request::Deliver {
            host_run: Token::new(1),
            owner,
            change: change(),
            name: crate::CallName { activation: 1, completion: 1, position: 0 },
            deadline: h.env.now.saturating_add(LIMITS.delivery_timeout).min(EXPIRY)
        }]
    );
    assert_eq!(h.domain.calls(), 1);
    assert_eq!(h.domain.runs.get(skein_lib::Id::from_token(run)).unwrap().lease, held);
    let emitted = h.step(Event::Delivered { owner, delivery: delivered() });
    assert_eq!(h.domain.runs.get(skein_lib::Id::from_token(run)).unwrap().lease, crate::lease::Lease::Free);
    assert_eq!(&*emitted, &[returned(7, Returned::Delivered(receipts())), Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    let accepted = Answer::Accepted { outcome: Declared::Change(change()), spent: Spend::ZERO, turns: 0 };
    assert_eq!(answered(emitted), (1, accepted));
    h.domain.reclaim();
    assert_eq!((h.domain.runs(), h.domain.conversations(), h.domain.calls()), (0, 0, 0));
}

#[test]
fn a_change_that_fails_its_checks_or_its_push_goes_back_to_the_llm() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    let failing = Ran { exit: Exit::Code { code: 1 }, output: bytes(b"test parse ... FAILED"), cut: 12 };
    let emitted = h.step(Event::Checked { owner, ran: failing });
    let failing = Ran { exit: Exit::Code { code: 1 }, output: bytes(b"test parse ... FAILED"), cut: 12 };
    assert_eq!(&*emitted, &[returned(7, Returned::ChecksFailed { repository: bytes(b"temper"), ran: failing })]);
    let owner = h.land(conversation, 8);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    assert_eq!(
        &*h.step(Event::Delivered {
            owner,
            delivery: Delivery::Failed(crate::DeliveryFailure::new(0, crate::DeliveryReason::Unknown))
        }),
        &[returned(
            8,
            Returned::DeliveryFailed { failure: crate::DeliveryFailure::new(0, crate::DeliveryReason::Unknown) }
        )]
    );
    assert!(h.step(end_turn(conversation)).len() == 1, "the run goes on: a nudge");
}

#[test]
fn a_change_can_be_delivered_after_failing_checks_when_its_contract_allows_it() {
    let mut harness = Harness::new(LIMITS);
    let mut policy = coding();
    policy.outcome.change.as_mut().expect("change contract").checks_must_pass = false;
    let (_, conversation) = harness.coding_policy(1, 100, policy);
    let owner = harness.land(conversation, 7);
    let requests = harness.step(Event::Checked { owner, ran: ran(1, b"first failed") });
    let [Request::Check { program, .. }, Request::Checking { .. }] = requests.as_ref() else {
        panic!("the second check still runs: {requests:?}");
    };
    assert_eq!(program.root, Token::new(901));
    let requests = harness.step(Event::Checked {
        owner,
        ran: Ran { exit: Exit::Signalled { signal: 15 }, output: bytes(b"second failed"), cut: 0 },
    });
    let [Request::Deliver { .. }] = requests.as_ref() else { panic!("delivery follows both checks: {requests:?}") };
    let mut check_exits = List::with_capacity(2);
    for fact in facts(&mut harness).as_ref() {
        match fact {
            Fact::CheckFinished { exit, .. } => check_exits.push(*exit).expect("two requested checks"),
            Fact::MessageRead { .. }
            | Fact::MessageFence { .. }
            | Fact::MessageUnread { .. }
            | Fact::MessageReceived { .. }
            | Fact::MessageRefused { .. }
            | Fact::Admitted { .. }
            | Fact::Prepared { .. }
            | Fact::Opened { .. }
            | Fact::Budget { .. }
            | Fact::Ended { .. }
            | Fact::Called { .. }
            | Fact::Returned { .. }
            | Fact::CheckStarted { .. }
            | Fact::Delivered { .. }
            | Fact::Answered { .. } => {}
        }
    }
    assert_eq!(check_exits.as_slice(), [Exit::Code { code: 1 }, Exit::Signalled { signal: 15 }]);
    assert_eq!(
        harness.step(Event::Delivered { owner, delivery: delivered() }).as_ref(),
        [returned(7, Returned::Delivered(receipts())), Request::Close { peer: Token::new(100) }]
    );
    assert_eq!(
        answered(harness.step(Event::Ended {
            conversation,
            end: End::Closed,
            spend: Spend::ZERO,
            detail: Box::default()
        })),
        (1, Answer::Accepted { outcome: Declared::Change(change()), spent: Spend::ZERO, turns: 0 })
    );
}

#[test]
fn a_change_whose_branch_moved_ends_the_run_as_stale() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    let emitted = h.step(Event::Delivered { owner, delivery: Delivery::Stale });
    assert_eq!(&*emitted, &[returned(7, Returned::Stale), Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (1, failed(Failure::Stale, Spend::ZERO)));
}

#[test]
fn past_the_budget_the_deadline_fails_the_run_for_the_part_it_went_past() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    h.after(BUDGET.time);
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(100) }]);
    let emitted =
        h.step(Event::Ended { conversation, end: End::Closed, spend: spend(BUDGET.spend + 1), detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget {
                    exhausted: Exhausted::Spend,
                    used: Some(BUDGET.spend + 1),
                    bound: Some(BUDGET.spend)
                },
                spend(BUDGET.spend + 1)
            )
        )
    );
}

#[test]
fn a_guide_that_is_not_text_is_not_there() {
    let mut h = Harness::new(LIMITS);
    let run = h.prepare(1);
    let emitted = h.step(Event::Read { owner: run, read: Read::NotText });
    let [Request::Open { opening, .. }] = &*emitted else { panic!("expected main to open, got {emitted:?}") };
    assert_eq!(
        opening.system,
        super::prompt::system(&charter(), Some(&workspace()), &Found::with_capacity(1), opening.budget, false)
    );
}

#[test]
fn a_withdrawn_landing_stops_what_is_in_flight_and_returns_once_it_has() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    assert!(h.step(Event::Withdraw { conversation, call: Token::new(6) }).is_empty(), "not the landing call");
    assert_eq!(&*h.step(Event::Withdraw { conversation, call: Token::new(7) }), &[Request::Abort { owner }]);
    assert_eq!(&*h.step(Event::Aborted { owner }), &[returned(7, Returned::Cancelled)]);
    assert_eq!(h.checks_ended, 1, "abort terminal closes the check span");
    assert!(h.step(Event::Withdraw { conversation, call: Token::new(7) }).is_empty(), "returned already");
    h.domain.reclaim();

    // Withdrawn while pushing, and the push wins the race: it landed.
    let owner = h.land(conversation, 8);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    assert!(
        h.step(Event::Withdraw { conversation, call: Token::new(8) }).is_empty(),
        "submitted delivery is never abandoned"
    );
    let emitted = h.step(Event::Delivered { owner, delivery: delivered() });
    assert_eq!(&*emitted, &[returned(8, Returned::Delivered(receipts())), Request::Close { peer: Token::new(100) }]);
}

#[test]
fn a_landing_past_its_deadline_is_stopped_and_returns_timed_out_once_it_has() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let minute = Duration::from_secs(60);
    let emitted = h.step(finish_by(conversation, 7, Declared::Change(change()), Time::ZERO.saturating_add(minute)));
    let [Request::Check { owner, deadline, .. }, Request::Checking { .. }] = &*emitted else {
        panic!("expected the first check, got {emitted:?}");
    };
    // A check may not outlive its caller's earlier deadline.
    assert_eq!(*deadline, Time::ZERO.saturating_add(minute).min(Time::ZERO.saturating_add(LIMITS.check_timeout)));
    let owner = *owner;
    h.after(minute);
    assert_eq!(&*h.fire(), &[Request::Abort { owner }]);
    assert!(!h.domain.is_due(h.env.now), "a call's deadline fires once");
    // Checks that pass before the abort lands push nothing.
    assert_eq!(&*h.step(Event::Checked { owner, ran: ran(0, b"") }), &[returned(7, Returned::TimedOut)]);
    assert_eq!(h.step(end_turn(conversation)).len(), 1, "the run goes on: a nudge");
    h.domain.reclaim();

    // Past its deadline while it is pushed.
    let deadline = h.env.now.saturating_add(minute);
    let emitted = h.step(finish_by(conversation, 8, Declared::Change(change()), deadline));
    let [Request::Check { owner, .. }, Request::Checking { .. }] = &*emitted else {
        panic!("expected the first check, got {emitted:?}");
    };
    let owner = *owner;
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    h.after(minute);
    assert!(h.fire().is_empty(), "call expiry retains the submitted host operation");
    assert_eq!(
        &*h.step(Event::Delivered {
            owner,
            delivery: Delivery::Failed(crate::DeliveryFailure::new(0, crate::DeliveryReason::TimedOut))
        }),
        &[returned(
            8,
            Returned::DeliveryFailed { failure: crate::DeliveryFailure::new(0, crate::DeliveryReason::TimedOut) }
        )]
    );
    h.domain.reclaim();
    assert_eq!(h.domain.calls(), 0);
}

#[test]
fn past_the_budget_a_finish_in_the_turn_in_flight_still_counts() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    let emitted = h.step(finish(conversation, 7, verdict(b"approve", Box::new([]))));
    assert_eq!(&*emitted, &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]);
    let total = spend(BUDGET.spend + 1);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: total, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (1, Answer::Accepted { outcome: verdict(b"approve", Box::new([])), spent: total, turns: 0 })
    );

    // A refused one closes main, and the run fails for budget.
    let (_, conversation) = h.running(2, 101);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    let emitted = h.step(finish(conversation, 8, verdict(b"reject", Box::new([]))));
    let problems = Problems { listed: Box::new([Problem::UnknownVerdict]), more: 0 };
    assert_eq!(&*emitted, &[returned(8, Returned::Rejected { problems }), Request::Close { peer: Token::new(101) }]);
}

#[test]
fn a_run_out_of_time_while_it_prepares_answers_once_its_look_has_ended() {
    let mut h = Harness::new(LIMITS);
    let run = h.prepare(1);
    h.after(BUDGET.time);
    assert!(h.fire().is_empty(), "the look is in flight");
    let emitted = h.step(Event::Read { owner: run, read: Read::Missing });
    assert_eq!(
        answered(emitted),
        (1, failed(Failure::Budget { exhausted: Exhausted::Time, used: None, bound: None }, Spend::ZERO))
    );
}

// A cancel in every state.

fn cancelled() -> Answer {
    failed(Failure::Cancelled, Spend::ZERO)
}

#[test]
fn a_cancel_while_preparing_stops_the_run_once_its_look_has_ended() {
    let mut h = Harness::new(LIMITS);
    let run = h.prepare(1);
    assert!(h.step(Event::Cancel { run }).is_empty(), "the look is in flight");
    assert_eq!(h.domain.next_deadline(), None);
    assert!(h.step(Event::Cancel { run }).is_empty(), "stopping: the ending is decided");
    let emitted = h.step(Event::Read { owner: run, read: Read::Missing });
    assert_eq!(answered(emitted), (1, cancelled()));
    h.domain.reclaim();
    assert_eq!((h.domain.runs(), h.domain.conversations()), (0, 0), "main was never opened, and is gone too");
}

#[test]
fn a_cancel_while_main_opens_closes_main_once_it_starts() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.admit(1);
    assert!(h.step(Event::Cancel { run }).is_empty(), "main has no peer to close yet");
    assert!(h.step(Event::Cancel { run }).is_empty(), "the ending is decided");
    let peer = Token::new(100);
    assert_eq!(&*h.step(Event::Started { conversation, peer }), &[Request::Close { peer }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (1, cancelled()));

    // Or main is refused, and the cancel is still how the run ends.
    let (run, conversation) = h.admit(2);
    drop(h.step(Event::Cancel { run }));
    let emitted = h.step(Event::Ended { conversation, end: End::Busy, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (2, cancelled()));
}

#[test]
fn a_cancel_while_main_works_closes_it() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.running(1, 100);
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(100) }]);
    // The conversation failed before it saw the close: the cancel still wins.
    let emitted = h.step(Event::Ended {
        conversation,
        end: End::Fault(Fault::Provider),
        spend: Spend::ZERO,
        detail: Box::default(),
    });
    assert_eq!(answered(emitted), (1, cancelled()));
}

#[test]
fn a_cancel_while_main_is_over_the_budget_closes_it() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.running(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(100) }]);
    let spent = spend(BUDGET.spend + 1);
    let emitted =
        h.step(Event::Ended { conversation, end: End::Budget(Exhausted::Spend), spend: spent, detail: Box::default() });
    assert_eq!(answered(emitted), (1, failed(Failure::Cancelled, spent)));
}

#[test]
fn a_cancel_while_a_change_is_checked_stops_the_checks_once_main_withdraws_its_call() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(100) }]);
    assert_eq!(&*h.step(Event::Withdraw { conversation, call: Token::new(7) }), &[Request::Abort { owner }]);
    // The checks pass before the abort lands: nothing is pushed.
    assert_eq!(&*h.step(Event::Checked { owner, ran: ran(0, b"") }), &[returned(7, Returned::Cancelled)]);
    assert_eq!(h.checks_ended, 1, "the losing abort still closes the check span");
    // A finish that crossed the close is cancelled too.
    let crossed = finish(conversation, 8, verdict(b"approve", Box::new([])));
    assert_eq!(&*h.step(crossed), &[returned(8, Returned::Cancelled)]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (1, cancelled()));
}

#[test]
fn a_push_that_lands_while_a_cancel_closes_main_wins_over_it() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(100) }]);
    assert!(
        h.step(Event::Withdraw { conversation, call: Token::new(7) }).is_empty(),
        "submitted delivery is never abandoned"
    );
    // The host terminal reports a landing while run cancellation settles.
    assert_eq!(
        &*h.step(Event::Delivered { owner, delivery: delivered() }),
        &[returned(7, Returned::Delivered(receipts()))]
    );
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    let accepted = Answer::Accepted { outcome: Declared::Change(change()), spent: Spend::ZERO, turns: 0 };
    assert_eq!(answered(emitted), (1, accepted));

    // Or the host operation times out while run cancellation settles.
    // Cancellation does not abandon that submitted operation or invent its result.
    let (run, conversation) = h.coding(2, 101);
    let owner = h.land(conversation, 8);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Cancel { run }));
    drop(h.step(Event::Withdraw { conversation, call: Token::new(8) }));
    assert_eq!(
        &*h.step(Event::Delivered {
            owner,
            delivery: Delivery::Failed(crate::DeliveryFailure::new(0, crate::DeliveryReason::TimedOut))
        }),
        &[returned(
            8,
            Returned::DeliveryFailed { failure: crate::DeliveryFailure::new(0, crate::DeliveryReason::TimedOut) }
        )]
    );
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (2, cancelled()));
}

#[test]
fn a_push_that_lands_after_the_deadline_wins_over_it() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    h.after(BUDGET.time);
    // The run's deadline is main's expiry, and its call's deadline.
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(100) }]);
    assert!(h.fire().is_empty(), "call expiry retains the submitted host operation");
    // The push wins the race with its cancel.
    assert_eq!(
        &*h.step(Event::Delivered { owner, delivery: delivered() }),
        &[returned(7, Returned::Delivered(receipts()))]
    );
    assert!(h.step(Event::Withdraw { conversation, call: Token::new(7) }).is_empty(), "returned already");
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    let accepted = Answer::Accepted { outcome: Declared::Change(change()), spent: Spend::ZERO, turns: 0 };
    assert_eq!(answered(emitted), (1, accepted));

    // Over the budget, too.
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    drop(h.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
    let owner = h.land(conversation, 8);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    h.after(BUDGET.time);
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(100) }]);
    assert!(h.fire().is_empty(), "call expiry retains the submitted host operation");
    assert!(h.step(Event::Withdraw { conversation, call: Token::new(8) }).is_empty(), "stopped already");
    assert_eq!(
        &*h.step(Event::Delivered { owner, delivery: delivered() }),
        &[returned(8, Returned::Delivered(receipts()))]
    );
    let total = spend(BUDGET.spend + 1);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: total, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (1, Answer::Accepted { outcome: Declared::Change(change()), spent: total, turns: 0 })
    );
}

#[test]
fn checks_that_pass_once_the_run_winds_down_push_nothing() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    h.after(BUDGET.time);
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(100) }]);
    // Main's caller deadline equals the run's time budget and has passed;
    // this pre-submission check returns timeout without any host delivery.
    assert_eq!(&*h.step(Event::Checked { owner, ran: ran(0, b"") }), &[returned(7, Returned::TimedOut)]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget { exhausted: Exhausted::Time, used: Some(600_000), bound: Some(600_000) },
                Spend::ZERO
            )
        )
    );
}

#[test]
fn a_cancel_while_the_run_winds_down_changes_nothing() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.running(1, 100);
    h.after(BUDGET.time);
    drop(h.fire());
    assert!(h.step(Event::Cancel { run }).is_empty(), "the ending is decided");
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget { exhausted: Exhausted::Time, used: Some(600_000), bound: Some(600_000) },
                Spend::ZERO
            )
        )
    );

    // Winding down to an accepted outcome, too.
    let (run, conversation) = h.running(2, 101);
    drop(h.step(finish(conversation, 7, verdict(b"approve", Box::new([])))));
    assert!(h.step(Event::Cancel { run }).is_empty(), "the ending is decided");
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    let accepted = Answer::Accepted { outcome: verdict(b"approve", Box::new([])), spent: Spend::ZERO, turns: 0 };
    assert_eq!(answered(emitted), (2, accepted));
}

#[test]
fn a_cancel_of_a_run_that_has_answered_changes_nothing() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.running(1, 100);
    drop(h.step(Event::Ended {
        conversation,
        end: End::Fault(Fault::Provider),
        spend: Spend::ZERO,
        detail: Box::default(),
    }));
    assert!(h.step(Event::Cancel { run }).is_empty(), "answered, not reclaimed yet");
    h.domain.reclaim();
    assert!(h.step(Event::Cancel { run }).is_empty(), "answered and gone");
    // Its slot taken by another run, the old name still finds nothing.
    let (_, _) = h.running(2, 101);
    assert!(h.step(Event::Cancel { run }).is_empty(), "a stale name");
    assert_eq!(h.domain.runs(), 1);
}

#[test]
fn a_finish_with_no_room_for_its_call_is_busy_and_the_run_goes_on() {
    let mut h = Harness::new(Limits { calls: 1, ..LIMITS });
    let (_, first) = h.coding(1, 100);
    let _: Token = h.land(first, 7);
    let (_, second) = h.coding(2, 101);
    let emitted = h.step(finish(second, 8, Declared::Change(change())));
    assert_eq!(&*emitted, &[returned(8, Returned::Busy)]);
    assert_eq!(h.domain.calls(), 1);
    assert_eq!(h.step(end_turn(second)).len(), 1, "the run goes on: a nudge");
}

// Sub-agents.

fn refused(refusal: AskRefusal) -> Returned {
    Returned::Refused { refusal }
}

fn families(inspect: bool, modify: bool, agents: bool) -> crate::charter::Families {
    crate::charter::Families { tools: Tools { inspect, modify, shell: false }, agents }
}

/// The test charter, granting sub-agents and listing one LLM for them.
fn agents() -> Charter {
    let grants = Grants { agents: true, ..charter().grants };
    let models = Box::new([Llm {
        effort: None,
        prices: crate::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
        account: Some(0),
        endpoint: Endpoint(2),
        model: bytes(b"model-b"),
        window: 8192,
        output: 512,
        dialect: 1,
    }]);
    Charter { grants, models, ..charter() }
}

fn ask(
    conversation: Token,
    call: u64,
    wanted: crate::charter::Families,
    llm: Option<Box<[u8]>>,
    share: Option<crate::Share>,
) -> Event {
    let ask = Ask::SubAgent {
        brief: bytes(b"Find the parser."),
        families: wanted,
        llm,
        share: share.unwrap_or(crate::Share::UNASKED),
    };
    Event::Delegated {
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        conversation,
        call: Token::new(call),
        ask,
        deadline: EXPIRY,
    }
}

/// What `conversation` asks for a sub-agent that may inspect, as its call
/// `call` due by `deadline`.
fn ask_by(conversation: Token, call: u64, deadline: Time) -> Event {
    let ask = Ask::SubAgent {
        brief: bytes(b"Find it."),
        families: families(true, false, false),
        llm: None,
        share: crate::Share::UNASKED,
    };
    Event::Delegated {
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        conversation,
        call: Token::new(call),
        ask,
        deadline,
    }
}

impl Harness {
    /// Starts a run of `charter` for call `call`, finding no guide, and its
    /// main conversation as `peer`: the run's token and main's.
    fn running_on(&mut self, call: u64, peer: u64, charter: Charter) -> (Token, Token) {
        let emitted = self.start(call, charter);
        let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else {
            panic!("expected a read, got {emitted:?}");
        };
        let run = *run;
        let emitted = self.step(Event::Read { owner: run, read: Read::Missing });
        let [Request::Open { conversation, .. }] = &*emitted else {
            panic!("expected main to open, got {emitted:?}");
        };
        let conversation = *conversation;
        drop(self.step(Event::Started { conversation, peer: Token::new(peer) }));
        (run, conversation)
    }

    /// Has `asker` ask for a sub-agent as `call`, started as `peer`: its
    /// opening and its token.
    fn child(&mut self, asker: Token, call: u64, wanted: crate::charter::Families, peer: u64) -> (Token, Opening) {
        let emitted = self.step(ask(asker, call, wanted, None, None));
        let Ok(one) = Box::<[Request; 1]>::try_from(emitted) else { panic!("expected one request") };
        let [Request::Open { conversation, opening }] = *one else { panic!("expected the sub-agent to open") };
        drop(self.step(Event::Started { conversation, peer: Token::new(peer) }));
        (conversation, opening)
    }
}

#[test]
fn a_child_closed_after_its_last_charge_tells_the_budget_of_its_return() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let (child, _) = h.child(main, 7, families(true, false, false), 101);
    drop(h.step(Event::Used { conversation: child, spend: spend(50) }));
    drop(h.step(Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"Found.") }));
    h.after(Duration::from_nanos(2_000_000_500));
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: spend(50), detail: Box::default() });
    let [Request::Return { result: Returned::Answered { left, .. }, .. }] = &*emitted else {
        panic!("expected the child's answer, got {emitted:?}");
    };
    assert_eq!(*left, Budget { turns: 9, spend: 9_950, time: Duration::from_millis(597_999) });
    let mut told = List::with_capacity(2);
    for fact in &facts(&mut h) {
        if let Fact::Budget { standing, call: Some(call), .. } = fact {
            told.push((*call, standing.time)).expect("two slots admit any duplicate");
        }
    }
    assert_eq!(told.as_slice(), &[(Token::new(7), Duration::from_millis(2_001))], "one snapshot names the call");
}

#[test]
fn a_snapshot_rounds_used_time_up_to_a_whole_millisecond() {
    for (nanos, millis) in [(0, 0), (1, 1), (1_000_000, 1), (2_000_000_500, 2_001)] {
        let mut h = Harness::new(LIMITS);
        let (_, main) = h.running_on(1, 100, agents());
        h.after(Duration::from_nanos(nanos));
        drop(h.step(Event::Used { conversation: main, spend: spend(50) }));
        let mut snapshots = List::with_capacity(2);
        for fact in &facts(&mut h) {
            if let Fact::Budget { standing, .. } = fact {
                snapshots.push(standing.time).expect("two slots admit any duplicate");
            }
        }
        assert_eq!(snapshots.as_slice(), &[Duration::from_millis(millis)]);
    }
}

#[test]
fn a_childs_return_pushes_one_budget_even_with_its_stage_change() {
    let mut h = Harness::new(LIMITS);
    let mut charter = agents();
    charter.reserve.turns = 2;
    let (_, main) = h.running_on(1, 100, charter);
    let (child, _) = h.child(main, 7, families(true, false, false), 101);
    drop(h.step(Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"Found.") }));
    drop(facts(&mut h));
    drop(h.step(Event::Ended {
        conversation: child,
        end: End::Closed,
        spend: Spend { turns: 8, ..spend(50) },
        detail: Box::default(),
    }));
    let mut snapshots = List::with_capacity(2);
    for fact in &facts(&mut h) {
        if let Fact::Budget { standing, call, .. } = fact {
            snapshots.push((standing.stage, *call)).expect("two slots admit any duplicate");
        }
    }
    assert_eq!(snapshots.as_slice(), &[(crate::Stage::WindDown, Some(Token::new(7)))]);
}

#[test]
fn mains_end_names_no_call_in_its_budget() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    drop(h.step(finish(main, 8, verdict(b"approve", Box::new([])))));
    drop(facts(&mut h));
    drop(h.step(Event::Ended { conversation: main, end: End::Closed, spend: spend(50), detail: Box::default() }));
    let mut snapshots = List::with_capacity(2);
    for fact in &facts(&mut h) {
        if let Fact::Budget { call, .. } = fact {
            snapshots.push(*call).expect("two slots admit any duplicate");
        }
    }
    assert_eq!(snapshots.as_slice(), &[None]);
}

#[test]
fn a_sub_agent_answers_with_its_last_message_once_it_has_ended() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    drop(h.step(Event::Used { conversation: main, spend: spend(100) }));
    let (child, opening) = h.child(main, 7, families(true, false, false), 101);
    assert_eq!((&opening.llm, opening.finish, opening.families), (&agents().llm, false, families(true, false, false)));
    let left = Budget { turns: BUDGET.turns - 1, spend: BUDGET.spend - 100, ..BUDGET };
    assert_eq!(opening.budget, left, "its share is what the run has left");
    assert!(opening.system.starts_with(b"Find the parser."), "its brief is its asker's");
    drop(h.step(Event::Used { conversation: child, spend: spend(50) }));
    let yielded =
        Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"The parser is in src/parse.rs.") };
    assert_eq!(&*h.step(yielded), &[Request::Close { peer: Token::new(101) }], "a sub-agent that yields is done");
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: spend(50), detail: Box::default() });
    let answer = Returned::Answered {
        text: bytes(b"The parser is in"),
        cut: 14,
        stop: Stop::EndTurn,
        used: crate::facts::Used { turns: 1, time: Duration::ZERO, spend: 50 },
        left: Budget { turns: 8, spend: 9850, ..BUDGET },
    };
    assert_eq!(
        &*emitted,
        &[Request::Return { call: Token::new(7), result: answer, spent: 50 }],
        "its answer is cut at the limit and carries the exact priced subtree bill"
    );
    h.domain.reclaim();
    assert_eq!((h.domain.conversations(), h.domain.calls()), (1, 0));
    let mut returned = 0_u32;
    while let Some(fact) = h.pop_fact() {
        if let crate::facts::FactKind::Returned { call, result: Return::Answered { cut, .. }, .. } = fact.kind
            && call == Token::new(7)
        {
            assert_eq!(cut, 14, "actual child cap omission");
            returned += 1;
        }
    }
    assert_eq!(returned, 1, "the actual child cap reaches its fact once");
}

#[test]
fn writing_children_hold_the_lease_until_their_actual_end_even_after_withdrawal() {
    for tools in
        [Tools { inspect: true, modify: true, shell: false }, Tools { inspect: true, modify: false, shell: true }]
    {
        let mut harness = Harness::new(LIMITS);
        let mut charter = agents();
        charter.grants.tools.modify = true;
        let (run, main) = harness.running_on(1, 100, charter);
        let run = skein_lib::Id::from_token(run);
        assert_eq!(harness.domain.runs.get(run).unwrap().lease, crate::lease::Lease::Free);
        let (child, _) = harness.child(main, 7, crate::charter::Families { tools, agents: false }, 101);
        let call = harness.domain.calls.find(skein_lib::Id::from_token(main), Token::new(7)).unwrap();
        let held = crate::lease::Lease::Child { call };
        assert_eq!(harness.domain.runs.get(run).unwrap().lease, held);
        assert_eq!(
            harness.step(Event::Withdraw { conversation: main, call: Token::new(7) }).as_ref(),
            &[Request::Close { peer: Token::new(101) }]
        );
        assert_eq!(harness.domain.runs.get(run).unwrap().lease, held);
        assert_eq!(
            harness
                .step(Event::Ended {
                    conversation: child,
                    end: End::Closed,
                    spend: Spend::ZERO,
                    detail: Box::default()
                })
                .len(),
            1
        );
        assert_eq!(harness.domain.runs.get(run).unwrap().lease, crate::lease::Lease::Free);
    }
}

#[test]
fn inspect_only_children_keep_the_lease_free_while_their_calls_overlap() {
    let mut harness = Harness::new(LIMITS);
    let (run, main) = harness.running_on(1, 100, agents());
    let run = skein_lib::Id::from_token(run);
    let (first, _) = harness.child(main, 7, families(true, false, false), 101);
    let (second, _) = harness.child(main, 8, families(true, false, false), 102);
    assert_eq!(harness.domain.calls(), 2);
    assert_eq!(harness.domain.runs.get(run).unwrap().lease, crate::lease::Lease::Free);
    for child in [second, first] {
        drop(harness.step(Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"found") }));
        drop(harness.step(Event::Ended {
            conversation: child,
            end: End::Closed,
            spend: Spend::ZERO,
            detail: Box::default(),
        }));
        assert_eq!(harness.domain.runs.get(run).unwrap().lease, crate::lease::Lease::Free);
    }
}

#[test]
#[should_panic(expected = "one write holder over overlapping directories")]
fn opening_a_second_writing_child_is_a_broken_schedule() {
    let mut harness = Harness::new(LIMITS);
    let mut charter = agents();
    charter.grants.tools.modify = true;
    let (_, main) = harness.running_on(1, 100, charter);
    drop(harness.child(main, 7, families(true, true, false), 101));
    drop(harness.child(
        main,
        8,
        crate::charter::Families { tools: Tools { inspect: true, modify: false, shell: true }, agents: false },
        102,
    ));
}

#[test]
fn simultaneous_sessions_reserve_one_run_budget_and_return_unused_credit() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running_on(1, 100, agents());
    let (child, _) = harness.child(main, 7, families(true, false, false), 101);
    let first = Token::new(801);
    let second = Token::new(802);

    assert_eq!(crate::reserve(&mut harness.domain, main, first, crate::Most::Bounded(6_000)), Ok(()));
    assert_eq!(crate::reserve(&mut harness.domain, child, second, crate::Most::Bounded(5_000)), Err(Exhausted::Spend));
    assert!(crate::settle_reservation(&mut harness.domain, main, first, 2_000));
    drop(harness.step(Event::Priced { conversation: main, own_spent: 2_000, subtree_spent: 2_000 }));
    assert_eq!(crate::reserve(&mut harness.domain, child, second, crate::Most::Bounded(5_000)), Ok(()));
    assert!(!crate::settle_reservation(&mut harness.domain, child, second, 5_001));
    assert_eq!(crate::reserve(&mut harness.domain, child, Token::new(803), crate::Most::Bounded(8_000)), Ok(()));
}

#[test]
fn a_sub_agent_runs_on_the_llm_named_for_it_among_the_charters() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let emitted = h.step(ask(main, 7, families(true, false, false), Some(bytes(b"model-b")), None));
    let [Request::Open { opening, .. }] = &*emitted else { panic!("expected the sub-agent to open") };
    assert_eq!(&opening.llm, &agents().models[0]);
    let share = crate::Share { turns: Some(2), time: Some(Duration::from_secs(30)) };
    let emitted = h.step(ask(main, 8, families(true, false, false), None, Some(share)));
    let [Request::Open { opening, .. }] = &*emitted else { panic!("expected the sub-agent to open") };
    let budget = Budget { turns: 2, spend: BUDGET.spend, time: Duration::from_secs(30) };
    assert_eq!(opening.budget, budget, "a share asked for, no larger than what is left");
}

#[test]
fn a_child_with_no_requested_share_gets_only_the_ordinary_pool() {
    let mut harness = Harness::new(LIMITS);
    let mut charter = agents();
    charter.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) };
    let (_, main) = harness.running_on(1, 100, charter);
    drop(harness.step(Event::Used { conversation: main, spend: spend(100) }));
    let (_, opening) = harness.child(main, 7, families(true, false, false), 101);
    assert_eq!(opening.budget, Budget { turns: 7, spend: BUDGET.spend - 102, time: Duration::from_secs(540) });
}

#[test]
fn asked_child_parts_are_clamped_independently_and_name_the_cause() {
    use crate::{Granted, Set, Share};

    let mut charter = agents();
    charter.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) };
    let ordinary = Duration::from_secs(540);
    for (asked, call_left, turns, time, turns_set, time_set) in [
        (Share::UNASKED, ordinary, 8, ordinary, Set::Pool, Set::Pool),
        (
            Share { turns: Some(3), time: Some(Duration::from_secs(4)) },
            ordinary,
            3,
            Duration::from_secs(4),
            Set::Asked,
            Set::Asked,
        ),
        (Share { turns: Some(8), time: Some(ordinary) }, ordinary, 8, ordinary, Set::Asked, Set::Asked),
        (Share { turns: Some(u32::MAX), time: Some(BUDGET.time) }, ordinary, 8, ordinary, Set::Reserve, Set::Reserve),
        (Share { turns: Some(3), time: None }, ordinary, 3, ordinary, Set::Asked, Set::Pool),
        (
            Share { turns: None, time: Some(Duration::from_secs(4)) },
            ordinary,
            8,
            Duration::from_secs(4),
            Set::Pool,
            Set::Asked,
        ),
        (Share::UNASKED, Duration::from_secs(1), 8, Duration::from_secs(1), Set::Pool, Set::Deadline),
        (
            Share { turns: Some(3), time: Some(Duration::from_secs(4)) },
            Duration::from_secs(1),
            3,
            Duration::from_secs(1),
            Set::Asked,
            Set::Deadline,
        ),
    ] {
        let plan = crate::agent::plan(
            &charter,
            crate::agent::Means { spent: Spend::ZERO, conversations: 1, left: BUDGET.time, call_left },
            families(true, false, true),
            families(true, false, false),
            None,
            asked,
            &LIMITS,
        )
        .expect("positive ordinary pool");
        assert_eq!(plan.granted, Granted { turns, time, spend: BUDGET.spend - 2, turns_set, time_set });
        assert_eq!(plan.budget, Budget { turns, time, spend: BUDGET.spend - 2 });
    }
}

#[test]
fn the_entrance_names_a_reserve_that_its_budget_cannot_hold() {
    for (reserve, part) in [
        (crate::charter::Reserve { turns: BUDGET.turns, time: Duration::ZERO }, crate::Part::Turns),
        (crate::charter::Reserve { turns: 0, time: BUDGET.time }, crate::Part::Time),
    ] {
        let mut charter = charter();
        charter.reserve = reserve;
        assert_eq!(
            answered(Harness::new(LIMITS).start(7, charter)),
            (7, Answer::Refused(Refusal::Invalid(Invalid::Budget(crate::BudgetRefusal::Reserve(part)))))
        );
    }
}

#[test]
fn change_contracts_reserve_the_checks_and_other_contracts_need_no_check_allowance() {
    let mut policy = coding();
    let reserve = Duration::from_nanos(LIMITS.check_timeout.as_nanos() - 1_000_000);
    policy.reserve.time = reserve;
    assert_eq!(
        answered(Harness::new(LIMITS).start(7, policy)),
        (
            7,
            Answer::Refused(Refusal::Invalid(Invalid::Budget(crate::BudgetRefusal::Checks {
                reserve,
                checks: LIMITS.check_timeout
            })))
        )
    );
    let equal = Harness::new(LIMITS).start(7, coding());
    let [Request::Admitted { .. }, ..] = equal.as_ref() else { panic!("equal checks reserve: {equal:?}") };
    let mut policy = charter();
    policy.reserve.time = Duration::from_millis(1);
    let report = Harness::new(LIMITS).start(7, policy);
    let [Request::Admitted { .. }, ..] = report.as_ref() else { panic!("report reserve: {report:?}") };
}

#[test]
fn pending_children_cannot_reserve_the_same_last_ordinary_turn() {
    let mut harness = Harness::new(LIMITS);
    let mut charter = agents();
    charter.budget.turns = 3;
    charter.reserve = crate::charter::Reserve { turns: 1, time: Duration::ZERO };
    let (_, main) = harness.running_on(1, 100, charter);
    drop(harness.step(Event::Used { conversation: main, spend: spend(1) }));
    let (first, _) = harness.child(main, 7, families(true, false, false), 101);
    let (second, _) = harness.child(main, 8, families(true, false, false), 102);
    let owner = Token::new(701);
    assert_eq!(crate::reserve(&mut harness.domain, first, owner, crate::Most::Bounded(1)), Ok(()));
    assert_eq!(
        crate::completion_permit(&mut harness.domain, second, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Closing
    );
    assert_eq!(
        crate::reserve(&mut harness.domain, second, Token::new(702), crate::Most::Bounded(1)),
        Err(Exhausted::Turns)
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Held
    );
    assert!(crate::settle_reservation(&mut harness.domain, first, owner, 1));
    drop(harness.step(Event::Used { conversation: first, spend: spend(1) }));
    assert_eq!(harness.resume().as_ref(), &[Request::Close { peer: Token::new(101) }]);
    assert_eq!(harness.resume().as_ref(), &[Request::Close { peer: Token::new(102) }]);
    let second_return = harness.step(Event::Ended {
        conversation: second,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    });
    let [Request::Return { call, result: Returned::Unanswered { end, .. }, .. }] = second_return.as_ref() else {
        panic!("one settled child return: {second_return:?}");
    };
    assert_eq!((*call, end.clone()), (Token::new(8), End::Budget(Exhausted::Turns)));
    let first_return =
        harness.step(Event::Ended { conversation: first, end: End::Closed, spend: spend(1), detail: Box::default() });
    let [
        Request::Return { call, result: Returned::Unanswered { end, .. }, .. },
        Request::CompletionReady { conversation },
    ] = first_return.as_ref()
    else {
        panic!("last child releases main: {first_return:?}");
    };
    assert_eq!((*call, end.clone(), *conversation), (Token::new(7), End::Budget(Exhausted::Turns), main));
    let note = super::prompt::wind_down_note(
        crate::Mode::Finite,
        crate::wind_down::Cause::Budget(Exhausted::Turns),
        crate::charter::Reserve { turns: 1, time: Duration::ZERO },
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Final { note: Some(note) }
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Final { note: None }
    );
    assert_eq!(crate::reserve(&mut harness.domain, main, Token::new(703), crate::Most::Bounded(1)), Ok(()));
    assert!(crate::settle_reservation(&mut harness.domain, main, Token::new(703), 1));
    drop(harness.step(Event::Used { conversation: main, spend: spend(1) }));
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Denied(Exhausted::Turns)
    );
}

#[test]
fn ordinary_time_closes_a_child_then_main_receives_its_final_permit() {
    let mut harness = Harness::new(LIMITS);
    let mut charter = agents();
    charter.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) };
    let (_, main) = harness.running_on(1, 100, charter);
    let (child, _) = harness.child(main, 7, families(true, false, false), 101);
    harness.env.now = Time::ZERO.saturating_add(Duration::from_secs(540));
    assert_eq!(harness.fire().as_ref(), &[]);
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Held
    );
    assert_eq!(harness.resume().as_ref(), &[Request::Close { peer: Token::new(101) }]);
    let returned = harness.step(Event::Ended {
        conversation: child,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    });
    let [Request::Return { result: Returned::Unanswered { end, .. }, .. }, Request::CompletionReady { conversation }] =
        returned.as_ref()
    else {
        panic!("last child releases main: {returned:?}");
    };
    assert_eq!((end.clone(), *conversation), (End::Budget(Exhausted::Time), main));
    let note = super::prompt::wind_down_note(
        crate::Mode::Finite,
        crate::wind_down::Cause::Budget(Exhausted::Turns),
        crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) },
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Final { note: Some(note) }
    );
    harness.env.now = EXPIRY;
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Denied(Exhausted::Time)
    );
}

#[test]
fn an_ask_the_run_cannot_grant_returns_why_and_the_run_goes_on() {
    // Not granted sub-agents, or wider families than its own.
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, charter());
    assert_eq!(
        &*h.step(ask(main, 7, families(true, false, false), None, None)),
        &[returned(7, refused(AskRefusal::NotGranted))]
    );
    let (_, main) = h.running_on(2, 101, agents());
    assert_eq!(
        &*h.step(ask(main, 8, families(true, true, false), None, None)),
        &[returned(8, refused(AskRefusal::NotGranted))]
    );
    // An LLM the charter does not list.
    assert_eq!(
        &*h.step(ask(main, 9, families(true, false, false), Some(bytes(b"model-z")), None)),
        &[returned(9, refused(AskRefusal::UnknownLlm))]
    );
    // A share with no turn in it.
    let none = crate::Share { turns: Some(0), time: None };
    assert_eq!(
        &*h.step(ask(main, 10, families(true, false, false), None, Some(none))),
        &[returned(10, refused(AskRefusal::Unworkable))]
    );

    // Too many.
    let mut h = Harness::new(Limits { run_conversations: 2, ..LIMITS });
    let (_, main) = h.running_on(1, 100, agents());
    drop(h.child(main, 7, families(true, false, false), 101));
    assert_eq!(
        &*h.step(ask(main, 8, families(true, false, false), None, None)),
        &[returned(8, refused(AskRefusal::TooMany))]
    );

    // No room for the call or the conversation.
    let mut h = Harness::new(Limits { calls: 1, run_conversations: 3, ..LIMITS });
    let (_, main) = h.running_on(1, 100, agents());
    drop(h.child(main, 7, families(true, false, false), 101));
    assert_eq!(&*h.step(ask(main, 8, families(true, false, false), None, None)), &[returned(8, Returned::Busy)]);

    // Past the budget, new sub-agent work is refused as over.
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    drop(h.step(Event::Used { conversation: main, spend: spend(BUDGET.spend + 1) }));
    assert_eq!(
        &*h.step(ask(main, 7, families(true, false, false), None, None)),
        &[returned(7, refused(AskRefusal::Over))]
    );
}

#[test]
fn a_sub_agent_that_ends_without_answering_returns_how_it_ended() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let (child, _) = h.child(main, 7, families(true, false, false), 101);
    let emitted = h.step(Event::Ended {
        conversation: child,
        end: End::Fault(Fault::Provider),
        spend: Spend::ZERO,
        detail: Box::default(),
    });
    assert_eq!(
        &*emitted,
        &[returned(
            7,
            Returned::Unanswered {
                end: End::Fault(Fault::Provider),
                used: crate::facts::Used { turns: 0, time: Duration::ZERO, spend: 0 },
                left: BUDGET
            }
        )]
    );
    assert_eq!(h.step(end_turn(main)).len(), 1, "the run goes on: main is nudged");
}

#[test]
fn a_withdrawn_sub_agent_is_closed_and_its_call_returns_once_it_has_ended() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let (child, _) = h.child(main, 7, families(true, false, false), 101);
    assert_eq!(
        &*h.step(Event::Withdraw { conversation: main, call: Token::new(7) }),
        &[Request::Close { peer: Token::new(101) }]
    );
    // Its answer crossed the close: the call is cancelled all the same.
    let yielded = Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"late") };
    assert!(h.step(yielded).is_empty(), "closing already");
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(&*emitted, &[returned(7, Returned::Cancelled)]);
    h.domain.reclaim();

    // One withdrawn before it starts is closed once it does.
    let emitted = h.step(ask(main, 8, families(true, false, false), None, None));
    let [Request::Open { conversation: child, .. }] = &*emitted else { panic!("expected the sub-agent to open") };
    let child = *child;
    assert!(h.step(Event::Withdraw { conversation: main, call: Token::new(8) }).is_empty(), "not started yet");
    assert_eq!(
        &*h.step(Event::Started { conversation: child, peer: Token::new(102) }),
        &[Request::Close { peer: Token::new(102) }]
    );
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(&*emitted, &[returned(8, Returned::Cancelled)]);
}

#[test]
fn a_sub_agent_past_its_deadline_is_closed_and_returns_timed_out_once_it_has_ended() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let minute = Duration::from_secs(60);
    let emitted = h.step(ask_by(main, 7, Time::ZERO.saturating_add(minute)));
    let [Request::Open { conversation: child, opening }] = &*emitted else { panic!("expected the sub-agent to open") };
    assert_eq!(opening.budget.time, minute, "its time runs out by its call's deadline");
    let child = *child;
    drop(h.step(Event::Started { conversation: child, peer: Token::new(101) }));
    h.after(minute);
    assert_eq!(&*h.fire(), &[Request::Close { peer: Token::new(101) }]);
    // Its answer crossed the close.
    let yielded = Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"late") };
    assert!(h.step(yielded).is_empty(), "closing already");
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(&*emitted, &[returned(7, Returned::TimedOut)]);
    assert_eq!(h.step(end_turn(main)).len(), 1, "the run goes on: main is nudged");
    h.domain.reclaim();

    // One withdrawn first returns as cancelled, its deadline no longer armed.
    let emitted = h.step(ask_by(main, 8, h.env.now.saturating_add(minute)));
    let [Request::Open { conversation: child, .. }] = &*emitted else { panic!("expected the sub-agent to open") };
    let child = *child;
    drop(h.step(Event::Started { conversation: child, peer: Token::new(102) }));
    drop(h.step(Event::Withdraw { conversation: main, call: Token::new(8) }));
    h.after(minute);
    assert!(!h.domain.is_due(h.env.now), "a withdraw cancels its call's deadline");
    let emitted =
        h.step(Event::Ended { conversation: child, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(&*emitted, &[returned(8, Returned::Cancelled)]);
}

#[test]
fn a_sub_agent_that_spends_past_the_budget_winds_the_run_down() {
    let mut h = Harness::new(LIMITS);
    let (_, main) = h.running_on(1, 100, agents());
    let (child, _) = h.child(main, 7, families(true, false, false), 101);
    let emitted = h.step(Event::Used { conversation: child, spend: spend(BUDGET.spend + 1) });
    assert!(emitted.is_empty(), "the crossing child and main settle their admitted turn");
    drop(h.step(Event::Withdraw { conversation: main, call: Token::new(7) }));
    drop(h.step(Event::Ended {
        conversation: child,
        end: End::Closed,
        spend: spend(BUDGET.spend + 1),
        detail: Box::default(),
    }));
    assert_eq!(&*h.step(end_turn(main)), &[Request::Close { peer: Token::new(100) }]);
    let emitted =
        h.step(Event::Ended { conversation: main, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(
        answered(emitted),
        (
            1,
            failed(
                Failure::Budget {
                    exhausted: Exhausted::Spend,
                    used: Some(BUDGET.spend + 1),
                    bound: Some(BUDGET.spend)
                },
                spend(BUDGET.spend + 1)
            )
        )
    );
}

// Facts.

/// The facts the domain holds, oldest first.
fn facts(h: &mut Harness) -> Box<[Fact]> {
    let mut facts = List::with_capacity(LIMITS.facts);
    for _ in 0..LIMITS.facts {
        let Some(fact) = h.pop_fact() else { break };
        facts.push(fact.kind).expect("room for every fact kept");
    }
    facts.into_boxed()
}

#[test]
fn a_run_tells_what_it_did_as_content_free_facts() {
    let mut h = Harness::new(LIMITS);
    let reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) };
    let emitted = h.start(1, Charter { reserve, ..agents() });
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else { panic!("expected a read") };
    let run = *run;
    drop(h.step(Event::Read { owner: run, read: Read::Text { text: bytes(b"Be kind."), whole: true } }));
    let main = Token::new(0);
    drop(h.step(Event::Started { conversation: main, peer: Token::new(100) }));
    let asked = crate::Share { turns: Some(u32::MAX), time: Some(BUDGET.time) };
    let opened = h.step(ask(main, 7, families(true, false, false), None, Some(asked)));
    let [Request::Open { conversation: child, .. }] = opened.as_ref() else { panic!("child opens") };
    let child = *child;
    drop(h.step(Event::Started { conversation: child, peer: Token::new(101) }));
    h.after(Duration::from_secs(3));
    drop(h.step(Event::Used { conversation: child, spend: spend(100) }));
    h.after(Duration::from_secs(2));
    drop(h.step(Event::Yielded { conversation: child, stop: Stop::EndTurn, text: bytes(b"done") }));
    drop(h.step(Event::Ended { conversation: child, end: End::Closed, spend: spend(100), detail: Box::default() }));
    drop(h.step(finish(main, 8, verdict(b"approve", Box::new([])))));
    drop(h.step(Event::Ended { conversation: main, end: End::Closed, spend: Spend::ZERO, detail: Box::default() }));
    let told = facts(&mut h);
    let expected = [
        Fact::Admitted { run, resumed: false },
        Fact::Prepared { run, guides: 1, checks: 0 },
        Fact::Opened { run, conversation: main, kind: crate::facts::Kind::Main },
        Fact::Called { run, conversation: main, call: Token::new(7), ask: Asked::SubAgent },
        Fact::Opened {
            run,
            conversation: child,
            kind: crate::facts::Kind::Child {
                parent: main,
                call: Token::new(7),
                asked,
                granted: crate::Granted {
                    turns: 8,
                    time: Duration::from_secs(540),
                    spend: BUDGET.spend - 2,
                    turns_set: crate::Set::Reserve,
                    time_set: crate::Set::Reserve,
                },
            },
        },
        Fact::Budget {
            run,
            call: None,
            standing: crate::facts::Standing {
                stage: crate::Stage::Ordinary,
                turns: 1,
                turns_limit: BUDGET.turns,
                spend: 100,
                spend_limit: BUDGET.spend,
                time: Duration::from_secs(3),
                time_limit: BUDGET.time,
                reserve,
                reserve_spend: 2,
            },
        },
        Fact::Ended { run, conversation: child, end: crate::facts::EndKind::Closed },
        Fact::Budget {
            run,
            call: Some(Token::new(7)),
            standing: crate::facts::Standing {
                stage: crate::Stage::Ordinary,
                turns: 1,
                turns_limit: BUDGET.turns,
                spend: 100,
                spend_limit: BUDGET.spend,
                time: Duration::from_secs(5),
                time_limit: BUDGET.time,
                reserve,
                reserve_spend: 2,
            },
        },
        Fact::Returned {
            run,
            call: Token::new(7),
            result: Return::Answered {
                cut: 0,
                used: crate::facts::Used { turns: 1, time: Duration::from_secs(5), spend: 100 },
            },
        },
        Fact::Called { run, conversation: main, call: Token::new(8), ask: Asked::Finish },
        Fact::Returned { run, call: Token::new(8), result: Return::Accepted },
        Fact::Ended { run, conversation: main, end: crate::facts::EndKind::Closed },
        Fact::Answered { run, answer: Answered::Accepted },
    ];
    assert_eq!(&*told, &expected);
}

#[test]
fn a_minimum_fact_reserve_keeps_the_admitted_terminal_and_every_prior_fact() {
    let required = crate::max_facts(&LIMITS);
    assert!(worst_case(&Limits { facts: required - 1, ..LIMITS }).is_none());
    let mut h = Harness::new(Limits { facts: required, ..LIMITS });
    let (run, conversation) = h.running(1, 100);
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(100) }]);
    let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
    assert_eq!(answered(emitted), (1, cancelled()));
    let observed = facts(&mut h);
    assert_eq!(observed.len(), 5);
    assert_eq!(
        observed.last(),
        Some(&Fact::Answered { run, answer: Answered::Failed(crate::facts::FailureKind::Cancelled) })
    );
}

#[test]
fn nothing_to_push_is_specific_feedback_and_the_run_can_retry() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    assert_eq!(&*h.step(Event::Delivered { owner, delivery: Delivery::Nothing }), &[returned(7, Returned::Nothing)]);
    assert_eq!(h.step(end_turn(conversation)).len(), 1, "a retry is nudged");
}

#[test]
fn failed_push_reason_and_diagnostics_return_to_the_finish_caller() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    let failure = crate::DeliveryFailure {
        directory: 1,
        reason: crate::DeliveryReason::RefusedByTarget,
        diagnostic: crate::Diagnostic::new(b"remote: protected branch", 17),
    };
    assert_eq!(
        &*h.step(Event::Delivered { owner, delivery: Delivery::Failed(failure) }),
        &[returned(7, Returned::DeliveryFailed { failure })]
    );
    let mut pushed = None;
    for fact in facts(&mut h) {
        match fact {
            Fact::Delivered { status: push, .. } => pushed = Some(push),
            Fact::MessageReceived { .. }
            | Fact::MessageRefused { .. }
            | Fact::MessageRead { .. }
            | Fact::MessageFence { .. }
            | Fact::MessageUnread { .. }
            | Fact::Admitted { .. }
            | Fact::Budget { .. }
            | Fact::Prepared { .. }
            | Fact::Opened { .. }
            | Fact::Ended { .. }
            | Fact::Called { .. }
            | Fact::Returned { .. }
            | Fact::CheckStarted { .. }
            | Fact::CheckFinished { .. }
            | Fact::Answered { .. } => {}
        }
    }
    let pushed = pushed.expect("delivery ended fact");
    assert_eq!(
        pushed,
        crate::DeliveryStatus::Failed(crate::DeliveryReason::RefusedByTarget),
        "facts retain classification, no diagnostic content"
    );
}

#[test]
fn a_failed_delivery_with_an_unmounted_directory_is_broken_evidence() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.coding(1, 100);
    let owner = h.land(conversation, 7);
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    drop(h.step(Event::Checked { owner, ran: ran(0, b"") }));
    let failure = crate::DeliveryFailure::new(99, crate::DeliveryReason::RefusedByTarget);
    assert_eq!(
        &*h.step(Event::Delivered { owner, delivery: Delivery::Failed(failure) }),
        &[returned(
            7,
            Returned::DeliveryFailed { failure: crate::DeliveryFailure::new(0, crate::DeliveryReason::Broken) }
        )]
    );
}

fn text_charter(failure: bool) -> Charter {
    let contract =
        crate::outcome::TextSpec { max: 8, fields: Box::new([FieldRule { name: bytes(b"source"), max: 4 }]) };
    Charter {
        outcome: OutcomeSpec {
            change: None,
            verdicts: Box::new([]),
            report: if failure { None } else { Some(contract.clone()) },
            failure: if failure { Some(contract) } else { None },
        },
        ..charter()
    }
}

fn text_result(failure: bool, text: &[u8]) -> Declared {
    let fields = Box::new([Field { name: bytes(b"source"), value: bytes(b"ref") }]);
    if failure {
        Declared::Failure(crate::outcome::DeclaredFailure { reason: bytes(text), fields })
    } else {
        Declared::Report(crate::outcome::Report { text: bytes(text), fields })
    }
}

#[test]
fn reports_and_declared_failures_settle_once_without_checks_or_push() {
    for (failure, text) in [(false, b"".as_slice()), (true, b"no".as_slice())] {
        let mut h = Harness::new(LIMITS);
        let (run, conversation) = h.running_on(1, 100, text_charter(failure));
        let emitted = h.step(finish(conversation, 7, text_result(failure, text)));
        assert_eq!(&*emitted, &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]);
        assert!(h.step(Event::Cancel { run: Token::new(999) }).is_empty());
        assert!(
            h.step(Event::Priced { conversation, own_spent: 5, subtree_spent: 5 }).is_empty(),
            "an own price notice charges the terminal's usage independently"
        );
        let emitted = h.step(Event::Ended { conversation, end: End::Closed, spend: spend(5), detail: Box::default() });
        assert_eq!(
            answered(emitted),
            (1, Answer::Accepted { outcome: text_result(failure, text), spent: spend(5), turns: 0 })
        );
        assert!(h.step(Event::Cancel { run }).is_empty(), "a late cancel emits no second answer");
    }
}

#[test]
fn empty_failure_reason_is_accepted_without_starting_delivery() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running_on(1, 100, text_charter(true));
    let emitted = h.step(finish(conversation, 7, text_result(true, b"")));
    assert_eq!(&*emitted, &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]);
}

#[test]
fn impossible_result_contracts_refuse_before_any_preparation_or_session() {
    let mut c = text_charter(false);
    c.outcome.report.as_mut().unwrap().fields[0].max = 0;
    let mut h = Harness::new(LIMITS);
    assert_eq!(answered(h.start(1, c)), (1, Answer::Refused(Refusal::Invalid(Invalid::Outcome))));
    assert_eq!((h.domain.runs(), h.domain.conversations()), (0, 0));
    let min = u64::try_from(size_of::<Field>()).unwrap() + 6 + 1;
    let mut h = Harness::new(Limits { outcome_bytes: min - 1, ..LIMITS });
    assert_eq!(answered(h.start(2, text_charter(false))), (2, Answer::Refused(Refusal::Invalid(Invalid::Outcome))));
    assert_eq!((h.domain.runs(), h.domain.conversations()), (0, 0));
}

#[test]
fn unknown_extra_field_ownership_is_checked_before_shape_judgement() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running_on(1, 100, text_charter(false));
    let mut oversized = List::with_capacity(u32::try_from(LIMITS.outcome_bytes).expect("the fixture cap is small"));
    for _ in 0..LIMITS.outcome_bytes {
        oversized.push(b'x').expect("room for exactly the declared cap");
    }
    let value = Declared::Report(crate::outcome::Report {
        text: Box::new([]),
        fields: Box::new([
            Field { name: bytes(b"source"), value: bytes(b"ref") },
            Field { name: bytes(b"opaque"), value: oversized.into_boxed() },
        ]),
    });
    let emitted = h.step(finish(conversation, 7, value));
    assert_eq!(
        &*emitted,
        &[returned(7, Returned::Rejected { problems: crate::outcome::too_large(LIMITS.outcome_bytes) })]
    );
    assert_eq!(
        &*h.step(finish(conversation, 8, text_result(false, b""))),
        &[returned(8, Returned::Accepted), Request::Close { peer: Token::new(100) }]
    );
}

fn receipts() -> crate::Delivered {
    crate::Delivered::new(Box::new([crate::Receipt::new(0, bytes(b"host receipt")).expect("bounded receipt")]))
        .expect("one changed directory")
}

fn delivered() -> Delivery {
    Delivery::Delivered(receipts())
}

fn mid_report() -> Charter {
    let mut charter = text_charter(false);
    charter.reserve.time = LIMITS.check_timeout;
    charter.grants.deliver = Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) });
    charter
}

fn mid_running(harness: &mut Harness) -> (Token, Token) {
    mid_running_policy(harness, mid_report())
}

fn mid_running_policy(harness: &mut Harness, policy: Charter) -> (Token, Token) {
    let mut mounted = workspace();
    mounted.directories[0].writable = true;
    let requests = harness.start_workspace(1, policy, Some(mounted));
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*requests else { panic!("admitted read") };
    let run = *run;
    let requests = harness.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Probe { .. }] = requests.as_ref() else {
        panic!("prepare writable check");
    };
    let requests = harness.step(Event::Probed { owner: run, executable: true });
    let [Request::Open { conversation, opening }] = &*requests else { panic!("prepared main") };
    assert!(opening.deliver);
    let conversation = *conversation;
    drop(harness.step(Event::Started { conversation, peer: Token::new(100) }));
    (run, conversation)
}

#[test]
fn a_mid_run_delivery_without_a_change_contract_needs_passing_checks() {
    let mut harness = Harness::new(LIMITS);
    let (_, conversation) = mid_running(&mut harness);
    let requests = harness.step(mid_ask(conversation, 3));
    let [Request::Check { owner, .. }, Request::Checking { .. }] = requests.as_ref() else {
        panic!("discovered check runs: {requests:?}");
    };
    let owner = *owner;
    let failing = ran(1, b"draft failed");
    assert_eq!(
        harness.step(Event::Checked { owner, ran: ran(1, b"draft failed") }).as_ref(),
        [returned(50, Returned::ChecksFailed { repository: bytes(b"temper"), ran: failing })]
    );
}

#[test]
fn a_mid_run_delivery_uses_the_change_contracts_optional_checks() {
    let mut harness = Harness::new(LIMITS);
    let mut policy = mid_report();
    policy.outcome.change = Some(ChangeSpec { checks_must_pass: false, fields: Box::new([]) });
    let (_, conversation) = mid_running_policy(&mut harness, policy);
    let requests = harness.step(mid_ask(conversation, 3));
    let [Request::Check { owner, .. }, Request::Checking { .. }] = requests.as_ref() else {
        panic!("discovered check runs: {requests:?}");
    };
    let owner = *owner;
    let requests = harness.step(Event::Checked { owner, ran: ran(1, b"draft failed") });
    let [Request::Deliver { .. }] = requests.as_ref() else { panic!("delivery follows the check: {requests:?}") };
    assert_eq!(
        harness.step(Event::Delivered { owner, delivery: delivered() }).as_ref(),
        [returned(50, Returned::Delivered(receipts()))]
    );
}

fn mid_ask(conversation: Token, completion: u32) -> Event {
    Event::Delegated {
        conversation,
        call: Token::new(50),
        name: crate::CallName { activation: 1, completion, position: 2 },
        ask: Ask::Deliver { change: Change { fields: Box::new([]) } },
        deadline: EXPIRY,
    }
}

fn submit_mid(harness: &mut Harness, conversation: Token) -> Token {
    let requests = harness.step(mid_ask(conversation, 3));
    let [Request::Check { owner, .. }, Request::Checking { .. }] = &*requests else { panic!("all writable checks") };
    let owner = *owner;
    let requests = harness.step(Event::Checked { owner, ran: ran(0, b"passed") });
    let [Request::Deliver { name, deadline, .. }] = &*requests else { panic!("actual bounded submission") };
    assert_eq!(*name, crate::CallName { activation: 1, completion: 3, position: 2 });
    assert_eq!(*deadline, harness.env.now.saturating_add(LIMITS.delivery_timeout).min(EXPIRY));
    owner
}

#[test]
fn mid_report_landing_settles_before_an_interrupted_run_answers() {
    for interrupted in [false, true] {
        let mut harness = Harness::new(LIMITS);
        let (run, conversation) = mid_running(&mut harness);
        let owner = submit_mid(&mut harness, conversation);
        if interrupted {
            drop(harness.step(Event::Cancel { run }));
            assert!(harness.step(Event::Withdraw { conversation, call: Token::new(50) }).is_empty());
        }
        let requests = harness.step(Event::Delivered { owner, delivery: delivered() });
        assert_eq!(requests.as_ref(), &[returned(50, Returned::Delivered(receipts()))]);
        assert!(
            harness.step(Event::Delivered { owner, delivery: delivered() }).is_empty(),
            "stale duplicate is inert before reclaim"
        );
        if !interrupted {
            drop(harness.step(finish(conversation, 51, report())));
        }
        let answer = answered(harness.step(Event::Ended {
            conversation,
            end: End::Closed,
            spend: Spend::ZERO,
            detail: Box::default(),
        }))
        .1;
        if interrupted {
            assert_eq!(answer, Answer::Failed { failure: Failure::Cancelled, spent: Spend::ZERO, turns: 0 });
        } else {
            assert_eq!(answer, Answer::Accepted { outcome: report(), spent: Spend::ZERO, turns: 0 });
        }
        harness.domain.reclaim();
        assert!(
            harness.step(Event::Delivered { owner, delivery: delivered() }).is_empty(),
            "stale generation is inert after reclaim"
        );
    }
}

#[test]
fn malformed_host_mount_or_zero_origin_never_becomes_successful_delivery() {
    let mut harness = Harness::new(LIMITS);
    let (_, conversation) = mid_running(&mut harness);
    assert_eq!(
        harness.step(mid_ask(conversation, 0)).as_ref(),
        &[returned(50, Returned::Refused { refusal: AskRefusal::Name })]
    );
    let owner = submit_mid(&mut harness, conversation);
    let wrong = crate::Delivered::new(Box::new([crate::Receipt::new(1, bytes(b"unmounted")).expect("sealed ordinal")]))
        .expect("sealed terminal");
    assert_eq!(
        harness.step(Event::Delivered { owner, delivery: Delivery::Delivered(wrong) }).as_ref(),
        &[returned(
            50,
            Returned::DeliveryFailed { failure: crate::DeliveryFailure::new(0, crate::DeliveryReason::Broken) }
        )]
    );
    drop(harness.step(finish(conversation, 51, report())));
    let answer = answered(harness.step(Event::Ended {
        conversation,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    }))
    .1;
    assert_eq!(answer, Answer::Accepted { outcome: report(), spent: Spend::ZERO, turns: 0 });
}

fn report() -> Declared {
    text_result(false, b"done")
}

#[test]
fn time_and_spend_shutdown_keep_an_already_submitted_mid_landing() {
    for timed in [false, true] {
        let mut harness = Harness::new(LIMITS);
        let (_, conversation) = mid_running(&mut harness);
        let owner = submit_mid(&mut harness, conversation);
        let expected = if timed {
            harness.env.now = EXPIRY;
            for _ in 0_u32..2 {
                if harness.domain.is_due(harness.env.now) {
                    drop(harness.fire());
                }
            }
            Failure::Budget { exhausted: Exhausted::Time, used: Some(600_000), bound: Some(600_000) }
        } else {
            drop(harness.step(Event::Used { conversation, spend: spend(BUDGET.spend + 1) }));
            Failure::Budget { exhausted: Exhausted::Spend, used: Some(BUDGET.spend + 1), bound: Some(BUDGET.spend) }
        };
        let expected_spend = if timed { Spend::ZERO } else { spend(BUDGET.spend + 1) };
        let requests = harness.step(Event::Delivered { owner, delivery: delivered() });
        if timed {
            assert_eq!(requests.as_ref(), &[returned(50, Returned::Delivered(receipts()))]);
        } else {
            assert_eq!(
                requests.as_ref(),
                &[returned(50, Returned::Delivered(receipts())), Request::Close { peer: Token::new(100) },]
            );
        }
        // Closing the session waits for the host terminal; its one End
        // then reports the spend it already used, without another delivery.
        let answer = answered(harness.step(Event::Ended {
            conversation,
            end: End::Closed,
            spend: expected_spend,
            detail: Box::default(),
        }))
        .1;
        assert_eq!(answer, Answer::Failed { failure: expected, spent: expected_spend, turns: 0 });
    }
}

#[test]
fn generic_non_marker_host_refusal_is_feedback_and_report_can_finish() {
    let mut harness = Harness::new(LIMITS);
    let (_, conversation) = mid_running(&mut harness);
    let owner = submit_mid(&mut harness, conversation);
    let refusal = crate::DeliveryRefusal::new(None, bytes(b"host asks for corrected metadata"))
        .expect("bounded generic correctable feedback");
    assert_eq!(
        harness.step(Event::Delivered { owner, delivery: Delivery::Refused(refusal.clone()) }).as_ref(),
        &[returned(50, Returned::DeliveryRefused(refusal))]
    );
    drop(harness.step(finish(conversation, 51, report())));
    let answer = answered(harness.step(Event::Ended {
        conversation,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    }))
    .1;
    assert_eq!(answer, Answer::Accepted { outcome: report(), spent: Spend::ZERO, turns: 0 });
}

#[test]
fn impossible_delivery_grant_and_zero_host_deadline_refuse_before_preparation() {
    let mut charter = mid_report();
    charter.grants.deliver =
        Some(ChangeSpec { checks_must_pass: true, fields: Box::new([FieldRule { name: bytes(b"ticket"), max: 0 }]) });
    let mut harness = Harness::new(LIMITS);
    assert_eq!(answered(harness.start(1, charter)), (1, Answer::Refused(Refusal::Invalid(Invalid::Grants))));
    assert_eq!((harness.domain.runs(), harness.domain.conversations()), (0, 0));
    let mut harness = Harness::new(Limits { delivery_timeout: Duration::ZERO, ..LIMITS });
    assert_eq!(answered(harness.start(2, mid_report())), (2, Answer::Refused(Refusal::Invalid(Invalid::Workspace))));
    assert_eq!((harness.domain.runs(), harness.domain.conversations()), (0, 0));
}

#[test]
fn caller_only_stop_keeps_submitted_mid_delivery_and_ordinary_continuation() {
    for expired in [false, true] {
        for later_cancel in [false, true] {
            let mut harness = Harness::new(LIMITS);
            let (run, conversation) = mid_running(&mut harness);
            let caller_deadline = harness.env.now.saturating_add(Duration::from_secs(1));
            let mut ask = mid_ask(conversation, 3);
            let Event::Delegated { deadline, .. } = &mut ask else { unreachable!("delegated fixture") };
            *deadline = caller_deadline;
            let requests = harness.step(ask);
            let [Request::Check { owner, .. }, Request::Checking { .. }] = requests.as_ref() else {
                panic!("delivery checks first")
            };
            let owner = *owner;
            let requests = harness.step(Event::Checked { owner, ran: ran(0, b"passed") });
            let [Request::Deliver { deadline, .. }] = requests.as_ref() else {
                panic!("checks submit exactly one delivery")
            };
            assert_eq!(*deadline, caller_deadline);
            if expired {
                harness.env.now = caller_deadline;
                assert!(harness.domain.is_due(harness.env.now));
                assert!(harness.fire().is_empty(), "expiry retains the actual host terminal right");
            } else {
                assert!(harness.step(Event::Withdraw { conversation, call: Token::new(50) }).is_empty());
            }
            assert_eq!(
                harness.step(Event::Delivered { owner, delivery: delivered() }).as_ref(),
                &[returned(50, Returned::Delivered(receipts()))],
                "caller-only stop neither abandons the host nor closes main"
            );
            if later_cancel {
                assert_eq!(harness.step(Event::Cancel { run }).as_ref(), &[Request::Close { peer: Token::new(100) }]);
            } else {
                drop(harness.step(finish(conversation, 51, report())));
            }
            let answer = answered(harness.step(Event::Ended {
                conversation,
                end: End::Closed,
                spend: Spend::ZERO,
                detail: Box::default(),
            }))
            .1;
            assert_eq!(
                answer,
                if later_cancel {
                    Answer::Failed { failure: Failure::Cancelled, spent: Spend::ZERO, turns: 0 }
                } else {
                    Answer::Accepted { outcome: report(), spent: Spend::ZERO, turns: 0 }
                }
            );
        }
    }
}

fn host_ask(conversation: Token, call: u64, deadline: Time) -> Event {
    Event::Delegated {
        conversation,
        call: Token::new(call),
        name: crate::CallName { activation: 1, completion: 7, position: 3 },
        ask: Ask::Host {
            tool: bytes(b"comment"),
            effect: crate::HostEffect::Read,
            input: crate::HostInput::attested(bytes(br#" {"whole":{"opaque":[true,1]},"verbatim":"\u0041"} "#))
                .expect("protocol-attested fixture"),
        },
        deadline,
    }
}

fn host_submission(emitted: &[Request]) -> crate::RelayName {
    let [Request::HostCall { relay, host_run, name, input, deadline, tool, effect }] = emitted else {
        panic!("one opaque relay, got {emitted:?}");
    };
    assert_eq!(*host_run, Token::new(71));
    assert_eq!(*name, crate::CallName { activation: 1, completion: 7, position: 3 });
    assert_eq!(tool.as_ref(), b"comment");
    assert_eq!(*effect, crate::HostEffect::Read);
    assert_eq!(input.bytes(), br#" {"whole":{"opaque":[true,1]},"verbatim":"\u0041"} "#);
    assert!(*deadline > Time::ZERO);
    *relay
}

#[test]
fn host_relay_retries_keep_the_call_name_and_input() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(71, 99);
    let deadline = h.env.now.saturating_add(Duration::from_secs(30));
    let first = host_submission(&h.step(host_ask(conversation, 41, deadline)));
    assert_eq!(first.attempt, 1);
    assert!(h.step(Event::HostReturned { relay: first, reply: crate::HostReply::Busy }).is_empty());
    h.after(LIMITS.host_backoff);
    let second = host_submission(&h.fire());
    assert_eq!((second.owner, second.attempt), (first.owner, 2));
    assert!(
        h.step(Event::HostReturned {
            relay: first,
            reply: crate::HostReply::Answered(crate::HostAnswer::new(bytes(b"stale"), false).expect("small answer"))
        })
        .is_empty(),
        "stale attempt cannot resolve current attempt"
    );
    assert!(
        h.step(Event::HostReturned { relay: second, reply: crate::HostReply::Unanswered(crate::Unanswered::Lost) })
            .is_empty()
    );
    h.after(LIMITS.host_backoff);
    let third = host_submission(&h.fire());
    assert_eq!(third.attempt, 3);
    let actual = crate::HostAnswer::new(bytes(b"recorded first decision"), true).expect("bounded exact error");
    assert_eq!(
        &*h.step(Event::HostReturned { relay: third, reply: crate::HostReply::Answered(actual.clone()) }),
        &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostAnswered(actual) }]
    );
    assert!(
        h.step(Event::HostReturned { relay: third, reply: crate::HostReply::Busy }).is_empty(),
        "one logical feedback"
    );
    assert_eq!(h.domain.next_deadline(), Some(Time::ZERO.saturating_add(BUDGET.time)));
}

#[test]
fn a_host_call_busy_many_times_keeps_its_name_until_it_succeeds() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(71, 99);
    let deadline = h.env.now.saturating_add(Duration::from_secs(30));
    let mut relay = host_submission(&h.step(host_ask(conversation, 41, deadline)));
    for attempt in 1..=8 {
        assert_eq!(relay.attempt, attempt);
        assert!(h.step(Event::HostReturned { relay, reply: crate::HostReply::Busy }).is_empty());
        h.after(LIMITS.host_backoff);
        relay = host_submission(&h.fire());
    }
    let answer = crate::HostAnswer::new(bytes(b"decided after eight busy attempts"), false).expect("bounded text");
    assert_eq!(
        &*h.step(Event::HostReturned { relay, reply: crate::HostReply::Answered(answer.clone()) }),
        &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostAnswered(answer) }]
    );
}

#[test]
fn a_decided_host_answer_over_the_receiving_cap_is_not_unknown() {
    let limits = Limits { host_reply_bytes: 8, ..LIMITS };
    let mut h = Harness::new(limits);
    let (_, conversation) = h.running(71, 99);
    let deadline = h.env.now.saturating_add(Duration::from_secs(30));
    let relay = host_submission(&h.step(host_ask(conversation, 41, deadline)));
    let answer = crate::HostAnswer::new(bytes(b"nine bytes"), false).expect("constructor-bounded host text");
    assert_eq!(
        &*h.step(Event::HostReturned { relay, reply: crate::HostReply::Answered(answer) }),
        &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostTooLarge { bytes: 10, max: 8 } }]
    );
}

#[test]
fn a_host_reported_too_large_answer_is_an_error_and_is_not_retried() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(71, 99);
    let deadline = h.env.now.saturating_add(Duration::from_secs(30));
    let relay = host_submission(&h.step(host_ask(conversation, 41, deadline)));
    assert_eq!(
        &*h.step(Event::HostReturned { relay, reply: crate::HostReply::TooLarge }),
        &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostReportedTooLarge }]
    );
    assert!(
        h.step(Event::HostReturned { relay, reply: crate::HostReply::TooLarge }).is_empty(),
        "the settled reply cannot ask the host again"
    );
}

#[test]
fn host_uncertainty_survives_busy_until_withdrawal_or_caller_expiry() {
    enum Stop {
        Withdraw,
        CallerExpiry,
    }
    for stop in [Stop::Withdraw, Stop::CallerExpiry] {
        let mut h = Harness::new(LIMITS);
        let (_, conversation) = h.running(71, 99);
        let deadline = h.env.now.saturating_add(Duration::from_secs(30));
        let first = host_submission(&h.step(host_ask(conversation, 41, deadline)));
        assert!(
            h.step(Event::HostReturned { relay: first, reply: crate::HostReply::Unanswered(crate::Unanswered::Lost) })
                .is_empty()
        );
        h.after(LIMITS.host_backoff);
        let second = host_submission(&h.fire());
        assert!(h.step(Event::HostReturned { relay: second, reply: crate::HostReply::Busy }).is_empty());
        let emitted = match stop {
            Stop::Withdraw => h.step(Event::Withdraw { conversation, call: Token::new(41) }),
            Stop::CallerExpiry => {
                h.env.now = deadline;
                h.fire()
            }
        };
        assert_eq!(&*emitted, &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostUnknown }]);
        assert_eq!((h.domain.runs(), h.domain.conversations()), (1, 1), "call-only stop retains ordinary run");
    }
}

#[test]
fn host_withdrawal_and_timeout_retain_relay_until_terminal_answer_wins() {
    enum Stop {
        Withdraw,
        RelayTimeout,
        CancelRun,
    }
    for stop in [Stop::Withdraw, Stop::RelayTimeout, Stop::CancelRun] {
        let mut h = Harness::new(LIMITS);
        let (run, conversation) = h.running(71, 99);
        let deadline = h.env.now.saturating_add(Duration::from_secs(30));
        let relay = host_submission(&h.step(host_ask(conversation, 41, deadline)));
        match stop {
            Stop::Withdraw => assert_eq!(
                &*h.step(Event::Withdraw { conversation, call: Token::new(41) }),
                &[Request::WithdrawHost { relay }]
            ),
            Stop::RelayTimeout => {
                h.after(Duration::from_secs(5));
                assert_eq!(&*h.fire(), &[Request::WithdrawHost { relay }]);
            }
            Stop::CancelRun => {
                assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(99) }]);
                assert_eq!(
                    &*h.step(Event::Withdraw { conversation, call: Token::new(41) }),
                    &[Request::WithdrawHost { relay }]
                );
            }
        }
        assert!(h.step(Event::Withdraw { conversation, call: Token::new(41) }).is_empty());
        assert_eq!(h.domain.calls(), 1, "withdrawal did not fabricate terminal");
        let actual = crate::HostAnswer::new(bytes(b"already recorded"), false).expect("small answer");
        assert_eq!(
            &*h.step(Event::HostReturned { relay, reply: crate::HostReply::Answered(actual.clone()) }),
            &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostAnswered(actual) }]
        );
        match stop {
            Stop::CancelRun => {
                let (_, answer) = answered(h.step(Event::Ended {
                    conversation,
                    end: End::Closed,
                    spend: Spend::ZERO,
                    detail: Box::default(),
                }));
                assert_eq!(answer, failed(Failure::Cancelled, Spend::ZERO));
            }
            Stop::Withdraw | Stop::RelayTimeout => {}
        }
    }
}

#[test]
fn host_unknown_is_conveyed_after_withdrawn_terminal_when_shutdown_disallows_recovery() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(71, 99);
    let deadline = h.env.now.saturating_add(Duration::from_secs(30));
    let relay = host_submission(&h.step(host_ask(conversation, 41, deadline)));
    assert_eq!(&*h.step(Event::Withdraw { conversation, call: Token::new(41) }), &[Request::WithdrawHost { relay }]);
    assert_eq!(
        &*h.step(Event::HostReturned { relay, reply: crate::HostReply::Unanswered(crate::Unanswered::Withdrawn) }),
        &[Request::Return { spent: 0, call: Token::new(41), result: Returned::HostUnknown }]
    );
}

#[test]
fn host_declarations_are_admitted_as_bounded_unique_contracts_before_io() {
    let baseline = charter();
    assert_eq!(crate::charter::check(&baseline, Some(&workspace()), &LIMITS), Ok(LIMITS.guide_bytes));
    for invalid in 0_u32..5 {
        let mut declared = charter();
        match invalid {
            0 => declared.grants.host_tools[0].name = bytes(b"finish"),
            1 => {
                declared.grants.host_tools =
                    Box::new([declared.grants.host_tools[0].clone(), declared.grants.host_tools[0].clone()]);
            }
            2 => declared.grants.host_tools[0].timeout = Duration::ZERO,
            3 => declared.grants.host_tools[0].description = Box::new([]),
            4 => declared.grants.host_tools[0].schema = Box::new([]),
            _ => unreachable!("five concrete refusals"),
        }
        let mut h = Harness::new(LIMITS);
        let (_, answer) = answered(h.start(71, declared));
        assert_eq!(answer, Answer::Refused(Refusal::Invalid(Invalid::Grants)));
        assert_eq!((h.domain.runs(), h.domain.conversations(), h.domain.calls()), (0, 0, 0));
    }
    let mut h = Harness::new(Limits { host_backoff: Duration::ZERO, ..LIMITS });
    let (_, answer) = answered(h.start(71, baseline));
    assert_eq!(answer, Answer::Refused(Refusal::Invalid(Invalid::Grants)));
}

#[test]
fn the_prompt_tool_s_name_is_reserved() {
    let mut declared = charter();
    declared.grants.host_tools[0].name = b"permission_prompt".as_slice().into();
    let mut harness = Harness::new(LIMITS);
    assert_eq!(answered(harness.start(71, declared)), (71, Answer::Refused(Refusal::Invalid(Invalid::Grants))));
}

#[test]
fn host_declarations_reserve_only_smith_tool_names() {
    for name in crate::charter::RESERVED_NAMES {
        let mut declared = charter();
        declared.grants.host_tools[0].name = name.into();
        let mut h = Harness::new(LIMITS);
        assert_eq!(answered(h.start(71, declared)), (71, Answer::Refused(Refusal::Invalid(Invalid::Grants))));
    }
    for name in [b"read_file".as_slice(), b"list_dir", b"write_file", b"edit_file", b"run_shell", b"subagent"] {
        let mut declared = charter();
        declared.grants.host_tools[0].name = name.into();
        let mut h = Harness::new(LIMITS);
        let emitted = h.start(71, declared);
        match emitted.first() {
            Some(Request::Admitted { .. }) => {}
            _ => panic!("{name:?}: {emitted:?}"),
        }
    }
}

#[test]
fn undeclared_host_and_effect_mismatch_are_refused_before_relay() {
    let mut h = Harness::new(LIMITS);
    let (_, conversation) = h.running(71, 99);
    for (tool, effect, problem) in [
        (b"absent".as_slice(), crate::HostEffect::Read, crate::HostProblem::Undeclared),
        (b"comment".as_slice(), crate::HostEffect::Write, crate::HostProblem::Effect),
    ] {
        let event = Event::Delegated {
            conversation,
            call: Token::new(101),
            name: crate::CallName { activation: 1, completion: 1, position: 0 },
            ask: Ask::Host {
                tool: bytes(tool),
                effect,
                input: crate::HostInput::attested(bytes(b"{}")).expect("attested empty object"),
            },
            deadline: Time::ZERO.saturating_add(Duration::from_secs(30)),
        };
        assert_eq!(
            &*h.step(event),
            &[Request::Return { spent: 0, call: Token::new(101), result: Returned::HostRejected(problem) }]
        );
    }
    assert_eq!(h.domain.calls(), 0);
}

#[test]
fn opaque_fifo_wakes_waiting_and_read_advances_only_on_main_turn() {
    let mut h = Harness::new(LIMITS);
    let (run, conversation) = h.running(1, 9);
    let call = Token::new(20);
    let wait = Event::Delegated {
        conversation,
        call,
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        ask: Ask::Wait,
        deadline: Time::from_nanos(u64::MAX),
    };
    assert_eq!(h.step(wait).as_ref(), &[Request::Waiting { host_run: Token::new(1), read: None }]);
    for (name, text) in [(0_u64, b"first".as_slice()), (99_u64, b"second".as_slice()), (7_u64, b"third".as_slice())] {
        let got = h.step(Event::Message {
            finish_requested: false,
            run,
            name: Token::new(name),
            label: bytes(b"person"),
            text: bytes(text),
        });
        if name == 0 {
            assert_eq!(got.as_ref(), &[returned(20, Returned::Waited { text: crate::prompt::waited() })]);
        } else {
            assert!(got.is_empty());
        }
    }
    assert_eq!(
        h.step(Event::Turn { conversation, record: Token::new(50), sequence: 1, opens_window: true }).as_ref(),
        &[Request::Turn {
            host_run: Token::new(1),
            record: Token::new(50),
            number: 1,
            position: 1,
            opens_window: true,
            read: None,
            spent: Spend::ZERO,
        }]
    );
    assert_eq!(
        h.step(end_turn(conversation)).as_ref(),
        &[Request::Say { peer: Token::new(9), text: bytes(b"person: first\n\nperson: second\n\nperson: third") }]
    );
    assert_eq!(
        h.step(Event::Turn { conversation, record: Token::new(51), sequence: 2, opens_window: false }).as_ref(),
        &[Request::Turn {
            host_run: Token::new(1),
            record: Token::new(51),
            number: 2,
            position: 2,
            opens_window: false,
            read: Some(Token::new(7)),
            spent: Spend::ZERO,
        }]
    );
    assert_eq!(
        h.step(Event::Delegated {
            conversation,
            call,
            name: crate::CallName { activation: 1, completion: 3, position: 0 },
            ask: Ask::Wait,
            deadline: Time::from_nanos(u64::MAX),
        })
        .as_ref(),
        &[Request::Waiting { host_run: Token::new(1), read: Some(Token::new(7)) }]
    );
    h.after(Duration::from_secs(30));
    assert_eq!(h.fire().as_ref(), &[returned(20, Returned::Withheld), Request::Close { peer: Token::new(9) }]);
    h.step(Event::Turn { conversation, record: Token::new(52), sequence: 3, opens_window: false });
    assert_eq!(
        answered(h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() })),
        (1, Answer::Parked { spent: Spend::ZERO, turns: 3 })
    );
    h.domain.reclaim();
    assert_eq!((h.domain.runs(), h.domain.conversations(), h.domain.calls()), (0, 0, 0));
}

#[test]
fn zero_waiting_time_parks_with_its_wait_result_withheld() {
    let mut h = Harness::new(LIMITS);
    let policy = Charter { waiting: Duration::ZERO, ..charter() };
    let emitted = h.start_workspace(70, policy, None);
    let [Request::Admitted { .. }, Request::Open { conversation, .. }] = emitted.as_ref() else {
        panic!("opens");
    };
    let conversation = *conversation;
    h.step(Event::Started { conversation, peer: Token::new(71) });
    assert_eq!(
        h.step(Event::Delegated {
            conversation,
            call: Token::new(72),
            name: crate::CallName { activation: 1, completion: 1, position: 0 },
            ask: Ask::Wait,
            deadline: EXPIRY,
        })
        .as_ref(),
        &[
            Request::Waiting { host_run: Token::new(70), read: None },
            returned(72, Returned::Withheld),
            Request::Close { peer: Token::new(71) }
        ]
    );
    h.step(Event::Turn { conversation, record: Token::new(73), sequence: 1, opens_window: true });
    assert_eq!(
        answered(h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() })),
        (70, Answer::Parked { spent: Spend::ZERO, turns: 1 })
    );
}

#[test]
fn bounded_message_and_input_at_idle_deadline_preserve_existing_fifo() {
    let limits = Limits { messages: 1, message_bytes: 10, ..LIMITS };
    let mut h = Harness::new(limits);
    let (run, conversation) = h.running(1, 9);
    assert!(
        h.step(Event::Message {
            finish_requested: false,
            label: Box::from(&b"host"[..]),
            run,
            name: Token::new(5),
            text: bytes(b"full")
        })
        .is_empty()
    );
    assert_eq!(
        h.step(end_turn(conversation)).as_ref(),
        &[Request::Say { peer: Token::new(9), text: bytes(b"host: full") }]
    );
    h.step(Event::Turn { conversation, record: Token::new(50), sequence: 1, opens_window: true });
    assert_eq!(
        h.step(Event::Delegated {
            conversation,
            call: Token::new(20),
            name: crate::CallName { activation: 1, completion: 2, position: 0 },
            ask: Ask::Wait,
            deadline: EXPIRY,
        })
        .as_ref(),
        &[Request::Waiting { host_run: Token::new(1), read: Some(Token::new(5)) }]
    );
    h.after(Duration::from_secs(30));
    assert_eq!(
        &*h.step(Event::Message {
            finish_requested: false,
            label: Box::from(&b"host"[..]),
            run,
            name: Token::new(0),
            text: bytes(b"wake")
        }),
        &[returned(20, Returned::Waited { text: crate::prompt::waited() })]
    );
    assert_eq!(
        h.step(end_turn(conversation)).as_ref(),
        &[Request::Say { peer: Token::new(9), text: bytes(b"host: wake") }]
    );
    assert!(!h.domain.is_due(h.env.now), "same-iteration input cancels idle expiry before fire");
    assert_eq!(&*h.step(Event::Cancel { run }), &[Request::Close { peer: Token::new(9) }]);
    assert_eq!(
        &*h.step(Event::Ended { conversation, end: End::Closed, spend: Spend::ZERO, detail: Box::default() }),
        &[Request::Answer {
            to: ReplyTo::new(Token::new(1)),
            answer: Answer::Failed { failure: Failure::Cancelled, spent: Spend::ZERO, turns: 1 },
            read: Some(Token::new(5)),
            detail: Box::default(),
        }]
    );
    h.domain.reclaim();
    assert_eq!(h.domain.runs(), 0);
}

#[test]
fn convention_path_payloads_fill_the_exact_charter_cap_and_cancel_discovery_settles() {
    let mut selected = charter();
    selected.conventions = Some(crate::Conventions {
        guide: Box::new([b'g'; crate::Conventions::PATH_CAPACITY]),
        checks: Box::new([b'c'; crate::Conventions::PATH_CAPACITY]),
    });
    let exact = crate::charter::cost(&selected).expect("bounded maximum paths")
        + crate::workspace::cost(Some(&workspace())).unwrap();
    let baseline = crate::charter::cost(&Charter { conventions: None, ..charter() }).unwrap()
        + crate::workspace::cost(Some(&workspace())).unwrap();
    // The legacy fixture contributes explicit path payloads; remove them from
    // this independent cap comparison rather than reducing any fixture maximum.
    assert_eq!(exact, baseline + u64::try_from(crate::Conventions::PATH_CAPACITY * 2).unwrap());
    let mut harness = Harness::new(Limits { run_bytes: exact, ..LIMITS });
    let emitted = harness.start(77, selected);
    let [Request::Admitted { run, .. }, Request::Read { owner, at, .. }] = emitted.as_ref() else {
        panic!("exact aggregate convention storage is admitted: {emitted:?}");
    };
    assert_eq!(at.path.len(), crate::Conventions::PATH_CAPACITY);
    assert_eq!(*owner, *run);
    let run = *run;
    assert!(harness.step(Event::Cancel { run }).is_empty(), "cancel cannot manufacture the outstanding Read terminal");
    assert_eq!(
        answered(harness.step(Event::Read { owner: run, read: Read::Missing })),
        (77, failed(Failure::Cancelled, Spend::ZERO))
    );
    harness.domain.reclaim();
    assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (0, 0, 0));

    let oversized = Charter {
        conventions: Some(crate::Conventions {
            guide: Box::new([b'g'; crate::Conventions::PATH_CAPACITY]),
            checks: Box::new([b'c'; crate::Conventions::PATH_CAPACITY]),
        }),
        instructions: {
            let brief = charter().instructions;
            let mut writer = skein_lib::Writer::new(brief.len().checked_add(1).unwrap());
            writer.put(&brief).expect("room for the original brief");
            writer.put(b"x").expect("one exact extra byte");
            writer.finish()
        },
        ..charter()
    };
    assert_eq!(answered(harness.start(78, oversized)), (78, Answer::Refused(Refusal::Invalid(Invalid::TooLarge))));
    assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (0, 0, 0));
}

/// Independent host workspace fixtures exercise admission before any IO.
fn workspace_directory(name: &[u8], root: u64, writable: bool, git: bool, conflicts: &[&[u8]]) -> Directory {
    let mut paths = List::with_capacity(u32::try_from(conflicts.len()).unwrap());
    for path in conflicts {
        paths.push(bytes(path)).expect("bounded fixture conflict count");
    }
    Directory {
        host: Box::from(b"/host".as_slice()),
        name: bytes(name),
        root: Token::new(root),
        writable,
        git,
        conflicts: paths.into_boxed(),
    }
}

fn mixed_workspace() -> Workspace {
    Workspace {
        directories: Box::new([
            workspace_directory(b"work", 41, true, false, &[]),
            workspace_directory(b"reference", 42, false, true, &[b"src/merge.rs"]),
        ]),
    }
}

#[test]
fn accepted_mixed_workspace_drives_discovery_opening_and_child_metadata() {
    let mut harness = Harness::new(LIMITS);
    let mounted = mixed_workspace();
    let mut policy = charter();
    policy.reserve.time = LIMITS.check_timeout;
    policy.grants.deliver = Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) });
    let emitted = harness.start_workspace(51, policy, Some(mounted.clone()));
    let [Request::Admitted { run, .. }, Request::Read { at, .. }] = emitted.as_ref() else {
        panic!("mixed workspace starts discovery: {emitted:?}");
    };
    let run = *run;
    assert_eq!(at.root, Token::new(41));
    let emitted = harness.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Probe { at, .. }] = emitted.as_ref() else { panic!("writable plain directory probes checks") };
    assert_eq!(at.root, Token::new(41));
    let emitted = harness.step(Event::Probed { owner: run, executable: false });
    let [Request::Read { at, .. }] = emitted.as_ref() else { panic!("read-only git guide") };
    assert_eq!(at.root, Token::new(42));
    let emitted = harness.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Open { opening, .. }] = emitted.as_ref() else { panic!("read-only git has no check probe") };
    assert_eq!(opening.workspace, Some(mounted.clone()));
    for fragment in [b"a plain directory".as_slice(), b"a git working tree", b"initial merge conflicts: `src/merge.rs`"]
    {
        assert!(skein_lib::bytes::find(&opening.system, fragment).is_some());
    }
    let child = super::prompt::child(
        &charter(),
        Some(&mounted),
        &Found::with_capacity(0),
        b"Inspect",
        opening.families,
        crate::Granted {
            turns: 3,
            time: Duration::from_secs(4),
            spend: 100,
            turns_set: crate::Set::Pool,
            time_set: crate::Set::Pool,
        },
        LIMITS.answer_bytes,
    );
    assert!(skein_lib::bytes::find(&child, b"initial merge conflicts: `src/merge.rs`").is_some());
    assert!(skein_lib::bytes::find(&child, b"`reference`, which you may only read").is_some());
}

#[test]
fn absent_workspace_suppresses_workspace_families_and_preserves_main_services() {
    let mut harness = Harness::new(LIMITS);
    let mut policy = charter();
    policy.grants.tools = Tools { inspect: true, modify: true, shell: true };
    policy.grants.agents = true;
    policy.grants.host_tools = Box::new([HostTool {
        name: bytes(b"host_action"),
        description: bytes(b"Host action"),
        schema: bytes(b"{}"),
        effect: crate::HostEffect::Write,
        timeout: Duration::from_secs(5),
    }]);
    let emitted = harness.start_workspace(52, policy, None);
    let [Request::Admitted { .. }, Request::Open { opening, .. }] = emitted.as_ref() else {
        panic!("workspace-free start has no discovery effects: {emitted:?}");
    };
    assert_eq!(opening.workspace, None);
    assert_eq!(opening.tools, Tools { inspect: false, modify: false, shell: false });
    assert_eq!(opening.families.tools, opening.tools);
    assert!(opening.families.agents && opening.finish && opening.wait);
    assert!(!opening.deliver);
    assert_eq!(opening.host_tools.len(), 1);
    assert_eq!(opening.host_tools[0].name.as_ref(), b"host_action");
    assert!(skein_lib::bytes::find(&opening.system, b"There is no checkout.").is_some());
}

#[test]
fn a_run_with_only_finish_does_not_offer_or_accept_wait() {
    let mut harness = Harness::new(LIMITS);
    let mut policy = charter();
    policy.grants.wait = false;
    policy.grants.tools = Tools { inspect: false, modify: false, shell: false };
    policy.grants.agents = false;
    policy.grants.host_tools = Box::new([]);
    let emitted = harness.start_workspace(53, policy, None);
    let [Request::Admitted { .. }, Request::Open { conversation, opening }] = emitted.as_ref() else {
        panic!("workspace-free start opens main: {emitted:?}");
    };
    assert!(!opening.wait);
    assert!(opening.finish);
    assert!(!opening.deliver);
    assert_eq!(opening.tools, Tools { inspect: false, modify: false, shell: false });
    assert!(opening.host_tools.is_empty());
    let conversation = *conversation;
    assert!(harness.step(Event::Started { conversation, peer: Token::new(54) }).is_empty());
    let call = Token::new(55);
    assert_eq!(
        harness
            .step(Event::Delegated {
                conversation,
                call,
                name: crate::CallName { activation: 1, completion: 1, position: 0 },
                ask: Ask::Wait,
                deadline: Time::from_nanos(u64::MAX),
            })
            .as_ref(),
        [Request::Return { spent: 0, call, result: Returned::Refused { refusal: AskRefusal::NotGranted } }]
    );
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "programming-model.md, section 10.2: the harness receives a copied immutable profile"
)]
fn refuse_workspace(mounted: Option<Workspace>, limits: Limits, expected: Invalid) {
    let mut harness = Harness::new(limits);
    assert_eq!(
        answered(harness.start_workspace(53, charter(), mounted)),
        (53, Answer::Refused(Refusal::Invalid(expected)))
    );
    assert_eq!(harness.domain.runs(), 0, "invalid metadata is refused before retained state or effects");
}

#[test]
fn workspace_names_roots_and_presence_are_admitted_independently() {
    refuse_workspace(Some(Workspace { directories: Box::new([]) }), LIMITS, Invalid::Workspace);
    for name in [b"".as_slice(), b".", b"..", b"bad/name", b"bad\0name"] {
        refuse_workspace(
            Some(Workspace { directories: Box::new([workspace_directory(name, 1, false, false, &[])]) }),
            LIMITS,
            Invalid::Workspace,
        );
    }
    for directories in [
        Box::new([workspace_directory(b"one", 1, false, false, &[]), workspace_directory(b"one", 2, true, true, &[])]),
        Box::new([workspace_directory(b"one", 1, false, false, &[]), workspace_directory(b"two", 1, true, true, &[])]),
    ] {
        refuse_workspace(Some(Workspace { directories }), LIMITS, Invalid::Workspace);
    }
    refuse_workspace(Some(mixed_workspace()), Limits { directories: 1, ..LIMITS }, Invalid::Workspace);
    refuse_workspace(
        Some(Workspace { directories: Box::new([workspace_directory(b"name", 1, false, false, &[])]) }),
        Limits { directory_name_bytes: 3, ..LIMITS },
        Invalid::Workspace,
    );
}

#[test]
fn initial_conflicts_require_git_unique_relative_paths_and_independent_caps() {
    for path in [b"".as_slice(), b"/root", b"a//b", b"a/./b", b"a/../b", b"a/", b"a\0b"] {
        refuse_workspace(
            Some(Workspace { directories: Box::new([workspace_directory(b"git", 1, false, true, &[path])]) }),
            LIMITS,
            Invalid::Workspace,
        );
    }
    for (directory, limits) in [
        (workspace_directory(b"plain", 1, true, false, &[b"a"]), LIMITS),
        (workspace_directory(b"git", 1, false, true, &[b"a", b"a"]), LIMITS),
        (workspace_directory(b"git", 1, false, true, &[b"a", b"b"]), Limits { conflicts: 1, ..LIMITS }),
        (workspace_directory(b"git", 1, false, true, &[b"abcd"]), Limits { conflict_path_bytes: 3, ..LIMITS }),
    ] {
        refuse_workspace(Some(Workspace { directories: Box::new([directory]) }), limits, Invalid::Workspace);
    }
    let limits = Limits { run_bytes: 65_536, ..LIMITS };
    let path = Box::new([b'p'; crate::Marker::CAPACITY]);
    let mounted =
        Workspace { directories: Box::new([workspace_directory(b"git", 1, false, true, &[path.as_slice()])]) };
    assert_eq!(crate::charter::check(&charter(), Some(&mounted), &limits), Ok(limits.guide_bytes));
    let over = Box::new([b'p'; crate::Marker::CAPACITY + 1]);
    refuse_workspace(
        Some(Workspace { directories: Box::new([workspace_directory(b"git", 1, false, true, &[over.as_slice()])]) }),
        limits,
        Invalid::Workspace,
    );
    let invalid_limits = Limits { conflict_path_bytes: 4097, ..limits };
    assert_eq!(worst_case(&invalid_limits), None);
    refuse_workspace(None, invalid_limits, Invalid::Workspace);
}

#[test]
fn exact_workspace_limits_and_aggregate_price_include_every_owner() {
    let limits = Limits { directories: 2, directory_name_bytes: 9, conflicts: 1, conflict_path_bytes: 12, ..LIMITS };
    let mounted = mixed_workspace();
    let directory_cells = 2 * u64::try_from(size_of::<Directory>()).unwrap();
    let conflict_cell = u64::try_from(size_of::<Box<[u8]>>()).unwrap();
    let owned = directory_cells + 4 + 9 + 10 + conflict_cell + 12;
    assert_eq!(crate::workspace::cost(Some(&mounted)), Some(owned));
    assert_eq!(crate::workspace::cost(None), Some(0));
    let policy = charter();
    let exact = crate::charter::cost(&policy).unwrap() + owned;
    let exact_limits = Limits { run_bytes: exact, ..limits };
    assert_eq!(crate::charter::check(&policy, Some(&mounted), &exact_limits), Ok(exact_limits.guide_bytes));
    refuse_workspace(Some(mounted.clone()), Limits { run_bytes: exact - 1, ..limits }, Invalid::TooLarge);
    // Aggregate ownership refuses before quadratic semantic duplicate scans.
    let mut duplicate = mounted;
    duplicate.directories[1].root = duplicate.directories[0].root;
    refuse_workspace(Some(duplicate), Limits { run_bytes: exact - 1, ..limits }, Invalid::TooLarge);
    assert!(worst_case(&exact_limits).is_some());
}

#[test]
fn contracts_requiring_writes_without_writable_directories_refuse_before_effects() {
    for mounted in [None, Some(workspace())] {
        let mut harness = Harness::new(LIMITS);
        let change = Charter {
            reserve: crate::charter::Reserve { turns: 0, time: LIMITS.check_timeout },
            outcome: OutcomeSpec {
                change: Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) }),
                ..charter().outcome
            },
            ..charter()
        };
        assert_eq!(
            answered(harness.start_workspace(54, change, mounted.clone())),
            (54, Answer::Refused(Refusal::Invalid(Invalid::Outcome)))
        );
        let mut deliver = charter();
        deliver.reserve.time = LIMITS.check_timeout;
        deliver.grants.deliver = Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) });
        assert_eq!(
            answered(harness.start_workspace(55, deliver, mounted)),
            (55, Answer::Refused(Refusal::Invalid(Invalid::Grants)))
        );
        assert_eq!(harness.domain.runs(), 0);
    }
}

#[test]
fn brief_section_count_refuses_before_discovery_even_when_aggregate_would_fit() {
    let limits = Limits { brief_sections: 2, ..LIMITS };
    let mut harness = Harness::new(limits);
    let selected = Charter {
        brief: crate::Brief {
            sections: Box::new([
                crate::Section { title: Box::new([]), text: Box::new([]) },
                crate::Section { title: Box::new([]), text: Box::new([]) },
                crate::Section { title: Box::new([]), text: Box::new([]) },
            ]),
        },
        ..charter()
    };
    assert!(crate::charter::cost(&selected).unwrap() < limits.run_bytes);
    assert_eq!(answered(harness.start(91, selected)), (91, Answer::Refused(Refusal::Invalid(Invalid::TooLarge))));
    assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (0, 0, 0));
}

#[test]
fn instructions_and_ordered_section_cells_titles_and_text_attain_the_exact_aggregate() {
    let selected = Charter {
        instructions: bytes(b"Literal role"),
        brief: crate::Brief {
            sections: Box::new([
                crate::Section { title: bytes(b"Repeated"), text: bytes(b"first body") },
                crate::Section { title: Box::new([]), text: bytes(b"empty title") },
                crate::Section { title: bytes(b"Repeated"), text: Box::new([]) },
            ]),
        },
        ..charter()
    };
    let empty = Charter { instructions: Box::new([]), brief: crate::Brief { sections: Box::new([]) }, ..charter() };
    let expected_context = u64::try_from(
        b"Literal role".len()
            + 3 * size_of::<crate::Section>()
            + 2 * b"Repeated".len()
            + b"first body".len()
            + b"empty title".len(),
    )
    .unwrap();
    let expected =
        crate::charter::cost(&empty).unwrap() + expected_context + crate::workspace::cost(Some(&workspace())).unwrap();
    assert_eq!(
        crate::charter::cost(&selected).unwrap() + crate::workspace::cost(Some(&workspace())).unwrap(),
        expected
    );
    let limits = Limits { run_bytes: expected, brief_sections: 3, ..LIMITS };
    assert_eq!(crate::charter::check(&selected, Some(&workspace()), &limits), Ok(limits.guide_bytes));
    assert_eq!(
        crate::charter::check(&selected, Some(&workspace()), &Limits { run_bytes: expected - 1, ..limits }),
        Err(Invalid::TooLarge)
    );
    assert_eq!(
        crate::charter::check(&selected, Some(&workspace()), &Limits { brief_sections: 2, ..limits }),
        Err(Invalid::TooLarge)
    );
    let mut harness = Harness::new(limits);
    let emitted = harness.start(92, selected);
    match emitted.as_ref() {
        [Request::Admitted { .. }, Request::Read { .. }] => {}
        _ => panic!("exact count/byte cap admits before discovery: {emitted:?}"),
    }
}

#[test]
fn empty_main_instructions_and_brief_are_valid_even_with_zero_section_limit() {
    let selected = Charter { instructions: Box::new([]), brief: crate::Brief { sections: Box::new([]) }, ..charter() };
    assert_eq!(
        crate::charter::check(&selected, Some(&workspace()), &Limits { brief_sections: 0, ..LIMITS }),
        Ok(LIMITS.guide_bytes)
    );
}

fn priced(conversation: Token, own_spent: u64, subtree_spent: u64) -> Event {
    Event::Priced { conversation, own_spent, subtree_spent }
}

#[test]
fn reaching_the_scalar_cap_settles_wait_and_fails_only_after_yield() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running(1, 100);
    assert!(harness.step(priced(main, BUDGET.spend, BUDGET.spend)).is_empty());
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Denied(Exhausted::Spend)
    );
    let wait = Event::Delegated {
        conversation: main,
        call: Token::new(91),
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        ask: Ask::Wait,
        deadline: EXPIRY,
    };
    assert_eq!(harness.step(wait).as_ref(), &[returned(91, Returned::Refused { refusal: AskRefusal::Over })]);
    assert_eq!(harness.step(end_turn(main)).as_ref(), &[Request::Close { peer: Token::new(100) }]);
    let answer = answered(harness.step(Event::Ended {
        conversation: main,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    }))
    .1;
    assert_eq!(
        answer,
        failed(
            Failure::Budget { exhausted: Exhausted::Spend, used: Some(BUDGET.spend), bound: Some(BUDGET.spend) },
            Spend { units: BUDGET.spend, ..Spend::ZERO }
        )
    );
}

#[test]
fn raw_run_counts_keep_absence_distinct_from_reported_zero_across_gaps() {
    let zero = Spend { turns: 1, input: Some(0), reasoning: Some(0), ..Spend::ZERO };
    assert_eq!(Spend::ZERO.accumulate(zero), Some(zero));
    let gap = Spend { turns: 1, ..Spend::ZERO };
    let known = zero.accumulate(gap).expect("one gap has no reported counts");
    assert_eq!(known, Spend { turns: 2, ..zero });
    let next = Spend { turns: 1, output: Some(3), cache_write: Some(2), reasoning: Some(1), ..Spend::ZERO };
    let total = known.accumulate(next).expect("reported fields remain separate");
    assert_eq!(
        total,
        Spend {
            turns: 3,
            input: Some(0),
            output: Some(3),
            cache_read: None,
            cache_write: Some(2),
            reasoning: Some(1),
            units: 0,
        }
    );
    assert_eq!(
        total.unreported(known),
        Spend {
            turns: 1,
            input: Some(0),
            output: Some(3),
            cache_read: None,
            cache_write: Some(2),
            reasoning: Some(1),
            units: 0,
        }
    );
    assert_eq!(Spend::ZERO.accumulate(gap), Some(gap), "no reported count stays absent");
}

#[test]
fn checked_spend_addition_rejects_the_whole_increment() {
    let known = Spend { turns: 2, input: Some(4), output: Some(u64::MAX), units: 10, ..Spend::ZERO };
    let next = Spend { turns: 1, input: Some(3), output: Some(1), units: 11, ..Spend::ZERO };
    assert_eq!(known.accumulate(next), None);
    let known = Spend { units: u64::MAX, ..Spend::ZERO };
    assert_eq!(known.accumulate(next), None);
    let known = Spend { reasoning: Some(u64::MAX), ..Spend::ZERO };
    assert_eq!(known.accumulate(Spend { reasoning: Some(1), ..Spend::ZERO }), None);
}

#[test]
fn completion_gate_distinguishes_child_withdrawal_from_budget_denial() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running_on(1, 100, agents());
    let (child, _) = harness.child(main, 7, families(true, false, false), 101);
    assert_eq!(
        crate::completion_permit(&mut harness.domain, child, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Allowed
    );
    assert_eq!(
        harness.step(Event::Withdraw { conversation: main, call: Token::new(7) }).as_ref(),
        &[Request::Close { peer: Token::new(101) }]
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, child, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Closing
    );
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
        crate::CompletionPermit::Allowed
    );
    assert_eq!(
        harness
            .step(Event::Ended { conversation: child, end: End::Closed, spend: Spend::ZERO, detail: Box::default() })
            .as_ref(),
        &[returned(7, Returned::Cancelled)]
    );
}

#[test]
fn run_wide_overflow_preflight_preserves_charged_prefix() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running(1, 100);
    assert!(harness.step(priced(main, u64::MAX, u64::MAX)).is_empty());
    assert_eq!(
        crate::completion_overflow(&harness.domain, main, 1, Spend::ZERO),
        Some(Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Spend), used: None, bound: None })
    );
    assert_eq!(crate::completion_overflow(&harness.domain, main, 0, Spend { turns: 1, ..Spend::ZERO }), None);
    let answer = answered(harness.step(Event::Ended {
        conversation: main,
        end: End::PriceOverflow,
        spend: Spend::ZERO,
        detail: Box::default(),
    }))
    .1;
    assert_eq!(
        answer,
        failed(
            Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Spend), used: None, bound: None },
            Spend { units: u64::MAX, ..Spend::ZERO }
        )
    );
}

#[test]
fn raw_usage_overflow_ends_with_only_the_charged_prefix() {
    for (full, overflow) in [
        (Spend { output: Some(u64::MAX), ..Spend::ZERO }, Spend { output: Some(1), ..Spend::ZERO }),
        (Spend { reasoning: Some(u64::MAX), ..Spend::ZERO }, Spend { reasoning: Some(1), ..Spend::ZERO }),
    ] {
        let mut harness = Harness::new(LIMITS);
        let (_, main) = harness.running(1, 100);
        assert!(harness.step(Event::Used { conversation: main, spend: full }).is_empty());
        assert_eq!(
            crate::completion_overflow(&harness.domain, main, 0, overflow),
            Some(Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Usage), used: None, bound: None })
        );
        assert_eq!(
            harness.step(Event::Used { conversation: main, spend: overflow }).as_ref(),
            &[Request::Close { peer: Token::new(100) }]
        );
        let answer = answered(harness.step(Event::Ended {
            conversation: main,
            end: End::Closed,
            spend: full,
            detail: Box::default(),
        }))
        .1;
        assert_eq!(
            answer,
            failed(
                Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Usage), used: None, bound: None },
                full
            )
        );
    }
}

#[test]
fn a_session_usage_overflow_becomes_the_run_failure() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running(1, 100);
    let answer = answered(harness.step(Event::Ended {
        conversation: main,
        end: End::UsageOverflow,
        spend: Spend::ZERO,
        detail: Box::default(),
    }))
    .1;
    assert_eq!(
        answer,
        failed(
            Failure::Budget { exhausted: Exhausted::Overflow(crate::Overflow::Usage), used: None, bound: None },
            Spend::ZERO
        )
    );
}

#[test]
fn labelled_message_ingress_checks_rendered_bytes_names_and_inbox_before_retaining() {
    let mut harness = Harness::new(Limits { message_bytes: 12, messages: 2, ..LIMITS });
    let (run, _) = harness.running(1, 9);
    let message = |name, text: &[u8]| Event::Message {
        finish_requested: false,
        run,
        name: Token::new(name),
        label: bytes(b"peer"),
        text: bytes(text),
    };
    assert!(harness.step(message(10, b"123456")).is_empty(), "exact rendered bound is admitted");
    for (name, text, reason) in [
        (11, b"1234567".as_slice(), crate::MessageRefusal::TooLarge),
        (10, b"x".as_slice(), crate::MessageRefusal::NameInUse),
    ] {
        assert_eq!(
            &*harness.step(message(name, text)),
            &[Request::MessageRefused { host_run: Token::new(1), name: Token::new(name), reason }]
        );
    }
    assert!(harness.step(message(12, b"x")).is_empty(), "refused inputs consume no inbox room");
    assert_eq!(
        &*harness.step(message(13, b"x")),
        &[Request::MessageRefused {
            host_run: Token::new(1),
            name: Token::new(13),
            reason: crate::MessageRefusal::Full
        }]
    );
    harness.step(Event::Cancel { run });
    assert_eq!(
        &*harness.step(message(14, b"x")),
        &[Request::MessageRefused {
            host_run: Token::new(1),
            name: Token::new(14),
            reason: crate::MessageRefusal::Ending
        }]
    );
}

#[test]
fn preparation_refuses_bad_messages_without_disrupting_the_admitted_opening() {
    let mut harness = Harness::new(Limits { message_bytes: 12, messages: 1, ..LIMITS });
    let run = harness.prepare(1);
    assert_eq!(
        &*harness.step(Event::Message {
            finish_requested: false,
            run,
            name: Token::new(1),
            label: bytes(b"peer"),
            text: bytes(b"1234567")
        }),
        &[Request::MessageRefused {
            host_run: Token::new(1),
            name: Token::new(1),
            reason: crate::MessageRefusal::TooLarge
        }]
    );
    assert!(
        harness
            .step(Event::Message {
                finish_requested: false,
                run,
                name: Token::new(2),
                label: bytes(b"peer"),
                text: bytes(b"123456")
            })
            .is_empty()
    );
    let emitted = harness.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Open { conversation, opening }] = &*emitted else { panic!("prepared run opens main") };
    assert_eq!(opening.prompt.as_ref(), b"Begin the work your brief describes.\n\npeer: 123456");
    let main = *conversation;
    harness.step(Event::Started { conversation: main, peer: Token::new(9) });
    let emitted =
        harness.step(Event::Turn { conversation: main, record: Token::new(40), sequence: 1, opens_window: true });
    let [Request::Turn { read, .. }] = emitted.as_ref() else { panic!("one told turn") };
    assert_eq!(*read, Some(Token::new(2)));
    harness.step(end_turn(main));
}

#[test]
fn messages_during_stopping_over_winding_and_closed_each_have_an_ending_terminal() {
    let mut preparing = Harness::new(LIMITS);
    let stopping = preparing.prepare(1);
    preparing.step(Event::Cancel { run: stopping });
    assert_eq!(
        &*preparing.step(Event::Message {
            finish_requested: false,
            run: stopping,
            name: Token::new(20),
            label: bytes(b"peer"),
            text: bytes(b"stopping"),
        }),
        &[Request::MessageRefused {
            host_run: Token::new(1),
            name: Token::new(20),
            reason: crate::MessageRefusal::Ending,
        }]
    );

    let mut running = Harness::new(LIMITS);
    let (run, main) = running.running(2, 9);
    running.step(Event::Used { conversation: main, spend: spend(BUDGET.spend) });
    for (name, phase) in [(21, "over"), (22, "winding"), (23, "closed")] {
        assert_eq!(
            &*running.step(Event::Message {
                finish_requested: false,
                run,
                name: Token::new(name),
                label: bytes(b"peer"),
                text: bytes(phase.as_bytes()),
            }),
            &[Request::MessageRefused {
                host_run: Token::new(2),
                name: Token::new(name),
                reason: crate::MessageRefusal::Ending,
            }],
            "{phase} refuses each message once"
        );
        match name {
            21 => {
                running.step(end_turn(main));
            }
            22 => {
                running.step(Event::Ended {
                    conversation: main,
                    end: End::Closed,
                    spend: spend(BUDGET.spend),
                    detail: Box::default(),
                });
            }
            23 => {}
            _ => unreachable!("three known phases"),
        }
    }
}

#[test]
fn ordered_offers_end_each_message_read_or_unread_and_the_answer_keeps_the_last_told_fence() {
    for (offer_messages, offer_bytes) in [(2, 128), (5, 20)] {
        let mut harness = Harness::new(Limits { messages: 5, message_bytes: 9, offer_messages, offer_bytes, ..LIMITS });
        let (run, main) = harness.running(1, 9);
        for name in [99, 0, 8, 7, 6] {
            assert!(
                harness
                    .step(Event::Message {
                        finish_requested: false,
                        run,
                        name: Token::new(name),
                        label: bytes(b"peer"),
                        text: bytes(b"aaa")
                    })
                    .is_empty()
            );
        }
        assert_eq!(
            &*harness.step(end_turn(main)),
            &[Request::Say { peer: Token::new(9), text: bytes(b"peer: aaa\n\npeer: aaa") }]
        );
        let told =
            harness.step(Event::Turn { conversation: main, record: Token::new(40), sequence: 1, opens_window: true });
        let [Request::Turn { read, .. }] = &*told else { panic!("one actual told turn") };
        assert_eq!(*read, Some(Token::new(0)), "names are opaque, including a fence of zero");
        assert_eq!(
            &*harness.step(end_turn(main)),
            &[Request::Say { peer: Token::new(9), text: bytes(b"peer: aaa\n\npeer: aaa") }]
        );
        harness.step(Event::Cancel { run });
        let terminal = harness.step(Event::Ended {
            conversation: main,
            end: End::Closed,
            spend: Spend::ZERO,
            detail: Box::default(),
        });
        let [Request::Answer { read, .. }] = &*terminal else { panic!("one run terminal") };
        assert_eq!(*read, Some(Token::new(0)), "an untold offer cannot move the final fence");
        let mut terminals = alloc::collections::BTreeMap::new();
        while let Some(fact) = harness.pop_fact() {
            match fact.kind {
                Fact::MessageRead { name, turn, .. } => {
                    assert_eq!(turn, 1);
                    assert!(terminals.insert(name, "read").is_none(), "each message ends once");
                }
                Fact::MessageUnread { name, .. } => {
                    assert!(terminals.insert(name, "unread").is_none(), "each message ends once");
                }
                Fact::MessageFence { .. }
                | Fact::MessageReceived { .. }
                | Fact::MessageRefused { .. }
                | Fact::Admitted { .. }
                | Fact::Budget { .. }
                | Fact::Prepared { .. }
                | Fact::Opened { .. }
                | Fact::Ended { .. }
                | Fact::Called { .. }
                | Fact::Returned { .. }
                | Fact::CheckStarted { .. }
                | Fact::CheckFinished { .. }
                | Fact::Delivered { .. }
                | Fact::Answered { .. } => {}
            }
        }
        assert_eq!(terminals.len(), 5, "each accepted message ends");
        for name in [99, 0] {
            assert_eq!(terminals[&Token::new(name)], "read");
        }
        for name in [8, 7, 6] {
            assert_eq!(terminals[&Token::new(name)], "unread", "offered and queued both end unread");
        }
    }
}

#[test]
fn a_charters_host_deadline_uses_the_tool_ceiling_and_preserves_the_default() {
    let limits = Limits { host_timeout: Duration::from_secs(1), host_timeout_max: Duration::from_secs(7), ..LIMITS };
    for seconds in [5_u64, 9] {
        let mut policy = charter();
        policy.grants.host_tools[0].timeout = Duration::from_secs(seconds);
        let mut harness = Harness::new(limits);
        let (_, conversation) = harness.running_on(71, 99, policy);
        let caller = harness.env.now.saturating_add(Duration::from_secs(30));
        let emitted = harness.step(host_ask(conversation, 41, caller));
        let [Request::HostCall { deadline, .. }] = emitted.as_ref() else { panic!("one admitted relay") };
        assert_eq!(*deadline, harness.env.now.saturating_add(Duration::from_secs(seconds.min(7))));
        assert_eq!(harness.env.limits.host_timeout, Duration::from_secs(1));
    }
}

#[test]
fn delayed_facts_keep_activation_history_and_child_parentage() {
    for transcript in [None, Some(Token::new(77))] {
        let mut h = Harness::new(LIMITS);
        h.env.now = Time::from_nanos(7);
        let started = h.step(Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter: agents(),
            workspace: Some(workspace()),
            transcript,
            resumed: transcript.is_some(),
        });
        let [Request::Admitted { run, .. }, Request::Read { .. }] = &*started else { panic!("run admitted") };
        let run = *run;
        h.env.now = Time::from_nanos(11);
        let opening = h.step(Event::Read { owner: run, read: Read::Text { text: bytes(b"Be kind."), whole: true } });
        let [Request::Open { conversation: main, opening }] = &*opening else { panic!("main opens") };
        let main = *main;
        assert_eq!(opening.transcript, transcript);
        drop(h.step(Event::Started { conversation: main, peer: Token::new(100) }));
        h.env.now = Time::from_nanos(13);
        let (child, _) = h.child(main, 7, families(true, false, false), 101);
        h.env.now = Time::from_nanos(99);
        let facts = observed_facts(&mut h);
        assert_eq!(facts[0].at, Time::from_nanos(7));
        assert_eq!(facts[0].kind, Fact::Admitted { run, resumed: transcript.is_some() });
        assert_eq!(facts[2].at, Time::from_nanos(11));
        assert_eq!(facts[2].kind, Fact::Opened { run, conversation: main, kind: crate::facts::Kind::Main });
        assert_eq!(facts[4].at, Time::from_nanos(13));
        assert_eq!(
            facts[4].kind,
            Fact::Opened {
                run,
                conversation: child,
                kind: crate::facts::Kind::Child {
                    parent: main,
                    call: Token::new(7),
                    asked: crate::Share::UNASKED,
                    granted: crate::Granted {
                        turns: BUDGET.turns,
                        time: EXPIRY.saturating_since(Time::from_nanos(13)),
                        spend: BUDGET.spend,
                        turns_set: crate::Set::Pool,
                        time_set: crate::Set::Deadline
                    },
                }
            }
        );
    }
}

fn observed_facts(harness: &mut Harness) -> Box<[crate::facts::Fact]> {
    let mut facts = List::with_capacity(LIMITS.facts);
    for _ in 0..LIMITS.facts {
        if let Some(fact) = harness.pop_fact() {
            facts.push(fact).expect("room for all observations");
        }
    }
    facts.into_boxed()
}

fn carried_start(charter: Charter, messages: Box<[crate::Message]>, workspace: Option<Workspace>) -> Event {
    Event::Start {
        messages,
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
        reply_to: ReplyTo::new(Token::new(1)),
        host_run: Token::new(1),
        identity: None,
        activation: 1,
        charter,
        workspace,
        transcript: None,
        resumed: false,
    }
}

fn carried(name: u64, text: &[u8]) -> crate::Message {
    crate::Message { finish_requested: false, name: Token::new(name), label: bytes(b"person"), text: bytes(text) }
}

#[test]
fn carried_messages_open_after_the_briefs_instruction_and_before_later_messages() {
    let mut harness = Harness::new(LIMITS);
    let emitted = harness.step(carried_start(
        charter(),
        Box::new([carried(99, b"first"), carried(0, b"second")]),
        Some(workspace()),
    ));
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else { panic!("admitted preparation") };
    let run = *run;
    assert!(
        harness
            .step(Event::Message {
                finish_requested: false,
                run,
                name: Token::new(8),
                label: bytes(b"peer"),
                text: bytes(b"third")
            })
            .is_empty()
    );
    let emitted = harness.step(Event::Read { owner: run, read: Read::Missing });
    let [Request::Open { conversation, opening }] = &*emitted else { panic!("one opening") };
    let main = *conversation;
    assert_eq!(
        &*opening.prompt,
        b"Begin the work your brief describes.\n\nperson: first\n\nperson: second\n\npeer: third"
    );
    harness.step(Event::Started { conversation: main, peer: Token::new(9) });
    let emitted =
        harness.step(Event::Turn { conversation: main, record: Token::new(40), sequence: 1, opens_window: true });
    let [Request::Turn { read, .. }] = &*emitted else { panic!("one told turn") };
    assert_eq!(*read, Some(Token::new(8)));
    let mut reads = List::with_capacity(LIMITS.messages);
    for _ in 0..LIMITS.facts {
        if let Some(fact) = harness.pop_fact()
            && let Fact::MessageRead { name, .. } = fact.kind
        {
            reads.push(name).expect("bounded read count");
        }
    }
    assert_eq!(reads.as_slice(), [Token::new(99), Token::new(0), Token::new(8)]);
}

#[test]
fn an_empty_brief_opens_on_carried_messages_or_on_its_instructions_without_wait() {
    let batches: [Box<[crate::Message]>; 2] = [Box::new([]), Box::new([carried(0, b"work")])];
    for messages in batches {
        let mut charter = charter();
        charter.brief.sections = Box::new([]);
        charter.grants.wait = false;
        charter.instructions = bytes(b"Follow these instructions.");
        let expected: &[u8] =
            if messages.is_empty() { b"Begin the work your instructions describe." } else { b"person: work" };
        let mut harness = Harness::new(LIMITS);
        let emitted = harness.step(carried_start(charter, messages, None));
        let [Request::Admitted { .. }, Request::Open { opening, .. }] = &*emitted else {
            panic!("one immediate opening")
        };
        assert_eq!(&*opening.prompt, expected);
        let mut instructions_found = false;
        for chunk in opening.system.windows(b"Follow these instructions.".len()) {
            instructions_found |= chunk == b"Follow these instructions.";
        }
        assert!(instructions_found);
    }
}

#[test]
fn a_start_refuses_all_carried_messages_atomically_at_its_count_byte_and_name_bounds() {
    let limits = Limits { messages: 2, message_bytes: 9, ..LIMITS };
    let batches: [Box<[crate::Message]>; 3] = [
        Box::new([carried(1, b"a"), carried(2, b"a"), carried(3, b"a")]),
        Box::new([carried(1, b"aa")]),
        Box::new([carried(0, b"a"), carried(0, b"a")]),
    ];
    for messages in batches {
        let mut harness = Harness::new(limits);
        let emitted = harness.step(carried_start(charter(), messages, None));
        assert_eq!(answered(emitted), (1, Answer::Refused(Refusal::Invalid(Invalid::Messages))));
        assert!(harness.pop_fact().is_none(), "no partial message admission");
        let emitted = harness.step(carried_start(charter(), Box::new([carried(0, b"a")]), None));
        let [Request::Admitted { .. }, Request::Open { opening, .. }] = &*emitted else {
            panic!("exact bound admitted after refusal")
        };
        assert!(opening.prompt.ends_with(b"person: a"));
    }
}

fn message_driven_charter() -> Charter {
    Charter { brief: crate::Brief { sections: Box::default() }, ..charter() }
}

#[test]
fn prepared_empty_brief_awaits_first_message_without_opening_main() {
    let mut harness = Harness::new(LIMITS);
    let emitted = harness.start_workspace(80, message_driven_charter(), None);
    let [Request::Admitted { run, .. }, Request::Waiting { host_run, read }] = emitted.as_ref() else {
        panic!("prepared main awaits without a session: {emitted:?}")
    };
    let run = *run;
    assert_eq!((*host_run, *read), (Token::new(80), None));
    assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (1, 1, 0));
    assert_eq!(harness.domain.next_deadline(), Some(Time::ZERO.saturating_add(message_driven_charter().waiting)));
    harness.after(Duration::from_secs(1));
    let emitted = harness.step(Event::Message {
        finish_requested: false,
        run,
        name: Token::new(0),
        label: bytes(b"person"),
        text: bytes(b"real task"),
    });
    let [Request::Open { conversation, opening }] = emitted.as_ref() else {
        panic!("first message opens main: {emitted:?}")
    };
    assert_eq!(opening.prompt.as_ref(), b"person: real task");
    let conversation = *conversation;
    assert!(harness.step(Event::Started { conversation, peer: Token::new(81) }).is_empty());
    let emitted = harness.step(Event::Turn { conversation, record: Token::new(82), sequence: 1, opens_window: true });
    let [Request::Turn { read, number, spent, .. }] = emitted.as_ref() else { panic!("actual first turn") };
    assert_eq!((*read, *number, *spent), (Some(Token::new(0)), 1, Spend::ZERO));
    let facts = observed_facts(&mut harness);
    let mut prepared = 0_u32;
    for fact in &facts {
        if let Fact::Prepared { .. } = fact.kind {
            prepared += 1;
            assert_eq!(fact.at, Time::ZERO, "prepared time precedes later work");
        }
    }
    assert_eq!(prepared, 1, "one preparation even when its opening comes later");
}

#[test]
fn a_message_before_preparation_opens_directly_and_one_after_preparation_wakes_once() {
    for before in [true, false] {
        let mut harness = Harness::new(LIMITS);
        let emitted = harness.start(83, message_driven_charter());
        let [Request::Admitted { run, .. }, Request::Read { .. }] = emitted.as_ref() else { panic!("preparation") };
        let run = *run;
        if before {
            assert!(
                harness
                    .step(Event::Message {
                        finish_requested: false,
                        run,
                        name: Token::new(1),
                        label: bytes(b"person"),
                        text: bytes(b"task")
                    })
                    .is_empty()
            );
        }
        let emitted = harness.step(Event::Read { owner: run, read: Read::Missing });
        let emitted = if before {
            emitted
        } else {
            assert_eq!(emitted.as_ref(), [Request::Waiting { host_run: Token::new(83), read: None }]);
            harness.step(Event::Message {
                finish_requested: false,
                run,
                name: Token::new(1),
                label: bytes(b"person"),
                text: bytes(b"task"),
            })
        };
        let [Request::Open { opening, .. }] = emitted.as_ref() else { panic!("one task opening") };
        assert_eq!(opening.prompt.as_ref(), b"person: task");
    }
}

#[test]
fn awaiting_cancellation_retires_unopened_main_and_answers_once() {
    let mut harness = Harness::new(LIMITS);
    let emitted = harness.start_workspace(84, message_driven_charter(), None);
    let [Request::Admitted { run, .. }, Request::Waiting { .. }] = emitted.as_ref() else { panic!("awaiting") };
    let run = *run;
    assert_eq!(answered(harness.step(Event::Cancel { run })), (84, failed(Failure::Cancelled, Spend::ZERO)));
    assert!(harness.step(Event::Cancel { run }).is_empty());
    assert_eq!(harness.domain.next_deadline(), None);
    harness.domain.reclaim();
    assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (0, 0, 0));
}

#[test]
fn awaiting_idle_wall_and_zero_waiting_deadlines_park_without_usage_or_session() {
    for (waiting, time) in [(7, 30), (30, 7), (0, 30)] {
        let mut harness = Harness::new(LIMITS);
        let charter = Charter {
            waiting: Duration::from_secs(waiting),
            budget: Budget { time: Duration::from_secs(time), ..BUDGET },
            ..message_driven_charter()
        };
        let emitted = harness.start_workspace(85, charter, None);
        if waiting == 0 {
            let [
                Request::Admitted { .. },
                Request::Waiting { read: None, .. },
                Request::Answer { answer, read: None, .. },
            ] = emitted.as_ref()
            else {
                panic!("immediate awaiting park: {emitted:?}")
            };
            assert_eq!(*answer, Answer::Parked { spent: Spend::ZERO, turns: 0 });
        } else {
            let [Request::Admitted { .. }, Request::Waiting { read: None, .. }] = emitted.as_ref() else {
                panic!("awaiting")
            };
            harness.after(Duration::from_secs(waiting.min(time)));
            assert_eq!(answered(harness.fire()), (85, Answer::Parked { spent: Spend::ZERO, turns: 0 }));
        }
        assert_eq!(harness.domain.next_deadline(), None);
        harness.domain.reclaim();
        assert_eq!((harness.domain.runs(), harness.domain.conversations(), harness.domain.calls()), (0, 0, 0));
    }
}

#[test]
fn awaiting_keeps_resume_binding_until_first_message_opens_main() {
    let mut harness = Harness::new(LIMITS);
    let mut event = carried_start(Charter { resume: true, ..message_driven_charter() }, Box::default(), None);
    if let Event::Start { transcript, activation, .. } = &mut event {
        *transcript = Some(Token::new(900));
        *activation = 2;
    }
    let emitted = harness.step(event);
    let [Request::Admitted { run, .. }, Request::Waiting { .. }] = emitted.as_ref() else { panic!("awaiting") };
    let emitted = harness.step(Event::Message {
        finish_requested: false,
        run: *run,
        name: Token::new(1),
        label: bytes(b"person"),
        text: bytes(b"new task"),
    });
    let [Request::Open { opening, .. }] = emitted.as_ref() else { panic!("restored main opens") };
    assert_eq!(opening.transcript, Some(Token::new(900)));
    assert_eq!(opening.activation, 2);
    assert_eq!(opening.prompt.as_ref(), b"person: new task");
}

#[test]
fn a_finish_crossing_a_message_continues_only_with_wait_authority() {
    for wait in [true, false] {
        let mut harness = Harness::new(LIMITS);
        let mut charter = text_charter(false);
        charter.grants.wait = wait;
        let (run, main) = harness.running_on(95, 100, charter);
        assert!(
            harness
                .step(Event::Message {
                    finish_requested: false,
                    run,
                    name: Token::new(0),
                    label: bytes(b"person"),
                    text: bytes(b"late work")
                })
                .is_empty()
        );
        let emitted = harness.step(finish(main, 96, text_result(false, b"done")));
        if wait {
            assert_eq!(emitted.as_ref(), [returned(96, Returned::Crossed { text: crate::prompt::crossed() })]);
            let emitted = harness.step(end_turn(main));
            let [Request::Say { peer, text }] = emitted.as_ref() else { panic!("queued work is the continuation") };
            assert_eq!((*peer, text.as_ref()), (Token::new(100), b"person: late work".as_slice()));
            let emitted = harness.step(Event::Turn {
                conversation: main,
                record: Token::new(97),
                sequence: 1,
                opens_window: true,
            });
            let [Request::Turn { read, .. }] = emitted.as_ref() else { panic!("told turn fence") };
            assert_eq!(*read, Some(Token::new(0)));
            assert_eq!(
                harness.step(finish(main, 98, text_result(false, b"done"))).as_ref(),
                [returned(98, Returned::Accepted), Request::Close { peer: Token::new(100) }]
            );
        } else {
            assert_eq!(emitted.as_ref(), [returned(96, Returned::Accepted), Request::Close { peer: Token::new(100) }]);
        }
        harness.step(Event::Ended { conversation: main, end: End::Closed, spend: Spend::ZERO, detail: Box::default() });
        let facts = observed_facts(&mut harness);
        let mut read = 0_u32;
        let mut unread = 0_u32;
        for fact in facts {
            if let Fact::MessageRead { name, .. } = fact.kind
                && name == Token::new(0)
            {
                read += 1;
            }
            if let Fact::MessageUnread { name, .. } = fact.kind
                && name == Token::new(0)
            {
                unread += 1;
            }
        }
        assert_eq!((read, unread), if wait { (1, 0) } else { (0, 1) });
    }
}

fn identity_open(harness: &mut Harness, identity: Option<crate::Identity>) -> (Token, Token, Opening) {
    let emitted = harness.step(Event::Start {
        identity,
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
        reply_to: ReplyTo::new(Token::new(1)),
        host_run: Token::new(1),
        activation: 1,
        charter: agents(),
        workspace: None,
        transcript: None,
        resumed: false,
    });
    let Ok(pair) = Box::<[Request; 2]>::try_from(emitted) else { panic!("admission and main opening") };
    let [Request::Admitted { run, .. }, Request::Open { conversation, opening }] = *pair else {
        panic!("admission and main opening");
    };
    drop(harness.step(Event::Started { conversation, peer: Token::new(100) }));
    (run, conversation, opening)
}

#[test]
fn a_supplied_identity_reaches_main_and_children_with_ordinals_never_reused() {
    let identity = crate::Identity::new([0; 16]);
    let mut harness = Harness::new(LIMITS);
    let (_, main, opening) = identity_open(&mut harness, Some(identity));
    assert_eq!((opening.identity, opening.ordinal), (identity, 0));
    assert_eq!(identity.bytes(), [0; 16]);
    let (first, opening) = harness.child(main, 7, families(false, false, false), 101);
    assert_eq!((opening.identity, opening.ordinal), (identity, 1));
    drop(harness.step(Event::Ended {
        conversation: first,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    }));
    harness.domain.reclaim();
    let (_, second) = harness.child(main, 8, families(false, false, false), 102);
    assert_eq!((second.identity, second.ordinal), (identity, 2), "ended children never free their ordinal");
    assert_eq!(harness.domain.calls(), 1);
}

#[test]
fn absent_identities_draw_deterministically_only_for_admitted_unspecified_starts() {
    let mut first = Harness::new(LIMITS);
    first.domain = Domain::new(&LIMITS, 91);
    let (_, _, first_opening) = identity_open(&mut first, None);
    let mut same = Harness::new(LIMITS);
    same.domain = Domain::new(&LIMITS, 91);
    let mut invalid = charter();
    invalid.budget.turns = 0;
    let refused = same.step(carried_start(invalid, Box::default(), None));
    assert_eq!(refused.len(), 1);
    assert_eq!(same.domain.runs(), 0, "a refused start admits no run or identity");
    let (_, _, same_opening) = identity_open(&mut same, None);
    assert_eq!(first_opening.identity, same_opening.identity, "a refused start does not consume randomness");
    let mut supplied = Harness::new(LIMITS);
    supplied.domain = Domain::new(&LIMITS, 91);
    let (_, _, opening) = identity_open(&mut supplied, Some(crate::Identity::new([255; 16])));
    assert_eq!(opening.identity.bytes(), [255; 16]);
    let (_, _, opening) = identity_open(&mut supplied, None);
    assert_eq!(opening.identity, first_opening.identity, "a host identity consumes no admission randomness");
    let (_, _, next) = identity_open(&mut first, None);
    assert_ne!(next.identity, first_opening.identity, "successive admissions draw independent opaque bytes");
    let mut different = Harness::new(LIMITS);
    different.domain = Domain::new(&LIMITS, 92);
    assert_ne!(identity_open(&mut different, None).2.identity, first_opening.identity);
}

#[test]
fn workspace_host_paths_fit_their_receiving_bound_and_normalized_absolute_shape() {
    let mut mounted = workspace();
    let exact = u32::try_from(mounted.directories[0].host.len()).unwrap();
    assert_eq!(crate::workspace::check(Some(&mounted), &Limits { host_path_bytes: exact, ..LIMITS }), Ok(()));
    assert_eq!(
        crate::workspace::check(Some(&mounted), &Limits { host_path_bytes: exact - 1, ..LIMITS }),
        Err(Invalid::Workspace)
    );
    for host in
        [b"relative".as_slice(), b"/a/../b".as_slice(), b"/a//b".as_slice(), b"/a/".as_slice(), b"/a\0b".as_slice()]
    {
        mounted.directories[0].host = bytes(host);
        assert_eq!(crate::workspace::check(Some(&mounted), &LIMITS), Err(Invalid::Workspace));
    }
    mounted.directories[0].host = bytes(b"/");
    assert_eq!(crate::workspace::check(Some(&mounted), &LIMITS), Ok(()));
}
#[test]
fn final_turns_ignore_ordinary_nudges_and_fail_once_only_after_the_last() {
    let mut harness = Harness::new(Limits { nudges: 0, ..LIMITS });
    let mut selected = charter();
    selected.budget.turns = 4;
    selected.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(10) };
    let (_, main) = harness.running_on(1, 100, selected);
    drop(harness.step(Event::Used { conversation: main, spend: Spend { turns: 2, ..spend(1) } }));
    assert_eq!(
        harness.step(end_turn(main)).as_ref(),
        &[Request::Say { peer: Token::new(100), text: super::prompt::finish_now() }]
    );
    let first = crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0));
    let crate::CompletionPermit::Final { note: Some(note) } = first else {
        panic!("first final permit");
    };
    assert_eq!(note.as_ref(), b"The ordinary budget is spent. Finish now with what you have. You have at most 2 final completions and 10 seconds to finish. Use only finish, or deliver when offered.");
    for index in 0..2 {
        assert_eq!(
            crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0)),
            crate::CompletionPermit::Final { note: None }
        );
        let owner = Token::new(500 + index);
        assert_eq!(crate::reserve(&mut harness.domain, main, owner, crate::Most::Bounded(1)), Ok(()));
        assert!(crate::settle_reservation(&mut harness.domain, main, owner, 1));
        drop(harness.step(Event::Used { conversation: main, spend: spend(1) }));
        let requests = harness.step(end_turn(main));
        if index == 0 {
            assert_eq!(requests.as_ref(), &[Request::Say { peer: Token::new(100), text: super::prompt::finish_now() }]);
        } else {
            assert_eq!(requests.as_ref(), &[Request::Close { peer: Token::new(100) }]);
        }
    }
    let actual = Spend {
        turns: 4,
        input: Some(3),
        output: Some(30),
        cache_read: Some(0),
        cache_write: Some(0),
        reasoning: None,
        units: 3,
    };
    let (_, answer) = answered(harness.step(Event::Ended {
        conversation: main,
        end: End::Closed,
        spend: actual,
        detail: Box::default(),
    }));
    assert_eq!(answer, failed(Failure::Budget { exhausted: Exhausted::Turns, used: Some(4), bound: Some(4) }, actual));
}

#[test]
fn a_report_accepted_in_main_final_work_remains_accepted_once() {
    let mut harness = Harness::new(LIMITS);
    let mut selected = text_charter(false);
    selected.budget.turns = 4;
    selected.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(10) };
    let (run, main) = harness.running_on(1, 100, selected);
    drop(harness.step(Event::Used { conversation: main, spend: Spend { turns: 2, ..Spend::ZERO } }));
    let permit = crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(0));
    let crate::CompletionPermit::Final { note: Some(_) } = permit else {
        panic!("the accepted report is in actual final work");
    };
    assert_eq!(crate::reserve(&mut harness.domain, main, Token::new(500), crate::Most::Bounded(1)), Ok(()));
    assert!(crate::settle_reservation(&mut harness.domain, main, Token::new(500), 0));
    drop(harness.step(Event::Used { conversation: main, spend: Spend { turns: 1, ..Spend::ZERO } }));
    assert_eq!(
        harness.step(finish(main, 7, report())).as_ref(),
        &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]
    );
    let actual = Spend { turns: 3, ..Spend::ZERO };
    assert_eq!(
        answered(harness.step(Event::Ended {
            conversation: main,
            end: End::Closed,
            spend: actual,
            detail: Box::default()
        }))
        .1,
        Answer::Accepted { outcome: report(), spent: actual, turns: 0 }
    );
    assert!(harness.step(Event::Cancel { run }).is_empty(), "no second answer after acceptance");
}

#[test]
fn ordinary_time_exhaustion_parks_a_wait_and_withholds_its_actual_call() {
    let mut harness = Harness::new(Limits { waiting: Duration::from_secs(600), ..LIMITS });
    let mut selected = charter();
    selected.grants.wait = true;
    selected.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(60) };
    selected.waiting = Duration::from_secs(600);
    let (_, main) = harness.running_on(1, 100, selected);
    assert_eq!(
        harness
            .step(Event::Delegated {
                conversation: main,
                call: Token::new(91),
                name: crate::CallName { activation: 1, completion: 1, position: 0 },
                ask: Ask::Wait,
                deadline: EXPIRY
            })
            .as_ref(),
        &[Request::Waiting { host_run: Token::new(1), read: None }]
    );
    harness.env.now = Time::ZERO.saturating_add(Duration::from_secs(540));
    assert_eq!(harness.fire().as_ref(), &[returned(91, Returned::Withheld), Request::Close { peer: Token::new(100) }]);
    let (_, answer) = answered(harness.step(Event::Ended {
        conversation: main,
        end: End::Closed,
        spend: Spend::ZERO,
        detail: Box::default(),
    }));
    assert_eq!(answer, Answer::Parked { spent: Spend::ZERO, turns: 0 });
}

#[test]
fn child_wrap_up_is_an_exact_quarter_of_750ms_and_sixty_seconds_of_one_hour() {
    assert_eq!(
        super::agent::final_allowance(Duration::from_millis(750), Duration::from_secs(60)),
        Duration::from_nanos(187_500_000)
    );
    assert_eq!(
        super::agent::final_allowance(Duration::from_secs(3600), Duration::from_secs(60)),
        Duration::from_secs(60)
    );
    for (time, expected) in [
        (Duration::from_millis(750), Duration::from_nanos(187_500_000)),
        (Duration::from_secs(3600), Duration::from_secs(60)),
    ] {
        let mut harness = Harness::new(Limits {
            wrap_up: Duration::from_secs(60),
            budget: Budget { time: Duration::from_secs(7200), ..LIMITS.budget },
            ..LIMITS
        });
        let mut selected = agents();
        selected.budget.time = Duration::from_secs(7200);
        let (_, main) = harness.running_on(1, 100, selected);
        let mut request =
            ask(main, 7, families(true, false, false), None, Some(crate::Share { turns: Some(2), time: Some(time) }));
        let Event::Delegated { deadline, .. } = &mut request else { unreachable!() };
        *deadline = Time::ZERO.saturating_add(Duration::from_secs(7200));
        let emitted = harness.step(request);
        let [Request::Open { opening, .. }] = emitted.as_ref() else {
            panic!("actual child opening");
        };
        assert_eq!(opening.budget.time, time);
        let final_turn = opening.final_turn.as_ref().expect("child gets its final allowance");
        assert_eq!(final_turn.allowance, expected);
        assert_eq!(
            final_turn.note.as_ref(),
            b"Your share is ending. This is your final turn. Answer now with what you have. No tool calls will run."
        );
    }
}

#[test]
fn a_capped_child_answer_keeps_its_budget_stop_until_the_actual_end_and_returns_once() {
    let mut harness = Harness::new(LIMITS);
    let (_, main) = harness.running_on(1, 100, agents());
    let (child, _) = harness.child(main, 7, families(true, false, false), 101);
    drop(harness.step(Event::Used { conversation: child, spend: spend(50) }));
    assert_eq!(
        harness
            .step(Event::Yielded { conversation: child, stop: Stop::Budget, text: bytes(b"What I found.") })
            .as_ref(),
        &[Request::Close { peer: Token::new(101) }]
    );
    let emitted =
        harness.step(Event::Ended { conversation: child, end: End::Closed, spend: spend(50), detail: Box::default() });
    let [Request::Return { call, result: Returned::Answered { text, stop, used, .. }, spent }] = emitted.as_ref()
    else {
        panic!("actual parent receives one answered terminal: {emitted:?}");
    };
    assert_eq!(*call, Token::new(7));
    assert_eq!(text.as_ref(), b"What I found.");
    assert_eq!(*stop, Stop::Budget);
    assert_eq!(used.turns, 1);
    assert_eq!(*spent, 50);
    assert!(
        harness
            .step(Event::Ended { conversation: child, end: End::Closed, spend: spend(50), detail: Box::default() })
            .is_empty(),
        "stale end cannot answer twice"
    );
}

mod room_controls {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "programming-model.md, section 10.2: deterministic test harnesses are ordinary Rust"
    )]
    use super::{
        Answer, Ask, AskRefusal, Charter, EXPIRY, Event, FieldRule, Harness, Invalid, LIMITS, Limits, Refusal, Request,
        Returned, Token, agents, bytes, charter, families,
    };
    use alloc::boxed::Box;
    fn large_host_charter(instructions: &[u8]) -> Charter {
        let mut selected = charter();
        selected.grants.host_tools = Box::default();
        selected.instructions = bytes(instructions);
        selected.outcome.change = None;
        selected.grants.deliver = None;
        selected.outcome.report = Some(crate::outcome::TextSpec {
            max: 32,
            fields: (0_u32..20)
                .map(|index| FieldRule { name: format!("long_contract_field_{index:02}").into_bytes().into(), max: 32 })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        selected
    }

    #[test]
    fn charter_rendering_at_the_host_edge_is_admitted_and_one_byte_more_refuses() {
        let selected = large_host_charter(b"base");
        let receiving = Limits { run_bytes: 16_384, outcome_bytes: 8192, ..LIMITS };
        let edge = crate::prompt::host_bytes(&selected, None, receiving.guide_bytes).expect("rendered host")
            + receiving.schema_costs.finish(&selected.outcome).expect("schema")
            + receiving.schema_costs.finish_text;
        let receiving = Limits { host_part: edge, ..receiving };
        let mut admitted = Harness::new(receiving);
        let requests = admitted.start_workspace(1, selected, None);
        let Some(Request::Admitted { .. }) = requests.first() else { panic!("exact edge: {requests:?}") };
        let selected = large_host_charter(b"basex");
        let mut refused = Harness::new(receiving);
        let requests = refused.start_workspace(1, selected, None);
        let [Request::Answer { answer: Answer::Refused(Refusal::Invalid(Invalid::HostPart { bound })), .. }] =
            &requests[..]
        else {
            panic!("one byte over: {requests:?}")
        };
        assert_eq!(*bound, edge);
    }

    #[test]
    fn a_charter_model_below_the_derived_room_is_refused_before_preparation() {
        let selected = charter();
        let [numerator, denominator] = LIMITS.compaction_threshold;
        let room = u64::from(selected.llm.window) * u64::from(numerator) / u64::from(denominator)
            - 2 * u64::from(selected.llm.output);
        let receiving = Limits { room: room + 1, ..LIMITS };
        let mut refused = Harness::new(receiving);
        let requests = refused.start(1, selected);
        let [Request::Answer { answer: Answer::Refused(Refusal::Invalid(Invalid::Room { bound })), .. }] =
            &requests[..]
        else {
            panic!("room refusal: {requests:?}")
        };
        assert_eq!(*bound, room + 1);
    }

    #[test]
    fn a_sub_agent_brief_and_its_inherited_context_must_fit_the_host_part_before_any_open() {
        for extra in [0, 1] {
            let bound = 10_000_u64;
            let receiving = Limits { host_part: bound, ..LIMITS };
            let mut harness = Harness::new(receiving);
            let (_, main) = harness.running_on(1, 100, agents());
            let requests = harness.step(Event::Delegated {
                name: crate::CallName { activation: 1, completion: 1, position: 0 },
                conversation: main,
                call: Token::new(7),
                ask: Ask::SubAgent {
                    brief: vec![b'x'; usize::try_from(bound + extra).expect("small test bound")].into(),
                    families: families(true, false, false),
                    llm: None,
                    share: crate::Share::UNASKED,
                },
                deadline: EXPIRY,
            });
            let [
                Request::Return {
                    call,
                    result: Returned::Refused { refusal: AskRefusal::TooLarge { bound: actual } },
                    spent: 0,
                },
            ] = &requests[..]
            else {
                panic!("no child opens: {requests:?}")
            };
            assert_eq!(*call, Token::new(7));
            assert_eq!(*actual, bound);
        }
    }
}

#[test]
fn the_completion_maximum_prices_the_full_window_and_rounds_each_reserved_turn_once() {
    let main = Llm {
        window: 7,
        output: 4,
        prices: crate::Prices { input: 2, cached: 5, cache_write: Some(3), output: 3, unit: 10 },
        ..charter().llm
    };
    assert_eq!(crate::most_costly(&main), Some(5));
    let reserve = crate::charter::Reserve { turns: 3, time: Duration::from_secs(1) };
    assert_eq!(crate::reserve_spend(reserve, &main), Some(15));
    let tiny = Llm { window: 1, output: 1, prices: crate::Prices { unit: 3, ..charter().llm.prices }, ..main.clone() };
    assert_eq!(crate::most_costly(&tiny), Some(1));
    assert_eq!(crate::reserve_spend(reserve, &tiny), Some(3));
    let written = Llm { prices: crate::Prices { cache_write: Some(8), ..main.prices }, ..main.clone() };
    assert_eq!(crate::most_costly(&written), Some(7));
}

#[test]
fn completion_and_reserve_derivation_refuse_unrepresentable_host_units() {
    let main = Llm {
        window: 1,
        output: 1,
        prices: crate::Prices { input: u64::MAX - 1, cached: 1, cache_write: None, output: 1, unit: 1 },
        ..charter().llm
    };
    assert_eq!(crate::most_costly(&main), Some(u64::MAX));
    let reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(1) };
    assert_eq!(crate::reserve_spend(reserve, &main), None);
    assert_eq!(crate::most_costly(&Llm { window: 2, ..main.clone() }), None);
    assert_eq!(crate::most_costly(&Llm { prices: crate::Prices { unit: 0, ..main.prices }, ..main }), None);
}

#[test]
fn entrance_names_spend_for_a_completion_or_an_impossible_finishing_reserve() {
    let main = Llm {
        window: 7,
        output: 4,
        prices: crate::Prices { input: 2, cached: 5, cache_write: Some(3), output: 3, unit: 10 },
        ..charter().llm
    };
    let reserve = crate::charter::Reserve { turns: 3, time: Duration::from_secs(1) };
    assert_eq!(
        crate::admit(&Budget { spend: 4, ..BUDGET }, reserve, &main, &LIMITS.budget, None),
        Err(crate::BudgetRefusal::Completion { most: 5 })
    );
    assert_eq!(
        crate::admit(&Budget { spend: 15, ..BUDGET }, reserve, &main, &LIMITS.budget, None),
        Err(crate::BudgetRefusal::Reserve(crate::Part::Spend))
    );
    assert_eq!(crate::admit(&Budget { spend: 16, ..BUDGET }, reserve, &main, &LIMITS.budget, None), Ok(()));
}

#[test]
fn spend_above_the_reserve_runs_out_then_main_finishes_and_is_accepted() {
    let mut harness = Harness::new(LIMITS);
    let mut selected = text_charter(false);
    selected.budget.spend = 3;
    selected.reserve = crate::charter::Reserve { turns: 2, time: Duration::from_secs(10) };
    let (run, main) = harness.running_on(1, 100, selected);
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(1)),
        crate::CompletionPermit::Allowed
    );
    assert_eq!(crate::reserve(&mut harness.domain, main, Token::new(500), crate::Most::Bounded(1)), Ok(()));
    assert!(crate::settle_reservation(&mut harness.domain, main, Token::new(500), 1));
    drop(harness.step(Event::Used { conversation: main, spend: spend(1) }));
    let permit = crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(1));
    let crate::CompletionPermit::Final { note: Some(_) } = permit else { panic!("spend starts final work") };
    assert_eq!(crate::reserve(&mut harness.domain, main, Token::new(501), crate::Most::Bounded(1)), Ok(()));
    assert!(crate::settle_reservation(&mut harness.domain, main, Token::new(501), 1));
    drop(harness.step(Event::Used { conversation: main, spend: spend(1) }));
    assert_eq!(
        harness.step(finish(main, 7, report())).as_ref(),
        &[returned(7, Returned::Accepted), Request::Close { peer: Token::new(100) }]
    );
    let actual = spend(1).accumulate(spend(1)).expect("two bounded charges");
    assert_eq!(
        answered(harness.step(Event::Ended {
            conversation: main,
            end: End::Closed,
            spend: actual,
            detail: Box::default()
        }))
        .1,
        Answer::Accepted { outcome: report(), spent: actual, turns: 0 }
    );
    assert!(harness.step(Event::Cancel { run }).is_empty());
}

#[test]
fn no_final_turns_keeps_the_spend_trigger_when_the_next_maximum_cannot_fit() {
    let mut harness = Harness::new(LIMITS);
    let mut selected = charter();
    selected.budget.spend = 10;
    selected.llm.prices.unit = 1000;
    let (_, main) = harness.running_on(1, 100, selected);
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(9)),
        crate::CompletionPermit::Allowed
    );
    assert_eq!(crate::reserve(&mut harness.domain, main, Token::new(500), crate::Most::Bounded(9)), Ok(()));
    assert!(crate::settle_reservation(&mut harness.domain, main, Token::new(500), 2));
    drop(harness.step(Event::Used { conversation: main, spend: spend(2) }));
    assert_eq!(
        crate::completion_permit(&mut harness.domain, main, harness.env.now, crate::Most::Bounded(9)),
        crate::CompletionPermit::Denied(Exhausted::Spend)
    );
}

#[test]
fn withdrawn_checks_tell_one_cancelled_terminal_before_or_after_spawn() {
    for after_spawn in [false, true] {
        let mut harness = Harness::new(LIMITS);
        let (_, conversation) = harness.coding(1, 100);
        drop(facts(&mut harness));
        let owner = harness.land(conversation, 7);
        assert_eq!(
            harness.step(Event::Withdraw { conversation, call: Token::new(7) }).as_ref(),
            [Request::Abort { owner }]
        );
        let mut machine = smith_machine_world::World::new(827, &[]);
        let ended = machine.aborted_check(owner, after_spawn);
        assert_eq!(harness.step(Event::Aborted { owner: ended }).as_ref(), [returned(7, Returned::Cancelled)]);
        assert_eq!(harness.checks_ended, 1);
        assert_check_fact_pair(&mut harness, b"Cancelled");
        assert!(harness.step(Event::Withdraw { conversation, call: Token::new(7) }).is_empty());
        assert!(facts(&mut harness).is_empty(), "settled withdrawal cannot make another terminal");
        machine.settle();
    }
}

#[test]
fn expired_checks_tell_one_timed_out_terminal_before_or_after_spawn() {
    for after_spawn in [false, true] {
        let mut harness = Harness::new(LIMITS);
        let (_, conversation) = harness.coding(1, 100);
        drop(facts(&mut harness));
        let deadline = Time::ZERO.saturating_add(Duration::from_secs(60));
        let emitted = harness.step(finish_by(conversation, 7, Declared::Change(change()), deadline));
        let [Request::Check { owner, .. }, Request::Checking { .. }] = emitted.as_ref() else {
            panic!("checks start");
        };
        let owner = *owner;
        harness.after(Duration::from_secs(60));
        assert_eq!(harness.fire().as_ref(), [Request::Abort { owner }]);
        assert!(!harness.domain.is_due(harness.env.now), "call deadline fires once");
        let mut machine = smith_machine_world::World::new(828, &[]);
        let ended = machine.aborted_check(owner, after_spawn);
        assert_eq!(harness.step(Event::Aborted { owner: ended }).as_ref(), [returned(7, Returned::TimedOut)]);
        assert_eq!(harness.checks_ended, 1);
        assert_check_fact_pair(&mut harness, b"TimedOut");
        machine.settle();
    }
}

#[expect(
    clippy::disallowed_macros,
    reason = "programming-model.md10.2: test Debug comparison gives current-parent runtime RED without requiring the new enum variant"
)]
fn assert_check_fact_pair(harness: &mut Harness, expected: &[u8]) {
    let mut records = List::<Box<[u8]>>::with_capacity(2);
    let mut started_run = None;
    for fact in facts(harness).as_ref() {
        match fact {
            Fact::CheckStarted { run, .. } => {
                started_run = Some(*run);
                records.push(Box::from(b"started".as_slice())).expect("one start and one terminal");
            }
            Fact::CheckFinished { run, exit } => {
                assert_eq!(Some(*run), started_run, "terminal belongs to the actual started run");
                records
                    .push(alloc::format!("{exit:?}").into_bytes().into_boxed_slice())
                    .expect("one start and one terminal");
            }
            Fact::MessageRead { .. }
            | Fact::MessageFence { .. }
            | Fact::MessageUnread { .. }
            | Fact::MessageReceived { .. }
            | Fact::MessageRefused { .. }
            | Fact::Admitted { .. }
            | Fact::Prepared { .. }
            | Fact::Opened { .. }
            | Fact::Budget { .. }
            | Fact::Ended { .. }
            | Fact::Called { .. }
            | Fact::Returned { .. }
            | Fact::Delivered { .. }
            | Fact::Answered { .. } => {}
        }
    }
    assert_eq!(records.len(), 2, "each real check start owes its terminal");
    assert_eq!(records.get(0).expect("start").as_ref(), b"started");
    assert_eq!(records.get(1).expect("terminal").as_ref(), expected);
}

#[test]
fn the_summary_instruction_and_its_preamble_fit_smiths_own_part() {
    let fixed = super::prompt::fixed_prompt();
    assert!(fixed.ends_with(b"Summarise the work so far. The work is not finished: a new context window will continue it, opening with the task's own words. Answer with text only; call no tool. Write what the next window needs that the task does not say: what was done and found, with exact values, names and paths as they were read; what remains; and the decisions taken. Do not restate the task or its required answer. Keep within 1,000 words.What follows is your own summary of an earlier context window. The work continues in this window:"));
    assert_eq!(super::prompt::summary_bytes(), 415);
    assert!(super::prompt::fixed_prompt_bytes().expect("bounded own part") <= 8_192);
}

#[test]
fn a_charter_model_with_a_negative_spare_is_refused() {
    let mut selected = charter();
    selected.llm.window = 10_000;
    selected.llm.output = 1019;
    let mut harness = Harness::new(Limits { compaction_material: 982, ..LIMITS });
    let requests = harness.start(1, selected);
    let [Request::Answer { answer, .. }] = requests.as_ref() else { panic!("refused: {requests:?}") };
    assert_eq!(*answer, Answer::Refused(Refusal::Invalid(Invalid::Llm)));
}

#[test]
fn a_charter_model_with_a_spare_of_zero_is_admitted() {
    let mut selected = charter();
    selected.llm.window = 10_000;
    selected.llm.output = 1018;
    let mut harness = Harness::new(Limits { compaction_material: 982, ..LIMITS });
    let requests = harness.start(1, selected);
    let [Request::Admitted { host_run, .. }, Request::Read { .. }] = requests.as_ref() else {
        panic!("admitted: {requests:?}")
    };
    assert_eq!(*host_run, Token::new(1));
}

#[test]
fn a_sub_agent_model_with_a_negative_spare_is_refused() {
    let mut selected = agents();
    selected.models[0].window = 10_000;
    selected.models[0].output = 1019;
    let mut harness = Harness::new(Limits { compaction_material: 982, ..LIMITS });
    let requests = harness.start(1, selected);
    let [Request::Answer { answer, .. }] = requests.as_ref() else { panic!("refused: {requests:?}") };
    assert_eq!(*answer, Answer::Refused(Refusal::Invalid(Invalid::Llm)));
}

#[test]
fn each_conversation_opens_with_its_line() {
    let mut selected = agents();
    selected.llm.window = 272_000;
    selected.llm.output = 32_000;
    selected.models[0].window = 272_000;
    selected.models[0].output = 32_000;
    let mut harness = Harness::new(Limits { compaction_material: 982, ..LIMITS });
    let requests = harness.start(1, selected);
    let [Request::Admitted { run, .. }, Request::Read { .. }] = requests.as_ref() else {
        panic!("admitted: {requests:?}")
    };
    let requests = harness.step(Event::Read { owner: *run, read: Read::Missing });
    let [Request::Open { conversation: main, opening }] = requests.as_ref() else {
        panic!("main opening: {requests:?}")
    };
    assert_eq!(opening.line, 239_018);
    let main = *main;
    drop(harness.step(Event::Started { conversation: main, peer: Token::new(100) }));
    let requests = harness.step(ask(main, 7, families(true, false, false), Some(bytes(b"model-b")), None));
    let [Request::Open { opening, .. }] = requests.as_ref() else { panic!("child opening: {requests:?}") };
    assert_eq!(opening.line, 239_018);
}
