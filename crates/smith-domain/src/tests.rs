//! Feed the domain events, inspect the requests that come out: the paths that
//! cross from the run to the sessions and back, the tickets the top level
//! keeps for them, and the hand-offs it defers to the ready list. What each
//! child domain does on its own is its own tests' business.

#[path = "tests/builtin.rs"]
mod builtin;

#[path = "tests/jobs.rs"]
mod jobs;

mod program;

use alloc::boxed::Box;

use skein_lib::{Duration, Env, List, Queue, ReplyTo, Time, Token, Wall};
use smith_domain_run::charter::{Endpoint, Families, Grants, Llm, Tools};
use smith_domain_run::outcome::{Change, ChangeSpec, Declared, OutcomeSpec, Verdict, VerdictRule};
use smith_domain_run::{self as run, Ask, Charter};
use smith_domain_run::{Directory, Workspace};
use smith_domain_session as session;

use crate::limits;
use crate::llm::{Block, Completion, Decoded, Message, Problem, Prompt, Returned, Role, Said, Served, Stop, Usage};
use crate::tools::{self, Call, Name, Op, Part, Path, Place};
use crate::{Domain, Event, Fact, Limits, Request, fire, max_out, resume, step, valid};

#[test]
fn oversized_saved_answer_says_the_call_was_decided() {
    let answered = Box::from([crate::AnsweredCall {
        name: run::CallName { activation: 2, completion: 3, position: 1 },
        tool: Box::from(&b"check"[..]),
        answer: crate::Answered::TooLarge,
    }]);
    let prompt = crate::waking::render(answered, &LIMITS).expect("bounded waking text");
    let mut decided = false;
    let mut oversized = false;
    for part in prompt.windows(b"host decided this call".len()) {
        if part == b"host decided this call" {
            decided = true;
        }
    }
    for part in prompt.windows(b"answer was too large to give".len()) {
        if part == b"answer was too large to give" {
            oversized = true;
        }
    }
    assert!(decided && oversized);
}

const BUDGET: run::Budget = run::Budget { turns: 10, spend: 10_000, time: Duration::from_secs(600) };

const CEILING: session::Budget = session::Budget {
    turns: 100,
    input: 1_000_000,
    output: 100_000,
    cache_read: 1_000_000,
    cache_write: 1_000_000,
    time: Duration::from_secs(3600),
};

const LIMITS: Limits = Limits {
    accounts: 4,
    endpoints: 3,
    configured_model_bytes: 4096,
    decoded_call_bytes: 4096,
    skew: Duration::ZERO,
    run: run::Limits {
        runs: 2,
        conversations: 4,
        run_bytes: 4096,
        brief_sections: 4,
        host_part: 1 << 20,
        room: 0,
        compaction_threshold: [4, 5],
        compaction_material: 0,
        schema_costs: run::SchemaCosts::NONE,
        directories: 2,
        directory_name_bytes: 64,
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
        calls: 4,
        budget: run::Budget { turns: CEILING.turns, spend: 10_000, time: CEILING.time },

        models: 1,
        run_conversations: 3,
        answer_bytes: 64,
        nudges: 1,
        guide_bytes: 64,
        io_timeout: Duration::from_secs(10),
        outcome_bytes: 1024,
        delivery_timeout: Duration::from_secs(300),
        check_timeout: Duration::from_secs(300),
        wrap_up: Duration::from_secs(60),
        check_tail: 256,
        facts: 64,
        messages: 8,
        message_bytes: 4096,
        offer_messages: 8,
        offer_bytes: 32_782,
        waiting: Duration::from_secs(300),
    },
    session: session::Limits {
        threshold: [4, 5],
        token_bytes: 8,
        sessions: 4,
        spend: 10_000,
        protocol_allowance: 4096,
        history_blocks: u32::MAX,
        history_messages: 16,
        history_bytes: 2_097_152,
        completion_bytes: 4096,
        answer_bytes: 4096,
        completion_blocks: 16,
        failure_bytes: 512,
        delegated_result_bytes: 262_144,
        budget: CEILING,
        retries: 1,
        backoff_base: Duration::from_millis(100),
        backoff_max: Duration::from_secs(1),

        facts: 128,
        parallel_tools: 2,
        tools: tools::Limits {
            shell_output: 128,
            kits: 4,
            calls: 4,
            repos: 2,
            path_bytes: 64,
            host_path_bytes: 4096,
            known_files: 8,
            file_bytes: 65_536,
            tool_payload: 65_536,
            read_bytes: 4096,
            list_entries: 16,
            list_bytes: 4096,
            match_lines: 4,
            file_timeout: Duration::from_secs(60),
            jobs: 4,
            job_poll: Duration::from_secs(30),
            job_wait: Duration::from_secs(30),
            shell_timeout: Duration::from_secs(60),
            tool_deadline: Duration::from_secs(600),
            shell_head: 64,
            shell_tail: 64,
            search_hits: 8,
            search_bytes: 256,
            search_timeout: Duration::from_secs(30),
            facts: 64,
        },
    },
};

const USAGE: Usage = Usage {
    input_tokens: Some(100),
    output_tokens: Some(10),
    cache_read_tokens: Some(0),
    cache_write_tokens: Some(0),
    reasoning_tokens: None,
};

#[test]
fn decoded_batch_reserves_refusal_cells_before_any_payload() {
    let cell = u64::try_from(size_of::<Decoded>()).expect("cell size fits");
    let offered = crate::translate::Offered {
        main: true,
        host_tools: Box::new([]),
        finish: false,
        deliver: false,
        agents: false,
        wait: true,
    };
    let mut peer = crate::peer::Peer::new(Token::new(17), 1, Some(0), 0, offered, &LIMITS.session);
    let completion = Completion {
        reasoning_dropped: Box::default(),
        content: Box::new([
            Said::ToolCall {
                id: skein_lib::bytes::copy_of(b"oversized"),
                name: skein_lib::bytes::copy_of(b"invalid"),
                input: skein_lib::bytes::copy_of(b"{}"),
                call: Decoded::Invalid { problem: Problem::Missing { field: Box::new([b'x'; 32]) } },
                replay: None,
            },
            Said::ToolCall {
                id: skein_lib::bytes::copy_of(b"fits"),
                name: skein_lib::bytes::copy_of(b"wait"),
                input: skein_lib::bytes::copy_of(b"{}"),
                call: Decoded::Served { ask: Ask::Wait },
                replay: None,
            },
        ]),
        stop: Stop::ToolUse,
        usage: USAGE,
    };
    let completion = peer.completion(completion, cell.checked_mul(2).expect("two cells fit"));
    let mut calls = List::with_capacity(2);
    for block in completion.content {
        match block {
            session::llm::Block::ToolCall { call, .. } => calls.push(call).expect("two calls"),
            session::llm::Block::Text { .. }
            | session::llm::Block::Refusal { .. }
            | session::llm::Block::Opaque { .. }
            | session::llm::Block::ToolResult { .. } => unreachable!("provider supplied only calls"),
        }
    }
    match calls.get(0).expect("first call") {
        session::llm::Decoded::Invalid { problem, .. } => assert_eq!(problem, &Problem::TooLarge),
        session::llm::Decoded::Owned { .. }
        | session::llm::Decoded::Delegated { .. }
        | session::llm::Decoded::Builtin { .. }
        | session::llm::Decoded::Historical => unreachable!("oversized payload is refused"),
    }
    match calls.get(1).expect("second call") {
        session::llm::Decoded::Delegated { .. } => assert_eq!(peer.tickets(), 1),
        session::llm::Decoded::Owned { .. }
        | session::llm::Decoded::Invalid { .. }
        | session::llm::Decoded::Builtin { .. }
        | session::llm::Decoded::Historical => unreachable!("later payload-free wait fits"),
    }
}

#[test]
fn decoded_receiving_cells_are_required_before_provider_work() {
    let mut limits = LIMITS;
    limits.decoded_call_bytes = u64::from(limits.session.completion_blocks)
        .checked_mul(u64::try_from(size_of::<Decoded>()).expect("cell size fits"))
        .expect("configured cells fit");
    assert!(valid(&limits));
    limits.decoded_call_bytes = limits.decoded_call_bytes.checked_sub(1).expect("nonempty receiving cells");
    assert!(!valid(&limits));
}

/// The domain, its environment, and room for one entry point's output.
struct Harness {
    observed: Queue<Fact>,
    domain: Domain,
    env: Env<Limits>,
    out: Queue<Request>,
    turns: List<crate::Turn>,
    long: List<Option<crate::Operation>>,
    checking: bool,
}

impl Harness {
    fn keep_facts(&mut self) {
        while let Some(fact) = self.domain.pop_fact() {
            self.observed.push(fact);
        }
    }

    fn pop_fact(&mut self) -> Option<Fact> {
        match self.observed.pop() {
            Some(fact) => Some(fact),
            None => self.domain.pop_fact(),
        }
    }

    fn new() -> Harness {
        Harness::with(&LIMITS)
    }

    fn with(limits: &Limits) -> Harness {
        Harness {
            observed: Queue::with_capacity(4096),
            domain: Domain::new(
                limits,
                crate::Config {
                    programs: Box::new([]),
                    endpoints: Box::new([Endpoint(1), Endpoint(2)]),
                    models: Box::new([
                        crate::ConfiguredModel {
                            efforts: Box::new([]),
                            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
                            protocol_allowance: 0,
                            endpoint: Endpoint(1),
                            model: Box::from(&b"model-a"[..]),
                            window: 8192,
                            output: 1024,
                            head: Duration::from_secs(60),
                            idle: Duration::from_secs(30),
                        },
                        crate::ConfiguredModel {
                            efforts: Box::new([]),
                            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
                            protocol_allowance: 0,
                            endpoint: Endpoint(1),
                            model: Box::from(&b"model-b"[..]),
                            window: 8192,
                            output: 1024,
                            head: Duration::from_secs(60),
                            idle: Duration::from_secs(30),
                        },
                        crate::ConfiguredModel {
                            efforts: Box::new([]),
                            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
                            protocol_allowance: 0,
                            endpoint: Endpoint(2),
                            model: Box::from(&b"model-a"[..]),
                            window: 8192,
                            output: 1024,
                            head: Duration::from_secs(60),
                            idle: Duration::from_secs(30),
                        },
                        crate::ConfiguredModel {
                            efforts: Box::new([]),
                            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
                            protocol_allowance: 0,
                            endpoint: Endpoint(2),
                            model: Box::from(&b"model-b"[..]),
                            window: 8192,
                            output: 1024,
                            head: Duration::from_secs(60),
                            idle: Duration::from_secs(30),
                        },
                    ]),
                },
                1,
            ),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits },
            out: Queue::with_capacity(max_out(limits)),
            turns: List::with_capacity(CEILING.turns),
            long: List::with_capacity(4096),
            checking: false,
        }
    }

    /// Steps `event`, returning what it emitted, oldest first.
    fn step(&mut self, event: Event) -> Box<[Request]> {
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

    /// The loop's next iteration, as far as the domain goes: the reclaim point
    /// of this one, then whatever is ready, at the start of the domain's stage.
    fn next(&mut self) -> Box<[Request]> {
        self.domain.reclaim();
        let mut all = List::with_capacity(max_out(&LIMITS));
        for _ in 0..max_out(&LIMITS) {
            if !self.domain.is_ready() {
                break;
            }
            self.keep_facts();
            resume(&mut self.domain, &self.env, &mut self.out);
            for request in self.drain() {
                all.push(request).expect("a test's iteration emits little");
            }
        }
        assert!(!self.domain.is_ready(), "what is made ready while the list is drained waits for the next iteration");
        all.into_boxed()
    }

    fn drain(&mut self) -> Box<[Request]> {
        let mut requests = List::with_capacity(max_out(&LIMITS));
        for _ in 0..max_out(&LIMITS) {
            let Some(request) = self.out.pop() else { break };
            match request {
                Request::Turn { turn, number, .. } => {
                    assert_eq!(
                        number,
                        self.turns.len().checked_add(1).expect("next actual turn number"),
                        "actual main output numbering"
                    );
                    self.turns.push(turn).expect("bounded activation fixture turns");
                }
                Request::Answer { to, answer, read, detail: _ } => {
                    match &answer {
                        run::Answer::Parked { turns, .. }
                        | run::Answer::Accepted { turns, .. }
                        | run::Answer::Failed { turns, .. } => {
                            assert_eq!(*turns, self.turns.len(), "terminal follows every actual Turn");
                        }
                        run::Answer::Refused(_) => assert!(self.turns.is_empty()),
                    }
                    requests
                        .push(Request::Answer { to, answer, read, detail: Box::default() })
                        .expect("room for max_out");
                }
                Request::Long { host_run, operation, until } => {
                    self.long.push(Some(operation)).expect("bounded issued operation history");
                    match operation {
                        crate::Operation::Checks => {
                            self.checking = true;
                            requests.push(Request::Long { host_run, operation, until }).expect("room for max_out");
                        }
                        crate::Operation::Completion | crate::Operation::Command => {}
                    }
                }
                Request::LongDone { host_run } => {
                    self.long.push(None).expect("bounded actual operation terminals");
                    if self.checking {
                        self.checking = false;
                        requests.push(Request::LongDone { host_run }).expect("room for max_out");
                    }
                }
                request @ (Request::HostCall { .. }
                | Request::WithdrawHost { .. }
                | Request::Admitted { .. }
                | Request::Deliver { .. }
                | Request::Complete { .. }
                | Request::Unauthorized { .. }
                | Request::Refused { .. }
                | Request::Limited { .. }
                | Request::Cancel { .. }
                | Request::Yield { .. }
                | Request::Io { .. }
                | Request::Job { .. }
                | Request::CancelIo { .. }
                | Request::Read { .. }
                | Request::Probe { .. }
                | Request::Check { .. }
                | Request::Abort { .. }
                | Request::MessageRefused { .. }
                | Request::Waiting { .. }) => {
                    requests.push(request).expect("room for max_out");
                }
            }
        }
        assert!(self.out.is_empty(), "an entry point emits at most max_out");
        requests.into_boxed()
    }

    /// Starts a run of `charter` for call `call`, and has it find no guide:
    /// the run's token, and main's first call to its LLM.
    fn admit(&mut self, call: u64, charter: Charter) -> (Token, Token, Prompt) {
        let (run, owner, prompt, _) = self.admit_named(call, charter);
        (run, owner, prompt)
    }

    fn admit_named(&mut self, call: u64, charter: Charter) -> (Token, Token, Prompt, crate::llm::CompletionName) {
        self.admit_named_grants(call, charter, account_grants())
    }

    fn admit_named_grants(
        &mut self,
        call: u64,
        charter: Charter,
        grants: Box<[crate::Grant]>,
    ) -> (Token, Token, Prompt, crate::llm::CompletionName) {
        let emitted = self.step(Event::Start {
            messages: Box::default(),
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
            answered: Box::default(),
            workspace: Some(workspace()),
            grants,
            reply_to: ReplyTo::new(Token::new(call)),
            host_run: Token::new(call),
            identity: None,
            activation: 1,
            charter,
            transcript: None,
        });
        let [Request::Admitted { host_run: _, run }, Request::Read { owner, .. }] = &*emitted else {
            panic!("expected an admitted run, got {emitted:?}");
        };
        assert_eq!(owner, run);
        let run = *run;
        let emitted = self.step(Event::Read { owner: run, read: run::Read::Missing });
        let prepared = match emitted.as_ref() {
            [Request::Probe { owner, .. }] => self.step(Event::Probed { owner: *owner, executable: false }),
            [Request::Complete { .. }] => emitted,
            _ => panic!("expected convention probe or prepared main, got {emitted:?}"),
        };
        let (main, prompt, name) = completion_retry::named(prepared);
        (run, main, prompt, name)
    }

    /// The session `owner`'s LLM answers with `content`, stopping for tools.
    fn answer(&mut self, owner: Token, content: Box<[Said]>) -> Box<[Request]> {
        let completion = Completion { reasoning_dropped: Box::default(), content, stop: Stop::ToolUse, usage: USAGE };
        self.step(Event::Completed { owner, completion })
    }

    /// The session `owner`'s LLM ends its turn saying `text`.
    fn says(&mut self, owner: Token, text: &[u8]) -> Box<[Request]> {
        let completion = Completion {
            reasoning_dropped: Box::default(),
            content: Box::new([Said::Text { text: bytes(text), replay: None }]),
            stop: Stop::EndTurn,
            usage: USAGE,
        };
        self.step(Event::Completed { owner, completion })
    }
}

fn bytes(text: &[u8]) -> Box<[u8]> {
    Box::from(text)
}

fn workspace() -> Workspace {
    Workspace {
        directories: Box::new([Directory {
            host: Box::from(b"/host".as_slice()),
            name: bytes(b"temper"),
            root: Token::new(900),
            writable: true,
            git: true,
            conflicts: Box::new([]),
        }]),
    }
}

fn account_grants() -> Box<[crate::Grant]> {
    Box::new([crate::Grant {
        renewable: true,
        name: crate::GrantName { account: 0, generation: 0 },
        valid: Duration::from_secs(100_000),
    }])
}

fn charter() -> Charter {
    Charter {
        mode: run::Mode::Finite,
        reserve: run::charter::Reserve { turns: 0, time: Duration::ZERO },
        instructions: Box::new([]),
        brief: smith_domain_run::Brief {
            sections: Box::new([smith_domain_run::Section {
                title: b"Task".as_slice().into(),
                text: bytes(b"Review the change."),
            }]),
        },

        grants: Grants { wait: true, deliver: None, tools: TOOLS, agents: true, host_tools: Box::new([]) },
        outcome: OutcomeSpec { change: None, verdicts: Box::new([rule(b"approve")]), report: None, failure: None },
        budget: BUDGET,
        llm: Llm {
            effort: None,
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            account: Some(0),
            endpoint: Endpoint(1),
            model: bytes(b"model-a"),
            window: 8192,
            output: 512,
            dialect: 1,
        },
        models: Box::new([]),
        conventions: Some(smith_domain_run::Conventions {
            guide: b"AGENTS.md".as_slice().into(),
            checks: b".temper/pre-pr".as_slice().into(),
        }),
        resume: false,
        waiting: Duration::from_secs(30),
    }
}

#[test]
fn root_refuses_invalid_grants_before_admission() {
    let mut h = Harness::new();
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: Box::new(
            [crate::Grant {
                renewable: true,
                name: crate::GrantName { account: 0, generation: 0 },
                valid: Duration::from_secs(60),
            }; 5],
        ),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: charter(),
        transcript: None,
    });
    let [Request::Answer { answer, .. }] = &*emitted else { panic!("expected admission refusal: {emitted:?}") };
    assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Grants)));
}

#[test]
fn root_refuses_unconfigured_main_and_sub_agent_endpoints_before_admission() {
    for (main, child) in [(Endpoint(3), Endpoint(2)), (Endpoint(1), Endpoint(3))] {
        let mut h = Harness::new();
        let mut requested = charter();
        requested.llm.endpoint = main;
        requested.models =
            Box::new([Llm { effort: None, endpoint: child, model: bytes(b"model-b"), ..requested.llm.clone() }]);
        let emitted = h.step(Event::Start {
            messages: Box::default(),
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
            answered: Box::default(),
            workspace: None,
            grants: account_grants(),
            reply_to: ReplyTo::new(Token::new(7)),
            host_run: Token::new(7),
            identity: None,
            activation: 1,
            charter: requested,
            transcript: None,
        });
        assert_eq!(
            &*emitted,
            &[Request::Answer {
                read: None,
                to: ReplyTo::new(Token::new(7)),
                answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Endpoint)),
                detail: Box::default(),
            }]
        );
        assert_eq!(h.domain.peers(), 0);
    }
    let mut h = Harness::new();
    let mut requested = charter();
    requested.models =
        Box::new([Llm { effort: None, endpoint: Endpoint(2), model: bytes(b"model-b"), ..requested.llm.clone() }]);
    let mut grants = account_grants();
    grants[0].valid = Duration::ZERO;
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: None,
        grants,
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: requested,
        transcript: None,
    });
    let [Request::Admitted { .. }] = &*emitted else {
        panic!("configured endpoints admit the start: {emitted:?}");
    };
}

#[test]
fn root_refuses_incompatible_conversation_limits_before_admission() {
    let limits = Limits { run: run::Limits { directories: 3, ..LIMITS.run }, ..LIMITS };
    let mut h = Harness::with(&limits);
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: charter(),
        transcript: None,
    });
    let [Request::Answer { answer, .. }] = &*emitted else { panic!("expected admission refusal: {emitted:?}") };
    assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Conversation)));
}

#[test]
fn oversized_resume_history_is_refused_when_the_conversation_opens() {
    let mut h = Harness::new();
    let mut turns = List::with_capacity(17);
    for sequence in 1..=17_u32 {
        turns
            .push(session::record::Turn {
                model: b"model-a".as_slice().into(),
                effort: None,
                kind: if sequence == 1 {
                    session::record::Kind::First(Box::new(session::record::Opener {
                        affinity: session::llm::Affinity { key: [0; 16], thread: 0 },
                        account: None,
                        backend: session::record::Backend::Direct,
                    }))
                } else {
                    session::record::Kind::Ordinary
                },
                version: session::record::VERSION,
                endpoint: Some(session::llm::Endpoint(1)),
                dialect: 1,
                sequence,
                usage: Usage::ZERO,
                spent: 0,
                jobs: 0,
                messages: Box::new([session::llm::Message { role: Role::User, content: Box::new([]) }]),
            })
            .expect("room for each oversized turn");
    }
    let transcript = session::record::Transcript {
        version: session::record::VERSION,
        endpoint: Some(session::llm::Endpoint(1)),
        dialect: 1,
        turns: turns.into_boxed(),
    };
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: Charter { resume: true, ..charter() },
        transcript: Some(transcript),
    });
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else {
        panic!("expected an admitted run: {emitted:?}")
    };
    let emitted = h.step(Event::Read { owner: *run, read: run::Read::Missing });
    let emitted = match emitted.as_ref() {
        [Request::Probe { owner, .. }] => h.step(Event::Probed { owner: *owner, executable: false }),
        _ => emitted,
    };
    let [Request::Answer { answer, .. }] = &*emitted else { panic!("expected history refusal: {emitted:?}") };
    match answer {
        run::Answer::Failed { failure, spent, turns } => {
            assert_eq!(
                (failure.clone(), *spent, *turns),
                (run::Failure::Transcript(run::TranscriptRefusal::TooLarge), run::Spend::ZERO, 0)
            );
        }
        run::Answer::Refused(_) | run::Answer::Parked { .. } | run::Answer::Accepted { .. } => {
            panic!("expected transcript refusal: {answer:?}")
        }
    }
}

const TOOLS: Tools = Tools { inspect: true, modify: true, shell: false };

fn rule(name: &[u8]) -> VerdictRule {
    VerdictRule {
        name: bytes(name),
        text_max: 1024,
        fields: Box::new([]),
        items: smith_domain_run::outcome::ItemSpec { min: 0, max: 0, kinds: Box::new([]) },
    }
}

fn verdict(name: &[u8]) -> Ask {
    Ask::Finish {
        outcome: Declared::Verdict(Verdict {
            name: bytes(name),
            text: bytes(b"ok"),
            items: Box::new([]),
            fields: Box::new([]),
        }),
    }
}

/// A sub-agent that may only read, and itself ask for sub-agents.
fn sub_agent(brief: &[u8]) -> Ask {
    let families = Families { tools: Tools { inspect: true, modify: false, shell: false }, agents: false };
    Ask::SubAgent { brief: bytes(brief), families, llm: None, share: run::Share::UNASKED }
}

fn served(id: &[u8], ask: Ask) -> Said {
    Said::ToolCall {
        id: bytes(id),
        name: bytes(b"served"),
        input: bytes(b"{}"),
        call: Decoded::Served { ask },
        replay: None,
    }
}

fn owned(id: &[u8], call: Call) -> Said {
    Said::ToolCall {
        id: bytes(id),
        name: bytes(b"read"),
        input: bytes(b"{}"),
        call: Decoded::Owned { call },
        replay: None,
    }
}

fn path(absolute: bool, names: &[&[u8]]) -> Path {
    let mut parts = List::with_capacity(4);
    for name in names {
        parts.push(Part::Name { name: Name::new(bytes(name)).expect("a name") }).expect("short paths");
    }
    Path { absolute, parts: parts.into_boxed() }
}

/// The one call to an LLM `emitted` holds: its owner and its prompt.
fn completing(emitted: Box<[Request]>) -> (Token, Prompt) {
    let Ok(one) = Box::<[Request; 1]>::try_from(emitted) else {
        panic!("expected one request");
    };
    let [Request::Complete { owner, prompt, deadlines, grant: _, .. }] = *one else {
        panic!("expected a call to an LLM");
    };
    assert_eq!(deadlines.head, Duration::from_secs(60));
    assert_eq!(deadlines.idle, Duration::from_secs(30));
    (owner, prompt)
}

/// The blocks of `prompt`'s last message.
fn last(prompt: &Prompt) -> &[Block] {
    &prompt.messages.last().expect("a prompt has messages").content
}

#[test]
fn opaque_reasoning_returns_in_its_position_without_becoming_text() {
    let mut h = Harness::new();
    let (_, owner, _) = h.admit(7, charter());
    let content = Box::new([
        Said::Opaque { bytes: bytes(b"provider reasoning") },
        owned(b"r", Call::Read { path: path(false, &[b"src"]), offset: 1, window: tools::Budget::Default }),
    ]);
    let emitted = h.answer(owner, content);
    let [Request::Io { owner: tool, .. }] = &*emitted else { panic!("expected read") };
    let done = tools::Done::Loaded { content: bytes(b"line"), version: tools::Version::new([1; 4]) };
    let (_, prompt) = completing(h.step(Event::Done { owner: *tool, done }));
    let assistant = prompt.messages.get(1).expect("assistant message kept");
    let Some(Block::Opaque { bytes: value }) = assistant.content.first() else { panic!("opaque block preserved") };
    assert_eq!(value.as_ref(), b"provider reasoning");
    let Some(crate::Content::Call { input, .. }) = h.domain.pop_content() else { panic!("call content is available") };
    assert_eq!(input.as_ref(), b"{}", "call arguments remain content");
    let Some(crate::Content::Usage { usage, .. }) = h.domain.pop_content() else { panic!("usage fact") };
    assert_eq!(usage, USAGE);
    let Some(crate::Content::Tool { done: tools::Done::Loaded { content, .. }, .. }) = h.domain.pop_content() else {
        panic!("tool output content")
    };
    assert_eq!(content.as_ref(), b"line");
}

#[test]
fn content_overflow_drops_and_counts_without_changing_decisions() {
    let mut keeping = Harness::new();
    let mut dropping = Harness::new();
    dropping.domain.content = Queue::with_capacity(0);
    let (_, kept, _) = keeping.admit(7, charter());
    let (_, dropped, _) = dropping.admit(7, charter());
    assert_eq!(keeping.says(kept, b"working"), dropping.says(dropped, b"working"));
    assert!(keeping.domain.pop_content().is_some());
    assert!(dropping.domain.pop_content().is_none());
    assert!(dropping.domain.content_lost() > keeping.domain.content_lost(), "content loss is counted");
}

#[test]
fn grant_updates_do_not_change_a_call_in_flight_and_rejections_are_once_per_generation() {
    let limits = Limits { session: session::Limits { retries: 3, ..LIMITS.session }, ..LIMITS };
    let mut h = Harness::with(&limits);
    let (_, owner, _) = h.admit(7, charter());
    let next = crate::Grant {
        renewable: true,
        name: crate::GrantName { account: 0, generation: 1 },
        valid: Duration::from_secs(60),
    };
    assert!(h.step(Event::Grant { grant: next }).is_empty());
    assert_eq!(
        &*h.step(Event::Failed {
            owner,
            failure: crate::llm::Failure::Unauthorized,
            evidence: crate::llm::Evidence::Unknown,
            detail: Box::default(),
            report: crate::llm::Report::none(),
        }),
        &[Request::Unauthorized { grant: crate::GrantName { account: 0, generation: 0 } }]
    );
    h.env.now = h.domain.next_deadline().expect("unauthorized retry backoff");
    let emitted = h.fire();
    let [Request::Complete { grant, .. }] = &*emitted else { panic!("expected retry") };
    assert_eq!(*grant, Some(next.name));
    assert_eq!(
        &*h.step(Event::Failed {
            owner,
            failure: crate::llm::Failure::Unauthorized,
            evidence: crate::llm::Evidence::Unknown,
            detail: Box::default(),
            report: crate::llm::Report::none(),
        }),
        &[Request::Unauthorized { grant: next.name }]
    );
    h.env.now = h.domain.next_deadline().expect("second retry");
    drop(h.fire());
    assert!(
        h.step(Event::Failed {
            owner,
            failure: crate::llm::Failure::Unauthorized,
            evidence: crate::llm::Evidence::Unknown,
            detail: Box::default(),
            report: crate::llm::Report::none(),
        })
        .is_empty()
    );
}

#[test]
fn no_grant_fails_locally_and_exhaustion_reports_the_account() {
    let mut h = Harness::new();
    let mut ungranted = charter();
    ungranted.llm.account = Some(9);
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: ungranted,
        grants: Box::new([]),
        transcript: None,
    });
    let [Request::Answer { answer, .. }] = emitted.as_ref() else { panic!("missing grant is refused") };
    assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Grants)));
    let mut h = Harness::new();
    let (_, owner, _) = h.admit(7, charter());
    let retry_after = Duration::from_secs(30);
    let emitted = h.step(Event::Failed {
        owner,
        failure: crate::llm::Failure::Exhausted { retry_after: Some(retry_after) },
        evidence: crate::llm::Evidence::Unknown,
        detail: Box::default(),
        report: crate::llm::Report::none(),
    });
    let Some(Request::Limited { account: 0, reset: span }) = emitted.first() else { panic!("exhaustion notice") };
    assert_eq!(*span, Some(retry_after));
    let mut failed = false;
    for request in &emitted {
        if let Request::Answer {
            answer:
                run::Answer::Failed {
                    failure: run::Failure::Model(run::Fault::Limited { account: 0, reset_at: Some(_) }),
                    ..
                },
            ..
        } = request
        {
            failed = true;
        }
    }
    assert!(failed, "the exhausted account ends the run");
}

#[test]
fn the_limits_fit_and_a_session_must_take_what_the_run_asks() {
    assert!(valid(&LIMITS));
    let fewer = Limits { session: session::Limits { sessions: 3, spend: 1, ..LIMITS.session }, ..LIMITS };
    assert!(!valid(&fewer), "a session for every conversation the run may have");
    let budget = session::Budget { turns: 50, ..CEILING };
    let smaller = Limits { session: session::Limits { budget, ..LIMITS.session }, ..LIMITS };
    assert!(!valid(&smaller), "a session's budget as large as the run's");
}

#[test]
fn what_an_entry_point_may_emit_grows_with_the_tools_cancels_only_once() {
    let base = Limits {
        session: session::Limits { tools: tools::Limits { jobs: 0, ..LIMITS.session.tools }, ..LIMITS.session },
        ..LIMITS
    };
    assert_eq!(max_out(&base), 1142);
    // A kit's close cancels as many operations as the tools run, which go out
    // to io and lead nowhere else: what the run is sent does not grow with
    // them.
    let tools = tools::Limits { calls: 256, ..base.session.tools };
    let wide = Limits { session: session::Limits { parallel_tools: 8, tools, ..base.session }, ..base };
    // Four session records plus eight delegated calls may reach the run;
    // each can open/say at most MAX_OUT sessions. The kit's io cancels do not.
    assert_eq!(limits::session_steps(&wide), 74);
    assert_eq!(max_out(&wide), 22646);
    let wider =
        Limits { session: session::Limits { tools: tools::Limits { calls: 512, ..tools }, ..wide.session }, ..wide };
    assert_eq!(limits::run_out(&wider), limits::run_out(&wide), "io cancels never multiply run hand-offs");
    assert_eq!(max_out(&wider) - max_out(&wide), 74 * 256, "one additional kit cancel per session step");
}

#[test]
fn a_run_opens_main_as_a_session_through_to_its_first_call_to_the_llm() {
    let mut h = Harness::new();
    let (_, main, prompt) = h.admit(7, charter());
    assert_eq!((prompt.endpoint, &*prompt.model, prompt.max_tokens), (session::llm::Endpoint(1), &b"model-a"[..], 512));
    assert_eq!(prompt.tools, tools::Grants { inspect: true, modify: true, shell: false });
    assert_eq!(
        &*prompt.served,
        &[Served::Wait, Served::Finish, Served::SubAgent],
        "main may finish, and ask for sub-agents"
    );
    let [Message { role: Role::User, content }] = &*prompt.messages else {
        panic!("expected the first message only");
    };
    assert!(matches_text(content), "the run's first message");
    assert_eq!((h.domain.peers(), h.domain.tickets(), h.domain.flights()), (1, 0, 0));

    // Relative paths start in the first repository, mounted under its name.
    let relative = Call::Read { path: path(false, &[b"src", b"lib.rs"]), offset: 1, window: tools::Budget::Default };
    let absolute = Call::Read { path: path(true, &[b"temper", b"README"]), offset: 1, window: tools::Budget::Default };
    let emitted = h.answer(main, Box::new([owned(b"c1", relative), owned(b"c2", absolute)]));
    let [Request::Io { op: Op::Load { at: first, .. }, .. }, Request::Io { op: Op::Load { at: second, .. }, .. }] =
        &*emitted
    else {
        panic!("expected two loads, got {emitted:?}");
    };
    assert_eq!(first, &Place { root: Token::new(900), path: bytes(b"src/lib.rs") });
    assert_eq!(second, &Place { root: Token::new(900), path: bytes(b"README") });
}

fn matches_text(content: &[Block]) -> bool {
    match content {
        [Block::Text { text, replay: None }] => !text.is_empty(),
        _ => false,
    }
}

#[test]
fn an_unsafe_workspace_mount_refuses_start_before_discovery() {
    let mut h = Harness::new();
    let checkout = Workspace {
        directories: Box::new([Directory {
            host: Box::from(b"/host".as_slice()),
            name: bytes(b"ai/temper"),
            root: Token::new(900),
            writable: true,
            git: true,
            conflicts: Box::new([]),
        }]),
    };
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: charter(),
        workspace: Some(checkout),
        transcript: None,
    });
    let requests = Box::<[Request; 1]>::try_from(emitted).expect("one refusal terminal, no admission or discovery");
    let [Request::Answer { to, answer, read: _, detail: _ }] = *requests else {
        panic!("unsafe mount refuses before admission or discovery");
    };
    assert_eq!(to.into_token(), Token::new(7));
    assert_eq!(answer, run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Workspace)));
    assert_eq!(h.domain.peers(), 0);
}

#[test]
fn an_opening_larger_than_a_session_holds_refuses_main_as_invalid() {
    // Limits a session honours whatever the charter, but not every opening:
    // that is the charter's, and is refused at the sessions' entrance.
    let run = run::Limits { run_bytes: 16_384, ..LIMITS.run };
    let limits = Limits {
        run,
        session: session::Limits {
            history_bytes: 8192,
            completion_bytes: 128,
            answer_bytes: 128,
            completion_blocks: 1,
            delegated_result_bytes: crate::feedback_worst_case(&run).expect("complete compatible feedback"),
            ..LIMITS.session
        },
        ..LIMITS
    };
    assert!(valid(&limits));
    let mut h = Harness::with(&limits);
    let brief = filler(10_240);
    let emitted = h.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        charter: Charter { instructions: brief, ..charter() },
        transcript: None,
    });
    let [Request::Admitted { run, .. }, Request::Read { .. }] = &*emitted else {
        panic!("expected an admitted run, got {emitted:?}");
    };
    let emitted = h.step(Event::Read { owner: *run, read: run::Read::Missing });
    let [Request::Probe { .. }] = &*emitted else {
        panic!("writable directory probes checks before opening: {emitted:?}");
    };
    let emitted = h.step(Event::Probed { owner: *run, executable: false });
    let [Request::Answer { to: _, answer, read: _, detail: _ }] = &*emitted else {
        panic!("expected the run's answer, got {emitted:?}");
    };
    assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Conversation)));
    h.domain.reclaim();
    assert_eq!((h.domain.peers(), h.domain.session().sessions()), (0, 0));
}

#[test]
fn a_finish_the_run_rejects_at_once_comes_back_from_the_ready_list_as_the_sessions_answer() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    // The run judges the finish within the step, and rejects it; the answer
    // waits for the next iteration.
    assert!(h.answer(main, Box::new([served(b"f1", verdict(b"merge"))])).is_empty());
    assert_eq!((h.domain.tickets(), h.domain.flights()), (0, 1), "concrete rejection waits on its actual call right");
    assert!(!h.domain.is_ready(), "not before the reclaim point");

    let (owner, prompt) = completing(h.next());
    assert_eq!(owner, main);
    assert_eq!(prompt.asker, crate::llm::Asker::Main);
    let [Block::ToolResult { id, result: Returned::Text { text, error: true, replay: None } }] = last(&prompt) else {
        panic!("expected the rejection, got {:?}", last(&prompt));
    };
    assert_eq!(&**id, b"f1");
    assert!(text.starts_with(b"rejected"), "canonical rejection retains its problem details: {text:?}");
    let [_, Message { role: Role::Assistant, content }, _] = &*prompt.messages else {
        panic!("expected the call and its result");
    };
    assert_eq!(
        &**content,
        &[Block::ToolCall { id: bytes(b"f1"), name: bytes(b"served"), input: bytes(b"{}"), replay: None }]
    );
    assert_eq!(
        (h.domain.tickets(), h.domain.flights()),
        (0, 0),
        "concrete answer stays in the session transcript without a local ticket"
    );
}

#[test]
fn an_accepted_finish_closes_main_and_the_run_answers_the_worker() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    assert!(h.answer(main, Box::new([served(b"f1", verdict(b"approve"))])).is_empty());
    // The answer and the close come in the next iteration: the close first,
    // which withdraws the call, whose answer has won.
    let emitted = h.next();
    let [Request::Answer { to, answer: run::Answer::Accepted { outcome: _, spent, .. }, read: _, detail: _ }] =
        &*emitted
    else {
        panic!("expected the run accepted, got {emitted:?}");
    };
    assert_eq!(to, &ReplyTo::new(Token::new(7)));
    assert_eq!((spent.turns, spent.input), (1, USAGE.input_tokens));
    h.domain.reclaim();
    assert_eq!((h.domain.peers(), h.domain.tickets(), h.domain.flights()), (0, 0, 0), "main's tickets went with it");
}

#[test]
fn a_change_lands_through_the_worker_and_the_run_answers_with_it() {
    let mut h = Harness::new();
    let outcome = OutcomeSpec {
        change: Some(ChangeSpec {
            checks_must_pass: true,
            fields: Box::new([
                smith_domain_run::outcome::FieldRule { name: b"title".as_slice().into(), max: 1024 },
                smith_domain_run::outcome::FieldRule { name: b"body".as_slice().into(), max: 1024 },
            ]),
        }),
        verdicts: Box::new([]),
        report: None,
        failure: None,
    };
    let (_, main, _) = h.admit(
        7,
        Charter {
            outcome,
            reserve: run::charter::Reserve { turns: 0, time: h.env.limits.run.check_timeout },
            ..charter()
        },
    );
    let change = Change {
        fields: Box::new([
            smith_domain_run::outcome::Field { name: b"title".as_slice().into(), value: bytes(b"Fix the bug") },
            smith_domain_run::outcome::Field { name: b"body".as_slice().into(), value: bytes(b"It was in main.rs.") },
        ]),
    };
    let finish = Ask::Finish { outcome: Declared::Change(change.clone()) };
    let emitted = h.answer(main, Box::new([served(b"f1", finish)]));
    let [Request::Deliver { host_run, owner, change: pushed, name: _, deadline: _ }] = &*emitted else {
        panic!("expected the change pushed, got {emitted:?}");
    };
    assert_eq!((host_run, pushed), (&Token::new(7), &change));
    assert!(
        h.step(Event::Delivered { owner: *owner, delivery: delivered() }).is_empty(),
        "the answer and the close wait"
    );
    let emitted = h.next();
    let [Request::Answer { to: _, answer: run::Answer::Accepted { outcome, spent: _, .. }, read: _, detail: _ }] =
        &*emitted
    else {
        panic!("expected the run accepted, got {emitted:?}");
    };
    assert_eq!(outcome, &Declared::Change(change));
    h.domain.reclaim();
    assert_eq!((h.domain.peers(), h.domain.flights(), h.domain.tickets()), (0, 0, 0));
}

#[test]
fn a_sub_agent_opens_a_child_session_whose_last_message_answers_the_call() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    // The run opens the child within the step: its session calls its LLM.
    let (child, prompt) = completing(h.answer(main, Box::new([served(b"a1", sub_agent(b"Find the bug."))])));
    assert_ne!(child, main);
    assert_eq!(prompt.asker, crate::llm::Asker::Child);
    assert_eq!(prompt.tools, tools::Grants { inspect: true, modify: false, shell: false }, "the families asked for");
    assert!(prompt.served.is_empty(), "a child receives workspace tools only");
    assert_eq!((h.domain.peers(), h.domain.flights()), (2, 1));

    // The child calls finish, which it was not offered: no call. Its next
    // provider request waits until the next iteration's reclaim point.
    assert!(h.answer(child, Box::new([served(b"f1", verdict(b"approve"))])).is_empty());
    let (again, prompt) = completing(h.next());
    assert_eq!(again, child);
    let [Block::ToolResult { id: _, result: Returned::Invalid { problem: Problem::UnknownTool } }] = last(&prompt)
    else {
        panic!("expected the call refused, got {:?}", last(&prompt));
    };

    // The child yields: the run closes it (from the ready list), and its end
    // answers main's call (from the ready list again).
    assert!(h.says(child, b"It is in main.rs.").is_empty());
    assert!(h.next().is_empty(), "the child closes, and ends");
    let (owner, prompt) = completing(h.next());
    assert_eq!(owner, main);
    let [Block::ToolResult { id, result: Returned::Text { text, error: false, replay: None } }] = last(&prompt) else {
        panic!("expected the child's answer, got {:?}", last(&prompt));
    };
    assert_eq!(&**id, b"a1");
    assert_eq!(&**text, b"It is in main.rs.\n\nChild usage: used 2 turns and 0 seconds; stopped for budget: no; ordinary left 7 turns and 600 seconds.");
    h.domain.reclaim();
    assert_eq!((h.domain.peers(), h.domain.flights(), h.domain.tickets()), (1, 0, 0));
}

/// Main, admitted, whose LLM says `text` bytes and asks for two read-only
/// sub-agents side by side: what that emitted.
fn side_by_side_with(limits: &Limits, text: u32) -> (Harness, Token, Box<[Request]>) {
    let mut h = Harness::with(limits);
    let (run, main, _) = h.admit(7, charter());
    let content = Box::new([
        Said::Text { text: filler(text), replay: None },
        served(b"a1", sub_agent(b"Look here.")),
        served(b"a2", sub_agent(b"Look there.")),
    ]);
    let emitted = h.answer(main, content);
    (h, run, emitted)
}

/// The sub-agents `emitted` opened, whose sessions call their LLMs.
fn children(emitted: &[Request]) -> Option<[Token; 2]> {
    let [Request::Complete { owner: first, .. }, Request::Complete { owner: second, .. }] = emitted else {
        return None;
    };
    Some([*first, *second])
}

fn filler(len: u32) -> Box<[u8]> {
    let mut text = List::with_capacity(len);
    for _ in 0..len {
        text.push(b'x').expect("room for the text");
    }
    text.into_boxed()
}

fn assert_no_provider_completion(emitted: &[Request]) {
    for request in emitted {
        match request {
            Request::Complete { .. } => {
                panic!("no room for another provider receiving reserve after the exact result edge")
            }
            Request::MessageRefused { .. }
            | Request::Waiting { .. }
            | Request::Turn { .. }
            | Request::HostCall { .. }
            | Request::WithdrawHost { .. }
            | Request::Admitted { .. }
            | Request::Answer { .. }
            | Request::Long { .. }
            | Request::LongDone { .. }
            | Request::Deliver { .. }
            | Request::Unauthorized { .. }
            | Request::Refused { .. }
            | Request::Limited { .. }
            | Request::Cancel { .. }
            | Request::Yield { .. }
            | Request::Io { .. }
            | Request::Job { .. }
            | Request::CancelIo { .. }
            | Request::Read { .. }
            | Request::Probe { .. }
            | Request::Check { .. }
            | Request::Abort { .. } => {}
        }
    }
}

#[test]
#[expect(clippy::disallowed_methods, reason = "test assembles an independent literal result golden")]
fn full_history_reserves_every_child_answer_before_effect_and_keeps_late_bytes() {
    // At the old two-result-cap boundary every call now runs with its share.
    // Actual child replies still survive cancellation before their deferred delivery.
    let bound = LIMITS.session.delegated_result_bytes.checked_mul(2).expect("two result caps");
    let limits = Limits { session: session::Limits { history_bytes: bound, ..LIMITS.session }, ..LIMITS };
    let (mut world, run, emitted) = side_by_side_with(&limits, 256);
    let [first, second] = children(&emitted).expect("the complete batch was reserved");
    let answer = filler(limits.run.answer_bytes);
    let expected = [
        answer.as_ref(),
        b"\n\nChild usage: used 1 turns and 0 seconds; stopped for budget: no; ordinary left 7 turns and 600 seconds.",
    ]
    .concat()
    .into_boxed_slice();
    assert!(world.says(first, &answer).is_empty());
    assert!(world.says(second, &answer).is_empty());
    assert!(world.next().is_empty(), "actual child endings queue both owning results");
    assert_eq!(world.domain.flights(), 2);
    assert_eq!(world.domain.run().runs(), 1, "only main remains after both child terminals");
    assert!(world.step(Event::Cancel { run }).is_empty());
    // Cancel emission cannot replace either result which already won its terminal.
    let emitted = world.next();
    assert_no_provider_completion(&emitted);
    let turn = world.turns.last().expect("actual main Turn precedes its failed final answer");
    for (index, id) in [b"a1".as_slice(), b"a2".as_slice()].into_iter().enumerate() {
        assert_eq!(
            *turn
                .messages
                .last()
                .expect("actual result message")
                .content
                .get(index)
                .expect("one result per admitted child"),
            session::llm::Block::ToolResult {
                id: bytes(id),
                result: session::llm::Returned::Text { text: expected.clone(), error: false, replay: None }
            }
        );
    }
    assert_eq!(world.domain.tickets(), 0, "concrete records carry no local replay tickets");
    assert!(
        u64::from(limits.run.answer_bytes).checked_mul(2).expect("two exact child answers")
            <= limits::uncharged(&limits).expect("priced queued payload")
    );
}

#[test]
fn the_asks_of_an_answer_that_yields_are_forgotten() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    let completion = Completion {
        reasoning_dropped: Box::default(),
        content: Box::new([served(b"f1", verdict(b"approve"))]),
        stop: Stop::MaxTokens,
        usage: USAGE,
    };
    // The run nudges at once: the session is continued within the step.
    let (owner, prompt) = completing(h.step(Event::Completed { owner: main, completion }));
    assert_eq!(owner, main);
    assert_eq!(h.domain.tickets(), 0, "the call that did not run left no ticket");
    let [_, _, Message { role: Role::User, content }] = &*prompt.messages else {
        panic!("expected the nudge after the call that did not run");
    };
    let [
        Block::ToolResult { id: _, result: Returned::NotRun { why: crate::llm::NotRun::Stopped } },
        Block::Text { .. },
    ] = &**content
    else {
        panic!("expected the call not run, then the nudge, got {content:?}");
    };
}

#[test]
fn an_ask_larger_than_the_session_may_hold_is_too_large() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    let mut brief = List::with_capacity(70_000);
    for _ in 0..70_000_u32 {
        brief.push(b'x').expect("room for the brief");
    }
    let brief = brief.into_boxed();
    let ask = Ask::SubAgent {
        brief,
        families: Families { tools: TOOLS, agents: false },
        llm: None,
        share: run::Share::UNASKED,
    };
    assert!(h.answer(main, Box::new([served(b"a1", ask)])).is_empty());
    let (_, prompt) = completing(h.next());
    let [Block::ToolResult { id: _, result: Returned::Invalid { problem: Problem::TooLarge } }] = last(&prompt) else {
        panic!("expected the call too large, got {:?}", last(&prompt));
    };
    assert_eq!((h.domain.tickets(), h.domain.peers()), (0, 1));
}

#[test]
fn the_runs_deadline_fires_before_the_sessions_expiry_at_the_same_instant() {
    let mut h = Harness::new();
    let (_, main, _) = h.admit(7, charter());
    h.env.now = Time::ZERO.saturating_add(BUDGET.time);
    // The run winds down: its close of main waits for the next iteration, and
    // main's own expiry fires meanwhile.
    assert!(h.fire().is_empty());
    assert_eq!(&*h.fire(), &[Request::Cancel { owner: main }]);
    assert!(!h.domain.is_due(h.env.now));
    assert!(h.next().is_empty(), "the close finds main closing already");
    let emitted = h.step(Event::Cancelled { owner: main });
    let [Request::Answer { to: _, answer: run::Answer::Failed { failure, spent: _, .. }, read: _, detail: _ }] =
        &*emitted
    else {
        panic!("expected the run failed, got {emitted:?}");
    };
    assert_eq!(
        failure,
        &run::Failure::Budget { exhausted: run::Exhausted::Time, used: Some(600_000), bound: Some(600_000) }
    );
}

#[test]
fn an_owner_that_drains_late_loses_no_fact() {
    // Ten thousand single-fact entrances exceed the former composed reserve
    // for LIMITS; the edge inbox still retains at most LIMITS.run.messages.
    const ENTRANCES: u32 = 10_000;
    let mut harness = Harness::new();
    let (run, _, _) = harness.admit(7, charter());
    while harness.pop_fact().is_some() {}
    for number in 1..=ENTRANCES {
        step(
            &mut harness.domain,
            &harness.env,
            Event::Message {
                finish_requested: false,
                run,
                name: Token::new(u64::from(number)),
                label: bytes(b"host"),
                text: bytes(b"hello"),
            },
            &mut harness.out,
        );
        drop(harness.drain());
    }
    // Even a no-op fire or resume needs no observation reserve.
    fire(&mut harness.domain, &harness.env, &mut harness.out);
    resume(&mut harness.domain, &harness.env, &mut harness.out);
    assert_eq!(harness.domain.backlog(), ENTRANCES, "every entrance's fact awaits the owner");
    for number in 1..=ENTRANCES {
        let Some(Fact::Run {
            fact:
                run::facts::Fact {
                    kind:
                        run::facts::FactKind::MessageReceived { name, .. }
                        | run::facts::FactKind::MessageRefused { name, .. },
                    ..
                },
            ..
        }) = harness.domain.pop_fact()
        else {
            panic!("one message observation per entrance: {number}")
        };
        assert_eq!(name.raw(), u64::from(number), "accepted and refused messages retain their entrance order");
    }
    assert_eq!(harness.domain.backlog(), 0);
    assert!(!harness.domain.has_facts());
}

#[test]
fn the_facts_of_both_child_domains_are_gathered() {
    let mut h = Harness::new();
    let (_run, _main, prompt) = h.admit(7, charter());
    let mut run = 0_u32;
    let mut sessions = 0_u32;
    let mut opened = 0_u32;
    for _ in 0..64_u32 {
        match h.pop_fact() {
            Some(Fact::Run { opening, .. }) => {
                run += 1;
                if let Some(opening) = opening {
                    opened += 1;
                    assert_eq!(opening.activation, 1);
                    assert_eq!(opening.ordinal, 0);
                    assert_eq!(opening.llm.model, prompt.model);
                    assert_eq!(opening.llm.endpoint.0, prompt.endpoint.0);
                    assert_eq!(opening.ordinal, prompt.affinity.thread);
                }
            }
            Some(Fact::Session { .. }) => sessions += 1,
            None => break,
        }
    }
    assert!(run > 0 && sessions > 0, "{run} of the run's, {sessions} of the sessions'");
    assert_eq!(opened, 1, "one actual main opening precedes the provider request");
    assert_eq!(h.domain.content_lost(), 0);
}

fn delivered() -> run::Delivery {
    run::Delivery::Delivered(
        run::Delivered::new(Box::new([
            run::Receipt::new(0, b"host receipt".as_slice().into()).expect("bounded receipt")
        ]))
        .expect("one writable mount"),
    )
}

fn opaque_tool(name: &[u8], effect: run::HostEffect) -> run::HostTool {
    run::HostTool {
        name: bytes(name),
        description: bytes(b"Uninterpreted host operation"),
        schema: bytes(br#"{"type":"object"}"#),
        effect,
        timeout: Duration::from_secs(2),
    }
}

fn opaque_call(id: &[u8], tool: &[u8], effect: run::HostEffect) -> Said {
    let input = bytes(br#" {"whole":[true,{"external":"policy"}]} "#);
    Said::ToolCall {
        id: bytes(id),
        name: bytes(tool),
        input: input.clone(),
        call: Decoded::Served {
            ask: Ask::Host {
                tool: bytes(tool),
                effect,
                input: run::HostInput::attested(input).expect("typed protocol attestation"),
            },
        },
        replay: None,
    }
}

#[test]
fn declared_host_reads_run_together_write_waits_and_mismatched_effect_never_relays() {
    let mut h = Harness::new();
    let mut charter = charter();
    let read = opaque_tool(b"outside_read", run::HostEffect::Read);
    let write = opaque_tool(b"outside_write", run::HostEffect::Write);
    charter.grants.host_tools = Box::new([read.clone(), write.clone()]);
    let (_, owner, prompt) = h.admit(71, charter);
    assert!(prompt.served.contains(&Served::Host(read)) && prompt.served.contains(&Served::Host(write)));
    let emitted = h.answer(
        owner,
        Box::new([
            opaque_call(b"one", b"outside_read", run::HostEffect::Read),
            opaque_call(b"two", b"outside_read", run::HostEffect::Read),
            opaque_call(b"three", b"outside_write", run::HostEffect::Write),
        ]),
    );
    let [
        Request::HostCall { relay: first, effect: run::HostEffect::Read, name: first_name, .. },
        Request::HostCall { relay: second, effect: run::HostEffect::Read, name: second_name, .. },
    ] = &*emitted
    else {
        panic!("adjacent host reads are concurrent, got {emitted:?}");
    };
    assert_ne!(first_name, second_name);
    let first = *first;
    let second = *second;
    let answer = run::HostAnswer::new(bytes(b"exact external error"), true).expect("bounded text");
    assert!(h.step(Event::HostReturned { relay: first, reply: run::HostReply::Answered(answer.clone()) }).is_empty());
    assert!(h.next().is_empty(), "one pending read prevents exclusive write");
    assert!(h.step(Event::HostReturned { relay: second, reply: run::HostReply::Answered(answer.clone()) }).is_empty());
    let emitted = h.next();
    let [Request::HostCall { relay, effect: run::HostEffect::Write, .. }] = &*emitted else {
        panic!("write follows settled reads, got {emitted:?}");
    };
    assert!(h.step(Event::HostReturned { relay: *relay, reply: run::HostReply::Answered(answer.clone()) }).is_empty());
    let (owner, prompt) = completing(h.next());
    let [Block::ToolResult { result: Returned::Text { text: actual, error: true, replay: None }, .. }, ..] =
        last(&prompt)
    else {
        panic!("exact error reaches next completion");
    };
    assert_eq!(actual.as_ref(), answer.text());
    assert!(h.answer(owner, Box::new([opaque_call(b"bad", b"outside_write", run::HostEffect::Read)])).is_empty());
    let (_, prompt) = completing(h.next());
    let [Block::ToolResult { result: Returned::Invalid { problem: Problem::UnknownTool }, .. }] = last(&prompt) else {
        panic!("effect mismatch is rejected before session scheduling");
    };
}

fn convention_workspace() -> Workspace {
    Workspace {
        directories: Box::new([
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"work"),
                root: Token::new(900),
                writable: true,
                git: true,
                conflicts: Box::new([]),
            },
            Directory {
                host: Box::from(b"/host".as_slice()),
                name: bytes(b"reference"),
                root: Token::new(901),
                writable: false,
                git: true,
                conflicts: Box::new([]),
            },
        ]),
    }
}

/// Actual composed root entrance; decoy paths belong to the fake filesystem,
/// not to Smith's selection. Read-only mounts supply guides but no check probe.
fn convention_main(harness: &mut Harness, selected: Option<run::Conventions>) -> (Token, Token, Prompt) {
    let (guide_path, check_path) = match &selected {
        Some(conventions) => (conventions.guide.as_ref(), conventions.checks.as_ref()),
        None => (b"AGENTS.md".as_slice(), b".smith/check".as_slice()),
    };
    let guide_path = bytes(guide_path);
    let check_path = bytes(check_path);
    let mounted = Some(convention_workspace());
    let charter = Charter {
        conventions: selected,
        reserve: run::charter::Reserve { turns: 0, time: harness.env.limits.run.check_timeout },

        outcome: OutcomeSpec {
            change: Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) }),
            verdicts: Box::new([]),
            report: None,
            failure: None,
        },
        ..charter()
    };
    let emitted = harness.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        reply_to: ReplyTo::new(Token::new(77)),
        host_run: Token::new(77),
        identity: None,
        activation: 1,
        charter,
        workspace: mounted,
        transcript: None,
        grants: Box::new([crate::Grant {
            renewable: true,
            name: crate::GrantName { account: 0, generation: 1 },
            valid: Duration::from_secs(3600),
        }]),
    });
    let [Request::Admitted { run, .. }, Request::Read { owner, at, max, deadline }] = emitted.as_ref() else {
        panic!("actual admitted discovery: {emitted:?}");
    };
    let run = *run;
    assert_eq!(*owner, run);
    assert_eq!(at.root, Token::new(900));
    assert_eq!((max, deadline), (&LIMITS.run.guide_bytes, &Time::ZERO.saturating_add(LIMITS.run.io_timeout)));
    // All three fake paths exist. Only the caller-selected guide is meaningful;
    // the others return decoy text and would fail the public prompt assertion.
    let files = [
        (guide_path.as_ref(), b"Caller-selected writable guide".as_slice()),
        (b"AGENTS.md".as_slice(), b"DECOY legacy guide".as_slice()),
        (b"legacy/GUIDE".as_slice(), b"DECOY other guide".as_slice()),
    ];
    let mut found = None;
    for (path, content) in files {
        if path == at.path.as_ref() {
            found = Some(content);
            break;
        }
    }
    let text = found.expect("fake guide path exists");
    assert_eq!(at.path, guide_path);
    let emitted = harness.step(Event::Read { owner: run, read: run::Read::Text { text: bytes(text), whole: false } });
    let [Request::Probe { owner, at, deadline }] = emitted.as_ref() else {
        panic!("only writable directory has a check probe: {emitted:?}");
    };
    assert_eq!(*owner, run);
    assert_eq!(at.root, Token::new(900));
    assert_eq!(at.path, check_path);
    assert_eq!(*deadline, Time::ZERO.saturating_add(LIMITS.run.io_timeout));
    let emitted = harness.step(Event::Probed { owner: run, executable: true });
    let [Request::Read { owner, at, .. }] = emitted.as_ref() else {
        panic!("readonly mount still supplies a guide: {emitted:?}");
    };
    assert_eq!(*owner, run);
    assert_eq!(at.root, Token::new(901));
    assert_eq!(at.path, guide_path);
    let (main, prompt) = completing(harness.step(Event::Read {
        owner: run,
        read: run::Read::Text { text: bytes(b"Caller-selected readonly guide"), whole: true },
    }));
    let system = prompt.system.as_ref();
    for fragment in [
        b"Caller-selected writable guide".as_slice(),
        b"Caller-selected readonly guide",
        b"The file goes on: read the rest with your tools.",
        guide_path.as_ref(),
        check_path.as_ref(),
    ] {
        assert!(skein_lib::bytes::find(system, fragment).is_some(), "selected policy is rendered verbatim");
    }
    assert!(skein_lib::bytes::find(system, b"DECOY").is_none());
    if guide_path.as_ref() != b"AGENTS.md" {
        assert!(skein_lib::bytes::find(system, b"AGENTS.md").is_none());
    }
    if check_path.as_ref() != b".temper/pre-pr" {
        assert!(skein_lib::bytes::find(system, b".temper/pre-pr").is_none());
    }
    (run, main, prompt)
}

#[test]
fn caller_conventions_select_default_custom_and_explicit_legacy_check_delivery_paths() {
    for (selected, expected_path) in [
        (None, b".smith/check".as_slice()),
        (
            Some(run::Conventions { guide: bytes(b"docs/WORKFLOW"), checks: bytes(b".ci/check-suite") }),
            b".ci/check-suite".as_slice(),
        ),
        (
            Some(run::Conventions { guide: bytes(b"AGENTS.md"), checks: bytes(b".temper/pre-pr") }),
            b".temper/pre-pr".as_slice(),
        ),
    ] {
        let mut harness = Harness::new();
        let (_, main, _) = convention_main(&mut harness, selected);
        let change = Change { fields: Box::new([]) };
        let emitted = harness.answer(
            main,
            Box::new([served(b"finish-convention", Ask::Finish { outcome: Declared::Change(change.clone()) })]),
        );
        let [
            Request::Check { owner, program, deadline, tail },
            Request::Long { host_run, operation: crate::Operation::Checks, until: said },
        ] = emitted.as_ref()
        else {
            panic!("actual exclusive Check precedes host delivery: {emitted:?}");
        };
        assert_eq!(program.root, Token::new(900), "readonly mount is never checked");
        assert_eq!(program.path.as_ref(), expected_path);
        assert_eq!(*host_run, Token::new(77));
        assert_eq!(deadline, said);
        assert_eq!(*tail, LIMITS.run.check_tail);
        assert_eq!(*deadline, Time::ZERO.saturating_add(LIMITS.run.check_timeout));
        let owner = *owner;
        let emitted = harness.step(Event::Checked {
            owner,
            ran: run::Ran { exit: run::Exit::Code { code: 0 }, output: bytes(b"check passed"), cut: 0 },
        });
        let [Request::LongDone { host_run }, Request::Deliver { owner: delivered_owner, change: delivered_change, .. }] =
            emitted.as_ref()
        else {
            panic!("only actual successful writable Check admits host delivery: {emitted:?}");
        };
        assert_eq!(*host_run, Token::new(77));
        assert_eq!(*delivered_owner, owner);
        assert_eq!(delivered_change, &change);
        assert!(harness.step(Event::Delivered { owner, delivery: delivered() }).is_empty());
        let emitted = harness.next();
        let [Request::Answer { answer: run::Answer::Accepted { outcome, .. }, .. }] = emitted.as_ref() else {
            panic!("one actual accepted terminal after settled Check and delivery: {emitted:?}");
        };
        assert_eq!(outcome, &Declared::Change(change));
        harness.domain.reclaim();
        assert_eq!((harness.domain.peers(), harness.domain.flights(), harness.domain.tickets()), (0, 0, 0));
    }
}

#[test]
fn custom_check_cancellation_waits_for_the_abort_terminal() {
    let mut harness = Harness::new();
    let (run, main, _) = convention_main(
        &mut harness,
        Some(run::Conventions { guide: bytes(b"docs/WORKFLOW"), checks: bytes(b".ci/check-suite") }),
    );
    let emitted = harness.answer(
        main,
        Box::new([served(
            b"finish-convention",
            Ask::Finish { outcome: Declared::Change(Change { fields: Box::new([]) }) },
        )]),
    );
    let [Request::Check { owner, .. }, Request::Long { .. }] = emitted.as_ref() else {
        panic!("actual custom Check: {emitted:?}");
    };
    let owner = *owner;
    assert!(harness.step(Event::Cancel { run }).is_empty());
    let emitted = harness.next();
    let [Request::Abort { owner: aborted }] = emitted.as_ref() else {
        panic!("one actual check abort request: {emitted:?}");
    };
    assert_eq!(*aborted, owner);
    assert!(harness.next().is_empty(), "cancel requests no settlement or delivery");
    assert_eq!(harness.step(Event::Aborted { owner }).as_ref(), &[Request::LongDone { host_run: Token::new(77) }]);
    let emitted = harness.next();
    let [Request::Answer { answer: run::Answer::Failed { failure: run::Failure::Cancelled, .. }, .. }] =
        emitted.as_ref()
    else {
        panic!("only the actual abort terminal permits the original Start cancellation answer: {emitted:?}");
    };
    harness.domain.reclaim();
    assert_eq!((harness.domain.peers(), harness.domain.flights(), harness.domain.tickets()), (0, 0, 0));
}

#[test]
fn invalid_conventions_are_refused_at_original_root_start_before_any_effect() {
    let oversized = Box::new([b'x'; run::Conventions::PATH_CAPACITY + 1]);
    for path in [
        b"".as_slice(),
        b"/abs",
        b"a//b",
        b"a/../b",
        b"a/./b",
        b"a/",
        b".",
        b"..",
        b"zero\0byte",
        b"a\\b",
        b"line\nbreak",
        oversized.as_ref(),
    ] {
        for bad_guide in [true, false] {
            let mut harness = Harness::new();
            let conventions = if bad_guide {
                run::Conventions { guide: bytes(path), checks: bytes(b".ci/check") }
            } else {
                run::Conventions { guide: bytes(b"docs/GUIDE"), checks: bytes(path) }
            };
            let emitted = harness.step(Event::Start {
                messages: Box::default(),
                window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
                answered: Box::default(),
                workspace: Some(workspace()),
                reply_to: ReplyTo::new(Token::new(77)),
                host_run: Token::new(77),
                identity: None,
                activation: 1,
                charter: Charter { conventions: Some(conventions), ..charter() },
                transcript: None,
                grants: account_grants(),
            });
            let [Request::Answer { answer, .. }] = emitted.as_ref() else {
                panic!("invalid selection has only its original Start terminal: {emitted:?}");
            };
            assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Conventions)));
            assert!(harness.next().is_empty());
            harness.domain.reclaim();
            assert_eq!((harness.domain.peers(), harness.domain.flights(), harness.domain.tickets()), (0, 0, 0));
        }
    }
}

/// Settle the positive default-path companion using its provider terminal.
fn settle_default_convention_control(harness: &mut Harness, run: Token, main: Token, receiving: &Limits) {
    let mut cancelled = List::with_capacity(max_out(receiving));
    for request in harness.step(Event::Cancel { run }) {
        cancelled.push(request).unwrap();
    }
    for request in harness.next() {
        cancelled.push(request).unwrap();
    }
    let [Request::Cancel { owner }] = cancelled.as_slice() else {
        panic!("positive default control cancels its actual provider request: {cancelled:?}");
    };
    assert_eq!(*owner, main);
    let mut ended = List::with_capacity(max_out(receiving));
    for request in harness.step(Event::Cancelled { owner: main }) {
        ended.push(request).unwrap();
    }
    for request in harness.next() {
        ended.push(request).unwrap();
    }
    let [Request::Answer { answer: run::Answer::Failed { failure: run::Failure::Cancelled, .. }, .. }] =
        ended.as_slice()
    else {
        panic!("positive default control settles its original Start: {ended:?}");
    };
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one maximum-bound guide scenario retains every receiving and refusal assertion"
)]
fn maximum_custom_guide_headings_obey_the_session_receiving_limit_after_discovery() {
    for maximum_paths in [false, true] {
        let run = run::Limits { run_bytes: 16_384, ..LIMITS.run };
        let receiving = Limits {
            run,
            session: session::Limits {
                history_bytes: 8192,
                completion_bytes: 128,
                answer_bytes: 128,
                completion_blocks: 1,
                delegated_result_bytes: crate::feedback_worst_case(&run).unwrap(),
                ..LIMITS.session
            },
            ..LIMITS
        };
        assert!(valid(&receiving));
        let mut harness = Harness::with(&receiving);
        let selected = if maximum_paths {
            Some(run::Conventions {
                guide: Box::new([b'g'; run::Conventions::PATH_CAPACITY]),
                checks: Box::new([b'c'; run::Conventions::PATH_CAPACITY]),
            })
        } else {
            None
        };
        let emitted = harness.step(Event::Start {
            messages: Box::default(),
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
            answered: Box::default(),
            reply_to: ReplyTo::new(Token::new(7)),
            host_run: Token::new(7),
            identity: None,
            activation: 1,
            workspace: Some(Workspace {
                directories: Box::new([
                    Directory {
                        host: Box::from(b"/host".as_slice()),
                        name: bytes(b"work"),
                        root: Token::new(900),
                        writable: false,
                        git: true,
                        conflicts: Box::new([]),
                    },
                    Directory {
                        host: Box::from(b"/host".as_slice()),
                        name: bytes(b"reference"),
                        root: Token::new(901),
                        writable: false,
                        git: true,
                        conflicts: Box::new([]),
                    },
                ]),
            }),
            charter: Charter { conventions: selected, ..charter() },
            transcript: None,
            grants: Box::new([crate::Grant {
                renewable: true,
                name: crate::GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(3600),
            }]),
        });
        let [Request::Admitted { run, .. }, Request::Read { .. }] = emitted.as_ref() else {
            panic!("valid maximum paths admit before actual opening size is known: {emitted:?}");
        };
        let run = *run;
        let emitted = harness.step(Event::Read {
            owner: run,
            read: run::Read::Text { text: bytes(b"Small actual guide"), whole: true },
        });
        let [Request::Read { .. }] = emitted.as_ref() else {
            panic!("both admitted guide requests occur before actual opening refusal: {emitted:?}");
        };
        let emitted = harness.step(Event::Read {
            owner: run,
            read: run::Read::Text { text: bytes(b"Small actual guide"), whole: true },
        });
        if maximum_paths {
            let [
                Request::Answer {
                    answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Conversation)), ..
                },
            ] = emitted.as_ref()
            else {
                panic!(
                    "repeated maximum guide headings exceed receiving ownership before any provider or tool effect: {emitted:?}"
                );
            };
        } else {
            let (main, _) = completing(emitted);
            settle_default_convention_control(&mut harness, run, main, &receiving);
        }
        harness.domain.reclaim();
        assert_eq!(
            (
                harness.domain.peers(),
                harness.domain.flights(),
                harness.domain.tickets(),
                harness.domain.session().sessions()
            ),
            (0, 0, 0, 0)
        );
    }
}

fn scalar_limits() -> Limits {
    Limits {
        run: run::Limits { budget: run::Budget { spend: 100_000, ..LIMITS.run.budget }, ..LIMITS.run },
        session: session::Limits { spend: 100_000, ..LIMITS.session },
        ..LIMITS
    }
}

fn scalar_charter() -> Charter {
    let selected = charter();
    Charter {
        budget: run::Budget { spend: 100, ..selected.budget },
        llm: Llm {
            window: 8,
            output: 1,
            prices: run::Prices { input: 1, cached: 2, cache_write: None, output: 3, unit: 1 },
            ..selected.llm
        },
        ..selected
    }
}

fn scalar_prepared(charter: Charter) -> (Harness, Box<[Request]>) {
    let mut harness = Harness::with(&scalar_limits());
    let emitted = harness.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(1)),
        host_run: Token::new(1),
        identity: None,
        activation: 1,
        charter,
        transcript: None,
    });
    let [Request::Admitted { run, .. }, Request::Read { .. }] = emitted.as_ref() else {
        panic!("run admission: {emitted:?}");
    };
    let emitted = harness.step(Event::Read { owner: *run, read: run::Read::Missing });
    let prepared = match emitted.as_ref() {
        [Request::Probe { owner, .. }] => harness.step(Event::Probed { owner: *owner, executable: false }),
        _ => emitted,
    };
    let mut prepared = prepared;
    for _ in 0_u32..10 {
        if !prepared.is_empty() {
            break;
        }
        prepared = harness.next();
    }
    (harness, prepared)
}

#[test]
fn scalar_reservation_denies_before_any_provider_call_or_credential_lookup() {
    let (mut harness, emitted) = scalar_prepared(scalar_charter());
    let [Request::Answer { answer: run::Answer::Failed { failure, spent, turns }, .. }] = emitted.as_ref() else {
        panic!("unaffordable maximum must settle locally: {emitted:?}")
    };
    assert_eq!(
        *failure,
        run::Failure::Model(run::Fault::Completion {
            failure: run::CompletionFailure::Limit { which: run::Limit::Window, bound: 8 },
            evidence: run::CompletionEvidence::Unsent,
            model: 0,
            attempts: 0,
            report: run::Report::none(),
        })
    );
    assert_eq!((spent.units, spent.turns, *turns), (0, 0, 0));
    assert!(harness.turns.is_empty());
    harness.domain.reclaim();
    assert_eq!((harness.domain.completions.len(), harness.domain.peers(), harness.domain.flights()), (0, 0, 0));
}

#[test]
fn a_priced_finish_settles_within_its_prior_reservation() {
    let mut harness = Harness::with(&scalar_limits());
    let selected = scalar_charter();
    let selected = Charter {
        budget: run::Budget { spend: 100_000, ..selected.budget },
        llm: Llm { window: 8192, output: 10, ..selected.llm },
        ..selected
    };
    let (_, main, _) = harness.admit(1, selected);
    assert!(harness.answer(main, Box::new([served(b"finish", verdict(b"approve"))])).is_empty());
    let emitted = harness.next();
    let [Request::Answer { answer: run::Answer::Accepted { spent, turns, .. }, .. }] = emitted.as_ref() else {
        panic!("priced finish should settle: {emitted:?}")
    };
    assert_eq!((spent.units, spent.turns, *turns), (130, 1, 1));
    assert_eq!(harness.turns.len(), 1);
    assert_eq!(harness.turns.iter().next().expect("final record").spent, 130);
}

#[test]
fn a_provider_charge_above_its_reserved_maximum_fails_without_recording_it() {
    let mut harness = Harness::with(&scalar_limits());
    let selected = scalar_charter();
    let selected = Charter {
        budget: run::Budget { spend: 100_000, ..selected.budget },
        llm: Llm { window: 8192, output: 10, ..selected.llm },
        ..selected
    };
    let (_, main, _) = harness.admit(1, selected);
    let completion = Completion {
        reasoning_dropped: Box::new([]),
        content: Box::new([Said::Text { text: bytes(b"overbound"), replay: None }]),
        stop: Stop::EndTurn,
        usage: Usage { output_tokens: Some(100_000), ..Usage::ZERO },
    };
    let emitted = harness.step(Event::Completed { owner: main, completion });
    let emitted = if emitted.is_empty() { harness.next() } else { emitted };
    let [Request::Answer { answer: run::Answer::Failed { failure, spent, turns }, .. }] = emitted.as_ref() else {
        panic!("overbound terminal fails the run: {emitted:?}");
    };
    assert_eq!(
        *failure,
        run::Failure::Budget { exhausted: run::Exhausted::Overflow(run::Overflow::Spend), used: None, bound: None }
    );
    assert_eq!((*spent, *turns), (run::Spend::ZERO, 0));
    assert!(harness.turns.is_empty());
}

#[test]
fn overflowing_reservation_runs_no_calls_and_tells_no_turn() {
    let selected = Charter {
        llm: Llm {
            prices: run::Prices { input: u64::MAX, cached: 1, cache_write: None, output: 1, unit: 1 },
            ..scalar_charter().llm
        },
        ..scalar_charter()
    };
    let mut harness = Harness::with(&scalar_limits());
    let emitted = harness.step(Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        answered: Box::default(),
        workspace: Some(workspace()),
        grants: account_grants(),
        reply_to: ReplyTo::new(Token::new(1)),
        host_run: Token::new(1),
        identity: None,
        activation: 1,
        charter: selected,
        transcript: None,
    });
    let [Request::Answer { answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Budget(reason))), .. }] =
        emitted.as_ref()
    else {
        panic!("overflow refuses at entrance: {emitted:?}")
    };
    assert_eq!(*reason, run::BudgetRefusal::Reserve(run::Part::Spend));
    assert!(harness.turns.is_empty());
    assert_eq!(harness.domain.flights(), 0);
}

mod completion_retry;

#[test]
fn each_main_or_child_model_quantity_is_checked_before_admission() {
    for child in [false, true] {
        for bad in 0_u32..5 {
            let mut requested = charter();
            let mut model = requested.llm.clone();
            match bad {
                0 => model.model = bytes(b"not-configured"),
                1 => model.window = 8193,
                2 => model.output = 1025,
                3 => model.window = 0,
                4 => model.output = 0,
                _ => unreachable!("five entrance refusals"),
            }
            if child {
                requested.models = Box::new([model]);
            } else {
                requested.llm = model;
            }
            let mut harness = Harness::new();
            let emitted = harness.step(Event::Start {
                messages: Box::default(),
                reply_to: ReplyTo::new(Token::new(7)),
                host_run: Token::new(7),
                identity: None,
                activation: 1,
                window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
                charter: requested,
                workspace: None,
                grants: account_grants(),
                transcript: None,
                answered: Box::new([]),
            });
            let [Request::Answer { answer, .. }] = emitted.as_ref() else {
                panic!("one admission refusal: {emitted:?}")
            };
            assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Llm)));
            assert!(harness.turns.is_empty());
        }
    }
}

#[test]
fn the_selected_model_progress_reaches_the_original_root_completion() {
    for selected in 0..4 {
        let mut harness = Harness::new();
        for (index, model) in harness.domain.config.models.iter_mut().enumerate() {
            let index = u64::try_from(index).expect("four declarations");
            model.head = Duration::from_secs(11 + index);
            model.idle = Duration::from_secs(21 + index);
        }
        let declared = harness.domain.config.models[selected].clone();
        let mut requested = charter();
        requested.llm.endpoint = declared.endpoint;
        requested.llm.model = declared.model;
        requested.conventions = None;
        let emitted = harness.step(Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(7)),
            host_run: Token::new(7),
            identity: None,
            activation: 1,
            window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
            charter: requested,
            workspace: Some(workspace()),
            grants: account_grants(),
            transcript: None,
            answered: Box::default(),
        });
        let [Request::Admitted { run, .. }, Request::Read { owner, .. }] = emitted.as_ref() else {
            panic!("original root discovery: {emitted:?}");
        };
        assert_eq!(run, owner);
        let emitted = harness.step(Event::Read { owner: *owner, read: run::Read::Missing });
        let prepared = match emitted.as_ref() {
            [Request::Probe { owner, .. }] => harness.step(Event::Probed { owner: *owner, executable: false }),
            [Request::Complete { .. }] => emitted,
            _ => panic!("original root preparation: {emitted:?}"),
        };
        let [Request::Complete { deadlines, .. }] = prepared.as_ref() else {
            panic!("original root completion: {prepared:?}");
        };
        assert_eq!((deadlines.head, deadlines.idle), (declared.head, declared.idle));
        assert_eq!(deadlines.whole, harness.env.now.saturating_add(BUDGET.time));
    }
}

fn awaiting_start(transcript: Option<session::record::Transcript>, answered: Box<[crate::AnsweredCall]>) -> Event {
    Event::Start {
        messages: Box::default(),
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        reply_to: ReplyTo::new(Token::new(91)),
        host_run: Token::new(91),
        identity: None,
        activation: 2,
        charter: Charter { brief: smith_domain_run::Brief { sections: Box::new([]) }, resume: true, ..charter() },
        workspace: None,
        transcript,
        answered,
        grants: account_grants(),
    }
}

#[test]
fn root_awaiting_opens_no_session_or_provider_call_until_the_first_message() {
    let mut harness = Harness::new();
    let emitted = harness.step(awaiting_start(None, Box::default()));
    let [Request::Admitted { run, .. }, Request::Waiting { host_run, read }] = emitted.as_ref() else {
        panic!("awaiting root emits no completion: {emitted:?}");
    };
    let run = *run;
    assert_eq!((*host_run, *read), (Token::new(91), None));
    assert_eq!((harness.domain.starts.len(), harness.domain.peers.len(), harness.domain.sessions.len()), (1, 0, 0));
    assert!(harness.next().is_empty());
    let emitted = harness.step(Event::Message {
        finish_requested: false,
        run,
        name: Token::new(1),
        label: bytes(b"person"),
        text: bytes(b"real coding task"),
    });
    let [Request::Complete { prompt, .. }] = emitted.as_ref() else {
        panic!("one direct provider completion: {emitted:?}")
    };
    assert_eq!(prompt.messages.len(), 1);
    assert_eq!(
        prompt.messages[0].content.as_ref(),
        [Block::Text { text: bytes(b"person: real coding task"), replay: None }]
    );
    assert_eq!(harness.turns.len(), 0);
}

#[test]
fn root_cancel_while_awaiting_releases_unused_restore_context_without_a_session_terminal() {
    let mut harness = Harness::new();
    let emitted = harness.step(awaiting_start(None, Box::default()));
    let [Request::Admitted { run, .. }, Request::Waiting { .. }] = emitted.as_ref() else {
        panic!("awaiting: {emitted:?}")
    };
    let emitted = harness.step(Event::Cancel { run: *run });
    let [Request::Answer { answer, .. }] = emitted.as_ref() else { panic!("direct cancelled answer: {emitted:?}") };
    assert_eq!(answer, &run::Answer::Failed { failure: run::Failure::Cancelled, spent: run::Spend::ZERO, turns: 0 });
    assert!(harness.next().is_empty());
    assert_eq!((harness.domain.starts.len(), harness.domain.peers.len(), harness.domain.sessions.len()), (0, 0, 0));
    assert_eq!((harness.domain.run.runs(), harness.domain.run.conversations()), (0, 0));
}

#[test]
fn first_message_after_awaiting_restores_history_and_absent_host_answers_once() {
    let mut harness = Harness::new();
    let transcript = session::record::Transcript {
        version: session::record::VERSION,
        endpoint: Some(session::llm::Endpoint(1)),
        dialect: 1,
        turns: Box::new([session::record::Turn {
            model: b"model-a".as_slice().into(),
            effort: None,
            kind: session::record::Kind::First(Box::new(session::record::Opener {
                affinity: crate::llm::Affinity { key: [0; 16], thread: 0 },
                account: Some(0),
                backend: session::record::Backend::Direct,
            })),
            version: session::record::VERSION,
            endpoint: Some(session::llm::Endpoint(1)),
            dialect: 1,
            sequence: 1,
            usage: Usage::ZERO,
            spent: 0,
            jobs: 0,
            messages: Box::new([
                session::llm::Message {
                    role: Role::User,
                    content: Box::new([session::llm::Block::Text { text: bytes(b"old task"), replay: None }]),
                },
                session::llm::Message {
                    role: Role::Assistant,
                    content: Box::new([session::llm::Block::Text { text: bytes(b"old response"), replay: None }]),
                },
            ]),
        }]),
    };
    let answered = Box::new([crate::AnsweredCall {
        name: run::CallName { activation: 1, completion: 1, position: 0 },
        tool: bytes(b"check"),
        answer: crate::Answered::TooLarge,
    }]);
    let emitted = harness.step(awaiting_start(Some(transcript), answered));
    let [Request::Admitted { run, .. }, Request::Waiting { .. }] = emitted.as_ref() else {
        panic!("awaiting: {emitted:?}")
    };
    let run = *run;
    let emitted = harness.step(Event::Message {
        finish_requested: false,
        run,
        name: Token::new(2),
        label: bytes(b"person"),
        text: bytes(b"new coding task"),
    });
    let [Request::Complete { prompt, .. }] = emitted.as_ref() else { panic!("restored completion: {emitted:?}") };
    assert_eq!(prompt.messages.len(), 3);
    assert_eq!(prompt.messages[0].content.as_ref(), [Block::Text { text: bytes(b"old task"), replay: None }]);
    assert_eq!(prompt.messages[1].content.as_ref(), [Block::Text { text: bytes(b"old response"), replay: None }]);
    let [Block::Text { text, .. }] = prompt.messages[2].content.as_ref() else { panic!("waking text") };
    assert!(text.starts_with(b"Earlier host answers absent from the saved transcript:"));
    assert!(text.ends_with(b"person: new coding task"));
    assert_eq!(harness.domain.starts.len(), 1);
    assert_eq!(harness.turns.len(), 0, "restored history is not retold as an activation turn");
}

#[test]
#[expect(
    clippy::wildcard_enum_match_arm,
    clippy::single_match,
    reason = "the charge story selects its Used observation"
)]
fn missing_provider_usage_settles_the_root_reservation_and_keeps_the_raw_fact() {
    for raw in [
        Usage { output_tokens: Some(2), cache_write_tokens: Some(0), reasoning_tokens: Some(1), ..Usage::NONE },
        Usage { input_tokens: Some(0), reasoning_tokens: Some(0), ..Usage::NONE },
        Usage::NONE,
    ] {
        let mut harness = Harness::with(&scalar_limits());
        let selected = scalar_charter();
        let selected = Charter {
            budget: run::Budget { spend: 100_000, ..selected.budget },
            llm: Llm {
                window: 8192,
                output: 10,
                prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 3_000_000, unit: 1_000_000 },
                ..selected.llm
            },
            ..selected
        };
        let (_, main, _) = harness.admit(1, selected);
        // Ten output tokens cost 30; this positive input fraction rounds the
        // full reservation to 31, independent of this fixture's prompt size.
        assert!(
            harness
                .step(Event::Completed {
                    owner: main,
                    completion: Completion {
                        reasoning_dropped: Box::default(),
                        content: Box::new([served(b"finish", verdict(b"approve"))]),
                        stop: Stop::ToolUse,
                        usage: raw,
                    },
                })
                .is_empty()
        );
        let emitted = harness.next();
        let [Request::Answer { answer: run::Answer::Accepted { spent, turns, .. }, .. }] = emitted.as_ref() else {
            panic!("missing usage still settles the reserved finish: {emitted:?}");
        };
        assert_eq!(
            (spent.units, spent.turns, spent.input, spent.output, *turns),
            (31, 1, raw.input_tokens, raw.output_tokens, 1)
        );
        assert_eq!(
            [spent.input, spent.output, spent.cache_read, spent.cache_write, spent.reasoning],
            [raw.input_tokens, raw.output_tokens, raw.cache_read_tokens, raw.cache_write_tokens, raw.reasoning_tokens],
            "the terminal preserves every count's presence, and reasoning is not charged twice"
        );
        let turn = harness.turns.iter().next().expect("one saved turn");
        assert_eq!((turn.usage, turn.spent), (raw, 31));
        let mut observed = false;
        for _ in 0..harness.env.limits.session.facts {
            match harness.pop_fact() {
                Some(Fact::Session { fact }) => match fact.kind {
                    session::FactKind::Used { usage, .. } => {
                        assert_eq!(usage, raw);
                        let info = fact.response.expect("Used carries its accepted charge");
                        assert_eq!((info.number, info.spent), (1, Some(31)));
                        observed = true;
                    }
                    _ => {}
                },
                Some(Fact::Run { .. }) | None => {}
            }
        }
        assert!(observed);
    }
}

#[test]
fn command_span_ends_at_the_actual_io_terminal_after_cancel() {
    let mut h = Harness::new();
    let mut requested = charter();
    requested.grants.tools.shell = true;
    let (run, owner, _) = h.admit(7, requested);
    assert_eq!(h.long.as_slice(), &[Some(crate::Operation::Completion)]);
    let emitted = h.answer(
        owner,
        Box::new([owned(
            b"command",
            Call::Shell {
                after: smith_domain_tools::Yield::Never,
                command: bytes(b"sleep 12"),
                timeout: tools::Timeout::Asked(Duration::from_secs(30)),
                output: tools::Budget::Default,
            },
        )]),
    );
    let [Request::Io { owner: command, op: Op::Spawn { .. }, .. }] = emitted.as_ref() else {
        panic!("actual shell IO: {emitted:?}");
    };
    let command = *command;
    assert_eq!(h.long.as_slice(), &[Some(crate::Operation::Completion), None, Some(crate::Operation::Command)]);
    assert_eq!(h.domain.commands.len(), 1);
    h.env.now = Time::ZERO.saturating_add(Duration::from_secs(11));
    assert!(h.step(Event::Cancel { run }).is_empty());
    assert_eq!(h.next().as_ref(), &[Request::CancelIo { owner: command }]);
    assert_eq!(h.domain.commands.len(), 1, "cancellation leaves actual IO outstanding");
    assert_eq!(h.long.len(), 3, "no long terminal before actual IO settlement");
    let ended = h.step(Event::Done { owner: command, done: tools::Done::Cancelled });
    assert_eq!(h.domain.commands.len(), 0);
    assert_eq!(h.long.as_slice(), &[Some(crate::Operation::Completion), None, Some(crate::Operation::Command), None]);
    let ended = if ended.is_empty() { h.next() } else { ended };
    let [Request::Answer { answer: run::Answer::Failed { failure: run::Failure::Cancelled, .. }, .. }] = ended.as_ref()
    else {
        panic!("only actual IO settlement permits the cancellation answer: {ended:?}");
    };
}

#[test]
fn an_unchecked_charter_effort_is_refused_before_any_run_work() {
    let mut harness = Harness::new();
    for model in &mut harness.domain.config.models {
        model.efforts = Box::new([b"low".as_slice().into()]);
    }
    let mut requested = charter();
    requested.llm.effort = Some(b"high".as_slice().into());
    let emitted = harness.step(Event::Start {
        messages: Box::new([]),
        reply_to: ReplyTo::new(Token::new(7)),
        host_run: Token::new(7),
        identity: None,
        activation: 1,
        window: crate::Window { turns: u32::MAX, bytes: u64::MAX },
        charter: requested,
        workspace: None,
        grants: account_grants(),
        transcript: None,
        answered: Box::new([]),
    });
    let [Request::Answer { answer, .. }] = emitted.as_ref() else { panic!("one refusal") };
    assert_eq!(answer, &run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Effort)));
    assert!(harness.turns.is_empty());
}

#[test]
fn a_refused_account_raises_one_notice_with_its_reason() {
    let mut harness = Harness::new();
    let (_, owner, _) = harness.admit(7, charter());
    let emitted = harness.step(Event::Failed {
        owner,
        failure: crate::llm::Failure::Refused,
        evidence: crate::llm::Evidence::Unknown,
        detail: bytes(b"provider reason"),
        report: crate::llm::Report { code: bytes(b"not-eligible"), request: Box::default(), field: Box::default() },
    });
    let notices = account_notices(&emitted).into_boxed();
    assert_eq!(
        &*notices,
        [&Request::Refused { account: 0, code: bytes(b"not-eligible"), reason: bytes(b"provider reason") }]
    );
    assert!(harness.domain.next_deadline().is_none());
}

#[test]
fn an_unsupported_request_raises_no_notice() {
    let mut harness = Harness::new();
    let (_, owner, _) = harness.admit(7, charter());
    let emitted = harness.step(Event::Failed {
        owner,
        failure: crate::llm::Failure::Unsupported,
        evidence: crate::llm::Evidence::Unknown,
        detail: bytes(b"unsupported field"),
        report: crate::llm::Report::none(),
    });
    assert_eq!(account_notices(&emitted).len(), 0);
}

fn account_notices(requests: &[Request]) -> List<&Request> {
    let mut notices = List::with_capacity(3);
    for request in requests {
        if let Request::Unauthorized { .. } | Request::Limited { .. } | Request::Refused { .. } = request {
            notices.push(request).expect("bounded notices");
        }
    }
    notices
}

#[test]
fn a_key_race_uses_the_new_generation_then_ends_on_its_own_policy() {
    let limits = Limits { session: session::Limits { retries: 3, ..LIMITS.session }, ..LIMITS };
    for renewable in [false, true] {
        let mut h = Harness::with(&limits);
        let first = crate::Grant {
            name: crate::GrantName { account: 0, generation: 1 },
            valid: Duration::from_secs(60),
            renewable,
        };
        let (_, owner, _, _) = h.admit_named_grants(7, charter(), Box::new([first]));
        let next = crate::Grant { name: crate::GrantName { account: 0, generation: 2 }, renewable: false, ..first };
        assert!(h.step(Event::Grant { grant: next }).is_empty());
        // Repeated or older refreshes cannot replace the usable generation or its policy.
        assert!(h.step(Event::Grant { grant: crate::Grant { renewable: true, ..next } }).is_empty());
        assert!(h.step(Event::Grant { grant: first }).is_empty());
        let failed = || Event::Failed {
            owner,
            failure: crate::llm::Failure::Unauthorized,
            evidence: crate::llm::Evidence::Response { status: Some(401) },
            detail: Box::default(),
            report: crate::llm::Report::none(),
        };
        assert_eq!(&*h.step(failed()), &[Request::Unauthorized { grant: first.name }]);
        h.env.now = h.domain.next_deadline().expect("retry of the old attempt");
        let emitted = h.fire();
        let [Request::Complete { grant, .. }] = emitted.as_ref() else { panic!("retry on newer generation") };
        assert_eq!(*grant, Some(next.name));
        let terminal = h.step(failed());
        let [Request::Unauthorized { grant }, Request::Answer { answer, .. }] = terminal.as_ref() else {
            panic!("one notice and one activation terminal, got {terminal:?}");
        };
        assert_eq!(*grant, next.name);
        assert_eq!(
            answer,
            &run::Answer::Failed {
                failure: run::Failure::Model(run::Fault::Completion {
                    failure: run::CompletionFailure::Unauthorized,
                    evidence: run::CompletionEvidence::Response { status: Some(401) },
                    model: 0,
                    attempts: 2,
                    report: run::Report::none(),
                }),
                spent: run::Spend::ZERO,
                turns: 0,
            }
        );
        assert!(h.domain.next_deadline().is_none());
        h.domain.reclaim();
        assert_eq!((h.domain.peers(), h.domain.flights(), h.domain.tickets()), (0, 0, 0));
        assert!(h.domain.completions.is_empty());
    }
}

#[test]
fn invalid_limits_are_refused() {
    assert!(valid(&LIMITS));
    // Aggregate heap accounting is independent of constructor validity.
    assert!(valid(&Limits { configured_model_bytes: u64::MAX, ..LIMITS }));
    for budget in [
        run::Budget { turns: CEILING.turns + 1, ..BUDGET },
        run::Budget { spend: LIMITS.session.spend + 1, ..BUDGET },
        run::Budget { time: Duration::from_secs(3601), ..BUDGET },
    ] {
        assert!(!valid(&Limits { run: run::Limits { budget, ..LIMITS.run }, ..LIMITS }));
    }
    for run in [
        run::Limits { conversations: LIMITS.session.sessions + 1, ..LIMITS.run },
        run::Limits { directories: LIMITS.session.tools.repos + 1, ..LIMITS.run },
        run::Limits { directory_name_bytes: LIMITS.session.tools.path_bytes + 1, ..LIMITS.run },
        run::Limits { host_path_bytes: LIMITS.session.tools.host_path_bytes + 1, ..LIMITS.run },
        run::Limits { answered_bytes: u32::try_from(LIMITS.session.history_bytes + 1).expect("small"), ..LIMITS.run },
        // Feedback's live price must be representable independently of receiving room.
        run::Limits { run_bytes: u64::MAX, ..LIMITS.run },
        // Run-child validity is checked even when root compatibility holds.
        run::Limits { offer_messages: 0, ..LIMITS.run },
    ] {
        assert!(!valid(&Limits { run, ..LIMITS }));
    }
    assert!(!valid(&Limits {
        run: run::Limits { budget: run::Budget { spend: 0, ..BUDGET }, ..LIMITS.run },
        session: session::Limits { spend: 0, ..LIMITS.session },
        ..LIMITS
    }));
    let feedback = crate::feedback_worst_case(&LIMITS.run).expect("representable feedback");
    assert!(valid(&Limits {
        session: session::Limits { delegated_result_bytes: feedback, ..LIMITS.session },
        ..LIMITS
    }));
    assert!(!valid(&Limits {
        session: session::Limits { delegated_result_bytes: feedback - 1, ..LIMITS.session },
        ..LIMITS
    }));
    let cells =
        u64::from(LIMITS.session.completion_blocks) * u64::try_from(size_of::<Decoded>()).expect("cell fits u64");
    assert!(valid(&Limits { decoded_call_bytes: cells, ..LIMITS }));
    assert!(!valid(&Limits { decoded_call_bytes: cells - 1, ..LIMITS }));
    let receiving = Limits { run: run::Limits { answered_bytes: 0, ..LIMITS.run }, ..LIMITS };
    assert!(valid(&Limits { session: session::Limits { history_bytes: cells, ..receiving.session }, ..receiving }));
    assert!(!valid(&Limits {
        session: session::Limits { history_bytes: cells - 1, ..receiving.session },
        ..receiving
    }));
    assert!(!valid(&Limits { session: session::Limits { history_messages: 1, ..LIMITS.session }, ..LIMITS }));

    // Child tables already impose tighter caps. Exercise these defensive root
    // capacities directly as well as refusing the resulting invalid limits.
    let flights = Limits { run: run::Limits { conversations: u32::MAX, ..LIMITS.run }, ..LIMITS };
    assert_eq!(limits::flights(&flights), None);
    assert!(!valid(&flights));
    let handoffs = Limits {
        run: run::Limits { conversations: u32::MAX / 2 + 1, ..LIMITS.run },
        session: session::Limits { parallel_tools: 1, ..LIMITS.session },
        ..LIMITS
    };
    assert!(limits::flights(&handoffs).is_some());
    assert_eq!(limits::handoffs(&handoffs), None);
    assert!(!valid(&handoffs));
    let commands = Limits {
        session: session::Limits {
            tools: tools::Limits { kits: u32::MAX, calls: 2, ..LIMITS.session.tools },
            ..LIMITS.session
        },
        ..LIMITS
    };
    assert_eq!(limits::commands(&commands), None);
    assert!(!valid(&commands));
}
