//! Admission/source-law controls (Temper 19735a06; domain/host.md, 4 and 10).
//! The independent V2 scripted world carries the remaining boundary histories.
use crate::{Domain, End, Input, Output, Start};
use crate::{parent, process};
use alloc::boxed::Box;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

#[test]
fn an_unread_charter_version_is_a_distinct_agent_refusal() {
    let unread = crate::RunResult::Refused { refusal: crate::Refusal::Invalid(crate::RunInvalid::CharterVersion) };
    let malformed = crate::RunResult::Refused { refusal: crate::Refusal::Invalid(crate::RunInvalid::MalformedCharter) };
    let oversized = crate::RunResult::Refused { refusal: crate::Refusal::Invalid(crate::RunInvalid::TooLarge) };
    assert_ne!(unread, malformed);
    assert_ne!(malformed, oversized);
    assert_ne!(unread, oversized);
}

fn limits() -> crate::Limits {
    crate::Limits {
        agents: 1,
        directories: 0,
        conflicts: 0,
        path_bytes: 0,
        name_bytes: 64,
        accounts: 0,
        charter_bytes: 64,
        transcript_bytes: 64,
        answered_bytes: 64,
        message_bytes: 64,
        messages: 1,
        calls: 1,
        call_bytes: 64,
        answer_bytes: crate::Delivered::worst_case(),
        turns: 1,
        turn_bytes: 64,
        unacknowledged_bytes: 64,
        fact_bytes: 64,
        outcome_bytes: 64,
        detail_bytes: 64,
        spawn_timeout: Duration::from_secs(1),
        no_progress: Duration::from_secs(10),
        silence: Duration::ZERO,
        long_span: Duration::from_secs(60),
        long_open: 64,
        wall_time: Duration::from_secs(100),
        cancel_grace: Duration::from_secs(5),
        exit_grace: Duration::from_secs(5),
        teardown: Duration::ZERO,
        kill_after: Duration::from_secs(2),
        facts: 4,
    }
}

fn admitted() -> (Domain, Env<crate::Limits>, Queue<Output>, Token) {
    admitted_with_limits(limits())
}

fn admitted_with_limits(limits: crate::Limits) -> (Domain, Env<crate::Limits>, Queue<Output>, Token) {
    let mut domain = Domain::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::uncapped();
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }),
        &mut out,
    );
    let Some(Output::Process(process::Request::Spawn { owner, .. })) = out.pop() else { panic!("spawn admitted") };
    crate::step(&mut domain, &env, Input::Process(process::Event::Spawned { owner, process: Token::new(2) }), &mut out);
    while out.pop().is_some() {}
    crate::step(
        &mut domain,
        &env,
        Input::Process(process::Event::Received { owner, message: crate::Up::Admitted }),
        &mut out,
    );
    while out.pop().is_some() {}
    (domain, env, out, owner)
}

#[test]
fn a_parent_that_drains_late_loses_no_fact() {
    // Spawned and Admitted each emit one observation, beyond the estimate of one.
    const START_FACTS: u32 = 2;
    let mut limits = limits();
    limits.facts = 1;
    let (mut domain, _env, _out, _owner) = admitted_with_limits(limits);
    let client = Token::new(1);
    assert_eq!(domain.backlog(), START_FACTS);
    assert_eq!(domain.pop_fact(), Some(crate::Fact::Started { client }));
    assert_eq!(domain.pop_fact(), Some(crate::Fact::Admitted { client }));
    assert_eq!(domain.pop_fact(), None);
    assert_eq!(domain.backlog(), 0);
    assert_eq!(domain.facts_lost(), 0);
}

fn cancelled_answer() -> crate::Answer {
    crate::Answer {
        read: None,
        turns: 0,
        spent: 0,
        result: crate::RunResult::Failed { failure: crate::RunFailure::Cancelled },
    }
}

fn answer_count(out: &Queue<Output>, expected: &crate::Answer) -> u32 {
    let mut count = 0_u32;
    for request in out {
        match request {
            Output::Parent(parent::Request::Answered { answer, .. }) => {
                assert_eq!(answer, expected);
                count = count.checked_add(1).expect("bounded answers");
            }
            Output::Parent(
                parent::Request::Started { .. }
                | parent::Request::Admitted { .. }
                | parent::Request::Called { .. }
                | parent::Request::Withdrawn { .. }
                | parent::Request::Turn { .. }
                | parent::Request::Waiting { .. }
                | parent::Request::Long { .. }
                | parent::Request::LongDone { .. }
                | parent::Request::Unread { .. }
                | parent::Request::Unauthorized { .. }
                | parent::Request::Refused { .. }
                | parent::Request::Limited { .. }
                | parent::Request::Told { .. }
                | parent::Request::Faulted { .. }
                | parent::Request::MessageRefused { .. }
                | parent::Request::Gone { .. },
            )
            | Output::Process(_) => {}
        }
    }
    count
}

#[test]
fn a_parent_drop_before_the_answer_discards_a_later_cancelled_answer() {
    let (mut domain, env, mut out, owner) = admitted();
    crate::step(&mut domain, &env, Input::Parent(parent::Event::Drop { agent: owner }), &mut out);
    while out.pop().is_some() {}
    crate::step(
        &mut domain,
        &env,
        Input::Process(process::Event::Received { owner, message: crate::Up::Answer { answer: cancelled_answer() } }),
        &mut out,
    );
    assert_eq!(answer_count(&out, &cancelled_answer()), 0, "a dropped run never answers later");
}

#[test]
fn a_parent_drop_after_the_answer_preserves_that_answer() {
    let (mut domain, env, mut out, owner) = admitted();
    let answer = cancelled_answer();
    crate::step(
        &mut domain,
        &env,
        Input::Process(process::Event::Received { owner, message: crate::Up::Answer { answer: cancelled_answer() } }),
        &mut out,
    );
    crate::step(&mut domain, &env, Input::Parent(parent::Event::Drop { agent: owner }), &mut out);
    assert_eq!(answer_count(&out, &answer), 1);
}

fn start() -> Start {
    Start {
        live: None,
        messages: Box::default(),
        logical_run: Token::new(1),
        identity: None,
        activation: 1,
        workspace: None,
        charter: empty_charter(),
        transcript: None,
        answered: Box::new([]),
        directories: Box::new([]),
        grants: Box::new([]),
    }
}
#[test]
fn a_spawn_beyond_the_slots_is_refused_as_busy_without_displacing_the_active_owner() {
    let limits = limits();
    let mut domain = Domain::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::uncapped();
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }),
        &mut out,
    );
    let first = match out.pop().expect("first spawn admitted") {
        Output::Process(process::Request::Spawn { owner, .. }) => owner,
        Output::Parent(
            parent::Request::Started { .. }
            | parent::Request::Admitted { .. }
            | parent::Request::Called { .. }
            | parent::Request::Withdrawn { .. }
            | parent::Request::Turn { .. }
            | parent::Request::Waiting { .. }
            | parent::Request::Long { .. }
            | parent::Request::LongDone { .. }
            | parent::Request::Unread { .. }
            | parent::Request::Unauthorized { .. }
            | parent::Request::Refused { .. }
            | parent::Request::Limited { .. }
            | parent::Request::Told { .. }
            | parent::Request::Answered { .. }
            | parent::Request::Faulted { .. }
            | parent::Request::MessageRefused { .. }
            | parent::Request::Gone { .. },
        )
        | Output::Process(
            process::Request::Send { .. }
            | process::Request::Read { .. }
            | process::Request::Signal { .. }
            | process::Request::Wait { .. }
            | process::Request::Reap { .. },
        ) => panic!("expected actual Spawn"),
    };
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Spawn { client: Token::new(2), start: start() }),
        &mut out,
    );
    match out.pop().expect("second spawn refused") {
        Output::Parent(parent::Request::Gone { client, end, detail }) => {
            assert_eq!(client, Token::new(2));
            assert_eq!(end, End::Busy);
            assert!(detail.is_empty());
        }
        Output::Process(
            process::Request::Spawn { .. }
            | process::Request::Send { .. }
            | process::Request::Read { .. }
            | process::Request::Signal { .. }
            | process::Request::Wait { .. }
            | process::Request::Reap { .. },
        )
        | Output::Parent(
            parent::Request::Started { .. }
            | parent::Request::Admitted { .. }
            | parent::Request::Called { .. }
            | parent::Request::Withdrawn { .. }
            | parent::Request::Turn { .. }
            | parent::Request::Waiting { .. }
            | parent::Request::Long { .. }
            | parent::Request::LongDone { .. }
            | parent::Request::Unread { .. }
            | parent::Request::Unauthorized { .. }
            | parent::Request::Refused { .. }
            | parent::Request::Limited { .. }
            | parent::Request::Told { .. }
            | parent::Request::Answered { .. }
            | parent::Request::Faulted { .. }
            | parent::Request::MessageRefused { .. },
        ) => panic!("expected exact Busy terminal"),
    }
    assert_eq!(domain.agents(), 1);
    crate::step(
        &mut domain,
        &env,
        Input::Process(process::Event::Unspawned { owner: first, detail: Box::new([]) }),
        &mut out,
    );
    domain.reclaim();
    assert_eq!(domain.agents(), 0);
}

fn mount(name: &[u8], writable: bool, git: bool, conflicts: &[&[u8]]) -> crate::Directory {
    let mut paths = skein_lib::List::with_capacity(u32::try_from(conflicts.len()).unwrap());
    for path in conflicts {
        paths.push(Box::from(*path)).expect("fixture conflict count");
    }
    crate::Directory {
        host: Box::from(b"/host".as_slice()),
        root: Token::new(1),
        name: Box::from(name),
        writable,
        git,
        conflicts: paths.into_boxed(),
    }
}

fn admit_start(start: Start, limits: crate::Limits) -> Output {
    let mut domain = Domain::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::uncapped();
    crate::step(&mut domain, &env, Input::Parent(parent::Event::Spawn { client: Token::new(91), start }), &mut out);
    out.pop().expect("one actual spawn or invalid terminal")
}

#[test]
fn host_mount_metadata_admission_agrees_with_optional_workspace_and_git_kind() {
    let receiving = crate::Limits { directories: 2, conflicts: 2, path_bytes: 4096, ..limits() };
    let selected = Start {
        workspace: Some(Token::new(7)),
        directories: Box::new([mount(b"plain", true, false, &[]), mount(b"git", false, true, &[b"src/conflict.rs"])]),
        ..start()
    };
    match admit_start(selected, receiving) {
        Output::Process(process::Request::Spawn { .. }) => {}
        Output::Parent(
            parent::Request::Started { .. }
            | parent::Request::Admitted { .. }
            | parent::Request::Called { .. }
            | parent::Request::Withdrawn { .. }
            | parent::Request::Turn { .. }
            | parent::Request::Waiting { .. }
            | parent::Request::Long { .. }
            | parent::Request::LongDone { .. }
            | parent::Request::Unread { .. }
            | parent::Request::Unauthorized { .. }
            | parent::Request::Refused { .. }
            | parent::Request::Limited { .. }
            | parent::Request::Told { .. }
            | parent::Request::Answered { .. }
            | parent::Request::Faulted { .. }
            | parent::Request::MessageRefused { .. }
            | parent::Request::Gone { .. },
        )
        | Output::Process(
            process::Request::Send { .. }
            | process::Request::Read { .. }
            | process::Request::Signal { .. }
            | process::Request::Wait { .. }
            | process::Request::Reap { .. },
        ) => panic!("readonly git conflict evidence is valid"),
    }
    let cases = [
        Start { workspace: None, directories: Box::new([mount(b"git", false, true, &[])]), ..start() },
        Start { workspace: Some(Token::new(7)), directories: Box::new([]), ..start() },
        Start { workspace: Some(Token::new(7)), directories: Box::new([mount(b"a/b", true, true, &[])]), ..start() },
        Start {
            workspace: Some(Token::new(7)),
            directories: Box::new([mount(b"plain", true, false, &[b"src/conflict"])]),
            ..start()
        },
        Start {
            workspace: Some(Token::new(7)),
            directories: Box::new([mount(b"git", false, true, &[b"src/conflict", b"src/conflict"])]),
            ..start()
        },
        Start {
            workspace: Some(Token::new(7)),
            directories: Box::new([mount(b"git", false, true, &[b"../conflict"])]),
            ..start()
        },
    ];
    for rejected in cases {
        match admit_start(rejected, receiving) {
            Output::Parent(parent::Request::Gone { end, .. }) => {
                assert_eq!(end, End::Invalid(crate::Invalid::Directories));
            }
            Output::Process(
                process::Request::Spawn { .. }
                | process::Request::Send { .. }
                | process::Request::Read { .. }
                | process::Request::Signal { .. }
                | process::Request::Wait { .. }
                | process::Request::Reap { .. },
            )
            | Output::Parent(
                parent::Request::Started { .. }
                | parent::Request::Admitted { .. }
                | parent::Request::Called { .. }
                | parent::Request::Withdrawn { .. }
                | parent::Request::Turn { .. }
                | parent::Request::Waiting { .. }
                | parent::Request::Long { .. }
                | parent::Request::LongDone { .. }
                | parent::Request::Unread { .. }
                | parent::Request::Unauthorized { .. }
                | parent::Request::Refused { .. }
                | parent::Request::Limited { .. }
                | parent::Request::Told { .. }
                | parent::Request::Answered { .. }
                | parent::Request::Faulted { .. }
                | parent::Request::MessageRefused { .. },
            ) => panic!("metadata refuses before lower spawn"),
        }
    }
    assert!(!crate::valid(&crate::Limits { path_bytes: 4097, ..receiving }));
}

#[test]
fn message_admission_includes_the_label_separator_at_the_bound_and_one_byte_over() {
    let limits = crate::Limits { message_bytes: 12, ..limits() };
    let mut domain = Domain::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::uncapped();
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }),
        &mut out,
    );
    let Some(Output::Process(process::Request::Spawn { owner, .. })) = out.pop() else { panic!("spawn admitted") };
    crate::step(&mut domain, &env, Input::Process(process::Event::Spawned { owner, process: Token::new(2) }), &mut out);
    while out.pop().is_some() {}
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Message {
            finish_requested: false,
            agent: owner,
            name: Token::new(10),
            label: Box::from(*b"peer"),
            text: Box::from(*b"123456"),
        }),
        &mut out,
    );
    assert!(out.pop().is_none(), "exact rendered bound queues behind the first Send");
    crate::step(
        &mut domain,
        &env,
        Input::Parent(parent::Event::Message {
            finish_requested: false,
            agent: owner,
            name: Token::new(11),
            label: Box::from(*b"peer"),
            text: Box::from(*b"1234567"),
        }),
        &mut out,
    );
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::MessageRefused {
            client: Token::new(1),
            name: Token::new(11),
            reason: crate::MessageRefusal::TooLarge
        }))
    );
    assert!(out.pop().is_none());
}

#[test]
fn carried_messages_are_checked_before_spawn_with_the_runs_rendered_byte_arithmetic() {
    let limits = crate::Limits { messages: 2, message_bytes: 9, ..limits() };
    let message = |name, text: &[u8]| crate::Message {
        finish_requested: false,
        name: Token::new(name),
        label: b"person".as_slice().into(),
        text: text.into(),
    };
    let batches: [Box<[crate::Message]>; 4] = [
        Box::new([message(1, b"a"), message(2, b"a"), message(3, b"a")]),
        Box::new([message(1, b"aa")]),
        Box::new([message(0, b"a"), message(0, b"a")]),
        Box::new([message(0, b"a"), message(99, b"a")]),
    ];
    for (at, messages) in batches.into_iter().enumerate() {
        let mut domain = Domain::new(&limits);
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
        let mut out = Queue::uncapped();
        let start = Start { messages, ..start() };
        crate::step(&mut domain, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start }), &mut out);
        let request = out.pop().expect("one start result");
        if at == 3 {
            let Output::Process(process::Request::Spawn { .. }) = request else {
                panic!("exact rendered byte bound accepted")
            };
        } else {
            let Output::Parent(parent::Request::Gone { end, .. }) = request else {
                panic!("atomic typed start refusal")
            };
            assert_eq!(end, End::Invalid(crate::Invalid::Messages));
            assert_eq!(domain.agents(), 0);
        }
        assert!(out.is_empty());
    }
}

fn empty_charter() -> crate::Charter {
    use smith_domain::run;

    let bytes = 0_u64;
    let value = run::Charter {
        mode: run::Mode::Finite,
        reserve: run::charter::Reserve { turns: 0, time: Duration::ZERO },
        resume: false,
        waiting: Duration::ZERO,
        instructions: Box::new([]),
        brief: run::charter::Brief { sections: Box::new([]) },
        conventions: None,
        grants: run::charter::Grants {
            wait: false,
            deliver: None,
            tools: run::charter::Tools { inspect: false, modify: false, shell: false },
            agents: false,
            host_tools: Box::new([]),
        },
        outcome: run::outcome::OutcomeSpec { change: None, verdicts: Box::new([]), report: None, failure: None },
        budget: run::Budget { turns: 0, spend: 0, time: Duration::ZERO },
        llm: run::charter::Llm {
            effort: None,
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 0 },
            dialect: 0,
            account: Some(0),
            endpoint: run::charter::Endpoint(0),
            model: Box::new([]),
            window: 0,
            output: 0,
        },
        models: Box::new([]),
    };
    crate::Charter::new(value, bytes).expect("bounded charter fixture")
}

#[test]
fn no_progress_not_above_silence_is_refused_as_watchdog_over_silence() {
    let mut limits = limits();
    limits.silence = Duration::from_secs(10);
    for no_progress in [Duration::from_secs(9), Duration::from_secs(10)] {
        limits.no_progress = no_progress;
        assert_eq!(
            crate::check(&limits),
            Err(crate::Check::WatchdogOverSilence { no_progress, silence: Duration::from_secs(10) })
        );
    }
    limits.no_progress = Duration::from_secs(11);
    assert_eq!(crate::check(&limits), Ok(()));
}

#[test]
fn exit_grace_not_above_teardown_is_refused_as_grace_over_teardown() {
    let mut limits = limits();
    limits.teardown = Duration::from_secs(40);
    for exit_grace in [Duration::from_secs(39), Duration::from_secs(40)] {
        limits.exit_grace = exit_grace;
        assert_eq!(
            crate::check(&limits),
            Err(crate::Check::GraceOverTeardown { exit_grace, teardown: Duration::from_secs(40) }),
        );
    }
    limits.exit_grace = Duration::from_secs(45);
    assert_eq!(crate::check(&limits), Ok(()));
}

#[test]
fn a_piece_counts_as_progress_for_the_watchdog() {
    let (mut domain, mut env, mut out, owner) = admitted();
    assert_eq!(domain.next_deadline(), Some(Time::from_nanos(10_000_000_000)));
    env.now = Time::from_nanos(5_000_000_000);
    crate::step(&mut domain, &env, Input::Process(process::Event::Progress { owner }), &mut out);
    assert_eq!(domain.next_deadline(), Some(Time::from_nanos(15_000_000_000)));
    assert_eq!(out.pop(), Some(Output::Process(process::Request::Read { owner, process: Token::new(2) })));
    assert_eq!(out.len(), 0);
}

#[test]
fn a_notice_for_an_account_not_lent_is_refused_as_rules() {
    for (message, admitted) in [
        (crate::Up::Unauthorized { account: 8, generation: 1 }, true),
        (crate::Up::Limited { account: 8, reset: None }, true),
        (crate::Up::Refused { account: 8, code: Box::default(), reason: Box::default() }, true),
        (crate::Up::Refused { account: 7, code: Box::default(), reason: Box::default() }, false),
    ] {
        let fault = notice_fault(message, admitted);
        assert_eq!(fault, Some(crate::Fault::Rules), "admitted: {admitted}");
    }
}

fn notice_fault(message: crate::Up, admit: bool) -> Option<crate::Fault> {
    let limits = crate::Limits { accounts: 1, ..limits() };
    let mut start = start();
    start.grants = Box::new([crate::Grant {
        account: 7,
        generation: 1,
        valid: Duration::from_secs(60),
        form: crate::Form::Credential(crate::Scheme::Bearer),
    }]);
    let mut domain = Domain::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::uncapped();
    crate::step(&mut domain, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start }), &mut out);
    let Some(Output::Process(process::Request::Spawn { owner, .. })) = out.pop() else { panic!("spawn admitted") };
    crate::step(&mut domain, &env, Input::Process(process::Event::Spawned { owner, process: Token::new(2) }), &mut out);
    while out.pop().is_some() {}
    if admit {
        crate::step(
            &mut domain,
            &env,
            Input::Process(process::Event::Received { owner, message: crate::Up::Admitted }),
            &mut out,
        );
        while out.pop().is_some() {}
    }
    crate::step(&mut domain, &env, Input::Process(process::Event::Received { owner, message }), &mut out);
    while let Some(request) = out.pop() {
        if let Output::Parent(parent::Request::Faulted { fault, .. }) = request {
            return Some(fault);
        }
    }
    None
}

#[test]
fn a_source_name_takes_the_channel_s_set() {
    use crate::SourceName;
    for (name, accepted) in [
        (b"a".as_slice(), true),
        (b"abcdefghijklmnopqrstuvwxyz01234-".as_slice(), true),
        (b"".as_slice(), false),
        (b"abcdefghijklmnopqrstuvwxyz012345-".as_slice(), false),
        (b"A".as_slice(), false),
        (b"_".as_slice(), false),
        (b" ".as_slice(), false),
    ] {
        assert_eq!(SourceName::new(name).is_some(), accepted, "{name:?}");
    }
}

#[test]
#[expect(
    clippy::disallowed_macros,
    reason = "unit tests format the public source name's Debug output (programming-model.md, section 10.2)"
)]
fn a_source_name_reads_back_its_bytes() {
    for (bytes, debug) in [
        (b"a".as_slice(), "SourceName(\"a\")"),
        (b"abcdefghijklmnopqrstuvwxyz01234-".as_slice(), "SourceName(\"abcdefghijklmnopqrstuvwxyz01234-\")"),
    ] {
        let name = crate::SourceName::new(bytes).expect("channel name");
        assert_eq!(name.as_bytes(), bytes);
        assert_eq!(format!("{name:?}"), debug);
    }
}

#[test]
fn invalid_limits_are_refused() {
    let limits = limits();
    assert!(crate::valid(&limits));
    assert!(crate::valid(&crate::Limits { charter_bytes: u64::MAX, ..limits }));
    assert!(crate::valid(&crate::Limits { path_bytes: 4096, ..limits }));
    for invalid in [
        crate::Limits { path_bytes: 4097, ..limits },
        crate::Limits { turns: 0, ..limits },
        crate::Limits { turn_bytes: 0, ..limits },
        crate::Limits { unacknowledged_bytes: limits.turn_bytes - 1, ..limits },
        crate::Limits { answer_bytes: crate::Delivered::worst_case() - 1, ..limits },
        // Leave room in the outbox so only the entry output count overflows.
        crate::Limits { calls: u32::MAX - 8, messages: 0, accounts: 0, turns: 1, ..limits },
        crate::Limits { messages: u32::MAX, ..limits },
        crate::Limits { agents: u32::MAX / 3 + 1, ..limits },
    ] {
        assert!(!crate::valid(&invalid));
    }
}
