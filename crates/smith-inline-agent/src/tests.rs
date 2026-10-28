//! Entry-point and boundary ownership cases; clocks and values are supplied.
use alloc::boxed::Box;
use skein_lib::{Duration, Env, Id, List, Queue, Time, Token, Wall};
use smith_domain::{self as smith, run};
use smith_host_domain::{self as host, parent};

use crate::{Below, Domain, Input, Limits, Lower, Output, fire, resume, step, translate};

fn bounds() -> Limits {
    let smith = smith_agent_world::LIMITS;
    Limits {
        slots: 2,
        smith,
        window: smith::Window {
            turns: 8,
            bytes: smith::max_turn_bytes(&smith).expect("root bound").checked_mul(8).expect("small window"),
        },
        wall_time: Duration::from_secs(3600),
        cancel_grace: Duration::from_secs(5),
        facts: 32,
    }
}

fn configuration() -> smith::Config {
    smith::Config {
        programs: Box::new([]),
        endpoints: Box::from([run::charter::Endpoint(0)]),
        models: Box::from([smith::ConfiguredModel {
            efforts: Box::new([]),
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            protocol_allowance: 0,
            endpoint: run::charter::Endpoint(0),
            model: b"fake-1".as_slice().into(),
            window: 65_536,
            output: 4096,
            head: Duration::from_secs(60),
            idle: Duration::from_secs(30),
        }]),
    }
}

fn start() -> host::Start {
    let settings =
        smith_agent_world::Settings { job: smith_agent_world::Job::HostTools, ..smith_agent_world::Settings::calm(1) };
    let mut charter = smith_agent_world::scripted_charter(&settings);
    charter.conventions = None;
    charter.models = Box::default();
    charter.grants.agents = false;
    charter.grants.tools = run::charter::Tools { inspect: false, modify: false, shell: false };
    host::Start {
        live: None,
        messages: Box::default(),
        logical_run: Token::new(7),
        identity: None,
        activation: 1,
        workspace: None,
        charter: host::Charter::new(charter, settings.limits.run.run_bytes).expect("root policy"),
        transcript: None,
        answered: Box::default(),
        directories: Box::default(),
        grants: Box::from([host::Grant {
            account: 0,
            generation: 1,
            valid: Duration::from_secs(7200),
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
        }]),
    }
}

fn env() -> Env<Limits> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits: bounds() }
}

#[test]
fn a_start_lending_a_program_source_is_refused_as_invalid_grants() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 1);
    let mut out = Queue::uncapped();
    let mut source = start();
    source.grants[0].form = host::Form::Program(host::SourceName::new(b"claude-code").expect("source name"));
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: source }), &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Invalid(host::Invalid::Grants),
            detail: Box::default(),
        }))
    );
    assert!(out.is_empty());
    assert_eq!(agent.hosted(), 0);
}

#[test]
fn empty_workspace_is_refused_before_a_slot_or_provider_request() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 1);
    let mut out = Queue::uncapped();
    let mut source = start();
    source.workspace = Some(Token::new(8));
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: source }), &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Invalid(host::Invalid::Directories),
            detail: Box::default()
        }))
    );
    assert!(out.is_empty());
    assert_eq!(agent.hosted(), 0);
}

#[test]
fn a_told_turn_keeps_the_roots_exact_concrete_value() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 2);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    while out.pop().is_some() {}
    step(
        &mut agent,
        &env,
        Input::Parent(parent::Event::Message {
            finish_requested: false,
            agent: handle,
            name: Token::new(42),
            label: Box::default(),
            text: b"read me".as_slice().into(),
        }),
        &mut out,
    );
    while out.pop().is_some() {}
    let turn = smith::session::record::Turn {
        model: b"model".as_slice().into(),
        effort: None,
        kind: smith_domain::session::record::Kind::First(Box::new(smith_domain::session::record::Opener {
            affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
            account: None,
            backend: smith_domain::session::record::Backend::Direct,
        })),
        version: smith::session::record::VERSION,
        endpoint: Some(smith::session::llm::Endpoint(0)),
        dialect: 1,
        sequence: 1,
        usage: smith::session::llm::Usage::ZERO,
        spent: 0,
        jobs: 0,
        messages: Box::default(),
    };
    agent.lower.push(smith::Request::Turn {
        host_run: Token::new(7),
        number: 1,
        position: 1,
        opens_window: true,
        read: Some(Token::new(42)),
        spent: run::Spend::ZERO,
        turn: turn.clone(),
    });
    translate::route(&mut agent, &env, Id::from_token(handle), &mut out);
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::Turn {
            client: Token::new(1),
            turn: host::Turn {
                opens_window: true,
                number: 1,
                spent: 0,
                read: Some(Token::new(42)),
                body: host::TurnValue::new(turn, u64::MAX).expect("concrete value")
            }
        }))
    );
    assert!(out.is_empty());
}

#[test]
fn cancel_fire_resume_terminal_and_reclaim_settle_exactly_once() {
    let mut env = env();
    env.limits.wall_time = Duration::from_secs(1);
    let mut agent = Domain::new(&env.limits, configuration(), 3);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    let mut completion_owner = None;
    while let Some(output) = out.pop() {
        if let Output::Lower { request: Lower::Complete { owner, .. }, .. } = output {
            completion_owner = Some(owner);
        }
    }
    let owner = completion_owner.expect("actual provider request");
    resume(&mut agent, &env, &mut out);
    assert!(out.is_empty());
    env.now = Time::ZERO.saturating_add(Duration::from_secs(1));
    assert!(agent.is_due(env.now));
    fire(&mut agent, &env, &mut out);
    agent.reclaim();
    for _ in 0_u32..8 {
        if !agent.is_ready() {
            break;
        }
        resume(&mut agent, &env, &mut out);
        agent.reclaim();
    }
    assert_eq!(out.pop(), Some(Output::Lower { agent: handle, request: Lower::Cancel { owner } }));
    assert!(out.is_empty());
    step(&mut agent, &env, Input::Parent(parent::Event::Stop { agent: handle }), &mut out);
    assert!(out.is_empty());
    assert_eq!(agent.hosted(), 1);
    step(&mut agent, &env, Input::Below { agent: handle, terminal: Below::Cancelled { owner } }, &mut out);
    agent.reclaim();
    for _ in 0_u32..8 {
        if !agent.is_ready() {
            break;
        }
        resume(&mut agent, &env, &mut out);
        agent.reclaim();
    }
    assert_eq!(out.pop(), Some(Output::Parent(parent::Request::LongDone { client: Token::new(1) })));
    let Some(Output::Parent(parent::Request::Answered { answer, .. })) = out.pop() else { panic!("cancel answer") };
    assert_eq!(answer.result, host::RunResult::Failed { failure: host::RunFailure::Cancelled });
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::Unread { client: Token::new(1), names: Box::default() }))
    );
    assert_eq!(
        out.pop(),
        Some(Output::Parent(parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Settled,
            detail: Box::default()
        }))
    );
    step(&mut agent, &env, Input::Below { agent: handle, terminal: Below::Cancelled { owner } }, &mut out);
    assert!(out.is_empty());
    assert!(agent.pop_fact(Token::new(1)).is_some(), "native observations survive retirement");
    agent.reclaim();
    assert_eq!(agent.hosted(), 1, "an ended slot retains undrained native observations");
    while agent.pop_fact(Token::new(1)).is_some() {}
    while agent.pop_content(Token::new(1)).is_some() {}
    agent.reclaim();
    assert_eq!(agent.hosted(), 0);
    step(&mut agent, &env, Input::Below { agent: handle, terminal: Below::Cancelled { owner } }, &mut out);
    assert!(out.is_empty());
}

#[test]
fn checked_limits_require_one_concrete_turn_and_positive_wall_time() {
    let mut limits = bounds();
    assert!(crate::valid(&limits));
    limits.window.bytes = smith::max_turn_bytes(&limits.smith).expect("turn bound") - 1;
    assert!(!crate::valid(&limits));
    limits = bounds();
    limits.wall_time = Duration::ZERO;
    assert!(!crate::valid(&limits));
}

#[test]
fn an_occupied_or_duplicate_client_is_refused_without_touching_its_live_slot() {
    let mut env = env();
    env.limits.slots = 1;
    let mut domain = Domain::new(&env.limits, configuration(), 4);
    let mut out = Queue::uncapped();
    step(&mut domain, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    while out.pop().is_some() {}
    for client in [Token::new(1), Token::new(2)] {
        step(&mut domain, &env, Input::Parent(parent::Event::Spawn { client, start: start() }), &mut out);
        assert_eq!(
            out.pop(),
            Some(Output::Parent(parent::Request::Gone { client, end: host::End::Busy, detail: Box::default() }))
        );
        assert!(out.is_empty());
        assert_eq!(domain.hosted(), 1);
    }
}

#[test]
fn a_full_parent_reply_is_a_typed_too_large_terminal() {
    let capacity = u32::try_from(run::HostAnswer::CAPACITY + 1).expect("small vocabulary cap");
    let mut body = List::with_capacity(capacity);
    for _ in 0..capacity {
        body.push(b'x').expect("fixed checked capacity");
    }
    let reply = host::Reply::Host { error: false, body: body.into_boxed() };
    assert_eq!(translate::host_reply(reply), Some(run::HostReply::TooLarge));
}

#[test]
fn a_slot_roots_undrained_facts_refuse_nothing_and_are_all_drained() {
    // T1 first refused message 2044 after stepping 2043 messages without a drain.
    const OLD_REFUSAL: u32 = 2044;
    const LIFECYCLE_FACTS: u32 = 2; // One Opened per slot.
    for messages in [OLD_REFUSAL, OLD_REFUSAL * 2] {
        let env = env();
        let mut domain = Domain::new(&env.limits, configuration(), 5);
        let mut out = Queue::uncapped();
        let mut handles = [Token::new(0); 2];
        for client in 1_u32..=2 {
            let client_token = Token::new(u64::from(client));
            step(
                &mut domain,
                &env,
                Input::Parent(parent::Event::Spawn { client: client_token, start: start() }),
                &mut out,
            );
            let Some(Output::Parent(parent::Request::Started { agent, .. })) = out.pop() else {
                panic!("actual slot handle")
            };
            handles[usize::try_from(client - 1).expect("two slots")] = agent;
            while out.pop().is_some() {}
            while domain.pop_fact(client_token).is_some() {}
        }
        // Full inline ingress refuses before entering the root. As in the old
        // test, drive A's native public entry to get actual accepted/refused
        // message facts while leaving its observations undrained.
        let native_env = Env { now: env.now, wall: env.wall, limits: env.limits.smith };
        let mut native_out = Queue::uncapped();
        for name in 1..=messages {
            let slot = domain.slots.get_mut(Id::from_token(handles[0])).expect("actual native slot");
            smith::step(
                &mut slot.root,
                &native_env,
                smith::Event::Message {
                    finish_requested: false,
                    run: slot.run.expect("admitted native run"),
                    name: Token::new(u64::from(name)),
                    label: Box::default(),
                    text: b"held".as_slice().into(),
                },
                &mut native_out,
            );
            while native_out.pop().is_some() {}
            assert_eq!(domain.backlog(), LIFECYCLE_FACTS + name, "each message is taken and produces a fact");
        }
        let independent_name = Token::new(9000);
        step(
            &mut domain,
            &env,
            Input::Parent(parent::Event::Message {
                finish_requested: false,
                agent: handles[1],
                name: independent_name,
                label: Box::default(),
                text: b"independent".as_slice().into(),
            }),
            &mut out,
        );
        assert_eq!(domain.backlog(), LIFECYCLE_FACTS + messages + 1);
        let Some(smith::Fact::Run { fact, .. }) = domain.pop_fact(Token::new(2)) else {
            panic!("the other slot still steps while A's facts await draining")
        };
        let run::facts::FactKind::MessageReceived { name, .. } = fact.kind else {
            panic!("the other slot receives its message")
        };
        assert_eq!(name, independent_name);
        assert_eq!(domain.pop_fact(Token::new(2)), None);
        let mut drained = 0;
        while let Some(fact) = domain.pop_fact(Token::new(1)) {
            drained += 1;
            let smith::Fact::Run { fact, .. } = fact else { panic!("one native message fact") };
            let (run::facts::FactKind::MessageReceived { name, .. }
            | run::facts::FactKind::MessageRefused { name, .. }) = fact.kind
            else {
                panic!("accepted or refused native message")
            };
            assert_eq!(name, Token::new(u64::from(drained)), "every message's fact drains in order");
        }
        assert_eq!(drained, messages);
        assert_eq!(domain.backlog(), LIFECYCLE_FACTS);
        for client in [Token::new(1), Token::new(2)] {
            assert_eq!(
                domain.pop_slot_fact(),
                Some(crate::Fact { at: env.now, kind: crate::FactKind::Opened { client } })
            );
        }
        assert_eq!(domain.backlog(), 0);
    }
}

#[test]
fn lifecycle_fact_estimates_do_not_restrict_valid_limits() {
    for facts in [0, 1, 2, u32::MAX] {
        let limits = Limits { facts, ..bounds() };
        assert!(crate::valid(&limits));
    }
}

#[test]
fn workspace_routes_keep_equal_tokens_in_separate_lower_families() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 91);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    while out.pop().is_some() {}
    let owner = Token::new(55);
    agent.lower.push(smith::Request::Io {
        owner,
        op: smith::tools::Op::Load {
            at: smith::tools::Place { root: Token::new(9), path: b"a".as_slice().into() },
            max: 3,
        },
        deadline: Time::from_nanos(100),
    });
    agent.lower.push(smith::Request::Read {
        owner,
        at: run::Place { root: Token::new(9), path: b"guide".as_slice().into() },
        max: 3,
        deadline: Time::from_nanos(100),
    });
    translate::route(&mut agent, &env, Id::from_token(handle), &mut out);
    let Some(Output::Lower { agent: emitted_agent, request: Lower::Io { owner: name, .. } }) = out.pop() else {
        panic!("expected Io")
    };
    assert_eq!(emitted_agent, handle);
    assert_eq!(name, owner);
    let Some(Output::Lower { agent: emitted_agent, request: Lower::Read { owner: name, .. } }) = out.pop() else {
        panic!("expected Read")
    };
    assert_eq!(emitted_agent, handle);
    assert_eq!(name, owner);
    let slot = agent.slots.get_mut(Id::from_token(handle)).expect("retained slot");
    assert!(crate::workspace::terminal(slot, Below::Probed { owner, executable: true }).is_none());
    assert!(crate::workspace::terminal(slot, Below::Done { owner, done: smith::tools::Done::Cancelled }).is_some());
    assert_eq!(slot.machine.len(), 1);
    assert!(crate::workspace::terminal(slot, Below::Done { owner, done: smith::tools::Done::Cancelled }).is_none());
    assert!(crate::workspace::terminal(slot, Below::Read { owner, read: run::Read::Missing }).is_some());
    assert!(crate::workspace::terminal(slot, Below::Read { owner, read: run::Read::Missing }).is_none());
}

#[test]
fn a_job_act_is_lowered_and_settles_with_its_done() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 91);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    while out.pop().is_some() {}
    let owner = Token::new(55);
    let act = smith::tools::JobAct::Read { head: 3, tail: 2, wait: Duration::from_secs(1) };
    agent.lower.push(smith::Request::Job { owner, act });
    translate::route(&mut agent, &env, Id::from_token(handle), &mut out);
    assert_eq!(out.pop(), Some(Output::Lower { agent: handle, request: Lower::Job { owner, act } }));
    let slot = agent.slots.get_mut(Id::from_token(handle)).expect("retained slot");
    assert_eq!(
        crate::workspace::terminal(slot, Below::Done { owner, done: smith::tools::Done::Cancelled }),
        Some(smith::Event::Done { owner, done: smith::tools::Done::Cancelled })
    );
    assert_eq!(slot.io.len(), 0);
}

#[test]
fn probe_check_and_abort_route_once_and_wait_for_the_matching_terminal() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 92);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    while out.pop().is_some() {}
    let owner = Token::new(56);
    let slot = agent.slots.get_mut(Id::from_token(handle)).expect("retained slot");
    crate::workspace::probe(
        slot,
        handle,
        owner,
        run::Place { root: Token::new(9), path: b"check".as_slice().into() },
        Time::from_nanos(100),
        &mut out,
    );
    let Some(Output::Lower { request: Lower::Probe { owner: name, .. }, .. }) = out.pop() else {
        panic!("expected Probe")
    };
    assert_eq!(name, owner);
    assert!(crate::workspace::terminal(slot, Below::Probed { owner, executable: true }).is_some());
    crate::workspace::check(
        slot,
        handle,
        owner,
        run::Place { root: Token::new(9), path: b"check".as_slice().into() },
        Time::from_nanos(100),
        3,
        &mut out,
    );
    let Some(Output::Lower { request: Lower::Check { owner: name, .. }, .. }) = out.pop() else {
        panic!("expected Check")
    };
    assert_eq!(name, owner);
    crate::workspace::abort(slot, handle, owner, &mut out);
    crate::workspace::abort(slot, handle, owner, &mut out);
    assert_eq!(out.pop(), Some(Output::Lower { agent: handle, request: Lower::Abort { owner } }));
    assert!(out.is_empty());
    assert_eq!(slot.machine.len(), 1);
    assert!(crate::workspace::terminal(slot, Below::Read { owner, read: run::Read::Missing }).is_none());
    assert!(crate::workspace::terminal(slot, Below::Aborted { owner }).is_some());
    assert!(crate::workspace::terminal(slot, Below::Aborted { owner }).is_none());
}

#[test]
fn directories_move_the_exact_io_root_into_the_run() {
    let bounds = bounds();
    let workspace = crate::workspace::start(
        Some(Token::new(10)),
        Box::from([host::Directory {
            host: Box::from(b"/host".as_slice()),
            root: Token::new(900),
            name: b"mounted".as_slice().into(),
            writable: false,
            git: true,
            conflicts: Box::from([b"conflict".as_slice().into()]),
        }]),
        &bounds,
    )
    .expect("bounded mount")
    .expect("present workspace");
    assert_eq!(workspace.directories[0].root, Token::new(900));
    assert!(!workspace.directories[0].writable);
    assert_eq!(workspace.directories[0].conflicts[0].as_ref(), b"conflict");
}

#[test]
fn a_dropped_slot_keeps_a_command_right_until_its_terminal() {
    let env = env();
    let mut agent = Domain::new(&env.limits, configuration(), 93);
    let mut out = Queue::uncapped();
    step(&mut agent, &env, Input::Parent(parent::Event::Spawn { client: Token::new(1), start: start() }), &mut out);
    let Some(Output::Parent(parent::Request::Started { agent: handle, .. })) = out.pop() else {
        panic!("Started first")
    };
    let mut completion = None;
    while let Some(output) = out.pop() {
        if let Output::Lower { request: Lower::Complete { owner, .. }, .. } = output {
            completion = Some(owner);
        }
    }
    // This unit supplies a root-boundary command fixture; the composed worlds
    // supply actual root operations and the native fake-machine settlement.
    agent.lower.push(smith::Request::Io {
        owner: Token::new(100),
        op: smith::tools::Op::Spawn {
            cwd: smith::tools::Place { root: Token::new(9), path: b".".as_slice().into() },
            command: b"sleep".as_slice().into(),
            head: 1,
            tail: 1,
            after: None,
        },
        deadline: Time::from_nanos(100),
    });
    translate::route(&mut agent, &env, Id::from_token(handle), &mut out);
    while out.pop().is_some() {}
    step(&mut agent, &env, Input::Parent(parent::Event::Drop { agent: handle }), &mut out);
    let mut faulted = false;
    let mut cancel_io = false;
    let mut provider_cancelled = false;
    while let Some(output) = out.pop() {
        match output {
            Output::Parent(parent::Request::Faulted { fault: smith_host_domain::Fault::Exited, .. }) => faulted = true,
            Output::Parent(parent::Request::Answered { .. }) => panic!("dropped root never answers"),
            Output::Lower { request: Lower::Cancel { .. }, .. } => provider_cancelled = true,
            Output::Lower { request: Lower::CancelIo { owner }, .. } => {
                assert_eq!(owner, Token::new(100));
                cancel_io = true;
            }
            Output::Parent(parent::Request::Gone { .. }) => panic!("command terminal is still owed"),
            Output::Parent(_) | Output::Lower { .. } => {}
        }
    }
    assert!(faulted);
    assert!(cancel_io);
    assert!(provider_cancelled);
    step(
        &mut agent,
        &env,
        Input::Below { agent: handle, terminal: Below::Cancelled { owner: completion.expect("actual Complete") } },
        &mut out,
    );
    assert!(out.is_empty(), "the command still owns its terminal");
    assert_eq!(agent.hosted(), 1);
    // A dropped root hears nothing, so this boundary fixture needs no native tools job.
    // Actual composed cancellation and answer settlement stay in the host worlds.
    step(
        &mut agent,
        &env,
        Input::Below {
            agent: handle,
            terminal: Below::Done { owner: Token::new(100), done: smith::tools::Done::Cancelled },
        },
        &mut out,
    );
    let Some(Output::Parent(parent::Request::Gone { .. })) = out.pop() else { panic!("command terminal settles Gone") };
    assert!(out.is_empty());
    step(
        &mut agent,
        &env,
        Input::Below {
            agent: handle,
            terminal: Below::Done { owner: Token::new(100), done: smith::tools::Done::Cancelled },
        },
        &mut out,
    );
    assert!(out.is_empty());
}

#[test]
fn the_inline_answer_measures_the_exhausted_accounts_wait() {
    for (reset_at, now, expected) in [
        (Some(Time::from_nanos(100_000_000)), Time::from_nanos(20_000_000), Some(Duration::from_nanos(80_000_000))),
        (Some(Time::from_nanos(100_000_000)), Time::from_nanos(60_000_000), Some(Duration::from_nanos(40_000_000))),
        (Some(Time::ZERO), Time::from_nanos(20_000_000), Some(Duration::ZERO)),
        (None, Time::from_nanos(20_000_000), None),
    ] {
        let answer = run::Answer::Failed {
            failure: run::Failure::Model(run::Fault::Limited { account: 23, reset_at }),
            spent: run::Spend::ZERO,
            turns: 0,
        };
        let translated = translate::answer(answer, None, now);
        assert_eq!(
            translated.result,
            host::RunResult::Failed {
                failure: host::RunFailure::Model(host::ModelFault::Limited { account: 23, reset: expected }),
            }
        );
    }
}

#[test]
fn grant_forms_keep_their_policy_at_start_and_refresh() {
    for (form, renewable, admitted) in [
        (host::Form::Credential(host::Scheme::Key), false, true),
        (host::Form::Credential(host::Scheme::Bearer), true, true),
        (host::Form::Withdrawn, true, true),
        (host::Form::Program(host::SourceName::new(b"claude-code").expect("source name")), true, false),
    ] {
        let mut source = start();
        source.grants[0].form = form;
        let refresh = translate::grant(source.grants[0]);
        assert_eq!(refresh.renewable, renewable, "refresh {form:?}");
        assert_eq!(refresh.name, smith::GrantName { account: 0, generation: 1 });
        assert_eq!(refresh.valid, source.grants[0].valid);
        let decoded = translate::start(Token::new(1), source, &bounds());
        if admitted {
            let smith::Event::Start { grants, .. } = decoded.expect("existing form admitted") else {
                panic!("expected root Start");
            };
            assert_eq!(grants.as_ref(), &[refresh], "start {form:?}");
        } else {
            assert_eq!(decoded, Err(host::Invalid::Grants), "program admission remains restricted");
        }
    }
}

#[test]
fn invalid_limits_are_refused() {
    let limits = bounds();
    assert!(crate::valid(&limits));
    let mut invalid = limits;
    invalid.window.turns = 0;
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.window.bytes = smith::max_turn_bytes(&limits.smith).expect("turn bound") - 1;
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.wall_time = Duration::ZERO;
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.smith.session.history_bytes = u64::MAX;
    assert_eq!(smith::max_turn_bytes(&invalid.smith), None);
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.smith.run.offer_messages = 0;
    assert!(!crate::valid(&invalid));
    // A heap total can overflow while all constructor and edge bounds fit.
    let mut large = limits;
    large.smith.configured_model_bytes = u64::MAX;
    assert!(crate::valid(&large));
    // The root/child guards also reject these tighter capacity failures.
    invalid = limits;
    invalid.smith.session.tools.kits = u32::MAX;
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.smith.run.calls = u32::MAX;
    assert!(!crate::valid(&invalid));
    invalid = limits;
    invalid.smith.run.conversations = u32::MAX;
    assert!(!crate::valid(&invalid));
}
