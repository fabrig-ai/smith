#![expect(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    reason = "unit harnesses are ordinary Rust (programming-model.md, section 10.2)"
)]

use alloc::boxed::Box;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use smith_domain::run::{self, charter::Endpoint, outcome};

use crate::{Config, Contract, Domain, Event, Invalid, Limits, Request, charter};

fn idle_chat(seed: u64) -> (Domain, Env<Limits>, Queue<Request>) {
    let limits = limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut domain = Domain::new(config(), &limits, seed).unwrap();
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    crate::resume(&mut domain, &env, &mut out);
    drop(drain_requests(&mut out));
    crate::step(&mut domain, &env, Event::Loaded { state: None, transcript: None, deliveries: Box::new([]) }, &mut out);
    (domain, env, out)
}

#[test]
fn the_next_charter_keeps_the_startup_sub_agent_models_and_accounts() {
    let limits = limits();
    let mut configured = config();
    let first = configured.models[0].clone();
    let mut second = first.clone();
    second.model = b"other".as_slice().into();
    configured.models = Box::new([first, second]);
    configured.efforts = Box::new([Box::new([]), Box::new([])]);
    configured.progress = Box::new([configured.progress[0], configured.progress[0]]);
    configured.protocol_allowances = Box::new([0, 0]);
    configured.sub_agents = true;
    let mut domain = Domain::new(configured, &limits, 11).unwrap();
    let before = domain.charter().0;
    domain.main_model = 1;
    let after = domain.charter().0;
    assert_eq!(after.llm.model.as_ref(), b"other");
    assert_eq!(after.models, before.models);
    assert_eq!(domain.config.accounts.as_ref(), &[7]);
    assert_eq!(domain.config.models[0].model.as_ref(), b"small");
}

#[test]
fn the_next_charters_effort_leaves_the_startup_inventory_unchanged() {
    let (mut domain, _, _) = idle_chat(11);
    domain.effort = Some(b"high".as_slice().into());
    assert_eq!(domain.charter().0.llm.effort.as_deref(), Some(b"high".as_slice()));
    assert!(domain.config.models[0].effort.is_none());
}

#[test]
fn a_refused_selection_keeps_the_current_chat_identity() {
    let (mut domain, env, mut out) = idle_chat(11);
    let identity = domain.chat.state.identity;
    crate::step(&mut domain, &env, Event::SelectChat { name: b"absent".as_slice().into(), create: false }, &mut out);
    let Some(Request::SelectChat { create: false, .. }) = out.pop() else { panic!("typed terminal") };
    crate::resume(&mut domain, &env, &mut out);
    assert!(out.is_empty(), "selection owns the next store terminal");
    crate::step(
        &mut domain,
        &env,
        Event::ChatSelected { name: b"absent".as_slice().into(), result: Err(crate::ChatRefusal::Missing) },
        &mut out,
    );
    let Some(Request::Notice { notice: crate::Notice::ChatRefused { reason: crate::ChatRefusal::Missing, .. } }) =
        out.pop()
    else {
        panic!("typed terminal")
    };
    assert_eq!(domain.chat.state.identity, identity);
}

#[test]
fn selecting_an_idle_chat_loads_its_saved_identity_after_a_fresh_one() {
    let (mut domain, env, mut out) = idle_chat(11);
    let identity = domain.chat.state.identity;
    crate::step(&mut domain, &env, Event::SelectChat { name: b"new".as_slice().into(), create: true }, &mut out);
    let Some(Request::SelectChat { create: true, .. }) = out.pop() else { panic!("typed terminal") };
    crate::step(&mut domain, &env, Event::ChatSelected { name: b"new".as_slice().into(), result: Ok(()) }, &mut out);
    let Some(Request::Load) = out.pop() else { panic!("typed terminal") };
    assert_ne!(domain.chat.state.identity, identity);
    assert_eq!(domain.config.chat.as_ref(), b"new");
    let saved = crate::ChatState { identity, activation: 7, next_message: 9, read: None };
    crate::step(
        &mut domain,
        &env,
        Event::Loaded { state: Some(saved), transcript: None, deliveries: Box::new([]) },
        &mut out,
    );
    assert_eq!(domain.chat.state, saved);
}

#[test]
fn selection_refuses_while_loading() {
    let limits = limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    let mut domain = Domain::new(config(), &limits, 11).unwrap();
    crate::step(&mut domain, &env, Event::SelectChat { name: b"new".as_slice().into(), create: true }, &mut out);
    let Some(Request::Notice { notice: crate::Notice::ChatRefused { reason: crate::ChatRefusal::Busy, .. } }) =
        out.pop()
    else {
        panic!("typed terminal")
    };
    assert!(out.is_empty());
}

#[test]
fn selection_refuses_names_beyond_the_local_bound() {
    let (mut domain, env, mut out) = idle_chat(11);
    crate::step(&mut domain, &env, Event::SelectChat { name: Box::from([b'x'; 65]), create: false }, &mut out);
    let Some(Request::Notice { notice: crate::Notice::ChatRefused { reason: crate::ChatRefusal::Name, .. } }) =
        out.pop()
    else {
        panic!("typed terminal")
    };
    assert!(out.is_empty());
}

#[test]
fn selection_stops_before_seed_exhaustion() {
    let (mut domain, env, mut out) = idle_chat(u64::MAX);
    crate::step(&mut domain, &env, Event::SelectChat { name: b"new".as_slice().into(), create: true }, &mut out);
    let Some(Request::Notice { notice: crate::Notice::Failure(crate::Failure::NamesExhausted) }) = out.pop() else {
        panic!("typed terminal")
    };
    let Some(Request::Exit { .. }) = out.pop() else { panic!("typed terminal") };
    assert!(out.is_empty(), "no store selection follows exhausted identity names");
}

#[test]
fn a_refused_selection_after_input_closes_finishes_the_idle_chat() {
    let (mut domain, env, mut out) = idle_chat(11);
    crate::step(&mut domain, &env, Event::SelectChat { name: b"absent".as_slice().into(), create: false }, &mut out);
    drop(drain_requests(&mut out));
    crate::step(&mut domain, &env, Event::Closed, &mut out);
    assert!(out.is_empty());
    crate::step(
        &mut domain,
        &env,
        Event::ChatSelected { name: b"absent".as_slice().into(), result: Err(crate::ChatRefusal::Missing) },
        &mut out,
    );
    let Some(Request::Notice { notice: crate::Notice::ChatRefused { .. } }) = out.pop() else {
        panic!("named refusal")
    };
    let Some(Request::Exit { status: crate::ExitStatus::Accepted }) = out.pop() else { panic!("closed idle chat") };
    assert!(out.is_empty());
}

pub(super) fn limits() -> Limits {
    Limits {
        smith: smith_agent_world::LIMITS,
        agent: crate::AgentLimits::inline(
            smith_agent_world::LIMITS,
            2,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .expect("bounded inline child"),
        endpoints: Box::new([Endpoint(4)]),
        chat_bytes: 64,
        text_bytes: 1024,
        models: 2,
        line_bytes: 256,
        show_bytes: 1024,
        unsaved: 2,
        facts: 16,
    }
}

pub(super) fn config() -> Config {
    Config {
        reserve: run::charter::Reserve { turns: 0, time: Duration::ZERO },
        chat: Box::from(&b"main"[..]),
        instructions: Box::from(&b"Assist the person"[..]),
        brief: run::charter::Brief {
            sections: Box::new([run::charter::Section {
                title: Box::from(&b"Context"[..]),
                text: Box::from(&b"Work carefully"[..]),
            }]),
        },
        protocol_allowances: Box::new([0]),
        progress: Box::new([smith_domain::session::Progress {
            head: Duration::from_secs(60),
            idle: Duration::from_secs(30),
        }]),
        efforts: Box::new([Box::new([])]),
        models: Box::new([run::charter::Llm {
            effort: None,
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            dialect: 1,
            account: Some(7),
            endpoint: Endpoint(4),
            model: Box::from(&b"small"[..]),
            window: 8192,
            output: 128,
        }]),
        sub_agents: false,
        budget: run::Budget { turns: 8, spend: 1, time: Duration::from_secs(60) },
        conventions: Some(run::Conventions { guide: Box::from(&b"GUIDE.md"[..]), checks: Box::from(&b"check"[..]) }),
        contract: Contract::Report(outcome::TextSpec { max: 256, fields: Box::new([]) }),
        deliver: None,
        title_field: Box::from(&b"title"[..]),
        live_text: false,
        mode: crate::Mode::Chat { waiting: Duration::from_secs(30) },
        resume: true,
        accounts: Box::new([7]),
        workspace: None,
        push: None,
    }
}

#[test]
fn the_local_inline_child_must_have_one_slot_and_matching_native_receiving_bounds() {
    for extra_slot in [false, true] {
        let mut limits = limits();
        let crate::AgentLimits::Inline(child) = &mut limits.agent else { panic!("inline fixture") };
        if extra_slot {
            child.slots = 2;
        } else {
            child.smith.accounts = child.smith.accounts.checked_add(1).expect("small receiving profile");
        }
        let Err(Invalid::Limits) = Domain::new(config(), &limits, 97) else {
            panic!("incompatible inline child refuses startup")
        };
    }
}

#[test]
fn report_charter_copies_every_local_choice_without_workspace_tools() {
    let config = config();
    let limits = limits();
    assert_eq!(config.validate(&limits), Ok(()));
    let charter = charter(&config);
    assert!(charter.resume, "configured resume is retained");
    assert_eq!(charter.waiting, config.mode.waiting());
    assert_eq!(charter.instructions, config.instructions);
    assert_eq!(charter.brief.sections, config.brief.sections);
    assert_eq!(charter.conventions, config.conventions);
    assert_eq!(charter.budget, config.budget);
    assert_eq!(charter.llm, config.models[0]);
    assert!(charter.models.is_empty(), "only the first model is the main model");
    assert!(charter.outcome.report.is_some(), "report is selected");
    assert!(charter.outcome.change.is_none(), "change was not selected");
    assert!(charter.grants.wait, "local chat may wait for the person");
    assert!(!charter.grants.tools.inspect && !charter.grants.tools.modify && !charter.grants.tools.shell);
    assert!(!charter.grants.agents, "sub-agent authority is not configured");
    assert!(charter.grants.host_tools.is_empty(), "local host has no tools");
    assert!(Domain::new(config, &limits, 11).is_ok(), "validated local domain builds its agent");
}

#[test]
fn change_and_second_model_map_to_distinct_charter_choices() {
    let mut config = config();
    let mut second = config.models[0].clone();
    second.model = Box::from(&b"large"[..]);
    config.models = Box::new([config.models[0].clone(), second.clone()]);
    config.efforts = Box::new([Box::new([]), Box::new([])]);
    config.contract = Contract::Change(outcome::ChangeSpec { checks_must_pass: true, fields: Box::new([]) });
    config.sub_agents = true;
    config.resume = false;
    config.mode = crate::Mode::Chat { waiting: Duration::ZERO };
    let charter = charter(&config);
    assert_eq!(charter.models.as_ref(), &[second]);
    assert!(charter.grants.agents, "the configured grant permits native sub-agent calls");
    assert!(!charter.resume, "fresh run was configured");
    assert_eq!(charter.waiting, Duration::ZERO);
    assert!(charter.outcome.report.is_none(), "report was not selected");
    assert!(charter.outcome.change.is_some(), "change was selected");
}

#[test]
fn invalid_local_configuration_is_refused_before_agent_construction() {
    let limits = limits();
    let mut invalid = config();
    invalid.chat = Box::new([]);
    assert_eq!(invalid.validate(&limits), Err(Invalid::Chat));
    assert_eq!(Domain::new(invalid, &limits, 1).err(), Some(Invalid::Chat));

    let mut invalid = config();
    invalid.instructions = Box::from([b'x'; 1025]);
    assert_eq!(invalid.validate(&limits), Err(Invalid::Text));

    let mut invalid = config();
    invalid.models = Box::new([]);
    assert_eq!(invalid.validate(&limits), Err(Invalid::Models));

    let mut invalid = config();
    invalid.accounts = Box::new([7, 7]);
    assert_eq!(invalid.validate(&limits), Err(Invalid::Accounts));

    let mut invalid = config();
    invalid.models[0].endpoint = Endpoint(99);
    assert_eq!(invalid.validate(&limits), Err(Invalid::Endpoint));
}

#[test]
fn budget_and_rate_refusals_precede_agent_construction() {
    let limits = limits();
    let ceiling = limits.smith.run.budget;
    for (budget, reason) in [
        (run::Budget { turns: ceiling.turns + 1, ..config().budget }, run::Ceiling::Turns(ceiling.turns)),
        (run::Budget { spend: ceiling.spend + 1, ..config().budget }, run::Ceiling::Spend(ceiling.spend)),
        (
            run::Budget {
                time: ceiling.time.checked_add(Duration::from_secs(1)).expect("bounded time"),
                ..config().budget
            },
            run::Ceiling::Time(ceiling.time),
        ),
    ] {
        let mut invalid = config();
        invalid.budget = budget;
        assert_eq!(Domain::new(invalid, &limits, 1).err(), Some(Invalid::Budget(run::BudgetRefusal::Ceiling(reason))));
    }
    for rate in [run::Rate::Input, run::Rate::Cached, run::Rate::CacheWrite, run::Rate::Output] {
        let mut invalid = config();
        match rate {
            run::Rate::Input => invalid.models[0].prices.input = 0,
            run::Rate::Cached => invalid.models[0].prices.cached = 0,
            run::Rate::Output => invalid.models[0].prices.output = 0,
            run::Rate::CacheWrite => invalid.models[0].prices.cache_write = Some(0),
        }
        assert_eq!(Domain::new(invalid, &limits, 1).err(), Some(Invalid::Price(rate)));
    }
}

#[test]
#[expect(
    clippy::wildcard_enum_match_arm,
    clippy::match_like_matches_macro,
    reason = "unit follows one actual process lifecycle and durable ACK through Gone"
)]
fn spawned_agent_receives_saved_start_message_and_durable_turn_ack() {
    use smith_host_domain::{self as host, process};
    let (mut domain, env, mut out, owner) = spawned_chat_with_saved_hello();
    let turn = smith_domain::Turn {
        model: b"model".as_slice().into(),
        effort: None,
        kind: smith_domain::session::record::Kind::First(Box::new(smith_domain::session::record::Opener {
            affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
            account: None,
            backend: smith_domain::session::record::Backend::Direct,
        })),
        version: smith_domain::session::record::VERSION,
        endpoint: Some(smith_domain::session::llm::Endpoint(4)),
        dialect: 1,
        sequence: 1,
        usage: smith_domain::session::llm::Usage::ZERO,
        spent: 0,
        jobs: 0,
        messages: Box::default(),
    };
    crate::step(
        &mut domain,
        &env,
        Event::Process(process::Event::Received {
            owner,
            message: host::Up::Turn {
                turn: host::Turn {
                    opens_window: true,
                    number: 1,
                    read: Some(Token::new(1)),
                    spent: 0,
                    body: host::TurnValue::new(turn, host_limits().turn_bytes).expect("sealed concrete turn"),
                },
            },
        }),
        &mut out,
    );
    let outputs = drain_requests(&mut out);
    assert!(outputs.iter().any(|request| match request {
        Request::SaveTurn { number: 1, read: Some(name), .. } if *name == Token::new(1) => true,
        _ => false,
    }));
    assert!(!outputs.iter().any(|request| match request {
        Request::Process(process::Request::Send { message: host::Down::Acknowledge { .. }, .. }) => true,
        _ => false,
    }));
    crate::step(&mut domain, &env, Event::TurnSaved { number: 1 }, &mut out);
    let outputs = drain_requests(&mut out);
    assert!(outputs.iter().any(|request| match request {
        Request::Process(process::Request::Send { message: host::Down::Acknowledge { turn: 1 }, .. }) => true,
        _ => false,
    }));
    crate::step(&mut domain, &env, Event::Process(process::Event::Sent { owner }), &mut out);
    drain_requests(&mut out);
    crate::step(
        &mut domain,
        &env,
        Event::Process(process::Event::Received {
            owner,
            message: host::Up::Answer {
                answer: host::Answer { read: Some(Token::new(1)), turns: 1, spent: 0, result: host::RunResult::Parked },
            },
        }),
        &mut out,
    );
    let outputs = drain_requests(&mut out);
    assert!(
        !outputs.iter().any(|request| match request {
            Request::Show { text } if text.as_ref() == b"Chat parked" => true,
            _ => false,
        }),
        "answer waits for process exit and empty tree"
    );
    for request in outputs {
        if let Request::Process(process::Request::Signal { owner, .. }) = request {
            crate::step(&mut domain, &env, Event::Process(process::Event::Signalled { owner }), &mut out);
        }
    }
    drain_requests(&mut out);
    crate::step(&mut domain, &env, Event::Process(process::Event::Hangup { owner }), &mut out);
    crate::step(
        &mut domain,
        &env,
        Event::Process(process::Event::Exited { owner, exit: Some(smith_host_domain::Exit::Code(0)) }),
        &mut out,
    );
    crate::step(&mut domain, &env, Event::Process(process::Event::Reaped { owner, detail: Box::default() }), &mut out);
    let outputs = drain_requests(&mut out);
    assert!(outputs.iter().any(|request| match request {
        Request::Show { text } if text.as_ref() == b"Chat parked" => true,
        _ => false,
    }));
    assert!(outputs.iter().any(|request| match request {
        Request::Load => true,
        _ => false,
    }));
}

#[expect(
    clippy::wildcard_enum_match_arm,
    clippy::match_like_matches_macro,
    reason = "unit fixture validates selected process effects"
)]
pub(super) fn spawned_chat_with_saved_hello() -> (Domain, Env<Limits>, Queue<Request>, Token) {
    use smith_host_domain::{self as host, process};
    let mut limits = limits();
    let mut child_limits = host_limits();
    child_limits.turns = 9;
    limits.agent = crate::AgentLimits::Spawned(child_limits);
    let env = Env { now: Time::ZERO, wall: Wall::from_nanos(0), limits: limits.clone() };
    let mut domain = Domain::new(config(), &limits, 11).expect("spawned local domain");
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    crate::resume(&mut domain, &env, &mut out);
    assert!(match out.pop() {
        Some(Request::Load) => true,
        _ => false,
    });
    crate::step(
        &mut domain,
        &env,
        Event::Loaded { state: None, transcript: None, deliveries: Box::default() },
        &mut out,
    );
    crate::step(&mut domain, &env, Event::Line { text: Box::from(&b"hello"[..]) }, &mut out);
    assert!(match out.pop() {
        Some(Request::Credential { account: 7 }) => true,
        _ => false,
    });
    let Some(Request::SaveState { state, fresh: false }) = out.pop() else { panic!("activation saved") };
    assert_eq!(state.activation, 1);
    crate::step(
        &mut domain,
        &env,
        Event::Credential {
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
            grant: smith_domain::Grant {
                name: smith_domain::GrantName { account: 7, generation: 1 },
                renewable: true,
                valid: Duration::from_secs(60),
            },
        },
        &mut out,
    );
    crate::step(&mut domain, &env, Event::StateSaved, &mut out);
    let Some(Request::Process(process::Request::Spawn { owner, .. })) = out.pop() else { panic!("owned child spawn") };
    crate::step(&mut domain, &env, Event::Process(process::Event::Spawned { owner, process: Token::new(3) }), &mut out);
    let Some(Request::Process(process::Request::Send { message: host::Down::Start { start, window }, .. })) = out.pop()
    else {
        panic!("typed start first")
    };
    assert_eq!(window.turns, limits.unsaved, "advertised credit fits the durable save queue");
    assert_eq!(start.activation, 1);
    assert_eq!(start.messages[0].name, Token::new(1));
    assert_eq!(start.messages[0].label.as_ref(), b"person");
    assert_eq!(start.messages[0].text.as_ref(), b"hello");
    assert_eq!(start.grants.len(), 1);
    assert_eq!(start.grants[0].form, smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer));
    assert_eq!(start.charter.into_value().instructions.as_ref(), b"Assist the person");
    assert!(match out.pop() {
        Some(Request::Process(process::Request::Wait { .. })) => true,
        _ => false,
    });
    assert!(match out.pop() {
        Some(Request::Process(process::Request::Reap { .. })) => true,
        _ => false,
    });
    crate::step(&mut domain, &env, Event::Process(process::Event::Sent { owner }), &mut out);
    assert!(match out.pop() {
        Some(Request::Process(process::Request::Read { .. })) => true,
        _ => false,
    });
    crate::step(
        &mut domain,
        &env,
        Event::Process(process::Event::Received { owner, message: host::Up::Admitted }),
        &mut out,
    );
    let outputs = drain_requests(&mut out);
    assert!(!outputs.iter().any(|request| match request {
        Request::Process(process::Request::Send { message: host::Down::Message { .. }, .. }) => true,
        _ => false,
    }));
    (domain, env, out, owner)
}

pub(super) fn drain_requests(out: &mut Queue<Request>) -> Vec<Request> {
    let mut requests = Vec::new();
    while let Some(request) = out.pop() {
        requests.push(request);
    }
    requests
}

pub(super) fn host_limits() -> smith_host_domain::Limits {
    smith_host_domain::Limits {
        agents: 1,
        directories: 4,
        conflicts: 16,
        path_bytes: 1024,
        name_bytes: 256,
        accounts: 8,
        charter_bytes: 65_536,
        transcript_bytes: 1_048_576,
        answered_bytes: 1_048_576,
        message_bytes: 4096,
        messages: 8,
        calls: 8,
        call_bytes: 4096,
        answer_bytes: smith_host_domain::Delivered::worst_case(),
        turns: 2,
        turn_bytes: 1_048_576,
        unacknowledged_bytes: 2_097_152,
        fact_bytes: 512,
        outcome_bytes: 4096,
        detail_bytes: 512,
        spawn_timeout: Duration::from_secs(5),
        no_progress: Duration::from_secs(60),
        silence: Duration::ZERO,
        long_span: Duration::from_secs(3600),
        long_open: 64,
        wall_time: Duration::from_secs(3600),
        cancel_grace: Duration::from_secs(5),
        exit_grace: Duration::from_secs(5),
        teardown: Duration::ZERO,
        kill_after: Duration::from_secs(1),
        facts: 64,
    }
}

#[test]
fn invalid_or_unowned_model_declarations_refuse_local_startup() {
    let limits = limits();
    for output in [false, true] {
        let mut invalid = config();
        if output {
            invalid.models[0].output = 0;
        } else {
            invalid.models[0].window = 0;
        }
        assert_eq!(invalid.validate(&limits), Err(Invalid::Models));
    }
    let config = config();
    let limits = Limits { smith: smith_domain::Limits { configured_model_bytes: 0, ..limits.smith }, ..limits };
    assert_eq!(config.validate(&limits), Err(Invalid::Limits));
}

#[test]
fn impossible_completion_or_reserve_spend_refuses_before_local_agent_construction() {
    let limits = limits();
    for (spend, reason) in [
        (4_127, run::BudgetRefusal::Completion { most: 4_128 }),
        (8_256, run::BudgetRefusal::Reserve(run::Part::Spend)),
    ] {
        let mut impossible = config();
        impossible.models[0].prices.unit = 1;
        impossible.models[0].window = 4000;
        impossible.budget.spend = spend;
        impossible.reserve = run::charter::Reserve { turns: 2, time: Duration::from_secs(1) };
        assert_eq!(Domain::new(impossible, &limits, 71).err(), Some(Invalid::Budget(reason)));
    }
}

#[test]
fn a_one_turn_configuration_keeps_scalar_and_completion_startup_checks() {
    let limits = limits();
    let mut configured = config();
    configured.models[0].prices.unit = 1;
    configured.models[0].window = 4000;
    configured.budget.turns = 1;
    configured.reserve = run::charter::Reserve { turns: 2, time: Duration::from_secs(1) };
    configured.budget.spend = 4_127;
    assert_eq!(configured.validate(&limits), Err(Invalid::Budget(run::BudgetRefusal::Completion { most: 4_128 })));
    configured.budget.spend = 4_128;
    assert_eq!(configured.validate(&limits), Ok(()), "the real run still refuses the finishing turns");
    configured.budget.turns = 0;
    assert_eq!(configured.validate(&limits), Err(Invalid::Budget(run::BudgetRefusal::Empty(run::Part::Turns))));
    configured.budget.turns = limits.smith.run.budget.turns + 1;
    assert_eq!(
        configured.validate(&limits),
        Err(Invalid::Budget(run::BudgetRefusal::Ceiling(run::Ceiling::Turns(limits.smith.run.budget.turns))))
    );
}

pub(super) fn saved_window_turn() -> smith_domain::Turn {
    smith_domain::Turn {
        model: b"model".as_slice().into(),
        effort: None,
        kind: smith_domain::session::record::Kind::First(Box::new(smith_domain::session::record::Opener {
            affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
            account: None,
            backend: smith_domain::session::record::Backend::Direct,
        })),
        version: smith_domain::session::record::VERSION,
        endpoint: Some(smith_domain::session::llm::Endpoint(4)),
        dialect: 1,
        sequence: 1,
        usage: smith_domain::session::llm::Usage::ZERO,
        spent: 0,
        jobs: 0,
        messages: Box::default(),
    }
}

#[test]
fn a_told_turns_window_flag_reaches_its_save() {
    use smith_host_domain::{self as host, process};
    for opens_window in [false, true] {
        let (mut domain, env, mut out, owner) = spawned_chat_with_saved_hello();
        let turn = saved_window_turn();
        crate::step(
            &mut domain,
            &env,
            Event::Process(process::Event::Received {
                owner,
                message: host::Up::Turn {
                    turn: host::Turn {
                        opens_window,
                        number: 1,
                        read: None,
                        spent: 0,
                        body: host::TurnValue::new(turn, host_limits().turn_bytes).expect("sealed turn"),
                    },
                },
            }),
            &mut out,
        );
        let outputs = drain_requests(&mut out);
        assert!(outputs.iter().any(|request| {
            if let Request::SaveTurn { opens_window: saved, .. } = request { *saved == opens_window } else { false }
        }));
    }
}

#[test]
fn an_inline_runs_facts_wait_for_the_service() {
    let limits = limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut domain = Domain::new(config(), &limits, 11).expect("inline chat");
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    crate::resume(&mut domain, &env, &mut out);
    drop(drain_requests(&mut out));
    let loaded = Event::Loaded { state: None, transcript: None, deliveries: Box::default() };
    crate::step(&mut domain, &env, loaded, &mut out);
    crate::step(&mut domain, &env, Event::Line { text: b"hello".as_slice().into() }, &mut out);
    crate::step(
        &mut domain,
        &env,
        Event::Credential {
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
            grant: smith_domain::Grant {
                name: smith_domain::GrantName { account: 7, generation: 1 },
                renewable: true,
                valid: Duration::from_secs(60),
            },
        },
        &mut out,
    );
    crate::step(&mut domain, &env, Event::StateSaved, &mut out);
    for _ in 0_u32..4 {
        drop(drain_requests(&mut out));
        crate::resume(&mut domain, &env, &mut out);
    }
    let backlog = domain.backlog();
    let mut facts = 0_u32;
    while domain.pop_run_fact().is_some() {
        facts += 1;
    }
    assert!(facts > 0, "ordinary local entrances retained the native run facts");
    assert_eq!(backlog, facts, "every held native fact contributes to the local backlog");
    assert_eq!(domain.backlog(), 0);
    assert!(domain.pop_run_fact().is_none());
}

#[test]
fn slash_effort_with_no_declared_names_accepts_a_bounded_name() {
    let (mut domain, env, mut out) = idle_chat(11);
    crate::step(&mut domain, &env, Event::Slash(crate::Slash::Effort { level: b"future".as_slice().into() }), &mut out);
    assert_eq!(domain.charter().0.llm.effort.as_deref(), Some(b"future".as_slice()));
}

#[test]
fn slash_effort_refuses_a_name_outside_the_declared_list_in_words() {
    let (mut domain, env, mut out) = idle_chat(11);
    domain.config.efforts[0] = Box::new([b"low".as_slice().into(), b"high".as_slice().into()]);
    crate::step(
        &mut domain,
        &env,
        Event::Slash(crate::Slash::Effort { level: b"mystery".as_slice().into() }),
        &mut out,
    );
    assert!(domain.charter().0.llm.effort.is_none());
    let outputs = drain_requests(&mut out);
    let text = outputs
        .iter()
        .find_map(|request| if let Request::Show { text } = request { Some(text) } else { None })
        .expect("refusal words");
    assert!(text.as_ref() == b"Effort mystery is not declared; accepted: low, high");
}

#[test]
fn slash_effort_without_a_list_still_refuses_more_than_32_bytes() {
    let (mut domain, env, mut out) = idle_chat(11);
    crate::step(&mut domain, &env, Event::Slash(crate::Slash::Effort { level: Box::new([b'x'; 33]) }), &mut out);
    assert!(domain.charter().0.llm.effort.is_none());
    let outputs = drain_requests(&mut out);
    let text = outputs
        .iter()
        .find_map(|request| if let Request::Show { text } = request { Some(text) } else { None })
        .expect("refusal words");
    assert!(text.ends_with(b"any effort within 32 bytes"));
}

#[test]
fn slash_effort_waits_for_the_live_activation_to_end() {
    let (mut domain, env, mut out) = idle_chat(11);
    domain.chat.phase = crate::chat::Phase::Running;
    crate::step(&mut domain, &env, Event::Slash(crate::Slash::Effort { level: b"high".as_slice().into() }), &mut out);
    assert!(domain.charter().0.llm.effort.is_none());
    domain.chat.phase = crate::chat::Phase::Idle;
    crate::choices::apply(&mut domain, &env, &mut out);
    assert_eq!(domain.charter().0.llm.effort.as_deref(), Some(b"high".as_slice()));
}

#[test]
fn slash_model_on_another_endpoint_names_the_kept_chats_invocation() {
    let (mut domain, env, mut out) = idle_chat(11);
    let identity = domain.chat.state.identity;
    crate::step(
        &mut domain,
        &env,
        Event::Slash(crate::Slash::Model { model: b"elsewhere/model".as_slice().into(), selected: None }),
        &mut out,
    );
    assert_eq!(domain.chat.state.identity, identity);
    let outputs = drain_requests(&mut out);
    let text = outputs
        .iter()
        .find_map(|request| if let Request::Show { text } = request { Some(text) } else { None })
        .expect("refusal words");
    assert!(text.as_ref() == b"Model elsewhere/model needs another invocation: smith --chat main -m elsewhere/model");
}

#[test]
fn slash_model_selects_a_startup_model_only_at_idle() {
    let limits = limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut configured = config();
    let first = configured.models[0].clone();
    let mut other = first.clone();
    other.model = b"other".as_slice().into();
    configured.models = Box::new([first, other]);
    configured.efforts = Box::new([Box::new([]), Box::new([])]);
    configured.progress = Box::new([configured.progress[0], configured.progress[0]]);
    configured.protocol_allowances = Box::new([0, 0]);
    let mut domain = Domain::new(configured, &limits, 11).unwrap();
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    domain.chat.phase = crate::chat::Phase::Running;
    crate::step(
        &mut domain,
        &env,
        Event::Slash(crate::Slash::Model { model: b"endpoint/other".as_slice().into(), selected: Some(1) }),
        &mut out,
    );
    assert_eq!(domain.charter().0.llm.model.as_ref(), b"small");
    domain.chat.phase = crate::chat::Phase::Idle;
    crate::choices::apply(&mut domain, &env, &mut out);
    assert_eq!(domain.charter().0.llm.model.as_ref(), b"other");
}

#[test]
fn slash_model_keeps_an_effort_the_new_model_accepts() {
    for efforts in [Box::<[run::charter::Effort]>::default(), Box::new([b"high".as_slice().into()])] {
        let limits = limits();
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
        let mut configured = config();
        let first = configured.models[0].clone();
        let mut other = first.clone();
        other.model = b"other".as_slice().into();
        configured.models = Box::new([first, other]);
        configured.efforts = Box::new([Box::new([]), efforts]);
        configured.progress = Box::new([configured.progress[0], configured.progress[0]]);
        configured.protocol_allowances = Box::new([0, 0]);
        let mut domain = Domain::new(configured, &limits, 11).unwrap();
        let mut out = Queue::with_capacity(crate::max_out(&limits));
        domain.chat.phase = crate::chat::Phase::Idle;
        domain.effort = Some(b"high".as_slice().into());
        crate::step(
            &mut domain,
            &env,
            Event::Slash(crate::Slash::Model { model: b"endpoint/other".as_slice().into(), selected: Some(1) }),
            &mut out,
        );
        assert_eq!(domain.charter().0.llm.model.as_ref(), b"other");
        assert_eq!(domain.charter().0.llm.effort.as_deref(), Some(b"high".as_slice()));
    }
}

fn a_headless_start_reading_its_repository_head() -> (Domain, Env<Limits>, Queue<Request>) {
    let limits = limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut config = config();
    config.mode = crate::Mode::Headless { prompt: b"hello".as_slice().into() };
    config.workspace = Some(run::Workspace {
        directories: Box::new([run::Directory {
            root: Token::new(1),
            name: b"work".as_slice().into(),
            host: b"/work".as_slice().into(),
            writable: true,
            git: true,
            conflicts: Box::new([]),
        }]),
    });
    let mut domain = Domain::new(config, &limits, 11).expect("headless domain");
    let mut out = Queue::with_capacity(crate::max_out(&limits));
    crate::resume(&mut domain, &env, &mut out);
    drop(drain_requests(&mut out));
    crate::step(&mut domain, &env, Event::Loaded { state: None, transcript: None, deliveries: Box::new([]) }, &mut out);
    drop(drain_requests(&mut out));
    crate::step(
        &mut domain,
        &env,
        Event::Credential {
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
            grant: smith_domain::Grant {
                name: smith_domain::GrantName { account: 7, generation: 1 },
                renewable: true,
                valid: Duration::from_secs(60),
            },
        },
        &mut out,
    );
    crate::step(&mut domain, &env, Event::StateSaved, &mut out);
    let Some(Request::Git { owner, directory: 0, op: crate::GitOp::Head, .. }) = out.pop() else {
        panic!("start surveys the repository head")
    };
    assert_eq!(owner, Token::new(0));
    assert!(out.is_empty());
    (domain, env, out)
}

#[test]
fn a_headless_start_whose_head_cannot_be_read_exits_refused() {
    let (mut domain, env, mut out) = a_headless_start_reading_its_repository_head();
    crate::step(
        &mut domain,
        &env,
        Event::Git { owner: Token::new(0), result: crate::GitResult::Head { head: Box::new([]) } },
        &mut out,
    );
    let requests = drain_requests(&mut out);
    let failures = requests
        .iter()
        .filter_map(|request| {
            if let Request::Notice { notice: crate::Notice::Failure(failure) } = request { Some(failure) } else { None }
        })
        .collect::<Vec<_>>();
    assert_eq!(failures, [&crate::Failure::HeadUnavailable { directory: b"work".as_slice().into() }]);
    let exits = requests
        .iter()
        .filter_map(|request| if let Request::Exit { status } = request { Some(*status) } else { None })
        .collect::<Vec<_>>();
    assert_eq!(exits, [crate::ExitStatus::Refused]);
    assert_eq!(domain.chat.phase, crate::chat::Phase::Done);
    crate::resume(&mut domain, &env, &mut out);
    assert!(out.is_empty(), "a failed start never opens a run");
}

#[test]
fn each_model_fault_has_its_exit_status() {
    use smith_host_domain::{self as host, ModelFault};
    for (fault, expected) in [
        (ModelFault::Limited { account: 7, reset: None }, crate::ExitStatus::RunFailed),
        (
            ModelFault::Refused { account: 7, code: b"quota".as_slice().into() },
            crate::ExitStatus::Credentials { account: 7 },
        ),
        (ModelFault::Unauthorized { account: 8 }, crate::ExitStatus::Credentials { account: 8 }),
        (
            ModelFault::Program { name: b"login".as_slice().into(), exit: Some(1), detail: Box::default() },
            crate::ExitStatus::RunFailed,
        ),
        (completion_fault(host::CompletionFailure::Unsupported), crate::ExitStatus::RunFailed),
        (completion_fault(host::CompletionFailure::Unauthorized), crate::ExitStatus::Credentials { account: 9 }),
    ] {
        let answer = host::Answer {
            read: None,
            turns: 0,
            spent: 0,
            result: host::RunResult::Failed { failure: host::RunFailure::Model(fault.clone()) },
        };
        assert_eq!(crate::end::answer_status(&answer, Some(9)), expected, "{fault:?}");
    }
}

fn completion_fault(failure: smith_host_domain::CompletionFailure) -> smith_host_domain::ModelFault {
    smith_host_domain::ModelFault::Completion {
        failure,
        evidence: smith_host_domain::CompletionEvidence::Unsent,
        model: 0,
        attempts: 1,
        report: smith_host_domain::Report::none(),
    }
}
