//! Profile construction stays with the service whose limits it selects.

#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "programming-model.md section10.2: deterministic test harnesses use ordinary Rust"
)]

#[path = "tests/builtin.rs"]
mod builtin;

use alloc::boxed::Box;
use skein_http::{self as http, Header};
use skein_lib::List;
use skein_llm::openai;
use smith_protocol_llm as llm;

#[test]
fn every_settled_check_exit_preserves_its_kind_and_only_zero_passes() {
    use smith_domain::run;
    use smith_events::CheckExit;
    let cases = [
        (run::Exit::Code { code: 0 }, CheckExit::Code { code: 0 }, true),
        (run::Exit::Code { code: 255 }, CheckExit::Code { code: 255 }, false),
        (run::Exit::Signalled { signal: u32::MAX }, CheckExit::Signal { signal: u64::from(u32::MAX) }, false),
        (run::Exit::TimedOut, CheckExit::TimedOut, false),
        (run::Exit::Unstarted, CheckExit::Unstarted, false),
        (run::Exit::Cancelled, CheckExit::Cancelled, false),
    ];
    for (actual, expected, passed) in cases {
        assert_eq!(crate::check_exit(actual), (expected, passed));
    }
}

use crate as profile_service;

#[path = "../tests/support/declarations.rs"]
mod profile_declarations;

pub(crate) use profile_declarations::declarations;

pub(crate) fn priced_charter() -> smith_domain::run::Charter {
    use skein_lib::Duration;
    use smith_domain::run;
    run::Charter {
        mode: run::Mode::Finite,
        reserve: run::charter::Reserve { turns: 0, time: Duration::ZERO },
        instructions: Box::default(),
        brief: run::Brief {
            sections: Box::new([run::Section { title: Box::default(), text: b"Review the change.".as_slice().into() }]),
        },
        conventions: None,
        resume: false,
        waiting: Duration::from_secs(300),
        grants: run::charter::Grants {
            wait: true,
            deliver: None,
            agents: false,
            host_tools: Box::default(),
            tools: run::charter::Tools { inspect: false, modify: false, shell: false },
        },
        outcome: run::outcome::OutcomeSpec {
            change: None,
            verdicts: Box::default(),
            failure: None,
            report: Some(run::outcome::TextSpec { max: 4096, fields: Box::default() }),
        },
        budget: run::Budget { turns: 512, spend: 25_000_000, time: Duration::from_secs(3600) },
        llm: run::charter::Llm {
            effort: None,
            prices: run::Prices {
                input: 1_250_000,
                cached: 125_000,
                cache_write: None,
                output: 10_000_000,
                unit: 1_000_000,
            },
            account: Some(0),
            endpoint: run::charter::Endpoint(1),
            model: b"gpt-5".as_slice().into(),
            window: 2_097_152,
            output: 65_536,
            dialect: 1,
        },
        models: Box::default(),
    }
}

#[test]
fn standard_profile_admits_the_priced_shell_run_defaults() {
    use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::run;
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    let limits = crate::profile::derive(&profile, &declarations(0)).expect("standard profile").domain;
    let charter = priced_charter();
    let mut domain = run::Domain::new(&limits.run, 0);
    let mut out = Queue::with_capacity(run::MAX_OUT);
    run::step(
        &mut domain,
        &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.run },
        run::Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter,
            workspace: None,
            transcript: None,
            resumed: false,
        },
        &mut out,
    );
    let Some(run::Request::Admitted { .. }) = out.pop() else { panic!("priced defaults are admitted") };
    let Some(run::Request::Open { .. }) = out.pop() else { panic!("priced main opens") };
    assert!(out.is_empty());
    assert!(limits.session.spend >= 25_000_000);
    assert!(limits.session.budget.turns >= 512);
    assert!(limits.session.budget.time >= Duration::from_secs(3600));
}

fn actual_entrance_answer(
    charter: smith_domain::run::Charter,
    limits: &smith_domain::run::Limits,
) -> smith_domain::run::Answer {
    use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::run;
    let mut domain = run::Domain::new(limits, 0);
    let mut out = Queue::with_capacity(run::MAX_OUT);
    run::step(
        &mut domain,
        &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits },
        run::Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter,
            workspace: None,
            transcript: None,
            resumed: false,
        },
        &mut out,
    );
    let Some(run::Request::Answer { answer, read, .. }) = out.pop() else { panic!("the actual start refuses") };
    assert_eq!(read, None);
    assert!(out.is_empty(), "a refused entrance emits no admission, conversation or provider request");
    answer
}

#[test]
fn actual_reserve_refusals_keep_their_part_through_every_capture() {
    use skein_lib::Duration;
    use smith_domain::run;
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    let limits = crate::profile::derive(&profile, &declarations(0)).expect("standard profile").domain.run;
    let mut turns = priced_charter();
    turns.budget.turns = 1;
    turns.reserve.turns = 2;
    let mut time = priced_charter();
    time.reserve.time = time.budget.time;
    let mut spend = priced_charter();
    spend.reserve.turns = 2;
    spend.budget.spend = run::reserve_spend(spend.reserve, &spend.llm).expect("representable actual reserve");
    let mut empty = priced_charter();
    empty.budget.turns = 0;
    for (charter, refusal, which) in [
        (turns, run::BudgetRefusal::Reserve(run::Part::Turns), b"budget_reserve_turns".as_slice()),
        (time, run::BudgetRefusal::Reserve(run::Part::Time), b"budget_reserve_time".as_slice()),
        (spend, run::BudgetRefusal::Reserve(run::Part::Spend), b"budget_reserve_spend".as_slice()),
        (empty, run::BudgetRefusal::Empty(run::Part::Turns), b"budget".as_slice()),
    ] {
        let answer = actual_entrance_answer(charter, &limits);
        assert_eq!(answer, run::Answer::Refused(run::Refusal::Invalid(run::Invalid::Budget(refusal))));
        for (capture, wire_capture) in [
            (crate::sinks::Capture::None, smith_events::Capture::None),
            (crate::sinks::Capture::Calls, smith_events::Capture::Calls),
            (crate::sinks::Capture::Everything, smith_events::Capture::Everything),
        ] {
            let projected = crate::answer_record(
                crate::AnswerMeta {
                    now: skein_lib::Time::ZERO,
                    run: 1,
                    wind_down: false,
                    read: None,
                    unread: 0,
                    elapsed: Duration::ZERO,
                },
                &answer,
                capture,
            );
            assert_eq!(projected.status, smith_events::Status::Refused);
            assert_eq!(
                (projected.turns, projected.spent, projected.read, projected.result.as_ref()),
                (0, 0, None, None)
            );
            let failure = projected.failure.as_ref().expect("actual invalid refusal");
            assert_eq!(failure.class, smith_events::AnswerClass::Invalid);
            assert_eq!(failure.which.as_deref(), Some(which), "preserve the actual reserve part");
            assert_eq!(failure.reason, None, "content-free classification, not provider prose");
            let record = smith_events::Record { t_ms: 0, event: smith_events::Event::RunCompleted(projected) };
            let receiving = smith_events::Limits { string: 64, content: 128, items: 4 };
            let wire = smith_events::write_with_ceiling(&record, &wire_capture, &receiving, 4096)
                .expect("bounded wire")
                .expect("authoritative refusal");
            assert_eq!(
                smith_events::read_with_ceiling(&wire, &receiving, 4096).expect("actual codec").expect("known record"),
                record
            );
        }
    }
}

#[test]
fn standard_profile_builds_a_complete_bounded_service() {
    let limits = crate::profile::derive(
        &{
            let mut profile = crate::profile::standard();
            profile.declared.memory = u64::MAX;
            profile
        },
        &declarations(0),
    )
    .expect("standard profile");
    let configuration = crate::Config {
        program: None,
        limits,
        domain: smith_domain::Config { programs: Box::new([]), endpoints: Box::new([]), models: Box::new([]) },
        channel_endpoints: smith_protocol_channel::Endpoints::new(List::with_capacity(0)),
        llm_endpoints: smith_protocol_llm::Endpoints::new(Box::new([]), crate::profile::ENDPOINTS)
            .expect("empty endpoints"),
        environment: smith_protocol_machine::Environment::encode(&[], &limits.machine).expect("empty environment fits"),
        stream_mode: skein_channel::StreamMode::Two,
    };
    let bound = crate::worst_case(&configuration.limits, &configuration.llm_endpoints).expect("checked bound");
    assert!(bound > 0);
    let service = crate::Service::new(configuration, 1).expect("service");
    assert!(crate::done(&service).is_none());
}

#[test]
fn an_unrepresentable_render_bound_names_its_layer() {
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    profile.declared.search_bytes = u32::MAX;
    assert_eq!(
        crate::profile::derive(&profile, &declarations(0)).err(),
        Some(crate::profile::Refusal::Derived {
            limit: crate::profile::LimitName::deployment(crate::profile::Layer::Render, "result"),
        })
    );
}

#[test]
fn every_declared_conversation_can_use_the_same_endpoint() {
    for conversations in [1, 2, 3, 5, 8] {
        let mut profile = crate::profile::standard();
        profile.declared.memory = u64::MAX;
        profile.declared.conversations = conversations;
        let configuration = declarations(0);
        let limits = crate::profile::derive(&profile, &configuration).expect("declared conversations derive");
        assert_eq!(limits.llm.connection.calls, conversations);
        assert_eq!(limits.llm.connection.connections, conversations);
        assert_eq!(limits.llm.connection.per_endpoint, conversations);
    }
}

#[test]
fn scalar_event_projection_preserves_subsecond_child_time_and_absent_requests() {
    use skein_lib::{Duration, Token};
    use smith_domain::run;
    use smith_events::{ChildStop, ShareBound, SharePart, Usage};
    for millis in [750, 1999] {
        let kind = run::facts::Kind::Child {
            parent: Token::new(1),
            call: Token::new(2),
            asked: run::Share::UNASKED,
            granted: run::Granted {
                turns: 3,
                time: Duration::from_millis(millis),
                spend: 100,
                turns_set: run::Set::Pool,
                time_set: run::Set::Deadline,
            },
        };
        let fields = crate::child_share(kind).expect("actual child role");
        assert_eq!(fields.requested, smith_events::Requested { turns: None, seconds: None });
        assert_eq!(fields.share.time_ms, millis);
        assert_eq!(
            fields.clamped.as_slice(),
            &[smith_events::Clamp { part: SharePart::Time, by: ShareBound::Deadline }]
        );
        let usage = Usage {
            input_tokens: None,
            cache_read_tokens: Some(7),
            cache_write_tokens: None,
            output_tokens: Some(8),
            reasoning_tokens: None,
        };
        let terminal = crate::child_completed(
            2,
            ChildStop::Budget,
            run::facts::Used { turns: 1, time: Duration::from_millis(millis), spend: 3 },
            usage.clone(),
        );
        assert_eq!(terminal.time_ms, millis);
        assert_eq!(terminal.usage, usage, "unknown native counts remain unknown");
    }
    assert!(crate::child_share(run::facts::Kind::Main).is_none());
}

#[test]
fn actual_root_budget_facts_encode_without_content_under_every_capture() {
    use smith_domain::{Fact, run};
    use smith_events::{Capture, Event};
    let mut world = smith_agent_world::World::new(smith_agent_world::Settings::calm(34));
    world.run(1_000_000);
    let mut budget_count = 0_u32;
    let limits = smith_events::Limits { string: 64, content: 1024, items: 8 };
    for fact in world.facts() {
        let Fact::Run { fact, opening: _ } = fact else { continue };
        let Some(record) = crate::budget_record(fact, skein_lib::Time::ZERO, 1, None) else { continue };
        budget_count += 1;
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            let line = smith_events::write(&record, &capture, &limits)
                .expect("valid bounded event fixture")
                .expect("valid bounded event fixture");
            assert_eq!(smith_events::read(&line, &limits).expect("valid bounded event fixture"), Some(record.clone()));
            let Event::Budget(value) = &record.event else { panic!("budget projection") };
            let run::facts::FactKind::Budget { standing, .. } = fact.kind else { panic!("actual budget fact") };
            assert_eq!(
                value.time_ms.used,
                standing.time.as_nanos() / 1_000_000,
                "run time retains its admission origin"
            );
        }
    }
    assert!(budget_count > 0, "real root/session completions produce charges");
}

#[test]
fn a_budget_record_names_the_provider_id_of_its_call() {
    use smith_domain::run;
    use smith_events::{Capture, Event};
    let fact = run::facts::Fact {
        at: skein_lib::Time::from_nanos(2_001_000_000),
        kind: run::facts::FactKind::Budget {
            run: skein_lib::Token::new(3),
            call: Some(skein_lib::Token::new(7)),
            standing: run::facts::Standing {
                stage: run::Stage::Ordinary,
                turns: 1,
                turns_limit: 10,
                spend: 50,
                spend_limit: 10_000,
                time: skein_lib::Duration::from_millis(2_001),
                time_limit: skein_lib::Duration::from_secs(600),
                reserve: run::charter::Reserve { turns: 2, time: skein_lib::Duration::from_secs(60) },
                reserve_spend: 2,
            },
        },
    };
    let record = crate::budget_record(&fact, skein_lib::Time::ZERO, 1, Some(b"provider-child-7".as_slice().into()))
        .expect("actual budget fact");
    let Event::Budget(value) = &record.event else { panic!("budget projection") };
    assert_eq!(value.call.as_deref(), Some(b"provider-child-7".as_slice()));
    for capture in [Capture::None, Capture::Calls, Capture::Everything] {
        let limits = smith_events::Limits { string: 64, content: 1024, items: 8 };
        let line =
            smith_events::write(&record, &capture, &limits).expect("bounded budget record").expect("budget event");
        assert_eq!(smith_events::read(&line, &limits).expect("bounded budget record"), Some(record.clone()));
    }
}

#[test]
fn the_measured_large_contract_is_admitted_at_the_host_edge_and_refused_one_byte_later() {
    use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::run;
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    let receiving = crate::profile::derive(&profile, &declarations(0)).expect("declared model").domain.run;
    let enter = |bound, instructions: &[u8]| {
        let limits = run::Limits { host_part: bound, ..receiving };
        let mut charter = priced_charter();
        charter.instructions = instructions.into();
        charter.outcome.report = Some(run::outcome::TextSpec {
            max: 32,
            fields: (0_u32..20)
                .map(|index| run::outcome::FieldRule {
                    name: format!("long_contract_field_{index:02}").into_bytes().into(),
                    max: 32,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        });
        let mut domain = run::Domain::new(&limits, 0);
        let mut out = Queue::with_capacity(run::MAX_OUT);
        run::step(
            &mut domain,
            &Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            run::Event::Start {
                messages: Box::default(),
                reply_to: ReplyTo::new(Token::new(1)),
                host_run: Token::new(1),
                identity: None,
                activation: 1,
                window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
                charter,
                workspace: None,
                transcript: None,
                resumed: false,
            },
            &mut out,
        );
        out.pop().expect("one entrance terminal")
    };
    let mut lower = 0;
    let mut upper = receiving.host_part;
    let run::Request::Admitted { .. } = enter(upper, b"base") else { panic!("upper edge admits") };
    while lower < upper {
        let middle = lower + (upper - lower) / 2;
        match enter(middle, b"base") {
            run::Request::Admitted { .. } => upper = middle,
            run::Request::Answer {
                answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::HostPart { .. })),
                ..
            } => lower = middle + 1,
            other @ (run::Request::MessageRefused { .. }
            | run::Request::CompletionReady { .. }
            | run::Request::Waiting { .. }
            | run::Request::Turn { .. }
            | run::Request::HostCall { .. }
            | run::Request::WithdrawHost { .. }
            | run::Request::Answer { .. }
            | run::Request::Open { .. }
            | run::Request::Say { .. }
            | run::Request::CancelJobs { .. }
            | run::Request::Yield { .. }
            | run::Request::Close { .. }
            | run::Request::Read { .. }
            | run::Request::Probe { .. }
            | run::Request::Check { .. }
            | run::Request::Abort { .. }
            | run::Request::Checking { .. }
            | run::Request::ChecksEnded { .. }
            | run::Request::Deliver { .. }
            | run::Request::Return { .. }) => panic!("unexpected entrance: {other:?}"),
        }
    }
    assert!(lower > 2000, "the contract carries substantial rendered schemas and text");
    let run::Request::Admitted { .. } = enter(lower, b"base") else { panic!("exact edge admits") };
    let run::Request::Answer {
        answer: run::Answer::Refused(run::Refusal::Invalid(run::Invalid::HostPart { bound })),
        ..
    } = enter(lower, b"basex")
    else {
        panic!("one byte over refuses host part")
    };
    assert_eq!(bound, lower);
}

#[test]
fn an_admitted_opening_fits_the_derived_request_with_escaped_host_text() {
    use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::{llm, run};
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    let limits = crate::profile::derive(&profile, &declarations(0)).expect("real model");
    let mut selected = priced_charter();
    selected.outcome.report = Some(run::outcome::TextSpec { max: 32, fields: Box::default() });
    let mut domain = run::Domain::new(&limits.domain.run, 0);
    let mut out = Queue::with_capacity(run::MAX_OUT);
    selected.instructions = vec![1; 3500].into();
    run::step(
        &mut domain,
        &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.domain.run },
        run::Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter: selected,
            workspace: None,
            transcript: None,
            resumed: false,
        },
        &mut out,
    );
    let Some(run::Request::Admitted { .. }) = out.pop() else { panic!("opening admitted") };
    let Some(run::Request::Open { opening, .. }) = out.pop() else { panic!("admitted opening") };
    let prompt = llm::Prompt {
        effort: None,
        purpose: llm::Purpose::Ordinary { guard: None },
        choice: llm::ToolChoice::Auto,
        namespace: smith_domain::tools::Namespace::One,
        affinity: llm::Affinity { key: [0; 16], thread: 0 },
        asker: llm::Asker::Main,
        endpoint: llm::Endpoint(opening.llm.endpoint.0),
        model: opening.llm.model,
        system: opening.system,
        tools: smith_domain::tools::Grants { inspect: false, modify: false, shell: false },
        served: Box::new([llm::Served::Wait, llm::Served::Finish]),
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: opening.prompt, replay: None }]),
        }]),
        max_tokens: opening.llm.output,
    };
    let application = smith_protocol_llm::schemas_for_contract(
        &prompt,
        &limits.llm.adapter.stated,
        &run::outcome::OutcomeSpec {
            change: None,
            verdicts: Box::default(),
            failure: None,
            report: Some(run::outcome::TextSpec { max: 32, fields: Box::default() }),
        },
        None,
        limits.llm.adapter.client.request,
    )
    .expect("schemas");
    let mut translated =
        smith_protocol_llm::prompt(prompt, &application, &limits.llm.adapter).expect("translated opening");
    translated.output_ceiling(skein_llm::Provider::OpenAiCodex, 4096).expect("configured model output");
    let prepared = skein_llm::client::Client::prepare(
        skein_llm::Call {
            owner: Token::new(1),
            prompt: translated,
            endpoint: skein_llm::Endpoint::codex(),
            credential: Some(skein_llm::Credential {
                value: b"fake".as_slice().into(),
                account_id: b"fake".as_slice().into(),
                scheme: skein_llm::Scheme::Bearer,
            }),
        },
        &limits.llm.adapter.client,
    );
    assert!(prepared.is_ok(), "admitted host bytes must fit the derived request: {:?}", prepared.err());
}

fn standard_limits() -> Result<crate::Limits, crate::profile::Refusal> {
    let mut profile = crate::profile::standard();
    profile.declared.memory = u64::MAX;
    crate::profile::derive(&profile, &declarations(0))
}

#[test]
fn standard_profile_accepts_full_workspace_read_results() {
    let limits = standard_limits().expect("standard limits");
    let read_bytes = limits.domain.session.tools.read_bytes;
    let outcome = smith_domain::tools::Outcome::Read {
        content: vec![0xff; usize::try_from(read_bytes).expect("read bytes fit usize")].into_boxed_slice(),
        first: 1,
        lines: 1,
        total: 1,
        next: smith_domain::tools::Next::End,
        cut: smith_domain::tools::Cut::Whole,
        window: smith_domain::tools::BytesClamp::Within,
    };
    let (rendered, error) =
        llm::render_outcome(&outcome, &smith_domain::tools::Namespace::One, limits.llm.adapter.rendered_result)
            .expect("full read fits after escaping and adding line metadata");
    assert!(!error);
    assert!(rendered.len() > 16_384, "escaped bytes and metadata exceed the old string bound");
}

#[test]
fn standard_profile_accepts_conversation_history_with_workspace_results() {
    let limits = standard_limits().expect("standard limits").llm.adapter.client.native();
    let mut input = Vec::new();
    for index in 0_u32..12 {
        let call_id = format!("read_{index}").into_bytes().into_boxed_slice();
        input.push(openai::Input::FunctionCall {
            call_id: call_id.clone(),
            item_id: None,
            name: b"read".as_slice().into(),
            arguments: br#"{"path":"README.md"}"#.as_slice().into(),
        });
        input.push(openai::Input::FunctionOutput { call_id, output: vec![b'x'; 1024].into_boxed_slice() });
    }
    let request = openai::Request {
        model: b"provider-model".as_slice().into(),
        instructions: b"Read the workspace and report.".as_slice().into(),
        tools: Box::new([]),
        input: input.into_boxed_slice(),
        effort: None,
        prompt_cache_key: None,
        choice: skein_llm::ToolChoice::Auto,
    };
    let encoded = openai::encode_request(&request, &limits).expect("workspace results fit the request");
    assert!(encoded.len() > 8192, "workspace history exceeds the old request bound");
}

#[test]
fn standard_profile_accepts_provider_heads_with_many_metadata_fields() {
    use skein_lib::stream::{Down, Up};
    use skein_lib::{Env, Queue, Time, Wall};

    let limits = standard_limits().expect("standard limits").llm.adapter.client.http;
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut client = http::client::Client::new(&limits);
    let mut above = Queue::with_capacity(4);
    let mut below = Queue::with_capacity(4);
    http::client::down(
        &mut client,
        &env,
        http::client::Request::Call(http::client::Call {
            method: http::Method::Get,
            target: b"/".as_slice().into(),
            headers: Box::new([Header { name: b"Host".as_slice().into(), value: b"provider.test".as_slice().into() }]),
            body: http::client::Body::None,
            close: false,
        }),
        &mut above,
        &mut below,
    );
    let Some(Down::Demand { .. }) = below.pop() else { panic!("the client demands response bytes") };
    http::client::up(&mut client, &env, Up::Room, &mut above, &mut below);
    let Some(Down::Send(_)) = below.pop() else { panic!("the client sends its request") };

    let mut lines = vec![b"HTTP/1.1 200 OK\r\n".to_vec()];
    for index in 0_u32..40 {
        lines.push(format!("X-Provider-Metadata-{index}: value\r\n").into_bytes());
    }
    lines.push(b"Content-Type: text/event-stream\r\n".to_vec());
    lines.push(b"Content-Length: 0\r\n".to_vec());
    lines.push(b"\r\n".to_vec());
    for line in lines {
        let Some(Down::Demand { .. }) = below.pop() else { panic!("each head line answers a read demand") };
        http::client::up(&mut client, &env, Up::Bytes(line.into()), &mut above, &mut below);
    }
    let Some(http::client::Event::Response(response)) = above.pop() else {
        panic!("the standard profile accepts the provider's response head");
    };
    assert_eq!(response.status, 200);
    assert_eq!(response.headers.len(), 42);
}

#[test]
fn standard_profile_accepts_provider_events_that_echo_tool_schemas() {
    use skein_lib::stream::{Down, Up};
    use skein_lib::{Env, Queue, Time, Wall};

    let limits = standard_limits().expect("standard limits").llm.adapter.client;
    let tools: Vec<_> = (0_u32..32)
        .map(|index| {
            serde_json::json!({
                "type": "function",
                "name": format!("tool_{index}"),
                "description": "x".repeat(300),
                "parameters": { "type": "object", "properties": {} }
            })
        })
        .collect();
    let payload = serde_json::to_vec(&serde_json::json!({
        "type": "response.created",
        "response": { "id": "response", "status": "in_progress", "tools": tools }
    }))
    .expect("provider event JSON");
    assert!(payload.len() > 8192, "echoed schemas exceed the old event and document bounds");
    let mut data = b"data: ".to_vec();
    data.extend_from_slice(&payload);
    data.push(b'\n');

    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.sse };
    let mut reader = http::sse::Reader::new(&limits.sse);
    let mut above = Queue::uncapped();
    let mut below = Queue::uncapped();
    let mut received = Vec::new();
    let mut dispatched = false;
    http::sse::down(&mut reader, &env, http::sse::Request::Next, &mut above, &mut below);
    for line in [b"event: response.created\n".as_slice(), data.as_slice(), b"\n"] {
        for chunk in line.chunks(usize::try_from(limits.sse.chunk).expect("chunk fits usize")) {
            let Some(Down::Demand { .. }) = below.pop() else { panic!("each chunk answers a read demand") };
            http::sse::up(&mut reader, &env, Up::Bytes(chunk.into()), &mut above, &mut below);
            while let Some(event) = above.pop() {
                match event {
                    http::sse::Event::Opened | http::sse::Event::Data(Up::Bytes(_)) => {
                        if let http::sse::Event::Data(Up::Bytes(bytes)) = event {
                            received.extend_from_slice(&bytes);
                        }
                        http::sse::down(
                            &mut reader,
                            &env,
                            http::sse::Request::Data(Down::Demand { read: skein_lib::stream::Read::Fill(1), room: 0 }),
                            &mut above,
                            &mut below,
                        );
                    }
                    http::sse::Event::Data(Up::End) => {}
                    http::sse::Event::Dispatched(dispatch) => {
                        assert_eq!(dispatch.name.as_ref(), b"response.created");
                        dispatched = true;
                    }
                    other @ (http::sse::Event::Data(Up::Room | Up::Failed(_))
                    | http::sse::Event::Ended
                    | http::sse::Event::Failed(_)
                    | http::sse::Event::Closed) => panic!("the standard profile accepts the provider event: {other:?}"),
                }
            }
        }
    }
    assert!(dispatched);
    assert_eq!(received, payload);
    let json = openai::Json::from_bytes(&received, &limits.native().document()).expect("provider event document");
    assert_eq!(openai::decode_event(&json, &limits.native()), Ok(openai::Event::Created { echo: None }));
}

fn stream_limits() -> smith_events::Limits {
    // Reuse the sink fixture's record bounds; codecs need no IO destinations.
    crate::sinks::tests::limits(0).records
}

fn assert_stream_record(actual: &smith_events::Record, literal: &[u8]) {
    let limits = stream_limits();
    let expected = smith_events::read(literal, &limits).unwrap().unwrap();
    assert_eq!(*actual, expected);
}

fn compaction_intent(book: &mut crate::responses::Book, owner: skein_lib::Token, window: u32) {
    use crate::responses::tests::fact;
    use smith_domain::session;
    assert!(
        book.fact(
            skein_lib::Time::ZERO,
            &fact(
                3,
                session::FactKind::CompletionStarted {
                    opener: owner,
                    window,
                    attempt: 0,
                    messages: 0,
                    max_tokens: 7,
                    choice: session::Choice::Auto,
                },
                3,
                None
            )
        )
        .is_empty()
    );
}

fn compaction_answer(book: &mut crate::responses::Book, owner: skein_lib::Token, stop: smith_domain::llm::Stop) {
    use crate::responses::tests::fact;
    use smith_domain::{llm, session};
    let usage = llm::Usage { input_tokens: Some(2), output_tokens: Some(3), ..llm::Usage::NONE };
    book.received(
        skein_lib::Token::new(99),
        &llm::Completion { content: Box::new([]), stop, usage, reasoning_dropped: Box::new([]) },
    );
    book.fact(
        skein_lib::Time::ZERO,
        &fact(
            5,
            session::FactKind::CompletionAnswered { opener: owner, stop, blocks: 0, calls: 0, invalid: 0 },
            3,
            None,
        ),
    );
    let records =
        book.fact(skein_lib::Time::ZERO, &fact(6, session::FactKind::Used { opener: owner, usage }, 3, Some(7)));
    let end = match stop {
        llm::Stop::Refusal => "refusal",
        llm::Stop::EndTurn | llm::Stop::ToolUse | llm::Stop::MaxTokens => "end",
    };
    let literal = alloc::format!(
        "{{\"v\":4,\"type\":\"response.completed\",\"t_ms\":5,\"run\":7,\"conversation\":1,\"response\":1,\"outcome\":\"completed\",\"stop\":\"{end}\",\"blocks\":0,\"calls\":0,\"invalid\":0,\"oversized\":0,\"cut\":0,\"usage\":{{\"input_tokens\":2,\"cache_read_tokens\":null,\"cache_write_tokens\":null,\"output_tokens\":3,\"reasoning_tokens\":null}},\"spent\":7,\"request_bytes\":null,\"first_byte_ms\":null,\"largest_gap_ms\":null,\"duration_ms\":1}}"
    );
    assert_stream_record(records.get(0).unwrap(), literal.as_bytes());
}

#[test]
fn a_compaction_is_a_conversation_of_kind_compaction_in_the_stream() {
    use crate::responses::tests::{at, fact, opened, prompt};
    use skein_lib::{Time, Token};
    use smith_domain::{llm, session};
    let owner = Token::new(11);
    let mut book = crate::responses::Book::new(1);
    book.opened(at(1), owner, &opened(0, 0));
    let opened = book.fact(
        Time::ZERO,
        &fact(2, session::FactKind::compaction_started(owner, 1, b"model-7", 123, 7, Some(b"medium")), 3, None),
    );
    assert_stream_record(opened.get(0).unwrap(), br#"{"v":4,"type":"conversation.opened","t_ms":2,"run":7,"conversation":1,"kind":"compaction","parent":0,"thread":0,"model":{"endpoint":"endpoint-7","model":"model-7","window_tokens":123,"output_tokens":7,"effort":"medium","account":null,"backend":"direct"}}"#);
    compaction_intent(&mut book, owner, 1);
    let started = book.request(at(4), Time::ZERO, Token::new(99), &prompt(0));
    assert_stream_record(&started, br#"{"v":4,"type":"response.started","t_ms":4,"run":7,"conversation":1,"response":1,"turn":1,"attempt":0,"window":1,"model":{"endpoint":"endpoint-7","model":"model-7","window_tokens":123,"output_tokens":7,"effort":"medium","account":null,"backend":"direct"},"messages":0,"output_tokens":7,"tool_choice":"only"}"#);
    assert_stream_record(
        &book.delta(at(4), Time::ZERO, Token::new(99), 0, b"summary".as_slice().into()),
        br#"{"v":4,"type":"text.delta","t_ms":4,"run":7,"conversation":1,"response":1,"block":0,"text":"summary"}"#,
    );
    book.prompt_recorded(Token::new(99), 4);
    compaction_answer(&mut book, owner, llm::Stop::EndTurn);
    let closed = book.fact(
        Time::ZERO,
        &fact(
            7,
            session::FactKind::CompactionEnded { opener: owner, window: 2, end: session::Compacted::Opened },
            3,
            None,
        ),
    );
    assert_stream_record(closed.get(0).unwrap(), br#"{"v":4,"type":"conversation.closed","t_ms":7,"run":7,"conversation":1,"end":"closed","turns":1,"usage":{"input_tokens":2,"cache_read_tokens":null,"cache_write_tokens":null,"output_tokens":3,"reasoning_tokens":null},"spent":7,"duration_ms":5}"#);
    compaction_intent(&mut book, owner, 2);
    let next = book.request(at(8), Time::ZERO, Token::new(100), &prompt(0));
    assert_stream_record(&next, br#"{"v":4,"type":"response.started","t_ms":8,"run":7,"conversation":0,"response":2,"turn":3,"attempt":0,"window":2,"model":{"endpoint":"endpoint-7","model":"model-7","window_tokens":123,"output_tokens":7,"effort":"medium","account":null,"backend":"direct"},"messages":0,"output_tokens":7,"tool_choice":"only"}"#);
    assert!(book.observation(Token::new(100)).first);
    assert_eq!(book.observation(Token::new(100)).messages, 0);
    assert_eq!(book.next_conversation(7), 2);
}

#[test]
fn a_refused_compaction_closes_failed_with_no_completion_failure() {
    use crate::responses::tests::{at, fact, opened, prompt};
    use skein_lib::{Time, Token};
    use smith_domain::{llm, session};
    let owner = Token::new(11);
    let mut book = crate::responses::Book::new(1);
    book.opened(at(1), owner, &opened(0, 0));
    book.fact(
        Time::ZERO,
        &fact(2, session::FactKind::compaction_started(owner, 1, b"model-7", 123, 7, Some(b"medium")), 3, None),
    );
    compaction_intent(&mut book, owner, 1);
    book.request(at(4), Time::ZERO, Token::new(99), &prompt(0));
    compaction_answer(&mut book, owner, llm::Stop::Refusal);
    let closed = book.fact(
        Time::ZERO,
        &fact(
            7,
            session::FactKind::CompactionEnded { opener: owner, window: 1, end: session::Compacted::Failed },
            3,
            None,
        ),
    );
    assert_stream_record(closed.get(0).unwrap(), br#"{"v":4,"type":"conversation.closed","t_ms":7,"run":7,"conversation":1,"end":"failed","turns":1,"usage":{"input_tokens":2,"cache_read_tokens":null,"cache_write_tokens":null,"output_tokens":3,"reasoning_tokens":null},"spent":7,"duration_ms":5}"#);
    let parent = book.fact(
        Time::ZERO,
        &fact(
            7,
            session::FactKind::Ended {
                opener: owner,
                end: session::End::NoSummary { fault: session::NoSummary::Refused },
                turns: 3,
                usage: llm::Usage::NONE,
            },
            3,
            None,
        ),
    );
    assert_stream_record(parent.get(0).unwrap(), br#"{"v":4,"type":"conversation.closed","t_ms":7,"run":7,"conversation":0,"end":"failed","turns":3,"usage":{"input_tokens":2,"cache_read_tokens":null,"cache_write_tokens":null,"output_tokens":3,"reasoning_tokens":null},"spent":7,"duration_ms":6}"#);
}

#[test]
fn a_failed_compaction_settles_its_response_before_closing_and_keeps_the_parent_limit() {
    use crate::responses::tests::{at, fact, opened, prompt};
    use skein_lib::{Time, Token};
    use smith_domain::{llm, session};
    use smith_events::Event;
    let owner = Token::new(11);
    let mut book = crate::responses::Book::new(1);
    book.opened(at(1), owner, &opened(0, 0));
    book.fact(
        Time::ZERO,
        &fact(2, session::FactKind::compaction_started(owner, 1, b"model-7", 123, 7, Some(b"medium")), 3, None),
    );
    compaction_intent(&mut book, owner, 1);
    book.request(at(4), Time::ZERO, Token::new(99), &prompt(0));
    book.failed(
        Token::new(99),
        llm::Failure::ContextTooLong,
        llm::Evidence::Response { status: Some(400) },
        b"context",
        crate::sinks::Capture::Calls,
        &llm::Report::none(),
    );
    book.fact(
        Time::ZERO,
        &fact(
            5,
            session::FactKind::CompletionFailed {
                opener: owner,
                failure: llm::Failure::ContextTooLong,
                evidence: llm::Evidence::Response { status: None },
            },
            3,
            None,
        ),
    );
    let records = book.fact(
        Time::ZERO,
        &fact(
            6,
            session::FactKind::CompactionEnded { opener: owner, window: 1, end: session::Compacted::Failed },
            3,
            None,
        ),
    );
    assert_eq!(records.len(), 2);
    let Event::ResponseCompleted(response) = &records.get(0).unwrap().event else { panic!("failed summary response") };
    let Event::ConversationClosed(compaction) = &records.get(1).unwrap().event else { panic!("failed compaction") };
    assert_eq!(response.conversation, 1);
    assert_eq!(compaction.conversation, 1);
    assert_eq!(response.failure, compaction.failure);
    let parent = book.fact(
        Time::ZERO,
        &fact(
            6,
            session::FactKind::Ended {
                opener: owner,
                end: session::End::Failed {
                    detail: Box::default(),
                    failure: llm::Failure::Limit { which: llm::Limit::Window, bound: 123 },
                    evidence: llm::Evidence::Response { status: None },
                    attempts: 1,
                },
                turns: 2,
                usage: llm::Usage::NONE,
            },
            3,
            None,
        ),
    );
    let Event::ConversationClosed(parent) = &parent.get(0).unwrap().event else { panic!("parent terminal") };
    assert_eq!(parent.conversation, 0);
    let failure = parent.failure.as_ref().unwrap();
    assert_eq!(failure.class, smith_events::FailureClass::Limit);
    assert_eq!(failure.limit.as_ref().unwrap().which.as_ref(), b"window");
    assert_eq!(failure.status, Some(400));
    assert_eq!(failure.detail.as_deref(), Some(b"context".as_slice()));
}

fn account_completion_record(
    failure: smith_domain::llm::Failure,
    capture: crate::sinks::Capture,
) -> smith_events::Record {
    use crate::responses::tests::{at, fact, intent, opened, prompt};
    use skein_lib::{Time, Token};
    use smith_domain::{llm, session};
    let owner = Token::new(11);
    let request = Token::new(99);
    let mut book = crate::responses::Book::new(1);
    book.opened(at(1), owner, &opened(0, 0));
    intent(&mut book, owner, session::Choice::Auto, 0);
    book.request(at(3), Time::ZERO, request, &prompt(0));
    let report = llm::Report {
        field: b"effort".as_slice().into(),
        code: b"not_allowed".as_slice().into(),
        request: b"request-7".as_slice().into(),
    };
    book.failed(request, failure, llm::Evidence::Response { status: Some(400) }, b"provider text", capture, &report);
    book.fact(
        Time::ZERO,
        &fact(
            4,
            session::FactKind::CompletionFailed {
                opener: owner,
                failure,
                evidence: llm::Evidence::Response { status: Some(400) },
            },
            1,
            None,
        ),
    );
    book.fact(
        Time::ZERO,
        &fact(
            5,
            session::FactKind::Ended {
                opener: owner,
                end: session::End::Failed {
                    detail: Box::default(),
                    failure,
                    evidence: llm::Evidence::Response { status: Some(400) },
                    attempts: 1,
                },
                turns: 0,
                usage: llm::Usage::NONE,
            },
            1,
            None,
        ),
    )
    .get(0)
    .unwrap()
    .clone()
}

#[test]
fn an_unsupported_completion_names_its_field_with_no_retry() {
    let record = account_completion_record(smith_domain::llm::Failure::Unsupported, crate::sinks::Capture::None);
    assert_stream_record(&record, br#"{"v":4,"type":"response.completed","t_ms":4,"run":7,"conversation":0,"response":1,"outcome":"failed","blocks":0,"calls":0,"invalid":0,"oversized":0,"cut":0,"usage":{"input_tokens":null,"cache_read_tokens":null,"cache_write_tokens":null,"output_tokens":null,"reasoning_tokens":null},"spent":0,"request_bytes":null,"first_byte_ms":null,"largest_gap_ms":null,"duration_ms":1,"failure":{"class":"unsupported","evidence":"response","status":400,"field":"effort","code":"not_allowed","request_id":"request-7"}}"#);
}

#[test]
fn a_refused_completions_detail_is_content() {
    struct Row {
        name: &'static str,
        capture: crate::sinks::Capture,
        detail: &'static str,
    }
    for row in [
        Row { name: "none", capture: crate::sinks::Capture::None, detail: "" },
        Row { name: "calls", capture: crate::sinks::Capture::Calls, detail: ",\"detail\":\"provider text\"" },
    ] {
        let record = account_completion_record(smith_domain::llm::Failure::Refused, row.capture);
        let literal = alloc::format!(
            "{{\"v\":4,\"type\":\"response.completed\",\"t_ms\":4,\"run\":7,\"conversation\":0,\"response\":1,\"outcome\":\"failed\",\"blocks\":0,\"calls\":0,\"invalid\":0,\"oversized\":0,\"cut\":0,\"usage\":{{\"input_tokens\":null,\"cache_read_tokens\":null,\"cache_write_tokens\":null,\"output_tokens\":null,\"reasoning_tokens\":null}},\"spent\":0,\"request_bytes\":null,\"first_byte_ms\":null,\"largest_gap_ms\":null,\"duration_ms\":1,\"failure\":{{\"class\":\"refused\",\"evidence\":\"response\",\"status\":400,\"field\":\"effort\",\"code\":\"not_allowed\",\"request_id\":\"request-7\"{detail}}}}}",
            detail = row.detail
        );
        let limits = stream_limits();
        let expected = smith_events::read(literal.as_bytes(), &limits).unwrap().unwrap();
        assert_eq!(record, expected, "{}", row.name);
    }
}

mod accounts;

#[path = "tests/line.rs"]
mod line;
