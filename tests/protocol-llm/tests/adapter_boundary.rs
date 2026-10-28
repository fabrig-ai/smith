//! Positive-first adapter boundary controls over actual shared Client terminals.
//! Caller schemas, restored results and a literal Wait attestation are fixture
//! data; no application decoder or Finish policy lives in this test.
//! Contract: scratch/client.md, sections 1–6; testing-strategy.md, sections 2.5 and 6.

use skein_fake_llm_domain::{self as fake, api};
use skein_lib::{Env, Queue, Time, Token, Wall};
use skein_llm::{self as shared, client};
use skein_llm_world::fake::Exchange;
use smith_domain::{Event, llm, run, tools};
use smith_protocol_llm_world::adapter::{
    self as adapter, Context, Input, Limits, Receiving, ResolvedCall, ToolKind, ToolSchema,
};
use smith_protocol_llm_world::wire::{self, Configuration};

const OWNER: Token = Token::new(71);
const OTHER: Token = Token::new(72);
const WAIT_SCHEMA: &[u8] = br#"{"type":"object","properties":{},"additionalProperties":false}"#;

fn limits() -> Limits {
    Limits {
        client: skein_llm_world::limits(),
        tool_bytes: 32_768,
        history_bytes: u64::from(skein_llm_world::limits().request),
        rendered_result: 2048,
        stated: smith_protocol_llm_world::adapter::stated(),
    }
}

fn prompt() -> llm::Prompt {
    llm::Prompt {
        effort: None,
        purpose: llm::Purpose::Ordinary { guard: None },
        choice: smith_domain::llm::ToolChoice::Auto,
        namespace: smith_domain::tools::Namespace::One,
        affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
        asker: llm::Asker::Main,
        endpoint: llm::Endpoint(1),
        model: b"fixture-model".as_slice().into(),
        system: b"@adapter-boundary".as_slice().into(),
        tools: tools::Grants { inspect: false, modify: false, shell: false },
        served: Box::new([llm::Served::Wait]),
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"Start.".as_slice().into(), replay: None }]),
        }]),
        max_tokens: 128,
    }
}

fn schema(kind: ToolKind, name: &[u8]) -> ToolSchema {
    ToolSchema {
        kind,
        name: name.into(),
        description: b"Boundary caller declaration.".as_slice().into(),
        schema: WAIT_SCHEMA.into(),
    }
}

fn input(configuration: &Configuration) -> Input {
    let bounds = limits();
    let decoded_call_bytes = 4096;
    Input {
        owner: OWNER,
        prompt: prompt(),
        endpoint_name: llm::Endpoint(1),
        endpoint: configuration.endpoint.clone(),
        credential: shared::Credential {
            value: configuration.credential.value.clone(),
            account_id: configuration.credential.account_id.clone(),
            scheme: skein_llm::Scheme::Bearer,
        },
        application: Box::new([schema(ToolKind::Wait, b"wait")]),
        receiving: Receiving {
            max_completion_bytes: adapter::completion_worst_case(&bounds.client, decoded_call_bytes)
                .expect("compatible full completion receiving allowance"),
            max_completion_blocks: bounds.client.output_items,
            decoded_call_bytes,
            max_failure_bytes: bounds.client.detail_bytes,
        },
    }
}

fn scripts(calls: bool) -> Box<[api::Script]> {
    let lines: Box<[api::Line]> = if calls {
        Box::new([
            api::Line::Text { text: b"before".as_slice().into() },
            api::Line::Call { name: b"wait".as_slice().into(), arguments: b"{}".as_slice().into() },
            api::Line::Text { text: b"after".as_slice().into() },
        ])
    } else {
        Box::new([api::Line::Text { text: b"admitted exact".as_slice().into() }])
    };
    Box::new([api::Script {
        cue: b"@adapter-boundary".as_slice().into(),
        turns: Box::new([api::Turn {
            lines,
            finish: if calls { api::Finish::ToolCalls } else { api::Finish::Stop },
            tokens: 7,
        }]),
    }])
}

fn adopt(input: Input, calls: bool) -> (Context, Exchange) {
    let endpoint = input.endpoint.clone();
    let credential = shared::Credential {
        value: input.credential.value.clone(),
        account_id: input.credential.account_id.clone(),
        scheme: skein_llm::Scheme::Bearer,
    };
    let adapter::Prepared { client, context } = adapter::prepare(input, &limits()).expect("positive admission");
    let peer = Exchange::prepared(client, endpoint, Some(credential), limits().client, scripts(calls));
    assert!(peer.queries.is_empty(), "preparation starts no wire request");
    assert!(peer.requests.is_empty());
    (context, peer)
}

fn quiesce(peer: &mut Exchange) {
    for _ in 0..100_000_u32 {
        if !peer.tick(true) {
            return;
        }
    }
    panic!("bounded actual exchange did not quiesce");
}

fn take_terminal(peer: &mut Exchange) -> client::Event {
    let positions: Vec<_> = peer
        .seen
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match event {
            client::Event::Completed { .. } | client::Event::Failed { .. } | client::Event::Cancelled { .. } => {
                Some(index)
            }
            client::Event::Delta { .. }
            | client::Event::Block { .. }
            | client::Event::Reusable
            | client::Event::Close
            | client::Event::Closed => None,
        })
        .collect();
    let [position] = positions.as_slice() else {
        panic!("one actual native terminal: {:?}", peer.seen);
    };
    peer.seen.remove(*position)
}

fn close_won(peer: &mut Exchange) {
    peer.request(client::Request::Close);
    assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
    assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 0);
    peer.settle();
    assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
    assert!(peer.seen.iter().all(|event| !matches!(
        event,
        client::Event::Completed { .. } | client::Event::Failed { .. } | client::Event::Cancelled { .. }
    )));
    let events = peer.seen.len();
    peer.settle();
    assert_eq!(peer.seen.len(), events, "repeated actual settlement emits nothing");
    assert_eq!(peer.machine.waiting(), client::Waiting::Nothing);
}

fn completed(configuration: &Configuration, calls: bool) -> (Context, Exchange, Token, shared::Completion) {
    let (context, mut peer) = adopt(input(configuration), calls);
    peer.start();
    peer.run();
    assert_eq!(peer.queries.len(), 1);
    assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Reusable)).count(), 1);
    let client::Event::Completed { owner, completion } = take_terminal(&mut peer) else {
        panic!("actual scripted successful terminal");
    };
    assert_eq!(owner, OWNER);
    (context, peer, owner, completion)
}

#[test]
fn completed_owner_is_preserved_and_a_different_owner_is_refused() {
    for configuration in &wire::configurations() {
        for wrong_owner in [false, true] {
            let (context, mut peer, owner, completion) = completed(configuration, false);
            let returned =
                adapter::completion(context, if wrong_owner { OTHER } else { owner }, completion, Box::new([]));
            if wrong_owner {
                assert!(matches!(returned, Err(adapter::Error::Invalid)));
            } else {
                let Ok(Event::Completed { owner, completion }) = returned else {
                    panic!("the successful terminal translates once");
                };
                assert_eq!(owner, OWNER);
                assert_eq!(completion.stop, llm::Stop::EndTurn);
                assert_eq!(completion.usage.output_tokens, Some(7));
                let [llm::Said::Text { text, .. }] = completion.content.as_ref() else {
                    panic!("literal actual text completion");
                };
                assert_eq!(text.as_ref(), b"admitted exact");
            }
            close_won(&mut peer);
        }
    }
}

// The pinned fake provider emits HTTP Service Unavailable for both OpenAI routes.
const OPENAI_OVERLOADED_STATUS: u16 = 503;

// The pinned fake provider emits Anthropic's overloaded status, with no exported constant.
const ANTHROPIC_OVERLOADED_STATUS: u16 = 529;

fn overloaded_status(configuration: &Configuration) -> u16 {
    match configuration.endpoint.provider {
        shared::Provider::OpenAi | shared::Provider::OpenAiCodex => OPENAI_OVERLOADED_STATUS,
        shared::Provider::Anthropic => ANTHROPIC_OVERLOADED_STATUS,
    }
}

fn failed(configuration: &Configuration) -> (Context, Exchange, client::Event) {
    let (context, mut peer) = adopt(input(configuration), false);
    peer.manual_replies = true;
    peer.start();
    quiesce(&mut peer);
    assert_eq!(peer.pending.len(), 1, "one actual HTTP request issued a domain reply right");
    let config = fake::Config { overloaded: 1000, ..skein_llm_world::fake::config() };
    let mut domain = fake::Domain::new(&config, 7);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: config };
    let mut out = Queue::uncapped();
    fake::step(&mut domain, &env, peer.pending.pop().expect("actual independent peer request"), &mut out);
    assert!(out.pop().is_none(), "the actual fake decision awaits its timer entrance");
    fake::fire(&mut domain, &env, &mut out);
    peer.reply(out.pop().expect("actual configured fake overloaded terminal"));
    assert!(out.pop().is_none());
    domain.reclaim();
    assert_eq!(domain.calls(), 0);
    assert_eq!(domain.next_deadline(), None);
    peer.run();
    let terminal = take_terminal(&mut peer);
    assert!(matches!(
        &terminal,
        client::Event::Failed {
            owner,
            failure: shared::Failure::Overloaded,
            evidence: client::Evidence::Response { status },
            report: _,
            detail
        } if *owner == OWNER && *status == overloaded_status(configuration) && detail.as_ref() == b"overloaded_error"
    ));
    (context, peer, terminal)
}

#[test]
fn failed_owner_class_evidence_and_detail_survive_only_the_matching_context() {
    for configuration in &wire::configurations() {
        for wrong_owner in [false, true] {
            let (context, mut peer, terminal) = failed(configuration);
            let client::Event::Failed { owner, failure, evidence, report, detail } = terminal else {
                panic!("actual byte-peer failure established above");
            };
            let returned =
                adapter::failed(context, if wrong_owner { OTHER } else { owner }, failure, evidence, detail, report);
            if wrong_owner {
                assert!(matches!(returned, Err(adapter::Error::Invalid)));
            } else {
                let Ok(Event::Failed { owner, failure, evidence, detail, .. }) = returned else {
                    panic!("failure translates once");
                };
                assert_eq!(owner, OWNER);
                assert_eq!(failure, llm::Failure::Overloaded);
                assert_eq!(evidence, llm::Evidence::Response { status: Some(overloaded_status(configuration)) });
                assert_eq!(detail.as_ref(), b"overloaded_error");
            }
            close_won(&mut peer);
        }
    }
}

#[test]
fn cancelled_owner_is_checked_only_after_genuine_lower_settlement() {
    for configuration in &wire::configurations() {
        for wrong_owner in [false, true] {
            let (context, mut peer) = adopt(input(configuration), false);
            peer.start();
            peer.request(client::Request::Cancel);
            assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Close)).count(), 1);
            assert!(!peer.seen.iter().any(|event| matches!(event, client::Event::Cancelled { .. })));
            peer.settle();
            let client::Event::Cancelled { owner } = take_terminal(&mut peer) else {
                panic!("lower settlement yields the native cancellation terminal");
            };
            assert_eq!(owner, OWNER);
            let returned = adapter::cancelled(context, if wrong_owner { OTHER } else { owner });
            if wrong_owner {
                assert!(matches!(returned, Err(adapter::Error::Invalid)));
            } else {
                assert!(matches!(returned, Ok(Event::Cancelled { owner }) if owner == OWNER));
            }
            assert_eq!(peer.seen.iter().filter(|event| matches!(event, client::Event::Closed)).count(), 1);
            let events = peer.seen.len();
            peer.settle();
            assert_eq!(peer.seen.len(), events, "no second cancellation or close terminal");
            assert_eq!(peer.machine.waiting(), client::Waiting::Nothing);
        }
    }
}

#[test]
fn schema_inventory_admits_first_then_refuses_missing_duplicate_and_unoffered_entries() {
    for configuration in &wire::configurations() {
        let (context, mut peer, owner, completion) = completed(configuration, false);
        let [query] = peer.queries.as_slice() else { panic!("one observed actual query") };
        let [declaration] = query.tools.as_ref() else { panic!("one actual offered application schema") };
        assert_eq!(declaration.name.as_ref(), b"wait");
        assert_eq!(declaration.description.as_ref(), b"Boundary caller declaration.");
        assert_eq!(declaration.parameters.as_ref(), WAIT_SCHEMA);
        assert!(matches!(adapter::completion(context, owner, completion, Box::new([])), Ok(Event::Completed { .. })));
        close_won(&mut peer);

        let mut missing = input(configuration);
        missing.application = Box::new([]);
        assert!(matches!(adapter::prepare(missing, &limits()), Err(adapter::Error::Invalid)));
        let mut duplicate = input(configuration);
        duplicate.application = Box::new([schema(ToolKind::Wait, b"wait"), schema(ToolKind::Wait, b"wait")]);
        assert!(matches!(adapter::prepare(duplicate, &limits()), Err(adapter::Error::Invalid)));
        let mut unoffered = input(configuration);
        unoffered.application =
            Box::new([schema(ToolKind::Wait, b"wait"), schema(ToolKind::Owned(tools::Tool::Shell), b"shell")]);
        assert!(matches!(adapter::prepare(unoffered, &limits()), Err(adapter::Error::Invalid)));
    }
}

fn result_input(configuration: &Configuration) -> Input {
    let mut input = input(configuration);
    input.prompt.tools.shell = true;
    input.application = Box::new([
        schema(ToolKind::Wait, b"wait"),
        schema(ToolKind::Owned(tools::Tool::Shell), b"shell"),
        schema(ToolKind::Owned(tools::Tool::Job), b"job"),
    ]);
    input.prompt.messages = Box::new([
        llm::Message {
            role: llm::Role::Assistant,
            content: Box::new([llm::Block::ToolCall {
                id: b"prior-shell".as_slice().into(),
                name: b"shell".as_slice().into(),
                input: br#"{"command":"echo prior"}"#.as_slice().into(),
                replay: None,
            }]),
        },
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"prior-shell".as_slice().into(),
                result: llm::Returned::Owned {
                    outcome: tools::Outcome::Exited {
                        exit: tools::Exit::Code { code: 0 },
                        head: b"prior".as_slice().into(),
                        tail: Box::new([]),
                        dropped: 0,
                        timeout: tools::TimeoutClamp::Within,
                        output: tools::BytesClamp::Within,
                    },
                },
            }]),
        },
    ]);
    input
}

#[test]
fn owned_results_use_the_production_renderer_before_the_request_goes() {
    for configuration in &wire::configurations() {
        let (context, mut peer) = adopt(result_input(configuration), false);
        peer.start();
        peer.run();
        let [query] = peer.queries.as_slice() else { panic!("one query") };
        let [_, user] = query.messages.as_ref() else { panic!("ordered history") };
        let [api::Part::ToolOutput { id, output, is_error }] = user.parts.as_ref() else { panic!("result") };
        assert_eq!(id.as_ref(), b"prior-shell");
        assert!(output.starts_with(b"exit code 0\nprior"));
        assert!(!*is_error);
        let client::Event::Completed { owner, completion } = take_terminal(&mut peer) else { panic!("completion") };
        assert!(matches!(adapter::completion(context, owner, completion, Box::new([])), Ok(Event::Completed { .. })));
        close_won(&mut peer);
    }
}

fn resolution() -> ResolvedCall {
    ResolvedCall {
        position: 1,
        name: b"wait".as_slice().into(),
        input: b"{}".as_slice().into(),
        call: llm::Decoded::Served { ask: run::Ask::Wait },
    }
}

#[derive(Clone, Copy, Debug)]
enum Attestation {
    Exact,
    Missing,
    Position,
    Name,
    Input,
    Kind,
    Duplicate,
    Unused,
}

fn resolutions(case: Attestation) -> Box<[ResolvedCall]> {
    let mut exact = resolution();
    match case {
        Attestation::Exact => {}
        Attestation::Missing => return Box::new([]),
        Attestation::Position => exact.position = 0,
        Attestation::Name => exact.name = b"another-name".as_slice().into(),
        Attestation::Input => exact.input = b"{".as_slice().into(),
        Attestation::Kind => {
            exact.call = llm::Decoded::Owned {
                call: tools::Call::Shell {
                    after: smith_domain_tools::Yield::Never,
                    command: Box::new([]),
                    timeout: tools::Timeout::Default,
                    output: tools::Budget::Default,
                },
            };
        }
        Attestation::Duplicate => return Box::new([resolution(), resolution()]),
        Attestation::Unused => {
            let mut unused = resolution();
            unused.position = 2;
            return Box::new([exact, unused]);
        }
    }
    Box::new([exact])
}

#[test]
fn call_attestation_checks_position_name_literal_input_kind_uniqueness_and_consumption() {
    for configuration in &wire::configurations() {
        for case in [
            Attestation::Exact,
            Attestation::Missing,
            Attestation::Position,
            Attestation::Name,
            Attestation::Input,
            Attestation::Kind,
            Attestation::Duplicate,
            Attestation::Unused,
        ] {
            let (context, mut peer, owner, completion) = completed(configuration, true);
            let [
                shared::Block::Text { text: first, .. },
                shared::Block::ToolCall { id, name, arguments, .. },
                shared::Block::Text { text: last, .. },
            ] = completion.content.as_ref()
            else {
                panic!("the native call has its independent text/call/text positions");
            };
            assert_eq!(first.as_ref(), b"before");
            assert_eq!(last.as_ref(), b"after");
            assert_eq!(name.as_ref(), b"wait");
            assert_eq!(arguments.as_ref(), b"{}");
            assert_eq!(id.as_ref(), b"call_0000000000000001");
            let returned = adapter::completion(context, owner, completion, resolutions(case));
            match case {
                Attestation::Exact => {
                    let Ok(Event::Completed { owner, completion }) = returned else {
                        panic!("the exact Wait attestation is admitted first");
                    };
                    assert_eq!(owner, OWNER);
                    assert_eq!(completion.stop, llm::Stop::ToolUse);
                    assert_eq!(completion.usage.output_tokens, Some(7));
                    let [
                        llm::Said::Text { text: first, .. },
                        llm::Said::ToolCall { id, name, input, call, .. },
                        llm::Said::Text { text: last, .. },
                    ] = completion.content.as_ref()
                    else {
                        panic!("adapter preserves original positions and fields");
                    };
                    assert_eq!(first.as_ref(), b"before");
                    assert_eq!(last.as_ref(), b"after");
                    assert_eq!(id.as_ref(), b"call_0000000000000001");
                    assert_eq!(name.as_ref(), b"wait");
                    assert_eq!(input.as_ref(), b"{}");
                    assert!(matches!(call, llm::Decoded::Served { ask: run::Ask::Wait }));
                }
                Attestation::Missing
                | Attestation::Position
                | Attestation::Name
                | Attestation::Input
                | Attestation::Kind
                | Attestation::Duplicate
                | Attestation::Unused => {
                    assert!(matches!(returned, Err(adapter::Error::Invalid)), "bad attestation {case:?}");
                }
            }
            close_won(&mut peer);
        }
    }
}

#[test]
fn a_result_past_its_bound_is_cut_with_its_marker_and_the_request_goes() {
    for configuration in &wire::configurations() {
        let mut input = result_input(configuration);
        let byte_count = usize::try_from(limits().rendered_result).expect("u32 fits usize") * 2;
        input.prompt.messages[1].content[0] = llm::Block::ToolResult {
            id: b"prior-shell".as_slice().into(),
            result: llm::Returned::Owned {
                outcome: tools::Outcome::Exited {
                    exit: tools::Exit::Code { code: 0 },
                    head: vec![b'x'; byte_count].into(),
                    tail: Box::new([]),
                    dropped: 0,
                    timeout: tools::TimeoutClamp::Within,
                    output: tools::BytesClamp::Within,
                },
            },
        };
        let (context, mut peer) = adopt(input, false);
        peer.start();
        peer.run();
        let [query] = peer.queries.as_slice() else { panic!("one request reached the peer") };
        let [_, user] = query.messages.as_ref() else { panic!("ordered history") };
        let [api::Part::ToolOutput { id, output, is_error }] = user.parts.as_ref() else { panic!("one result") };
        assert_eq!(id.as_ref(), b"prior-shell");
        assert!(!*is_error);
        assert!(output.ends_with(b" bytes omitted]"));
        assert!(output.len() <= usize::try_from(limits().rendered_result).expect("u32 fits usize"));
        let marker = output.windows(2).position(|bytes| bytes == b"\n[").expect("cut marker");
        let omitted = std::str::from_utf8(&output[marker + 2..])
            .expect("ASCII marker")
            .split(' ')
            .next()
            .expect("byte count")
            .parse::<usize>()
            .expect("decimal omitted count");
        assert_eq!(omitted + marker, byte_count + b"exit code 0\n".len());
        let client::Event::Completed { owner, completion } = take_terminal(&mut peer) else { panic!("completion") };
        assert!(matches!(adapter::completion(context, owner, completion, Box::new([])), Ok(Event::Completed { .. })));
        close_won(&mut peer);
    }
}

fn child_completed(configuration: &Configuration, arguments: &[u8]) -> (Context, Exchange, Token, shared::Completion) {
    let mut offered = input(configuration);
    offered.prompt.served = Box::new([llm::Served::SubAgent]);
    offered.application = adapter::schemas(&offered.prompt, &limits().stated);
    let schema = offered.application[0].schema.clone();
    let endpoint = offered.endpoint.clone();
    let credential = shared::Credential {
        value: offered.credential.value.clone(),
        account_id: offered.credential.account_id.clone(),
        scheme: skein_llm::Scheme::Bearer,
    };
    let adapter::Prepared { client, context } = adapter::prepare(offered, &limits()).expect("actual admission");
    let script = Box::new([api::Script {
        cue: b"@adapter-boundary".as_slice().into(),
        turns: Box::new([api::Turn {
            lines: Box::new([api::Line::Call { name: b"sub_agent".as_slice().into(), arguments: arguments.into() }]),
            finish: api::Finish::ToolCalls,
            tokens: 7,
        }]),
    }]);
    let mut peer = Exchange::prepared(client, endpoint, Some(credential), limits().client, script);
    peer.start();
    peer.run();
    let [query] = peer.queries.as_slice() else { panic!("one actual provider query") };
    let [tool] = query.tools.as_ref() else { panic!("one offered child tool") };
    assert_eq!(tool.name.as_ref(), b"sub_agent");
    assert_eq!(tool.parameters, schema, "the provider sees the complete production schema");
    for name in [b"max_turns".as_slice(), b"max_seconds"] {
        assert!(tool.parameters.windows(name.len()).any(|part| part == name));
    }
    assert!(!tool.parameters.windows(b"maximum".len()).any(|part| part == b"maximum"));
    let client::Event::Completed { owner, completion } = take_terminal(&mut peer) else {
        panic!("one actual provider completion");
    };
    (context, peer, owner, completion)
}

#[test]
fn optional_child_shares_cross_both_provider_schemas_and_actual_completions() {
    use skein_lib::Duration;

    let cases = [
        (br#"{"brief":"inspect","tools":["inspect"]}"#.as_slice(), Some(run::Share::UNASKED), b"".as_slice()),
        (
            br#"{"brief":"inspect","tools":["inspect"],"max_turns":1}"#,
            Some(run::Share { turns: Some(1), time: None }),
            b"",
        ),
        (
            br#"{"brief":"inspect","tools":["inspect"],"max_seconds":2}"#,
            Some(run::Share { turns: None, time: Some(Duration::from_secs(2)) }),
            b"",
        ),
        (
            br#"{"brief":"inspect","tools":["inspect"],"max_turns":4294967295,"max_seconds":4294967295}"#,
            Some(run::Share { turns: Some(u32::MAX), time: Some(Duration::from_secs(u64::from(u32::MAX))) }),
            b"",
        ),
        (br#"{"brief":"inspect","tools":["inspect"],"max_turns":0}"#, None, b"max_turns"),
        (br#"{"brief":"inspect","tools":["inspect"],"max_turns":1.5}"#, None, b"max_turns"),
        (br#"{"brief":"inspect","tools":["inspect"],"max_seconds":0}"#, None, b"max_seconds"),
        (br#"{"brief":"inspect","tools":["inspect"],"max_seconds":1.5}"#, None, b"max_seconds"),
    ];
    for configuration in &wire::configurations() {
        for (arguments, expected, field) in cases {
            let (context, mut peer, owner, completion) = child_completed(configuration, arguments);
            let [shared::Block::ToolCall { name, arguments: received, .. }] = completion.content.as_ref() else {
                panic!("the provider retains the child call");
            };
            assert_eq!(received.as_ref(), arguments);
            let resolved = Box::new([ResolvedCall {
                position: 0,
                name: name.clone(),
                input: received.clone(),
                call: adapter::decode(
                    name,
                    received,
                    context.grants(),
                    context.served(),
                    context.asker(),
                    &context.limits(),
                ),
            }]);
            let Event::Completed { completion, .. } =
                adapter::completion(context, owner, completion, resolved).expect("actual checked adapter callback")
            else {
                panic!("the actual call has one root completion");
            };
            let [llm::Said::ToolCall { input, call, .. }] = completion.content.as_ref() else {
                panic!("the root retains the original call")
            };
            assert_eq!(input.as_ref(), arguments);
            match expected {
                Some(expected) => {
                    let llm::Decoded::Served { ask: run::Ask::SubAgent { share, .. } } = call else {
                        panic!("valid child quota is admitted: {call:?}")
                    };
                    assert_eq!(*share, expected);
                }
                None => {
                    assert_eq!(
                        *call,
                        llm::Decoded::Invalid { problem: llm::Problem::BadValue { field: field.into() } }
                    );
                }
            }
            close_won(&mut peer);
        }
    }
}

#[test]
fn a_prompt_takes_its_sessions_effort_and_none_for_the_provider_default() {
    let bounds = limits();
    let application = schema(ToolKind::Wait, b"wait");
    for effort in [None, Some(b"high".as_slice())] {
        let mut requested = prompt();
        requested.effort = effort.map(Into::into);
        let translated = smith_protocol_llm::prompt(requested, core::slice::from_ref(&application), &bounds)
            .expect("admitted prompt");
        assert_eq!(translated.reasoning_effort.as_deref(), effort);
    }
}
