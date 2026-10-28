//! Application translation over one actual shared Client and scripted byte peer.
//! These are boundary controls; the root composition uses the same wire helper.
//! Incoming streamed argument bytes are checked exactly at the root. Continued
//! native embedded objects use handwritten outside wire expectations: whitespace
//! serialization preserves the complete nested value and every extra field.
use skein_fake_llm_domain::api::{Finish, Line, Part, Script, Turn};
use skein_lib::{Duration, Token};
use smith_domain::{Event, llm, run, tools};
use smith_protocol_llm_world::adapter::{self as adapter, Limits, Receiving};
use smith_protocol_llm_world::wire::{self, Configuration, Observed, Wire};

const BODY: &[u8] = br#"{ "opaque" : {"future":[1,true,null]}, "extra":"unchanged" }"#;
const SCHEMA: &[u8] = br#"{"type":"object","properties":{"opaque":{"type":"object"}},"required":["opaque"],"x-caller":{"nested":[1,null,true]}}"#;
const FEEDBACK: &[u8] = b"exact host receipt\nsecond line";

fn limits() -> Limits {
    Limits {
        client: skein_llm_world::limits(),
        tool_bytes: 32768,
        history_bytes: u64::from(skein_llm_world::limits().request),
        rendered_result: skein_llm_world::limits().strings,
        stated: smith_protocol_llm_world::adapter::stated(),
    }
}

fn receiving(bounds: &Limits) -> Receiving {
    let decoded_call_bytes = 4096;
    Receiving {
        max_completion_bytes: adapter::completion_worst_case(&bounds.client, decoded_call_bytes)
            .expect("full translated completion reservation"),
        max_completion_blocks: bounds.client.output_items,
        decoded_call_bytes,
        max_failure_bytes: bounds.client.detail_bytes,
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
        system: b"@client-host".as_slice().into(),
        tools: tools::Grants { inspect: false, modify: false, shell: false },
        served: Box::new([llm::Served::Host(run::HostTool {
            name: b"opaque_host".as_slice().into(),
            description: b"Whole caller host contract.".as_slice().into(),
            schema: SCHEMA.into(),
            effect: run::HostEffect::Write,
            timeout: Duration::from_secs(3),
        })]),
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text {
                text: b"Perform the configured host operation.".as_slice().into(),
                replay: None,
            }]),
        }]),
        max_tokens: 128,
    }
}

fn scripts() -> Box<[Script]> {
    Box::new([Script {
        cue: b"@client-host".as_slice().into(),
        turns: Box::new([
            Turn {
                lines: Box::new([Line::Call { name: b"opaque_host".as_slice().into(), arguments: BODY.into() }]),
                finish: Finish::ToolCalls,
                tokens: 8,
            },
            Turn {
                lines: Box::new([Line::Text { text: b"actual continued result".as_slice().into() }]),
                finish: Finish::Stop,
                tokens: 4,
            },
        ]),
    }])
}

fn run_wire(prompt: llm::Prompt, configuration: &Configuration, owner: u64) -> (Wire, Event) {
    let bounds = limits();
    let mut wire = Wire::prepare(Token::new(owner), prompt, receiving(&bounds), configuration, &bounds, scripts())
        .expect("whole prompt and receiving configuration admitted before actual Client effects");
    let mut terminals = wire.start();
    for _ in 0..100_000 {
        let (progress, returned) = wire.tick();
        terminals.extend(returned);
        if !progress {
            break;
        }
    }
    let [terminal] = terminals.as_slice() else {
        panic!("one actual translated terminal");
    };
    assert!(matches!(terminal, Event::Completed { .. } | Event::Failed { .. } | Event::Cancelled { .. }));
    let terminal = terminals.pop().expect("one actual terminal");
    assert_eq!(wire.peer.machine.waiting(), skein_llm::client::Waiting::Idle);
    assert!(wire.observed.contains(&Observed::Reusable));
    assert!(wire.close().is_empty(), "closing a won terminal never adds another root terminal");
    assert!(wire.settle().is_empty());
    assert!(wire.settle().is_empty());
    assert_eq!(wire.observed.iter().filter(|observed| **observed == Observed::Closed).count(), 1);
    (wire, terminal)
}

fn feedback(
    query: &skein_fake_llm_domain::api::Query,
    expected_id: &[u8],
    expected_arguments: &[u8],
    expected_text: &[u8],
    expected_error: bool,
) -> bool {
    let [_, assistant, user] = query.messages.as_ref() else {
        return false;
    };
    let [Part::ToolCall { id, name, arguments }] = assistant.parts.as_ref() else {
        return false;
    };
    let [Part::ToolOutput { id: result_id, output, is_error }] = user.parts.as_ref() else {
        return false;
    };
    id.as_ref() == expected_id
        && result_id.as_ref() == expected_id
        && name.as_ref() == b"opaque_host"
        && arguments.as_ref() == expected_arguments
        && output.as_ref() == expected_text
        && *is_error == expected_error
}

fn conveys(configuration: &Configuration, error: bool) {
    let (first, terminal) = run_wire(prompt(), configuration, 17);
    let [query] = first.peer.queries.as_slice() else {
        panic!("one actual observed wire query");
    };
    let [tool] = query.tools.as_ref() else {
        panic!("one whole actual host descriptor");
    };
    assert_eq!(tool.name.as_ref(), b"opaque_host");
    assert_eq!(tool.description.as_ref(), b"Whole caller host contract.");
    assert!(
        tool.parameters
            .windows(br#""x-caller":{"nested":[1,null,true]}"#.len())
            .any(|bytes| bytes == br#""x-caller":{"nested":[1,null,true]}"#),
        "independent literal observation retains the compact schema extension"
    );
    assert_eq!(
        skein_llm::Json::from_bytes(&tool.parameters, &limits().client.native().request_document())
            .expect("actual full schema"),
        skein_llm::Json::from_bytes(SCHEMA, &limits().client.native().request_document())
            .expect("independent complete expected schema")
    );
    let Event::Completed { owner, completion } = terminal else {
        panic!("actual host call completion");
    };
    assert_eq!(owner, Token::new(17));
    assert_eq!(completion.stop, llm::Stop::ToolUse);
    let content = completion.content;
    let [llm::Said::ToolCall { id, name, input, call, replay }] = content.as_ref() else {
        panic!("one actual host tool call");
    };
    let llm::Decoded::Served { ask: run::Ask::Host { tool, effect, input: host_input } } = call else {
        panic!("actual declared host operation");
    };
    assert_eq!(tool.as_ref(), b"opaque_host");
    assert_eq!(*effect, run::HostEffect::Write);
    assert_eq!(host_input.bytes(), BODY);
    assert_eq!(input.as_ref(), BODY);
    let mut next = prompt();
    next.messages = Box::new([
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text {
                text: b"Perform the configured host operation.".as_slice().into(),
                replay: None,
            }]),
        },
        llm::Message {
            role: llm::Role::Assistant,
            content: Box::new([llm::Block::ToolCall {
                id: id.clone(),
                name: name.clone(),
                input: input.clone(),
                replay: replay.clone(),
            }]),
        },
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: id.clone(),
                result: llm::Returned::Text { text: FEEDBACK.into(), error, replay: None },
            }]),
        },
    ]);
    let (second, terminal) = run_wire(next, configuration, 18);
    let [query] = second.peer.queries.as_slice() else {
        panic!("one actual continued wire query");
    };
    let expected_text =
        if error { [configuration.error_prefix.as_ref(), FEEDBACK].concat() } else { FEEDBACK.to_vec() };
    let expected_error = error && configuration.error_flag;
    assert!(
        feedback(query, id, &configuration.continuation_arguments, &expected_text, expected_error),
        "positive actual whole input and exact paired feedback: provider={:?}, error={error}, expected_id={id:?}, expected_text={expected_text:?}, expected_error={expected_error}, actual_query={query:?}",
        configuration.endpoint.provider
    );
    let mut missing = query.clone();
    missing.messages.last_mut().expect("actual user message").parts = Box::new([]);
    assert!(
        !feedback(&missing, id, &configuration.continuation_arguments, &expected_text, expected_error),
        "missing concrete host feedback cannot satisfy outside evidence"
    );
    let mut changed = query.clone();
    let [Part::ToolOutput { output, .. }] = changed.messages.last_mut().expect("actual user message").parts.as_mut()
    else {
        panic!("actual result");
    };
    *output = b"rewritten host receipt".as_slice().into();
    assert!(
        !feedback(&changed, id, &configuration.continuation_arguments, &expected_text, expected_error),
        "rewritten feedback cannot satisfy outside evidence"
    );
    corrupted_arguments(query, id, &configuration.continuation_arguments, &expected_text, expected_error);
    let Event::Completed { owner, completion } = terminal else {
        panic!("actual normal continuation");
    };
    assert_eq!(owner, Token::new(18));
    let [llm::Said::Text { text, .. }] = completion.content.as_ref() else {
        panic!("actual continued text");
    };
    assert_eq!(text.as_ref(), b"actual continued result");
}

fn corrupted_arguments(
    query: &skein_fake_llm_domain::api::Query,
    expected_id: &[u8],
    expected_arguments: &[u8],
    expected_text: &[u8],
    expected_error: bool,
) {
    for drop_extra in [false, true] {
        let mut changed = query.clone();
        let [Part::ToolCall { arguments, .. }] = changed.messages[1].parts.as_mut() else {
            panic!("positive control established the actual assistant call");
        };
        let mut bytes = arguments.clone().into_vec();
        if drop_extra {
            let field = br#""extra":"unchanged""#;
            let start = bytes.windows(field.len()).position(|window| window == field).expect("actual extra field");
            let comma = bytes[..start].iter().rposition(|byte| *byte == b',').expect("extra field separator");
            drop(bytes.drain(comma..start + field.len()));
        } else {
            let nested = b"[1,true,null]";
            let start = bytes.windows(nested.len()).position(|window| window == nested).expect("actual nested value");
            bytes[start + 1] = b'2';
        }
        *arguments = bytes.into();
        assert!(
            !feedback(&changed, expected_id, expected_arguments, expected_text, expected_error),
            "changed nested value or dropped extra field cannot satisfy the exact outside continuation oracle"
        );
    }
}

#[test]
fn both_shared_configurations_convey_whole_host_contract_input_and_exact_feedback() {
    for configuration in &wire::configurations() {
        for error in [false, true] {
            conveys(configuration, error);
        }
    }
}

#[test]
fn invalid_limits_are_refused() {
    let bounds = limits();
    let accepted = receiving(&bounds);
    assert!(adapter::valid(&bounds, &accepted));
    smith_protocol_llm::prepare_prompt(Token::new(1), prompt(), Box::new([]), accepted, &bounds)
        .expect("valid limits prepare a prompt");
    let mut cases = Vec::new();
    let mut invalid = bounds;
    invalid.rendered_result = smith_protocol_llm::CUT_MARKER_BYTES - 1;
    cases.push(("rendered result", invalid, accepted));
    let mut invalid = accepted;
    invalid.max_completion_bytes -= 1;
    cases.push(("completion bytes", bounds, invalid));
    let mut invalid = accepted;
    invalid.max_completion_blocks -= 1;
    cases.push(("completion blocks", bounds, invalid));
    let mut invalid = accepted;
    invalid.max_failure_bytes -= 1;
    cases.push(("failure bytes", bounds, invalid));
    let mut invalid = accepted;
    invalid.decoded_call_bytes = 0;
    cases.push(("decoded cell floor", bounds, invalid));
    let mut invalid = bounds;
    invalid.tool_bytes = u64::from(u32::MAX) + 1;
    cases.push(("outgoing document bytes", invalid, accepted));
    let mut invalid = bounds;
    invalid.client.reasoning = u32::MAX;
    cases.push(("replay representability", invalid, accepted));
    for (check, bounds, receiving) in cases {
        assert!(!adapter::valid(&bounds, &receiving), "{check}");
        assert!(
            matches!(
                smith_protocol_llm::prepare_prompt(Token::new(1), prompt(), Box::new([]), receiving, &bounds),
                Err(adapter::Error::Limit)
            ),
            "{check}"
        );
    }
    let mut large = bounds;
    large.history_bytes = u64::MAX;
    assert!(adapter::valid(&large, &accepted), "aggregate heap arithmetic is not validity");
}

#[test]
fn receiving_incompatibility_refuses_before_client_effects() {
    let bounds = limits();
    let mut receiving = receiving(&bounds);
    receiving.max_completion_bytes -= 1;
    for configuration in &wire::configurations() {
        assert!(matches!(
            Wire::prepare(Token::new(1), prompt(), receiving, configuration, &bounds, scripts()),
            Err(adapter::Error::Limit)
        ));
    }
}

fn restored_result(replay: Option<llm::Replay>) -> llm::Prompt {
    let mut restored = prompt();
    restored.messages = Box::new([
        llm::Message {
            role: llm::Role::Assistant,
            content: Box::new([llm::Block::ToolCall {
                id: b"actual-call".as_slice().into(),
                name: b"opaque_host".as_slice().into(),
                input: BODY.into(),
                replay: None,
            }]),
        },
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"actual-call".as_slice().into(),
                result: llm::Returned::Text { text: FEEDBACK.into(), error: false, replay },
            }]),
        },
    ]);
    restored
}

#[test]
fn restored_result_metadata_has_explicit_unsupported_refusal_before_start() {
    let bounds = limits();
    for configuration in &wire::configurations() {
        let mut positive =
            Wire::prepare(Token::new(19), restored_result(None), receiving(&bounds), configuration, &bounds, scripts())
                .expect("ordinary concrete result is representable before any wire start");
        assert!(positive.peer.queries.is_empty());
        assert!(positive.close().is_empty());
        let actual = positive.settle();
        let [Event::Cancelled { owner }] = actual.as_slice() else {
            panic!("actual accepted prepared Client closes once");
        };
        assert_eq!(*owner, Token::new(19));
        assert!(positive.settle().is_empty());
        let opaque = skein_llm::Replay {
            provider: configuration.endpoint.provider,
            value: skein_llm::Json::from_bytes(
                br#"{"caller-result-extension":"retained"}"#,
                &bounds.client.native().request_document(),
            )
            .expect("bounded actual opaque object"),
        }
        .to_bytes(&bounds.client.native().request_document())
        .expect("complete caller-owned durable envelope");
        assert!(
            matches!(
                Wire::prepare(
                    Token::new(20),
                    restored_result(Some(llm::Replay { bytes: opaque })),
                    receiving(&bounds),
                    configuration,
                    &bounds,
                    scripts()
                ),
                Err(adapter::Error::Unsupported)
            ),
            "restored metadata cannot silently disappear into a ToolResult without replay support"
        );
    }
}

fn two_calls(
    configuration: &Configuration,
    bounds: &Limits,
    receiving: Receiving,
) -> (adapter::Context, skein_llm_world::fake::Exchange) {
    let mut input = prompt();
    input.served = Box::new([input.served[0].clone(), llm::Served::Finish]);
    let application = wire::schemas(&input, &bounds.stated);
    let endpoint = configuration.endpoint.clone();
    let credential = skein_llm::Credential {
        value: configuration.credential.value.clone(),
        account_id: configuration.credential.account_id.clone(),
        scheme: skein_llm::Scheme::Bearer,
    };
    let adapter::Prepared { client, context } = adapter::prepare(
        adapter::Input {
            owner: Token::new(31),
            endpoint_name: input.endpoint,
            prompt: input,
            endpoint: endpoint.clone(),
            credential: skein_llm::Credential {
                value: credential.value.clone(),
                account_id: credential.account_id.clone(),
                scheme: skein_llm::Scheme::Bearer,
            },
            application,
            receiving,
        },
        bounds,
    )
    .expect("admitted Client reserves every possible fixed decoded cell");
    let scripts = Box::new([Script {
        cue: b"@client-host".as_slice().into(),
        turns: Box::new([Turn {
            lines: Box::new([
                Line::Call { name: b"finish".as_slice().into(), arguments: b"{}".as_slice().into() },
                Line::Call { name: b"opaque_host".as_slice().into(), arguments: BODY.into() },
            ]),
            finish: Finish::ToolCalls,
            tokens: 8,
        }]),
    }]);
    let peer = skein_llm_world::fake::Exchange::prepared(client, endpoint, Some(credential), bounds.client, scripts);
    (context, peer)
}

fn owned_completion(peer: &mut skein_llm_world::fake::Exchange) -> (Token, skein_llm::Completion) {
    peer.start();
    peer.run();
    let mut actual: Vec<_> = core::mem::take(&mut peer.seen)
        .into_iter()
        .filter_map(|event| match event {
            skein_llm::client::Event::Completed { owner, completion } => Some((owner, completion)),
            skein_llm::client::Event::Failed { .. } | skein_llm::client::Event::Cancelled { .. } => {
                panic!("actual two-call native completion")
            }
            skein_llm::client::Event::Block { .. }
            | skein_llm::client::Event::Delta { .. }
            | skein_llm::client::Event::Reusable
            | skein_llm::client::Event::Close
            | skein_llm::client::Event::Closed => None,
        })
        .collect();
    assert_eq!(actual.len(), 1, "one Client terminal");
    let value = actual.pop().expect("one actual owned terminal");
    assert_eq!(value.0, Token::new(31));
    value
}

fn reservation_control(configuration: &Configuration, exact: bool) {
    let mut bounds = limits();
    bounds.client.output_items = 2;
    let cells = u64::try_from(core::mem::size_of::<llm::Decoded>()).expect("bounded fixed cell") * 2;
    let payload = u64::try_from(b"opaque_host".len() + BODY.len()).expect("bounded actual host payload");
    let decoded_call_bytes = cells + payload - u64::from(!exact);
    let receiving = Receiving {
        max_completion_bytes: adapter::completion_worst_case(&bounds.client, decoded_call_bytes)
            .expect("wrapper floor admitted before actual call"),
        max_completion_blocks: 2,
        decoded_call_bytes,
        max_failure_bytes: bounds.client.detail_bytes,
    };
    let (context, mut peer) = two_calls(configuration, &bounds, receiving);
    let (owner, actual_completion) = owned_completion(&mut peer);
    let resolved = Box::new([adapter::ResolvedCall {
        position: 0,
        name: b"finish".as_slice().into(),
        input: b"{}".as_slice().into(),
        call: llm::Decoded::Invalid { problem: llm::Problem::Missing { field: vec![b'x'; 4096].into() } },
    }]);
    let Event::Completed { completion, .. } = adapter::completion(context, owner, actual_completion, resolved)
        .expect("fallback wrappers stay in the secured aggregate before later host decoding")
    else {
        panic!("translated actual completion");
    };
    let [llm::Said::ToolCall { call: first, .. }, llm::Said::ToolCall { call: second, input, .. }] =
        completion.content.as_ref()
    else {
        panic!("both original actual call records retained");
    };
    assert!(matches!(first, llm::Decoded::Invalid { problem: llm::Problem::TooLarge }));
    assert_eq!(input.as_ref(), BODY);
    let total = first.owned_bytes().expect("fixed fallback is fully priced")
        + second.owned_bytes().expect("second decoded value priced");
    assert!(total <= decoded_call_bytes);
    if exact {
        assert!(
            matches!(second, llm::Decoded::Served { ask: run::Ask::Host { .. } }),
            "later host fits the exact residual after full fallback charge"
        );
        assert_eq!(total, decoded_call_bytes);
    } else {
        assert!(
            matches!(second, llm::Decoded::Invalid { problem: llm::Problem::TooLarge }),
            "one byte less cannot hide an unpriced earlier fallback"
        );
        assert_eq!(total, cells);
    }
    peer.request(skein_llm::client::Request::Close);
    peer.settle();
    peer.settle();
}

#[test]
fn oversized_first_decoding_retains_its_cell_before_exact_residual_host_admission() {
    for configuration in &wire::configurations() {
        reservation_control(configuration, true);
        reservation_control(configuration, false);
        let bounds = limits();
        assert!(adapter::completion_worst_case(&bounds.client, 0).is_none());
        let mut no_cells = receiving(&bounds);
        no_cells.decoded_call_bytes = 0;
        assert!(
            matches!(
                Wire::prepare(Token::new(32), prompt(), no_cells, configuration, &bounds, scripts()),
                Err(adapter::Error::Limit)
            ),
            "cap zero refuses before a Client could return even an unknown call's fixed Invalid cell"
        );
    }
}

#[test]
fn unknown_call_uses_its_full_secured_fallback_cell() {
    for configuration in &wire::configurations() {
        let mut bounds = limits();
        bounds.client.output_items = 1;
        let floor = u64::try_from(core::mem::size_of::<llm::Decoded>()).expect("fixed fallback cell");
        let secured = Receiving {
            max_completion_bytes: adapter::completion_worst_case(&bounds.client, floor).expect("full cell is secured"),
            max_completion_blocks: 1,
            decoded_call_bytes: floor,
            max_failure_bytes: bounds.client.detail_bytes,
        };
        let script = Box::new([Script {
            cue: b"@client-host".as_slice().into(),
            turns: Box::new([Turn {
                lines: Box::new([Line::Call {
                    name: b"not-offered".as_slice().into(),
                    arguments: b"{}".as_slice().into(),
                }]),
                finish: Finish::ToolCalls,
                tokens: 4,
            }]),
        }]);
        let mut wire = Wire::prepare(Token::new(33), prompt(), secured, configuration, &bounds, script)
            .expect("one actual unknown call has its fixed wrapper reserved before Client preparation");
        let mut returned = wire.start();
        for _ in 0..100_000 {
            let (progress, terminals) = wire.tick();
            returned.extend(terminals);
            if !progress {
                break;
            }
        }
        let [Event::Completed { completion, .. }] = returned.as_slice() else {
            panic!("one actual unknown call completion");
        };
        let [llm::Said::ToolCall { name, input, call, .. }] = completion.content.as_ref() else {
            panic!("full actual unknown call preserved");
        };
        assert_eq!(name.as_ref(), b"not-offered");
        assert_eq!(input.as_ref(), b"{}");
        assert!(matches!(call, llm::Decoded::Invalid { problem: llm::Problem::UnknownTool }));
        assert_eq!(call.owned_bytes(), Some(floor));
        assert!(wire.close().is_empty());
        assert!(wire.settle().is_empty());
        assert!(wire.settle().is_empty());
    }
}

#[test]
fn actual_requests_render_the_run_key_and_thread_only_for_codex() {
    for configuration in wire::configurations() {
        for (thread, expected_thread) in
            [(0, "42424242-4242-4242-4242-424242424242"), (0x0102_0304, "42424242-4242-4242-4242-424243404146")]
        {
            let mut prompt = prompt();
            prompt.affinity = llm::Affinity { key: [0x42; 16], thread };
            let (wire, terminal) = run_wire(prompt, &configuration, 200 + u64::from(thread));
            assert!(matches!(terminal, Event::Completed { .. }));
            let request = smith_protocol_llm_world::referee::NativeRequest::from_http(&wire.peer.requests);
            match configuration.endpoint.provider {
                skein_llm::Provider::OpenAi => panic!("fixture exercises the withdrawn routes"),
                skein_llm::Provider::OpenAiCodex => {
                    assert_eq!(
                        request.affinity,
                        Some([
                            "\"42424242-4242-4242-4242-424242424242\"".into(),
                            "42424242-4242-4242-4242-424242424242".into(),
                            expected_thread.into()
                        ])
                    );
                    assert_eq!(wire.peer.queries[0].caching, skein_fake_llm_domain::api::Caching::Scope([0x42; 16]));
                }
                skein_llm::Provider::Anthropic => {
                    assert_eq!(request.affinity, None);
                    assert!(!String::from_utf8_lossy(&wire.peer.requests).contains("session-id:"));
                    assert!(!String::from_utf8_lossy(&wire.peer.requests).contains("thread-id:"));
                }
            }
        }
    }
}

#[test]
fn only_uses_automatic_wire_choice_and_keeps_all_definitions_and_not_run_feedback() {
    for configuration in &wire::configurations() {
        let mut ordinary = prompt();
        ordinary.tools.inspect = true;
        let (auto, _) = run_wire(ordinary, configuration, 801);
        let mut final_prompt = prompt();
        final_prompt.tools.inspect = true;
        let descriptor = final_prompt.served[0].clone();
        final_prompt.choice = llm::ToolChoice::Only(Box::new([descriptor]));
        final_prompt.messages = Box::new([
            final_prompt.messages.into_vec().remove(0),
            llm::Message {
                role: llm::Role::Assistant,
                content: Box::new([llm::Block::ToolCall {
                    id: b"outside-call".as_slice().into(),
                    name: b"read".as_slice().into(),
                    input: br#"{"path":"data.txt"}"#.as_slice().into(),
                    replay: None,
                }]),
            },
            llm::Message {
                role: llm::Role::User,
                content: Box::new([llm::Block::ToolResult {
                    id: b"outside-call".as_slice().into(),
                    result: llm::Returned::NotRun {
                        why: llm::NotRun::OutsideChoice { choice: Some(Box::new([b"opaque_host".as_slice().into()])) },
                    },
                }]),
            },
        ]);
        let (only, _) = run_wire(final_prompt, configuration, 802);
        let ordinary = smith_protocol_llm_world::referee::NativeRequest::from_http(&auto.peer.requests);
        let narrowed = smith_protocol_llm_world::referee::NativeRequest::from_http(&only.peer.requests);
        assert_eq!(ordinary.tools, narrowed.tools, "narrowing never changes actual native definitions");
        let choice = |wire: &Wire| {
            let requests = skein_llm_world::identity(&wire.peer.requests);
            let body = requests.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("HTTP body");
            let body: serde_json::Value = serde_json::from_slice(&requests[body + 4..]).expect("JSON");
            body.get("tool_choice").cloned()
        };
        let automatic = choice(&auto);
        assert_eq!(choice(&only), automatic, "Only renders the exact same provider selection as Auto");
        assert!(
            automatic.is_none()
                || automatic == Some(serde_json::json!("auto"))
                || automatic == Some(serde_json::json!({"type":"auto"})),
            "the automatic provider mode may be its omitted default"
        );
        let [query] = only.peer.queries.as_slice() else { panic!("one actual request") };
        assert!(
            query.messages.iter().flat_map(|message| &message.parts).any(|part| {
                matches!(part, Part::ToolOutput { id, output, .. }
                if id.as_ref() == b"outside-call" && output.windows(b"not run: allowed calls now: opaque_host".len()).any(|part| part == b"not run: allowed calls now: opaque_host"))
            }),
            "actual replay tells the disallowed call which unchanged descriptor is permitted"
        );
    }
}

fn native_choice(wire: &Wire) -> Option<serde_json::Value> {
    let requests = skein_llm_world::identity(&wire.peer.requests);
    let body = requests.windows(4).position(|bytes| bytes == b"\r\n\r\n").expect("HTTP body");
    let body: serde_json::Value = serde_json::from_slice(&requests[body + 4..]).expect("JSON");
    body.get("tool_choice").cloned()
}

#[test]
fn a_compaction_asks_none_in_each_dialect_and_the_next_window_its_choice() {
    let choices =
        [(serde_json::json!("none"), Some(serde_json::json!("auto"))), (serde_json::json!({"type":"none"}), None)];
    for (configuration, (none, only)) in wire::configurations().iter().zip(choices) {
        let mut summary = prompt();
        summary.purpose = llm::Purpose::Compaction { room: 4096 };
        summary.choice = llm::ToolChoice::None;
        summary.tools.inspect = true;
        let (compaction, _) = run_wire(summary, configuration, 811);
        let mut next = prompt();
        next.tools.inspect = true;
        next.choice = llm::ToolChoice::Only(Box::new([next.served[0].clone()]));
        let (continued, _) = run_wire(next, configuration, 812);
        assert_eq!(native_choice(&compaction), Some(none));
        assert_eq!(native_choice(&continued), only);
        let old = smith_protocol_llm_world::referee::NativeRequest::from_http(&compaction.peer.requests);
        let new = smith_protocol_llm_world::referee::NativeRequest::from_http(&continued.peer.requests);
        assert_eq!(old.tools, new.tools);
        assert!(old.tools.is_some());
    }
}
