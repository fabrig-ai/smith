#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "ordinary Rust deterministic harness, programming-model.md 10.2"
)]
use super::lifecycle_tests::{backend, env, request};
use super::*;
use protocol::{Measure, Observation};
use smith_events::{Block, Role};

fn active(observe: bool) -> Backend {
    let mut backend = backend();
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
    let mut request = request(&backend);
    let FromDomain::Complete { prompt, observation, deltas, .. } = &mut request else { panic!("complete") };
    *observation = if observe { Some(Observation { first: true, messages: 0 }) } else { None };
    deltas.text = observe;
    prompt.system = b"system".as_slice().into();
    prompt.tools.inspect = true;
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"start text".as_slice().into(), replay: None }]),
    }]);
    backend.begin_prompt(&request);
    backend.request = Some(request);
    backend.owner = Some(Token::new(80));
    backend
}

#[test]
fn a_complete_measures_its_user_messages() {
    let mut backend = active(true);
    backend.first_prompt_measure(Time::ZERO);
    backend.flush_prompt_measure(Time::ZERO);
    let Some(Measure::Prompt { owner, prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    assert_eq!(owner, Token::new(80));
    assert_eq!(prompt.system.as_deref(), Some(b"system".as_slice()));
    assert_eq!(prompt.messages.get(0).unwrap().role, Role::User);
    assert_eq!(
        prompt.messages.get(0).unwrap().blocks.as_slice(),
        &[Block::Text { text: b"start text".as_slice().into() }]
    );
    assert!(prompt.tools.unwrap().iter().any(|name| name.as_ref() == b"Read"));
    let Some(FromDomain::Complete { observation, .. }) = &mut backend.request else { panic!("complete") };
    *observation = Some(Observation { first: false, messages: 2 });
    backend.prompt_due = true;
    backend.prompt_measure(Time::ZERO, None);
    backend.flush_prompt_measure(Time::ZERO);
    let Some(Measure::Prompt { prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    assert!(prompt.messages.is_empty());
    assert_eq!(prompt.system, None);
    assert_eq!(prompt.tools, None);
}

#[test]
fn a_partial_text_is_a_delta() {
    let mut backend = active(true);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"piece"}}}"#, &mut up, &mut below);
    let Some(Measure::Prompt { .. }) = backend.pop_measure() else { panic!("prompt first") };
    let Some(Measure::Delta { owner, block, text, .. }) = backend.pop_measure() else { panic!("delta") };
    assert_eq!(owner, Token::new(80));
    assert_eq!(block, 0);
    assert_eq!(text.as_ref(), b"piece");
}

#[test]
fn an_api_retry_is_a_retry_measure() {
    let mut backend = active(true);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(
        &env(0),
        br#"{"type":"system","subtype":"api_retry","error_status":503,"retry_delay_ms":250}"#,
        &mut up,
        &mut below,
    );
    let Some(Measure::Prompt { .. }) = backend.pop_measure() else { panic!("prompt first") };
    let Some(Measure::Retried { owner, status, retry_ms, .. }) = backend.pop_measure() else { panic!("retry") };
    assert_eq!(owner, Token::new(80));
    assert_eq!(status, Some(503));
    assert_eq!(retry_ms, Some(250));
    assert!(up.is_empty());
}

#[test]
fn no_measure_without_an_observation() {
    let mut backend = active(false);
    backend.first_prompt_measure(Time::ZERO);
    backend.prompt_measure(Time::ZERO, Some(b"not captured"));
    assert!(backend.pop_measure().is_none());
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"type":"assistant","message":{"id":"one","content":[{"type":"tool_use","id":"f","name":"mcp__smith__finish","input":{}}]}}"#, &mut up, &mut below);
    let Some(Measure::Terminal { .. }) = backend.pop_measure() else {
        panic!("terminal with absent transport observations")
    };
    assert!(backend.pop_measure().is_none());
}

#[test]
fn a_shared_stdout_read_waits_for_the_next_owner() {
    let mut backend = active(true);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    let bytes = br#"{"type":"assistant","message":{"id":"one","content":[{"type":"tool_use","id":"f","name":"mcp__smith__finish","input":{}}]}}
{"type":"system","subtype":"api_retry","error_status":503}
"#;
    for byte in bytes {
        backend.intake.push(*byte).unwrap();
    }
    backend.consume_stdout(&env(0), &mut up, &mut below);
    assert!(backend.owner.is_some());
    let Some(Measure::Prompt { .. }) = backend.pop_measure() else { panic!("prompt first") };
    backend.consume_stdout(&env(0), &mut up, &mut below);
    assert!(backend.owner.is_none());
    let Some(Measure::Completion { .. }) = backend.pop_measure() else { panic!("completion") };
    let Some(Measure::Terminal { .. }) = backend.pop_measure() else { panic!("terminal") };
    backend.consume_stdout(&env(0), &mut up, &mut below);
    assert!(backend.pop_measure().is_none());
    backend.owner = Some(Token::new(81));
    backend.consume_stdout(&env(0), &mut up, &mut below);
    let Some(Measure::Retried { owner, .. }) = backend.pop_measure() else { panic!("retry") };
    assert_eq!(owner, Token::new(81));
}

#[test]
fn a_continuation_is_measured_as_user_text() {
    let mut backend = active(true);
    backend.turns.result = true;
    let mut next = request(&backend);
    let FromDomain::Complete { observation, .. } = &mut next else { panic!("complete") };
    *observation = Some(Observation { first: false, messages: 2 });
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    backend.flush_prompt_measure(Time::ZERO);
    let Some(Measure::Prompt { prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    assert_eq!(
        prompt.messages.get(0).unwrap().blocks.as_slice(),
        &[Block::Text { text: b"Continue with the tool results.".as_slice().into() }]
    );
}

#[test]
fn a_wind_down_measures_the_encoded_user_text() {
    let mut backend = active(true);
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, observation, .. } = &mut next else { panic!("complete") };
    *observation = Some(Observation { first: false, messages: 2 });
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish]));
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"finish \"now\"\n".as_slice().into(), replay: None }]),
    }]);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    backend.line(
        &env(0),
        br#"{"type":"control_response","response":{"subtype":"success","request_id":"smith-1"}}"#,
        &mut up,
        &mut below,
    );
    backend.line(&env(0), br#"{"type":"result","subtype":"error_during_execution","is_error":true,"terminal_reason":"aborted_streaming"}"#, &mut up, &mut below);
    backend.flush_prompt_measure(Time::ZERO);
    let Some(Measure::Prompt { prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    let sent: serde_json::Value = serde_json::from_slice(backend.outgoing.as_ref().unwrap()).unwrap();
    let Block::Text { text } = prompt.messages.get(0).unwrap().blocks.get(0).unwrap() else { panic!("text") };
    assert_eq!(text.as_ref(), sent["message"]["content"].as_str().unwrap().as_bytes());
    assert_eq!(text.as_ref(), b"(The agent paused your last turn to send the finishing note.)\nfinish \"now\"\n\n");
}

#[test]
fn an_empty_prompt_defers_until_its_release() {
    let mut backend = active(true);
    backend.prompt_measure(env(1).now, None);
    assert!(backend.pop_measure().is_none());
    backend.prompt_measure(env(2).now, Some(b"later text"));
    backend.flush_prompt_measure(env(3).now);
    let Some(Measure::Prompt { at, prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    assert_eq!(at, env(3).now);
    assert_eq!(
        prompt.messages.get(0).unwrap().blocks.as_slice(),
        &[Block::Text { text: b"later text".as_slice().into() }]
    );
    assert!(backend.pop_measure().is_none());
}

#[test]
fn the_first_prompt_prices_every_program_tool() {
    let mut backend = active(true);
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else { panic!("request") };
    prompt.tools = smith_domain::tools::Grants { inspect: true, modify: true, shell: true };
    backend.first_prompt_measure(Time::ZERO);
    assert_eq!(backend.measures.room(), measures::capacity(&backend.settings).unwrap());
    backend.flush_prompt_measure(Time::ZERO);
    assert_eq!(backend.measures.room(), measures::capacity(&backend.settings).unwrap() - 1);
    let Some(Measure::Prompt { prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    let tools = prompt.tools.unwrap();
    assert_eq!(tools.len(), 7);
    for name in [b"Read".as_slice(), b"Glob", b"Grep", b"Write", b"Edit", b"Bash", b"permission_prompt"] {
        assert!(tools.iter().any(|tool| tool.as_ref() == name));
    }
    assert!(
        measures::worst_case(&backend.settings).unwrap() >= Queue::<Measure>::worst_case(measures::MEASURES).unwrap()
    );
}

fn delta(backend: &mut Backend, text: &[u8]) {
    let bytes = serde_json::to_vec(&serde_json::json!({"type":"stream_event","event":{
        "type":"content_block_delta","index":0_u32,"delta":{"type":"text_delta","text":core::str::from_utf8(text).unwrap()}
    }}))
    .unwrap();
    backend.line(&env(0), &bytes, &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
}

fn delta_text(backend: &mut Backend) -> alloc::vec::Vec<u8> {
    let mut text = alloc::vec::Vec::new();
    while let Some(measure) = backend.pop_measure() {
        if let Measure::Delta { text: piece, .. } = measure {
            text.extend_from_slice(&piece);
        }
    }
    text
}

#[test]
fn private_text_is_redacted_across_delta_boundaries() {
    for width in [1, 3, 64] {
        let mut backend = active(true);
        let secret = mcp::secret_text(backend.names.mcp.expose().try_into().unwrap());
        let mut text = alloc::vec::Vec::new();
        if width == 3 {
            delta(&mut backend, &secret[..3]);
            text.extend(delta_text(&mut backend));
            delta(&mut backend, &secret[3..]);
        } else {
            for piece in secret.chunks(width) {
                delta(&mut backend, piece);
                text.extend(delta_text(&mut backend));
            }
        }
        backend.line(
            &env(0),
            br#"{"type":"stream_event","event":{"type":"content_block_stop","index":0}}"#,
            &mut Queue::with_capacity(32),
            &mut Queue::with_capacity(64),
        );
        text.extend(delta_text(&mut backend));
        assert_eq!(text.len(), secret.len());
        assert!(!text.windows(secret.len()).any(|window| window == secret));
        assert!(text.iter().all(|byte| *byte == b'*'));
    }
}

#[test]
fn ordinary_delta_text_passes_without_delay() {
    let mut backend = active(true);
    delta(&mut backend, b"ordinary words!");
    assert_eq!(delta_text(&mut backend), b"ordinary words!");
    let secret = mcp::secret_text(backend.names.mcp.expose().try_into().unwrap());
    delta(&mut backend, &secret[..3]);
    assert!(delta_text(&mut backend).is_empty());
    delta(&mut backend, b"!");
    assert_eq!(delta_text(&mut backend), [secret[..3].as_ref(), b"!"].concat());
}

#[test]
fn private_carries_flush_at_every_terminal_boundary() {
    for boundary in 0_u32..5 {
        let mut backend = active(true);
        let secret = mcp::secret_text(backend.names.mcp.expose().try_into().unwrap());
        delta(&mut backend, &secret[..10]);
        assert!(delta_text(&mut backend).is_empty());
        let mut up = Queue::with_capacity(32);
        let mut below = Queue::with_capacity(64);
        match boundary {
            0 => backend.line(
                &env(1),
                br#"{"type":"stream_event","event":{"type":"message_stop"}}"#,
                &mut up,
                &mut below,
            ),
            1 => backend.line(
                &env(1),
                br#"{"type":"system","subtype":"api_retry","error_status":503}"#,
                &mut up,
                &mut below,
            ),
            2 => backend.close(&env(1), &mut up, &mut below),
            3 => backend.fail(&env(1), llm::Failure::Invalid, b"failure", &mut up, &mut below),
            4 => backend.stop(&env(1), true, true, &mut up, &mut below),
            _ => unreachable!(),
        }
        assert_eq!(delta_text(&mut backend), b"**********", "boundary {boundary}");
    }
}

#[test]
fn a_retry_discards_the_unfinished_message() {
    let mut backend = active(true);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    for line in [
        br#"{"type":"stream_event","event":{"type":"message_start","message":{"id":"a"}}}"#.as_slice(),
        br#"{"type":"stream_event","event":{"type":"content_block_start","index":7}}"#,
        br#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"discarded"}}}"#,
        br#"{"type":"system","subtype":"api_retry","error_status":503}"#,
        br#"{"type":"stream_event","event":{"type":"message_start","message":{"id":"b"}}}"#,
        br#"{"type":"stream_event","event":{"type":"content_block_start","index":0}}"#,
        br#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"second"}}}"#,
        br#"{"type":"assistant","message":{"id":"b","content":[{"type":"text","text":"second"}]}}"#,
        br#"{"type":"stream_event","event":{"type":"content_block_stop","index":0}}"#,
        br#"{"type":"stream_event","event":{"type":"message_stop"}}"#,
        br#"{"type":"result","is_error":false}"#,
    ] {
        backend.line(&env(0), line, &mut up, &mut below);
        while backend.pop_measure().is_some() {}
        backend.progress(&env(0), &mut up, &mut below);
    }
    let mut completions = 0_u32;
    while let Some(event) = up.pop() {
        match event {
            ToDomain::Completed { completion, .. } => {
                completions += 1;
                let llm::Said::Text { text, .. } = &completion.content[0] else { panic!("text") };
                assert_eq!(text.as_ref(), b"second");
            }
            ToDomain::Text { .. } => {}
            event @ (ToDomain::BuiltinResult { .. } | ToDomain::Failed { .. } | ToDomain::Cancelled { .. }) => {
                panic!("unexpected {event:?}");
            }
        }
    }
    assert_eq!(completions, 1);
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn held_call_user_text_is_captured() {
    for with_text in [false, true] {
        let mut backend = active(true);
        let mut held = reply_tests::call(b"held");
        held.name = b"foo".as_slice().into();
        assert!(backend.ledger.push(held).is_ok());
        review_tests::connect_server(&mut backend);
        let server = backend.server.as_mut().expect("server");
        server.from_io(
            env(0).now,
            io::Event::Stream {
                owner: Token::new(101),
                up: skein_lib::stream::Up::Bytes(
                    br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"foo","arguments":{}}}
"#
                    .as_slice()
                    .into(),
                ),
            },
            &mut backend.io,
        );
        let call = server.up().expect("MCP call");
        backend.calls.push(call).expect("held call room");
        backend.match_calls();
        let mut next = request(&backend);
        let FromDomain::Complete { prompt, observation, .. } = &mut next else { panic!("complete") };
        *observation = Some(Observation { first: false, messages: 2 });
        let mut content = alloc::vec::Vec::new();
        content.push(reply_tests::result(b"held"));
        if with_text {
            content.push(llm::Block::Text { text: b"new user text".as_slice().into(), replay: None });
        }
        prompt.messages = Box::new([llm::Message { role: llm::Role::User, content: content.into_boxed_slice() }]);
        backend.resume(&env(0), next, &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
        let reply = review_tests::server_reply(&mut backend);
        let received = reply["result"]["content"][0]["text"].as_str().expect("received reply text");
        assert_eq!(received, if with_text { "done\nnew user text" } else { "done" });
        backend.flush_prompt_measure(Time::ZERO);
        let Some(Measure::Prompt { prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
        if with_text {
            assert_eq!(
                prompt.messages.get(0).unwrap().blocks.as_slice(),
                &[Block::Text { text: b"done\nnew user text".as_slice().into() }]
            );
        } else {
            assert!(prompt.messages.is_empty());
        }
    }
}

#[test]
fn a_results_only_complete_offers_one_prompt() {
    let mut backend = active(true);
    backend.turns.closed = 1;
    backend.turns.assistant_events = 1;
    assert!(backend.ledger.push(reply_tests::call(b"held")).is_ok());
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, observation, .. } = &mut next else { panic!("complete") };
    *observation = Some(Observation { first: false, messages: 2 });
    prompt.messages =
        Box::new([llm::Message { role: llm::Role::User, content: Box::new([reply_tests::result(b"held")]) }]);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    backend.line(
        &env(1),
        br#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"held","content":"done"}]}}"#,
        &mut up,
        &mut below,
    );
    assert!(backend.pop_measure().is_none());
    backend.line(&env(2), br#"{"type":"result","is_error":false}"#, &mut up, &mut below);
    backend.line(
        &env(3),
        br#"{"type":"stream_event","event":{"type":"message_start","message":{"id":"next"}}}"#,
        &mut up,
        &mut below,
    );
    let Some(Measure::Prompt { at, prompt, .. }) = backend.pop_measure() else { panic!("prompt") };
    assert_eq!(at, env(3).now);
    assert_eq!(
        prompt.messages.get(0).unwrap().blocks.as_slice(),
        &[Block::Text { text: b"Continue with the tool results.".as_slice().into() }]
    );
    backend.prompt_measure(env(4).now, Some(b"a second offer"));
    backend.flush_prompt_measure(env(4).now);
    assert!(backend.pop_measure().is_none());
    assert_eq!(backend.phase, Phase::Running);
}
