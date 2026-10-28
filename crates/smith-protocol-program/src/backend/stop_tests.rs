#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "ordinary Rust deterministic harness, programming-model.md 10.2"
)]
use super::lifecycle_tests::{backend, env, request};
use super::*;

fn active() -> Backend {
    let mut backend = backend();
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
    backend.owner = Some(Token::new(80));
    backend.request = Some(request(&backend));
    backend
}

fn signal(below: &Queue<Below>, expected: kernel::Signal) -> usize {
    let mut count = 0_usize;
    for request in below {
        if let Below::Io(io::Request::Signal { signal, to: kernel::Target::Group, .. }) = request
            && *signal == expected
        {
            count = count.checked_add(1).expect("bounded effect queue");
        }
    }
    count
}

fn input_closed(below: &Queue<Below>) -> bool {
    for request in below {
        if let Below::Io(io::Request::Close { entity }) = request
            && *entity == Token::new(31)
        {
            return true;
        }
    }
    false
}

#[test]
fn a_cancel_interrupts_a_running_turn() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), FromDomain::Cancel { owner: Token::new(80) }, &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Interrupt), 1);
    assert!(!input_closed(&below));
    let Some(ToDomain::Cancelled { owner }) = up.pop() else {
        panic!("cancel terminal");
    };
    assert_eq!(owner, Token::new(80));
}

#[test]
fn a_cancel_of_an_idle_program_closes_its_input() {
    let mut backend = active();
    backend.turns.result = true;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), FromDomain::Cancel { owner: Token::new(80) }, &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Interrupt), 0);
    assert!(input_closed(&below));
}

#[test]
fn a_program_past_its_grace_is_terminated() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.close(&env(0), &mut up, &mut below);
    backend.fire(&env(1), &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Terminate), 1);
}

#[test]
fn an_interrupted_result_after_a_stop_is_no_failure() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.close(&env(0), &mut up, &mut below);
    backend.line(&env(0), br#"{"type":"result","is_error":true,"terminal_reason":"interrupted"}"#, &mut up, &mut below);
    assert!(backend.pending_failure.is_none());
    let Some(ToDomain::Cancelled { .. }) = up.pop() else {
        panic!("cancel terminal");
    };
    assert!(up.is_empty());
    backend.flush(&mut below);
    assert!(input_closed(&below));
}

#[test]
fn the_wind_down_interrupts_once() {
    let mut backend = active();
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish]));
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"finish now".as_slice().into(), replay: None }]),
    }]);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Interrupt), 1);
    backend.line(&env(0), br#"{"type":"result","is_error":true,"terminal_reason":"interrupted"}"#, &mut up, &mut below);
    backend.flush(&mut below);
    assert!(backend.winding == stop::WindDown::Finishing);
    assert_eq!(backend.phase, Phase::Running);
    let mut second = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut second else { panic!("complete") };
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish]));
    backend.resume(&env(0), second, &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Interrupt), 1);
}

#[test]
fn the_wind_down_sends_answers_before_the_note() {
    let backend = active();
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("complete") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([
            llm::Block::ToolResult {
                id: b"call".as_slice().into(),
                result: llm::Returned::Text { text: b"answer".as_slice().into(), error: false, replay: None },
            },
            llm::Block::Text { text: b"finish now".as_slice().into(), replay: None },
        ]),
    }]);
    let tail = tail::render(&mut prompt, &backend.ledger, &backend.settings.adapter, 4096).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&tail).unwrap();
    assert_eq!(decoded["message"]["content"], "result of call: answer\nfinish now\n");
}

fn builtin(backend: &mut Backend) {
    assert!(
        backend
            .ledger
            .push(calls::Call {
                id: b"r".as_slice().into(),
                name: b"Read".as_slice().into(),
                input: b"{}".as_slice().into(),
                rpc: None,
                answer: None,
                answered: false,
                prompted: false,
                eligible: true,
                message: 1,
                builtin: true,
                native_write: false,
                owner: Some(Token::new(80)),
            })
            .is_ok()
    );
    backend.turns.closed = 1;
    backend.owner = None;
}

fn prompt(backend: &mut Backend, labelled: bool) {
    prompt_input(backend, labelled, serde_json::json!({}));
}

fn prompt_input(backend: &mut Backend, labelled: bool, input: serde_json::Value) {
    let mut message = serde_json::json!({"jsonrpc":"2.0","id":1_u32,"method":"tools/call","params":{"name":"permission_prompt","arguments":{"tool_name":"Read","input":{}}}});
    message["params"]["arguments"]["input"] = input;
    if labelled {
        message["params"]["arguments"]["tool_use_id"] = serde_json::json!("r");
    }
    let bytes = mcp::json::join(&[&serde_json::to_vec(&message).expect("prompt JSON"), b"\n"]);
    let server = backend.server.as_mut().expect("permission server");
    server.from_io(
        env(0).now,
        io::Event::Stream { owner: Token::new(101), up: skein_lib::stream::Up::Bytes(bytes) },
        &mut backend.io,
    );
    let prompt = server.up().expect("permission prompt");
    backend.permission(prompt);
}

fn decision(backend: &mut Backend) -> serde_json::Value {
    let reply = review_tests::server_reply(backend);
    assert_eq!(reply["id"], serde_json::json!(1_u32));
    serde_json::from_str(reply["result"]["content"][0]["text"].as_str().expect("permission JSON")).expect("verdict")
}

#[test]
fn a_prompt_waits_for_its_complete() {
    let mut backend = active();
    builtin(&mut backend);
    review_tests::connect_server(&mut backend);
    prompt(&mut backend, true);
    assert_eq!(backend.calls.len(), 1, "the permission prompt is held");
    for request in &backend.io {
        if let io::Request::Output { .. } = request {
            panic!("no permission response before Complete");
        }
    }
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), request(&backend), &mut up, &mut below);
    assert!(backend.calls.is_empty());
    assert_eq!(decision(&mut backend)["behavior"], "deny");
}

#[test]
fn a_prompt_after_its_complete_is_answered_at_once() {
    let mut backend = active();
    builtin(&mut backend);
    backend.ledger.get_mut(0).expect("call").input = b"{ }".as_slice().into();
    review_tests::connect_server(&mut backend);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), request(&backend), &mut up, &mut below);
    prompt(&mut backend, false);
    assert!(backend.calls.is_empty());
    assert_eq!(decision(&mut backend)["behavior"], "deny");
}

#[test]
fn a_built_in_outside_the_choice_is_denied() {
    let mut backend = active();
    builtin(&mut backend);
    review_tests::connect_server(&mut backend);
    backend.winding = stop::WindDown::Finishing;
    let mut next = request(&backend);
    let FromDomain::Complete { prompt: next_prompt, .. } = &mut next else { panic!("complete") };
    next_prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish, llm::Served::Deliver]));
    backend.resume(&env(0), next, &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
    prompt(&mut backend, true);
    assert_eq!(
        decision(&mut backend),
        serde_json::json!({"behavior":"deny","message":"not run: allowed calls now: finish deliver "})
    );
}

#[test]
fn an_answered_builtin_does_not_release_the_next_prompt() {
    let mut backend = active();
    builtin(&mut backend);
    backend.permitted = 2;
    backend.ledger.get_mut(0).unwrap().answered = true;
    backend.ledger.get_mut(0).unwrap().input = br#"{"a":1}"#.as_slice().into();
    builtin(&mut backend);
    backend.ledger.get_mut(1).unwrap().id = b"next".as_slice().into();
    backend.ledger.get_mut(1).unwrap().message = 2;
    backend.ledger.get_mut(1).unwrap().input = br#"{"a":1}"#.as_slice().into();
    backend.turns.closed = 2;
    review_tests::connect_server(&mut backend);
    prompt_input(&mut backend, false, serde_json::json!({"a":1_u32}));
    assert_eq!(backend.calls.len(), 1);
    assert!(!backend.ledger.get(1).unwrap().prompted);
    backend.resume(&env(0), request(&backend), &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
    assert!(backend.calls.is_empty());
    assert!(backend.ledger.get(1).unwrap().prompted);
    assert_eq!(decision(&mut backend)["behavior"], "deny");
}

#[test]
fn equal_permission_prompts_bind_distinct_calls() {
    let mut backend = active();
    builtin(&mut backend);
    builtin(&mut backend);
    backend.permitted = 2;
    assert!(backend.permission_ready(b"Read", b"{}", None));
    assert!(backend.permission_ready(b"Read", b"{}", None));
    assert!(!backend.permission_ready(b"Read", b"{}", None));
}

#[test]
#[expect(clippy::match_like_matches_macro, reason = "effect selectors avoid the subset's disallowed matches macro")]
fn an_exited_child_drains_for_one_grace_without_signals() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.close(&env(0), &mut up, &mut below);
    while below.pop().is_some() {}
    backend.from_io(
        &env(1),
        BelowEvent::Io(io::Event::Exited { owner: backend.token(4), exit: kernel::Exit::Code(0) }),
        &mut up,
        &mut below,
    );
    assert_eq!(backend.next_deadline(), Some(env(1).now));
    backend.fire(&env(1), &mut up, &mut below);
    assert_eq!(backend.next_deadline(), Some(env(2).now));
    assert!(!below.iter().any(|effect| if let Below::Io(io::Request::Abort { .. }) = effect { true } else { false }));
    backend.fire(&env(2), &mut up, &mut below);
    for pipe in [31, 32, 33] {
        assert!(below.iter().any(|effect| if let Below::Io(io::Request::Abort { entity }) = effect {
            *entity == Token::new(pipe)
        } else {
            false
        }));
    }
    assert!(below.iter().any(|effect| if let Below::Io(io::Request::Close { entity }) = effect {
        *entity == Token::new(30)
    } else {
        false
    }));
    assert!(!below.iter().any(|effect| if let Below::Io(io::Request::Signal { .. }) = effect { true } else { false }));
}

#[test]
fn streaming_during_wind_down_keeps_the_stop_grace() {
    let mut backend = active();
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, deadlines, .. } = &mut next else { panic!("complete") };
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish]));
    deadlines.idle = Duration::from_secs(10);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Stream {
            owner: Token::new(32),
            up: skein_lib::stream::Up::Bytes(b"{".as_slice().into()),
        }),
        &mut up,
        &mut below,
    );
    assert_eq!(backend.next_deadline(), Some(env(1).now));
    backend.fire(&env(1), &mut up, &mut below);
    assert_eq!(signal(&below, kernel::Signal::Terminate), 1);
}

#[test]
fn a_tail_checks_its_exact_encoded_length() {
    let backend = active();
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("complete") };
    let mut text = (0_u8..=127).collect::<alloc::vec::Vec<_>>();
    text.extend_from_slice("é水".as_bytes());
    let mut expected = text.clone();
    expected.push(b'\n');
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: text.into(), replay: None }]),
    }]);
    let encoded = mcp::json::join(&[
        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":",
        &serde_json::to_vec(core::str::from_utf8(&expected).unwrap()).unwrap(),
        b"}}\n",
    ]);
    let length = u32::try_from(encoded.len()).unwrap();
    assert!(tail::render(&mut prompt, &backend.ledger, &backend.settings.adapter, length - 1).is_none());
    assert_eq!(tail::render(&mut prompt, &backend.ledger, &backend.settings.adapter, length).unwrap(), encoded);
    assert!(prompt.messages.is_empty());
}

#[test]
fn a_tail_refuses_invalid_utf8_before_encoding() {
    let backend = active();
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("complete") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: Box::new([255]), replay: None }]),
    }]);
    assert!(tail::render(&mut prompt, &backend.ledger, &backend.settings.adapter, 4096).is_none());
}

#[test]
fn a_tail_moves_served_results_after_the_last_assistant() {
    let mut backend = active();
    builtin(&mut backend);
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("complete") };
    prompt.messages = Box::new([
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"already sent".as_slice().into(), replay: None }]),
        },
        llm::Message { role: llm::Role::Assistant, content: Box::new([]) },
        llm::Message {
            role: llm::Role::User,
            content: Box::new([
                llm::Block::ToolResult {
                    id: b"r".as_slice().into(),
                    result: llm::Returned::Served {
                        returned: smith_domain::run::Returned::Waited { text: b"new reply".as_slice().into() },
                        error: false,
                    },
                },
                llm::Block::Text { text: b"finish now".as_slice().into(), replay: None },
            ]),
        },
    ]);
    let line = tail::render(&mut prompt, &backend.ledger, &backend.settings.adapter, 4096).unwrap();
    let decoded: serde_json::Value = serde_json::from_slice(&line).unwrap();
    assert_eq!(decoded["message"]["content"], "result of Read: new reply\nfinish now\n");
}

#[test]
fn a_call_arriving_during_interrupt_is_not_run() {
    let mut backend = active();
    review_tests::connect_server(&mut backend);
    backend.stop = stop::Stop::Yielding;
    let bytes =
        b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"foo\",\"arguments\":{}}}\n";
    backend.server.as_mut().expect("server").from_io(
        env(0).now,
        io::Event::Stream { owner: Token::new(101), up: skein_lib::stream::Up::Bytes(bytes.as_slice().into()) },
        &mut backend.io,
    );
    let call = backend.server.as_mut().expect("server").up().expect("call");
    backend.calls.push(call).expect("one held call");
    backend.match_calls();
    let reply = review_tests::server_reply(&mut backend);
    assert_eq!(reply["result"]["isError"], true);
    assert_eq!(reply["result"]["content"][0]["text"], "not run: the program turn was interrupted");
}

#[test]
fn a_prompt_arriving_during_interrupt_is_not_run() {
    let mut backend = active();
    review_tests::connect_server(&mut backend);
    backend.stop = stop::Stop::Yielding;
    prompt(&mut backend, true);
    assert_eq!(
        decision(&mut backend),
        serde_json::json!({
            "behavior":"deny", "message":"not run: the program turn was interrupted",
        })
    );
}
