#![expect(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    reason = "ordinary Rust step harness, programming-model.md 10.2"
)]
#![expect(clippy::match_like_matches_macro, reason = "the no_std test harness uses explicit record filters")]
use super::lifecycle_tests::{backend, env, request};
use super::*;
use serde_json::json;

fn active() -> Backend {
    let mut backend = backend();
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
    backend.owner = Some(Token::new(80));
    let request = request(&backend);
    let FromDomain::Complete { prompt, .. } = &request else { panic!("complete") };
    let session::Opening::New { id } = session::opening(prompt, None, None) else { panic!("new window") };
    backend.running = Some(id);
    backend.request = Some(request);
    backend
}
fn feed(backend: &mut Backend, value: serde_json::Value, up: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
    backend.line(&env(0), &serde_json::to_vec(&value).expect("fixture JSON"), up, below);
}
fn text() -> serde_json::Value {
    json!({"type":"assistant","message":{"id":"text","content":[{"type":"text","text":"thinking"}]}})
}
fn tool(id: &str, input: serde_json::Value) -> serde_json::Value {
    json!({"type":"assistant","message":{"id":id,"content":[{"type":"tool_use","id":id,"name":"mcp__smith__foo","input":input}]}})
}
#[test]
fn a_nudge_after_stdin_closes_fails_with_the_exit() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Exited { owner: backend.token(4), exit: kernel::Exit::Code(7) }),
        &mut up,
        &mut below,
    );
    backend.from_io(&env(0), BelowEvent::Io(io::Event::Closed { owner: Token::new(31) }), &mut up, &mut below);
    feed(&mut backend, text(), &mut up, &mut below);
    feed(&mut backend, json!({"type":"result","is_error":false}), &mut up, &mut below);
    let Some(ToDomain::Text { .. }) = up.pop() else { panic!("text event") };
    let Some(ToDomain::Completed { .. }) = up.pop() else { panic!("completion event") };
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("completion") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"continue".as_slice().into(), replay: None }]),
    }]);
    backend.resume(&env(0), next, &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("exit terminal") };
    assert_eq!(failure, llm::Failure::Program { exit: Some(7) });
}
#[test]
fn assistant_text_redacts_private_values_before_completion() {
    let mut backend = active();
    let secret = mcp::secret_text(backend.names.mcp.expose().try_into().expect("entropy"));
    let path = backend.run_path(b"mcp.json");
    let input = mcp::json::join(&[b"program error ", &path, b" ", &secret, b" end"]);
    let mut value = text();
    value["message"]["content"][0]["text"] = json!(core::str::from_utf8(&input).expect("fixture text"));
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, value, &mut up, &mut below);
    feed(&mut backend, json!({"type":"result","is_error":false}), &mut up, &mut below);
    let Some(ToDomain::Text { bytes, .. }) = up.pop() else { panic!("text count") };
    assert_eq!(usize::try_from(bytes).expect("count"), input.len());
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("completion") };
    let llm::Said::Text { text, .. } = &completion.content[0] else { panic!("text") };
    assert!(!text.windows(secret.len()).any(|window| window == secret));
    assert!(!text.windows(path.len()).any(|window| window == path.as_ref()));
    assert!(!text.windows(backend.names.run.len()).any(|window| window == backend.names.run));
    assert!(text.ends_with(b" end"));
}
#[test]
fn an_open_call_matches_before_the_closed_name_fallback() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, tool("a", json!({"value":"A"})), &mut up, &mut below);
    feed(
        &mut backend,
        json!({"type":"stream_event","event":{"type":"message_start","message":{"id":"b"}}}),
        &mut up,
        &mut below,
    );
    feed(&mut backend, tool("b", json!({"value":"B"})), &mut up, &mut below);
    let index = calls::find(&backend.ledger, b"foo", br#"{"value":"B"}"#, 32, 256).expect("open match");
    assert_eq!(backend.ledger.get(index).expect("call").id.as_ref(), b"b");
}
#[test]
fn cancelling_an_unjudged_finish_never_accepts_it() {
    let mut backend = active();
    backend
        .ledger
        .push(calls::Call {
            id: b"finish".as_slice().into(),
            name: b"finish".as_slice().into(),
            input: b"{}".as_slice().into(),
            rpc: None,
            answer: None,
            answered: false,
            prompted: false,
            eligible: true,
            message: 1,
            builtin: false,
            native_write: false,
            owner: None,
        })
        .ok()
        .expect("ledger room");
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), FromDomain::Cancel { owner: Token::new(80) }, &mut up, &mut below);
    assert!(backend.ledger.get(0).expect("finish").answer.is_none(), "the program cancels its held call");
}
#[test]
fn a_delivered_assistant_is_not_charged_again_at_result() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, tool("a", json!({})), &mut up, &mut below);
    let Some(ToDomain::Completed { .. }) = up.pop() else { panic!("completion event") };
    backend.from_domain(&env(0), request(&backend), &mut up, &mut below);
    feed(&mut backend, json!({"type":"result","is_error":false}), &mut up, &mut below);
    assert!(up.is_empty(), "result has no second API message");
    assert_eq!(backend.owner, Some(Token::new(80)));
    assert!(backend.outgoing.is_some(), "pending Complete resumes the ended program turn");
}
#[test]
fn a_silent_completion_obeys_its_head_deadline() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), request(&backend), &mut up, &mut below);
    backend.fire(&env(1), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("head deadline") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Head });
}
#[test]
fn an_incomplete_message_obeys_its_idle_deadline() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), request(&backend), &mut up, &mut below);
    feed(
        &mut backend,
        json!({"type":"stream_event","event":{"type":"message_start","message":{"id":"b"}}}),
        &mut up,
        &mut below,
    );
    backend.fire(&env(1), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("idle deadline") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Idle });
}
#[test]
fn a_text_message_without_result_obeys_its_idle_deadline() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_domain(&env(0), request(&backend), &mut up, &mut below);
    feed(&mut backend, text(), &mut up, &mut below);
    backend.fire(&env(1), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("idle terminal") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Idle });
}

pub(super) fn connect_server(backend: &mut Backend) {
    let first = Token::new(100);
    let owner = Token::new(101);
    let mut io = Queue::with_capacity(64);
    let mut server = mcp::Server::new(
        first,
        0,
        &[7; 32],
        Box::new([mcp::Tool {
            name: b"foo".as_slice().into(),
            description: b"Tool".as_slice().into(),
            input_schema: br#"{"type":"object"}"#.as_slice().into(),
        }]),
        backend.settings.mcp,
        &mut io,
    )
    .expect("server");
    server.from_io(
        env(0).now,
        io::Event::Listening {
            owner: first,
            listener: Token::new(200),
            addr: io::Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], 47100))),
        },
        &mut io,
    );
    let Some(mcp::Up::Listening { .. }) = server.up() else { panic!("listening") };
    server.from_io(
        env(0).now,
        io::Event::Accepted {
            owner: first,
            socket: Token::new(201),
            peer: io::Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], 50000))),
        },
        &mut io,
    );
    let secret = mcp::secret_text(&[7; 32]);
    server.from_io(
        env(0).now,
        io::Event::Stream { owner, up: skein_lib::stream::Up::Bytes(mcp::json::join(&[&secret, b"\n"])) },
        &mut io,
    );
    let right = io
        .iter()
        .find_map(|request| match request {
            io::Request::Output { down: skein_lib::stream::OutputDown::Room { right, .. }, .. } => Some(*right),
            io::Request::Listen { .. }
            | io::Request::Connect { .. }
            | io::Request::Bind { .. }
            | io::Request::Reject { .. }
            | io::Request::Stream { .. }
            | io::Request::Output { .. }
            | io::Request::Spawn { .. }
            | io::Request::Signal { .. }
            | io::Request::Usage { .. }
            | io::Request::Window { .. }
            | io::Request::Close { .. }
            | io::Request::Abort { .. } => None,
        })
        .expect("ack room");
    server.from_io(
        env(0).now,
        io::Event::Output {
            owner,
            up: skein_lib::stream::OutputUp::Settled { right, outcome: skein_lib::stream::OutputOutcome::Granted },
        },
        &mut io,
    );
    assert_eq!(server.up(), Some(mcp::Up::Connected));
    backend.server = Some(server);
}
#[test]
fn an_unmatched_call_after_text_closes_is_rejected_immediately() {
    let mut backend = active();
    connect_server(&mut backend);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, text(), &mut up, &mut below);
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
    backend.calls.push(call).expect("held room");
    backend.match_calls();
    assert!(backend.calls.is_empty(), "closed text must not hold unmatched calls waiting for result");
    let reply = server_reply(&mut backend);
    assert_eq!(reply["id"], json!(1_u32));
    assert_eq!(reply["result"]["isError"], true);
    assert_eq!(reply["result"]["content"][0]["text"], "smith: this call was not in the model's message");
}

#[test]
fn closed_text_expires_the_preceding_messages_calls() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, tool("a", json!({})), &mut up, &mut below);
    feed(&mut backend, text(), &mut up, &mut below);
    assert_eq!(calls::find(&backend.ledger, b"foo", b"{}", 32, 256), None);
}

#[test]
fn an_accepted_finish_receives_its_acceptance_reply() {
    let mut backend = active();
    backend
        .ledger
        .push(calls::Call {
            id: b"finish".as_slice().into(),
            name: b"finish".as_slice().into(),
            input: b"{}".as_slice().into(),
            rpc: None,
            answer: None,
            answered: false,
            prompted: false,
            eligible: true,
            message: 1,
            builtin: false,
            native_write: false,
            owner: None,
        })
        .ok()
        .expect("ledger room");
    finish_verdict(&mut backend, b"finish", b"accepted", false, &mut Queue::with_capacity(64));
    backend.close(&env(0), &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
    let Some(mcp::Answer::Text(text)) = backend.ledger.get(0).expect("finish").answer.as_ref() else {
        panic!("accepted finish")
    };
    assert_eq!(text.as_ref(), b"accepted");
}

#[test]
fn a_result_before_resume_does_not_charge_another_completion() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(&mut backend, tool("a", json!({})), &mut up, &mut below);
    let Some(ToDomain::Completed { .. }) = up.pop() else { panic!("completion event") };
    feed(&mut backend, json!({"type":"result","is_error":false}), &mut up, &mut below);
    backend.from_domain(&env(0), request(&backend), &mut up, &mut below);
    assert!(up.is_empty());
    assert_eq!(backend.owner, Some(Token::new(80)));
    assert!(backend.outgoing.is_some());
}

#[test]
fn stdout_progress_replaces_the_head_with_an_idle_deadline() {
    let mut backend = active();
    let mut next = request(&backend);
    let FromDomain::Complete { deadlines, .. } = &mut next else { panic!("completion") };
    deadlines.head = Duration::from_secs(3);
    deadlines.idle = Duration::from_secs(2);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.resume(&env(0), next, &mut up, &mut below);
    assert_eq!(backend.next_deadline(), Some(env(3).now));
    backend.from_io(
        &env(2),
        BelowEvent::Io(io::Event::Stream {
            owner: Token::new(32),
            up: skein_lib::stream::Up::Bytes(b"{".as_slice().into()),
        }),
        &mut up,
        &mut below,
    );
    assert_eq!(backend.next_deadline(), Some(env(4).now));
    backend.fire(&env(3), &mut up, &mut below);
    assert!(up.is_empty());
    backend.fire(&env(4), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("idle terminal") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Idle });
}

#[test]
fn failure_never_accepts_an_unjudged_finish() {
    let mut backend = active();
    backend
        .ledger
        .push(calls::Call {
            id: b"finish".as_slice().into(),
            name: b"finish".as_slice().into(),
            input: b"{}".as_slice().into(),
            rpc: None,
            answer: None,
            answered: false,
            prompted: false,
            eligible: true,
            message: 1,
            builtin: false,
            native_write: false,
            owner: None,
        })
        .ok()
        .expect("ledger room");
    feed(
        &mut backend,
        json!({"type":"result","is_error":true}),
        &mut Queue::with_capacity(32),
        &mut Queue::with_capacity(64),
    );
    assert!(backend.ledger.get(0).expect("finish").answer.is_none(), "the failed turn's held call is dropped");
}

#[test]
fn an_input_grant_after_stdin_closes_fails_without_writing() {
    let mut backend = active();
    backend.pipes[0] = None;
    backend.exit = Some(7);
    backend.outgoing = Some(b"pending".as_slice().into());
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Output {
            owner: Token::new(31),
            up: skein_lib::stream::OutputUp::Settled {
                right: Token::new(1),
                outcome: skein_lib::stream::OutputOutcome::Granted,
            },
        }),
        &mut up,
        &mut below,
    );
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("program terminal") };
    assert_eq!(failure, llm::Failure::Program { exit: Some(7) });
    assert!(!below.iter().any(|request| {
        if let Below::Io(io::Request::Output { down: skein_lib::stream::OutputDown::Send { .. }, .. }) = request {
            true
        } else {
            false
        }
    }));
}

pub(super) fn server_reply(backend: &mut Backend) -> serde_json::Value {
    for _ in 0_u8..16 {
        for _ in 0..backend.io.capacity() {
            let Some(request) = backend.io.pop() else { break };
            match request {
                io::Request::Output { down: skein_lib::stream::OutputDown::Room { right, .. }, .. } => {
                    backend.server.as_mut().expect("server").from_io(
                        env(0).now,
                        io::Event::Output {
                            owner: Token::new(101),
                            up: skein_lib::stream::OutputUp::Settled {
                                right,
                                outcome: skein_lib::stream::OutputOutcome::Granted,
                            },
                        },
                        &mut backend.io,
                    );
                }
                io::Request::Output { down: skein_lib::stream::OutputDown::Send { bytes, .. }, .. } => {
                    return serde_json::from_slice(&bytes).expect("MCP reply");
                }
                io::Request::Listen { .. }
                | io::Request::Connect { .. }
                | io::Request::Bind { .. }
                | io::Request::Reject { .. }
                | io::Request::Stream { .. }
                | io::Request::Output { .. }
                | io::Request::Spawn { .. }
                | io::Request::Signal { .. }
                | io::Request::Usage { .. }
                | io::Request::Window { .. }
                | io::Request::Close { .. }
                | io::Request::Abort { .. } => {}
            }
        }
    }
    panic!("MCP response pending");
}

#[test]
fn accepting_one_finish_does_not_accept_an_unjudged_finish() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    feed(
        &mut backend,
        json!({"type":"assistant","message":{"id":"two-finishes","content":[
            {"type":"tool_use","id":"first","name":"mcp__smith__finish","input":{}},
            {"type":"tool_use","id":"second","name":"mcp__smith__finish","input":{}}
        ]}}),
        &mut up,
        &mut below,
    );
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("one API completion") };
    assert_eq!(completion.content.len(), 2);
    // The first call alone carries an accepted per-call verdict. Closing an
    // accepted run must not extend that verdict to its unjudged neighbour.
    finish_verdict(&mut backend, b"first", b"accepted", false, &mut below);
    backend.close(&env(0), &mut up, &mut below);
    let Some(mcp::Answer::Text(text)) = backend.ledger.get(0).expect("first finish").answer.as_ref() else {
        panic!("accepted verdict")
    };
    assert_eq!(text.as_ref(), b"accepted");
    assert!(backend.ledger.get(1).expect("second finish").answer.is_none(), "unjudged call is dropped");
}

#[test]
fn bytes_after_init_in_the_same_read_start_the_idle_deadline() {
    let mut backend = active();
    backend.phase = Phase::Starting;
    let Some(FromDomain::Complete { deadlines, .. }) = &mut backend.request else { panic!("completion") };
    deadlines.head = Duration::from_secs(30);
    deadlines.idle = Duration::from_secs(2);
    deadlines.whole = env(60).now;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    let bytes = br#"{"type":"system","subtype":"init","mcp_servers":[{"name":"smith","status":"connected"}],"tools":["mcp__smith__permission_prompt"]}
{"#;
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Stream {
            owner: Token::new(32),
            up: skein_lib::stream::Up::Bytes(bytes.as_slice().into()),
        }),
        &mut up,
        &mut below,
    );
    assert_eq!(backend.next_deadline(), Some(env(2).now));
    backend.fire(&env(1), &mut up, &mut below);
    assert!(up.is_empty());
    backend.fire(&env(2), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("idle failure") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Idle });
}

fn finish_verdict(backend: &mut Backend, id: &[u8], text: &[u8], error: bool, below: &mut Queue<Below>) {
    use smith_domain::session::llm as session;
    backend.settled(
        &[session::Message {
            role: session::Role::User,
            content: Box::new([session::Block::ToolResult {
                id: id.into(),
                result: session::Returned::Text { text: text.into(), error, replay: None },
            }]),
        }],
        below,
    );
}
