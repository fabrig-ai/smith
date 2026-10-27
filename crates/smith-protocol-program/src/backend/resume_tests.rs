#![expect(clippy::disallowed_methods, reason = "ordinary deterministic harness, programming-model.md 10.2")]
use super::{
    lifecycle_tests::{backend, env, request},
    *,
};
use alloc::string::ToString;

fn history(backend: &Backend) -> FromDomain {
    let mut request = request(backend);
    let FromDomain::Complete { prompt, .. } = &mut request else { panic!("complete") };
    prompt.messages = Box::new([
        llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"task".as_slice().into(), replay: None }]),
        },
        llm::Message {
            role: llm::Role::Assistant,
            content: Box::new([llm::Block::ToolCall {
                id: b"wait-id".as_slice().into(),
                name: b"wait".as_slice().into(),
                input: b"{}".as_slice().into(),
                replay: None,
            }]),
        },
        llm::Message {
            role: llm::Role::User,
            content: Box::new([
                llm::Block::ToolResult {
                    id: b"wait-id".as_slice().into(),
                    result: llm::Returned::Text { text: b"wait result".as_slice().into(), error: false, replay: None },
                },
                llm::Block::Text { text: b"new instruction".as_slice().into(), replay: None },
            ]),
        },
    ]);
    request
}

pub(super) fn launchable() -> Backend {
    let mut backend = backend();
    backend.settings.directory = Some(Root { file: Token::new(40), descriptor: kernel::Fd::new(41) });
    backend.grant = Some(Grant {
        name: GrantName { account: 0, generation: 1 },
        source: b"source".as_slice().into(),
        lapses: env(30).now,
    });
    let request = history(&backend);
    let FromDomain::Complete { prompt, .. } = &request else { panic!("complete") };
    let session::Opening::Resume { id } = session::opening(prompt, None, None) else { panic!("resume") };
    backend.names.session = session::uuid(id);
    backend.resuming = true;
    backend.request = Some(request);
    backend
}

#[test]
fn a_resume_names_the_window_s_session() {
    let mut backend = launchable();
    let id = backend.names.session;
    let spawn = backend.launch_spec().expect("resume spawn");
    assert!(spawn.args.windows(2).any(|pair| pair[0].as_ref() == b"--resume" && pair[1].as_ref() == id));
    assert!(!spawn.args.iter().any(|argument| argument.as_ref() == b"--session-id"));
}

#[test]
fn the_tail_goes_as_one_user_message() {
    let mut backend = launchable();
    backend.launch_spec().expect("resume spawn");
    let value: serde_json::Value = serde_json::from_slice(backend.outgoing.as_ref().expect("tail")).expect("line");
    assert_eq!(
        value["message"]["content"],
        "(The agent paused your last turn: the run was parked and has resumed.)\nresult of wait-id: wait result\nnew instruction\n"
    );
    assert!(!value.to_string().contains("task"));
}

fn stderr_failure(backend: &mut Backend, detail: &[u8]) -> llm::Failure {
    backend.stderr = List::with_capacity(128);
    backend.stderr_wrapped = false;
    backend.tail = 0;
    backend.pipes[2] = Some(Token::new(33));
    backend.io_event(
        &env(0),
        io::Event::Stream { owner: Token::new(33), up: skein_lib::stream::Up::Bytes(detail.into()) },
        &mut Queue::with_capacity(32),
        &mut Queue::with_capacity(64),
    );
    backend.exit_failure()
}

#[test]
fn a_missing_session_is_gone() {
    let mut backend = launchable();
    backend.phase = Phase::Starting;
    backend.exit = Some(1);
    assert_eq!(
        stderr_failure(&mut backend, b"No conversation found with session ID: example"),
        llm::Failure::SessionGone
    );
    assert_eq!(stderr_failure(&mut backend, b"Session ID is already in use"), llm::Failure::Program { exit: Some(1) });
    assert_eq!(
        stderr_failure(&mut backend, b"Other failure: No conversation found with session ID"),
        llm::Failure::Program { exit: Some(1) }
    );
    backend.phase = Phase::Running;
    assert_eq!(
        stderr_failure(&mut backend, b"No conversation found with session ID"),
        llm::Failure::Program { exit: Some(1) }
    );
    backend.phase = Phase::Starting;
    backend.resuming = false;
    assert_eq!(
        stderr_failure(&mut backend, b"No conversation found with session ID"),
        llm::Failure::Program { exit: Some(1) }
    );
}

#[test]
fn a_park_while_waiting_interrupts() {
    let mut backend = launchable();
    backend.phase = Phase::Running;
    backend.child = Some(backend.token(4));
    backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
    backend.owner = None; // The wait completion was delivered; its held RPC remains unanswered.
    backend.turns.result = false;
    let mut held = reply_tests::call(b"wait-id");
    held.name = b"foo".as_slice().into();
    backend.ledger.push(held).ok().expect("ledger");
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
    let call = server.up().expect("held RPC");
    backend.calls.push(call).expect("call room");
    backend.match_calls();
    assert!(backend.ledger.get(0).expect("held call").rpc.is_some());
    backend.request = None;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.close(&env(0), &mut up, &mut below);
    for effect in &below {
        if let Below::Io(io::Request::Signal { signal: kernel::Signal::Interrupt, .. }) = effect {
            return;
        }
    }
    panic!("held wait must be interrupted");
}

fn no_spawn(below: &Queue<Below>) {
    for effect in below {
        if let Below::Io(io::Request::Spawn { .. }) = effect {
            panic!("spawn before the old stop settled");
        }
    }
}

fn launch_from_listener(
    backend: &mut Backend,
    seconds: u64,
    up: &mut Queue<ToDomain>,
    below: &mut Queue<Below>,
) -> kernel::Spawn {
    backend.from_io(
        &env(seconds),
        BelowEvent::Io(io::Event::Listening {
            owner: backend.first,
            listener: backend.first,
            addr: io::Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], 47100))),
        }),
        up,
        below,
    );
    for _ in 0_u32..3 {
        backend.from_io(
            &env(seconds),
            BelowEvent::File {
                event: file::Event::Stored { owner: backend.token(5), digest: io::digest::digest(b"fixture") },
                descriptor: None,
            },
            up,
            below,
        );
    }
    let mut spawn = None;
    while let Some(effect) = below.pop() {
        if let Below::Io(io::Request::Spawn { spawn: found, .. }) = effect {
            assert!(spawn.replace(found).is_none(), "one launch");
        }
    }
    spawn.expect("backend emitted the spawn")
}

fn running_history(up: &mut Queue<ToDomain>, below: &mut Queue<Below>) -> (Backend, [u8; 16]) {
    let mut backend = launchable();
    backend.phase = Phase::Starting;
    backend.owner = Some(Token::new(80));
    backend.open_runs(env(0).now, below);
    for (file, descriptor) in [(Token::new(42), kernel::Fd::new(43)), (Token::new(44), kernel::Fd::new(45))] {
        backend.from_io(
            &env(0),
            BelowEvent::File {
                event: file::Event::Opened { owner: backend.token(5), file, len: 0 },
                descriptor: Some(descriptor),
            },
            up,
            below,
        );
    }
    let first = launch_from_listener(&mut backend, 0, up, below);
    let id = backend.running.expect("window ID");
    assert!(first.args.windows(2).any(|pair| pair[0].as_ref() == b"--resume" && pair[1].as_ref() == session::uuid(id)));
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Spawned {
            owner: backend.token(4),
            child: backend.token(4),
            pipes: Box::new([Token::new(31), Token::new(32), Token::new(33)]),
        }),
        up,
        below,
    );
    backend.from_io(
        &env(0),
        BelowEvent::Io(io::Event::Output {
            owner: Token::new(31),
            up: skein_lib::stream::OutputUp::Settled {
                right: Token::new(1),
                outcome: skein_lib::stream::OutputOutcome::Granted,
            },
        }),
        up,
        below,
    );
    backend.admit_init(&env(0), br#"{"type":"system","subtype":"init","mcp_servers":[{"name":"smith","status":"connected"}],"tools":["mcp__smith__permission_prompt"]}"#, up, below);
    (backend, id)
}

#[test]
fn a_retry_after_a_stopped_process_resumes() {
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    let (mut backend, id) = running_history(&mut up, &mut below);
    below = Queue::with_capacity(64);
    backend.fire(&env(1), &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("head failure") };
    assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Head });
    let mut retry = history(&backend);
    let FromDomain::Complete { owner, .. } = &mut retry else { panic!("retry") };
    *owner = Token::new(81);
    backend.from_domain(&env(1), retry, &mut up, &mut below);
    no_spawn(&below);
    backend.from_io(
        &env(1),
        BelowEvent::Io(io::Event::Stream {
            owner: Token::new(32),
            up: skein_lib::stream::Up::Bytes(b"{\"type\":\"result\",\"is_error\":true}\n".as_slice().into()),
        }),
        &mut up,
        &mut below,
    );
    backend.from_io(
        &env(1),
        BelowEvent::Io(io::Event::Exited { owner: backend.token(4), exit: kernel::Exit::Code(0) }),
        &mut up,
        &mut below,
    );
    for owner in [Token::new(31), Token::new(32), Token::new(33), backend.token(4)] {
        backend.from_io(&env(1), BelowEvent::Io(io::Event::Closed { owner }), &mut up, &mut below);
        no_spawn(&below);
    }
    assert!(backend.switching, "the MCP listener still belongs to the old process");
    backend.from_io(&env(1), BelowEvent::Io(io::Event::Closed { owner: backend.first }), &mut up, &mut below);
    no_spawn(&below);
    assert!(up.is_empty(), "the old result has no terminal for the retry");
    assert!(backend.resuming);
    assert!(!backend.closed());
    assert!(backend.settings.directory.is_some());
    let spawn = launch_from_listener(&mut backend, 1, &mut up, &mut below);
    assert!(spawn.args.windows(2).any(|pair| pair[0].as_ref() == b"--resume" && pair[1].as_ref() == session::uuid(id)));
    assert!(!spawn.args.iter().any(|argument| argument.as_ref() == b"--session-id"));
    backend.from_io(
        &env(1),
        BelowEvent::Io(io::Event::Spawned {
            owner: backend.token(4),
            child: backend.token(4),
            pipes: Box::new([Token::new(51), Token::new(52), Token::new(53)]),
        }),
        &mut up,
        &mut below,
    );
    backend.from_io(
        &env(1),
        BelowEvent::Io(io::Event::Output {
            owner: Token::new(51),
            up: skein_lib::stream::OutputUp::Settled {
                right: Token::new(1),
                outcome: skein_lib::stream::OutputOutcome::Granted,
            },
        }),
        &mut up,
        &mut below,
    );
    let sent = below
        .iter()
        .find_map(|effect| {
            if let Below::Io(io::Request::Output { stream, down: skein_lib::stream::OutputDown::Send { bytes, .. } }) =
                effect
            {
                assert_eq!(*stream, Token::new(51));
                Some(bytes)
            } else {
                None
            }
        })
        .expect("published input");
    let value: serde_json::Value = serde_json::from_slice(sent).expect("one user message");
    assert_eq!(value["message"]["content"], "result of wait-id: wait result\nnew instruction\n");
    assert!(backend.outgoing.is_none());
}

#[test]
fn the_tool_timeout_is_the_run_s_time() {
    let mut backend = launchable();
    backend.run_time(Duration::from_secs(3));
    let spawn = backend.launch_spec().expect("spawn");
    assert_eq!(spawn.env.iter().filter(|entry| entry.starts_with(b"MCP_TOOL_TIMEOUT=")).count(), 1);
    assert!(spawn.env.iter().any(|entry| entry.as_ref() == b"MCP_TOOL_TIMEOUT=3000"));
}

fn first_request(backend: &Backend) -> FromDomain {
    let mut request = request(backend);
    let FromDomain::Complete { prompt, .. } = &mut request else { panic!("complete") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"first task".as_slice().into(), replay: None }]),
    }]);
    request
}

#[test]
fn a_first_call_retry_resumes_the_recorded_session() {
    for init_first in [false, true] {
        let mut backend = launchable();
        let request = first_request(&backend);
        let FromDomain::Complete { prompt, .. } = &request else { panic!("complete") };
        let session::Opening::New { id } = session::opening(prompt, None, None) else { panic!("new") };
        backend.request = Some(request);
        backend.running = Some(id);
        backend.names.session = session::uuid(id);
        backend.resuming = false;
        backend.owner = Some(Token::new(80));
        backend.phase = Phase::Starting;
        backend.child = Some(backend.token(4));
        backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
        backend.launch_spec().expect("new spawn");
        let mut up = Queue::with_capacity(32);
        let mut below = Queue::with_capacity(64);
        let init = br#"{"type":"system","subtype":"init","mcp_servers":[{"name":"smith","status":"connected"}],"tools":["mcp__smith__permission_prompt"]}"#;
        if init_first {
            backend.admit_init(&env(0), init, &mut up, &mut below);
        }
        assert_eq!(backend.recorded, None);
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
        if !init_first {
            assert_eq!(backend.recorded, None, "input alone does not record the ID");
            backend.admit_init(&env(0), init, &mut up, &mut below);
        }
        assert_eq!(backend.recorded, Some(id));
        backend.fire(&env(1), &mut up, &mut below);
        let Some(ToDomain::Failed { failure, .. }) = up.pop() else { panic!("head timeout") };
        assert_eq!(failure, llm::Failure::TimedOut { phase: llm::Phase::Head });
        let retry = first_request(&backend);
        backend.from_domain(&env(1), retry, &mut up, &mut below);
        assert!(backend.switching);
        for owner in [backend.token(4), Token::new(31), Token::new(32), Token::new(33)] {
            backend.from_io(&env(1), BelowEvent::Io(io::Event::Closed { owner }), &mut up, &mut below);
        }
        assert!(backend.resuming);
        let spawn = backend.launch_spec().expect("retry spawn");
        assert!(
            spawn.args.windows(2).any(|pair| pair[0].as_ref() == b"--resume" && pair[1].as_ref() == session::uuid(id))
        );
        assert!(!spawn.args.iter().any(|argument| argument.as_ref() == b"--session-id"));
        let sent: serde_json::Value = serde_json::from_slice(backend.outgoing.as_ref().expect("tail")).expect("input");
        assert_eq!(sent["message"]["content"], "first task\n");
    }
}

#[test]
fn a_wrapped_missing_prefix_stays_a_program_error() {
    let mut backend = launchable();
    backend.phase = Phase::Starting;
    backend.exit = Some(1);
    let prefix = b"No conversation found with session ID";
    backend.stderr = List::with_capacity(u32::try_from(prefix.len()).expect("prefix length"));
    backend.pipes[2] = Some(Token::new(33));
    for bytes in [[b'x'; b"No conversation found with session ID".len()].as_slice(), prefix.as_slice()] {
        backend.io_event(
            &env(0),
            io::Event::Stream { owner: Token::new(33), up: skein_lib::stream::Up::Bytes(bytes.into()) },
            &mut Queue::with_capacity(32),
            &mut Queue::with_capacity(64),
        );
    }
    assert_eq!(backend.tail, 0, "one whole revolution");
    assert_eq!(backend.stderr.as_slice(), prefix);
    assert!(backend.stderr_wrapped);
    assert_eq!(backend.exit_failure(), llm::Failure::Program { exit: Some(1) });
}
