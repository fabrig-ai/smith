//! Open stream boundaries (protocol/programs.md 5.9).
#![expect(clippy::disallowed_methods, reason = "ordinary Rust step harness, programming-model.md 10.2")]
use super::lifecycle_tests::{backend, env, request};
use super::*;

fn running() -> Backend {
    let mut backend = backend();
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes = [Some(Token::new(31)), Some(Token::new(32)), Some(Token::new(33))];
    backend.owner = Some(Token::new(80));
    backend.request = Some(request(&backend));
    backend
}

fn running_with_queues() -> (Backend, Queue<ToDomain>, Queue<Below>) {
    (running(), Queue::with_capacity(32), Queue::with_capacity(64))
}

fn assistant(backend: &mut Backend, up: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
    backend.line(&env(0), br#"{"type":"assistant","message":{"id":"reply","content":[{"type":"text","text":"Hello."}],"usage":{"input_tokens":10,"output_tokens":5}}}"#, up, below);
}

fn reply(backend: &mut Backend, up: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
    assistant(backend, up, below);
    result(backend, up, below);
}

fn result(backend: &mut Backend, up: &mut Queue<ToDomain>, below: &mut Queue<Below>) {
    backend.line(&env(0), br#"{"type":"result","is_error":false}"#, up, below);
}

fn interrupt_sent(below: &Queue<Below>) -> bool {
    below
        .iter()
        .filter_map(|effect| {
            if let Below::Io(io::Request::Output { down: skein_lib::stream::OutputDown::Room { right, .. }, .. }) =
                effect
            {
                Some(*right)
            } else {
                None
            }
        })
        .any(|right| right == Token::new(2))
}

fn user(text: &[u8]) -> llm::Message {
    llm::Message { role: llm::Role::User, content: Box::new([llm::Block::Text { text: text.into(), replay: None }]) }
}

fn completed(up: &Queue<ToDomain>) -> &llm::Completion {
    up.iter()
        .find_map(|event| if let ToDomain::Completed { completion, .. } = event { Some(completion) } else { None })
        .expect("completion")
}

#[test]
fn an_open_result_completes_text_and_keeps_input_open() {
    let (mut backend, mut up, mut below) = running_with_queues();
    reply(&mut backend, &mut up, &mut below);
    let completion = completed(&up);
    assert_eq!(completion.stop, llm::Stop::EndTurn);
    let llm::Said::Text { text, .. } = &completion.content[0] else { panic!("text reply") };
    assert_eq!(text.as_ref(), b"Hello.");
    assert_eq!(backend.pipes[0], Some(Token::new(31)));
    assert_eq!(
        backend
            .io
            .iter()
            .filter_map(|effect| { if let io::Request::Close { entity } = effect { Some(entity) } else { None } })
            .count(),
        0
    );
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn a_listening_message_writes_one_new_user_line() {
    let (mut backend, mut up, mut below) = running_with_queues();
    reply(&mut backend, &mut up, &mut below);
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.messages = Box::new([
        user(b"old input"),
        llm::Message { role: llm::Role::Assistant, content: Box::default() },
        user(b"new host message"),
    ]);
    backend.resume(&env(0), next, &mut up, &mut below);
    let line = backend.outgoing.as_ref().expect("new user line");
    let message: serde_json::Value = serde_json::from_slice(line).expect("stream JSON");
    assert_eq!(message["message"]["content"], "new host message\n");
    assert_eq!(line.iter().position(|byte| *byte == b'\n'), line.len().checked_sub(1));
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
    let written: Box<[_]> = below
        .iter()
        .filter_map(|effect| {
            if let Below::Io(io::Request::Output { down: skein_lib::stream::OutputDown::Send { bytes, .. }, .. }) =
                effect
            {
                Some(bytes)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(written.len(), 1);
    let message: serde_json::Value = serde_json::from_slice(written[0]).expect("written user line");
    assert_eq!(message["message"]["content"], "new host message\n");
}

#[test]
fn an_interrupted_open_turn_yields_without_failure() {
    let (mut backend, mut up, mut below) = running_with_queues();
    backend.yield_for_wind_down(&env(0), &mut up, &mut below);
    backend.line(&env(0), br#"{"type":"result","is_error":true,"subtype":"error_during_execution","terminal_reason":"aborted_streaming"}"#, &mut up, &mut below);
    assert_eq!(completed(&up).stop, llm::Stop::EndTurn);
    assert!(completed(&up).content.is_empty());
    assert!(backend.pending_failure.is_none());
    assert_eq!(backend.phase, Phase::Running);
    assert!(interrupt_sent(&below));
}

#[test]
fn an_open_final_request_enters_the_wind_down() {
    let mut backend = running();
    backend.turns.result = true;
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.choice = llm::ToolChoice::None;
    prompt.messages = Box::new([user(b"Reply now with what you have")]);
    backend.resume(&env(0), next, &mut Queue::with_capacity(32), &mut Queue::with_capacity(64));
    assert!(backend.winding == stop::WindDown::Finishing);
    assert_eq!(backend.phase, Phase::Running);
    let line: serde_json::Value =
        serde_json::from_slice(backend.outgoing.as_ref().expect("finishing input")).expect("user line");
    assert_eq!(line["message"]["content"], "Reply now with what you have\n");
}

#[test]
fn an_open_launch_has_no_result_schema() {
    let arguments = start::arguments(b"fake", None, b"instructions", b"mcp", b"", 8, b"session", b"").expect("argv");
    assert!(!arguments.iter().any(|argument| argument.as_ref() == b"--json-schema"));
}

#[test]
fn a_host_message_uses_input_room_not_tool_reply_room() {
    let mut backend = running();
    backend.settings.adapter.rendered_result = 4;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    reply(&mut backend, &mut up, &mut below);
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.messages = Box::new([user(b"a longer host message")]);
    backend.resume(&env(0), next, &mut up, &mut below);
    let line: serde_json::Value =
        serde_json::from_slice(backend.outgoing.as_ref().expect("host input fits")).expect("user line");
    assert_eq!(line["message"]["content"], "a longer host message\n");
}

#[test]
fn a_yield_interrupts_and_returns_the_held_completion() {
    let (mut backend, mut up, mut below) = running_with_queues();
    assistant(&mut backend, &mut up, &mut below);
    backend.from_domain(&env(0), FromDomain::Yield { owner: Token::new(80) }, &mut up, &mut below);
    assert!(interrupt_sent(&below));
    result(&mut backend, &mut up, &mut below);
    let completions: Box<[_]> = up
        .iter()
        .filter_map(|event| match event {
            ToDomain::Completed { owner, completion } => Some((*owner, completion)),
            ToDomain::BuiltinResult { .. }
            | ToDomain::Failed { .. }
            | ToDomain::Cancelled { .. }
            | ToDomain::Text { .. } => None,
        })
        .collect();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].0, Token::new(80));
    let llm::Said::Text { text, .. } = &completions[0].1.content[0] else { panic!("held text") };
    assert_eq!(text.as_ref(), b"Hello.");
}

#[test]
fn a_yield_explains_the_next_finishing_note() {
    let (mut backend, mut up, mut below) = running_with_queues();
    backend.yield_for_wind_down(&env(0), &mut up, &mut below);
    result(&mut backend, &mut up, &mut below);
    while backend.pop_measure().is_some() {}
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.choice = llm::ToolChoice::None;
    prompt.messages = Box::new([user(b"Reply now with what you have")]);
    backend.resume(&env(0), next, &mut up, &mut below);
    let note: serde_json::Value =
        serde_json::from_slice(backend.outgoing.as_ref().expect("finishing input")).expect("user line");
    assert_eq!(
        note["message"]["content"],
        "(The agent paused your last turn to send the finishing note.)\nReply now with what you have\n"
    );
}

#[test]
fn a_yield_without_an_outstanding_completion_is_ignored() {
    let (mut backend, mut up, mut below) = running_with_queues();
    backend.owner = None;
    backend.request = None;
    backend.from_domain(&env(0), FromDomain::Yield { owner: Token::new(80) }, &mut up, &mut below);
    assert!(up.is_empty());
    assert!(below.is_empty());
    assert!(backend.stop == stop::Stop::None);
}

#[test]
fn a_yield_during_startup_waits_for_the_running_boundary() {
    let (mut backend, mut up, mut below) = running_with_queues();
    backend.phase = Phase::Starting;
    backend.from_domain(&env(0), FromDomain::Yield { owner: Token::new(80) }, &mut up, &mut below);
    assert!(!interrupt_sent(&below));
    backend.phase = Phase::Running;
    backend.fire(&env(0), &mut up, &mut below);
    assert!(interrupt_sent(&below));
    assert!(up.is_empty());
}
