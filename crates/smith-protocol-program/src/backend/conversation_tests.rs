use super::lifecycle_tests::{backend, env, request};
use super::*;

fn active() -> Backend {
    running(backend())
}

fn running(mut backend: Backend) -> Backend {
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes[0] = Some(Token::new(31));
    backend.request = Some(request(&backend));
    backend.owner = Some(Token::new(80));
    backend
}

#[test]
fn an_unframed_tool_message_completes_without_any_partial() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0),br#"{"type":"assistant","message":{"id":"unframed","content":[{"type":"tool_use","id":"t","name":"mcp__smith__finish","input":{}}],"usage":{"input_tokens":17,"output_tokens":3}}}"#,&mut up,&mut below);
    let Some(ToDomain::Completed { owner, completion }) = up.pop() else {
        panic!("unframed completion");
    };
    assert_eq!(owner, Token::new(80));
    assert_eq!(completion.usage.input_tokens, Some(17));
    assert!(backend.turns.message.is_none());
}

#[test]
fn an_open_next_message_does_not_receive_a_duplicate_nudge() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(
        &env(0),
        br#"{"type":"assistant","message":{"id":"one","content":[{"type":"text","text":"thinking"}]}}"#,
        &mut up,
        &mut below,
    );
    backend.line(
        &env(0),
        br#"{"type":"stream_event","event":{"type":"message_start","message":{"id":"next"}}}"#,
        &mut up,
        &mut below,
    );
    assert!(backend.owner.is_none(), "the first message was delivered");
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, owner, .. } = &mut next else {
        panic!("next completion");
    };
    *owner = Token::new(81);
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"finish now".as_slice().into(), replay: None }]),
    }]);
    backend.resume(&env(0), next, &mut up, &mut below);
    assert!(backend.outgoing.is_none());
    assert_eq!(backend.owner, Some(Token::new(81)));
    assert!(below.is_empty());
}

#[test]
fn a_line_outside_the_stream_grammar_is_a_program_failure() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"other":true}"#, &mut up, &mut below);
    let Some(ToDomain::Failed { failure, .. }) = up.pop() else {
        panic!("program failure");
    };
    assert_eq!(failure, llm::Failure::Program { exit: None });
}
#[test]
fn a_result_without_an_assistant_hands_the_nudge_to_the_domain() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"type":"result","subtype":"success","is_error":false}"#, &mut up, &mut below);
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else {
        panic!("no-tool completion");
    };
    assert!(completion.content.is_empty());
    assert_eq!(completion.stop, llm::Stop::EndTurn);
    assert_eq!(completion.usage, llm::Usage::ZERO);
}
#[test]
fn the_unoffered_sub_agent_tool_is_not_executed() {
    let mut backend = active();
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else {
        panic!("completion");
    };
    prompt.served = Box::new([llm::Served::SubAgent]);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0),br#"{"type":"assistant","message":{"id":"unframed","content":[{"type":"tool_use","id":"t","name":"mcp__smith__sub_agent","input":{}}]}}"#,&mut up,&mut below);
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else {
        panic!("completion");
    };
    let llm::Said::ToolCall { call, .. } = &completion.content[0] else {
        panic!("tool call");
    };
    assert_eq!(*call, llm::Decoded::Invalid { problem: llm::Problem::UnknownTool });
}

#[test]
fn arrays_keep_both_boundary_tokens() {
    // Reader records count both opening and closing container brackets.
    for (bytes, count) in [
        (b"[]".as_slice(), 2),
        (b"[[]]".as_slice(), 4),
        (b"[[[]]]".as_slice(), 6),
        (b"[0]".as_slice(), 3),
        (b"[0,0]".as_slice(), 4),
    ] {
        let length = u32::try_from(bytes.len()).unwrap();
        let document = stream::collect(bytes, length, Settings::stream_tokens(length)).expect("bounded JSON");
        assert_eq!(document.len(), count);
    }
}

// The review's actual service ceiling: six escapes per 65,536 answer bytes,
// plus the existing 65,536-byte line framing allowance. This is never enlarged
// to accommodate a token-derived fixture.
const REVIEW_LINE_BYTES: u32 = 458_752;

#[expect(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    reason = "ordinary JSON fixture builder, programming-model.md, section 10.2"
)]
fn dense_result(element: &[u8], length: u32) -> alloc::vec::Vec<u8> {
    const PREFIX: &[u8] = br#"{"type":"result","padding":["#;
    const SUFFIX: &[u8] = b"]}";
    let length = usize::try_from(length).unwrap();
    let room = length.checked_sub(PREFIX.len()).unwrap().checked_sub(SUFFIX.len()).unwrap();
    let width = element.len().checked_add(1).unwrap();
    let count = room.checked_add(1).unwrap().checked_div(width).unwrap();
    let mut bytes = PREFIX.to_vec();
    for index in 0..count {
        if index != 0 {
            bytes.push(b',');
        }
        bytes.extend_from_slice(element);
    }
    bytes.extend_from_slice(SUFFIX);
    bytes.resize(length, b' ');
    bytes
}

fn result_in_stdout(element: &[u8], length: u32) -> Queue<ToDomain> {
    // Backend::new owns one 4096-byte deferred stdout intake.
    const READ_BYTES: usize = 4096;
    let mut backend = running(lifecycle_tests::backend_with_line_bytes(REVIEW_LINE_BYTES));
    let mut bytes = dense_result(element, length);
    bytes.push(b'\n');
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    // The real stdout entrance retains at most one 4096-byte read at a time.
    for chunk in bytes.chunks(READ_BYTES) {
        for byte in chunk {
            backend.intake.push(*byte).unwrap();
        }
        backend.consume_stdout(&env(0), &mut up, &mut below);
    }
    up
}

#[test]
fn a_dense_result_at_the_line_bound_decodes() {
    // Empty array elements retain more tokens per byte than one-digit numbers.
    for element in [b"0".as_slice(), b"[]".as_slice()] {
        let mut up = result_in_stdout(element, REVIEW_LINE_BYTES);
        let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("result at line bound") };
        assert_eq!(completion.stop, llm::Stop::EndTurn);
    }
}

#[test]
fn a_result_past_the_line_bound_is_refused() {
    let mut up = result_in_stdout(b"0", REVIEW_LINE_BYTES.checked_add(1).unwrap());
    let Some(ToDomain::Failed { failure, detail, .. }) = up.pop() else { panic!("line overflow") };
    assert_eq!(failure, llm::Failure::Program { exit: None });
    assert_eq!(detail.as_ref(), b"smith: the program stream exceeds its line bound");
}

#[test]
fn a_result_with_the_reviews_ignored_array_decodes() {
    // The review's 131,610 zeros plus seven container/key/value records:
    // 131,617 tokens, encoded in 263,249 bytes, below REVIEW_LINE_BYTES.
    const REVIEW_RESULT_BYTES: u32 = 263_249;
    let mut up = result_in_stdout(b"0", REVIEW_RESULT_BYTES);
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("review result") };
    assert_eq!(completion.stop, llm::Stop::EndTurn);
}

// Queue capacities used by the existing conversation step harness.
const UP_EVENTS: u32 = 32;
const BELOW_EFFECTS: u32 = 64;

const STATUS: &[u8] = br#"{"type":"system","subtype":"status","status":null}"#;
const REPLY: &[u8] = br#"{"type":"assistant","message":{"id":"reply","content":[{"type":"text","text":"Hello."}]}}"#;
const RESULT: &[u8] = br#"{"type":"result","is_error":false,"terminal_reason":"completed","stop_reason":"end_turn"}"#;

fn active_with_queues() -> (Backend, Queue<ToDomain>, Queue<Below>) {
    (active(), Queue::with_capacity(UP_EVENTS), Queue::with_capacity(BELOW_EFFECTS))
}

fn expected_reply() -> Box<[llm::Said]> {
    Box::new([llm::Said::Text { text: b"Hello.".as_slice().into(), replay: None }])
}

fn finished_reply(up: &Queue<ToDomain>) -> &llm::Completion {
    for event in up {
        match event {
            ToDomain::Completed { completion, .. } => return completion,
            ToDomain::BuiltinResult { .. }
            | ToDomain::Failed { .. }
            | ToDomain::Cancelled { .. }
            | ToDomain::Text { .. } => {}
        }
    }
    panic!("completed reply")
}

fn reply_after_notice(notice: &[u8]) -> (Backend, Queue<ToDomain>) {
    let (mut backend, mut up, mut below) = active_with_queues();
    backend.line(&env(0), notice, &mut up, &mut below);
    backend.line(&env(0), REPLY, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    (backend, up)
}

#[test]
fn status_before_a_completion_preserves_the_reply() {
    let (backend, up) = reply_after_notice(STATUS);
    assert_eq!(finished_reply(&up).content, expected_reply());
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn status_after_a_tool_result_preserves_the_next_completion() {
    let (mut backend, mut up, mut below) = active_with_queues();
    backend.line(&env(0), br#"{"type":"assistant","message":{"id":"call","content":[{"type":"tool_use","id":"read","name":"Read","input":{}}]}}"#, &mut up, &mut below);
    backend.line(
        &env(0),
        br#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"read","content":"file text"}]}}"#,
        &mut up,
        &mut below,
    );
    while backend.pop_measure().is_some() {}
    backend.resume(&env(0), request(&backend), &mut up, &mut below);
    up = Queue::with_capacity(UP_EVENTS);
    backend.line(&env(0), STATUS, &mut up, &mut below);
    backend.line(&env(0), REPLY, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    assert_eq!(finished_reply(&up).content, expected_reply());
    assert_eq!(backend.phase, Phase::Running);
}

fn second_turn() -> (Backend, Queue<ToDomain>, Queue<Below>) {
    let (mut backend, mut up, mut below) = active_with_queues();
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else { panic!("complete") };
    prompt.served = Box::new([llm::Served::Finish]);
    backend.line(&env(0), REPLY, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    while backend.pop_measure().is_some() {}
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("complete") };
    prompt.served = Box::new([llm::Served::Finish]);
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::Text { text: b"second message".as_slice().into(), replay: None }]),
    }]);
    backend.resume(&env(0), next, &mut up, &mut below);
    up = Queue::with_capacity(UP_EVENTS);
    (backend, up, below)
}

#[test]
fn a_valid_later_init_allows_the_second_turn() {
    let (mut backend, mut up, mut below) = second_turn();
    let line = stream::tests::init_line(&["mcp__smith__finish"]);
    backend.line(&env(0), &line, &mut up, &mut below);
    backend.line(&env(0), STATUS, &mut up, &mut below);
    backend.line(&env(0), REPLY, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    assert_eq!(finished_reply(&up).stop, llm::Stop::EndTurn);
    assert_eq!(backend.phase, Phase::Running);
}

fn init_failure(mut up: Queue<ToDomain>) -> (llm::Failure, Box<[u8]>) {
    let Some(ToDomain::Failed { failure, detail, .. }) = up.pop() else { panic!("init failure") };
    (failure, detail)
}

#[test]
fn a_later_init_fails_like_startup_when_tools_are_missing() {
    let line = stream::tests::init_line(&[]);
    let (mut startup, mut startup_up, mut startup_below) = second_turn();
    startup.admit_init(&env(0), &line, &mut startup_up, &mut startup_below);
    let (mut later, mut later_up, mut later_below) = second_turn();
    later.line(&env(0), &line, &mut later_up, &mut later_below);
    let expected =
        (llm::Failure::Program { exit: None }, b"smith: the program started without smith's tools".as_slice().into());
    assert_eq!(init_failure(startup_up), expected);
    assert_eq!(init_failure(later_up), expected);
}

#[test]
fn a_later_init_rechecks_the_runs_builtin_grants() {
    let (mut backend, mut up, mut below) = second_turn();
    let line = stream::tests::init_line(&["mcp__smith__finish", "Read"]);
    backend.line(&env(0), &line, &mut up, &mut below);
    assert_eq!(
        init_failure(up),
        (
            llm::Failure::Program { exit: None },
            b"smith: the program offers a built-in tool the run did not grant: Read".as_slice().into()
        )
    );
}

#[test]
fn an_unknown_system_subtype_preserves_the_reply() {
    let (backend, up) = reply_after_notice(br#"{"type":"system","subtype":"something_new"}"#);
    assert_eq!(finished_reply(&up).content, expected_reply());
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn thinking_tokens_is_a_notice_without_state_or_measure() {
    let (mut backend, mut up, mut below) = active_with_queues();
    backend.line(
        &env(0),
        br#"{"type":"system","subtype":"thinking_tokens","estimated_tokens":50,"estimated_tokens_delta":50}"#,
        &mut up,
        &mut below,
    );
    assert_eq!(backend.phase, Phase::Running);
    assert!(backend.turns.message.is_none());
    assert_eq!(backend.turns.closed, 0);
    assert!(backend.pop_measure().is_none());
    assert!(up.is_empty());
    assert!(below.is_empty());
}
