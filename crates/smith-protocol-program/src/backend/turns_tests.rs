#![expect(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    reason = "ordinary Rust step harness, programming-model.md 10.2"
)]
use super::{stream, turns::Turns};
use serde_json::{Value, json};
use smith_protocol_llm::Receiving;

fn turns() -> Turns {
    Turns::new(
        &Receiving {
            max_completion_bytes: 4096,
            max_completion_blocks: 8,
            decoded_call_bytes: 4096,
            max_failure_bytes: 256,
        },
        4096,
    )
}
fn feed(turns: &mut Turns, value: Value) -> Option<()> {
    let bytes = serde_json::to_vec(&value).expect("bounded test fixture");
    let doc = stream::collect(&bytes, 32, 256).expect("bounded test fixture");
    match value["type"].as_str().expect("bounded test fixture") {
        "assistant" => turns.assistant(&doc, 4096).map(|_| ()),
        "stream_event" => turns.event(&doc, crate::mcp::json::field(&doc, 0, b"event").expect("bounded test fixture")),
        _ => unreachable!(),
    }
}
fn partial(kind: &str) -> Value {
    json!({"type":"stream_event","event":{"type":kind,"index":0,"message":{"id":"m"}}})
}
fn assistant(content: Value) -> Value {
    json!({"type":"assistant","message":{"id":"m","content":content,"usage":{"input_tokens":13,"output_tokens":7}}})
}
fn call() -> Value {
    json!({"type":"tool_use","id":"t","name":"mcp__smith__finish","input":{"form":"report","text":"done","fields":{}}})
}

#[test]
fn a_partial_first_message_waits_for_its_content() {
    let mut turns = turns();
    for kind in ["message_start", "content_block_start", "message_stop"] {
        feed(&mut turns, partial(kind)).expect("bounded test fixture");
    }
    assert_eq!(turns.ready.len(), 0);
    feed(&mut turns, assistant(json!([call()]))).expect("bounded test fixture");
    let completion = turns.ready.pop().expect("bounded test fixture");
    assert_eq!(completion.usage.input_tokens, Some(13));
    assert_eq!(completion.content.len(), 1);
}
#[test]
fn full_content_waits_for_the_stop() {
    let mut turns = turns();
    feed(&mut turns, partial("message_start")).expect("bounded test fixture");
    feed(&mut turns, partial("content_block_start")).expect("bounded test fixture");
    feed(&mut turns, assistant(json!([call()]))).expect("bounded test fixture");
    assert_eq!(turns.ready.len(), 0);
    feed(&mut turns, partial("message_stop")).expect("bounded test fixture");
    assert_eq!(turns.ready.len(), 1);
}
#[test]
fn split_full_content_fills_all_started_blocks() {
    let mut turns = turns();
    feed(&mut turns, partial("message_start")).expect("bounded test fixture");
    feed(&mut turns, partial("content_block_start")).expect("bounded test fixture");
    let mut second = partial("content_block_start");
    second["event"]["index"] = json!(1_u32);
    feed(&mut turns, second).expect("bounded test fixture");
    feed(&mut turns, partial("message_stop")).expect("bounded test fixture");
    feed(&mut turns, assistant(json!([{"type":"text","text":"first"}]))).expect("bounded test fixture");
    assert_eq!(turns.ready.len(), 0);
    feed(&mut turns, assistant(json!([call()]))).expect("bounded test fixture");
    assert_eq!(turns.ready.pop().expect("bounded test fixture").content.len(), 2);
}
#[test]
fn an_unframed_assistant_is_complete_on_receipt() {
    let mut turns = turns();
    feed(&mut turns, assistant(json!([call()]))).expect("bounded test fixture");
    assert_eq!(turns.ready.len(), 1);
}
#[test]
fn a_fifth_waiting_completion_is_refused() {
    let mut turns = turns();
    for _ in 0_u32..4 {
        feed(&mut turns, assistant(json!([call()]))).expect("bounded test fixture");
    }
    assert!(feed(&mut turns, assistant(json!([call()]))).is_none());
    assert_eq!(turns.ready.len(), 4);
}

#[test]
fn consecutive_unframed_texts_have_distinct_completions() {
    let mut turns = turns();
    feed(&mut turns, assistant(json!([{"type":"text","text":"one"}]))).expect("first atomic message");
    let mut next = assistant(json!([{"type":"text","text":"two"}]));
    next["message"]["id"] = json!("second");
    feed(&mut turns, next).expect("next atomic message");
    assert_eq!(turns.ready.len(), 1);
    turns.close(true).expect("turn result");
    assert_eq!(turns.ready.len(), 2);
}
#[test]
fn structured_output_completes_the_pending_text_message() {
    let mut turns = turns();
    feed(&mut turns, assistant(json!([{"type":"text","text":"one"}]))).expect("atomic text");
    turns.structured(br#"{"form":"report","text":"done","fields":{}}"#.as_slice().into()).expect("synthetic finish");
    let completion = turns.ready.pop().expect("one message");
    assert_eq!(completion.content.len(), 2);
    assert_eq!(completion.usage.input_tokens, Some(13));
    assert!(turns.ready.is_empty());
}

#[test]
fn ignored_thinking_still_fills_its_started_block() {
    let mut turns = turns();
    feed(&mut turns, partial("message_start")).expect("start");
    feed(&mut turns, partial("content_block_start")).expect("thinking start");
    let mut next = partial("content_block_start");
    next["event"]["index"] = json!(1_u32);
    feed(&mut turns, next).expect("tool start");
    feed(&mut turns, partial("message_stop")).expect("stop");
    feed(
        &mut turns,
        assistant(json!([{"type":"thinking","thinking":"private reasoning","signature":"opaque"},call()])),
    )
    .expect("all blocks arrive");
    let completion = turns.ready.pop().expect("closed message");
    assert_eq!(completion.content.len(), 1);
    assert_eq!(completion.usage.input_tokens, Some(13));
}

// Owner's live thinking turn: empty deltas, signed thinking, then a separate
// block record under the same ID; final usage supersedes both initial copies.
fn thinking_turn(turns: &mut Turns, block: Value) {
    let records = [
        partial("message_start"),
        json!({"type":"stream_event","event":{"type":"content_block_start","index":0_u32,"content_block":{"type":"thinking","thinking":"","signature":""}}}),
        json!({"type":"stream_event","event":{"type":"content_block_delta","index":0_u32,"delta":{"type":"thinking_delta","thinking":"","estimated_tokens":100_u32}}}),
        json!({"type":"stream_event","event":{"type":"content_block_delta","index":0_u32,"delta":{"type":"signature_delta","signature":"opaque"}}}),
        assistant(json!([{"type":"thinking","thinking":"","signature":"opaque"}])),
        partial("content_block_stop"),
        json!({"type":"stream_event","event":{"type":"content_block_start","index":1_u32,"content_block":block}}),
        assistant(json!([block])),
        json!({"type":"stream_event","event":{"type":"content_block_stop","index":1_u32}}),
        json!({"type":"stream_event","event":{"type":"message_delta","usage":{"input_tokens":13_u32,"output_tokens":154_u32,"cache_creation_input_tokens":648_u32,"cache_read_input_tokens":2646_u32,"output_tokens_details":{"thinking_tokens":100_u32}}}}),
        partial("message_stop"),
    ];
    for record in records {
        feed(turns, record).expect("recorded thinking sequence");
    }
}

fn thinking_usage() -> smith_domain::llm::Usage {
    smith_domain::llm::Usage {
        input_tokens: Some(13),
        output_tokens: Some(154),
        cache_write_tokens: Some(648),
        cache_read_tokens: Some(2646),
        reasoning_tokens: None,
    }
}

#[test]
fn thinking_and_a_call_are_one_completion_with_final_usage() {
    let mut turns = turns();
    let mut tool = call();
    tool["caller"] = json!({"type":"direct"});
    thinking_turn(&mut turns, tool);
    let completion = turns.ready.pop().expect("one completion");
    match completion.content.as_ref() {
        [smith_domain::llm::Said::ToolCall { name, .. }] => assert_eq!(name.as_ref(), b"finish"),
        _ => panic!("one tool call"),
    }
    assert_eq!(completion.usage, thinking_usage());
    assert!(turns.ready.is_empty());
    assert_eq!(turns.closed, 1);
}

#[test]
fn thinking_and_text_keep_the_text_in_one_completion() {
    let mut turns = turns();
    thinking_turn(&mut turns, json!({"type":"text","text":"Hello."}));
    turns.close(true).expect("result boundary");
    let completion = turns.ready.pop().expect("one completion");
    assert_eq!(
        completion.content.as_ref(),
        [smith_domain::llm::Said::Text { text: b"Hello.".as_slice().into(), replay: None }]
    );
    assert_eq!(completion.usage, thinking_usage());
    assert!(turns.ready.is_empty());
    assert_eq!(turns.closed, 1);
}

#[test]
fn a_new_id_after_thinking_opens_a_new_completion() {
    let mut turns = turns();
    thinking_turn(&mut turns, json!({"type":"text","text":"Hello."}));
    let mut next = assistant(json!([call()]));
    next["message"]["id"] = json!("next");
    feed(&mut turns, next).expect("new ID");
    assert_eq!(turns.closed, 2);
    assert_eq!(turns.ready.pop().expect("thinking message").usage, thinking_usage());
    assert_eq!(turns.ready.pop().expect("next message").usage.output_tokens, Some(7));
    assert!(turns.ready.is_empty());
}

fn structured_call() -> Value {
    json!({"type":"tool_use","id":"s","name":"StructuredOutput","input":{"outcome":{"form":"report","text":"draft","fields":{}}}})
}

const FINISH_INPUT: &[u8] = br#"{"form":"report","text":"done","fields":{}}"#;

#[test]
fn structured_output_and_its_outcome_are_one_model_completion() {
    let mut turns = turns();
    // The sole model block uses all available block credit; translation adds none.
    turns.blocks = 1;
    feed(&mut turns, assistant(json!([structured_call()]))).expect("structured message");
    assert!(turns.ready.is_empty());
    turns.structured(FINISH_INPUT.into()).expect("finish in the same message");
    let completion = turns.ready.pop().expect("model completion");
    let [smith_domain::llm::Said::ToolCall { name, input, .. }] = completion.content.as_ref() else {
        panic!("one finish")
    };
    assert_eq!(name.as_ref(), b"finish");
    assert_eq!(input.as_ref(), FINISH_INPUT);
    assert_eq!(completion.usage.input_tokens, Some(13));
    assert_eq!(completion.usage.output_tokens, Some(7));
    assert!(turns.ready.is_empty());
}

#[test]
fn finish_keeps_the_structured_calls_position_among_blocks() {
    let mut turns = turns();
    feed(
        &mut turns,
        assistant(json!([{"type":"text","text":"before"}, structured_call(), {"type":"text","text":"after"}])),
    )
    .expect("structured message");
    turns.structured(FINISH_INPUT.into()).expect("finish in position");
    let completion = turns.ready.pop().expect("model completion");
    let [
        smith_domain::llm::Said::Text { text: before, .. },
        smith_domain::llm::Said::ToolCall { name, input, .. },
        smith_domain::llm::Said::Text { text: after, .. },
    ] = completion.content.as_ref()
    else {
        panic!("original block order")
    };
    assert_eq!(before.as_ref(), b"before");
    assert_eq!(name.as_ref(), b"finish");
    assert_eq!(input.as_ref(), FINISH_INPUT);
    assert_eq!(after.as_ref(), b"after");
    assert!(turns.ready.is_empty());
}

#[test]
fn an_mcp_name_does_not_become_structured_output() {
    let mut turns = turns();
    let block = json!({"type":"tool_use","id":"host","name":"mcp__smith__StructuredOutput","input":{}});
    feed(&mut turns, assistant(json!([block]))).expect("MCP message");
    let completion = turns.ready.pop().expect("MCP calls are delivered immediately");
    let [smith_domain::llm::Said::ToolCall { id, name, .. }] = completion.content.as_ref() else { panic!("MCP call") };
    assert_eq!(id.as_ref(), b"host");
    assert_eq!(name.as_ref(), b"StructuredOutput");
}
