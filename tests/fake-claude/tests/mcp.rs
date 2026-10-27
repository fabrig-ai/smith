//! The fake and its MCP child hosted together, per protocol/programs.md,
//! sections 5 and 8. Referee signals reach the fake rather than its peer.

#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use skein_io::kernel::{Exit, ServiceSignal};
use smith_fake_claude::{Answer, Decision, End, Seen, ServerScript, Step, Story, Tool, Turn, Verdict, seen};
use support::print::{self, messages};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

fn story(steps: Vec<Step>) -> Story {
    Story { turns: vec![Turn { steps, end: End::default() }], ..Story::default() }
}

fn script(answer: Answer) -> ServerScript {
    ServerScript { tools: vec![Tool { name: "finish".into(), answer }], ..ServerScript::default() }
}

fn builtin() -> Step {
    Step::Builtin { tool: "Read".into(), input: json!({"file_path":"private-value"}), result: "scripted result".into() }
}

fn tool_result(run: &support::Run) -> Value {
    messages(run).into_iter().find(|message| message["type"] == "user").expect("tool result")["message"]["content"][0]
        .clone()
}

#[test]
fn init_reports_the_configured_server_connected_with_its_tools() {
    let run = print::with_server(Story::default(), print::invocation(0), script(Answer::Text("done".into())));
    let init = &messages(&run)[0];
    assert_eq!(init["mcp_servers"], json!([{"name":"smith", "status":"connected"}]));
    assert_eq!(init["tools"], json!(["Read", "Bash", "mcp__smith__finish"]));
    assert_eq!(run.server_exit, Some(Exit::Code(0)));
}

#[test]
fn a_server_that_cannot_start_is_reported_failed() {
    let config = br#"{"mcpServers":{"smith":{"command":"missing"}}}"#;
    let run = print::run(Story::default(), print::invocation(0), None, Some(config));
    assert_eq!(messages(&run)[0]["mcp_servers"][0]["status"], "failed");
    assert_eq!(messages(&run)[0]["tools"], json!(["Read", "Bash"]));
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn a_silent_server_withholds_the_init() {
    let invocation = print::signal(print::invocation(0), ServiceSignal::Terminate, "server");
    let run =
        print::with_server(Story::default(), invocation, ServerScript { connects: false, ..ServerScript::default() });
    assert!(run.signalled);
    assert!(run.records[0].stdout.is_empty());
    assert_eq!(run.records[0].exit, Some(Exit::Code(143)));
    assert_eq!(run.server_exit, Some(Exit::Code(0)));
}

#[test]
fn a_server_tool_call_returns_its_answer_to_the_model() {
    let run = print::with_server(
        story(vec![Step::Call { tool: "finish".into(), input: json!({"answer":"private-value"}) }]),
        print::invocation(1),
        script(Answer::Text("accepted".into())),
    );
    let message = &messages(&run)[2]["message"];
    assert_eq!(
        message["content"][0],
        json!({"type":"tool_use", "id":"toolu_1", "name":"mcp__smith__finish", "input":{"answer":"private-value"}, "caller":{"type":"direct"}})
    );
    assert_eq!(
        tool_result(&run),
        json!({"type":"tool_result", "tool_use_id":"toolu_1", "content":"accepted", "is_error":false})
    );
    assert_eq!(run.calls, ["finish"]);
    let result = messages(&run).pop().expect("result");
    assert_eq!(result["usage"]["input_tokens"], 10);
}

#[test]
fn a_builtin_call_asks_the_prompt_tool_first() {
    let run = print::with_server(story(vec![builtin()]), print::invocation(1), ServerScript::default());
    assert_eq!(run.calls, ["prompt"]);
    let message = &messages(&run)[2]["message"]["content"][0];
    assert_eq!(message["name"], "Read");
    assert_eq!(message["id"], tool_result(&run)["tool_use_id"]);
    assert_eq!(
        seen(&run.observations.expect("observations"))[1],
        Seen::Prompted { tool: "Read".into(), decision: Decision::Allow { unchanged: true } }
    );
}

#[test]
fn an_allowed_builtin_returns_the_story_s_result() {
    let run = print::with_server(story(vec![builtin()]), print::invocation(1), ServerScript::default());
    assert_eq!(tool_result(&run)["content"], "scripted result");
    assert_eq!(tool_result(&run)["is_error"], false);
}

#[test]
fn a_denied_builtin_returns_the_denial_to_the_model() {
    let run = print::with_server(
        story(vec![builtin(), builtin()]),
        print::invocation(1),
        ServerScript { verdicts: vec![Verdict::Deny("outside workspace".into())], ..ServerScript::default() },
    );
    assert_eq!(tool_result(&run)["content"], "outside workspace");
    assert_eq!(tool_result(&run)["is_error"], true);
    let results: Vec<_> = messages(&run).into_iter().filter(|message| message["type"] == "user").collect();
    assert_eq!(results[1]["message"]["content"][0]["is_error"], false, "verdicts default to allow after the script");
}

#[test]
fn an_unreadable_prompt_answer_is_an_error_for_the_model() {
    let run = print::with_server(
        story(vec![builtin()]),
        print::invocation(1),
        ServerScript {
            prompt: "other".into(),
            tools: vec![Tool { name: "prompt".into(), answer: Answer::Text("not JSON".into()) }],
            ..ServerScript::default()
        },
    );
    assert_eq!(tool_result(&run)["is_error"], true);
    assert_eq!(
        seen(&run.observations.expect("observations"))[1],
        Seen::Prompted { tool: "Read".into(), decision: Decision::Unreadable }
    );
}

#[test]
fn an_interrupt_during_a_server_call_ends_the_turn() {
    let invocation = print::signal(print::invocation(1), ServiceSignal::Interrupt, "call");
    let run = print::with_server(
        story(vec![Step::Call { tool: "finish".into(), input: json!({}) }]),
        invocation,
        script(Answer::Never),
    );
    assert!(run.signalled);
    let messages = messages(&run);
    assert!(!messages.iter().any(|message| message["type"] == "user"));
    let results: Vec<_> = messages.iter().filter(|message| message["type"] == "result").collect();
    assert_eq!(results[0]["terminal_reason"], "aborted_streaming");
    assert_eq!(results.len(), 1);
    assert_eq!(run.records[0].exit, Some(skein_io::kernel::Exit::Code(0)));
    assert_eq!(run.calls, ["finish"]);
    assert_eq!(
        seen(run.observations.as_ref().expect("observations"))[2],
        Seen::Called { tool: "finish".into(), error: true }
    );
}

#[test]
fn a_termination_ends_the_server_first() {
    let invocation = print::signal(print::invocation(1), ServiceSignal::Terminate, "call");
    let run = print::with_server(
        story(vec![Step::Call { tool: "finish".into(), input: json!({}) }]),
        invocation,
        script(Answer::Never),
    );
    assert_eq!(run.server_exit, Some(Exit::Code(0)));
    assert_eq!(run.records[0].exit, Some(Exit::Code(143)));
    assert_eq!(
        seen(run.observations.as_ref().expect("observations"))[2],
        Seen::Called { tool: "finish".into(), error: true }
    );
}

#[test]
fn each_call_is_observed_without_its_values() {
    let run = print::with_server(
        story(vec![Step::Call { tool: "finish".into(), input: json!({"secret":"private-value"}) }, builtin()]),
        print::invocation(1),
        script(Answer::Error("private-value".into())),
    );
    let bytes = run.observations.expect("observations");
    assert!(!String::from_utf8_lossy(&bytes).contains("private-value"));
    assert_eq!(
        &seen(&bytes)[1..],
        [
            Seen::Called { tool: "finish".into(), error: true },
            Seen::Prompted { tool: "Read".into(), decision: Decision::Allow { unchanged: true } }
        ]
    );
    assert_eq!(smith_fake_claude::observations(&bytes).len(), 1);
}

#[test]
fn a_late_server_answer_is_dropped() {
    let first = Step::Call { tool: "finish".into(), input: json!({}) };
    let mut story = story(vec![first.clone()]);
    story.turns.push(Turn { steps: vec![first], end: End::default() });
    let invocation = print::control(print::invocation(2), "call");
    let run = print::with_server(story, invocation, script(Answer::Text("late".repeat(256))));
    let messages = messages(&run);
    assert_eq!(messages.iter().filter(|message| message["type"] == "user").count(), 1);
    assert_eq!(tool_result(&run)["tool_use_id"], "toolu_2");
    assert_eq!(run.calls, ["finish", "finish"]);
}

// T1 measured a relay tools/list line of 25,566 bytes. A long tool name
// exercises that scale through the hosted fake and its actual I/O driver.
const RELAY_LIST_BYTES: usize = 25_566;

#[test]
fn a_relay_sized_tool_list_fits_the_memory_envelope() {
    let run = print::with_server(
        Story::default(),
        print::invocation(0),
        ServerScript {
            tools: vec![Tool { name: "x".repeat(RELAY_LIST_BYTES), answer: Answer::default() }],
            ..ServerScript::default()
        },
    );
    assert_eq!(messages(&run)[0]["mcp_servers"][0]["status"], "connected");
    let heap = run.heap.expect("world checks memory");
    eprintln!("relay-sized tool list process (peak, envelope): {heap:?}");
    assert!(heap.iter().all(|(peak, envelope)| peak <= envelope));
}
