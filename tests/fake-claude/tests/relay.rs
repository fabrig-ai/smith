//! Fake Claude, shipped relay and real MCP server (programs.md, 5.3, 5.4, 9).
#[allow(dead_code)]
mod support;
use serde_json::{Value, json};
use smith_fake_claude::{End, Step, Story, Turn};
use smith_protocol_program::mcp::Up;
use support::relay::{self, Run};
#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;
fn story(steps: Vec<Step>) -> Story {
    Story { turns: vec![Turn { steps, end: End::default() }], ..Story::default() }
}

fn builtin() -> Step {
    Step::Builtin { tool: "Read".into(), input: json!({"file_path":"file"}), result: "story result".into() }
}

fn result(run: &Run) -> &Value {
    &run.messages.iter().find(|message| message["type"] == "user").expect("tool result")["message"]["content"][0]
}

#[test]
fn the_relay_connects_claude_to_the_agent_s_server() {
    let run = relay::run(Story::default(), false, false);
    assert_eq!(run.messages[0]["mcp_servers"], json!([{"name":"smith","status":"connected"}]));
    let tools = run.messages[0]["tools"].as_array().expect("tools");
    assert!(tools.contains(&json!("mcp__smith__finish")));
    assert!(!tools.contains(&json!("mcp__smith__permission_prompt")));
    assert_eq!(run.seen[0], Up::Listening { port: 47100 });
    assert_eq!(run.seen[1], Up::Connected);
}

#[test]
fn a_tool_call_reaches_the_agent_then_its_answer_reaches_the_model() {
    let run = relay::run(story(vec![Step::Call { tool: "finish".into(), input: json!({"answer":42}) }]), false, false);
    let Up::Call { tool, input, .. } = &run.seen[2] else { panic!("call") };
    assert_eq!(tool.as_ref(), b"finish");
    assert_eq!(serde_json::from_slice::<Value>(input).expect("input"), json!({"answer":42}));
    assert_eq!(result(&run)["content"], "accepted");
    assert_eq!(result(&run)["is_error"], false);
}

#[test]
fn an_allowed_builtin_returns_the_story_s_result() {
    let run = relay::run(story(vec![builtin()]), false, false);
    let Up::Prompt { tool_name, input, tool_use_id, .. } = &run.seen[2] else { panic!("prompt") };
    assert_eq!(tool_name.as_ref(), b"Read");
    assert_eq!(serde_json::from_slice::<Value>(input).expect("input"), json!({"file_path":"file"}));
    assert_eq!(tool_use_id.as_deref(), Some(b"toolu_1".as_slice()));
    assert_eq!(result(&run)["content"], "story result");
    assert_eq!(result(&run)["is_error"], false);
}

#[test]
fn a_denied_builtin_returns_the_agent_s_message() {
    let run = relay::run(story(vec![builtin()]), true, false);
    assert_eq!(result(&run)["content"], "smith: no built-in tool is granted");
    assert_eq!(result(&run)["is_error"], true);
}

#[test]
fn a_relay_with_the_wrong_secret_is_refused() {
    let run = relay::run(Story::default(), false, true);
    assert!(!run.seen.iter().any(|up| matches!(up, Up::Connected)));
    assert_eq!(run.messages[0]["mcp_servers"], json!([{"name":"smith","status":"failed"}]));
    assert_eq!(run.relay_exit, Some(skein_io::kernel::Exit::Code(1)));
    assert_eq!(run.relay_detail, Some(b"smith-agent: the relay's agent refused it\n".as_slice()));
}
