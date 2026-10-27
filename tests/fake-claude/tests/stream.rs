//! The owner's recorded stream shapes, reports/live-claude-sequence.txt.
#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use smith_fake_claude::{Answer, End, ServerScript, Step, Story, Tool, Turn};
use support::print::{self, messages};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[test]
fn init_has_the_recorded_keys_and_thirteen_capabilities() {
    let run = print::normal(Story::default(), 0);
    let init = &messages(&run)[0];
    assert_eq!(
        init.as_object().expect("init object").keys().map(String::as_str).collect::<Vec<_>>(),
        [
            "additional_directories",
            "agents",
            "analytics_disabled",
            "apiKeySource",
            "capabilities",
            "claude_code_version",
            "cwd",
            "fast_mode_disabled_reason",
            "fast_mode_state",
            "mcp_servers",
            "memory_paths",
            "messaging_socket_path",
            "model",
            "original_cwd",
            "output_style",
            "per_turn_effort_active",
            "permissionMode",
            "plugins",
            "product_feedback_disabled",
            "session_id",
            "skills",
            "slash_commands",
            "subtype",
            "terminal_slash_commands",
            "tools",
            "type",
            "uuid",
            "view_mode"
        ]
    );
    assert_eq!(
        init["capabilities"],
        json!([
            "interrupt_receipt_v1",
            "interrupt_cancel_queued_v1",
            "interrupt_send_now_v1",
            "interrupt_requeue_taken_v1",
            "msg_lifecycle_v1",
            "request_marker_lists_v1",
            "lifecycle_wakeup_source_v1",
            "sdk_mcp_tools_list_changed",
            "sdk_mcp_manifests",
            "mcp_read_resource_v1",
            "mcp_tool_ui_meta_v1",
            "ui_surface_v1",
            "mcp_call_result_not_for_model_v1"
        ])
    );
    assert_eq!(init["tools"], json!(["Read", "Bash"]));
}

fn shape(message: &Value) -> (&str, &str) {
    let kind = message["type"].as_str().expect("record type");
    let detail = match kind {
        "system" | "result" => &message["subtype"],
        "stream_event" => &message["event"]["type"],
        "assistant" | "user" => &message["message"]["content"][0]["type"],
        _ => panic!("unexpected stream record"),
    };
    (kind, detail.as_str().expect("record detail"))
}

#[test]
fn a_tool_reply_and_second_turn_follow_the_recorded_order() {
    let story = Story {
        turns: vec![Turn {
            steps: vec![Step::Call { tool: "echo".into(), input: json!({}) }, Step::Text("reply".into())],
            end: End::default(),
        }],
        ..Story::default()
    };
    let mut arguments = print::arguments();
    arguments.push("--include-partial-messages".into());
    let run = print::with_server(
        story,
        print::invocation(2).arguments(&arguments.iter().map(String::as_str).collect::<Vec<_>>()),
        ServerScript {
            tools: vec![Tool { name: "echo".into(), answer: Answer::Text("echo".into()) }],
            ..ServerScript::default()
        },
    );
    let messages = messages(&run);
    assert_eq!(
        messages.iter().map(shape).collect::<Vec<_>>(),
        [
            ("system", "init"),
            ("system", "status"),
            ("stream_event", "message_start"),
            ("stream_event", "content_block_start"),
            ("stream_event", "content_block_delta"),
            ("assistant", "tool_use"),
            ("stream_event", "content_block_stop"),
            ("stream_event", "message_delta"),
            ("stream_event", "message_stop"),
            ("user", "tool_result"),
            ("system", "status"),
            ("stream_event", "message_start"),
            ("stream_event", "content_block_start"),
            ("stream_event", "content_block_delta"),
            ("assistant", "text"),
            ("stream_event", "content_block_stop"),
            ("stream_event", "message_delta"),
            ("stream_event", "message_stop"),
            ("result", "success"),
            ("system", "init"),
            ("system", "status"),
            ("stream_event", "message_start"),
            ("stream_event", "content_block_start"),
            ("stream_event", "content_block_delta"),
            ("assistant", "text"),
            ("stream_event", "content_block_stop"),
            ("stream_event", "message_delta"),
            ("stream_event", "message_stop"),
            ("result", "success"),
        ]
    );
    for result in messages.iter().filter(|message| message["type"] == "result") {
        assert_eq!(result["terminal_reason"], "completed");
        assert_eq!(result["stop_reason"], "end_turn");
    }
}
