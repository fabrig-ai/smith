//! Init fixtures follow the owner's 2.1.311 probe, reports/live-claude-shapes.md.
#![expect(
    clippy::disallowed_macros,
    clippy::disallowed_types,
    reason = "ordinary Rust fixtures, programming-model.md 10.2"
)]

use super::*;
use serde_json::json;

// Full init with two declared server tools, the recorded keys and 13 capabilities.
pub(in crate::backend) fn init_line(tools: &[&str]) -> Box<[u8]> {
    serde_json::to_vec(&json!({
        "type":"system", "subtype":"init", "tools":tools,
        "mcp_servers":[{"name":"smith", "status":"connected"}],
        "capabilities":["interrupt_receipt_v1", "interrupt_cancel_queued_v1", "interrupt_send_now_v1",
            "interrupt_requeue_taken_v1", "msg_lifecycle_v1", "request_marker_lists_v1",
            "lifecycle_wakeup_source_v1", "sdk_mcp_tools_list_changed", "sdk_mcp_manifests",
            "mcp_read_resource_v1", "mcp_tool_ui_meta_v1", "ui_surface_v1", "mcp_call_result_not_for_model_v1"],
        "additional_directories":[], "agents":[], "analytics_disabled":false, "apiKeySource":"none",
        "claude_code_version":"2.1.311", "cwd":"", "original_cwd":"",
        "fast_mode_disabled_reason":null, "fast_mode_state":"off", "memory_paths":[],
        "messaging_socket_path":null, "model":"fake", "output_style":"default", "per_turn_effort_active":false,
        "permissionMode":"default", "plugins":[], "product_feedback_disabled":false,
        "session_id":"fake", "skills":[], "slash_commands":[], "terminal_slash_commands":[],
        "uuid":"fake", "view_mode":"default"
    }))
    .expect("recorded init JSON")
    .into_boxed_slice()
}

fn tools() -> [mcp::Tool; 2] {
    [b"finish".as_slice(), b"echo"].map(|name| mcp::Tool {
        name: name.into(),
        description: b"tool".as_slice().into(),
        input_schema: b"{}".as_slice().into(),
    })
}

// The whole recorded fixture fits the normal step harness's line cap.
const LINE_BYTES: u32 = 4096;
const DEPTH: u32 = 32;

#[test]
fn the_real_init_hides_the_callable_prompt_tool() {
    let line = init_line(&["mcp__smith__finish", "mcp__smith__echo"]);
    assert_eq!(checked_init(&line, &tools(), DEPTH, super::super::Settings::stream_tokens(LINE_BYTES)), Some(()));
}

#[test]
fn an_init_may_list_the_prompt_tool_once() {
    let line = init_line(&["mcp__smith__finish", "mcp__smith__echo", "mcp__smith__permission_prompt"]);
    assert_eq!(checked_init(&line, &tools(), DEPTH, super::super::Settings::stream_tokens(LINE_BYTES)), Some(()));
}

#[test]
fn an_init_refuses_missing_repeated_and_foreign_tools() {
    let cases: &[&[&str]] = &[
        &["mcp__smith__finish"],
        &["mcp__smith__finish", "mcp__smith__permission_prompt"],
        &["mcp__smith__finish", "mcp__smith__echo", "mcp__smith__echo"],
        &["mcp__smith__finish", "mcp__smith__echo", "mcp__smith__unknown"],
        &["mcp__smith__finish", "mcp__smith__echo", "mcp__other__x"],
        &["mcp__smith__finish", "mcp__smith__echo", "mcp__smith__permission_prompt", "mcp__smith__permission_prompt"],
    ];
    for listed in cases {
        let line = init_line(listed);
        assert_eq!(
            checked_init(&line, &tools(), DEPTH, super::super::Settings::stream_tokens(LINE_BYTES)),
            None,
            "{listed:?}"
        );
    }
}
