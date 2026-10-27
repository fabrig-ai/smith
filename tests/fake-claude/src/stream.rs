//! JSON stream messages for protocol/programs.md, sections 5 and 8. These
//! builders retain no state and know no input or configuration values.

use serde_json::{Value, json};

use crate::{Init, Usage};

pub(crate) fn line(mut value: Value, session: &str) -> Vec<u8> {
    value["session_id"] = json!(session);
    let mut bytes = serde_json::to_vec(&value).expect("stream JSON");
    bytes.push(b'\n');
    bytes
}

pub(crate) fn init(init: &Init, tools: &[String], server: &str, model: &str) -> Value {
    let mut tools = tools.to_vec();
    tools.extend(init.extra_tools.clone());
    let mut servers = vec![json!({"name": server, "status": "pending"})];
    servers.extend(init.extra_servers.iter().map(|name| json!({"name": name, "status": "connected"})));
    json!({"type":"system", "subtype":"init", "tools":tools, "mcp_servers":servers,
        "model":model, "permissionMode":"default", "apiKeySource":"none",
        "capabilities":init.capabilities,
        "additional_directories":[], "agents":[], "analytics_disabled":false,
        "claude_code_version":"2.1.311", "cwd":"", "original_cwd":"",
        "fast_mode_disabled_reason":null, "fast_mode_state":"off", "memory_paths":[],
        "messaging_socket_path":null, "output_style":"default", "per_turn_effort_active":false,
        "plugins":[], "product_feedback_disabled":false, "skills":[], "slash_commands":[],
        "terminal_slash_commands":[], "uuid":"fake", "view_mode":"default"})
}

pub(crate) fn status() -> Value {
    json!({"type":"system", "subtype":"status", "status":null})
}

pub(crate) fn usage(usage: &Usage, count: u64) -> Value {
    json!({"input_tokens": usage.input_tokens.checked_mul(count).expect("input tokens fit"),
        "output_tokens":usage.output_tokens.checked_mul(count).expect("output tokens fit"),
        "cache_creation_input_tokens":0, "cache_read_input_tokens":0})
}

pub(crate) fn assistant(text: &str, model: &str, id: usize, tokens: &Usage) -> Value {
    json!({"type":"assistant", "message":{"id":format!("msg_{id}"), "role":"assistant",
        "model":model, "content":[{"type":"text", "text":text}], "usage":usage(tokens, 1)},
        "parent_tool_use_id":null})
}

pub(crate) fn partials(text: &str, model: &str, id: usize) -> Vec<Value> {
    vec![
        json!({"type":"message_start", "message":{"id":format!("msg_{id}"), "role":"assistant", "model":model,
            "content":[], "usage":usage(&Usage {input_tokens:0, output_tokens:0}, 0)}}),
        json!({"type":"content_block_start", "index":0, "content_block":{"type":"text", "text":""}}),
        json!({"type":"content_block_delta", "index":0, "delta":{"type":"text_delta", "text":text}}),
        json!({"type":"content_block_stop", "index":0}),
        json!({"type":"message_delta", "delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ]
    .into_iter()
    .map(|event| json!({"type":"stream_event", "event":event, "parent_tool_use_id":null}))
    .collect()
}

pub(crate) fn failure(status: Option<u16>, reason: Option<String>, errors: Vec<String>) -> Value {
    json!({"type":"result", "subtype":"error_during_execution", "is_error":true,
        "api_error_status":status, "terminal_reason":reason, "errors":errors})
}

pub(crate) fn user(bytes: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(bytes) else { return false };
    if value["type"] != "user" || value["message"]["role"] != "user" {
        return false;
    }
    match &value["message"]["content"] {
        Value::String(_) => true,
        Value::Array(blocks) => blocks.iter().all(|block| block["type"] == "text" && block["text"].is_string()),
        _ => false,
    }
}
