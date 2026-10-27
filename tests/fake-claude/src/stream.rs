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

// The owner recorded one assistant record per block before that block's stop.
// Thinking is withheld; its estimated tokens are already in output_tokens.
pub(crate) fn message(mut assistant: Value, thinking: Option<u64>, partial: bool) -> Vec<Value> {
    const SIGNATURE_BYTES: usize = 1000;
    let mut blocks = assistant["message"]["content"].as_array().expect("assistant blocks").clone();
    if thinking.is_some() {
        blocks.insert(0, json!({"type":"thinking","thinking":"","signature":"s".repeat(SIGNATURE_BYTES)}));
    }
    if !partial {
        assistant["message"]["content"] = json!(blocks);
        return vec![assistant];
    }
    let mut tokens = assistant["message"]["usage"].clone();
    if let Some(estimated) = thinking {
        tokens["output_tokens_details"] = json!({"thinking_tokens":estimated});
        // Block records precede the final output-token count in message_delta.
        assistant["message"]["usage"]["output_tokens"] = json!(0);
    }
    let mut values = vec![event(json!({"type":"message_start","message":{
        "id":assistant["message"]["id"],"role":"assistant","model":assistant["message"]["model"],
        "content":[],"usage":assistant["message"]["usage"]}}))];
    let mut calls = false;
    for (index, block) in blocks.into_iter().enumerate() {
        let (start, deltas) = match block["type"].as_str().expect("block type") {
            "thinking" => {
                let estimated = thinking.expect("thinking tokens");
                let first = estimated.checked_div(2).expect("two estimate updates");
                (
                    json!({"type":"thinking","thinking":"","signature":""}),
                    vec![
                        json!({"type":"system","subtype":"thinking_tokens","estimated_tokens":first,
                        "estimated_tokens_delta":first}),
                        event(json!({"type":"content_block_delta","index":index,
                        "delta":{"type":"thinking_delta","thinking":"","estimated_tokens":first}})),
                        json!({"type":"system","subtype":"thinking_tokens","estimated_tokens":estimated,
                        "estimated_tokens_delta":estimated.checked_sub(first).expect("estimate grows")}),
                        event(json!({"type":"content_block_delta","index":index,
                        "delta":{"type":"signature_delta","signature":block["signature"]}})),
                    ],
                )
            }
            "text" => (
                json!({"type":"text","text":""}),
                vec![event(json!({
                "type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":block["text"]}}))],
            ),
            "tool_use" => {
                calls = true;
                (
                    json!({"type":"tool_use","id":block["id"],"name":block["name"],"input":{},"caller":block["caller"]}),
                    vec![event(json!({"type":"content_block_delta","index":index,
                        "delta":{"type":"input_json_delta","partial_json":serde_json::to_string(&block["input"]).expect("tool JSON")}}))],
                )
            }
            _ => unreachable!("scripted block"),
        };
        values.push(event(json!({"type":"content_block_start","index":index,"content_block":start})));
        values.extend(deltas);
        let mut record = assistant.clone();
        record["message"]["content"] = json!([block]);
        values.push(record);
        values.push(event(json!({"type":"content_block_stop","index":index})));
    }
    values.push(event(
        json!({"type":"message_delta","delta":{"stop_reason":if calls {"tool_use"} else {"end_turn"}},"usage":tokens}),
    ));
    values.push(event(json!({"type":"message_stop"})));
    values
}

fn event(value: Value) -> Value {
    json!({"type":"stream_event","event":value,"parent_tool_use_id":null})
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
