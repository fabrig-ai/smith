//! Result-paced print parent and private configuration fixtures, per
//! protocol/programs.md, sections 5 and 8.

#![allow(dead_code)]

use super::{Invocation, Run};
use skein_fake_machine::Item;
use skein_io::kernel::ServiceSignal;
use smith_fake_claude::Story;

pub const SESSION: &str = "12345678-1234-1234-1234-123456789abc";
pub const MCP: &[u8] = br#"{"mcpServers":{"smith":{"type":"stdio","command":"server","args":["{}"],"env":{}}}}"#;

pub fn arguments() -> Vec<String> {
    [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--setting-sources",
        "user",
        "--model",
        "test-model",
        "--append-system-prompt-file",
        "unread",
        "--mcp-config",
        "run/mcp.json",
        "--strict-mcp-config",
        "--permission-prompt-tool",
        "mcp__smith__prompt",
        "--tools",
        "Read,Bash",
        "--session-id",
        SESSION,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub fn invocation(count: usize) -> Invocation {
    let mut invocation = Invocation::auth("unused");
    invocation.args = arguments();
    invocation.stream = true;
    invocation.input = (0..count)
        .flat_map(|index| {
            let content = if index % 2 == 0 {
                serde_json::json!("hello")
            } else {
                serde_json::json!([{"type":"text", "text":"next"}])
            };
            format!("{}\n", serde_json::json!({"type":"user", "message":{"role":"user", "content":content}}))
                .into_bytes()
        })
        .collect();
    invocation
}

pub fn signal(mut invocation: Invocation, signal: ServiceSignal, after: &str) -> Invocation {
    invocation.signal = Some((signal, after.into()));
    invocation
}

pub fn run(story: Story, invocation: Invocation, settings: Option<&[u8]>, mcp: Option<&[u8]>) -> Run {
    let mut files = vec![Item::directory(b"run")];
    if let Some(mcp) = mcp {
        files.push(Item::file(b"run/mcp.json", mcp));
    }
    if let Some(settings) = settings {
        files.push(Item::file(b"config/settings.json", settings));
    }
    super::run_files(Some(story), Some(b"claude.ai"), vec![invocation], files)
}

pub fn messages(run: &Run) -> Vec<serde_json::Value> {
    run.records[0]
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("stream message"))
        .collect()
}

pub fn normal(story: Story, count: usize) -> Run {
    run(story, invocation(count), None, Some(MCP))
}

pub fn unpaced(bytes: &[u8]) -> Invocation {
    let mut invocation = invocation(0).input(bytes);
    invocation.stream = false;
    invocation
}

pub fn without_input() -> Invocation {
    let mut invocation = invocation(0);
    invocation.no_input = true;
    invocation
}

pub fn with_server(story: Story, invocation: Invocation, script: smith_fake_claude::ServerScript) -> Run {
    let config = serde_json::json!({"mcpServers":{"smith":{"command":"server","args":[script.encode()],"env":{"SERVER_TEST":"overridden"}}}});
    run(story, invocation, None, Some(&serde_json::to_vec(&config).expect("configuration")))
}

pub fn sequence(story: Story, invocations: Vec<Invocation>) -> Run {
    sequence_files(story, invocations, Vec::new())
}

pub fn sequence_files(story: Story, invocations: Vec<Invocation>, extra: Vec<Item>) -> Run {
    let mut files = vec![Item::directory(b"run"), Item::file(b"run/mcp.json", MCP)];
    files.extend(extra);
    super::run_files(Some(story), Some(b"claude.ai"), invocations, files)
}
