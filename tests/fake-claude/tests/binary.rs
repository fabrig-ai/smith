//! The same fake as an executable (protocol/programs.md, section 8;
//! testing.md, sections 2.2, 2.3 and 4). All process state stays in scratch.

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Command, Output},
};

use skein_scratch::Scratch;
use smith_fake_claude::{CONFIG_DIR, LOGIN_FILE, OBSERVATIONS_FILE, observations};

fn invoke(home: &Path, config: Option<&Path>, args: &[&str]) -> Output {
    let mut command = Command::new(skein_scratch::paths::binary("fake-claude"));
    command.env_clear().env("HOME", home).current_dir(home).args(args);
    if let Some(config) = config {
        command.env(CONFIG_DIR, config);
    }
    command.output().expect("fake binary runs")
}

#[test]
fn the_binary_signs_in_and_reports_its_status() {
    let scratch = Scratch::new("fake-claude-login");
    let config = scratch.path().join("config");
    fs::create_dir(&config).expect("configuration directory");
    fs::set_permissions(&config, fs::Permissions::from_mode(0o700)).expect("private configuration");
    let login = invoke(scratch.path(), Some(&config), &["auth", "login"]);
    assert_eq!(login.status.code(), Some(0));
    assert_eq!(login.stdout, b"Signed in.\n");
    assert!(login.stderr.is_empty());
    assert_eq!(fs::read(config.join(LOGIN_FILE)).expect("login"), b"claude.ai");
    let status = invoke(scratch.path(), Some(&config), &["auth", "status"]);
    assert_eq!(status.status.code(), Some(0));
    assert!(status.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&status.stdout).expect("status JSON"),
        serde_json::json!({"loggedIn": true, "authMethod": "claude.ai", "apiProvider": "firstParty"})
    );
    let records = observations(&fs::read(config.join(OBSERVATIONS_FILE)).expect("observations"));
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].arguments, ["auth", "login"]);
    assert_eq!(records[1].arguments, ["auth", "status"]);
    for record in records {
        assert_eq!(record.environment, ["CLAUDE_CONFIG_DIR", "HOME"]);
    }
    assert!(!scratch.path().join(".claude").exists());
}

#[test]
fn the_binary_refuses_without_a_configuration_directory() {
    let scratch = Scratch::new("fake-claude-no-config");
    let output = invoke(scratch.path(), None, &["auth", "login"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(output.stderr, b"fake claude: CLAUDE_CONFIG_DIR is not set\n");
    assert_eq!(fs::read_dir(scratch.path()).expect("scratch").count(), 0);
}

#[test]
fn the_binary_without_its_directory_is_not_signed_in() {
    let scratch = Scratch::new("fake-claude-absent-status");
    let config = scratch.path().join("absent");
    let output = invoke(scratch.path(), Some(&config), &["auth", "status"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).expect("status JSON"),
        serde_json::json!({"loggedIn": false, "authMethod": "none", "apiProvider": "firstParty"})
    );
    assert!(!config.exists());
}

#[test]
fn the_binary_login_refuses_without_its_directory() {
    let scratch = Scratch::new("fake-claude-absent-login");
    let config = scratch.path().join("absent");
    let output = invoke(scratch.path(), Some(&config), &["auth", "login"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(!output.stderr.is_empty());
    assert!(output.stdout.is_empty());
    assert!(!config.exists());
}

#[test]
fn the_binary_logout_accepts_an_absent_directory() {
    let scratch = Scratch::new("fake-claude-absent-logout");
    let config = scratch.path().join("absent");
    let output = invoke(scratch.path(), Some(&config), &["auth", "logout"]);
    assert_eq!(output.status.code(), Some(0));
    assert!(output.stderr.is_empty());
    assert_eq!(output.stdout, b"Signed out.\n");
    assert!(!config.exists());
}

#[allow(dead_code)]
mod support;

use smith_fake_claude::{Answer, End, STORY_FILE, ServerScript, Step, Story, Tool, Turn};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Stdio};

fn print_child(story: Story) -> (Scratch, Child) {
    print_scripted_child(story, ServerScript::default())
}

fn print_scripted_child(story: Story, script: ServerScript) -> (Scratch, Child) {
    let scratch = Scratch::new("fake-claude-print");
    let config = scratch.path().join("config");
    let run = scratch.path().join("run");
    fs::create_dir(&config).expect("configuration");
    fs::create_dir(&run).expect("run directory");
    fs::write(config.join(LOGIN_FILE), b"claude.ai").expect("login");
    fs::write(config.join(STORY_FILE), story.encode()).expect("story");
    fs::write(scratch.path().join("unread"), b"test system prompt").expect("system prompt");
    let configuration = serde_json::json!({"mcpServers":{"smith":{"type":"stdio", "command":skein_scratch::paths::binary("fake-claude-server"),"args":[script.encode()],"env":{}}}});
    fs::write(run.join("mcp.json"), serde_json::to_vec(&configuration).expect("configuration JSON"))
        .expect("MCP configuration");
    let child = Command::new(skein_scratch::paths::binary("fake-claude"))
        .env_clear()
        .env("HOME", scratch.path())
        .env(CONFIG_DIR, &config)
        .current_dir(scratch.path())
        .args(support::print::arguments())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("print child");
    (scratch, child)
}

fn read_message(reader: &mut BufReader<std::process::ChildStdout>) -> serde_json::Value {
    let mut line = String::new();
    assert!(reader.read_line(&mut line).expect("read message") > 0, "message before EOF");
    serde_json::from_str(&line).expect("JSON message")
}

fn send_user(child: &mut Child) {
    child
        .stdin
        .as_mut()
        .expect("input")
        .write_all(b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hello\"}}\n")
        .expect("user message");
}

fn signal(child: &Child, name: &str, scratch: &Scratch) {
    assert!(
        Command::new("/bin/kill")
            .env_clear()
            .env("HOME", scratch.path())
            .env(CONFIG_DIR, scratch.path().join("config"))
            .args(["-s", name, &child.id().to_string()])
            .status()
            .expect("signal command")
            .success()
    );
}

#[test]
fn the_binary_runs_a_turn() {
    let (_scratch, mut child) = print_child(Story::default());
    let mut reader = BufReader::new(child.stdout.take().expect("output"));
    assert_eq!(read_message(&mut reader)["subtype"], "init");
    send_user(&mut child);
    assert_eq!(read_message(&mut reader)["type"], "assistant");
    assert_eq!(read_message(&mut reader)["result"], "Done.");
    child.stdin.take();
    assert_eq!(child.wait().expect("exit").code(), Some(0));
}

#[test]
fn the_binary_ends_a_turn_on_an_interrupt() {
    let story = Story {
        turns: vec![Turn { steps: vec![Step::Text("waiting".into()), Step::Hang], end: End::default() }],
        ..Story::default()
    };
    let (scratch, mut child) = print_child(story);
    let mut reader = BufReader::new(child.stdout.take().expect("output"));
    assert_eq!(read_message(&mut reader)["subtype"], "init");
    send_user(&mut child);
    assert_eq!(read_message(&mut reader)["type"], "assistant");
    signal(&child, "INT", &scratch);
    assert_eq!(read_message(&mut reader)["terminal_reason"], "interrupted");
    send_user(&mut child);
    assert_eq!(read_message(&mut reader)["type"], "assistant");
    assert_eq!(read_message(&mut reader)["subtype"], "success");
    child.stdin.take();
    assert_eq!(child.wait().expect("exit").code(), Some(0));
}

#[test]
fn the_binary_exits_143_on_a_termination() {
    let story = Story {
        turns: vec![Turn { steps: vec![Step::Text("waiting".into()), Step::Hang], end: End::default() }],
        ..Story::default()
    };
    let (scratch, mut child) = print_child(story);
    let mut reader = BufReader::new(child.stdout.take().expect("output"));
    assert_eq!(read_message(&mut reader)["subtype"], "init");
    send_user(&mut child);
    assert_eq!(read_message(&mut reader)["type"], "assistant");
    child.stdin.take();
    signal(&child, "TERM", &scratch);
    assert_eq!(child.wait().expect("exit").code(), Some(143));
}

#[test]
fn the_binary_connects_to_its_server() {
    let story = Story {
        turns: vec![Turn {
            steps: vec![Step::Call { tool: "finish".into(), input: serde_json::json!({"answer":"done"}) }],
            end: End::default(),
        }],
        ..Story::default()
    };
    let script = ServerScript {
        tools: vec![Tool { name: "finish".into(), answer: Answer::Text("accepted".into()) }],
        ..ServerScript::default()
    };
    let (_scratch, mut child) = print_scripted_child(story, script);
    let mut reader = BufReader::new(child.stdout.take().expect("output"));
    let init = read_message(&mut reader);
    assert_eq!(init["mcp_servers"][0]["status"], "connected");
    assert!(init["tools"].as_array().expect("tools").contains(&serde_json::json!("mcp__smith__finish")));
    send_user(&mut child);
    assert_eq!(read_message(&mut reader)["message"]["content"][0]["name"], "mcp__smith__finish");
    assert_eq!(read_message(&mut reader)["message"]["content"][0]["content"], "accepted");
    assert_eq!(read_message(&mut reader)["subtype"], "success");
    child.stdin.take();
    assert_eq!(child.wait().expect("exit").code(), Some(0));
}

#[test]
fn the_binary_uses_its_system_prompt_to_select() {
    let story = Story {
        cued: vec![smith_fake_claude::Script {
            cue: "test system prompt".into(),
            turns: vec![Turn { steps: vec![Step::Text("matched".into())], ..Turn::default() }],
        }],
        ..Story::default()
    };
    let (_scratch, mut child) = print_child(story);
    let mut reader = BufReader::new(child.stdout.take().expect("output"));
    read_message(&mut reader);
    send_user(&mut child);
    read_message(&mut reader);
    assert_eq!(read_message(&mut reader)["result"], "matched");
    child.stdin.take();
    assert_eq!(child.wait().expect("exit").code(), Some(0));
}
