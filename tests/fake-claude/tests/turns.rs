//! Print conversations through the simulated process face, per
//! protocol/programs.md, sections 5 and 8.

#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use skein_io::kernel::{Exit, ServiceSignal};
use smith_fake_claude::{End, Resume, Seen, Step, Story, Turn, seen};
use support::print::{self, MCP, SESSION, messages, normal};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

fn story(steps: Vec<Step>, end: End) -> Story {
    Story { turns: vec![Turn { steps, end }], ..Story::default() }
}

fn results(run: &support::Run) -> Vec<Value> {
    messages(run).into_iter().filter(|value| value["type"] == "result").collect()
}

#[test]
fn a_print_run_starts_with_its_init() {
    let mut story = Story::default();
    story.init.extra_tools.push("ungranted".into());
    story.init.extra_servers.push("other".into());
    story.init.capabilities = vec!["stream".into()];
    let run = normal(story, 0);
    let init = &messages(&run)[0];
    assert_eq!(init["subtype"], "init");
    assert_eq!(init["tools"], json!(["Read", "Bash", "ungranted"]));
    assert_eq!(
        init["mcp_servers"],
        json!([{"name":"smith", "status":"connected"}, {"name":"other", "status":"connected"}])
    );
    assert_eq!(init["capabilities"], json!(["stream"]));
    assert_eq!(init["model"], "test-model");
    assert_eq!(init["permissionMode"], "default");
    assert_eq!(init["apiKeySource"], "none");
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn a_turn_answers_each_user_message_with_its_result() {
    let run = normal(story(vec![Step::Text("one".into()), Step::Text("two".into())], End::default()), 2);
    let result = results(&run);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0]["result"], "two");
    assert_eq!(
        result[0]["usage"],
        json!({"input_tokens":20,"output_tokens":10,"cache_creation_input_tokens":0,"cache_read_input_tokens":0})
    );
    assert_eq!(result[1]["result"], "Done.");
    for message in messages(&run) {
        assert_eq!(message["session_id"], SESSION);
    }
    let assistant = messages(&run).into_iter().find(|value| value["type"] == "assistant").expect("assistant");
    assert_eq!(assistant["message"]["role"], "assistant");
    assert_eq!(assistant["message"]["model"], "test-model");
    assert_eq!(assistant["message"]["usage"]["input_tokens"], 10);
    assert!(assistant["message"]["id"].is_string());
    assert!(assistant["parent_tool_use_id"].is_null());
}

#[test]
fn partial_messages_come_only_when_asked() {
    let run = normal(Story::default(), 1);
    assert!(!messages(&run).iter().any(|message| message["type"] == "stream_event"));
    let mut invocation = print::invocation(1);
    invocation = invocation.arguments(
        &print::arguments().iter().map(String::as_str).chain(["--include-partial-messages"]).collect::<Vec<_>>(),
    );
    let run = print::run(Story::default(), invocation, None, Some(MCP));
    let messages = messages(&run);
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["type"] == "stream_event")
            .map(|message| message["event"]["type"].as_str().expect("event"))
            .collect::<Vec<_>>(),
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop"
        ]
    );
    assert_eq!(messages[5]["type"], "assistant");
}

#[test]
fn a_usage_limit_is_told_with_its_reset() {
    let run = normal(story(vec![Step::RateLimit { resets_at: Some(123) }], End::default()), 1);
    assert_eq!(messages(&run)[2]["rate_limit_info"], json!({"status":"rejected", "resetsAt":123}));
}

#[test]
fn a_usage_limit_is_told_without_a_reset() {
    let run = normal(story(vec![Step::RateLimit { resets_at: None }], End::default()), 1);
    assert_eq!(messages(&run)[2]["rate_limit_info"], json!({"status":"rejected"}));
}

#[test]
fn the_program_s_own_retry_is_told() {
    let run = normal(story(vec![Step::Retry { attempt: 2, status: 503, delay_ms: Some(250) }], End::default()), 1);
    assert_eq!(
        messages(&run)[2],
        json!({"type":"system", "subtype":"api_retry", "attempt":2,"error_status":503,"retry_delay_ms":250,"session_id":SESSION})
    );
}

#[test]
fn a_turn_without_finish_ends_with_its_structured_output() {
    let story = story(Vec::new(), End::Success { structured_output: Some(json!({"done":true})) });
    assert!(results(&normal(story.clone(), 1))[0].get("structured_output").is_none());
    let mut args = print::arguments();
    args.extend(["--json-schema".into(), "{}".into()]);
    let run = print::run(
        story,
        print::invocation(1).arguments(&args.iter().map(String::as_str).collect::<Vec<_>>()),
        None,
        Some(MCP),
    );
    assert_eq!(results(&run)[0]["structured_output"], json!({"done":true}));
}

#[test]
fn an_error_result_carries_its_status() {
    let run = normal(
        story(
            Vec::new(),
            End::Error {
                api_error_status: Some(429),
                resets_at: Some(123),
                terminal_reason: Some("limited".into()),
                errors: vec!["limit".into()],
            },
        ),
        1,
    );
    let result = &results(&run)[0];
    assert_eq!(result["api_error_status"], 429);
    assert_eq!(result["rate_limit_info"]["resetsAt"], 123);
    assert_eq!(result["terminal_reason"], "limited");
    assert_eq!(result["errors"], json!(["limit"]));
    assert_eq!(result["is_error"], true);
    assert_eq!(result["subtype"], "error_during_execution");
}

#[test]
fn a_login_lost_mid_run_fails_the_next_turn_unauthorized() {
    let run = normal(story(vec![Step::LoseLogin], End::default()), 2);
    assert_eq!(results(&run)[0]["is_error"], false);
    assert_eq!(results(&run)[1]["api_error_status"], 401);
    assert!(run.login.is_none());
}

#[test]
fn a_program_that_exits_before_its_result() {
    let run = normal(story(Vec::new(), End::Exit(7)), 1);
    assert!(results(&run).is_empty());
    assert_eq!(run.records[0].exit, Some(Exit::Code(7)));
}

#[test]
fn an_undecodable_line_ends_the_stream() {
    let run = normal(story(Vec::new(), End::Undecodable), 1);
    assert!(run.records[0].stdout.ends_with(b"not json\n"));
    assert_eq!(run.records[0].exit, Some(Exit::Code(1)));
}

#[test]
fn a_sigint_ends_a_hanging_turn_and_exits() {
    let invocation = print::signal(print::invocation(1), ServiceSignal::Interrupt, "assistant");
    let run =
        print::run(story(vec![Step::Text("waiting".into()), Step::Hang], End::default()), invocation, None, Some(MCP));
    assert!(run.signalled);
    assert_eq!(results(&run)[0]["terminal_reason"], "aborted_streaming");
    assert_eq!(results(&run).len(), 1);
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn a_sigint_between_turns_exits() {
    let invocation = print::signal(print::invocation(1), ServiceSignal::Interrupt, "success");
    let run = print::run(Story::default(), invocation, None, Some(MCP));
    assert!(run.signalled);
    assert_eq!(results(&run).len(), 1);
    assert!(results(&run).iter().all(|result| result["is_error"] == false));
}

#[test]
fn a_termination_exits_143() {
    let invocation = print::signal(print::invocation(1), ServiceSignal::Terminate, "assistant");
    let run =
        print::run(story(vec![Step::Text("waiting".into()), Step::Hang], End::default()), invocation, None, Some(MCP));
    assert!(results(&run).is_empty());
    assert_eq!(run.records[0].exit, Some(Exit::Code(143)));
}

#[test]
fn refused_flags_exit_2() {
    let args = print::arguments();
    let mut cases = Vec::new();
    for flag in [
        "--bare",
        "--permission-mode",
        "--dangerously-skip-permissions",
        "--allowedTools",
        "--continue",
        "--max-budget-usd",
        "a prompt",
        "--verbose",
    ] {
        let mut invalid = args.clone();
        invalid.push(flag.into());
        cases.push(invalid);
    }
    for index in 0..args.len() {
        if args[index].starts_with('-') {
            let mut invalid = args.clone();
            invalid.remove(index);
            if !["-p", "--verbose", "--strict-mcp-config"].contains(&args[index].as_str()) {
                invalid.remove(index);
            }
            cases.push(invalid);
        }
    }
    for (flag, value) in [
        ("--session-id", "bad"),
        ("--permission-prompt-tool", "bad"),
        ("--tools", "Read,Web"),
        ("--input-format", "text"),
        ("--max-turns", "0"),
        ("--max-turns", "-1"),
        ("--json-schema", "{"),
    ] {
        let mut invalid = args.clone();
        if let Some(index) = invalid.iter().position(|argument| argument == flag) {
            invalid[index + 1] = value.into();
        } else {
            invalid.extend([flag.into(), value.into()]);
        }
        cases.push(invalid);
    }
    for invalid in cases {
        let run = print::run(
            Story::default(),
            print::invocation(0).arguments(&invalid.iter().map(String::as_str).collect::<Vec<_>>()),
            None,
            Some(MCP),
        );
        assert_eq!(run.records[0].exit, Some(Exit::Code(2)), "{invalid:?}");
        assert_eq!(run.records[0].stderr, b"fake claude: unknown command or flag\n");
    }
}

#[test]
fn the_settings_found_are_observed_by_their_rules() {
    let run = print::run(
        Story::default(),
        print::invocation(0),
        Some(br#"{"permissions":{"allow":["mcp__smith__*"],"ask":["Read"],"deny":["Bash"]},"private":"not recorded"}"#),
        Some(MCP),
    );
    let observations = run.observations.expect("observations");
    assert_eq!(
        seen(&observations),
        [Seen::Settings {
            allow: vec!["mcp__smith__*".into()],
            ask: vec!["Read".into()],
            deny: vec!["Bash".into()],
            keys: vec!["permissions".into(), "private".into()]
        }]
    );
    assert!(!String::from_utf8_lossy(&observations).contains("not recorded"));
    assert_eq!(smith_fake_claude::observations(&observations).len(), 1);
    assert_eq!(seen(&normal(Story::default(), 0).observations.expect("observations")), [Seen::UnreadableSettings]);
}

#[test]
fn a_resume_of_an_unknown_session_fails() {
    let args: Vec<_> = print::arguments()
        .into_iter()
        .map(|argument| if argument == "--session-id" { "--resume".into() } else { argument })
        .collect();
    let invocation = print::invocation(1).arguments(&args.iter().map(String::as_str).collect::<Vec<_>>());
    let run = print::run(Story { resume: Resume::Missing, ..Story::default() }, invocation.clone(), None, Some(MCP));
    assert_eq!(run.records[0].exit, Some(Exit::Code(1)));
    assert_eq!(run.records[0].stderr, format!("No conversation found with session ID: {SESSION}\n").as_bytes());
    assert_eq!(results(&print::run(Story::default(), invocation, None, Some(MCP))).len(), 1);
}

#[test]
fn an_invalid_configuration_exits_one() {
    for configuration in [
        None,
        Some(b"{".as_slice()),
        Some(br#"{"mcpServers":{}}"#),
        Some(br#"{"mcpServers":{"other":{"command":"server"}}}"#),
        Some(br#"{"mcpServers":{"smith":{"type":"http","command":"server"}}}"#),
    ] {
        let run = print::run(Story::default(), print::invocation(0), None, configuration);
        assert_eq!(run.records[0].exit, Some(Exit::Code(1)));
        assert!(!run.records[0].stderr.is_empty());
    }
}

#[test]
fn optional_flags_accept_values_in_any_order() {
    let mut args = print::arguments();
    let tool = args.iter().position(|argument| argument == "--tools").expect("tools");
    args[tool + 1].clear();
    let print = args.remove(0);
    args.extend(["--effort".into(), "opaque-level".into(), "--max-turns".into(), "1".into(), print]);
    let run = print::run(
        Story::default(),
        print::invocation(2).arguments(&args.iter().map(String::as_str).collect::<Vec<_>>()),
        None,
        Some(MCP),
    );
    assert_eq!(messages(&run)[0]["tools"], json!([]));
    assert_eq!(results(&run).len(), 2);
}

#[test]
fn a_non_user_input_line_exits_one() {
    for input in [
        b"not json\n".as_slice(),
        b"{\"type\":\"assistant\"}\n",
        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":1}}\n",
        b"{\"type\":\"user\"}",
    ] {
        let invocation = print::unpaced(input);
        let run = print::run(Story::default(), invocation, None, Some(MCP));
        assert_eq!(run.records[0].exit, Some(Exit::Code(1)));
        assert!(!run.records[0].stderr.is_empty());
    }
}

#[test]
fn input_eof_does_not_end_a_hanging_turn() {
    let input = b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"hello\"}}\n";
    let invocation = print::signal(print::unpaced(input), ServiceSignal::Terminate, "assistant");
    let run =
        print::run(story(vec![Step::Text("waiting".into()), Step::Hang], End::default()), invocation, None, Some(MCP));
    assert!(run.signalled);
    assert_eq!(run.records[0].exit, Some(Exit::Code(143)));
}

#[test]
fn absent_standard_input_is_its_end() {
    let run = print::run(Story::default(), print::without_input(), None, Some(MCP));
    assert_eq!(messages(&run).len(), 2);
    assert_eq!(messages(&run)[0]["subtype"], "init");
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn a_tool_message_is_framed_before_its_mcp_reply() {
    let mut args = print::arguments();
    args.push("--include-partial-messages".into());
    let run = print::with_server(
        story(vec![Step::Call { tool: "finish".into(), input: json!({"answer":"done"}) }], End::default()),
        print::invocation(1).arguments(&args.iter().map(String::as_str).collect::<Vec<_>>()),
        smith_fake_claude::ServerScript {
            tools: vec![smith_fake_claude::Tool {
                name: "finish".into(),
                answer: smith_fake_claude::Answer::Text("accepted".into()),
            }],
            ..Default::default()
        },
    );
    let messages = messages(&run);
    assert_eq!(
        messages
            .iter()
            .filter(|message| message["type"] == "stream_event")
            .map(|message| message["event"]["type"].as_str().expect("event"))
            .collect::<Vec<_>>(),
        [
            "message_start",
            "content_block_start",
            "content_block_delta",
            "content_block_stop",
            "message_delta",
            "message_stop"
        ]
    );
    assert_eq!(messages[2]["event"]["message"]["id"], messages[5]["message"]["id"]);
    assert_eq!(messages[4]["event"]["delta"]["type"], "input_json_delta");
    assert_eq!(messages[9]["message"]["content"][0]["type"], "tool_result");
}

#[test]
fn together_calls_share_one_assistant_message() {
    let run = print::with_server(
        story(
            vec![Step::Together(vec![
                Step::Call { tool: "lookup".into(), input: json!({"q":"one"}) },
                Step::Builtin { tool: "Read".into(), input: json!({"file_path":"a"}), result: "file text".into() },
            ])],
            End::default(),
        ),
        print::invocation(1),
        smith_fake_claude::ServerScript {
            tools: vec![smith_fake_claude::Tool {
                name: "lookup".into(),
                answer: smith_fake_claude::Answer::Text("found".into()),
            }],
            ..Default::default()
        },
    );
    let messages = messages(&run);
    let assistants: Vec<_> = messages.iter().filter(|message| message["type"] == "assistant").collect();
    assert_eq!(assistants.len(), 1);
    assert_eq!(assistants[0]["message"]["content"].as_array().expect("tool blocks").len(), 2);
    let users: Vec<_> = messages.iter().filter(|message| message["type"] == "user").collect();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["message"]["content"][0]["tool_use_id"], assistants[0]["message"]["content"][0]["id"]);
    assert_eq!(users[1]["message"]["content"][0]["tool_use_id"], assistants[0]["message"]["content"][1]["id"]);
}

#[test]
fn a_deaf_program_waits_through_its_interrupt() {
    let invocation = print::signal(print::invocation(1).terminate(), ServiceSignal::Interrupt, "assistant");
    let run =
        print::run(story(vec![Step::Text("waiting".into()), Step::Deaf], End::default()), invocation, None, Some(MCP));
    assert!(results(&run).is_empty());
    assert_eq!(run.records[0].exit, Some(Exit::Code(143)));
    let observations = seen(run.observations.as_ref().expect("observations"));
    assert!(observations.contains(&Seen::Interrupted));
    assert!(observations.contains(&Seen::Terminated));
}

#[test]
fn accepted_user_messages_are_observed() {
    let run = normal(Story::default(), 1);
    let received = smith_fake_claude::received(&run.observations.expect("observations"));
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["type"], "user");
    assert_eq!(received[0]["message"]["role"], "user");
    assert_eq!(received[0]["message"]["content"], "hello");
}

#[test]
fn a_compact_step_reports_the_programs_boundary() {
    let run = normal(story(vec![Step::Compact, Step::Text("after boundary".into())], End::default()), 1);
    let messages = messages(&run);
    assert!(messages.iter().any(|value| value["type"] == "system" && value["subtype"] == "compact_boundary"));
    assert!(messages.iter().any(|value| value["message"]["content"][0]["text"] == "after boundary"));
}

#[test]
fn a_tool_free_invocation_answers_with_the_summary() {
    let mut invocation = print::invocation(1);
    invocation = invocation.arguments(&[
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
        "--tools",
        "",
        "--max-turns",
        "1",
        "--session-id",
        SESSION,
    ]);
    let run = print::run(Story { summary: "the summary".into(), ..Story::default() }, invocation, None, None);
    let messages = messages(&run);
    assert_eq!(messages[0]["tools"], json!([]));
    assert_eq!(messages[0]["mcp_servers"], json!([]));
    assert!(messages.iter().any(|value| value["message"]["content"][0]["text"] == "the summary"));
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn a_recorded_session_id_cannot_be_reused() {
    let mut resume = print::invocation(1);
    let mut arguments = print::arguments();
    *arguments.iter_mut().find(|value| value.as_str() == "--session-id").expect("new flag") = "--resume".into();
    resume = resume.arguments(&arguments.iter().map(String::as_str).collect::<Vec<_>>());
    let run = print::sequence(Story::default(), vec![print::invocation(1), print::invocation(1), resume]);
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
    assert_eq!(run.records[1].exit, Some(Exit::Code(1)));
    assert!(run.records[1].stdout.is_empty(), "refused before init");
    assert_eq!(run.records[1].stderr, format!("Error: Session ID {SESSION} is already in use.\n").as_bytes());
    assert_eq!(run.records[2].exit, Some(Exit::Code(0)));
}

#[test]
fn an_unread_session_id_is_still_new() {
    let run = print::sequence(Story::default(), vec![print::without_input(), print::invocation(1)]);
    assert!(run.records.iter().all(|record| record.exit == Some(Exit::Code(0))));
}

#[test]
fn adjacent_json_lines_run_their_turns_in_order() {
    let first = json!({"type":"user", "message":{"role":"user", "content":"first"}});
    let second = json!({"type":"user", "message":{"role":"user", "content":"second"}});
    let bytes = format!("{first}\n{second}\n");
    let run = print::run(
        story(vec![Step::Text("first answer".into())], End::default()),
        print::unpaced(bytes.as_bytes()),
        None,
        Some(MCP),
    );
    assert_eq!(smith_fake_claude::received(run.observations.as_deref().expect("observations")), [first, second]);
    let result = results(&run);
    assert_eq!(
        result.iter().map(|value| value["result"].as_str().expect("result text")).collect::<Vec<_>>(),
        ["first answer", "Done."]
    );
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

// The fake's existing whole-line bound, including its LF (fake::FILE_BYTES).
const INPUT_LINE_BYTES: usize = 65_536;

fn input_line(length: usize) -> Vec<u8> {
    let mut value = json!({"type":"user", "message":{"role":"user", "content":""}});
    let envelope = serde_json::to_vec(&value).expect("user JSON").len() + 1;
    value["message"]["content"] = json!("x".repeat(length - envelope));
    format!("{value}\n").into_bytes()
}

#[test]
fn an_input_line_at_the_bound_runs_its_turn() {
    let run = print::run(Story::default(), print::unpaced(&input_line(INPUT_LINE_BYTES)), None, Some(MCP));
    assert_eq!(results(&run).len(), 1);
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn an_input_line_past_the_bound_is_refused() {
    let run = print::run(Story::default(), print::unpaced(&input_line(INPUT_LINE_BYTES + 1)), None, Some(MCP));
    assert!(results(&run).is_empty());
    assert_eq!(run.records[0].exit, Some(Exit::Code(1)));
    assert!(String::from_utf8_lossy(&run.records[0].stderr).contains("input line too long"));
}

fn control_input() -> Value {
    json!({"type":"control_request","request_id":"probe-1","request":{"subtype":"interrupt"}})
}

#[test]
fn a_control_interrupt_ends_a_turn_and_keeps_reading() {
    let user = json!({"type":"user","message":{"role":"user","content":"hello"}});
    let input = format!("{user}\n{}\n{user}\n", control_input());
    let run = print::run(story(vec![Step::Hang], End::default()), print::unpaced(input.as_bytes()), None, Some(MCP));
    let output = messages(&run);
    assert_eq!(output[2]["type"], "control_response");
    assert_eq!(output[2]["response"]["request_id"], "probe-1");
    assert_eq!(output[2]["response"]["subtype"], "success");
    assert_eq!(results(&run)[0]["terminal_reason"], "aborted_streaming");
    assert_eq!(results(&run)[1]["subtype"], "success");
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}

#[test]
fn an_idle_control_interrupt_only_acknowledges() {
    let input = format!("{}\n", control_input());
    let run = print::run(Story::default(), print::unpaced(input.as_bytes()), None, Some(MCP));
    let output = messages(&run);
    // Startup init and status precede the control acknowledgement.
    const RECORDS: usize = 3;
    assert_eq!(output.len(), RECORDS);
    assert_eq!(output[2]["type"], "control_response");
    assert_eq!(output[2]["response"]["subtype"], "success");
    assert_eq!(output[2]["response"]["request_id"], "probe-1");
    assert_eq!(run.records[0].exit, Some(Exit::Code(0)));
}
