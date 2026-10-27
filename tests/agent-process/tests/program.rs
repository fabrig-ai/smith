#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

use smith_agent_process_world::{
    charter, program,
    program_world::{self, Case, Run},
};
use smith_channel::{ModelFault, RunFailure, RunResult};
use smith_fake_claude::Story;

fn scenario() -> Run {
    program_world::run(Case::default(), &Story::default(), Some(b"claude.ai"), &charter(), &program(b"source"), None)
}

#[test]
fn no_credential_variable_reaches_the_program() {
    let story = Story {
        turns: vec![smith_fake_claude::Turn {
            steps: vec![smith_fake_claude::Step::Outcome {
                input: serde_json::json!({"outcome":{"form":"report","text":"done","fields":{}}}),
            }],
            end: smith_fake_claude::End::default(),
        }],
        ..Story::default()
    };
    let run = program_world::run(Case::Credentials, &story, Some(b"claude.ai"), &charter(), &program(b"source"), None);
    assert!(matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)));
    assert_eq!(run.invocations.len(), 2, "status and print both ran");
    assert_eq!(run.invocations[0].arguments, ["auth", "status"]);
    assert_eq!(run.invocations[0].environment, ["WAIT_PROOF", "HOME", "EARLY_EXIT", "TERM", "CLAUDE_CONFIG_DIR"]);
    assert_eq!(run.invocations[1].arguments[0], "-p");
    assert_eq!(
        run.invocations[1].environment,
        ["WAIT_PROOF", "HOME", "EARLY_EXIT", "TERM", "MCP_TOOL_TIMEOUT", "CLAUDE_CONFIG_DIR"]
    );
    assert_eq!(run.relay_environments.len(), 1, "the print run reached its relay");
    for name in [
        "ANTHROPIC_API_KEY",
        "ANTHROPIC_AUTH_TOKEN",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "CLAUDE_CODE_USE_BEDROCK",
        "AWS_BEARER_TOKEN_BEDROCK",
    ] {
        assert!(!run.relay_environments[0].iter().any(|kept| kept == name), "relay kept {name}");
    }
    assert_eq!(run.exits, vec![skein_io::kernel::Exit::Code(0); 5]);
    assert!(run.run_directories.is_empty());
}

fn fault(run: &Run) -> Option<ModelFault> {
    match run.answer()?.result() {
        RunResult::Failed(failed) => match failed.reason() {
            RunFailure::Model(model) => Some(model.value().clone()),
            RunFailure::Transcript(_)
            | RunFailure::Budget(_)
            | RunFailure::Policy(_)
            | RunFailure::Cancelled
            | RunFailure::Stale => None,
        },
        RunResult::Accepted(_) | RunResult::Parked | RunResult::Refused(_) => None,
    }
}

#[test]
fn a_cancelled_program_run_ends_claude() {
    let run = scenario();
    assert_eq!(
        run.invocations.len(),
        2,
        "status precedes the program: answer={:?} errors={:?} invocations={:?}",
        run.answer(),
        String::from_utf8_lossy(&run.errors),
        run.invocations
    );
    assert_eq!(run.exits, vec![skein_io::kernel::Exit::Code(0); 5], "parent, agent, status, program, relay settle");
    assert!(run.run_directories.is_empty(), "the private run directory is removed");
    assert!(fault(&run).is_none(), "cancel is not a program fault");
}

#[test]
fn a_program_grant_starts_claude_with_its_command() {
    let run = scenario();
    let arguments = &run.invocations[1].arguments;
    assert_eq!(
        &arguments[..10],
        [
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--include-partial-messages",
            "--setting-sources",
            "user",
            "--model"
        ]
    );
    let value = |flag| {
        arguments.iter().position(|argument| argument == flag).map(|at| arguments[at + 1].as_str()).expect("flag value")
    };
    assert_eq!(value("--model"), "fake");
    assert_eq!(value("--tools"), "");
    assert_eq!(value("--max-turns"), "1");
    assert_eq!(value("--permission-prompt-tool"), "mcp__smith__permission_prompt");
    assert!(value("--append-system-prompt-file").starts_with("/programs/.runs/"));
    assert!(value("--append-system-prompt-file").ends_with("/instructions.md"));
    assert!(value("--mcp-config").ends_with("/mcp.json"));
    assert!(arguments.iter().any(|argument| argument == "--strict-mcp-config"));
    assert_eq!(value("--session-id").len(), 36);
    let schema: serde_json::Value = serde_json::from_str(value("--json-schema")).expect("inline outcome schema");
    assert!(schema["properties"]["outcome"].get("oneOf").is_some());
    assert_eq!(
        run.seen,
        vec![
            smith_fake_claude::Seen::Settings {
                allow: vec!["mcp__smith".into()],
                ask: ["Read", "Glob", "Grep", "Write", "Edit", "Bash"].map(str::to_owned).into(),
                deny: Vec::new(),
                keys: vec!["permissions".into()],
            },
            smith_fake_claude::Seen::Interrupted
        ]
    );
}

#[test]
fn a_program_start_reaches_its_init() {
    let run = scenario();
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Failed(failed) if matches!(failed.reason(), RunFailure::Cancelled))
    );
    assert!(
        matches!(run.seen.first(), Some(smith_fake_claude::Seen::Settings { .. })),
        "the fake read the settings before its init"
    );
    assert_eq!(run.exits.len(), 5, "the program reached its relay");
}

fn unauthorized(login: Option<&[u8]>) {
    let run = program_world::run(Case::default(), &Story::default(), login, &charter(), &program(b"source"), None);
    assert!(matches!(fault(&run), Some(ModelFault::Unauthorized(fault)) if fault.account() == 0));
    assert!(run.frames.iter().any(|frame| frame.kind == 0x010d), "the unauthorized notice reaches the host");
    assert_eq!(run.invocations.len(), 1, "only the source status was run");
}

#[test]
fn a_source_not_signed_in_fails_unauthorized() {
    unauthorized(None);
}

#[test]
fn a_source_signed_in_otherwise_fails_unauthorized() {
    unauthorized(Some(b"api_key"));
}

#[test]
fn a_failed_status_check_fails_with_its_exit() {
    let run = program_world::run(
        Case::default(),
        &Story { status: smith_fake_claude::Status::Unreadable, ..Story::default() },
        Some(b"claude.ai"),
        &charter(),
        &program(b"source"),
        None,
    );
    assert!(
        matches!(fault(&run), Some(ModelFault::Program(fault)) if *fault.exit() == Some(0) && fault.name() == b"claude_code")
    );
    assert!(!run.frames.iter().any(|frame| frame.kind == 0x010d));
}

fn refused(case: Case, charter: &[u8], value: &smith_channel::GrantValue) {
    let run = program_world::run(case, &Story::default(), Some(b"claude.ai"), charter, value, None);
    assert!(matches!(run.answer().expect("refusal").result(), RunResult::Refused(refused)
        if matches!(refused.reason(), smith_channel::StartRefusal::Invalid(invalid) if invalid.value() == &smith_channel::InvalidStart::Grants)));
    assert!(run.invocations.is_empty(), "the start is refused before any program starts");
}

#[test]
fn an_endpoint_without_a_program_refuses_a_program_grant() {
    refused(Case::Unbound, &charter(), &program(b"source"));
}

#[test]
fn a_credential_grant_for_a_program_endpoint_is_refused() {
    refused(Case::default(), &charter(), &smith_agent_process_world::withdrawn(b"account", b"value"));
}

fn changed_charter(change: impl FnOnce(&mut smith_charter::CharterParts)) -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&charter()))
        .expect("charter")
        .into_parts();
    change(&mut parts);
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("modified charter");
    let mut writer = skein_lib::Writer::new(usize::try_from(charter.measure()).expect("charter bytes"));
    charter.encode(&mut writer).expect("charter body");
    writer.finish()
}

#[test]
fn a_program_grant_for_a_sub_agent_is_refused() {
    let charter = changed_charter(|parts| {
        parts.models = skein_lib::List::with_capacity(1);
        parts.models.push(parts.main.clone()).expect("sub-agent model");
        let mut main = parts.main.clone().into_parts();
        main.account = None;
        parts.main = smith_charter::Llm::new(&smith_charter::CEILINGS, main).expect("main without account");
    });
    refused(Case::default(), &charter, &program(b"source"));
}

#[test]
fn a_later_grant_of_the_same_source_renews_it() {
    let run = program_world::run(
        Case::default(),
        &Story::default(),
        Some(b"claude.ai"),
        &charter(),
        &program(b"source"),
        Some(&program(b"source")),
    );
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Failed(failed) if matches!(failed.reason(), RunFailure::Cancelled))
    );
    assert_eq!(run.invocations.len(), 2);
}

#[test]
fn a_later_grant_of_another_source_fails_the_run() {
    let run = program_world::run(
        Case::default(),
        &Story::default(),
        Some(b"claude.ai"),
        &charter(),
        &program(b"source"),
        Some(&program(b"another")),
    );
    assert!(run.answer().is_none());
    assert!(String::from_utf8_lossy(&run.errors).contains("a host credential could not be installed"));
}

#[test]
fn an_init_without_the_server_fails_the_run() {
    let run = program_world::run(
        Case::MissingServer,
        &Story::default(),
        Some(b"claude.ai"),
        &charter(),
        &program(b"source"),
        None,
    );
    assert!(matches!(fault(&run), Some(ModelFault::Program(fault)) if fault.exit().is_none()
        && fault.detail() == b"smith: the program started without smith's tools"));
}

#[test]
fn a_program_that_exits_before_its_init_fails_the_run() {
    let run = program_world::run(
        Case::EarlyExit,
        &Story::default(),
        Some(b"claude.ai"),
        &charter(),
        &program(b"source"),
        None,
    );
    assert!(
        matches!(fault(&run), Some(ModelFault::Program(fault)) if *fault.exit() == Some(2) && !fault.detail().is_empty())
    );
}

#[test]
fn a_program_main_may_share_its_account_with_a_declared_child() {
    let charter = changed_charter(|parts| {
        parts.models = skein_lib::List::with_capacity(1);
        parts.models.push(parts.main.clone()).expect("declared child model");
    });
    let run =
        program_world::run(Case::Ordinary, &Story::default(), Some(b"claude.ai"), &charter, &program(b"source"), None);
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Failed(failed) if matches!(failed.reason(), RunFailure::Cancelled))
    );
    assert_eq!(run.invocations.len(), 2, "the account shared with main admits the program start");
}
