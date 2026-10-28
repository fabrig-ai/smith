//! Program account checks before runs (domain/host.md, section 8).
use super::{program_check, support};
use serde_json::{Value, json};
use smith_local_process_world::terminal::Action;
use smith_real_world::Scratch;

const ANSWER: &str = "Program answer";

fn setup(status: &Value) -> Scratch {
    let (scratch, _) = program_check::setup(Some("claude.ai"), "Truthful");
    std::fs::write(
        scratch.path().join("state/smith/programs/work/fake-claude.story"),
        serde_json::to_vec(&json!({
            "summary":"earlier answer", "status":status, "login":{"Immediate":"claude.ai"}, "logout":"Succeeds",
            "turns":[{"steps":[{"Outcome":{
                "input":{"outcome":{"form":"report","text":ANSWER,"fields":{}}}
            }}]}]
        }))
        .expect("program story"),
    )
    .expect("scratch story file");
    scratch
}

fn calls(scratch: &Scratch) -> Vec<Value> {
    let bytes =
        std::fs::read(scratch.path().join("state/smith/programs/work/fake-claude.observations")).expect("observations");
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).expect("observation record"))
        .filter_map(|record| record.get("arguments").cloned())
        .collect()
}

fn counts(scratch: &Scratch, statuses: usize, runs: usize) {
    let calls = calls(scratch);
    assert_eq!(calls.iter().filter(|arguments| arguments[0] == "-p").count(), runs, "print-mode runs");
    assert_eq!(
        calls.iter().filter(|arguments| **arguments == json!(["auth", "status"])).count(),
        statuses,
        "host and agent checks"
    );
}

fn two_lines() -> Vec<Action> {
    vec![
        Action::Send(b"Ask first\n".as_slice().into()),
        Action::Wait(ANSWER.as_bytes().into()),
        Action::Send(b"Ask second\n".as_slice().into()),
        Action::Eof,
    ]
}

#[test]
fn program_exec_keeps_the_agent_check() {
    let scratch = setup(&json!("Truthful"));
    let ran = support::run_product(&scratch, &["exec", "Ask first"], b"", None);
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(ran.stdout, ANSWER.as_bytes());
    counts(&scratch, 2, 1);
}

#[test]
fn program_chat_refuses_a_signed_out_second_run() {
    let scratch = setup(&json!({"Sequence":["Truthful", "Truthful", "NotSignedIn"]}));
    let ran = support::run_product_scripted(&scratch, &["--plain"], two_lines());
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(String::from_utf8_lossy(&ran.stdout).contains(ANSWER));
    assert_eq!(
        String::from_utf8_lossy(&ran.stderr).lines().last(),
        Some("smith: account work: Claude Code is not signed in: run `smith login work`")
    );
    counts(&scratch, 3, 1);
}

#[test]
fn program_chat_interrupts_a_silent_recheck() {
    let scratch = setup(&json!({"Sequence":["Truthful", "Truthful", "Silent"]}));
    let ran = support::program_interrupt(&scratch, two_lines());
    assert_eq!(ran.code, 130, "{}", String::from_utf8_lossy(&ran.stderr));
    counts(&scratch, 3, 1);
}

#[test]
fn program_chat_runs_both_lines_after_input_ends() {
    let scratch = setup(&json!("Truthful"));
    let ran = support::run_product_scripted(&scratch, &["--plain"], two_lines());
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(
        String::from_utf8_lossy(&ran.stdout).matches("Program answer").count(),
        2,
        "both answers: {} / {}",
        String::from_utf8_lossy(&ran.stdout),
        String::from_utf8_lossy(&ran.stderr)
    );
    let calls = calls(&scratch);
    let print: Vec<_> = calls.iter().filter(|arguments| arguments[0] == "-p").collect();
    assert_eq!(print.len(), 2, "{calls:?}");
    let first = print[0].as_array().expect("argv");
    let next = print[1].as_array().expect("argv");
    let id = &first.windows(2).find(|pair| pair[0] == json!("--session-id")).expect("new session")[1];
    assert!(next.windows(2).any(|pair| pair[0] == json!("--resume") && &pair[1] == id));
    for ordinary in print {
        assert!(ordinary.as_array().expect("argv").contains(&json!("--mcp-config")));
    }
    counts(&scratch, 4, 2);
}
