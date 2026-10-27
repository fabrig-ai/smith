//! Allowed workspace operations through the shared simulated process face
//! (protocol/programs.md, sections 5.3, 5.4 and 8).

#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use skein_fake_machine::Item;
use smith_fake_claude::{Decision, Seen, ServerScript, Step, Story, Turn, Verdict, seen};
use support::{Run, print};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

const FILE: &[u8] = b"file.txt";
const ORIGINAL: &[u8] = b"before\n";

fn act(tool: &str, input: Value) -> Step {
    Step::Act { tool: tool.into(), input }
}

fn run(steps: Vec<Step>, original: Option<&[u8]>, verdicts: Vec<Verdict>) -> Run {
    let files = original.into_iter().map(|bytes| Item::file(FILE, bytes)).collect();
    print::with_server_files(
        Story { turns: vec![Turn { steps, ..Turn::default() }], ..Story::default() },
        print::invocation(1),
        ServerScript { verdicts, ..ServerScript::default() },
        files,
        &[FILE],
    )
}

fn result(run: &Run) -> Value {
    print::messages(run).into_iter().find(|message| message["type"] == "user").expect("tool result")["message"]["content"][0].clone()
}

fn edit(old: &str, new: &str, replace_all: bool) -> Step {
    act("Edit", json!({"file_path":"file.txt", "old_string":old, "new_string":new,"replace_all":replace_all}))
}

fn observations(run: &Run) -> Vec<Seen> {
    seen(run.observations.as_ref().expect("observations"))
}

#[test]
fn an_allowed_edit_changes_the_file() {
    let run = run(vec![edit("before", "after", false)], Some(ORIGINAL), Vec::new());
    assert_eq!(run.file(FILE), Some(b"after\n".as_slice()));
    assert_eq!(result(&run)["is_error"], false);
    assert!(
        observations(&run)
            .contains(&Seen::Prompted { tool: "Edit".into(), decision: Decision::Allow { unchanged: true } })
    );
    assert!(observations(&run).contains(&Seen::Acted { tool: "Edit".into(), path: "file.txt".into(), error: false }));
}

#[test]
fn an_edit_whose_text_is_absent_fails_and_changes_nothing() {
    let run = run(vec![edit("absent", "after", false)], Some(ORIGINAL), Vec::new());
    assert_eq!(run.file(FILE), Some(ORIGINAL));
    assert_eq!(result(&run)["is_error"], true);
    assert_eq!(result(&run)["content"], "old_string was not found");
    assert!(observations(&run).contains(&Seen::Acted { tool: "Edit".into(), path: "file.txt".into(), error: true }));
}

#[test]
fn an_allowed_write_creates_the_file() {
    let run = run(vec![act("Write", json!({"file_path":"file.txt","content":"created\n"}))], None, Vec::new());
    assert_eq!(run.file(FILE), Some(b"created\n".as_slice()));
    assert_eq!(result(&run)["is_error"], false);
}

#[test]
fn a_read_returns_the_files_text() {
    let run = run(vec![act("Read", json!({"file_path":"file.txt"}))], Some(ORIGINAL), Vec::new());
    assert_eq!(result(&run)["content"], "before\n");
    assert_eq!(result(&run)["is_error"], false);
    assert_eq!(run.file(FILE), Some(ORIGINAL));
}

#[test]
fn a_denied_act_touches_nothing() {
    let run =
        run(vec![edit("before", "after", false)], Some(ORIGINAL), vec![Verdict::Deny("outside workspace".into())]);
    assert_eq!(run.file(FILE), Some(ORIGINAL));
    assert_eq!(result(&run)["is_error"], true);
    assert_eq!(result(&run)["content"], "outside workspace");
    assert!(!observations(&run).iter().any(|seen| matches!(seen, Seen::Acted { .. })));
}

#[test]
fn an_ambiguous_edit_changes_nothing() {
    let original = b"before before\n";
    let run = run(vec![edit("before", "after", false)], Some(original), Vec::new());
    assert_eq!(run.file(FILE), Some(original.as_slice()));
    assert_eq!(result(&run)["is_error"], true);
    assert_eq!(result(&run)["content"], "old_string occurs more than once");
}

#[test]
fn replace_all_edits_every_occurrence() {
    let run = run(vec![edit("before", "after", true)], Some(b"before before\n"), Vec::new());
    assert_eq!(run.file(FILE), Some(b"after after\n".as_slice()));
    assert_eq!(result(&run)["is_error"], false);
}

#[test]
fn an_allowed_write_replaces_the_file() {
    let run =
        run(vec![act("Write", json!({"file_path":"file.txt","content":"replaced\n"}))], Some(ORIGINAL), Vec::new());
    assert_eq!(run.file(FILE), Some(b"replaced\n".as_slice()));
}

#[test]
fn consecutive_acts_observe_the_latest_file() {
    let run = run(
        vec![
            act("Write", json!({"file_path":"file.txt","content":"before\n"})),
            edit("before", "after", false),
            act("Read", json!({"file_path":"file.txt"})),
        ],
        None,
        Vec::new(),
    );
    let messages: Vec<_> = print::messages(&run).into_iter().filter(|message| message["type"] == "user").collect();
    assert_eq!(messages.last().expect("read result")["message"]["content"][0]["content"], "after\n");
    assert_eq!(run.file(FILE), Some(b"after\n".as_slice()));
}

// The fake's existing whole-file limit, fake::FILE_BYTES.
const FILE_BYTES: usize = 65_536;

#[test]
fn an_edit_at_the_file_bound_succeeds() {
    // Doubling each byte of a half-limit file reaches FILE_BYTES exactly.
    let original = "x".repeat(FILE_BYTES / 2);
    let run = run(vec![edit("x", "xx", true)], Some(original.as_bytes()), Vec::new());
    assert_eq!(run.file(FILE).expect("edited file").len(), FILE_BYTES);
    assert_eq!(result(&run)["is_error"], false);
}

#[test]
fn an_edit_past_the_file_bound_changes_nothing() {
    let original = format!("{}y", "x".repeat(FILE_BYTES / 2));
    let run = run(vec![edit("x", "xx", true)], Some(original.as_bytes()), Vec::new());
    assert_eq!(run.file(FILE), Some(original.as_bytes()));
    assert_eq!(result(&run)["is_error"], true);
    assert_eq!(result(&run)["content"], "edit exceeds the file byte limit");
}

#[test]
fn overlapping_matches_make_an_edit_ambiguous() {
    let run = run(vec![edit("aa", "b", false)], Some(b"aaa"), Vec::new());
    assert_eq!(run.file(FILE), Some(b"aaa".as_slice()));
    assert_eq!(result(&run)["content"], "old_string occurs more than once");
    assert_eq!(result(&run)["is_error"], true);
}

#[test]
fn an_unsupported_act_is_a_story_error() {
    let run = run(vec![act("Bash", json!({"command":"not executed"}))], Some(ORIGINAL), Vec::new());
    assert_eq!(run.file(FILE), Some(ORIGINAL));
    assert_eq!(run.records[0].exit, Some(skein_io::kernel::Exit::Code(1)));
    assert_eq!(run.records[0].stderr, b"fake claude: Act requires Read, Write or Edit\n");
}

#[test]
fn together_acts_complete_in_their_scripted_order() {
    let run = run(
        vec![Step::Together(vec![edit("before", "after", false), act("Read", json!({"file_path":"file.txt"}))])],
        Some(ORIGINAL),
        Vec::new(),
    );
    let messages: Vec<_> = print::messages(&run).into_iter().filter(|message| message["type"] == "user").collect();
    assert_eq!(messages.last().expect("read result")["message"]["content"][0]["content"], "after\n");
    assert_eq!(run.file(FILE), Some(b"after\n".as_slice()));
}
