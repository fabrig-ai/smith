//! Conversation cues and resumed cursors through the simulated process face
//! (protocol/programs.md, sections 5.8 and 8; skein fake-llm.md, section 2.4).

#[allow(dead_code)]
mod support;

use serde_json::{Value, json};
use skein_fake_machine::Item;
use smith_fake_claude::{Script, Seen, Selection, Step, Story, Turn, selections};
use support::{Invocation, Run, print};

#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

const OTHER_SESSION: &str = "12345678-1234-1234-1234-123456789abd";
const PROMPT: &str = "run/instructions.md";

fn turns(texts: &[&str]) -> Vec<Turn> {
    texts.iter().map(|text| Turn { steps: vec![Step::Text((*text).into())], ..Turn::default() }).collect()
}

fn script(cue: &str, texts: &[&str]) -> Script {
    Script { cue: cue.into(), turns: turns(texts) }
}

fn invocation(contents: &[Value], session: &str, resume: bool, prompt: &str) -> Invocation {
    let invocation = print::invocation(0);
    let mut arguments = print::arguments();
    let prompt_flag = arguments.iter().position(|value| value == "--append-system-prompt-file").expect("prompt flag");
    arguments[prompt_flag + 1] = prompt.into();
    let session_flag = arguments.iter().position(|value| value == "--session-id").expect("session flag");
    arguments[session_flag] = if resume { "--resume" } else { "--session-id" }.into();
    arguments[session_flag + 1] = session.into();
    let input: Vec<u8> = contents
        .iter()
        .flat_map(|content| {
            format!("{}\n", json!({"type":"user","message":{"role":"user","content":content}})).into_bytes()
        })
        .collect();
    invocation.arguments(&arguments.iter().map(String::as_str).collect::<Vec<_>>()).input(&input)
}

fn run(story: Story, contents: &[Value], prompt: &str) -> Run {
    print::sequence_files(
        story,
        vec![invocation(contents, print::SESSION, false, PROMPT)],
        vec![Item::file(PROMPT.as_bytes(), prompt.as_bytes())],
    )
}

fn answers(run: &Run) -> Vec<Value> {
    run.records
        .iter()
        .flat_map(|record| {
            record
                .stdout
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice::<Value>(line).expect("stream JSON"))
                .filter(|message| message["type"] == "result")
                .map(|message| message["result"].clone())
        })
        .collect()
}

fn selected(run: &Run) -> Vec<Seen> {
    selections(run.observations.as_ref().expect("observations"))
}

#[test]
fn a_system_cue_selects_its_script() {
    let story =
        Story { cued: vec![script("writer", &["written"]), script("reviewer", &["reviewed"])], ..Story::default() };
    let run = print::sequence_files(
        story,
        vec![
            invocation(&[json!("hello")], print::SESSION, false, PROMPT),
            invocation(&[json!("hello")], OTHER_SESSION, false, "run/review.md"),
        ],
        vec![
            Item::file(PROMPT.as_bytes(), b"you are the writer"),
            Item::file(b"run/review.md", b"you are the reviewer"),
        ],
    );
    assert_eq!(answers(&run), [json!("written"), json!("reviewed")]);
    assert_eq!(
        selected(&run),
        [
            Seen::Selected { script: Selection::Cued(0), turn: 0 },
            Seen::Selected { script: Selection::Cued(1), turn: 0 },
        ]
    );
}

#[test]
fn a_request_cue_selects_its_script_and_restarts_at_each_request() {
    let story = Story { asked: vec![script("write", &["first", "second"])], ..Story::default() };
    let run = run(story, &[json!("please write"), json!("continue"), json!("write again")], "");
    assert_eq!(answers(&run), [json!("first"), json!("second"), json!("first")]);
    assert_eq!(
        selected(&run),
        [
            Seen::Selected { script: Selection::Asked(0), turn: 0 },
            Seen::Selected { script: Selection::Asked(0), turn: 1 },
            Seen::Selected { script: Selection::Asked(0), turn: 0 },
        ]
    );
}

#[test]
fn neither_cue_falls_back_to_turns() {
    let story = Story {
        cued: vec![script("system cue", &["system"])],
        asked: vec![script("request cue", &["request"])],
        turns: turns(&["fallback zero", "fallback one"]),
        ..Story::default()
    };
    let run = run(story, &[json!("hello"), json!("continue"), json!("again")], "no match");
    assert_eq!(answers(&run), [json!("fallback zero"), json!("fallback one"), json!("Done.")]);
    assert_eq!(
        selected(&run),
        [
            Seen::Selected { script: Selection::Otherwise, turn: 0 },
            Seen::Selected { script: Selection::Otherwise, turn: 1 },
            Seen::Selected { script: Selection::Otherwise, turn: 2 },
        ]
    );
}

#[test]
fn a_resumed_session_goes_on_with_its_next_turn() {
    let story = Story { cued: vec![script("chat", &["zero", "one", "two"])], ..Story::default() };
    let run = print::sequence_files(
        story,
        vec![
            invocation(&[json!("hello")], print::SESSION, false, PROMPT),
            invocation(&[json!("continue")], print::SESSION, true, PROMPT),
            invocation(&[json!("continue"), json!("finished?")], print::SESSION, true, PROMPT),
        ],
        vec![Item::file(PROMPT.as_bytes(), b"chat")],
    );
    assert_eq!(answers(&run), [json!("zero"), json!("one"), json!("two"), json!("Done.")]);
    assert_eq!(
        selected(&run),
        [
            Seen::Selected { script: Selection::Cued(0), turn: 0 },
            Seen::Selected { script: Selection::Cued(0), turn: 1 },
            Seen::Selected { script: Selection::Cued(0), turn: 2 },
            Seen::Selected { script: Selection::Cued(0), turn: 3 },
        ]
    );
}

#[test]
fn the_earliest_cue_wins_then_the_first_listed() {
    let scripts = vec![script("late", &["late"]), script("early", &["early first"]), script("ear", &["early second"])];
    for story in
        [Story { cued: scripts.clone(), ..Story::default() }, Story { asked: scripts.clone(), ..Story::default() }]
    {
        let run = run(story, &[json!("early then late")], "early then late");
        assert_eq!(answers(&run), [json!("early first")]);
    }
}

#[test]
fn a_system_cue_takes_precedence_over_a_request() {
    let story = Story {
        cued: vec![script("system", &["system zero", "system one"])],
        asked: vec![script("request", &["request"])],
        ..Story::default()
    };
    let run = run(story, &[json!("request"), json!("request")], "system");
    assert_eq!(answers(&run), [json!("system zero"), json!("system one")]);
}

#[test]
fn a_cue_cannot_span_two_request_text_parts() {
    let story = Story { asked: vec![script("cue", &["matched"])], turns: turns(&["fallback"]), ..Story::default() };
    let run = run(story, &[json!([{"type":"text","text":"c"},{"type":"text","text":"ue"}])], "");
    assert_eq!(answers(&run), [json!("fallback")]);
}

#[test]
fn a_request_cue_in_a_later_part_is_found() {
    let story = Story { asked: vec![script("cue", &["matched"])], ..Story::default() };
    let run = run(story, &[json!([{"type":"text","text":"no match"},{"type":"text","text":"cue"}])], "");
    assert_eq!(answers(&run), [json!("matched")]);
}

#[test]
fn the_latest_matching_request_changes_the_script() {
    let story =
        Story { asked: vec![script("write", &["writer"]), script("review", &["reviewer"])], ..Story::default() };
    let run = run(story, &[json!("write"), json!("review"), json!("continue")], "");
    assert_eq!(answers(&run), [json!("writer"), json!("reviewer"), json!("Done.")]);
}

#[test]
fn a_resumed_request_keeps_its_script_and_place() {
    let story = Story { asked: vec![script("write", &["zero", "one", "two"])], ..Story::default() };
    let run = print::sequence(
        story,
        vec![
            invocation(&[json!("before"), json!("write"), json!("continue")], print::SESSION, false, PROMPT),
            invocation(&[json!("continue"), json!("write")], print::SESSION, true, PROMPT),
        ],
    );
    assert_eq!(answers(&run), [json!("Done."), json!("zero"), json!("one"), json!("two"), json!("zero")]);
}

#[test]
fn a_legacy_session_marker_starts_at_zero() {
    let story = Story { turns: turns(&["zero", "one"]), ..Story::default() };
    let marker = format!("config/fake-claude.session.{}", print::SESSION);
    let run = print::sequence_files(
        story,
        vec![invocation(&[json!("continue")], print::SESSION, true, PROMPT)],
        vec![Item::file(marker.as_bytes(), &[1])],
    );
    assert_eq!(answers(&run), [json!("zero")]);
}

#[test]
fn an_uncued_resume_preserves_legacy_turn_restarts() {
    let story = Story { turns: turns(&["zero", "one"]), ..Story::default() };
    let run = print::sequence(
        story,
        vec![
            invocation(&[json!("hello")], print::SESSION, false, PROMPT),
            invocation(&[json!("continue")], print::SESSION, true, PROMPT),
        ],
    );
    assert_eq!(answers(&run), [json!("zero"), json!("zero")]);
}

// The fake's file driver admits at most 64 KiB per whole-file read.
const PROMPT_BYTES: usize = 65_536;

#[test]
fn a_system_prompt_at_the_file_bound_is_read() {
    let story = Story { cued: vec![script("cue", &["matched"])], ..Story::default() };
    let prompt = format!("{}cue", "x".repeat(PROMPT_BYTES - "cue".len()));
    let run = run(story, &[json!("hello")], &prompt);
    assert_eq!(answers(&run), [json!("matched")]);
}

#[test]
fn a_system_prompt_past_the_file_bound_is_refused() {
    let story = Story { cued: vec![script("cue", &["matched"])], ..Story::default() };
    let prompt = format!("{}cue", "x".repeat(PROMPT_BYTES - "cue".len() + 1));
    let run = run(story, &[json!("hello")], &prompt);
    assert_eq!(run.records[0].exit, Some(skein_io::kernel::Exit::Code(1)));
    assert_eq!(run.records[0].stderr, b"fake claude: cannot read system prompt\n");
    assert!(run.records[0].stdout.is_empty());
}

#[test]
fn a_prompt_outside_the_mcp_directory_is_read() {
    let story = Story { cued: vec![script("chat", &["matched"])], ..Story::default() };
    let run = print::sequence_files(
        story,
        vec![invocation(&[json!("hello")], print::SESSION, false, "instructions.md")],
        vec![Item::file(b"instructions.md", b"chat")],
    );
    assert_eq!(answers(&run), [json!("matched")]);
}
