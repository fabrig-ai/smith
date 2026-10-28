#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

use serde_json::json;
use skein_lib::{List, Reader, Writer};
use smith_agent_process_world::{
    charter, program,
    program_world::{self, Case, Run},
};
use smith_channel::{ModelFault, RunFailure, RunResult};
use smith_fake_claude::{Decision, End, Seen, Step, Story, Turn};

fn charter_tools(shell: bool) -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&charter()))
        .expect("charter")
        .into_parts();
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 20;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("budget");
    let mut tools = parts.tools.into_parts();
    tools.families = smith_charter::Families::new(
        &smith_charter::CEILINGS,
        smith_charter::FamiliesParts { skein_bools: [true, true, shell, false] },
    )
    .expect("workspace families");
    tools.host = List::with_capacity(1);
    tools
        .host
        .push(
            smith_charter::HostTool::new(
                &smith_charter::CEILINGS,
                smith_charter::HostToolParts {
                    name: b"lookup".as_slice().into(),
                    description: b"Look up".as_slice().into(),
                    input: br#"{"type":"object","properties":{}}"#.as_slice().into(),
                    effect: smith_charter::Effect::Read,
                    deadline: skein_lib::Duration::from_secs(1),
                },
            )
            .expect("host descriptor"),
        )
        .expect("host tool");
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("tools");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("charter with tools");
    let mut writer = Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("charter bytes");
    writer.finish()
}

fn finish() -> Step {
    Step::Call { tool: "finish".into(), input: json!({"form":"report","text":"done","fields":{}}) }
}

fn read(path: &str) -> Step {
    Step::Builtin { tool: "Read".into(), input: json!({"file_path":path}), result: "file text".into() }
}

fn run(story: &Story) -> Run {
    program_world::run(Case::Tools, story, Some(b"claude.ai"), &charter_tools(true), &program(b"source"), None)
}

fn steps(steps: Vec<Step>) -> Run {
    run(&Story { turns: vec![Turn { steps, end: End::default() }], ..Default::default() })
}

fn accepted(run: &Run) {
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)),
        "answer={:?} stderr={}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
}

fn turns(run: &Run) -> Vec<smith_transcript::Turn> {
    run.frames
        .iter()
        .filter(|frame| frame.kind == 0x0109)
        .map(|frame| {
            let turn = smith_channel::Turn::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body))
                .expect("channel turn");
            smith_transcript::Turn::decode(&smith_transcript::CEILINGS, &mut Reader::new(turn.body()))
                .expect("turn body")
        })
        .collect()
}

fn told_result(run: &Run, tool: &[u8], input: &serde_json::Value) -> smith_transcript::ToolResult {
    let mut call_id = None;
    let mut results = Vec::new();
    for turn in turns(run) {
        for message in turn.messages().as_slice() {
            for block in message.blocks().as_slice() {
                match block {
                    smith_transcript::Block::Call(call) if call.name() == tool => {
                        call_id = Some(call.id().to_vec());
                        assert_eq!(&serde_json::from_slice::<serde_json::Value>(call.input()).expect("input"), input);
                    }
                    smith_transcript::Block::Result(result) => results.push(result.clone()),
                    smith_transcript::Block::Text(_)
                    | smith_transcript::Block::Refusal(_)
                    | smith_transcript::Block::Call(_)
                    | smith_transcript::Block::Opaque(_) => {}
                }
            }
        }
    }
    let id = call_id.expect("tool call is told");
    results.into_iter().find(|result| result.id() == id).expect("tool result is told")
}

fn result(run: &Run, path: &str, expected: &[u8], error: bool) {
    let result = told_result(run, b"Read", &json!({"file_path":path}));
    let smith_transcript::Returned::Said(said) = result.returned() else { panic!("built-in text") };
    assert_eq!(said.text(), expected);
    assert_eq!(said.error(), error);
}

#[test]
fn a_program_grant_starts_claude_with_its_tools() {
    let run = steps(vec![finish()]);
    accepted(&run);
    let arguments = &run.invocations[1].arguments;
    let index = arguments.iter().position(|argument| argument == "--tools").expect("tools flag");
    assert_eq!(arguments[index + 1], "Read,Glob,Grep,Write,Edit,Bash");
    assert_eq!(
        run.seen[0],
        Seen::Settings {
            allow: vec!["mcp__smith".into()],
            ask: ["Read", "Glob", "Grep", "Write", "Edit", "Bash"].map(str::to_owned).into(),
            deny: vec![],
            keys: vec!["permissions".into()]
        }
    );
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "this observation selects only the Read lifecycle records")]
fn a_built_in_call_is_told_with_its_text() {
    let run = steps(vec![read("a"), finish()]);
    accepted(&run);
    result(&run, "a", b"file text", false);
    let start = run
        .events
        .iter()
        .find_map(|record| match &record.event {
            smith_events::Event::ToolStarted(start) if start.tool.as_ref() == b"Read" => Some(start),
            _ => None,
        })
        .expect("tool.started");
    assert_eq!(start.source, smith_events::Source::Workspace);
    let completed = run
        .events
        .iter()
        .find_map(|record| match &record.event {
            smith_events::Event::ToolCompleted(done) if done.call == start.call => Some(done),
            _ => None,
        })
        .expect("tool.completed");
    assert_eq!(completed.bytes, 9);
    assert_eq!(completed.verdict, smith_events::Verdict::Result);
    assert!(run.seen.contains(&Seen::Prompted { tool: "Read".into(), decision: Decision::Allow { unchanged: true } }));
}

#[test]
fn a_read_outside_the_workspace_is_denied() {
    let run = steps(vec![read("/etc/passwd"), finish()]);
    accepted(&run);
    result(&run, "/etc/passwd", b"path is outside the workspace: /etc/passwd", true);
    assert!(run.seen.contains(&Seen::Prompted {
        tool: "Read".into(),
        decision: Decision::Deny("path is outside the workspace: /etc/passwd".into())
    }));
}

#[test]
fn a_built_in_after_a_host_call_runs() {
    let run = steps(vec![
        Step::Together(vec![Step::Call { tool: "lookup".into(), input: json!({}) }, read("/library/a")]),
        finish(),
    ]);
    accepted(&run);
    result(&run, "/library/a", b"file text", false);
    assert!(run.seen.contains(&Seen::Called { tool: "lookup".into(), error: false }));
    assert!(run.seen.contains(&Seen::Prompted { tool: "Read".into(), decision: Decision::Allow { unchanged: true } }));
}

#[test]
fn an_ungranted_built_in_in_init_fails_the_start() {
    let mut story = Story { turns: vec![Turn { steps: vec![finish()], end: End::default() }], ..Default::default() };
    story.init.extra_tools.push("WebFetch".into());
    let run = run(&story);
    let answer = run.answer().expect("failure");
    let RunResult::Failed(failed) = answer.result() else { panic!("program failure: {answer:?}") };
    let RunFailure::Model(model) = failed.reason() else { panic!("model failure") };
    assert!(
        matches!(model.value(), ModelFault::Program(fault) if fault.exit().is_none() && fault.detail() == b"smith: the program offers a built-in tool the run did not grant: WebFetch")
    );
}

fn edit(path: &str, old: &str, new: &str) -> Step {
    Step::Act { tool: "Edit".into(), input: json!({"file_path":path,"old_string":old,"new_string":new}) }
}

#[test]
fn a_program_edits_its_workspace_through_the_prompt() {
    for path in ["a", "/work/repo/a"] {
        let run = steps(vec![edit(path, "before", "after"), finish()]);
        accepted(&run);
        assert_eq!(run.workspace_file.as_deref(), Some(b"after\n".as_slice()));
        assert!(
            run.seen.contains(&Seen::Prompted { tool: "Edit".into(), decision: Decision::Allow { unchanged: true } })
        );
        assert!(run.seen.contains(&Seen::Acted { tool: "Edit".into(), path: path.into(), error: false }));
    }
}

#[test]
fn an_edit_outside_the_workspace_is_denied_and_nothing_changes() {
    let run = steps(vec![edit("/outside/a", "outside", "changed"), finish()]);
    accepted(&run);
    assert_eq!(run.outside_file.as_deref(), Some(b"outside text".as_slice()));
    assert_eq!(run.workspace_file.as_deref(), Some(b"before\n".as_slice()));
    assert!(run.seen.contains(&Seen::Prompted {
        tool: "Edit".into(),
        decision: Decision::Deny("path is outside the workspace: /outside/a".into())
    }));
    assert!(!run.seen.iter().any(|seen| matches!(seen, Seen::Acted { .. })));
}

#[test]
fn a_program_reads_its_absolute_library_path() {
    let run = steps(vec![Step::Act { tool: "Read".into(), input: json!({"file_path":"/library/a"}) }, finish()]);
    accepted(&run);
    result(&run, "/library/a", b"library text", false);
    assert_eq!(run.library_file.as_deref(), Some(b"library text".as_slice()));
    assert!(run.seen.contains(&Seen::Acted { tool: "Read".into(), path: "/library/a".into(), error: false }));
}

#[test]
fn a_nested_host_input_is_told_and_answered() {
    // Object/key/array tokens: 5 + 27 * 6 + one extra scalar = 168.
    // Each array/object boundary is a token, as in skein's JSON document.
    const ENTRIES: usize = 27;
    let mut queries = vec![json!([{}, {}]); ENTRIES];
    queries[0] = json!([{}, {}, 0]);
    let input = json!({"queries":queries});
    let run = steps(vec![Step::Call { tool: "lookup".into(), input: input.clone() }, finish()]);
    accepted(&run);
    let result = told_result(&run, b"lookup", &input);
    let smith_transcript::Returned::Said(said) = result.returned() else { panic!("host answer") };
    assert_eq!(said.text(), b"found");
    assert!(!said.error());
    assert!(run.seen.contains(&Seen::Called { tool: "lookup".into(), error: false }));
}

fn removal_steps(steps: Vec<Step>) -> Run {
    let story = Story { turns: vec![Turn { steps, end: End::default() }], ..Default::default() };
    program_world::run(Case::Tools, &story, Some(b"claude.ai"), &charter_tools(false), &program(b"source"), None)
}

fn remove_file() -> Step {
    Step::CallChecked { tool: "remove".into(), input: json!({"path":"a"}), contains: "removed".into() }
}

#[test]
fn a_whole_native_read_then_mcp_remove_deletes_the_read_file() {
    let run = removal_steps(vec![
        Step::Act { tool: "Read".into(), input: json!({"file_path":"a"}) },
        remove_file(),
        finish(),
    ]);
    accepted(&run);
    assert_eq!(run.workspace_file, None);
    assert!(run.seen.contains(&Seen::Called { tool: "remove".into(), error: false }));
}

#[test]
fn a_partial_native_read_does_not_allow_mcp_remove_and_says_to_read_whole() {
    let run = removal_steps(vec![
        Step::Builtin {
            tool: "Read".into(),
            input: json!({"file_path":"a", "offset":1,"limit":1}),
            result: "before".into(),
        },
        Step::CallChecked { tool: "remove".into(), input: json!({"path":"a"}), contains: "read the whole file".into() },
        finish(),
    ]);
    accepted(&run);
    assert_eq!(run.workspace_file.as_deref(), Some(b"before\n".as_slice()));
    assert!(run.seen.contains(&Seen::Called { tool: "remove".into(), error: true }));
}

#[test]
fn a_native_write_then_mcp_remove_deletes_the_written_version() {
    let run = removal_steps(vec![
        Step::Act { tool: "Write".into(), input: json!({"file_path":"a","content":"new bytes"}) },
        remove_file(),
        finish(),
    ]);
    accepted(&run);
    assert_eq!(run.workspace_file, None);
}

#[test]
fn a_native_edit_then_mcp_remove_deletes_the_edited_version() {
    let run = removal_steps(vec![edit("a", "before", "after"), remove_file(), finish()]);
    accepted(&run);
    assert_eq!(run.workspace_file, None);
}

#[test]
fn mcp_remove_deletes_an_empty_directory_without_a_native_read() {
    let run = removal_steps(vec![
        Step::CallChecked { tool: "remove".into(), input: json!({"path":"empty"}), contains: "removed".into() },
        finish(),
    ]);
    accepted(&run);
    assert!(run.seen.contains(&Seen::Called { tool: "remove".into(), error: false }));
}
