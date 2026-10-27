//! Carried windows cross the shipped channel, service and program boundary (programs.md 5.8).
#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;
use skein_fake_llm_domain::api;
use skein_lib::{Reader, Writer};
use smith_agent_process_world::{
    World, program,
    program_world::{self, Case},
};
use smith_channel::RunResult;
use smith_fake_claude::{End, Resume, Seen, Step, Story, Turn};

fn charter() -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(
        &smith_charter::CEILINGS,
        &mut skein_lib::Reader::new(&smith_agent_process_world::charter()),
    )
    .expect("charter")
    .into_parts();
    parts.resume = true;
    let mut budget = parts.budget.into_parts();
    budget.turns = 4;
    budget.spend = 20;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("budget");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("charter");
    let mut writer = skein_lib::Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("charter");
    writer.finish()
}

fn finish() -> api::Turn {
    api::Turn {
        lines: Box::new([api::Line::Call {
            name: b"finish".as_slice().into(),
            arguments: br#"{"form":"report","text":"done","fields":{}}"#.as_slice().into(),
        }]),
        finish: api::Finish::ToolCalls,
        tokens: 1,
    }
}

fn story() -> Story {
    Story {
        summary: "portable summary".into(),
        turns: vec![Turn {
            steps: vec![Step::Outcome {
                input: serde_json::json!({"outcome":{"form":"report","text":"done","fields":{}}}),
            }],
            ..Turn::default()
        }],
        ..Story::default()
    }
}

fn direct() -> World {
    let mut world = World::new(7, &charter());
    world.script(Box::new([finish()]));
    assert!(world.settle());
    assert!(matches!(world.answer().expect("answer").result(), RunResult::Accepted(_)));
    world
}

#[test]
fn a_conversation_is_carried_over_from_the_direct_backend() {
    let old = direct();
    let turns = program_world::from_frames(old.observed());
    assert!(!turns.is_empty());
    let run = program_world::run_with_window(&story(), &charter(), &turns);
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)),
        "{:?} {}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    let print: Vec<_> = run.invocations.iter().filter(|invocation| invocation.arguments[0] == "-p").collect();
    assert_eq!(print.len(), 2, "summary then ordinary window");
    assert!(!print[0].arguments.contains(&"--mcp-config".into()));
    assert!(print[1].arguments.contains(&"--mcp-config".into()));
    let text = run.received[0]["message"]["content"].as_str().expect("summary input");
    assert!(text.contains("assistant:\nfinish:"));
    assert!(
        run.received[1]["message"]["content"]
            .as_array()
            .expect("window blocks")
            .iter()
            .any(|block| block["text"] == "portable summary")
    );
}

#[test]
fn a_conversation_is_carried_over_to_the_direct_backend() {
    let old = program_world::run(Case::Turns, &story(), Some(b"claude.ai"), &charter(), &program(b"source"), None);
    assert!(matches!(old.answer().expect("answer").result(), RunResult::Accepted(_)));
    let turns = program_world::from_frames(&old.frames);
    assert!(!turns.is_empty());
    let mut world = World::new(7, &charter());
    world.restore(turns);
    world.script(Box::new([
        finish(),
        api::Turn {
            lines: Box::new([api::Line::Text { text: b"direct summary".as_slice().into() }]),
            finish: api::Finish::Stop,
            tokens: 1,
        },
    ]));
    assert!(world.settle());
    assert!(matches!(world.answer().expect("answer").result(), RunResult::Accepted(_)), "{:?}", world.answer());
    assert_eq!(world.peer_queries().len(), 2, "summary precedes ordinary completion");
}

#[test]
fn a_compaction_runs_its_own_program() {
    let old = direct();
    let turns = program_world::from_frames(old.observed());
    let run = program_world::run_with_window(&story(), &charter(), &turns);
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)),
        "{:?} {}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    let print: Vec<_> = run.invocations.iter().filter(|invocation| invocation.arguments[0] == "-p").collect();
    assert_eq!(print.len(), 2);
    let ids: Vec<_> = print
        .iter()
        .map(|invocation| {
            invocation.arguments.windows(2).find(|pair| pair[0] == "--session-id").expect("new session")[1].clone()
        })
        .collect();
    assert_ne!(ids[0], ids[1]);
    assert!(run.run_directories.is_empty());
    assert!(run.exits.iter().all(|exit| *exit == skein_io::kernel::Exit::Code(0)), "{:?}", run.exits);
}

#[test]
fn a_waiting_program_run_reads_its_message() {
    let mut parts = smith_charter::Charter::decode(
        &smith_charter::CEILINGS,
        &mut Reader::new(&smith_agent_process_world::charter()),
    )
    .expect("charter")
    .into_parts();
    parts.waiting = skein_lib::Duration::from_secs(1);
    let mut tools = parts.tools.into_parts();
    tools.wait = true;
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("wait granted");
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 20;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("budget");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("wait charter");
    let mut body = Writer::new(charter.measure() as usize);
    charter.encode(&mut body).expect("charter bytes");
    let story = Story {
        turns: vec![Turn {
            steps: vec![
                Step::CallChecked {
                    tool: "wait".into(),
                    input: serde_json::json!({}),
                    contains: "The next instruction".into(),
                },
                Step::Outcome { input: serde_json::json!({"outcome":{"form":"report", "text":"done", "fields":{}}}) },
            ],
            end: End::default(),
        }],
        ..Story::default()
    };
    let run = program_world::run(Case::Waiting, &story, Some(b"claude.ai"), &body.finish(), &program(b"source"), None);
    assert!(
        matches!(run.answer().expect("answer").result(), smith_channel::RunResult::Accepted(_)),
        "answer={:?}; frames={:?}; errors={}",
        run.answer(),
        run.frames.iter().map(|frame| frame.kind).collect::<Vec<_>>(),
        String::from_utf8_lossy(&run.errors)
    );
    assert!(run.seen.iter().any(|seen| matches!(seen, Seen::Called { tool, error: false } if tool == "wait")));
    let invocation = run
        .invocations
        .iter()
        .find(|invocation| invocation.arguments.first().is_some_and(|argument| argument == "-p"))
        .expect("program invocation");
    assert!(invocation.environment.iter().any(|name| name == "MCP_TOOL_TIMEOUT"));
}

fn waiting_charter() -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&charter()))
        .expect("charter")
        .into_parts();
    parts.waiting = skein_lib::Duration::from_millis(10);
    let mut tools = parts.tools.into_parts();
    tools.wait = true;
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("wait");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("charter");
    let mut writer = Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("charter");
    writer.finish()
}

fn session_argument<'a>(invocation: &'a smith_fake_claude::Observation, flag: &str) -> &'a str {
    &invocation.arguments.windows(2).find(|pair| pair[0] == flag).expect("session argument")[1]
}

#[test]
fn a_parked_run_resumes_its_session() {
    let charter = waiting_charter();
    let waiting = Story {
        turns: vec![Turn {
            steps: vec![Step::Call { tool: "wait".into(), input: serde_json::json!({}) }],
            ..Turn::default()
        }],
        ..Story::default()
    };
    let old = program_world::run(Case::ParkWaiting, &waiting, Some(b"claude.ai"), &charter, &program(b"source"), None);
    assert!(
        matches!(old.answer().expect("answer").result(), RunResult::Parked),
        "{:?} {}",
        old.answer(),
        String::from_utf8_lossy(&old.errors)
    );
    assert!(old.seen.contains(&Seen::Interrupted));
    assert!(!old.seen.contains(&Seen::Terminated));
    let first = old.invocations.iter().find(|call| call.arguments[0] == "-p").expect("first print");
    let run = program_world::run_with_window(&story(), &charter, &program_world::from_frames(&old.frames));
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)),
        "{:?} {}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    let print: Vec<_> = run.invocations.iter().filter(|call| call.arguments[0] == "-p").collect();
    assert_eq!(print.len(), 1);
    assert_eq!(session_argument(print[0], "--resume"), session_argument(first, "--session-id"));
    let text = run.received[0]["message"]["content"].as_str().expect("one tail message");
    assert!(text.contains("Begin"));
    assert!(text.contains("result of"));
    assert!(run.run_directories.is_empty());
}

#[test]
fn a_gone_session_is_carried_over() {
    let old = program_world::run(Case::Turns, &story(), Some(b"claude.ai"), &charter(), &program(b"source"), None);
    let first = old.invocations.iter().find(|call| call.arguments[0] == "-p").expect("first print");
    let run = program_world::run_with_window(
        &Story { resume: Resume::Missing, ..story() },
        &charter(),
        &program_world::from_frames(&old.frames),
    );
    assert!(
        matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)),
        "{:?} {}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    let print: Vec<_> = run.invocations.iter().filter(|call| call.arguments[0] == "-p").collect();
    assert_eq!(print.len(), 3, "missing resume, summary, new window");
    assert_eq!(session_argument(print[0], "--resume"), session_argument(first, "--session-id"));
    assert!(!print[1].arguments.contains(&"--mcp-config".into()));
    assert!(print[2].arguments.contains(&"--mcp-config".into()));
    assert_ne!(session_argument(print[2], "--session-id"), session_argument(first, "--session-id"));
    assert!(
        run.received.last().expect("new prompt")["message"]["content"]
            .as_array()
            .expect("blocks")
            .iter()
            .any(|block| block["text"] == "portable summary")
    );
    assert!(run.run_directories.is_empty());
}
