//! Open conversations on one program input (protocol/programs.md 5.9).
#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

use skein_lib::{Duration, Reader, Writer};
use smith_agent_process_world::{
    charter,
    program_world::{self, HostMessage, Run, When},
};
use smith_fake_claude::{Seen, Step, Story, Turn};

// Three reserve completions cover the interrupted turn, a refused wait, and text.
const RESERVE_TURNS: u32 = 3;
const ORDINARY_TURNS: u32 = 8;
const WAITING: Duration = Duration::from_millis(10);

fn open_charter() -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&charter()))
        .expect("charter")
        .into_parts();
    parts.mode = smith_charter::Mode::Open;
    parts.waiting = WAITING;
    parts.resume = true;
    let mut tools = parts.tools.into_parts();
    tools.wait = true;
    tools.deliver = None;
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("Open tools");
    let mut budget = parts.budget.into_parts();
    budget.turns = ORDINARY_TURNS + RESERVE_TURNS;
    budget.spend = 20; // Same small allowance as the existing program-turn fixture.
    budget.reserve = smith_charter::Reserve::new(
        &smith_charter::CEILINGS,
        smith_charter::ReserveParts { turns: RESERVE_TURNS, time: Duration::from_secs(1) },
    )
    .expect("reserve");
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("budget");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("Open charter");
    let mut writer = Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("charter body");
    writer.finish()
}

fn turn(steps: Vec<Step>) -> Turn {
    Turn { steps, ..Turn::default() }
}
fn text(text: &str) -> Turn {
    turn(vec![Step::Text(text.into())])
}
fn story(turns: Vec<Turn>) -> Story {
    Story { turns, observe_tools: true, ..Story::default() }
}
fn run(turns: Vec<Turn>, releases: &[HostMessage]) -> Run {
    program_world::open(&story(turns), &open_charter(), releases)
}

fn resume(old: &Run, turns: Vec<Turn>) -> Run {
    program_world::open_with_window(&story(turns), &open_charter(), &[], Some(&program_world::from_frames(&old.frames)))
}

fn host(when: When, finish_requested: bool) -> HostMessage {
    HostMessage { when, text: "Please continue with this message".into(), finish_requested }
}
fn parked(run: &Run) {
    assert!(
        matches!(run.answer().expect("answer").result(), smith_channel::RunResult::Parked),
        "answer={:?}; errors={}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    assert!(run.run_directories.is_empty());
}
fn turns(run: &Run) -> Vec<smith_channel::Turn> {
    run.frames
        .iter()
        .filter(|frame| frame.kind == 0x0109)
        .map(|frame| {
            smith_channel::Turn::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body)).expect("turn envelope")
        })
        .collect()
}
fn texts(run: &Run) -> Vec<String> {
    turns(run)
        .iter()
        .map(|envelope| {
            let turn = smith_transcript::Turn::decode(&smith_transcript::CEILINGS, &mut Reader::new(envelope.body()))
                .expect("transcript turn");
            let mut text = String::new();
            for message in turn.messages() {
                if message.role() == &smith_transcript::Role::Assistant {
                    for block in message.blocks() {
                        if let smith_transcript::Block::Text(part) = block {
                            text.push_str(std::str::from_utf8(part.text()).expect("assistant text"));
                        }
                    }
                }
            }
            text
        })
        .collect()
}
fn waitings(run: &Run) -> Vec<Option<u64>> {
    run.frames
        .iter()
        .filter(|frame| frame.kind == 0x010a)
        .map(|frame| {
            *smith_channel::Waiting::decode(&smith_channel::CEILINGS, &mut Reader::new(&frame.body))
                .expect("Waiting")
                .last_read()
        })
        .collect()
}
fn prints(run: &Run) -> Vec<&smith_fake_claude::Observation> {
    run.invocations.iter().filter(|invocation| invocation.arguments[0] == "-p").collect()
}
fn argument<'a>(invocation: &'a smith_fake_claude::Observation, flag: &str) -> &'a str {
    &invocation.arguments.windows(2).find(|pair| pair[0] == flag).expect("flag")[1]
}
fn received_text(run: &Run, index: usize) -> &str {
    run.received[index]["message"]["content"].as_str().expect("one user text")
}

#[test]
fn an_open_reply_is_the_turns_text() {
    let run = run(vec![text("Hello.")], &[]);
    parked(&run);
    assert_eq!(texts(&run), ["Hello."]);
    assert_eq!(*turns(&run)[0].last_read(), Some(0));
    assert_eq!(waitings(&run), [Some(0)]);
    let prints = prints(&run);
    assert_eq!(prints.len(), 1);
    assert!(!prints[0].arguments.iter().any(|argument| argument == "--json-schema"));
    assert!(run.seen.contains(&Seen::Tools { names: vec!["wait".into(), "permission_prompt".into()] }));
    assert!(!run.seen.contains(&Seen::Interrupted));
    assert!(!run.seen.contains(&Seen::Terminated));
    assert!(run.exits.iter().all(|exit| *exit == skein_io::kernel::Exit::Code(0)));
    let turn_index = run.frames.iter().position(|frame| frame.kind == 0x0109).expect("turn");
    let waiting_index = run.frames.iter().position(|frame| frame.kind == 0x010a).expect("waiting");
    assert!(turn_index < waiting_index);
}

#[test]
fn a_message_while_it_listens_is_the_programs_next_turn() {
    let release = host(When::Waiting(1), false);
    let run = run(vec![text("Hello."), text("Second reply.")], &[release]);
    parked(&run);
    assert_eq!(texts(&run), ["Hello.", "Second reply."]);
    assert_eq!(turns(&run).iter().map(|turn| *turn.last_read()).collect::<Vec<_>>(), [Some(0), Some(1)]);
    assert_eq!(waitings(&run), [Some(0), Some(1)]);
    assert_eq!(prints(&run).len(), 1);
    assert_eq!(run.received.len(), 2);
    assert_eq!(received_text(&run, 1), "host: Please continue with this message\n");
}

#[test]
fn a_parked_open_run_resumes_its_program_session() {
    let old = run(vec![text("Hello.")], &[]);
    let run = resume(&old, vec![text("Resumed.")]);
    parked(&run);
    assert_eq!(texts(&run), ["Resumed."]);
    assert_eq!(prints(&run).len(), 1);
    assert_eq!(argument(prints(&run)[0], "--resume"), argument(prints(&old)[0], "--session-id"));
    assert!(!received_text(&run, 0).starts_with("(The agent paused"), "the saved turn ended naturally");
}

#[test]
fn an_open_turn_with_no_text_is_nudged() {
    let run = run(vec![turn(vec![]), text("Hello.")], &[]);
    parked(&run);
    assert_eq!(texts(&run), ["", "Hello."]);
    assert_eq!(prints(&run).len(), 1);
    assert_eq!(run.received.len(), 2);
    assert!(received_text(&run, 1).contains("reply"));
    assert_eq!(waitings(&run), [Some(0)]);
}

#[test]
fn a_park_while_held_on_wait_interrupts_the_turn() {
    let old = run(vec![turn(vec![Step::Call { tool: "wait".into(), input: serde_json::json!({}) }])], &[]);
    parked(&old);
    assert!(old.seen.contains(&Seen::Interrupted));
    assert!(!old.seen.contains(&Seen::Terminated));
    let run = resume(&old, vec![text("Resumed.")]);
    parked(&run);
    assert!(
        received_text(&run, 0).starts_with("(The agent paused your last turn: the run was parked and has resumed.)\n")
    );
    assert_eq!(texts(&run), ["Resumed."]);
}

#[test]
fn a_finish_request_while_it_listens_winds_down_to_a_last_reply() {
    let run = run(vec![text("Hello."), text("Last reply.")], &[host(When::Waiting(1), true)]);
    parked(&run);
    assert_eq!(texts(&run), ["Hello.", "Last reply."]);
    assert_eq!(run.received.len(), 2);
    assert!(received_text(&run, 1).contains("Reply now with what you have"));
    assert!(received_text(&run, 1).contains("Please continue with this message"));
    assert!(!received_text(&run, 1).starts_with("(The agent paused"), "the preceding turn ended naturally");
    assert_eq!(waitings(&run), [Some(0), Some(1)]);
    assert_eq!(*run.answer().expect("parked").last_read(), Some(1));
}

#[test]
fn a_finish_request_mid_turn_interrupts_then_takes_the_last_reply() {
    let run = run(
        vec![
            turn(vec![Step::Hang]),
            turn(vec![
                Step::CallChecked { tool: "wait".into(), input: serde_json::json!({}), contains: "not run".into() },
                Step::Text("Last reply.".into()),
            ]),
        ],
        &[host(When::Turn(1), true)],
    );
    parked(&run);
    assert!(run.seen.contains(&Seen::Interrupted));
    assert!(!run.seen.contains(&Seen::Terminated));
    assert!(run.seen.contains(&Seen::Called { tool: "wait".into(), error: true }));
    assert!(received_text(&run, 1).starts_with("(The agent paused your last turn to send the finishing note.)\n"));
    assert!(received_text(&run, 1).contains("Reply now with what you have"));
    assert!(received_text(&run, 1).contains("Please continue with this message"));
    assert_eq!(texts(&run).last().expect("last text"), "Last reply.");
    assert_eq!(prints(&run).len(), 1);
    assert_eq!(*run.answer().expect("parked").last_read(), Some(1));
}
