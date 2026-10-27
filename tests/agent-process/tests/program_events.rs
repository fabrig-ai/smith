#![expect(
    clippy::wildcard_enum_match_arm,
    reason = "ordinary fixture selects event kinds; the shared referee checks every variant"
)]
#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

use serde_json::json;
use skein_lib::{Reader, Writer};
use smith_agent_process_world::{
    charter, events, program,
    program_world::{self, Case, Run},
};
use smith_events::{Backend, Event, FailureClass, Outcome};
use smith_fake_claude::{End, Step, Story, Turn};

fn run(steps: Vec<Step>, agents: bool) -> Run {
    scripted(&Story { turns: vec![Turn { steps, end: End::default() }], ..Default::default() }, agents)
}

fn scripted(story: &Story, agents: bool) -> Run {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&charter()))
        .expect("bounded event fixture")
        .into_parts();
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 20;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("bounded event fixture");
    let mut tools = parts.tools.into_parts();
    tools.families = smith_charter::Families::new(
        &smith_charter::CEILINGS,
        smith_charter::FamiliesParts { skein_bools: [false, false, false, agents] },
    )
    .expect("bounded event fixture");
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("bounded event fixture");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("bounded event fixture");
    let mut writer = Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("bounded event fixture");
    let run = program_world::run(Case::Events, story, Some(b"claude.ai"), &writer.finish(), &program(b"source"), None);
    assert!(
        matches!(run.answer().expect("bounded event fixture").result(), smith_channel::RunResult::Accepted(_)),
        "{:?} {}",
        run.answer(),
        String::from_utf8_lossy(&run.errors)
    );
    events::check_program(&run.events, &run.received);
    run
}

fn finish() -> Step {
    Step::Outcome { input: json!({"outcome":{"form":"report","text":"done","fields":{}}}) }
}

#[test]
fn a_program_run_s_events_name_its_backend() {
    let run = run(vec![finish()], false);
    let mut models = 0;
    for record in &run.events {
        let model = match &record.event {
            Event::RunStarted(start) => Some(&start.main),
            Event::ConversationOpened(opening) => Some(&opening.model),
            Event::ResponseStarted(start) => Some(&start.model),
            _ => None,
        };
        if let Some(model) = model {
            assert_eq!(model.backend, Backend::ClaudeCode);
            models += 1;
        }
    }
    assert!(models >= 3);
    let prompt = run
        .events
        .iter()
        .find_map(|record| match &record.event {
            Event::ResponseStarted(start) => start.prompt.as_ref(),
            _ => None,
        })
        .expect("bounded event fixture");
    assert!(prompt.system.is_some());
    assert!(!prompt.tools.as_ref().expect("bounded event fixture").iter().any(|tool| tool.as_ref() == b"finish"));
    assert!(!prompt.messages.is_empty(), "first hand-off carries the start's text");
}

#[test]
fn partial_text_reaches_the_events() {
    let run = run(vec![Step::Text("partial words".into()), finish()], false);
    let text: Vec<u8> = run
        .events
        .iter()
        .filter_map(|record| match &record.event {
            Event::TextDelta(delta) => Some(delta.text.as_ref()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(text, b"partial words");
}

#[test]
fn an_api_retry_is_a_failed_response() {
    let run = run(vec![Step::Retry { attempt: 1, status: 503, delay_ms: Some(125) }, finish()], false);
    let index = run
        .events
        .iter()
        .position(
            |record| matches!(&record.event, Event::ResponseCompleted(response) if response.outcome == Outcome::Failed),
        )
        .expect("bounded event fixture");
    let Event::ResponseCompleted(failed) = &run.events[index].event else { unreachable!() };
    assert_eq!(failed.failure.as_ref().expect("bounded event fixture").class, FailureClass::Unavailable);
    assert_eq!(failed.failure.as_ref().expect("bounded event fixture").status, Some(503));
    assert_eq!(failed.retry_ms, Some(125));
    let Event::ResponseStarted(next) = &run.events[index + 1].event else { panic!("retry start") };
    assert_eq!(next.attempt, 1);
    assert_ne!(next.response, failed.response);
    assert!(next.prompt.as_ref().expect("bounded event fixture").messages.is_empty());
}

#[test]
fn the_run_s_tools_withhold_sub_agents() {
    let run = run(vec![finish()], true);
    let start = run
        .events
        .iter()
        .find_map(|record| match &record.event {
            Event::RunStarted(start) => Some(start),
            _ => None,
        })
        .expect("bounded event fixture");
    assert_eq!(start.tools.withheld.as_slice(), &[smith_events::Family::SubAgents]);
    for record in &run.events {
        if let Event::ResponseStarted(start) = &record.event {
            assert!(
                !start
                    .prompt
                    .as_ref()
                    .expect("bounded event fixture")
                    .tools
                    .as_ref()
                    .is_some_and(|tools| tools.iter().any(|tool| tool.as_ref() == b"sub_agent")),
                "the accepted init contains no sub_agent server tool"
            );
        }
    }
}

#[test]
fn a_program_deadline_keeps_unknown_transport_measures() {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&charter()))
        .expect("charter")
        .into_parts();
    let mut budget = parts.budget.into_parts();
    budget.time = skein_lib::Duration::from_secs(1);
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("short budget");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("short run");
    let mut writer = Writer::new(charter.measure() as usize);
    charter.encode(&mut writer).expect("charter bytes");
    let run = program_world::run(
        Case::Events,
        &Story { turns: vec![Turn { steps: vec![Step::Hang], end: End::default() }], ..Default::default() },
        Some(b"claude.ai"),
        &writer.finish(),
        &program(b"source"),
        None,
    );
    events::check_program(&run.events, &run.received);
    assert!(run.events.iter().any(|record| matches!(&record.event, Event::ResponseCompleted(response) if response.outcome == Outcome::Failed && response.request_bytes.is_none() && response.first_byte_ms.is_none() && response.largest_gap_ms.is_none())));
}

#[test]
fn a_retry_abandons_its_partial_attempt() {
    let run = run(
        vec![Step::Partial("unfinished".into()), Step::Retry { attempt: 1, status: 503, delay_ms: None }, finish()],
        false,
    );
    let responses: Vec<_> = run
        .events
        .iter()
        .filter_map(|record| match &record.event {
            Event::ResponseCompleted(response) => Some(response),
            _ => None,
        })
        .collect();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].outcome, Outcome::Failed);
    assert_eq!(responses[1].outcome, Outcome::Completed);
    let failed = run
        .events
        .iter()
        .position(|record| {
            matches!(&record.event,
        Event::ResponseCompleted(response) if response.outcome == Outcome::Failed)
        })
        .expect("program event observed");
    assert!(matches!(&run.events[failed + 1].event, Event::ResponseStarted(start) if start.attempt == 1));
}

#[test]
fn a_slow_init_precedes_the_response_start() {
    let mut story = Story {
        turns: vec![Turn { steps: vec![Step::Text("words".into()), finish()], end: End::default() }],
        ..Default::default()
    };
    story.init.delay_ms = Some(200);
    let run = scripted(&story, false);
    let started = run
        .events
        .iter()
        .find(|record| matches!(record.event, Event::ResponseStarted(_)))
        .expect("program event observed");
    let Event::ResponseStarted(response) = &started.event else { unreachable!() };
    let ended = run
        .events
        .iter()
        .find(|record| {
            matches!(&record.event,
        Event::ResponseCompleted(done) if done.response == response.response)
        })
        .expect("program event observed");
    let Event::ResponseCompleted(done) = &ended.event else { unreachable!() };
    let admitted =
        run.events.iter().find(|record| matches!(record.event, Event::RunStarted(_))).expect("program event observed");
    assert!(started.t_ms >= admitted.t_ms + 200, "start includes spawn and slow init");
    assert_eq!(done.duration_ms, ended.t_ms - started.t_ms);
    assert!(done.duration_ms < 200, "duration excludes init");
}

#[test]
fn the_program_referee_checks_sent_message_order() {
    let run = scripted(
        &Story {
            turns: vec![
                Turn { steps: vec![Step::Text("first answer".into())], end: End::default() },
                Turn { steps: vec![finish()], end: End::default() },
            ],
            ..Default::default()
        },
        false,
    );
    assert!(run.received.len() >= 2, "initial user text and a later continuation");
    let mut changed = run.received.clone();
    changed[0]["message"]["content"] = json!("different input");
    assert!(std::panic::catch_unwind(|| events::check_program(&run.events, &changed)).is_err());
    let mut reordered = run.received.clone();
    reordered.swap(0, 1);
    assert!(std::panic::catch_unwind(|| events::check_program(&run.events, &reordered)).is_err());
    assert!(std::panic::catch_unwind(|| events::check_program(&run.events, &run.received[1..])).is_err());
}
