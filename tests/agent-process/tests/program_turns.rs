#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

use serde_json::json;
use smith_agent_process_world::{
    charter, program,
    program_world::{self, Case, Run},
};
use smith_channel::{ModelFault, RunFailure, RunResult};
use smith_fake_claude::{End, Step, Story, Turn};

fn charter_turns() -> Box<[u8]> {
    let source = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&charter()))
        .expect("bounded test fixture");
    let mut parts = source.into_parts();
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 20;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("bounded test fixture");
    let record = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("bounded test fixture");
    let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).expect("charter size"));
    record.encode(&mut writer).expect("bounded test fixture");
    writer.finish()
}
fn run(turns: Vec<Turn>) -> Run {
    program_world::run(
        Case::Turns,
        &Story { turns, ..Default::default() },
        Some(b"claude.ai"),
        &charter_turns(),
        &program(b"source"),
        None,
    )
}
fn finish() -> Step {
    Step::Outcome { input: json!({"outcome":{"form":"report","text":"done","fields":{}}}) }
}
fn accepted(run: &Run) {
    assert!(matches!(run.answer().expect("answer").result(), RunResult::Accepted(_)), "{:?}", run.answer());
}
fn fault(run: &Run) -> ModelFault {
    let answer = run.answer().expect("answer");
    match answer.result() {
        RunResult::Failed(failed) => match failed.reason() {
            RunFailure::Model(model) => model.value().clone(),
            other @ (RunFailure::Transcript(_)
            | RunFailure::Budget(_)
            | RunFailure::Policy(_)
            | RunFailure::Cancelled
            | RunFailure::Stale) => panic!("{other:?}"),
        },
        other @ (RunResult::Accepted(_) | RunResult::Parked | RunResult::Refused(_)) => panic!("{other:?}"),
    }
}
#[test]
fn a_program_turn_runs_to_its_finish() {
    let run = run(vec![Turn { steps: vec![finish()], end: End::default() }]);
    accepted(&run);
    assert!(
        !run.seen.iter().any(|seen| matches!(seen, smith_fake_claude::Seen::Called {tool,..} if tool == "finish")),
        "contract finish is never an MCP call"
    );
}
#[test]
fn a_structured_output_without_finish_is_its_finish() {
    accepted(&run(vec![Turn {
        steps: vec![],
        end: End::Success { structured_output: Some(json!({"outcome":{"form":"report","text":"done","fields":{}}})) },
    }]));
}
#[test]
fn a_rejected_finish_reaches_the_model() {
    let run = run(vec![
        Turn {
            steps: vec![Step::Outcome {
                input: json!({"outcome":{"form":"report","text":"x".repeat(129),"fields":{}}}),
            }],
            end: End::default(),
        },
        Turn { steps: vec![finish()], end: End::default() },
    ]);
    accepted(&run);
    assert!(
        run.received.iter().any(|message| message.to_string().contains("problem text-too-large form=report max=128")),
        "rejected outcome reaches the next turn: {:?}",
        run.received
    );
}
#[test]
fn a_text_only_turn_is_nudged() {
    accepted(&run(vec![
        Turn { steps: vec![Step::Text("thinking".into())], end: End::default() },
        Turn { steps: vec![finish()], end: End::default() },
    ]));
}
#[test]
fn usage_is_counted_per_message() {
    let story = Story {
        turns: vec![Turn { steps: vec![Step::Text("thinking".into()), finish()], end: End::default() }],
        usage: smith_fake_claude::Usage { input_tokens: 600_000, output_tokens: 0 },
        ..Default::default()
    };
    let run = program_world::run(Case::Turns, &story, Some(b"claude.ai"), &charter_turns(), &program(b"source"), None);
    accepted(&run);
    let answer = run.answer().expect("bounded test fixture");
    assert_eq!(answer.turns(), 2);
    assert_eq!(answer.spent(), 2);
}

fn limited(run: &Run, reset: Option<u64>) {
    let ModelFault::Limited(fault) = fault(run) else { panic!("limited fault") };
    assert_eq!(fault.account(), 0);
    let notices: Vec<_> = run.frames.iter().filter(|frame| frame.kind == 0x010e).collect();
    assert_eq!(notices.len(), 1, "one limited notice reaches the host");
    let notice =
        smith_channel::Limited::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&notices[0].body))
            .expect("limited notice");
    assert_eq!(notice.account(), 0);
    assert_eq!(notice.reset(), fault.reset(), "notice retains the fault's reset");
    match reset {
        Some(seconds) => {
            let duration = notice.reset().expect("the supplied reset is retained");
            assert!(
                duration
                    <= skein_lib::Duration::from_secs(
                        seconds - skein_sim::Config::calm().wall.as_nanos() / 1_000_000_000
                    )
            );
            assert!(
                duration
                    > skein_lib::Duration::from_secs(
                        seconds - skein_sim::Config::calm().wall.as_nanos() / 1_000_000_000 - 1
                    ),
                "absolute reset becomes a remaining duration"
            );
        }
        None => assert_eq!(*notice.reset(), None, "missing reset remains absent"),
    }
}
#[test]
fn a_usage_limit_fails_the_run_limited() {
    for resets_at in [None, Some(skein_sim::Config::calm().wall.as_nanos() / 1_000_000_000 + 123)] {
        let run = run(vec![Turn { steps: vec![Step::RateLimit { resets_at }], end: End::default() }]);
        limited(&run, resets_at);
    }
}
#[test]
fn an_error_result_retains_the_usage_limit_notice() {
    for resets_at in [None, Some(skein_sim::Config::calm().wall.as_nanos() / 1_000_000_000 + 123)] {
        let run = run(vec![Turn {
            steps: vec![],
            end: End::Error {
                api_error_status: Some(429),
                terminal_reason: Some("usage_limit".into()),
                errors: vec!["limit".into()],
                resets_at,
            },
        }]);
        limited(&run, resets_at);
    }
}
#[test]
fn a_login_lost_mid_run_fails_unauthorized() {
    let run = run(vec![Turn { steps: vec![Step::LoseLogin, Step::Text("continue".into())], end: End::default() }]);
    let ModelFault::Unauthorized(fault) = fault(&run) else { panic!("unauthorized fault") };
    assert_eq!(fault.account(), 0);
    let notices: Vec<_> = run.frames.iter().filter(|frame| frame.kind == 0x010d).collect();
    assert_eq!(notices.len(), 1, "one unauthorized notice reaches the host");
    let notice =
        smith_channel::Unauthorized::decode(&smith_channel::CEILINGS, &mut skein_lib::Reader::new(&notices[0].body))
            .expect("unauthorized notice");
    assert_eq!(notice.account(), 0);
}
#[test]
fn a_program_that_exits_before_its_result_fails_the_run() {
    assert!(matches!(fault(&run(vec![Turn { steps: vec![], end: End::Exit(7) }])), ModelFault::Program(_)));
}
#[test]
fn an_undecodable_line_fails_the_run() {
    assert!(matches!(fault(&run(vec![Turn { steps: vec![], end: End::Undecodable }])), ModelFault::Program(_)));
}

#[test]
fn a_finished_tool_turn_resumes_without_an_extra_completion() {
    let run = run(vec![
        Turn {
            steps: vec![Step::Outcome {
                input: json!({"outcome":{"form":"report","text":"x".repeat(129),"fields":{}}}),
            }],
            end: End::default(),
        },
        Turn { steps: vec![finish()], end: End::default() },
    ]);
    accepted(&run);
    assert_eq!(run.answer().expect("answer").turns(), 2);
}

#[test]
fn a_structured_failure_ends_with_its_declared_failure() {
    let mut parts =
        smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&charter_turns()))
            .expect("charter")
            .into_parts();
    let mut contract = parts.contract.into_parts();
    contract.failure = contract.report.take();
    parts.contract = smith_charter::Contract::new(&smith_charter::CEILINGS, contract).expect("failure contract");
    let charter = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("charter");
    let mut writer = skein_lib::Writer::new(usize::try_from(charter.measure()).expect("charter bytes"));
    charter.encode(&mut writer).expect("charter");
    let story = Story {
        turns: vec![Turn {
            steps: vec![Step::Outcome {
                input: json!({"outcome":{"form":"failure","reason":"cannot proceed","fields":{}}}),
            }],
            end: End::default(),
        }],
        ..Story::default()
    };
    let run = program_world::run(Case::Turns, &story, Some(b"claude.ai"), &writer.finish(), &program(b"source"), None);
    let answer = run.answer().expect("answer");
    let RunResult::Accepted(accepted) = answer.result() else { panic!("declared result: {answer:?}") };
    let result =
        smith_charter::RunResult::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(accepted.result()))
            .expect("outcome");
    assert_eq!(result.form(), &smith_charter::Form::Failure);
    assert_eq!(result.text(), b"cannot proceed");
    assert_eq!(answer.turns(), 1);
    assert!(
        !run.seen.iter().any(|seen| matches!(seen, smith_fake_claude::Seen::Called { tool, .. } if tool == "finish"))
    );
}
