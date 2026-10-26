use smith_agent_process_world::{World, charter};
use smith_channel::{RunFailure, RunResult};

#[test]
fn an_answered_agent_exits_0_with_no_signal_from_its_host() {
    let charter = charter();
    let mut world = World::new(7, &charter);
    assert!(world.settle(), "the admitted run answered");
    assert_eq!(world.observed().iter().filter(|frame| frame.kind == 0x0110).count(), 1);
    assert!(world.peer_replied(), "the fake LLM served a plaintext response: {:?}", world.peer_observations());
    assert_eq!(world.peer_queries().len(), 1, "the independent peer decoded one model request");
    let answer = world.answer().expect("host saw final answer");
    assert_eq!(answer.turns(), 1);
    assert!(matches!(answer.result(), RunResult::Failed(_)), "one-turn budget ended the run");
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    assert!(!world.signalled(), "answered process exits without a host signal");
    assert!(!String::from_utf8(world.errors()).expect("diagnostic UTF-8").contains("could not answer"));
    assert!(world.errors().starts_with(b"smith: commands run in process groups and are not contained\n"));
    world.assert_agent_clean();
}

#[test]
fn a_failed_answer_ends_standard_error_with_its_failure_in_words() {
    let mut world = World::new(7, &charter());
    assert!(world.settle());
    let errors = String::from_utf8(world.errors()).expect("diagnostic UTF-8");
    assert_eq!(errors.lines().last(), Some("smith: run failed: out of turns: used 1 of 1"));
    assert_eq!(errors.lines().filter(|line| line.starts_with("smith: run failed:")).count(), 1);
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    world.assert_agent_clean();
}

#[test]
fn a_termination_signal_in_the_middle_of_a_turn_answers_cancelled() {
    let charter = charter();
    let mut world = World::new(8, &charter);
    world.signal();
    assert!(world.settle(), "cancellation was answered");
    assert!(world.observed().iter().any(|frame| frame.kind == 0x0106), "run admitted before signal");
    let answer = world.answer().expect("host saw final answer");
    assert!(matches!(answer.result(), RunResult::Failed(failed) if failed.reason() == &RunFailure::Cancelled));
    world.assert_agent_clean();
}

#[test]
fn a_termination_signal_after_the_answer_aborts_a_peer_that_ignores_the_close() {
    let mut world = World::new(17, &charter());
    world.signal_after_answer();
    assert!(world.settle());
    assert!(world.answer().is_some());
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    let smith_events::Event::SessionEnded(end) = &world.records().last().expect("authoritative end").event else {
        panic!("session end is last")
    };
    assert_eq!(end.aborted, Some(true));
    assert!(end.teardown_ms.expect("answered teardown") < 5000);
    assert_eq!(world.signals(), 1);
    world.assert_agent_clean();
}

#[test]
fn a_second_signal_after_the_answer_changes_nothing() {
    let mut world = World::new(19, &charter());
    world.signal_after_answer();
    world.signal_after_answer();
    assert!(world.settle());
    assert_eq!(world.signals(), 2);
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    let notices = world.records().iter().filter(|record| {
        matches!(&record.event, smith_events::Event::Notice(notice) if notice.kind == smith_events::NoticeKind::TeardownAborted)
    });
    assert_eq!(notices.count(), 1);
    let smith_events::Event::SessionEnded(end) = &world.records().last().expect("one terminal").event else {
        panic!("session end is last")
    };
    assert_eq!(end.aborted, Some(true));
    world.assert_agent_clean();
}

#[test]
fn full_kernel_replay_leaves_the_same_answer() {
    for cancel in [false, true] {
        skein_world::domain::assert_replays(7, 8, |seed| {
            let mut world = World::new(seed, &charter());
            if cancel {
                world.signal();
            }
            assert!(world.settle());
            (world.trace(), world.answer())
        });
    }
}

#[test]
fn an_agent_whose_host_hangs_up_before_its_answer_says_so_and_exits_1() {
    let mut world = World::new(9, &charter());
    world.hang_up();
    assert!(world.settle(), "the unstarted channel settles without a signal");
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(1)));
    assert!(world.answer().is_none());
    assert!(!world.signalled());
    let errors = String::from_utf8(world.errors()).expect("diagnostic UTF-8");
    assert!(
        errors.starts_with(
            "smith: commands run in process groups and are not contained\nsmith: agent started; worst case "
        )
    );
    assert_eq!(errors.lines().last(), Some("smith: the run could not answer: the channel ended before the answer"));
    assert_eq!(errors.lines().count(), 3, "containment, startup and failure each appear once");
    world.assert_agent_clean();
}

#[test]
fn a_refused_configuration_writes_one_line_and_exits_1_without_a_channel() {
    use smith_agent_shell::{Agent, Resources};
    let errors = smith_agent_process_world::AgentErrors::default();
    let startup = Agent::read(
        std::path::Path::new(""),
        Resources {
            programs: None,
            input: skein_io::kernel::Fd::new(100),
            output: skein_io::kernel::Fd::new(101),
            signals: skein_io::kernel::Fd::new(102),
            error: None,
            seed: 1,
            pid: 101,
            trace: None,
            roots: Some(Box::new([])),
        },
        Box::new(errors.clone()),
    );
    let exit = match startup {
        Ok(agent) => {
            assert!(skein_world::Host::is_empty(&agent), "a refused startup never adopts these nonexistent streams");
            skein_world::Host::exit(&agent)
        }
        Err(_) => Some(skein_io::kernel::Exit::Code(1)),
    };
    assert_eq!(exit, Some(skein_io::kernel::Exit::Code(1)));
    let text = String::from_utf8(errors.bytes()).expect("diagnostic UTF-8");
    assert!(text.starts_with("smith: configuration metadata:"));
    assert_eq!(text.lines().count(), 1);
}

#[test]
fn the_configured_environment_reaches_the_actual_agents_command_spawn() {
    let mut world = World::new(42, &charter());
    world.configured_environment();
    assert!(world.settle());
    let observed = world.command_environments();
    assert_eq!(observed.len(), 1, "the granted model call started one actual command");
    let expected: &[Box<[u8]>] =
        &[Box::from(b"BUILD_FLAG=configured=value".as_slice()), Box::from(b"EMPTY=".as_slice())];
    assert_eq!(observed[0].as_ref(), expected);
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    world.assert_agent_clean();
}

#[test]
fn a_broken_declared_relationship_is_named_before_channel_adoption() {
    use smith_agent_shell::{Agent, Resources};
    let errors = smith_agent_process_world::AgentErrors::default();
    let path = skein_scratch::paths::package().join("fixtures/read-past-payload.json");
    let startup = Agent::read(
        &path,
        Resources {
            programs: None,
            input: skein_io::kernel::Fd::new(100),
            output: skein_io::kernel::Fd::new(101),
            signals: skein_io::kernel::Fd::new(102),
            error: None,
            seed: 1,
            pid: 101,
            trace: None,
            roots: Some(Box::new([])),
        },
        Box::new(errors.clone()),
    );
    match startup {
        Ok(_) => panic!("broken read/payload relationship must refuse before adopting nonexistent channel handles"),
        Err(refusal) => {
            assert_eq!(refusal, "read-in-payload: declared.read_window=1025 bytes; declared.tool_payload=1024 bytes");
        }
    }
    let text = String::from_utf8(errors.bytes()).expect("diagnostic UTF-8");
    assert_eq!(text, "smith: read-in-payload: declared.read_window=1025 bytes; declared.tool_payload=1024 bytes\n");
}

#[test]
fn the_complete_shell_memory_refusal_precedes_channel_adoption() {
    use smith_agent_shell::{Agent, Resources, config};
    let errors = smith_agent_process_world::AgentErrors::default();
    let mut configuration = config::parse(
        br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"tool_payload":256,"read_window":256}},"endpoints":[{"name":"fake","number":0,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","transport":"plaintext","connect":1000,"handshake":1000,"models":[{"name":"fake","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":1000,"idle":1000}]}],"environment":[]}"#,
    )
    .expect("valid startup declaration");
    let available =
        smith_agent_service::worst_case(&configuration.service.limits, &configuration.service.llm_endpoints)
            .expect("bounded service");
    configuration.memory = available;
    let startup = Agent::new(
        configuration,
        Resources {
            programs: None,
            input: skein_io::kernel::Fd::new(100),
            output: skein_io::kernel::Fd::new(101),
            signals: skein_io::kernel::Fd::new(102),
            error: None,
            seed: 1,
            pid: 101,
            trace: None,
            roots: Some(Box::new([])),
        },
        Box::new(errors.clone()),
    );
    let Err(refusal) = startup else {
        panic!("outer shell memory must refuse before adopting nonexistent channel handles");
    };
    assert!(refusal.starts_with("memory: service.worst_case="));
    assert!(refusal.ends_with(&format!("; declared.memory={available} bytes")));
    assert_eq!(String::from_utf8(errors.bytes()).expect("UTF-8 refusal"), format!("smith: {refusal}\n"));
}

#[test]
fn an_answered_agent_exits_while_its_host_holds_the_channel_open() {
    let mut world = World::new(7, &charter());
    world.hold_channel();
    assert!(world.settle(), "the agent ends by itself");
    assert!(world.answer().is_some());
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    assert!(!world.signalled());
    world.assert_agent_clean();
}

#[test]
fn a_host_that_hangs_up_mid_call_gets_one_cancelled_terminal_per_call() {
    let mut world = World::new(8, &charter());
    world.observe_facts(smith_agent_process_world::TraceCase::Accepted, 128);
    world.hang_up_mid_call();
    assert!(world.settle(), "the cancelled run and its components settle");
    assert!(world.answer().is_none());
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(1)));
    assert!(!world.signalled());
    assert_eq!(
        world
            .records()
            .iter()
            .filter(|record| matches!(&record.event, smith_events::Event::ResponseCompleted(response) if response.outcome == smith_events::Outcome::Cancelled))
            .count(),
        1
    );
    world.assert_agent_clean();
}

#[test]
fn a_provider_refusal_ends_standard_error_with_its_clipped_text() {
    let mut world = World::new(6815, &charter());
    world.reject_provider();
    assert!(world.settle());
    let errors = String::from_utf8(world.errors()).expect("operator UTF-8");
    let last = errors.lines().last().expect("failure line");
    assert!(last.starts_with("smith: run failed: "));
    assert!(last.ends_with(": context_length_exceeded"));
    assert_eq!(errors.lines().filter(|line| line.starts_with("smith: run failed:")).count(), 1);
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    world.assert_agent_clean();
}

fn stopped_reasoning_charter() -> Box<[u8]> {
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut skein_lib::Reader::new(&charter()))
        .expect("ordinary charter")
        .into_parts();
    let mut main = parts.main.into_parts();
    main.window = 272_000;
    main.output = 16_384;
    parts.main = smith_charter::Llm::new(&smith_charter::CEILINGS, main).expect("live window");
    let mut budget = parts.budget.into_parts();
    budget.turns = 8;
    budget.spend = 1_000_000;
    parts.budget = smith_charter::Budget::new(&smith_charter::CEILINGS, budget).expect("three-turn story");
    let mut tools = parts.tools.into_parts();
    let mut families = tools.families.into_parts();
    families.skein_bools[0] = true;
    tools.families = smith_charter::Families::new(&smith_charter::CEILINGS, families).expect("read authority");
    parts.tools = smith_charter::Tools::new(&smith_charter::CEILINGS, tools).expect("read and finish");
    let record = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("stopped-turn charter");
    let mut bytes = skein_lib::Writer::new(usize::try_from(record.measure()).expect("charter size"));
    record.encode(&mut bytes).expect("charter bytes");
    bytes.finish()
}

#[test]
fn a_reasoning_turn_that_stops_after_a_read_is_told_and_nudged() {
    let mut world = World::new(1_791_840_074_363_144_235, &stopped_reasoning_charter());
    world.observe_facts(smith_agent_process_world::TraceCase::StoppedReasoning, 8);
    world.acknowledgement_window(1);
    assert!(world.settle());
    let queries = world.peer_queries();
    assert_eq!(queries.len(), 3);
    assert!(queries[2].messages.iter().flat_map(|message| &message.parts).any(|part| {
        matches!(part, skein_fake_llm_domain::api::Part::Text { text } if text.windows(b"You stopped without calling `finish`".len()).any(|part| part == b"You stopped without calling `finish`"))
    }));
    let answer = world.answer().expect("the finish answered");
    assert!(matches!(answer.result(), RunResult::Accepted(_)));
    assert_eq!(answer.turns(), 3);
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    assert!(!world.signalled());
    world.assert_agent_clean();
}

#[test]
fn a_hosted_agent_opens_a_root_beneath_a_supplied_one() {
    use skein_fake_llm_domain::api;
    let mut world = World::new(42, &charter());
    world.configured_environment();
    world.root_path(b"/host/supplied/sub");
    world.script(Box::new([
        api::Turn {
            lines: Box::new([api::Line::Call {
                name: b"read".as_slice().into(),
                arguments: br#"{"path":"/host/supplied/sub/problem.txt"}"#.as_slice().into(),
            }]),
            finish: api::Finish::ToolCalls,
            tokens: 1,
        },
        api::Turn {
            lines: Box::new([api::Line::Text { text: b"done".as_slice().into() }]),
            finish: api::Finish::Stop,
            tokens: 1,
        },
    ]));
    assert!(world.settle(), "the hosted root opens beneath the base");
    assert!(world.observed().iter().any(|frame| frame.kind == 0x0106), "the Start was admitted");
    assert_eq!(world.directory_opens().first().map(AsRef::as_ref), Some(b"sub".as_slice()));
    let queries = world.peer_queries();
    assert!(queries.len() >= 2, "the root-backed read reaches its next provider turn");
    assert!(queries[1].messages.iter().flat_map(|message| &message.parts).any(|part| {
        matches!(part, api::Part::ToolOutput { output, is_error: false, .. } if output.windows(b"beneath the base".len()).any(|bytes| bytes == b"beneath the base"))
    }), "the read found the file under sub through its requested host path");
    world.assert_agent_clean();
}

#[test]
fn a_hosted_agent_refuses_a_relative_root() {
    let mut world = World::new(42, &charter());
    world.root_path(b"relative-root");
    assert!(world.settle(), "a refused Start still answers and settles");
    assert!(!world.observed().iter().any(|frame| frame.kind == 0x0106), "the relative root is never admitted");
    assert!(world.peer_queries().is_empty(), "a root refusal precedes the provider");
    let answer = world.answer().expect("the host receives the refusal");
    assert!(
        matches!(answer.result(), RunResult::Refused(refused) if matches!(refused.reason(), smith_channel::StartRefusal::Invalid(invalid) if invalid.value() == &smith_channel::InvalidStart::Workspace)),
        "the Start's workspace was refused: {answer:?}"
    );
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    world.assert_agent_clean();
}

#[test]
fn a_hosted_agent_refuses_a_root_beneath_none() {
    assert_hosted_root_refused(b"/elsewhere");
}

#[test]
fn a_hosted_agent_refuses_a_root_climbing_out_of_its_base() {
    assert_hosted_root_refused(b"/host/supplied/../other");
}

fn assert_hosted_root_refused(path: &[u8]) {
    let mut world = World::new(42, &charter());
    world.root_path(path);
    assert!(world.settle(), "a refused Start still answers and settles");
    assert!(!world.observed().iter().any(|frame| frame.kind == 0x0106), "the root is never admitted");
    assert!(world.peer_queries().is_empty(), "a root refusal precedes the provider");
    assert!(world.directory_opens().is_empty(), "a refused path submits no directory open");
    let errors = String::from_utf8(world.errors()).expect("diagnostic UTF-8");
    assert!(
        errors.contains(&format!("the root {} is beneath no root the host supplied", String::from_utf8_lossy(path)))
    );
    let answer = world.answer().expect("the host receives the refusal");
    assert!(
        matches!(answer.result(), RunResult::Refused(refused) if matches!(refused.reason(), smith_channel::StartRefusal::Invalid(invalid) if invalid.value() == &smith_channel::InvalidStart::Workspace))
    );
    assert_eq!(world.exit(), Some(skein_io::kernel::Exit::Code(0)));
    world.assert_agent_clean();
}
