//! Ordered host sections through actual main/child sessions and native restore.
//! Expected system bodies are handwritten outside literals; concrete history,
//! child results, usage and lower settlement come from the composed routes.
//! Contract: domain/run.md, sections 3.1, 3.3, 5.3, 6 and 13;
//! testing-strategy.md, sections 2.3, 6 and 7.

use skein_fake_checkout::Checkout;
use skein_fake_llm_domain::api::{Finish, Line, Part, Query, Script, Turn};
use skein_lib::Token;
use skein_world::domain::Span;
use smith_agent_world::{Boundary, Job, Settings, World};
use smith_domain::{run, session::llm, tools};

const MAIN_INSTRUCTIONS: &str = "@brief-main PARENT-INSTRUCTIONS: coordinate this task.\nSeparate role line.";
const CHILD_INPUT: &[u8] = br#"{"brief":"@brief-child CHILD-TASK-ONLY: inspect both mounts.","tools":["inspect"]}"#;
const CHILD_RESULT: &[u8] = b"CHILD-RESULT: parent-change in work; unchanged in archive.";
const FINISH_INPUT: &[u8] = br#"{"report":"The child inspected the changed workspace.","source":"workspace"}"#;

// These literals do not call the production renderer or reproduce its algorithm.
// Duplicate Zulu titles and the intervening Alpha title must stay in host order.
const SECTIONS: &str = "## Zulu PARENT-TITLE-Z\n\nPARENT-BODY-ONE: café\nline one\n\n\
## Alpha PARENT-TITLE-A\n\nPARENT-BODY-TWO: λ ends with LF\n\n\
## Zulu PARENT-TITLE-Z\n\nPARENT-BODY-THREE: 東京\nthird line\n\n";
const GUIDES: &str = "## AGENTS.md in `work`\n\nWORK-GUIDE: inspect data.txt.\n\n\
## AGENTS.md in `archive`\n\nARCHIVE-GUIDE: keep every byte.\n\n";
const MAIN_MECHANICS: &str = "## Tools\n\n\
You can read, list and search the files in the checkout.\n\
You can write and edit files in its writable repositories.\n\n\
## Checkout\n\n\
Your working directory is `work`. Relative paths start there; reach the other directories as `/name/...`. Use `list /` to list their names.\n\
- `work`, which you may change, a git working tree, with checks (`.temper/pre-pr`)\n\
- `archive`, which you may only read, a plain directory\n\n\
## Sub-agents\n\n\
You can ask for a sub-agent: an LLM of its own, working on a brief you write, with tools no wider than \
yours and a share of the ordinary budget above the reserve. Its last message comes back with usage.\n\
A child starts cold: give it a focused brief naming paths, decisions and scope. Inspect-only children may work side by side; a writing child works alone.\n\
It runs on the \
run's main LLM unless you name one of these: `fake-2`.\n\n\
Call `wait` when you need another message; its result is withheld until messages arrive and returns with them in one request.\n\n\
## Finishing\n\n\
When the work is done, call `finish` with its result. A result outside the host's contract returns \
typed feedback; fix it and call `finish` again. Stopping without `finish` does not finish the run.\n\n\
Report: text up to 1024 bytes, with these host-required fields:\n\
- `source`: nonempty, at most 128 bytes\n\
Extra fields are allowed within the aggregate result byte limit; no field name may repeat.\n";
const MAIN_BUDGET: &str = "\n## Budget\n\nAt open, you have 64 turns and 3599 seconds of wall time. Spend is bounded too.\nKeep 0 turns and 0 seconds to finish. When the ordinary budget is spent, live children close and you finish on that reserve.\nSub-agents share only what is above the reserve. Ask for focused turn or time shares; leaving a part unasked gives the child all of that part left.\n";
const CHILD_MECHANICS: &str = "## Tools\n\n\
You can read, list and search the files in the checkout.\n\n\
## Checkout\n\n\
Your working directory is `work`. Relative paths start there; reach the other directories as `/name/...`. Use `list /` to list their names.\n\
- `work`, which you may change, a git working tree, with checks (`.temper/pre-pr`)\n\
- `archive`, which you may only read, a plain directory\n\n\
## Answering\n\n\
When you are done, end your turn with your answer. The run adds your usage to the result.\n\n\
## Your share\n\n\
At open, your share is 60 turns and 120 seconds. Your answer may hold at most 1024 bytes; anything past it is cut with a marker. Your last turn has no tools: answer with what you have.\n";

fn charter(settings: &Settings, instructions: &str) -> run::Charter {
    use run::charter::{Brief, Endpoint, Grants, Llm, Section, Tools};
    use run::outcome::{FieldRule, OutcomeSpec, TextSpec};
    let llm = Llm {
        effort: None,
        prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
        dialect: 1,
        account: Some(0),
        endpoint: Endpoint(0),
        model: b"fake-1".as_slice().into(),
        window: 65_536,
        output: 4096,
    };
    run::Charter {
        mode: run::Mode::Finite,
        reserve: run::charter::Reserve { turns: 0, time: skein_lib::Duration::ZERO },
        instructions: instructions.as_bytes().into(),
        brief: Brief {
            sections: Box::new([
                Section {
                    title: b"Zulu PARENT-TITLE-Z".as_slice().into(),
                    text: "PARENT-BODY-ONE: café\nline one".as_bytes().into(),
                },
                Section {
                    title: b"Alpha PARENT-TITLE-A".as_slice().into(),
                    text: "PARENT-BODY-TWO: λ ends with LF\n".as_bytes().into(),
                },
                Section {
                    title: b"Zulu PARENT-TITLE-Z".as_slice().into(),
                    text: "PARENT-BODY-THREE: 東京\nthird line".as_bytes().into(),
                },
            ]),
        },
        grants: Grants {
            wait: true,
            deliver: None,
            tools: Tools { inspect: true, modify: true, shell: false },
            agents: true,
            host_tools: Box::new([]),
        },
        outcome: OutcomeSpec {
            change: None,
            verdicts: Box::new([]),
            report: Some(TextSpec {
                max: 1024,
                fields: Box::new([FieldRule { name: b"source".as_slice().into(), max: 128 }]),
            }),
            failure: None,
        },
        budget: settings.budget,
        models: Box::new([Llm { effort: None, model: b"fake-2".as_slice().into(), ..llm.clone() }]),
        llm,
        conventions: Some(run::Conventions {
            guide: b"AGENTS.md".as_slice().into(),
            checks: b".temper/pre-pr".as_slice().into(),
        }),
        resume: settings.resume,
        waiting: settings.waiting,
    }
}

fn workspace() -> (Checkout, run::Workspace) {
    let mut disk = Checkout::new();
    disk.mkdir(b"work");
    disk.mkdir(b"archive");
    disk.write(b"work/AGENTS.md", b"WORK-GUIDE: inspect data.txt.\n");
    disk.write(b"archive/AGENTS.md", b"ARCHIVE-GUIDE: keep every byte.");
    disk.write(b"work/.temper/pre-pr", b"#!checks");
    disk.write(b"archive/.temper/pre-pr", b"#!readonly-check-must-not-run");
    disk.write(b"work/.git/HEAD", b"host-owned-git-metadata");
    disk.write(b"work/data.txt", b"old");
    disk.write(b"archive/data.txt", b"unchanged");
    let mounted = run::Workspace {
        directories: Box::new([
            run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"work".as_slice().into(),
                root: Token::new(disk.root(b"work")),
                writable: true,
                git: true,
                conflicts: Box::new([]),
            },
            run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"archive".as_slice().into(),
                root: Token::new(disk.root(b"archive")),
                writable: false,
                git: false,
                conflicts: Box::new([]),
            },
        ]),
    };
    (disk, mounted)
}

fn call(name: &[u8], arguments: &[u8]) -> Line {
    Line::Call { name: name.into(), arguments: arguments.into() }
}

fn calls(lines: Vec<Line>, tokens: u64) -> Turn {
    Turn { lines: lines.into(), finish: Finish::ToolCalls, tokens }
}

fn says(text: &[u8], tokens: u64) -> Turn {
    Turn { lines: Box::new([Line::Text { text: text.into() }]), finish: Finish::Stop, tokens }
}

fn typed_scripts() -> Box<[Script]> {
    Box::new([
        Script {
            cue: b"@brief-main".as_slice().into(),
            turns: Box::new([
                calls(vec![call(b"read", br#"{"path":"data.txt"}"#)], 2),
                calls(vec![call(b"edit", br#"{"path":"data.txt","edits":[{"old":"old","new":"parent-change"}]}"#)], 3),
                calls(vec![call(b"write", br#"{"path":"../archive/data.txt","content":"forbidden-main"}"#)], 5),
                calls(vec![call(b"sub_agent", CHILD_INPUT)], 7),
                calls(vec![call(b"finish", FINISH_INPUT)], 11),
            ]),
        },
        Script {
            cue: b"@brief-child".as_slice().into(),
            turns: Box::new([
                calls(
                    vec![call(b"read", br#"{"path":"data.txt"}"#), call(b"read", br#"{"path":"../archive/data.txt"}"#)],
                    13,
                ),
                calls(
                    vec![call(
                        b"edit",
                        br#"{"path":"data.txt","edits":[{"old":"parent-change","new":"forbidden-child"}]}"#,
                    )],
                    17,
                ),
                says(CHILD_RESULT, 19),
            ]),
        },
    ])
}

fn contains(bytes: &[u8], part: &[u8]) -> bool {
    bytes.windows(part.len()).any(|bytes| bytes == part)
}

fn feedback_has(query: &Query, part: &[u8]) -> bool {
    query
        .messages
        .iter()
        .flat_map(|message| &message.parts)
        .any(|item| matches!(item, Part::ToolOutput { output, is_error: true, .. } if contains(output, part)))
}

fn expected_main(instructions_literal: &str) -> Vec<u8> {
    [instructions_literal, SECTIONS, GUIDES, MAIN_MECHANICS, MAIN_BUDGET].concat().into_bytes()
}

fn typed_openings(world: &World) {
    let main_system = expected_main("@brief-main PARENT-INSTRUCTIONS: coordinate this task.\nSeparate role line.\n\n");
    let child_system =
        ["@brief-child CHILD-TASK-ONLY: inspect both mounts.\n\n", GUIDES, CHILD_MECHANICS].concat().into_bytes();
    let main = world.prompts().iter().filter(|query| query.system.starts_with(b"@brief-main")).collect::<Vec<_>>();
    let child = world.prompts().iter().filter(|query| query.system.starts_with(b"@brief-child")).collect::<Vec<_>>();
    assert_eq!(main.len(), 5, "actual main turns, including child result and accepted finish");
    assert_eq!(child.len(), 3, "actual child reads, denied edit and terminal answer");
    assert_eq!(main.len() + child.len(), world.prompts().len());
    for query in &main {
        assert_eq!(query.system.as_ref(), main_system, "literal complete ordered main system");
        assert_eq!(query.model.as_ref(), b"fake-1");
    }
    for query in &child {
        assert_eq!(query.system.as_ref(), child_system, "literal task, actual guides and narrowed mechanics");
        for sentinel in [
            b"PARENT-INSTRUCTIONS".as_slice(),
            b"PARENT-TITLE-Z",
            b"PARENT-TITLE-A",
            b"PARENT-BODY-ONE",
            b"PARENT-BODY-TWO",
            b"PARENT-BODY-THREE",
        ] {
            assert!(!contains(&query.system, sentinel), "parent payload must not implicitly reach child: {sentinel:?}");
        }
        assert_eq!(
            query.tools.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>(),
            [b"read".as_slice(), b"list", b"search"]
        );
    }
    assert!(main.iter().any(|query| feedback_has(query, b"ReadOnly")));
    assert!(child.iter().any(|query| feedback_has(query, b"UnknownTool")));
    assert!(main[4].messages.iter().flat_map(|message| &message.parts).any(|part| {
        matches!(part, Part::ToolOutput { output, is_error: false, .. } if output.starts_with(CHILD_RESULT) && contains(output, b"\n\nChild usage: used 3 turns"))
    }));
}

#[test]
fn ordered_sections_and_instructions_reach_main_but_child_receives_only_its_own_task() {
    let settings = Settings { job: Job::Reporting, network: Span::millis(1, 1), ..Settings::calm(1101) };
    let (disk, mounted) = workspace();
    let readonly = disk.tree(b"archive");
    let roots = mounted.directories.iter().map(|directory| directory.root).collect::<Vec<_>>();
    let mut world = World::with_workspace_scripts_charter(
        settings,
        None,
        Some(mounted),
        disk,
        typed_scripts(),
        charter(&settings, MAIN_INSTRUCTIONS),
    );
    world.run(10_000);
    typed_openings(&world);
    assert_eq!(world.disk().content(b"work/data.txt"), Some(b"parent-change".as_slice()));
    assert_eq!(world.disk().content(b"work/.git/HEAD"), Some(b"host-owned-git-metadata".as_slice()));
    assert_eq!(world.disk().tree(b"archive"), readonly);
    let stores = world
        .boundaries()
        .iter()
        .filter_map(|(_, boundary)| {
            if let Boundary::Io { op: tools::Op::Store { at, content, .. } } = boundary {
                Some((at.root, at.path.as_ref(), content.as_ref()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(stores, [(roots[0], b"data.txt".as_slice(), b"parent-change".as_slice())]);
    let loads = world
        .boundaries()
        .iter()
        .filter_map(|(_, boundary)| {
            if let Boundary::Io { op: tools::Op::Load { at, .. } } = boundary {
                Some((at.root, at.path.as_ref()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(
        loads.iter().filter(|(root, path)| *root == roots[0] && *path == b"data.txt").count(),
        3,
        "main's read and edit reload, then child's read"
    );
    assert_eq!(loads.iter().filter(|(root, path)| *root == roots[1] && *path == b"data.txt").count(), 1);
    assert_eq!(world.turns().len(), 5);
    assert_eq!(world.turns().iter().map(|turn| turn.sequence).collect::<Vec<_>>(), [1, 2, 3, 4, 5]);
    let delegated = &world.turns()[3];
    let child_call = delegated
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            llm::Block::ToolCall { id, name, input, call: llm::Decoded::Historical, .. }
                if name.as_ref() == b"sub_agent" && input.as_ref() == CHILD_INPUT =>
            {
                Some(id)
            }
            llm::Block::Opaque { .. }
            | llm::Block::Refusal { .. }
            | llm::Block::Text { .. }
            | llm::Block::ToolCall { .. }
            | llm::Block::ToolResult { .. } => None,
        })
        .expect("persisted raw child-task call");
    assert_eq!(
        delegated
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .filter(|block| {
                matches!(block, llm::Block::ToolResult { id, result: llm::Returned::Text {
                text, error: false, replay: None
            }} if id == child_call && text.starts_with(CHILD_RESULT) && contains(text, b"\n\nChild usage: used 3 turns"))
            })
            .count(),
        1,
        "actual child result is paired once with its caller's concrete Turn"
    );
    assert!(matches!(world.answer(), run::Answer::Accepted {
        outcome: run::outcome::Declared::Report(report), turns: 5, ..
    } if report.text.as_ref() == b"The child inspected the changed workspace."
        && report.fields.as_ref() == [run::outcome::Field {
            name: b"source".as_slice().into(), value: b"workspace".as_slice().into()
        }]));
    assert!(world.checked().is_empty() && world.pushes().is_empty() && world.host_submissions().is_empty());
    assert!(world.judged().0 > 0 && world.judged().1 > 0);
}

#[test]
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "the golden collects literal request text rather than every provider part"
)]
fn the_first_provider_request_contains_the_carried_messages_with_the_briefs_instruction() {
    use skein_lib::ReplyTo;
    let settings = Settings { job: Job::Reporting, network: Span::millis(1, 1), ..Settings::calm(1191) };
    for has_brief in [true, false] {
        let mut charter = charter(&settings, "@carried-opening");
        if !has_brief {
            charter.brief.sections = Box::new([]);
        }
        let (disk, mounted) = workspace();
        let start = smith_domain::Event::Start {
            messages: Box::new([
                run::Message {
                    finish_requested: false,
                    name: Token::new(99),
                    label: b"person".as_slice().into(),
                    text: b"first".as_slice().into(),
                },
                run::Message {
                    finish_requested: false,
                    name: Token::new(0),
                    label: b"person".as_slice().into(),
                    text: b"second".as_slice().into(),
                },
            ]),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: smith_domain::Window { turns: 100, bytes: u64::MAX },
            charter,
            workspace: Some(mounted),
            transcript: None,
            answered: Box::default(),
            grants: Box::new([smith_domain::Grant {
                renewable: true,
                name: smith_domain::GrantName { account: 0, generation: 1 },
                valid: skein_lib::Duration::from_secs(7200),
            }]),
        };
        let scripts = Box::new([Script {
            cue: b"@carried-opening".as_slice().into(),
            turns: Box::new([calls(vec![call(b"finish", FINISH_INPUT)], 1)]),
        }]);
        let mut world = World::with_workspace_scripts_start(settings, disk, scripts, start);
        world.run(10_000);
        assert_eq!(
            world.prompts().len(),
            1,
            "an immediate finish takes one actual request; answer={:?}",
            world.answer()
        );
        let expected: &[u8] = if has_brief {
            b"Begin the work your brief describes.\n\nperson: first\n\nperson: second"
        } else {
            b"person: first\n\nperson: second"
        };
        let messages: Vec<_> = world.prompts()[0]
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match part {
                Part::Text { text } => Some(text.as_ref()),
                _ => None,
            })
            .collect();
        assert_eq!(messages, [expected]);
        let mut referee =
            skein_world::domain::Referee::new(smith_agent_world::messages_referee::Meeting::new(0, settings.waiting));
        for (at, seen) in world.messages_seen() {
            referee.observe(*at, seen.clone(), &mut Vec::new());
        }
        assert_eq!(
            referee.verdict(),
            skein_world::domain::Verdict::Passed,
            "actual carried opening and finish satisfy the message oracle"
        );
        assert_eq!(world.turns().len(), 1);
        assert!(world.messages_seen().iter().any(|(_, seen)| matches!(seen, smith_agent_world::messages_referee::Seen::Turn { read: Some(name), .. } if *name == Token::new(0))));
    }
}

#[test]
fn the_request_after_a_crossed_finish_contains_the_call_answer_and_queued_messages_together() {
    use skein_lib::{Duration, Time};
    use skein_world::domain::{Referee, Verdict};
    use smith_agent_world::messages_referee::{Meeting, Seen};
    for wait in [true, false] {
        let settings = Settings {
            job: Job::Reporting,
            network: Span::millis(0, 0),
            provider: skein_fake_llm_domain::Config {
                latency_min: Duration::from_millis(100),
                latency_max: Duration::from_millis(100),
                ..Settings::calm(1192).provider
            },
            ..Settings::calm(1192)
        };
        let mut charter = charter(&settings, "@finish-crossing");
        charter.grants.wait = wait;
        let scripts = Box::new([Script {
            cue: b"@finish-crossing".as_slice().into(),
            turns: Box::new([
                calls(vec![call(b"finish", FINISH_INPUT)], 1),
                calls(vec![call(b"finish", FINISH_INPUT)], 1),
            ]),
        }]);
        let mut world = World::with_workspace_scripts_charter(settings, None, None, Checkout::new(), scripts, charter);
        world.message_at(
            Time::ZERO.saturating_add(Duration::from_millis(50)),
            Token::new(0),
            b"person".as_slice().into(),
            b"late work".as_slice().into(),
        );
        world.run(10_000);
        assert_eq!(world.prompts().len(), if wait { 2 } else { 1 });
        assert!(matches!(world.answer(), run::Answer::Accepted { .. }));
        if wait {
            let query = &world.prompts()[1];
            assert!(query.messages.iter().flat_map(|message| &message.parts).any(|part| matches!(part,
                Part::ToolOutput { output, is_error: false, .. } if output.as_ref() == b"Finish was not accepted because messages arrived. Read them before finishing.")));
            assert!(matches!(query.messages.last().expect("queued work").parts.as_ref(),
                [Part::Text { text }] if text.as_ref() == b"person: late work"));
            let mut referee = Referee::new(Meeting::new(0, settings.waiting));
            for (at, seen) in world.messages_seen() {
                referee.observe(*at, seen.clone(), &mut Vec::new());
            }
            assert_eq!(referee.verdict(), Verdict::Passed);
            assert!(world.messages_seen().iter().any(
                |(_, seen)| matches!(seen, Seen::Turn { number: 2, read: Some(name), .. } if *name == Token::new(0))
            ));
            assert!(
                world.messages_seen().iter().any(|(_, seen)| matches!(seen, Seen::Turn { number: 1, read: None, .. }))
            );
        } else {
            assert!(world.facts().iter().any(|fact| matches!(fact, smith_domain::Fact::Run {
                fact: run::facts::Fact { kind: run::facts::FactKind::MessageUnread { name, .. }, .. }, opening: _
            } if *name == Token::new(0))));
        }
    }
}

#[test]
fn a_wait_is_answered_with_its_message_in_one_request() {
    use skein_lib::{Duration, Time};
    for queued in [true, false] {
        let settings = Settings {
            job: Job::Reporting,
            network: Span::millis(0, 0),
            provider: skein_fake_llm_domain::Config {
                latency_min: Duration::from_millis(100),
                latency_max: Duration::from_millis(100),
                ..Settings::calm(1193).provider
            },
            ..Settings::calm(1193)
        };
        let scripts = Box::new([Script {
            cue: b"@deferred-wait".as_slice().into(),
            turns: Box::new([calls(vec![call(b"wait", b"{}")], 1), calls(vec![call(b"finish", FINISH_INPUT)], 1)]),
        }]);
        let mut world = World::with_workspace_scripts_charter(
            settings,
            None,
            None,
            Checkout::new(),
            scripts,
            charter(&settings, "@deferred-wait"),
        );
        world.message_at(
            Time::ZERO.saturating_add(Duration::from_millis(if queued { 50 } else { 250 })),
            Token::new(0),
            b"person".as_slice().into(),
            b"follow up".as_slice().into(),
        );
        world.run(10_000);
        assert!(matches!(world.answer(), run::Answer::Accepted { turns: 2, .. }));
        assert_eq!(world.prompts().len(), 2, "the wait adds no provider completion before the waking exchange");
        let request = &world.prompts()[1];
        assert!(request.messages.iter().flat_map(|message| &message.parts).any(|part| matches!(part,
            Part::ToolOutput { output, is_error: false, .. } if output.as_ref() == b"Messages arrived. Read them before continuing.")));
        assert!(matches!(request.messages.last().expect("waking offer").parts.as_ref(),
            [Part::Text { text }] if text.as_ref() == b"person: follow up"));
        assert_eq!(world.waiting().len(), usize::from(!queued));
        assert_eq!(
            world.turn_metadata().iter().map(|(_, read, _)| *read).collect::<Vec<_>>(),
            [None, Some(Token::new(0))]
        );
    }
}

fn parked_wait(settings: &Settings, scripts: Box<[Script]>, charter: run::Charter) -> World {
    let mut first = World::with_workspace_scripts_charter(*settings, None, None, Checkout::new(), scripts, charter);
    first.run(10_000);
    first
}

fn saved_wait(first: &World) -> (smith_domain::Transcript, Box<[u8]>) {
    let saved = smith_domain::Transcript {
        version: smith_domain::session::record::VERSION,
        endpoint: Some(llm::Endpoint(0)),
        dialect: 1,
        turns: first.turns().to_vec().into(),
    };
    let call = first.turns()[0]
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            llm::Block::ToolCall { id, name, .. } if name.as_ref() == b"wait" => Some(id.clone()),
            llm::Block::ToolCall { .. }
            | llm::Block::Text { .. }
            | llm::Block::Opaque { .. }
            | llm::Block::Refusal { .. }
            | llm::Block::ToolResult { .. } => None,
        })
        .expect("actual wait identity");
    (saved, call)
}

fn resume_on_message(
    settings: &Settings,
    scripts: Box<[Script]>,
    charter: run::Charter,
    saved: smith_domain::Transcript,
    message: run::Message,
) -> World {
    use skein_lib::{Duration, ReplyTo};
    let start = smith_domain::Event::Start {
        reply_to: ReplyTo::new(Token::new(1)),
        host_run: Token::new(1),
        identity: None,
        activation: 2,
        window: smith_domain::Window { turns: u32::MAX, bytes: u64::MAX },
        charter,
        workspace: None,
        transcript: Some(saved),
        answered: Box::default(),
        grants: Box::new([smith_domain::Grant {
            renewable: true,
            name: smith_domain::GrantName { account: 0, generation: 1 },
            valid: Duration::from_secs(7200),
        }]),
        messages: Box::new([message]),
    };
    World::with_workspace_scripts_start(*settings, Checkout::new(), scripts, start)
}

#[test]
fn a_wait_withheld_through_a_park_is_supplied_on_resuming() {
    use skein_lib::Duration;
    let settings = Settings {
        job: Job::Reporting,
        waiting: Duration::from_secs(1),
        network: Span::millis(0, 0),
        ..Settings::calm(1194)
    };
    let scripts = Box::new([Script {
        cue: b"@parked-wait".as_slice().into(),
        turns: Box::new([calls(vec![call(b"wait", b"{}")], 1), calls(vec![call(b"finish", FINISH_INPUT)], 1)]),
    }]);
    let first = parked_wait(&settings, scripts.clone(), charter(&settings, "@parked-wait"));
    assert!(matches!(first.answer(), run::Answer::Parked { turns: 1, .. }));
    assert_eq!(first.prompts().len(), 1);
    let (saved, call) = saved_wait(&first);
    assert!(
        !first.turns()[0]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|block| matches!(block, llm::Block::ToolResult { id, .. } if *id == call))
    );
    let resumed = Settings { resume: true, ..settings };
    let mut next = resume_on_message(
        &resumed,
        scripts,
        charter(&resumed, "@parked-wait"),
        saved,
        run::Message {
            finish_requested: false,
            name: Token::new(0),
            label: b"person".as_slice().into(),
            text: b"waking line".as_slice().into(),
        },
    );
    next.run(10_000);
    assert!(matches!(next.answer(), run::Answer::Accepted { turns: 1, .. }));
    assert_eq!(next.prompts().len(), 1);
    assert!(matches!(next.prompts()[0].messages.last().expect("waking result").parts.as_ref(),
        [Part::ToolOutput { id, output, is_error: false }] if *id == call && output.ends_with(b"person: waking line")));
    assert_eq!(next.turns()[0].sequence, 2);
    assert_eq!(next.turn_metadata()[0].1, Some(Token::new(0)));
    let mut referee =
        skein_world::domain::Referee::new(smith_agent_world::messages_referee::Meeting::new(1, settings.waiting));
    for (at, seen) in next.messages_seen() {
        referee.observe(*at, seen.clone(), &mut Vec::new());
    }
    assert_eq!(
        referee.verdict(),
        skein_world::domain::Verdict::Passed,
        "actual restored wait output crosses the offered-person fence"
    );
}

#[test]
fn a_parked_open_run_resumes_on_the_delegate_result_without_a_new_begin() {
    use skein_fake_llm_domain::api::{Message, Role};
    use skein_lib::Duration;
    const RESULT_MESSAGE: Token = Token::new(73);
    const REPLY: &[u8] = b"Task 2 landed at 4acedd81.";
    const REPLYING: &[u8] = b"## Replying\n\nEnd your turn with text to reply; a reply does not end the conversation. The run then waits as for `wait`, but with no call to answer: the next messages come as a user message, at once if any came while you worked. To wait without replying, call `wait` with no text.\n";
    let settings = Settings {
        job: Job::Reporting,
        waiting: Duration::from_secs(1),
        network: Span::millis(0, 0),
        ..Settings::calm(1195)
    };
    let scripts = Box::new([Script {
        cue: b"@parked-delegate-result".as_slice().into(),
        turns: Box::new([calls(vec![call(b"wait", b"{}")], 1), says(REPLY, 1)]),
    }]);
    let mut opening = charter(&settings, "@parked-delegate-result");
    opening.mode = run::Mode::Open;
    let first = parked_wait(&settings, scripts.clone(), opening);
    assert!(matches!(first.answer(), run::Answer::Parked { turns: 1, .. }));
    assert_eq!(first.prompts().len(), 1);
    assert!(matches!(first.prompts()[0].messages[0].parts.as_ref(),
        [Part::Text { text }] if text.as_ref() == SECTIONS.as_bytes()));
    let (saved, wait_id) = saved_wait(&first);
    let saved_messages: Vec<_> = saved
        .turns
        .iter()
        .flat_map(|turn| &turn.messages)
        .map(|message| Message {
            role: match message.role {
                llm::Role::User => Role::User,
                llm::Role::Assistant => Role::Assistant,
            },
            parts: message
                .content
                .iter()
                .map(|block| match block {
                    llm::Block::Text { text, .. } => Part::Text { text: text.clone() },
                    llm::Block::ToolCall { id, name, input, .. } => {
                        Part::ToolCall { id: id.clone(), name: name.clone(), arguments: input.clone() }
                    }
                    llm::Block::Opaque { .. } | llm::Block::Refusal { .. } | llm::Block::ToolResult { .. } => {
                        panic!("the actual first turn contains only its opening and unanswered wait")
                    }
                })
                .collect(),
        })
        .collect();
    let resumed = Settings { resume: true, ..settings };
    let mut opening = charter(&resumed, "@parked-delegate-result");
    opening.mode = run::Mode::Open;
    let mut next = resume_on_message(
        &resumed,
        scripts,
        opening,
        saved,
        run::Message {
            finish_requested: false,
            name: RESULT_MESSAGE,
            label: b"2".as_slice().into(),
            text: b"Result from task 2:\n4acedd81".as_slice().into(),
        },
    );
    next.run(10_000);
    assert_eq!(next.prompts().len(), 1, "one new completion answers the waking result");
    let request = &next.prompts()[0];
    assert_eq!(
        &request.messages[..saved_messages.len()],
        saved_messages.as_slice(),
        "saved messages remain an unchanged ordered prefix"
    );
    assert!(
        matches!(&request.messages[saved_messages.len()..],
        [Message { role: Role::User, parts }] if matches!(parts.as_ref(),
            [Part::ToolOutput { id, output, is_error: false }]
            if *id == wait_id && output.as_ref() == b"2: Result from task 2:\n4acedd81")),
        "the only new payload is exactly the delegate result: {:?}",
        &request.messages[saved_messages.len()..]
    );
    assert!(contains(&request.system, REPLYING));
    assert!(request.tools.iter().any(|tool| tool.name.as_ref() == b"wait"));
    assert!(matches!(next.answer(), run::Answer::Parked { turns: 1, .. }));
    assert_eq!(next.turns().len(), 1);
    assert_eq!(next.turns()[0].sequence, 2);
    assert!(
        next.turns()[0]
            .messages
            .iter()
            .flat_map(|message| &message.content)
            .any(|block| matches!(block, llm::Block::Text { text, .. } if text.as_ref() == REPLY))
    );
    assert_eq!(
        next.turn_metadata().iter().map(|(_, read, _)| *read).collect::<Vec<_>>(),
        [Some(RESULT_MESSAGE)],
        "the actual told turn reads the result exactly once"
    );
}
