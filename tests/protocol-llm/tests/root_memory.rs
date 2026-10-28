//! Attained V2 root ownership at public Start, Complete and concrete Turn seams.
//! Records come from actual shared Client terminals, including the Wait
//! result; fixture ownership is counted independently from production pricing.
//! Root entrances and caller-copy transients are measured separately. One whole
//! actual Client/adapter/peer lifecycle is measured while root and caller
//! histories remain live, including two physical Clients across one logical
//! callback: the won closing Client and its replacement with one active context.
//! Passive simulator, referee and composition bookkeeping are outside this
//! component ownership contract (skein-world/src/heap.rs).
//! Contract: scratch/client.md, sections 1, 4, 5 and 6;
//! domain/run.md, sections 3, 6, 13 and 14; domain/session.md, section 3;
//! programming-model.md, section 6.3; testing-strategy.md, sections 2.3 and 6.

use skein_fake_llm_domain::api::{Finish, Line, Script, Turn as ScriptTurn};
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};
use skein_llm_world::fake::ObservationLimits;
use skein_world::domain::heap::{Counting, Meter};
use smith_domain::{self as root, Domain, Event, Grant, GrantName, Limits, Request, llm, run, session};
use smith_protocol_llm_world::adapter::{self as adapter, Receiving, ToolKind, ToolSchema};
use smith_protocol_llm_world::{
    LIMITS,
    wire::{self, Configuration, Observed, Wire},
};

#[global_allocator]
static HEAP: Counting = Counting;

const WORKER: Token = Token::new(91);
const PARENT: Token = Token::new(92);
const LARGE: usize = 8192;
const ID: &[u8] = b"call_0000000000000001";
const SAID: &[u8] = b"tiny actual text";
const RESTORED: &[u8] = b"actual restored text";

fn size<T>() -> u64 {
    u64::try_from(size_of::<T>()).expect("fixture cell size fits")
}

fn len(value: &[u8]) -> u64 {
    u64::try_from(value.len()).expect("bounded fixture payload")
}

fn sum(values: impl IntoIterator<Item = u64>) -> u64 {
    values.into_iter().try_fold(0_u64, u64::checked_add).expect("checked fixture ownership sum")
}

fn cells<T>(count: usize) -> u64 {
    u64::try_from(count).expect("bounded fixture cells").checked_mul(size::<T>()).expect("checked fixture array")
}

fn endpoint_bytes(endpoint: &skein_llm::Endpoint) -> u64 {
    sum([
        len(&endpoint.authority),
        len(&endpoint.target),
        u64::try_from(core::mem::size_of_val(endpoint.headers.as_ref())).expect("public header array"),
        sum(endpoint.headers.iter().map(|header| sum([len(&header.name), len(&header.value)]))),
    ])
}

fn credential_bytes(credential: &skein_llm::Credential) -> u64 {
    sum([len(&credential.value), len(&credential.account_id)])
}

fn configuration_bytes(configuration: &Configuration) -> u64 {
    sum([
        endpoint_bytes(&configuration.endpoint),
        credential_bytes(&configuration.credential),
        len(&configuration.error_prefix),
        len(&configuration.continuation_arguments),
    ])
}

fn observation_limits(limits: &adapter::Limits) -> ObservationLimits {
    ObservationLimits {
        heads: 1,
        head_bytes: 4096,
        events: skein_llm::client::MAX_OUT.above,
        event_bytes: 32768,
        queries: 1,
        query_bytes: u64::from(limits.client.request),
        pending: 0,
        request: limits.client.http.request.checked_add(limits.client.request).expect("whole request tape"),
        response_bytes: 4096,
    }
}

fn bounds(messages: u32) -> Limits {
    let decoded_call_bytes = 32_768;
    let completion_bytes = adapter::completion_worst_case(&wire_limits().client, decoded_call_bytes)
        .expect("explicit native fixture receiving ownership")
        .max(131_072);
    let mut limits = Limits {
        accounts: 1,
        endpoints: 1,
        configured_model_bytes: 4096,
        decoded_call_bytes,
        run: run::Limits {
            runs: 1,
            conversations: 1,
            run_conversations: 1,
            calls: 1,
            run_bytes: 4096,
            brief_sections: 4,
            host_part: 1 << 20,
            room: 0,
            compaction_threshold: [4, 5],
            compaction_material: 0,
            schema_costs: run::SchemaCosts::NONE,
            directories: 1,
            directory_name_bytes: 64,
            host_path_bytes: 4096,
            conflicts: 64,
            conflict_path_bytes: 4096,
            host_tools: 0,
            host_input_bytes: 64,
            host_reply_bytes: 64,
            answered_calls: 16,
            answered_bytes: 4096,
            answer_bytes: 64,
            guide_bytes: 64,
            outcome_bytes: 256,
            check_tail: 64,
            facts: 256,
            nudges: messages,
            messages: 1,
            message_bytes: u32::try_from(LARGE).expect("bounded caller message"),
            ..LIMITS.run
        },
        session: session::Limits {
            sessions: 1,
            history_messages: messages,
            // Source generation has independent room for maximum provider and
            // delegate receiving credit plus context. Restore replaces this
            // allowance with the separately observed exact payload/credit cap.
            history_bytes: 1_048_576,
            completion_bytes,
            completion_blocks: 64,
            failure_bytes: wire_limits().client.detail_bytes,
            delegated_result_bytes: 262_144,
            parallel_tools: 1,
            facts: 256,
            tools: root::tools::Limits {
                shell_output: 0,
                kits: 1,
                calls: 1,
                repos: 1,
                path_bytes: 64,
                host_path_bytes: 4096,
                known_files: 1,
                file_bytes: 64,
                tool_payload: 64,
                read_bytes: 64,
                list_entries: 1,
                list_bytes: 4096,
                match_lines: 1,
                shell_head: 0,
                shell_tail: 0,
                search_hits: 1,
                search_bytes: 64,
                facts: 18,
                ..LIMITS.session.tools
            },
            ..LIMITS.session
        },
        ..LIMITS
    };
    limits.session.facts = session::max_facts(&limits.session);
    limits
}

fn workspace() -> run::Workspace {
    run::Workspace {
        directories: Box::new([run::Directory {
            host: Box::from(b"/host".as_slice()),
            name: b"work".as_slice().into(),
            root: Token::new(7),
            writable: false,
            git: true,
            conflicts: Box::new([]),
        }]),
    }
}

fn charter(restoring: bool) -> run::Charter {
    run::Charter {
        mode: run::Mode::Finite,
        reserve: run::charter::Reserve { turns: 0, time: skein_lib::Duration::ZERO },
        instructions: Box::new([]),
        brief: run::Brief {
            sections: Box::new([run::Section {
                title: b"Task".as_slice().into(),
                text: b"@root-memory".as_slice().into(),
            }]),
        },

        grants: run::charter::Grants {
            wait: true,
            deliver: None,
            tools: run::charter::Tools { inspect: false, modify: false, shell: false },
            agents: false,
            host_tools: Box::new([]),
        },
        outcome: run::outcome::OutcomeSpec {
            change: None,
            verdicts: Box::new([]),
            report: Some(run::outcome::TextSpec { max: 64, fields: Box::new([]) }),
            failure: None,
        },
        budget: LIMITS.run.budget,
        llm: run::charter::Llm {
            effort: None,
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            account: Some(0),
            endpoint: run::charter::Endpoint(0),
            model: b"fixture-model".as_slice().into(),
            window: 16_384,
            output: 128,
            dialect: 2,
        },
        models: Box::new([]),
        conventions: None,
        resume: restoring,
        waiting: Duration::from_secs(1),
    }
}

/// Independent owning boxes of this fixed public caller Start fixture. Charter
/// and Conventions wrappers are inline in Counted, not heap allocations.
fn workspace_bytes(workspace: Option<&run::Workspace>) -> u64 {
    workspace.map_or(0, |workspace| {
        sum([
            cells::<run::Directory>(workspace.directories.len()),
            sum(workspace.directories.iter().map(|directory| {
                sum([
                    len(&directory.name),
                    len(&directory.host),
                    cells::<Box<[u8]>>(directory.conflicts.len()),
                    sum(directory.conflicts.iter().map(|path| len(path))),
                ])
            })),
        ])
    })
}

fn charter_bytes(charter: &run::Charter) -> u64 {
    assert!(charter.grants.host_tools.is_empty() && charter.models.is_empty());
    assert!(charter.outcome.verdicts.is_empty());
    assert!(charter.outcome.change.as_ref().is_none_or(|spec| spec.fields.is_empty()));
    assert!(charter.outcome.report.as_ref().is_some_and(|spec| spec.fields.is_empty()));
    sum([
        len(&charter.instructions),
        cells::<run::Section>(charter.brief.sections.len()),
        sum(charter.brief.sections.iter().map(|section| sum([len(&section.title), len(&section.text)]))),
        len(&charter.llm.model),
        charter.conventions.as_ref().map_or(0, |conventions| sum([len(&conventions.guide), len(&conventions.checks)])),
    ])
}

fn scripts(cycles: u32) -> Box<[Script]> {
    let count = cycles.checked_mul(2).and_then(|turns| turns.checked_add(1)).expect("finite scripted turns");
    let mut turns = Vec::with_capacity(usize::try_from(count).expect("bounded scripted array"));
    for _ in 0..cycles {
        turns.push(ScriptTurn {
            lines: Box::new([Line::Call { name: b"wait".as_slice().into(), arguments: b"{}".as_slice().into() }]),
            finish: Finish::ToolCalls,
            tokens: 1,
        });
        turns.push(ScriptTurn { lines: Box::new([Line::Text { text: SAID.into() }]), finish: Finish::Stop, tokens: 1 });
    }
    turns.push(ScriptTurn { lines: Box::new([Line::Text { text: RESTORED.into() }]), finish: Finish::Stop, tokens: 1 });
    assert_eq!(turns.capacity(), usize::try_from(count).expect("exact caller array reservation"));
    Box::new([Script { cue: b"@root-memory".as_slice().into(), turns: turns.into() }])
}

fn wire_limits() -> adapter::Limits {
    let mut client = skein_llm_world::limits();
    client.http.request = 32_768;
    client.output_items = 64;
    client.request = 32_768;
    client.request = 32_768;
    client.strings = 16_384;
    client.tokens = 4096;
    client.answer = 256;
    adapter::Limits {
        client,
        tool_bytes: 16_384,
        history_bytes: u64::from(client.request),
        rendered_result: client.strings,
        stated: smith_protocol_llm_world::adapter::stated(),
    }
}

struct Complete {
    owner: Token,
    prompt: llm::Prompt,
    receiving: Receiving,
}

// Physical ownership survives the logical terminal under the scenario ceiling.
// Wire::take consumed the old Context at that terminal.
struct Retained {
    wire: Wire,
    owner: Token,
    held: u64,
}

fn native_peak(meter: &Meter, bound: u64) -> u64 {
    let measured = meter.end();
    assert!(
        measured.peak() <= bound,
        "combined root/caller/overlapping-native entrance peak {} exceeds {bound}",
        measured.peak()
    );
    measured.held()
}

fn attributed(held: u64, before: u64, after: u64, received: u64, handed: u64) -> u64 {
    sum([held, after, received])
        .checked_sub(sum([before, handed]))
        .expect("native attribution includes only its measured net and exact public handoffs")
}

fn retire(retained: Retained, meter: &Meter, bound: u64, now: Time, wall: Wall) {
    let Retained { mut wire, owner, held, .. } = retained;
    let before = meter.held();
    assert_eq!(wire.observed, [Observed::Completed(owner), Observed::Reusable, Observed::Close]);
    assert_eq!(wire.peer.machine.waiting(), skein_llm::client::Waiting::Closing);
    for _ in 0..2 {
        meter.start();
        wire.peer.at(now, wall);
        assert!(wire.settle().is_empty(), "old physical settlement owes no second root callback");
        native_peak(meter, bound);
    }
    assert_eq!(wire.observed, [Observed::Completed(owner), Observed::Reusable, Observed::Close, Observed::Closed]);
    assert_eq!(wire.observed.capacity(), 4);
    let mut closed_quiet = false;
    for _ in 0..128 {
        meter.start();
        wire.peer.at(now, wall);
        let (progress, returned) = wire.tick();
        assert!(returned.is_empty(), "settled native binding owes no second callback");
        drop(returned);
        native_peak(meter, bound);
        if !progress {
            closed_quiet = true;
            break;
        }
    }
    assert!(closed_quiet, "Closed drains remaining bounded peer work");
    assert_eq!(wire.peer.machine.waiting(), skein_llm::client::Waiting::Nothing);
    assert_eq!(wire.peer.service.calls(), 0, "retired native provider routes reclaimed before drop");
    assert_eq!(wire.observed, [Observed::Completed(owner), Observed::Reusable, Observed::Close, Observed::Closed]);
    meter.start();
    drop(wire);
    let after = native_peak(meter, bound);
    assert_eq!(
        before.checked_sub(after),
        Some(held),
        "physical Closed/drain/drop releases exactly its captured native heap without root or caller mutations"
    );
}

fn actual(
    complete: Complete,
    configuration: &Configuration,
    cycles: u32,
    native: &Env<adapter::Limits>,
    meter: &Meter,
    bound: u64,
    old: &mut Option<Retained>,
) -> (Event, Retained, bool) {
    let now = native.now;
    let wall = native.wall;
    let limits = &native.limits;
    let owner = complete.owner;
    let messages = complete.prompt.messages.len();
    let incoming = prompt_bytes(&complete.prompt);
    let large = complete.prompt.messages.iter().flat_map(|message| &message.content).any(|block| {
        matches!(block, llm::Block::Text { text, .. } if text.len() == LARGE && text.iter().all(|byte| *byte == b'x'))
    });
    let observations = observation_limits(limits);
    let before = meter.held();
    meter.start();
    let mut wire = Wire::prepare(owner, complete.prompt, complete.receiving, configuration, limits, scripts(cycles))
        .expect("actual root receiving contract admits the Client");
    wire.peer.observe(observations);
    wire.observed.reserve_exact(4);
    wire.peer.at(now, wall);
    assert!(wire.start().is_empty());
    let after = native_peak(meter, bound);
    let mut held = attributed(0, before, after, incoming, 0);
    let overlapping = old.is_some();
    if let Some(previous) = old.take() {
        assert_eq!(previous.owner, owner, "new actual Client reuses the same live logical callback");
        assert_eq!(
            previous.wire.observed,
            [Observed::Completed(owner), Observed::Reusable, Observed::Close],
            "old Completed/Reusable/Close precedes new Start and old Closed"
        );
        assert_eq!(previous.wire.peer.machine.waiting(), skein_llm::client::Waiting::Closing);
        assert!(wire.observed.is_empty(), "new Start has no terminal while the old won Client still closes");
        assert!(
            !matches!(
                wire.peer.machine.waiting(),
                skein_llm::client::Waiting::Start
                    | skein_llm::client::Waiting::Idle
                    | skein_llm::client::Waiting::Closing
                    | skein_llm::client::Waiting::Nothing
            ),
            "new actual Client is active before old Closed"
        );
        // The new Wire and root/caller owners remain untouched throughout this
        // separate physical retirement measurement.
        retire(previous, meter, bound, now, wall);
    }
    let mut terminal = None;
    let mut quiet = false;
    for _ in 0..100_000 {
        let before = meter.held();
        meter.start();
        wire.peer.at(now, wall);
        let (progress, returned) = wire.tick();
        assert!(returned.capacity() <= 4, "one actual terminal's public vector reservation");
        let mut handed = 0;
        for event in returned {
            handed = terminal_bytes(&event);
            assert!(terminal.replace(event).is_none(), "one native terminal");
        }
        let after = native_peak(meter, bound);
        held = attributed(held, before, after, 0, handed);
        if !progress {
            quiet = true;
            break;
        }
    }
    assert!(quiet, "actual byte peer reached bounded drainage");
    let terminal = terminal.expect("bounded actual byte peer completed");
    assert!(matches!(&terminal, Event::Completed { owner: got, .. } if *got == owner));
    assert_eq!(wire.observed, [Observed::Completed(owner), Observed::Reusable]);
    assert_native_query(&wire, messages, large);
    let before = meter.held();
    meter.start();
    wire.peer.at(now, wall);
    assert!(wire.close().is_empty());
    let after = native_peak(meter, bound);
    held = attributed(held, before, after, 0, 0);
    assert_eq!(wire.observed, [Observed::Completed(owner), Observed::Reusable, Observed::Close]);
    assert_eq!(wire.peer.machine.waiting(), skein_llm::client::Waiting::Closing);
    (terminal, Retained { wire, owner, held }, overlapping)
}

// These passive public observations allocate no scratch inside the measured
// native lifecycle. The shared peer's focused control independently validates
// raw response chunk payloads with the shared HTTP reference reader.
fn assert_native_query(wire: &Wire, messages: usize, large: bool) {
    let [query] = wire.peer.queries.as_slice() else { panic!("one actual native query") };
    assert_eq!(query.model.as_ref(), b"fixture-model");
    assert_eq!(query.messages.len(), messages, "actual whole alternating history reached the native peer");
    assert!(query.system.windows(b"@root-memory".len()).any(|bytes| bytes == b"@root-memory"));
    assert_eq!(wire.peer.service.count(), 1);
    assert_eq!(wire.peer.service.calls(), 0);
    assert!(!wire.peer.requests.is_empty());
    assert!(!wire.peer.responses.is_empty());
    assert!(wire.peer.responses.starts_with(b"HTTP/1.1 200 OK\r\n"));
    if large {
        assert!(query.messages.iter().flat_map(|message| &message.parts).any(|part| {
            matches!(part, skein_fake_llm_domain::api::Part::Text { text } if text.len() == LARGE && text.iter().all(|byte| *byte == b'x'))
        }));
        assert!(wire.peer.requests.windows(LARGE).any(|bytes| bytes.iter().all(|byte| *byte == b'x')));
    }
}

fn terminal_bytes(event: &Event) -> u64 {
    let Event::Completed { completion, .. } = event else { panic!("actual native successful terminal") };
    let [said] = completion.content.as_ref() else { panic!("one actual scripted assistant block") };
    let payload = match said {
        llm::Said::Text { text, replay } => {
            assert!(text.as_ref() == SAID || text.as_ref() == RESTORED);
            assert_eq!(completion.stop, llm::Stop::EndTurn);
            sum([len(text), replay.as_ref().map_or(0, |value| len(&value.bytes))])
        }
        llm::Said::ToolCall { id, name, input, call: llm::Decoded::Served { ask: run::Ask::Wait }, replay } => {
            assert_eq!(id.as_ref(), ID);
            assert_eq!(name.as_ref(), b"wait");
            assert_eq!(input.as_ref(), b"{}");
            assert_eq!(completion.stop, llm::Stop::ToolUse);
            sum([len(id), len(name), len(input), replay.as_ref().map_or(0, |value| len(&value.bytes))])
        }
        llm::Said::Refusal { .. } | llm::Said::Opaque { .. } | llm::Said::ToolCall { .. } => {
            panic!("only actual literal Text and decoded Wait terminals belong to this measured fixture")
        }
    };
    sum([size::<llm::Said>(), payload])
}

fn schemas_bytes(schemas: &[ToolSchema]) -> u64 {
    assert_eq!(schemas.len(), 2, "actual fixture offers only Finish and Wait");
    assert!(schemas.iter().any(|schema| schema.kind == ToolKind::Finish && schema.name.as_ref() == b"finish"));
    assert!(schemas.iter().any(|schema| schema.kind == ToolKind::Wait && schema.name.as_ref() == b"wait"));
    sum([
        cells::<ToolSchema>(schemas.len()),
        sum(schemas.iter().map(|schema| sum([len(&schema.name), len(&schema.description), len(&schema.schema)]))),
    ])
}

// The fixture ledger counts actual public cells and owning byte fields. It
// neither calls owned_bytes/content_cost nor recreates admission or rendering.
fn record_blocks(blocks: &[session::llm::Block]) -> u64 {
    let mut bytes = u64::try_from(blocks.len()).expect("bounded blocks") * size::<session::llm::Block>();
    for block in blocks {
        bytes += match block {
            session::llm::Block::Text { text, replay } => {
                len(text) + replay.as_ref().map_or(0, |value| len(&value.bytes))
            }
            session::llm::Block::ToolCall { id, name, input, call: session::llm::Decoded::Historical, replay } => {
                len(id) + len(name) + len(input) + replay.as_ref().map_or(0, |value| len(&value.bytes))
            }
            session::llm::Block::ToolResult { id, result: session::llm::Returned::Text { text, replay, .. } } => {
                len(id) + len(text) + replay.as_ref().map_or(0, |value| len(&value.bytes))
            }
            session::llm::Block::Refusal { .. }
            | session::llm::Block::Opaque { .. }
            | session::llm::Block::ToolCall { .. }
            | session::llm::Block::ToolResult { .. } => {
                panic!("generated fixture has only literal text, historical Wait and concrete feedback")
            }
        };
    }
    bytes
}

fn messages_bytes(messages: &[session::llm::Message]) -> u64 {
    u64::try_from(messages.len()).expect("bounded messages") * size::<session::llm::Message>()
        + messages.iter().map(|message| record_blocks(&message.content)).sum::<u64>()
}

fn turn_bytes(turn: &session::record::Turn) -> u64 {
    use session::record::{Backend, Kind, Opener};
    let opener = match &turn.kind {
        Kind::First(opener) | Kind::Window(opener) => {
            size::<Opener>()
                + match &opener.backend {
                    Backend::Direct => 0,
                    Backend::Program { name } => len(name),
                }
        }
        Kind::Ordinary => 0,
    };
    opener + messages_bytes(&turn.messages) + len(&turn.model) + turn.effort.as_ref().map_or(0, |effort| len(effort))
}

fn transcript_bytes(history: &root::Transcript) -> u64 {
    u64::try_from(history.turns.len()).expect("bounded turns") * size::<session::record::Turn>()
        + history.turns.iter().map(turn_bytes).sum::<u64>()
}

fn prompt_bytes(prompt: &llm::Prompt) -> u64 {
    let mut bytes = len(&prompt.model)
        + len(&prompt.system)
        + u64::try_from(prompt.served.len()).expect("bounded descriptors") * size::<llm::Served>()
        + u64::try_from(prompt.messages.len()).expect("bounded messages") * size::<llm::Message>();
    bytes += match &prompt.namespace {
        root::tools::Namespace::NoWorkspace | root::tools::Namespace::One => 0,
        root::tools::Namespace::Several { working } => len(working.as_bytes()),
    };
    for served in &prompt.served {
        assert!(matches!(served, llm::Served::Finish | llm::Served::Wait));
    }
    for message in &prompt.messages {
        bytes += u64::try_from(message.content.len()).expect("bounded blocks") * size::<llm::Block>();
        for block in &message.content {
            bytes += match block {
                llm::Block::Text { text, replay } => len(text) + replay.as_ref().map_or(0, |value| len(&value.bytes)),
                llm::Block::ToolCall { id, name, input, replay } => {
                    len(id) + len(name) + len(input) + replay.as_ref().map_or(0, |value| len(&value.bytes))
                }
                llm::Block::ToolResult { id, result: llm::Returned::Text { text, replay, .. } } => {
                    len(id) + len(text) + replay.as_ref().map_or(0, |value| len(&value.bytes))
                }
                llm::Block::Refusal { .. } | llm::Block::Opaque { .. } | llm::Block::ToolResult { .. } => {
                    panic!("literal supported root fixture fields")
                }
            };
        }
    }
    bytes
}

// Prompt deliberately has no Clone; the outside caller copies only the whole
// observed fixture values. This does not translate records or classify calls.
fn copy_prompt(prompt: &llm::Prompt) -> llm::Prompt {
    let expected = prompt_bytes(prompt);
    let copy_meter = Meter::new();
    copy_meter.start();
    let messages = prompt
        .messages
        .iter()
        .map(|message| llm::Message {
            role: message.role,
            content: message
                .content
                .iter()
                .map(|block| match block {
                    llm::Block::Text { text, replay } => {
                        llm::Block::Text { text: text.clone(), replay: replay.clone() }
                    }
                    llm::Block::ToolCall { id, name, input, replay } => llm::Block::ToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        input: input.clone(),
                        replay: replay.clone(),
                    },
                    llm::Block::ToolResult { id, result: llm::Returned::Text { text, error, replay } } => {
                        llm::Block::ToolResult {
                            id: id.clone(),
                            result: llm::Returned::Text { text: text.clone(), error: *error, replay: replay.clone() },
                        }
                    }
                    llm::Block::Refusal { .. } | llm::Block::Opaque { .. } | llm::Block::ToolResult { .. } => {
                        panic!("outside copies only the observed supported fixture fields")
                    }
                })
                .collect(),
        })
        .collect();
    let copied = llm::Prompt {
        effort: None,
        purpose: prompt.purpose,
        choice: prompt.choice.clone(),
        namespace: prompt.namespace.clone(),
        affinity: prompt.affinity,
        asker: prompt.asker,
        endpoint: prompt.endpoint,
        model: prompt.model.clone(),
        system: prompt.system.clone(),
        tools: prompt.tools,
        served: prompt.served.clone(),
        messages,
        max_tokens: prompt.max_tokens,
    };
    let measured = copy_meter.end();
    assert_eq!(measured.held(), expected, "whole prompt arrays and payload/replay fields are owned independently");
    assert!(measured.peak() <= expected, "caller prompt copy has no unpriced construction transient");
    copied
}

fn copy_transcript(history: &root::Transcript) -> root::Transcript {
    let expected = transcript_bytes(history);
    let copy_meter = Meter::new();
    copy_meter.start();
    let copied = history.clone();
    let measured = copy_meter.end();
    assert_eq!(measured.held(), expected, "copied Turn/Message/Block arrays and payloads are independently owned");
    assert!(measured.peak() <= expected, "caller history copy has no unpriced construction transient");
    copied
}

fn copy_turn(turn: &session::record::Turn) -> session::record::Turn {
    let expected = turn_bytes(turn);
    let copy_meter = Meter::new();
    copy_meter.start();
    let copied = turn.clone();
    let measured = copy_meter.end();
    assert_eq!(measured.held(), expected, "copied concrete Turn fields are independently owned");
    assert!(measured.peak() <= expected, "caller Turn copy has no unpriced construction transient");
    copied
}

fn assert_rewritten(history: &root::Transcript, prompt: &llm::Prompt) {
    let expected = history.turns.iter().flat_map(|turn| &turn.messages);
    for (record, message) in expected.zip(&prompt.messages) {
        assert_eq!(message.role, record.role);
        assert_eq!(message.content.len(), record.content.len());
        for (original, block) in record.content.iter().zip(&message.content) {
            match (original, block) {
                (session::llm::Block::Text { text: original, replay: envelope }, llm::Block::Text { text, replay }) => {
                    assert_eq!(text, original);
                    assert_eq!(replay, envelope);
                }
                (
                    session::llm::Block::ToolCall {
                        id: original_id,
                        name: original_name,
                        input: original_input,
                        replay: envelope,
                        ..
                    },
                    llm::Block::ToolCall { id, name, input, replay },
                ) => {
                    assert_eq!(id, original_id);
                    assert_eq!(name, original_name);
                    assert_eq!(input, original_input);
                    assert_eq!(replay, envelope);
                }
                (
                    session::llm::Block::ToolResult {
                        id: original_id,
                        result: session::llm::Returned::Text { text: original, error: original_error, replay: envelope },
                    },
                    llm::Block::ToolResult { id, result: llm::Returned::Text { text, error, replay } },
                ) => {
                    assert_eq!(id, original_id);
                    assert_eq!(text, original);
                    assert_eq!(error, original_error);
                    assert_eq!(replay, envelope);
                }
                _ => panic!("actual root rewrite preserves the independent observed fields"),
            }
        }
    }
    let waking = prompt.messages.last().expect("actual waking User");
    assert_eq!(waking.role, llm::Role::User);
    let [llm::Block::Text { text, replay: None }] = waking.content.as_ref() else { panic!("waking text") };
    assert_eq!(text.as_ref(), b"Continue the saved conversation.");
}

struct Counted {
    meter: Meter,
    ceiling: u64,
    domain: Option<Domain>,
    env: Env<Limits>,
    out: Queue<Request>,
    configuration_bytes: u64,
    caller_charter: Option<run::Charter>,
    caller_workspace: Option<run::Workspace>,
    native_limits: adapter::Limits,
    records: Vec<session::record::Turn>,
    prefix: Option<root::Transcript>,
    saved: Option<root::Transcript>,
    initial: Option<llm::Prompt>,
    held_prompt: Option<llm::Prompt>,
    prompt_copy: Option<llm::Prompt>,
    turn_copy: Option<session::record::Turn>,
    complete: Option<Complete>,
    admitted: Option<Token>,
    read: Option<Token>,
    probe: Option<Token>,
    waiting: bool,
    answer: Option<run::Answer>,
    calls: u32,
    long_open: u32,
    retained: Option<Retained>,
    overlaps: u32,
    restored_overlap: bool,
}

impl Counted {
    fn new(limits: &Limits, ceiling: u64) -> Self {
        Self::with_window(limits, 16_384, ceiling)
    }

    fn with_window(limits: &Limits, window: u32, ceiling: u64) -> Self {
        let meter = Meter::new();
        meter.start();
        let out = Queue::with_capacity(root::max_out(limits));
        let domain = Domain::new(
            limits,
            root::Config {
                programs: Box::new([]),
                endpoints: Box::new([run::charter::Endpoint(0)]),
                models: Box::new([smith_domain::ConfiguredModel {
                    efforts: Box::new([]),
                    prices: smith_domain::run::Prices {
                        input: 1,
                        cached: 1,
                        cache_write: None,
                        output: 1,
                        unit: 1_000_000,
                    },
                    protocol_allowance: 0,
                    endpoint: smith_domain::run::charter::Endpoint(0),
                    model: Box::from(&b"fixture-model"[..]),
                    window,
                    output: 4096,
                    head: Duration::from_secs(60),
                    idle: Duration::from_secs(30),
                }]),
            },
            41,
        );
        let measured = meter.end();
        meter.check(measured, ceiling, limits);
        let mut native_limits = wire_limits();
        native_limits.stated = adapter::Stated::from_tools(&limits.session.tools, native_limits.client.input)
            .expect("actual root numeric quantities fit");
        Self {
            meter,
            ceiling,
            domain: Some(domain),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits },
            out,
            configuration_bytes: 0,
            caller_charter: None,
            caller_workspace: None,
            native_limits,
            records: Vec::new(),
            prefix: None,
            saved: None,
            initial: None,
            held_prompt: None,
            prompt_copy: None,
            turn_copy: None,
            complete: None,
            admitted: None,
            read: None,
            probe: None,
            waiting: false,
            answer: None,
            calls: 0,
            long_open: 0,
            retained: None,
            overlaps: 0,
            restored_overlap: false,
        }
    }

    fn configuration(&mut self) -> Configuration {
        let before = self.meter.held();
        self.meter.start();
        let configurations = wire::configurations();
        assert_eq!(configurations.len(), 2, "finite public native fixture configuration array");
        let constructor =
            sum([cells::<Configuration>(configurations.len()), sum(configurations.iter().map(configuration_bytes))]);
        let constructed = self.meter.end();
        assert_eq!(
            constructed.held(),
            sum([before, constructor]),
            "all configuration fields and outer wrappers owned independently"
        );
        assert!(
            constructed.peak() <= sum([before, constructor]),
            "configuration construction has no unpriced transient"
        );
        self.meter.start();
        let configuration = configurations.into_vec().pop().expect("actual native Anthropic fixture");
        self.configuration_bytes = configuration_bytes(&configuration);
        let selected = self.meter.end();
        assert!(selected.peak() <= sum([before, constructor]));
        assert_eq!(
            selected.held(),
            sum([before, self.configuration_bytes]),
            "selected caller configuration remains owned after dropping the other fixture"
        );
        configuration
    }

    fn retire_native(&mut self) {
        if let Some(retained) = self.retained.take() {
            retire(retained, &self.meter, self.ceiling, self.env.now, self.env.wall);
        }
    }

    fn drain(&mut self, measured: skein_world::domain::heap::Measured) {
        while let Some(request) = self.out.pop() {
            match request {
                Request::Admitted { host_run, run } => {
                    assert_eq!(host_run, WORKER);
                    assert!(self.admitted.replace(run).is_none());
                }
                Request::Read { owner, at, .. } => {
                    if let Some(caller) = &self.caller_charter {
                        let selected = caller.conventions.as_ref().expect("maximum path Start");
                        assert_eq!(at.path.as_ref(), selected.guide.as_ref());
                        assert_eq!(at.path.len(), run::Conventions::PATH_CAPACITY);
                    }
                    assert!(self.read.replace(owner).is_none());
                }
                Request::Probe { owner, at, .. } => {
                    if let Some(caller) = &self.caller_charter {
                        let selected = caller.conventions.as_ref().expect("maximum path Start");
                        assert_eq!(at.path.as_ref(), selected.checks.as_ref());
                        assert_eq!(at.path.len(), run::Conventions::PATH_CAPACITY);
                        assert_eq!(at.root, Token::new(7), "only the writable mount is probed");
                    }
                    assert!(self.probe.replace(owner).is_none());
                }
                Request::Complete {
                    owner,
                    prompt,
                    max_completion_bytes,
                    max_completion_blocks,
                    decoded_call_bytes,
                    max_failure_bytes,
                    ..
                } => {
                    self.calls += 1;
                    assert!(
                        self.complete
                            .replace(Complete {
                                owner,
                                prompt,
                                receiving: Receiving {
                                    max_completion_bytes,
                                    max_completion_blocks,
                                    decoded_call_bytes,
                                    max_failure_bytes,
                                }
                            })
                            .is_none()
                    );
                }
                Request::Turn { host_run, number, turn, .. } => {
                    assert_eq!(host_run, WORKER);
                    assert_eq!(number, u32::try_from(self.records.len()).expect("bounded records") + 1);
                    self.records.push(turn);
                }
                Request::Waiting { host_run, .. } => {
                    assert_eq!(host_run, WORKER);
                    self.waiting = true;
                }
                Request::Answer { to, answer, read: _, detail: _ } => {
                    assert_eq!(to.into_token(), PARENT);
                    assert_eq!(self.long_open, 0, "provider work ended before the original Start terminal");
                    assert!(self.answer.replace(answer).is_none(), "one original Start terminal");
                }
                Request::Long { host_run, operation, .. } => {
                    assert_eq!(host_run, WORKER);
                    assert_eq!(operation, root::Operation::Completion, "this fixture only issues provider work");
                    self.long_open = self.long_open.checked_add(1).expect("bounded actual provider spans");
                    assert_eq!(self.long_open, 1, "one actual issued provider call per session");
                }
                Request::LongDone { host_run } => {
                    assert_eq!(host_run, WORKER);
                    self.long_open = self.long_open.checked_sub(1).expect("terminal belongs to actual provider work");
                }
                Request::Yield { .. } => {}
                Request::Cancel { owner } => {
                    let complete = self.complete.as_ref().expect("pending compaction is cancelled before native work");
                    assert_eq!(complete.owner, owner);
                    assert!(matches!(complete.prompt.purpose, llm::Purpose::Compaction { .. }));
                }
                Request::HostCall { .. }
                | Request::WithdrawHost { .. }
                | Request::Deliver { .. }
                | Request::Unauthorized { .. }
                | Request::Refused { .. }
                | Request::Limited { .. }
                | Request::MessageRefused { .. }
                | Request::Io { .. }
                | Request::Job { .. }
                | Request::CancelIo { .. }
                | Request::Check { .. }
                | Request::Abort { .. } => {
                    panic!("this positive Wait/text fixture owes only discovery, provider and parent observations")
                }
            }
        }
        self.meter.check(measured, self.ceiling, &self.env.limits);
    }

    fn step(&mut self, event: Event) {
        self.meter.start();
        let domain = self.domain.as_mut().expect("live root");
        domain.reclaim();
        root::step(domain, &self.env, event, &mut self.out);
        while domain.pop_fact().is_some() {}
        while domain.pop_content().is_some() {}
        let measured = self.meter.end();
        self.drain(measured);
    }

    fn resume(&mut self) {
        self.meter.start();
        let domain = self.domain.as_mut().expect("live root");
        domain.reclaim();
        root::resume(domain, &self.env, &mut self.out);
        while domain.pop_fact().is_some() {}
        while domain.pop_content().is_some() {}
        let measured = self.meter.end();
        self.drain(measured);
    }

    fn start(&mut self, history: Option<root::Transcript>, restoring: bool) {
        self.step(Event::Start {
            messages: Box::default(),
            answered: Box::default(),
            workspace: Some(workspace()),
            reply_to: ReplyTo::new(PARENT),
            host_run: WORKER,
            identity: None,
            activation: if restoring { 2 } else { 1 },
            window: root::Window { turns: u32::MAX, bytes: u64::MAX },
            charter: charter(restoring),
            transcript: history,
            grants: Box::new([Grant {
                renewable: true,
                name: GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(3600),
            }]),
        });
        assert!(self.admitted.is_some() && self.read.is_some());
        assert_eq!(self.calls, 0, "selected Start ownership survives outstanding discovery");
        assert!(self.complete.is_none());
        assert_eq!(self.long_open, 0, "actual provider work settled before outside waiting");
    }

    fn discover(&mut self) {
        let owner = self.read.take().expect("actual root discovery request");
        self.step(Event::Read { owner, read: run::Read::Missing });
        if let Some(owner) = self.probe.take() {
            self.step(Event::Probed { owner, executable: false });
        }
    }

    fn cycle(&mut self, configuration: &Configuration, cycles: u32) {
        for _ in 0..100 {
            if let Some(complete) = self.complete.take() {
                if self.initial.is_none() {
                    self.initial = Some(copy_prompt(&complete.prompt));
                }
                if self.held_prompt.is_some() && self.turn_copy.is_none() {
                    self.turn_copy = self.records.last().map(copy_turn);
                }
                let event = self.native(complete, configuration, cycles);
                self.step(event);
            } else {
                self.resume();
            }
            if self.complete.is_none() && (self.waiting || self.answer.is_some()) {
                break;
            }
        }
        assert!(self.complete.is_none());
        assert!(!self.domain.as_ref().expect("live root").is_ready());
        assert!(self.waiting || self.answer.is_some(), "bounded root loop reached an outside wait or actual terminal");
    }

    fn until_compaction(&mut self, configuration: &Configuration, cycles: u32) {
        let complete = self.complete.take().expect("actual restored ordinary request");
        assert!(matches!(complete.prompt.purpose, llm::Purpose::Ordinary { guard: Some(_) }));
        let event = self.native(complete, configuration, cycles);
        self.step(event);
        for _ in 0..100 {
            if self.complete.is_some() {
                break;
            }
            self.resume();
        }
        let complete = self.complete.as_ref().expect("the next request is the compaction");
        assert!(matches!(complete.prompt.purpose, llm::Purpose::Compaction { .. }));
        assert!(self.answer.is_none());
        assert!(!self.domain.as_ref().expect("live root").is_ready());
    }

    fn wake(&mut self, name: Token, text: Box<[u8]>) {
        self.waiting = false;
        let run = self.admitted.expect("actual admitted handle");
        self.step(Event::Message { finish_requested: false, label: b"host".as_slice().into(), run, name, text });
    }

    fn schema_constructor(&self, prompt: &llm::Prompt) -> u64 {
        const OWN_DECLARATION_SLOTS: usize = 11;
        let before = self.meter.held();
        self.meter.start();
        let schemas = wire::schemas(prompt, &self.native_limits.stated);
        let owned = schemas_bytes(&schemas);
        // The production inventory uses an eleven-cell bounded List before the
        // final boxed slice. Price both arrays if reboxing overlaps them.
        let constructor = sum([owned, cells::<ToolSchema>(OWN_DECLARATION_SLOTS)]);
        let built = self.meter.end();
        assert_eq!(
            built.held(),
            sum([before, owned]),
            "public caller schema boxes own exactly their arrays and fields"
        );
        assert!(
            built.peak() <= sum([before, constructor]),
            "caller schema construction fits its independently priced arrays"
        );
        drop(schemas);
        assert_eq!(self.meter.held(), before, "schema preflight reclaims every temporary owner before the native span");
        constructor
    }

    fn native(&mut self, complete: Complete, configuration: &Configuration, cycles: u32) -> Event {
        let incoming = prompt_bytes(&complete.prompt);
        self.schema_constructor(&complete.prompt);
        let limits = self.native_limits;
        let terminal_cap = complete.receiving.max_completion_bytes;
        let old_held = self.retained.as_ref().map_or(0, |retained| retained.held);
        let before = self.meter.held();
        let native = Env { now: self.env.now, wall: self.env.wall, limits };
        let (event, retained, overlapping) =
            actual(complete, configuration, cycles, &native, &self.meter, self.ceiling, &mut self.retained);
        if overlapping {
            self.overlaps += 1;
            if self.prefix.is_some()
                && self.saved.is_some()
                && self.held_prompt.is_some()
                && self.prompt_copy.is_some()
                && self.turn_copy.is_some()
            {
                self.restored_overlap = true;
            }
        }
        let terminal = terminal_bytes(&event);
        assert!(terminal <= terminal_cap, "actual transferred terminal fits the root's advertised caller reserve");
        let expected = before
            .checked_sub(sum([incoming, old_held]))
            .and_then(|held| held.checked_add(sum([terminal, retained.held])))
            .expect("native ownership transfer is bounded");
        assert_eq!(
            self.meter.held(),
            expected,
            "native public Prompt/terminal handoffs and old physical retirement retain exactly the new closing Wire"
        );
        assert!(self.retained.replace(retained).is_none(), "old physical ownership retired before new retention");
        event
    }

    fn replace_root(&mut self, limits: &Limits) {
        self.retire_native();
        assert!(self.answer.is_some(), "previous original Start settled");
        self.meter.start();
        drop(self.domain.take());
        let measured = self.meter.end();
        self.meter.check(measured, self.ceiling, &self.env.limits);
        self.env.limits = *limits;
        self.answer = None;
        self.admitted = None;
        self.read = None;
        self.probe = None;
        self.waiting = false;
        self.calls = 0;
        self.records.clear();
        self.meter.start();
        self.domain = Some(Domain::new(
            limits,
            root::Config {
                programs: Box::new([]),
                endpoints: Box::new([run::charter::Endpoint(0)]),
                models: Box::new([smith_domain::ConfiguredModel {
                    efforts: Box::new([]),
                    prices: smith_domain::run::Prices {
                        input: 1,
                        cached: 1,
                        cache_write: None,
                        output: 1,
                        unit: 1_000_000,
                    },
                    protocol_allowance: 0,
                    endpoint: smith_domain::run::charter::Endpoint(0),
                    model: Box::from(&b"fixture-model"[..]),
                    window: 16_384,
                    output: 4096,
                    head: Duration::from_secs(60),
                    idle: Duration::from_secs(30),
                }]),
            },
            42,
        ));
        let measured = self.meter.end();
        self.drain(measured);
    }
}

fn assert_generated(history: &root::Transcript, messages: u32) {
    assert_eq!(history.turns.len(), usize::try_from(2 * ((messages - 4) / 5)).expect("reachable Turns"));
    let mut previous = None;
    let mut count = 0_u32;
    let mut large = 0;
    for (index, turn) in history.turns.iter().enumerate() {
        assert_eq!(turn.sequence, u32::try_from(index).expect("bounded sequence") + 1);
        assert_eq!(turn.version, session::record::VERSION);
        assert_eq!(turn.endpoint, Some(session::llm::Endpoint(0)));
        assert_eq!(turn.dialect, 2);
        assert_eq!(turn.messages.len(), if index % 2 == 0 { 3 } else { 2 });
        assert_eq!(turn.messages.iter().filter(|message| message.role == session::llm::Role::Assistant).count(), 1);
        for message in &turn.messages {
            if message.role == session::llm::Role::Assistant {
                assert_eq!(previous, Some(session::llm::Role::User), "whole-history predecessor, not per-Turn minimum");
            } else if previous.is_none() {
                assert_eq!(message.role, session::llm::Role::User);
            }
            previous = Some(message.role);
            count += 1;
            for block in &message.content {
                match block {
                    session::llm::Block::Text { text, .. } => {
                        if text.len() == LARGE {
                            assert_eq!(&text[..6], b"host: ");
                            assert!(text[6..].iter().all(|byte| *byte == b'x'));
                            large += 1;
                        }
                    }
                    session::llm::Block::ToolCall { id, name, input, call, .. } => {
                        assert_eq!(id.as_ref(), ID);
                        assert_eq!(name.as_ref(), b"wait");
                        assert_eq!(input.as_ref(), b"{}");
                        assert_eq!(call, &session::llm::Decoded::Historical);
                    }
                    session::llm::Block::ToolResult {
                        id,
                        result: session::llm::Returned::Text { text, error, replay },
                    } => {
                        assert_eq!(id.as_ref(), ID);
                        assert_eq!(text.as_ref(), b"Messages arrived. Read them before continuing.");
                        assert!(!*error);
                        assert!(replay.is_none());
                    }
                    session::llm::Block::Refusal { .. }
                    | session::llm::Block::Opaque { .. }
                    | session::llm::Block::ToolResult { .. } => panic!("supported fixture records"),
                }
            }
        }
    }
    assert_eq!(count, messages - 4, "maximum reachable history leaves waking and provider slots");
    assert_eq!(large, 1);
}

fn cap(source: &Counted, waking: &[u8]) -> u64 {
    // Fixed SUMMARY and PREAMBLE texts in smith-domain-run/src/prompt.rs;
    // domain/run.md, section 3.3. The summary includes the 1,000-word limit.
    const SUMMARY_BYTES: u64 = 415;
    const PREAMBLE_BYTES: u64 = 97;

    let first = source.initial.as_ref().expect("observed fresh root prompt");
    let [message] = first.messages.as_ref() else { panic!("fresh actual waking message") };
    let [llm::Block::Text { text, replay: None }] = message.content.as_ref() else { panic!("plain charter brief") };
    assert_eq!(text.as_ref(), b"Begin the work your brief describes.");
    assert_eq!(first.served.len(), 2);
    // Independent public opening ownership plus actual concrete record fields.
    // Message/Turn envelopes are deliberately separate from this payload cap.
    let opening = len(&first.model)
        + len(&first.system)
        + 2 * size::<session::llm::Descriptor>()
        + size::<session::llm::Block>()
        + len(text)
        + len(waking)
        + SUMMARY_BYTES
        + PREAMBLE_BYTES; // Retained summary instruction and preamble beside the task and history.
    let prefix = source
        .prefix
        .as_ref()
        .expect("actual prefix")
        .turns
        .iter()
        .flat_map(|turn| &turn.messages)
        .map(|message| record_blocks(&message.content))
        .sum::<u64>();
    opening
        + prefix
        + session::completion_reserve(&source.env.limits.session).expect("public required receiving credit")
        + size::<session::llm::Block>()
        + u64::from(source.env.limits.session.tools.path_bytes) * u64::from(source.env.limits.session.completion_blocks)
}

fn generate_prefix(counted: &mut Counted, configuration: &Configuration, messages: u32, cycles: u32) -> Limits {
    counted.start(None, false);
    counted.discover();
    counted.cycle(configuration, cycles + 1);
    assert!(counted.waiting);
    for at in 0..cycles {
        counted.waiting = false;
        let run = counted.admitted.expect("actual admitted handle");
        let text = if at == 0 { vec![b'x'; LARGE - 6].into_boxed_slice() } else { b"x".as_slice().into() };
        counted.step(Event::Message {
            finish_requested: false,
            label: Box::from(&b"host"[..]),
            run,
            name: Token::new(u64::from(at)),
            text,
        });
        counted.cycle(configuration, cycles + 1);
        assert!(counted.waiting, "wake {at}: records={} answer={:?}", counted.records.len(), counted.answer);
    }
    let copy_meter = Meter::new();
    copy_meter.start();
    let prefix = root::Transcript {
        version: session::record::VERSION,
        endpoint: Some(session::llm::Endpoint(0)),
        dialect: 2,
        turns: counted.records.clone().into(),
    };
    let copied = copy_meter.end();
    let expected = transcript_bytes(&prefix);
    assert_eq!(copied.held(), expected, "concrete source-record handoff owns all public envelopes and payloads");
    assert!(copied.peak() <= expected, "record handoff has no unpriced construction transient");
    counted.prefix = Some(prefix);
    assert_generated(counted.prefix.as_ref().expect("actual selected history"), messages);
    counted.saved = counted.prefix.as_ref().map(copy_transcript);
    let first = counted.initial.as_ref().expect("observed fresh root prompt");
    let [llm::Block::Text { text, replay: None }] = first.messages[0].content.as_ref() else {
        panic!("actual fresh waking text")
    };
    let capped = cap(counted, text);
    let tight = Limits {
        session: session::Limits { history_bytes: capped, ..counted.env.limits.session },
        ..counted.env.limits
    };
    let run = counted.admitted.expect("actual source run");
    counted.waiting = false;
    counted.step(Event::Cancel { run });
    counted.cycle(configuration, cycles + 1);
    assert!(
        matches!(counted.answer, Some(run::Answer::Failed { failure: run::Failure::Cancelled, turns, .. }) if turns == 2 * cycles + 1)
    );
    counted.retire_native();

    tight
}

fn restore_overlap(counted: &mut Counted, configuration: &Configuration, messages: u32, cycles: u32) {
    // The full prefix owns messages - 4 slots. Restored waking, the actual
    // Wait/result, the offered message and text, and the next nudge occupy six
    // more. Two provider slots and one summary slot remain for the next held
    // wait, so this overlap uses messages + 5. Payload caps retain the source contract.
    let overlapping = bounds(messages.checked_add(5).expect("second receiving entrance and its summary slot"));
    counted.replace_root(&overlapping);
    counted.start(counted.prefix.as_ref().map(copy_transcript), true);
    counted.discover();
    assert_eq!(counted.calls, 1, "full source history starts one actual restored callback");
    let complete = counted.complete.as_ref().expect("actual restored Complete before overlapping physical bindings");
    assert_eq!(complete.prompt.messages.len(), usize::try_from(messages - 3).expect("full restored plus waking"));
    assert_rewritten(counted.prefix.as_ref().expect("maximum generated prefix"), &complete.prompt);
    counted.held_prompt = Some(copy_prompt(&complete.prompt));
    counted.prompt_copy = counted.held_prompt.as_ref().map(copy_prompt);
    // The same whole native script adds the next Wait/text pair. The old
    // Wait terminal's closing Client remains live when its text Client starts.
    counted.cycle(configuration, cycles + 2);
    assert!(counted.waiting && counted.records.is_empty(), "first restored native wait remains held");
    counted.wake(Token::new(100), b"wake".as_slice().into());
    counted.cycle(configuration, cycles + 2);
    assert_eq!(counted.calls, 3, "two settled callbacks and the next held wait use three actual Clients");
    assert_eq!(counted.records.len(), 2, "both restored terminals produced concrete Turns");
    assert_eq!(counted.records[0].sequence, 2 * cycles + 1);
    assert_eq!(counted.records[1].sequence, 2 * cycles + 2);
    assert!(
        counted.restored_overlap,
        "full generated histories, both restored Prompt copies and a Turn copy coexist with native overlap"
    );
    assert!(counted.waiting, "actual restored Wait/text pair reaches Waiting after both native terminals");
    let run = counted.admitted.expect("actual overlapping restore activation");
    counted.waiting = false;
    counted.step(Event::Cancel { run });
    counted.cycle(configuration, cycles + 2);
    assert!(matches!(counted.answer, Some(run::Answer::Failed { failure: run::Failure::Cancelled, turns: 3, .. })));
    counted.resume();
    counted.retire_native();
}

fn restore_exact(counted: &mut Counted, configuration: &Configuration, messages: u32, cycles: u32, tight: &Limits) {
    let prompt = counted.held_prompt.as_ref().expect("observed overlapping restore");
    let waking = prompt.messages.last().expect("actual resumed waking message");
    let [llm::Block::Text { text: waking, replay: None }] = waking.content.as_ref() else {
        panic!("plain resumed waking text")
    };
    // Restore retains the original task, but opens with the shorter resumed
    // wake. Fill that observed room in one saved text, preserving the LARGE
    // message and every history envelope.
    let padding = tight.session.history_bytes.checked_sub(cap(counted, waking)).expect("shorter resumed opening");
    let history = counted.prefix.as_mut().expect("generated prefix");
    let last = history.turns.last_mut().expect("generated turns").messages.last_mut().expect("saved assistant");
    let [session::llm::Block::Text { text, replay: None }] = last.content.as_mut() else {
        panic!("actual saved assistant text")
    };
    let mut padded = text.to_vec();
    padded.resize(padded.len() + usize::try_from(padding).expect("bounded wake difference"), b'x');
    *text = padded.into_boxed_slice();
    assert_eq!(cap(counted, waking), tight.session.history_bytes, "history plus actual wake attains the payload cap");
    counted.replace_root(tight);
    counted.start(counted.prefix.as_ref().map(copy_transcript), true);
    counted.discover();
    assert_eq!(counted.calls, 1, "exact reservation-inclusive payload is admitted before native work");
    let complete = counted.complete.as_ref().expect("actual rewritten Complete");
    assert_eq!(complete.prompt.messages.len(), usize::try_from(messages - 3).expect("restored plus waking"));
    assert_eq!(complete.prompt.model, counted.initial.as_ref().expect("original observation").model);
    assert_eq!(complete.prompt.system, counted.initial.as_ref().expect("original observation").system);
    assert_rewritten(counted.prefix.as_ref().expect("original generated prefix"), &complete.prompt);
    counted.held_prompt = Some(copy_prompt(&complete.prompt));
    counted.prompt_copy = counted.held_prompt.as_ref().map(copy_prompt);
    counted.until_compaction(configuration, cycles);
    let [turn] = counted.records.as_slice() else { panic!("one actual concrete restored Turn") };
    assert_eq!(turn.sequence, 2 * cycles + 1);
    assert_eq!(turn.messages.len(), 2);
    let [_, assistant] = turn.messages.as_ref() else { panic!("actual wake/assistant Turn") };
    let [session::llm::Block::Text { text, .. }] = assistant.content.as_ref() else {
        panic!("actual restored native text")
    };
    assert_eq!(text.as_ref(), RESTORED);
    counted.turn_copy = Some(copy_turn(turn));
    let owner = counted.complete.as_ref().expect("held compaction callback").owner;
    let run = counted.admitted.expect("restored activation");
    counted.step(Event::Cancel { run });
    counted.resume();
    drop(counted.complete.take());
    counted.step(Event::Cancelled { owner });
    assert!(matches!(counted.answer, Some(run::Answer::Failed { failure: run::Failure::Cancelled, turns: 1, .. })));
    counted.resume();
    counted.retire_native();
}

fn refuse_one_over(counted: &mut Counted, configuration: &Configuration, cycles: u32, tight: &Limits) {
    for extra_message in [true, false] {
        let mut negative = *tight;
        if extra_message {
            negative.session.history_bytes += 16_384;
            assert!(size::<session::llm::Block>() + len(b"x") < 16_384, "count control has independent byte headroom");
        } else {
            // Pin the byte-only refusal to its prior receiving cap, independent
            // of the new minimum-result and summary admission headroom.
            negative.session.history_bytes -= size::<session::llm::Block>()
                + u64::from(negative.session.tools.path_bytes) * u64::from(negative.session.completion_blocks);
        }
        counted.replace_root(&negative);
        let mut history = copy_transcript(counted.prefix.as_ref().expect("saved actual records"));
        if extra_message {
            let last = history.turns.last_mut().expect("saved turns");
            let mut messages = last.messages.to_vec();
            messages.push(session::llm::Message {
                role: session::llm::Role::User,
                content: Box::new([session::llm::Block::Text { text: b"x".as_slice().into(), replay: None }]),
            });
            last.messages = messages.into();
        } else {
            let text = history
                .turns
                .iter_mut()
                .flat_map(|turn| &mut turn.messages)
                .flat_map(|message| &mut message.content)
                .find_map(|block| match block {
                    session::llm::Block::Text { text, .. } if text.len() == LARGE => Some(text),
                    session::llm::Block::Text { .. }
                    | session::llm::Block::Refusal { .. }
                    | session::llm::Block::Opaque { .. }
                    | session::llm::Block::ToolCall { .. }
                    | session::llm::Block::ToolResult { .. } => None,
                })
                .expect("literal generated large message");
            // restore_exact filled the actual resumed wake's headroom in
            // this prefix, so one more byte is exactly over receiving credit.
            *text = vec![b'x'; LARGE + 1].into_boxed_slice();
        }
        counted.start(Some(history), true);
        counted.discover();
        counted.cycle(configuration, cycles);
        assert_eq!(counted.calls, 0, "one-over valid history starts no provider or owned tool");
        assert!(counted.records.is_empty());
        assert!(matches!(
            counted.answer,
            Some(run::Answer::Failed {
                failure: run::Failure::Transcript(run::TranscriptRefusal::TooLarge),
                turns: 0,
                ..
            })
        ));
    }
}

fn reclaim_every_owner(mut counted: Counted, tight: &Limits, configuration: Configuration) {
    counted.retire_native();
    assert!(counted.overlaps > 0, "the memory run attained overlapping actual physical Clients");
    counted.meter.start();
    drop(counted.domain.take());
    drop(counted.prefix.take());
    drop(counted.saved.take());
    drop(counted.initial.take());
    drop(counted.caller_charter.take());
    drop(counted.caller_workspace.take());
    drop(counted.held_prompt.take());
    drop(counted.prompt_copy.take());
    drop(counted.turn_copy.take());
    counted.records.clear();
    counted.records.shrink_to_fit();
    drop(configuration);
    counted.configuration_bytes = 0;
    let measured = counted.meter.end();
    counted.meter.check(measured, counted.ceiling, tight);
    let Counted { meter, out, .. } = counted;
    drop(out);
    assert_eq!(meter.held(), 0, "all root, native fixture configuration and retained outside copies reclaimed");
}

#[test]
fn restored_root_arrays_payload_rewrites_and_turn_copies_stay_within_the_attained_bound() {
    // Peak through generated history, restore overlap and exact receiving-credit refusals.
    // Measured 5_925_491 B on 2026-10-28; a quarter of headroom, rounded up.
    const RESTORE_CEILING: u64 = 7_500_000;
    for messages in [19, 39] {
        let cycles = (messages - 4) / 5;
        let mut counted = Counted::new(&bounds(messages), RESTORE_CEILING);
        let configuration = counted.configuration();
        let tight = generate_prefix(&mut counted, &configuration, messages, cycles);
        restore_overlap(&mut counted, &configuration, messages, cycles);
        restore_exact(&mut counted, &configuration, messages, cycles, &tight);
        refuse_one_over(&mut counted, &configuration, cycles, &tight);
        reclaim_every_owner(counted, &tight, configuration);
    }
}

fn maximum_workspace(limits: &run::Limits) -> run::Workspace {
    let mounted = run::Workspace {
        directories: Box::new([
            run::Directory {
                host: format!(
                    "/{:h<width$}",
                    "",
                    width = usize::try_from(limits.host_path_bytes - 1).expect("positive host cap")
                )
                .into_bytes()
                .into(),
                name: vec![b'w'; 64].into_boxed_slice(),
                root: Token::new(7),
                writable: true,
                git: true,
                conflicts: (0..limits.conflicts)
                    .map(|index| {
                        if index == 0 {
                            vec![
                                b'p';
                                usize::try_from(limits.conflict_path_bytes).expect("bounded initial conflict path cap")
                            ]
                            .into_boxed_slice()
                        } else {
                            format!("p{index:02}").into_bytes().into_boxed_slice()
                        }
                    })
                    .collect(),
            },
            run::Directory {
                host: format!(
                    "/{:h<width$}",
                    "",
                    width = usize::try_from(limits.host_path_bytes - 1).expect("positive host cap")
                )
                .into_bytes()
                .into(),
                name: vec![b'r'; 64].into_boxed_slice(),
                root: Token::new(8),
                writable: false,
                git: true,
                conflicts: (0..limits.conflicts)
                    .map(|index| format!("q{index:02}").into_bytes().into_boxed_slice())
                    .collect(),
            },
        ]),
    };
    assert_eq!(mounted.directories.len(), usize::try_from(limits.directories).expect("bounded directory count"));
    for directory in &mounted.directories {
        assert_eq!(directory.name.len(), usize::try_from(limits.directory_name_bytes).expect("bounded mount name cap"));
        assert_eq!(
            directory.conflicts.len(),
            usize::try_from(limits.conflicts).expect("bounded initial conflict count")
        );
        assert!(directory.conflicts.iter().all(|path| path.len()
            <= usize::try_from(limits.conflict_path_bytes).expect("bounded initial conflict path cap")));
    }
    assert_eq!(
        mounted.directories[0].conflicts[0].len(),
        usize::try_from(limits.conflict_path_bytes).expect("bounded initial conflict path cap")
    );
    mounted
}

/// Fill the existing Start aggregate, pricing independent public Section cells
/// before redistributing its remaining bytes among role, titles and bodies.
fn fill_main_context(charter: &mut run::Charter, workspace: Option<&run::Workspace>, limits: &run::Limits) {
    let previous = sum([
        len(&charter.instructions),
        cells::<run::Section>(charter.brief.sections.len()),
        sum(charter.brief.sections.iter().map(|section| sum([len(&section.title), len(&section.text)]))),
    ]);
    let fixed = charter_bytes(charter) - previous + workspace_bytes(workspace);
    let count = usize::try_from(limits.brief_sections).expect("bounded receiving section count");
    let payload = limits.run_bytes - fixed - cells::<run::Section>(count);
    let instruction_bytes = usize::try_from(payload / 4).expect("bounded role payload");
    let mut instructions = vec![b'i'; instruction_bytes];
    instructions[..b"@root-memory".len()].copy_from_slice(b"@root-memory");
    charter.instructions = instructions.into_boxed_slice();
    let mut remainder = payload - len(&charter.instructions);
    let mut sections = Vec::with_capacity(count);
    for section in 0..count {
        let remaining = u64::try_from(count - section).expect("bounded section suffix");
        let title_bytes = usize::try_from(remainder / remaining / 3).expect("bounded title payload");
        let text_bytes =
            usize::try_from(remainder / remaining).expect("bounded Section payload fits usize") - title_bytes;
        let title = vec![b't'; title_bytes].into_boxed_slice();
        let text = vec![b's'; text_bytes].into_boxed_slice();
        remainder -= len(&title) + len(&text);
        sections.push(run::Section { title, text });
    }
    assert_eq!(remainder, 0, "every original aggregate byte is allocated exactly");
    assert!(sections.iter().all(|section| !section.title.is_empty() && !section.text.is_empty()));
    assert_eq!(sections.len(), count, "maximum receiving Section count is attained");
    charter.brief = run::Brief { sections: sections.into_boxed_slice() };
    assert_eq!(charter_bytes(charter) + workspace_bytes(workspace), limits.run_bytes);
}

fn maximum_convention_charter() -> run::Charter {
    let path_bytes = run::Conventions::PATH_CAPACITY;
    let make_path = |byte| {
        let mut path = vec![byte; path_bytes];
        for slash in (63..path_bytes - 1).step_by(64) {
            path[slash] = b'/';
        }
        path.into_boxed_slice()
    };
    run::Charter {
        reserve: run::charter::Reserve { turns: 0, time: bounds(16).run.check_timeout },
        conventions: Some(run::Conventions { guide: make_path(b'g'), checks: make_path(b'c') }),
        outcome: run::outcome::OutcomeSpec {
            change: Some(run::outcome::ChangeSpec { checks_must_pass: true, fields: Box::new([]) }),
            ..charter(false).outcome
        },
        ..charter(false)
    }
}

#[test]
fn a_restored_run_keeps_full_brief_and_custom_paths_with_two_clients() {
    // Peak with a full brief, maximum workspace paths and two physical clients.
    // Measured 5_976_792 B on 2026-10-28; a quarter of headroom, rounded up.
    const BRIEF_CEILING: u64 = 7_500_000;
    let path_bytes = run::Conventions::PATH_CAPACITY;
    let mut receiving = bounds(16);
    receiving.run.directories = 2;
    // Host alias metadata is retained independently of the unchanged maximum
    // Brief/convention text; give this attained fixture its additional bytes.
    receiving.run.run_bytes = 16_384 + u64::from(receiving.run.directories) * u64::from(receiving.run.host_path_bytes);
    receiving.session.tools.repos = 2;
    // The receiving root must retain every rendered actual run result. Select
    // the public checked feedback bound exactly, rather than widening the
    // original history/actual completion caps or bypassing root admission.
    receiving.session.delegated_result_bytes = root::feedback_worst_case(&receiving.run)
        .expect("maximum path charter has a finite exact receiving feedback bound")
        .max(receiving.session.delegated_result_bytes);
    let mut counted = Counted::with_window(&receiving, 65_536, BRIEF_CEILING);
    // This combined fixture's actual system contains both maximum convention
    // labels plus full nested workspace metadata. Select explicit native input
    // string cap for that new prompt: 32,768 rather than the default 16,384.
    // HTTP request-head and dialect request/document caps stay 32,768, matching
    // the shared peer's fixed HTTP body/intake bounds. Original history/count/
    // completion/refusal tests keep wire_limits() unchanged; root caps stay fixed.
    counted.native_limits.client.strings = 32_768;
    let configuration = counted.configuration();
    let mut caller = maximum_convention_charter();
    caller.llm.window = 65_536;
    let mounted = Some(maximum_workspace(&receiving.run));
    fill_main_context(&mut caller, mounted.as_ref(), &receiving.run);
    let caller_bytes = charter_bytes(&caller);
    let both_paths = path_bytes.checked_mul(2).expect("two bounded convention paths fit a fixture size");
    assert!(caller_bytes >= u64::try_from(both_paths).expect("bounded maximum convention payload fits u64"));
    assert_eq!(caller_bytes + workspace_bytes(mounted.as_ref()), receiving.run.run_bytes);
    counted.caller_workspace = mounted.clone();
    let active = maximum_caller_start(&caller);
    counted.caller_charter = Some(caller);
    counted.step(Event::Start {
        messages: Box::default(),
        answered: Box::default(),
        reply_to: ReplyTo::new(PARENT),
        host_run: WORKER,
        identity: None,
        activation: 1,
        window: root::Window { turns: u32::MAX, bytes: u64::MAX },
        charter: active,
        workspace: mounted,
        transcript: None,
        grants: Box::new([Grant {
            renewable: true,
            name: GrantName { account: 0, generation: 1 },
            valid: Duration::from_secs(3600),
        }]),
    });
    assert!(counted.admitted.is_some() && counted.read.is_some());
    assert_eq!(counted.calls, 0, "maximum retained paths and caller Start coexist before any provider effect");
    let owner = counted.read.take().expect("actual maximum-path discovery");
    counted.step(Event::Read {
        owner,
        read: run::Read::Text { text: b"Maximum host-selected guide".as_slice().into(), whole: true },
    });
    let owner = counted.probe.take().expect("actual maximum check-path Probe");
    counted.step(Event::Probed { owner, executable: true });
    let owner = counted.read.take().expect("actual maximum guide-path Read for the readonly mount");
    counted.step(Event::Read {
        owner,
        read: run::Read::Text { text: b"Maximum readonly guide".as_slice().into(), whole: true },
    });
    let first = counted.complete.as_ref().expect("actual first root Complete");
    assert!(first.prompt.system.len() > 16_384, "combined metadata exceeds the original native string cap");
    assert!(first.prompt.system.len() <= 32_768, "explicit native string cap fits the actual combined prompt");
    let source = counted.caller_charter.as_ref().expect("original caller Start remains independently owned");
    assert!(first.prompt.system.starts_with(&source.instructions), "actual native main receives full literal role");
    assert_eq!(
        source.brief.sections.len(),
        usize::try_from(receiving.run.brief_sections).expect("bounded receiving Section count fits usize")
    );
    let selected = counted
        .caller_charter
        .as_ref()
        .expect("caller retains its original maximum-path Start")
        .conventions
        .as_ref()
        .expect("caller explicitly selected both maximum convention paths");
    assert_eq!(selected.guide.len(), path_bytes);
    assert_eq!(selected.checks.len(), path_bytes);
    assert_eq!(
        first.prompt.system.windows(path_bytes).filter(|path| *path == selected.guide.as_ref()).count(),
        2,
        "both actual guide headings own the maximum selected path"
    );
    assert_eq!(
        first.prompt.system.windows(path_bytes).filter(|path| *path == selected.checks.as_ref()).count(),
        1,
        "the writable mount owns the maximum selected check label"
    );
    assert!(counted.probe.is_none(), "the readonly mount adds no second check probe");
    counted.cycle(&configuration, 1);
    assert!(counted.waiting && counted.records.is_empty(), "actual wait is held before the native text callback");
    counted.wake(Token::new(100), b"wake".as_slice().into());
    counted.cycle(&configuration, 2);
    assert!(counted.waiting && counted.overlaps > 0, "actual Wait/text terminals settle with two Clients overlapping");
    assert!(counted.caller_charter.is_some(), "caller owns both maximum paths throughout actual native handoffs");
    let run = counted.admitted.expect("maximum-path activation was actually admitted");
    counted.waiting = false;
    counted.step(Event::Cancel { run });
    counted.cycle(&configuration, 1);
    assert!(matches!(counted.answer, Some(run::Answer::Failed { failure: run::Failure::Cancelled, turns: 3, .. })));
    reclaim_every_owner(counted, &receiving, configuration);
}

/// Copy caller-owned maximum context into an independent actual Start.
fn maximum_caller_start(caller: &run::Charter) -> run::Charter {
    run::Charter {
        instructions: caller.instructions.clone(),
        brief: run::Brief {
            sections: caller
                .brief
                .sections
                .iter()
                .map(|section| run::Section { title: section.title.clone(), text: section.text.clone() })
                .collect(),
        },
        conventions: caller.conventions.clone(),
        outcome: caller.outcome.clone(),
        reserve: caller.reserve,
        llm: caller.llm.clone(),
        ..charter(false)
    }
}
