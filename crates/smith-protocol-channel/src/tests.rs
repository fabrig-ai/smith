//! Opening entry points (protocol/channel.md, section 2).
use skein_channel::{Lower, StreamMode};
use skein_lib::{Queue, stream};

use crate::{Component, Limits, OpenEvent};

fn limits() -> Limits {
    Limits {
        bodies: smith_channel::CEILINGS,
        start_bytes: u32::try_from(
            smith_channel::Start::worst_case_bytes(&smith_channel::CEILINGS).expect("fixture Start bound"),
        )
        .expect("fixture frame"),
        charter: smith_charter::CEILINGS,
        transcript: smith_transcript::CEILINGS,
        resume_turns: 64,
        search_hits: 256,
        endpoints: 0,
        calls: 0,
        turns: 0,
        fact_reserve_frames: 1,
        fact_reserve_bytes: 128,
        grants: 0,
        channel: skein_channel::Limits {
            chunk: 8,
            credential: 0,
            skip: 8,
            output_bytes: u32::MAX,
            output_frames: 2,
            kinds: 20,
        },
    }
}

#[test]
fn the_agent_demands_the_open_before_the_domain_hears_anything() {
    let limits = limits();
    let mut component =
        Component::new(&limits, StreamMode::Two, crate::Endpoints::new(skein_lib::List::with_capacity(0)))
            .expect("checked schema");
    let mut to_service = Queue::<OpenEvent>::with_capacity(2);
    let mut below = Queue::<Lower>::with_capacity(8);
    component.fire(&mut to_service, &mut below);
    assert!(to_service.is_empty());
    match below.pop() {
        Some(Lower::Read(stream::Down::Demand { read: stream::Read::Fill(1), room: 0 })) => {}
        other => panic!("expected Open read: {other:?}"),
    }
}

fn saved_turn(place: u32, dialect: u32) -> Box<[u8]> {
    use smith_transcript as wire;
    let limits = wire::CEILINGS;
    let result = wire::ToolResult::new(
        &limits,
        wire::ToolResultParts {
            id: Box::from(*b"call-1"),
            returned: wire::Returned::Invalid(
                wire::Invalid::new(&limits, wire::InvalidParts { problem: wire::CallProblem::UnknownTool })
                    .expect("bounded invalid result"),
            ),
        },
    )
    .expect("bounded result");
    let mut blocks = skein_lib::List::with_capacity(2);
    blocks
        .push(wire::Block::Call(
            wire::Call::new(
                &limits,
                wire::CallParts {
                    id: Box::from(*b"call-1"),
                    name: Box::from(*b"missing"),
                    input: Box::from(*b"{}"),
                    replay: None,
                },
            )
            .expect("bounded call"),
        ))
        .expect("first block");
    blocks.push(wire::Block::Result(result)).expect("second block");
    let mut messages = skein_lib::List::with_capacity(1);
    messages
        .push(
            wire::Message::new(&limits, wire::MessageParts { role: wire::Role::Assistant, blocks })
                .expect("bounded message"),
        )
        .expect("first message");
    let turn = wire::Turn::new(
        &limits,
        wire::TurnParts {
            model: b"model".as_slice().into(),
            effort: None,
            kind: wire::TurnKind::Ordinary,
            opening: if place == 1 {
                Some(
                    wire::Opening::new(
                        &limits,
                        wire::OpeningParts {
                            affinity: wire::Affinity::new(&limits, wire::AffinityParts { key: [0; 16], thread: 0 })
                                .expect("fixture affinity"),
                            account: None,
                            backend: wire::Backend::Direct,
                        },
                    )
                    .expect("fixture opening"),
                )
            } else {
                None
            },
            endpoint: Box::default(),
            dialect,
            place,
            usage: wire::Usage::new(
                &limits,
                wire::UsageParts {
                    input: Some(2),
                    output: Some(3),
                    cache_read: Some(4),
                    cache_write: Some(5),
                    reasoning: None,
                },
            )
            .expect("bounded usage"),
            spent: 7,
            jobs: 0,
            messages,
        },
    )
    .expect("bounded turn");
    let mut writer = skein_lib::Writer::new(usize::try_from(turn.measure()).expect("bounded turn length"));
    turn.encode(&mut writer).expect("measured turn");
    writer.finish()
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "test reports the observed value")]
fn transcript_decodes_history_with_one_invalid_call_problem() {
    use smith_domain_session::{llm, record};
    let mut configured = skein_lib::List::with_capacity(1);
    configured.push(crate::Endpoint { name: Box::default(), number: 3, dialect: 0 }).expect("one endpoint");
    let endpoints = crate::Endpoints::new(configured);
    let bytes = Box::from([saved_turn(1, 0)]);
    let transcript = decode_saved_transcript(&bytes, &smith_transcript::CEILINGS, &endpoints)
        .expect("valid transcript")
        .expect("saved history");
    assert_eq!(transcript.version, record::VERSION);
    assert_eq!(transcript.endpoint, Some(llm::Endpoint(3)));
    assert_eq!(transcript.turns[0].sequence, 1);
    assert_eq!(transcript.turns[0].spent, 7);
    assert_eq!(transcript.turns[0].usage.cache_read_tokens, Some(4));
    match &transcript.turns[0].messages[0].content[0] {
        llm::Block::ToolCall { call: llm::Decoded::Historical, .. } => {}
        other => panic!("expected historical call: {other:?}"),
    }
    match &transcript.turns[0].messages[0].content[1] {
        llm::Block::ToolResult { result: llm::Returned::Invalid { problem: llm::Problem::UnknownTool }, .. } => {}
        other => panic!("expected single invalid problem: {other:?}"),
    }
}

#[test]
fn transcript_refuses_a_gap_and_a_different_dialect() {
    use smith_domain_session::record;
    struct Case {
        name: &'static str,
        turns: Box<[Box<[u8]>]>,
        expected: record::Refusal,
    }
    let cases = [
        Case { name: "gap", turns: Box::new([saved_turn(2, 0)]), expected: record::Refusal::Malformed },
        Case {
            name: "dialect",
            turns: Box::new([saved_turn(1, 0), saved_turn(2, 99)]),
            expected: record::Refusal::Malformed,
        },
    ];
    for row in cases {
        assert_eq!(
            decode_saved_transcript(&row.turns, &smith_transcript::CEILINGS, &saved_endpoints()),
            Err(row.expected),
            "{}",
            row.name
        );
    }
}

#[test]
fn a_live_turn_decodes_at_its_number_without_the_saved_prefix() {
    use smith_domain_session::record;
    let endpoints = saved_endpoints();
    let bytes = saved_turn(4, 0);
    let turn = crate::decode_turn(&bytes, 4, &smith_transcript::CEILINGS, &endpoints).expect("live turn");
    assert_eq!(turn.sequence, 4);
    assert_eq!(turn.spent, 7);
    assert_eq!(crate::decode_turn(&bytes, 3, &smith_transcript::CEILINGS, &endpoints), Err(record::Refusal::Malformed));
}

#[test]
fn optional_usage_round_trips_without_turning_absence_into_zero() {
    use smith_domain_session::llm;
    let endpoints = saved_endpoints();
    let bytes = saved_turn(1, 0);
    let mut turn = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    for usage in [
        llm::Usage::NONE,
        llm::Usage::ZERO,
        llm::Usage {
            input_tokens: None,
            output_tokens: Some(0),
            cache_read_tokens: Some(9),
            cache_write_tokens: None,
            reasoning_tokens: Some(7),
        },
    ] {
        turn.usage = usage;
        let encoded = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("turn encodes");
        let decoded = crate::decode_turn(&encoded, 1, &smith_transcript::CEILINGS, &endpoints).expect("turn decodes");
        assert_eq!(decoded.usage, usage);
    }
}

#[test]
fn size_refusal_subject_size_and_bound_survive_version_three_resume() {
    use smith_domain_session::llm;
    use smith_domain_tools::{Outcome, SizePart};
    let endpoints = saved_endpoints();
    let mut turn =
        crate::decode_turn(&saved_turn(1, 0), 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    for what in [
        SizePart::File,
        SizePart::WriteContent,
        SizePart::EditOld,
        SizePart::EditNew,
        SizePart::EditItems,
        SizePart::EditResult,
        SizePart::EditReplacements,
        SizePart::ListGlob,
    ] {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"size-refusal".as_slice().into(),
                result: llm::Returned::Owned { outcome: Outcome::TooLarge { what, size: 50_008, bound: 40_960 } },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("bounded size refusal");
        let decoded = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints).expect("resumed refusal");
        assert_eq!(decoded.messages, turn.messages, "live and saved refusals carry identical quantities");
    }
}

#[test]
fn resolved_failures_round_trip_in_version_three_and_refuse_unnormalized_paths() {
    use smith_domain_session::{llm, record};
    use smith_domain_tools::{Name, Outcome, Resolved};
    let endpoints = saved_endpoints();
    let mut turn =
        crate::decode_turn(&saved_turn(1, 0), 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    let cases = [
        Outcome::NotFound { path: Resolved::Working { path: "src/café.rs".as_bytes().into() } },
        Outcome::NotDirectory { path: Resolved::Working { path: Box::new([]) } },
        Outcome::ReadOnly {
            path: Resolved::Other {
                directory: Name::new(b"docs".as_slice().into()).expect("name"),
                path: b"guide.md".as_slice().into(),
            },
        },
        Outcome::Outside { path: Resolved::Outside { path: b"other/file".as_slice().into() } },
    ];
    for outcome in cases {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"path-result".as_slice().into(),
                result: llm::Returned::Owned { outcome },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("bounded turn");
        assert_eq!(&bytes[..2], &[0, 6]);
        let decoded = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints).expect("saved failure");
        assert_eq!(decoded.messages, turn.messages, "typed descriptors survive durable translation");
    }
    for path in [b"../file".as_slice(), b"a/./b", b"a//b", b"/file", b"file/", b"a\0b"] {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"path-result".as_slice().into(),
                result: llm::Returned::Owned {
                    outcome: Outcome::NotFound { path: Resolved::Working { path: path.into() } },
                },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("wire byte bound");
        assert_eq!(
            crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints),
            Err(record::Refusal::Malformed),
            "{path:?}"
        );
    }
}

#[test]
fn listed_tree_paths_survive_replay_and_refuse_unnormalized_entries() {
    use smith_domain_session::{llm, record};
    use smith_domain_tools::{Entry, Kind, Outcome};
    let endpoints = saved_endpoints();
    let mut turn =
        crate::decode_turn(&saved_turn(1, 0), 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    for path in ["src/café.rs".as_bytes(), b"../file", b"a/./b", b"a//b", b"/file", b"file/", b"a\0b", b""] {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"tree-result".as_slice().into(),
                result: llm::Returned::Owned {
                    outcome: Outcome::Listed {
                        entries: Box::new([Entry { name: path.into(), kind: Kind::File }]),
                        more: 7,
                    },
                },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("bounded entry");
        let decoded = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints);
        if path == "src/café.rs".as_bytes() {
            assert_eq!(decoded.expect("nested relative path").messages, turn.messages);
        } else {
            assert_eq!(decoded, Err(record::Refusal::Malformed), "{path:?}");
        }
    }
}

fn with_recorded_affinity(bytes: &[u8], opening: Option<smith_transcript::Opening>) -> Box<[u8]> {
    let limits = smith_transcript::CEILINGS;
    let mut parts =
        smith_transcript::Turn::decode(&limits, &mut skein_lib::Reader::new(bytes)).expect("fixture turn").into_parts();
    parts.opening = opening;
    encode_saved_parts(parts)
}

#[test]
fn a_transcript_records_affinity_only_in_its_opening_turn() {
    use smith_domain_session::record;
    let limits = smith_transcript::CEILINGS;
    let endpoints = saved_endpoints();
    let affinity =
        smith_transcript::Affinity::new(&limits, smith_transcript::AffinityParts { key: [0x42; 16], thread: 7 })
            .expect("fixed affinity");
    let first = with_recorded_affinity(
        &saved_turn(1, 0),
        Some(recorded_opening(affinity, None, smith_transcript::Backend::Direct)),
    );
    let ordinary = saved_turn(2, 0);
    let decoded = decode_saved_transcript(&[first.clone(), ordinary.clone()], &limits, &endpoints)
        .expect("opening and ordinary")
        .expect("transcript");
    assert_eq!(
        decoded.turns[0].kind,
        record::Kind::First(Box::new(record::Opener {
            affinity: smith_domain_session::llm::Affinity { key: [0x42; 16], thread: 7 },
            account: None,
            backend: record::Backend::Direct
        }))
    );
    assert_eq!(decoded.turns[1].kind, record::Kind::Ordinary);
    assert_eq!(
        decode_saved_transcript(&[with_recorded_affinity(&first, None)], &limits, &endpoints),
        Err(record::Refusal::Malformed)
    );
    let extra =
        smith_transcript::Affinity::new(&limits, smith_transcript::AffinityParts { key: [0x42; 16], thread: 7 })
            .expect("fixed affinity");
    assert_eq!(
        decode_saved_transcript(
            &[
                first,
                with_recorded_affinity(
                    &ordinary,
                    Some(recorded_opening(extra, None, smith_transcript::Backend::Direct))
                )
            ],
            &limits,
            &endpoints
        ),
        Err(record::Refusal::Malformed)
    );
}

#[test]
fn a_window_suffix_round_trips_its_noninitial_place_and_rejects_a_second_window() {
    use smith_domain_session::{llm, record};
    let limits = smith_transcript::CEILINGS;
    let endpoints = saved_endpoints();
    let window = record::Turn {
        model: b"model".as_slice().into(),
        effort: None,
        version: record::VERSION,
        kind: record::Kind::Window(Box::new(record::Opener {
            affinity: llm::Affinity { key: [0x25; 16], thread: 0 },
            account: None,
            backend: record::Backend::Direct,
        })),
        endpoint: Some(llm::Endpoint(3)),
        dialect: 0,
        sequence: 8,
        usage: llm::Usage::ZERO,
        spent: 9,
        jobs: 0,
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"task and summary".as_slice().into(), replay: None }]),
        }]),
    };
    let first = crate::encode_turn(&window, &limits, &endpoints).expect("window turn");
    let decoded = decode_saved_transcript(&[first.clone(), saved_turn(9, 0)], &limits, &endpoints)
        .expect("window suffix")
        .expect("transcript");
    assert_eq!(decoded.turns[0], window);
    assert!(decoded.turns[0].kind.opens_window());
    assert!(!decoded.turns[1].kind.opens_window());
    let mut second = window.clone();
    second.sequence = 9;
    let second = crate::encode_turn(&second, &limits, &endpoints).expect("second window bytes");
    assert_eq!(decode_saved_transcript(&[first, second], &limits, &endpoints), Err(record::Refusal::Malformed));
}

#[test]
fn attached_host_paths_are_normalized_before_copying_and_stay_bounded() {
    assert_eq!(crate::host_path(b"/tmp/work/./src/../", 32), Some(Box::from(b"/tmp/work".as_slice())));
    assert_eq!(crate::host_path(b"/../../", 32), Some(Box::from(b"/".as_slice())));
    assert_eq!(crate::host_path(b"/abc", 4), Some(Box::from(b"/abc".as_slice())));
    assert_eq!(crate::host_path(b"/abc", 3), None);
    assert_eq!(crate::host_path(b"relative", 32), None);
    assert_eq!(crate::host_path(b"/a\0b", 32), None);
}

#[test]
fn read_window_metadata_round_trips_and_invalid_one_based_values_are_refused() {
    use smith_domain_session::{llm, record};
    use smith_domain_tools::{BytesClamp, Cut, Next, Outcome};
    let endpoints = saved_endpoints();
    let mut turn =
        crate::decode_turn(&saved_turn(1, 0), 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    for (first, next, window, valid) in [
        (1, Next::Line(2), BytesClamp::Within, true),
        (1, Next::End, BytesClamp::Clamped { asked: 99, ceiling: 8 }, true),
        (0, Next::End, BytesClamp::Within, false),
        (1, Next::Line(0), BytesClamp::Within, false),
        (1, Next::End, BytesClamp::Clamped { asked: 8, ceiling: 8 }, false),
    ] {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"window".as_slice().into(),
                result: llm::Returned::Owned {
                    outcome: Outcome::Read {
                        content: b"part".as_slice().into(),
                        first,
                        lines: 1,
                        total: 2,
                        next,
                        cut: Cut::Line,
                        window,
                    },
                },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("bounded read turn");
        let decoded = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints);
        if valid {
            assert_eq!(decoded.expect("read metadata").messages, turn.messages);
        } else {
            assert_eq!(decoded, Err(record::Refusal::Malformed));
        }
    }
}

#[test]
fn command_clamps_round_trip_losslessly_and_invalid_relationships_are_refused() {
    use skein_lib::Duration;
    use smith_domain_session::{llm, record};
    use smith_domain_tools::{BytesClamp, Exit, Outcome, TimeoutClamp};
    let endpoints = saved_endpoints();
    let mut turn =
        crate::decode_turn(&saved_turn(1, 0), 1, &smith_transcript::CEILINGS, &endpoints).expect("saved turn");
    for (timeout, output, valid) in [
        (TimeoutClamp::Within, BytesClamp::Within, true),
        (
            TimeoutClamp::Clamped {
                asked: Duration::from_nanos(501_000_001),
                ceiling: Duration::from_nanos(300_000_000),
            },
            BytesClamp::Clamped { asked: 99, ceiling: 8 },
            true,
        ),
        (
            TimeoutClamp::Clamped { asked: Duration::from_nanos(1), ceiling: Duration::from_nanos(1) },
            BytesClamp::Within,
            false,
        ),
        (TimeoutClamp::Within, BytesClamp::Clamped { asked: 1, ceiling: 0 }, false),
    ] {
        turn.messages = Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::ToolResult {
                id: b"command".as_slice().into(),
                result: llm::Returned::Owned {
                    outcome: Outcome::Exited {
                        exit: Exit::TimedOut,
                        head: b"first".as_slice().into(),
                        tail: b"last".as_slice().into(),
                        dropped: 99,
                        timeout,
                        output,
                    },
                },
            }]),
        }]);
        let bytes = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("bounded command turn");
        let decoded = crate::decode_turn(&bytes, 1, &smith_transcript::CEILINGS, &endpoints);
        if valid {
            assert_eq!(decoded.expect("command metadata").messages, turn.messages);
        } else {
            assert_eq!(decoded, Err(record::Refusal::Malformed));
        }
    }
}

#[test]
fn an_endpoint_number_resolves_its_name_or_is_absent() {
    let mut entries = skein_lib::List::with_capacity(1);
    entries.push(crate::Endpoint { name: b"codex".as_slice().into(), number: 7, dialect: 91 }).expect("one name");
    let names = crate::Endpoints::new(entries);
    assert_eq!(names.name_for(7), Some(b"codex".as_slice()));
    assert_eq!(names.name_for(8), None);
}

fn decode_saved_transcript(
    bodies: &[Box<[u8]>],
    limits: &smith_transcript::v6::Limits,
    endpoints: &crate::Endpoints,
) -> Result<Option<smith_domain_session::record::Transcript>, smith_domain_session::record::Refusal> {
    crate::decode_transcript(
        &crate::transcript_piece(bodies).expect("fixture piece"),
        u32::try_from(bodies.len()).expect("fixture turns"),
        u32::MAX,
        limits,
        endpoints,
    )
}

#[test]
fn a_piece_whose_lengths_pass_its_end_is_malformed() {
    let endpoints = crate::Endpoints::new(skein_lib::List::with_capacity(0));
    assert_eq!(
        crate::decode_transcript(&[0, 0, 0, 2, 1], 1, u32::MAX, &smith_transcript::CEILINGS, &endpoints),
        Err(smith_domain_session::record::Refusal::Malformed)
    );
}

#[test]
fn a_piece_of_more_turns_than_the_resume_limit_is_too_large() {
    let endpoints = crate::Endpoints::new(skein_lib::List::with_capacity(0));
    assert_eq!(
        crate::decode_transcript(&[0, 0, 0, 0], 0, u32::MAX, &smith_transcript::CEILINGS, &endpoints),
        Err(smith_domain_session::record::Refusal::TooLarge)
    );
}

#[test]
fn a_search_hit_holding_all_of_search_bytes_is_told_and_resumed() {
    use smith_domain_session::llm;
    let mut names = skein_lib::List::with_capacity(1);
    names.push(crate::Endpoint { name: Box::default(), number: 3, dialect: 0 }).expect("endpoint");
    let endpoints = crate::Endpoints::new(names);
    let original = decode_saved_transcript(&[saved_turn(1, 0)], &smith_transcript::CEILINGS, &endpoints)
        .expect("saved fixture")
        .expect("history");
    let mut turn = original.turns[0].clone();
    turn.messages[0].content[1] = llm::Block::ToolResult {
        id: b"call-1".as_slice().into(),
        result: llm::Returned::Owned {
            outcome: smith_domain_tools::Outcome::Found {
                hits: Box::new([smith_domain_tools::Hit {
                    path: b"a".as_slice().into(),
                    line: 1,
                    text: skein_lib::bytes::zeroed(16_383),
                }]),
                more: 0,
                timed_out: false,
            },
        },
    };
    let encoded = crate::encode_turn(&turn, &smith_transcript::CEILINGS, &endpoints).expect("full search hit is told");
    let piece = crate::transcript_piece(&[encoded]).expect("saved piece");
    let resumed = crate::decode_transcript(&piece, 1, 100, &smith_transcript::CEILINGS, &endpoints)
        .expect("full search hit resumes")
        .expect("history");
    assert_eq!(resumed.turns[0], turn);
}

#[test]
fn a_turns_model_and_effort_round_trip_in_its_transcript() {
    let limits = smith_transcript::CEILINGS;
    let mut configured = skein_lib::List::with_capacity(1);
    configured.push(crate::Endpoint { name: Box::default(), number: 3, dialect: 0 }).unwrap();
    let endpoints = crate::Endpoints::new(configured);
    let mut turn = crate::decode_turn(&saved_turn(1, 0), 1, &limits, &endpoints).unwrap();
    turn.model = b"another-model".as_slice().into();
    for effort in [None, Some(b"high".as_slice())] {
        turn.effort = None;
        if let Some(value) = effort {
            turn.effort = Some(value.into());
        }
        let encoded = crate::encode_turn(&turn, &limits, &endpoints).unwrap();
        let decoded = crate::decode_turn(&encoded, 1, &limits, &endpoints).unwrap();
        assert_eq!(decoded.model.as_ref(), b"another-model");
        assert_eq!(decoded.effort.as_deref(), effort);
        assert_eq!(decoded, turn);
    }
}

mod jobs;

fn recorded_opening(
    affinity: smith_transcript::Affinity,
    account: Option<u32>,
    backend: smith_transcript::Backend,
) -> smith_transcript::Opening {
    smith_transcript::Opening::new(
        &smith_transcript::CEILINGS,
        smith_transcript::OpeningParts { affinity, account, backend },
    )
    .expect("fixture opening")
}

#[test]
fn an_opening_on_a_later_turn_is_malformed() {
    use smith_transcript as wire;
    let limits = wire::CEILINGS;
    let endpoints = saved_endpoints();
    for (account, backend) in [
        (Some(7), wire::Backend::Direct),
        (
            None,
            wire::Backend::Program(
                wire::Program::new(&limits, wire::ProgramParts { name: b"claude_code".as_slice().into() })
                    .expect("program"),
            ),
        ),
    ] {
        let affinity = wire::Affinity::new(&limits, wire::AffinityParts { key: [0; 16], thread: 0 }).expect("affinity");
        let bytes = with_recorded_affinity(&saved_turn(2, 0), Some(recorded_opening(affinity, account, backend)));
        assert_eq!(
            crate::decode_turn(&bytes, 2, &limits, &endpoints),
            Err(smith_domain_session::record::Refusal::Malformed),
            "account {account:?}"
        );
    }
}

#[test]
fn a_first_turn_without_its_opening_is_malformed() {
    use smith_transcript as wire;
    let limits = wire::CEILINGS;
    let endpoints = saved_endpoints();
    for kind in [wire::TurnKind::Ordinary, wire::TurnKind::Window] {
        let bytes = with_recorded_affinity(&saved_turn(1, 0), None);
        let mut parts = wire::Turn::decode(&limits, &mut skein_lib::Reader::new(&bytes)).expect("turn").into_parts();
        parts.kind = kind.clone();
        let turn = wire::Turn::new(&limits, parts).expect("turn without opening");
        let mut writer = skein_lib::Writer::new(usize::try_from(turn.measure()).expect("length"));
        turn.encode(&mut writer).expect("encoding");
        assert_eq!(
            crate::decode_turn(&writer.finish(), 1, &limits, &endpoints),
            Err(smith_domain_session::record::Refusal::Malformed),
            "{kind:?}"
        );
    }
}

fn saved_endpoints() -> crate::Endpoints {
    let mut configured = skein_lib::List::with_capacity(1);
    configured.push(crate::Endpoint { name: Box::default(), number: 3, dialect: 0 }).expect("endpoint");
    crate::Endpoints::new(configured)
}

#[test]
fn another_dialects_opaque_block_in_a_window_is_malformed() {
    use smith_transcript as wire;
    let limits = wire::CEILINGS;
    let mut parts =
        wire::Turn::decode(&limits, &mut skein_lib::Reader::new(&saved_turn(1, 0))).expect("fixture turn").into_parts();
    parts.kind = wire::TurnKind::Window;
    parts.place = 7;
    let mut blocks = skein_lib::List::with_capacity(1);
    blocks
        .push(wire::Block::Opaque(
            wire::Opaque::new(&limits, wire::OpaqueParts { dialect: 99, bytes: b"another dialect".as_slice().into() })
                .expect("opaque fixture"),
        ))
        .expect("one block");
    let mut messages = skein_lib::List::with_capacity(1);
    messages
        .push(
            wire::Message::new(&limits, wire::MessageParts { role: wire::Role::Assistant, blocks })
                .expect("fixture message"),
        )
        .expect("one message");
    parts.messages = messages;
    let bytes = encode_saved_parts(parts);
    assert_eq!(
        decode_saved_transcript(&[bytes], &limits, &saved_endpoints()),
        Err(smith_domain_session::record::Refusal::Malformed)
    );
}

fn encode_saved_parts(parts: smith_transcript::TurnParts) -> Box<[u8]> {
    let turn = smith_transcript::Turn::new(&smith_transcript::CEILINGS, parts).expect("bounded fixture turn");
    let mut writer = skein_lib::Writer::new(usize::try_from(turn.measure()).expect("fixture length"));
    turn.encode(&mut writer).expect("fixture room");
    writer.finish()
}

fn grant_charter() -> smith_domain::run::Charter {
    use skein_lib::Duration;
    use smith_domain::run::{self, charter, outcome};
    run::Charter {
        mode: run::Mode::Finite,
        resume: false,
        waiting: Duration::from_secs(30),
        instructions: Box::default(),
        brief: run::Brief { sections: Box::default() },
        conventions: None,
        grants: charter::Grants {
            wait: true,
            deliver: None,
            agents: false,
            host_tools: Box::default(),
            tools: charter::Tools { inspect: false, modify: false, shell: false },
        },
        outcome: outcome::OutcomeSpec {
            change: None,
            verdicts: Box::default(),
            failure: None,
            report: Some(outcome::TextSpec { max: 1024, fields: Box::default() }),
        },
        budget: run::Budget { turns: 64, spend: 10_000, time: Duration::from_secs(3600) },
        reserve: charter::Reserve { turns: 0, time: Duration::ZERO },
        llm: charter::Llm {
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            dialect: 0,
            account: Some(7),
            endpoint: charter::Endpoint(0),
            model: b"model".as_slice().into(),
            effort: None,
            window: 8192,
            output: 512,
        },
        models: Box::default(),
    }
}

#[test]
fn grant_forms_keep_their_policy_at_start_and_refresh() {
    use skein_lib::{Duration, List, Secret};
    use smith_channel as wire;
    let limits = wire::CEILINGS;
    let credential = |scheme| {
        wire::GrantValue::Credential(
            wire::CredentialValue::new(
                &limits,
                wire::CredentialValueParts { scheme, bytes: Secret::new(b"fixture".as_slice().into()) },
            )
            .expect("bounded credential"),
        )
    };
    for (value, renewable) in [
        (credential(wire::Scheme::Key), false),
        (credential(wire::Scheme::Bearer), true),
        (
            wire::GrantValue::Withdrawn(
                wire::WithdrawnValue::new(
                    &limits,
                    wire::WithdrawnValueParts {
                        id: b"fixture-id".as_slice().into(),
                        bearer: Secret::new(b"fixture".as_slice().into()),
                    },
                )
                .expect("bounded withdrawn form"),
            ),
            true,
        ),
        (
            wire::GrantValue::Program(
                wire::ProgramValue::new(&limits, wire::ProgramValueParts { name: b"claude-code".as_slice().into() })
                    .expect("bounded program form"),
            ),
            true,
        ),
    ] {
        let grant = wire::Grant::new(
            &limits,
            wire::GrantParts { account: 7, generation: 2, valid: Duration::from_secs(3600), value },
        )
        .expect("bounded wire grant");
        let refresh = crate::translate::refreshed_grant(&grant);
        assert_eq!(
            refresh,
            smith_domain::Grant {
                name: smith_domain::GrantName { account: 7, generation: 2 },
                valid: Duration::from_secs(3600),
                renewable,
            },
            "actual refresh conversion"
        );
        let mut grants = List::with_capacity(1);
        grants.push(grant).expect("one grant");
        let start = wire::Start::new(
            &limits,
            wire::StartParts {
                identity: None,
                activation: 1,
                charter: Box::default(),
                workspace: None,
                transcript: Box::default(),
                answered: List::with_capacity(0),
                grants,
                window: wire::Window::new(&limits, wire::WindowParts { turns: 8, bytes: 65_536 }).expect("window"),
                messages: List::with_capacity(0),
                live: None,
            },
        )
        .expect("bounded start envelope");
        let decoded = crate::start_context(start, grant_charter(), None).expect("actual start conversion");
        assert_eq!(decoded.grants.len(), 1);
        let kept = &decoded.grants[0];
        assert_eq!((kept.name, kept.valid, kept.renewable), (refresh.name, refresh.valid, renewable));
    }
}

#[test]
fn invalid_limits_are_refused() {
    let base = limits();
    assert!(crate::valid(&base));
    let mut cases = Vec::new();
    cases.push(Limits { start_bytes: 0, ..base });
    cases.push(Limits { start_bytes: base.start_bytes + 1, ..base });
    let mut body = base;
    body.bodies.message_text = u32::MAX;
    cases.push(body);
    for channel in [
        skein_channel::Limits { chunk: 0, ..base.channel },
        skein_channel::Limits { output_bytes: 7, ..base.channel },
        skein_channel::Limits { output_frames: 0, ..base.channel },
        skein_channel::Limits { kinds: 19, ..base.channel },
        skein_channel::Limits { output_bytes: 8, ..base.channel },
        skein_channel::Limits { output_frames: u32::MAX, ..base.channel },
        skein_channel::Limits { kinds: u32::MAX, ..base.channel },
        skein_channel::Limits { kinds: u32::MAX / 6, ..base.channel },
        skein_channel::Limits { credential: u32::MAX, ..base.channel },
        skein_channel::Limits { credential: u32::MAX - 14, ..base.channel },
    ] {
        cases.push(Limits { channel, ..base });
    }
    let mut charter = base;
    charter.charter.contract_verdicts = u32::MAX;
    charter.charter.verdict_rule_kinds = u32::MAX;
    charter.charter.item_kind_fields = u32::MAX;
    cases.push(charter);
    let mut transcript = base;
    transcript.transcript.turn_messages = u32::MAX;
    transcript.transcript.message_blocks = u32::MAX;
    transcript.transcript.listed_entries = u32::MAX;
    cases.push(transcript);
    for (index, limits) in cases.into_iter().enumerate() {
        assert!(!crate::valid(&limits), "invalid channel case {index}");
    }
}
