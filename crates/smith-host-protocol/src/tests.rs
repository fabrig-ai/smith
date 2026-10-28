//! Opening entry points (protocol/channel.md, section 2).
use skein_channel::{Lower, StreamMode};
use skein_lib::{Queue, stream};

use crate::{Component, Limits, OpenEvent};

#[test]
fn a_start_keeps_host_bytes_and_service_values_in_order() {
    use alloc::boxed::Box;
    use skein_lib::{Duration, Reader, Token};
    use smith_host_domain::channel;

    let limits = Limits {
        bodies: smith_channel::CEILINGS,
        charter: smith_charter::CEILINGS,
        transcript: smith_transcript::CEILINGS,
        endpoints: 1,
        calls: 8,
        start_bytes: u32::try_from(
            smith_channel::v5::Start::worst_case_bytes(&smith_channel::CEILINGS).expect("Start ceiling"),
        )
        .expect("Start frame range"),
        channel: skein_channel::Limits {
            chunk: 8,
            credential: 0,
            skip: 8,
            output_bytes: u32::MAX,
            output_frames: 2,
            kinds: 20,
        },
    };
    let start = channel::Start {
        live: None,
        messages: Box::default(),
        logical_run: Token::new(2),
        identity: Some(smith_domain::run::Identity::new(*b"stable-chat-name")),
        activation: 3,
        workspace: Some(Token::new(4)),
        charter: smith_host_world::charter_value(7),
        transcript: Some(smith_host_world::transcript_value(512)),
        answered: Box::from([channel::AnsweredCall {
            name: channel::CallName { activation: 1, completion: 2, position: 3 },
            tool: Box::from(*b"check"),
            reply: channel::SavedReply::Host { error: false, body: Box::from(*b"ok") },
        }]),
        directories: Box::from([channel::Directory {
            host: Box::from(b"/host".as_slice()),
            root: Token::new(1),
            name: Box::from(*b"src"),
            writable: true,
            git: true,
            conflicts: Box::from([Box::from(*b"file")]),
        }]),
        grants: Box::from([channel::Grant {
            account: 5,
            generation: 6,
            valid: Duration::from_nanos(7),
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
        }]),
    };
    let values = crate::Values {
        paths: Box::from([Box::from(*b"/tmp/src")]),
        credentials: Box::from([smith_protocol_channel::GrantValue::Bearer(Box::from(&b"secret"[..]))]),
    };
    let frame = crate::encode_start(
        start,
        channel::Window { turns: 1, bytes: 4096 },
        values,
        &limits.bodies,
        &limits.charter,
        &limits.transcript,
        &endpoints(),
    )
    .expect("bounded Start");
    assert_eq!(frame.kind(), 0x0100);
    let mut reader = Reader::new(&frame.bytes()[8..]);
    let start = smith_channel::Start::decode(&limits.bodies, &mut reader).expect("Start decodes");
    assert_eq!(start.activation(), 3);
    assert_eq!(start.identity(), &Some(*b"stable-chat-name"));
    assert_eq!(
        smith_protocol_channel::decode_charter(start.charter(), &limits.charter, &endpoints()).expect("typed charter"),
        smith_host_world::charter_value(7).into_value()
    );
    assert_eq!(
        smith_protocol_channel::decode_transcript(start.transcript(), 64, u32::MAX, &limits.transcript, &endpoints())
            .expect("typed transcript")
            .expect("history"),
        smith_host_world::transcript_value(512).into_value()
    );
    assert_eq!(start.answered().get(0).expect("answered call").tool(), b"check");
    assert_eq!(
        start.workspace().as_ref().expect("workspace").directories().get(0).expect("directory").path(),
        b"/tmp/src"
    );
    let grant = start.grants().get(0).expect("grant");
    let smith_channel::GrantValue::Credential(value) = grant.value() else {
        panic!("bearer grant");
    };
    assert_eq!(value.scheme(), &smith_channel::Scheme::Bearer);
    assert!(value.bytes().expose() == b"secret");
}

#[test]
fn the_host_sends_open_before_the_domain_hears_anything() {
    let limits = Limits {
        bodies: smith_channel::CEILINGS,
        charter: smith_charter::CEILINGS,
        transcript: smith_transcript::CEILINGS,
        endpoints: 1,
        calls: 8,
        start_bytes: u32::try_from(
            smith_channel::v5::Start::worst_case_bytes(&smith_channel::CEILINGS).expect("Start ceiling"),
        )
        .expect("Start frame range"),
        channel: skein_channel::Limits {
            chunk: 8,
            credential: 0,
            skip: 8,
            output_bytes: u32::MAX,
            output_frames: 2,
            kinds: 20,
        },
    };
    let mut component = Component::new(&limits, StreamMode::Two, endpoints()).expect("checked schema");
    let mut to_service = Queue::<OpenEvent>::with_capacity(2);
    let mut below = Queue::<Lower>::with_capacity(8);
    component.open(&mut to_service, &mut below);
    assert!(to_service.is_empty());
    match below.pop() {
        Some(Lower::Read(stream::Down::Demand { .. })) => {}
        other => panic!("expected opening read: {other:?}"),
    }
    match below.pop() {
        Some(Lower::Write(stream::OutputDown::Room { .. })) => {}
        other => panic!("expected Open write room: {other:?}"),
    }
}

pub(super) fn process_limits() -> Limits {
    Limits {
        bodies: smith_channel::CEILINGS,
        charter: smith_charter::CEILINGS,
        transcript: smith_transcript::CEILINGS,
        endpoints: 1,
        calls: 8,
        start_bytes: u32::try_from(
            smith_channel::v5::Start::worst_case_bytes(&smith_channel::CEILINGS).expect("Start ceiling"),
        )
        .expect("Start frame range"),
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

fn launch() -> crate::Launch {
    use alloc::boxed::Box;
    crate::Launch {
        program: Box::from(&b"smith"[..]),
        arguments: Box::from([Box::from(&b"agent"[..])]),
        environment: Box::from([Box::from(&b"LANG=C"[..])]),
        root: skein_io::kernel::Fd::new(4),
        directory: Box::from(&b"."[..]),
    }
}

#[test]
fn spawn_has_three_credential_free_standard_pipes() {
    use skein_io::{Request as IoRequest, kernel};
    use skein_lib::{Time, Token};

    let agent = Token::new(7);
    let mut process = crate::Process::new(agent, &process_limits(), 8, endpoints()).expect("channel");
    let mut below = Queue::with_capacity(8);
    process.spawn(launch(), Time::from_nanos(100), &mut below);
    assert_eq!(process.next_deadline(), Some(Time::from_nanos(100)));
    let Some(IoRequest::Spawn { owner, spawn }) = below.pop() else {
        panic!("one lower spawn");
    };
    assert_eq!(owner, process.io_owner());
    assert_eq!(&*spawn.program, b"smith");
    assert_eq!(&*spawn.args[0], b"agent");
    assert_eq!(&*spawn.env[0], b"LANG=C");
    assert_eq!(spawn.pipes.len(), 3);
    assert_eq!(spawn.pipes[0], kernel::Pipe { child: 0, way: kernel::Way::In, parent: None });
    assert_eq!(spawn.pipes[1], kernel::Pipe { child: 1, way: kernel::Way::Out, parent: None });
    assert_eq!(spawn.pipes[2], kernel::Pipe { child: 2, way: kernel::Way::Out, parent: None });
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "test rejects every other terminal")]
fn refused_spawn_reports_one_unspawned_terminal() {
    use skein_io::{Error as IoError, Event as IoEvent};
    use skein_lib::{Time, Token};

    let agent = Token::new(7);
    let mut process = crate::Process::new(agent, &process_limits(), 8, endpoints()).expect("channel");
    let io_owner = process.io_owner();
    let mut below = Queue::with_capacity(8);
    let mut above = Queue::with_capacity(8);
    process.spawn(launch(), Time::from_nanos(100), &mut below);
    below.pop();
    process.from_io(
        Time::from_nanos(2),
        IoEvent::Failed { owner: io_owner, error: IoError::Busy },
        &mut above,
        &mut below,
    );
    process.from_io(Time::from_nanos(2), IoEvent::Closed { owner: io_owner }, &mut above, &mut below);
    match above.pop().expect("one terminal") {
        crate::ProcessEvent::Unspawned { agent: observed, detail } => {
            assert_eq!(observed, agent);
            assert!(detail.is_empty());
        }
        other => panic!("unexpected event: {other:?}"),
    }
    assert!(above.is_empty());
}

#[test]
#[expect(clippy::wildcard_enum_match_arm, reason = "test rejects every other terminal")]
fn expired_opening_kills_child_and_keeps_errors_tail() {
    use alloc::boxed::Box;
    use skein_io::{Event as IoEvent, Request as IoRequest};
    use skein_lib::{Time, Token, stream};

    let agent = Token::new(7);
    let child = Token::new(8);
    let input = Token::new(9);
    let output = Token::new(10);
    let error = Token::new(11);
    let mut process = crate::Process::new(agent, &process_limits(), 4, endpoints()).expect("channel");
    let io_owner = process.io_owner();
    let mut below = Queue::with_capacity(16);
    let mut above = Queue::with_capacity(8);
    process.spawn(launch(), Time::from_nanos(100), &mut below);
    below.pop();
    process.from_io(
        Time::from_nanos(101),
        IoEvent::Spawned { owner: io_owner, child, pipes: Box::from([input, output, error]) },
        &mut above,
        &mut below,
    );
    assert_eq!(process.next_deadline(), None);
    assert_eq!(below.pop(), Some(IoRequest::Abort { entity: child }));
    assert_eq!(below.pop(), Some(IoRequest::Abort { entity: input }));
    assert_eq!(below.pop(), Some(IoRequest::Abort { entity: output }));
    assert_eq!(
        below.pop(),
        Some(IoRequest::Stream {
            stream: error,
            down: stream::Down::Demand { read: stream::Read::Arrived { max: 4096 }, room: 0 }
        })
    );
    process.from_io(
        Time::from_nanos(102),
        IoEvent::Stream { owner: error, up: stream::Up::Bytes(Box::from(&b"abcdef"[..])) },
        &mut above,
        &mut below,
    );
    below.pop();
    process.from_io(
        Time::from_nanos(102),
        IoEvent::Stream { owner: error, up: stream::Up::End },
        &mut above,
        &mut below,
    );
    assert_eq!(below.pop(), Some(IoRequest::Close { entity: error }));
    process.from_io(
        Time::from_nanos(103),
        IoEvent::Exited { owner: io_owner, exit: skein_io::kernel::Exit::Code(1) },
        &mut above,
        &mut below,
    );
    below.pop();
    below.pop();
    for owner in [input, output, error, io_owner] {
        process.from_io(Time::from_nanos(104), IoEvent::Closed { owner }, &mut above, &mut below);
    }
    match above.pop().expect("unspawned") {
        crate::ProcessEvent::Unspawned { agent: observed, detail } => {
            assert_eq!(observed, agent);
            assert_eq!(&*detail, b"cdef");
        }
        other => panic!("unexpected event: {other:?}"),
    }
    assert!(above.is_empty());
}

#[test]
fn host_stop_signals_keep_the_child_identity() {
    use alloc::boxed::Box;
    use skein_io::{Event as IoEvent, Request as IoRequest, kernel};
    use skein_lib::{Time, Token};

    let agent = Token::new(7);
    let child = Token::new(8);
    let mut process = crate::Process::new(agent, &process_limits(), 4, endpoints()).expect("channel");
    let io_owner = process.io_owner();
    let mut below = Queue::with_capacity(16);
    let mut above = Queue::with_capacity(8);
    process.spawn(launch(), Time::from_nanos(100), &mut below);
    below.pop();
    process.from_io(
        Time::from_nanos(1),
        IoEvent::Spawned { owner: io_owner, child, pipes: Box::from([Token::new(9), Token::new(10), Token::new(11)]) },
        &mut above,
        &mut below,
    );
    below.pop();
    process.signal(kernel::Signal::Terminate, &mut below);
    process.signal(kernel::Signal::Kill, &mut below);
    assert_eq!(
        below.pop(),
        Some(IoRequest::Signal { child, to: kernel::Target::Child, signal: kernel::Signal::Terminate })
    );
    assert_eq!(below.pop(), Some(IoRequest::Signal { child, to: kernel::Target::Child, signal: kernel::Signal::Kill }));
}

pub(super) fn endpoints() -> smith_protocol_channel::Endpoints {
    let mut names = skein_lib::List::with_capacity(1);
    names.push(smith_protocol_channel::Endpoint { name: Box::new([]), number: 0, dialect: 0 }).expect("one endpoint");
    smith_protocol_channel::Endpoints::new(names)
}

#[test]
fn a_delivery_ask_decodes_into_its_named_values_before_domain_ingress() {
    use alloc::boxed::Box;
    use skein_lib::{Duration, List};
    use smith_channel as wire;
    use smith_domain::run::outcome::{Change, Field};

    let limits = wire::CEILINGS;
    let source = Change { fields: Box::new([Field { name: Box::from(*b"ticket"), value: Box::from(*b"host value") }]) };
    let mut fields = List::with_capacity(1);
    fields
        .push(
            wire::Field::new(
                &limits,
                wire::FieldParts { name: source.fields[0].name.clone(), text: source.fields[0].value.clone() },
            )
            .expect("bounded field"),
        )
        .expect("one field");
    let ask = wire::Ask::Deliver(wire::DeliverAsk::new(&limits, wire::DeliverAskParts { fields }).expect("delivery"));
    let name = wire::CallName::new(&limits, wire::CallNameParts { activation: 1, completion: 2, position: 3 })
        .expect("call name");
    let record = wire::Call::new(
        &limits,
        wire::CallParts { name, effect: wire::Effect::Write, deadline: Duration::from_secs(30), ask },
    )
    .expect("call envelope");
    let Some(smith_host_domain::Ask::Deliver { fields }) = crate::translate::decode_ask(&record).expect("typed ask")
    else {
        panic!("delivery enters the typed parent face")
    };
    assert_eq!(fields.into_value(), source);
}

#[test]
fn invalid_start_round_trip_keeps_every_budget_reason_and_price_rate() {
    use skein_lib::{Duration, Reader};
    use smith_domain::run;
    use smith_host_domain::channel as host;
    let reasons = [
        (
            run::Invalid::Budget(run::BudgetRefusal::Ceiling(run::Ceiling::Turns(4096))),
            host::RunInvalid::Budget(host::BudgetRefusal::Ceiling(host::Ceiling::Turns(4096))),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Ceiling(run::Ceiling::Spend(1_000_000_000))),
            host::RunInvalid::Budget(host::BudgetRefusal::Ceiling(host::Ceiling::Spend(1_000_000_000))),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Ceiling(run::Ceiling::Time(Duration::from_secs(86_400)))),
            host::RunInvalid::Budget(host::BudgetRefusal::Ceiling(host::Ceiling::Time(Duration::from_secs(86_400)))),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Empty(run::Part::Turns)),
            host::RunInvalid::Budget(host::BudgetRefusal::Empty(host::Part::Turns)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Empty(run::Part::Spend)),
            host::RunInvalid::Budget(host::BudgetRefusal::Empty(host::Part::Spend)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Empty(run::Part::Time)),
            host::RunInvalid::Budget(host::BudgetRefusal::Empty(host::Part::Time)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Reserve(run::Part::Turns)),
            host::RunInvalid::Budget(host::BudgetRefusal::Reserve(host::Part::Turns)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Reserve(run::Part::Spend)),
            host::RunInvalid::Budget(host::BudgetRefusal::Reserve(host::Part::Spend)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Reserve(run::Part::Time)),
            host::RunInvalid::Budget(host::BudgetRefusal::Reserve(host::Part::Time)),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Checks {
                reserve: Duration::from_secs(59),
                checks: Duration::from_secs(60),
            }),
            host::RunInvalid::Budget(host::BudgetRefusal::Checks {
                reserve: Duration::from_secs(59),
                checks: Duration::from_secs(60),
            }),
        ),
        (
            run::Invalid::Budget(run::BudgetRefusal::Completion { most: 123 }),
            host::RunInvalid::Budget(host::BudgetRefusal::Completion { most: 123 }),
        ),
        (run::Invalid::Price(run::Rate::Input), host::RunInvalid::Price(host::Rate::Input)),
        (run::Invalid::Price(run::Rate::Cached), host::RunInvalid::Price(host::Rate::Cached)),
        (run::Invalid::Price(run::Rate::CacheWrite), host::RunInvalid::Price(host::Rate::CacheWrite)),
        (run::Invalid::Price(run::Rate::Output), host::RunInvalid::Price(host::Rate::Output)),
    ];
    for (reason, expected) in reasons {
        let record = smith_protocol_channel::answer_record(
            skein_lib::Time::ZERO,
            run::Answer::Refused(run::Refusal::Invalid(reason)),
            None,
            &smith_channel::CEILINGS,
            &smith_charter::CEILINGS,
        )
        .expect("bounded refusal");
        let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).expect("measured"));
        record.encode(&mut writer).expect("measured encoding");
        let encoded = writer.finish();
        let record =
            smith_channel::Answer::decode(&smith_channel::CEILINGS, &mut Reader::new(&encoded)).expect("wire refusal");
        let decoded = crate::answer::decode_answer(&record, &smith_charter::CEILINGS).expect("host refusal");
        assert_eq!(decoded.result, host::RunResult::Refused { refusal: host::Refusal::Invalid(expected) });
        assert_eq!((decoded.spent, decoded.turns), (0, 0));
    }
}

#[test]
fn the_host_reserves_its_joint_start_frame_in_its_sending_direction() {
    let mut limits = process_limits();
    limits.start_bytes = 32_065_030;
    let required = crate::output_bytes(&limits).expect("directional output");
    assert_eq!(required, limits.start_bytes + 8);
    limits.channel.output_bytes = required;
    let schema = smith_protocol_channel::schema_for_start(&limits.bodies, limits.start_bytes).expect("joint schema");
    schema.check(skein_channel::Role::Initiator, &limits.channel).expect("exact host frame allowance");
    Component::new(&limits, StreamMode::Two, endpoints()).expect("constructor uses the same joint schema");
    limits.channel.output_bytes = required - 1;
    assert_eq!(schema.check(skein_channel::Role::Initiator, &limits.channel), Err(skein_channel::SchemaError::Kind));
    match Component::new(&limits, StreamMode::Two, endpoints()) {
        Err(error) => assert_eq!(error, crate::Error::Channel(skein_channel::SchemaError::Kind)),
        Ok(_) => panic!("short host output was admitted"),
    }
    // A responder's smaller sending allowance cannot stand in for the host's Start.
    limits.channel.output_bytes = 8_388_642;
    match Component::new(&limits, StreamMode::Two, endpoints()) {
        Err(error) => assert_eq!(error, crate::Error::Channel(skein_channel::SchemaError::Kind)),
        Ok(_) => panic!("short host output was admitted"),
    }
}

#[test]
fn the_answer_carries_the_exhausted_account_and_its_remaining_wait() {
    use skein_lib::{Duration, Time};
    use smith_domain::run;
    use smith_host_domain as host;
    for (reset_at, expected) in [
        (Some(Time::from_nanos(100_000_000)), Some(Duration::from_nanos(80_000_000))),
        (Some(Time::ZERO), Some(Duration::ZERO)),
        (None, None),
    ] {
        let answer = run::Answer::Failed {
            failure: run::Failure::Model(run::Fault::Limited { account: 23, reset_at }),
            spent: run::Spend::ZERO,
            turns: 0,
        };
        let record = smith_protocol_channel::answer_record(
            Time::from_nanos(20_000_000),
            answer,
            None,
            &smith_channel::CEILINGS,
            &smith_charter::CEILINGS,
        )
        .expect("bounded answer");
        let decoded = crate::answer::decode_answer(&record, &smith_charter::CEILINGS).expect("host answer");
        assert_eq!(
            decoded.result,
            host::RunResult::Failed {
                failure: host::RunFailure::Model(host::ModelFault::Limited { account: 23, reset: expected }),
            }
        );
    }
}

#[test]
fn the_answer_carries_budget_usage_and_bound_in_the_runs_unit() {
    use skein_lib::Time;
    use smith_domain::run;
    use smith_host_domain as host;
    for (exhausted, expected) in [
        (run::Exhausted::Turns, host::Exhausted::Turns),
        (run::Exhausted::Spend, host::Exhausted::Spend),
        (run::Exhausted::Time, host::Exhausted::Time),
    ] {
        let answer = run::Answer::Failed {
            failure: run::Failure::Budget { exhausted, used: Some(1073), bound: Some(1000) },
            spent: run::Spend::ZERO,
            turns: 0,
        };
        let record = smith_protocol_channel::answer_record(
            Time::ZERO,
            answer,
            None,
            &smith_channel::CEILINGS,
            &smith_charter::CEILINGS,
        )
        .expect("bounded answer");
        let decoded = crate::answer::decode_answer(&record, &smith_charter::CEILINGS).expect("host answer");
        assert_eq!(
            decoded.result,
            host::RunResult::Failed {
                failure: host::RunFailure::Budget { exhausted: expected, used: Some(1073), bound: Some(1000) },
            }
        );
    }
}

#[test]
fn a_hosts_standard_error_keeps_its_last_bytes_whatever_its_deliveries() {
    use skein_io::{Event as IoEvent, Request as IoRequest};
    use skein_lib::{Time, Token, stream};
    for cut in [20, 1] {
        let mut process = crate::Process::new(Token::new(7), &process_limits(), 8, endpoints()).expect("channel");
        let owner = process.io_owner();
        let mut below = Queue::with_capacity(16);
        let mut above = Queue::with_capacity(8);
        process.spawn(launch(), Time::ZERO, &mut below);
        below.pop();
        let event = IoEvent::Spawned {
            owner,
            child: Token::new(8),
            pipes: Box::new([Token::new(9), Token::new(10), Token::new(11)]),
        };
        process.from_io(Time::ZERO, event, &mut above, &mut below);
        while below.pop().is_some() {}
        for bytes in b"abcdefghijklmnopqrst".chunks(cut) {
            let up = stream::Up::Bytes(Box::from(bytes));
            process.from_io(Time::ZERO, IoEvent::Stream { owner: Token::new(11), up }, &mut above, &mut below);
            assert_eq!(
                below.pop(),
                Some(IoRequest::Stream {
                    stream: Token::new(11),
                    down: stream::Down::Demand { read: stream::Read::Arrived { max: 4096 }, room: 0 }
                })
            );
        }
        for closed in [Token::new(9), Token::new(10), Token::new(11), owner] {
            process.from_io(Time::ZERO, IoEvent::Closed { owner: closed }, &mut above, &mut below);
        }
        assert_eq!(
            above.pop(),
            Some(crate::ProcessEvent::Unspawned { agent: Token::new(7), detail: Box::from(&b"mnopqrst"[..]) })
        );
        assert!(above.is_empty());
    }
}

#[test]
fn each_version_4_fault_reads_as_its_own() {
    use smith_channel as wire;
    use smith_host_domain as host;
    let limits = &wire::CEILINGS;
    for (fault, expected) in fault_rows() {
        let value = wire::ModelFaultValue::new(limits, wire::ModelFaultValueParts { value: fault }).unwrap();
        let reason = wire::RunFailure::Model(value);
        let result = wire::RunResult::Failed(wire::Failed::new(limits, wire::FailedParts { reason }).unwrap());
        let record =
            wire::Answer::new(limits, wire::AnswerParts { turns: 0, spent: 0, last_read: None, result }).unwrap();
        let decoded = crate::answer::decode_answer(&record, &smith_charter::CEILINGS).unwrap();
        assert_eq!(decoded.result, host::RunResult::Failed { failure: host::RunFailure::Model(expected) });
    }
}

fn wire_completion(failure: smith_channel::CompletionFailure) -> smith_channel::ModelFault {
    use smith_channel as wire;
    wire::ModelFault::Completion(
        wire::CompletionFault::new(
            &wire::CEILINGS,
            wire::CompletionFaultParts {
                failure,
                evidence: wire::CompletionEvidence::Unknown,
                retry_after: None,
                model: 1,
                attempts: 2,
                code: b"code".as_slice().into(),
                request_id: b"request".as_slice().into(),
                field: b"feature".as_slice().into(),
            },
        )
        .unwrap(),
    )
}

fn host_completion(failure: smith_host_domain::CompletionFailure) -> smith_host_domain::ModelFault {
    use smith_host_domain as host;
    host::ModelFault::Completion {
        failure,
        evidence: host::CompletionEvidence::Unknown,
        model: 1,
        attempts: 2,
        report: host::Report {
            code: b"code".as_slice().into(),
            request: b"request".as_slice().into(),
            field: b"feature".as_slice().into(),
        },
    }
}

fn fault_rows() -> [(smith_channel::ModelFault, smith_host_domain::ModelFault); 11] {
    use smith_channel as wire;
    use smith_host_domain as host;
    let limits = &wire::CEILINGS;
    [
        (wire::ModelFault::Provider, host::ModelFault::Provider),
        (wire::ModelFault::Declined, host::ModelFault::Declined),
        (wire::ModelFault::Truncated, host::ModelFault::Truncated),
        (wire::ModelFault::Malformed, host::ModelFault::Malformed),
        (
            wire::ModelFault::Limited(
                wire::LimitedFault::new(limits, wire::LimitedFaultParts { account: 7, reset: None }).unwrap(),
            ),
            host::ModelFault::Limited { account: 7, reset: None },
        ),
        (
            wire::ModelFault::Refused(
                wire::RefusedFault::new(
                    limits,
                    wire::RefusedFaultParts { account: 7, code: b"quota".as_slice().into() },
                )
                .unwrap(),
            ),
            host::ModelFault::Refused { account: 7, code: b"quota".as_slice().into() },
        ),
        (
            wire::ModelFault::Unauthorized(
                wire::UnauthorizedFault::new(limits, wire::UnauthorizedFaultParts { account: 8 }).unwrap(),
            ),
            host::ModelFault::Unauthorized { account: 8 },
        ),
        (
            wire::ModelFault::Program(
                wire::ProgramFault::new(
                    limits,
                    wire::ProgramFaultParts {
                        name: b"login".as_slice().into(),
                        exit: Some(2),
                        detail: b"unavailable".as_slice().into(),
                    },
                )
                .unwrap(),
            ),
            host::ModelFault::Program {
                name: b"login".as_slice().into(),
                exit: Some(2),
                detail: b"unavailable".as_slice().into(),
            },
        ),
        (wire_completion(wire::CompletionFailure::Refused), host_completion(host::CompletionFailure::Refused)),
        (wire_completion(wire::CompletionFailure::Unsupported), host_completion(host::CompletionFailure::Unsupported)),
        (
            wire_completion(wire::CompletionFailure::Exhausted),
            host_completion(host::CompletionFailure::Exhausted { retry_after: None }),
        ),
    ]
}
