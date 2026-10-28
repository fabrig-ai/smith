use super::request::domain_request;
use super::routing::{record_usage, route_io_event, route_owned_io, route_submission};
use super::teardown::Step;
use super::up::channel_event;
use super::*;
use skein_lib::Duration;
use teardown::{Answer, Close};

#[expect(clippy::too_many_lines, reason = "complete composed test limits keep each owned layer visible")]
fn limits() -> Limits {
    let client = skein_llm_world::limits();
    let decoded_call_bytes = 4096;
    let channel_bodies = smith_channel::CEILINGS;
    let schema = smith_channel::schema(&channel_bodies).expect("bounded channel schema");
    let version = schema.version(5).expect("v5");
    let mut largest = 0;
    for kind in &version.kinds {
        largest = largest.max(kind.largest);
    }
    let llm_io = io::Limits {
        sockets: 3,
        names: 0,
        refusals: 1,
        intake: 19_000,
        receive: 1024,
        output: 19_000,
        sends: 2,
        accepts: 1,
        backlog: 2,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    };
    let io = io::Limits { sockets: 16, ..llm_io };
    Limits {
        domain: smith_agent_world::LIMITS,
        channel: channel::Limits {
            bodies: channel_bodies,
            start_bytes: u32::try_from(
                smith_channel::Start::worst_case_bytes(&channel_bodies).expect("fixture Start bound"),
            )
            .expect("fixture frame"),
            charter: smith_charter::CEILINGS,
            transcript: smith_transcript::CEILINGS,
            resume_turns: 64,
            search_hits: 256,
            channel: skein_channel::Limits {
                chunk: 4096,
                credential: 0,
                skip: 4096,
                output_bytes: largest.checked_add(8).expect("largest frame"),
                output_frames: 4,
                kinds: 20,
            },
            endpoints: 1,
            calls: 8,
            turns: 8,
            fact_reserve_frames: 1,
            fact_reserve_bytes: 128,
            grants: 8,
        },
        llm: llm::ComponentLimits {
            adapter: llm::Limits {
                client,
                tool_bytes: 32_768,
                history_bytes: u64::from(client.request),
                rendered_result: client.strings,
                stated: llm::Stated::from_tools(&smith_agent_world::LIMITS.session.tools, client.input)
                    .expect("test effective quantities fit"),
            },
            connection: skein_llm_connection::Limits {
                memory: 1 << 40,
                endpoints: 1,
                connections: 2,
                calls: 2,
                per_endpoint: 2,
                idle_keep: Duration::from_secs(1),
                io: llm_io,
                tls: skein_tls::client::Limits { read: 4096, send: 4096, records: skein_tls::client::MAX_RECORD },
            },
            receiving: llm::Receiving {
                max_completion_bytes: llm::completion_worst_case(&client, decoded_call_bytes).expect("receiving bound"),
                max_completion_blocks: client.output_items,
                decoded_call_bytes,
                max_failure_bytes: client.detail_bytes,
            },
            contract_bytes: 4096,
            llms: smith_agent_world::LIMITS.run.models.checked_add(1).expect("main LLM fits"),
            accounts: 1,
            grant_value_bytes: 128,
        },
        machine: machine::Limits {
            operations: 2,
            roots: 1,
            path_bytes: 64,
            file_bytes: 16,
            tool_payload: smith_agent_world::LIMITS.session.tools.tool_payload,
            entries: 2,
            entry_bytes: 512,
            processes: 2,
            output_bytes: 64,
            search_hits: 8,
            search_bytes: 256,
            search_line_bytes: 512,
            env_bytes: 512,
            group_stop: Duration::from_millis(10),
        },
        io,
        file_slots: 4,
        file_read: 64,
        file_entries: 2,
        file_bytes: 512,
        file_timeout: Duration::from_secs(1),
        live_interval: crate::profile::standard().policy.live_interval,
        queue: domain::max_out(&smith_agent_world::LIMITS).max(256),
        routes: io::operations(&io).expect("bounded test IO").checked_add(2).expect("file routes fit"),
        memory: u64::MAX,
        events: None,
    }
}

pub(crate) fn configuration() -> Config {
    let limits = limits();
    let channel_endpoints = channel::Endpoints::new(List::with_capacity(1));
    let llm_endpoints = llm::Endpoints::new(Box::new([]), 1).expect("empty endpoint table");
    Config {
        program: None,
        limits,
        domain: domain::Config { programs: Box::new([]), endpoints: Box::new([]), models: Box::new([]) },
        channel_endpoints,
        llm_endpoints,
        environment: machine::Environment::encode(&[], &limits.machine).expect("empty environment fits"),
        stream_mode: StreamMode::Two,
    }
}

fn service() -> Service {
    Service::new(configuration(), 1).expect("bounded service")
}

#[test]
fn a_colocated_host_gets_typed_machine_terminals_without_an_agent_channel() {
    let mut effects = Effects::new(configuration(), 1).expect("bounded effects");
    effects.request(domain::Request::Probe {
        owner: Token::new(17),
        at: run::Place { root: Token::new(19), path: Box::from(b"checks".as_slice()) },
        deadline: Time::from_nanos(50),
    });
    effects.iterate(Time::ZERO, Wall::EPOCH);
    effects.iterate(Time::ZERO, Wall::EPOCH);
    let Some(domain::Event::Probed { owner, executable: false }) = effects.event() else {
        panic!("typed machine terminal")
    };
    assert_eq!(owner, Token::new(17));
    assert!(effects.event().is_none());
    assert!(effects.submissions().is_empty());
    let grant = domain::Grant {
        renewable: true,
        name: domain::GrantName { account: 0, generation: 1 },
        valid: Duration::from_secs(60),
    };
    let value = channel::GrantValue::Withdrawn { account_id: Box::from(&b"acc"[..]), bearer: Box::from(&b"token"[..]) };
    effects.grant(grant, &value, Time::ZERO).expect("first grant");
    effects.grant(grant, &value, Time::from_nanos(1)).expect("same saved generation on another activation");
    effects.close();
    for _ in 0..3_u32 {
        effects.iterate(Time::ZERO, Wall::EPOCH);
    }
    assert!(effects.closed());
}

#[test]
fn each_grant_value_installs_its_scheme() {
    use channel::GrantValue;
    use skein_llm::Scheme;
    let rows = [
        (GrantValue::Bearer(Box::from(&b"value"[..])), Scheme::Bearer, &b""[..]),
        (GrantValue::Key(Box::from(&b"value"[..])), Scheme::Key, &b""[..]),
        (
            GrantValue::Withdrawn { account_id: Box::from(&b"account"[..]), bearer: Box::from(&b"value"[..]) },
            Scheme::Bearer,
            &b"account"[..],
        ),
        (
            GrantValue::Withdrawn { account_id: Box::default(), bearer: Box::from(&b"value"[..]) },
            Scheme::Bearer,
            &b""[..],
        ),
    ];
    for (index, (input, scheme, account_id)) in rows.into_iter().enumerate() {
        let installed = credential(&input).expect("installable form");
        assert_eq!((installed.scheme, installed.account_id.as_ref()), (scheme, account_id), "row {index}");
    }
}

#[test]
fn io_routes_preserve_component_owner_and_retire_on_close() {
    let mut service = service();
    let route = service.owners.insert(Owner::Llm(Token::new(42))).expect("route");
    route_owned_io(&mut service, io::Event::Closed { owner: component_owner(route.token()) });
    assert_eq!(service.owners.len(), 1);
    service.owners.reclaim();
    assert!(service.owners.is_empty());
}

#[test]
fn machine_terminal_and_channel_message_keep_their_domain_names() {
    let mut service = service();
    let owner = Token::new(42);
    service.machine_events.push(machine::ToDomain::Probed { owner, executable: true });
    component_up(&mut service);
    match service.domain_events.pop().expect("terminal") {
        domain::Event::Probed { owner: observed, executable } => {
            assert_eq!(observed, owner);
            assert!(executable);
        }
        other => panic!("unexpected terminal: {other:?}"),
    }
    channel_event(
        &mut service,
        channel::OpenEvent::Message {
            finish_requested: true,
            name: owner,
            label: Box::from(&b"host"[..]),
            text: Box::from(&b"hello"[..]),
        },
    );
    assert_eq!(service.pending_messages.len(), 1);
    assert!(service.domain_events.is_empty());
    service.admitted = Some(Token::new(9));
    domain_down(&mut service);
    match service.domain_events.pop().expect("message") {
        domain::Event::Message { finish_requested: true, run, name, label, text } => {
            assert_eq!(label.as_ref(), b"host");
            assert_eq!(run, Token::new(9));
            assert_eq!(name, owner);
            assert_eq!(&*text, b"hello");
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn llm_and_host_terminals_reach_the_domain_with_their_owners() {
    let mut service = service();
    let owner = Token::new(73);
    service.llm_events.push(llm::ToDomain::Cancelled { owner });
    component_up(&mut service);
    match service.domain_events.pop().expect("LLM terminal") {
        domain::Event::Cancelled { owner: observed } => assert_eq!(observed, owner),
        other => panic!("unexpected LLM terminal: {other:?}"),
    }
    let relay = run::RelayName { owner: Token::new(5), attempt: 1 };
    channel_event(
        &mut service,
        channel::OpenEvent::HostReturned { relay, reply: run::HostReply::Unanswered(run::Unanswered::Lost) },
    );
    match service.domain_events.pop().expect("host terminal") {
        domain::Event::HostReturned { relay: observed, reply } => {
            assert_eq!(observed, relay);
            assert_eq!(reply, run::HostReply::Unanswered(run::Unanswered::Lost));
        }
        other => panic!("unexpected host terminal: {other:?}"),
    }
}

#[test]
fn kernel_cancel_targets_the_right_layer_operation() {
    let mut service = service();
    route_submission(
        &mut service,
        kernel::Submit { op: Token::new(11), kind: kernel::Op::Close { fd: kernel::Fd::new(3) } },
        false,
    );
    route_submission(
        &mut service,
        kernel::Submit { op: Token::new(11), kind: kernel::Op::Close { fd: kernel::Fd::new(4) } },
        true,
    );
    route_submission(
        &mut service,
        kernel::Submit { op: Token::new(12), kind: kernel::Op::Cancel { target: Token::new(11) } },
        true,
    );
    let io = service.submissions.pop().expect("io close");
    let file = service.submissions.pop().expect("file close");
    let cancel = service.submissions.pop().expect("file cancel");
    assert_ne!(io.op, file.op);
    assert_eq!(cancel.kind, kernel::Op::Cancel { target: file.op });
}

#[test]
fn termination_signal_cancels_the_admitted_run() {
    let mut service = service();
    service.admitted = Some(Token::new(91));
    route_io_event(&mut service, io::Event::Shutdown { signal: kernel::ServiceSignal::Terminate, from: None });
    match service.domain_events.pop().expect("cancel event") {
        domain::Event::Cancel { run } => assert_eq!(run, Token::new(91)),
        other => panic!("unexpected signal result: {other:?}"),
    }
}

#[test]
fn signal_during_opening_waits_for_the_admitted_run_handle() {
    let mut service = service();
    route_io_event(&mut service, io::Event::Shutdown { signal: kernel::ServiceSignal::Interrupt, from: None });
    assert!(service.cancel_pending);
    service.admitted = Some(Token::new(19));
    domain_down(&mut service);
    match service.domain_events.pop().expect("deferred cancel") {
        domain::Event::Cancel { run } => assert_eq!(run, Token::new(19)),
        other => panic!("unexpected deferred result: {other:?}"),
    }
}

#[test]
fn channel_loss_before_answer_keeps_a_concrete_failure_reason() {
    let mut service = service();
    channel_event(&mut service, channel::OpenEvent::Ended { why: skein_channel::Closed::Truncated });
    assert_eq!(failure(&service), Some(Failure::ChannelEnded));
    assert_eq!(
        service.teardown,
        Teardown::Answered { at: Time::ZERO, answer: Answer::Unsent(Failure::ChannelEnded), close: Close::Graceful }
    );
}

#[test]
fn the_answer_requests_every_close_in_one_pass() {
    let mut service = service();
    service.input = Some(Token::new(11));
    service.output = Some(Token::new(12));
    service.signals = Some(Token::new(13));
    service.mark_failed(Failure::Send);
    teardown::answered(&mut service);
    assert!(!service.llm.closed());
    teardown::advance(&mut service);
    assert!(service.llm.closed(), "an empty pool reports its close immediately");
    for expected in [11, 12] {
        match service.io_requests.pop() {
            Some(io::Request::Close { entity }) => assert_eq!(entity, Token::new(expected)),
            unexpected => panic!("one close per descriptor: {unexpected:?}"),
        }
    }
    assert_eq!(
        service.teardown,
        Teardown::Closing {
            at: Time::ZERO,
            answer: Answer::Unsent(Failure::Send),
            close: Close::Graceful,
            step: Step::Components,
        }
    );
}

#[test]
fn a_sent_answer_keeps_its_channel_output_right_until_finish() {
    let mut service = service();
    service.input = Some(Token::new(11));
    service.output = Some(Token::new(12));
    teardown::answered(&mut service);
    teardown::advance(&mut service);
    match service.io_requests.pop() {
        Some(io::Request::Close { entity }) => assert_eq!(entity, Token::new(11)),
        unexpected => panic!("input close: {unexpected:?}"),
    }
    assert!(service.io_requests.is_empty(), "the channel owns its queued frames and output finish");
}

#[test]
fn an_answer_requests_its_close_at_once() {
    let mut service = service();
    service.input = Some(Token::new(11));
    for _ in 0..service.limits.queue {
        service.io_requests.push(io::Request::Usage { owner: Token::new(1) });
    }
    teardown::answered(&mut service);
    teardown::advance(&mut service);
    assert_eq!(
        service.teardown,
        Teardown::Closing { at: Time::ZERO, answer: Answer::Sent, close: Close::Graceful, step: Step::Components }
    );
    assert!(service.llm.closed());
    for _ in 0..service.limits.queue {
        let Some(io::Request::Usage { .. }) = service.io_requests.pop() else {
            panic!("the existing IO requests keep their order")
        };
    }
    let Some(io::Request::Close { entity }) = service.io_requests.pop() else {
        panic!("the input close is requested immediately")
    };
    assert_eq!(entity, Token::new(11));
    assert_eq!(done(&service), None);
}

#[test]
fn done_is_some_only_when_closed() {
    let mut service = service();
    service.channel = None;
    assert_eq!(done(&service), None);
    teardown::answered(&mut service);
    assert_eq!(done(&service), None);
    teardown::advance(&mut service);
    service.operations.insert(Token::new(77), Operation::Io(Token::new(4))).expect("one route");
    teardown::advance(&mut service);
    assert_eq!(done(&service), None);
    service.operations.remove(&Token::new(77));
    teardown::advance(&mut service);
    assert_eq!(
        service.teardown,
        Teardown::Closing { at: Time::ZERO, answer: Answer::Sent, close: Close::Graceful, step: Step::Stream }
    );
    assert_eq!(done(&service), None);
    assert!(work_pending(&service, Time::ZERO), "the last close wakes the phase after reclaim");
    teardown::advance(&mut service);
    assert_eq!(done(&service), Some(true));
}

pub(crate) fn account_service() -> Service {
    let mut agent = event_service();
    agent.channel = service().channel;
    agent
        .events
        .as_mut()
        .unwrap()
        .sinks()
        .add(crate::sinks::Destination::Trace, Token::new(400), crate::sinks::Delivery::Complete)
        .unwrap();
    agent
}

pub(crate) fn account_request(agent: &mut Service, request: domain::Request) -> smith_events::Record {
    domain_request(agent, request);
    let records = crate::sinks::tests::drain_ready(agent.events.as_mut().unwrap().sinks(), Token::new(400));
    assert_eq!(records.len(), 1);
    records.into_iter().next().unwrap()
}

fn event_service() -> Service {
    let mut config = configuration();
    config.limits.events = Some(
        crate::events::EventLimits::derive(
            &config.limits,
            crate::sinks::Capture::Calls,
            1,
            0,
            &[],
            Duration::from_secs(1),
        )
        .expect("bounded event configuration"),
    );
    let mut service = Service::new(config, 1).expect("bounded producer service");
    service.channel = None;
    service.start_session(
        Time::ZERO,
        smith_events::SessionStarted {
            chat: None,
            wall_ms: 0,
            agent: smith_events::Agent { name: b"test".as_slice().into(), version: Box::default() },
            mode: smith_events::Mode::Agent,
            containment: smith_events::Containment::Groups,
            pid: 1,
            profile: b"test".as_slice().into(),
            capture: smith_events::Capture::Calls,
            delivery: smith_events::Delivery::Complete,
            versions: smith_events::Versions { events: 4, channel: 5, charter: 4, transcript: 6, chat_store: None },
            limits: List::with_capacity(0),
        },
    );
    service
}

#[test]
fn a_settled_event_producer_is_runnable_before_its_usage_request() {
    let mut service = event_service();
    teardown::answered(&mut service);
    teardown::advance(&mut service);
    assert!(work_pending(&service, Time::ZERO), "usage must be requested before the kernel waits");
    teardown::advance(&mut service);
    match service.io_requests.pop() {
        Some(io::Request::Usage { owner }) => assert_eq!(owner, USAGE_OWNER),
        unexpected => panic!("usage request: {unexpected:?}"),
    }
    assert_eq!(done(&service), None);
    assert!(!teardown::work_pending(&service), "the usage terminal is owed exactly once");
    teardown::advance(&mut service);
    assert!(service.io_requests.is_empty());
}

#[test]
fn usage_writes_the_session_end_before_the_stream_closes() {
    let mut service = event_service();
    teardown::answered(&mut service);
    teardown::advance(&mut service);
    teardown::advance(&mut service);
    service.io_requests.pop().expect("usage request");
    record_usage(&mut service, io::Measured::Unread);
    assert!(service.pop_usage_unread());
    assert_eq!(
        service.teardown,
        Teardown::Closing { at: Time::ZERO, answer: Answer::Sent, close: Close::Graceful, step: Step::Stream }
    );
    assert_eq!(done(&service), None);
    teardown::advance(&mut service);
    assert_eq!(done(&service), Some(true));
}

// A full kernel edge holds internal IO submissions at the watermark across
// an iteration. Only the pending internal submissions contribute to backlog.
fn at_watermark() -> Service {
    let mut config = configuration();
    config.limits.queue = 8;
    let mut service = Service::new(config, 1).expect("small watermark service");
    service.admitted = Some(Token::new(9));
    for number in 0..service.limits.queue {
        service.submissions.push(kernel::Submit {
            op: Token::new(u64::from(number)),
            kind: kernel::Op::Close { fd: kernel::Fd::new(90) },
        });
    }
    assert_eq!(backlog(&service), 0, "kernel-edge submissions are outside admission");
    assert_eq!(service.submissions.capacity(), service.limits.queue);
    assert_eq!(service.completions.capacity(), service.limits.queue);
    for number in 0..service.limits.queue {
        service.io_submissions.push(kernel::Submit {
            op: Token::new(u64::from(number)),
            kind: kernel::Op::Close { fd: kernel::Fd::new(91) },
        });
    }
    assert_eq!(backlog(&service), u64::from(service.limits.queue));
    service
}

fn host_message(name: u64) -> channel::OpenEvent {
    channel::OpenEvent::Message {
        finish_requested: false,
        name: Token::new(name),
        label: Box::from(b"host".as_slice()),
        text: Box::from(b"hello".as_slice()),
    }
}

#[test]
fn effects_report_their_handoffs_backlog() {
    let mut effects = Effects::new(configuration(), 1).expect("bounded effects");
    let owner = Token::new(17);
    assert_eq!(effects.backlog(), 0);
    effects.request(domain::Request::Cancel { owner });
    assert_eq!(effects.backlog(), 1, "one domain request waits for the lower stages");
    effects.io_event(io::Event::Resized);
    assert_eq!(effects.backlog(), 2, "one IO event waits for routing");
    effects.io_requests().push(io::Request::Close { entity: owner });
    assert_eq!(effects.backlog(), 3, "one IO request waits for the colocated owner");
    effects.io_requests().pop().expect("the colocated owner takes its request");
    assert_eq!(effects.backlog(), 2);
}

#[test]
fn a_message_waits_at_the_watermark_and_is_taken_once_the_backlog_falls() {
    let mut service = at_watermark();
    service.channel_events.push(host_message(31));
    service.channel_events.push(host_message(32));
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert_eq!(service.channel_events.len(), 2, "neither message entered the domain");
    assert!(service.domain_events.is_empty());
    assert_eq!(backlog(&service), u64::from(service.limits.queue));
    for _ in 0_u32..2 {
        service.submissions.pop().expect("shell takes two submissions");
    }
    io_down(&mut service);
    assert_eq!(service.io_submissions.len(), service.limits.queue - 2);
    // Inspect this iteration's domain-facing boundary before domain_up consumes it.
    component_up(&mut service);
    assert!(service.channel_events.is_empty());
    for expected in [31, 32] {
        let Some(domain::Event::Message { run, name, .. }) = service.domain_events.pop() else {
            panic!("message reaches the domain in host order")
        };
        assert_eq!(run, Token::new(9));
        assert_eq!(name, Token::new(expected));
    }
}

fn preparing_message_run(service: &mut Service) -> Token {
    let mut charter = smith_agent_world::scripted_charter(&smith_agent_world::Settings::calm(1));
    charter.models = Box::default();
    charter.llm.account = None;
    let model = &charter.llm;
    service.domain = Some(domain::Domain::new(
        &service.limits.domain,
        domain::Config {
            programs: Box::default(),
            endpoints: Box::new([model.endpoint]),
            models: Box::new([domain::ConfiguredModel {
                endpoint: model.endpoint,
                model: model.model.clone(),
                efforts: Box::default(),
                prices: model.prices,
                window: model.window,
                output: model.output,
                protocol_allowance: 0,
                head: Duration::from_secs(60),
                idle: Duration::from_secs(30),
            }]),
        },
        1,
    ));
    service.events.as_mut().unwrap().starting(1, 0, &charter);
    let domain = service.domain.as_mut().unwrap();
    domain::step(
        domain,
        &service.domain_env,
        domain::Event::Start {
            reply_to: ReplyTo::new(Token::new(9)),
            host_run: Token::new(9),
            activation: 1,
            identity: None,
            window: domain::Window { turns: u32::MAX, bytes: u64::MAX },
            messages: Box::default(),
            charter,
            workspace: Some(run::Workspace {
                directories: Box::new([run::Directory {
                    name: Box::from(b"work".as_slice()),
                    host: Box::from(b"/work".as_slice()),
                    root: Token::new(900),
                    writable: true,
                    git: true,
                    conflicts: Box::default(),
                }]),
            }),
            grants: Box::default(),
            transcript: None,
            answered: Box::default(),
        },
        &mut service.domain_requests,
    );
    let run = match service.domain_requests.pop() {
        Some(domain::Request::Admitted { run, .. }) => run,
        unexpected => panic!("the run waits in preparation: {unexpected:?}"),
    };
    // Preparation's effects and facts precede the two entrances under test.
    while service.domain_requests.pop().is_some() {}
    while domain.pop_fact().is_some() {}
    run
}

#[test]
fn a_domain_entrance_waits_until_the_previous_entrances_facts_reach_the_sinks() {
    const FIRST: u64 = 31;
    const SECOND: u64 = 32;
    const TRACE: Token = Token::new(400);
    // One message fact from each of the two queued entrances.
    const FACTS: u32 = 2;
    let mut service = event_service();
    let run = preparing_message_run(&mut service);
    let sinks = service.events.as_mut().unwrap().sinks();
    sinks.add(crate::sinks::Destination::Trace, TRACE, crate::sinks::Delivery::Complete).unwrap();
    // As in the slow complete-destination fixture, withhold its IO credit
    // and fill ordinary lines until the sink stops accepting observations.
    for _ in 0..service.limits.queue {
        if !sinks.takes() {
            break;
        }
        sinks.record(&smith_events::Record {
            t_ms: 0,
            event: smith_events::Event::MessageUnread(smith_events::MessageUnread { run: 1, name: 0 }),
        });
    }
    for name in [FIRST, SECOND] {
        service.domain_events.push(domain::Event::Message {
            finish_requested: false,
            run,
            name: Token::new(name),
            label: Box::from(b"host".as_slice()),
            text: Box::from(b"hello".as_slice()),
        });
    }
    domain_up(&mut service);
    assert!(!service.events.as_ref().unwrap().takes(), "the complete sink is stalled");
    assert!(service.domain_requests.is_empty(), "effects do not hold the next entrance");
    assert!(service.domain.as_ref().unwrap().has_facts(), "the first entrance awaits its sink");
    assert_eq!(service.domain_events.len(), 1, "the second entrance has not run");
    domain_up(&mut service);
    assert_eq!(service.domain_events.len(), 1, "waiting again cannot advance the second entrance");
    crate::sinks::tests::drain_ready(service.events.as_mut().unwrap().sinks(), TRACE);
    let mut names = List::with_capacity(FACTS);
    for _ in [FIRST, SECOND] {
        domain_up(&mut service);
        for record in crate::sinks::tests::drain_ready(service.events.as_mut().unwrap().sinks(), TRACE) {
            match record.event {
                smith_events::Event::MessageReceived(message) => {
                    names.push(message.name).expect("one fact per entrance");
                }
                unexpected => panic!("each entrance emits its message fact: {unexpected:?}"),
            }
        }
    }
    assert_eq!(&*names.into_boxed(), &[FIRST, SECOND], "all first-entrance facts reach the sink before the second's");
    assert!(service.domain_events.is_empty());
    assert!(!service.domain.as_ref().unwrap().has_facts());
}

#[test]
fn an_answer_is_taken_past_the_watermark() {
    let mut service = at_watermark();
    // These advance previously admitted work and can increase the backlog.
    let relay = run::RelayName { owner: Token::new(5), attempt: 1 };
    service
        .channel_events
        .push(channel::OpenEvent::HostReturned { relay, reply: run::HostReply::Unanswered(run::Unanswered::Lost) });
    service.channel_events.push(channel::OpenEvent::Acknowledged { turn: 1 });
    service.channel_events.push(channel::OpenEvent::Cancel);
    service.llm_events.push(llm::ToDomain::Cancelled { owner: Token::new(73) });
    component_up(&mut service);
    assert!(service.channel_events.is_empty());
    assert!(service.llm_events.is_empty());
    assert!(backlog(&service) > u64::from(service.limits.queue));
    let Some(domain::Event::HostReturned { relay: observed, .. }) = service.domain_events.pop() else {
        panic!("host answer advances")
    };
    assert_eq!(observed, relay);
    let Some(domain::Event::Acknowledge { turn, .. }) = service.domain_events.pop() else {
        panic!("acknowledgement advances")
    };
    assert_eq!(turn, 1);
    let Some(domain::Event::Cancel { .. }) = service.domain_events.pop() else { panic!("cancellation advances") };
    let Some(domain::Event::Cancelled { owner }) = service.domain_events.pop() else {
        panic!("provider terminal advances")
    };
    assert_eq!(owner, Token::new(73));
}

#[test]
fn pending_messages_wait_at_the_watermark() {
    let mut service = at_watermark();
    service.pending_messages.push(PendingMessage {
        finish_requested: false,
        name: Token::new(31),
        label: Box::from(b"host".as_slice()),
        text: Box::from(b"hello".as_slice()),
    });
    domain_down(&mut service);
    assert_eq!(service.pending_messages.len(), 1);
    assert!(service.domain_events.is_empty());
    service.submissions.pop().expect("shell takes one submission");
    io_down(&mut service);
    domain_down(&mut service);
    assert!(service.pending_messages.is_empty());
    let Some(domain::Event::Message { name, .. }) = service.domain_events.pop() else {
        panic!("retained message reaches the domain")
    };
    assert_eq!(name, Token::new(31));
}

#[test]
fn startup_accepts_a_queue_below_its_childrens_max_out() {
    let mut config = configuration();
    config.limits.queue = 1;
    let service = Service::new(config, 1).expect("one-entry watermark builds");
    assert_eq!(service.channel_below.capacity(), 1, "host edge stays capped");
    // Effects exercises the same constructors and stages without the channel's
    // retained six-cell edge reservation, which a one-cell frame queue cannot meet.
    let mut config = configuration();
    config.limits.queue = 1;
    let mut effects = Effects::new(config, 1).expect("one-entry effects builds");
    effects.request(domain::Request::Probe {
        owner: Token::new(17),
        at: run::Place { root: Token::new(19), path: Box::from(b"checks".as_slice()) },
        deadline: Time::from_nanos(50),
    });
    effects.iterate(Time::ZERO, Wall::EPOCH);
    effects.iterate(Time::ZERO, Wall::EPOCH);
    let Some(domain::Event::Probed { owner, executable: false }) = effects.event() else {
        panic!("one-entry effects reaches its typed terminal")
    };
    assert_eq!(owner, Token::new(17));
    assert!(effects.event().is_none());
    effects.close();
    for _ in 0_u32..3 {
        effects.iterate(Time::ZERO, Wall::EPOCH);
    }
    assert!(effects.closed());
}

#[test]
fn a_start_waits_at_the_watermark_before_attaching_its_roots() {
    let mut service = at_watermark();
    service.admitted = None;
    service.channel_events.push(channel::OpenEvent::Start {
        start: Box::new(channel::DecodedStart {
            live: None,
            identity: None,
            messages: Box::default(),
            activation: 1,
            charter: smith_agent_world::scripted_charter(&smith_agent_world::Settings::calm(1)),
            mounts: Some(Box::new([channel::Mount {
                name: Box::from(b"work".as_slice()),
                path: Box::from(b"/work".as_slice()),
                writable: false,
                git: false,
                conflicts: Box::default(),
            }])),
            transcript: None,
            answered: Box::default(),
            grants: Box::default(),
            window: domain::Window { turns: 8, bytes: 4096 },
        }),
    });
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert_eq!(service.channel_events.len(), 1);
    assert!(service.root_to_open().is_none());
    service.submissions.pop().expect("shell takes one submission");
    io_down(&mut service);
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert!(service.channel_events.is_empty());
    assert_eq!(service.root_to_open(), Some(b"/work".as_slice()));
}

#[test]
fn an_uncapped_drain_stops_at_the_iterations_work_bound() {
    let mut service = at_watermark();
    for _ in 0..=service.limits.queue {
        service.io_events.push(io::Event::Resized);
    }
    lower_events(&mut service);
    assert_eq!(service.io_events.len(), 1, "one iteration drains only queue entries");
}

#[test]
fn kernel_submission_credit_holds_both_lower_routes() {
    let mut config = configuration();
    config.limits.queue = 1;
    let mut service = Service::new(config, 1).expect("one kernel-edge cell");
    service.submissions.push(kernel::Submit { op: Token::new(1), kind: kernel::Op::Close { fd: kernel::Fd::new(90) } });
    service
        .io_submissions
        .push(kernel::Submit { op: Token::new(2), kind: kernel::Op::Close { fd: kernel::Fd::new(91) } });
    service
        .file_submissions
        .push(kernel::Submit { op: Token::new(3), kind: kernel::Op::Close { fd: kernel::Fd::new(92) } });
    io_down(&mut service);
    assert_eq!(service.io_submissions.len(), 1);
    assert_eq!(service.file_submissions.len(), 1);
    assert!(service.operations.is_empty(), "neither internal route crossed the full edge");
    service.submissions.pop().expect("shell takes its first submission");
    io_down(&mut service);
    assert!(service.io_submissions.is_empty(), "IO keeps priority at the edge");
    assert_eq!(service.file_submissions.len(), 1);
    service.submissions.pop().expect("shell takes the IO submission");
    io_down(&mut service);
    assert!(service.file_submissions.is_empty());
    let submit = service.submissions.pop().expect("file submission now crosses the edge");
    assert_eq!(submit.kind, kernel::Op::Close { fd: kernel::Fd::new(92) });
    assert_eq!(backlog(&service), 0);
}

#[test]
fn work_pending_waits_on_the_kernel_for_a_message_at_the_watermark() {
    let mut service = at_watermark();
    service.channel_events.push(host_message(31));
    assert!(!work_pending(&service, Time::ZERO), "the kernel holds the only runnable route");
    assert!(!channel_event_ready(&service));
    service.submissions.pop().expect("kernel takes one submission");
    assert!(work_pending(&service, Time::ZERO), "IO can now leave the backlog");
    io_down(&mut service);
    assert!(channel_event_ready(&service), "the message now fits admission");
    component_up(&mut service);
    assert!(service.channel_events.is_empty());
}

#[test]
fn work_pending_waits_on_the_kernel_for_a_pending_message_at_the_watermark() {
    let mut service = at_watermark();
    service.pending_messages.push(PendingMessage {
        finish_requested: false,
        name: Token::new(31),
        label: Box::from(b"host".as_slice()),
        text: Box::from(b"hello".as_slice()),
    });
    assert!(!work_pending(&service, Time::ZERO));
    service.submissions.pop().expect("kernel takes one submission");
    assert!(work_pending(&service, Time::ZERO));
    io_down(&mut service);
    domain_down(&mut service);
    assert!(service.pending_messages.is_empty());
}

mod validity {
    use super::*;

    mod effects {
        use super::*;

        #[test]
        fn invalid_limits_are_refused() {
            let config = configuration();
            let base = config.limits;
            assert!(effects_valid(&base, &config.llm_endpoints));
            let mut cases = vec![
                Limits { file_slots: 0, ..base },
                Limits { file_read: 0, ..base },
                Limits { file_entries: 0, ..base },
                Limits { file_bytes: 0, ..base },
                Limits { file_timeout: Duration::ZERO, ..base },
            ];
            let mut limits = base;
            limits.machine.processes = u32::MAX;
            cases.push(limits);
            let mut limits = base;
            limits.llm.accounts = 0;
            cases.push(limits);
            let mut limits = base;
            limits.events = Some(crate::EventLimits {
                sinks: crate::sinks::tests::limits(1),
                capture: crate::sinks::Capture::None,
                front: false,
                write_deadline: Duration::from_secs(1),
            });
            limits.events.as_mut().unwrap().sinks.output = 0;
            cases.push(limits);
            for limits in cases {
                assert!(!effects_valid(&limits, &config.llm_endpoints));
            }
            // Effects has no root, framed channel or native IO owner.
            let mut limits = base;
            limits.domain.run.conversations = u32::MAX;
            limits.channel.start_bytes = 0;
            limits.io.sockets = 0;
            assert!(effects_valid(&limits, &config.llm_endpoints));
            let mut config = configuration();
            config.limits.file_read = 0;
            assert!(matches!(Effects::new(config, 1), Err(ConfigError::Limits)));
        }
    }

    mod native_io {
        use super::*;

        #[test]
        fn invalid_limits_are_refused() {
            let base = limits();
            assert!(io_valid(&base));
            for native in [
                io::Limits { sockets: u32::MAX, ..base.io },
                io::Limits { sockets: u32::MAX / 8 + 1, ..base.io },
                io::Limits { sockets: 1, ..base.io },
                io::Limits { sockets: 2, ..base.io },
                io::Limits { intake: base.io.intake + 1, ..base.io },
                io::Limits { receive: base.io.receive + 1, ..base.io },
                io::Limits { output: base.io.output + 1, ..base.io },
                io::Limits { sends: base.io.sends + 1, ..base.io },
                io::Limits { refusals: 0, ..base.io },
                io::Limits { accepts: 0, ..base.io },
                io::Limits { retry: Duration::ZERO, ..base.io },
            ] {
                assert!(!io_valid(&Limits { io: native, ..base }), "{native:?}");
            }
            let mut limits = base;
            limits.events = Some(crate::EventLimits {
                sinks: crate::sinks::tests::limits(u32::MAX),
                capture: crate::sinks::Capture::None,
                front: false,
                write_deadline: Duration::from_secs(1),
            });
            assert!(!io_valid(&limits));
            limits.events.as_mut().unwrap().sinks.destinations = base.io.sockets - base.llm.connection.connections;
            assert!(!io_valid(&limits));
            limits.events.as_mut().unwrap().sinks.destinations -= 1;
            assert!(io_valid(&limits));
            // Only the four shared buffer capacities must agree.
            limits.llm.connection.io.refusals += 1;
            assert!(io_valid(&limits));
        }
    }

    #[test]
    fn invalid_limits_are_refused() {
        let config = configuration();
        assert!(valid(&config.limits, &config.llm_endpoints));
        for domain_mismatch in [false, true] {
            let mut config = configuration();
            if domain_mismatch {
                config.limits.domain.run.conversations = config.limits.domain.session.sessions + 1;
            } else {
                config.limits.io.receive += 1;
            }
            assert!(!valid(&config.limits, &config.llm_endpoints));
            assert!(matches!(Service::new(config, 1), Err(ConfigError::Limits)));
        }
        let mut config = configuration();
        config.limits.channel.start_bytes = 0;
        assert!(!valid(&config.limits, &config.llm_endpoints));
        assert!(matches!(Service::new(config, 1), Err(ConfigError::Limits)));
    }
}
