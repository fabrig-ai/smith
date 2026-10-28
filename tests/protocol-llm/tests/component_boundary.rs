//! Component boundary controls before any peer traffic and across a refresh.

use core::net::SocketAddr;

use skein_io::Request as IoRequest;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_llm::{self as shared, Credential};
use skein_world::domain::heap::{Counting, Meter};
use smith_domain::{GrantName, llm, run, tools};
use smith_protocol_llm_world::adapter::{
    self as adapter, Component, ComponentLimits, ConfiguredEndpoint, Endpoints, FromDomain, Receiving, ToDomain,
};

#[global_allocator]
static HEAP: Counting = Counting;

fn limits() -> ComponentLimits {
    let client = skein_llm_world::limits();
    let adapter = adapter::Limits {
        client,
        tool_bytes: 32_768,
        history_bytes: u64::from(client.request),
        rendered_result: client.strings,
        stated: smith_protocol_llm_world::adapter::stated(),
    };
    let decoded_call_bytes = 4096;
    ComponentLimits {
        adapter,
        connection: skein_llm_connection::Limits {
            memory: 1 << 40,
            endpoints: 1,
            connections: 2,
            calls: 2,
            per_endpoint: 2,
            idle_keep: smith_agent_service::profile::standard().policy.connection_keep,
            io: skein_io::Limits {
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
            },
            tls: skein_tls::client::Limits {
                read: skein_llm::client::largest_read(&client),
                send: 4096,
                records: skein_tls::client::MAX_RECORD,
            },
        },
        receiving: Receiving {
            max_completion_bytes: adapter::completion_worst_case(&client, decoded_call_bytes).expect("receiving bound"),
            max_completion_blocks: client.output_items,
            decoded_call_bytes,
            max_failure_bytes: client.detail_bytes,
        },
        contract_bytes: 4096,
        llms: 1,
        accounts: 1,
        grant_value_bytes: 128,
    }
}

fn endpoints() -> Endpoints {
    endpoints_with_connect(Duration::from_secs(1))
}

fn endpoints_with_connect(connect: Duration) -> Endpoints {
    endpoints_with_client(connect, limits().adapter.client)
}

fn endpoints_with_client(connect: Duration, client: skein_llm::client::Limits) -> Endpoints {
    let mut roots = skein_tls::RootCertStore::empty();
    roots.add(skein_tls::CertificateDer::from(include_bytes!("../fixtures/root.der").to_vec())).expect("fixture root");
    Endpoints::new(
        Box::new([ConfiguredEndpoint {
            connect,
            handshake: skein_lib::Duration::from_secs(1),
            name: llm::Endpoint(42),
            destination: skein_llm_connection::Endpoint {
                limits: client,
                credential: skein_llm::client::CredentialLimits {
                    value: limits().grant_value_bytes,
                    account_id: limits().grant_value_bytes,
                },
                address: SocketAddr::from(([127, 0, 0, 1], 443)),
                transport: skein_llm_connection::Transport::Tls {
                    server_name: skein_tls::Name::new("example.test").expect("server name"),
                    trust: skein_tls::Config::new(roots, &[]).expect("test trust"),
                },
                llm: shared::Endpoint::codex(),
            },
            identity: smith_protocol_llm::IdentityProfile::Plain,
            models: Box::new([]),
        }]),
        1,
    )
    .expect("one endpoint")
}

fn prompt() -> llm::Prompt {
    llm::Prompt {
        effort: None,
        purpose: llm::Purpose::Ordinary { guard: None },
        choice: smith_domain::llm::ToolChoice::Auto,
        namespace: smith_domain::tools::Namespace::One,
        affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
        asker: llm::Asker::Main,
        endpoint: llm::Endpoint(42),
        model: b"fixture-model".as_slice().into(),
        system: b"testing component".as_slice().into(),
        tools: tools::Grants { inspect: false, modify: false, shell: false },
        served: Box::new([]),
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"hello".as_slice().into(), replay: None }]),
        }]),
        max_tokens: 128,
    }
}

fn complete(owner: u64, generation: u64, bounds: Receiving) -> FromDomain {
    FromDomain::Complete {
        owner: Token::new(owner),
        grant: Some(GrantName { account: 0, generation }),
        prompt: prompt(),
        deadlines: smith_domain::session::Deadlines {
            head: Duration::from_secs(2),
            idle: Duration::from_secs(3),
            whole: Time::from_nanos(10_000_000_000),
        },
        bounds,
        outcome: run::outcome::OutcomeSpec { change: None, verdicts: Box::new([]), report: None, failure: None },
        deliver: None,
        observation: None,
        deltas: smith_protocol_llm::Deltas { text: false, input: false },
    }
}

fn env(now: u64, limits: &ComponentLimits) -> Env<ComponentLimits> {
    Env { now: Time::from_nanos(now), wall: Wall::from_nanos(1_893_456_000_000_000_000), limits: *limits }
}

fn credential(value: &[u8]) -> Credential {
    Credential { value: value.into(), account_id: b"account".as_slice().into(), scheme: skein_llm::Scheme::Bearer }
}

#[test]
fn invalid_limits_are_refused() {
    let bounds = limits();
    assert!(adapter::component_valid(&bounds, &endpoints()));
    Component::new(&bounds, endpoints()).expect("valid component");
    let mut cases = Vec::new();
    for check in 0..12 {
        let mut invalid = bounds;
        let name = match check {
            0 => {
                invalid.accounts = 0;
                "accounts"
            }
            1 => {
                invalid.llms = 0;
                "LLMs"
            }
            2 => {
                invalid.accounts = u32::MAX;
                "grant generations"
            }
            3 => {
                invalid.connection.endpoints = 0;
                "endpoints"
            }
            4 => {
                invalid.connection.connections = 0;
                invalid.connection.calls = 0;
                invalid.connection.per_endpoint = 0;
                "connections"
            }
            5 => {
                invalid.connection.calls = 0;
                "calls"
            }
            6 => {
                invalid.connection.per_endpoint = 0;
                "per endpoint"
            }
            7 => {
                invalid.connection.calls = invalid.connection.connections + 1;
                "calls exceed connections"
            }
            8 => {
                invalid.connection.per_endpoint = invalid.connection.connections + 1;
                "per endpoint exceeds connections"
            }
            9 => {
                invalid.adapter.rendered_result = 0;
                "adapter"
            }
            10 => {
                invalid.connection.memory = 0;
                "call reservation"
            }
            11 => {
                invalid.connection.tls.read = 0;
                "TLS edge"
            }
            _ => unreachable!(),
        };
        cases.push((name, invalid));
    }
    for (check, invalid) in cases {
        assert!(!adapter::component_valid(&invalid, &endpoints()), "{check}");
        assert!(matches!(Component::new(&invalid, endpoints()), Err(adapter::ComponentError::Limits)), "{check}");
    }
    let mut incompatible = bounds;
    incompatible.adapter.client.tools -= 1;
    assert!(matches!(Component::new(&incompatible, endpoints()), Err(adapter::ComponentError::ClientLimits)));
    let mut large = bounds;
    large.contract_bytes = u64::MAX;
    assert!(adapter::component_valid(&large, &endpoints()), "retained heap total is not validity");
}

#[test]
fn a_lapsed_grant_fails_unsent_and_refresh_starts_a_call() {
    let limits = limits();
    let mut component = Component::new(&limits, endpoints()).expect("component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, credential(b"old"), Time::from_nanos(10))
        .expect("first grant");
    component.from_domain(&env(11, &limits), complete(7, 1, limits.receiving), &mut up, &mut io);
    assert!(io.is_empty(), "lapsed grant never touches io");
    assert!(matches!(
        up.pop(),
        Some(ToDomain::Failed {
            owner,
            failure: llm::Failure::Unauthorized,
            evidence: llm::Evidence::Unsent,
            ..
        }) if owner == Token::new(7)
    ));
    component
        .grant(GrantName { account: 0, generation: 2 }, credential(b"fresh"), Time::from_nanos(100))
        .expect("refresh");
    component.from_domain(&env(12, &limits), complete(8, 2, limits.receiving), &mut up, &mut io);
    assert!(matches!(io.pop(), Some(IoRequest::Connect { .. })), "refreshed grant starts a connection");
    assert!(up.is_empty());
}

#[test]
fn a_grant_refreshed_between_calls_preserves_the_first_start() {
    let limits = limits();
    let mut component = Component::new(&limits, endpoints()).expect("component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, credential(b"first"), Time::from_nanos(100))
        .expect("first grant");
    component.from_domain(&env(1, &limits), complete(7, 1, limits.receiving), &mut up, &mut io);
    assert!(matches!(io.pop(), Some(IoRequest::Connect { .. })));
    component
        .grant(GrantName { account: 0, generation: 2 }, credential(b"second"), Time::from_nanos(100))
        .expect("refresh");
    component.from_domain(&env(2, &limits), complete(8, 2, limits.receiving), &mut up, &mut io);
    assert!(matches!(io.pop(), Some(IoRequest::Connect { .. })));
    assert!(up.is_empty(), "both calls accepted with distinct generations");
}

#[test]
fn a_connect_deadline_keeps_timed_out_failure_evidence() {
    let limits = limits();
    let mut component = Component::new(&limits, endpoints()).expect("component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, credential(b"first"), Time::from_nanos(20_000_000_000))
        .expect("grant");
    component.from_domain(&env(0, &limits), complete(7, 1, limits.receiving), &mut up, &mut io);
    assert!(matches!(io.pop(), Some(IoRequest::Connect { .. })));
    component.fire(&env(1_000_000_001, &limits), &mut up, &mut io);
    assert!(matches!(
        up.pop(),
        Some(ToDomain::Failed {
            owner,
            failure: llm::Failure::TimedOut { phase: llm::Phase::Connect },
            evidence: llm::Evidence::Unsent,
            ..
        }) if owner == Token::new(7)
    ));
}

fn shared_failure_cases() -> [(shared::Failure, llm::Failure); 18] {
    let retry = Duration::from_secs(7);
    [
        (shared::Failure::Refused, llm::Failure::Refused),
        (shared::Failure::Unsupported, llm::Failure::Unsupported),
        (shared::Failure::Exhausted { retry_after: None }, llm::Failure::Exhausted { retry_after: None }),
        (shared::Failure::Unauthorized, llm::Failure::Unauthorized),
        (shared::Failure::Exhausted { retry_after: Some(retry) }, llm::Failure::Exhausted { retry_after: Some(retry) }),
        (shared::Failure::RateLimited { retry_after: retry }, llm::Failure::RateLimited { retry_after: retry }),
        (shared::Failure::Overloaded, llm::Failure::Overloaded),
        (shared::Failure::Unavailable, llm::Failure::Unavailable),
        (shared::Failure::ContextTooLong, llm::Failure::ContextTooLong),
        (shared::Failure::Invalid, llm::Failure::Invalid),
        (
            shared::Failure::Limit { which: shared::Cap::Answer, bound: 512 },
            llm::Failure::Limit { which: llm::Limit::Path(llm::Cap::Answer), bound: 512 },
        ),
        (shared::Failure::Protocol, llm::Failure::Protocol),
        (shared::Failure::Cancelled, llm::Failure::Cancelled),
        (
            shared::Failure::TimedOut { phase: shared::Phase::Connect },
            llm::Failure::TimedOut { phase: llm::Phase::Connect },
        ),
        (
            shared::Failure::TimedOut { phase: shared::Phase::Handshake },
            llm::Failure::TimedOut { phase: llm::Phase::Handshake },
        ),
        (shared::Failure::TimedOut { phase: shared::Phase::Head }, llm::Failure::TimedOut { phase: llm::Phase::Head }),
        (shared::Failure::TimedOut { phase: shared::Phase::Idle }, llm::Failure::TimedOut { phase: llm::Phase::Idle }),
        (
            shared::Failure::TimedOut { phase: shared::Phase::Whole },
            llm::Failure::TimedOut { phase: llm::Phase::Whole },
        ),
    ]
}

fn prepared_failure(limits: &ComponentLimits) -> adapter::Prepared {
    let input = adapter::Input {
        owner: Token::new(71),
        prompt: prompt(),
        endpoint_name: llm::Endpoint(42),
        endpoint: shared::Endpoint::codex(),
        credential: credential(b"secret"),
        application: Box::new([]),
        receiving: limits.receiving,
    };
    adapter::prepare(input, &limits.adapter).expect("bounded preparation")
}

#[test]
fn every_shared_failure_class_keeps_each_evidence_and_detail() {
    let limits = limits();
    let evidence = [
        (shared::client::Evidence::Unsent, llm::Evidence::Unsent),
        (shared::client::Evidence::Unknown, llm::Evidence::Unknown),
        (shared::client::Evidence::Response { status: 429 }, llm::Evidence::Response { status: Some(429) }),
    ];
    for (source, expected) in shared_failure_cases() {
        for (sent, expected_sent) in evidence {
            let prepared = prepared_failure(&limits);
            let event = adapter::failed(
                prepared.context,
                Token::new(71),
                source,
                sent,
                b"literal detail".as_slice().into(),
                shared::Report::none(),
            )
            .expect("matching terminal");
            let smith_domain::Event::Failed { owner, failure, evidence, detail, report } = event else {
                panic!("failed event")
            };
            let observed = Some((owner, failure, evidence, detail, report));
            assert_eq!(
                observed,
                Some((
                    Token::new(71),
                    expected,
                    expected_sent,
                    Box::from(b"literal detail".as_slice()),
                    llm::Report::none()
                ))
            );
        }
    }
}

#[test]
fn a_full_connection_pool_and_two_grant_generations_fit_the_component_bound() {
    let limits = limits();
    let destinations = endpoints();
    let component_bound = adapter::component_worst_case(&limits, &destinations).expect("component worst case");
    let first = complete(7, 1, limits.receiving);
    let second = complete(8, 2, limits.receiving);
    let first_credential = credential(b"first");
    let second_credential = credential(b"second");
    let meter = Meter::new();
    meter.start();
    let mut component = Component::new(&limits, destinations).expect("bounded component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, first_credential, Time::from_nanos(100))
        .expect("first grant");
    component
        .grant(GrantName { account: 0, generation: 2 }, second_credential, Time::from_nanos(100))
        .expect("second generation");
    component.from_domain(&env(1, &limits), first, &mut up, &mut io);
    component.from_domain(&env(2, &limits), second, &mut up, &mut io);
    let measured = meter.end();
    assert_eq!(io.len(), 2, "the pool holds two concurrent connections");
    let bound = component_bound
        + Queue::<ToDomain>::worst_case(adapter::MAX_OUT.above).expect("domain queue")
        + Queue::<IoRequest>::worst_case(adapter::MAX_OUT.below).expect("io queue");
    assert!(measured.peak() <= bound, "component peak {} exceeds {bound}", measured.peak());
}

#[test]
fn a_tls_handshake_deadline_stops_the_unsent_call() {
    let limits = limits();
    let mut component = Component::new(&limits, endpoints()).expect("component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, credential(b"first"), Time::from_nanos(20_000_000_000))
        .expect("grant");
    component.from_domain(&env(0, &limits), complete(7, 1, limits.receiving), &mut up, &mut io);
    let owner = match io.pop() {
        Some(IoRequest::Connect { owner, .. }) => owner,
        other => panic!("expected connect, got {other:?}"),
    };
    component.from_below(
        &env(0, &limits),
        skein_io::Event::Connecting { owner, socket: Token::new(99) },
        &mut up,
        &mut io,
    );
    component.from_below(&env(0, &limits), skein_io::Event::Connected { owner }, &mut up, &mut io);
    component.fire(&env(1_000_000_001, &limits), &mut up, &mut io);
    assert!(matches!(
        up.pop(),
        Some(ToDomain::Failed {
            owner,
            failure: llm::Failure::TimedOut { phase: llm::Phase::Handshake },
            evidence: llm::Evidence::Unsent,
            ..
        }) if owner == Token::new(7)
    ));
}

#[test]
fn the_whole_deadline_precedes_a_later_endpoint_connect_deadline() {
    let limits = limits();
    let mut component = Component::new(&limits, endpoints_with_connect(Duration::from_secs(60))).expect("component");
    let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
    let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
    component
        .grant(GrantName { account: 0, generation: 1 }, credential(b"first"), Time::from_nanos(20_000_000_000))
        .expect("grant");
    component.from_domain(&env(0, &limits), complete(7, 1, limits.receiving), &mut up, &mut io);
    assert_eq!(component.next_deadline(), Some(Time::from_nanos(10_000_000_000)));
    component.fire(&env(10_000_000_001, &limits), &mut up, &mut io);
    assert!(matches!(
        up.pop(),
        Some(ToDomain::Failed {
            failure: llm::Failure::TimedOut { phase: llm::Phase::Whole },
            evidence: llm::Evidence::Unsent,
            ..
        })
    ));
}

#[test]
fn an_observed_request_is_native_validated_before_its_payload_is_copied() {
    let limits = limits();
    for malformed in [false, true] {
        let mut client = limits.adapter.client;
        if !malformed {
            client.request = 1;
        }
        let mut component = Component::new(&limits, endpoints_with_client(Duration::from_secs(1), client))
            .expect("actual endpoint with its own request bound");
        component
            .grant(GrantName { account: 0, generation: 1 }, credential(b"private-grant"), Time::from_nanos(100))
            .expect("valid private grant");
        let mut request = complete(7, 1, limits.receiving);
        if let FromDomain::Complete { prompt, observation, deltas, .. } = &mut request {
            deltas.text = true;
            *observation = Some(smith_protocol_llm::Observation { first: true, messages: 0 });
            if malformed {
                prompt.system = vec![0xff].into();
            }
        }
        let mut up = Queue::with_capacity(adapter::MAX_OUT.above);
        let mut io = Queue::with_capacity(adapter::MAX_OUT.below);
        component.from_domain(&env(0, &limits), request, &mut up, &mut io);
        assert!(component.pop_measure().is_none(), "the refused native request is never copied as content");
        assert!(component.measures_drained(), "no refused handoff remains held");
        assert!(io.is_empty(), "native preparation refusal never reaches a connection");
        let Some(ToDomain::Failed { owner, failure, evidence, .. }) = up.pop() else {
            panic!("one exact preparation refusal");
        };
        assert_eq!(owner, Token::new(7));
        assert_eq!(evidence, llm::Evidence::Unsent);
        assert_eq!(
            failure,
            if malformed {
                llm::Failure::Invalid
            } else {
                llm::Failure::Limit { which: llm::Limit::Path(llm::Cap::Request), bound: 1 }
            }
        );
        assert!(up.is_empty(), "one refusal terminal");
    }
}

#[test]
fn a_response_failure_keeps_its_status_in_the_evidence() {
    let limits = limits();
    let prepared = adapter::prepare(
        adapter::Input {
            owner: Token::new(71),
            prompt: prompt(),
            endpoint_name: llm::Endpoint(42),
            endpoint: shared::Endpoint::codex(),
            credential: credential(b"secret"),
            application: Box::new([]),
            receiving: limits.receiving,
        },
        &limits.adapter,
    )
    .expect("bounded preparation");
    let terminal = adapter::failed(
        prepared.context,
        Token::new(71),
        shared::Failure::Unavailable,
        shared::client::Evidence::Response { status: 503 },
        Box::default(),
        shared::Report::none(),
    )
    .expect("matching terminal");
    let smith_domain::Event::Failed { failure, evidence, .. } = terminal else { panic!("failure terminal") };
    assert_eq!(failure, llm::Failure::Unavailable);
    assert_eq!(evidence, llm::Evidence::Response { status: Some(503) });
}

#[test]
fn skeins_report_reaches_the_session_whole() {
    let limits = limits();
    for (code, request, field, expected) in [
        (
            b"code".as_slice(),
            b"".as_slice(),
            b"".as_slice(),
            llm::Report { code: b"code".as_slice().into(), request: Box::default(), field: Box::default() },
        ),
        (
            b"".as_slice(),
            b"request".as_slice(),
            b"".as_slice(),
            llm::Report { code: Box::default(), request: b"request".as_slice().into(), field: Box::default() },
        ),
        (
            b"".as_slice(),
            b"".as_slice(),
            b"field".as_slice(),
            llm::Report { code: Box::default(), request: Box::default(), field: b"field".as_slice().into() },
        ),
        (b"".as_slice(), b"".as_slice(), b"".as_slice(), llm::Report::none()),
    ] {
        let prepared = prepared_failure(&limits);
        let report = shared::Report { code: code.into(), request: request.into(), field: field.into() };
        let terminal = adapter::failed(
            prepared.context,
            Token::new(71),
            shared::Failure::Unsupported,
            shared::client::Evidence::Unsent,
            Box::default(),
            report,
        )
        .expect("matching terminal");
        let smith_domain::Event::Failed { report, .. } = terminal else { panic!("failed event") };
        let received = Some(report);
        assert_eq!(received, Some(expected));
    }
}
