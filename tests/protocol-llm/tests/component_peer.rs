//! Smith's LLM component routes io and TLS to skein's fake byte peer in both dialects.
//! Contract: protocol/llm.md, sections 6 and 10.

use core::net::Ipv4Addr;

use skein_fake_llm_domain::{self as fake, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_io::kernel::Addr;
use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Intake, Queue, Time, Token};
use skein_llm::{Credential, Provider};
use skein_tls_world::{drive::Wire, pki, server::Server};
use smith_domain::{GrantName, llm, run, tools};
use smith_protocol_llm_world::adapter::{
    self as adapter, Component, ComponentLimits, ConfiguredEndpoint, Endpoints, FromDomain, MAX_OUT, Receiving,
    ToDomain,
};

fn limits() -> ComponentLimits {
    let client = skein_llm_world::limits();
    let decoded_call_bytes = 4096;
    ComponentLimits {
        adapter: adapter::Limits {
            client,
            tool_bytes: 32768,
            history_bytes: u64::from(client.request),
            rendered_result: client.strings,
            stated: smith_protocol_llm_world::adapter::stated(),
        },
        connection: skein_llm_connection::Limits {
            memory: 1 << 40,
            endpoints: 1,
            connections: 1,
            calls: 1,
            per_endpoint: 1,
            idle_keep: smith_agent_service::profile::standard().policy.connection_keep,
            io: skein_io::Limits {
                sockets: 1,
                names: 0,
                refusals: 1,
                intake: 19_000,
                receive: 1024,
                output: 19_000,
                sends: 2,
                accepts: 1,
                backlog: 1,
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

#[expect(clippy::too_many_lines, reason = "the protocol story drives the component, io and fake peer")]
fn run(dialect: Provider, calls: bool, observed: bool, owner_close: bool) {
    let mut call = skein_llm_world::call(7);
    match dialect {
        skein_llm::Provider::OpenAi => panic!("fixture exercises the withdrawn routes"),
        Provider::OpenAiCodex => {}
        Provider::Anthropic => {
            call.endpoint = skein_llm::Endpoint::anthropic();
            call.credential = Some(Credential::anthropic(b"fake-token".as_slice().into()));
            call.prompt.affinity = None;
        }
    }
    call.prompt.output_ceiling(dialect, 4096).expect("provider output ceiling");
    call.prompt.instructions = b"connection-world".as_slice().into();
    let peer_credential = Credential {
        value: call.credential.as_ref().expect("fixture credential").value.clone(),
        account_id: call.credential.as_ref().expect("fixture credential").account_id.clone(),
        scheme: skein_llm::Scheme::Bearer,
    };
    let peer_limits = skein_llm_world::fake::limits(&skein_llm_world::limits());
    let provider_kind = match dialect {
        skein_llm::Provider::OpenAi => panic!("fixture exercises the withdrawn routes"),
        Provider::OpenAiCodex => documents::Provider::OpenAi,
        Provider::Anthropic => documents::Provider::Anthropic,
    };
    let mut service = provider::Service::new(
        provider::Config {
            echo: skein_llm::openai::Echo::NONE,
            usage_fields: documents::UsageFields::ALL,
            provider: provider_kind,
            path: call.endpoint.target.clone(),
            headers: Box::new([]),
            without_credential: false,
        },
        &peer_limits,
    )
    .expect("fake service");
    let mut peer = provider::Server::new(Token::new(2), &peer_limits).expect("fake server");
    let fake_limits = skein_llm_world::fake::config();
    let lines = if calls {
        Box::new([api::Line::Call {
            name: b"read".as_slice().into(),
            arguments: br#"{"path":"README.md"}"#.as_slice().into(),
        }])
    } else {
        Box::new([api::Line::Text { text: b"scripted answer".as_slice().into() }])
    };
    let scripts = Box::new([api::Script {
        cue: b"connection-world".as_slice().into(),
        turns: Box::new([api::Turn {
            lines,
            finish: if calls { api::Finish::ToolCalls } else { api::Finish::Stop },
            tokens: 2,
        }]),
    }]);
    let mut domain = fake::Domain::try_scripted(&fake_limits, 3, scripts).expect("scripted domain");
    let endpoints = Endpoints::new(
        Box::new([ConfiguredEndpoint {
            connect: skein_lib::Duration::from_secs(1),
            handshake: skein_lib::Duration::from_secs(1),
            name: llm::Endpoint(1),
            destination: skein_llm_connection::Endpoint {
                limits: limits().adapter.client,
                credential: skein_llm::client::CredentialLimits {
                    value: limits().grant_value_bytes,
                    account_id: limits().grant_value_bytes,
                },
                address: Addr::from((Ipv4Addr::LOCALHOST, 443)),
                transport: skein_llm_connection::Transport::Tls {
                    server_name: skein_tls::Name::new("skein.test").expect("test name"),
                    trust: pki::client(&[]),
                },
                llm: call.endpoint,
            },
            identity: smith_protocol_llm::IdentityProfile::Plain,
            models: Box::new([]),
        }]),
        1,
    )
    .expect("one endpoint");
    let mut component = Component::new(&limits(), endpoints).expect("component");
    component
        .grant(
            GrantName { account: 0, generation: 1 },
            call.credential.expect("fixture credential"),
            Time::from_nanos(10_000_000_000),
        )
        .expect("grant");
    let env = Env { now: Time::ZERO, wall: pki::VALID, limits: limits() };
    let peer_env = Env { now: Time::ZERO, wall: pki::VALID, limits: peer_limits };
    let fake_env = Env { now: Time::ZERO, wall: pki::VALID, limits: fake_limits };
    let mut up = Queue::with_capacity(MAX_OUT.above);
    let mut io = Queue::with_capacity(MAX_OUT.below);
    let mut peer_up = Queue::with_capacity(provider::MAX_UP);
    let mut peer_down = Queue::with_capacity(provider::MAX_DOWN);
    let mut replies = Queue::with_capacity(fake::MAX_OUT);
    let mut to_peer = Intake::with_capacity(32768);
    let mut peer_demand: Option<(Read, u32)> = None;
    let mut grant: Option<u32> = None;
    let mut wire = Wire::new(Server::new(pki::Server::plain().config()));
    let mut received = 0_usize;
    let mut owner = None;
    let mut closing = false;
    let mut close_terminals = 0;
    let mut completed = 0_u32;
    let mut captured = Vec::new();
    provider::start(
        &mut peer,
        &mut service,
        core::slice::from_ref(&peer_credential),
        &peer_env,
        &mut peer_up,
        &mut peer_down,
    );
    component.from_domain(
        &env,
        FromDomain::Complete {
            owner: Token::new(7),
            grant: Some(GrantName { account: 0, generation: 1 }),
            prompt: llm::Prompt {
                effort: None,
                purpose: llm::Purpose::Ordinary { guard: None },
                choice: smith_domain::llm::ToolChoice::Auto,
                namespace: smith_domain::tools::Namespace::One,
                affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
                asker: llm::Asker::Main,
                endpoint: llm::Endpoint(1),
                model: call.prompt.model,
                system: b"connection-world".as_slice().into(),
                tools: tools::Grants { inspect: calls, modify: false, shell: false },
                served: Box::new([]),
                messages: Box::new([llm::Message {
                    role: llm::Role::User,
                    content: Box::new([llm::Block::Text { text: b"hi".as_slice().into(), replay: None }]),
                }]),
                max_tokens: 4096,
            },
            deadlines: smith_domain::session::Deadlines {
                head: Duration::from_secs(2),
                idle: Duration::from_secs(3),
                whole: Time::from_nanos(10_000_000_000),
            },
            bounds: limits().receiving,
            outcome: run::outcome::OutcomeSpec { change: None, verdicts: Box::new([]), report: None, failure: None },
            deliver: None,
            deltas: smith_protocol_llm::Deltas { text: observed, input: false },
            observation: observed.then_some(smith_protocol_llm::Observation { first: true, messages: 0 }),
        },
        &mut up,
        &mut io,
    );
    if observed {
        assert!(io.is_empty(), "the observed prompt is recorded before the request reaches the connection");
        assert!(up.is_empty(), "a provider terminal cannot precede its recorded prompt");
        assert!(!component.measures_drained(), "the held handoff owes its actual prompt observation");
    }
    for _ in 0_u32..40_000 {
        while let Some(measure) = component.pop_measure() {
            let prompt = match &measure {
                smith_protocol_llm::Measure::Prompt { owner, .. } => Some(*owner),
                smith_protocol_llm::Measure::Completion { .. }
                | smith_protocol_llm::Measure::Call { .. }
                | smith_protocol_llm::Measure::Arguments { .. }
                | smith_protocol_llm::Measure::Delta { .. }
                | smith_protocol_llm::Measure::Terminal { .. }
                | smith_protocol_llm::Measure::Retried { .. } => None,
            };
            captured.push(measure);
            if let Some(owner) = prompt {
                component.prompt_recorded(&env, owner, &mut up, &mut io);
            }
        }
        if let Some(event) = up.pop() {
            match event {
                ToDomain::NativeVersion { .. } | ToDomain::BuiltinResult { .. } => {
                    panic!("a direct backend emits no program result")
                }
                ToDomain::Text { owner, bytes } => {
                    assert_eq!(owner, Token::new(7));
                    assert!(bytes > 0);
                }
                ToDomain::Completed { owner, completion } => {
                    assert_eq!(owner, Token::new(7));
                    assert!(!completion.content.is_empty());
                    if calls {
                        assert!(completion.content.iter().any(|block| matches!(
                            block,
                            llm::Said::ToolCall { name, call: llm::Decoded::Owned { call: tools::Call::Read { .. } }, .. }
                                if name.as_ref() == b"read"
                        )));
                    } else {
                        assert!(completion.content.iter().any(|block| matches!(
                            block,
                            llm::Said::Text { text, .. } if text.as_ref() == b"scripted answer"
                        )));
                    }
                    completed += 1;
                }
                ToDomain::Failed { .. } | ToDomain::Cancelled { .. } => {
                    panic!("unexpected component event: {event:?}");
                }
            }
        }
        for _ in 0..MAX_OUT.below {
            let Some(request) = io.pop() else { break };
            match request {
                IoRequest::Connect { owner: token, .. } => {
                    assert!(owner.replace(token).is_none());
                    component.from_below(
                        &env,
                        IoEvent::Connecting { owner: token, socket: Token::new(100) },
                        &mut up,
                        &mut io,
                    );
                    component.from_below(&env, IoEvent::Connected { owner: token }, &mut up, &mut io);
                }
                IoRequest::Stream { stream, down } => {
                    assert_eq!(stream, Token::new(100));
                    wire.take(down);
                }
                IoRequest::Close { entity } | IoRequest::Abort { entity } => {
                    assert_eq!(entity, Token::new(100));
                    assert!(!component.closed(), "the socket still owes its close terminal");
                    close_terminals += 1;
                    component.from_below(
                        &env,
                        IoEvent::Closed { owner: owner.expect("connected owner") },
                        &mut up,
                        &mut io,
                    );
                }
                other @ (IoRequest::Listen { .. }
                | IoRequest::Bind { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Signal { .. }
                | IoRequest::Window { .. }
                | IoRequest::Usage { .. }) => panic!("unexpected io request: {other:?}"),
            }
        }
        if wire.server.received.len() > received {
            to_peer.append(&wire.server.received[received..]).expect("bounded request intake");
            received = wire.server.received.len();
        }
        if let Some(answer) = wire.answer() {
            component.from_below(
                &env,
                IoEvent::Stream { owner: owner.expect("connected owner"), up: answer },
                &mut up,
                &mut io,
            );
        }
        if let Some(event) = peer_up.pop() {
            match event {
                provider::Event::Domain(input) => fake::step(&mut domain, &fake_env, input, &mut replies),
                provider::Event::Head { .. } | provider::Event::Close | provider::Event::Closed => {}
            }
        }
        if let Some(reply) = replies.pop() {
            provider::down(
                &mut peer,
                &mut service,
                core::slice::from_ref(&peer_credential),
                &peer_env,
                reply,
                &mut peer_up,
                &mut peer_down,
            );
            domain.reclaim();
            service.reclaim();
        }
        if let Some(down) = peer_down.pop() {
            match down {
                Down::Demand { read: Read::Nothing, room: 0 } => peer_demand = None,
                Down::Demand { read, room } => {
                    assert!(peer_demand.is_none(), "one demand at a time");
                    peer_demand = Some((read, room));
                }
                Down::Send(bytes) => {
                    assert!(bytes.len() <= usize::try_from(grant.take().expect("room grant")).expect("room fits"));
                    wire.server.write(&bytes);
                    wire.pull();
                }
                Down::Finish => wire.server.close_notify(),
            }
        }
        if let Some((read, room)) = peer_demand {
            let answer = match to_peer.meet(read) {
                Some(bytes) => Some(Up::Bytes(bytes)),
                None if room > 0 => {
                    grant = Some(room);
                    Some(Up::Room)
                }
                None => None,
            };
            if let Some(answer) = answer {
                peer_demand = None;
                provider::up(
                    &mut peer,
                    &mut service,
                    core::slice::from_ref(&peer_credential),
                    &peer_env,
                    answer,
                    &mut peer_up,
                    &mut peer_down,
                );
            }
        }
        if peer.has_work() {
            provider::resume(
                &mut peer,
                &mut service,
                core::slice::from_ref(&peer_credential),
                &peer_env,
                &mut peer_up,
                &mut peer_down,
            );
        }
        if domain.is_due(fake_env.now) {
            fake::fire(&mut domain, &fake_env, &mut replies);
        }
        while let Some(measure) = component.pop_measure() {
            let prompt = match &measure {
                smith_protocol_llm::Measure::Prompt { owner, .. } => Some(*owner),
                smith_protocol_llm::Measure::Completion { .. }
                | smith_protocol_llm::Measure::Call { .. }
                | smith_protocol_llm::Measure::Arguments { .. }
                | smith_protocol_llm::Measure::Delta { .. }
                | smith_protocol_llm::Measure::Terminal { .. }
                | smith_protocol_llm::Measure::Retried { .. } => None,
            };
            captured.push(measure);
            if let Some(owner) = prompt {
                component.prompt_recorded(&env, owner, &mut up, &mut io);
            }
        }
        if component.has_work() {
            component.fire(&env, &mut up, &mut io);
        }
        if owner_close && completed == 1 && !closing && component.measures_drained() {
            closing = true;
            assert!(!component.closed());
            component.close(&env, &mut up, &mut io);
        }
        if (!owner_close || component.closed())
            && completed == 1
            && !component.has_work()
            && io.is_empty()
            && up.is_empty()
            && !peer.has_work()
        {
            break;
        }
    }
    assert_eq!(service.count(), 1, "the independent fake decoded the call");
    assert_eq!(completed, 1);
    if owner_close {
        assert!(component.closed());
        assert_eq!(close_terminals, 1, "every socket settles once");
        assert!(up.is_empty(), "close creates no additional call terminal");
    }
    let mut terminal_count = 0;
    let mut content = Vec::new();
    for measure in captured {
        match measure {
            smith_protocol_llm::Measure::Terminal { owner, measures } => {
                assert_eq!(owner, Token::new(7));
                assert!(measures.request_bytes.expect("the native client encoded its request") > 0);
                assert!(measures.first_byte.is_some(), "the TLS peer supplied plaintext bytes");
                assert!(measures.largest_gap.is_some(), "the peer supplied multiple SSE events");
                terminal_count += 1;
            }
            smith_protocol_llm::Measure::Retried { .. } => unreachable!("direct components do not retry internally"),
            other @ (smith_protocol_llm::Measure::Prompt { .. }
            | smith_protocol_llm::Measure::Completion { .. }
            | smith_protocol_llm::Measure::Call { .. }
            | smith_protocol_llm::Measure::Arguments { .. }
            | smith_protocol_llm::Measure::Delta { .. }) => content.push(other),
        }
    }
    assert_eq!(terminal_count, 1, "transport observations do not depend on content capture");
    if observed {
        let mut prompt_count = 0;
        let mut completion_count = 0;
        let mut deltas = Vec::new();
        for measure in content {
            match measure {
                smith_protocol_llm::Measure::Prompt { at: _, owner, prompt } => {
                    assert_eq!(owner, Token::new(7));
                    assert_eq!(prompt.system.as_deref(), Some(b"connection-world".as_slice()));
                    assert_eq!(prompt.messages.len(), 1);
                    assert_eq!(
                        prompt.messages.get(0).expect("first actual message").blocks.as_slice(),
                        &[smith_events::Block::Text { text: b"hi".to_vec().into() }]
                    );
                    assert_eq!(
                        prompt
                            .tools
                            .expect("first prompt carries its tool names")
                            .iter()
                            .any(|name| name.as_ref() == b"read"),
                        calls
                    );
                    prompt_count += 1;
                }
                smith_protocol_llm::Measure::Delta { at: _, owner, block, text } => {
                    assert_eq!(owner, Token::new(7));
                    assert_eq!(block, 0);
                    deltas.extend_from_slice(&text);
                }
                smith_protocol_llm::Measure::Completion { at: _, owner, completion } => {
                    assert_eq!(owner, Token::new(7));
                    if calls {
                        assert!(completion.blocks.iter().any(|block| matches!(block, smith_events::Block::Call { tool, input, .. } if tool.as_ref() == b"read" && input.as_ref() == br#"{"path":"README.md"}"#)));
                    } else {
                        match dialect {
                            skein_llm::Provider::OpenAi => panic!("fixture exercises the withdrawn routes"),
                            Provider::OpenAiCodex => assert!(
                                deltas.is_empty(),
                                "the shared fake Codex renderer emits Added/Done, without text deltas"
                            ),
                            Provider::Anthropic => assert_eq!(deltas, b"scripted answer"),
                        }
                        assert_eq!(
                            completion.blocks.as_slice(),
                            &[smith_events::Block::Text { text: b"scripted answer".to_vec().into() }]
                        );
                    }
                    completion_count += 1;
                }
                smith_protocol_llm::Measure::Call { .. }
                | smith_protocol_llm::Measure::Arguments { .. }
                | smith_protocol_llm::Measure::Terminal { .. }
                | smith_protocol_llm::Measure::Retried { .. } => {
                    unreachable!("separated above")
                }
            }
        }
        assert_eq!((prompt_count, completion_count), (1, 1));
    } else {
        assert!(content.is_empty(), "disabled capture retains no content");
    }
}

#[test]
fn codex_fake_peer_over_tls() {
    run(Provider::OpenAiCodex, false, false, false);
}

#[test]
fn anthropic_fake_peer_over_tls() {
    run(Provider::Anthropic, false, false, false);
}

#[test]
fn codex_tool_input_is_typed_through_the_owned_connection() {
    run(Provider::OpenAiCodex, true, false, false);
}

#[test]
fn anthropic_tool_input_is_typed_through_the_owned_connection() {
    run(Provider::Anthropic, true, false, false);
}

#[test]
fn captured_native_codex_text_and_calls_keep_actual_prompt_and_deltas() {
    run(Provider::OpenAiCodex, false, true, false);
    run(Provider::OpenAiCodex, true, true, false);
}

#[test]
fn captured_native_anthropic_text_and_calls_keep_actual_prompt_and_deltas() {
    run(Provider::Anthropic, false, true, false);
    run(Provider::Anthropic, true, true, false);
}

#[test]
fn an_owner_close_reports_closed_once_every_socket_settled() {
    run(Provider::OpenAiCodex, false, false, true);
    run(Provider::Anthropic, false, false, true);
}
