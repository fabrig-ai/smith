//! Exact declaration identity and provider bytes through the production translator.
#![expect(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::arithmetic_side_effects,
    reason = "ordinary bounded test fixtures and captured wire bytes"
)]

use super::*;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

fn limits() -> Limits {
    Limits {
        client: skein_llm_world::limits(),
        tool_bytes: 32768,
        history_bytes: 65536,
        rendered_result: 8192,
        stated: crate::Stated {
            host_part: 1024,
            path_bytes: 256,
            read_window: 512,
            tool_payload: 1024,
            file_bytes: 4096,
            call_input_bytes: 8192,
            list_entries: 64,
            search_hits: 64,
            search_bytes: 4096,
            shell_timeout: Duration::from_secs(30),
            tool_deadline: Duration::from_secs(60),
            job_poll: Duration::from_secs(1),
            job_wait: Duration::from_secs(30),
            jobs: 4,
            shell_output: 1024,
            output_ceiling: 4096,
        },
    }
}

fn receiving(limits: &Limits) -> crate::Receiving {
    crate::Receiving {
        max_completion_bytes: crate::completion_worst_case(&limits.client, 4096).expect("valid test fixture"),
        max_completion_blocks: limits.client.output_items,
        decoded_call_bytes: 4096,
        max_failure_bytes: limits.client.detail_bytes,
    }
}

fn input() -> llm::Prompt {
    llm::Prompt {
        purpose: llm::Purpose::Ordinary { guard: None },
        asker: llm::Asker::Main,
        endpoint: llm::Endpoint(1),
        affinity: llm::Affinity { key: [0; 16], thread: 0 },
        model: b"fixture-model".as_slice().into(),
        effort: None,
        system: b"instructions".as_slice().into(),
        tools: tools::Grants { inspect: true, modify: false, shell: false },
        namespace: tools::Namespace::One,
        served: Box::new([
            llm::Served::Finish,
            llm::Served::Deliver,
            llm::Served::Host(run::HostTool {
                name: b"host".as_slice().into(),
                description: b"host description".as_slice().into(),
                schema:
                    br#"{"type":"object","properties":{"word":{"type":"string"}},"x":[null,true,1]}"#.as_slice().into(),
                effect: run::HostEffect::Read,
                timeout: Duration::from_secs(2),
            }),
        ]),
        choice: llm::ToolChoice::Auto,
        messages: Box::new([llm::Message {
            role: llm::Role::User,
            content: Box::new([llm::Block::Text { text: b"hi".as_slice().into(), replay: None }]),
        }]),
        max_tokens: 128,
    }
}

fn outcome() -> run::outcome::OutcomeSpec {
    run::outcome::OutcomeSpec {
        change: None,
        verdicts: Box::new([]),
        report: Some(run::outcome::TextSpec { max: 1024, fields: Box::new([]) }),
        failure: None,
    }
}

fn delivery() -> run::outcome::ChangeSpec {
    run::outcome::ChangeSpec { checks_must_pass: true, fields: Box::new([]) }
}

fn cached(cache: &mut Option<Declarations>, prompt: llm::Prompt, limits: &Limits) -> Result<shared::Prompt, Error> {
    prepare_component(Token::new(1), prompt, &outcome(), Some(&delivery()), receiving(limits), limits, cache)
        .map(|(p, _)| p)
}

fn wire(mut prompt: shared::Prompt, provider: shared::Provider) -> Vec<u8> {
    use shared::client;
    use skein_lib::stream::{Down, Up};
    let endpoint = match provider {
        shared::Provider::OpenAiCodex => shared::Endpoint::codex(),
        shared::Provider::OpenAi => shared::Endpoint::openai(),
        shared::Provider::Anthropic => shared::Endpoint::anthropic(),
    };
    prompt.output_ceiling(provider, 128).expect("valid test fixture");
    let credential = match provider {
        shared::Provider::OpenAiCodex => {
            shared::Credential::codex(b"secret".as_slice().into(), b"account".as_slice().into())
        }
        shared::Provider::OpenAi | shared::Provider::Anthropic => shared::Credential::key(b"key".as_slice().into()),
    };
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits().client };
    let mut client = client::Client::prepare(
        shared::Call { owner: Token::new(1), prompt, endpoint, credential: Some(credential) },
        &env.limits,
    )
    .expect("valid test fixture");
    let mut above = Queue::with_capacity(client::MAX_OUT.above);
    let mut below = Queue::with_capacity(client::MAX_OUT.below);
    client::down(&mut client, &env, client::Request::Start, &mut above, &mut below);
    let mut bytes = Vec::new();
    for _ in 0_u32..100_000_u32 {
        if client.has_work() {
            client::resume(&mut client, &env, &mut above, &mut below);
        }
        let mut room = false;
        while let Some(record) = below.pop() {
            match record {
                Down::Send(part) => bytes.extend_from_slice(&part),
                Down::Demand { room: wanted, .. } => room |= wanted > 0,
                Down::Finish => {}
            }
        }
        while above.pop().is_some() {}
        if room {
            client::up(&mut client, &env, Up::Room, &mut above, &mut below);
        } else if !client.has_work() {
            return bytes;
        }
    }
    unreachable!("bounded request upload settles")
}

#[test]
fn retained_attestations_preserve_every_provider_request_byte() {
    for provider in [shared::Provider::OpenAiCodex, shared::Provider::OpenAi, shared::Provider::Anthropic] {
        let mut cache = None;
        let cold = cached(&mut cache, input(), &limits()).expect("valid test fixture");
        let address = cache.as_ref().expect("valid test fixture").offered.as_ptr();
        let warm = cached(&mut cache, input(), &limits()).expect("valid test fixture");
        assert_eq!(cache.as_ref().expect("valid test fixture").offered.as_ptr(), address);
        let application = crate::tools::schemas_for_contract(
            &input(),
            &limits().stated,
            &outcome(),
            Some(&delivery()),
            limits().client.request,
        )
        .expect("valid test fixture");
        let fresh = prepare_prompt(Token::new(1), input(), application, receiving(&limits()), &limits())
            .expect("valid test fixture")
            .0;
        assert_eq!(wire(cold, provider), wire(fresh, provider));
        let expected = cached(&mut None, input(), &limits()).expect("valid test fixture");
        assert_eq!(wire(warm, provider), wire(expected, provider));
    }
}

fn invalidates(case: u32) {
    let mut cache = None;
    cached(&mut cache, input(), &limits()).expect("valid test fixture");
    let mut prompt = input();
    let mut bounds = limits();
    let mut result = outcome();
    let mut deliver = delivery();
    match case {
        0 => prompt.tools.modify = true,
        1 => {
            prompt.namespace = tools::Namespace::Several {
                working: tools::Name::new(b"work".as_slice().into()).expect("valid test fixture"),
            }
        }
        2 => result.report.as_mut().expect("valid test fixture").max += 1,
        3 => deliver.checks_must_pass = false,
        4 => bounds.client.depth += 1,
        5 => bounds.client.request += 1,
        6 => bounds.stated.read_window += 1,
        11 => bounds.tool_bytes += 1,
        12 => bounds.client.tools += 1,
        7 => prompt.served = Box::new([llm::Served::Wait]),
        8..=10 => {
            let llm::Served::Host(host) = prompt.served.last_mut().expect("valid test fixture") else {
                unreachable!("fixture host")
            };
            match case {
                8 => host.name = b"other".as_slice().into(),
                9 => host.description = b"other description".as_slice().into(),
                10 => host.schema = br#"{"type":"object","properties":{}}"#.as_slice().into(),
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
    assert!(!cache.as_ref().expect("valid test fixture").accepts(&prompt, &result, Some(&deliver), &bounds));
    let expected_grants = prompt.tools;
    let expected_namespace = prompt.namespace.clone();
    let expected_served = prompt.served.clone();
    let actual =
        prepare_component(Token::new(2), prompt, &result, Some(&deliver), receiving(&bounds), &bounds, &mut cache)
            .expect("valid test fixture");
    let admitted = cache.as_ref().expect("valid test fixture");
    assert_eq!(admitted.grants, expected_grants);
    assert_eq!(admitted.namespace, expected_namespace);
    assert_eq!(admitted.served, expected_served);
    assert_eq!(admitted.outcome, result);
    assert_eq!(admitted.deliver.as_ref(), Some(&deliver));
    assert_eq!(admitted.limits, bounds);
    assert!(!actual.0.tools.is_empty());
}

#[test]
fn changed_tool_grants_invalidate() {
    invalidates(0);
}

#[test]
fn changed_namespace_invalidates() {
    invalidates(1);
}

#[test]
fn changed_outcome_invalidates() {
    invalidates(2);
}

#[test]
fn changed_delivery_invalidates() {
    invalidates(3);
}

#[test]
fn changed_attestation_depth_invalidates() {
    invalidates(4);
}

#[test]
fn changed_request_limit_invalidates() {
    invalidates(5);
}

#[test]
fn changed_stated_description_limits_invalidate() {
    invalidates(6);
}

#[test]
fn changed_served_set_invalidates() {
    invalidates(7);
}

#[test]
fn changed_host_name_invalidates() {
    invalidates(8);
}

#[test]
fn changed_host_description_invalidates() {
    invalidates(9);
}

#[test]
fn changed_host_schema_invalidates() {
    invalidates(10);
}

#[test]
fn retained_attestation_never_admits_a_changed_malformed_schema_or_history() {
    let mut cache = None;
    cached(&mut cache, input(), &limits()).expect("valid test fixture");
    let mut bad = input();
    let llm::Served::Host(host) = bad.served.last_mut().expect("valid test fixture") else { unreachable!() };
    host.schema = b"{".as_slice().into();
    cached(&mut cache, bad, &limits()).expect_err("a changed malformed schema is refused");
    assert!(cache.is_none());
    cached(&mut cache, input(), &limits()).expect("valid test fixture");
    let mut bad = input();
    bad.messages = (0_u32..1000_u32)
        .map(|_| llm::Message { role: llm::Role::User, content: Box::new([]) })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    assert_eq!(cached(&mut cache, bad, &limits()).err(), Some(Error::Limit));
}

#[test]
fn changed_declaration_byte_limit_invalidates() {
    invalidates(11);
}

#[test]
fn changed_tool_count_limit_invalidates() {
    invalidates(12);
}

#[test]
fn changed_working_namespace_name_invalidates() {
    let mut cache = None;
    let mut first = input();
    first.namespace =
        tools::Namespace::Several { working: tools::Name::new(b"first".as_slice().into()).expect("name") };
    cached(&mut cache, first, &limits()).expect("admitted first namespace");
    let mut next = input();
    next.namespace =
        tools::Namespace::Several { working: tools::Name::new(b"second".as_slice().into()).expect("name") };
    assert!(!cache.as_ref().expect("attested").accepts(&next, &outcome(), Some(&delivery()), &limits()));
    cached(&mut cache, next, &limits()).expect("admitted next namespace");
}

#[test]
fn changed_delivery_presence_cannot_reuse_an_admitted_deliver_schema() {
    let mut cache = None;
    cached(&mut cache, input(), &limits()).expect("admitted declarations");
    assert!(!cache.as_ref().expect("attested").accepts(&input(), &outcome(), None, &limits()));
    assert_eq!(
        prepare_component(Token::new(1), input(), &outcome(), None, receiving(&limits()), &limits(), &mut cache).err(),
        Some(Error::Invalid)
    );
    assert!(cache.is_none());
}

#[test]
fn history_and_non_attestation_limits_stay_fresh_without_invalidating_declarations() {
    let mut cache = None;
    cached(&mut cache, input(), &limits()).expect("admitted declarations");
    let pointer = cache.as_ref().expect("attested").offered.as_ptr();
    let mut bounds = limits();
    bounds.history_bytes += 1;
    let mut prompt = input();
    prompt.model = b"another-model".as_slice().into();
    prompt.system = b"fresh instructions".as_slice().into();
    assert!(cache.as_ref().expect("attested").accepts(&prompt, &outcome(), Some(&delivery()), &bounds));
    let translated = cached(&mut cache, prompt, &bounds).expect("fresh prompt");
    assert_eq!(translated.model.as_ref(), b"another-model");
    assert_eq!(translated.instructions.as_ref(), b"fresh instructions");
    assert_eq!(cache.as_ref().expect("attested").offered.as_ptr(), pointer);
}

#[test]
fn retained_declarations_do_not_retain_the_previous_tool_choice() {
    let mut cache = None;
    cached(&mut cache, input(), &limits()).expect("admitted declarations");
    let pointer = cache.as_ref().expect("attested").offered.as_ptr();
    let mut prompt = input();
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Finish]));
    let translated = cached(&mut cache, prompt, &limits()).expect("fresh choice");
    assert_eq!(translated.choice, shared::ToolChoice::Only(Box::new([b"finish".as_slice().into()])));
    let mut prompt = input();
    prompt.choice = llm::ToolChoice::Only(Box::new([llm::Served::Wait]));
    assert_eq!(cached(&mut cache, prompt, &limits()).err(), Some(Error::Invalid));
    assert_eq!(cache.as_ref().expect("attested").offered.as_ptr(), pointer);
}
