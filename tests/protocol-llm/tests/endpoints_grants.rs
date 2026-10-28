//! Startup endpoint mapping and two-generation credential table.

use core::net::SocketAddr;

use skein_lib::Time;
use skein_llm::{self as shared, Credential};
use smith_domain::{GrantName, llm};
use smith_protocol_llm::{ConfiguredEndpoint, EndpointError, Endpoints, GrantError, Grants};

fn destination(name: u32, _account: u32, provider: shared::Provider) -> ConfiguredEndpoint {
    let mut roots = skein_tls::RootCertStore::empty();
    roots
        .add(skein_tls::CertificateDer::from(include_bytes!("../fixtures/root.der").to_vec()))
        .expect("fixture TLS root");
    let llm = match provider {
        shared::Provider::OpenAi => shared::Endpoint::openai(),
        shared::Provider::OpenAiCodex => shared::Endpoint::codex(),
        shared::Provider::Anthropic => shared::Endpoint::anthropic(),
    };
    ConfiguredEndpoint {
        connect: skein_lib::Duration::from_secs(1),
        handshake: skein_lib::Duration::from_secs(1),
        name: llm::Endpoint(name),
        destination: skein_llm_connection::Endpoint {
            limits: skein_llm_world::limits(),
            credential: skein_llm::client::CredentialLimits { value: 64, account_id: 64 },
            address: SocketAddr::from(([127, 0, 0, 1], 443)),
            transport: skein_llm_connection::Transport::Tls {
                server_name: skein_tls::Name::new("example.test").expect("server name"),
                trust: skein_tls::Config::new(roots, &[]).expect("test trust"),
            },
            llm,
        },
        identity: smith_protocol_llm::IdentityProfile::Plain,
        models: Box::new([]),
    }
}

#[test]
fn numeric_names_resolve_the_complete_destination_and_options() {
    let endpoints = Endpoints::new(
        Box::new([destination(42, 1, shared::Provider::OpenAiCodex), destination(7, 0, shared::Provider::Anthropic)]),
        2,
    )
    .expect("two named endpoints");
    let codex = endpoints.resolve(llm::Endpoint(42)).expect("configured name");
    assert_eq!(codex.index, 0);
    assert_eq!(codex.provider, shared::Provider::OpenAiCodex);
    assert_eq!(endpoints.resolve(llm::Endpoint(7)).expect("second name").index, 1);
    let mut prompt = shared::Prompt {
        affinity: Some(shared::Affinity { key: [0; 16], thread: 0 }),
        model: b"model".as_slice().into(),
        instructions: Box::new([]),
        tools: Box::new([]),
        choice: shared::ToolChoice::Auto,
        messages: Box::new([]),
        reasoning_effort: None,
        max_output_tokens: Some(32),
    };
    assert_eq!(endpoints.apply(llm::Endpoint(42), &mut prompt), Ok(0));
    assert_eq!(prompt.reasoning_effort, None);
    assert_eq!(prompt.affinity, Some(skein_llm::Affinity { key: [0; 16], thread: 0 }));
    assert_eq!(endpoints.apply(llm::Endpoint(3), &mut prompt), Err(EndpointError::Unknown));
    let (destinations, options) = endpoints.into_parts();
    assert_eq!(destinations.len(), 2);
    assert_eq!(options.get(&42).expect("named options").index, 0);
    assert_eq!(destinations.get(1).expect("second destination").llm.provider, shared::Provider::Anthropic);
}

#[test]
fn duplicate_names_fail_at_startup() {
    assert!(matches!(
        Endpoints::new(
            Box::new([
                destination(9, 0, shared::Provider::Anthropic),
                destination(9, 0, shared::Provider::OpenAiCodex),
            ]),
            2
        ),
        Err(EndpointError::Duplicate)
    ));
    assert!(matches!(
        Endpoints::new(Box::new([destination(9, 0, shared::Provider::Anthropic)]), 0),
        Err(EndpointError::TooMany)
    ));
}

fn credential(value: &[u8], account_id: &[u8]) -> Credential {
    Credential { value: value.into(), account_id: account_id.into(), scheme: skein_llm::Scheme::Bearer }
}

#[test]
fn a_refresh_preserves_the_first_call_then_evicts_the_oldest_generation() {
    let mut grants = Grants::new(1, 64).expect("bounded table");
    let first = GrantName { account: 0, generation: 1 };
    let second = GrantName { account: 0, generation: 2 };
    let third = GrantName { account: 0, generation: 3 };
    grants.grant(first, credential(b"first", b"account-id"), Time::from_nanos(100)).expect("first grant");
    let started = grants.read(first, Time::from_nanos(10)).expect("call reads its starting value");
    grants.grant(second, credential(b"second", b"account-id"), Time::from_nanos(200)).expect("refresh");
    assert!(started.value.as_ref() == b"first");
    assert!(grants.read(second, Time::from_nanos(20)).expect("next call").value.as_ref() == b"second");
    assert!(grants.read(first, Time::from_nanos(20)).expect("overlapping first").value.as_ref() == b"first");
    grants.grant(third, credential(b"third", b"account-id"), Time::from_nanos(300)).expect("next refresh");
    assert!(matches!(grants.read(first, Time::from_nanos(20)), Err(GrantError::Missing)));
    assert!(matches!(grants.read(second, Time::from_nanos(201)), Err(GrantError::Lapsed)));
    assert!(grants.read(third, Time::from_nanos(201)).expect("new value").value.as_ref() == b"third");
    assert!(matches!(grants.grant(second, credential(b"stale", b""), Time::from_nanos(400)), Err(GrantError::Stale)));
    assert!(!format!("{grants:?}").contains("third"), "debug hides bearer values");
}

#[test]
fn configured_claude_identity_precedes_domain_instructions_only_for_anthropic() {
    let mut anthropic = destination(8, 0, shared::Provider::Anthropic);
    anthropic.identity = smith_protocol_llm::IdentityProfile::ClaudeCode;
    let endpoints = Endpoints::new(Box::new([anthropic]), 1).expect("provider supports identity");
    let mut prompt = shared::Prompt {
        affinity: Some(shared::Affinity { key: [0; 16], thread: 0 }),
        model: b"model".as_slice().into(),
        instructions: b"domain system".as_slice().into(),
        tools: Box::new([]),
        choice: shared::ToolChoice::Auto,
        messages: Box::new([]),
        reasoning_effort: None,
        max_output_tokens: Some(32),
    };
    assert_eq!(endpoints.apply(llm::Endpoint(8), &mut prompt), Ok(0));
    assert!(prompt.instructions.starts_with(shared::anthropic::identity::CLAUDE_CODE_SYSTEM_IDENTITY));
    assert!(prompt.instructions.ends_with(b"domain system"));
    let mut codex = destination(9, 0, shared::Provider::OpenAiCodex);
    codex.identity = smith_protocol_llm::IdentityProfile::ClaudeCode;
    assert!(matches!(Endpoints::new(Box::new([codex]), 1), Err(EndpointError::Identity)));
}

#[test]
fn reasoning_drop_is_selected_per_declared_model() {
    use smith_protocol_llm::{ConfiguredModel, OversizedReasoning};
    let mut endpoint = destination(1, 0, shared::Provider::OpenAiCodex);
    endpoint.models = Box::new([
        ConfiguredModel { name: b"strict".as_slice().into(), oversized_reasoning: OversizedReasoning::Fail },
        ConfiguredModel { name: b"optional".as_slice().into(), oversized_reasoning: OversizedReasoning::Drop },
    ]);
    let endpoints = Endpoints::new(Box::new([endpoint]), 1).expect("named models");
    let options = endpoints.resolve(llm::Endpoint(1)).expect("endpoint");
    assert!(!options.drop_reasoning(b"strict"));
    assert!(options.drop_reasoning(b"optional"));
    assert!(!options.drop_reasoning(b"undeclared"));
    assert!(!options.drop_reasoning(b"strict"));
}

#[test]
fn a_withdrawn_pair_keeps_the_bearer_in_value() {
    let mut grants = Grants::new(1, 64).expect("bounded table");
    let name = GrantName { account: 0, generation: 1 };
    let input = Credential::codex(b"bearer-bytes".as_slice().into(), b"account-id".as_slice().into());
    grants.grant(name, input, Time::from_nanos(100)).expect("grant");
    let output = grants.read(name, Time::ZERO).expect("read grant");
    assert!(output.value.as_ref() == b"bearer-bytes");
    assert!(output.account_id.as_ref() == b"account-id");
    assert_eq!(output.scheme, shared::Scheme::Bearer);
}

// profile::ACCOUNTS admits four distinct names, independent of their numeric value.
const ACCOUNT_COUNT: u32 = 4;
const SPARSE_ACCOUNT: u32 = 0x9f3a_0001;
const GRANT_BYTES: u32 = 64;

#[test]
fn a_sparse_account_number_is_granted_and_used() {
    let mut grants = Grants::new(ACCOUNT_COUNT, GRANT_BYTES).expect("bounded table");
    let name = GrantName { account: SPARSE_ACCOUNT, generation: 1 };
    grants.grant(name, Credential::key(b"key".as_slice().into()), Time::from_nanos(100)).expect("sparse grant");
    let installed = grants.read(name, Time::ZERO).expect("sparse name resolves");
    assert_eq!(installed.scheme, shared::Scheme::Key);
    assert!(installed.account_id.is_empty());
}

#[test]
fn a_fifth_distinct_account_is_refused() {
    let mut grants = Grants::new(ACCOUNT_COUNT, GRANT_BYTES).expect("bounded table");
    for account in 0..ACCOUNT_COUNT {
        grants
            .grant(GrantName { account, generation: 1 }, credential(b"value", b""), Time::from_nanos(100))
            .expect("four accounts fit");
    }
    assert_eq!(
        grants.grant(
            GrantName { account: SPARSE_ACCOUNT, generation: 1 },
            credential(b"value", b""),
            Time::from_nanos(100)
        ),
        Err(GrantError::Full)
    );
}
