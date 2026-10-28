mod native;
mod programs;
mod prompt_line;

use super::*;
use prompt_line::assert_native_prompt_line;

fn document() -> Document {
    Document {
        profile: ProfileDocument {
            name: "standard".into(),
            declared: DeclaredOverrides {
                memory: Some(u64::MAX),
                tool_payload: Some(256),
                read_window: Some(256),
                ..DeclaredOverrides::default()
            },
            policy: PolicyOverrides::default(),
        },
        endpoints: serde_json::from_value(shaped_document()["endpoints"].clone()).expect("tiny endpoint"),
        environment: vec![],
        trace: None,
        programs: std::collections::BTreeMap::new(),
    }
}

#[test]
fn an_endpoints_account_is_read_and_ignored() {
    for account in [None, Some(u32::MAX)] {
        let mut document = document();
        let mut endpoint = shaped_document()["endpoints"][0].clone();
        endpoint.as_object_mut().expect("endpoint object").remove("account");
        if let Some(account) = account {
            endpoint["account"] = account.into();
        }
        document.endpoints[0] = serde_json::from_value(endpoint).expect("withdrawn account or absent");
        document.endpoints[0].dialect = 1;
        let configuration = build(document).expect("withdrawn account is ignored");
        let endpoint = configuration.service.channel_endpoints.resolve(b"local").expect("configured endpoint");
        assert_eq!((endpoint.number, endpoint.dialect), (0, 1));
    }
}

#[test]
fn empty_endpoint_and_model_lists_name_the_missing_field() {
    let mut empty = document();
    empty.endpoints.clear();
    assert_eq!(build(empty).err().as_deref(), Some("field: declared.endpoints must not be empty"));
    let mut empty = document();
    empty.endpoints[0].models.clear();
    assert_eq!(build(empty).err().as_deref(), Some("field: declared.models[endpoint=0] must not be empty"));
}

#[test]
fn shared_environment_validation_names_the_invalid_or_repeated_position() {
    for (name, value) in [("", "v"), ("A=B", "v"), ("A\0B", "v"), ("A", "v\0")] {
        let mut document = document();
        document.environment = vec![Variable { name: name.into(), value: value.into() }];
        assert_eq!(build(document).err().as_deref(), Some("environment: entry 0 has an invalid or repeated name"));
    }
    let mut document = document();
    document.environment = vec![
        Variable { name: "A".into(), value: "first".into() },
        Variable { name: "A".into(), value: "second".into() },
    ];
    assert_eq!(build(document).err().as_deref(), Some("environment: entry 1 has an invalid or repeated name"));
}

#[test]
fn empty_and_equals_values_reach_the_shared_environment_encoder() {
    let mut document = document();
    document.environment = vec![
        Variable { name: "EMPTY".into(), value: String::new() },
        Variable { name: "MULTI".into(), value: "a=b".into() },
    ];
    let configuration = build(document).expect("valid environment");
    assert_eq!(configuration.declarations.environment_bytes, 15);
    assert_eq!(configuration.service.limits.machine.env_bytes, 15);
}

#[test]
fn a_trust_file_that_is_not_a_regular_file_is_refused_naming_it() {
    let path = skein_scratch::paths::package();
    let error = trust(path.to_str()).expect_err("directory is not a certificate file");
    assert!(error.contains(path.to_str().expect("fixture path")), "refusal names the path");
    assert!(error.contains("NotAFile"), "refusal names the non-file");
}

#[test]
fn a_machine_bundle_past_its_certificate_bound_is_refused_naming_the_bound() {
    let path = std::env::temp_dir().join(format!("smith-trust-bundle-{}.pem", std::process::id()));
    std::fs::write(&path, b"-----BEGIN CERTIFICATE-----\nAA==\n-----END CERTIFICATE-----\n".repeat(513))
        .expect("bounded PEM fixture");
    let error = trust_source(&skein_shell::Trust::Machine { ssl_cert_file: Some(path.clone()) })
        .expect_err("too many certificates");
    std::fs::remove_file(path).expect("remove fixture");
    assert_eq!(error, "trust roots exceed 512 certificates");
}

#[test]
fn named_endpoint_resolves_and_its_tls_trust_builds_before_start() {
    let mut document = document();
    document.endpoints.clear();
    document.endpoints.push(Endpoint {
        connect: 10_000,
        handshake: 10_000,
        models: vec![ModelDocument {
            prices: PriceDocument { input: 1, cached: 1, cache_write: None, output: 1, unit: 1 },
            name: "test".into(),
            window: 65_536,
            output: 512,
            reasoning_item: 64,
            oversized_reasoning: None,
            head: 60_000,
            idle: 30_000,
            efforts: Vec::new(),
        }],
        name: "test".into(),
        number: 42,
        dialect: 1,
        _account: 0,
        provider: "codex".into(),
        address: "127.0.0.1:443".into(),
        transport: "tls".into(),
        server_name: Some("example.test".into()),
        authority: None,
        target: None,
        trust_der: Some(
            skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string(),
        ),
        trust: None,
        headers: vec![],
        identity: None,
        program: None,
    });
    let configuration = build(document).expect("resolved endpoint");
    service::Service::new(configuration.service, 1).expect("LLM and channel endpoints");
}

#[test]
fn a_dialect_name_and_number_must_match() {
    let rows = [
        ("openai", 3, None),
        ("anthropic", 2, None),
        ("codex", 1, None),
        ("openai", 1, Some("endpoint dialect does not match provider")),
        ("anthropic", 3, Some("endpoint dialect does not match provider")),
    ];
    for (index, (provider, dialect, expected)) in rows.into_iter().enumerate() {
        let mut endpoint = document().endpoints.remove(0);
        endpoint.provider = provider.into();
        endpoint.dialect = dialect;
        let refusal = build_models(&endpoint).err().map(ConfigurationRefusal::detail);
        assert_eq!(refusal.as_deref(), expected, "row {index}");
    }
}

#[test]
fn an_openai_endpoint_takes_the_public_route() {
    let mut document = document();
    let name = document.endpoints[0].number;
    document.endpoints[0].provider = "openai".into();
    document.endpoints[0].dialect = 3;
    let configured = build(document).expect("public endpoint derives");
    let options = configured.service.llm_endpoints.resolve(domain::llm::Endpoint(name)).expect("endpoint");
    assert_eq!(options.provider, shared::Provider::OpenAi);
    assert_eq!(options.endpoint.authority.as_ref(), b"api.openai.com");
    assert_eq!(options.endpoint.target.as_ref(), b"/v1/responses");
}

#[test]
fn affinity_header_names_and_static_cache_keys_are_refused_in_configuration() {
    for name in ["session-id", "SESSION-ID", "thread-id", "ThReAd-Id"] {
        let json = format!(
            r#"{{"profile":{{"name":"standard","declared":{{"memory":1099511627776,"tool_payload":256,"read_window":256}},"policy":{{"group_stop":10}}}},"endpoints":[{{"name":"local","number":1,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","connect":10000,"handshake":10000,"models":[{{"name":"test","window":65536,"output":512,"reasoning_item":64,"prices":{{"input":1,"cached":1,"output":1,"unit":1}},"head":60000,"idle":30000}}],"transport":"plaintext","headers":[{{"name":"{name}","value":"host override"}}]}}],"environment":[]}}"#
        );
        assert_eq!(parse(json.as_bytes()).err().as_deref(), Some("invalid endpoint HTTP header"));
    }
    let error = serde_json::from_str::<Endpoint>(r#"{"name":"old","number":1,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","transport":"plaintext","cache_key":"static"}"#).err().expect("removed field is rejected");
    assert!(error.to_string().contains("unknown field `cache_key`"));
}

#[test]
fn plaintext_configuration_names_its_transport_and_admits_only_loopback() {
    for address in ["127.0.0.1:8080", "[::1]:8080"] {
        let json = format!(
            r#"{{"profile":{{"name":"standard","declared":{{"memory":1099511627776,"tool_payload":256,"read_window":256}},"policy":{{"group_stop":10}}}},
            "endpoints":[{{"name":"local","number":1,"dialect":1,"account":0,"provider":"codex",
            "address":"{address}","connect":10000,"handshake":10000,"models":[{{"name":"test","window":65536,"output":512,"reasoning_item":64,"prices":{{"input":1,"cached":1,"output":1,"unit":1}},"head":60000,"idle":30000}}],"transport":"plaintext"}}],"environment":[]}}"#
        );
        let configuration = parse(json.as_bytes()).expect("loopback plaintext configuration");
        service::Service::new(configuration.service, 1).expect("loopback plaintext service");
    }
    for address in ["192.0.2.1:8080", "[2001:db8::1]:8080"] {
        let json = format!(
            r#"{{"profile":{{"name":"standard","declared":{{"memory":1099511627776,"tool_payload":256,"read_window":256}},"policy":{{"group_stop":10}}}},
            "endpoints":[{{"name":"local","number":1,"dialect":1,"account":0,"provider":"codex",
            "address":"{address}","connect":10000,"handshake":10000,"models":[{{"name":"test","window":65536,"output":512,"reasoning_item":64,"prices":{{"input":1,"cached":1,"output":1,"unit":1}},"head":60000,"idle":30000}}],"transport":"plaintext"}}],"environment":[]}}"#
        );
        assert_eq!(parse(json.as_bytes()).err().as_deref(), Some("plaintext endpoint address must be loopback"));
    }
}

#[test]
fn ambiguous_or_unknown_endpoint_transports_refuse_at_configuration() {
    for extra in [
        r#""transport":"plaintext","server_name":"example.test""#,
        r#""transport":"plaintext","trust_der":"missing.der""#,
        r#""transport":"unknown""#,
        r#""transport":"tls""#,
    ] {
        let json = format!(
            r#"{{"profile":{{"name":"standard","declared":{{"memory":1099511627776,"tool_payload":256,"read_window":256}},"policy":{{"group_stop":10}}}},
            "endpoints":[{{"name":"local","number":1,"dialect":1,"account":0,"provider":"codex",
            "address":"127.0.0.1:8080","connect":10000,"handshake":10000,"models":[{{"name":"test","window":65536,"output":512,"reasoning_item":64,"prices":{{"input":1,"cached":1,"output":1,"unit":1}},"head":60000,"idle":30000}}],{extra}}}],"environment":[]}}"#
        );
        assert!(parse(json.as_bytes()).is_err(), "invalid transport {extra}");
    }
}

#[test]
fn overflowing_declared_durations_name_the_first_field_and_its_scope() {
    let mut policy = document();
    policy.profile.policy.max_time = Some(u64::MAX);
    assert_eq!(build(policy).err().as_deref(), Some("derived: policy.max_time does not fit its type"));
    let endpoint = LimitName { layer: Layer::Declared, field: "idle", per: Per::Model { endpoint: 7, model: 3 } };
    assert_eq!(
        duration(u64::MAX, endpoint).err().map(ConfigurationRefusal::detail).as_deref(),
        Some("derived: declared.idle[endpoint=7,model=3] does not fit its type")
    );
}

#[test]
fn bad_profile_and_zero_grace_refuse_before_a_channel_is_opened() {
    let mut bad = document();
    bad.profile.name = "unknown".into();
    assert!(build(bad).is_err());
    let mut bad = document();
    bad.profile.policy.group_stop = Some(0);
    assert!(build(bad).is_err());
}

#[test]
fn malformed_json_refuses_missing_required_configuration_fields() {
    assert!(serde_json::from_slice::<Document>(br#"{"profile":"standard"}"#).is_err());
    assert!(serde_json::from_slice::<Document>(br#"{"profile":"standard","memory_bytes":1,"grace_ms":1,"endpoints":[],"environment":[],"credential":"bad"}"#).is_err());
}

fn shaped_document() -> serde_json::Value {
    serde_json::json!({
        "profile":{"name":"standard","declared":{"memory":2_147_483_648_u64,"tool_payload":256,"read_window":256}},
        "endpoints":[{"name":"local","number":0,"dialect":1,"account":0,"provider":"codex",
            "address":"127.0.0.1:8080","transport":"plaintext","connect":10000,"handshake":10000,
            "models":[{"name":"test","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":60000,"idle":30000}]}],
        "environment":[]
    })
}

fn parse_value(value: &serde_json::Value) -> Result<Configuration, String> {
    parse(&serde_json::to_vec(value).expect("configuration JSON"))
}

#[test]
fn omitted_memory_uses_the_standard_process_declaration() {
    let mut document = shaped_document();
    document["profile"]["declared"].as_object_mut().expect("declarations").remove("memory");
    let configuration = parse_value(&document).expect("default memory is accepted");
    assert_eq!(configuration.memory, 2_281_701_376);
}

#[test]
fn a_declared_memory_below_the_old_worst_case_starts() {
    let mut document = shaped_document();
    document["profile"]["declared"]["memory"] = 1.into();
    let configuration = from_value(document).expect("one declared byte is accepted");
    assert_eq!(configuration.memory, 1);
    assert_eq!(configuration.service.limits.memory, 1);
    crate::Agent::new(
        configuration,
        crate::Resources {
            input: skein_io::kernel::Fd::new(0),
            output: skein_io::kernel::Fd::new(1),
            signals: skein_io::kernel::Fd::new(2),
            seed: 1,
            pid: 1,
            trace: None,
            error: None,
            roots: None,
            programs: None,
        },
        Box::new(std::io::sink()),
    )
    .expect("one declared byte starts through prepare");
}

#[test]
fn quantities_override_one_value_and_derive_through_the_service() {
    let mut document = shaped_document();
    document["profile"]["declared"]["conversations"] = 2.into();
    document["profile"]["declared"]["shell_tail"] = 64.into();
    document["profile"]["policy"] =
        serde_json::json!({"inbox":3,"group_stop":7,"max_turns":9,"unacknowledged":2,"job_poll":2000,"job_wait":7000});
    let configured = parse_value(&document).expect("quantity overrides");
    let limits = configured.service.limits;
    assert_eq!(limits.domain.run.conversations, 2);
    assert_eq!(limits.domain.session.sessions, 2);
    assert_eq!(limits.domain.session.tools.kits, 2);
    assert_eq!(limits.domain.run.messages, 3);
    assert_eq!(limits.domain.run.budget.turns, 9);
    assert_eq!(limits.channel.turns, 2);
    assert_eq!(limits.machine.group_stop, Duration::from_millis(7));
    assert_eq!(configured.profile.policy.job_poll, Duration::from_secs(2));
    assert_eq!(configured.profile.policy.job_wait, Duration::from_secs(7));
    assert_eq!(limits.domain.session.tools.job_poll, Duration::from_secs(2));
    assert_eq!(limits.domain.session.tools.job_wait, Duration::from_secs(7));
    assert_eq!(limits.llm.adapter.stated.job_poll, Duration::from_secs(2));
    assert_eq!(limits.llm.adapter.stated.job_wait, Duration::from_secs(7));
    assert_eq!(limits.domain.session.tools.shell_tail, 64);
    assert_eq!(limits.domain.run.check_tail, 64);
    assert_eq!(limits.domain.session.tools.tool_deadline, configured.profile.policy.tool_deadline);
    assert_eq!(limits.domain.session.tools.shell_output, configured.profile.declared.shell_output);
    assert_eq!(limits.domain.run.host_timeout_max, configured.profile.policy.tool_deadline);
    assert_eq!(limits.llm.adapter.stated.tool_deadline, configured.profile.policy.tool_deadline);
    assert_eq!(configured.profile.declared.shell_head, service::profile::standard().declared.shell_head);
    assert_eq!(configured.declarations.endpoints[0].models[0].window, 65536);
}

#[test]
fn configuration_refuses_old_shape_raw_limits_duplicates_and_zero_quantities() {
    assert!(
        parse(br#"{"profile":"standard","memory_bytes":1099511627776,"grace_ms":10,"endpoints":[],"environment":[]}"#)
            .is_err()
    );
    for field in ["request_bytes", "unexpected"] {
        let mut document = shaped_document();
        document["profile"]["declared"][field] = 100.into();
        assert!(parse_value(&document).is_err(), "raw or unknown field {field}");
    }
    let mut document = shaped_document();
    let duplicate = document["endpoints"][0]["models"][0].clone();
    document["endpoints"][0]["models"].as_array_mut().expect("models").push(duplicate);
    assert_eq!(parse_value(&document).err().as_deref(), Some("duplicate declared model"));
    for field in ["window", "output", "reasoning_item", "head", "idle"] {
        let mut document = shaped_document();
        document["endpoints"][0]["models"][0][field] = 0.into();
        assert!(parse_value(&document).is_err(), "zero model quantity {field}");
    }
    for field in ["conversations", "tool_payload", "file_bytes", "calls_per_response", "memory"] {
        let mut document = shaped_document();
        document["profile"]["declared"][field] = 0.into();
        assert!(parse_value(&document).is_err(), "zero deployment quantity {field}");
    }
}
#[test]
fn the_whole_file_declaration_is_independent_and_payload_checked() {
    let mut document = shaped_document();
    document["profile"]["declared"]["file_bytes"] = 1_048_576.into();
    let configured = parse_value(&document).expect("explicit whole file ceiling");
    assert_eq!(configured.profile.declared.file_bytes, 1_048_576);
    document["profile"]["declared"]["file_bytes"] = 1.into();
    let reason = parse_value(&document).err().expect("payload does not fit");
    assert!(reason.starts_with("payload-in-file:"), "{reason}");
}
#[test]
fn oversized_reasoning_defaults_to_fail_and_drop_requires_explicit_selection() {
    let mut document = shaped_document();
    let configured = parse_value(&document).expect("default policy");
    assert_eq!(
        configured.declarations.endpoints[0].models[0].oversized_reasoning,
        service::profile::OversizedReasoning::Fail
    );
    document["endpoints"][0]["models"][0]["oversized_reasoning"] = "drop".into();
    let configured = parse_value(&document).expect("explicit drop");
    assert_eq!(
        configured.declarations.endpoints[0].models[0].oversized_reasoning,
        service::profile::OversizedReasoning::Drop
    );
    document["endpoints"][0]["models"][0]["oversized_reasoning"] = "fallback".into();
    assert_eq!(parse_value(&document).err().as_deref(), Some("model oversized_reasoning must be fail or drop"));
}

#[path = "../../../tests/support/product.rs"]
mod product;

use product::product_document;

fn all_product_models(mut document: serde_json::Value) -> serde_json::Value {
    let mut astra = document["endpoints"][0]["models"][0].clone();
    astra["name"] = "gpt-6-astra".into();
    document["endpoints"][0]["models"].as_array_mut().unwrap().push(astra);
    let mut anthropic = document["endpoints"][0].clone();
    anthropic["name"] = "anthropic".into();
    anthropic["number"] = 2.into();
    anthropic["dialect"] = 2.into();
    anthropic["provider"] = "anthropic".into();
    anthropic["server_name"] = "api.anthropic.com".into();
    anthropic["identity"] = "claude-code".into();
    let mut models = Vec::new();
    for name in ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"] {
        let mut model = document["endpoints"][0]["models"][0].clone();
        model["name"] = name.into();
        model["window"] = 200_000.into();
        model["idle"] = 120_000.into();
        models.push(model);
    }
    anthropic["models"] = models.into();
    document["endpoints"].as_array_mut().unwrap().push(anthropic);
    let trust = skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der");
    for endpoint in document["endpoints"].as_array_mut().unwrap() {
        endpoint["trust_der"] = trust.to_str().expect("fixture trust path").into();
    }
    document
}

#[test]
fn native_validated_dense_prompts_fit_the_actual_joint_receiving_ceiling() {
    let configured = parse_value(&product_document()).expect("actual product startup");
    let client = configured.service.limits.llm.adapter.client;
    let events = configured.service.limits.events.expect("actual product destination");
    let ceiling = events.sinks.line_ceiling(Capture::Everything).expect("joint ceiling");
    let credential = shared::Credential {
        value: b"secret-prompt-bound-token".as_slice().into(),
        account_id: b"secret-prompt-bound-account".as_slice().into(),
        scheme: skein_llm::Scheme::Bearer,
    };
    for dense in [true, false] {
        let count = if dense { client.history_items } else { 8 };
        let text = if dense { b"x".as_slice().into() } else { vec![0; 256_000].into_boxed_slice() };
        let native = shared::Prompt {
            model: b"gpt-6-luna".as_slice().into(),
            instructions: b"native system".as_slice().into(),
            tools: Box::new([]),
            choice: shared::ToolChoice::None,
            messages: (0..count)
                .map(|_| shared::Message {
                    role: shared::Role::User,
                    content: Box::new([shared::Block::Text { text: text.clone(), replay: None }]),
                })
                .collect(),
            reasoning_effort: None,
            affinity: None,
            max_output_tokens: None,
        };
        let measured = shared::client::measure(&native, Some(&credential), &shared::Endpoint::codex(), &client)
            .expect("the actual native writer admits the complete source payload");
        assert!(measured.body <= client.request, "native body stays within its owning cap");
        let mut messages = List::with_capacity(count);
        for _ in 0..count {
            let mut blocks = List::with_capacity(1);
            blocks.push(smith_events::Block::Text { text: text.clone() }).expect("one actual block");
            messages
                .push(smith_events::Message { role: smith_events::Role::User, blocks })
                .expect("actual admitted history");
        }
        let record = smith_events::Record {
            t_ms: u64::MAX,
            event: smith_events::Event::ResponseStarted(smith_events::ResponseStarted {
                run: u64::MAX,
                conversation: u64::MAX,
                response: u64::MAX,
                turn: u64::MAX,
                attempt: u64::MAX,
                window: u64::MAX,
                model: smith_events::Model {
                    account: None,
                    backend: smith_events::Backend::Direct,
                    endpoint: b"codex".as_slice().into(),
                    model: native.model.clone(),
                    window_tokens: 272_000,
                    output_tokens: 32_000,
                    effort: Some(b"low".as_slice().into()),
                },
                messages: u64::from(count),
                output_tokens: 32_000,
                tool_choice: smith_events::ToolChoice::None,
                prompt: Some(smith_events::Prompt {
                    system: Some(native.instructions.clone()),
                    tools: Some(List::with_capacity(0)),
                    messages,
                }),
            }),
        };
        let line = smith_events::write_with_ceiling(
            &record,
            &smith_events::Capture::Everything,
            &events.sinks.records,
            ceiling,
        )
        .expect("source-admitted prompt fits both semantic and joint caps")
        .expect("actual response start is emitted");
        assert!(
            line.len() > if dense { 1_000_000 } else { 12_000_000 },
            "exercise dense framing and escaped payload maxima"
        );
        assert_native_prompt_line(&line, &native, &text, &credential);
    }
}

#[test]
fn product_values_pass_every_startup_check() {
    check_product_startup(false);
}

#[test]
fn a_program_product_passes_every_startup_check() {
    check_product_startup(true);
}

fn check_product_startup(with_program: bool) {
    let mut document = all_product_models(product_document());
    if with_program {
        document["programs"] = serde_json::json!({"claude_code": {"path":"/programs/claude", "directory":"/programs"}});
        document["endpoints"][1]["program"] = "claude_code".into();
    }
    let configuration = parse_value(&document).expect("product declarations pass every startup check");
    let limits = &configuration.service.limits;
    assert!(limits.file_bytes >= limits.domain.session.tools.file_bytes);
    assert!(configuration.service.domain.valid(&limits.domain), "product declarations fit the root");
}

#[test]
fn invalid_limits_are_refused() {
    for domain in [false, true] {
        let mut configuration = build(document()).expect("valid test configuration");
        assert!(check_limits(&configuration.service).is_ok());
        if domain {
            configuration.service.limits.domain.run.conversations =
                configuration.service.limits.domain.session.sessions + 1;
        } else {
            configuration.service.limits.io.receive += 1;
        }
        let refusal = check_limits(&configuration.service).expect_err("invalid limits");
        assert!(matches!(refusal, ConfigurationRefusal::Profile(Refusal::Derived { limit })
            if limit == LimitName::deployment(Layer::Service, "limits")));
    }
}
