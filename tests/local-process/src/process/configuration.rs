//! Native local policy and generated agent configuration for the shared test world.
use super::Launch;
use skein_io::kernel;
use skein_lib::Duration;
use smith_agent_service as agent;
use smith_local_domain as local;
use smith_local_service as service;
use smith_local_shell::local_host::token_limits;
use std::path::PathBuf;

/// The bounded agent settings shared by both local placements.
pub fn lower_configuration() -> agent::Config {
    lower_configuration_with(false)
}

pub(super) fn specialize(mut lower: agent::Config) -> agent::Config {
    let fixture = smith_agent_process_world::limits();
    lower.limits.llm.connection.endpoints = fixture.llm.connection.endpoints;
    lower.limits.llm.connection.connections = fixture.llm.connection.connections;
    lower.limits.llm.connection.calls = fixture.llm.connection.calls;
    lower.limits.llm.connection.per_endpoint = fixture.llm.connection.per_endpoint;
    lower.limits.llm.accounts = fixture.llm.accounts;
    lower.limits.llm.grant_value_bytes = fixture.llm.grant_value_bytes;
    lower.limits.file_timeout = fixture.file_timeout;
    for model in &mut lower.domain.models {
        model.head = Duration::from_secs(10);
    }
    lower.limits.domain.session.tools.file_bytes = 4096;
    lower.limits.domain.session.tools.list_entries = 32;
    lower.limits.domain.session.tools.list_bytes = 512;
    lower.limits.machine.file_bytes = lower.limits.domain.session.tools.file_bytes;
    lower.limits.machine.path_bytes = lower.limits.domain.session.tools.path_bytes;
    lower.limits.machine.search_hits = lower.limits.domain.session.tools.search_hits;
    lower.limits.machine.search_bytes = lower.limits.domain.session.tools.search_bytes;
    lower.limits.machine.output_bytes = lower
        .limits
        .domain
        .run
        .check_tail
        .max(lower.limits.domain.session.tools.shell_head)
        .max(lower.limits.domain.session.tools.shell_tail);
    lower.limits.machine.entries = 32;
    lower.limits.file_bytes = 8192;
    lower.limits.file_entries = 32;
    lower.limits.file_slots = 8;
    lower
}

/// Plaintext local configuration for a replayable simulated invocation.
pub fn configuration(root: kernel::Fd, change: bool) -> (service::Config, agent::Config) {
    configuration_with(root, change, false)
}

/// Same bounded local policy with transport selected by the scenario settings.
#[must_use]
pub fn configuration_with(root: kernel::Fd, change: bool, tls: bool) -> (service::Config, agent::Config) {
    configuration_with_tools(root, change, tls, false)
}

/// The shared policy optionally requires the extended tool story's real checks.
#[must_use]
pub fn configuration_with_tools(
    root: kernel::Fd,
    change: bool,
    tls: bool,
    tools: bool,
) -> (service::Config, agent::Config) {
    configuration_for(root, change, tools, lower_configuration_with(tls))
}

pub(super) fn configuration_for_launch(root: kernel::Fd, launch: &Launch) -> (service::Config, agent::Config) {
    let configured = smith_agent_shell::config::from_value(agent_document(launch)).expect("generated fixture JSON");
    let (mut local, lower) = configuration_for(root, launch.change, launch.tools, specialize(configured.service));
    if launch.lending == Some(crate::world::Lending::Key) {
        for model in &mut local.local.models {
            model.dialect = 2;
        }
    }
    (local, lower)
}

fn configuration_for(
    root: kernel::Fd,
    change: bool,
    tools: bool,
    lower: agent::Config,
) -> (service::Config, agent::Config) {
    let endpoints = lower.channel_endpoints.clone();
    let mut policy = local_policy(&lower, change, tools);
    let mut host = smith_host_world::limits();
    host.cancel_grace = agent::profile::standard().policy.cancel_grace;
    host.accounts = 1;
    host.charter_bytes = 65_536;
    host.transcript_bytes = 1_048_576;
    host.answered_bytes = 65_536;
    host.message_bytes = 4096;
    host.messages = 8;
    host.call_bytes = 32_768;
    host.turns = 8;
    host.turn_bytes = 65_536;
    host.unacknowledged_bytes = 1_000_000_000;
    host.fact_bytes = 512;
    host.outcome_bytes = 4096;
    // A workspace's host path is the checkout's absolute path: production's
    // bound keeps the story independent of where the checkout lives.
    host.path_bytes = 4096;
    // Actual provider spans carry the charter's whole-session deadline.
    host.long_span = policy.0.budget.time;
    let inline_out = local::max_out(&policy.1);
    policy.1.agent = local::AgentLimits::Spawned(host);
    let queue = local::max_out(&policy.1).max(inline_out).max(smith_host_domain::max_out(&host)).max(256);
    let mut process = service::ProcessLimits {
        io: lower.limits.io,
        channel: smith_host_protocol::Limits {
            bodies: lower.limits.channel.bodies,
            charter: lower.limits.channel.charter,
            transcript: lower.limits.channel.transcript,
            endpoints: lower.limits.channel.endpoints,
            channel: lower.limits.channel.channel,
            start_bytes: lower.limits.channel.start_bytes,
            calls: host.calls,
        },
        detail_bytes: host.detail_bytes,
        queue,
    };
    process.channel.channel.output_bytes =
        smith_host_protocol::output_bytes(&process.channel).expect("host directional frame");
    (
        service::Config {
            agent: service::AgentConfig::Spawned,
            local: policy.0,
            limits: service::Limits {
                local: policy.1,
                envelope_bytes: host.answer_bytes,
                process,
                queue,
                watermark: queue,
            },
            endpoints,
            paths: policy.2,
            launch: service::Launch {
                program: b"smith-agent".as_slice().into(),
                arguments: Box::new([]),
                environment: Box::new([]),
                root,
                directory: b".".as_slice().into(),
            },
        },
        lower,
    )
}

fn local_policy(lower: &agent::Config, change: bool, tools: bool) -> (local::Config, local::Limits, Box<[Box<[u8]>]>) {
    use smith_domain::run::{self, charter, outcome};
    let paths: Box<[Box<[u8]>]> = if change {
        Box::new([std::path::absolute("repo").unwrap().as_os_str().as_encoded_bytes().into()])
    } else {
        Box::new([])
    };
    let workspace = change.then(|| run::Workspace {
        directories: Box::new([run::Directory {
            host: paths[0].clone(),
            name: b"repo".as_slice().into(),
            root: skein_lib::Token::new(1),
            writable: true,
            git: true,
            conflicts: Box::new([]),
        }]),
    });
    let config = local::Config {
        chat: b"main".as_slice().into(),
        instructions: b"@local-shell Assist".as_slice().into(),
        brief: charter::Brief { sections: Box::new([]) },
        models: Box::new([charter::Llm {
            effort: None,
            prices: run::Prices {
                input: 1_250_000,
                cached: 125_000,
                cache_write: None,
                output: 10_000_000,
                unit: 1_000_000,
            },
            dialect: 1,
            account: Some(0),
            endpoint: charter::Endpoint(0),
            model: b"fake".as_slice().into(),
            window: 1_048_576,
            output: 512,
        }]),
        efforts: Box::new([Box::new([])]),
        sub_agents: false,
        protocol_allowances: Box::new([lower.domain.models[0].protocol_allowance]),
        progress: Box::new([smith_domain::session::Progress {
            head: Duration::from_secs(60),
            idle: Duration::from_secs(30),
        }]),
        budget: run::Budget { turns: 512, spend: 25_000_000, time: Duration::from_secs(3600) },
        reserve: charter::Reserve { turns: 2, time: Duration::from_secs(300) },
        conventions: tools.then(|| run::Conventions {
            guide: b".smith-test/guide".as_slice().into(),
            checks: b".smith-test/check".as_slice().into(),
        }),
        contract: if change {
            local::Contract::Change(outcome::ChangeSpec {
                checks_must_pass: tools,
                fields: Box::new([
                    outcome::FieldRule { name: b"title".as_slice().into(), max: 256 },
                    outcome::FieldRule { name: b"body".as_slice().into(), max: 1024 },
                ]),
            })
        } else {
            local::Contract::Report(outcome::TextSpec { max: 128, fields: Box::new([]) })
        },
        deliver: None,
        title_field: b"title".as_slice().into(),
        live_text: false,
        mode: local::Mode::Chat { waiting: Duration::from_secs(30) },
        resume: true,
        accounts: Box::new([0]),
        workspace,
        push: None,
    };
    let native = lower.limits.domain;
    let limits = local::Limits {
        smith: native,
        agent: local::AgentLimits::inline(
            native,
            2,
            config.budget.time,
            agent::profile::standard().policy.cancel_grace,
        )
        .expect("inline fixture bounds"),
        endpoints: Box::new([charter::Endpoint(0)]),
        chat_bytes: 256,
        text_bytes: 1 << 16,
        models: 3,
        line_bytes: local::max_line_bytes(native.run.message_bytes).unwrap(),
        show_bytes: 8192,
        unsaved: 2,
        facts: 1024,
    };
    config.validate(&limits).expect("typed fixture local policy");
    (config, limits, paths)
}

/// The same agent policy, with loopback plaintext or trust in the test root.
#[must_use]
pub fn lower_configuration_with(tls: bool) -> agent::Config {
    static PLAINTEXT: std::sync::OnceLock<agent::Config> = std::sync::OnceLock::new();
    static TLS: std::sync::OnceLock<agent::Config> = std::sync::OnceLock::new();
    let stored = if tls { &TLS } else { &PLAINTEXT };
    let lower = stored.get_or_init(|| {
        let configuration = smith_agent_shell::config::from_value(agent_document(&fixture_launch(tls)))
            .expect("generated fixture JSON");
        specialize(configuration.service)
    });
    agent::Config {
        program: None,
        limits: lower.limits,
        domain: lower.domain.clone(),
        channel_endpoints: lower.channel_endpoints.clone(),
        llm_endpoints: cloned_endpoints(lower),
        environment: lower.environment.clone(),
        stream_mode: lower.stream_mode,
    }
}

fn cloned_endpoints(lower: &agent::Config) -> smith_protocol_llm::Endpoints {
    use skein_llm_connection::{Endpoint, Transport};
    let configured = lower
        .domain
        .endpoints
        .iter()
        .map(|name| {
            let name = smith_domain::llm::Endpoint(name.0);
            let options = lower.llm_endpoints.resolve(name).expect("configured fixture endpoint");
            let destination =
                lower.llm_endpoints.destinations().get(options.index).expect("resolved fixture destination");
            let transport = match &destination.transport {
                Transport::Plaintext => Transport::Plaintext,
                Transport::Tls { server_name, trust } => {
                    Transport::Tls { server_name: server_name.clone(), trust: trust.clone() }
                }
            };
            smith_protocol_llm::ConfiguredEndpoint {
                name,
                destination: Endpoint {
                    address: destination.address,
                    transport,
                    llm: destination.llm.clone(),
                    limits: destination.limits,
                    credential: destination.credential,
                },
                connect: options.connect,
                handshake: options.handshake,
                identity: options.identity,
                models: options.models.clone(),
            }
        })
        .collect();
    smith_protocol_llm::Endpoints::new(configured, lower.limits.llm.connection.endpoints)
        .expect("unchanged checked fixture endpoints")
}

fn fixture_launch(tls: bool) -> Launch {
    Launch {
        seed: 7,
        tls,
        trust_der: tls
            .then(|| PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../protocol-llm/fixtures/root.der"))),
        change: false,
        tools: false,
        read_report: false,
        headless: false,
        interactive: false,
        choices: false,
        budget_failure: false,
        placement: crate::Placement::InProcess,
        keep_facts: false,
        traced: false,
        authenticated: false,
        lending: None,
        state_directory: PathBuf::new(),
        token_directory: PathBuf::new(),
        root_path: PathBuf::new(),
    }
}

/// Generated agent JSON from the shared tiny declarations, with the spawned trace suffix.
#[must_use]
pub fn agent_document(launch: &Launch) -> serde_json::Value {
    let profile = smith_agent_process_world::profile();
    let declared = profile.declared;
    let policy = profile.policy;
    let mut model = serde_json::json!({"name":"fake","window":1_048_576,"output":512,"reasoning_item":64,
        "head":10000,"idle":30000,"prices":{"input":1,"cached":1,"output":1,"unit":1_000_000}});
    if launch.choices {
        model["efforts"] = serde_json::json!(["low", "high"]);
    }
    let mut models = vec![model.clone()];
    if launch.choices {
        model["name"] = serde_json::json!("alternate");
        model["efforts"] = serde_json::json!(["low"]);
        models.push(model);
    }
    let mut endpoint = serde_json::json!({"name":if launch.choices {"main"} else {"fake"},"number":0,"dialect":1,
        "account":0,"provider":"codex","connect":1000,"handshake":1000,"models":models,
        "identity":"plain","address":format!("{}:{}", smith_agent_process_world::fake::loopback(), if launch.tls {34_443} else {443}),
        "transport":if launch.tls {"tls"} else {"plaintext"},
        "headers":[{"name":"originator","value":"skein-world"}]});
    if launch.lending == Some(crate::world::Lending::Key) {
        endpoint["provider"] = serde_json::json!("anthropic");
        endpoint["dialect"] = serde_json::json!(2);
        endpoint.as_object_mut().expect("endpoint object").remove("identity");
    }
    if launch.tls {
        endpoint["server_name"] = serde_json::json!("skein.test");
        endpoint["trust_der"] = serde_json::json!(launch.trust_der);
    }
    let mut document = serde_json::json!({"profile":{"name":"standard","declared":{
        "conversations":declared.conversations,"calls_per_response":declared.calls_per_response,
        "tool_payload":declared.tool_payload,"file_bytes":declared.file_bytes,"shell_output":declared.shell_output,
        "read_window":declared.read_window,"shell_head":declared.shell_head,"shell_tail":declared.shell_tail,
        "search_hits":declared.search_hits,"search_bytes":declared.search_bytes,"list_entries":declared.list_entries,
        "guide":declared.guide,"memory":declared.memory,"llm_pool":declared.llm_pool},
        "policy":{"max_turns":policy.max_turns,"max_time":policy.max_time.as_nanos()/1_000_000,
        "inbox":policy.inbox,"sections":policy.sections,"host_tools":policy.host_tools,"verdicts":policy.verdicts,
        "check_timeout":policy.check_timeout.as_nanos()/1_000_000,"unacknowledged":policy.unacknowledged,
        "group_stop":policy.group_stop.as_nanos()/1_000_000,"exit_grace":policy.exit_grace.as_nanos()/1_000_000}},
        "endpoints":[endpoint],"environment":[]});
    if launch.traced {
        document["trace"] =
            serde_json::json!({"path":launch.root_path.join("events.jsonl.agent"),"capture":"everything"});
    }
    document
}

/// Hand synthetic records and public registration to the one native account driver.
#[must_use]
pub fn prepared_accounts(
    directory: &std::path::Path,
    authenticated: bool,
    tls: bool,
    root: Option<kernel::Fd>,
    io: skein_io::Limits,
) -> smith_local_shell::credentials::Prepared {
    let mut sources = skein_lib::List::with_capacity(1);
    let source = if authenticated {
        let port = if tls { 34_444 } else { 444 };
        let host = smith_agent_process_world::fake::loopback();
        let registration = skein_oauth::Registration {
            terms: None,
            authorization_url: format!("{}://{host}:{port}/authorize", if tls { "https" } else { "http" })
                .into_bytes()
                .into(),
            token_endpoint: format!("{}://{host}:{port}/token", if tls { "https" } else { "http" }).into_bytes().into(),
            client_id: b"client".as_slice().into(),
            redirect_uri: format!("http://127.0.0.1:{}/callback", smith_agent_process_world::fake::redirect_port())
                .into_bytes()
                .into(),
            scope: b"read".as_slice().into(),
            wire: skein_oauth::WireFormat::Form,
            client_secret: None,
            pkce_for_confidential: false,
            metadata_claim: None,
        };
        service::Source::SignIn {
            account: 0,
            root: root.unwrap_or_else(|| skein_shell::open_root(directory.parent().unwrap()).unwrap()),
            user: skein_shell::effective_user(),
            directory: directory.file_name().unwrap().as_encoded_bytes().into(),
            file: b"0.json".as_slice().into(),
            registration,
            endpoint: skein_oauth_accounts::Endpoint {
                address: (host, port).into(),
                transport: if tls {
                    skein_oauth_accounts::Transport::Tls {
                        server_name: skein_tls_world::pki::name(),
                        trust: skein_tls_world::pki::client(&[]),
                    }
                } else {
                    skein_oauth_accounts::Transport::Plaintext
                },
            },
            account_id: b"acc".as_slice().into(),
            interactive: true,
        }
    } else {
        let mut record = crate::credential_fixture::Tokens::new(directory, token_limits())
            .unwrap()
            .load(0)
            .unwrap()
            .expect("fixture access token");
        record.refresh_token = None;
        service::Source::Env {
            account: 0,
            borrowed: smith_local_protocol::Borrowed { record, account_id: b"acc".as_slice().into() },
        }
    };
    assert!(sources.push(source).is_ok(), "one fixture account");
    smith_local_shell::credentials::Prepared {
        sources,
        locations: skein_lib::List::with_capacity(0),
        lendable: skein_lib::Map::with_capacity(0),
        limits: smith_local_shell::credentials::limits(
            io,
            1,
            smith_agent_service::profile::standard().policy.refresh_lead,
        ),
    }
}
