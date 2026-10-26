//! Generated configuration values or JSON read once before the agent opens its channel.
//! It retains endpoint names and destinations, program paths, limits, command environment,
//! and future trace settings; no credential is read here.
//! Durations in JSON are milliseconds. Contract: protocol/agent.md, sections 4 and 6.

use std::net::SocketAddr;
#[cfg(unix)]
use std::net::ToSocketAddrs;
#[cfg(unix)]
use std::path::Path;

use serde::Deserialize;
use skein_channel::StreamMode;
use skein_http::Header;
use skein_lib::{Duration, List};
use skein_llm as shared;
use skein_llm_connection as connection;
use skein_tls as tls;
use smith_agent_service as service;
use smith_domain::{self as domain, run};
use smith_protocol_channel as channel;
use smith_protocol_llm as llm;
use smith_protocol_machine as machine;

use crate::trace::{Capture, TraceConfig};
use smith_agent_service::profile::{ENDPOINTS, Layer, LimitName, Per, Refusal};

const CONFIG_BYTES: u64 = 1 << 20;

mod profile;
mod programs;
mod refusal;
use profile::build_profile;
pub use programs::{Program, ProgramName};
mod trace;
pub use refusal::ConfigurationRefusal;

/// Parsed startup configuration and the service it can build.
pub struct Configuration {
    pub programs: std::collections::BTreeMap<ProgramName, Program>,
    /// Program selections keyed by endpoint number; only endpoints naming a program are present.
    #[expect(
        clippy::zero_sized_map_values,
        reason = "Endpoint selections retain the adapter name; Claude Code is currently the sole adapter."
    )]
    pub endpoint_programs: std::collections::BTreeMap<u32, ProgramName>,
    pub service: service::Config,
    pub memory: u64,
    pub trace: Option<TraceConfig>,
    pub profile: service::profile::Profile,
    pub declarations: service::profile::Configuration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    #[serde(default)]
    programs: std::collections::BTreeMap<String, programs::ProgramDocument>,
    profile: ProfileDocument,
    endpoints: Vec<Endpoint>,
    environment: Vec<Variable>,
    #[serde(default)]
    trace: Option<TraceDocument>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileDocument {
    name: String,
    #[serde(default)]
    declared: DeclaredOverrides,
    #[serde(default)]
    policy: PolicyOverrides,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DeclaredOverrides {
    conversations: Option<u32>,
    tool_payload: Option<u32>,
    file_bytes: Option<u32>,
    calls_per_response: Option<u32>,
    jobs: Option<u32>,
    read_window: Option<u32>,
    shell_output: Option<u32>,
    shell_head: Option<u32>,
    shell_tail: Option<u32>,
    search_hits: Option<u32>,
    search_bytes: Option<u32>,
    list_entries: Option<u32>,
    guide: Option<u32>,
    llm_pool: Option<u64>,
    memory: Option<u64>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyOverrides {
    max_turns: Option<u32>,
    max_spend: Option<u64>,
    max_time: Option<u64>,
    max_waiting: Option<u64>,
    sections: Option<u32>,
    host_tools: Option<u32>,
    verdicts: Option<u32>,
    items: Option<u32>,
    fields: Option<u32>,
    inbox: Option<u32>,
    unacknowledged: Option<u32>,
    compaction_threshold: Option<FractionDocument>,
    parallel_tools: Option<u32>,
    connection_keep: Option<u64>,
    tool_deadline: Option<u64>,
    shell_timeout: Option<u64>,
    job_poll: Option<u64>,
    job_wait: Option<u64>,
    check_timeout: Option<u64>,
    wrap_up: Option<u64>,
    group_stop: Option<u64>,
    close: Option<u64>,
    write_deadline: Option<u64>,
    cancel_grace: Option<u64>,
    exit_grace: Option<u64>,
    refresh_lead: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FractionDocument {
    numerator: u32,
    denominator: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelDocument {
    prices: PriceDocument,
    name: String,
    window: u32,
    output: u32,
    reasoning_item: u32,
    #[serde(default)]
    oversized_reasoning: Option<String>,
    head: u64,
    idle: u64,
    #[serde(default)]
    efforts: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PriceDocument {
    input: u64,
    cached: u64,
    #[serde(default)]
    cache_write: Option<u64>,
    output: u64,
    unit: u32,
}

impl PriceDocument {
    fn rates(&self) -> run::Prices {
        run::Prices {
            input: self.input,
            cached: self.cached,
            cache_write: self.cache_write,
            output: self.output,
            unit: self.unit,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TraceDocument {
    path: String,
    #[serde(default = "default_capture")]
    capture: String,
    #[serde(default = "default_delivery")]
    delivery: String,
}

fn default_capture() -> String {
    "none".into()
}

fn default_delivery() -> String {
    "complete".into()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Endpoint {
    #[serde(default)]
    program: Option<String>,
    connect: u64,
    handshake: u64,
    models: Vec<ModelDocument>,
    name: String,
    number: u32,
    dialect: u32,
    /// Withdrawn endpoint account, ignored; accounts-sources removes it.
    #[serde(default, rename = "account")]
    _account: u32,
    /// Provider route: openai, anthropic, or the withdrawn codex route.
    provider: String,
    address: String,
    #[serde(default = "tls_transport")]
    transport: String,
    #[serde(default)]
    server_name: Option<String>,
    #[serde(default)]
    authority: Option<String>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    trust_der: Option<String>,
    #[serde(default)]
    #[cfg(unix)]
    trust: Option<TrustDocument>,
    #[serde(default)]
    #[cfg(not(unix))]
    trust: Option<serde_json::Value>,
    #[serde(default)]
    headers: Vec<HeaderField>,
    #[serde(default)]
    identity: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HeaderField {
    name: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg(unix)]
struct TrustDocument {
    #[serde(default = "system_trust")]
    system: bool,
    #[serde(default)]
    der: Vec<String>,
    #[serde(default)]
    pem: Vec<String>,
}

#[cfg(unix)]
const fn system_trust() -> bool {
    true
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Variable {
    name: String,
    value: String,
}

/// Read one JSON document and reject unknown keys, oversized input and
/// invalid transport settings before any channel byte is read.
#[cfg(unix)]
pub fn read(path: &Path) -> Result<Configuration, String> {
    let bytes = skein_shell::read_file(path, u32::try_from(CONFIG_BYTES).expect("configuration bound fits u32"))
        .map_err(|why| match why {
            skein_shell::Unread::Absent => format!("configuration metadata: {}: file absent", path.display()),
            skein_shell::Unread::NotAFile => format!("configuration {}: not a regular file", path.display()),
            skein_shell::Unread::TooLarge { max } => {
                format!("configuration {} is larger than {max} bytes", path.display())
            }
            skein_shell::Unread::Unsafe(reason) => format!("configuration read {}: {reason:?}", path.display()),
            skein_shell::Unread::Kernel(error) => format!("configuration read {}: {error:?}", path.display()),
        })?;
    parse(&bytes)
}

/// Validate generated child configuration before the local host spawns it.
pub fn parse(bytes: &[u8]) -> Result<Configuration, String> {
    if u64::try_from(bytes.len()).expect("byte length fits u64") > CONFIG_BYTES {
        return Err(format!("configuration is larger than {CONFIG_BYTES} bytes"));
    }
    let document: Document = serde_json::from_slice(bytes).map_err(|error| format!("configuration JSON: {error}"))?;
    build(document)
}

// The exact encoded count supplies machine.env_bytes before the shared encoder
// validates names. Contract: protocol/limits.md, sections 3.5 and 4.
fn environment_size_refusal() -> ConfigurationRefusal {
    ConfigurationRefusal::Profile(Refusal::Derived { limit: LimitName::deployment(Layer::Machine, "env_bytes") })
}

fn environment_refusal(error: machine::Unencodable) -> ConfigurationRefusal {
    match error {
        machine::Unencodable::Variable { index } | machine::Unencodable::Repeated { index } => {
            ConfigurationRefusal::Profile(Refusal::Environment { index })
        }
        // This configuration's exact count derives the receiving bound; a
        // smaller bound would mean the derived field cannot hold that count.
        machine::Unencodable::TooLarge { .. } => environment_size_refusal(),
    }
}

fn event_limits(
    limits: &service::Limits,
    _endpoints: &llm::Endpoints,
    _declarations: &service::profile::Configuration,
    capture: Capture,
    header: &[smith_events::EffectiveLimit],
    write_deadline: Duration,
) -> Result<service::EventLimits, ConfigurationRefusal> {
    let events = service::EventLimits::derive(
        limits,
        capture,
        1,
        smith_charter::CEILINGS.llm_effort_some,
        header,
        write_deadline,
    )
    .ok_or("event ownership overflow")?;
    Ok(events)
}

fn effective_header(
    profile: &service::profile::Profile,
    declarations: &service::profile::Configuration,
    limits: &service::Limits,
    names: &channel::Endpoints,
    endpoints: &llm::Endpoints,
) -> Result<List<smith_events::EffectiveLimit>, ConfigurationRefusal> {
    let inventory =
        service::profile::effective(profile, declarations, limits, endpoints).ok_or("effective limits overflow")?;
    let header = service::profile::header(&inventory, names, declarations).ok_or("effective names overflow")?;
    Ok(header)
}

fn build(document: Document) -> Result<Configuration, String> {
    build_parts(document).map_err(ConfigurationRefusal::detail)
}

/// Build the agent from generated settings values, without a JSON file or byte round trip.
pub fn from_value(value: serde_json::Value) -> Result<Configuration, ConfigurationRefusal> {
    let document = serde_json::from_value(value)
        .map_err(|why| ConfigurationRefusal::Input(format!("configuration JSON: {why}")))?;
    build_parts(document)
}

/// Resolve profile overrides before destinations or credentials are consulted.
pub fn profile_from_value(value: serde_json::Value) -> Result<service::profile::Profile, ConfigurationRefusal> {
    let document =
        serde_json::from_value(value).map_err(|why| ConfigurationRefusal::Input(format!("profile JSON: {why}")))?;
    build_profile(&document)
}

fn build_parts(document: Document) -> Result<Configuration, ConfigurationRefusal> {
    let profile = build_profile(&document.profile)?;
    let memory = profile.declared.memory;
    let programs = programs::build(document.programs)?;
    let endpoint_programs = programs::endpoints(&document.endpoints, &programs)?;
    if document.endpoints.is_empty() {
        return Err(ConfigurationRefusal::Profile(Refusal::Field {
            limit: LimitName::deployment(Layer::Declared, "endpoints"),
        }));
    }
    if document.endpoints.len() > usize::try_from(ENDPOINTS).expect("endpoint count fits usize") {
        return Err("too many endpoints".into());
    }
    let trace = trace::build(document.trace)?;
    let mut environment = Vec::with_capacity(document.environment.len());
    let mut environment_bytes = 0_u64;
    for variable in document.environment {
        let entry_bytes = variable
            .name
            .len()
            .checked_add(variable.value.len())
            .and_then(|bytes| bytes.checked_add(1))
            .ok_or_else(environment_size_refusal)?;
        environment_bytes = environment_bytes
            .checked_add(u64::try_from(entry_bytes).map_err(|_| environment_size_refusal())?)
            .ok_or_else(environment_size_refusal)?;
        environment.push(machine::Variable {
            name: variable.name.into_bytes().into(),
            value: variable.value.into_bytes().into(),
        });
    }
    let mut declarations = Vec::with_capacity(document.endpoints.len());
    for endpoint in &document.endpoints {
        declarations.push(build_models(endpoint)?);
    }
    let declarations = service::profile::Configuration {
        role_bound: None,
        environment_bytes: u32::try_from(environment_bytes).map_err(|_| environment_size_refusal())?,
        endpoints: declarations.into_boxed_slice(),
    };
    let mut limits = service::profile::derive(&profile, &declarations).map_err(ConfigurationRefusal::Profile)?;
    let domain_programs = programs::domain(&endpoint_programs, &mut limits.domain)?;
    let environment = machine::Environment::encode(&environment, &limits.machine).map_err(environment_refusal)?;
    let prices: Vec<_> = document
        .endpoints
        .iter()
        .flat_map(|endpoint| endpoint.models.iter().map(|model| model.prices.rates()))
        .collect();
    let endpoints = build_endpoints(document.endpoints, &limits.llm, &profile)?;
    let llm_endpoints =
        llm::Endpoints::new(endpoints.llm, ENDPOINTS).map_err(|error| format!("LLM endpoints: {error:?}"))?;
    if let Some(trace) = &trace {
        // First admit the sink, then include its own limits in the receiving header.
        for _ in 0..2 {
            let header = effective_header(&profile, &declarations, &limits, &endpoints.channel, &llm_endpoints)?;
            limits.events = Some(event_limits(
                &limits,
                &llm_endpoints,
                &declarations,
                trace.capture,
                header.as_slice(),
                profile.policy.write_deadline,
            )?);
        }
    }
    let mut models = Vec::new();
    for endpoint in &declarations.endpoints {
        let protocol_allowance = service::profile::endpoint_allowance(&profile, endpoint, &limits.domain.session.tools)
            .map_err(ConfigurationRefusal::Profile)?;
        for model in &endpoint.models {
            models.push(domain::ConfiguredModel {
                prices: *prices.get(models.len()).expect("one resolved price per declared model"),
                endpoint: run::charter::Endpoint(endpoint.number),
                model: model.name.clone(),
                efforts: model.efforts.clone(),
                window: model.window,
                output: model.output,
                protocol_allowance,
                head: model.head,
                idle: model.idle,
            });
        }
    }
    let config = service::Config {
        program: programs::backend(&programs, &endpoint_programs),
        limits,
        domain: domain::Config {
            programs: domain_programs,
            endpoints: endpoints.domain,
            models: models.into_boxed_slice(),
        },
        channel_endpoints: endpoints.channel,
        llm_endpoints,
        environment,
        stream_mode: StreamMode::Two,
    };
    check_memory(&config, memory)?;
    Ok(Configuration { service: config, memory, trace, profile, declarations, programs, endpoint_programs })
}

fn check_memory(config: &service::Config, memory: u64) -> Result<(), ConfigurationRefusal> {
    let complete = service::worst_case(&config.limits, &config.llm_endpoints)
        .and_then(|bytes| bytes.checked_add(service::program_worst_case(config)?))
        .ok_or_else(|| {
            ConfigurationRefusal::Profile(Refusal::Derived {
                limit: LimitName::deployment(Layer::Service, "worst_case"),
            })
        })?;
    service::profile::validate::memory(complete, memory).map_err(ConfigurationRefusal::Profile)
}

struct PreparedEndpoints {
    domain: Box<[run::charter::Endpoint]>,
    channel: channel::Endpoints,
    llm: Box<[llm::ConfiguredEndpoint]>,
}

fn build_endpoints(
    endpoints: Vec<Endpoint>,
    limits: &llm::ComponentLimits,
    profile: &service::profile::Profile,
) -> Result<PreparedEndpoints, ConfigurationRefusal> {
    let mut domain_endpoints = Vec::with_capacity(endpoints.len());
    let mut channel_endpoints = List::with_capacity(ENDPOINTS);
    let mut llm_endpoints = Vec::with_capacity(endpoints.len());
    let mut names = std::collections::BTreeSet::new();
    let mut numbers = std::collections::BTreeSet::new();
    for endpoint in endpoints {
        let declared = build_models(&endpoint)?;
        let mut client = service::profile::endpoint_client(profile, &declared, limits.adapter.client.history_items)
            .map_err(ConfigurationRefusal::Profile)?;
        if endpoint.name.is_empty()
            || endpoint.name.len()
                > usize::try_from(smith_charter::CEILINGS.llm_endpoint).expect("endpoint name bound fits usize")
        {
            return Err("endpoint name is empty or too long".into());
        }
        if !names.insert(endpoint.name.clone()) || !numbers.insert(endpoint.number) {
            return Err("duplicate endpoint name or number".into());
        }
        #[cfg(not(unix))]
        if endpoint.transport == "tls" {
            return Err(format!("endpoint {}: TLS requires a native shell", endpoint.name).into());
        }
        let address = resolve(&endpoint.address)?;
        let transport = endpoint_transport(&endpoint, address)?;
        let mut llm_endpoint = match endpoint.provider.as_str() {
            "openai" => shared::Endpoint::openai(),
            "codex" => shared::Endpoint::codex(),
            "anthropic" => shared::Endpoint::anthropic(),
            _ => return Err("endpoint provider must be openai, codex or anthropic".into()),
        };
        if let Some(authority) = endpoint.authority {
            if authority.is_empty() || authority.contains(['\r', '\n']) {
                return Err("invalid endpoint HTTP authority".into());
            }
            llm_endpoint.authority = authority.into_bytes().into();
        }
        if let Some(target) = endpoint.target {
            if !target.starts_with('/') || target.contains(['\r', '\n']) {
                return Err("invalid endpoint HTTP target".into());
            }
            llm_endpoint.target = target.into_bytes().into();
        }
        let mut headers = Vec::with_capacity(endpoint.headers.len());
        for header in endpoint.headers {
            if header.name.eq_ignore_ascii_case("session-id")
                || header.name.eq_ignore_ascii_case("thread-id")
                || header.name.is_empty()
                || header.name.contains(['\r', '\n', ':'])
                || header.value.contains(['\r', '\n'])
            {
                return Err("invalid endpoint HTTP header".into());
            }
            headers.push(Header { name: header.name.into_bytes().into(), value: header.value.into_bytes().into() });
        }
        llm_endpoint.headers = headers.into_boxed_slice();
        let identity = endpoint_identity(endpoint.identity.as_deref())?;
        domain_endpoints.push(run::charter::Endpoint(endpoint.number));
        channel_endpoints
            .push(channel::Endpoint {
                name: endpoint.name.into_bytes().into(),
                number: endpoint.number,
                dialect: endpoint.dialect,
            })
            .map_err(|_| "too many channel endpoints")?;
        let credential = credential_limits(limits);
        let head = shared::client::request_head(&llm_endpoint, &credential, &client).map_err(|error| {
            ConfigurationRefusal::Profile(Refusal::Connection(connection::EndpointError::Client(error)))
        })?;
        service::profile::validate::request_head(endpoint.number, head, tls::client::MAX_PLAINTEXT)
            .map_err(ConfigurationRefusal::Profile)?;
        client.http.request = head;
        let mut destination =
            connection::Endpoint { address, transport, llm: llm_endpoint, limits: client, credential };
        destination.pieces(&limits.connection);
        llm_endpoints.push(llm::ConfiguredEndpoint {
            name: domain::llm::Endpoint(endpoint.number),
            destination,
            connect: endpoint_duration(endpoint.connect, endpoint.number, "connect")?,
            handshake: endpoint_duration(endpoint.handshake, endpoint.number, "handshake")?,
            identity,
            models: endpoint_models(&endpoint.models)?,
        });
    }
    Ok(PreparedEndpoints {
        domain: domain_endpoints.into_boxed_slice(),
        channel: channel::Endpoints::new(channel_endpoints),
        llm: llm_endpoints.into_boxed_slice(),
    })
}

fn endpoint_transport(endpoint: &Endpoint, address: SocketAddr) -> Result<connection::Transport, ConfigurationRefusal> {
    match endpoint.transport.as_str() {
        #[cfg(not(unix))]
        "tls" => Err(format!("endpoint {}: TLS requires a native shell", endpoint.name).into()),
        #[cfg(unix)]
        "tls" => Ok(connection::Transport::Tls {
            server_name: tls::Name::new(endpoint.server_name.as_deref().ok_or("TLS endpoint requires server_name")?)
                .ok_or("invalid TLS server name")?,
            trust: endpoint_trust(endpoint)?,
        }),
        "plaintext" => {
            if !address.ip().is_loopback() {
                return Err("plaintext endpoint address must be loopback".into());
            }
            if endpoint.server_name.is_some() || endpoint.trust_der.is_some() || endpoint.trust.is_some() {
                return Err("plaintext endpoint cannot name TLS server_name or trust_der".into());
            }
            Ok(connection::Transport::Plaintext)
        }
        _ => Err("endpoint transport must be tls or plaintext".into()),
    }
}

fn endpoint_models(models: &[ModelDocument]) -> Result<Box<[llm::ConfiguredModel]>, ConfigurationRefusal> {
    let mut configured = Vec::with_capacity(models.len());
    for model in models {
        configured.push(llm::ConfiguredModel {
            name: model.name.as_bytes().into(),
            oversized_reasoning: reasoning_policy(model.oversized_reasoning.as_deref())?,
        });
    }
    Ok(configured.into_boxed_slice())
}

fn duration(millis: u64, name: LimitName) -> Result<Duration, ConfigurationRefusal> {
    let nanos = millis.checked_mul(1_000_000).ok_or(ConfigurationRefusal::Profile(Refusal::Derived { limit: name }))?;
    Ok(Duration::from_nanos(nanos))
}

fn endpoint_duration(millis: u64, endpoint: u32, field: &'static str) -> Result<Duration, ConfigurationRefusal> {
    duration(millis, LimitName { layer: Layer::Policy, field, per: Per::Endpoint(endpoint) })
}

fn endpoint_identity(value: Option<&str>) -> Result<llm::IdentityProfile, ConfigurationRefusal> {
    match value {
        None | Some("plain") => Ok(llm::IdentityProfile::Plain),
        Some("claude-code") => Ok(llm::IdentityProfile::ClaudeCode),
        Some(_) => Err("endpoint identity must be plain or claude-code".into()),
    }
}

fn reasoning_policy(value: Option<&str>) -> Result<llm::OversizedReasoning, ConfigurationRefusal> {
    match value {
        None | Some("fail") => Ok(llm::OversizedReasoning::Fail),
        Some("drop") => Ok(llm::OversizedReasoning::Drop),
        Some(_) => Err("model oversized_reasoning must be fail or drop".into()),
    }
}

fn build_models(endpoint: &Endpoint) -> Result<service::profile::Endpoint, ConfigurationRefusal> {
    let number = endpoint.number;
    let models = &endpoint.models;
    let (provider, dialect) = match endpoint.provider.as_str() {
        "openai" => (shared::Provider::OpenAi, 3),
        "codex" => (shared::Provider::OpenAiCodex, 1),
        "anthropic" => (shared::Provider::Anthropic, 2),
        _ => return Err("endpoint provider must be openai, codex or anthropic".into()),
    };
    if endpoint.dialect != dialect {
        return Err("endpoint dialect does not match provider".into());
    }
    let identity = endpoint_identity(endpoint.identity.as_deref())?;
    let option_bytes = llm::endpoint_own(identity).map_err(|_| {
        ConfigurationRefusal::Profile(Refusal::Derived {
            limit: LimitName { layer: Layer::Service, field: "endpoint_own", per: Per::Endpoint(number) },
        })
    })?;
    if models.is_empty() {
        return Err(ConfigurationRefusal::Profile(Refusal::Field {
            limit: LimitName { layer: Layer::Declared, field: "models", per: Per::Endpoint(number) },
        }));
    }
    let mut names = std::collections::BTreeSet::new();
    let mut prepared = Vec::with_capacity(models.len());
    for (index, model) in models.iter().enumerate() {
        if model.efforts.iter().any(|name| {
            name.is_empty()
                || name.len()
                    > usize::try_from(smith_charter::CEILINGS.llm_effort_some).expect("effort bound fits usize")
        }) {
            return Err("model efforts must be names within 32 bytes".into());
        }
        let index = u32::try_from(index).map_err(|_| {
            ConfigurationRefusal::Profile(Refusal::Derived {
                limit: LimitName::deployment(Layer::Service, "configured_models"),
            })
        })?;
        let per = Per::Model { endpoint: number, model: index };
        if model.name.is_empty()
            || model.name.len()
                > usize::try_from(smith_charter::CEILINGS.llm_model).expect("model name bound fits usize")
            || model.window == 0
            || model.output == 0
            || model.reasoning_item == 0
        {
            return Err("model declaration contains an empty or zero quantity".into());
        }
        if !names.insert(model.name.clone()) {
            return Err("duplicate declared model".into());
        }
        model.prices.rates().check().map_err(|_| "model prices must be positive")?;
        prepared.push(service::profile::Model {
            name: model.name.as_bytes().into(),
            efforts: model.efforts.iter().map(|name| name.as_bytes().into()).collect(),
            window: model.window,
            output: model.output,
            reasoning_item: model.reasoning_item,
            oversized_reasoning: reasoning_policy(model.oversized_reasoning.as_deref())?,
            head: duration(model.head, LimitName { layer: Layer::Declared, field: "head", per })?,
            idle: duration(model.idle, LimitName { layer: Layer::Declared, field: "idle", per })?,
        });
    }
    Ok(service::profile::Endpoint {
        number,
        provider,
        option_bytes,
        connect: endpoint_duration(endpoint.connect, number, "connect")?,
        handshake: endpoint_duration(endpoint.handshake, number, "handshake")?,
        models: prepared.into_boxed_slice(),
    })
}

fn credential_limits(limits: &llm::ComponentLimits) -> shared::client::CredentialLimits {
    shared::client::CredentialLimits {
        value: limits.grant_value_bytes,
        account_id: limits.grant_value_bytes.min(smith_channel::CEILINGS.withdrawn_value_id),
    }
}

fn tls_transport() -> String {
    "tls".into()
}

#[cfg(unix)]
fn resolve(address: &str) -> Result<SocketAddr, ConfigurationRefusal> {
    let mut resolved = address.to_socket_addrs().map_err(|error| format!("endpoint address {address:?}: {error}"))?;
    resolved.next().ok_or_else(|| ConfigurationRefusal::Input(format!("endpoint address {address:?} has no result")))
}

#[cfg(not(unix))]
fn resolve(address: &str) -> Result<SocketAddr, ConfigurationRefusal> {
    address.parse().map_err(|error| format!("endpoint address {address:?}: {error}; no name resolution").into())
}

// Startup trust material and decoded configuration are bounded together.
/// Bound on startup trust material and decoded configuration, in bytes.
pub const TRUST_BYTES: u64 = 16_777_216;

/// Load the bounded startup trust roots used by agent and local endpoints.
#[cfg(unix)]
pub fn trust(der: Option<&str>) -> Result<tls::Config, String> {
    let source = match der {
        Some(path) => skein_shell::Trust::Der(path.into()),
        None => skein_shell::Trust::Machine { ssl_cert_file: std::env::var_os("SSL_CERT_FILE").map(Into::into) },
    };
    trust_source(&source)
}

/// Load account trust arrays through the existing bounded root owner.
#[cfg(unix)]
pub fn configured_trust(sources: &[skein_shell::Trust]) -> Result<tls::Config, String> {
    trust_sources(sources).map_err(ConfigurationRefusal::detail)
}

#[cfg(unix)]
fn trust_source(source: &skein_shell::Trust) -> Result<tls::Config, String> {
    trust_sources(std::slice::from_ref(source)).map_err(ConfigurationRefusal::detail)
}

#[cfg(unix)]
fn endpoint_trust(endpoint: &Endpoint) -> Result<tls::Config, ConfigurationRefusal> {
    let Some(document) = &endpoint.trust else {
        return trust(endpoint.trust_der.as_deref()).map_err(ConfigurationRefusal::Input);
    };
    if endpoint.trust_der.is_some() {
        return Err("endpoint cannot name both trust_der and trust".into());
    }
    let mut sources = Vec::new();
    if document.system {
        sources.push(skein_shell::Trust::Machine { ssl_cert_file: std::env::var_os("SSL_CERT_FILE").map(Into::into) });
    }
    sources.extend(document.der.iter().map(|path| skein_shell::Trust::Der(path.into())));
    sources.extend(document.pem.iter().map(|path| skein_shell::Trust::Machine { ssl_cert_file: Some(path.into()) }));
    trust_sources(&sources)
}

#[cfg(unix)]
fn trust_sources(sources: &[skein_shell::Trust]) -> Result<tls::Config, ConfigurationRefusal> {
    let mut certificates = Vec::new();
    let mut bytes = 0_u64;
    let mut malformed = 0_u32;
    let mut other_blocks = 0_u32;
    let mut paths = Vec::new();
    for source in sources {
        let bounds = skein_shell::RootBounds {
            bundle_bytes: u32::try_from(TRUST_BYTES).expect("trust bound fits u32"),
            certificates: 512,
            certificate_bytes: match source {
                skein_shell::Trust::Der(_) => 65_536,
                skein_shell::Trust::Machine { .. } => 8192,
            },
        };
        let roots = skein_shell::read_trust_roots(source, &bounds).map_err(|why| {
            ConfigurationRefusal::Input(match why {
                skein_shell::RootsRefusal::NoBundle => "machine trust roots: no certificate bundle found".into(),
                skein_shell::RootsRefusal::Unread { path, why } => {
                    format!("trust certificate {}: {why:?}", path.display())
                }
                skein_shell::RootsRefusal::TooMany { bound } => format!("trust roots exceed {bound} certificates"),
                skein_shell::RootsRefusal::TooLong { index, bound } => {
                    format!("trust certificate {index} exceeds {bound} bytes")
                }
            })
        })?;
        for certificate in roots.certificates {
            bytes = bytes
                .checked_add(u64::try_from(certificate.len()).expect("bounded certificate"))
                .ok_or("trust roots exceed their byte bound")?;
            if bytes > TRUST_BYTES || certificates.len() >= 512 {
                return Err("trust roots exceed their combined bound".into());
            }
            certificates.push(certificate);
        }
        malformed = malformed.checked_add(roots.malformed).ok_or("trust block count overflow")?;
        other_blocks = other_blocks.checked_add(roots.other_blocks).ok_or("trust block count overflow")?;
        paths.push(roots.path.display().to_string());
    }
    let (config, parsed) =
        tls::Config::from_der(&certificates, &[]).map_err(|why| format!("TLS trust {}: {why:?}", paths.join(", ")))?;
    eprintln!(
        "smith: trust roots: {} taken, {} skipped, {} malformed blocks, {} other blocks",
        parsed.taken, parsed.skipped, malformed, other_blocks
    );
    Ok(config)
}

pub(crate) fn profile_refusal(error: Refusal) -> String {
    String::from_utf8(service::profile::words(&error).into_vec()).expect("refusal words are UTF-8")
}

#[cfg(test)]
mod tests;
