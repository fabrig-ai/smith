//! Startup's check document with value origins; credential values are redacted
//! (shell.md, sections 4.4, 5.3 and 7). It starts no run or sign-in.
use crate::{
    command::{self, Command},
    settings::{self, Origin, Problem, Refusal, Settings},
};
use serde::Serialize;
use std::{collections::BTreeMap, ffi::OsString, os::unix::fs::PermissionsExt, path::PathBuf};

mod accounts;
#[cfg(test)]
mod program;
pub(crate) mod warnings;

/// A startup warning names its configuration source, never a variable's value.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Warning {
    /// The kernel could not confirm one named cache's directory access.
    CacheAccess { cache: PathBuf, reason: String },
    /// A configured Rust wrapper lacks a listed, writable cache.
    WrapperCache { configured_by: String, origin: Origin, cache: Option<PathBuf> },
    /// Commands would receive a variable whose name suggests a credential.
    CredentialLikeName { name: String, origin: Origin },
    /// An account outside the selected run has an unusable credential source.
    Credential { account: String, credential: crate::credentials::Credential },
}

/// The version of check's JSON document, read by callers and the harness.
pub const CHECK_VERSION: u64 = 2;

/// One startup check's document; only names and sources describe environment.
#[derive(Debug, Serialize)]
pub struct Report {
    pub version: u64,
    pub settings: Settings,
    pub formats: BTreeMap<&'static str, u64>,
    pub agent: serde_json::Value,
    pub tools: BTreeMap<&'static str, PathBuf>,
    pub programs: BTreeMap<&'static str, PathBuf>,
    pub environment: Vec<String>,
    #[serde(skip)]
    pub limits: Vec<smith_agent_service::profile::Limit>,
    #[serde(rename = "limits")]
    named_limits: Vec<serde_json::Value>,
    pub caches: Vec<PathBuf>,
    pub warnings: Vec<Warning>,
    pub outside_models: BTreeMap<String, String>,
    pub credentials: BTreeMap<String, crate::credentials::AccountReport>,
}

impl Report {
    /// Only the resolver's needed accounts can make the check exit for credentials.
    #[must_use]
    pub fn credentials_ready(&self) -> bool {
        self.credentials.values().all(|account| !account.required || account.credential.usable())
    }
}

/// Startup either supplies the document or names its configuration problem.
#[derive(Debug)]
pub enum Verdict {
    /// The decoded settings and tools pass this stage's checks.
    Ready(Box<Report>),
    /// A setting or tool prevents startup, rendered with exit 2.
    Refused(Refusal),
}

/// Build the document with access values omitted from every reported state.
#[must_use]
pub fn report(settings: &Settings, wall: skein_lib::Wall, clock: &skein_shell::Clock) -> Verdict {
    report_for(settings, wall, clock, None)
}

fn report_for(
    settings: &Settings,
    wall: skein_lib::Wall,
    clock: &skein_shell::Clock,
    execution: Option<&command::Exec>,
) -> Verdict {
    let command = match execution {
        Some(execution) => Command::Exec(execution.clone()),
        None => Command::Check { shared: command::Shared::default(), json: false, execution: None },
    };
    let mut resolved = match settings::resolve::resolve(settings, &command) {
        Ok(resolved) => resolved,
        Err(why) => return Verdict::Refused(why),
    };
    let inventory = match startup_agent(&mut resolved).and_then(|()| invocation_limits(&mut resolved, execution, wall))
    {
        Ok(inventory) => inventory,
        Err(reason) => {
            return Verdict::Refused(Refusal {
                origin: Origin::BuiltIn,
                key: "limits".into(),
                problem: Problem::Input(reason),
            });
        }
    };
    let settings = &resolved.settings;
    let agent = &resolved.agent;
    let Some(limits) = inventory else {
        return effective_refusal();
    };
    let Some(named_limits) = named_limits(&limits, agent) else {
        return effective_refusal();
    };
    let values = match settings::environment::values(settings) {
        Ok(values) => values,
        Err(why) => return Verdict::Refused(why),
    };
    let path = values.get("PATH").map(OsString::from);
    let mut tools = BTreeMap::new();
    for name in ["sh", "rg", "git"] {
        let resolved = path.as_ref().and_then(|path| {
            std::env::split_paths(path).map(|directory| settings.workspace.join(directory).join(name)).find(|file| {
                std::fs::metadata(file)
                    .is_ok_and(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
            })
        });
        let Some(resolved) = resolved else {
            return Verdict::Refused(Refusal {
                origin: Origin::BuiltIn,
                key: format!("tools.{name}"),
                problem: Problem::MissingTool,
            });
        };
        tools.insert(name, resolved);
    }
    let environment = values.keys().cloned().collect();
    let caches = settings::environment::caches(settings, &values);
    let mut warnings = match warnings::collect(settings, &values, &caches) {
        Ok(warnings) => warnings,
        Err(why) => return Verdict::Refused(why),
    };
    let credentials = accounts::report(&resolved, wall, clock);
    let programs = resolved
        .agent
        .programs
        .iter()
        .map(|(name, program)| (crate::credentials::program_check::key(*name), program.path.clone()))
        .collect();
    for (account, state) in &credentials {
        if !state.required && !state.credential.usable() {
            warnings.push(Warning::Credential { account: account.clone(), credential: state.credential.clone() });
        }
    }
    let mut redacted = settings.clone();
    for (key, value) in &mut redacted.values {
        if key == "credentials"
            || key.starts_with("environment.set.")
            || key.starts_with("endpoints.") && key.contains(".headers.")
        {
            value.value = toml::Value::String("<redacted>".into());
        }
    }
    let identity = smith_agent_shell::identity();
    let text = |bytes: &[u8]| String::from_utf8_lossy(bytes).into_owned();
    Verdict::Ready(Box::new(Report {
        version: CHECK_VERSION,
        settings: redacted,
        formats: BTreeMap::from([
            ("channel", smith_channel::KindRule::FORMAT_VERSION),
            ("charter", smith_charter::Charter::FORMAT_VERSION),
            ("transcript", smith_transcript::Turn::FORMAT_VERSION),
            ("events", smith_events::VERSION),
            ("chat_store", smith_local_protocol::Store::FORMAT_VERSION),
            ("check", CHECK_VERSION),
        ]),
        agent: serde_json::json!({"name":text(&identity.name),"version":text(&identity.version)}),
        tools,
        programs,
        environment,
        limits: limits.into_vec(),
        named_limits,
        caches,
        warnings,
        credentials,
        outside_models: resolved.outside_models.into_iter().map(|(model, reason)| (model, reason.into())).collect(),
    }))
}

/// Check the same caches and environment names before composing a product run.
pub fn startup_warnings(settings: &Settings) -> Result<Vec<Warning>, Refusal> {
    let values = settings::environment::values(settings)?;
    let caches = settings::environment::caches(settings, &values);
    warnings::collect(settings, &values, &caches)
}

fn effective_refusal() -> Verdict {
    Verdict::Refused(Refusal {
        origin: Origin::BuiltIn,
        key: "limits".into(),
        problem: Problem::Input("effective limits or names exceed their bounds".into()),
    })
}

fn unit(value: smith_agent_service::profile::Value) -> &'static str {
    use smith_agent_service::profile::Value;
    match value {
        Value::Bytes(_) => "bytes",
        Value::Count(_) => "count",
        Value::Tokens(_) => "tokens",
        Value::Spend(_) => "micro-dollars",
        Value::Duration(_) => "ms",
        Value::Fraction(_) => "fraction",
    }
}

/// Render the same document for scripts as one JSON object.
#[must_use]
pub fn render_json(report: &Report) -> String {
    serde_json::to_string(report).expect("check document contains JSON values")
}

/// Render settings and their layers with environment values omitted.
#[must_use]
pub fn render_text(report: &Report) -> String {
    let mut lines = Vec::new();
    for (key, value) in &report.settings.values {
        let source = value.origin.description();
        lines.push(format!("{key} = {} (from {source})", value.value));
    }
    lines.push(format!("agent {}", report.agent));
    lines.push(format!("formats {}", serde_json::to_string(&report.formats).expect("format names")));
    for (name, path) in &report.tools {
        lines.push(format!("{name} = {}", path.display()));
    }
    for (name, path) in &report.programs {
        lines.push(format!("program {name} = {}", path.display()));
    }
    lines.push(format!("environment: {}", report.environment.join(", ")));
    for warning in &report.warnings {
        lines.push(warnings::line(warning));
    }
    for limit in &report.named_limits {
        lines.push(format!(
            "{} = {} {}",
            limit.get("name").expect("limit name").as_str().expect("name text"),
            limit.get("value").expect("limit value"),
            limit.get("unit").expect("limit unit").as_str().expect("unit text")
        ));
    }
    for (model, reason) in &report.outside_models {
        lines.push(format!("model {model} is outside the run: {reason}"));
    }
    for (name, account) in &report.credentials {
        lines.push(account.line(name));
        if account.required || account.credential.usable() {
            lines.push(account.credential.line(name));
        }
    }
    lines.join("\n")
}

fn startup_agent(resolved: &mut settings::resolve::Resolved) -> Result<(), String> {
    if crate::compose::program_account(&resolved.accounts) {
        let host = crate::compose::host_limits(&resolved.agent, true)
            .map_err(|why| String::from_utf8_lossy(&smith_agent_service::profile::words(&why)).into_owned())?;
        resolved.limits.agent = smith_local_domain::AgentLimits::Spawned(host);
    }
    Ok(())
}

fn invocation_limits(
    resolved: &mut settings::resolve::Resolved,
    execution: Option<&command::Exec>,
    wall: skein_lib::Wall,
) -> Result<Option<Box<[smith_agent_service::profile::Limit]>>, String> {
    let local = match execution {
        Some(execution) => Some(derive_execution(resolved, execution, wall)?),
        None => Some(interactive_limits(resolved)?),
    };
    let agent = &resolved.agent;
    Ok(match &local {
        Some((local, result_file)) => smith_local_service::profile::effective(
            &agent.profile,
            &agent.declarations,
            &agent.service,
            local,
            *result_file,
        ),
        None => smith_agent_service::profile::effective(
            &agent.profile,
            &agent.declarations,
            &agent.service.limits,
            &agent.service.llm_endpoints,
        ),
    })
}

fn named_limits(
    limits: &[smith_agent_service::profile::Limit],
    agent: &smith_agent_shell::config::Configuration,
) -> Option<Vec<serde_json::Value>> {
    let header = smith_agent_service::profile::header(limits, &agent.service.channel_endpoints, &agent.declarations)?;
    Some(
        header
            .iter()
            .zip(limits.iter())
            .map(|(named, native)| {
                let value = match named.value {
                    smith_events::LimitValue::Integer(value) => serde_json::json!(value),
                    smith_events::LimitValue::Fraction { numerator, denominator } => {
                        serde_json::json!({"numerator":numerator,"denominator":denominator})
                    }
                };
                serde_json::json!({"name":String::from_utf8_lossy(&named.name),"value":value,"unit":unit(native.value)})
            })
            .collect(),
    )
}

fn derive_execution(
    resolved: &mut settings::resolve::Resolved,
    invocation: &command::Exec,
    wall: skein_lib::Wall,
) -> Result<(smith_local_service::Limits, skein_lib::Duration), String> {
    derive_invocation(resolved, invocation, wall, false)
}

/// Derive and admit the interactive invocation's limits without opening resources.
pub fn interactive_limits(
    resolved: &mut settings::resolve::Resolved,
) -> Result<(smith_local_service::Limits, skein_lib::Duration), String> {
    let invocation = command::Exec {
        shared: command::Shared::default(),
        prompt: command::Prompt::Stdin,
        output: None,
        ephemeral: false,
        deliver: false,
        json: false,
    };
    derive_invocation(resolved, &invocation, skein_lib::Wall::from_nanos(0), true)
}

fn derive_invocation(
    resolved: &mut settings::resolve::Resolved,
    invocation: &command::Exec,
    wall: skein_lib::Wall,
    interactive: bool,
) -> Result<(smith_local_service::Limits, skein_lib::Duration), String> {
    if let Some(chat) = crate::compose::invocation_chat(&invocation.shared, 0) {
        resolved.local.chat = chat;
    }
    let files = crate::compose::Files::prepare(invocation, resolved, 0).map_err(|why| why.line())?;
    let result_file = files.teardown_bound().map_err(|why| why.line())?;
    let host = crate::compose::host_limits(
        &resolved.agent,
        matches!(resolved.limits.agent, smith_local_domain::AgentLimits::Spawned(_)),
    )
    .map_err(|why| String::from_utf8_lossy(&smith_agent_service::profile::words(&why)).into_owned())?;
    let mut limits = crate::compose::service_limits(resolved.limits.clone(), host, &resolved.agent.service)?;
    if interactive {
        interactive_header(resolved, &mut limits, result_file)?
    } else {
        crate::compose::prepare_stream(
            &mut resolved.agent,
            &resolved.local.chat,
            &mut limits,
            invocation.json,
            wall,
            result_file,
            false,
        )
        .map_err(|why| why.line())?;
    }
    match limits.local.agent {
        smith_local_domain::AgentLimits::Inline(_) => {
            limits.inline = Some(
                smith_local_service::InlineLimits::new(&resolved.agent.service)
                    .ok_or_else(|| "inline memory bound overflows".to_owned())?,
            );
        }
        smith_local_domain::AgentLimits::Spawned(_) => {}
    }
    let handed = handed_accounts(resolved)?;
    if interactive {
        crate::front::interactive::Interactive::new(false, &resolved.agent.service.limits)
            .ok_or_else(|| "interactive view bound overflows".to_owned())?;
    }
    let lower_routes = match limits.local.agent {
        smith_local_domain::AgentLimits::Inline(_) => resolved.agent.service.limits.routes,
        smith_local_domain::AgentLimits::Spawned(_) => 0,
    };
    crate::Local::startup_operations(&limits, lower_routes, Some(&handed), !interactive && files.has_output())
        .map_err(|why| why.detail().to_owned())?;
    Ok((limits, result_file))
}

fn handed_accounts(resolved: &settings::resolve::Resolved) -> Result<crate::credentials::Prepared, String> {
    let count = u32::try_from(resolved.accounts.len()).map_err(|_| "too many account sources".to_owned())?;
    let mut locations = skein_lib::List::with_capacity(count);
    let directory = crate::credentials::Directory::of(&resolved.settings);
    for account in &resolved.accounts {
        let source =
            crate::credentials::source(&account.fields, &resolved.settings, false, &account.name, directory.as_ref());
        locations
            .push(crate::credentials::Location { account: account.number, name: account.name.clone(), source })
            .expect("selected locations fit their account bound");
    }
    Ok(crate::credentials::Prepared {
        sources: skein_lib::List::with_capacity(count),
        lendable: skein_lib::Map::with_capacity(count),
        locations,
        limits: crate::credentials::limits(
            resolved.agent.service.limits.io,
            count.max(1),
            resolved.agent.profile.policy.refresh_lead,
        ),
    })
}

fn interactive_header(
    resolved: &mut settings::resolve::Resolved,
    limits: &mut smith_local_service::Limits,
    result_file: skein_lib::Duration,
) -> Result<(), String> {
    let destinations = u32::from(resolved.agent.trace.is_some());
    let trace = resolved.agent.trace.take().unwrap_or(smith_agent_shell::trace::TraceConfig {
        path: "stdout".into(),
        capture: smith_agent_service::sinks::Capture::None,
        delivery: smith_agent_service::sinks::Delivery::Complete,
    });
    crate::Local::prepare_header(
        &mut resolved.agent,
        limits,
        Some(&trace),
        &resolved.local.chat,
        0,
        crate::local_host::View { destinations, front: true },
        result_file,
    )
    .map(|_| ())
    .map_err(|why| why.detail().to_owned())
}

/// Handle check arguments with injected wall time and the program runner's clock.
#[must_use]
pub fn command(arguments: &[OsString], wall: skein_lib::Wall, clock: &skein_shell::Clock) -> Option<u8> {
    let (shared, json, execution) = match command::parse(arguments.iter().skip(1).cloned().collect()) {
        Ok(Command::Exec(_) | Command::Logout { .. } | Command::Login { .. } | Command::Interactive { .. }) => {
            return None;
        }
        Ok(Command::Check { shared, json, execution }) => (shared, json, execution),
        Err(usage) => {
            eprintln!("smith: usage: {}", usage.0);
            return Some(2);
        }
    };
    let verdict = match settings::read(&shared) {
        Ok(settings) => report_for(&settings, wall, clock, execution.as_ref()),
        Err(why) => Verdict::Refused(why),
    };
    match verdict {
        Verdict::Ready(report) => {
            println!("{}", if json { render_json(&report) } else { render_text(&report) });
            Some(if report.credentials_ready() { 0 } else { 3 })
        }
        Verdict::Refused(why) => {
            eprintln!("{}", why.line());
            Some(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interactive_checks_enable_the_live_front() {
        let mut resolved = execution_fixture();
        let (limits, _) = interactive_limits(&mut resolved).expect("interactive admission");
        let events = resolved.agent.service.limits.events.expect("interactive sink");
        assert!(events.front);
        assert!(smith_local_service::worst_case(&limits).is_some());
    }

    fn resolved(trace: bool) -> settings::resolve::Resolved {
        let certificate =
            skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
        let trace = if trace { "[trace]\npath='/unopened-events'\n" } else { "" };
        let text = format!(
            "model='codex/gpt-6-luna'\n[deployment]\nmemory=8589934592\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\ntrust.system=false\ntrust.der=['{certificate}']\n{trace}"
        );
        let settings = settings::merge(
            &command::Shared::default(),
            "/workspace".into(),
            &[settings::Source { layer: settings::Layer::User, path: "check-fixture.toml".into(), text }],
        )
        .unwrap_or_else(|why| panic!("{}", why.line()));
        settings::resolve::resolve(
            &settings,
            &Command::Check { shared: command::Shared::default(), json: true, execution: None },
        )
        .unwrap_or_else(|why| panic!("{}", why.line()))
    }

    #[test]
    fn check_execs_teardown_includes_only_its_result_file_deadlines() {
        let mut resolved = execution_fixture();
        for (output, milliseconds) in [(false, 10_000), (true, 12_000)] {
            let mut arguments = vec!["check", "--json", "exec"];
            if output {
                arguments.extend(["-o", "/unopened-output-parent/result"]);
            }
            let Command::Check { execution: Some(execution), .. } =
                command::parse(arguments.into_iter().map(OsString::from).collect()).expect("check exec arguments")
            else {
                panic!("invocation-aware check");
            };
            let inventory = invocation_limits(&mut resolved, Some(&execution), skein_lib::Wall::from_nanos(0))
                .expect("pure invocation derivation")
                .expect("effective inventory");
            let document = named_limits(&inventory, &resolved.agent).expect("check's named JSON limits");
            let teardown = document.iter().find(|limit| limit["name"] == "service.teardown").expect("teardown bound");
            assert_eq!(teardown["unit"], "ms");
            assert_eq!(teardown["value"], milliseconds);
        }
    }

    #[expect(
        clippy::zero_sized_map_values,
        reason = "The fixture retains adapter names even while Claude Code is the sole adapter."
    )]
    fn execution_fixture() -> settings::resolve::Resolved {
        let (mut local, service) =
            smith_local_process_world::process::configuration(skein_io::kernel::Fd::new(41), false);
        local.limits.local.agent = smith_local_domain::AgentLimits::inline(
            service.limits.domain,
            2,
            skein_lib::Duration::from_secs(3600),
            smith_agent_process_world::profile().policy.cancel_grace,
        )
        .expect("product inline fixture");
        settings::resolve::Resolved {
            agent: smith_agent_shell::config::Configuration {
                programs: BTreeMap::new(),
                endpoint_programs: BTreeMap::new(),
                service,
                memory: 8_589_934_592,
                trace: None,
                profile: smith_agent_process_world::profile(),
                declarations: smith_agent_process_world::declarations(0),
            },
            local: local.local,
            limits: local.limits.local,
            settings: Settings {
                paths: settings::Paths::default(),
                workspace: "/workspace".into(),
                values: BTreeMap::new(),
            },
            accounts: Vec::new(),
            outside_models: BTreeMap::new(),
        }
    }

    #[test]
    fn check_derives_execs_event_destinations_through_its_stream_owner() {
        for (trace, json, destinations) in [(false, false, 0), (false, true, 1), (true, false, 1), (true, true, 2)] {
            let mut resolved = resolved(trace);
            let invocation = command::Exec {
                shared: command::Shared::default(),
                prompt: command::Prompt::Stdin,
                output: Some("/unopened-output-parent/result".into()),
                ephemeral: false,
                deliver: false,
                json,
            };
            let (limits, result_file) = derive_execution(&mut resolved, &invocation, skein_lib::Wall::from_nanos(0))
                .expect("pure invocation derivation");
            let agent = &resolved.agent;
            let events = agent.service.limits.events.as_ref();
            assert_eq!(events.map_or(0, |events| events.sinks.destinations), destinations);
            let inventory = smith_local_service::profile::effective(
                &agent.profile,
                &agent.declarations,
                &agent.service,
                &limits,
                result_file,
            )
            .expect("one effective limits owner");
            assert!(inventory.iter().any(|limit| limit.name.field == "teardown"));
        }
    }

    #[test]
    fn interactive_chat_checks_derive_accounts_endpoints_and_the_live_sink() {
        // The smoke-tools Chat arm: standard profile, codex/gpt-6-luna
        // selected at low effort, an env token and an everything trace.
        // Invocation derivation checks the selected sources without reading the variable.
        let certificate =
            skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
        let text = format!(
            "model='codex/gpt-6-luna'\neffort='low'\n\
             [accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n\
             [endpoints.codex]\ntrust.system=false\ntrust.der=['{certificate}']\n\
             [trace]\npath='/unopened-events'\ncapture='everything'\n"
        );
        let settings = settings::merge(
            &command::Shared::default(),
            "/workspace".into(),
            &[settings::Source { layer: settings::Layer::User, path: "chat-start.toml".into(), text }],
        )
        .unwrap_or_else(|why| panic!("{}", why.line()));
        let mut resolved = settings::resolve::resolve(
            &settings,
            &Command::Check { shared: command::Shared::default(), json: true, execution: None },
        )
        .unwrap_or_else(|why| panic!("{}", why.line()));
        assert_eq!(resolved.accounts.len(), 1);
        assert_eq!(resolved.limits.endpoints.len(), 1);

        // This is check's derive_invocation -> interactive_header path; its
        // derivation includes the trace header's event limits as well.
        interactive_limits(&mut resolved).expect("standard Chat startup checks");
        let events = resolved.agent.service.limits.events.expect("interactive trace and view");
        assert!(events.front);
        assert_eq!(events.sinks.destinations, 1);
    }
}

#[cfg(test)]
mod catalog_tests;
