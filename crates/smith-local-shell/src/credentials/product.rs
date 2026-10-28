//! Startup source descriptors and the product's exact credential notice names.
//! Driver sources are handed in once; key and program values stay in memory
//! for repeated shell lends (shell.md, section 7.1). Locations contain no values.
use super::{Credential, Refusal, Source, environment, inspect, source};
use crate::settings::resolve::Resolved;
use skein_lib::{Duration, List, Map, Wall};
use smith_local_service as service;
use smith_protocol_channel::GrantValue;

/// Names used by the shell when its driver reports a env token's expiry.
#[derive(Debug)]
pub struct Location {
    pub account: u32,
    pub name: String,
    pub source: Source,
}

/// Startup's selected handed-in sources and bounded notice metadata.
pub struct Prepared {
    pub sources: List<service::Source>,
    pub locations: List<Location>,
    /// Last lent generation (zero at startup) and value per shell-owned account.
    pub lendable: Map<u32, (u64, GrantValue)>,
    pub limits: skein_oauth_accounts::Limits,
}

pub(crate) fn codex(resolved: &Resolved, account: u32) -> bool {
    for model in &resolved.local.models {
        if model.account == Some(account) {
            let endpoint = resolved
                .agent
                .service
                .llm_endpoints
                .resolve(smith_domain::llm::Endpoint(model.endpoint.0))
                .expect("resolved endpoint");
            match endpoint.provider {
                skein_llm::Provider::OpenAiCodex => return true,
                skein_llm::Provider::OpenAi | skein_llm::Provider::Anthropic => {}
            }
        }
    }
    false
}

/// Inspect selected accounts, report startup warnings once, and open private roots.
pub fn prepare(
    resolved: &Resolved,
    wall: Wall,
    interactive: bool,
    programs: &super::program_check::ProgramStates,
) -> Result<Prepared, Refusal> {
    warnings(resolved, wall, programs)?;
    let count = u32::try_from(resolved.accounts.len()).map_err(|_| {
        Refusal::Configuration(crate::settings::Refusal {
            origin: crate::settings::Origin::BuiltIn,
            key: "accounts".into(),
            problem: crate::settings::Problem::Input("too many account sources".into()),
        })
    })?;
    let mut sources = List::with_capacity(count);
    let mut locations = List::with_capacity(count);
    let mut lendable = Map::with_capacity(count);
    let directory = super::Directory::of(&resolved.settings);
    for account in &resolved.accounts {
        let descriptor = source(
            &account.fields,
            &resolved.settings,
            codex(resolved, account.number),
            &account.name,
            directory.as_ref(),
        );
        locations
            .push(Location { account: account.number, name: account.name.clone(), source: descriptor.clone() })
            .expect("selected locations fit their account bound");
        match &descriptor {
            Source::Key { file } => {
                let bytes = super::key::read(file).map_err(|credential| unavailable(&account.name, credential))?;
                lendable
                    .insert(account.number, (0, GrantValue::Key(bytes)))
                    .expect("selected keys fit their account bound");
                continue;
            }
            Source::Program { name } => {
                let credential = inspect(&descriptor, wall, programs);
                if !credential.usable() {
                    return Err(unavailable(&account.name, credential));
                }
                lendable
                    .insert(account.number, (0, GrantValue::Program(name.as_bytes().into())))
                    .expect("selected programs fit their account bound");
                continue;
            }
            Source::Nothing => continue,
            Source::Env { .. } | Source::SignIn { .. } => {}
        }
        let handed = match descriptor {
            Source::Env { variable, codex } => {
                let borrowed = environment(&variable, codex, wall)
                    .ok_or_else(|| unavailable(&account.name, Credential::Unset { variable: variable.clone() }))?;
                if borrowed.record.remaining(wall) == Duration::ZERO {
                    return Err(unavailable(
                        &account.name,
                        Credential::ExpiredVariable { at: borrowed.record.expires_at.as_nanos(), variable },
                    ));
                }
                service::Source::Env { account: account.number, borrowed }
            }
            Source::SignIn { file, required_scopes } => {
                let interactive = interactive && !account.fields.contains_key("provider");
                let credential = super::sign_in::inspect(&file, wall, required_scopes.as_deref());
                if !interactive && !credential.usable() {
                    return Err(unavailable(&account.name, credential));
                }
                super::sign_in::private_source(
                    account.number,
                    &file,
                    super::registration(account, codex(resolved, account.number), &resolved.settings.workspace)
                        .map_err(Refusal::Configuration)?,
                    interactive,
                )
                .map_err(|_| unavailable(&account.name, Credential::Unreadable { file }))?
            }
            Source::Key { .. } | Source::Program { .. } | Source::Nothing => {
                unreachable!("shell-owned source handled above")
            }
        };
        assert!(sources.push(handed).is_ok(), "selected sources fit their account bound");
    }
    let limits = limits(resolved.agent.service.limits.io, count.max(1), resolved.agent.profile.policy.refresh_lead);
    Ok(Prepared { sources, locations, lendable, limits })
}

fn warnings(resolved: &Resolved, wall: Wall, programs: &super::program_check::ProgramStates) -> Result<(), Refusal> {
    for warning in crate::check::startup_warnings(&resolved.settings).map_err(Refusal::Configuration)? {
        eprintln!("smith: {}", crate::check::warnings::line(&warning));
    }
    for warning in super::unused_warnings(resolved, wall, programs) {
        eprintln!("smith: {}", crate::check::warnings::line(&warning));
    }
    Ok(())
}

/// Read only a saved record's generation for a refresh observation (shell.md, 7.3).
pub fn record_generation(file: &std::path::Path) -> Result<Option<u64>, String> {
    super::sign_in::record(file).map(|record| record.map(|record| record.generation))
}

fn unavailable(account: &str, credential: Credential) -> Refusal {
    Refusal::Credentials { account: account.into(), credential }
}

/// Receiving bounds for handed-in accounts; no exchange or public listener is opened.
#[must_use]
pub fn limits(mut io: skein_io::Limits, accounts: u32, refresh_lead: Duration) -> skein_oauth_accounts::Limits {
    // One exchange, its callback and its listener need three socket slots.
    io.sockets = 3;
    skein_oauth_accounts::Limits {
        accounts,
        file_stall: Duration::from_secs(30),
        lock_wait: Duration::from_secs(1),
        exchanges: 1,
        listeners: 1,
        server: skein_http::server::Limits {
            head: 16_384,
            headers: 64,
            body: 1024,
            read: 1024,
            response: 1024,
            send: 1024,
        },
        refresh_lead,
        client: client_limits(),
        http: skein_http::client::Limits { request: 16_384, head: 16_384, headers: 128, read: 16_384, send: 16_384 },
        tls: skein_tls::client::Limits { read: 16_384, send: 16_384, records: 1_048_576 },
        io,
    }
}

/// Native exchange bounds shared by startup inspection and private sign-in.
#[must_use]
pub fn client_limits() -> skein_oauth::ClientLimits {
    skein_oauth::ClientLimits {
        parameter_bytes: 1024,
        resource_bytes: 1024,
        nonce_bytes: 64,
        metadata_bytes: 0,
        key_set_bytes: 32768,
        keys: 8,
        modulus_bits: 2048,
        skew: Duration::ZERO,
        document: super::sign_in::document_limits(),
        uri_bytes: 4096,
        scope_bytes: 1024,
        state_bytes: 64,
        code_bytes: 4096,
        url_bytes: 8192,
        request_bytes: 16_384,
        sign_in_time: Duration::from_secs(120),
        request_time: Duration::from_secs(20),
        backoff_base: Duration::from_secs(1),
        backoff_ceiling: Duration::from_secs(8),
        max_attempts: 3,
    }
}

/// Account words in both chat fronts (shell.md, section 4.1), without secrets.
pub(crate) fn status_words(source: &Source, openai: bool) -> &'static str {
    match source {
        Source::Program { .. } => "through Claude Code",
        Source::Key { .. } | Source::Env { .. } => "API key",
        Source::SignIn { .. } if openai => "Using ChatGPT plan",
        Source::SignIn { .. } | Source::Nothing => "",
    }
}

#[cfg(test)]
mod tests;
