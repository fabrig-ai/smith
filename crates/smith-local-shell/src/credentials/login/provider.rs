//! One explicit sign-in or revocation, under the record lock (shell.md, 4.5–4.6).
use super::super::{Account, Directory, host_id, registration, sign_in};
use super::{Environment, LoginOutcome};
use crate::settings::Settings;
use driver::{Login, Stage};
use skein_lib::{List, Queue};
use skein_shell::Clock;
use smith_local_service::{ACCOUNTS_MAX_OUT, Accounts};
use std::{collections::BTreeSet, path::Path};

mod driver;

/// The explicit credential command selected by the shell.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::credentials) enum Operation {
    /// Sign in and keep the provider record.
    SignIn,
    /// Revoke and remove the provider record.
    SignOut,
}

/// The endpoint defaults used for a configured registration.
#[derive(Clone, Copy)]
pub(in crate::credentials) enum Registration {
    /// Use the declaration without Codex defaults.
    Configured,
    /// Use the Codex endpoint defaults.
    Codex,
}

pub(in crate::credentials) fn run(
    account: &Account,
    file: &Path,
    settings: &Settings,
    kind: Registration,
    operation: Operation,
    clock: &Clock,
    environment: &Environment,
) -> Result<LoginOutcome, String> {
    let logout = operation == Operation::SignOut;
    let prior = sign_in::record(file)?;
    if logout && prior.is_none() {
        eprintln!("account {}: signed out", account.name);
        return Ok(LoginOutcome::Usable);
    }
    let provider = account.fields.get("provider").and_then(|value| value.value.as_str()) == Some("openai");
    let registered = if provider {
        let host = if logout {
            String::new()
        } else {
            let directory = Directory::of(settings).ok_or("an absolute credentials directory is required")?;
            host_id::read_or_draw(&directory, &crate::startup::oauth_entropy()?)?
        };
        registration::configured(account, &host, &settings.workspace)
    } else {
        registration(
            account,
            match kind {
                Registration::Configured => false,
                Registration::Codex => true,
            },
            &settings.workspace,
        )
    }
    .map_err(|why| why.line())?;
    let first_plan =
        prior.as_ref().is_none_or(|record| !skein_oauth::missing_scopes(&registered.registration, record).is_empty());
    let source = sign_in::private_source(0, file, registered, false)?;
    let io = skein_io::Limits {
        sockets: 3,
        names: 0,
        refusals: 4,
        intake: 32768,
        receive: 4096,
        output: 32768,
        sends: 8,
        accepts: 1,
        backlog: 2,
        close_timeout: skein_lib::Duration::from_secs(1),
        retry: skein_lib::Duration::from_millis(10),
    };
    let document = crate::settings::resolve::document::profile(settings).map_err(|why| why.line())?;
    let profile = smith_agent_shell::config::profile_from_value(document)
        .map_err(smith_agent_shell::config::ConfigurationRefusal::detail)?;
    let limits = super::super::limits(io, 1, profile.policy.refresh_lead);
    let mut sources = List::with_capacity(1);
    assert!(sources.push(source).is_ok(), "one login source");
    let mut driver = Accounts::shared(sources, limits, crate::startup::oauth_entropy()?)
        .map_err(|_| "account driver unavailable")?;
    if logout {
        driver.sign_out(0);
    } else {
        assert!(driver.sign_in(0), "selected sign-in account");
    }
    let directory = Directory::of(settings);
    let names: BTreeSet<_> = settings
        .values
        .iter()
        .filter_map(|(key, value)| {
            key.strip_prefix("accounts.")
                .and_then(|rest| rest.strip_suffix(".source"))
                .filter(|_| value.value.as_str() == Some("sign_in"))
                .map(|name| serde_json::from_str::<String>(name).unwrap_or_else(|_| name.into()))
        })
        .collect();
    let others = names
        .into_iter()
        .filter(|name| name != &account.name)
        .filter_map(|name| directory.as_ref().map(|directory| (name.clone(), directory.token(&name))))
        .collect();
    let ssh = environment
        .variables
        .iter()
        .find(|(key, _)| key == "SSH_CONNECTION")
        .map(|(_, value)| value.to_string_lossy().split_whitespace().nth(2).unwrap_or("<this host>").to_owned());
    let operations =
        skein_io::operations(&limits.io).and_then(|n| n.checked_add(64)).ok_or("login operation bound overflows")?;
    let mut kernel =
        skein_shell::Kernel::open(skein_shell::Config { operations }).map_err(|_| "login kernel unavailable")?;
    let mut host = Login {
        driver,
        name: account.name.clone(),
        outcome: None,
        operation: if logout { Stage::SignOut } else { Stage::SignIn },
        closing: false,
        notice: provider && first_plan,
        others,
        ssh,
        operations,
        out: Queue::with_capacity(ACCOUNTS_MAX_OUT),
        completions: Queue::with_capacity(8),
    };
    let _exit = skein_shell::drive(&mut kernel, clock, &mut host);
    Ok(host.outcome.expect("login closes after its terminal"))
}
