//! Program declarations and endpoint bindings (protocol/agent.md, section 4).
//! Keeps paths and adapter names only; it never opens or runs a program.

use std::{collections::BTreeMap, path::PathBuf};

use serde::Deserialize;
use smith_agent_service::profile::{Layer, LimitName, Per, Refusal};

use super::{ConfigurationRefusal, Endpoint};

/// A program the agent may run as a backend (protocol/agent.md, section 4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProgramName {
    /// Claude Code, selected by the configuration for program sources.
    ClaudeCode,
}

/// A program's executable and directory of source configurations (programs.md, 4.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Program {
    pub path: PathBuf,
    pub directory: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProgramDocument {
    path: Option<String>,
    directory: Option<String>,
}

fn name(value: &str, field: &str) -> Result<ProgramName, ConfigurationRefusal> {
    match value {
        "claude_code" => Ok(ProgramName::ClaudeCode),
        _ => Err(format!("{field}: unknown program {value}").into()),
    }
}

fn path(value: Option<String>, field: &'static str) -> Result<PathBuf, ConfigurationRefusal> {
    let value = value.ok_or_else(|| ConfigurationRefusal::Input(format!("{field}: required path")))?;
    if value.len() > 4096 || value.contains('\0') || !std::path::Path::new(&value).is_absolute() {
        return Err(format!("{field}: must be an absolute path of at most 4096 bytes without NUL").into());
    }
    Ok(value.into())
}

pub(super) fn build(
    documents: BTreeMap<String, ProgramDocument>,
) -> Result<BTreeMap<ProgramName, Program>, ConfigurationRefusal> {
    let mut programs = BTreeMap::new();
    for (key, document) in documents {
        let name = name(&key, &format!("programs.{key}"))?;
        programs.insert(
            name,
            Program {
                path: path(document.path, "programs.claude_code.path")?,
                directory: path(document.directory, "programs.claude_code.directory")?,
            },
        );
    }
    Ok(programs)
}

#[expect(
    clippy::zero_sized_map_values,
    reason = "Endpoint selections retain the adapter name; Claude Code is currently the sole adapter."
)]
pub(super) fn endpoints(
    endpoints: &[Endpoint],
    programs: &BTreeMap<ProgramName, Program>,
) -> Result<BTreeMap<u32, ProgramName>, ConfigurationRefusal> {
    let mut selections = BTreeMap::new();
    for endpoint in endpoints {
        match &endpoint.program {
            None => {}
            Some(value) => {
                let program = name(value, &format!("endpoints[{}].program", endpoint.number))?;
                if !programs.contains_key(&program) {
                    return Err(ConfigurationRefusal::Profile(Refusal::Field {
                        limit: LimitName {
                            layer: Layer::Declared,
                            field: "program",
                            per: Per::Endpoint(endpoint.number),
                        },
                    }));
                }
                selections.insert(endpoint.number, program);
            }
        }
    }
    Ok(selections)
}

/// Prepare the service's program declaration; the shell later supplies entropy and relay resources.
#[expect(
    clippy::zero_sized_map_values,
    reason = "endpoint selections retain the named adapter as more adapters become available"
)]
#[cfg(unix)]
pub(super) fn backend(
    programs: &BTreeMap<ProgramName, Program>,
    endpoints: &BTreeMap<u32, ProgramName>,
) -> Option<smith_agent_service::ProgramBackend> {
    use std::os::unix::ffi::OsStrExt;
    let program = programs.get(&ProgramName::ClaudeCode)?;
    Some(smith_agent_service::ProgramBackend {
        program: smith_protocol_program::Programs {
            claude: program.path.as_os_str().as_bytes().into(),
            directory: program.directory.as_os_str().as_bytes().into(),
        },
        endpoints: endpoints.keys().copied().collect(),
        relay: Box::default(),
        entropy: skein_lib::Secret::new(Box::new([0; 32])),
        effective_user: skein_shell::effective_user(),
    })
}

#[cfg(not(unix))]
#[expect(clippy::zero_sized_map_values, reason = "endpoint selections retain the named adapter")]
pub(super) fn backend(
    _programs: &BTreeMap<ProgramName, Program>,
    _endpoints: &BTreeMap<u32, ProgramName>,
) -> Option<smith_agent_service::ProgramBackend> {
    None
}

/// Charge program declarations to the same startup pool as model declarations.
#[expect(clippy::zero_sized_map_values, reason = "endpoint selections retain the adapter name")]
pub(super) fn domain(
    endpoints: &BTreeMap<u32, ProgramName>,
    limits: &mut smith_domain::Limits,
) -> Result<Box<[smith_domain::Program]>, ConfigurationRefusal> {
    let mut programs = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints.keys() {
        let name: Box<[u8]> = b"claude_code".as_slice().into();
        let owned = u64::try_from(size_of::<smith_domain::Program>())
            .ok()
            .and_then(|cell| cell.checked_add(u64::try_from(name.len()).ok()?))
            .and_then(|owned| limits.configured_model_bytes.checked_add(owned))
            .ok_or_else(|| {
                ConfigurationRefusal::Profile(Refusal::Derived {
                    limit: LimitName::deployment(Layer::Service, "configured_model_bytes"),
                })
            })?;
        limits.configured_model_bytes = owned;
        programs.push(smith_domain::Program { endpoint: smith_domain::run::charter::Endpoint(*endpoint), name });
    }
    Ok(programs.into_boxed_slice())
}
