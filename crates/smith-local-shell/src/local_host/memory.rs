//! Startup resource counts for the local shell (protocol/limits.md, sections 3–4).
//! Keeps the selected sources and fronts until construction, knows no resource
//! descriptors, and derives kernel operation counts before resource adoption.
use super::{Local, configuration_failure, derivation_refusal};
use smith_agent_service as agent;
use smith_local_service as service;

/// Product startup owners prepared together before a destination is adopted.
pub(crate) struct Admission {
    pub handed: Option<crate::credentials::Prepared>,
    pub front: Option<crate::front::Front>,
    pub producer: Option<agent::Config>,
    pub programs: Option<smith_protocol_program::Programs>,
}

impl Local {
    /// Validate service limits and derive kernel operations before adoption.
    pub(crate) fn startup_operations(
        limits: &service::Limits,
        lower_routes: u32,
        handed: Option<&crate::credentials::Prepared>,
        result_file: bool,
    ) -> Result<u32, crate::startup::Failure> {
        let mut io_limits = limits.process.io;
        let programs = handed.is_some_and(has_programs);
        if programs {
            io_limits.sockets = io_limits
                .sockets
                .checked_add(3)
                .ok_or_else(|| configuration_failure("program I/O count overflows".into()))?;
        }
        let operations = Self::kernel_operations(&io_limits, lower_routes, handed.is_some())
            .and_then(|operations| operations.checked_add(if result_file { 4 } else { 0 }))
            .ok_or_else(|| configuration_failure(derivation_refusal("local_routes")))?;
        if !service::valid(limits) {
            return Err(configuration_failure(derivation_refusal("local_limits")));
        }
        if programs {
            let mut checked = limits.clone();
            checked.process.io = io_limits;
            if !service::valid(&checked) {
                return Err(configuration_failure("invalid program check limits".into()));
            }
        }
        Ok(operations)
    }
}

pub(super) fn has_programs(prepared: &crate::credentials::Prepared) -> bool {
    prepared.locations.iter().any(|location| matches!(location.source, crate::credentials::Source::Program { .. }))
}
