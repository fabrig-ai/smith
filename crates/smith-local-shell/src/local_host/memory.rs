//! The product's full startup footprint (protocol/limits.md, sections 3–4).
//! Native owners price their allocations; this owner adds the shell, handed
//! credentials, headless front and routing once, before any resource adoption.
use super::{Local, Route, configuration_failure, derivation_refusal};
use skein_io::{self as io, kernel};
use skein_lib::{Map, Queue, Token};
use smith_agent_service as agent;
use smith_local_domain as domain;
use smith_local_service as service;

/// Product startup owners admitted together before a destination is adopted.
pub(crate) struct Admission {
    pub handed: Option<crate::credentials::Prepared>,
    pub front: Option<crate::front::Front>,
    pub producer: Option<agent::Config>,
    pub programs: Option<smith_protocol_program::Programs>,
}

impl Local {
    pub(crate) fn producer_memory(configuration: &agent::Config) -> Option<u64> {
        match configuration.limits.events {
            Some(_) => agent::events::worst_case(&configuration.limits),
            None => Some(0),
        }
    }

    /// Price the same startup owners for composition and invocation-aware check.
    pub(crate) fn startup_memory(
        limits: &service::Limits,
        lower_routes: u32,
        handed: Option<&crate::credentials::Prepared>,
        headless: Option<&crate::front::headless::Headless>,
        interactive: Option<&crate::front::interactive::Interactive>,
    ) -> Result<(u64, u32), crate::startup::Failure> {
        let mut io_limits = limits.process.io;
        let programs = handed.is_some_and(has_programs);
        if programs {
            io_limits.sockets = io_limits
                .sockets
                .checked_add(3)
                .ok_or_else(|| configuration_failure("program I/O count overflows".into()))?;
        }
        let file_operations = if headless.is_some_and(|front| front.file_memory != 0) { 4 } else { 0 };
        let base_operations = Self::kernel_operations(&io_limits, lower_routes, handed.is_some())
            .ok_or_else(|| configuration_failure(derivation_refusal("local_routes")))?;
        let operations = base_operations
            .checked_add(file_operations)
            .ok_or_else(|| configuration_failure(derivation_refusal("local_routes")))?;
        let mut worst = accounted_memory(limits, lower_routes).map_err(configuration_failure)?;
        if programs {
            let mut priced = limits.clone();
            priced.process.io = io_limits;
            let extra = service::worst_case(&priced)
                .and_then(|bytes| bytes.checked_sub(service::worst_case(limits)?))
                .and_then(|bytes| {
                    bytes.checked_add(service::program_checks_worst_case(
                        limits.local.smith.accounts,
                        crate::credentials::program_source::DETAIL_BYTES,
                    )?)
                })
                .and_then(|bytes| bytes.checked_add(Map::<Token, Route>::worst_case(base_operations)?))
                .and_then(|bytes| {
                    bytes.checked_sub(Map::<Token, Route>::worst_case(Self::kernel_operations(
                        &limits.process.io,
                        lower_routes,
                        handed.is_some(),
                    )?)?)
                })
                .ok_or_else(|| configuration_failure("program check memory overflows".into()))?;
            worst = worst
                .checked_add(extra)
                .ok_or_else(|| configuration_failure("program check memory overflows".into()))?;
        }
        if let Some(prepared) = handed {
            worst = worst
                .checked_add(
                    prepared
                        .worst_case()
                        .ok_or_else(|| configuration_failure(derivation_refusal("account_worst_case")))?,
                )
                .and_then(|bytes| bytes.checked_add(Map::<Token, Route>::worst_case(8)?))
                .ok_or_else(|| configuration_failure(derivation_refusal("account_worst_case")))?;
        }
        if let Some(front) = headless {
            worst = worst
                .checked_add(
                    front
                        .worst_case()
                        .ok_or_else(|| configuration_failure(derivation_refusal("headless_worst_case")))?,
                )
                .ok_or_else(|| configuration_failure(derivation_refusal("headless_worst_case")))?;
        }
        let file_routes = Map::<Token, Route>::worst_case(operations)
            .and_then(|bytes| bytes.checked_sub(Map::<Token, Route>::worst_case(base_operations)?))
            .ok_or_else(|| configuration_failure(derivation_refusal("local_routes")))?;
        // Interactive prices the capped remote prefix and render scratch; the
        // service already prices four live slots and one popped piece.
        worst = worst
            .checked_add(interactive.map_or(0, crate::front::interactive::Interactive::worst_case))
            .ok_or_else(|| configuration_failure(derivation_refusal("interactive_worst_case")))?;
        worst = worst
            .checked_add(file_routes)
            .ok_or_else(|| configuration_failure(derivation_refusal("local_worst_case")))?;
        Ok((worst, operations))
    }

    /// Price the selected local service and every outer shell owner before adopting resources.
    /// `lower_routes` is zero for a spawned child; handed sources are priced at startup.
    /// Contract: protocol/limits.md, sections 3 and 4.
    #[must_use]
    pub fn required_memory(limits: &service::Limits, lower_routes: u32) -> Option<u64> {
        accounted_memory(limits, lower_routes).ok()
    }

    /// The inline product's admitted ownership by part, including its front and routing.
    pub fn memory_parts(
        limits: &service::Limits,
        lower: &agent::Config,
        lower_routes: u32,
    ) -> Result<Vec<(&'static str, u64)>, String> {
        let mut parts = Vec::new();
        for (name, value) in service::memory_parts(limits).ok_or_else(|| derivation_refusal("local_parts"))? {
            if name != "effects_and_io" {
                parts.push((name, value));
            }
        }
        let effects = agent::effects_parts(&lower.limits, &lower.llm_endpoints)
            .ok_or_else(|| derivation_refusal("effects_parts"))?;
        let io = agent::io_worst_case(&lower.limits).ok_or_else(|| derivation_refusal("io_parts"))?;
        let expected = effects
            .iter()
            .try_fold(io, |total, (_, value)| total.checked_add(*value))
            .ok_or_else(|| derivation_refusal("effects_parts"))?;
        if limits.inline.as_ref().map(|inline| inline.memory) != Some(expected) {
            return Err(derivation_refusal("inline_parts"));
        }
        parts.extend(effects);
        parts.push(("io", io));
        parts.extend(shell_parts(limits, lower_routes)?);
        Ok(parts)
    }
}

/// The constructor and offline startup checks share one complete shell accounting entrance.
fn accounted_memory(limits: &service::Limits, lower_routes: u32) -> Result<u64, String> {
    let mut total = service::worst_case(limits).ok_or_else(|| derivation_refusal("local_worst_case"))?;
    for (_, value) in shell_parts(limits, lower_routes)? {
        total = total.checked_add(value).ok_or_else(|| derivation_refusal("local_worst_case"))?;
    }
    Ok(total)
}

fn shell_parts(limits: &service::Limits, lower_routes: u32) -> Result<[(&'static str, u64); 3], String> {
    let queue = limits.queue;
    let show_bytes = limits.local.show_bytes;
    let io_limits = limits.process.io;
    let operations = io::operations(&io_limits)
        .and_then(|count| count.checked_mul(2))
        .and_then(|count| count.checked_add(64))
        .and_then(|count| count.checked_add(lower_routes))
        .ok_or_else(|| derivation_refusal("local_routes"))?;
    let io = match limits.local.agent {
        domain::AgentLimits::Inline(_) => 0,
        domain::AgentLimits::Spawned(_) => {
            io::worst_case(&io_limits).ok_or_else(|| derivation_refusal("local_worst_case"))?
        }
    };
    let front = u64::from(queue)
        .checked_mul(u64::from(show_bytes.max(8192)))
        .and_then(|count| count.checked_add(u64::from(show_bytes.max(512))))
        .ok_or_else(|| derivation_refusal("front_worst_case"))?;
    let routing = Queue::<kernel::Complete>::worst_case(queue)
        .and_then(|count| count.checked_mul(3))
        .and_then(|count| count.checked_add(Queue::<kernel::Submit>::worst_case(queue)?.checked_mul(3)?))
        .and_then(|count| count.checked_add(Queue::<io::Event>::worst_case(queue)?))
        .and_then(|count| count.checked_add(Queue::<io::Request>::worst_case(queue)?))
        .and_then(|count| count.checked_add(Map::<Token, Route>::worst_case(operations)?))
        .and_then(|count| count.checked_add(u64::try_from(size_of::<Local>()).ok()?))
        .ok_or_else(|| derivation_refusal("local_worst_case"))?;
    Ok([("shell_io", io), ("front", front), ("shell_routing", routing)])
}

pub(super) fn has_programs(prepared: &crate::credentials::Prepared) -> bool {
    prepared.locations.iter().any(|location| matches!(location.source, crate::credentials::Source::Program { .. }))
}
