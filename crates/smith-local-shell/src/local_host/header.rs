//! Product startup names and their receiving and allocation bounds.
//! Contract: protocol/events.md, section 3.2; protocol/limits.md, section 9.

use skein_lib::List;
use smith_agent_shell::{config, trace::TraceConfig};
use smith_local_domain::AgentLimits;
use smith_local_service as service;

use super::{Local, configuration_failure, derivation_refusal};

/// Startup's destination count and whether the product asks for a typed live view.
#[derive(Clone, Copy)]
pub struct View {
    pub destinations: u32,
    pub front: bool,
}

impl Local {
    /// Prepare the product's startup record before any local resources move.
    pub fn prepare_header(
        configuration: &mut config::Configuration,
        limits: &mut service::Limits,
        trace: Option<&TraceConfig>,
        chat: &[u8],
        pid: u64,
        view: View,
        result_file: skein_lib::Duration,
    ) -> Result<Option<smith_events::SessionStarted>, crate::startup::Failure> {
        match trace {
            Some(trace) => {
                let header = Self::trace_header(configuration, limits, trace, chat.into(), pid, view, result_file)?;
                Ok(Some(header))
            }
            None => Ok(None),
        }
    }

    /// Admit the product inventory before its configured producer or inline effects are built.
    pub fn trace_header(
        configuration: &mut config::Configuration,
        limits: &mut service::Limits,
        trace: &TraceConfig,
        chat: Box<[u8]>,
        pid: u64,
        view: View,
        result_file: skein_lib::Duration,
    ) -> Result<smith_events::SessionStarted, crate::startup::Failure> {
        let options = smith_charter::CEILINGS.llm_effort_some;
        let write_deadline = match configuration.service.limits.events {
            Some(events) => events.write_deadline,
            None => configuration.profile.policy.write_deadline,
        };
        // The second admission includes the sink's own named limits.
        for _ in 0..2 {
            let header = names(configuration, limits, result_file)?;
            configuration.service.limits.events = Some(
                smith_agent_service::EventLimits::derive(
                    &configuration.service.limits,
                    trace.capture,
                    view.destinations,
                    options,
                    header.as_slice(),
                    write_deadline,
                )
                .ok_or_else(|| configuration_failure(derivation_refusal("local_header")))?,
            );
            configuration.service.limits.events.as_mut().expect("derived event limits").front = view.front;
            match limits.local.agent {
                AgentLimits::Inline(_) => {
                    if !smith_agent_service::effects_valid(
                        &configuration.service.limits,
                        &configuration.service.llm_endpoints,
                    ) || !smith_agent_service::io_valid(&configuration.service.limits)
                    {
                        return Err(configuration_failure(derivation_refusal("local_header")));
                    }
                }
                AgentLimits::Spawned(_) => {}
            }
        }
        Ok(crate::startup::stream_header(
            chat,
            trace.capture,
            trace.delivery,
            pid,
            names(configuration, limits, result_file)?,
        ))
    }
}

fn names(
    configuration: &config::Configuration,
    limits: &service::Limits,
    result_file: skein_lib::Duration,
) -> Result<List<smith_events::EffectiveLimit>, crate::startup::Failure> {
    let inventory = service::profile::effective(
        &configuration.profile,
        &configuration.declarations,
        &configuration.service,
        limits,
        result_file,
    )
    .ok_or_else(|| configuration_failure(derivation_refusal("local_header")))?;
    smith_agent_service::profile::header(
        &inventory,
        &configuration.service.channel_endpoints,
        &configuration.declarations,
    )
    .ok_or_else(|| configuration_failure(derivation_refusal("local_header")))
}
