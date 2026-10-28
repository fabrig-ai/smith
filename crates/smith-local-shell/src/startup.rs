//! Shared product refusals, startup entropy and local service derivation.
//! Contract: shell.md, sections 4, 5.3 and 9; protocol/limits.md, section 4.
use smith_host_domain as host;
use smith_host_protocol as protocol;
use smith_local_domain as local;
use smith_local_service as service;

/// A local invocation failure, returned by the shell before or during host setup.
#[derive(Debug)]
pub enum Failure {
    /// A settings or derived-limit refusal; the binary reports its named detail and exits 2.
    Configuration { detail: String },
    /// A host effect failure; the binary reports its detail and preserves exit 1.
    Host { detail: String },
}

impl Failure {
    /// The existing interactive invocation category for this failure.
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        match self {
            Self::Configuration { .. } => 2,
            Self::Host { .. } => 1,
        }
    }

    /// The setting, limit or host operation that refused the invocation.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Configuration { detail } | Self::Host { detail } => detail,
        }
    }
}

/// Collect the fixed startup entropy required by the OAuth composition.
pub(super) fn oauth_entropy() -> Result<[u8; 32], String> {
    let mut entropy = [0_u8; 32];
    for part in entropy.chunks_exact_mut(8) {
        let seed = skein_shell::seed().map_err(|error| format!("OAuth entropy failed (errno {error})"))?;
        part.copy_from_slice(&seed.to_be_bytes());
    }
    Ok(entropy)
}

/// Bound the common local IO and the selected child before constructing either.
pub(crate) fn service_limits(
    local: local::Limits,
    host: host::Limits,
    agent: &smith_agent_service::Config,
) -> Result<service::Limits, String> {
    let queue = local::max_out(&local).max(host::max_out(&host)).max(256);
    let mut process = service::ProcessLimits {
        io: agent.limits.io,
        channel: protocol::Limits {
            bodies: agent.limits.channel.bodies,
            charter: agent.limits.channel.charter,
            transcript: agent.limits.channel.transcript,
            endpoints: agent.limits.channel.endpoints,
            channel: agent.limits.channel.channel,
            start_bytes: agent.limits.channel.start_bytes,
            calls: host.calls,
        },
        detail_bytes: host.detail_bytes,
        queue,
    };
    process.channel.channel.output_bytes = protocol::output_bytes(&process.channel).ok_or_else(|| {
        profile_refusal(smith_agent_service::profile::Refusal::Derived {
            limit: smith_agent_service::profile::LimitName::deployment(
                smith_agent_service::profile::Layer::Channel,
                "host_output_bytes",
            ),
        })
    })?;
    Ok(service::Limits { local, inline: None, envelope_bytes: host.answer_bytes, process, queue, watermark: queue })
}

fn profile_refusal(refusal: smith_agent_service::profile::Refusal) -> String {
    String::from_utf8(smith_agent_service::profile::words(&refusal).into_vec()).expect("refusal words are UTF-8")
}

/// Startup metadata for the product stream, with its chat and capture policy.
#[must_use]
pub fn stream_header(
    chat: Box<[u8]>,
    capture: smith_agent_service::sinks::Capture,
    delivery: smith_agent_service::sinks::Delivery,
    pid: u64,
    limits: skein_lib::List<smith_events::EffectiveLimit>,
) -> smith_events::SessionStarted {
    smith_events::SessionStarted {
        wall_ms: 0,
        agent: smith_agent_shell::identity(),
        mode: smith_events::Mode::Chat,
        chat: Some(chat),
        containment: smith_events::Containment::Groups,
        pid,
        profile: b"standard".as_slice().into(),
        capture: capture.encoded(),
        delivery: delivery.encoded(),
        versions: smith_events::Versions {
            events: 4,
            channel: 5,
            charter: 4,
            transcript: 6,
            chat_store: Some(smith_local_protocol::CHAT_STORE_VERSION),
        },
        limits,
    }
}
