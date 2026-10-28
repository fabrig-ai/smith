//! Limit validity for local policy and its selected child agent
//! (domain/host.md, sections 6 and 8; programming-model.md, section 6.3).
//! Keeps only receiving limits, never chat state or lower IO. `valid` checks
//! notice and inbox bounds; `AgentLimits::inline` derives the single child.

use alloc::boxed::Box;
use smith_domain::{self as agent, run};

/// Capacity of one local chat and its child agent.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Native typed charter, transcript, turn and delivery receiving bounds for either kind.
    pub smith: agent::Limits,
    /// Child kind and its own routing and lifecycle allowances.
    pub agent: AgentLimits,
    /// Endpoint names permitted by the child agent's configuration.
    pub endpoints: Box<[run::charter::Endpoint]>,
    /// Maximum chat store-key bytes.
    pub chat_bytes: u32,
    /// Maximum local instructions and brief text bytes.
    pub text_bytes: u32,
    /// Maximum model choices, main included.
    pub models: u32,
    /// Maximum terminal line bytes.
    pub line_bytes: u32,
    /// Maximum rendered text bytes per notice.
    pub show_bytes: u32,
    /// Maximum turns awaiting durable acknowledgement.
    pub unsaved: u32,
    /// Maximum content-free observations.
    pub facts: u32,
}

/// The child selected by the host, sharing the same typed parent vocabulary.
#[derive(Clone, Copy, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "fixed receiving profiles remain copyable and are included in Domain ownership"
)]
pub enum AgentLimits {
    /// Native roots with LLM and machine effects delegated in this process.
    Inline(smith_inline_agent::Limits),
    /// An agent process supervised by the host kit.
    Spawned(smith_host_domain::Limits),
}

impl AgentLimits {
    /// Build the single inline child with enough credit for one complete native turn.
    #[must_use]
    pub fn inline(
        smith: agent::Limits,
        turns: u32,
        wall_time: skein_lib::Duration,
        cancel_grace: skein_lib::Duration,
    ) -> Option<Self> {
        let limits = smith_inline_agent::Limits {
            slots: 1,
            smith,
            window: agent::Window { turns, bytes: agent::max_turn_bytes(&smith)? },
            wall_time,
            cancel_grace,
            facts: 32,
        };
        if !smith_inline_agent::valid(&limits) {
            return None;
        }
        Some(Self::Inline(limits))
    }

    pub(crate) fn max_out(self) -> u32 {
        match self {
            Self::Inline(limits) => smith_inline_agent::max_out(&limits),
            Self::Spawned(limits) => smith_host_domain::max_out(&limits),
        }
    }

    fn valid(self) -> bool {
        match self {
            Self::Inline(limits) => smith_inline_agent::valid(&limits),
            Self::Spawned(limits) => smith_host_domain::valid(&limits),
        }
    }
}

/// Largest terminal line that fits the run's rendered message bound (domain/host.md, section 8).
#[must_use]
pub fn max_line_bytes(message_bytes: u32) -> Option<u32> {
    let label_bytes = u32::try_from(crate::person::LABEL.len()).ok()?;
    message_bytes.checked_sub(label_bytes)?.checked_sub(2)
}

/// Whether local notices, inbox capacity and child limits can be represented.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let Some(show_minimum) = limits.chat_bytes.checked_add(52) else { return false };
    limits.models > 0
        && limits.unsaved > 0
        && limits.show_bytes >= show_minimum.max(64)
        && limits.smith.run.messages.checked_add(1).is_some()
        && agent::valid(&limits.smith)
        && limits.agent.valid()
}
