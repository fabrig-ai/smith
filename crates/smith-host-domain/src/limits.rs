//! Checked capacities and ownership (domain/host.md, sections 3, 4 and 6;
//! programming-model.md, section 6). Parent owns forwarded turn payloads; kit
//! owns metadata and queued responses through final Answer. No live ownership is known
//! here; `valid` checks constructor capacities.
use crate::Delivered;
use skein_lib::Duration;

/// Immutable parent bounds; invalid arithmetic refuses construction.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Contained process slots.
    pub agents: u32,
    /// Mount descriptors per start.
    pub directories: u32,
    /// Conflict paths per mount.
    pub conflicts: u32,
    /// Relative conflict path bytes, at most 4096; larger configurations are
    /// checked by admission and `valid`.
    pub path_bytes: u32,
    /// Mount and generic tool label bytes.
    pub name_bytes: u32,
    /// Distinct credential names.
    pub accounts: u32,
    /// Typed start charter ownership, in bytes.
    pub charter_bytes: u64,
    /// Concrete transcript ownership, in bytes.
    pub transcript_bytes: u64,
    /// Opaque post-transcript answer bytes.
    pub answered_bytes: u64,
    /// Rendered message bytes, including label, separator and text.
    pub message_bytes: u64,
    /// Queued plus sent-unread message names.
    pub messages: u32,
    /// Outstanding parent operation rights and reply transmissions.
    pub calls: u32,
    /// Generic tool or delivery argument bytes.
    pub call_bytes: u64,
    /// Generic tool result and sealed delivery ownership cap.
    pub answer_bytes: u64,
    /// Forwarded turn metadata including queued ACK transmissions.
    pub turns: u32,
    /// Maximum one turn payload.
    pub turn_bytes: u64,
    /// Total parent-owned turn payload credit.
    pub unacknowledged_bytes: u64,
    /// Opaque agent fact bytes.
    pub fact_bytes: u64,
    /// Concrete declared answer ownership, in bytes.
    pub outcome_bytes: u64,
    /// Operator process-tail bytes.
    pub detail_bytes: u32,
    /// Lower spawn terminal deadline.
    pub spawn_timeout: Duration,
    /// Unpaused progress deadline.
    pub no_progress: Duration,
    /// The agent's longest silent deadline between progress reports.
    pub silence: Duration,
    /// Maximum declared long-operation extension.
    pub long_span: Duration,
    /// Simultaneously announced spans, each ended by one `LongDone`.
    pub long_open: u32,
    /// Independent working wall deadline.
    pub wall_time: Duration,
    /// A cancelled run's grace to answer before tree termination.
    pub cancel_grace: Duration,
    /// An answered agent's grace to exit, strictly above teardown.
    pub exit_grace: Duration,
    /// The agent's teardown bound, supplied by its configuration owner.
    pub teardown: Duration,
    /// Terminate grace before tree kill.
    pub kill_after: Duration,
    /// Legacy observation allowance, carried without capping the facts queue.
    pub facts: u32,
}

/// A cross-process relationship the kit refuses by name (protocol/limits.md, section 4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Check {
    /// The answered agent's exit grace must exceed its teardown bound.
    GraceOverTeardown { exit_grace: Duration, teardown: Duration },
    /// The watchdog must exceed the agent's silent deadline.
    WatchdogOverSilence { no_progress: Duration, silence: Duration },
}

/// Refuse a supervisor grace or watchdog that can end an agent within its allowed bound.
pub fn check(limits: &Limits) -> Result<(), Check> {
    if limits.exit_grace <= limits.teardown {
        return Err(Check::GraceOverTeardown { exit_grace: limits.exit_grace, teardown: limits.teardown });
    }
    if limits.no_progress <= limits.silence {
        return Err(Check::WatchdogOverSilence { no_progress: limits.no_progress, silence: limits.silence });
    }
    Ok(())
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    limits.path_bytes <= 4096
        && limits.turns > 0
        && limits.turn_bytes > 0
        && limits.unacknowledged_bytes >= limits.turn_bytes
        && limits.answer_bytes >= Delivered::worst_case()
        && limits.calls.checked_add(9).is_some()
        && outbox(limits).is_some()
        && limits.agents.checked_mul(3).is_some()
}

pub(crate) fn outbox(limits: &Limits) -> Option<u32> {
    limits.messages.checked_add(limits.calls)?.checked_add(limits.turns)?.checked_add(limits.accounts)?.checked_add(1)
}
