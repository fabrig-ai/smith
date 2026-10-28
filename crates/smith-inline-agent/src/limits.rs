//! Checked slot and routing bounds (domain/host.md, sections 9.1 and 9.4;
//! programming-model.md, section 6.3). This module keeps no runtime state.
//! `valid` checks compatibility and constructor capacities without knowing live ownership.

use skein_lib::Duration;
use smith_domain as smith;

/// Startup allowances supplied by the parent, shared by every inline slot.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub slots: u32,
    pub smith: smith::Limits,
    /// Exact concrete turn ownership awaiting the parent's acknowledgements.
    pub window: smith::Window,
    /// Independent wall allowance; progress and waits never pause it.
    pub wall_time: Duration,
    /// Host allowance after cancellation before the slot drops its root.
    pub cancel_grace: Duration,
    /// Legacy lifecycle fact allowance; any value is valid.
    pub facts: u32,
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let Some(turn_bytes) = smith::max_turn_bytes(&limits.smith) else {
        return false;
    };
    limits.window.turns > 0
        && limits.window.bytes >= turn_bytes
        && limits.wall_time != Duration::ZERO
        && limits.smith.session.tools.kits.checked_mul(limits.smith.session.tools.calls).is_some()
        && limits.smith.run.calls.checked_add(limits.smith.run.runs).is_some()
        && smith::valid(&limits.smith)
}
