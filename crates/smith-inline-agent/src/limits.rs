//! Checked slot and routing bounds (domain/host.md, sections 9.1 and 9.4;
//! programming-model.md, section 6.3). This module keeps no runtime state.
//! `valid` checks compatibility and constructor capacities without knowing live ownership.
//! `worst_case` prices each root and every retained routing container;
//! caller-owned emitted payloads remain covered by the root's outgoing bound.
//! Uncapped lower and lifecycle queues are estimated at their former capacities.

use core::mem::size_of;
use skein_lib::{Duration, Id, List, Map, Queue, Slab, Token};
use smith_domain as smith;

use crate::domain::{Domain, Relay, Slot};
use crate::{Fact, Output};

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
    /// Entry estimate for lifecycle facts; any value is valid.
    pub facts: u32,
}

/// Maximum ordered outputs from one entry point, including cleanup fanout.
#[must_use]
pub fn max_out(limits: &Limits) -> u32 {
    output_count(limits).expect("valid limits")
}

fn output_count(limits: &Limits) -> Option<u32> {
    smith::max_out(&limits.smith)
        .checked_add(crate::workspace::call_capacity(limits))?
        .checked_add(crate::workspace::io_capacity(limits))?
        .checked_add(limits.smith.run.runs)?
        .checked_add(limits.smith.run.conversations)?
        .checked_add(4)
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
        && output_count(limits).is_some()
        && smith::valid(&limits.smith)
}

/// Retained roots, configuration, routing, scratch and caller queue ownership.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let outputs = output_count(limits)?;
    let per_slot = smith::worst_case(&limits.smith)?
        .checked_add(Map::<Token, Relay>::worst_case(crate::workspace::call_capacity(limits))?)?
        .checked_add(Map::<Token, bool>::worst_case(limits.smith.run.conversations)?)?
        .checked_add(Map::<Token, bool>::worst_case(crate::workspace::io_capacity(limits))?)?
        .checked_add(Map::<Token, crate::workspace::Machine>::worst_case(limits.smith.run.runs)?)?
        .checked_add(Map::<u32, u64>::worst_case(limits.window.turns)?)?
        .checked_add(Queue::<Token>::worst_case(limits.smith.run.messages)?)?;
    Slab::<Slot>::worst_case(limits.slots)?
        .checked_add(Map::<Token, Id<Slot>>::worst_case(limits.slots)?)?
        .checked_add(u64::from(limits.slots).checked_mul(per_slot)?)?
        .checked_add(Queue::<smith::Request>::worst_case(smith::max_out(&limits.smith))?)?
        .checked_add(Queue::<Output>::worst_case(outputs)?)?
        .checked_add(u64::from(limits.slots).checked_mul(smith_host_domain::Report::BYTES)?)?
        .checked_add(Queue::<Fact>::worst_case(limits.facts)?)?
        .checked_add(List::<Token>::worst_case(limits.slots)?)?
        .checked_add(List::<Token>::worst_case(limits.smith.run.messages)?)?
        .checked_add(List::<Token>::worst_case(crate::workspace::io_capacity(limits))?)?
        .checked_add(List::<Token>::worst_case(limits.smith.run.runs)?)?
        .checked_add(List::<Token>::worst_case(crate::workspace::call_capacity(limits))?)?
        .checked_add(List::<Token>::worst_case(limits.smith.run.conversations)?)?
        .checked_add(List::<smith::run::Message>::worst_case(limits.smith.run.messages)?)?
        .checked_add(List::<smith::run::Directory>::worst_case(limits.smith.run.directories)?)?
        .checked_add(List::<smith::Grant>::worst_case(limits.smith.accounts)?)?
        .checked_add(List::<smith::AnsweredCall>::worst_case(limits.smith.run.answered_calls)?)?
        .checked_add(u64::from(limits.smith.run.answered_calls).checked_mul(smith::run::Delivered::worst_case())?)?
        .checked_add(limits.smith.configured_model_bytes)?
        .checked_add(
            u64::from(limits.smith.endpoints)
                .checked_mul(u64::try_from(size_of::<smith::run::charter::Endpoint>()).ok()?)?,
        )?
        .checked_add(u64::try_from(size_of::<Domain>()).ok()?)
}
