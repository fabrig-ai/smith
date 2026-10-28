//! The effects' parts and routing capacity (protocol/limits.md, section 5).
//! These formulas price the containers the selected constructor builds;
//! root domains, native IO and channels are admitted by their owners.
//! Internal queues are estimated at limits.queue entries each, their backlog watermark.
use super::{Effects, Limits, Operation, Owner, PendingMessage};
use alloc::boxed::Box;
use skein_channel::Lower as ChannelLower;
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::{List, Map, Queue, Slab, Token};
use smith_domain::{self as domain, run};
use smith_protocol_channel as channel;
use smith_protocol_llm as llm;
use smith_protocol_machine as machine;

/// The effects' retained parts, with no domain, channel or native IO.
#[must_use]
pub fn effects_parts(limits: &Limits, endpoints: &llm::Endpoints) -> Option<[(&'static str, u64); 5]> {
    if !super::effects_valid(limits, endpoints) {
        return None;
    }
    let mut parts = lower_parts(limits, endpoints, false)?;
    let routing = (*parts.get(4)?)
        .checked_add(Map::<u32, domain::GrantName>::worst_case(limits.llm.accounts)?)?
        .checked_add(u64::try_from(size_of::<Effects>()).ok()?)?;
    *parts.get_mut(4)? = routing;
    Some([
        ("llm", *parts.first()?),
        ("machine", *parts.get(1)?),
        ("files", *parts.get(2)?),
        ("events", *parts.get(3)?),
        ("effects_routing", *parts.get(4)?),
    ])
}

pub(super) fn parts_worst_case(limits: &Limits, endpoints: &llm::Endpoints, framed: bool) -> Option<u64> {
    let mut total = 0_u64;
    for part in lower_parts(limits, endpoints, framed)? {
        total = total.checked_add(part)?;
    }
    Some(total)
}

fn lower_parts(limits: &Limits, endpoints: &llm::Endpoints, framed: bool) -> Option<[u64; 5]> {
    let queue = limits.queue;
    let channel_cells = if framed { queue } else { 0 };
    let stages = Queue::<channel::OpenEvent>::worst_case(channel_cells)?
        .checked_add(Queue::<ChannelLower>::worst_case(channel_cells)?)?
        .checked_add(Queue::<PendingMessage>::worst_case(channel_cells)?)?
        .checked_add(Queue::<llm::ToDomain>::worst_case(queue)?)?
        .checked_add(Queue::<io::Request>::worst_case(queue)?.checked_mul(2)?)?
        .checked_add(Queue::<machine::ToDomain>::worst_case(queue)?)?
        .checked_add(Queue::<machine::Below>::worst_case(queue)?)?
        .checked_add(Queue::<domain::Event>::worst_case(queue)?)?
        .checked_add(Queue::<domain::Request>::worst_case(queue)?)?
        .checked_add(Queue::<io::Event>::worst_case(queue)?)?
        .checked_add(Queue::<file::Event>::worst_case(queue)?)?
        .checked_add(Queue::<kernel::Submit>::worst_case(queue)?.checked_mul(3)?)?
        .checked_add(Queue::<kernel::Complete>::worst_case(queue)?)?
        .checked_add(Slab::<Owner>::worst_case(limits.routes)?)?
        .checked_add(Map::<Token, Operation>::worst_case(limits.routes)?)?;
    let environment_copies = u64::from(limits.machine.env_bytes / 2)
        .checked_mul(u64::try_from(size_of::<Box<[u8]>>()).ok()?)?
        .checked_add(u64::from(limits.machine.env_bytes))?
        .checked_mul(u64::from(limits.machine.processes))?;
    let pipe_routes = Map::<Token, Token>::worst_case(limits.machine.processes.checked_mul(2)?)?;
    let host_bytes = u64::from(limits.machine.roots).checked_mul(u64::from(limits.domain.run.host_path_bytes))?;
    let roots = List::<run::Directory>::worst_case(limits.machine.roots)?
        .checked_add(host_bytes)?
        .checked_add(List::<Token>::worst_case(limits.machine.roots)?)?;
    let routing = stages
        .checked_add(environment_copies)?
        .checked_add(roots)?
        .checked_add(pipe_routes)?
        .checked_add(u64::from(limits.domain.session.failure_bytes).checked_mul(2)?)?;
    Some([
        llm::component_worst_case(&limits.llm, endpoints)?,
        machine::worst_case(&limits.machine)?,
        file_layer::FileIo::worst_case(limits.file_slots, limits.file_read, limits.file_entries, limits.file_bytes)?,
        match limits.events {
            Some(_) => crate::events::worst_case(limits)?,
            None => 0,
        },
        routing,
    ])
}
