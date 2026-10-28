//! Ownership caps and checked worst-case arithmetic. This module retains no runtime state.
//! `valid` checks limit compatibility and representable capacities, without knowing live ownership.
//! `worst_case` retains the transitional container and payload sum for the parent.
//!
//! Contract: domain/tools.md, section 4; programming-model.md, sections 4.4, 4.5 and 6.3.

use skein_lib::{Duration, Id, List, Map, Queue, Set, Slab};

use crate::authority::Mount;
use crate::call::{Call, Entry, Hit};
use crate::facts::Fact;
use crate::flight::{self, Flight};
use crate::kit::Kit;
use crate::knowledge::Seen;
use crate::path::{Name, Place};

/// The tools child domain's limits (programming-model.md, sections 4.5 and 6.3), handed by its parent to every
/// step read-only.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Kits at once, one per session. An open beyond them is refused as busy.
    pub kits: u32,
    /// Calls a kit runs at once. A call beyond them is answered `Busy`.
    pub calls: u32,
    /// Jobs held by a kit until their end is reported.
    pub jobs: u32,
    /// Repositories an authority may name.
    pub repos: u32,
    /// The longest path the tools take, in bytes, as an absolute path's names
    /// joined by `/`: a path a call names, a mount, the working directory.
    /// A call's spelling before normalisation must also fit, including dots.
    pub path_bytes: u32,
    /// Longest joined absolute host alias, independent of a tool call spelling.
    pub host_path_bytes: u32,
    /// Files a kit remembers the LLM read. Past them, the one read longest
    /// ago is forgotten, and must be read again before a write.
    pub known_files: u32,
    /// The largest file the tools load or store, independently of one piece.
    pub file_bytes: u32,
    /// Maximum content and aggregate replacement snippets in one call, in bytes.
    pub tool_payload: u32,
    /// The most content a read answers with.
    pub read_bytes: u32,
    /// The most entries a listing answers with.
    pub list_entries: u32,

    /// Maximum listing ownership: each entry's fixed size plus its name bytes.
    /// The lower listing returns a path-order prefix within this and `list_entries`,
    /// preserving the count of omitted entries. This also bounds late results.
    pub list_bytes: u64,

    /// The most line numbers an ambiguous edit answers with.
    pub match_lines: u32,
    /// How long a file operation may take, within its call's deadline.
    pub file_timeout: Duration,
    /// The command duration when its call asks for no deadline.
    pub shell_timeout: Duration,
    /// Default duration of a job wait.
    pub job_poll: Duration,
    /// Ceiling of a job wait and a shell yield.
    pub job_wait: Duration,
    /// Maximum duration any tool call may ask for; larger command requests are clamped.
    pub tool_deadline: Duration,
    /// Retained command bytes when the caller asks for no budget.
    pub shell_output: u32,
    /// With `shell_tail`, the capture ceiling and its head/tail proportion.
    pub shell_head: u32,
    /// Last output bytes retained from a shell process.
    pub shell_tail: u32,
    /// The most lines a search answers with, and the most text in them.
    pub search_hits: u32,
    /// Maximum aggregate retained search-path and text bytes.
    pub search_bytes: u32,
    /// How long a search may take, within its call's deadline.
    pub search_timeout: Duration,
    /// Reserved observations buffered until the parent drains them; at least [`crate::max_facts`].
    pub facts: u32,
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let smallest_entry = match u64::try_from(size_of::<Entry>()) {
        Ok(bytes) => match bytes.checked_add(1) {
            Some(bytes) => bytes,
            None => return false,
        },
        Err(_) => return false,
    };
    limits.shell_head.checked_add(limits.shell_tail).is_some()
        && limits.facts >= crate::max_facts(limits)
        && limits.list_bytes >= smallest_entry
        && flight::slots(limits).is_some()
        && limits.path_bytes.checked_add(1).is_some()
        && limits.host_path_bytes.checked_add(1).is_some()
}

/// The most memory the domain holds under `limits`, in bytes (programming-model.md, section 6.3), or `None`
/// if it does not fit a `u64` or the limits cannot be honoured.
///
/// It counts the containers, their bookkeeping included, and the payloads, not
/// allocator overhead. What travels in events and requests is counted by the
/// layer that holds it: the content of a file loaded or to be stored, and the
/// outcomes answered.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    limits.shell_head.checked_add(limits.shell_tail)?;
    if limits.facts < crate::max_facts(limits) {
        return None;
    }
    if !valid(limits) {
        return None;
    }
    let kits = Slab::<Kit>::worst_case(limits.kits)?.checked_add(u64::from(limits.kits).checked_mul(kit(limits)?)?)?;
    // Only running flights hold a place, at most `calls` a kit; the slab has more
    // slots, for the flights answered in an iteration.
    let running = u64::from(limits.kits).checked_mul(u64::from(limits.calls))?;
    // Every write in flight reserves a whole file and its result independently.
    let held = flight::held(limits)?.checked_add(u64::from(limits.file_bytes).checked_mul(2)?)?;
    let flights = Slab::<Flight>::worst_case(flight::slots(limits)?)?.checked_add(running.checked_mul(held)?)?;
    // Facts own nothing beyond their queue.
    let facts = Queue::<Fact>::worst_case(limits.facts)?;
    // One call normalises a path at a time: flags for its raw components,
    // references to those and the working directory, then the joined path
    // and its copy beneath the chosen repository.
    let parts = limits.path_bytes.checked_add(1)? / 2;
    let host_refs = List::<&Name>::worst_case(limits.host_path_bytes.checked_add(1)? / 2)?;
    let scratch = List::<bool>::worst_case(parts)?
        .checked_add(List::<&Name>::worst_case(parts.checked_mul(2)?)?)?
        .checked_add(u64::from(limits.path_bytes).checked_mul(2)?)?;
    // A pure namespace listing borrows and sorts at most one name per mount.
    let names = List::<&[u8]>::worst_case(limits.repos)?;
    // One step recaptures one job answer; retained cancellations are per kit.
    let capture = u64::from(limits.shell_head.checked_add(limits.shell_tail)?).checked_mul(2)?;
    kits.checked_add(flights)?
        .checked_add(facts)?
        .checked_add(scratch)?
        .checked_add(names)?
        .checked_add(host_refs)?
        .checked_add(capture)?
        .checked_add(u64::from(limits.file_bytes).checked_mul(2)?)
}

/// Maximum dynamic ownership of this call's outcome, excluding its
/// enclosing result block and copied provider ID, which the session reserves
/// separately. The parent uses this before dispatch, including for a terminal
/// that wins cancellation; `None` means a configured bound overflowed.
#[must_use]
pub fn result_worst_case(call: &Call, limits: &Limits) -> Option<u64> {
    let result = match call {
        Call::Read { window, .. } => {
            let bytes = match window {
                crate::Budget::Default => limits.read_bytes,
                crate::Budget::Asked(bytes) => (*bytes).min(limits.tool_payload),
            };
            Some(u64::from(bytes))
        }
        Call::List { .. } => Some(limits.list_bytes),
        Call::Search { .. } => {
            let cells = u64::try_from(size_of::<Hit>()).ok()?;
            cells.checked_mul(u64::from(limits.search_hits))?.checked_add(u64::from(limits.search_bytes))
        }
        Call::Write { .. } => Some(0),
        Call::Edit { .. } => {
            let ambiguity_slot_bytes = u64::try_from(size_of::<u32>()).ok()?;
            ambiguity_slot_bytes.checked_mul(u64::from(limits.match_lines))
        }
        Call::Shell { output, .. } | Call::Job { output, .. } => {
            limits.shell_head.checked_add(limits.shell_tail)?;
            let kept = crate::command::kept(*output, limits);
            u64::from(kept.head).checked_add(u64::from(kept.tail))
        }
    }?;
    Some(result.max(u64::from(limits.path_bytes)))
}

/// Largest dynamic result ownership of one legal owned batch, supplied to
/// the parent's startup check. Reads may run together; writes run alone.
/// Returns `None` when any receiving bound overflows.
#[must_use]
pub fn batch_result_worst_case(limits: &Limits, parallel: u32) -> Option<u64> {
    let path = u64::from(limits.path_bytes);
    let search = u64::try_from(size_of::<Hit>())
        .ok()?
        .checked_mul(u64::from(limits.search_hits))?
        .checked_add(u64::from(limits.search_bytes))?;
    let shell = u64::from(limits.shell_head.checked_add(limits.shell_tail)?);
    let read =
        u64::from(limits.read_bytes.max(limits.tool_payload)).max(limits.list_bytes).max(search).max(path).max(shell);
    let edit = u64::try_from(size_of::<u32>()).ok()?.checked_mul(u64::from(limits.match_lines))?;
    Some(read.checked_mul(u64::from(parallel))?.max(edit).max(shell).max(path))
}

/// What one kit holds beyond its slot: its authority, each path in it at most
/// `path_bytes` joined; what its LLM knows, a place for each file; and its
/// flights' names.
fn kit(limits: &Limits) -> Option<u64> {
    let known = u64::from(limits.known_files).checked_mul(u64::from(limits.path_bytes))?;
    let knowledge = Map::<Place, Seen>::worst_case(limits.known_files)?.checked_add(known)?;
    let flights = Set::<Id<Flight>>::worst_case(limits.calls)?;
    authority(limits)?.checked_add(knowledge)?.checked_add(flights)?.checked_add(crate::jobs::worst_case(limits)?)
}

/// What a kit's authority holds, each path in it at most `path_bytes` joined.
fn authority(limits: &Limits) -> Option<u64> {
    let path_bytes = u64::from(limits.path_bytes);
    // A name is at least a byte, and a slash parts it from the next.
    let cwd_names = limits.path_bytes.checked_add(1)? / 2;
    let cwd = List::<Name>::worst_case(cwd_names)?.checked_add(path_bytes)?;
    let mount_bytes = u64::from(limits.repos).checked_mul(path_bytes)?;
    let hosts = u64::from(limits.repos).checked_mul(u64::from(limits.host_path_bytes))?;
    let mounts = List::<Mount>::worst_case(limits.repos)?.checked_add(mount_bytes)?.checked_add(hosts)?;
    cwd.checked_add(mounts)
}

/// Numeric declarations needed by this layer's transitional derivation.
/// The service supplies only owned numbers and this domain's own child limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derivation {
    pub conversations: u32,
    pub calls_per_response: u32,
    pub jobs: u32,
    pub parallel_tools: u32,
    pub tool_payload: u32,
    pub file_bytes: u32,
    pub read_window: u32,
    pub shell_output: u32,
    pub shell_head: u32,
    pub shell_tail: u32,
    pub search_hits: u32,
    pub search_bytes: u32,
    pub list_entries: u32,
    pub tool_deadline: Duration,
    pub shell_timeout: Duration,
    /// Default duration of a job wait.
    pub job_poll: Duration,
    /// Ceiling of a job wait and a shell yield.
    pub job_wait: Duration,
}

/// The first field this layer cannot derive, returned to its parent before effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    /// Concurrent kits, derived from conversations.
    Kits,
    /// Concurrent calls, derived from calls per response.
    Calls,
    /// Listing ownership, derived from a read window and entry cells.
    ListBytes,
    /// The capture ceiling cannot be represented as one byte budget.
    ShellOutput,
}

impl Field {
    /// The field name in the effective limits, without its layer.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Kits => "kits",
            Self::Calls => "calls",
            Self::ListBytes => "list_bytes",
            Self::ShellOutput => "shell_output",
        }
    }
}

/// Derive this layer from declarations, refusing checked arithmetic overflow.
/// Contract: protocol/limits.md, sections 2 and 3.
pub fn derive(inputs: &Derivation) -> Result<Limits, Field> {
    if inputs.conversations == 0 {
        return Err(Field::Kits);
    }
    if inputs.calls_per_response == 0 || inputs.parallel_tools == 0 {
        return Err(Field::Calls);
    }
    inputs.shell_head.checked_add(inputs.shell_tail).ok_or(Field::ShellOutput)?;
    let entry_bytes = u64::try_from(size_of::<Entry>()).or(Err(Field::ListBytes))?;
    let smallest_entry = entry_bytes.checked_add(1).ok_or(Field::ListBytes)?;
    Ok(Limits {
        kits: inputs.conversations,
        calls: inputs.parallel_tools.min(inputs.calls_per_response),
        jobs: inputs.jobs,
        repos: 4,
        path_bytes: 256,
        host_path_bytes: 4096,
        known_files: 256,
        file_bytes: inputs.file_bytes,
        tool_payload: inputs.tool_payload,
        read_bytes: inputs.read_window,
        list_entries: inputs.list_entries,
        // A listing uses one default read window of ownership, with room for
        // at least one entry cell and a nonempty name.
        list_bytes: u64::from(inputs.read_window).max(smallest_entry),
        match_lines: 8,
        file_timeout: Duration::from_secs(30).min(inputs.tool_deadline),
        shell_timeout: inputs.shell_timeout,
        job_poll: inputs.job_poll,
        job_wait: inputs.job_wait,
        tool_deadline: inputs.tool_deadline,
        shell_output: inputs.shell_output,
        shell_head: inputs.shell_head,
        shell_tail: inputs.shell_tail,
        search_hits: inputs.search_hits,
        search_bytes: inputs.search_bytes,
        search_timeout: Duration::from_secs(30).min(inputs.tool_deadline),
        facts: 1024,
    })
}
