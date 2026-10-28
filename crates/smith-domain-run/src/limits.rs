//! Ownership caps and checked worst-case arithmetic. This module retains no runtime state.
//! `valid` checks limit compatibility and representable capacities, without knowing live ownership.
//! `worst_case` retains the transitional container and payload sum for the parent.
//!
//! Contract: domain/run.md, section 13; programming-model.md, sections 4.4, 4.5 and 6.3.

use skein_lib::{Deadlines, Duration, List, Queue, Slab};

use crate::budget::Budget;
use crate::call::Calls;
use crate::facts::Fact;
use crate::prepare::Guide;
use crate::run::{Alarm, Conversation, Run};

/// The run child domain's limits (programming-model.md, sections 4.5 and 6.3), handed by its parent to every
/// step read-only.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Live FIFO messages per admitted run. Full admission bounces before retention.
    pub messages: u32,

    /// Maximum attested labelled text bytes retained per queued message.
    pub message_bytes: u32,

    /// Maximum messages offered together at one main yield.
    pub offer_messages: u32,

    /// Rendered bytes in one offer, including the two-byte separators between messages.
    pub offer_bytes: u32,

    /// Maximum positive charter idle interval; it never pauses the wall budget.
    pub waiting: Duration,

    /// Maximum host operation duration after checked submission. It must
    /// be positive for a delivery-capable charter; caller expiry may be earlier.
    /// The host supplies the bounded host terminal even after run shutdown.
    pub delivery_timeout: Duration,
    /// Runs at once. A start beyond them is refused as busy.
    pub runs: u32,
    /// Conversations at once, across runs. A start with no room for its main
    /// conversation is refused as busy.
    pub conversations: u32,
    /// Bytes a run holds beyond inline slab fields: its charter and separate
    /// workspace, including Brief cells, instructions, title/text payloads and every
    /// Directory cell, name, conflict Box cell and path.
    pub run_bytes: u64,

    /// Host-written main Brief sections. Exact count is checked before section
    /// traversal, rendering or effects; excess refuses as `Invalid::TooLarge`.
    /// Cells, instructions and all title/text payloads also fit `run_bytes`.
    pub brief_sections: u32,
    /// Rendered charter text, generated schemas and guides admitted at entrance.
    pub host_part: u64,
    /// Minimum threshold less two outputs admitted for every charter model.
    pub room: u64,
    /// The window threshold numerator and denominator supplied by startup.
    pub compaction_threshold: [u32; 2],
    /// Summary instruction and owed-result markers above each conversation's line.
    pub compaction_material: u64,
    /// Exact protocol renderer projections, measured at startup.
    pub schema_costs: crate::SchemaCosts,
    /// Directories a present workspace may list; empty present workspaces refuse.
    pub directories: u32,

    /// Maximum opaque safe mount component bytes, checked before pairwise admission.
    pub directory_name_bytes: u32,

    /// Longest absolute host path retained for workspace aliases.
    pub host_path_bytes: u32,

    /// Maximum initial conflict paths per git directory, checked before comparisons.
    pub conflicts: u32,

    /// Maximum relative conflict path bytes, at most `Marker::CAPACITY` (4096).
    pub conflict_path_bytes: u32,
    /// Host tool declarations a charter may grant.
    pub host_tools: u32,

    /// Maximum immutable protocol-attested host input bytes per call, at most the vocabulary cap.
    pub host_input_bytes: u32,

    /// Maximum host answer text bytes accepted and retained per call.
    pub host_reply_bytes: u32,

    /// Maximum host answers described in a resumed run's first prompt.
    pub answered_calls: u32,
    /// Maximum bytes of their rendered text in that prompt.
    pub answered_bytes: u32,

    /// Default host-relay allowance; a charter may declare another within the maximum.
    pub host_timeout: Duration,

    /// Maximum host-relay allowance, also bounded by caller and run expiry.
    pub host_timeout_max: Duration,

    /// Positive deterministic backoff between settled retryable relays.
    pub host_backoff: Duration,

    /// Verdicts an outcome spec may list.
    pub verdicts: u32,
    /// Calls of conversations to the run in flight at once, across runs, each
    /// with an alarm for its deadline. A call beyond them is answered as busy.
    pub calls: u32,
    /// The largest budget a charter may ask for, part by part.
    pub budget: Budget,
    /// The LLMs a charter may list for sub-agents.
    pub models: u32,
    /// Conversations a run may have at once, main included.
    pub run_conversations: u32,
    /// The most bytes of a sub-agent's last message its asker is given.
    pub answer_bytes: u32,
    /// Nudges a run gives its LLM when it stops without finishing, after which
    /// the run fails.
    pub nudges: u32,
    /// The most bytes of a repository's selected guide a run reads and puts in
    /// its system text.
    pub guide_bytes: u32,
    /// How long io has for each look in the checkout.
    pub io_timeout: Duration,
    /// Largest aggregate result ownership beyond its inline declaration,
    /// counting every field/item container, name and value as
    /// `outcome::owned_bytes` does. A larger finish is feedback, before
    /// shape judgement or any checks/Delivery.
    pub outcome_bytes: u64,
    /// How long a repository's checks may run.
    pub check_timeout: Duration,
    /// Maximum time kept for a child's final answer, capped at a quarter of its share.
    pub wrap_up: Duration,
    /// The most bytes of a failed check's output the LLM is shown: its tail.
    pub check_tail: u32,
    /// Reserved observations buffered until the parent drains them; at least [`crate::max_facts`].
    pub facts: u32,
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let facts = match limits.messages.checked_mul(2) {
        Some(messages) => match messages.checked_add(8) {
            Some(facts) => facts,
            None => return false,
        },
        None => return false,
    };
    limits.facts >= facts
        && limits.offer_messages > 0
        && limits.offer_bytes >= limits.message_bytes
        && u64::from(limits.conflict_path_bytes)
            <= u64::try_from(crate::Marker::CAPACITY).expect("marker capacity fits u64")
        && alarms(limits).is_some()
}

/// The most memory the domain holds under `limits`, in bytes (programming-model.md, section 6.3), or `None`
/// if it does not fit a `u64`.
///
/// It counts the containers, their bookkeeping included, and the payloads, not
/// allocator overhead. What a run sends is a copy, which its receiver counts;
/// what it receives and only passes on (a check's output) is the sender's.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let runs = Slab::<Run>::worst_case(limits.runs)?;
    let ready = crate::wind_down::Ready::worst_case(limits.runs)?;
    let conversations = Slab::<Conversation>::worst_case(limits.conversations)?;
    // Two deadlines per run and two per call.
    let alarms = Deadlines::<Alarm>::worst_case(alarms(limits)?)?;
    // Each run holds its charter, including both convention path payloads, up
    // to its byte limit (inline path wrappers are in Slab<Run>), and what it found in
    // its checkout: a guide and a mark for checks per repository.
    // Guide payloads are bounded by both their per-directory caps and the
    // host part shared after the rendered charter; the list has its own cap
    // (protocol/limits.md, sections 3.9 and 5).
    let guide_bytes = u64::from(limits.directories).checked_mul(u64::from(limits.guide_bytes))?;
    let guides = List::<Guide>::worst_case(limits.directories)?.checked_add(guide_bytes.min(limits.host_part))?;
    let checks = List::<u32>::worst_case(limits.directories)?;
    // A winding run holds the outcome it accepted.
    let run = limits
        .run_bytes
        .checked_add(skein_lib::Set::<skein_lib::Id<Conversation>>::worst_case(
            limits.run_conversations.saturating_sub(1),
        )?)?
        .checked_add(Queue::<crate::inbox::Queued>::worst_case(limits.messages)?)?
        .checked_add(Queue::<skein_lib::Token>::worst_case(limits.messages)?)?
        .checked_add(u64::from(limits.offer_bytes))?
        .checked_add(u64::from(limits.messages).checked_mul(u64::from(limits.message_bytes))?)?
        .checked_add(guides)?
        .checked_add(checks)?
        .checked_add(limits.outcome_bytes.max(crate::Delivered::worst_case()).max(crate::Report::BYTES))?;
    let held = u64::from(limits.runs).checked_mul(run)?;
    // A call landing a change holds it; a sub-agent's call, its answer.
    let host = limits.run_bytes.checked_add(u64::from(limits.host_input_bytes))?;
    let opening = limits
        .host_part
        .checked_add(limits.run_bytes)?
        .checked_add(crate::prompt::budget_prompt_bytes()?)?
        .checked_add(u64::try_from(size_of::<crate::Opening>()).ok()?)?
        .checked_add(crate::prompt::BEGIN.len().try_into().ok()?)?
        .checked_add(crate::prompt::SUMMARY.len().try_into().ok()?)?
        .checked_add(crate::prompt::PREAMBLE.len().try_into().ok()?)?
        .checked_add(crate::prompt::CHILD_FINAL.len().try_into().ok()?)?;
    let call = limits.outcome_bytes.max(u64::from(limits.answer_bytes)).max(host).max(crate::Report::BYTES);
    // The lease admits one writing child per run. Only that child's call
    // retains an opening while its parent's jobs settle; other calls keep
    // their ordinary payload. Replace one payload per run by the larger one.
    let waiting = u64::from(limits.runs.min(limits.calls)).checked_mul(opening.saturating_sub(call))?;
    let calls = Calls::worst_case(limits.calls)?.checked_add(u64::from(limits.calls).checked_mul(call)?)?;
    let facts = Queue::<Fact>::worst_case(limits.facts)?;
    runs.checked_add(ready)?
        .checked_add(conversations)?
        .checked_add(alarms)?
        .checked_add(held)?
        .checked_add(calls)?
        .checked_add(waiting)?
        .checked_add(facts)
}

/// Two alarms per run and two per delegated call.
pub(crate) fn alarms(limits: &Limits) -> Option<u32> {
    limits.runs.checked_mul(2)?.checked_add(limits.calls.checked_mul(2)?)
}

/// Numeric declarations needed by this layer's transitional derivation.
/// The service supplies only owned numbers and this domain's own child limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derivation {
    pub conversations: u32,
    pub calls_per_response: u32,
    pub inbox: u32,
    pub max_waiting: Duration,
    pub max_turns: u32,
    pub max_spend: u64,
    pub max_time: Duration,
    pub check_timeout: Duration,
    pub wrap_up: Duration,
    pub sections: u32,
    pub host_tools: u32,
    pub verdicts: u32,
    pub guide: u32,
    pub shell_tail: u32,
    pub tool_payload: u32,
    pub tool_deadline: Duration,
    pub host: u64,
    pub offer: u64,
    pub message: u32,
    pub room: u64,
    pub compaction_threshold: [u32; 2],
    /// Summary instruction and owed-result markers above each conversation's line.
    pub compaction_material: u64,
    pub schema_costs: crate::SchemaCosts,
}

/// The first field this layer cannot derive, returned to its parent before effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    /// One offer, derived from the inbox and message bound.
    OfferBytes,
}

impl Field {
    /// The field name in the effective limits, without its layer.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::OfferBytes => "offer_bytes",
        }
    }
}

/// Derive this layer from declarations, refusing checked arithmetic overflow.
/// Contract: protocol/limits.md, sections 2 and 3.
pub fn derive(inputs: &Derivation) -> Result<Limits, Field> {
    let message_bytes = inputs.message;
    let offer_bytes = u32::try_from(inputs.offer).or(Err(Field::OfferBytes))?;
    Ok(Limits {
        runs: 1,
        conversations: inputs.conversations,
        run_bytes: 1 << 16,
        brief_sections: inputs.sections,
        host_part: inputs.host,
        room: inputs.room,
        compaction_threshold: inputs.compaction_threshold,
        compaction_material: inputs.compaction_material,
        schema_costs: inputs.schema_costs,
        directories: 4,
        directory_name_bytes: 256,
        host_path_bytes: 4096,
        conflicts: 64,
        conflict_path_bytes: 4096,
        host_tools: inputs.host_tools,
        host_input_bytes: inputs.tool_payload,
        host_reply_bytes: inputs.tool_payload,
        answered_calls: 16,
        answered_bytes: 4096,
        host_timeout: Duration::from_secs(60),
        host_timeout_max: inputs.tool_deadline,
        host_backoff: Duration::from_millis(50),
        verdicts: inputs.verdicts,
        calls: inputs.calls_per_response,
        budget: Budget { turns: inputs.max_turns, spend: inputs.max_spend, time: inputs.max_time },
        models: 2,
        run_conversations: inputs.conversations,
        answer_bytes: 1024,
        nudges: 1,
        guide_bytes: inputs.guide,
        io_timeout: Duration::from_secs(5),
        outcome_bytes: 4096,
        delivery_timeout: Duration::from_secs(60),
        check_timeout: inputs.check_timeout,
        wrap_up: inputs.wrap_up,
        check_tail: inputs.shell_tail,
        facts: 1024,
        messages: inputs.inbox,
        message_bytes,
        offer_messages: inputs.inbox,
        offer_bytes,
        waiting: inputs.max_waiting,
    })
}
