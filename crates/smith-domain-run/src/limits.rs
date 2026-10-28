//! Admission limits and checked capacities. This module retains no runtime state.
//! `valid` checks limit compatibility and representable capacities, without knowing live ownership.
//!
//! Contract: domain/run.md, section 13; programming-model.md, sections 4.4, 4.5 and 6.3.

use skein_lib::Duration;

use crate::budget::Budget;

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
