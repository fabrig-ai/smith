//! Admission limits and checked capacities. This module retains no runtime state.
//! `valid` checks limit compatibility and representable capacities, without knowing live ownership.
//!
//! Contract: domain/session.md, sections 3 and 6; programming-model.md, sections 4.4, 4.5 and 6.3.

use skein_lib::Duration;
use smith_domain_tools as tools;

use crate::boundary::Budget;
use crate::llm::Block;

/// The most tool calls a session runs at once: what `Limits::parallel_tools`
/// may be, and what bounds the requests a step emits.
pub const MAX_PARALLEL: u32 = 8;

/// Allowance for escaping and framing when estimating encoded bytes from the
/// session's retained count; the protocol guard still measures the exact request.
pub const ENCODED_BYTES: [u32; 2] = [5, 4];

/// The session child domain's limits (programming-model.md, sections 4.5 and 6.3), handed by its parent to every
/// step read-only.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    /// Fraction of the model window admitted before compaction.
    pub threshold: [u32; 2],
    /// Most payload bytes per token for the measured appended count.
    pub token_bytes: u32,
    /// Sessions at once. An `Open` beyond them is refused as busy.
    pub sessions: u32,
    /// Largest deployment-unit budget admitted by the parent opening.
    pub spend: u64,
    /// Maximum provider input tokens added beyond the domain request's bytes:
    /// protocol schemas, framing and provider template, configured by the agent.
    pub protocol_allowance: u64,
    /// Messages a session's transcript holds, the spec's prompt included: at
    /// least two, the prompt and an answer.
    pub history_messages: u32,
    /// Blocks retained across the whole window, including its initial prompt.
    pub history_blocks: u32,
    /// Bytes a session holds: its spec, its transcript and the tool results
    /// it is collecting, each block counted at its fixed size plus its payload.
    pub history_bytes: u64,

    /// Maximum owned content of one provider completion, including
    /// block cells, replay envelopes and decoded owned-call fields. The session
    /// reserves this and every possible unstarted result before asking the provider.
    pub completion_bytes: u64,

    /// One completion's joint answer payload, supplied by the service.
    pub answer_bytes: u64,

    /// Maximum blocks in a completion; result-slot storage is
    /// reserved independently before the provider request. The adapter's full
    /// translated completion bound must obey both receiving caps.
    pub completion_blocks: u32,

    /// Maximum exact failure diagnostic bytes. Provider receiving credit
    /// covers this terminal before work; policy drops detail after consumption.
    pub failure_bytes: u32,

    /// Maximum concrete opener-result payload per admitted delegated call.
    /// A whole batch reserves its possible results before any effect; each
    /// terminal owns its reservation through close and turn emission.
    pub delegated_result_bytes: u64,

    /// The largest budget a spec may ask for, dimension by dimension. Its time
    /// is the longest a session may live.
    pub budget: Budget,
    /// Owned tool calls a session runs at once: adjacent calls that read run
    /// together, up to this many, and a call that writes runs alone. Between
    /// one and [`MAX_PARALLEL`].
    pub parallel_tools: u32,
    /// Retries of a call that failed transiently, after which the session
    /// fails.
    pub retries: u32,
    /// The wait before the first retry, doubled for each one after it, with
    /// jitter.
    pub backoff_base: Duration,
    /// The longest wait before a retry, unless the provider asks for longer.
    pub backoff_max: Duration,
    /// Reserved observations, including tools, buffered until the parent drains them.
    pub facts: u32,
    /// The tools child domain's, which the session owns: a kit for each
    /// session, with room for its widest batch.
    pub tools: tools::Limits,
}

/// Logical room secured before each provider request: maximum completion,
/// copied result IDs/details and independent Block/Slot skeleton wrappers.
/// This does not allocate memory or lower the transcript payload ceiling; it
/// prevents a provider effect whose in-cap terminal cannot be retained.
/// It also covers the full bounded failure terminal; diagnostic policy consumes
/// and drops that transient detail. Returns None on zero completion caps or
/// checked arithmetic overflow.
#[must_use]
pub fn completion_reserve(limits: &Limits) -> Option<u64> {
    crate::session::provider_reserve(limits)
}

/// Whether a domain can be built under `limits`: compatible limits and
/// representable constructor capacities (programming-model.md, section 6.3).
/// The shell refuses limits for which this is false.
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let tools_facts = match limits.parallel_tools.checked_add(2) {
        Some(batches) => match batches.checked_mul(tools::max_facts(&limits.tools)) {
            Some(facts) => facts,
            None => return false,
        },
        None => return false,
    };
    limits.tools.facts >= tools_facts
        && limits.facts >= crate::max_facts(limits)
        && (1..=MAX_PARALLEL).contains(&limits.parallel_tools)
        && limits.parallel_tools <= limits.tools.calls
        && limits.history_messages >= 2
        && limits.history_blocks > 0
        && limits.tools.kits >= limits.sessions
        && alarms(limits).is_some()
        && runs(limits).is_some()
        && tools::valid(&limits.tools)
}

/// Two regular batches and one completion's retired program observations per
/// session. A session starts at most one regular batch in a step and rests
/// after immediate answers, so at most two regular batches coexist until the
/// reclaim point. Program observations retire as their stand-ins are made;
/// tooling rests before the next completion, bounding them by one completion.
pub(crate) fn runs(limits: &Limits) -> Option<u32> {
    limits.sessions.checked_mul(limits.parallel_tools.checked_mul(2)?.checked_add(limits.completion_blocks)?)
}

/// The alarm table's capacity: every session may have two alarms armed.
pub(crate) fn alarms(limits: &Limits) -> Option<u32> {
    limits.sessions.checked_mul(2)
}

/// Numeric declarations needed by this layer's transitional derivation.
/// The service supplies only owned numbers and this domain's own child limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derivation {
    pub conversations: u32,
    pub calls_per_response: u32,
    pub parallel_tools: u32,
    pub max_turns: u32,
    pub max_spend: u64,
    pub max_time: Duration,
    pub tools: tools::Limits,
    /// Prompt line in decoded bytes, `TOKEN_BYTES` applied by the service.
    pub line_bytes: u64,
    pub completion_bytes: u64,
    pub answer_bytes: u64,
    pub batch_result_bytes: u64,
    pub delegated_result_bytes: u64,
    pub output_items: u32,
    pub detail: u32,
    /// Receiving ceiling for the actual opening's measured protocol contribution.
    pub protocol_allowance: u64,
    /// Numerator and denominator of the compaction share.
    pub threshold: [u32; 2],
    /// Most payload bytes per token for the measured appended count.
    pub token_bytes: u32,
}

/// The first field this layer cannot derive, returned to its parent before effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    /// Open sessions, derived from conversations.
    Sessions,
    /// Concurrent tools, derived from calls per response.
    ParallelTools,
    /// History bytes after escaped fixed framing and prompt allowance.
    HistoryBytes,
    /// History messages and blocks after the system and tool entries.
    HistoryItems,
    /// Blocks received from one completion.
    CompletionBlocks,
    /// Protocol text added to the session's input reservation.
    Allowance,
}

impl Field {
    /// The field name in the effective limits, without its layer.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sessions => "sessions",
            Self::ParallelTools => "parallel_tools",
            Self::HistoryBytes => "history_bytes",
            Self::HistoryItems => "history_items",
            Self::CompletionBlocks => "completion_blocks",
            Self::Allowance => "protocol_allowance",
        }
    }
}

/// Derive this layer from declarations, refusing checked arithmetic overflow.
/// Contract: protocol/limits.md, sections 2 and 3.
pub fn derive(inputs: &Derivation) -> Result<Limits, Field> {
    if inputs.conversations == 0 {
        return Err(Field::Sessions);
    }
    if inputs.calls_per_response == 0 || !(1..=MAX_PARALLEL).contains(&inputs.parallel_tools) {
        return Err(Field::ParallelTools);
    }
    if inputs.output_items == 0 {
        return Err(Field::CompletionBlocks);
    }
    let mut limits = Limits {
        threshold: inputs.threshold,
        token_bytes: inputs.token_bytes,
        sessions: inputs.conversations,
        spend: inputs.max_spend,
        protocol_allowance: inputs.protocol_allowance,
        history_messages: 0,
        history_blocks: 0,
        history_bytes: 0,
        completion_bytes: inputs.completion_bytes,
        answer_bytes: inputs.answer_bytes,
        completion_blocks: inputs.output_items,
        failure_bytes: inputs.detail,
        delegated_result_bytes: inputs.delegated_result_bytes,
        budget: Budget {
            turns: inputs.max_turns,
            input: 1 << 24,
            output: 1 << 24,
            cache_read: 1 << 26,
            cache_write: 1 << 24,
            time: inputs.max_time,
        },
        retries: 3,
        backoff_base: Duration::from_millis(200),
        backoff_max: Duration::from_secs(5),
        facts: 1024,
        parallel_tools: inputs.parallel_tools.min(inputs.calls_per_response),
        tools: inputs.tools,
    };
    // Rules 1 and 4: the window side bounds decoded history. Receiving beyond
    // one whole completion and one batch coexist before the next window check.
    let reserve = completion_reserve(&limits).ok_or(Field::HistoryBytes)?;
    limits.history_bytes = inputs
        .line_bytes
        .checked_add(reserve)
        .ok_or(Field::HistoryBytes)?
        .checked_add(inputs.batch_result_bytes)
        .ok_or(Field::HistoryBytes)?;
    // Rule 3: preallocated messages and blocks use an in-memory Block cell.
    let cell = u64::try_from(size_of::<Block>()).expect("cell fits u64");
    let items =
        limits.history_bytes.checked_div(cell).expect("nonzero cell").checked_add(2).ok_or(Field::HistoryItems)?;
    let items = u32::try_from(items).or(Err(Field::HistoryItems))?;
    limits.history_messages = items;
    limits.history_blocks = items;
    Ok(limits)
}
