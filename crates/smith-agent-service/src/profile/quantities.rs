//! Declared deployment and model quantities retained at startup. This module
//! knows no transport destinations or derived byte limits. `standard` supplies
//! the standard deployment bundle; the service composes derivation separately.
//! Contracts: protocol/limits.md, section 2; protocol/agent.md, section 4.

use alloc::boxed::Box;
use skein_lib::Duration;

/// A named deployment bundle, selected by the shell before startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Name {
    /// The standard deployment and policy bundle.
    Standard,
}

/// An exact share of a window, supplied by the profile to compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fraction {
    pub numerator: u32,
    pub denominator: u32,
}

/// Deployment quantities from which the service derives ownership bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Declared {
    /// Open conversations across a run tree.
    pub conversations: u32,
    /// Bytes exchanged in one tool piece.
    pub tool_payload: u32,
    /// Largest file the tools hold whole, in bytes.
    pub file_bytes: u32,
    /// Tool calls one response may make.
    pub calls_per_response: u32,
    /// Jobs held by one session, including ended jobs not yet read.
    pub jobs: u32,
    /// Default read window, in bytes.
    pub read_window: u32,
    /// Default kept command output, in bytes.
    pub shell_output: u32,
    /// Maximum first command output bytes.
    pub shell_head: u32,
    /// Maximum last command output bytes.
    pub shell_tail: u32,
    /// Maximum retained search matches.
    pub search_hits: u32,
    /// Maximum retained search bytes.
    pub search_bytes: u32,
    /// Maximum retained listing entries.
    pub list_entries: u32,
    /// Whole guide bytes before truncation.
    pub guide: u32,
    /// Shared LLM admission memory, in bytes.
    pub llm_pool: u64,
    /// The process's real memory, in bytes: `standard()` declares 2,181 MiB.
    pub memory: u64,
}

/// Count and duration ceilings selected by the deployment profile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Maximum charter turns.
    pub max_turns: u32,
    /// Maximum charter spend, in the host unit.
    pub max_spend: u64,
    /// Maximum charter wall time.
    pub max_time: Duration,
    /// Maximum charter waiting time.
    pub max_waiting: Duration,
    /// Maximum brief sections.
    pub sections: u32,
    /// Maximum charter host tools.
    pub host_tools: u32,
    /// Maximum outcome verdicts.
    pub verdicts: u32,
    /// Maximum outcome items.
    pub items: u32,
    /// Maximum outcome fields.
    pub fields: u32,
    /// Unread messages per run.
    pub inbox: u32,
    /// Maximum unacknowledged channel turns.
    pub unacknowledged: u32,
    /// Window share at which compaction begins.
    pub compaction_threshold: Fraction,
    /// Adjacent reads per batch, from 1 to 8.
    pub parallel_tools: u32,
    /// Idle connection retention.
    pub connection_keep: Duration,
    /// Maximum requested tool duration.
    pub tool_deadline: Duration,
    /// Default command duration.
    pub shell_timeout: Duration,
    /// Default duration of a job wait.
    pub job_poll: Duration,
    /// Ceiling for job waits and shell yields.
    pub job_wait: Duration,
    /// Check deadline that a change-capable charter's finishing reserve must hold.
    pub check_timeout: Duration,
    /// Maximum time retained for a child's final answer.
    pub wrap_up: Duration,
    /// Duration of each command group stop step.
    pub group_stop: Duration,
    /// IO close deadline.
    pub close: Duration,
    /// Event stream write deadline.
    pub write_deadline: Duration,
    /// Inline agent cancellation grace.
    pub cancel_grace: Duration,
    /// Spawned agent exit grace.
    pub exit_grace: Duration,
    /// Lead before access-token expiry at which the local host refreshes a sign-in record.
    pub refresh_lead: Duration,
    /// Longest hold before projecting live bytes (protocol/limits.md 2.3).
    pub live_interval: Duration,
}

/// The named declarations and policies supplied by the shell to derivation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Profile {
    pub name: Name,
    pub declared: Declared,
    pub policy: Policy,
}

/// One configured model's quantities, sent by the shell and kept until shutdown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub name: Box<[u8]>,
    /// Accepted model effort names, empty for the provider's own validation.
    pub efforts: Box<[smith_domain::run::charter::Effort]>,
    /// Declared usable input tokens.
    pub window: u32,
    /// Declared maximum completion tokens, thinking included.
    pub output: u32,
    /// Largest opaque reasoning item, in bytes.
    pub reasoning_item: u32,
    pub oversized_reasoning: smith_protocol_llm::OversizedReasoning,
    pub head: Duration,
    pub idle: Duration,
}

/// One endpoint's progress deadlines and its declared models, supplied at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    pub number: u32,
    /// The dialect whose limits are derived for this destination.
    pub provider: skein_llm::Provider,
    /// Actual configured option and identity bytes added to Smith's own part.
    pub option_bytes: u64,
    pub connect: Duration,
    pub handshake: Duration,
    pub models: Box<[Model]>,
}

/// Numeric endpoint declarations used by service derivation, without credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    /// The shell's task role bound in bytes, absent for profiles without that role.
    pub role_bound: Option<u32>,
    /// Configured environment encoding: names, values and one equals byte each.
    pub environment_bytes: u32,
    pub endpoints: Box<[Endpoint]>,
}

/// The standard deployment and policy values of shell.md, sections 6.3 and 6.5.
#[must_use]
pub const fn standard() -> Profile {
    Profile {
        name: Name::Standard,
        declared: Declared {
            conversations: 5,
            tool_payload: 40_960,
            file_bytes: 1_048_576,
            calls_per_response: 16,
            jobs: 4,
            read_window: 32_768,
            shell_output: 16_384,
            shell_head: 16_384,
            shell_tail: 49_152,
            search_hits: 100,
            search_bytes: 16_384,
            list_entries: 512,
            guide: 16_384,
            llm_pool: 134_217_728,
            memory: 2_286_944_256,
        },
        policy: Policy {
            max_turns: 4096,
            max_spend: 1_000_000_000,
            max_time: Duration::from_secs(24 * 60 * 60),
            max_waiting: Duration::from_secs(24 * 60 * 60),
            sections: smith_charter::CEILINGS.charter_brief,
            host_tools: smith_charter::CEILINGS.tools_host,
            verdicts: smith_charter::CEILINGS.contract_verdicts,
            items: smith_charter::CEILINGS.run_result_items,
            fields: smith_charter::CEILINGS.text_rule_fields,
            inbox: 64,
            unacknowledged: 64,
            compaction_threshold: Fraction { numerator: 4, denominator: 5 },
            parallel_tools: 4,
            connection_keep: Duration::from_secs(30),
            tool_deadline: Duration::from_secs(20 * 60),
            shell_timeout: Duration::from_secs(2 * 60),
            job_poll: Duration::from_secs(30),
            job_wait: Duration::from_secs(300),
            check_timeout: Duration::from_secs(5 * 60),
            wrap_up: Duration::from_secs(60),
            group_stop: Duration::from_secs(1),
            close: Duration::from_secs(5),
            write_deadline: Duration::from_secs(30),
            cancel_grace: Duration::from_secs(10),
            exit_grace: Duration::from_secs(45),
            refresh_lead: Duration::from_secs(60),
            live_interval: Duration::from_millis(100),
        },
    }
}
