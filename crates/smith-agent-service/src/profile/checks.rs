//! Startup comparisons retain the first broken relationship and its actual
//! values. No provider wire or operator formatting is known here. Entry points
//! check declarations and composed limits. Contract: protocol/limits.md,
//! sections 4 and 8.

use skein_llm_connection::EndpointError;

use super::{Limit, LimitName};

/// A relationship checked at startup, sent in a typed configuration refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Check {
    /// Every derived limit fits its type.
    Derived,
    /// A fresh window and its output fit the compaction threshold.
    FreshWindow,
    /// The host's part holds the shell's role and every directory's guide.
    HostPartHoldsTheRole,
    /// Compaction's output, instruction and result markers fit above the threshold.
    CompactionHeadroom,
    /// Each model's output is less than its window.
    OutputInWindow,
    /// History reaches compaction before its ownership limits.
    WindowFirst,
    /// The transcript's block limit holds one completion.
    CallsInTranscript,
    /// Resume holds the largest session window.
    ResumeHoldsWindow,
    /// A largest turn fits the transcript vocabulary.
    TurnInTranscript,
    /// A largest turn, start and message fit the channel vocabulary.
    TurnInChannel,
    /// Outcomes at their caps render within one result.
    RenderInResult,
    /// The default read window fits one tool payload.
    ReadInPayload,
    /// One tool piece fits a whole file.
    PayloadInFile,
    /// The machine's bounds hold the tools' bounds.
    MachineHoldsTools,
    /// Environment names are valid and distinct.
    Environment,
    /// The request head holds all configured and credential headers.
    RequestHead,
    /// Policy ceilings fit the charter and channel vocabularies.
    CeilingsInVocabulary,
    /// Deadlines are positive and the shell default fits the tool deadline.
    Deadlines,
    /// Cancellation grace holds IO close and a second close as margin.
    CancelOverClose,
    /// The host's exit grace exceeds the agent's teardown bound.
    GraceOverTeardown,
    /// The host's watchdog exceeds the run's longest silence.
    WatchdogOverSilence,
}

impl Check {
    /// The relationship's name in protocol/limits.md, section 4.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Derived => "derived",
            Self::FreshWindow => "fresh-window",
            Self::HostPartHoldsTheRole => "host-part-holds-the-role",
            Self::CompactionHeadroom => "compaction-headroom",
            Self::OutputInWindow => "output-in-window",
            Self::WindowFirst => "window-first",
            Self::CallsInTranscript => "calls-in-transcript",
            Self::ResumeHoldsWindow => "resume-holds-window",
            Self::TurnInTranscript => "turn-in-transcript",
            Self::TurnInChannel => "turn-in-channel",
            Self::RenderInResult => "render-in-result",
            Self::ReadInPayload => "read-in-payload",
            Self::PayloadInFile => "payload-in-file",
            Self::MachineHoldsTools => "machine-holds-tools",
            Self::Environment => "environment",
            Self::RequestHead => "request-head",
            Self::CeilingsInVocabulary => "ceilings-in-vocabulary",
            Self::Deadlines => "deadlines",
            Self::CancelOverClose => "cancel-over-close",
            Self::GraceOverTeardown => "grace-over-teardown",
            Self::WatchdogOverSilence => "watchdog-over-silence",
        }
    }
}

/// Why startup refuses a configuration, sent to the shell before its channel opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refusal {
    /// A required configured list is empty, sent by startup with its field name.
    Field { limit: LimitName },
    /// A smith relationship fails, retaining both named values for the shell.
    Broken { check: Check, left: Limit, right: Limit },
    /// Derivation overflowed the named field, before any service effects.
    Derived { limit: LimitName },
    /// An environment entry has an invalid or repeated name, identified by position.
    Environment { index: u32 },
    /// The native connection component rejected its configured endpoints or bounds.
    Connection(EndpointError),
    /// Native endpoint derivation refused its declared quantities before startup.
    Llm { endpoint: u32, violation: skein_llm::Violation },
}

impl Refusal {
    /// The smith relationship refused; native connection checks retain their own names.
    #[must_use]
    pub const fn check(&self) -> Option<Check> {
        match self {
            Self::Broken { check, .. } => Some(*check),
            Self::Derived { .. } => Some(Check::Derived),
            Self::Environment { .. } => Some(Check::Environment),
            Self::Field { .. } | Self::Connection(_) | Self::Llm { .. } => None,
        }
    }
}
