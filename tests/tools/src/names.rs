//! What the tools answered, by name, as the tests count it.

use smith_domain_tools::Outcome;

/// The name of the kind of `outcome`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn kind(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Read { .. } => "read",
        Outcome::Listed { .. } => "listed",
        Outcome::Found { .. } => "found",
        Outcome::Removed => "removed",
        Outcome::NotEmpty => "not empty",
        Outcome::Written { .. } => "written",
        Outcome::Edited { .. } => "edited",
        Outcome::Exited { .. } => "exited",
        Outcome::Running { .. } => "running",
        Outcome::Job { end, .. } => match end {
            smith_domain_tools::JobEnd::Running => "running",
            smith_domain_tools::JobEnd::Ended(_) => "exited",
            smith_domain_tools::JobEnd::Stopped => "stopped",
            smith_domain_tools::JobEnd::Cancelled => "cancelled",
        },
        Outcome::EditFailed { why, .. } => match why {
            smith_domain_tools::Mismatch::NoMatch => "no match",
            smith_domain_tools::Mismatch::Ambiguous { .. } => "ambiguous",
            smith_domain_tools::Mismatch::Unchanged => "unchanged",
        },
        Outcome::NotGranted => "not granted",
        Outcome::Outside { .. } => "outside",
        Outcome::ReadOnly { .. } => "read only",
        Outcome::TooLong => "too long",
        Outcome::NotFound { .. } => "not found",
        Outcome::NotFile => "not a file",
        Outcome::Linked => "linked",
        Outcome::Protected => "protected",
        Outcome::NotDirectory { .. } => "not a directory",
        Outcome::TooLarge { .. } => "too large",
        Outcome::NotRead => "not read",
        Outcome::Stale => "stale",
        Outcome::Failed { .. } => "failed",
        Outcome::TimedOut => "timed out",
        Outcome::Cancelled => "cancelled",
        Outcome::Busy => "busy",
        Outcome::NulByte => "nul byte",
    }
}
