//! Generated contract schema pieces and text widths for startup admission.
//! No receiving policy is retained. `schema_costs` supplies fixed renderer
//! facts; its writer and verifier render the production grammar again.
//! Contract: protocol/limits.md, section 3.9.

use crate::Error;
use smith_domain::run::SchemaCosts;

/// Fixed first/subsequent element costs, including all separators.
/// Numeric digits and escaped host text are summed separately by the run.
pub const fn schema_costs() -> Result<SchemaCosts, Error> {
    Ok(crate::generated_schema_costs::COSTS)
}

#[cfg(test)]
mod renderer;

#[cfg(test)]
mod tests;
