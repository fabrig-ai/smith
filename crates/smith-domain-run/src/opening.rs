//! Main's immutable conversation opening (domain/run.md, sections 3.3 and
//! 3.4; programming-model.md, section 6.2). This module keeps no state and
//! knows no provider dialect, host policy or live clock. `main` copies the
//! admitted charter and preparation findings into the opening value; the
//! run retains its phase, message offer and transcript ownership.

use skein_lib::Duration;
use skein_lib::bytes::copy_of;

use crate::boundary::Opening;
use crate::budget::Spend;
use crate::charter::{Charter, Families, Mode};
use crate::prepare::Found;
use crate::prompt;
use crate::workspace::{self, Workspace};

/// The opening of a run's main conversation, given what the run found in its
/// checkout, what it has spent and the time it has left. What it holds is
/// rendered from the charter or copied: the run keeps the charter (copy at
/// emission).
#[expect(
    clippy::too_many_arguments,
    reason = "one opening copies the admitted charter, identity, findings, budget and schema measurements"
)]
pub(crate) fn main(
    charter: &Charter,
    resumed: bool,
    activation: u64,
    identity: crate::Identity,
    workspace: Option<&Workspace>,
    found: &Found,
    spent: Spend,
    left: Duration,
    limits: &crate::Limits,
) -> Opening {
    let costs = &limits.schema_costs;
    let mut generated_schema_bytes = match charter.mode {
        Mode::Finite => costs
            .finish(&charter.outcome)
            .expect("admitted contract schema arithmetic")
            .checked_add(costs.finish_text)
            .expect("admitted contract schema text arithmetic"),
        Mode::Open => 0,
    };
    if let Some(change) = &charter.grants.deliver {
        generated_schema_bytes = generated_schema_bytes
            .checked_add(costs.deliver(change).expect("admitted delivery schema arithmetic"))
            .expect("admitted schema aggregate")
            .checked_add(costs.deliver_text)
            .expect("admitted schema text aggregate");
    }
    Opening {
        final_turn: None,
        generated_schema_bytes,
        identity,
        ordinal: 0,
        activation,
        transcript: None,
        wait: charter.grants.wait,
        host_tools: charter.grants.host_tools.clone(),
        deliver: charter.grants.deliver.is_some(),
        llm: charter.llm.clone(),
        line: super::charter::line(&charter.llm, limits.compaction_material),
        system: prompt::system(charter, workspace, found, charter.budget.remainder(spent, left), resumed),
        prompt: copy_of(prompt::BEGIN),
        summary: copy_of(prompt::SUMMARY),
        preamble: copy_of(prompt::PREAMBLE),
        tools: workspace::families(workspace, Families::of(&charter.grants)).tools,
        workspace: workspace.cloned(),
        budget: charter.budget.remainder(spent, left),
        finish: match charter.mode {
            Mode::Finite => true,
            Mode::Open => false,
        },
        families: workspace::families(workspace, Families::of(&charter.grants)),
    }
}
