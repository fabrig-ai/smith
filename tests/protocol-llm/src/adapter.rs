//! The protocol world's direct-client setup; production owns only translation.

use skein_lib::Token;
use skein_llm::{Credential, Endpoint, client};
use smith_domain::{llm, run};

pub use smith_protocol_llm::{
    Below, BelowEvent, Component, ComponentError, ComponentLimits, ConfiguredEndpoint, Context, EndpointError,
    EndpointOptions, Endpoints, Error, FromDomain, GrantError, Grants, IdentityProfile, Limits, MAX_OUT, MaxOut,
    Receiving, ResolvedCall, Stated, ToDomain, ToolKind, ToolSchema, cancelled, completion, completion_worst_case,
    component_valid, component_worst_case, decode, decode_deliver, decode_finish, deliver_schema, failed,
    finish_schema, prompt, refusal, render_outcome, render_worst_case, schemas, schemas_for_contract, valid,
    worst_case,
};

/// All owned input to one preparation. Caller credentials are consumed as a grant;
/// acquisition and refresh remain outside Smith.
#[expect(missing_debug_implementations, reason = "input owns caller credential bytes")]
pub struct Input {
    /// Root completion callback owner, echoed exactly by its actual terminal.
    pub owner: Token,

    /// Actual root prompt, consumed without retained borrowed application data.
    pub prompt: llm::Prompt,

    /// Caller-configured neutral endpoint identity; must equal the prompt's identity.
    pub endpoint_name: llm::Endpoint,

    /// Shared Client endpoint configuration, passed unchanged without provider branches.
    pub endpoint: Endpoint,

    /// Actual caller-provided credential; Smith owns no exchange or refresh client.
    pub credential: Credential,

    /// Exact bounded application schema inventory; retained until actual terminal translation.
    pub application: Box<[ToolSchema]>,

    /// Receiving metadata of the root's actual Complete request.
    pub receiving: Receiving,
}

/// One prepared shared Client and its retained application translation context.
/// The caller drives the Client itself; no second client or protocol state machine is created.
#[expect(missing_debug_implementations, reason = "the actual Client holds credential-bearing HTTP state")]
pub struct Prepared {
    /// Actual shared Client, retained through real Reusable or Close/Closed settlement.
    pub client: client::Client,

    /// Owned declarations and receiving contract, consumed by exactly one actual terminal translation.
    pub context: Context,
}

/// Prepare the shared client with the production translator.
///
/// # Errors
/// Refuses incompatible receiving limits or invalid prompt declarations.
pub fn prepare(input: Input, limits: &Limits) -> Result<Prepared, Error> {
    let Input { owner, prompt, endpoint_name, endpoint, credential, application, receiving } = input;
    if prompt.endpoint != endpoint_name {
        return Err(Error::Invalid);
    }
    let ceiling = prompt.max_tokens;
    let (mut translated, context) = smith_protocol_llm::prepare_prompt(owner, prompt, application, receiving, limits)?;
    translated.output_ceiling(endpoint.provider, ceiling).map_err(Error::from_shared)?;
    let client = client::Client::prepare(
        skein_llm::Call { owner, endpoint, credential: Some(credential), prompt: translated },
        &limits.client,
    )
    .map_err(Error::from_shared)?;
    Ok(Prepared { client, context })
}

/// Prepare a client with the production schemas for the admitted contract.
///
/// # Errors
/// Refuses invalid schemas, declarations or receiving limits.
pub fn prepare_for_contract(
    mut input: Input,
    outcome: &run::outcome::OutcomeSpec,
    deliver: Option<&run::outcome::ChangeSpec>,
    limits: &Limits,
) -> Result<Prepared, Error> {
    input.application = schemas_for_contract(&input.prompt, &limits.stated, outcome, deliver, limits.client.request)?;
    prepare(input, limits)
}

/// Effective fixture quantities shared by direct-client tests.
#[must_use]
pub fn stated() -> Stated {
    Stated::from_tools(&crate::LIMITS.session.tools, skein_llm_world::limits().input)
        .expect("fixture numeric bounds fit")
}
