//! Agent-side opening limits (protocol/channel.md, sections 2 and 7).
use core::mem::size_of;
use skein_channel::{Lower, SchemaError};
use skein_lib::{List, Queue};

use crate::{Component, OpenEvent};

/// Codec, framing and queue allowances fixed before the channel opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub bodies: smith_channel::Limits,
    /// Aggregate incoming Start bytes, including its whole saved window.
    pub start_bytes: u32,
    /// This agent's bounded charter decoder allowances.
    pub charter: smith_charter::v4::Limits,
    /// This agent's bounded saved-turn decoder allowances.
    pub transcript: smith_transcript::v6::Limits,
    /// Saved turns admitted on resume, from one window's message capacity.
    pub resume_turns: u32,
    /// Hits admitted within one search outcome on resume.
    pub search_hits: u32,
    pub channel: skein_channel::Limits,
    /// Maximum configured endpoint names kept by this component.
    pub endpoints: u32,
    /// Maximum live host tool and delivery calls awaiting a channel answer.
    pub calls: u32,
    /// Maximum turns retained while durable host acknowledgement is pending.
    pub turns: u32,
    /// Authoritative frame slots kept free by best-effort senders.
    pub fact_reserve_frames: u32,
    /// Authoritative bytes kept free by best-effort senders.
    pub fact_reserve_bytes: u32,
    /// Credential generations retained below the domain; budget two per account.
    pub grants: u32,
}

/// A checked channel cannot be built from the supplied limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// A codec body exceeds the frame range.
    Codec(smith_channel::Overflow),
    /// Skein rejected the schema or framing limits.
    Channel(SchemaError),
    /// Configured endpoint names exceed their count or byte allowance.
    Endpoints,
    /// The accepted result is outside the configured charter codec limits.
    Result(smith_charter::v4::Problem),
    /// A bounded result list had no room for a field or item.
    ResultCapacity,
    /// A saved answer decodes but fails the domain's sealed result bounds.
    InvalidSavedAnswer,
    /// A result or delivery record does not form the concrete typed value.
    InvalidResult,
    /// A concrete turn cannot be written to the durable transcript format.
    Transcript(smith_domain_session::record::Refusal),
    /// A concrete turn exceeds the transcript codec bounds.
    TranscriptBody(smith_transcript::v6::Problem),
    /// The service sent admission or an answer out of order.
    Order,
    /// A live host operation name is repeated, unknown, or beyond capacity.
    Calls,
    /// The host's outstanding turn or byte window has no remaining credit.
    Window,
    /// A credential generation is stale, repeated, or exceeds the table.
    Grants,
    /// The measured result did not fit its output writer.
    ResultWrite(skein_lib::Overflow),
    /// A domain answer exceeds the configured channel body limits.
    Answer(smith_channel::v5::Problem),
    /// A measured application frame could not be built.
    Frame(skein_channel::FrameError),
}

impl From<smith_channel::v5::Problem> for Error {
    fn from(problem: smith_channel::v5::Problem) -> Error {
        Error::Answer(problem)
    }
}

impl From<smith_transcript::v6::Problem> for Error {
    fn from(problem: smith_transcript::v6::Problem) -> Error {
        Error::TranscriptBody(problem)
    }
}

impl From<skein_channel::FrameError> for Error {
    fn from(problem: skein_channel::FrameError) -> Error {
        Error::Frame(problem)
    }
}

impl From<smith_charter::v4::Problem> for Error {
    fn from(problem: smith_charter::v4::Problem) -> Error {
        Error::Result(problem)
    }
}

impl From<skein_lib::Overflow> for Error {
    fn from(problem: skein_lib::Overflow) -> Error {
        Error::ResultWrite(problem)
    }
}

/// Slots reserved by the caller at each entry point during opening.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxOut {
    pub to_domain: u32,
    pub below: u32,
}

/// Opening emits at most one owner event and six lower operations per step.
pub(crate) const MAX_OUT: MaxOut = MaxOut { to_domain: 1, below: 6 };

/// Opening emits at most one owner event and six lower operations per step.
#[must_use]
pub const fn max_out(_limits: &Limits) -> MaxOut {
    MAX_OUT
}

/// Price the machine, the opening event queue and caller output queues.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    if !valid(limits) {
        return None;
    }
    let schema = schema(limits).ok()?;
    let mut duplicate = List::<skein_channel::Version>::worst_case(schema.versions.capacity())?;
    for version in &schema.versions {
        duplicate = duplicate.checked_add(List::<skein_channel::Kind>::worst_case(version.kinds.capacity())?)?;
    }
    let grant_value_bytes = u64::from(limits.bodies.withdrawn_value_id)
        .checked_add(u64::from(limits.bodies.withdrawn_value_bearer))?
        .max(u64::from(limits.bodies.credential_value_bytes))
        .max(u64::from(limits.bodies.program_value_name));
    let bytes = skein_channel::worst_case(&schema, skein_channel::Role::Responder, &limits.channel)?
        .checked_add(duplicate)?
        .checked_add(u64::from(limits.endpoints).checked_mul(u64::try_from(size_of::<crate::Endpoint>()).ok()?)?)?
        .checked_add(u64::from(limits.endpoints).checked_mul(u64::from(smith_charter::CEILINGS.llm_endpoint))?)?
        // Rules 1 and 6: all decoded byte fields share the received frame.
        // Generated zero-payload prices retain every permitted list cell.
        // Input, decoded codec values and translated values can coexist.
        .checked_add(start_cells(&limits.bodies)?.checked_mul(2)?)?
        .checked_add(charter_cells(&limits.charter)?.checked_mul(2)?)?
        // One saved turn decodes at a time. The accumulating concrete turn
        // envelopes are separate from the whole window's shared payload.
        .checked_add(turn_cells(&limits.transcript)?.checked_mul(2)?)?
        .checked_add(List::<smith_domain_session::record::Turn>::worst_case(limits.resume_turns)?)?
        // Retain the first endpoint name while checking later turns, including unknown names.
        .checked_add(u64::from(limits.transcript.turn_endpoint))?
        // Body, decoded Start fields, accumulated translated fields, and
        // the one Charter/Turn currently being decoded. A saved window is
        // decoded sequentially; codec nodes are priced separately above.
        .checked_add(u64::from(limits.start_bytes).checked_mul(4)?)?
        .checked_add(Queue::<skein_channel::Event>::worst_case(8)?)?
        .checked_add(Queue::<OpenEvent>::worst_case(1)?)?
        .checked_add(Queue::<Lower>::worst_case(6)?)?;
    bytes
        .checked_add(skein_lib::Map::<crate::component::CallKey, crate::component::CallRoute>::worst_case(
            limits.calls,
        )?)?
        .checked_add(skein_lib::Map::<u32, u64>::worst_case(limits.turns)?)?
        .checked_add(List::<u32>::worst_case(limits.turns)?)?
        .checked_add(skein_lib::Map::<(u32, u64), crate::GrantValue>::worst_case(limits.grants)?)?
        .checked_add(u64::from(limits.grants).checked_mul(grant_value_bytes)?)?
        .checked_add(u64::try_from(size_of::<Component>()).ok()?)
}

/// Whether codec edges and responder capacities fit (programming-model.md, 6.3).
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    let schema = match schema(limits) {
        Ok(schema) => schema,
        Err(_) => return false,
    };
    // Skein's channel sum also checks these control-frame and queue capacities.
    // Check them explicitly rather than probing that heap sum.
    let terms = match limits.channel.kinds.checked_mul(6) {
        Some(kinds) => kinds.checked_add(4),
        None => return false,
    };
    let open = limits.channel.credential.checked_add(14);
    let control = match terms {
        Some(terms) => match open {
            Some(open) => terms.max(open).max(262),
            None => return false,
        },
        None => return false,
    };
    schema.check(skein_channel::Role::Responder, &limits.channel).is_ok()
        && limits.channel.output_frames.checked_add(3).is_some()
        && control.checked_add(8).is_some()
        && start_cells(&limits.bodies).is_some()
        && charter_cells(&limits.charter).is_some()
        && turn_cells(&limits.transcript).is_some()
}

/// Build this agent's directional schema, using its aggregate Start entrance.
/// Codec field bounds remain independently checked by their decoders.
pub fn schema(limits: &Limits) -> Result<skein_channel::Schema, smith_channel::Overflow> {
    schema_for_start(&limits.bodies, limits.start_bytes)
}

/// Build either channel role with the deployment's joint Start body bound.
/// Codec field ceilings remain independent of this aggregate frame allowance.
pub fn schema_for_start(
    bodies: &smith_channel::Limits,
    start_bytes: u32,
) -> Result<skein_channel::Schema, smith_channel::Overflow> {
    let mut schema = smith_channel::schema(bodies)?;
    for version_index in 0..schema.versions.len() {
        let version = schema.versions.get_mut(version_index).expect("indexed version");
        for kind_index in 0..version.kinds.len() {
            let kind = version.kinds.get_mut(kind_index).expect("indexed kind");
            if kind.kind == 0x0100 {
                if start_bytes == 0 || start_bytes > kind.largest {
                    return Err(smith_channel::Overflow::Frame);
                }
                kind.largest = start_bytes;
            }
        }
    }
    Ok(schema)
}

fn start_cells(limits: &smith_channel::Limits) -> Option<u64> {
    let cells = smith_channel::Limits {
        directory_name: 0,
        directory_path: 0,
        directory_conflicts_item: 0,
        credential_value_bytes: 0,
        program_value_name: 0,
        withdrawn_value_id: 0,
        withdrawn_value_bearer: 0,
        account_refused_code: 0,
        account_refused_reason: 0,
        completion_fault_code: 0,
        completion_fault_request_id: 0,
        completion_fault_field: 0,
        refused_fault_code: 0,
        program_fault_name: 0,
        program_fault_detail: 0,
        receipt_text: 0,
        marker_path: 0,
        delivery_refusal_explanation: 0,
        delivery_failure_diagnostic: 0,
        host_reply_text: 0,
        answered_call_tool: 0,
        message_label: 0,
        message_text: 0,
        start_charter: 0,
        start_transcript: 0,
        host_ask_tool: 0,
        host_ask_input: 0,
        field_name: 0,
        field_text: 0,
        turn_body: 0,
        accepted_result: 0,
        ..*limits
    };
    smith_channel::Start::worst_case_heap(&cells)
}

fn charter_cells(limits: &smith_charter::v4::Limits) -> Option<u64> {
    let cells = smith_charter::v4::Limits {
        section_title: 0,
        section_body: 0,
        host_tool_name: 0,
        host_tool_description: 0,
        host_tool_input: 0,
        field_rule_name: 0,
        item_kind_kind: 0,
        verdict_rule_label: 0,
        conventions_guide: 0,
        conventions_checks: 0,
        llm_endpoint: 0,
        llm_model: 0,
        charter_instructions: 0,
        field_name: 0,
        field_text: 0,
        item_kind: 0,
        run_result_label_some: 0,
        run_result_text: 0,
        ..*limits
    };
    smith_charter::Charter::worst_case_heap(&cells)
}

fn turn_cells(limits: &smith_transcript::v6::Limits) -> Option<u64> {
    let cells = smith_transcript::v6::Limits {
        replay_bytes: 0,
        text_text: 0,
        opaque_bytes: 0,
        call_id: 0,
        call_name: 0,
        call_name_name: 0,
        call_input: 0,
        entry_name: 0,
        hit_path: 0,
        hit_text: 0,
        read_content: 0,
        command_end_head: 0,
        command_end_last: 0,
        command_running_head: 0,
        command_running_last: 0,
        job_read_head: 0,
        job_read_last: 0,
        working_path_path: 0,
        other_path_directory: 0,
        other_path_path: 0,
        outside_path_path: 0,
        field_problem_field: 0,
        said_text: 0,
        tool_result_id: 0,
        turn_endpoint: 0,
        program_name: 0,
        ..*limits
    };
    smith_transcript::Turn::worst_case_heap(&cells)
}
