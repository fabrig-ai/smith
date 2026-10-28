//! The startup comparisons that use declared numbers and composed machine
//! limits. No wire destinations or environment names are known here.
//! `declarations` and `machine` return the first failed relationship.
//! Contract: protocol/limits.md, section 4.

use core::mem::size_of;

use skein_lib::Duration;
use smith_domain::tools;
use smith_protocol_machine as machine;

use super::{Check, Configuration, Layer, Limit, LimitName, Per, Profile, Refusal, Value};

/// Check declared relationships before any deriving or provider work.
pub fn declarations(profile: &Profile, configuration: &Configuration) -> Result<(), Refusal> {
    configured_models(configuration)?;
    if !(1..=smith_domain::session::MAX_PARALLEL).contains(&profile.policy.parallel_tools) {
        return Err(Refusal::Field { limit: LimitName::deployment(Layer::Policy, "parallel_tools") });
    }
    at_most(
        Check::ReadInPayload,
        bytes(Layer::Declared, "read_window", u64::from(profile.declared.read_window)),
        bytes(Layer::Declared, "tool_payload", u64::from(profile.declared.tool_payload)),
    )?;
    at_most(
        Check::PayloadInFile,
        bytes(Layer::Declared, "tool_payload", u64::from(profile.declared.tool_payload)),
        bytes(Layer::Declared, "file_bytes", u64::from(profile.declared.file_bytes)),
    )?;
    policy_deadlines(profile)?;
    vocabulary(profile)?;
    let required = profile
        .policy
        .close
        .as_nanos()
        .checked_mul(2)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Policy, "cancel_grace") })?;
    cancel_over_close(Duration::from_nanos(required), profile.policy.cancel_grace)?;
    for endpoint in &configuration.endpoints {
        positive_duration(
            LimitName { layer: Layer::Policy, field: "connect", per: Per::Endpoint(endpoint.number) },
            endpoint.connect,
        )?;
        positive_duration(
            LimitName { layer: Layer::Policy, field: "handshake", per: Per::Endpoint(endpoint.number) },
            endpoint.handshake,
        )?;
        for (index, model) in endpoint.models.iter().enumerate() {
            let per = Per::Model {
                endpoint: endpoint.number,
                model: u32::try_from(index)
                    .or(Err(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "configured_models") }))?,
            };
            let output = LimitName { layer: Layer::Declared, field: "output", per }
                .value(Value::Tokens(u64::from(model.output)));
            let window = LimitName { layer: Layer::Declared, field: "window", per }
                .value(Value::Tokens(u64::from(model.window)));
            if model.output >= model.window {
                return Err(Refusal::Broken { check: Check::OutputInWindow, left: output, right: window });
            }
            positive_duration(LimitName { layer: Layer::Declared, field: "head", per }, model.head)?;
            positive_duration(LimitName { layer: Layer::Declared, field: "idle", per }, model.idle)?;
        }
    }
    Ok(())
}

/// Refuse missing endpoint or model declarations before their bounds are derived.
/// Contract: protocol/agent.md, section 4.
pub fn configured_models(configuration: &Configuration) -> Result<(), Refusal> {
    if configuration.endpoints.is_empty() {
        return Err(Refusal::Field { limit: LimitName::deployment(Layer::Declared, "endpoints") });
    }
    for endpoint in &configuration.endpoints {
        if endpoint.models.is_empty() {
            return Err(Refusal::Field {
                limit: LimitName { layer: Layer::Declared, field: "models", per: Per::Endpoint(endpoint.number) },
            });
        }
    }
    Ok(())
}

// Contract: protocol/limits.md, section 4, `deadlines`.
fn policy_deadlines(profile: &Profile) -> Result<(), Refusal> {
    let policy = profile.policy;
    let values = [
        duration("max_time", policy.max_time),
        duration("max_waiting", policy.max_waiting),
        duration("connection_keep", policy.connection_keep),
        duration("tool_deadline", policy.tool_deadline),
        duration("check_timeout", policy.check_timeout),
        duration("shell_timeout", policy.shell_timeout),
        duration("job_poll", policy.job_poll),
        duration("job_wait", policy.job_wait),
        duration("wrap_up", policy.wrap_up),
        duration("group_stop", policy.group_stop),
        duration("close", policy.close),
        duration("write_deadline", policy.write_deadline),
        duration("cancel_grace", policy.cancel_grace),
        duration("exit_grace", policy.exit_grace),
        duration("refresh_lead", policy.refresh_lead),
        duration("live_interval", policy.live_interval),
    ];
    for value in values {
        match value.value {
            Value::Duration(duration) => positive_duration(value.name, duration)?,
            Value::Bytes(_) | Value::Count(_) | Value::Tokens(_) | Value::Spend(_) | Value::Fraction(_) => {
                unreachable!("deadline table retains durations")
            }
        }
    }
    let teardown_name = LimitName::deployment(Layer::Service, "teardown");
    let teardown = policy
        .close
        .checked_add(policy.close)
        .ok_or(Refusal::Derived { limit: teardown_name })?
        .checked_add(policy.write_deadline)
        .ok_or(Refusal::Derived { limit: teardown_name })?;
    if policy.exit_grace <= teardown {
        return Err(Refusal::Broken {
            check: Check::GraceOverTeardown,
            left: duration("exit_grace", policy.exit_grace),
            right: teardown_name.value(Value::Duration(teardown)),
        });
    }
    if policy.job_poll > policy.job_wait {
        return Err(Refusal::Broken {
            check: Check::Deadlines,
            left: duration("job_poll", policy.job_poll),
            right: duration("job_wait", policy.job_wait),
        });
    }
    if policy.job_wait > policy.tool_deadline {
        return Err(Refusal::Broken {
            check: Check::Deadlines,
            left: duration("job_wait", policy.job_wait),
            right: duration("tool_deadline", policy.tool_deadline),
        });
    }
    if policy.shell_timeout > policy.tool_deadline {
        return Err(Refusal::Broken {
            check: Check::Deadlines,
            left: duration("shell_timeout", policy.shell_timeout),
            right: duration("tool_deadline", policy.tool_deadline),
        });
    }
    Ok(())
}

/// Cancellation grace holds twice IO's close deadline before dropping a run.
pub fn cancel_over_close(required: Duration, available: Duration) -> Result<(), Refusal> {
    if required <= available {
        return Ok(());
    }
    Err(Refusal::Broken {
        check: Check::CancelOverClose,
        left: LimitName::deployment(Layer::Io, "twice_close").value(Value::Duration(required)),
        right: duration("cancel_grace", available),
    })
}

fn positive_duration(name: LimitName, duration: Duration) -> Result<(), Refusal> {
    if duration == Duration::ZERO {
        return Err(Refusal::Broken {
            check: Check::Deadlines,
            left: name.value(Value::Duration(duration)),
            right: LimitName::deployment(Layer::Policy, "deadline_minimum").value(Value::Duration(Duration::ZERO)),
        });
    }
    Ok(())
}

fn vocabulary(profile: &Profile) -> Result<(), Refusal> {
    let ceilings = smith_charter::CEILINGS;
    let policy = profile.policy;
    let checks = [
        CountPair {
            field: "sections",
            vocabulary: "charter_brief",
            value: policy.sections,
            bound: ceilings.charter_brief,
        },
        CountPair {
            field: "host_tools",
            vocabulary: "tools_host",
            value: policy.host_tools,
            bound: ceilings.tools_host,
        },
        CountPair {
            field: "verdicts",
            vocabulary: "contract_verdicts",
            value: policy.verdicts,
            bound: ceilings.contract_verdicts,
        },
        CountPair {
            field: "items",
            vocabulary: "run_result_items",
            value: policy.items,
            bound: ceilings.run_result_items,
        },
        CountPair {
            field: "fields",
            vocabulary: "text_rule_fields",
            value: policy.fields,
            bound: ceilings.text_rule_fields,
        },
        CountPair {
            field: "fields",
            vocabulary: "item_kind_fields",
            value: policy.fields,
            bound: ceilings.item_kind_fields,
        },
        CountPair {
            field: "fields",
            vocabulary: "verdict_rule_fields",
            value: policy.fields,
            bound: ceilings.verdict_rule_fields,
        },
        CountPair {
            field: "fields",
            vocabulary: "change_rule_fields",
            value: policy.fields,
            bound: ceilings.change_rule_fields,
        },
        CountPair { field: "fields", vocabulary: "item_fields", value: policy.fields, bound: ceilings.item_fields },
        CountPair {
            field: "fields",
            vocabulary: "run_result_fields",
            value: policy.fields,
            bound: ceilings.run_result_fields,
        },
    ];
    for pair in checks {
        at_most(
            Check::CeilingsInVocabulary,
            count(Layer::Policy, pair.field, pair.value),
            count(Layer::Channel, pair.vocabulary, pair.bound),
        )?;
    }
    Ok(())
}

struct CountPair {
    vocabulary: &'static str,
    field: &'static str,
    value: u32,
    bound: u32,
}

/// Check that each machine ownership bound holds the matching tools bound.
pub fn machine(machine: &machine::Limits, domain: &smith_domain::Limits) -> Result<(), Refusal> {
    let tools = &domain.session.tools;
    let comparisons = [
        BytePair {
            field: "tool_payload",
            lower: "tool_payload",
            tools: u64::from(tools.tool_payload),
            machine: u64::from(machine.tool_payload),
        },
        BytePair {
            field: "file_bytes",
            lower: "file_bytes",
            tools: u64::from(tools.file_bytes),
            machine: u64::from(machine.file_bytes),
        },
        BytePair {
            field: "path_bytes",
            lower: "path_bytes",
            tools: u64::from(tools.path_bytes),
            machine: u64::from(machine.path_bytes),
        },
        BytePair { field: "list_bytes", lower: "entry_bytes", tools: tools.list_bytes, machine: machine.entry_bytes },
        BytePair {
            field: "shell_head",
            lower: "output_bytes",
            tools: u64::from(tools.shell_head),
            machine: u64::from(machine.output_bytes),
        },
        BytePair {
            field: "shell_tail",
            lower: "output_bytes",
            tools: u64::from(tools.shell_tail),
            machine: u64::from(machine.output_bytes),
        },
        BytePair {
            field: "search_bytes",
            lower: "search_bytes",
            tools: u64::from(tools.search_bytes),
            machine: u64::from(machine.search_bytes),
        },
    ];
    for pair in comparisons {
        at_most(
            Check::MachineHoldsTools,
            bytes(Layer::Tools, pair.field, pair.tools),
            bytes(Layer::Machine, pair.lower, pair.machine),
        )?;
    }
    at_most(
        Check::MachineHoldsTools,
        count(Layer::Tools, "list_entries", tools.list_entries),
        count(Layer::Machine, "entries", machine.entries),
    )?;
    at_most(
        Check::MachineHoldsTools,
        count(Layer::Tools, "search_hits", tools.search_hits),
        count(Layer::Machine, "search_hits", machine.search_hits),
    )?;
    let processes = super::machine_processes(domain)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Machine, "processes") })?;
    at_most(
        Check::MachineHoldsTools,
        count(Layer::Machine, "processes_required", processes),
        count(Layer::Machine, "processes", machine.processes),
    )
}

struct BytePair {
    lower: &'static str,
    field: &'static str,
    tools: u64,
    machine: u64,
}

fn bytes(layer: Layer, field: &'static str, value: u64) -> Limit {
    LimitName::deployment(layer, field).value(Value::Bytes(value))
}

fn count(layer: Layer, field: &'static str, value: u32) -> Limit {
    LimitName::deployment(layer, field).value(Value::Count(u64::from(value)))
}

fn duration(field: &'static str, value: Duration) -> Limit {
    LimitName::deployment(Layer::Policy, field).value(Value::Duration(value))
}

fn at_most(check: Check, left: Limit, right: Limit) -> Result<(), Refusal> {
    let smaller = match left.value {
        Value::Bytes(left_value) => match right.value {
            Value::Bytes(right_value) => left_value <= right_value,
            Value::Count(_) | Value::Tokens(_) | Value::Spend(_) | Value::Duration(_) | Value::Fraction(_) => {
                unreachable!("byte comparisons use the same unit")
            }
        },
        Value::Count(left_value) => match right.value {
            Value::Count(right_value) => left_value <= right_value,
            Value::Bytes(_) | Value::Tokens(_) | Value::Spend(_) | Value::Duration(_) | Value::Fraction(_) => {
                unreachable!("count comparisons use the same unit")
            }
        },
        Value::Tokens(_) | Value::Spend(_) | Value::Duration(_) | Value::Fraction(_) => {
            unreachable!("at_most compares only bytes and counts")
        }
    };
    if smaller { Ok(()) } else { Err(Refusal::Broken { check, left, right }) }
}

/// The transcript vocabulary holds the largest completion or result message.
pub fn calls_in_transcript(required: u32, available: u32) -> Result<(), Refusal> {
    at_most(
        Check::CallsInTranscript,
        LimitName::deployment(Layer::Session, "message_blocks").value(Value::Count(u64::from(required))),
        LimitName::deployment(Layer::Transcript, "message_blocks").value(Value::Count(u64::from(available))),
    )
}

/// The largest turn's bytes fit the transcript vocabulary.
pub fn turn_in_transcript(required: u64, available: u64) -> Result<(), Refusal> {
    at_most(
        Check::TurnInTranscript,
        bytes(Layer::Session, "turn_bytes", required),
        bytes(Layer::Transcript, "turn_bytes", available),
    )
}

/// The resume allowance holds every turn of one window.
pub fn resume_holds_window(required: u64, available: u64) -> Result<(), Refusal> {
    at_most(
        Check::ResumeHoldsWindow,
        LimitName::deployment(Layer::Session, "history_messages").value(Value::Count(required)),
        LimitName::deployment(Layer::Channel, "resume_turns").value(Value::Count(available)),
    )
}

/// A largest turn, start or message fits its channel frame.
pub fn turn_in_channel(required: u64, available: u64) -> Result<(), Refusal> {
    at_most(
        Check::TurnInChannel,
        bytes(Layer::Session, "frame_bytes", required),
        bytes(Layer::Channel, "frame_bytes", available),
    )
}

pub(super) fn transcript_field(field: &'static str, required: Value, available: Value) -> Result<(), Refusal> {
    at_most(
        Check::TurnInTranscript,
        LimitName::deployment(Layer::Session, field).value(required),
        LimitName::deployment(Layer::Transcript, field).value(available),
    )
}

/// The actual configured head, with credential and affinity maxima, fits one transport piece.
/// Startup supplies the native measurement before constructing the connection component.
pub fn request_head(endpoint: u32, required: u32, available: u32) -> Result<(), Refusal> {
    at_most(
        Check::RequestHead,
        LimitName { layer: Layer::Llm, field: "request_head", per: Per::Endpoint(endpoint) }
            .value(Value::Bytes(u64::from(required))),
        LimitName { layer: Layer::Llm, field: "request_piece", per: Per::Endpoint(endpoint) }
            .value(Value::Bytes(u64::from(available))),
    )
}

/// Every model reaches its threshold while a provider terminal and its largest
/// owned or delegated result batch still fit the history.
/// Byte-as-token pricing includes the input's fixed cells, so the same threshold
/// divided by each smallest cell bounds the represented message and block counts.
pub fn window_first(
    profile: &Profile,
    configuration: &Configuration,
    limits: &smith_domain::session::Limits,
) -> Result<(), Refusal> {
    let reserve = smith_domain::session::completion_reserve(limits)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "completion_reserve") })?;
    let owned = tools::batch_result_worst_case(&limits.tools, limits.parallel_tools)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "batch_results") })?;
    let delegated = limits
        .delegated_result_bytes
        .checked_mul(u64::from(limits.parallel_tools))
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "batch_results") })?;
    // The completion owns its result skeletons, but their dynamic payloads
    // reserve separate credit immediately before the first batch's effects.
    let reserve = reserve
        .checked_add(owned.max(delegated))
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Session, "window_receiving") })?;
    let material =
        super::room::compaction_material(profile.declared.calls_per_response, smith_domain::run::summary_bytes())
            .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Run, "compaction_material") })?;
    for endpoint in &configuration.endpoints {
        for (index, model) in endpoint.models.iter().enumerate() {
            let per = Per::Model { endpoint: endpoint.number, model: u32::try_from(index).expect("bounded models") };
            let refusal = Refusal::Derived { limit: LimitName { layer: Layer::Session, field: "line", per } };
            let line_bytes = u64::from(model.window)
                .checked_sub(u64::from(model.output))
                .ok_or(refusal)?
                .checked_sub(material)
                .ok_or(refusal)?
                .checked_mul(u64::from(skein_llm::TOKEN_BYTES))
                .ok_or(refusal)?;
            let bytes = line_bytes
                .checked_add(reserve)
                .ok_or(Refusal::Derived { limit: LimitName { layer: Layer::Session, field: "history_bytes", per } })?;
            at_most(
                Check::WindowFirst,
                LimitName { layer: Layer::Session, field: "line", per }.value(Value::Bytes(bytes)),
                LimitName { layer: Layer::Session, field: "history_bytes", per }
                    .value(Value::Bytes(limits.history_bytes)),
            )?;
            let messages = line_bytes
                .checked_div(u64::try_from(size_of::<smith_domain::session::llm::Block>()).expect("cell fits"))
                .expect("nonzero cell")
                .checked_add(2)
                .ok_or(Refusal::Derived {
                    limit: LimitName { layer: Layer::Session, field: "history_messages", per },
                })?;
            let blocks = line_bytes
                .checked_div(u64::try_from(size_of::<smith_domain::session::llm::Block>()).expect("cell fits"))
                .expect("nonzero cell")
                .checked_add(u64::from(limits.completion_blocks).checked_mul(2).expect("u32 twice fits u64"))
                .ok_or(Refusal::Derived { limit: LimitName { layer: Layer::Session, field: "history_blocks", per } })?;
            at_most(
                Check::WindowFirst,
                LimitName { layer: Layer::Session, field: "window_messages", per }.value(Value::Count(messages)),
                LimitName { layer: Layer::Session, field: "history_messages", per }
                    .value(Value::Count(u64::from(limits.history_messages))),
            )?;
            at_most(
                Check::WindowFirst,
                LimitName { layer: Layer::Session, field: "window_blocks", per }.value(Value::Count(blocks)),
                LimitName { layer: Layer::Session, field: "history_blocks", per }
                    .value(Value::Count(u64::from(limits.history_blocks))),
            )?;
        }
    }
    Ok(())
}

/// Capped outcomes render whole; the marker remains an independent safeguard.
pub fn render_in_result(required: u32, available: u32) -> Result<(), Refusal> {
    at_most(
        Check::RenderInResult,
        bytes(Layer::Render, "uncut_result", u64::from(required)),
        bytes(Layer::Render, "result", u64::from(available)),
    )
}

/// The shell's role and its one whole guide fit the shared host part for every model.
pub fn host_part_holds_the_role(
    profile: &Profile,
    configuration: &Configuration,
    limits: &smith_domain::run::Limits,
) -> Result<(), Refusal> {
    let Some(role_bound) = configuration.role_bound else { return Ok(()) };
    let required =
        smith_domain::run::role_host_bytes(role_bound, 1, limits.directory_name_bytes, profile.declared.guide)
            .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Run, "role_and_guides") })?;
    at_most(
        Check::HostPartHoldsTheRole,
        bytes(Layer::Run, "role_and_guides", required),
        bytes(Layer::Run, "host_part", limits.host_part),
    )
}
