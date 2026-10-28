//! The product's effective inventory extends the agent's names with its host layers.
//! It retains no configuration. Contract: protocol/limits.md, section 9.

use alloc::boxed::Box;
use skein_lib::{Duration, List};
use smith_agent_service::profile::{Configuration, Layer, Limit, LimitName, Profile, Value};

mod channel;

/// Keep the selected host bounds, adding its result file to the product's teardown.
#[must_use]
pub fn effective(
    profile: &Profile,
    declarations: &Configuration,
    configuration: &smith_agent_service::Config,
    limits: &crate::Limits,
    result_file: Duration,
) -> Option<Box<[Limit]>> {
    let mut agent = smith_agent_service::profile::effective(
        profile,
        declarations,
        &configuration.limits,
        &configuration.llm_endpoints,
    )?;
    let teardown = smith_agent_service::profile::teardown_bound(&configuration.limits)?.checked_add(result_file)?;
    for limit in &mut agent {
        if limit.name == LimitName::deployment(Layer::Service, "teardown") {
            limit.value = Value::Duration(teardown);
        }
    }
    let local = local_domain(&limits.local);
    let selected = match limits.local.agent {
        smith_local_domain::AgentLimits::Inline(bounds) => inline_domain(&bounds),
        smith_local_domain::AgentLimits::Spawned(bounds) => host_domain(&bounds),
    };
    let process = match limits.local.agent {
        smith_local_domain::AgentLimits::Inline(_) => Box::from([]),
        smith_local_domain::AgentLimits::Spawned(_) => process(&limits.process),
    };
    let service = [
        entry(Layer::Local, "service.envelope_bytes", Value::Bytes(limits.envelope_bytes)),
        entry(Layer::Local, "service.queue", Value::Count(u64::from(limits.queue))),
        entry(Layer::Local, "service.watermark", Value::Count(u64::from(limits.watermark))),
    ];
    let (bodies, transcript) = match limits.local.agent {
        smith_local_domain::AgentLimits::Inline(_) => (Box::from([]), Box::from([])),
        smith_local_domain::AgentLimits::Spawned(_) => {
            (channel::bodies(&limits.process.channel.bodies), channel::transcript(&limits.process.channel.transcript))
        }
    };
    let count = agent
        .len()
        .checked_add(local.len())?
        .checked_add(selected.len())?
        .checked_add(process.len())?
        .checked_add(bodies.len())?
        .checked_add(transcript.len())?
        .checked_add(service.len())?;
    let mut result = List::with_capacity(u32::try_from(count).ok()?);
    for slice in [
        agent.as_ref(),
        local.as_ref(),
        selected.as_ref(),
        process.as_ref(),
        bodies.as_ref(),
        transcript.as_ref(),
        service.as_slice(),
    ] {
        for value in slice {
            result.push(*value).ok()?;
        }
    }
    Some(result.into_boxed())
}

fn entry(layer: Layer, field: &'static str, value: Value) -> Limit {
    LimitName::deployment(layer, field).value(value)
}

fn local_domain(limits: &smith_local_domain::Limits) -> Box<[Limit]> {
    let smith_local_domain::Limits {
        smith: _,
        agent: _,
        endpoints: _,
        chat_bytes,
        text_bytes,
        models,
        line_bytes,
        show_bytes,
        unsaved,
        facts,
    } = *limits;
    Box::new([
        entry(Layer::Local, "chat_bytes", Value::Bytes(u64::from(chat_bytes))),
        entry(Layer::Local, "text_bytes", Value::Bytes(u64::from(text_bytes))),
        entry(Layer::Local, "models", Value::Count(u64::from(models))),
        entry(Layer::Local, "line_bytes", Value::Bytes(u64::from(line_bytes))),
        entry(Layer::Local, "show_bytes", Value::Bytes(u64::from(show_bytes))),
        entry(Layer::Local, "unsaved", Value::Count(u64::from(unsaved))),
        entry(Layer::Local, "facts", Value::Count(u64::from(facts))),
    ])
}

fn inline_domain(limits: &smith_inline_agent::Limits) -> Box<[Limit]> {
    let smith_inline_agent::Limits { slots, smith: _, window: _, wall_time, cancel_grace, facts } = *limits;
    Box::new([
        entry(Layer::Inline, "slots", Value::Count(u64::from(slots))),
        entry(Layer::Inline, "wall_time", Value::Duration(wall_time)),
        entry(Layer::Inline, "cancel_grace", Value::Duration(cancel_grace)),
        entry(Layer::Inline, "facts", Value::Count(u64::from(facts))),
        entry(Layer::Inline, "window.turns", Value::Count(u64::from(limits.window.turns))),
        entry(Layer::Inline, "window.bytes", Value::Bytes(limits.window.bytes)),
    ])
}

fn host_domain(limits: &smith_host_domain::Limits) -> Box<[Limit]> {
    let smith_host_domain::Limits {
        agents,
        directories,
        conflicts,
        path_bytes,
        name_bytes,
        accounts,
        charter_bytes,
        transcript_bytes,
        answered_bytes,
        message_bytes,
        messages,
        calls,
        call_bytes,
        answer_bytes,
        turns,
        turn_bytes,
        unacknowledged_bytes,
        fact_bytes,
        outcome_bytes,
        detail_bytes,
        spawn_timeout,
        no_progress,
        silence,
        long_span,
        long_open,
        wall_time,
        cancel_grace,
        exit_grace,
        teardown,
        kill_after,
        facts,
    } = *limits;
    Box::new([
        entry(Layer::Host, "agents", Value::Count(u64::from(agents))),
        entry(Layer::Host, "directories", Value::Count(u64::from(directories))),
        entry(Layer::Host, "conflicts", Value::Count(u64::from(conflicts))),
        entry(Layer::Host, "path_bytes", Value::Bytes(u64::from(path_bytes))),
        entry(Layer::Host, "name_bytes", Value::Bytes(u64::from(name_bytes))),
        entry(Layer::Host, "accounts", Value::Count(u64::from(accounts))),
        entry(Layer::Host, "charter_bytes", Value::Bytes(charter_bytes)),
        entry(Layer::Host, "transcript_bytes", Value::Bytes(transcript_bytes)),
        entry(Layer::Host, "answered_bytes", Value::Bytes(answered_bytes)),
        entry(Layer::Host, "message_bytes", Value::Bytes(message_bytes)),
        entry(Layer::Host, "messages", Value::Count(u64::from(messages))),
        entry(Layer::Host, "calls", Value::Count(u64::from(calls))),
        entry(Layer::Host, "call_bytes", Value::Bytes(call_bytes)),
        entry(Layer::Host, "answer_bytes", Value::Bytes(answer_bytes)),
        entry(Layer::Host, "turns", Value::Count(u64::from(turns))),
        entry(Layer::Host, "turn_bytes", Value::Bytes(turn_bytes)),
        entry(Layer::Host, "unacknowledged_bytes", Value::Bytes(unacknowledged_bytes)),
        entry(Layer::Host, "fact_bytes", Value::Bytes(fact_bytes)),
        entry(Layer::Host, "outcome_bytes", Value::Bytes(outcome_bytes)),
        entry(Layer::Host, "detail_bytes", Value::Bytes(u64::from(detail_bytes))),
        entry(Layer::Host, "spawn_timeout", Value::Duration(spawn_timeout)),
        entry(Layer::Host, "no_progress", Value::Duration(no_progress)),
        entry(Layer::Host, "silence", Value::Duration(silence)),
        entry(Layer::Host, "long_span", Value::Duration(long_span)),
        entry(Layer::Host, "long_open", Value::Count(u64::from(long_open))),
        entry(Layer::Host, "wall_time", Value::Duration(wall_time)),
        entry(Layer::Host, "cancel_grace", Value::Duration(cancel_grace)),
        entry(Layer::Host, "exit_grace", Value::Duration(exit_grace)),
        entry(Layer::Host, "teardown", Value::Duration(teardown)),
        entry(Layer::Host, "kill_after", Value::Duration(kill_after)),
        entry(Layer::Host, "facts", Value::Count(u64::from(facts))),
    ])
}

fn process(limits: &crate::ProcessLimits) -> Box<[Limit]> {
    let crate::ProcessLimits { io, channel, detail_bytes, queue } = *limits;
    let skein_io::Limits {
        sockets,
        names: _,
        refusals,
        intake,
        receive,
        output,
        sends,
        accepts,
        backlog,
        close_timeout,
        retry,
    } = io;
    let smith_host_protocol::Limits { bodies: _, charter: _, transcript: _, endpoints, channel, start_bytes, calls } =
        channel;
    let skein_channel::Limits { chunk, credential, skip, output_bytes, output_frames, kinds } = channel;
    Box::new([
        entry(Layer::Host, "process.detail_bytes", Value::Bytes(u64::from(detail_bytes))),
        entry(Layer::Host, "process.queue", Value::Count(u64::from(queue))),
        entry(Layer::Host, "process.channel.endpoints", Value::Count(u64::from(endpoints))),
        entry(Layer::Host, "process.channel.start_bytes", Value::Bytes(u64::from(start_bytes))),
        entry(Layer::Host, "process.channel.calls", Value::Count(u64::from(calls))),
        entry(Layer::Host, "process.io.sockets", Value::Count(u64::from(sockets))),
        entry(Layer::Host, "process.io.refusals", Value::Count(u64::from(refusals))),
        entry(Layer::Host, "process.io.intake", Value::Bytes(u64::from(intake))),
        entry(Layer::Host, "process.io.receive", Value::Bytes(u64::from(receive))),
        entry(Layer::Host, "process.io.output", Value::Bytes(u64::from(output))),
        entry(Layer::Host, "process.io.sends", Value::Count(u64::from(sends))),
        entry(Layer::Host, "process.io.accepts", Value::Count(u64::from(accepts))),
        entry(Layer::Host, "process.io.backlog", Value::Count(u64::from(backlog))),
        entry(Layer::Host, "process.io.close_timeout", Value::Duration(close_timeout)),
        entry(Layer::Host, "process.io.retry", Value::Duration(retry)),
        entry(Layer::Host, "process.channel.framing.chunk", Value::Bytes(u64::from(chunk))),
        entry(Layer::Host, "process.channel.framing.credential", Value::Bytes(u64::from(credential))),
        entry(Layer::Host, "process.channel.framing.skip", Value::Bytes(u64::from(skip))),
        entry(Layer::Host, "process.channel.framing.output_bytes", Value::Bytes(u64::from(output_bytes))),
        entry(Layer::Host, "process.channel.framing.output_frames", Value::Count(u64::from(output_frames))),
        entry(Layer::Host, "process.channel.framing.kinds", Value::Count(u64::from(kinds))),
    ])
}
