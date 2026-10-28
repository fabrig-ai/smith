//! The standard host profile keeps the spawned-agent and delivery bounds.
//! It never knows a chat or an agent's charter. `host_limits` grants a window
//! large enough for the agent's largest turn. Contract: shell.md, section 2.1;
//! protocol/limits.md, sections 3.6 and 3.8.

use skein_lib::Duration;
use smith_agent_service::profile::{Check, Layer, LimitName, Refusal, Value};
use smith_host_domain as host;

mod effective;

pub use effective::effective;

/// Build the standard host bounds around the largest turn its agent can emit.
pub fn host_limits(
    profile: &smith_agent_service::profile::Profile,
    agent: &smith_agent_service::Limits,
) -> Result<host::Limits, Refusal> {
    let run = agent.domain.run;
    let largest_turn = smith_domain::max_turn_bytes(&agent.domain)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "turn_bytes") })?;
    // Concrete history and turn payloads share one window's aggregate bound,
    // rather than charging each reserved turn at the largest payload (rule 5).
    let answer_bytes = u64::from(run.host_reply_bytes).max(host::Delivered::worst_case()).max(
        u64::try_from(host::Marker::CAPACITY + host::DeliveryRefusal::CAPACITY).expect("sealed feedback fits u64"),
    );
    let answered_bytes = u64::from(run.answered_calls)
        .checked_mul(
            u64::from(agent.channel.bodies.answered_call_tool)
                .checked_add(answer_bytes)
                .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "answered_bytes") })?,
        )
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "answered_bytes") })?;
    let fact_bytes = smith_channel::Fact::worst_case_bytes(&agent.channel.bodies)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "fact_bytes") })?;
    let name = LimitName::deployment(Layer::Service, "silence");
    let silence = smith_agent_service::profile::silence_bound(agent).ok_or(Refusal::Derived { limit: name })?;
    let no_progress = silence
        .checked_add(agent.io.close_timeout)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "no_progress") })?;
    let teardown_name = LimitName::deployment(Layer::Service, "teardown");
    let teardown =
        smith_agent_service::profile::teardown_bound(agent).ok_or(Refusal::Derived { limit: teardown_name })?;
    if profile.policy.exit_grace <= teardown {
        return Err(Refusal::Broken {
            check: Check::GraceOverTeardown,
            left: LimitName::deployment(Layer::Policy, "exit_grace").value(Value::Duration(profile.policy.exit_grace)),
            right: teardown_name.value(Value::Duration(teardown)),
        });
    }
    let exit_grace = teardown
        .checked_add(agent.io.close_timeout)
        .ok_or(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "exit_grace") })?;
    let limits = host::Limits {
        agents: run.runs,
        directories: run.directories,
        conflicts: run.conflicts,
        path_bytes: run.host_path_bytes.max(run.conflict_path_bytes),
        name_bytes: run.directory_name_bytes.max(agent.channel.bodies.host_ask_tool),
        accounts: agent.domain.accounts,
        charter_bytes: run.run_bytes,
        transcript_bytes: largest_turn,
        answered_bytes,
        message_bytes: u64::from(run.message_bytes),
        messages: run.messages,
        calls: agent.channel.calls,
        call_bytes: u64::from(run.host_input_bytes).max(run.outcome_bytes),
        answer_bytes,
        turns: agent.channel.turns,
        turn_bytes: largest_turn,
        unacknowledged_bytes: largest_turn,
        fact_bytes,
        outcome_bytes: run.outcome_bytes,
        detail_bytes: agent.domain.session.failure_bytes,
        spawn_timeout: Duration::from_secs(20),
        no_progress,
        silence,
        long_span: profile.policy.max_time,
        long_open: profile
            .declared
            .conversations
            .checked_mul(profile.declared.calls_per_response.checked_add(1).expect("validated calls bound"))
            .expect("validated simultaneous provider and command bounds")
            .checked_add(1)
            .expect("validated check span bound"),
        wall_time: profile.policy.max_time,
        cancel_grace: profile.policy.cancel_grace,
        exit_grace,
        teardown,
        kill_after: Duration::from_secs(2),
        facts: run.facts,
    };
    match host::check(&limits) {
        Ok(()) => Ok(limits),
        Err(host::Check::GraceOverTeardown { exit_grace, teardown }) => Err(Refusal::Broken {
            check: Check::GraceOverTeardown,
            left: LimitName::deployment(Layer::Service, "exit_grace").value(Value::Duration(exit_grace)),
            right: teardown_name.value(Value::Duration(teardown)),
        }),
        Err(host::Check::WatchdogOverSilence { no_progress, silence }) => Err(Refusal::Broken {
            check: Check::WatchdogOverSilence,
            left: LimitName::deployment(Layer::Service, "no_progress").value(Value::Duration(no_progress)),
            right: name.value(Value::Duration(silence)),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::host_limits;
    use skein_lib::Duration;
    use smith_agent_service::profile::{Check, Layer, LimitName, Refusal, Value, standard};

    #[test]
    fn the_host_exit_grace_is_derived_from_its_agents_teardown_and_one_close() {
        let profile = standard();
        let mut agent = smith_agent_process_world::limits();
        agent.events = Some(
            smith_agent_service::EventLimits::derive(
                &agent,
                smith_agent_service::sinks::Capture::None,
                1,
                0,
                &[],
                profile.policy.write_deadline,
            )
            .expect("bounded event stream"),
        );
        let host = host_limits(&profile, &agent).expect("derived grace");
        assert_eq!(host.teardown, Duration::from_secs(40));
        assert_eq!(host.exit_grace, Duration::from_secs(45));
        assert_eq!(host.cancel_grace, profile.policy.cancel_grace);
    }

    #[test]
    fn a_policy_exit_grace_below_the_agents_bound_names_grace_over_teardown() {
        let mut profile = standard();
        profile.policy.exit_grace = Duration::from_secs(9);
        let agent = smith_agent_process_world::limits();
        assert_eq!(
            host_limits(&profile, &agent),
            Err(Refusal::Broken {
                check: Check::GraceOverTeardown,
                left: LimitName::deployment(Layer::Policy, "exit_grace").value(Value::Duration(Duration::from_secs(9))),
                right: LimitName::deployment(Layer::Service, "teardown")
                    .value(Value::Duration(Duration::from_secs(10))),
            }),
        );
    }

    #[test]
    fn host_payload_bounds_follow_the_agents_admitted_ownership() {
        let profile = standard();
        let mut agent = smith_agent_process_world::limits();
        for payload in [256, 4096, 40_960] {
            agent.domain.run.host_input_bytes = payload;
            agent.domain.run.host_reply_bytes = payload;
            agent.domain.run.outcome_bytes = u64::from(payload);
            let host = host_limits(&profile, &agent).expect("derived host bounds");
            assert_eq!(host.charter_bytes, agent.domain.run.run_bytes);
            assert_eq!(host.outcome_bytes, u64::from(payload));
            assert_eq!(host.call_bytes, u64::from(payload));
            assert!(host.answer_bytes >= u64::from(payload));
            assert!(host.answer_bytes >= smith_host_domain::Delivered::worst_case());
            assert_eq!(host.fact_bytes, 17, "one kind and two u64 fields");
            assert_eq!(host.unacknowledged_bytes, host.turn_bytes);
            assert_eq!(host.transcript_bytes, host.turn_bytes);
        }
    }

    #[test]
    fn an_unrepresentable_host_window_names_its_derived_limit() {
        let mut agent = smith_agent_process_world::limits();
        agent.domain.session.history_bytes = u64::MAX;
        assert_eq!(
            host_limits(&standard(), &agent),
            Err(Refusal::Derived { limit: LimitName::deployment(Layer::Service, "turn_bytes") }),
        );
    }
}
