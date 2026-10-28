//! Parent drains and accounts for its agent's held entries (protocol/limits.md 3.8).
use super::*;
use crate::tests::{drain_requests, host_limits, saved_window_turn, spawned_chat_with_saved_hello};
use smith_host_domain::{self as host, process};

fn spawned_backlog(domain: &Domain) -> u32 {
    let Agent::Spawned { domain: child, .. } = &domain.agent else { panic!("spawned child") };
    child.backlog()
}

#[test]
fn a_spawned_childs_facts_are_drained_by_the_local_domain() {
    const TURNS: u32 = 8; // The fixture's configured chat budget.
    let (mut domain, env, mut out, owner) = spawned_chat_with_saved_hello();
    assert_eq!(spawned_backlog(&domain), 0, "Started and Admitted were drained");
    for number in 1..=TURNS {
        let mut turn = saved_window_turn();
        turn.sequence = number;
        step(
            &mut domain,
            &env,
            Event::Process(process::Event::Received {
                owner,
                message: host::Up::Turn {
                    turn: host::Turn {
                        opens_window: true,
                        number,
                        read: Some(Token::new(1)),
                        spent: 0,
                        body: host::TurnValue::new(turn, host_limits().turn_bytes).expect("sealed turn"),
                    },
                },
            }),
            &mut out,
        );
        let requests = drain_requests(&mut out);
        let mut saved = false;
        for request in requests {
            if let Request::SaveTurn { number: saved_number, .. } = request {
                saved |= saved_number == number;
            }
        }
        assert!(saved, "the actual turn reaches the local store");
        assert_eq!(spawned_backlog(&domain), 0, "each turn leaves no host facts behind");
        for event in [Event::TurnSaved { number }, Event::Process(process::Event::Sent { owner })] {
            step(&mut domain, &env, event, &mut out);
            drain_requests(&mut out);
            assert_eq!(spawned_backlog(&domain), 0, "settlement leaves no host facts behind");
        }
    }
}

#[test]
fn the_local_backlog_counts_its_agents_held_entries() {
    let (mut domain, env, _, owner) = spawned_chat_with_saved_hello();
    // Stop before routing this child's fault: one host fact remains for the parent.
    domain.agent.process(env.now, env.wall, process::Event::Malformed { owner });
    let Agent::Spawned { out, .. } = &mut domain.agent else { panic!("spawned child") };
    while out.pop().is_some() {}
    assert_eq!(domain.backlog(), 1, "the child's one Faulted fact counts");
    let Agent::Spawned { out, .. } = &mut domain.agent else { panic!("spawned child") };
    out.push(host::Output::Process(process::Request::Read { owner, process: Token::new(3) }));
    assert_eq!(domain.backlog(), 2, "the fact and the unrouted child output each count once");
    let Agent::Spawned { domain: child, .. } = &mut domain.agent else { panic!("spawned child") };
    assert_eq!(child.pop_fact(), Some(host::Fact::Faulted { client: Token::new(1), fault: host::Fault::Rules }));
    assert_eq!(domain.backlog(), 1, "only the child output remains");
    domain.agent.pop().expect("held output");
    assert_eq!(domain.backlog(), 0);
}
