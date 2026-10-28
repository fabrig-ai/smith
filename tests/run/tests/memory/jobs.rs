//! A waiting writer owns one opening under its run's lease (domain/run.md, 5.3).

use super::{
    Ask, Asked, BUDGET, Domain, Env, Event, Families, LIMITS, Limits, MAX_OUT, Meter, Queue, Read, ReplyTo, Request,
    Time, Token, Tools, Wall, bytes, charter, measured_step, workspace,
};

const HOST_RUN: Token = Token::new(88);
const WRITER_CALL: Token = Token::new(900);
const CALL_SLOTS: u32 = 16;
const SMALL_HOST_PART: u64 = 131_072;
const LARGE_HOST_PART: u64 = SMALL_HOST_PART + 65_536;
const SMALL_BRIEF: u64 = 1;
const LARGE_BRIEF: u64 = SMALL_BRIEF + 65_536;

// Peak ceiling for a waiting writer with a one-byte brief (programming-model.md, section 6.3).
const SMALL_WRITER_CEILING: u64 = 15_000;

// Peak ceiling for a waiting writer with a 65537-byte brief (programming-model.md, section 6.3).
const LARGE_WRITER_CEILING: u64 = 180_000;

#[test]
fn a_waiting_writers_brief_is_held_once_per_run() {
    let small = Limits { calls: CALL_SLOTS, host_part: SMALL_HOST_PART, ..LIMITS };
    let large = Limits { host_part: LARGE_HOST_PART, ..small };
    let small_held = waiting_writer_bytes(&small, SMALL_BRIEF, SMALL_WRITER_CEILING);
    let large_held = waiting_writer_bytes(&large, LARGE_BRIEF, LARGE_WRITER_CEILING);
    assert_eq!(large_held - small_held, 65_536);
}

fn waiting_writer_bytes(limits: &Limits, brief: u64, bound: u64) -> u64 {
    let meter = Meter::new();
    let mut domain = Domain::new(limits, 0);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: *limits };
    let mut out = Queue::with_capacity(MAX_OUT);
    let conversation = open_main_for_waiting_writer(&mut domain, &env, &mut out, bound);
    let event = Event::Delegated {
        name: smith_domain_run::CallName { activation: 1, completion: 1, position: 0 },
        conversation,
        call: WRITER_CALL,
        ask: Ask::SubAgent {
            brief: bytes(brief),
            families: Families { tools: Tools { inspect: true, modify: true, shell: true }, agents: false },
            llm: None,
            share: smith_domain_run::Share::UNASKED,
        },
        deadline: Time::ZERO.saturating_add(BUDGET.time),
    };
    meter.start();
    smith_domain_run::step(&mut domain, &env, event, &mut out);
    let measured = meter.end();
    let Some(Request::CancelJobs { peer }) = out.pop() else { panic!("writer waits for jobs") };
    assert_eq!(peer, HOST_RUN);
    assert!(out.is_empty());
    assert_eq!(domain.calls(), 1);
    while domain.pop_fact().is_some() {}
    meter.check(measured, bound, limits);
    meter.held()
}

fn open_main_for_waiting_writer(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Request>, bound: u64) -> Token {
    let meter = Meter::new();
    let start = Event::Start {
        messages: Box::default(),
        workspace: Some(workspace()),
        reply_to: ReplyTo::new(HOST_RUN),
        host_run: HOST_RUN,
        identity: None,
        activation: 1,
        window: smith_domain_run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
        charter: charter(env.limits.run_bytes),
        transcript: None,
        resumed: false,
    };
    let [Asked::Other, Asked::Read { owner }] = measured_step(domain, env, out, &meter, start, None, bound)[..] else {
        panic!("admitted main prepares")
    };
    let [Asked::Probe { owner }] =
        measured_step(domain, env, out, &meter, Event::Read { owner, read: Read::Missing }, None, bound)[..]
    else {
        panic!("main probes checks")
    };
    let [Asked::Open { conversation }] =
        measured_step(domain, env, out, &meter, Event::Probed { owner, executable: false }, None, bound)[..]
    else {
        panic!("prepared main opens")
    };
    assert!(
        measured_step(domain, env, out, &meter, Event::Started { conversation, peer: HOST_RUN }, None, bound)
            .is_empty()
    );
    conversation
}
