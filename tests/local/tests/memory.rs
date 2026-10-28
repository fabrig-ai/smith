//! The full local line queue and child allocation stay under a measured ceiling.

use skein_lib::{Duration, Env, Queue, Time, Wall};
use skein_world::domain::heap::{Counting, Meter};
use smith_domain::run::{self, charter::Endpoint, outcome};
use smith_local_domain::{self as local, Contract, Event, Request};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn a_local_domain_with_every_line_slot_full_stays_under_its_ceiling() {
    // Peak 5_447_492 B, measured 2026-10-28; a quarter above, rounded up to two significant digits.
    const FULL_LOCAL_CEILING: u64 = 6_900_000;
    let limits = local::Limits {
        smith: smith_agent_world::LIMITS,
        agent: local::AgentLimits::inline(
            smith_agent_world::LIMITS,
            2,
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .expect("bounded inline child"),
        endpoints: Box::new([Endpoint(0)]),
        chat_bytes: 64,
        text_bytes: 4096,
        models: 1,
        line_bytes: 1024,
        show_bytes: 4096,
        unsaved: 2,
        facts: 64,
    };
    let config = local::Config {
        protocol_allowances: Box::new([0]),
        reserve: run::charter::Reserve { turns: 0, time: skein_lib::Duration::ZERO },
        chat: b"main".as_slice().into(),
        instructions: b"Assist the person".as_slice().into(),
        brief: run::charter::Brief { sections: Box::new([]) },
        progress: Box::new([smith_domain::session::Progress {
            head: Duration::from_secs(60),
            idle: Duration::from_secs(30),
        }]),
        efforts: Box::new([Box::new([])]),
        models: Box::new([run::charter::Llm {
            effort: None,
            prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
            dialect: 1,
            account: Some(0),
            endpoint: Endpoint(0),
            model: b"scripted".as_slice().into(),
            window: 8192,
            output: 256,
        }]),
        sub_agents: true,
        budget: smith_agent_world::BUDGET,
        conventions: None,
        contract: Contract::Report(outcome::TextSpec { max: 2048, fields: Box::new([]) }),
        deliver: None,
        title_field: b"title".as_slice().into(),
        live_text: false,
        mode: smith_local_domain::Mode::Chat { waiting: Duration::from_secs(30) },
        resume: true,
        accounts: Box::new([0]),
        workspace: None,
        push: None,
    };
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let meter = Meter::new();
    let mut out = Queue::<Request>::uncapped();
    meter.start();
    let mut domain = local::Domain::new(config, &env.limits, 83).expect("valid local domain");
    meter.check(meter.end(), FULL_LOCAL_CEILING, &env.limits);
    for _ in 0..env.limits.smith.run.messages {
        meter.start();
        local::step(
            &mut domain,
            &env,
            Event::Line { text: vec![b'x'; env.limits.line_bytes as usize].into_boxed_slice() },
            &mut out,
        );
        meter.check(meter.end(), FULL_LOCAL_CEILING, &env.limits);
    }
    assert!(meter.held() >= u64::from(env.limits.smith.run.messages) * u64::from(env.limits.line_bytes));
    assert!(matches!(out.pop(), Some(Request::Input { reading: local::Reading::Held })));
    assert!(out.is_empty(), "loading retains all lines until the store answers");
}
