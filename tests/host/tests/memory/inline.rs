//! Both inline slots retain independent root inventories under a scenario ceiling.
use skein_lib::{Env, Queue, Time, Token, Wall};
use skein_world::domain::heap::Meter;
use smith_agent_world::Job;
use smith_host_domain::parent;
use smith_host_world::inline;
use smith_inline_agent::{Domain, Input, Output};

// Peak with two full inline roots, their carried messages and emitted prompts.
const INLINE_SLOTS_CEILING: u64 = 5_500_000;

#[test]
fn two_full_inline_slots_and_their_emitted_prompts_stay_under_the_ceiling() {
    let limits = inline::limits();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let meter = Meter::new();
    let mut domain = Domain::new(&limits, inline::configuration(), 319);
    let mut out = Queue::with_capacity(smith_inline_agent::max_out(&limits));
    for client in 1..=2 {
        let mut start = inline::start(319, Job::HostTools);
        start.messages = (1..=limits.smith.run.messages)
            .map(|name| smith_host_domain::Message {
                finish_requested: false,
                name: Token::new(u64::from(name)),
                label: Box::default(),
                text: vec![b'x'; usize::try_from(limits.smith.run.message_bytes - 2).expect("small message cap")]
                    .into(),
            })
            .collect();
        start.grants = (0..limits.smith.accounts)
            .map(|account| smith_host_domain::Grant {
                account,
                generation: u64::MAX,
                valid: skein_lib::Duration::from_secs(7200),
                form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
            })
            .collect();
        meter.start();
        smith_inline_agent::step(
            &mut domain,
            &env,
            Input::Parent(parent::Event::Spawn { client: Token::new(client), start }),
            &mut out,
        );
        let measured = meter.end();
        let mut emitted = false;
        while let Some(request) = out.pop() {
            if matches!(request, Output::Lower { request: smith_inline_agent::Lower::Complete { .. }, .. }) {
                emitted = true;
            }
        }
        assert!(emitted, "each composed root reaches the actual provider boundary");
        meter.check(measured, INLINE_SLOTS_CEILING, &limits);
    }
    assert_eq!(domain.hosted(), 2, "maximum configured slots actually retained");
    assert!(meter.held() > 0);
}
