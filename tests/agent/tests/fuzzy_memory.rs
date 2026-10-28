//! Random agent memory stays under scenario ceilings at every entry point.

use skein_lib::Duration;
use skein_world::domain::heap::{self, Meter};
use smith_agent_world::memory::{LIMITS, churn};
use smith_domain::{Limits, run};

#[global_allocator]
static HEAP: heap::Counting = heap::Counting;

// Peak ceiling for random root-domain churn with default limits across twelve seeds (programming-model.md, section 6.3).
const RANDOM_AGENT_CEILING: u64 = 4_500_000;

// Peak ceiling for random root-domain churn with wider limits across twelve seeds (programming-model.md, section 6.3).
const WIDE_RANDOM_AGENT_CEILING: u64 = 5_900_000;

#[test]
fn a_domain_driven_at_random_stays_under_its_scenario_ceilings_at_every_entry_point() {
    let mut wider = Limits {
        configured_model_bytes: LIMITS.configured_model_bytes,
        accounts: LIMITS.accounts,
        endpoints: LIMITS.endpoints,
        decoded_call_bytes: LIMITS.decoded_call_bytes,
        skew: LIMITS.skew,
        run: run::Limits { runs: 3, conversations: 8, run_conversations: 4, calls: 8, ..LIMITS.run },
        session: smith_domain::session::Limits {
            sessions: 8,
            history_messages: 24,
            history_bytes: 1_048_576,
            completion_bytes: 4096,
            answer_bytes: 4096,
            completion_blocks: 16,
            failure_bytes: 512,
            delegated_result_bytes: 131_072,
            parallel_tools: 3,
            tools: smith_domain::tools::Limits { kits: 8, calls: 3, ..LIMITS.session.tools },
            ..LIMITS.session
        },
    };
    wider.session.facts = smith_domain::session::max_facts(&wider.session);
    let mut seen = [0; 18];
    for seed in 0..12 {
        for (limits, ceiling) in [(LIMITS, RANDOM_AGENT_CEILING), (wider, WIDE_RANDOM_AGENT_CEILING)] {
            eprintln!("agent memory replay: seed={seed}, rounds=3000, limits={limits:?}");
            let counted = churn(&limits, seed, 3_000, ceiling);
            for (all, one) in seen.iter_mut().zip(counted) {
                *all += one;
            }
        }
    }
    eprintln!("agent memory actual boundary coverage: {seen:?}");
    assert!(seen[..7].iter().all(|count| *count > 10), "every kind of request was made: {seen:?}");
    assert!(seen[7..].iter().all(|count| *count > 0), "actual terminal and refusal classes occurred: {seen:?}");
}

#[test]
fn longest_child_usage_feedback_stays_within_its_measured_reservation() {
    let mut limits = LIMITS.run;
    limits.answer_bytes = 8192;
    let bound = smith_domain::feedback_worst_case(&limits).expect("checked receiving bound");
    let used = run::facts::Used { turns: u32::MAX, time: Duration::from_nanos(u64::MAX), spend: u64::MAX };
    let left = run::Budget { turns: u32::MAX, time: Duration::from_nanos(u64::MAX), spend: u64::MAX };
    let meter = Meter::new();
    let returned = run::Returned::Answered {
        text: vec![b'x'; 8192].into_boxed_slice(),
        cut: u64::MAX,
        stop: run::Stop::MaxTokens,
        used,
        left,
    };
    meter.start();
    let result = smith_domain::feedback(returned, bound).expect("largest complete result admitted");
    let measured = meter.end();
    meter.check(measured, bound, &limits);
    assert!(result.text.ends_with(b"Child usage: used 4294967295 turns and 18446744073 seconds; stopped for budget: no; ordinary left 4294967295 turns and 18446744073 seconds."));
}
