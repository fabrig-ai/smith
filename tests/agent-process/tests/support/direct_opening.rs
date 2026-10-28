//! Native provider assertions shared by focused and seeded process stories.
use super::opening::{BRIEF, INSTRUCTIONS, charter};

// One initial user message; eight channel frames are the world's standard credit.
const USER_MESSAGES: usize = 1;
const FRAMES: u32 = 8;

pub fn direct(seed: u64, anthropic: bool) -> smith_agent_process_world::World {
    use skein_fake_llm_domain::api::{Part, Role};
    let mut world = smith_agent_process_world::World::new(seed, &charter());
    if anthropic {
        world.anthropic();
    }
    // The trace launcher selects the matching native provider configuration.
    world.observe_facts(smith_agent_process_world::TraceCase::Failed, FRAMES);
    world.initial_message(0, b"person", b"One more detail.");
    world.check_memory();
    assert!(world.settle());
    assert!(
        matches!(world.answer().expect("answer").result(), smith_channel::RunResult::Parked),
        "{:?}",
        world.answer()
    );
    let queries = world.peer_queries();
    let [query] = queries.as_slice() else { panic!("one actual native request") };
    assert!(query.system.starts_with(INSTRUCTIONS));
    let system = std::str::from_utf8(&query.system).expect("system text");
    assert!(!system.contains("## Request") && !system.contains("## Context"));
    assert_eq!(query.messages.len(), USER_MESSAGES);
    assert_eq!(query.messages[0].role, Role::User);
    assert_eq!(
        query.messages[0].parts.as_ref(),
        [Part::Text { text: [BRIEF.as_bytes(), b"person: One more detail."].concat().into() }]
    );
    world.assert_agent_clean();
    world
}
