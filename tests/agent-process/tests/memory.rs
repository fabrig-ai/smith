//! Reach receiving maxima while every hosted iteration and drop is metered.
//! The composed bound sums independently bounded layers, so equality with the
//! sum is not expected; the driver attains the declared run context/section
//! and decoded-completion limits rather than only measuring construction.
#[path = "memory/jobs.rs"]
mod jobs;

use skein_lib::{List, Reader, Writer};
use skein_world::domain::heap::Counting;
use smith_agent_process_world::{World, charter, limits};

#[global_allocator]
static HEAP: Counting = Counting;

fn full_context() -> Box<[u8]> {
    let base = charter();
    let mut parts = smith_charter::Charter::decode(&smith_charter::CEILINGS, &mut Reader::new(&base))
        .expect("charter")
        .into_parts();
    assert!(parts.models.is_empty() && parts.conventions.is_none() && parts.tools.host().is_empty());
    assert!(parts.contract.report().as_ref().expect("report").fields().is_empty());
    let count = limits().domain.run.brief_sections;
    let fixed =
        u64::from(count) * (size_of::<smith_domain_run::Section>() as u64 + 1) + parts.main.model().len() as u64;
    parts.instructions =
        vec![b'x'; usize::try_from(limits().domain.run.run_bytes - fixed).expect("payload")].into_boxed_slice();
    parts.brief = List::with_capacity(count);
    for _ in 0..count {
        parts
            .brief
            .push(
                smith_charter::Section::new(
                    &smith_charter::CEILINGS,
                    smith_charter::SectionParts { title: b"x".as_slice().into(), body: Box::new([]) },
                )
                .expect("section"),
            )
            .expect("maximum sections");
    }
    assert_eq!(
        parts.instructions.len() as u64 + fixed,
        limits().domain.run.run_bytes,
        "exact aggregate receiving maximum"
    );
    assert_eq!(parts.brief.len(), count, "maximum owning section array");
    let record = smith_charter::Charter::new(&smith_charter::CEILINGS, parts).expect("full charter");
    let mut writer = Writer::new(usize::try_from(record.measure()).expect("wire size"));
    record.encode(&mut writer).expect("wire");
    writer.finish()
}

#[test]
fn maximum_run_context_and_provider_completion_fit_each_process_at_every_iteration() {
    let mut context = World::new(7, &full_context());
    context.signal();
    context.check_memory();
    assert!(context.settle());
    assert!(context.observed().iter().any(|frame| frame.kind == 0x0106), "full context was admitted, not refused");
    drop(context);
    let mut completion = World::new(7, &charter());
    completion.full_completion();
    completion.observe_facts(smith_agent_process_world::TraceCase::Accepted, 4);
    completion.check_memory();
    assert!(completion.settle());
    assert!(completion.peer_replied(), "the actual maximum completion was emitted");
    let client = limits().llm.adapter.client;
    assert_eq!(
        completion.provider_answer_shape(),
        (usize::try_from(client.output_items).expect("parts"), usize::try_from(client.answer).expect("payload")),
        "both maximum parts and total provider answer bytes including actual ID/phase metadata reached the host transcript; answer={:?}; records={:?}",
        completion.answer(),
        completion.records()
    );
    drop(completion);
}

#[test]
fn configured_environment_commands_fit_the_composed_agent_bound() {
    let mut world = World::new(42, &charter());
    world.configured_environment();
    world.check_memory();
    assert!(world.settle());
    assert_eq!(world.command_environments().len(), 1);
    world.assert_agent_clean();
}

#[test]
fn every_conversation_open_with_the_pool_full_stays_under_the_ceiling() {
    // Peak 285_260 B on 2026-10-28; a quarter of headroom, rounded up.
    const FULL_POOL_CEILING: u64 = 360_000;

    use skein_lib::{Env, Queue, Time, Token, Wall};
    use skein_llm_connection::{Component, Deadlines, Request};
    use skein_world::domain::heap::Meter;
    let (endpoints, pool) = full_pool();
    let bound = FULL_POOL_CEILING;
    let meter = Meter::new();
    meter.start();
    let mut component = Component::new(endpoints, &pool).expect("bounded full pool");
    let mut jobs = jobs::fill();
    let mut up = Queue::with_capacity(32);
    let mut io = Queue::with_capacity(32);
    for number in 0..pool.calls {
        let call = skein_llm_world::call(u64::from(number));
        component.down(
            &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: pool },
            Request::Start {
                call: Token::new(u64::from(number)),
                endpoint: 0,
                prompt: call.prompt,
                credential: call.credential,
                deadlines: Deadlines::none(),
                drop_reasoning: false,
            },
            &mut up,
            &mut io,
        );
    }
    let measured = meter.end();
    assert!(up.is_empty(), "every declared conversation was admitted");
    assert_eq!(io.len(), pool.calls, "every conversation opened its connection");
    assert_eq!(component.reserved(), pool.memory, "admitted calls fill the real pool");
    assert!(measured.peak() <= bound, "peak {} exceeds {bound}", measured.peak());
    jobs.check_captures();
    assert_eq!(smith_agent_process_world::profile().declared.memory, 2_147_483_648);
}

fn full_pool() -> (List<skein_llm_connection::Endpoint>, skein_llm_connection::Limits) {
    let configured = smith_agent_process_world::configuration();
    let (mut endpoints, _) = configured.llm_endpoints.into_parts();
    let client = endpoints.get_mut(0).expect("one endpoint");
    let call = skein_llm_world::call(7);
    let measured = skein_llm::client::measure(&call.prompt, call.credential.as_ref(), &client.llm, &client.limits)
        .expect("fixture fits");
    // A gzip body is compressed only while it and its room fit the request
    // bound (skein llm.md, 4.8), so the bound holds both.
    client.limits.request = measured.body + measured.compressed;
    let reservation = skein_llm::client::reservation(measured, &client.limits).expect("reservation");
    let mut pool = configured.limits.llm.connection;
    pool.memory = reservation * u64::from(pool.calls);
    assert!(pool.memory <= smith_agent_process_world::profile().declared.llm_pool);
    (endpoints, pool)
}
