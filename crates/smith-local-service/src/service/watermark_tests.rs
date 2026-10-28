//! Tests local watermark admission and backlog accounting (protocol/limits.md, section 3.8).

use alloc::boxed::Box;
use skein_lib::{Time, Wall};
use smith_local_domain as local;

use crate::tests::config;
use crate::{Service, iterate};

// Two admitted entrances hold admission, then the line leaves Credential and
// SaveState waiting on the full shell edge. The watermark is their entry count.
const HELD_BACKLOG: u32 = 2;

fn admitted_at_watermark() -> Service {
    let mut config = config();
    config.limits.watermark = HELD_BACKLOG;
    let mut service = Service::new(config, 7).unwrap();
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    service.store_requests().pop().expect("initial Load handed to the store");
    for _ in 0..service.shell.capacity() {
        service.shell.push(local::Request::Credential { account: 99 });
    }
    service.local_event(local::Event::Loaded { state: None, transcript: None, deliveries: Box::new([]) });
    service.local_event(local::Event::Line { text: b"admitted".as_slice().into() });
    service
}

fn feed_line(service: &mut Service, line: &[u8]) {
    for byte in line {
        service.feed(*byte);
    }
}

#[test]
fn a_line_waits_at_the_watermark_while_an_admitted_answer_is_taken() {
    // The first iteration consumes Loaded and the already admitted line;
    // the next iteration still has no shell credit for their requests.
    const HELD_ITERATIONS: u32 = 2;
    let mut service = admitted_at_watermark();
    feed_line(&mut service, b"later\n");
    for _ in 0..HELD_ITERATIONS {
        iterate(&mut service, Time::ZERO, Wall::EPOCH);
        assert_eq!(service.terminal_events.len(), 1);
        assert!(service.local_events.is_empty(), "the admitted store answer was stepped");
        assert_eq!(service.local_requests.len(), HELD_BACKLOG);
    }
    while service.shell.pop().is_some() {}
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert!(service.terminal_events.is_empty(), "returning shell credit releases admission");
    let Some(local::Event::Line { text }) = service.local_events.iter().next() else {
        panic!("the released line waits at the local entrance")
    };
    assert_eq!(text.as_ref(), b"later");
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert!(service.local_events.is_empty(), "the released line reaches the local domain");
}

#[test]
fn an_interrupt_is_taken_at_the_watermark() {
    let mut service = admitted_at_watermark();
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    service.interrupt();
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert!(service.terminal_events.is_empty());
    assert!(service.local_events.is_empty());
    assert_eq!(service.interrupted_credentials, 1, "Interrupt cancels the undispatched credential prefix");
}

#[test]
fn work_pending_is_false_while_only_a_waiting_line_remains() {
    let mut service = admitted_at_watermark();
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    feed_line(&mut service, b"later\n");
    assert!(!service.work_pending(Time::ZERO), "the shell holds the backlog at the watermark");
    service.shell.pop().expect("shell returns edge credit");
    assert!(service.work_pending(Time::ZERO));
}

#[test]
fn the_backlog_counts_internal_queues_and_never_edges() {
    use skein_io::kernel::{Complete, Done, Fd, Op, Submit};
    use skein_lib::Token;
    use smith_host_domain::{Input, process};
    let mut service = Service::new(config(), 7).unwrap();
    let owner = Token::new(91);
    service.completions.push(Complete { op: owner, kind: Op::Close { fd: Fd::new(9) }, result: Ok(Done::Nothing) });
    service.submissions.push(Submit { op: owner, kind: Op::Close { fd: Fd::new(9) } });
    service.output.push(b"edge".as_slice().into());
    service.shell.push(local::Request::Credential { account: 99 });
    service.interrupt();
    service.writer_requests().push(skein_io::Request::Close { entity: owner });
    service.writer_events().push(skein_io::Event::Closed { owner });
    assert_eq!(service.backlog(), 0, "all service and process edges are excluded");
    service.local_events.push(local::Event::StateSaved);
    assert_eq!(service.backlog(), 1);
    service.local_requests.push(local::Request::Credential { account: 7 });
    assert_eq!(service.backlog(), 2);
    service.host_events.push(Input::Process(process::Event::Hangup { owner }));
    assert_eq!(service.backlog(), 3);
    service.lower.push(process::Request::Read { owner, process: owner });
    assert_eq!(service.backlog(), 4);
    service.below.process_mut().store_request(smith_local_protocol::StoreRequest::Load);
    assert_eq!(service.backlog(), 5, "process internal handoffs contribute too");
    let mut held = a_service_with_held_inline_facts();
    let child_entries = held.local.backlog();
    assert!(child_entries > 0, "native facts wait for the service");
    held.below.inline_mut().expect("inline effects").io_event(skein_io::Event::Resized);
    assert_eq!(held.backlog(), u64::from(child_entries) + 1, "inline effects' handoffs contribute too");
    service.local = held.local;
    assert_eq!(service.backlog(), 5 + u64::from(child_entries), "the local domain contributes its child entries");
}

// Native run facts survive a domain entrance while its complete sink waits.
fn a_service_with_held_inline_facts() -> Service {
    use skein_lib::{Duration, Env};
    use smith_host_domain::{Form, Scheme};
    let (mut config, _) = crate::tests::recording_colocated_config();
    config.limits.watermark = 1; // One retained fact must hold the next line.
    let mut service = Service::new(config, 7).unwrap();
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: service.limits.local.clone() };
    local::resume(&mut service.local, &env, &mut service.local_requests);
    while service.local_requests.pop().is_some() {}
    for event in [
        local::Event::Loaded { state: None, transcript: None, deliveries: Box::new([]) },
        local::Event::Line { text: b"hello".as_slice().into() },
        local::Event::Credential {
            form: Form::Credential(Scheme::Bearer),
            grant: smith_domain::Grant {
                name: smith_domain::GrantName { account: 0, generation: 1 },
                renewable: true,
                valid: Duration::from_secs(60),
            },
        },
        local::Event::StateSaved,
    ] {
        local::step(&mut service.local, &env, event, &mut service.local_requests);
        while service.local_requests.pop().is_some() {}
    }
    service
}

#[test]
fn a_line_waits_while_the_local_child_holds_the_backlog() {
    use skein_io::kernel::Fd;
    use smith_agent_service::sinks;
    // A destination has six line slots; eight offers reach its reservation.
    const SINK_OFFERS: u64 = 8;
    const HELD_ITERATIONS: u32 = 2; // Check retention across an iteration boundary.
    let mut service = a_service_with_held_inline_facts();
    let trace = Fd::new(20);
    service.adopt_trace(trace, Time::ZERO, crate::tests::session_started(), sinks::Delivery::Complete).unwrap();
    let (charter, messages) = service.local.charter();
    let effects = service.below.inline_mut().expect("inline effects");
    effects.starting(1, messages, &charter);
    let sinks = effects.front_sinks().expect("configured event sink");
    for name in 0..SINK_OFFERS {
        if !sinks.takes() {
            break;
        }
        sinks.record(&smith_events::Record {
            t_ms: 0,
            event: smith_events::Event::MessageRead(smith_events::MessageRead { run: 1, name, turn: 1 }),
        });
    }
    service.capture_activation = Some(1); // The held facts belong to the already started activation.
    feed_line(&mut service, b"later\n");
    for _ in 0..HELD_ITERATIONS {
        iterate(&mut service, Time::ZERO, Wall::EPOCH);
        assert!(service.local.backlog() > 0, "the complete sink retains native run facts");
        assert!(service.local_events.is_empty());
        assert!(service.local_requests.is_empty());
        assert_eq!(service.backlog(), u64::from(service.local.backlog()), "the held entries belong to the child");
        assert_eq!(service.terminal_events.len(), 1, "the child alone holds admission");
    }
    // Giving up this destination releases its credit; facts and the waiting line advance.
    let append = crate::tests::trace_append(&mut service, trace);
    service.completions().push(skein_io::kernel::Complete {
        op: append.op,
        kind: append.kind,
        result: Err(skein_io::kernel::Error::NoSpace),
    });
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert_eq!(service.local.backlog(), 0);
    // The newly resumed root can hand off lower work in this pass; admission
    // sees its dispatch on the next pass, as it does for other admitted work.
    iterate(&mut service, Time::ZERO, Wall::EPOCH);
    assert!(service.terminal_events.is_empty(), "draining the child releases the waiting line");
}
