//! Service-owned values supply the domain's forms (domain/host.md, section 7).

use super::{Service, local};
use alloc::boxed::Box;
use skein_lib::Duration;
use smith_host_domain::{Form, Scheme};
use smith_protocol_channel::GrantValue;

fn grant() -> smith_domain::Grant {
    smith_domain::Grant {
        renewable: true,
        name: smith_domain::GrantName { account: 7, generation: 1 },
        valid: Duration::from_secs(60),
    }
}

#[test]
fn a_lent_value_sets_its_credential_events_form() {
    let mut service = Service::new(crate::tests::config(), 7).expect("bounded service");
    for (value, form) in [
        (GrantValue::Bearer(Box::from(b"token".as_slice())), Form::Credential(Scheme::Bearer)),
        (
            GrantValue::Withdrawn { account_id: Box::from(b"acc".as_slice()), bearer: Box::from(b"token".as_slice()) },
            Form::Withdrawn,
        ),
    ] {
        service.credential(grant(), value);
        let Some(local::Event::Credential { grant: lent, form: lent_form }) = service.local_events.pop() else {
            panic!("credential terminal");
        };
        assert_eq!(lent, grant());
        assert_eq!(lent_form, form);
    }
}

#[test]
fn a_value_without_a_form_answers_missing() {
    let mut service = Service::new(crate::tests::config(), 7).expect("bounded service");
    service.credential(grant(), GrantValue::Program(Box::from(b"Bad_Name".as_slice())));
    let Some(local::Event::NoCredential { account, reason }) = service.local_events.pop() else {
        panic!("missing credential terminal");
    };
    assert_eq!(account, 7);
    assert_eq!(reason, local::CredentialFailure::Missing);
    assert!(service.values.is_empty());
}

#[test]
fn the_shell_writer_requests_wait_only_on_io_credit() {
    use skein_io::{self as io, kernel};
    use skein_lib::{List, Queue, Time, Token, Wall, stream};

    // Three shell writers exceed the service's two-entry queue estimate.
    const QUEUE: u32 = 2;
    const WRITERS: u32 = 3;
    let mut config = crate::tests::config();
    config.limits.queue = QUEUE;
    config.limits.process.queue = QUEUE;
    config.limits.process.io.sockets = WRITERS;
    let mut service = Service::new(config, 7).unwrap();
    let mut writers = List::with_capacity(WRITERS);
    for offset in 0..WRITERS {
        let descriptor = 10_i32 + i32::try_from(offset).unwrap();
        writers.push(service.adopt_writer(kernel::Fd::new(descriptor)).unwrap()).unwrap();
    }
    let adapter = service.below.process_mut();
    let env = adapter.at(Time::ZERO, Wall::EPOCH);
    io::down(
        adapter.io(),
        &env,
        io::Request::Connect {
            owner: Token::new(90),
            addr: io::Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], 1_u16))),
        },
        &mut Queue::uncapped(),
    );
    for writer in &writers {
        service.writer_request(io::Request::Stream {
            stream: *writer,
            down: stream::Down::Demand { read: stream::Read::Nothing, room: 1 },
        });
    }
    let adapter = service.below.process_mut();
    adapter.down(
        &env,
        &mut Queue::uncapped(),
        &mut None,
        &skein_lib::Map::with_capacity(0),
        &mut Queue::uncapped(),
        &mut None,
    );
    assert_eq!(adapter.io_requests().len(), WRITERS, "the method accepts the complete shell burst while IO waits");
    let mut refused = Queue::uncapped();
    io::resume(adapter.io(), &env, &mut refused, &mut Queue::uncapped());
    assert_eq!(refused.len(), 2, "IO returns its refusal slot");
    let mut rooms = List::with_capacity(WRITERS);
    for remaining in [1, 0, 0] {
        super::iterate(&mut service, Time::ZERO, Wall::EPOCH);
        assert_eq!(service.below.process_mut().io_requests().len(), remaining, "each down pass keeps its work bound");
        while let Some(event) = service.writer_events().pop() {
            let io::Event::Stream { owner, up: stream::Up::Room } = event else {
                panic!("one room terminal per shell request: {event:?}");
            };
            rooms.push(owner).unwrap();
        }
    }
    assert_eq!(rooms.as_slice(), writers.as_slice(), "every writer request reaches IO in the order handed in");
}
