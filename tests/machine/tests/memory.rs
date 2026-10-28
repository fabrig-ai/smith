//! Two saturated searches, their four output routes and root authority
//! under measured scenario ceilings.

use skein_lib::stream::Up;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_world::domain::heap::{Counting, Meter};
use smith_domain::{run, tools};
use smith_protocol_machine::{self as machine, Below, BelowEvent, Component, FromDomain, Limits, ToDomain};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn saturated_searches_and_pipe_routes_stay_within_the_machine_bound() {
    // Peak with two saturated searches and their four pipe routes.
    // Measured 9_537 B on 2026-10-28; a quarter of headroom, rounded up.
    const SEARCH_CEILING: u64 = 12_000;

    let limits = Limits {
        operations: 0,
        roots: 1,
        path_bytes: 16,
        file_bytes: 0,
        tool_payload: 16,
        entries: 0,
        entry_bytes: 0,
        processes: 2,
        output_bytes: 0,
        search_hits: 0,
        search_bytes: 256,
        search_line_bytes: 256,
        env_bytes: 128,
        group_stop: Duration::from_millis(1),
    };
    let workspace = run::Workspace {
        directories: Box::new([run::Directory {
            host: Box::from(b"/host".as_slice()),
            name: Box::from(&b"repo"[..]),
            root: Token::new(1),
            writable: false,
            git: true,
            conflicts: Box::new([]),
        }]),
    };
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut to_domain = Queue::<ToDomain>::with_capacity(4);
    let mut below = Queue::<Below>::with_capacity(8);
    let variables: Vec<_> =
        (0_u8..64).map(|index| machine::Variable { name: Box::new([b'A' + index]), value: Box::new([]) }).collect();
    let meter = Meter::new();
    meter.start();
    let mut component =
        Component::new(&limits, machine::Environment::encode(&variables, &limits).expect("empty environment fits"));
    component.workspace(&workspace);
    meter.check(meter.end(), SEARCH_CEILING, &"new and workspace");

    for index in 0_u64..2 {
        let owner = Token::new(index + 10);
        let request = FromDomain::Op {
            owner,
            op: tools::Op::Search {
                at: tools::Place { root: Token::new(1), path: Box::new([]) },
                pattern: Box::from(&b"needle"[..]),
                glob: None,
                hits: 0,
                bytes: 256,
            },
            deadline: Time::from_nanos(100),
        };
        meter.start();
        component.from_domain(&env, request, &mut to_domain, &mut below);
        let measured = meter.end();
        match below.pop() {
            Some(Below::Spawn { .. }) => {}
            other => panic!("search starts a child: {other:?}"),
        }
        meter.check(measured, SEARCH_CEILING, &"search admitted");

        meter.start();
        component.from_below(
            &env,
            BelowEvent::Process(skein_io::Event::Spawned {
                owner,
                child: Token::new(index + 20),
                pipes: Box::new([Token::new(index * 2 + 30), Token::new(index * 2 + 31)]),
            }),
            &mut to_domain,
            &mut below,
        );
        let measured = meter.end();
        assert!(below.pop().is_some() && below.pop().is_some(), "both pipe demands");
        meter.check(measured, SEARCH_CEILING, &"both pipe routes");
    }
    assert!(to_domain.is_empty(), "both searches remain live");
    let line =
        br#"{"type":"match","data":{"path":{"text":"a"},"lines":{"text":"needle\n"},"line_number":1,"submatches":[]}}"#;
    meter.start();
    for byte in line.iter().copied().chain([b'\n']) {
        component.from_below(
            &env,
            BelowEvent::Pipe(skein_io::Event::Stream { owner: Token::new(30), up: Up::Bytes(Box::new([byte])) }),
            &mut to_domain,
            &mut below,
        );
        assert!(below.pop().is_some(), "stream rearms after each byte");
    }
    meter.check(meter.end(), SEARCH_CEILING, &"bounded JSON line parse");
    assert!(meter.held() > 0, "the component retains bounded state");
}

#[test]
fn shared_harness_meters_file_child_and_flood_runs_through_drop() {
    let mut world = smith_machine_world::World::new(8, &[skein_fake_machine::Item::file(b"file", b"old")]);
    world.check_memory();
    let root = world.root();
    let loaded = world.request(FromDomain::Op {
        owner: Token::new(20),
        op: tools::Op::Load { at: tools::Place { root, path: b"file".as_slice().into() }, max: 128 },
        deadline: Time::from_nanos(100_000_000),
    });
    let ToDomain::Done { done: tools::Done::Loaded { version, .. }, .. } = loaded else {
        panic!("the memory fixture loaded its file");
    };
    let stored = world.request(FromDomain::Op {
        owner: Token::new(21),
        op: tools::Op::Store {
            at: tools::Place { root, path: b"file".as_slice().into() },
            content: Box::new([b'x'; 128]),
            expect: tools::Expect::Is { version },
        },
        deadline: Time::from_nanos(100_000_000),
    });
    assert!(matches!(stored, ToDomain::Done { done: tools::Done::Stored { .. }, .. }));
    let checked = world.request(FromDomain::Check {
        owner: Token::new(22),
        program: run::Place { root, path: b"check-hang".as_slice().into() },
        deadline: Time::from_nanos(10_000_000),
        tail: 32,
    });
    assert!(matches!(checked, ToDomain::Checked { ran: run::Ran { exit: run::Exit::TimedOut, .. }, .. }));
    let flooded = world.flood(Token::new(23), &[b'x'; 4096], 32, 32);
    assert!(matches!(flooded, ToDomain::Done { done: tools::Done::Exited { dropped: 4032, .. }, .. }));
    world.settle();
    assert_eq!(world.heap().len(), 4, "every independent run was metered");
    assert!(world.heap().iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
    assert!(!world.trace().is_empty(), "actual kernel operations ran");
}

#[test]
fn saturated_configured_environments_and_command_check_copies_fit_the_machine_bound() {
    // Peak of retained environments and command/check translation copies.
    // Measured 8_870 B on 2026-10-28; a quarter of headroom, rounded up.
    const ENVIRONMENT_CEILING: u64 = 12_000;

    for many in [false, true] {
        let limits = Limits {
            operations: 0,
            roots: 1,
            path_bytes: 16,
            file_bytes: 0,
            tool_payload: 16,
            entries: 0,
            entry_bytes: 0,
            processes: 2,
            output_bytes: 0,
            search_hits: 0,
            search_bytes: 0,
            search_line_bytes: 0,
            env_bytes: 128,
            group_stop: Duration::from_millis(1),
        };
        let variables = if many {
            (0_u8..64)
                .map(|index| machine::Variable { name: Box::new([b'A' + index]), value: Box::new([]) })
                .collect::<Vec<_>>()
        } else {
            vec![machine::Variable { name: Box::new([b'A']), value: Box::new([b'x'; 126]) }]
        };
        let workspace = run::Workspace {
            directories: Box::new([run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"repo".as_slice().into(),
                root: Token::new(1),
                writable: false,
                git: false,
                conflicts: Box::new([]),
            }]),
        };
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
        let mut up = Queue::with_capacity(2);
        let mut down = Queue::with_capacity(2);
        let meter = Meter::new();
        meter.start();
        let mut component =
            Component::new(&limits, machine::Environment::encode(&variables, &limits).expect("exact byte ceiling"));
        component.workspace(&workspace);
        meter.check(meter.end(), ENVIRONMENT_CEILING, &"maximum environment encoding and retained configuration");
        for check in [false, true] {
            let owner = Token::new(if check { 12 } else { 11 });
            let request = if check {
                FromDomain::Check {
                    owner,
                    program: run::Place { root: Token::new(1), path: b"check".as_slice().into() },
                    deadline: Time::from_nanos(100),
                    tail: 0,
                }
            } else {
                FromDomain::Op {
                    owner,
                    op: tools::Op::Spawn {
                        cwd: tools::Place { root: Token::new(1), path: Box::new([]) },
                        command: b"true".as_slice().into(),
                        head: 0,
                        tail: 0,
                        after: None,
                    },
                    deadline: Time::from_nanos(100),
                }
            };
            meter.start();
            component.from_domain(&env, request, &mut up, &mut down);
            match down.pop().expect("admitted configured process") {
                Below::Spawn { spawn, .. } => {
                    assert_eq!(spawn.env.len(), variables.len());
                    assert_eq!(spawn.env.iter().map(|entry| entry.len()).sum::<usize>(), 128);
                    drop(spawn);
                }
                other @ (Below::File { .. }
                | Below::CancelFile { .. }
                | Below::OpenRead { .. }
                | Below::OpenDirectory { .. }
                | Below::Process(_)) => {
                    panic!("configured process spawn: {other:?}")
                }
            }
            let peak =
                meter.check(meter.end(), ENVIRONMENT_CEILING, &"retained environment plus command/check translation");
            assert!(peak > 0);
        }
        assert!(up.is_empty());
    }
}

#[test]
fn a_selected_listing_at_both_caps_stays_within_the_machine_bound() {
    let mut world = smith_machine_world::World::new(9212, &[]);
    world.check_memory();
    let selected: Vec<u8> = (0..16)
        .flat_map(|index| {
            let mut path = format!("./a{index:02}/").into_bytes();
            path.extend([b'x'; 100]);
            path.push(0);
            path
        })
        .collect();
    let terminal = world.listing(Token::new(91), &selected, tools::Depth::Whole, 16, 2048);
    assert!(
        matches!(terminal, ToDomain::Done { done: tools::Done::Listed { ref entries, more }, .. } if entries.len() == 16 && more == 16)
    );
    world.settle();
    assert!(world.heap().iter().all(|(peak, bound)| *peak > 0 && peak <= bound));
}
