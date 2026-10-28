//! The machine's public stories (protocol/agent.md, section 8).

use skein_fake_machine::Item;
use skein_lib::{Time, Token};
use smith_domain::{run, tools};
use smith_machine_world::World;
use smith_protocol_machine::{FromDomain, ToDomain};

fn place(root: Token, path: &[u8]) -> tools::Place {
    tools::Place { root, path: Box::from(path) }
}

fn far() -> Time {
    Time::from_nanos(100_000_000)
}

#[test]
fn a_file_changed_behind_the_sessions_back_refuses_the_store() {
    let mut world = World::new(4, &[Item::file(b"a", b"old")]);
    let root = world.root();
    let loaded = world.request(FromDomain::Op {
        owner: Token::new(10),
        op: tools::Op::Load { at: place(root, b"a"), max: 10 },
        deadline: far(),
    });
    let version = match loaded {
        ToDomain::Done { done: tools::Done::Loaded { content, version }, .. } => {
            assert_eq!(content.as_ref(), b"old");
            version
        }
        other @ (ToDomain::Done { .. }
        | ToDomain::Read { .. }
        | ToDomain::Probed { .. }
        | ToDomain::Checked { .. }
        | ToDomain::Aborted { .. }) => panic!("expected load: {other:?}"),
    };
    world.change_behind_session(b"a", b"new");
    let done = world.request(FromDomain::Op {
        owner: Token::new(11),
        op: tools::Op::Store {
            at: place(root, b"a"),
            content: Box::from(&b"ours"[..]),
            expect: tools::Expect::Is { version },
        },
        deadline: far(),
    });
    assert!(matches!(done, ToDomain::Done { done: tools::Done::Conflict { .. }, .. }), "{done:?}");
    assert_eq!(world.content(b"a"), b"new");
    world.settle();
}

#[test]
fn a_path_with_a_git_part_or_through_a_symbolic_link_is_refused() {
    let mut world = World::new(5, &[Item::file(b"safe", b"old"), Item::link(b"link", b"safe")]);
    let root = world.root();
    let denied = world.request(FromDomain::Op {
        owner: Token::new(12),
        op: tools::Op::Store {
            at: place(root, b".GiT/config"),
            content: Box::from(&b"x"[..]),
            expect: tools::Expect::Absent,
        },
        deadline: far(),
    });
    assert_eq!(
        denied,
        ToDomain::Done { owner: Token::new(12), done: tools::Done::Failed { fault: tools::Fault::Denied } }
    );
    let linked = world.request(FromDomain::Op {
        owner: Token::new(13),
        op: tools::Op::Store { at: place(root, b"link"), content: Box::from(&b"x"[..]), expect: tools::Expect::Absent },
        deadline: far(),
    });
    assert_eq!(linked, ToDomain::Done { owner: Token::new(13), done: tools::Done::Conflict { now: None } });
    // Absence conflicts with any existing target. A digest-conditioned store
    // preserves the separate native refusal to write through a symbolic link.
    let linked = world.request(FromDomain::Op {
        owner: Token::new(14),
        op: tools::Op::Store {
            at: place(root, b"link"),
            content: Box::from(&b"x"[..]),
            expect: tools::Expect::Is { version: tools::Version::new(skein_io::digest::digest(b"old").0) },
        },
        deadline: far(),
    });
    assert_eq!(linked, ToDomain::Done { owner: Token::new(14), done: tools::Done::Linked });
    assert_eq!(world.content(b"safe"), b"old");
    world.settle();
}

#[test]
fn checks_that_pass_fail_and_time_out() {
    let mut world = World::new(6, &[]);
    for (number, name, expected) in [
        (20, &b"check-ok"[..], run::Exit::Code { code: 0 }),
        (21, &b"check-fail"[..], run::Exit::Code { code: 1 }),
        (22, &b"check-hang"[..], run::Exit::TimedOut),
    ] {
        let done = world.request(FromDomain::Check {
            owner: Token::new(number),
            program: run::Place { root: world.root(), path: Box::from(name) },
            deadline: Time::from_nanos(10_000_000),
            tail: 8,
        });
        assert_eq!(
            done,
            ToDomain::Checked {
                owner: Token::new(number),
                ran: run::Ran { exit: expected, output: Box::new([]), cut: 0 }
            }
        );
    }
    world.settle();
}

#[test]
fn a_command_that_hangs_is_stopped_at_its_deadline_and_answered_once_gone() {
    let mut world = World::new(7, &[]);
    let root = world.root();
    let done = world.request(FromDomain::Op {
        owner: Token::new(30),
        op: tools::Op::Spawn { cwd: place(root, b""), command: Box::from(&b"hang"[..]), head: 4, tail: 4, after: None },
        deadline: Time::from_nanos(1_000_000),
    });
    assert_eq!(
        done,
        ToDomain::Done {
            owner: Token::new(30),
            done: tools::Done::Exited {
                exit: tools::Exit::TimedOut,
                head: Box::new([]),
                tail: Box::new([]),
                dropped: 0,
            }
        }
    );
    world.settle();
}

#[test]
fn a_command_that_floods_its_output_keeps_its_head_and_tail() {
    let mut world = World::new(8, &[]);
    let output: Vec<u8> = (0_u32..4096).map(|at| b'a' + u8::try_from(at % 26).expect("alphabet")).collect();
    let done = world.flood(Token::new(40), &output, 8, 8);
    assert_eq!(
        done,
        ToDomain::Done {
            owner: Token::new(40),
            done: tools::Done::Exited {
                exit: tools::Exit::Code { code: 0 },
                head: Box::from(&output[..8]),
                tail: Box::from(&output[output.len() - 8..]),
                dropped: 4080,
            }
        }
    );
    world.settle();
}

#[test]
fn a_megabyte_of_output_takes_few_deliveries() {
    let mut world = World::new(8, &[]);
    let output: Vec<u8> = (0_u32..1_048_576).map(|at| b'a' + u8::try_from(at % 26).expect("alphabet")).collect();
    assert_eq!(
        world.flood(Token::new(40), &output, 8, 8),
        ToDomain::Done {
            owner: Token::new(40),
            done: tools::Done::Exited {
                exit: tools::Exit::Code { code: 0 },
                head: Box::from(&output[..8]),
                tail: Box::from(&output[output.len() - 8..]),
                dropped: 1_048_560,
            },
        }
    );
    assert!(world.deliveries <= 1024);
    world.settle();
}

#[test]
fn referee_writes_only_within_a_writable_root_and_answers_each_call_once() {
    let mut readonly = World::read_only(9, &[Item::file(b"kept", b"old")]);
    let denied = readonly.request(FromDomain::Op {
        owner: Token::new(41),
        op: tools::Op::Store {
            at: place(readonly.root(), b"kept"),
            content: Box::from(&b"new"[..]),
            expect: tools::Expect::Absent,
        },
        deadline: far(),
    });
    assert_eq!(
        denied,
        ToDomain::Done { owner: Token::new(41), done: tools::Done::Failed { fault: tools::Fault::Denied } }
    );
    assert_eq!(readonly.content(b"kept"), b"old");
    readonly.settle();

    let mut writable = World::new(10, &[Item::file(b"kept", b"old")]);
    let loaded = writable.request(FromDomain::Op {
        owner: Token::new(42),
        op: tools::Op::Load { at: place(writable.root(), b"kept"), max: 16 },
        deadline: far(),
    });
    let version = match loaded {
        ToDomain::Done { done: tools::Done::Loaded { version, .. }, .. } => version,
        other @ (ToDomain::Done { .. }
        | ToDomain::Read { .. }
        | ToDomain::Probed { .. }
        | ToDomain::Checked { .. }
        | ToDomain::Aborted { .. }) => panic!("expected a load: {other:?}"),
    };
    let stored = writable.request(FromDomain::Op {
        owner: Token::new(43),
        op: tools::Op::Store {
            at: place(writable.root(), b"kept"),
            content: Box::from(&b"new"[..]),
            expect: tools::Expect::Is { version },
        },
        deadline: far(),
    });
    assert!(matches!(stored, ToDomain::Done { owner, done: tools::Done::Stored { .. } } if owner == Token::new(43)));
    assert_eq!(writable.content(b"kept"), b"new");
    writable.settle();
}

#[test]
fn full_kernel_trace_and_file_terminal_replay_through_the_shared_kit() {
    let trace = skein_world::domain::assert_replays(40, 41, |seed| {
        let bytes = vec![b'x'; usize::try_from(seed % 16 + 1).expect("bounded seeded file")];
        let mut world = World::new(seed, &[Item::file(b"file", &bytes)]);
        let terminal = world.request(FromDomain::Op {
            owner: Token::new(40),
            op: tools::Op::Load { at: place(world.root(), b"file"), max: 16 },
            deadline: far(),
        });
        world.settle();
        (world.trace().to_vec(), terminal)
    });
    assert!(!trace.is_empty(), "the complete file IO trace was retained");
}

#[test]
fn successive_harness_runs_preserve_the_absolute_deadline_clock() {
    let mut world = World::new(42, &[]);
    let first = world.request(FromDomain::Check {
        owner: Token::new(50),
        program: run::Place { root: world.root(), path: b"check-hang".as_slice().into() },
        deadline: Time::from_nanos(10_000_000),
        tail: 8,
    });
    assert!(matches!(first, ToDomain::Checked { ran: run::Ran { exit: run::Exit::TimedOut, .. }, .. }));
    let before = world.now();
    assert!(before >= Time::from_nanos(10_000_000), "the first deadline elapsed");
    let second = world.request(FromDomain::Check {
        owner: Token::new(51),
        program: run::Place { root: world.root(), path: b"check-hang".as_slice().into() },
        deadline: Time::from_nanos(1_000_000),
        tail: 8,
    });
    assert!(matches!(second, ToDomain::Checked { ran: run::Ran { exit: run::Exit::TimedOut, .. }, .. }));
    assert_eq!(world.now(), before, "an expired absolute deadline does not restart its duration");
    world.settle();
}

#[test]
fn listing_and_searching_the_directory_itself_reach_io_as_dot() {
    let mut world = World::new(11, &[Item::file(b"a", b"x")]);
    let root = world.root();
    let listed = world.request(FromDomain::Op {
        owner: Token::new(50),
        op: tools::Op::List {
            at: place(root, b""),
            depth: smith_domain::tools::Depth::Whole,
            glob: None,
            max: 16,
            max_bytes: 2048,
        },
        deadline: far(),
    });
    assert!(matches!(listed, ToDomain::Done { done: tools::Done::Listed { ref entries, more: 0 }, .. }
        if entries.is_empty()));
    let searched = world.request(FromDomain::Op {
        owner: Token::new(51),
        op: tools::Op::Search { at: place(root, b""), pattern: Box::from(&b"x"[..]), glob: None, hits: 8, bytes: 128 },
        deadline: far(),
    });
    assert!(matches!(searched, ToDomain::Done { done: tools::Done::Found { .. }, .. }), "{searched:?}");
    world.settle();
}

#[test]
fn the_configured_environment_is_seen_by_commands_and_checks_and_none_by_searches() {
    use smith_protocol_machine::Variable;
    let variables = [
        Variable { name: b"BUILD_FLAG".as_slice().into(), value: b"whole=value".as_slice().into() },
        Variable { name: b"EMPTY".as_slice().into(), value: Box::new([]) },
    ];
    let mut world = World::configured(42, &[], &variables);
    for owner in 50..52 {
        let done = world.request(FromDomain::Op {
            owner: Token::new(owner),
            op: tools::Op::Spawn {
                cwd: place(world.root(), b""),
                command: b"true".as_slice().into(),
                head: 8,
                tail: 8,
                after: None,
            },
            deadline: world.now().saturating_add(skein_lib::Duration::from_secs(1)),
        });
        assert!(matches!(
            done,
            ToDomain::Done { done: tools::Done::Exited { exit: tools::Exit::Code { code: 0 }, .. }, .. }
        ));
    }
    let checked = world.request(FromDomain::Check {
        owner: Token::new(52),
        program: run::Place { root: world.root(), path: b"check-ok".as_slice().into() },
        deadline: world.now().saturating_add(skein_lib::Duration::from_secs(1)),
        tail: 8,
    });
    assert!(matches!(checked, ToDomain::Checked { ran: run::Ran { exit: run::Exit::Code { code: 0 }, .. }, .. }));
    let searched = world.request(FromDomain::Op {
        owner: Token::new(53),
        op: tools::Op::Search {
            at: place(world.root(), b""),
            pattern: b"needle".as_slice().into(),
            glob: None,
            hits: 8,
            bytes: 64,
        },
        deadline: world.now().saturating_add(skein_lib::Duration::from_secs(1)),
    });
    assert!(matches!(searched, ToDomain::Done { done: tools::Done::Found { .. }, .. }));
    let observed = world.spawns();
    assert_eq!(observed.len(), 4);
    let expected: &[Box<[u8]>] = &[Box::from(b"BUILD_FLAG=whole=value".as_slice()), Box::from(b"EMPTY=".as_slice())];
    for spawn in &observed[..3] {
        assert_eq!(spawn.environment.as_ref(), expected);
    }
    assert_eq!(observed[3].program.as_ref(), b"/usr/bin/rg");
    assert!(observed[3].environment.is_empty());
    world.settle();
}

#[test]
fn a_deadline_stops_both_the_leader_and_its_pipe_holding_group_child() {
    let mut world = World::new(98, &[]);
    let done = world.request(FromDomain::Op {
        owner: Token::new(80),
        op: tools::Op::Spawn {
            cwd: place(world.root(), b""),
            command: Box::from(b"fork".as_slice()),
            head: 4,
            tail: 4,
            after: None,
        },
        deadline: Time::from_nanos(1_000_000),
    });
    assert!(matches!(done, ToDomain::Done { done: tools::Done::Exited { exit: tools::Exit::TimedOut, .. }, .. }));
    assert!(world.trace().iter().any(|entry| entry.contains("to: Group")), "actual native group signal");
    world.settle();
}

#[test]
fn a_stop_reaches_the_group_and_settles_both_output_pipes_before_cancelled() {
    let mut world = World::new(99, &[]);
    let done = world.cancelled_request(FromDomain::Op {
        owner: Token::new(81),
        op: tools::Op::Spawn {
            cwd: place(world.root(), b""),
            command: Box::from(b"fork".as_slice()),
            head: 4,
            tail: 4,
            after: None,
        },
        deadline: far(),
    });
    assert_eq!(done, ToDomain::Done { owner: Token::new(81), done: tools::Done::Cancelled });
    assert!(world.trace().iter().any(|entry| entry.contains("to: Group")), "cancel targets the group");
    world.settle();
}

#[test]
fn a_naturally_exited_leader_closes_the_group_while_its_child_still_holds_the_pipes() {
    let mut world = World::new(100, &[]);
    let done = world.request(FromDomain::Op {
        owner: Token::new(82),
        op: tools::Op::Spawn {
            cwd: place(world.root(), b""),
            command: Box::from(b"fork-exit".as_slice()),
            head: 4,
            tail: 4,
            after: None,
        },
        deadline: far(),
    });
    assert!(matches!(
        done,
        ToDomain::Done { done: tools::Done::Exited { exit: tools::Exit::Code { code: 0 }, .. }, .. }
    ));
    assert!(world.now() < far(), "group cleanup does not wait for the command deadline");
    world.settle();
}

#[test]
fn selected_paths_form_a_tree_over_the_shared_fake_machine_and_settle_once() {
    let mut world = World::new(
        9211,
        &[
            Item::directory(b"src"),
            Item::directory(b"src/deep"),
            Item::file(b"src/deep/a.rs", b""),
            Item::directory(b"empty"),
            Item::directory(b"ignored"),
            Item::file(b"ignored/drop.rs", b""),
        ],
    );
    let selected = b"./src/deep/a.rs\0./src/z.rs\0";
    for depth in [tools::Depth::Whole, tools::Depth::Levels(1)] {
        let answer = world.listing(Token::new(91), selected, depth, 2, 2048);
        let (entries, more) = match answer {
            ToDomain::Done { done: tools::Done::Listed { entries, more }, .. } => (entries, more),
            other @ (ToDomain::Done { .. }
            | ToDomain::Read { .. }
            | ToDomain::Probed { .. }
            | ToDomain::Checked { .. }
            | ToDomain::Aborted { .. }) => panic!("selected listing: {other:?}"),
        };
        assert_eq!(entries[0].name.as_ref(), b"src");
        if depth == tools::Depth::Whole {
            assert_eq!(entries[1].name.as_ref(), b"src/deep");
            assert_eq!(more, 2);
        } else {
            assert_eq!(entries.len(), 1);
            assert_eq!(more, 0);
        }
        world.settle();
    }
}

#[test]
fn removal_checks_the_file_version_and_removes_empty_directories_without_following_links() {
    let mut world = World::new(
        115,
        &[
            Item::directory(b"src"),
            Item::file(b"src/a", b"old"),
            Item::directory(b"empty"),
            Item::link(b"alias", b"src"),
            Item::link(b"link", b"src/a"),
        ],
    );
    let root = world.root();
    let old = tools::Version::new(skein_io::digest::digest(b"old").0);
    for (number, path, expect, terminal) in [
        (50, b"src/a".as_slice(), tools::Expect::Absent, tools::Done::Conflict { now: Some(old) }),
        (51, b"src", tools::Expect::Absent, tools::Done::NotEmpty),
        (52, b"alias/a", tools::Expect::Is { version: old }, tools::Done::Linked),
        (53, b"link", tools::Expect::Is { version: old }, tools::Done::Linked),
        (54, b"empty", tools::Expect::Absent, tools::Done::Removed),
    ] {
        let owner = Token::new(number);
        let done = world.request(FromDomain::Op {
            owner,
            op: tools::Op::Remove { at: place(root, path), expect },
            deadline: far(),
        });
        assert_eq!(done, ToDomain::Done { owner, done: terminal });
    }
    world.change_behind_session(b"src/a", b"new");
    let done = world.request(FromDomain::Op {
        owner: Token::new(55),
        op: tools::Op::Remove { at: place(root, b"src/a"), expect: tools::Expect::Is { version: old } },
        deadline: far(),
    });
    assert!(matches!(done, ToDomain::Done { done: tools::Done::Conflict { .. }, .. }));
    assert_eq!(world.content(b"src/a"), b"new");
    let current = tools::Version::new(skein_io::digest::digest(b"new").0);
    assert_eq!(
        world.request(FromDomain::Op {
            owner: Token::new(56),
            op: tools::Op::Remove { at: place(root, b"src/a"), expect: tools::Expect::Is { version: current } },
            deadline: far()
        }),
        ToDomain::Done { owner: Token::new(56), done: tools::Done::Removed }
    );
    assert_eq!(
        world.request(FromDomain::Op {
            owner: Token::new(57),
            op: tools::Op::Load { at: place(root, b"src/a"), max: 10 },
            deadline: far()
        }),
        ToDomain::Done { owner: Token::new(57), done: tools::Done::Missing }
    );
    world.settle();
}
