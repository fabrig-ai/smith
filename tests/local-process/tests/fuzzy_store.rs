//! The store/io composition uses the shared world's seeded power-loss model.
use skein_fake_machine::{Machine as Files, Opened};
use skein_io::{
    file,
    file_layer::{self, FileIo},
    kernel::{self, Complete, Fd, Op, Submit},
};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_shell::Host;
use skein_sim::{Answer, Call, Handle};
use skein_world::{Cut, Inherited, Machine, Memory, Referee, StartupRoot, World};
use smith_local_domain as local;
use smith_local_protocol::{Action, History, Store, StoreLimits, StoreRequest};

#[global_allocator]
static HEAP: skein_world::domain::heap::Counting = skein_world::domain::heap::Counting;

const SIGNAL: Token = Token::new(u64::MAX);
fn limits() -> StoreLimits {
    StoreLimits { file_bytes: 4096, turns: 2, deliveries: 2, scan_bytes: 4096 }
}
struct Owner {
    store: Store,
    io: FileIo,
    signal: Fd,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    events: Queue<file::Event>,
    actions: Queue<Action>,
    reserved: Option<u64>,
    next: Option<local::ChatState>,
    settled: bool,
    submitted: u32,
    fresh: bool,
    refusal: Option<local::StoreFailure>,
    expects_old: bool,
}
fn make(inherited: &Inherited) -> Owner {
    let fd = inherited.roots[0].1;
    let mut io = FileIo::with_whole_limit(2, 4096, 8, 4096, Duration::from_secs(30));
    io.seed_randomness(19);
    let root = io.adopt_root(fd).unwrap();
    let mut store = Store::new(
        Token::new(1),
        root,
        fd,
        limits(),
        smith_local_process_world::process::lower_configuration().channel_endpoints,
    )
    .unwrap();
    let mut actions = Queue::with_capacity(3);
    store.request(StoreRequest::Load, Time::ZERO, &mut actions);
    Owner {
        store,
        io,
        signal: inherited.signal,
        completions: Queue::with_capacity(4),
        submissions: Queue::with_capacity(4),
        events: Queue::with_capacity(1),
        actions,
        reserved: None,
        next: None,
        settled: false,
        submitted: 0,
        fresh: inherited.roots[0].0.as_ref() == b"fresh",
        refusal: None,
        expects_old: inherited.roots[0].0.as_ref() == b"older",
    }
}
impl Owner {
    fn actions(&mut self, now: Time) {
        for _ in 0..3 {
            if !self.io.takes() || self.submissions.room() < 2 {
                break;
            }
            let Some(action) = self.actions.pop() else { break };
            match action {
                Action::Io { request, deadline } => {
                    if self.expects_old {
                        assert!(
                            matches!(
                                request,
                                file::Request::Load { .. } | file::Request::Scan { .. } | file::Request::Close { .. }
                            ),
                            "older root requested mutation: {request:?}"
                        );
                    }
                    file_layer::down_until(&mut self.io, deadline, request, &mut self.events, &mut self.submissions)
                }
                Action::Domain(local::Event::Loaded { state, .. }) => {
                    assert!(!self.expects_old, "an older root must never load");
                    let mut state = state.unwrap_or(local::ChatState {
                        identity: smith_domain::run::Identity::new([42; 16]),
                        activation: 0,
                        next_message: 1,
                        read: None,
                    });
                    state.activation += 1;
                    self.next = Some(state);
                    self.store.request(
                        StoreRequest::SaveState {
                            state,
                            history: if self.fresh { History::Fresh } else { History::Kept },
                        },
                        now,
                        &mut self.actions,
                    );
                }
                Action::Domain(local::Event::StateSaved) => {
                    self.reserved = Some(self.next.unwrap().activation);
                    self.store.close(now, &mut self.actions);
                }
                Action::Domain(local::Event::StoreFailed { reason, .. }) => {
                    assert!(self.expects_old, "current stores must not refuse: {reason:?}");
                    self.refusal = Some(reason);
                    self.store.close(now, &mut self.actions);
                }
                Action::Fault(_) if self.expects_old => {}
                Action::Closed => self.submissions.push(Submit { op: SIGNAL, kind: Op::Close { fd: self.signal } }),
                other => panic!("store reservation terminal: {other:?}"),
            }
        }
    }
}
impl Host for Owner {
    fn iterate(&mut self, now: Time, _: Wall) {
        while let Some(complete) = self.completions.pop() {
            if complete.op == SIGNAL {
                assert!(complete.result.is_ok());
                self.settled = true;
            } else {
                file_layer::up(&mut self.io, complete, &mut self.events, &mut self.submissions);
            }
        }
        if self.io.is_due(now) {
            file_layer::expire(&mut self.io, now, &mut self.submissions);
        }
        for _ in 0..4 {
            self.actions(now);
            let Some(event) = self.events.pop() else { break };
            self.store.from_io(event, now, &mut self.actions);
        }
        self.submitted += self.submissions.len();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _: Time) -> bool {
        !self.completions.is_empty() || !self.events.is_empty() || (!self.actions.is_empty() && self.io.takes())
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.settled
            && self.io.open_files() == 0
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.actions.is_empty()
            && self.events.is_empty()
    }
    fn worst_case(&self) -> u64 {
        // Peak across generated turn loading and every recovery/cut scenario.
        // Measured 20_926 B on 2026-10-28; a quarter of headroom, rounded up.
        const STORE_SCENARIOS_CEILING: u64 = 27_000;
        STORE_SCENARIOS_CEILING
    }
    fn operations(&self) -> u32 {
        4
    }
}
struct Disk(Files);
impl Machine for Disk {
    fn open_root(&mut self, _: &[u8]) -> Result<Handle, kernel::Error> {
        Ok(Handle::new(self.0.reopen_root(0).raw()))
    }
    fn close_root(&mut self, root: Handle) {
        self.0.close(Opened::new(root.raw()));
    }
    fn step(&mut self, call: Call, answers: &mut Queue<Answer>) {
        skein_fake_machine::step(&mut self.0, call, answers);
    }
    fn cut(&mut self, cut: Cut, held: &[Handle], seed: u64) {
        for handle in held {
            self.0.close(Opened::new(handle.raw()));
        }
        assert_eq!(cut, Cut::PowerLoss);
        self.0.crash(seed);
    }
}
#[derive(Default)]
struct Judge {
    activations: Vec<u64>,
    reserved: bool,
    refused: bool,
}
impl Referee<Owner> for Judge {
    fn act(&mut self, _: Time, _: &mut [Owner]) {}
    fn observe(&mut self, _: Time, owners: &[Owner]) {
        self.refused |= owners[0].refusal.is_some();
        if let Some(activation) = owners[0].reserved {
            if !self.reserved {
                assert!(self.activations.last().is_none_or(|prior| *prior < activation));
                self.activations.push(activation);
                self.reserved = true;
            }
        } else {
            self.reserved = false;
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        None
    }
    fn overdue(&self, _: Time) -> Option<String> {
        None
    }
    fn passed(&self) -> bool {
        !self.activations.is_empty() || self.refused
    }
}
fn world(seed: u64) -> World<Owner, Judge, Disk> {
    configured(seed, false)
}

fn configured(seed: u64, fresh: bool) -> World<Owner, Judge, Disk> {
    configured_mode(seed, fresh, Initial::Current, skein_sim::Config::calm())
}

#[derive(Clone, Copy, Debug)]
enum Initial {
    Current,
    Window,
    Empty,
    Unrecorded,
    StoreLayout,
    TranscriptLayout,
}

fn initial_files(initial: Initial) -> Vec<skein_fake_machine::Item> {
    use skein_fake_machine::Item;
    let state = local::ChatState {
        identity: smith_domain::run::Identity::new([42; 16]),
        activation: 1,
        next_message: 1,
        read: None,
    };
    match initial {
        Initial::Empty => Vec::new(),
        Initial::Window => {
            let mut items = initial_files(Initial::Current);
            let endpoints = smith_local_process_world::process::lower_configuration().channel_endpoints;
            let turn = smith_domain::Turn {
                version: smith_domain::session::record::VERSION,
                kind: smith_domain::session::record::Kind::First(Box::new(smith_domain::session::record::Opener {
                    affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
                    account: None,
                    backend: smith_domain::session::record::Backend::Direct,
                })),
                endpoint: Some(smith_domain::session::llm::Endpoint(0)),
                dialect: 1,
                model: b"model".as_slice().into(),
                effort: None,
                sequence: 1,
                usage: smith_domain::session::llm::Usage::ZERO,
                spent: 0,
                jobs: 0,
                messages: Box::new([smith_domain::session::llm::Message {
                    role: smith_domain::session::llm::Role::Assistant,
                    content: Box::new([smith_domain::session::llm::Block::Text {
                        text: Box::new([b'x'; 3000]),
                        replay: None,
                    }]),
                }]),
            };
            let file =
                smith_local_protocol::save_turn(1, None, true, &turn, &smith_transcript::CEILINGS, &endpoints).unwrap();
            assert!(file.bytes.len() > 3000 && file.bytes.len() < 4096);
            items.push(Item::file(&file.name, &file.bytes));
            items
        }
        Initial::Current => vec![
            Item::file(b"format", &smith_local_protocol::format::file().bytes),
            Item::file(b"state", &smith_local_protocol::save_state(state).bytes),
            Item::file(&delivery().name, &delivery().bytes),
        ],
        Initial::Unrecorded | Initial::StoreLayout | Initial::TranscriptLayout => {
            let mut items = vec![
                Item::file(b"state", &smith_local_protocol::save_state(state).bytes),
                Item::file(b"fresh", &smith_local_protocol::save_state(state).bytes),
                Item::file(b"0000000001.turn", b"old payload"),
                Item::file(&delivery().name, &delivery().bytes),
            ];
            if !matches!(initial, Initial::Unrecorded) {
                let mut layouts = smith_local_protocol::format::layouts().into_parts();
                match initial {
                    Initial::StoreLayout => layouts.store[0] ^= 1,
                    Initial::TranscriptLayout => layouts.transcript[0] ^= 1,
                    Initial::Current | Initial::Window | Initial::Empty | Initial::Unrecorded => unreachable!(),
                }
                let record = smith_local_protocol::local_store_v1::Layouts::new(
                    &smith_local_protocol::local_store_v1::CEILINGS,
                    layouts,
                )
                .unwrap();
                let mut writer = skein_lib::Writer::new(usize::try_from(record.measure()).unwrap());
                record.encode(&mut writer).unwrap();
                items.push(Item::file(b"format", &writer.finish()));
            }
            items
        }
    }
}

fn delivery() -> smith_local_protocol::File {
    smith_local_protocol::save_delivery(
        &local::DeliveryRecord {
            name: smith_domain::run::CallName { activation: 1, completion: 1, position: 0 },
            state: local::DeliveryState::Intent(local::DeliveryIntent { directories: Box::new([]) }),
            landed: Box::new([]),
        },
        4096,
    )
    .unwrap()
}

fn configured_mode(seed: u64, fresh: bool, initial: Initial, config: skein_sim::Config) -> World<Owner, Judge, Disk> {
    drop(smith_local_process_world::process::lower_configuration());
    let mut files = Files::new();
    let root = files.lay(&initial_files(initial));
    files.close(root);
    let mut world = World::new(seed, config, Judge::default(), Memory::Checked).with_machine(Disk(files));
    world.spawn_restartable(
        vec![StartupRoot {
            name: if matches!(initial, Initial::Unrecorded | Initial::StoreLayout | Initial::TranscriptLayout) {
                b"older".as_slice().into()
            } else if fresh {
                b"fresh".as_slice().into()
            } else {
                b"chat".as_slice().into()
            },
            path: b"chat".as_slice().into(),
        }],
        make,
    );
    world
}
#[test]
fn a_power_loss_at_any_store_operation_resumes_without_reusing_an_activation() {
    let baseline = world(70).run();
    let operations = baseline.procs[0].submitted;
    assert!(operations >= 12);
    for before in 0..operations {
        let mut world = world(70 + u64::from(before));
        world.cut(0, before, Cut::PowerLoss);
        let outcome = world.run();
        assert!(outcome.procs[0].reserved.is_some(), "cut {before}");
        assert_eq!(outcome.machine.0.open_handles(), 0);
    }
}

#[test]
fn a_fresh_journal_resumes_its_reset_after_a_power_loss() {
    let baseline = configured(80, true).run();
    for before in 0..baseline.procs[0].submitted {
        let mut world = configured(80 + u64::from(before), true);
        world.cut(0, before, Cut::PowerLoss);
        let mut outcome = world.run();
        let root = outcome.machine.0.reopen_root(0);
        let names = outcome.machine.0.list(root, 6, 4096).unwrap();
        assert_eq!(
            names.iter().filter(|(_, name)| name.len() == 42 || name.as_ref() == b"fresh").count(),
            0,
            "cut {before}"
        );
        outcome.machine.0.close(root);
    }
}

fn sweep(seed_base: u64, fresh: bool, initial: Initial) -> u32 {
    let baseline = configured_mode(seed_base, fresh, initial, skein_sim::Config::calm()).run();
    for before in 0..baseline.procs[0].submitted {
        let seed = seed_base + u64::from(before);
        let mut world = configured_mode(seed, fresh, initial, skein_sim::Config::calm());
        world.cut(0, before, Cut::PowerLoss);
        let outcome = world.run();
        assert!(outcome.procs[0].reserved.is_some(), "{initial:?}, seed {seed}, cut {before}");
        assert_eq!(outcome.machine.0.open_handles(), 0);
    }
    baseline.procs[0].submitted
}

#[test]
fn interrupted_initial_marker_creation_resumes_without_adopting_old_records() {
    sweep(7000, false, Initial::Empty);
}

fn disk_snapshot(files: &mut Files) -> Vec<(Box<[u8]>, skein_fake_machine::Facts, Vec<u8>)> {
    let root = files.reopen_root(0);
    let names = files.list(root, 8, 4096).unwrap();
    let mut snapshot = Vec::new();
    for (_, name) in names {
        let file = files.open(root, &name, skein_fake_machine::How::ReadNoFollow).unwrap();
        snapshot.push((name, files.stat(file), files.read(file, 0, 4096).unwrap()));
        files.close(file);
    }
    files.close(root);
    snapshot
}

#[test]
fn older_refusal_is_immutable_under_power_loss_and_injected_read_errors() {
    for initial in [Initial::Unrecorded, Initial::StoreLayout, Initial::TranscriptLayout] {
        let mut expected = Files::new();
        let root = expected.lay(&initial_files(initial));
        expected.close(root);
        let expected = disk_snapshot(&mut expected);
        for io_error in [0, 300, 1000] {
            let mut config = skein_sim::Config::calm();
            config.faults.io_error = io_error;
            let baseline = configured_mode(8000, false, initial, config).run();
            for before in 0..baseline.procs[0].submitted {
                let seed = 8000 + u64::from(before) + u64::from(io_error);
                let mut world = configured_mode(seed, false, initial, config);
                world.cut(0, before, Cut::PowerLoss);
                let mut outcome = world.run();
                assert!(outcome.procs[0].refusal.is_some(), "{initial:?} seed {seed}");
                assert_eq!(expected, disk_snapshot(&mut outcome.machine.0), "{initial:?} seed {seed}");
                assert_eq!(outcome.machine.0.open_handles(), 0);
            }
        }
    }
}

#[test]
#[ignore = "explicit 713 extra-seed QA sweep, outside the normal gate"]
fn extra_seed_713_store_power_loss_proof() {
    const KEPT_BASE: u64 = 713_000;
    const FRESH_BASE: u64 = 714_000;
    const FIRST_CREATE_BASE: u64 = 715_000;
    const SECOND_CREATE_BASE: u64 = 716_000;
    for (base, fresh, initial) in [
        (KEPT_BASE, false, Initial::Current),
        (FRESH_BASE, true, Initial::Current),
        (FIRST_CREATE_BASE, false, Initial::Empty),
        (SECOND_CREATE_BASE, true, Initial::Empty),
    ] {
        let operations = sweep(base, fresh, initial);
        println!("713 extra sweep: {initial:?}, fresh={fresh}, seed base {base}, cuts 0..{operations}");
    }
}

#[test]
fn generated_whole_turn_loading_and_recovery_fit_the_shared_metered_store_bound() {
    sweep(9000, false, Initial::Window);
}
