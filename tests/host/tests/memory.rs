//! Every retained V2 inventory and terminal path is metered by skein's allocator
//! (programming-model.md, section 6.3; domain/host.md, sections 4, 6 and 10).
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_world::domain::heap::{Counting, Meter};
use smith_host_domain::{
    self as host, Answer, AnsweredCall, Ask, CallName, Delivered, Delivery, Directory, Down, Effect, Grant, Input,
    Limits, Output, Receipt, Reply, RunResult, Start, Turn, Up,
};
use smith_host_domain::{parent, process};

#[global_allocator]
static HEAP: Counting = Counting;

fn bytes(length: u64) -> Box<[u8]> {
    vec![b'x'; usize::try_from(length).expect("small test length")].into_boxed_slice()
}
fn maximum_start(limits: Limits) -> Start {
    Start {
        live: None,
        messages: Box::default(),
        logical_run: Token::new(7),
        identity: None,
        activation: 1,
        workspace: (limits.directories > 0).then_some(Token::new(8)),
        charter: smith_host_world::charter_value(limits.charter_bytes),
        transcript: Some(smith_host_world::transcript_value(limits.transcript_bytes)),
        answered: if limits.answered_bytes > 0 {
            Box::new([AnsweredCall {
                name: CallName { activation: 1, completion: 1, position: 0 },
                tool: Box::from(&b"x"[..]),
                reply: smith_host_domain::SavedReply::Host { error: false, body: bytes(limits.answered_bytes - 1) },
            }])
        } else {
            Box::new([])
        },
        directories: (0..limits.directories)
            .map(|at| Directory {
                host: Box::from(b"/host".as_slice()),
                root: Token::new(u64::from(at) + 1),
                name: vec![
                    b'a' + u8::try_from(at).expect("few dirs");
                    usize::try_from(limits.name_bytes).expect("small")
                ]
                .into_boxed_slice(),
                writable: true,
                git: true,
                conflicts: (0..limits.conflicts)
                    .map(|index| {
                        let mut path = bytes(u64::from(limits.path_bytes));
                        *path.last_mut().expect("positive exact path cap") =
                            b'a' + u8::try_from(index).expect("finite conflict count");
                        path
                    })
                    .collect(),
            })
            .collect(),
        grants: (0..limits.accounts)
            .map(|account| Grant {
                account,
                generation: 1,
                valid: Duration::from_secs(100),
                form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
            })
            .collect(),
    }
}
fn maximum_receipts() -> Delivered {
    Delivered::new(
        (0..host::MAX_DIRECTORIES)
            .map(|directory| Receipt::new(directory, bytes(512)).expect("maximum receipt"))
            .collect(),
    )
    .expect("maximum delivered")
}

struct Measured {
    domain: host::Domain,
    env: Env<Limits>,
    out: Queue<Output>,
    meter: Meter,
    bound: u64,
    held_start: Option<Start>,
    held_messages: Vec<Box<[u8]>>,
    retain_message: bool,
}
impl Measured {
    fn new(limits: Limits, ceiling: u64) -> Self {
        let out = Queue::with_capacity(host::max_out(&limits));
        let held_messages = Vec::with_capacity(usize::try_from(limits.agents).expect("finite agent count"));
        let meter = Meter::new();
        let domain = host::Domain::new(&limits);
        Self {
            domain,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            out,
            meter,
            bound: ceiling,
            held_start: None,
            held_messages,
            retain_message: false,
        }
    }
    fn step(&mut self, event: Input, retain_start: bool) -> Option<Token> {
        self.meter.start();
        host::step(&mut self.domain, &self.env, event, &mut self.out);
        self.drain(retain_start)
    }
    fn fire(&mut self, seconds: u64) {
        self.env.now = Time::ZERO.saturating_add(Duration::from_secs(seconds));
        while self.domain.is_due(self.env.now) {
            self.meter.start();
            host::fire(&mut self.domain, &self.env, &mut self.out);
            self.drain(false);
        }
    }
    fn drain(&mut self, retain_start: bool) -> Option<Token> {
        let measurement = self.meter.end();
        let mut owner = None;
        while let Some(request) = self.out.pop() {
            match request {
                Output::Process(process::Request::Spawn { owner: token, .. }) => owner = Some(token),
                Output::Process(process::Request::Send { message, .. }) => match message {
                    Down::Start { start, .. } if retain_start => {
                        assert!(self.held_start.replace(start).is_none());
                    }
                    Down::Message { text: body, .. } if self.retain_message => {
                        assert!(self.held_messages.len() < self.held_messages.capacity());
                        self.held_messages.push(body);
                    }
                    Down::Start { .. }
                    | Down::Message { .. }
                    | Down::Answer { .. }
                    | Down::Acknowledge { .. }
                    | Down::Grant { .. }
                    | Down::Cancel => {}
                },
                Output::Parent(
                    parent::Request::Started { .. }
                    | parent::Request::Admitted { .. }
                    | parent::Request::Called { .. }
                    | parent::Request::Withdrawn { .. }
                    | parent::Request::Turn { .. }
                    | parent::Request::Waiting { .. }
                    | parent::Request::Long { .. }
                    | parent::Request::LongDone { .. }
                    | parent::Request::Unread { .. }
                    | parent::Request::Unauthorized { .. }
                    | parent::Request::Refused { .. }
                    | parent::Request::Limited { .. }
                    | parent::Request::Told { .. }
                    | parent::Request::Answered { .. }
                    | parent::Request::Faulted { .. }
                    | parent::Request::MessageRefused { .. }
                    | parent::Request::Gone { .. },
                )
                | Output::Process(
                    process::Request::Read { .. }
                    | process::Request::Signal { .. }
                    | process::Request::Wait { .. }
                    | process::Request::Reap { .. },
                ) => {}
            }
        }
        self.meter.check(measurement, self.bound, &self.env.limits);
        self.domain.reclaim();
        owner
    }
    fn spawn(&mut self, client: u64) -> Token {
        let start = maximum_start(self.env.limits);
        self.step(Input::Parent(parent::Event::Spawn { client: Token::new(client), start }), false)
            .expect("valid Spawn")
    }
    fn up(&mut self, owner: Token, message: Up) {
        self.step(Input::Process(process::Event::Received { owner, message }), false);
    }
    fn start(&mut self, owner: Token) {
        self.step(Input::Process(process::Event::Spawned { owner, process: owner }), false);
        self.up(owner, Up::Admitted);
    }
    fn cleanup(&mut self, owner: Token, signals: u32, pending_send: bool) {
        if pending_send {
            self.step(Input::Process(process::Event::Sent { owner }), false);
        }
        self.step(
            Input::Process(process::Event::Exited { owner, exit: Some(smith_host_domain::Exit::Code(0)) }),
            false,
        );
        self.step(
            Input::Process(process::Event::Reaped { owner, detail: bytes(u64::from(self.env.limits.detail_bytes)) }),
            false,
        );
        self.step(Input::Process(process::Event::Hangup { owner }), false);
        for _ in 0..signals {
            self.step(Input::Process(process::Event::Signalled { owner }), false);
        }
    }
}

// Peaks for 1, 8 and 32 slots retaining maximum starts and full queued replies.
const FULL_SLOT_CEILINGS: [(u32, u64); 3] = [(1, 97_000), (8, 240_000), (32, 700_000)];

#[test]
fn maximum_v2_starts_and_full_queued_replies_fit_every_slot() {
    for (agents, ceiling) in FULL_SLOT_CEILINGS {
        let mut limits = smith_host_world::limits();
        limits.agents = agents;
        limits.charter_bytes = 4096;
        limits.transcript_bytes = 2048;
        limits.answered_bytes = 1024;
        let mut measured = Measured::new(limits, ceiling);
        let owners: Vec<Token> = (1..=u64::from(agents)).map(|client| measured.spawn(client)).collect();
        let retained = u64::from(agents) * (limits.charter_bytes + limits.transcript_bytes + limits.answered_bytes);
        assert!(measured.meter.held() >= retained, "maximum starts actually retained");
        for owner in owners {
            measured.start(owner);
            for position in 0..limits.calls {
                measured.up(
                    owner,
                    Up::Call {
                        call: Token::new(20 + u64::from(position)),
                        name: CallName { activation: 1, completion: 1, position },
                        deadline: Time::ZERO.saturating_add(Duration::from_secs(50)),
                        ask: Ask::Host {
                            tool: Box::from(&b"tool"[..]),
                            effect: Effect::Read,
                            body: bytes(limits.call_bytes),
                        },
                    },
                );
                measured.step(
                    Input::Parent(parent::Event::Answer {
                        agent: owner,
                        call: Token::new(20 + u64::from(position)),
                        reply: Reply::Host { error: false, body: bytes(limits.answer_bytes) },
                    }),
                    false,
                );
            }
            for name in 0..limits.messages {
                measured.step(
                    Input::Parent(parent::Event::Message {
                        finish_requested: false,
                        agent: owner,
                        name: Token::new(100 + u64::from(name)),
                        label: Box::new([]),
                        text: bytes(limits.message_bytes - 2),
                    }),
                    false,
                );
            }
            for number in 1..=limits.turns {
                measured.up(
                    owner,
                    Up::Turn {
                        turn: Turn {
                            opens_window: number == 1,
                            number,
                            spent: u64::from(number),
                            read: None,
                            body: smith_host_world::turn_value(limits.turn_bytes),
                        },
                    },
                );
                measured.step(Input::Parent(parent::Event::Acknowledge { agent: owner, turn: number }), false);
            }
            for _ in 0..(1 + limits.calls + limits.messages + limits.turns) {
                measured.step(Input::Process(process::Event::Sent { owner }), false);
            }
            measured.up(
                owner,
                Up::Answer {
                    answer: Answer {
                        read: None,
                        turns: limits.turns,
                        spent: u64::from(limits.turns),
                        result: RunResult::Parked,
                    },
                },
            );
            measured.cleanup(owner, 0, false);
        }
        assert_eq!(measured.domain.agents(), 0);
        assert_eq!(measured.domain.next_deadline(), None);
    }
}

// Peak while IO retains the maximum Start beside a full pre-read message queue.
const START_IO_CEILING: u64 = 220_000;

#[test]
fn maximum_start_io_ownership_coexists_with_full_pre_read_message_queue() {
    let mut limits = smith_host_world::limits();
    limits.calls = 0;
    limits.directories = 0;
    limits.accounts = 0;
    limits.charter_bytes = 65_536;
    limits.transcript_bytes = 32_768;
    limits.answered_bytes = 16_384;
    limits.message_bytes = 16_384;
    let caller = limits.charter_bytes + limits.transcript_bytes + limits.answered_bytes;
    let mut measured = Measured::new(limits, START_IO_CEILING);
    let owner = measured.spawn(1);
    measured.step(
        Input::Parent(parent::Event::Message {
            finish_requested: false,
            agent: owner,
            name: Token::new(100),
            label: Box::new([]),
            text: bytes(limits.message_bytes - 2),
        }),
        false,
    );
    measured.step(Input::Process(process::Event::Spawned { owner, process: owner }), true);
    for name in 0..limits.messages {
        measured.step(
            Input::Parent(parent::Event::Message {
                finish_requested: false,
                agent: owner,
                name: Token::new(100 + u64::from(name)),
                label: Box::new([]),
                text: bytes(limits.message_bytes - 2),
            }),
            false,
        );
    }
    assert!(measured.held_start.is_some(), "actual IO still owns the maximum first Send");
    assert!(measured.meter.held() >= caller + u64::from(limits.messages) * (limits.message_bytes - 2));
    // The scenario ceiling includes domain and caller ownership at every step;
    // dropping the caller's Start is a send terminal ownership boundary.
    measured.held_start = None;
    measured.up(owner, Up::Admitted);
    measured.up(owner, Up::Answer { answer: Answer { read: None, turns: 0, spent: 0, result: RunResult::Parked } });
    measured.cleanup(owner, 0, true);
}

// Peak while maximum delivery receipts coexist with shutdown rights.
const DELIVERY_SHUTDOWN_CEILING: u64 = 54_000;

#[test]
fn delivery_replies_and_shutdown_rights_stay_priced() {
    let limits = smith_host_world::limits();
    let mut measured = Measured::new(limits, DELIVERY_SHUTDOWN_CEILING);
    let owner = measured.spawn(1);
    measured.start(owner);
    measured.step(Input::Process(process::Event::Sent { owner }), false);
    for completion in 1..=2 {
        measured.up(
            owner,
            Up::Call {
                call: Token::new(20),
                name: CallName { activation: 1, completion, position: 0 },
                deadline: Time::ZERO.saturating_add(Duration::from_secs(5)),
                ask: Ask::Deliver { fields: smith_host_world::fields_value(limits.call_bytes) },
            },
        );
        measured.step(
            Input::Parent(parent::Event::Answer {
                agent: owner,
                call: Token::new(20),
                reply: Reply::Delivery(Delivery::Delivered(maximum_receipts())),
            }),
            false,
        );
        measured.step(Input::Process(process::Event::Sent { owner }), false);
    }
    // Keep a parent delivery right through both tree signals and EOF;
    // disconnect's bounded withdrawal snapshot is measured too.
    measured.up(
        owner,
        Up::Call {
            call: Token::new(21),
            name: CallName { activation: 1, completion: 3, position: 0 },
            deadline: Time::ZERO.saturating_add(Duration::from_secs(5)),
            ask: Ask::Deliver { fields: smith_host_world::fields_value(limits.call_bytes) },
        },
    );
    measured.fire(5);
    measured.fire(15);
    measured.fire(17);
    measured.cleanup(owner, 2, false);
    assert_eq!(measured.domain.agents(), 1);
    measured.step(
        Input::Parent(parent::Event::Answer {
            agent: owner,
            call: Token::new(21),
            reply: Reply::Delivery(Delivery::Delivered(maximum_receipts())),
        }),
        false,
    );
    assert_eq!(measured.domain.agents(), 0);
}

#[test]
fn checked_capacity_arithmetic_refuses_unrepresentable_or_incompatible_caps() {
    let mut limits = smith_host_world::limits();
    assert!(host::valid(&limits));
    limits.answer_bytes = Delivered::worst_case() - 1;
    assert!(!host::valid(&limits));
    limits = smith_host_world::limits();
    limits.messages = u32::MAX;
    assert!(!host::valid(&limits));
    limits = smith_host_world::limits();
    limits.turns = 0;
    assert!(!host::valid(&limits));
}

// Peak while a spawning agent owns its maximum carried Start messages.
const CARRIED_START_CEILING: u64 = 13_000;

#[test]
fn maximum_carried_start_messages_are_owned_and_priced_during_spawn() {
    let limits = smith_host_world::limits();
    let mut measured = Measured::new(limits, CARRIED_START_CEILING);
    let mut start = maximum_start(limits);
    start.messages = (0..limits.messages)
        .map(|name| host::Message {
            finish_requested: false,
            name: Token::new(u64::from(name)),
            label: Box::default(),
            text: bytes(limits.message_bytes - 2),
        })
        .collect();
    let owner = measured
        .step(Input::Parent(parent::Event::Spawn { client: Token::new(1), start }), false)
        .expect("maximum carried Start admitted");
    assert!(
        measured.meter.held() >= u64::from(limits.messages) * (limits.message_bytes - 2),
        "actual message payloads remain retained until spawn settles"
    );
    measured.step(Input::Process(process::Event::Unspawned { owner, detail: Box::default() }), false);
    measured.domain.reclaim();
    assert_eq!(measured.domain.agents(), 0);
}

#[path = "memory/inline.rs"]
mod inline;
