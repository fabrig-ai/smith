//! Counting-allocator driver for agent memory contracts (protocol/limits.md, sections 5 and 11).
//! Containers are allocated before the meter; every entry point drains output and reclaims.

use crate::TIGHT;
use skein_lib::{Duration, Env, Queue, ReplyTo, Rng, Time, Token, Wall};
use skein_world::domain::heap::Meter;
use smith_domain::llm::{Completion, Decoded, Failure, Problem, Prompt, Said, Served, Stop, Usage};
use smith_domain::run::charter::{Endpoint, Families, Grants, Llm, Tools};
use smith_domain::run::outcome::{
    Change, ChangeSpec, Declared, DeclaredFailure, Field, Item, OutcomeSpec, Report, TextSpec,
};
use smith_domain::run::outcome::{Verdict, VerdictRule};
use smith_domain::run::{self, Ask, Charter};
use smith_domain::run::{Directory, Workspace};
use smith_domain::tools::{Call, Done, Entry, Exit, Fault, Hit, Kind, Name, Op, Part, Path, Version};
use smith_domain::{Domain, Event, Limits, Request, fire, max_out, resume, step};

use fixture::{charter, workspace};
use random::completion_owned;

mod fixture;
mod random;
mod scripted;

pub use scripted::Memory;

pub use fixture::LIMITS;

fn bytes(len: u64) -> Box<[u8]> {
    vec![b'x'; usize::try_from(len).expect("a test length fits")].into_boxed_slice()
}

fn name(text: &[u8]) -> Name {
    Name::new(text.into()).expect("a name")
}

fn path(names: &[&[u8]]) -> Path {
    Path { absolute: false, parts: names.iter().map(|text| Part::Name { name: name(text) }).collect() }
}

fn index(rng: &mut Rng, len: usize) -> usize {
    usize::try_from(rng.below(u64::try_from(len).expect("small"))).expect("an index")
}

/// What the domain asked for and the driver has yet to end, as the driver
/// keeps it: tokens and kinds, nothing the domain allocated.
#[derive(Clone, Copy, Debug)]
enum Asked {
    /// A call to an LLM, which may finish and ask for sub-agents if
    /// offered; and whether it was cancelled.
    Complete {
        owner: Token,
        finish: bool,
        agents: bool,
        receiving: Receiving,
        purpose: smith_domain::llm::Purpose,
        asker: smith_domain::llm::Asker,
        cancelled: bool,
    },
    Job {
        owner: Token,
        act: smith_domain_tools::JobAct,
    },
    Io {
        owner: Token,
        op: OpKind,
        cancelled: bool,
    },
    Read {
        owner: Token,
    },
    Probe {
        owner: Token,
    },
    Check {
        owner: Token,
        aborted: bool,
    },
    Delivery {
        owner: Token,
        host_run: Token,
        stopped: bool,
    },
}

/// The families of requests that may be cancelled.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Family {
    Llm,
    Io,
    Check,
    Delivery,
}

#[derive(Clone, Copy, Debug)]
enum OpKind {
    Load,
    Scan,
    Store,
    Spawn,
    Search { hits: u32, byte_cap: u32 },
}

/// Receiving contract observed on this actual root request, independently of
/// stored history capacity. Every terminal must fit all three allowances.
#[derive(Clone, Copy, Debug)]
struct Receiving {
    bytes: u64,
    blocks: u32,
    decoded: u64,
}

/// The driver: what is in flight, and the runs, in containers allocated
/// before the meter's base that never grow past their capacity.
struct Driver {
    rng: Rng,
    asked: Vec<Asked>,
    runs: Vec<(Token, Token)>,
    workers: u64,
    running: Option<smith_domain_tools::JobNumber>,
    /// Requests seen, by kind: completions, io, looks, checks, pushes,
    /// Requests: completions, io, looks, checks, pushes, answers; then actual
    /// full searches, passing/failing checks, delivered/stale/failed delivery,
    /// delivery after parent cancellation, full provider terminals, invalid semantic
    /// feedback, refused starts, provider terminals winning cancellation, and
    /// invalid decoded-call feedback.
    seen: [u32; 18],
}

impl Driver {
    /// Takes what an entry point asked for, keeping what the driver needs to
    /// end it, and drops the rest.
    fn drain(&mut self, out: &mut Queue<Request>) {
        while let Some(request) = out.pop() {
            match request {
                Request::HostCall { .. } | Request::WithdrawHost { .. } => {
                    panic!("random legacy driver has no host declarations")
                }
                Request::Admitted { host_run, run } => self.runs.push((host_run, run)),
                Request::Answer { answer, .. } => {
                    self.seen[5] += 1;
                    if matches!(answer, run::Answer::Refused(_)) {
                        self.seen[15] += 1;
                    }
                }
                Request::MessageRefused { .. }
                | Request::Turn { .. }
                | Request::Waiting { .. }
                | Request::Long { .. }
                | Request::LongDone { .. }
                | Request::Unauthorized { .. }
                | Request::Refused { .. }
                | Request::Limited { .. }
                | Request::Yield { .. } => {}
                Request::Deliver { owner, host_run, .. } => {
                    self.seen[4] += 1;
                    self.ask(Asked::Delivery { owner, host_run, stopped: false });
                }
                Request::Complete {
                    owner,
                    prompt,
                    max_completion_bytes,
                    max_completion_blocks,
                    decoded_call_bytes,
                    ..
                } => {
                    self.seen[0] += 1;
                    let (finish, agents) = served(&prompt);
                    self.observe_prompt(&prompt);
                    let receiving = Receiving {
                        bytes: max_completion_bytes,
                        blocks: max_completion_blocks,
                        decoded: decoded_call_bytes,
                    };
                    self.ask(Asked::Complete {
                        owner,
                        finish,
                        agents,
                        receiving,
                        purpose: prompt.purpose,
                        asker: prompt.asker,
                        cancelled: false,
                    });
                }
                Request::Cancel { owner } => self.cancel(Family::Llm, owner),
                Request::Io { owner, op, deadline: _ } => {
                    self.seen[1] += 1;
                    let op = match op {
                        Op::Load { .. } => OpKind::Load,
                        Op::List { .. } => OpKind::Scan,
                        Op::Store { .. } => OpKind::Store,
                        Op::Spawn { .. } => OpKind::Spawn,
                        Op::Search { hits, bytes: byte_cap, .. } => OpKind::Search { hits, byte_cap },
                    };
                    self.ask(Asked::Io { owner, op, cancelled: false });
                }
                Request::Job { owner, act } => self.ask(Asked::Job { owner, act }),
                Request::CancelIo { owner } => self.cancel(Family::Io, owner),
                Request::Read { owner, .. } => {
                    self.seen[2] += 1;
                    self.ask(Asked::Read { owner });
                }
                Request::Probe { owner, .. } => {
                    self.seen[2] += 1;
                    self.ask(Asked::Probe { owner });
                }
                Request::Check { owner, .. } => {
                    self.seen[3] += 1;
                    self.ask(Asked::Check { owner, aborted: false });
                }
                Request::Abort { owner } => self.cancel(Family::Check, owner),
            }
        }
    }

    fn observe_prompt(&mut self, prompt: &Prompt) {
        for message in &prompt.messages {
            for block in &message.content {
                if let smith_domain::llm::Block::ToolResult { result, .. } = block {
                    match result {
                        smith_domain::llm::Returned::Text { text, error: true, .. }
                            if text.starts_with(b"rejected") || text.starts_with(b"refused") =>
                        {
                            self.seen[14] += 1;
                        }
                        smith_domain::llm::Returned::Invalid { .. } => self.seen[17] += 1,
                        smith_domain::llm::Returned::Owned {
                            outcome: smith_domain_tools::Outcome::Running { job, .. },
                        } => self.running = Some(*job),
                        smith_domain::llm::Returned::Text { .. }
                        | smith_domain::llm::Returned::Served { .. }
                        | smith_domain::llm::Returned::Owned { .. }
                        | smith_domain::llm::Returned::Withdrawn
                        | smith_domain::llm::Returned::NotRun { .. } => {}
                    }
                }
            }
        }
    }

    fn ask(&mut self, asked: Asked) {
        assert!(self.asked.len() < self.asked.capacity(), "the driver's room is allocated before the base");
        self.asked.push(asked);
    }

    /// Marks what `owner` asked for of `family` as cancelled, if it is in
    /// flight: tokens of different families may be equal.
    fn cancel(&mut self, family: Family, owner: Token) {
        for asked in &mut self.asked {
            let (of, cancelled) = match asked {
                Asked::Complete { owner, cancelled, .. } => (Family::Llm, (owner, cancelled)),
                Asked::Io { owner, cancelled, .. } => (Family::Io, (owner, cancelled)),
                Asked::Check { owner, aborted } => (Family::Check, (owner, aborted)),
                Asked::Delivery { owner, stopped, .. } => (Family::Delivery, (owner, stopped)),
                Asked::Read { .. } | Asked::Probe { .. } | Asked::Job { .. } => continue,
            };
            if of == family && *cancelled.0 == owner {
                *cancelled.1 = true;
            }
        }
    }

    /// The next event: a start, an end of something in flight, or a cancel.
    fn event(&mut self, limits: &Limits) -> Option<Event> {
        let roll = self.rng.below(12);
        if roll == 0 || self.asked.is_empty() {
            if self.rng.chance(200) {
                return self.cancel_run();
            }
            self.workers += 1;
            let host_run = Token::new(self.workers);
            // Most charters as large as a run may hold, some a byte larger.
            let brief = limits.run.run_bytes - 900 + self.rng.below(901);
            return Some(Event::Start {
                messages: Box::default(),
                answered: Box::default(),
                workspace: Some(workspace()),
                grants: Box::new([smith_domain::Grant {
                    renewable: true,
                    name: smith_domain::GrantName { account: 0, generation: 0 },
                    valid: Duration::from_secs(100_000),
                }]),
                reply_to: ReplyTo::new(host_run),
                host_run,
                identity: None,
                activation: self.workers,
                window: smith_domain::Window { turns: u32::MAX, bytes: u64::MAX },
                charter: charter(brief),
                transcript: None,
            });
        }
        if roll == 1 && !self.runs.is_empty() && self.rng.chance(100) {
            return self.cancel_run();
        }
        let asked = self.asked.swap_remove(index(&mut self.rng, self.asked.len()));
        Some(match asked {
            Asked::Complete { owner, cancelled: true, .. } if self.rng.chance(700) => Event::Cancelled { owner },
            Asked::Complete { owner, finish, agents, receiving, cancelled, .. } => {
                if self.rng.below(10) == 0 {
                    Event::Failed {
                        owner,
                        failure: Failure::Overloaded,
                        evidence: smith_domain::llm::Evidence::Unknown,
                        detail: Box::default(),
                        report: smith_domain::llm::Report::none(),
                    }
                } else {
                    let completion = self.completion(limits, finish, agents, receiving);
                    let (owned, decoded) = completion_owned(&completion);
                    assert!(owned <= receiving.bytes && decoded <= receiving.decoded);
                    assert!(completion.content.len() <= usize::try_from(receiving.blocks).expect("bounded blocks"));
                    if owned == receiving.bytes {
                        self.seen[13] += 1;
                    }
                    if cancelled {
                        self.seen[16] += 1;
                    }
                    Event::Completed { owner, completion }
                }
            }
            Asked::Io { owner, cancelled: true, .. } if self.rng.chance(700) => {
                Event::Done { owner, done: Done::Cancelled }
            }
            Asked::Io { owner, op, .. } => Event::Done { owner, done: self.done(limits, op) },
            Asked::Job { owner, act } => Event::Done { owner, done: random::job_done(act) },
            Asked::Read { owner } => {
                let read = match self.rng.below(4) {
                    0 => run::Read::Missing,
                    _ => run::Read::Text { text: bytes(u64::from(limits.run.guide_bytes)), whole: false },
                };
                Event::Read { owner, read }
            }
            Asked::Probe { owner } => Event::Probed { owner, executable: self.rng.chance(700) },
            Asked::Check { owner, aborted: true } if self.rng.chance(700) => Event::Aborted { owner },
            Asked::Check { owner, .. } => {
                let exit = run::Exit::Code { code: u8::from(self.rng.chance(500)) };
                self.seen[if exit == (run::Exit::Code { code: 0 }) { 7 } else { 8 }] += 1;
                let ran = run::Ran { exit, output: bytes(u64::from(limits.run.check_tail)), cut: 1000 };
                Event::Checked { owner, ran }
            }
            Asked::Delivery { owner, stopped, .. } => {
                let choice = index(&mut self.rng, 3);
                let push = [
                    crate::delivered(),
                    run::Delivery::Stale,
                    run::Delivery::Failed(run::DeliveryFailure::new(0, run::DeliveryReason::Unknown)),
                ][choice]
                    .clone();
                self.seen[9 + choice] += 1;
                if stopped {
                    self.seen[12] += 1;
                }
                Event::Delivered { owner, delivery: push }
            }
        })
    }

    fn cancel_run(&mut self) -> Option<Event> {
        if self.runs.is_empty() {
            return None;
        }
        let pending_delivery = if self.rng.chance(500) {
            self.asked.iter().find_map(|asked| match asked {
                Asked::Delivery { host_run, .. } => {
                    self.runs.iter().find(|(candidate, _)| candidate == host_run).copied()
                }
                Asked::Complete { .. }
                | Asked::Io { .. }
                | Asked::Read { .. }
                | Asked::Probe { .. }
                | Asked::Check { .. }
                | Asked::Job { .. } => None,
            })
        } else {
            None
        };
        // Actual submissions make this race reachable; the other half retains
        // arbitrary stale parent cancellation handles from the original sweep.
        let (host_run, run) = pending_delivery.unwrap_or_else(|| self.runs[index(&mut self.rng, self.runs.len())]);
        for asked in &mut self.asked {
            if let Asked::Delivery { host_run: delivery_worker, stopped, .. } = asked
                && *delivery_worker == host_run
            {
                *stopped = true;
            }
        }
        Some(Event::Cancel { run })
    }
}

/// Whether `prompt` offers the run's finish and sub-agents.
fn served(prompt: &Prompt) -> (bool, bool) {
    let mut offered = (false, false);
    for tool in &prompt.served {
        match tool {
            Served::Host(_) => panic!("random legacy driver has no host declarations"),
            Served::Wait | Served::Deliver => {}
            Served::Finish => offered.0 = true,
            Served::SubAgent => offered.1 = true,
        }
    }
    offered
}

/// Drives a domain under `limits` for `rounds` rounds from `seed`, as the
/// loop would: each round resumes what is ready, takes an event, and fires
/// what is due, then reaches the reclaim point; checking the peak of the heap
/// in every entry point under its scenario ceiling.
#[must_use]
pub fn churn(limits: &Limits, seed: u64, rounds: u32, bound: u64) -> [u32; 18] {
    let limits = *limits;
    let mut env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::with_capacity(max_out(&limits));
    let mut driver = Driver {
        rng: Rng::new(seed),
        asked: Vec::with_capacity(4096),
        runs: Vec::with_capacity(usize::try_from(rounds).expect("small")),
        workers: 0,
        running: None,
        seen: [0; 18],
    };
    let meter = Meter::new();
    let mut domain = Domain::new(
        &limits,
        smith_domain::Config {
            programs: Box::new([]),
            endpoints: Box::new([Endpoint(0)]),
            models: Box::new([smith_domain::ConfiguredModel {
                efforts: Box::new([]),
                prices: smith_domain::run::Prices {
                    input: 1,
                    cached: 1,
                    cache_write: None,
                    output: 1,
                    unit: 1_000_000,
                },
                protocol_allowance: 0,
                endpoint: smith_domain::run::charter::Endpoint(0),
                model: Box::from(&b"m"[..]),
                window: 65_536,
                output: 4096,
                head: Duration::from_secs(60),
                idle: Duration::from_secs(30),
            }]),
        },
        seed,
    );
    let mut measure = |domain: &mut Domain, env: &Env<Limits>, driver: &mut Driver, call: Point| {
        meter.start();
        match call {
            Point::Resume => resume(domain, env, &mut out),
            Point::Fire => fire(domain, env, &mut out),
            Point::Step(event) => step(domain, env, *event, &mut out),
        }
        let measured = meter.end();
        driver.drain(&mut out);
        while domain.pop_fact().is_some() {}
        meter.check(measured, bound, &limits);
    };
    for _ in 0..rounds {
        env.now = env.now.saturating_add(Duration::from_millis(driver.rng.below(3000)));
        for _ in 0..1000 {
            if !domain.is_ready() {
                break;
            }
            measure(&mut domain, &env, &mut driver, Point::Resume);
        }
        if let Some(event) = driver.event(&limits) {
            measure(&mut domain, &env, &mut driver, Point::Step(Box::new(event)));
        }
        for _ in 0..1000 {
            if !domain.is_due(env.now) {
                break;
            }
            measure(&mut domain, &env, &mut driver, Point::Fire);
        }
        domain.reclaim();
    }
    driver.seen
}

/// An entry point the loop calls.
enum Point {
    Resume,
    Fire,
    Step(Box<Event>),
}
