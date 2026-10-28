mod jobs;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use skein_fake_checkout::Checkout;
use skein_lib::{Duration, ReplyTo, Rng, Time, Token};
use skein_world::domain::{Key, Ledger, Schedule, Span, Stage, Trace};
use smith_domain_tools as tools;
use smith_domain_tools::{Authority, Call, Done, Expect, Fault, Grants, Op, Outcome, Refusal, Repo};

use crate::translate;

/// Immutable seeded peer behavior and component limits used to construct this world.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Settings {
    /// Seeds the world.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub seed: u64,
    /// Immutable checkout-tool limits for the component under test.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub tools: tools::Limits,
    /// How long io takes to run an operation.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub io: Span,
    /// The chance, per mille, that an operation fails with a fault.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub faults: u32,
    /// The chance, per mille, that a cancel loses its race and the operation
    /// ends as it would have.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub late_cancels: u32,
    /// The chance, per mille, that an operation abandoned at its deadline had
    /// taken effect all the same.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub late_effects: u32,
    /// How long the session takes between one step of its script and the
    /// next.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub think: Span,
    /// How long the session gives each call.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub call_timeout: Duration,
}

impl Settings {
    /// A world where nothing goes wrong: no faults, io well within every
    /// deadline, room for a few kits.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub const fn calm(seed: u64) -> Settings {
        Settings {
            seed,
            tools: tools::Limits {
                shell_output: 192,
                kits: 4,
                calls: 8,
                repos: 4,
                path_bytes: 256,
                host_path_bytes: 4096,
                known_files: 16,
                file_bytes: 4096,
                tool_payload: 4096,
                read_bytes: 1024,
                list_entries: 16,
                list_bytes: 4096,
                match_lines: 4,
                file_timeout: Duration::from_secs(10),
                jobs: 4,
                job_poll: Duration::from_secs(30),
                job_wait: Duration::from_secs(30),
                shell_timeout: Duration::from_secs(60),
                tool_deadline: Duration::from_secs(600),
                shell_head: 64,
                shell_tail: 128,
                search_hits: 8,
                search_bytes: 256,
                search_timeout: Duration::from_secs(30),
                facts: 64,
            },
            io: Span::millis(1, 20),
            faults: 0,
            late_cancels: 0,
            late_effects: 0,
            think: Span::millis(1, 100),
            call_timeout: Duration::from_secs(60),
        }
    }
}

/// What a session does with its kit, one step at a time, before it closes it.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub enum Step {
    /// Sends the calls together, and waits for every answer.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Calls(Vec<Call>),
    /// Sends the calls, and goes on without waiting.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Send(Vec<Call>),
    /// Changes the checkout, as something other than the agent would.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Change(Box<dyn FnOnce(&mut Checkout)>),
    /// Waits.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Sleep(Duration),
    /// From now on io takes this long to run an operation, for every kit.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Latency(Span),
    /// Cancel held jobs, continuing only after all their groups settle.
    CancelJobs,
    /// End the wait of the call at this zero-based position in the session.
    EndWait(usize),
}

/// What the world counted.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Stats {
    /// Calls the sessions made.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub calls: u32,
    /// Native facts drained from the tools parent output.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub facts: u64,
    /// Operations the tools asked of io, and the commands among them.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub ops: u32,
    /// Count of commands observed at this scripted boundary.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub commands: u32,
    /// Operations that failed with a fault.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub faults: u32,
    /// Operations that ran out of time, and those that took effect all the
    /// same.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub timeouts: u32,
    /// Count of late effects observed at this scripted boundary.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub late_effects: u32,
    /// Operations the tools cancelled, those whose cancel lost the race, and
    /// cancels that came after their operation had ended.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub cancels: u32,
    /// Count of late cancels observed at this scripted boundary.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub late_cancels: u32,
    /// Count of stale cancels observed at this scripted boundary.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub stale_cancels: u32,
}

/// Something on its way, delivered at its time.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
enum Delivery {
    /// A session opens its kit.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Open {
        session: u64,
    },
    /// A session takes the next step of its script.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Next {
        session: u64,
    },
    /// io has run the operation of `owner`.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Ran {
        owner: Token,
    },
    /// The deadline of the operation of `owner` passes.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Deadline {
        owner: Token,
    },
    Job {
        owner: Token,
    },
}

/// An operation in flight, as io keeps it.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
struct Pending {
    work: Work,
    /// When it started; when it ends, and when its deadline passes, as
    /// deliveries.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    since: Time,
    ran: Key,
    deadline: Key,
}

/// What io does for an operation in flight.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
enum Work {
    /// A file operation, run on the checkout when it is due.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    File(Op),
    /// A command, running since `since`.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Command { started: translate::Started, since: Time },
    /// An operation that ends in this terminal, as a command that could not
    /// start.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Ending(Done),
}

/// A session, as the world plays it.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
struct Session {
    authority: Option<Authority>,
    script: VecDeque<Step>,
    state: State,
    /// The calls it sent, in order.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    calls: Vec<u64>,
    /// The calls of its last `Calls` step not answered yet.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    awaited: BTreeSet<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// Its open is on its way, or the tools have not answered it.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Opening,
    /// Running its script with its kit.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Running {
        kit: Token,
    },
    /// Waiting for the answers to its last `Calls` step.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Waiting {
        kit: Token,
    },
    /// Its close has been sent.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    Closing,
    Cancelling {
        kit: Token,
    },
    Closed,
    Refused(Refusal),
}

/// Real domain plus scripted typed peers, pending terminals and external observations for this component story.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub struct World {
    now: Time,
    rng: Rng,
    settings: Settings,
    checkout: Checkout,

    tools: tools::Domain,
    stage: Stage<tools::Limits, tools::Event, tools::Request>,

    /// Deliveries in flight, whose count names sessions and calls too.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    wire: Schedule<Delivery>,
    sessions: BTreeMap<u64, Session>,
    /// Every call made, by name: whose it is, and its answer once it came.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    calls: BTreeMap<u64, (u64, Option<Result<Outcome, tools::JobProblem>>)>,
    job_numbers: BTreeMap<(u64, u32), bool>,
    /// io's operations in flight.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    ops: Ledger<Token, Pending>,
    /// The session whose kit each operation, and each kit, is for.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    owners: BTreeMap<Token, u64>,
    kits: BTreeMap<Token, u64>,
    /// The versions io has told each session's kit of, for each place (root,
    /// path): the only ones a store of that kit may expect there.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    versions: BTreeSet<(u64, u64, Vec<u8>, u64)>,
    /// Each edit owner stores only over the version its own load actually returned.
    edit_loads: BTreeMap<Token, Option<(tools::Place, tools::Version)>>,
    /// The facts the tools told, by session.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    facts: BTreeMap<u64, Vec<tools::Fact>>,
    /// Whether some kit may write each root: those none may write change only
    /// by the world's own changes, which `untouched` follows.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    writable: BTreeMap<u64, bool>,
    untouched: Option<BTreeMap<Vec<u8>, Vec<u8>>>,

    jobs: BTreeMap<Token, jobs::Job>,
    job_outputs: BTreeMap<Vec<u8>, Vec<(Duration, Vec<u8>)>>,
    stats: Stats,
    trace: Trace,
}

impl World {
    /// A world over `checkout`, whose roots name the repositories of the
    /// authorities its sessions open kits with.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn new(settings: Settings, checkout: Checkout) -> World {
        assert!(tools::worst_case(&settings.tools).is_some(), "the shell refuses limits it cannot provision");
        let room = tools::max_out(&settings.tools).saturating_mul(2);
        World {
            now: Time::ZERO,
            rng: Rng::new(settings.seed),
            settings,
            checkout,
            tools: tools::Domain::new(&settings.tools),
            stage: Stage::new(settings.tools, tools::max_out(&settings.tools), room),
            wire: Schedule::new(),
            sessions: BTreeMap::new(),
            calls: BTreeMap::new(),
            job_numbers: BTreeMap::new(),
            ops: Ledger::new("operation"),
            owners: BTreeMap::new(),
            kits: BTreeMap::new(),
            versions: BTreeSet::new(),
            edit_loads: BTreeMap::new(),
            facts: BTreeMap::new(),
            writable: BTreeMap::new(),
            untouched: None,
            jobs: BTreeMap::new(),
            job_outputs: BTreeMap::new(),
            stats: Stats::default(),
            trace: Trace::default(),
        }
    }

    /// Current injected monotonic time in this world.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn now(&self) -> Time {
        self.now
    }

    /// Observed boundary counters.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Borrowed contained checkout after all effects delivered so far.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn checkout(&self) -> &Checkout {
        &self.checkout
    }

    /// What crossed between the tools and the world, in order, with times.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        self.trace.lines()
    }

    /// A session that opens a kit with `authority` at `at`, runs `script`
    /// with it, and closes it. Returns the session's name.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub fn session(&mut self, at: Time, authority: Authority, script: Vec<Step>) -> u64 {
        // A kit writes, and its commands write, only with modify.
        for repo in &authority.repos {
            *self.writable.entry(repo.root.raw()).or_insert(false) |= repo.writable && authority.grants.modify;
        }
        let session = self.wire.name();
        let state = State::Opening;
        let script = script.into();
        let entry = Session { authority: Some(authority), script, state, calls: Vec::new(), awaited: BTreeSet::new() };
        self.sessions.insert(session, entry);
        self.wire.send(at, Delivery::Open { session });
        session
    }

    /// The answers to the calls `session` made, in the order it made them.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn answers(&self, session: u64) -> Vec<&Outcome> {
        self.results(session)
            .into_iter()
            .map(|answer| answer.as_ref().expect("this story has no invalid calls"))
            .collect()
    }

    /// The typed answers and invalid-call problems, in call order.
    #[must_use]
    pub fn results(&self, session: u64) -> Vec<&Result<Outcome, tools::JobProblem>> {
        let calls = &self.sessions.get(&session).expect("a session the world made").calls;
        calls.iter().map(|call| self.calls[call].1.as_ref().expect("every call is answered")).collect()
    }

    /// Why the tools refused to open a kit for `session`, if they did.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn refusal(&self, session: u64) -> Option<Refusal> {
        match self.sessions.get(&session).expect("a session the world made").state {
            State::Refused(refusal) => Some(refusal),
            State::Opening
            | State::Running { .. }
            | State::Waiting { .. }
            | State::Cancelling { .. }
            | State::Closing
            | State::Closed => None,
        }
    }

    /// The names of every session.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub fn sessions(&self) -> impl Iterator<Item = u64> + '_ {
        self.sessions.keys().copied()
    }

    /// Runs until nothing is left to happen, then checks the invariants of a
    /// settled world. Panics if it takes more than `iterations`.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub fn run(&mut self, iterations: u32) {
        for _ in 0..iterations {
            self.iterate();
            if self.has_work_now() {
                continue;
            }
            let Some(next) = self.next_time() else {
                self.assert_settled();
                return;
            };
            assert!(next > self.now, "time moves forward");
            self.now = next;
        }
        panic!("the world did not settle in {iterations} iterations");
    }

    /// One iteration of the loop, as the shell would run it.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn iterate(&mut self) {
        if self.untouched.is_none() {
            self.untouched = Some(self.read_only());
        }
        self.stage.tick(self.now);
        self.deliver();

        // The tools take their events while they have room for what one more
        // may produce.
        while self.tools.facts_room() >= tools::max_facts(&self.stage.env.limits)
            && let Some(event) = self.stage.next_event()
        {
            self.log(&format!("tools <- {event:?}"));
            // The operations a step asks for are for the kit of the call, or
            // of the operation, the event is about.
            let whose = match &event {
                tools::Event::Call { kit, .. } => Some(*self.kits.get(kit).expect("a kit that opened")),
                tools::Event::Done { owner, .. } => Some(*self.owners.get(owner).expect("an operation io ran")),
                tools::Event::NativeVersion { .. }
                | tools::Event::Open { .. }
                | tools::Event::Close { .. }
                | tools::Event::CancelJobs { .. }
                | tools::Event::EndWait { .. } => None,
            };
            let editing = matches!(&event, tools::Event::Call { call: Call::Edit { .. }, .. });
            let before = self.stage.out.len();
            tools::step(&mut self.tools, &self.stage.env, event, &mut self.stage.out);
            if let Some(session) = whose {
                for request in self.stage.out.iter().skip(usize::try_from(before).expect("a small queue")) {
                    match request {
                        tools::Request::Io { owner, .. } => {
                            let _ = self.owners.insert(*owner, session);
                            if editing {
                                self.edit_loads.insert(*owner, None);
                            }
                        }
                        tools::Request::Opened { .. }
                        | tools::Request::Refused { .. }
                        | tools::Request::Answer { .. }
                        | tools::Request::Invalid { .. }
                        | tools::Request::JobsCancelled { .. }
                        | tools::Request::Closed { .. }
                        | tools::Request::Job { .. }
                        | tools::Request::CancelIo { .. } => {}
                    }
                }
            }
        }

        // What they asked for, carried out at the end of the iteration.
        while let Some(request) = self.stage.out.pop() {
            self.log(&format!("tools -> {request:?}"));
            self.request(request);
        }

        // Facts, at the world's own pace.
        while let Some(fact) = self.tools.pop_fact() {
            self.observe_job_fact(&fact);
            self.stats.facts += 1;
            self.log(&format!("tools tell {fact:?}"));
            self.facts.entry(session_of(&fact).raw()).or_default().push(fact);
        }

        // Before the reclaim point, what was retired in this iteration still
        // takes its slot.
        let limits = &self.settings.tools;
        assert!(self.tools.kits() <= limits.kits, "kits stay within their slots");
        assert!(self.tools.flights() <= limits.kits * (limits.calls + limits.jobs) * 2, "jobs stay within their slots");
        self.tools.reclaim();
    }

    /// The tools' requests, answered the way the session and io would.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn request(&mut self, request: tools::Request) {
        match request {
            tools::Request::Opened { session, kit } => {
                let entry = self.session_mut(session.raw());
                assert_eq!(entry.state, State::Opening, "a kit opens once");
                entry.state = State::Running { kit };
                self.kits.insert(kit, session.raw());
                self.next(session.raw());
            }
            tools::Request::Refused { session, refusal } => {
                let entry = self.session_mut(session.raw());
                assert_eq!(entry.state, State::Opening, "an open is answered once");
                entry.state = State::Refused(refusal);
            }
            tools::Request::Answer { to, outcome } => {
                self.answered(to, Ok(outcome));
            }
            tools::Request::Invalid { to, problem } => self.answered(to, Err(problem)),
            tools::Request::JobsCancelled { session } => {
                assert!(
                    self.jobs.keys().all(|owner| self.owners[owner] != session.raw()),
                    "cancel waits for every held owner"
                );
                let entry = self.session_mut(session.raw());
                let State::Cancelling { kit } = entry.state else { panic!("one cancel acknowledgement") };
                entry.state = State::Running { kit };
                self.next(session.raw());
            }
            tools::Request::Closed { session } => {
                let entry = self.sessions.get(&session.raw()).expect("a session the world made");
                assert_eq!(entry.state, State::Closing, "a kit closes once, when asked");
                for call in &entry.calls {
                    assert!(self.calls[call].1.is_some(), "a kit answers every call before it closes");
                }
                self.session_mut(session.raw()).state = State::Closed;
            }
            tools::Request::Io { owner, op, deadline } => {
                if self.start_yielded(owner, &op, deadline) {
                    self.stats.ops += 1;
                    return;
                }
                let (work, ran) = self.start(owner, op);
                let ran = self.wire.send(ran, Delivery::Ran { owner });
                let deadline = self.wire.send(deadline, Delivery::Deadline { owner });
                let pending = Pending { work, since: self.now, ran, deadline };
                self.ops.open(owner, pending);
                self.stats.ops += 1;
            }
            tools::Request::Job { owner, act } => self.job_act(owner, act),
            tools::Request::CancelIo { owner } => {
                if self.cancel_job(owner) {
                    self.stats.cancels += 1;
                    return;
                }
                // An operation that has ended has its terminal on the way: the
                // cancel lost the race and changes nothing.
                if !self.ops.contains(owner) {
                    self.stats.stale_cancels += 1;
                    return;
                }
                self.stats.cancels += 1;
                if self.rng.chance(self.settings.late_cancels) {
                    self.stats.late_cancels += 1;
                    return;
                }
                let pending = self.ops.end(owner);
                self.wire.withdraw(pending.ran);
                self.wire.withdraw(pending.deadline);
                self.abandon(pending.work);
                self.stage.push(tools::Event::Done { owner, done: Done::Cancelled });
            }
        }
    }

    fn answered(&mut self, to: ReplyTo, result: Result<Outcome, tools::JobProblem>) {
        let call = to.into_token().raw();
        let (session, answer) = self.calls.get_mut(&call).expect("an answer to a call that was made");
        assert!(answer.is_none(), "a call is answered once");
        let session = *session;
        self.observe_job_answer(session, &result);
        self.calls.get_mut(&call).expect("known call").1 = Some(result);
        let entry = self.session_mut(session);
        entry.awaited.remove(&call);
        match entry.state {
            State::Waiting { kit } if entry.awaited.is_empty() => {
                entry.state = State::Running { kit };
                self.next(session);
            }
            State::Waiting { .. }
            | State::Opening
            | State::Cancelling { .. }
            | State::Running { .. }
            | State::Closing
            | State::Closed
            | State::Refused(_) => {}
        }
    }

    /// Hands every delivery that is due to its destination.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn deliver(&mut self) {
        while let Some(delivery) = self.wire.next(self.now) {
            match delivery {
                Delivery::Open { session } => {
                    let authority = self.session_mut(session).authority.take().expect("a session opens once");
                    self.stage.push(tools::Event::Open { jobs: 0, session: Token::new(session), authority });
                }
                Delivery::Next { session } => self.step(session),
                Delivery::Job { owner } => self.job_tick(owner),
                Delivery::Ran { owner } => {
                    // A run is withdrawn when its operation ends first.
                    let pending = self.ops.end(owner);
                    self.wire.withdraw(pending.deadline);
                    let done = match pending.work {
                        // A fault may come after the operation took effect,
                        // as a store renamed into place before io failed.
                        Work::File(op) if self.rng.chance(self.settings.faults) => {
                            self.stats.faults += 1;
                            if self.rng.chance(self.settings.late_effects) {
                                drop(translate::perform(&mut self.checkout, op));
                                self.assert_untouched();
                                self.stats.late_effects += 1;
                            }
                            Done::Failed { fault: self.fault() }
                        }
                        Work::File(op) => self.perform(owner, op),
                        Work::Command { started, since: _ } => {
                            self.perform_command(&started.process);
                            self.assert_untouched();
                            let program = &started.process.program;
                            translate::exited(Some(program.exit), &program.output, started.head, started.tail)
                        }
                        Work::Ending(done) => done,
                    };
                    self.stage.push(tools::Event::Done { owner, done });
                }
                Delivery::Deadline { owner } => {
                    // A deadline is withdrawn when its operation ends first.
                    let pending = self.ops.end(owner);
                    self.wire.withdraw(pending.ran);
                    let done = match &pending.work {
                        // io kills a command at its deadline, and tells what
                        // it wrote by then.
                        Work::Command { started, since } => {
                            let program = &started.process.program;
                            let ran = u128::from(self.now.saturating_since(*since).as_nanos());
                            let written = usize::try_from(
                                ran * program.output.len() as u128 / program.duration.as_nanos().max(1),
                            )
                            .expect("no more than the output")
                            .min(program.output.len());
                            translate::exited(None, &program.output[..written], started.head, started.tail)
                        }
                        // io kills rg at its deadline, and tells what it had
                        // found by then: a share of what it would have.
                        Work::File(op @ Op::Search { .. }) => {
                            let found = translate::perform(&mut self.checkout, op.clone());
                            let ran = u128::from(self.now.saturating_since(pending.since).as_nanos());
                            let whole = u128::from(pending.ran.at.saturating_since(pending.since).as_nanos());
                            translate::cut(found, ran, whole)
                        }
                        Work::File(_) | Work::Ending(_) => Done::TimedOut,
                    };
                    self.abandon(pending.work);
                    self.stage.push(tools::Event::Done { owner, done });
                    self.stats.timeouts += 1;
                }
            }
        }
    }

    /// Starts `op` for `owner`, and says when it ends.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn start(&mut self, owner: Token, op: Op) -> (Work, Time) {
        let latency = self.now.saturating_add(self.settings.io.draw(&mut self.rng));
        match op {
            Op::Spawn { cwd, command, head, tail, after: _ } => {
                self.stats.commands += 1;
                let started = if self.rng.chance(self.settings.faults) {
                    self.stats.faults += 1;
                    Err(Done::Failed { fault: self.fault() })
                } else {
                    translate::spawn(
                        &self.checkout,
                        &cwd,
                        &command,
                        &[(b"PATH".to_vec(), b"/usr/bin:/bin".to_vec()), (b"HOME".to_vec(), b"/home/agent".to_vec())],
                        (head, tail),
                    )
                };
                match started {
                    Ok(started) => {
                        let duration = u64::try_from(started.process.program.duration.as_nanos()).expect("short");
                        let ends = self.now.saturating_add(Duration::from_nanos(duration));
                        (Work::Command { started, since: self.now }, ends)
                    }
                    Err(done) => (Work::Ending(done), latency),
                }
            }
            Op::Store { at, content, expect } => {
                match expect {
                    Expect::Is { version } => {
                        let session = *self.owners.get(&owner).expect("an operation of a kit");
                        let known = (session, at.root.raw(), at.path.to_vec(), version.raw()[0]);
                        assert!(self.versions.contains(&known), "a kit's store expects a version io gave it there");
                        if let Some(loaded) = self.edit_loads.get(&owner) {
                            assert_eq!(
                                loaded.as_ref(),
                                Some(&(at.clone(), version)),
                                "an edit stores only over the version its own load matched"
                            );
                        }
                    }
                    Expect::Absent => {}
                }
                (Work::File(Op::Store { at, content, expect }), latency)
            }
            op @ (Op::Remove { .. } | Op::Load { .. } | Op::List { .. } | Op::Search { .. }) => {
                (Work::File(op), latency)
            }
        }
    }

    /// Abandons `work`, at its deadline or for a cancel. What it did may
    /// have taken effect all the same: a store renamed into place, a command's
    /// changes made before it was killed.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn abandon(&mut self, work: Work) {
        if !self.rng.chance(self.settings.late_effects) {
            return;
        }
        match work {
            Work::File(op) => drop(translate::perform(&mut self.checkout, op)),
            Work::Command { started, since: _ } => self.perform_command(&started.process),
            Work::Ending(_) => return,
        }
        self.assert_untouched();
        self.stats.late_effects += 1;
    }

    /// Runs `op` on the checkout for `owner`, and notes the version it tells
    /// that kit of.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn perform(&mut self, owner: Token, op: Op) -> Done {
        let at = match &op {
            Op::Remove { at, .. }
            | Op::Load { at, .. }
            | Op::List { at, .. }
            | Op::Store { at, .. }
            | Op::Search { at, .. } => at.clone(),
            Op::Spawn { .. } => unreachable!("a spawn is started, not run"),
        };
        let done = translate::perform(&mut self.checkout, op);
        if let Done::Loaded { version, .. } = &done
            && let Some(loaded) = self.edit_loads.get_mut(&owner)
        {
            *loaded = Some((at.clone(), *version));
        }
        self.assert_untouched();
        match &done {
            Done::Loaded { version, .. } | Done::Stored { version } => {
                let session = *self.owners.get(&owner).expect("an operation of a kit");
                self.versions.insert((session, at.root.raw(), at.path.to_vec(), version.raw()[0]));
            }
            Done::Removed
            | Done::NotEmpty
            | Done::Listed { .. }
            | Done::Conflict { .. }
            | Done::Missing
            | Done::NotFile
            | Done::Linked
            | Done::Held { .. }
            | Done::Job { .. }
            | Done::Exited { .. }
            | Done::Found { .. }
            | Done::NotDirectory
            | Done::TooLarge { .. }
            | Done::Escapes
            | Done::Failed { .. }
            | Done::TimedOut
            | Done::Cancelled => {}
        }
        done
    }

    /// The session takes the next step of its script, or closes its kit once
    /// it has run out.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn step(&mut self, session: u64) {
        let entry = self.session_mut(session);
        let kit = match entry.state {
            State::Running { kit } => kit,
            State::Opening
            | State::Waiting { .. }
            | State::Cancelling { .. }
            | State::Closing
            | State::Closed
            | State::Refused(_) => {
                unreachable!("a session steps while it runs")
            }
        };
        let Some(step) = entry.script.pop_front() else {
            entry.state = State::Closing;
            self.stage.push(tools::Event::Close { kit });
            return;
        };
        match step {
            Step::EndWait(index) => {
                let call = Token::new(self.session_mut(session).calls[index]);
                self.stage.push(tools::Event::EndWait { kit, call });
                self.next(session);
            }
            Step::CancelJobs => {
                self.session_mut(session).state = State::Cancelling { kit };
                self.stage.push(tools::Event::CancelJobs { kit });
            }
            Step::Calls(calls) => {
                let sent = self.send(session, kit, calls);
                let entry = self.session_mut(session);
                entry.awaited.extend(sent);
                if entry.awaited.is_empty() {
                    self.next(session);
                } else {
                    entry.state = State::Waiting { kit };
                }
            }
            Step::Send(calls) => {
                drop(self.send(session, kit, calls));
                self.next(session);
            }
            Step::Change(change) => {
                change(&mut self.checkout);
                self.untouched = Some(self.read_only());
                self.log("checkout changed");
                self.next(session);
            }
            Step::Sleep(span) => {
                let at = self.now.saturating_add(span);
                self.wire.send(at, Delivery::Next { session });
            }
            Step::Latency(span) => {
                self.settings.io = span;
                self.next(session);
            }
        }
    }

    /// Sends `calls` to `kit`, for `session`, and returns their names.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn send(&mut self, session: u64, kit: Token, calls: Vec<Call>) -> Vec<u64> {
        let mut names = Vec::new();
        for call in calls {
            let name = self.wire.name();
            self.calls.insert(name, (session, None));
            self.session_mut(session).calls.push(name);
            let deadline = self.now.saturating_add(self.settings.call_timeout);
            let reply_to = ReplyTo::new(Token::new(name));
            self.stage.push(tools::Event::Call { kit, observation: None, reply_to, call, deadline });
            self.stats.calls += 1;
            names.push(name);
        }
        names
    }

    /// Schedules the session's next step, after it has thought about it.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn next(&mut self, session: u64) {
        let at = self.now.saturating_add(self.settings.think.draw(&mut self.rng));
        self.wire.send(at, Delivery::Next { session });
    }

    fn has_work_now(&self) -> bool {
        self.stage.has_events() || self.wire.is_due(self.now)
    }

    fn next_time(&self) -> Option<Time> {
        self.wire.next_time()
    }

    /// The invariants of a world where nothing is left to happen.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn assert_settled(&self) {
        assert_eq!(self.tools.kits(), 0, "every kit has closed and been reclaimed");
        assert_eq!(self.tools.flights(), 0, "every job has ended and been reclaimed");
        self.ops.assert_settled();
        assert!(self.jobs.is_empty(), "every held command ended");
        assert!(self.wire.is_empty() && !self.stage.has_events(), "nothing is on its way");
        for (name, session) in &self.sessions {
            match session.state {
                State::Closed | State::Refused(_) => {}
                State::Opening
                | State::Running { .. }
                | State::Waiting { .. }
                | State::Cancelling { .. }
                | State::Closing => {
                    panic!("session {name} ended, not {:?}", session.state)
                }
            }
        }
        for (call, (_, answer)) in &self.calls {
            assert!(answer.is_some(), "call {call} was answered");
        }
        self.assert_told();
    }

    /// The facts tell what happened, as the world saw it: each session's kit
    /// opened or was refused, closed once if it opened, and every call was
    /// answered once, having started or not.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn assert_told(&self) {
        for (name, session) in &self.sessions {
            let mut told = Told::default();
            for fact in self.facts.get(name).map_or(&[][..], Vec::as_slice) {
                match fact.kind {
                    tools::FactKind::Opened { .. } => told.opened += 1,
                    tools::FactKind::Refused { .. } => told.refused += 1,
                    tools::FactKind::Started { .. } => told.started += 1,
                    tools::FactKind::Answered { .. } => told.answered += 1,
                    tools::FactKind::Closing { .. } | tools::FactKind::JobEnded { .. } => {}
                    tools::FactKind::Closed { .. } => told.closed += 1,
                }
            }
            let calls =
                session.calls.iter().filter(|call| self.calls[call].1.as_ref().expect("answered").is_ok()).count();
            let refused = match session.state {
                State::Refused(_) => 1,
                State::Opening
                | State::Running { .. }
                | State::Waiting { .. }
                | State::Cancelling { .. }
                | State::Closing
                | State::Closed => 0,
            };
            let opened = 1 - refused;
            let expected = Told { opened, refused, started: told.started, answered: calls, closed: opened };
            assert_eq!(told, expected, "session {name}: the facts tell what the world saw");
            assert!(told.started <= session.calls.len(), "session {name}: a call starts at most once");
        }
    }

    /// The files of the repositories no kit may write.
    /// Apply this world's plain command fixture effects, independent of tool
    /// grants. Commands are not contained until Skein's later trees exist.
    fn perform_command(&mut self, process: &skein_fake_checkout::Process) {
        self.assert_untouched();
        translate::finish(&mut self.checkout, process);
        // These command effects are outside file-tool authority, like the
        // world's external writer. File tools still compare the next baseline.
        self.untouched = Some(self.read_only());
    }

    /// The files nothing but the world may change: those of the repositories
    /// no kit may write, and those in any git directory.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn read_only(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut files = BTreeMap::new();
        for (path, content) in self.checkout.files() {
            if skein_fake_checkout::in_git(path) {
                files.insert(path.to_vec(), content.to_vec());
            }
        }
        for (root, writable) in &self.writable {
            if *writable {
                continue;
            }
            let at = self.checkout.root_path(*root);
            for (path, content) in self.checkout.files() {
                let beneath = at.is_empty() || path == at || path.strip_prefix(at).is_some_and(|rest| rest[0] == b'/');
                if beneath {
                    files.insert(path.to_vec(), content.to_vec());
                }
            }
        }
        files
    }

    /// Read-only repositories and git directories change only by the world's
    /// own changes.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    fn assert_untouched(&self) {
        let untouched = self.untouched.as_ref().expect("taken at the first iteration");
        assert_eq!(&self.read_only(), untouched, "a read-only repository or a git directory changed");
    }

    fn session_mut(&mut self, session: u64) -> &mut Session {
        self.sessions.get_mut(&session).expect("a session the world made")
    }

    fn fault(&mut self) -> Fault {
        match self.rng.below(3) {
            0 => Fault::Denied,
            1 => Fault::NoSpace,
            _ => Fault::Other,
        }
    }

    fn log(&mut self, line: &str) {
        self.trace.log(self.now, line);
    }
}

/// How many facts of each kind a session was told about.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
struct Told {
    opened: usize,
    refused: usize,
    started: usize,
    answered: usize,
    closed: usize,
}

/// The session a fact is about.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
fn session_of(fact: &tools::Fact) -> Token {
    match &fact.kind {
        tools::FactKind::Opened { session }
        | tools::FactKind::Refused { session, .. }
        | tools::FactKind::Started { session, .. }
        | tools::FactKind::Answered { session, .. }
        | tools::FactKind::Closing { session, .. }
        | tools::FactKind::JobEnded { session, .. }
        | tools::FactKind::Closed { session } => *session,
    }
}

/// A repository of `checkout` at `mount`, made a root.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub fn repo(checkout: &mut Checkout, mount: &[u8], writable: bool) -> Repo {
    let root = checkout.root(mount.strip_prefix(b"/").expect("a mount is absolute"));
    Repo {
        host: Box::new([smith_domain_tools::Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
        mount: translate::names(mount),
        root: translate::token(root),
        writable,
    }
}

/// An authority whose relative paths start at `cwd`; the fake machine owns
/// the commands' configured environment.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn authority(cwd: &[u8], repos: Vec<Repo>, grants: Grants) -> Authority {
    Authority { cwd: translate::names(cwd), repos: repos.into(), grants }
}
