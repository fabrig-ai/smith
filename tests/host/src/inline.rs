//! Inline capability over the native fake provider, with the shared parent
//! referee (domain/host.md, sections 9 and 10; testing-strategy.md, section 6).
//! Stage, Schedule, Ledger and Trace own flow, time, races and observations.

use skein_fake_llm_domain as provider;
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token};
use skein_world::domain::{Ledger, Schedule, Stage, Trace};
use smith_domain::{self as smith, run};
use smith_host_domain::{self as host, parent};
use smith_inline_agent::{self as inline, Below, Input, Lower, Output};
use std::collections::BTreeMap;

use crate::Seen;

#[derive(Debug)]
struct Binding {
    agent: Token,
    owner: Token,
    tools: smith::tools::Grants,
    served: Box<[smith::llm::Served]>,
    asker: smith::llm::Asker,
    cancelled: bool,
}

/// Checked two-slot capacities, sharing the agent world's shipped root bounds.
#[must_use]
pub fn limits() -> inline::Limits {
    let smith = smith_agent_world::LIMITS;
    inline::Limits {
        slots: 2,
        smith,
        window: smith::Window { turns: 8, bytes: smith::max_turn_bytes(&smith).expect("root turn bound") * 8 },
        wall_time: Duration::from_secs(3600),
        cancel_grace: Duration::from_secs(5),
        facts: 32,
    }
}

/// Explicit value declarations for the existing native fake scripts.
#[must_use]
pub fn configuration() -> smith::Config {
    smith::Config {
        programs: Box::new([]),
        endpoints: Box::from([run::charter::Endpoint(0)]),
        models: [b"fake-1".as_slice(), b"fake-2".as_slice(), b"fake-3".as_slice()]
            .into_iter()
            .map(|model| smith::ConfiguredModel {
                efforts: Box::new([]),
                prices: smith_domain::run::Prices {
                    input: 1,
                    cached: 1,
                    cache_write: None,
                    output: 1,
                    unit: 1_000_000,
                },
                protocol_allowance: 0,
                endpoint: run::charter::Endpoint(0),
                model: model.into(),
                window: 65_536,
                output: 4096,
                head: Duration::from_secs(60),
                idle: Duration::from_secs(30),
            })
            .collect(),
    }
}

/// Workspace-free script, moved through the same typed parent Start.
#[must_use]
pub fn start(seed: u64, job: smith_agent_world::Job) -> host::Start {
    let mut settings = smith_agent_world::Settings::calm(seed);
    settings.job = job;
    let mut charter = smith_agent_world::scripted_charter(&settings);
    charter.conventions = None;
    charter.grants.tools = run::charter::Tools { inspect: false, modify: false, shell: false };
    charter.grants.agents = false;
    charter.models = Box::default();
    host::Start {
        live: None,
        messages: Box::default(),
        logical_run: Token::new(seed),
        identity: None,
        activation: 1,
        workspace: None,
        charter: host::Charter::new(charter, settings.limits.run.run_bytes).expect("bounded script charter"),
        transcript: None,
        answered: Box::default(),
        directories: Box::default(),
        grants: Box::from([host::Grant {
            account: 0,
            generation: 1,
            valid: Duration::from_secs(7200),
            form: smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer),
        }]),
    }
}

/// A prepared native workspace with original mounted-root identity.
#[must_use]
pub fn workspace_start(seed: u64, job: smith_agent_world::Job) -> host::Start {
    let mut settings = smith_agent_world::Settings::calm(seed);
    settings.job = job;
    let mut charter = smith_agent_world::scripted_charter(&settings);
    charter.models = Box::default();
    charter.grants.agents = false;
    charter.conventions =
        Some(run::Conventions { guide: b"AGENTS.md".as_slice().into(), checks: b"check-ok".as_slice().into() });
    let mut source = start(seed, job);
    source.charter = host::Charter::new(charter, settings.limits.run.run_bytes).expect("bounded workspace charter");
    source.workspace = Some(Token::new(9));
    source.directories = Box::from([host::Directory {
        host: Box::from(b"/host".as_slice()),
        name: b"work".as_slice().into(),
        root: Token::new(1),
        writable: true,
        git: true,
        conflicts: Box::default(),
    }]);
    source
}

/// Select whether native tool terminals cross the inline boundary immediately.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ToolTerminals {
    /// Forward each completed native command.
    Forward,
    /// Retain completed commands until explicitly released.
    Hold,
}

/// Boundary identities retained after the lower ledger settles its rights.
#[derive(Debug)]
pub enum Route {
    /// A provider request with its independent script identity.
    Completion { agent: Token, owner: Token, system: Box<[u8]> },
    /// A machine request with its original mounted path.
    Machine { agent: Token, request: smith_protocol_machine::FromDomain },
    /// A parent call with the logical run supplied at Spawn.
    Called { client: Token, logical_run: Token },
}

/// Real inline agent with native fake LLM and boundary-only rights accounting.
pub struct World {
    pub agent: inline::Domain,
    pub stage: Stage<inline::Limits, parent::Event, Output>,
    pub schedule: Schedule<parent::Event>,
    pub seen: BTreeMap<Token, Seen>,
    pub history: BTreeMap<Token, Vec<smith::session::record::Turn>>,
    pub facts: Vec<(Token, smith::Fact)>,
    pub content: Vec<(Token, smith::Content)>,
    pub observations: Vec<inline::Fact>,
    pub trace: Trace,
    pub long_notices: Vec<(Time, Option<(host::Operation, Duration)>)>,
    pub lower: Ledger<Token, (Token, Token)>,
    pub routes: Vec<(Time, Route)>,
    provider: provider::Domain,
    failure: Option<(smith::llm::Failure, smith::llm::Evidence)>,
    provider_env: Env<provider::Config>,
    provider_out: Queue<provider::Request>,
    bindings: BTreeMap<Token, Binding>,
    minted: u64,
    pub machine: smith_machine_world::World,
    pub machine_trace: Vec<String>,
    pub auto_machine: bool,
    pub tool_terminals: ToolTerminals,
    pub pending_machine: Vec<(Token, Token, Below)>,
    pub auto_ack: bool,
    pub auto_answer: bool,
    pending: Vec<parent::Event>,
}

impl World {
    /// Construct from bounded capacities and deterministic provider timing.
    #[must_use]
    pub fn new(seed: u64, bounds: inline::Limits) -> Self {
        let configuration = smith_agent_world::Settings::calm(seed).provider;
        let mut trace = Trace::default();
        trace.log(Time::ZERO, format!("seed {seed}"));
        Self {
            agent: inline::Domain::new(&bounds, configuration_value(), seed),
            stage: Stage::new(bounds, inline::max_out(&bounds), inline::max_out(&bounds) + 2),
            schedule: Schedule::new(),
            seen: BTreeMap::new(),
            history: BTreeMap::new(),
            facts: Vec::new(),
            content: Vec::new(),
            observations: Vec::new(),
            trace,
            long_notices: Vec::new(),
            lower: Ledger::new("inline native provider terminal"),
            routes: Vec::new(),
            provider: smith_agent_world::scripted_provider(&configuration, seed),
            failure: None,
            provider_env: Env { now: Time::ZERO, wall: skein_lib::Wall::EPOCH, limits: configuration },
            provider_out: Queue::uncapped(),
            bindings: BTreeMap::new(),
            minted: 0,
            machine: smith_machine_world::World::bounded(
                seed,
                &[
                    skein_fake_machine::Item::directory(b"src"),
                    skein_fake_machine::Item::file(b"src/lib.rs", b"pub fn answer() -> u32 { 42 }\n"),
                    skein_fake_machine::Item::file(b"README.md", b"smith"),
                    skein_fake_machine::Item::file(b"AGENTS.md", b"The answer is in src/lib.rs"),
                    skein_fake_machine::Item { mode: 0o755, ..skein_fake_machine::Item::file(b"check-ok", b"") },
                ],
                smith_protocol_machine::Limits {
                    operations: 8,
                    roots: 1,
                    path_bytes: 4096,
                    file_bytes: 1_048_576,
                    tool_payload: smith_agent_world::LIMITS.session.tools.tool_payload,
                    entries: 256,
                    entry_bytes: 65_536,
                    processes: 2,
                    output_bytes: 65_536,
                    search_hits: 256,
                    search_bytes: 65_536,
                    search_line_bytes: 65_536,
                    env_bytes: 65_536,
                    group_stop: Duration::from_millis(1),
                },
            ),
            machine_trace: Vec::new(),
            auto_machine: true,
            tool_terminals: ToolTerminals::Forward,
            pending_machine: Vec::new(),
            auto_ack: true,
            auto_answer: true,
            pending: Vec::new(),
        }
    }

    /// Fail actual provider requests with selected typed terminal metadata.
    pub fn fail_with(&mut self, failure: smith::llm::Failure, evidence: smith::llm::Evidence) {
        assert_eq!(self.provider.calls(), 0);
        self.provider_env.limits.overloaded = 1000;
        self.provider = smith_agent_world::scripted_provider(&self.provider_env.limits, 1);
        self.failure = Some((failure, evidence));
    }

    /// Replace the native provider script before the first request.
    pub fn scripts(&mut self, scripts: Box<[provider::api::Script]>) {
        assert_eq!(self.provider.calls(), 0);
        self.provider = provider::Domain::scripted(&self.provider_env.limits, 1, scripts);
    }

    /// Hold native provider replies for an exact virtual span, before any call.
    pub fn provider_latency(&mut self, span: Duration) {
        assert_eq!(self.provider.calls(), 0);
        self.provider_env.limits.latency_min = span;
        self.provider_env.limits.latency_max = span;
        self.provider = smith_agent_world::scripted_provider(&self.provider_env.limits, 1);
    }

    /// Return the generation-safe parent handle learned from Started.
    #[must_use]
    pub fn handle(&self, client: Token) -> Token {
        self.seen[&client].agent.expect("actual Started")
    }

    /// Deliver one parent command, recording actual parent terminals first.
    pub fn event(&mut self, event: parent::Event) {
        match &event {
            parent::Event::Spawn { client, start } => {
                let seen = self.seen.entry(*client).or_default();
                seen.messages.extend(start.messages.iter().map(|message| message.name));
                seen.sent_messages.extend(start.messages.iter().map(|message| message.name));
            }
            parent::Event::Message { agent, name, .. } => {
                if let Some(seen) =
                    self.seen.values_mut().find(|seen| seen.agent == Some(*agent) && seen.gone.is_none())
                {
                    seen.messages.push(*name);
                    seen.sent_messages.insert(*name);
                }
            }
            parent::Event::Answer { agent, call, .. } => {
                let seen =
                    self.seen.values_mut().find(|seen| seen.agent == Some(*agent)).expect("parent's actual slot");
                assert!(seen.calls.remove(call), "one actual parent terminal");
            }
            parent::Event::Acknowledge { agent, turn } => {
                let seen =
                    self.seen.values_mut().find(|seen| seen.agent == Some(*agent)).expect("parent's actual slot");
                seen.turns.remove(turn);
            }
            parent::Event::Drop { agent } => {
                let seen =
                    self.seen.values_mut().find(|seen| seen.agent == Some(*agent)).expect("parent's actual slot");
                seen.turns.clear();
            }
            parent::Event::Grant { .. } | parent::Event::Stop { .. } => {}
        }
        self.trace.log(self.stage.env.now, format!("up {event:?}"));
        self.stage.push(event);
        while let Some(event) = self.stage.next_event() {
            inline::step(&mut self.agent, &self.stage.env, Input::Parent(event), &mut self.stage.out);
            self.outputs();
        }
        self.pass();
    }

    /// Advance to one actual clock point; queued inputs precede root alarms.
    pub fn at(&mut self, now: Time) {
        assert!(now >= self.stage.env.now);
        self.stage.tick(now);
        self.provider_env.now = now;
        while let Some(event) = self.schedule.next(now) {
            self.event(event);
        }
        while self.provider.is_due(now) {
            provider::fire(&mut self.provider, &self.provider_env, &mut self.provider_out);
            self.provider_replies();
        }
        while self.agent.is_due(now) {
            assert!(self.stage.has_room());
            inline::fire(&mut self.agent, &self.stage.env, &mut self.stage.out);
            self.outputs();
            self.pass();
        }
        self.pass();
    }

    /// Earliest actual provider, slot or scheduled parent alarm.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.provider
            .next_deadline()
            .into_iter()
            .chain(self.agent.next_deadline())
            .chain(self.schedule.next_time())
            .min()
    }

    /// Drive real alarms until every requested slot has its containment terminal.
    pub fn run(&mut self) {
        for _ in 0..4096 {
            self.pass();
            if self.seen.values().all(|seen| seen.gone.is_some()) && !self.seen.is_empty() && self.schedule.is_empty() {
                return;
            }
            let next = self
                .provider
                .next_deadline()
                .into_iter()
                .chain(self.agent.next_deadline())
                .chain(self.schedule.next_time())
                .min()
                .unwrap_or_else(|| panic!("live owner has an alarm\n{}", self.trace.lines().join("\n")));
            self.at(next);
        }
        panic!("bounded inline story did not settle\n{}", self.trace.lines().join("\n"));
    }

    /// Verify every lower right and emitted parent call/turn is settled.
    pub fn settled(&self) {
        self.lower.assert_settled();
        assert!(self.bindings.is_empty());
        assert!(self.pending_machine.is_empty());
        assert_eq!(self.agent.hosted(), 0);
        for seen in self.seen.values() {
            assert!(seen.gone.is_some());
            assert!(seen.calls.is_empty());
            assert!(seen.turns.is_empty());
        }
        assert_eq!(self.agent.next_deadline(), None);
        assert!(self.schedule.is_empty());
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "diagnostics capture routing identities and long notices; the shared parent referee owns all requests"
    )]
    fn record_route(&mut self, request: &Output) {
        match request {
            Output::Parent(parent::Request::Called { client, logical_run, .. }) => {
                self.routes.push((self.stage.env.now, Route::Called { client: *client, logical_run: *logical_run }));
            }
            Output::Parent(parent::Request::Long { operation, span, .. }) => {
                self.long_notices.push((self.stage.env.now, Some((*operation, *span))));
            }
            Output::Parent(parent::Request::LongDone { .. }) => self.long_notices.push((self.stage.env.now, None)),
            Output::Lower { agent, request: Lower::Complete { owner, prompt, .. } } => self.routes.push((
                self.stage.env.now,
                Route::Completion { agent: *agent, owner: *owner, system: prompt.system.clone() },
            )),
            _ => {}
        }
    }

    fn outputs(&mut self) {
        while let Some(request) = self.stage.out.pop() {
            self.trace.log(self.stage.env.now, format!("down {request:?}"));
            self.record_route(&request);
            match request {
                Output::Parent(request) => {
                    let client = parent_client(&request);
                    let seen = self.seen.entry(client).or_default();
                    if let parent::Request::Turn { turn, .. } = &request {
                        self.history.entry(client).or_default().push(turn.body.value().clone());
                        if self.auto_ack {
                            self.pending.push(parent::Event::Acknowledge {
                                agent: seen.agent.expect("Started before Turn"),
                                turn: turn.number,
                            });
                        }
                    }
                    if let parent::Request::Called { call, ask, .. } = &request
                        && self.auto_answer
                    {
                        self.pending.push(parent::Event::Answer {
                            agent: seen.agent.expect("Started before Called"),
                            call: *call,
                            reply: match ask {
                                host::Ask::Host { .. } => {
                                    host::Reply::Host { error: false, body: b"host completed".as_slice().into() }
                                }
                                host::Ask::Deliver { .. } => host::Reply::Delivery(host::Delivery::Delivered(
                                    host::Delivered::new(Box::from([
                                        host::Receipt::new(0, b"kept".as_slice().into()).expect("bounded receipt")
                                    ]))
                                    .expect("bounded delivery"),
                                )),
                            },
                        });
                    }
                    let lower_settled = !self.bindings.values().any(|binding| Some(binding.agent) == seen.agent)
                        && !self.pending_machine.iter().any(|(agent, _, _)| Some(*agent) == seen.agent);
                    seen.observe_parent(request, lower_settled, lower_settled);
                }
                Output::Lower { agent, request } => match request {
                    Lower::Complete { owner, prompt, .. } => {
                        self.minted += 1;
                        let callback = Token::new(self.minted);
                        let asker = prompt.asker;
                        let tools = prompt.tools;
                        let served = prompt.served.clone();
                        assert!(
                            self.bindings
                                .insert(callback, Binding { agent, owner, tools, served, asker, cancelled: false })
                                .is_none()
                        );
                        self.lower.open(callback, (agent, owner));
                        provider::step(
                            &mut self.provider,
                            &self.provider_env,
                            provider::Event::Call {
                                reply_to: ReplyTo::new(callback),
                                call: callback,
                                query: smith_agent_world::translate::query(prompt),
                            },
                            &mut self.provider_out,
                        );
                        self.provider_replies();
                    }
                    Lower::Cancel { owner } => {
                        let binding = self
                            .bindings
                            .values_mut()
                            .find(|binding| binding.agent == agent && binding.owner == owner)
                            .expect("cancel retains actual provider call");
                        assert!(!binding.cancelled, "cancel emitted once");
                        binding.cancelled = true;
                    }
                    Lower::Io { owner, op, deadline } => {
                        self.machine_request(agent, smith_protocol_machine::FromDomain::Op { owner, op, deadline });
                    }
                    Lower::Job { owner, act } => {
                        self.machine_request(agent, smith_protocol_machine::FromDomain::Job { owner, act });
                    }
                    Lower::Read { owner, at, max, deadline } => self
                        .machine_request(agent, smith_protocol_machine::FromDomain::Read { owner, at, max, deadline }),
                    Lower::Probe { owner, at, deadline } => {
                        self.machine_request(agent, smith_protocol_machine::FromDomain::Probe { owner, at, deadline });
                    }
                    Lower::Check { owner, program, deadline, tail } => self.machine_request(
                        agent,
                        smith_protocol_machine::FromDomain::Check { owner, program, deadline, tail },
                    ),
                    Lower::CancelIo { owner } | Lower::Abort { owner } => {
                        self.machine_trace.push(format!("cancel {agent:?} {owner:?}"));
                    }
                },
            }
        }
    }

    fn machine_request(&mut self, agent: Token, request: smith_protocol_machine::FromDomain) {
        self.routes.push((self.stage.env.now, Route::Machine { agent, request: request.clone() }));
        self.machine_trace.push(format!("{request:?}"));
        let terminal = match self.machine.request(request) {
            smith_protocol_machine::ToDomain::Done { owner, done } => Below::Done { owner, done },
            smith_protocol_machine::ToDomain::Read { owner, read } => Below::Read { owner, read },
            smith_protocol_machine::ToDomain::Probed { owner, executable } => Below::Probed { owner, executable },
            smith_protocol_machine::ToDomain::Checked { owner, ran } => Below::Checked { owner, ran },
            smith_protocol_machine::ToDomain::Aborted { owner } => Below::Aborted { owner },
        };
        self.minted += 1;
        let callback = Token::new(self.minted);
        self.lower.open(callback, (agent, callback));
        self.pending_machine.push((agent, callback, terminal));
    }

    /// Return already settled native-machine callbacks, optionally held across a parent race.
    pub fn machine_replies(&mut self) {
        for (agent, callback, terminal) in std::mem::take(&mut self.pending_machine) {
            self.lower.end(callback);
            inline::step(&mut self.agent, &self.stage.env, Input::Below { agent, terminal }, &mut self.stage.out);
            self.outputs();
        }
    }

    fn machine_ready_replies(&mut self) {
        let mut held = Vec::new();
        for (agent, callback, terminal) in std::mem::take(&mut self.pending_machine) {
            if self.tool_terminals == ToolTerminals::Hold && matches!(&terminal, Below::Done { .. }) {
                held.push((agent, callback, terminal));
            } else {
                self.pending_machine.push((agent, callback, terminal));
            }
        }
        self.machine_replies();
        self.pending_machine.extend(held);
    }

    fn provider_replies(&mut self) {
        for _ in 0..self.provider_out.len() {
            let request = self.provider_out.pop().expect("provider output present at pass start");
            let (to, result) = match request {
                provider::Request::Reply { to, result } => (to, result),
                other @ (provider::Request::Piece { .. } | provider::Request::Hold { .. }) => {
                    panic!("typed provider expected a terminal reply, got {other:?}")
                }
            };
            let callback = to.into_token();
            let binding = self.bindings.remove(&callback).expect("one fake terminal");
            self.lower.end(callback);
            let completion = if binding.cancelled {
                Below::Cancelled { owner: binding.owner }
            } else {
                match result {
                    Ok(answer) => Below::Completed {
                        owner: binding.owner,
                        completion: smith_agent_world::translate::completion(
                            answer,
                            binding.tools,
                            &binding.served,
                            binding.asker,
                        ),
                    },
                    Err(error) => {
                        let (failure, evidence) = self.failure.unwrap_or((
                            smith_agent_world::translate::failure(error.error),
                            smith::llm::Evidence::Response { status: None },
                        ));
                        Below::Failed {
                            owner: binding.owner,
                            failure,
                            evidence,
                            detail: Box::default(),
                            report: smith_domain::llm::Report::none(),
                        }
                    }
                }
            };
            inline::step(
                &mut self.agent,
                &self.stage.env,
                Input::Below { agent: binding.agent, terminal: completion },
                &mut self.stage.out,
            );
            self.outputs();
        }
        self.provider.reclaim();
    }

    fn pass(&mut self) {
        for _ in 0..4096 {
            self.outputs();
            if self.auto_machine
                && self.pending_machine.iter().any(|(_, _, terminal)| {
                    self.tool_terminals == ToolTerminals::Forward || !matches!(terminal, Below::Done { .. })
                })
            {
                self.machine_ready_replies();
                continue;
            }
            let pending = std::mem::take(&mut self.pending);
            for event in pending {
                self.event(event);
            }
            for client in self.seen.keys() {
                while let Some(fact) = self.agent.pop_fact(*client) {
                    self.facts.push((*client, fact));
                }
                while let Some(content) = self.agent.pop_content(*client) {
                    self.content.push((*client, content));
                }
            }
            while let Some(fact) = self.agent.pop_slot_fact() {
                self.observations.push(fact);
            }
            self.agent.reclaim();
            if !self.agent.is_ready() {
                return;
            }
            assert!(self.stage.has_room());
            inline::resume(&mut self.agent, &self.stage.env, &mut self.stage.out);
        }
        panic!("bounded ready work did not settle");
    }
}

fn configuration_value() -> smith::Config {
    configuration()
}

fn parent_client(request: &parent::Request) -> Token {
    match request {
        parent::Request::Started { client, .. }
        | parent::Request::Admitted { client }
        | parent::Request::Called { client, .. }
        | parent::Request::Withdrawn { client, .. }
        | parent::Request::Turn { client, .. }
        | parent::Request::Waiting { client, .. }
        | parent::Request::Long { client, .. }
        | parent::Request::LongDone { client }
        | parent::Request::Unread { client, .. }
        | parent::Request::Unauthorized { client, .. }
        | parent::Request::Refused { client, .. }
        | parent::Request::Limited { client, .. }
        | parent::Request::Told { client, .. }
        | parent::Request::Answered { client, .. }
        | parent::Request::Faulted { client, .. }
        | parent::Request::MessageRefused { client, .. }
        | parent::Request::Gone { client, .. } => *client,
    }
}
