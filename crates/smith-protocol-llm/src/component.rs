//! A bounded LLM component over skein's connection pool.
//! It keeps endpoint names, the last charter's account-to-endpoint bindings,
//! two grant generations per account, and a context
//! for each accepted call, one activation's attested declarations,
//! and one exact request awaiting its prompt record.
//! It never knows retry policy, host credentials, or
//! provider wire grammar. `from_domain`, `from_below`, `fire` and `reclaim`
//! route one bounded pass; `close` ends the owner's retained connection pool.
//! Accepted calls have one typed terminal.
//! Contract: protocol/llm.md, sections 1, 6 to 10.

#![expect(clippy::manual_let_else, clippy::single_match, reason = "explicit bounded routing cases")]

use alloc::boxed::Box;
use core::fmt;

use skein_io::{Event as IoEvent, Request as IoRequest};
use skein_lib::{Env, List, Map, Queue, Set, Time, Token, bytes};
use skein_llm::{self as shared, Credential};
use skein_llm_connection as connection;
use smith_domain::{Event as DomainEvent, GrantName, llm, run};

use crate::IdentityProfile;
use crate::boundary::{FromDomain, ToDomain};
use crate::prompt::prepare_component;
use crate::{Context, EndpointOptions, Endpoints, Error, GrantError, Grants, Receiving};

/// Startup receiving and capacity bounds for the LLM component.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct ComponentLimits {
    /// Translation and receiving bounds.
    pub adapter: crate::Limits,
    /// The connection pool and child-machine bounds.
    pub connection: connection::Limits,
    /// Largest Complete receiving contract admitted by this service.
    pub receiving: Receiving,
    /// Maximum retained charter result and delivery rule bytes per call.
    pub contract_bytes: u64,
    /// Maximum main and sub-agent LLMs whose grant bindings a start retains.
    pub llms: u32,
    /// Maximum distinct credential accounts.
    pub accounts: u32,
    /// Maximum bearer and provider account-id bytes in one grant.
    pub grant_value_bytes: u32,
}

/// A startup configuration problem.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ComponentError {
    /// A configured endpoint and the adapter disagree on client bounds.
    ClientLimits,
    /// A finite bound or worst case is invalid.
    Limits,
    /// The grant table could not be allocated at its bound.
    Grants(GrantError),
    /// The resolved endpoint list cannot initialize the connection pool.
    Connection(connection::EndpointError),
}

struct Active {
    context: Context,
    outcome: run::outcome::OutcomeSpec,
    deliver: Option<run::outcome::ChangeSpec>,
    observation: Option<crate::Observation>,
    deltas: crate::Deltas,
}

// One translated request waits while its owner encodes the observed new part.
// The credential stays inside this component and is never an observation.
struct Prepared {
    owner: Token,
    whole: Time,
    request: connection::Request,
}

/// Named endpoints, grants and the connection child for active calls.
pub struct Component {
    limits: ComponentLimits,
    options: Map<u32, EndpointOptions>,
    bindings: Map<(u32, u32), ()>,
    grants: Grants,
    active: Map<Token, Active>,
    next: Set<Token>,
    connection: connection::Component,
    events: Queue<connection::Event>,
    measures: Queue<crate::Measure>,
    prepared: Option<Prepared>,
    declarations: Option<crate::prompt::Declarations>,
    closed: bool,
}

impl fmt::Debug for Component {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Component")
            .field("active", &self.active.len())
            .field("grants", &"[redacted]")
            .finish_non_exhaustive()
    }
}

/// Output room required before one component entrance.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    /// Domain events from one entrance.
    pub above: u32,
    /// Io requests from one entrance.
    pub below: u32,
}

/// A fire pass may issue one pending Next and advance one child phase.
pub const MAX_OUT: MaxOut = MaxOut { above: 128, below: 128 };

// One prompt plus at most completion content and terminal measures for each
// routed event. Text, call-start and argument deltas each need one cell. The service drains before re-entry.
const MEASURES: u32 = MAX_OUT
    .above
    .checked_mul(2)
    .expect("two measure cells per routed event")
    .checked_add(1)
    .expect("one translated prompt beside routed measures");

/// Checked retained pool, grant, context, and event-queue ownership, using
/// configured endpoints to count their request heads and credential bounds.
#[must_use]
pub fn component_worst_case(limits: &ComponentLimits, endpoints: &Endpoints) -> Option<u64> {
    if limits.accounts == 0 || limits.llms == 0 || limits.connection.connections == 0 {
        return None;
    }
    let entrance = crate::worst_case(&limits.adapter, &limits.receiving)?;
    let base = connection::worst_case(&limits.connection, endpoints.destinations())?
        .checked_add(Grants::worst_case(limits.accounts, limits.grant_value_bytes)?)?
        .checked_add(Map::<u32, EndpointOptions>::worst_case(limits.connection.endpoints)?)?
        .checked_add(Map::<(u32, u32), ()>::worst_case(limits.llms)?)?
        .checked_add(endpoints.option_bytes()?)?
        .checked_add(Map::<Token, Active>::worst_case(limits.connection.connections)?)?
        .checked_add(Set::<Token>::worst_case(limits.connection.connections)?)?
        .checked_add(Queue::<connection::Event>::worst_case(MAX_OUT.above)?)?;
    // Active keeps declarations and contracts, not the consumed neutral
    // prompt or parser workspace. Only one component entrance translates or
    // decodes at once; the connection child owns every retained native Client.
    let retained = limits
        .adapter
        .tool_bytes
        .checked_add(limits.contract_bytes)?
        .checked_mul(u64::from(limits.connection.connections))?;
    // One activation retains raw declarations, names/descriptions and attested
    // records beside each call's neutral prompt. Contract and namespace bounds
    // are the same admitted root bounds used by Active.
    let compact = u64::try_from(size_of::<skein_json::document::Compact>()).ok()?;
    let declarations = limits
        .adapter
        .tool_bytes
        .checked_mul(compact.checked_add(3)?)?
        .checked_add(limits.contract_bytes)?
        .checked_add(u64::from(limits.adapter.stated.path_bytes))?
        .checked_add(List::<shared::Tool>::worst_case(limits.adapter.client.tools)?)?
        .checked_add(u64::try_from(size_of::<crate::prompt::Declarations>()).ok()?)?;
    base.checked_add(retained)?
        .checked_add(declarations)?
        .checked_add(entrance)?
        .checked_add(capture_worst_case(limits)?)
}

fn capture_worst_case(limits: &ComponentLimits) -> Option<u64> {
    let client = limits.adapter.client;
    let history = u64::from(client.history_items).checked_mul(
        u64::try_from(size_of::<smith_events::Message>())
            .ok()?
            .checked_add(u64::try_from(size_of::<smith_events::Block>()).ok()?)?,
    )?;
    let tools = u64::from(client.tools).checked_mul(u64::try_from(size_of::<Box<[u8]>>()).ok()?)?;
    let prompt = u64::from(client.request).checked_add(history)?.checked_add(tools)?;
    // The Client admits the sum of text, call IDs/names/arguments and replay
    // token payload under answer, rather than one full string per block.
    // Capture omits replay payload. Two separate answer allowances cover text
    // and call identity/argument measures beside completed provider blocks.
    let completion = List::<smith_events::Block>::worst_case(client.output_items)?
        .checked_add(u64::from(client.answer).checked_mul(3)?)?
        .checked_mul(u64::from(limits.connection.calls))?;
    Queue::<crate::Measure>::worst_case(MEASURES)?.checked_add(prompt)?.checked_add(completion)
}

impl Component {
    /// Validate the configured endpoint and pool bounds before the loop.
    pub fn new(limits: &ComponentLimits, endpoints: Endpoints) -> Result<Component, ComponentError> {
        for endpoint in endpoints.destinations() {
            let native = endpoint.limits;
            let maximum = limits.adapter.client;
            let completion_fits = match crate::completion_worst_case(&native, limits.receiving.decoded_call_bytes) {
                Some(bytes) => bytes <= limits.receiving.max_completion_bytes,
                None => false,
            };
            if native.request > maximum.request
                || native.tools > maximum.tools
                || native.history_items > maximum.history_items
                || native.output_items > maximum.output_items
                || native.detail_bytes > maximum.detail_bytes
                || native.strings > maximum.strings
                || native.answer > maximum.answer
                || !completion_fits
            {
                return Err(ComponentError::ClientLimits);
            }
        }
        component_worst_case(limits, &endpoints).ok_or(ComponentError::Limits)?;
        let grants = match Grants::new(limits.accounts, limits.grant_value_bytes) {
            Ok(grants) => grants,
            Err(error) => return Err(ComponentError::Grants(error)),
        };
        let (destinations, options) = endpoints.into_parts();
        let connection = match connection::Component::new(destinations, &limits.connection) {
            Ok(connection) => connection,
            Err(error) => return Err(ComponentError::Connection(error)),
        };
        Ok(Component {
            limits: *limits,
            options,
            bindings: Map::with_capacity(limits.llms),
            grants,
            active: Map::with_capacity(limits.connection.connections),
            next: Set::with_capacity(limits.connection.connections),
            connection,
            events: Queue::with_capacity(MAX_OUT.above),
            measures: Queue::with_capacity(MEASURES),
            prepared: None,
            declarations: None,
            closed: false,
        })
    }

    /// The oldest content or transport observation; drained before routing terminals.
    pub fn pop_measure(&mut self) -> Option<crate::Measure> {
        self.measures.pop()
    }

    /// Whether every retained observation was drained before another component entrance.
    #[must_use]
    pub fn measures_drained(&self) -> bool {
        self.measures.is_empty() && self.prepared.is_none()
    }

    /// The owner encoded this prompt observation; release its exact request to the connection.
    pub fn prompt_recorded(
        &mut self,
        env: &Env<ComponentLimits>,
        owner: Token,
        to_domain: &mut Queue<ToDomain>,
        io: &mut Queue<IoRequest>,
    ) {
        let mut prepared = self.prepared.take().expect("one translated request owes its prompt observation");
        assert_eq!(prepared.owner, owner, "the observed prompt belongs to the held request");
        match &mut prepared.request {
            connection::Request::Start { deadlines, .. } => {
                deadlines.whole = Some(prepared.whole.saturating_since(env.now));
            }
            connection::Request::Next { .. }
            | connection::Request::Cancel { .. }
            | connection::Request::Close
            | connection::Request::Abort => unreachable!("only a Start waits on a prompt observation"),
        }
        let child_env = self.child_env(env);
        self.connection.down(&child_env, prepared.request, &mut self.events, io);
        self.next.insert(owner).expect("one pending demand per admitted call");
        self.route(env.now, to_domain);
    }

    /// Replace the last start's account-to-endpoint bindings before installing its grants.
    pub fn bind_grants(&mut self, charter: &run::Charter) -> Result<(), GrantError> {
        self.declarations = None;
        self.bindings = Map::with_capacity(self.limits.llms);
        for llm in core::iter::once(&charter.llm).chain(charter.models.iter()) {
            if let Some(account) = llm.account {
                match self.bindings.insert((account, llm.endpoint.0), ()) {
                    Ok(_) => {}
                    Err(_) => return Err(GrantError::Full),
                }
            }
        }
        Ok(())
    }

    /// Install a validated generation from the channel or a colocated host.
    pub fn grant(&mut self, name: GrantName, credential: Credential, lapses: Time) -> Result<(), GrantError> {
        if credential.value.is_empty() {
            return Err(GrantError::Scheme);
        }
        for ((account, endpoint), ()) in &self.bindings {
            if *account == name.account {
                let options = self.options.get(endpoint).ok_or(GrantError::Scheme)?;
                let accepted = match options.provider {
                    shared::Provider::OpenAiCodex => match credential.scheme {
                        shared::Scheme::Bearer => !credential.account_id.is_empty(),
                        shared::Scheme::Key => false,
                    },
                    shared::Provider::OpenAi | shared::Provider::Anthropic => credential.account_id.is_empty(),
                };
                if !accepted {
                    return Err(GrantError::Scheme);
                }
            }
        }
        self.grants.grant(name, credential, lapses)
    }

    /// Route one root-domain request to the connection child.
    pub fn from_domain(
        &mut self,
        env: &Env<ComponentLimits>,
        request: FromDomain,
        to_domain: &mut Queue<ToDomain>,
        io: &mut Queue<IoRequest>,
    ) {
        assert!(self.measures.is_empty(), "the owner drains observations before another entrance");
        match request {
            FromDomain::Complete { owner, grant, prompt, deadlines, bounds, outcome, deliver, observation, deltas } => {
                self.start(
                    env,
                    owner,
                    grant,
                    prompt,
                    deadlines,
                    bounds,
                    outcome,
                    deliver,
                    observation,
                    deltas,
                    to_domain,
                    io,
                );
            }
            FromDomain::Cancel { owner } => {
                self.next.remove(&owner);
                let child_env = self.child_env(env);
                self.connection.down(&child_env, connection::Request::Cancel { call: owner }, &mut self.events, io);
                self.route(env.now, to_domain);
            }
        }
    }

    /// Route one io answer to the connection child.
    pub fn from_below(
        &mut self,
        env: &Env<ComponentLimits>,
        event: IoEvent,
        to_domain: &mut Queue<ToDomain>,
        io: &mut Queue<IoRequest>,
    ) {
        assert!(self.measures.is_empty(), "the owner drains observations before another entrance");
        let child_env = self.child_env(env);
        self.connection.up(&child_env, event, &mut self.events, io);
        self.route(env.now, to_domain);
    }

    /// Advance one bounded pass of deadlines and buffered connection work.
    pub fn fire(&mut self, env: &Env<ComponentLimits>, to_domain: &mut Queue<ToDomain>, io: &mut Queue<IoRequest>) {
        assert!(self.measures.is_empty(), "the owner drains observations before another entrance");
        let child_env = self.child_env(env);
        match self.next.pop_first() {
            Some(call) => self.connection.down(&child_env, connection::Request::Next { call }, &mut self.events, io),
            None => {}
        }
        self.connection.fire(&child_env, &mut self.events, io);
        self.route(env.now, to_domain);
    }

    /// The next child deadline, if any.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.connection.next_deadline()
    }

    /// Whether buffered child work remains ready without another io answer.
    #[must_use]
    pub fn has_work(&self) -> bool {
        !self.events.is_empty() || !self.next.is_empty() || self.prepared.is_some() || self.connection.has_work()
    }

    /// Release settled child slots at the service's reclaim point.
    pub fn reclaim(&mut self) {
        self.connection.reclaim();
    }

    /// The owner has ended its run; close every retained physical connection.
    pub fn close(&mut self, env: &Env<ComponentLimits>, to_domain: &mut Queue<ToDomain>, io: &mut Queue<IoRequest>) {
        assert!(self.measures.is_empty(), "the owner drains observations before another entrance");
        self.declarations = None;
        let child_env = self.child_env(env);
        self.connection.down(&child_env, connection::Request::Close, &mut self.events, io);
        self.route(env.now, to_domain);
    }

    /// The owner interrupts teardown; abort every retained call and physical connection.
    pub fn abort(&mut self, env: &Env<ComponentLimits>, to_domain: &mut Queue<ToDomain>, io: &mut Queue<IoRequest>) {
        assert!(self.measures.is_empty(), "the owner drains observations before another entrance");
        self.declarations = None;
        let child_env = self.child_env(env);
        self.connection.down(&child_env, connection::Request::Abort, &mut self.events, io);
        self.route(env.now, to_domain);
    }

    /// The pool reported that every call and socket has settled.
    #[must_use]
    pub const fn closed(&self) -> bool {
        self.closed
    }

    fn child_env(&self, env: &Env<ComponentLimits>) -> Env<connection::Limits> {
        Env { now: env.now, wall: env.wall, limits: self.limits.connection }
    }

    #[expect(clippy::too_many_arguments, reason = "one domain Complete")]
    fn start(
        &mut self,
        env: &Env<ComponentLimits>,
        owner: Token,
        grant: Option<GrantName>,
        prompt: llm::Prompt,
        deadlines: smith_domain::session::Deadlines,
        bounds: Receiving,
        outcome: run::outcome::OutcomeSpec,
        deliver: Option<run::outcome::ChangeSpec>,
        observation: Option<crate::Observation>,
        deltas: crate::Deltas,
        to_domain: &mut Queue<ToDomain>,
        io: &mut Queue<IoRequest>,
    ) {
        if bounds.max_completion_bytes > self.limits.receiving.max_completion_bytes
            || bounds.max_completion_blocks > self.limits.receiving.max_completion_blocks
            || bounds.decoded_call_bytes > self.limits.receiving.decoded_call_bytes
            || bounds.max_failure_bytes > self.limits.receiving.max_failure_bytes
        {
            Self::local_failure(owner, llm::Failure::ApplicationLimit, bounds.max_failure_bytes, to_domain);
            return;
        }
        if self.active.contains_key(&owner) {
            Self::local_failure(owner, llm::Failure::Invalid, bounds.max_failure_bytes, to_domain);
            return;
        }
        let options = match self.options.get(&prompt.endpoint.0) {
            Some(options) => options,
            None => {
                Self::local_failure(owner, llm::Failure::Invalid, bounds.max_failure_bytes, to_domain);
                return;
            }
        };
        let drop_reasoning = options.drop_reasoning(&prompt.model);
        let credential = match self.grants.credential(grant, env.now) {
            Ok(credential) => credential,
            Err(GrantError::Missing | GrantError::Lapsed) => {
                Self::local_failure(owner, llm::Failure::Unauthorized, bounds.max_failure_bytes, to_domain);
                return;
            }
            Err(GrantError::Bounds | GrantError::Stale | GrantError::Full | GrantError::Scheme) => {
                Self::local_failure(owner, llm::Failure::Invalid, bounds.max_failure_bytes, to_domain);
                return;
            }
        };
        let purpose = prompt.purpose;
        let adapter = crate::Limits { client: options.client, ..self.limits.adapter };
        let (mut translated, context) = match prepare_component(
            owner,
            prompt,
            &outcome,
            deliver.as_ref(),
            bounds,
            &adapter,
            &mut self.declarations,
        ) {
            Ok(prepared) => prepared,
            Err(error) => {
                Self::refusal(owner, error, bounds.max_failure_bytes, to_domain);
                return;
            }
        };
        match options.identity {
            IdentityProfile::Plain => {}
            IdentityProfile::ClaudeCode => match shared::anthropic::identity::instructions(&translated.instructions) {
                Ok(instructions) => translated.instructions = instructions,
                Err(error) => {
                    Self::refusal(owner, Error::from_shared(error), bounds.max_failure_bytes, to_domain);
                    return;
                }
            },
        }
        if let Err(error) = translated.output_ceiling(options.provider, translated.max_output_tokens.unwrap_or(0)) {
            Self::refusal(owner, Error::from_shared(error), bounds.max_failure_bytes, to_domain);
            return;
        }
        if let Some(event) =
            crate::guard::check(purpose, &mut translated, credential.as_ref(), options, owner, bounds.max_failure_bytes)
        {
            Self::domain_event(event, to_domain);
            return;
        }
        let captured = match observed_prompt(&translated, credential.as_ref(), options, observation) {
            Ok(captured) => captured,
            Err(error) => {
                Self::refusal(owner, error, bounds.max_failure_bytes, to_domain);
                return;
            }
        };
        match self.active.insert(owner, Active { context, outcome, deliver, observation, deltas }) {
            Ok(None) => {}
            Ok(Some(_)) | Err(_) => {
                Self::local_failure(owner, llm::Failure::ApplicationLimit, bounds.max_failure_bytes, to_domain);
                return;
            }
        }
        match captured {
            Some(prompt) => self.measures.push(crate::Measure::Prompt { at: env.now, owner, prompt }),
            None => {}
        }
        let whole = deadlines.whole;
        let deadlines = connection_deadlines(env.now, options, deadlines);
        let request = connection::Request::Start {
            call: owner,
            endpoint: options.index,
            prompt: translated,
            credential,
            deadlines,
            drop_reasoning,
        };
        self.submit(env, Prepared { owner, whole, request }, observation.is_some(), to_domain, io);
    }

    fn submit(
        &mut self,
        env: &Env<ComponentLimits>,
        prepared: Prepared,
        observed: bool,
        to_domain: &mut Queue<ToDomain>,
        io: &mut Queue<IoRequest>,
    ) {
        if observed {
            assert!(self.prepared.is_none(), "the owner releases each prompt before the next entrance");
            self.prepared = Some(prepared);
            return;
        }
        let child_env = self.child_env(env);
        self.connection.down(&child_env, prepared.request, &mut self.events, io);
        self.next.insert(prepared.owner).expect("one pending demand per admitted call");
        self.route(env.now, to_domain);
    }

    fn record_closed(&mut self) {
        assert!(!self.closed, "one pool close terminal");
        self.closed = true;
    }

    fn route(&mut self, now: Time, to_domain: &mut Queue<ToDomain>) {
        for _ in 0..MAX_OUT.above {
            let event = match self.events.pop() {
                Some(event) => event,
                None => break,
            };
            match event {
                connection::Event::Closed => self.record_closed(),
                connection::Event::Refused { call, why } => {
                    self.next.remove(&call);
                    let active = self.active.remove(&call);
                    let maximum = match &active {
                        Some(active) => active.context.receiving().max_failure_bytes,
                        None => 0,
                    };
                    let error = match why {
                        connection::Refusal::Memory { reservation, bound } => {
                            Self::local_failure(
                                call,
                                llm::Failure::Limit { which: llm::Limit::Pool { reservation }, bound },
                                maximum,
                                to_domain,
                            );
                            continue;
                        }
                        connection::Refusal::Endpoint => Error::Invalid,
                        connection::Refusal::Calls { bound: _ } => Error::Limit,
                        connection::Refusal::Closed => unreachable!("the service closes the pool after its answer"),
                        connection::Refusal::Client(error) => Error::from_shared(error),
                    };
                    Self::refusal(call, error, maximum, to_domain);
                }
                connection::Event::Delta { call, delta } => {
                    self.delta(now, call, delta, to_domain);
                    self.next.insert(call).expect("one pending demand per admitted call");
                }
                connection::Event::Block { call, block: _ } => {
                    self.next.insert(call).expect("one pending demand per admitted call");
                }
                connection::Event::Completed { call, completion, measures } => {
                    self.next.remove(&call);
                    match self.active.remove(&call) {
                        Some(active) => {
                            self.measures.push(crate::Measure::Terminal { owner: call, measures });
                            match active.observation {
                                Some(_) => {
                                    let completion =
                                        crate::capture::completion(&completion, &active.context.limits.client)
                                            .expect("the actual client admitted its bounded blocks and replay");
                                    self.measures.push(crate::Measure::Completion { at: now, owner: call, completion });
                                }
                                None => {}
                            }
                            let resolved = self.resolve(&completion, &active);
                            match crate::completion(active.context, call, completion, resolved) {
                                Ok(event) => Self::domain_event(event, to_domain),
                                Err(error) => {
                                    Self::refusal(call, error, self.limits.adapter.client.detail_bytes, to_domain);
                                }
                            }
                        }
                        None => {}
                    }
                }
                connection::Event::Failed { call, failure, evidence, report, detail, measures } => {
                    self.next.remove(&call);
                    match self.active.remove(&call) {
                        Some(active) => {
                            self.measures.push(crate::Measure::Terminal { owner: call, measures });
                            match crate::failed(active.context, call, failure, evidence, detail, report) {
                                Ok(event) => Self::domain_event(event, to_domain),
                                Err(error) => {
                                    Self::refusal(call, error, self.limits.adapter.client.detail_bytes, to_domain);
                                }
                            }
                        }
                        None => {}
                    }
                }
                connection::Event::Cancelled { call, measures } => {
                    self.next.remove(&call);
                    match self.active.remove(&call) {
                        Some(active) => {
                            self.measures.push(crate::Measure::Terminal { owner: call, measures });
                            match crate::cancelled(active.context, call) {
                                Ok(event) => Self::domain_event(event, to_domain),
                                Err(error) => {
                                    Self::refusal(call, error, self.limits.adapter.client.detail_bytes, to_domain);
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
        }
    }

    fn delta(&mut self, now: Time, call: Token, delta: shared::Delta, to_domain: &mut Queue<ToDomain>) {
        match delta {
            shared::Delta::Text { index, content_index: _, text } => {
                let count = u32::try_from(text.len()).expect("the client admitted a u32 text bound");
                to_domain.push(ToDomain::Text { owner: call, bytes: count });
                let live = match self.active.get(&call) {
                    Some(active) => active.deltas.text,
                    None => false,
                };
                if live {
                    self.measures.push(crate::Measure::Delta { at: now, owner: call, block: index, text });
                }
            }
            shared::Delta::Reasoning { text, .. } => {
                let count = u32::try_from(text.len()).expect("the client admitted a u32 text bound");
                to_domain.push(ToDomain::Text { owner: call, bytes: count });
            }
            shared::Delta::ToolStart { index, id, name } => {
                if self.input_wanted(call) {
                    self.measures.push(crate::Measure::Call { at: now, owner: call, index, id, name });
                }
            }
            shared::Delta::ToolArguments { index, delta } => {
                if self.input_wanted(call) {
                    self.measures.push(crate::Measure::Arguments { at: now, owner: call, index, bytes: delta });
                }
            }
        }
    }

    fn input_wanted(&self, owner: Token) -> bool {
        match self.active.get(&owner) {
            Some(active) => active.deltas.input,
            None => false,
        }
    }

    fn resolve(&self, completion: &shared::Completion, active: &Active) -> Box<[crate::ResolvedCall]> {
        let mut resolved = List::with_capacity(self.limits.adapter.client.output_items);
        for (position, block) in completion.content.iter().enumerate() {
            let shared::Block::ToolCall { name, arguments, .. } = block else {
                continue;
            };
            let mut offered = false;
            for descriptor in active.context.application() {
                if descriptor.name.as_ref() == name.as_ref() {
                    offered = true;
                }
            }
            if !offered {
                continue;
            }
            let call = match name.as_ref() {
                b"finish" => crate::decode_finish(arguments, &active.outcome, &self.limits.adapter),
                b"deliver" => match &active.deliver {
                    Some(change) => crate::decode_deliver(arguments, change, &self.limits.adapter),
                    None => llm::Decoded::Invalid { problem: llm::Problem::UnknownTool },
                },
                _ => crate::decode(
                    name,
                    arguments,
                    active.context.grants(),
                    active.context.served(),
                    active.context.asker(),
                    &self.limits.adapter,
                ),
            };
            // The completion adapter resolves host declarations itself.
            let mut hosted = false;
            for offered in active.context.served() {
                match offered {
                    llm::Served::Host(tool) if tool.name.as_ref() == name.as_ref() => hosted = true,
                    llm::Served::Host(_)
                    | llm::Served::Wait
                    | llm::Served::Deliver
                    | llm::Served::Finish
                    | llm::Served::SubAgent => {}
                }
            }
            if hosted {
                continue;
            }
            match u32::try_from(position) {
                Ok(position) => {
                    resolved
                        .push(crate::ResolvedCall {
                            position,
                            name: bytes::copy_of(name),
                            input: bytes::copy_of(arguments),
                            call,
                        })
                        .expect("admitted completion block count");
                }
                Err(_) => break,
            }
        }
        resolved.into_boxed()
    }

    fn domain_event(event: DomainEvent, to_domain: &mut Queue<ToDomain>) {
        match event {
            DomainEvent::Completed { owner, completion } => to_domain.push(ToDomain::Completed { owner, completion }),
            DomainEvent::Failed { owner, failure, evidence, detail, report } => {
                to_domain.push(ToDomain::Failed { owner, failure, evidence, detail, report });
            }
            DomainEvent::Cancelled { owner } => to_domain.push(ToDomain::Cancelled { owner }),
            DomainEvent::HostReturned { .. }
            | DomainEvent::Start { .. }
            | DomainEvent::Message { .. }
            | DomainEvent::Acknowledge { .. }
            | DomainEvent::Grant { .. }
            | DomainEvent::Cancel { .. }
            | DomainEvent::Delivered { .. }
            | DomainEvent::Done { .. }
            | DomainEvent::Read { .. }
            | DomainEvent::Probed { .. }
            | DomainEvent::Checked { .. }
            | DomainEvent::BuiltinResult { .. }
            | DomainEvent::Aborted { .. } => unreachable!("translation returns only LLM terminals"),
        }
    }

    fn refusal(owner: Token, error: Error, maximum: u32, to_domain: &mut Queue<ToDomain>) {
        Self::domain_event(crate::refusal(owner, error, maximum), to_domain);
    }

    fn local_failure(owner: Token, failure: llm::Failure, maximum: u32, to_domain: &mut Queue<ToDomain>) {
        let detail = b"credential or endpoint unavailable";
        let length = detail.len().min(usize::try_from(maximum).expect("u32 fits usize"));
        to_domain.push(ToDomain::Failed {
            owner,
            failure,
            evidence: llm::Evidence::Unsent,
            report: llm::Report::none(),
            detail: bytes::copy_of(detail.get(..length).expect("clipped fixed local diagnostic")),
        });
    }
}

// Validate the native preparation entrance before copying its observed payload.
// This is not a transport measurement: bytes remain unobserved until sending.
fn observed_prompt(
    translated: &shared::Prompt,
    credential: Option<&Credential>,
    options: &EndpointOptions,
    observation: Option<crate::Observation>,
) -> Result<Option<smith_events::Prompt>, Error> {
    match observation {
        Some(observation) => {
            match shared::client::measure(translated, credential, &options.endpoint, &options.client) {
                Ok(_) => {}
                Err(error) => return Err(Error::from_shared(error)),
            }
            let prompt = crate::capture::prompt(translated, observation, &options.client)
                .expect("the owner's cursor and translated replay were admitted");
            Ok(Some(prompt))
        }
        None => Ok(None),
    }
}

fn connection_deadlines(
    now: Time,
    options: &EndpointOptions,
    deadlines: smith_domain::session::Deadlines,
) -> connection::Deadlines {
    connection::Deadlines {
        connect: Some(options.connect),
        handshake: Some(options.handshake),
        head: Some(deadlines.head),
        idle: Some(deadlines.idle),
        whole: Some(deadlines.whole.saturating_since(now)),
    }
}
