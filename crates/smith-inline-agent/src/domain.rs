//! Slot ownership and routing (domain/host.md, sections 9.1, 9.2 and 9.4).
//! Slots retain root domains and every emitted lower right through settlement.
//! Lower requests, lifecycle facts and slot roots' facts are uncapped; the parent counts held facts.
//! Root requests keep their owners, while a separate slab generation scopes
//! callbacks. The parent owns told payloads; exact ACK metadata stays here.

#![expect(clippy::manual_let_else, reason = "checked routes remain exhaustive")]

use alloc::boxed::Box;
use skein_lib::{Env, Id, List, Map, Queue, Slab, Time, Token};
use smith_domain as smith;
use smith_host_domain::{self as host, parent};

use crate::stop::{Settling, Stage};
use crate::{Below, Fact, FactKind, Limits, Output, stop, translate};

#[derive(Debug)]
pub(crate) struct Relay {
    pub(crate) name: crate::workspace::Call,
    pub(crate) withdrawn: bool,
}

#[derive(Debug)]
pub(crate) struct Slot {
    pub(crate) root: smith::Domain,
    pub(crate) run: Option<Token>,
    pub(crate) client: Token,
    pub(crate) logical_run: Token,
    pub(crate) state: Stage,
    pub(crate) wall: Time,
    pub(crate) completions: Map<Token, bool>,
    pub(crate) io: Map<Token, bool>,
    pub(crate) machine: Map<Token, crate::workspace::Machine>,
    pub(crate) next_call: u64,
    pub(crate) relays: Map<Token, Relay>,
    pub(crate) turns: Map<u32, u64>,
    pub(crate) inbox: host::Inbox,
    pub(crate) facts_drained: bool,
    pub(crate) content_drained: bool,
}

/// Bounded independent Smith roots, addressed through generation-safe handles.
#[derive(Debug)]
pub struct Domain {
    pub(crate) slots: Slab<Slot>,
    pub(crate) clients: Map<Token, Id<Slot>>,
    configuration: smith::Config,
    pub(crate) lower: Queue<smith::Request>,
    facts: Queue<Fact>,
    seed: u64,
}

impl Domain {
    /// Allocate routing containers from checked limits and configured model names.
    #[must_use]
    pub fn new(limits: &Limits, configuration: smith::Config, seed: u64) -> Self {
        assert!(crate::valid(limits), "inline capacities are representable");
        assert!(configuration.valid(&limits.smith), "configured endpoints and models fit every root");
        Self {
            slots: Slab::with_capacity(limits.slots),
            clients: Map::with_capacity(limits.slots),
            configuration,
            lower: Queue::uncapped(),
            facts: Queue::uncapped(),
            seed,
        }
    }

    /// Reclaim ended slots after their outputs, facts and content were handed on.
    pub fn reclaim(&mut self) {
        reclaim(self);
    }

    /// Occupied slots, including ended slots until their native drains are empty.
    #[must_use]
    pub const fn hosted(&self) -> u32 {
        self.slots.len()
    }

    /// Whether a live root has deferred work for the owner's next pass.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        for (_, id) in &self.clients {
            let slot = self.slots.get(*id).expect("client binding names its retained slot");
            match slot.state {
                Stage::Live | Stage::Cancelling { .. } => {
                    if slot.root.is_ready() {
                        return true;
                    }
                }
                Stage::Settling(_) | Stage::Gone => {}
            }
        }
        false
    }

    /// Earliest root or independent wall alarm; waits never pause either.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut next = None;
        for (_, id) in &self.clients {
            let slot = self.slots.get(*id).expect("retained slot");
            match slot.state {
                Stage::Live => {
                    next = Some(earlier(next, slot.wall));
                    if let Some(at) = slot.root.next_deadline() {
                        next = Some(earlier(next, at));
                    }
                }
                Stage::Cancelling { grace } => {
                    next = Some(earlier(next, grace));
                    if let Some(at) = slot.root.next_deadline() {
                        next = Some(earlier(next, at));
                    }
                }
                Stage::Settling(_) | Stage::Gone => {}
            }
        }
        next
    }

    /// Whether one bounded alarm may be fired at the injected clock.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.next_deadline() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// Drain one native root fact by parent client before iteration-end reclaim.
    pub fn pop_fact(&mut self, client: Token) -> Option<smith::Fact> {
        let id = *self.clients.get(&client)?;
        let slot = self.slots.get_mut(id)?;
        let fact = slot.root.pop_fact();
        slot.facts_drained = fact.is_none();
        fact
    }

    /// Drain one root content record by parent client under the owner's capture policy.
    pub fn pop_content(&mut self, client: Token) -> Option<smith::Content> {
        let id = *self.clients.get(&client)?;
        let slot = self.slots.get_mut(id)?;
        let content = slot.root.pop_content();
        slot.content_drained = content.is_none();
        content
    }

    /// Drain one inline lifecycle observation for the parent.
    pub fn pop_slot_fact(&mut self) -> Option<Fact> {
        self.facts.pop()
    }

    /// Entries held for the parent to drain (skein's draft
    /// dynamic-memory.md 4.1): its lifecycle facts and its slot roots'
    /// facts. The parent root counts them in its backlog.
    #[must_use]
    pub fn backlog(&self) -> u32 {
        let mut entries = self.facts.len();
        for (_, id) in &self.clients {
            let slot = self.slots.get(*id).expect("client binding names its retained slot");
            entries = entries.checked_add(slot.root.backlog()).expect("inline backlog fits u32");
        }
        entries
    }
}

fn earlier(current: Option<Time>, candidate: Time) -> Time {
    match current {
        Some(current) => current.min(candidate),
        None => candidate,
    }
}

pub(crate) fn observe(agent: &mut Domain, now: Time, kind: FactKind) {
    agent.facts.push(Fact { at: now, kind });
}

pub(crate) const fn child_env(env: &Env<Limits>) -> Env<smith::Limits> {
    Env { now: env.now, wall: env.wall, limits: env.limits.smith }
}

pub(crate) fn root_step(
    agent: &mut Domain,
    env: &Env<Limits>,
    id: Id<Slot>,
    event: smith::Event,
    out: &mut Queue<Output>,
) {
    let slot = agent.slots.get_mut(id).expect("event belongs to a retained root");
    slot.facts_drained = false;
    slot.content_drained = false;
    smith::step(&mut slot.root, &child_env(env), event, &mut agent.lower);
    translate::route(agent, env, id, out);
}

fn refused(agent: &mut Domain, now: Time, client: Token, end: host::End, out: &mut Queue<Output>) {
    out.push(Output::Parent(parent::Request::Gone { client, end, detail: Box::default() }));
    observe(agent, now, FactKind::Gone { client, end });
}

fn spawn(agent: &mut Domain, env: &Env<Limits>, client: Token, start: host::Start, out: &mut Queue<Output>) {
    if agent.slots.is_full() || agent.clients.contains_key(&client) {
        refused(agent, env.now, client, host::End::Busy, out);
        return;
    }
    let logical_run = start.logical_run;
    let mut inbox = host::Inbox::new(env.limits.smith.run.messages);
    for message in &start.messages {
        if inbox
            .admit(message.name, &message.label, &message.text, u64::from(env.limits.smith.run.message_bytes))
            .is_err()
        {
            refused(agent, env.now, client, host::End::Invalid(host::Invalid::Messages), out);
            return;
        }
        inbox.sent(message.name);
    }
    let event = match translate::start(client, start, &env.limits) {
        Ok(event) => event,
        Err(invalid) => {
            refused(agent, env.now, client, host::End::Invalid(invalid), out);
            return;
        }
    };
    let wall = match env.now.checked_add(env.limits.wall_time) {
        Some(at) => at,
        None => {
            refused(agent, env.now, client, host::End::Invalid(host::Invalid::Limits), out);
            return;
        }
    };
    let slot = Slot {
        root: smith::Domain::new(&env.limits.smith, agent.configuration.clone(), agent.seed ^ client.raw()),
        run: None,
        client,
        logical_run,
        state: Stage::Live,
        wall,
        completions: Map::with_capacity(env.limits.smith.run.conversations),
        io: Map::with_capacity(crate::workspace::io_capacity(&env.limits)),
        machine: Map::with_capacity(env.limits.smith.run.runs),
        next_call: 0,
        relays: Map::with_capacity(crate::workspace::call_capacity(&env.limits)),
        turns: Map::with_capacity(env.limits.window.turns),
        inbox,
        facts_drained: false,
        content_drained: false,
    };
    let id = agent.slots.insert(slot).expect("free slot was checked");
    assert!(
        agent.clients.insert(client, id).expect("slot bound also bounds clients").is_none(),
        "root output preserves its scoped identity and single right"
    );
    out.push(Output::Parent(parent::Request::Started { client, agent: id.token() }));
    observe(agent, env.now, FactKind::Opened { client });
    root_step(agent, env, id, event, out);
}

/// Handle one command in the shared agent parent vocabulary.
fn command(agent: &mut Domain, env: &Env<Limits>, event: parent::Event, out: &mut Queue<Output>) {
    match event {
        parent::Event::Spawn { client, start } => spawn(agent, env, client, start, out),
        parent::Event::Message { finish_requested, agent: handle, name, label, text } => {
            let id = Id::<Slot>::from_token(handle);
            let slot = match agent.slots.get_mut(id) {
                Some(slot) => slot,
                None => return,
            };
            let run = match slot.state {
                Stage::Live => slot.run,
                Stage::Cancelling { .. } | Stage::Settling(_) | Stage::Gone => None,
            };
            match run {
                Some(run) => {
                    match slot.inbox.admit(name, &label, &text, u64::from(env.limits.smith.run.message_bytes)) {
                        Ok(()) => slot.inbox.sent(name),
                        Err(reason) => {
                            out.push(Output::Parent(parent::Request::MessageRefused {
                                client: slot.client,
                                name,
                                reason,
                            }));
                            return;
                        }
                    }
                    root_step(agent, env, id, smith::Event::Message { finish_requested, run, name, label, text }, out);
                }
                None => out.push(Output::Parent(parent::Request::MessageRefused {
                    client: slot.client,
                    name,
                    reason: host::MessageRefusal::Ending,
                })),
            }
        }
        parent::Event::Answer { agent: handle, call, reply } => {
            let id = Id::<Slot>::from_token(handle);
            let slot = match agent.slots.get_mut(id) {
                Some(slot) => slot,
                None => return,
            };
            let relay = match slot.relays.get(&call) {
                Some(relay) => relay.name,
                None => return,
            };
            let event = match crate::workspace::parent_answer(relay, reply) {
                Some(event) => event,
                None => return,
            };
            slot.relays.remove(&call).expect("the retained relay receives one answer");
            match slot.state {
                Stage::Live | Stage::Cancelling { .. } | Stage::Settling(Settling::Answered) => {
                    root_step(agent, env, id, event, out);
                }
                Stage::Settling(Settling::Dropped) | Stage::Gone => {}
            }
            stop::settle(agent, env.now, id, out);
        }
        parent::Event::Acknowledge { agent: handle, turn } => {
            let id = Id::<Slot>::from_token(handle);
            let slot = match agent.slots.get_mut(id) {
                Some(slot) => slot,
                None => return,
            };
            if slot.turns.remove(&turn).is_none() {
                return;
            }
            let run = slot.run;
            match slot.state {
                Stage::Live | Stage::Cancelling { .. } | Stage::Settling(Settling::Answered) => {
                    if let Some(run) = run {
                        root_step(agent, env, id, smith::Event::Acknowledge { run, turn }, out);
                    }
                }
                Stage::Settling(Settling::Dropped) | Stage::Gone => {}
            }
            stop::settle(agent, env.now, id, out);
        }
        parent::Event::Grant { agent: handle, grant } => {
            let id = Id::<Slot>::from_token(handle);
            let slot = match agent.slots.get(id) {
                Some(slot) => slot,
                None => return,
            };
            match slot.state {
                Stage::Live | Stage::Cancelling { .. } => {
                    root_step(agent, env, id, smith::Event::Grant { grant: translate::grant(grant) }, out);
                }
                Stage::Settling(_) | Stage::Gone => {}
            }
        }
        parent::Event::Stop { agent: handle } => stop::cancel(agent, env, Id::<Slot>::from_token(handle), out),
        parent::Event::Drop { agent: handle } => stop::drop_run(agent, env.now, Id::<Slot>::from_token(handle), out),
    }
}

/// Return one provider terminal to the slot that emitted its request.
fn terminal(agent: &mut Domain, env: &Env<Limits>, handle: Token, completion: Below, out: &mut Queue<Output>) {
    let id = Id::<Slot>::from_token(handle);
    let slot = match agent.slots.get_mut(id) {
        Some(slot) => slot,
        None => return,
    };
    let event = match crate::workspace::terminal(slot, completion) {
        Some(event) => event,
        None => return,
    };
    match slot.state {
        Stage::Live | Stage::Cancelling { .. } | Stage::Settling(Settling::Answered) => {
            root_step(agent, env, id, event, out);
        }
        Stage::Settling(Settling::Dropped) | Stage::Gone => {}
    }
    stop::settle(agent, env.now, id, out);
}

/// Fire one due root or wall alarm and route its complete fanout.
pub fn fire(agent: &mut Domain, env: &Env<Limits>, out: &mut Queue<Output>) {
    let mut due = None;
    for (_, id) in &agent.clients {
        let slot = agent.slots.get(*id).expect("retained slot");
        match slot.state {
            Stage::Live => {
                if slot.wall <= env.now || slot.root.is_due(env.now) {
                    due = Some(*id);
                    break;
                }
            }
            Stage::Cancelling { grace } => {
                if grace <= env.now || slot.root.is_due(env.now) {
                    due = Some(*id);
                    break;
                }
            }
            Stage::Settling(_) | Stage::Gone => {}
        }
    }
    let id = match due {
        Some(id) => id,
        None => return,
    };
    let slot = agent.slots.get_mut(id).expect("due slot");
    match slot.state {
        Stage::Live if slot.wall <= env.now => stop::cancel(agent, env, id, out),
        Stage::Cancelling { grace } if grace <= env.now => stop::drop_run(agent, env.now, id, out),
        Stage::Live | Stage::Cancelling { .. } => {
            slot.facts_drained = false;
            slot.content_drained = false;
            smith::fire(&mut slot.root, &child_env(env), &mut agent.lower);
            translate::route(agent, env, id, out);
        }
        Stage::Settling(_) | Stage::Gone => unreachable!("settling slots have no alarms"),
    }
}

/// Resume one deferred root handoff and route its complete fanout.
pub fn resume(agent: &mut Domain, env: &Env<Limits>, out: &mut Queue<Output>) {
    let mut ready = None;
    for (_, id) in &agent.clients {
        let slot = agent.slots.get(*id).expect("retained slot");
        match slot.state {
            Stage::Live | Stage::Cancelling { .. } => {
                if slot.root.is_ready() {
                    ready = Some(*id);
                    break;
                }
            }
            Stage::Settling(_) | Stage::Gone => {}
        }
    }
    if let Some(id) = ready {
        let slot = agent.slots.get_mut(id).expect("ready slot");
        slot.facts_drained = false;
        slot.content_drained = false;
        smith::resume(&mut slot.root, &child_env(env), &mut agent.lower);
        translate::route(agent, env, id, out);
    }
}

/// Reclaim after outputs and native observations have been drained for this pass.
fn reclaim(agent: &mut Domain) {
    let mut gone = List::with_capacity(agent.clients.capacity());
    for (client, id) in &agent.clients {
        let slot = agent.slots.get_mut(*id).expect("retained slot");
        slot.root.reclaim();
        match slot.state {
            Stage::Gone => {
                if slot.facts_drained && slot.content_drained {
                    gone.push(*client).expect("one entry per slot");
                }
            }
            Stage::Live | Stage::Cancelling { .. } | Stage::Settling(_) => {}
        }
    }
    for client in &gone {
        let id = agent.clients.remove(client).expect("retained client");
        agent.slots.retire(id);
    }
    agent.slots.reclaim();
}

/// Consume one parent command or terminal and retain its root facts until drained.
pub fn step(agent: &mut Domain, env: &Env<Limits>, input: crate::Input, out: &mut Queue<Output>) {
    match input {
        crate::Input::Parent(event) => command(agent, env, event, out),
        crate::Input::Below { agent: handle, terminal: below } => terminal(agent, env, handle, below, out),
    }
}
