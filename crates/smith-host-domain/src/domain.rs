//! V2 supervision cells and surviving terminal ledgers (domain/host.md, sections
//! 2–7 and 9.2). Process exit never discards parent calls. A parent's drop
//! releases held turns unacknowledged, retaining submitted send terminals.
//! The kit knows channel send order, not private agent stop
//! decisions; policy and durable decisions remain in the parent.
//! Content-free facts are retained losslessly until the parent drains them.
use crate::{
    AnsweredCall, Ask, CallName, Delivery, Down, End, Exit, Fact, Fault, Forced, Grant, Input, Invalid, Limits,
    MessageRefusal, Output, Reply, RunFailure, RunResult, Signal, Start, Up,
};
use crate::{parent, process};
use alloc::boxed::Box;
use skein_lib::{Deadlines, Env, Id, Map, Queue, Slab, Time, Token};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Phase {
    Spawning,
    Starting,
    Live,
    Cancelled,
    Draining,
    Exiting,
    Terminating,
    Killing,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(crate) enum Alarm {
    Watch { agent: Id<Agent> },
    Wall { agent: Id<Agent> },
    Grace { agent: Id<Agent> },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    Host,
    Delivery,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CallStage {
    Parent,
    Queued,
    Sending,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Call {
    name: CallName,
    deadline: Time,
    kind: Kind,
    stage: CallStage,
    withdrawn: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum TurnStage {
    Parent,
    Queued,
    Sending,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct TurnMeta {
    bytes: u64,
    stage: TurnStage,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Account {
    latest: Grant,
    emitted: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Sent {
    Start,
    Message,
    Call(Token),
    Busy { call: Token, name: CallName },
    Ack(u32),
    Grant,
    Cancel,
}

#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "independent channel and process state spans finite phases")]
pub(crate) struct Agent {
    client: Token,
    logical_run: Token,
    activation: u64,
    phase: Phase,
    start: Option<Start>,
    process: Option<Token>,
    admitted: bool,
    last_word: bool,
    told: bool,
    dropped: bool,
    owed: Option<Fault>,
    host_stopped: bool,
    diagnostic: Option<Fault>,
    reading: bool,
    eof: bool,
    sending: Option<Sent>,
    signals: u32,
    exited: bool,
    exit: Option<Exit>,
    forced: Forced,
    reaped: Option<Box<[u8]>>,
    outbox: Queue<Down>,
    busy: Queue<Sent>,
    calls: Map<Token, Call>,
    turns: Map<u32, TurnMeta>,
    accounts: Map<u32, Account>,
    messages: u32,
    unread: crate::Inbox,
    number: u32,
    spent: u64,
    turn_bytes: u64,
    cancel_queued: bool,
    cancel_sent: bool,
    progress: Time,
    long: crate::long::Long,
    // Paused only while the run waits with no message still queued or unread.
    waiting: bool,
    paused: bool,
    wall: Time,
    until: Time,
}

/// Bounded process/channel state, parent terminal rights and lossless content-free facts;
/// no credentials, durable host policy or frame bytes.
#[derive(Debug)]
pub struct Domain {
    agents: Slab<Agent>,
    alarms: Deadlines<Alarm>,
    facts: Queue<Fact>,
}

impl Domain {
    /// Allocate bounded state and uncapped facts; invalid arithmetic/turn/delivery capacity
    /// panics before any process effect.
    #[must_use]
    pub fn new(limits: &Limits) -> Domain {
        assert!(crate::valid(limits), "host limits are representable and every sealed delivery fits");
        assert!(crate::check(limits).is_ok(), "host supervision exceeds the agent's checked bounds");
        Domain {
            agents: Slab::<Agent>::with_capacity(limits.agents),
            alarms: Deadlines::with_capacity(limits.agents.checked_mul(3).expect("valid limits")),
            facts: Queue::uncapped(),
        }
    }

    /// Retired slots count until iteration-boundary reclaim.
    #[must_use]
    pub fn agents(&self) -> u32 {
        self.agents.len()
    }

    /// Pure earliest armed monotonic deadline.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.alarms.next()
    }

    /// Pure scheduling query; expiration itself occurs in fire.
    #[must_use]
    pub fn is_due(&self, now: Time) -> bool {
        match self.alarms.next() {
            Some(at) => at <= now,
            None => false,
        }
    }

    /// Reclaim only after parent routed outputs for this iteration.
    pub fn reclaim(&mut self) {
        self.agents.reclaim();
    }

    /// Drain one losslessly retained observation; decisions never depend on it.
    pub fn pop_fact(&mut self) -> Option<Fact> {
        self.facts.pop()
    }

    /// Entries held for the parent to drain (skein's draft
    /// dynamic-memory.md 4.1): the parent root counts them in its backlog.
    #[must_use]
    pub fn backlog(&self) -> u32 {
        self.facts.len()
    }

    /// The host no longer drops facts; the name stays while parents still read it.
    #[must_use]
    pub const fn facts_lost(&self) -> u64 {
        0
    }
}

/// Consume one parent notice or lower terminal with injected clocks.
/// Caller drains the ordered outputs and returns every operation terminal,
/// even after cancellation/EOF.
#[expect(clippy::too_many_lines, reason = "one exhaustive typed boundary keeps terminal ownership visible")]
pub fn step(domain: &mut Domain, env: &Env<Limits>, event: Input, out: &mut Queue<Output>) {
    let before = out.len();
    let token = match &event {
        Input::Parent(parent::Event::Spawn { .. }) => {
            spawn(domain, env, event, out);
            return;
        }
        Input::Parent(
            parent::Event::Message { agent, .. }
            | parent::Event::Answer { agent, .. }
            | parent::Event::Acknowledge { agent, .. }
            | parent::Event::Grant { agent, .. }
            | parent::Event::Stop { agent }
            | parent::Event::Drop { agent },
        ) => *agent,
        Input::Process(
            process::Event::Spawned { owner, .. }
            | process::Event::Unspawned { owner, .. }
            | process::Event::Sent { owner }
            | process::Event::Unsent { owner }
            | process::Event::Received { owner, .. }
            | process::Event::Progress { owner }
            | process::Event::Malformed { owner }
            | process::Event::Hangup { owner }
            | process::Event::Signalled { owner }
            | process::Event::Exited { owner, .. }
            | process::Event::Reaped { owner, .. },
        ) => *owner,
    };
    let id = Id::from_token(token);
    let Some(agent) = domain.agents.get_mut(id) else {
        return;
    };
    if agent.phase == Phase::Closed {
        return;
    }
    match event {
        Input::Parent(parent::Event::Spawn { .. }) => unreachable!("spawn handled before lookup"),
        Input::Process(process::Event::Spawned { process, .. }) => {
            assert!(agent.phase == Phase::Spawning, "one terminal per process spawn");
            agent.process = Some(process);
            agent.phase = Phase::Starting;
            agent.progress = env.now;
            agent.wall = env.now.saturating_add(env.limits.wall_time);
            out.push(Output::Parent(parent::Request::Started { client: agent.client, agent: token }));
            let start = agent.start.take().expect("spawn retains first message");
            for message in &start.messages {
                agent.unread.sent(message.name);
            }
            for grant in &start.grants {
                agent.accounts.get_mut(&grant.account).expect("validated start grant").emitted = grant.generation;
            }
            out.push(Output::Process(process::Request::Send {
                owner: token,
                process,
                message: Down::Start {
                    start,
                    window: crate::Window { turns: env.limits.turns, bytes: env.limits.unacknowledged_bytes },
                },
            }));
            agent.sending = Some(Sent::Start);
            out.push(Output::Process(process::Request::Wait { owner: token, process }));
            out.push(Output::Process(process::Request::Reap { owner: token, process }));
        }
        Input::Process(process::Event::Unspawned { detail, .. }) => {
            assert!(agent.phase == Phase::Spawning, "one terminal per spawn");
            report_unread(agent, out);
            agent.phase = Phase::Closed;
            agent.start = None;
            out.push(Output::Parent(parent::Request::Gone {
                client: agent.client,
                end: End::Unspawned,
                detail: tail(&detail, env.limits.detail_bytes),
            }));
        }
        Input::Parent(parent::Event::Message { finish_requested, name, label, text, .. }) => {
            message(agent, name, label, text, finish_requested, env, out);
        }
        Input::Parent(parent::Event::Answer { call, reply, .. }) => answered(agent, call, reply, env),
        Input::Parent(parent::Event::Acknowledge { turn, .. }) => acknowledge_turn(agent, turn),
        Input::Parent(parent::Event::Grant { grant, .. }) => grant_name(agent, grant),
        Input::Parent(parent::Event::Drop { .. }) => drop_run(agent, token, env, out),
        Input::Parent(parent::Event::Stop { .. }) => {
            match agent.phase {
                Phase::Starting | Phase::Live | Phase::Cancelled | Phase::Draining => agent.host_stopped = true,
                Phase::Spawning | Phase::Exiting | Phase::Terminating | Phase::Killing | Phase::Closed => {}
            }
            if agent.owed == Some(Fault::WallTime) || agent.owed == Some(Fault::Exited) {
                agent.owed = None;
            }
            polite(agent, env, None);
        }
        Input::Process(process::Event::Sent { .. } | process::Event::Unsent { .. }) => {
            let unsent = match event {
                Input::Process(process::Event::Unsent { .. }) => true,
                Input::Process(process::Event::Sent { .. }) => false,
                Input::Parent(
                    parent::Event::Spawn { .. }
                    | parent::Event::Message { .. }
                    | parent::Event::Answer { .. }
                    | parent::Event::Acknowledge { .. }
                    | parent::Event::Grant { .. }
                    | parent::Event::Stop { .. }
                    | parent::Event::Drop { .. },
                )
                | Input::Process(
                    process::Event::Spawned { .. }
                    | process::Event::Unspawned { .. }
                    | process::Event::Received { .. }
                    | process::Event::Progress { .. }
                    | process::Event::Malformed { .. }
                    | process::Event::Hangup { .. }
                    | process::Event::Signalled { .. }
                    | process::Event::Exited { .. }
                    | process::Event::Reaped { .. },
                ) => unreachable!("send terminal branch"),
            };
            let sent = agent.sending.take().expect("send terminal ends one pending send");
            settle_send(agent, sent);
            if unsent && !agent.last_word {
                draining(agent, env);
            }
        }
        Input::Process(process::Event::Received { message, .. }) => {
            assert!(agent.reading, "record ends one requested read");
            agent.reading = false;
            receive(agent, token, message, env, out);
        }
        Input::Process(process::Event::Progress { .. }) => {
            assert!(agent.reading, "piece ends one requested read");
            agent.reading = false;
            if agent.last_word {
                fail(agent, token, Fault::Rules, env, out);
            } else if !agent.told && agent.phase != Phase::Terminating && agent.phase != Phase::Killing {
                if agent.admitted {
                    agent.progress = env.now;
                } else {
                    fail(agent, token, Fault::Rules, env, out);
                }
            }
        }
        Input::Process(process::Event::Malformed { .. }) => {
            assert!(agent.reading, "malformed terminal ends read");
            agent.reading = false;
            agent.eof = true;
            fail(agent, token, Fault::Rules, env, out);
        }
        Input::Process(process::Event::Hangup { .. }) => {
            assert!(agent.reading, "EOF ends pending read");
            agent.reading = false;
            agent.eof = true;
            if !agent.last_word {
                match agent.phase {
                    Phase::Cancelled | Phase::Draining => {
                        if let Some(fault) = agent.owed.take() {
                            report_fault(agent, fault, out);
                        }
                        agent.phase = Phase::Exiting;
                    }
                    Phase::Starting | Phase::Live => fail(agent, token, Fault::Exited, env, out),
                    Phase::Exiting | Phase::Terminating | Phase::Killing => {}
                    Phase::Spawning | Phase::Closed => unreachable!("spawned process reads"),
                }
            }
        }
        Input::Process(process::Event::Signalled { .. }) => {
            agent.signals = agent.signals.checked_sub(1).expect("one terminal per signal");
        }
        Input::Process(process::Event::Exited { exit, .. }) => {
            assert!(!agent.exited, "one terminal per Wait");
            agent.exited = true;
            agent.exit = exit;
            if !agent.last_word {
                draining(agent, env);
            }
        }
        Input::Process(process::Event::Reaped { detail, .. }) => {
            assert!(agent.exited && agent.reaped.is_none(), "Reap follows Exited once");
            agent.reaped = Some(tail(&detail, env.limits.detail_bytes));
        }
    }
    follow(domain, env, id, out);
    observations(domain, out, before);
}

/// Expire one due timer; repeat while due and drain the outputs. Wall never
/// pauses with progress credit.
pub fn fire(domain: &mut Domain, env: &Env<Limits>, out: &mut Queue<Output>) {
    let before = out.len();
    let Some(alarm) = domain.alarms.expire(env.now) else {
        return;
    };
    let id = match alarm {
        Alarm::Watch { agent } | Alarm::Wall { agent } | Alarm::Grace { agent } => agent,
    };
    let agent = domain.agents.get_mut(id).expect("armed agent lives");
    match alarm {
        Alarm::Watch { .. } => {
            if pause(agent, env) {
                agent.paused = true;
            } else if agent.paused {
                agent.paused = false;
                agent.progress = env.now;
            } else {
                fail(agent, id.token(), Fault::NoProgress, env, out);
            }
        }
        Alarm::Wall { .. } => polite(agent, env, Some(Fault::WallTime)),
        Alarm::Grace { .. } => {
            if let Some(fault) = agent.owed.take() {
                report_fault(agent, fault, out);
            }
            match agent.phase {
                Phase::Cancelled | Phase::Draining | Phase::Exiting => terminate(agent, id.token(), env, out),
                Phase::Terminating => {
                    signal(agent, id.token(), Signal::Kill, out);
                    agent.phase = Phase::Killing;
                }
                Phase::Spawning | Phase::Starting | Phase::Live | Phase::Killing | Phase::Closed => {
                    unreachable!("grace belongs to shutdown")
                }
            }
        }
    }
    follow(domain, env, id, out);
    observations(domain, out, before);
}

#[expect(clippy::manual_map, reason = "explicit admission cases follow programming-model.md, section 10")]
fn spawn(domain: &mut Domain, env: &Env<Limits>, event: Input, out: &mut Queue<Output>) {
    let (client, start) = match event {
        Input::Parent(parent::Event::Spawn { client, start }) => (client, start),
        Input::Process(
            process::Event::Spawned { .. }
            | process::Event::Unspawned { .. }
            | process::Event::Sent { .. }
            | process::Event::Unsent { .. }
            | process::Event::Received { .. }
            | process::Event::Progress { .. }
            | process::Event::Malformed { .. }
            | process::Event::Hangup { .. }
            | process::Event::Signalled { .. }
            | process::Event::Exited { .. }
            | process::Event::Reaped { .. },
        )
        | Input::Parent(
            parent::Event::Message { .. }
            | parent::Event::Answer { .. }
            | parent::Event::Acknowledge { .. }
            | parent::Event::Grant { .. }
            | parent::Event::Stop { .. }
            | parent::Event::Drop { .. },
        ) => unreachable!("only Spawn enters admission"),
    };
    let refusal = if domain.agents.is_full() {
        Some(End::Busy)
    } else {
        match valid_start(&start, &env.limits) {
            Some(invalid) => Some(End::Invalid(invalid)),
            None => None,
        }
    };
    if let Some(end) = refusal {
        out.push(Output::Parent(parent::Request::Gone { client, end, detail: Box::new([]) }));
        observation(domain, Fact::Gone { client, end });
        return;
    }
    let mut accounts = Map::with_capacity(env.limits.accounts);
    for grant in &start.grants {
        accounts.insert(grant.account, Account { latest: *grant, emitted: 0 }).expect("validated unique grant names");
    }
    let mut unread = crate::Inbox::new(env.limits.messages);
    for message in &start.messages {
        unread
            .admit(message.name, &message.label, &message.text, env.limits.message_bytes)
            .expect("validated start messages");
    }
    let workspace = start.workspace;
    let logical_run = start.logical_run;
    let activation = start.activation;
    let agent = Agent {
        client,
        logical_run,
        activation,
        phase: Phase::Spawning,
        start: Some(start),
        process: None,
        admitted: false,
        last_word: false,
        told: false,
        dropped: false,
        owed: None,
        host_stopped: false,
        diagnostic: None,
        reading: false,
        eof: false,
        sending: None,
        signals: 0,
        exited: false,
        exit: None,
        forced: Forced::No,
        reaped: None,
        outbox: Queue::with_capacity(crate::limits::outbox(&env.limits).expect("validated outbox")),
        busy: Queue::with_capacity(2),
        calls: Map::with_capacity(env.limits.calls),
        turns: Map::with_capacity(env.limits.turns),
        accounts,
        messages: 0,
        unread,
        number: 0,
        spent: 0,
        turn_bytes: 0,
        cancel_queued: false,
        cancel_sent: false,
        progress: env.now,
        long: crate::long::Long::new(env.now),
        waiting: false,
        paused: false,
        wall: env.now,
        until: env.now,
    };
    let id = domain.agents.insert(agent).expect("slot admitted");
    out.push(Output::Process(process::Request::Spawn {
        owner: id.token(),
        workspace,
        deadline: env.now.saturating_add(env.limits.spawn_timeout),
    }));
}

fn valid_start(start: &Start, limits: &Limits) -> Option<Invalid> {
    if start.messages.len() > usize::try_from(limits.messages).expect("u32 fits") {
        return Some(Invalid::Messages);
    }
    for (at, message) in start.messages.iter().enumerate() {
        match smith_domain::run::rendered_bytes(&message.label, &message.text) {
            Some(bytes) if bytes <= limits.message_bytes => {}
            Some(_) | None => return Some(Invalid::Messages),
        }
        for earlier in start.messages.get(..at).expect("enumerated prefix") {
            if earlier.name == message.name {
                return Some(Invalid::Messages);
            }
        }
    }
    if start.activation == 0 {
        return Some(Invalid::Activation);
    }
    if start.charter.owned_bytes() > limits.charter_bytes {
        return Some(Invalid::Charter);
    }
    let transcript_fits = match &start.transcript {
        Some(transcript) => transcript.owned_bytes() <= limits.transcript_bytes,
        None => true,
    };
    if !transcript_fits {
        return Some(Invalid::Transcript);
    }
    match answered_bytes(&start.answered, limits.name_bytes) {
        Some(bytes) if bytes <= limits.answered_bytes => {}
        Some(_) | None => return Some(Invalid::Answered),
    }
    if start.directories.len() > usize::try_from(limits.directories).expect("u32 fits usize") {
        return Some(Invalid::Directories);
    }
    match start.workspace {
        Some(_) if start.directories.is_empty() => return Some(Invalid::Directories),
        None if !start.directories.is_empty() => return Some(Invalid::Directories),
        Some(_) | None => {}
    }
    if limits.path_bytes > 4096 {
        return Some(Invalid::Directories);
    }
    for directory in &start.directories {
        if !within(&directory.host, u64::from(limits.path_bytes))
            || !within(&directory.name, u64::from(limits.name_bytes))
            || directory.conflicts.len() > usize::try_from(limits.conflicts).expect("u32 fits usize")
        {
            return Some(Invalid::Directories);
        }
        for path in &directory.conflicts {
            if !within(path, u64::from(limits.path_bytes)) {
                return Some(Invalid::Directories);
            }
        }
    }
    for (at, directory) in start.directories.iter().enumerate() {
        if !safe_name(&directory.name) || (!directory.git && !directory.conflicts.is_empty()) {
            return Some(Invalid::Directories);
        }
        for earlier in start.directories.get(..at).expect("enumerated directory") {
            if directory.name == earlier.name {
                return Some(Invalid::Directories);
            }
        }
        for (position, path) in directory.conflicts.iter().enumerate() {
            if !relative(path, limits.path_bytes) {
                return Some(Invalid::Directories);
            }
            for earlier in directory.conflicts.get(..position).expect("enumerated conflict") {
                if earlier == path {
                    return Some(Invalid::Directories);
                }
            }
        }
    }
    if start.grants.len() > usize::try_from(limits.accounts).expect("u32 fits usize") {
        return Some(Invalid::Grants);
    }
    for (at, grant) in start.grants.iter().enumerate() {
        if grant.generation == 0 {
            return Some(Invalid::Grants);
        }
        for earlier in start.grants.get(..at).expect("enumerated grant") {
            if grant.account == earlier.account {
                return Some(Invalid::Grants);
            }
        }
    }
    None
}

fn safe_name(name: &[u8]) -> bool {
    if name.is_empty() || name == b"." || name == b".." {
        return false;
    }
    for byte in name {
        if *byte == 0 || *byte == b'/' {
            return false;
        }
    }
    true
}

fn within(bytes: &[u8], limit: u64) -> bool {
    u64::try_from(bytes.len()).expect("length fits u64") <= limit
}

fn answered_bytes(calls: &[AnsweredCall], name_bytes: u32) -> Option<u64> {
    if calls.len() > 128 {
        return None;
    }
    let mut total = 0_u64;
    for call in calls {
        if call.tool.is_empty() || !within(&call.tool, u64::from(name_bytes)) {
            return None;
        }
        total = total.checked_add(u64::try_from(call.tool.len()).ok()?)?;
        let reply = match &call.reply {
            crate::SavedReply::Host { body, .. } => u64::try_from(body.len()).ok()?,
            crate::SavedReply::Delivery(delivery) => match delivery.as_ref() {
                Delivery::Delivered(delivered) => delivered.owned_bytes(),
                Delivery::Refused(refusal) => refusal.owned_bytes(),
                Delivery::Nothing | Delivery::Failed(_) | Delivery::Stale => 0,
            },
            crate::SavedReply::TooLarge => 0,
        };
        total = total.checked_add(reply)?;
    }
    Some(total)
}

fn relative(path: &[u8], limit: u32) -> bool {
    if path.is_empty() || !within(path, u64::from(limit)) {
        return false;
    }
    let mut from = 0;
    for (at, byte) in path.iter().enumerate() {
        if *byte == 0 {
            return false;
        }
        if *byte == b'/' {
            let part = path.get(from..at).expect("ordered path positions");
            if part.is_empty() || part == b"." || part == b".." {
                return false;
            }
            from = at.checked_add(1).expect("bounded path");
        }
    }
    let part = path.get(from..).expect("bounded path suffix");
    !part.is_empty() && part != b"." && part != b".."
}

fn tail(detail: &[u8], limit: u32) -> Box<[u8]> {
    let keep = detail.len().min(usize::try_from(limit).expect("u32 fits usize"));
    let from = detail.len().checked_sub(keep).expect("bounded tail");
    Box::from(detail.get(from..).expect("bounded suffix"))
}

fn accepts(agent: &Agent) -> bool {
    match agent.phase {
        Phase::Starting | Phase::Live => true,
        Phase::Spawning
        | Phase::Cancelled
        | Phase::Draining
        | Phase::Exiting
        | Phase::Terminating
        | Phase::Killing
        | Phase::Closed => false,
    }
}

fn listens(agent: &Agent) -> bool {
    match agent.phase {
        Phase::Starting | Phase::Live | Phase::Cancelled => !agent.eof && !agent.exited && !agent.last_word,
        Phase::Spawning | Phase::Draining | Phase::Exiting | Phase::Terminating | Phase::Killing | Phase::Closed => {
            false
        }
    }
}

fn message(
    agent: &mut Agent,
    name: Token,
    label: Box<[u8]>,
    text: Box<[u8]>,
    finish_requested: bool,
    env: &Env<Limits>,
    out: &mut Queue<Output>,
) {
    let admitted = if accepts(agent) {
        agent.unread.admit(name, &label, &text, env.limits.message_bytes)
    } else {
        Err(MessageRefusal::Ending)
    };
    if let Err(reason) = admitted {
        out.push(Output::Parent(parent::Request::MessageRefused { client: agent.client, name, reason }));
        return;
    }
    agent.messages = agent.messages.checked_add(1).expect("bounded messages");
    agent.waiting = false;
    agent.outbox.push(Down::Message { finish_requested, name, label, text });
}

fn known_read(agent: &Agent, read: Option<Token>) -> bool {
    agent.unread.known(read)
}

fn mark_read(agent: &mut Agent, read: Option<Token>) {
    agent.unread.fence(read);
}

fn acknowledge_turn(agent: &mut Agent, number: u32) {
    let Some(turn) = agent.turns.get_mut(&number) else {
        assert!(number <= agent.number, "parent ACK names a forwarded turn");
        return;
    };
    match turn.stage {
        TurnStage::Parent => {
            turn.stage = TurnStage::Queued;
            agent.turn_bytes = agent.turn_bytes.checked_sub(turn.bytes).expect("outstanding turn bytes");
            if listens(agent) {
                agent.outbox.push(Down::Acknowledge { turn: number });
            } else {
                agent.turns.remove(&number).expect("exact ACK metadata");
            }
        }
        TurnStage::Queued | TurnStage::Sending => {}
    }
}

fn grant_name(agent: &mut Agent, grant: Grant) {
    if !listens(agent) {
        return;
    }
    let Some(known) = agent.accounts.get_mut(&grant.account) else {
        return;
    };
    if grant.generation <= known.latest.generation {
        return;
    }
    known.latest = grant;
    let count = agent.outbox.len();
    let mut replaced = false;
    for _ in 0..count {
        let old = agent.outbox.pop().expect("stable bounded outbox");
        let next = match old {
            Down::Grant { grant: old } if old.account == grant.account => {
                replaced = true;
                Down::Grant { grant }
            }
            old @ (Down::Grant { .. }
            | Down::Start { .. }
            | Down::Message { .. }
            | Down::Answer { .. }
            | Down::Acknowledge { .. }
            | Down::Cancel) => old,
        };
        agent.outbox.push(next);
    }
    if !replaced {
        agent.outbox.push(Down::Grant { grant });
    }
}

fn answered(agent: &mut Agent, callback: Token, reply: Reply, env: &Env<Limits>) {
    let call = *agent.calls.get(&callback).expect("parent returns one known operation right");
    assert!(call.stage == CallStage::Parent, "one actual parent answer per call");
    let reply = match reply {
        Reply::Delivery(delivery) => {
            assert!(call.kind == Kind::Delivery, "delivery terminal matches delivery call");
            Reply::Delivery(delivery)
        }
        Reply::Host { error, body } => {
            assert!(call.kind == Kind::Host, "tool terminal matches tool call");
            if within(&body, env.limits.answer_bytes) { Reply::Host { error, body } } else { Reply::TooLarge }
        }
        reply @ (Reply::Busy | Reply::Unavailable | Reply::Withdrawn | Reply::TooLarge) => {
            assert!(call.kind == Kind::Host, "submitted delivery owes its full actual terminal");
            reply
        }
    };
    if listens(agent) {
        agent.calls.get_mut(&callback).expect("known call").stage = CallStage::Queued;
        agent.outbox.push(Down::Answer { call: callback, reply });
    } else {
        agent.calls.remove(&callback).expect("actual parent right settled");
    }
}

fn receive(agent: &mut Agent, owner: Token, message: Up, env: &Env<Limits>, out: &mut Queue<Output>) {
    if agent.last_word {
        fail(agent, owner, Fault::Rules, env, out);
        return;
    }
    if agent.told || discarded_work(agent, &message) {
        return;
    }
    if too_large(&message, &env.limits) {
        fail(agent, owner, Fault::TooLarge, env, out);
        return;
    }
    if !valid_record(agent, &message, env) {
        fail(agent, owner, Fault::Rules, env, out);
        return;
    }
    agent.progress = env.now;
    match message {
        Up::Admitted => {
            agent.admitted = true;
            if agent.phase == Phase::Starting {
                agent.phase = Phase::Live;
            }
            out.push(Output::Parent(parent::Request::Admitted { client: agent.client }));
        }
        Up::Call { call, name, deadline, ask } => {
            let kind = match &ask {
                Ask::Host { .. } => Kind::Host,
                Ask::Deliver { .. } => Kind::Delivery,
            };
            agent.waiting = false;
            if agent.calls.len() >= env.limits.calls {
                agent.busy.push(Sent::Busy { call, name });
            } else {
                agent
                    .calls
                    .insert(call, Call { name, deadline, kind, stage: CallStage::Parent, withdrawn: false })
                    .expect("call admitted within capacity");
                out.push(Output::Parent(parent::Request::Called {
                    client: agent.client,
                    logical_run: agent.logical_run,
                    call,
                    name,
                    deadline,
                    ask,
                }));
            }
        }
        Up::Withdraw { call } => match agent.calls.get_mut(&call) {
            Some(entry) if entry.stage == CallStage::Parent => {
                entry.withdrawn = true;
                out.push(Output::Parent(parent::Request::Withdrawn { client: agent.client, call }));
            }
            Some(_) | None => {}
        },
        Up::MessageRefused { name, reason } => {
            assert!(agent.unread.refused(name), "validated refusal owns an accepted message");
            out.push(Output::Parent(parent::Request::MessageRefused { client: agent.client, name, reason }));
        }
        Up::Turn { turn } => {
            agent.waiting = false;
            mark_read(agent, turn.read);
            agent.number = turn.number;
            agent.spent = turn.spent;
            let bytes = turn.body.owned_bytes();
            agent.turn_bytes = agent.turn_bytes.checked_add(bytes).expect("validated byte credit");
            agent
                .turns
                .insert(turn.number, TurnMeta { bytes, stage: TurnStage::Parent })
                .expect("maximum turn room reserved before read");
            out.push(Output::Parent(parent::Request::Turn { client: agent.client, turn }));
        }
        Up::Waiting { read } => {
            mark_read(agent, read);
            agent.waiting = agent.messages == 0 && agent.unread.is_empty();
            out.push(Output::Parent(parent::Request::Waiting { client: agent.client, read }));
        }
        Up::Long { operation, span } => {
            agent.waiting = false;
            agent.long.begin(env.now.saturating_add(span));
            out.push(Output::Parent(parent::Request::Long { client: agent.client, operation, span }));
        }
        Up::LongDone => {
            agent.long.end();
            out.push(Output::Parent(parent::Request::LongDone { client: agent.client }));
        }
        Up::Fact { body } => out.push(Output::Parent(parent::Request::Told { client: agent.client, body })),
        Up::Unauthorized { account, generation } => {
            out.push(Output::Parent(parent::Request::Unauthorized { client: agent.client, account, generation }));
        }
        Up::Limited { account, reset } => {
            out.push(Output::Parent(parent::Request::Limited { client: agent.client, account, reset }));
        }
        Up::Refused { account, code, reason } => {
            out.push(Output::Parent(parent::Request::Refused { client: agent.client, account, code, reason }));
        }
        Up::Answer { answer } => heard_answer(agent, answer, env, out),
    }
}

fn discarded_work(agent: &Agent, message: &Up) -> bool {
    match agent.phase {
        Phase::Terminating | Phase::Killing => match message {
            Up::Answer { .. } => agent.dropped,
            Up::MessageRefused { .. } => false,
            Up::Admitted
            | Up::Call { .. }
            | Up::Withdraw { .. }
            | Up::Turn { .. }
            | Up::Fact { .. }
            | Up::Long { .. }
            | Up::LongDone
            | Up::Waiting { .. }
            | Up::Unauthorized { .. }
            | Up::Refused { .. }
            | Up::Limited { .. } => true,
        },
        Phase::Spawning
        | Phase::Starting
        | Phase::Live
        | Phase::Cancelled
        | Phase::Draining
        | Phase::Exiting
        | Phase::Closed => false,
    }
}

fn heard_answer(agent: &mut Agent, answer: crate::Answer, env: &Env<Limits>, out: &mut Queue<Output>) {
    mark_read(agent, answer.read);
    agent.last_word = true;
    agent.spent = answer.spent;
    let wall_cancel = match &answer.result {
        RunResult::Failed { failure: RunFailure::Cancelled } => agent.owed == Some(Fault::WallTime),
        RunResult::Refused { .. } | RunResult::Accepted { .. } | RunResult::Parked | RunResult::Failed { .. } => false,
    };
    if wall_cancel {
        report_fault(agent, Fault::WallTime, out);
    } else {
        agent.told = true;
        out.push(Output::Parent(parent::Request::Answered { client: agent.client, answer, detail: Box::default() }));
        report_unread(agent, out);
    }
    agent.owed = None;
    match agent.phase {
        Phase::Starting | Phase::Live | Phase::Cancelled | Phase::Draining => {
            agent.until = env.now.saturating_add(env.limits.exit_grace);
        }
        Phase::Exiting | Phase::Terminating | Phase::Killing => {}
        Phase::Spawning | Phase::Closed => unreachable!("answer after process start"),
    }
    if agent.phase != Phase::Terminating && agent.phase != Phase::Killing {
        agent.phase = Phase::Exiting;
    }
}

fn too_large(message: &Up, limits: &Limits) -> bool {
    match message {
        Up::Call { ask, .. } => match ask {
            Ask::Host { tool, body, .. } => {
                !within(tool, u64::from(limits.name_bytes)) || !within(body, limits.call_bytes)
            }
            Ask::Deliver { fields } => fields.owned_bytes() > limits.call_bytes,
        },
        Up::Turn { turn } => turn.body.owned_bytes() > limits.turn_bytes,
        Up::Fact { body } => !within(body, limits.fact_bytes),
        Up::Answer { answer } => match &answer.result {
            RunResult::Accepted { outcome } => outcome.owned_bytes() > limits.outcome_bytes,
            RunResult::Refused { .. } | RunResult::Parked | RunResult::Failed { .. } => false,
        },
        Up::Admitted
        | Up::MessageRefused { .. }
        | Up::Withdraw { .. }
        | Up::Long { .. }
        | Up::LongDone
        | Up::Waiting { .. }
        | Up::Unauthorized { .. }
        | Up::Refused { .. }
        | Up::Limited { .. } => false,
    }
}

fn valid_record(agent: &Agent, message: &Up, env: &Env<Limits>) -> bool {
    let limits = &env.limits;
    match message {
        Up::Admitted => !agent.admitted && !agent.last_word,
        Up::MessageRefused { name, .. } => agent.admitted && agent.unread.refusal_known(*name),
        Up::Answer { answer } => valid_answer(agent, answer, limits),
        Up::Call { call, name, deadline, ask } => {
            if !agent.admitted || name.activation != agent.activation || name.completion == 0 || *deadline < env.now {
                return false;
            }
            let kind = match ask {
                Ask::Host { tool, body, .. } => {
                    if tool.is_empty()
                        || !within(tool, u64::from(limits.name_bytes))
                        || !within(body, limits.call_bytes)
                    {
                        return false;
                    }
                    Kind::Host
                }
                Ask::Deliver { fields } => {
                    if fields.owned_bytes() > limits.call_bytes {
                        return false;
                    }
                    Kind::Delivery
                }
            };
            if agent.calls.contains_key(call) {
                return false;
            }
            for (_, old) in &agent.calls {
                if old.name == *name || (old.kind == Kind::Delivery && kind == Kind::Delivery) {
                    return false;
                }
            }
            for busy in &agent.busy {
                match busy {
                    Sent::Busy { call: old, name: old_name } => {
                        if old == call || old_name == name {
                            return false;
                        }
                    }
                    Sent::Start | Sent::Message | Sent::Call(_) | Sent::Ack(_) | Sent::Grant | Sent::Cancel => {
                        unreachable!("busy queue only stores overflow rights")
                    }
                }
            }
            match agent.sending {
                Some(Sent::Busy { call: old, name: old_name }) => {
                    if old == *call || old_name == *name {
                        return false;
                    }
                }
                Some(Sent::Start | Sent::Message | Sent::Call(_) | Sent::Ack(_) | Sent::Grant | Sent::Cancel)
                | None => {}
            }
            true
        }
        Up::Withdraw { call } => {
            if !agent.admitted {
                return false;
            }
            match agent.calls.get(call) {
                Some(entry) => !entry.withdrawn || entry.stage != CallStage::Parent,
                None => true,
            }
        }
        Up::Turn { turn } => {
            agent.admitted
                && agent.number.checked_add(1) == Some(turn.number)
                && valid_spend(agent, turn.spent)
                && known_read(agent, turn.read)
                && turn.body.owned_bytes() <= limits.turn_bytes
                && agent.turns.len() < limits.turns
                && match agent.turn_bytes.checked_add(turn.body.owned_bytes()) {
                    Some(bytes) => bytes <= limits.unacknowledged_bytes,
                    None => false,
                }
        }
        Up::Fact { body } => agent.admitted && within(body, limits.fact_bytes),
        Up::Long { operation: _, span } => {
            agent.admitted && *span <= limits.long_span && agent.long.open < limits.long_open
        }
        Up::LongDone => agent.admitted && agent.long.open > 0,
        Up::Waiting { read } => agent.admitted && known_read(agent, *read),
        Up::Unauthorized { account, generation } => match agent.accounts.get(account) {
            Some(account) => agent.admitted && *generation > 0 && *generation <= account.emitted,
            None => false,
        },
        Up::Refused { account, .. } | Up::Limited { account, .. } => {
            agent.admitted && agent.accounts.contains_key(account)
        }
    }
}

// Agent spend never decreases (domain/host.md, section 6). The kit never reprices.
fn valid_spend(agent: &Agent, spent: u64) -> bool {
    spent >= agent.spent
}

fn valid_answer(agent: &Agent, answer: &crate::Answer, limits: &Limits) -> bool {
    for (_, call) in &agent.calls {
        if call.kind == Kind::Delivery {
            match call.stage {
                CallStage::Parent | CallStage::Queued => return false,
                CallStage::Sending => {}
            }
        }
    }
    if answer.read != agent.unread.read() || answer.turns != agent.number || !valid_spend(agent, answer.spent) {
        return false;
    }
    match &answer.result {
        RunResult::Refused { .. } => !agent.admitted && answer.turns == 0 && answer.spent == 0,
        RunResult::Accepted { outcome } => agent.admitted && outcome.owned_bytes() <= limits.outcome_bytes,
        RunResult::Parked | RunResult::Failed { .. } => agent.admitted,
    }
}

fn settle_send(agent: &mut Agent, sent: Sent) {
    match sent {
        Sent::Call(callback) => {
            let call = agent.calls.remove(&callback).expect("call transmission still reserved");
            assert!(call.stage == CallStage::Sending, "send terminal owns reservation");
        }
        Sent::Ack(number) => {
            let turn = agent.turns.remove(&number).expect("ACK transmission still reserved");
            assert!(turn.stage == TurnStage::Sending, "ACK send terminal owns metadata");
        }
        Sent::Start | Sent::Message | Sent::Busy { .. } | Sent::Grant | Sent::Cancel => {}
    }
}

fn polite(agent: &mut Agent, env: &Env<Limits>, owed: Option<Fault>) {
    match agent.phase {
        Phase::Starting | Phase::Live => {
            if !agent.cancel_queued {
                agent.outbox.push(Down::Cancel);
                agent.cancel_queued = true;
            }
            agent.owed = owed;
            agent.phase = Phase::Cancelled;
            agent.until = env.now.saturating_add(env.limits.cancel_grace);
        }
        Phase::Spawning
        | Phase::Cancelled
        | Phase::Draining
        | Phase::Exiting
        | Phase::Terminating
        | Phase::Killing
        | Phase::Closed => {}
    }
}

fn draining(agent: &mut Agent, env: &Env<Limits>) {
    match agent.phase {
        Phase::Starting | Phase::Live => {
            agent.phase = Phase::Draining;
            agent.owed = Some(Fault::Exited);
            agent.until = env.now.saturating_add(env.limits.cancel_grace);
        }
        Phase::Cancelled => {
            agent.phase = Phase::Draining;
        }
        Phase::Spawning | Phase::Draining | Phase::Exiting | Phase::Terminating | Phase::Killing | Phase::Closed => {}
    }
}

fn report_unread(agent: &mut Agent, out: &mut Queue<Output>) {
    if let Some(names) = agent.unread.report() {
        out.push(Output::Parent(parent::Request::Unread { client: agent.client, names }));
    }
}

fn report_fault(agent: &mut Agent, fault: Fault, out: &mut Queue<Output>) {
    if !agent.told {
        report_unread(agent, out);
        agent.told = true;
        out.push(Output::Parent(parent::Request::Faulted { client: agent.client, fault }));
    }
}

fn fail(agent: &mut Agent, owner: Token, fault: Fault, env: &Env<Limits>, out: &mut Queue<Output>) {
    if !agent.host_stopped && !agent.told {
        report_fault(agent, fault, out);
    } else {
        agent.diagnostic = Some(fault);
    }
    agent.owed = None;
    match agent.phase {
        Phase::Starting | Phase::Live | Phase::Cancelled | Phase::Draining | Phase::Exiting => {
            terminate(agent, owner, env, out);
        }
        Phase::Terminating | Phase::Killing => {}
        Phase::Spawning | Phase::Closed => unreachable!("only spawned processes fail channel/progress"),
    }
}

fn drop_run(agent: &mut Agent, owner: Token, env: &Env<Limits>, out: &mut Queue<Output>) {
    agent.dropped = !agent.told;
    agent.host_stopped = true;
    agent.owed = None;
    let mut released = skein_lib::List::with_capacity(env.limits.turns);
    for (number, turn) in &agent.turns {
        match turn.stage {
            TurnStage::Parent => released.push(*number).expect("bounded held turns"),
            TurnStage::Queued | TurnStage::Sending => {}
        }
    }
    for number in &released {
        let turn = agent.turns.remove(number).expect("held turn released exactly once");
        agent.turn_bytes = agent.turn_bytes.checked_sub(turn.bytes).expect("held turn byte credit");
    }
    match agent.phase {
        Phase::Starting | Phase::Live | Phase::Cancelled | Phase::Draining | Phase::Exiting => {
            terminate(agent, owner, env, out);
        }
        Phase::Spawning | Phase::Terminating | Phase::Killing | Phase::Closed => {}
    }
}

fn terminate(agent: &mut Agent, owner: Token, env: &Env<Limits>, out: &mut Queue<Output>) {
    signal(agent, owner, Signal::Terminate, out);
    agent.phase = Phase::Terminating;
    agent.until = env.now.saturating_add(env.limits.kill_after);
}

fn signal(agent: &mut Agent, owner: Token, signal: Signal, out: &mut Queue<Output>) {
    agent.forced = match signal {
        Signal::Terminate => Forced::Terminated,
        Signal::Kill => Forced::Killed,
    };
    agent.signals = agent.signals.checked_add(1).expect("two bounded tree signals at most");
    out.push(Output::Process(process::Request::Signal {
        owner,
        process: agent.process.expect("spawned process"),
        signal,
    }));
}

fn turn_room(agent: &Agent, limits: &Limits) -> bool {
    agent.turns.len() < limits.turns
        && match agent.turn_bytes.checked_add(limits.turn_bytes) {
            Some(bytes) => bytes <= limits.unacknowledged_bytes,
            None => false,
        }
}

fn pause(agent: &Agent, env: &Env<Limits>) -> bool {
    if agent.waiting || !turn_room(agent, &env.limits) || agent.busy.room() == 0 {
        return true;
    }
    for (_, call) in &agent.calls {
        if call.stage == CallStage::Parent && call.deadline > env.now {
            return true;
        }
    }
    false
}

fn send_next(agent: &mut Agent, owner: Token, out: &mut Queue<Output>) {
    if agent.sending.is_some() || !listens(agent) {
        return;
    }
    let (sent, message) = match agent.busy.pop() {
        Some(sent) => {
            let call = match sent {
                Sent::Busy { call, .. } => call,
                Sent::Start | Sent::Message | Sent::Call(_) | Sent::Ack(_) | Sent::Grant | Sent::Cancel => {
                    unreachable!("overflow queue")
                }
            };
            (sent, Down::Answer { call, reply: Reply::Busy })
        }
        None => {
            let Some(message) = agent.outbox.pop() else {
                return;
            };
            let sent = match &message {
                Down::Start { .. } => unreachable!("Start sent directly once"),
                Down::Message { name, .. } => {
                    agent.messages = agent.messages.checked_sub(1).expect("queued message count");
                    agent.unread.sent(*name);
                    Sent::Message
                }
                Down::Answer { call, .. } => {
                    let entry = agent.calls.get_mut(call).expect("answer retains callback until Sent");
                    entry.stage = CallStage::Sending;
                    Sent::Call(*call)
                }
                Down::Acknowledge { turn } => {
                    agent.turns.get_mut(turn).expect("exact ACK metadata").stage = TurnStage::Sending;
                    Sent::Ack(*turn)
                }
                Down::Grant { grant } => {
                    agent.accounts.get_mut(&grant.account).expect("known grant").emitted = grant.generation;
                    Sent::Grant
                }
                Down::Cancel => {
                    agent.cancel_sent = true;
                    Sent::Cancel
                }
            };
            (sent, message)
        }
    };
    agent.sending = Some(sent);
    out.push(Output::Process(process::Request::Send {
        owner,
        process: agent.process.expect("spawned process"),
        message,
    }));
}

fn disconnect(agent: &mut Agent, limits: &Limits, out: &mut Queue<Output>) {
    // Exit can precede the last buffered turn and answer on the read pipe.
    if agent.eof || agent.last_word {
        report_unread(agent, out);
    }
    let count = agent.outbox.len();
    for _ in 0..count {
        let message = agent.outbox.pop().expect("bounded queue snapshot");
        match message {
            Down::Answer { call, .. } => {
                agent.calls.remove(&call).expect("already answered queued call");
            }
            Down::Acknowledge { turn } => {
                agent.turns.remove(&turn).expect("committed queued turn");
            }
            Down::Message { .. } => {
                agent.messages = agent.messages.checked_sub(1).expect("queued message count");
            }
            Down::Start { .. } => unreachable!("start never queued"),
            Down::Grant { .. } | Down::Cancel => {}
        }
    }
    let busy_count = agent.busy.len();
    for _ in 0..busy_count {
        agent.busy.pop().expect("bounded busy snapshot");
    }
    let mut withdrawn = skein_lib::List::with_capacity(limits.calls);
    for (callback, call) in &agent.calls {
        if call.stage == CallStage::Parent && !call.withdrawn {
            withdrawn.push(*callback).expect("one entry per admitted call");
        }
    }
    for callback in withdrawn.into_boxed() {
        agent.calls.get_mut(&callback).expect("retained parent right").withdrawn = true;
        out.push(Output::Parent(parent::Request::Withdrawn { client: agent.client, call: callback }));
    }
}

fn follow(domain: &mut Domain, env: &Env<Limits>, id: Id<Agent>, out: &mut Queue<Output>) {
    let agent = domain.agents.get_mut(id).expect("entry lives until rights settle");
    let owner = id.token();
    let client = agent.client;
    let diagnostic = agent.diagnostic.take();
    if !listens(agent) && agent.phase != Phase::Spawning {
        disconnect(agent, &env.limits, out);
    }
    send_next(agent, owner, out);
    let may_read = match agent.phase {
        Phase::Starting | Phase::Live | Phase::Cancelled | Phase::Draining => {
            turn_room(agent, &env.limits) && agent.busy.room() > 0
        }
        Phase::Exiting | Phase::Terminating | Phase::Killing => true,
        Phase::Spawning | Phase::Closed => false,
    };
    if may_read && !agent.reading && !agent.eof {
        agent.reading = true;
        out.push(Output::Process(process::Request::Read { owner, process: agent.process.expect("spawned process") }));
    }
    let working = match agent.phase {
        Phase::Starting | Phase::Live => true,
        Phase::Spawning
        | Phase::Cancelled
        | Phase::Draining
        | Phase::Exiting
        | Phase::Terminating
        | Phase::Killing
        | Phase::Closed => false,
    };
    let paused = working && pause(agent, env);
    if agent.paused && !paused {
        agent.progress = env.now;
    }
    agent.paused = paused;
    let watch = if working {
        if paused {
            let mut deadline = None;
            for (_, call) in &agent.calls {
                if call.stage == CallStage::Parent && call.deadline > env.now {
                    deadline = Some(match deadline {
                        Some(at) => call.deadline.min(at),
                        None => call.deadline,
                    });
                }
            }
            deadline
        } else {
            Some(agent.long.stretch(agent.progress).saturating_add(env.limits.no_progress))
        }
    } else {
        None
    };
    let wall = if working { Some(agent.wall) } else { None };
    let grace = match agent.phase {
        Phase::Cancelled | Phase::Draining | Phase::Exiting | Phase::Terminating => Some(agent.until),
        Phase::Spawning | Phase::Starting | Phase::Live | Phase::Killing | Phase::Closed => None,
    };
    set(&mut domain.alarms, Alarm::Watch { agent: id }, watch);
    set(&mut domain.alarms, Alarm::Wall { agent: id }, wall);
    set(&mut domain.alarms, Alarm::Grace { agent: id }, grace);
    let done = agent.phase == Phase::Closed
        || (agent.exited
            && agent.reaped.is_some()
            && agent.eof
            && !agent.reading
            && agent.sending.is_none()
            && agent.signals == 0
            && agent.calls.is_empty()
            && agent.turns.is_empty());
    if done {
        if agent.phase != Phase::Closed {
            if let Some(fault) = agent.owed.take() {
                report_fault(agent, fault, out);
            }
            out.push(Output::Parent(parent::Request::Gone {
                client: agent.client,
                end: End::Exited { exit: agent.exit, forced: agent.forced },
                detail: agent.reaped.take().expect("empty tree proof"),
            }));
        }
        agent.phase = Phase::Closed;
        for alarm in [Alarm::Watch { agent: id }, Alarm::Wall { agent: id }, Alarm::Grace { agent: id }] {
            domain.alarms.cancel(alarm);
        }
        domain.agents.retire(id);
    }
    if let Some(fault) = diagnostic {
        observation(domain, Fact::Faulted { client, fault });
    }
}

fn set(alarms: &mut Deadlines<Alarm>, alarm: Alarm, deadline: Option<Time>) {
    if let Some(at) = deadline {
        alarms.arm(alarm, at).expect("three bounded alarms per agent");
    } else {
        alarms.cancel(alarm);
    }
}

fn observation(domain: &mut Domain, fact: Fact) {
    domain.facts.push(fact);
}

fn observations(domain: &mut Domain, out: &Queue<Output>, before: u32) {
    let mut at = 0;
    for request in out {
        if at >= before {
            let fact = match request {
                Output::Parent(parent::Request::Started { client, .. }) => Some(Fact::Started { client: *client }),
                Output::Parent(parent::Request::Admitted { client }) => Some(Fact::Admitted { client: *client }),
                Output::Parent(parent::Request::Answered { client, .. }) => Some(Fact::Answered { client: *client }),
                Output::Parent(parent::Request::Faulted { client, fault }) => {
                    Some(Fact::Faulted { client: *client, fault: *fault })
                }
                Output::Parent(parent::Request::Gone { client, end, .. }) => {
                    Some(Fact::Gone { client: *client, end: *end })
                }
                Output::Parent(
                    parent::Request::Called { .. }
                    | parent::Request::Withdrawn { .. }
                    | parent::Request::Turn { .. }
                    | parent::Request::Waiting { .. }
                    | parent::Request::Long { .. }
                    | parent::Request::LongDone { .. }
                    | parent::Request::Unauthorized { .. }
                    | parent::Request::Refused { .. }
                    | parent::Request::Limited { .. }
                    | parent::Request::Told { .. }
                    | parent::Request::MessageRefused { .. }
                    | parent::Request::Unread { .. },
                )
                | Output::Process(
                    process::Request::Spawn { .. }
                    | process::Request::Send { .. }
                    | process::Request::Read { .. }
                    | process::Request::Signal { .. }
                    | process::Request::Wait { .. }
                    | process::Request::Reap { .. },
                ) => None,
            };
            if let Some(fact) = fact {
                observation(domain, fact);
            }
        }
        at = at.checked_add(1).expect("bounded output queue");
    }
}
