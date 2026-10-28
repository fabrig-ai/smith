//! A kit's numbered commands and the reads awaiting their unread output.
//! Original spawn tokens stay live until IO settles; cancelled terminals and
//! bounded unread output stay until read or close. Provider ids are never known.
//! Same-job reads reach the machine one at a time.
//! Contract: domain/tools.md, section 5; programming-model.md, sections 5.2 and 6.3.

use crate::command::{Kept, Timed};
use crate::domain::Domain;
use crate::facts::{self, FactKind};
use crate::flight::{self, Flight};
use crate::kit::{self, Kit};
use crate::{CallInfo, Done, JobAct, JobEnd, JobNumber, JobProblem, Limits, Outcome, Request};
use alloc::boxed::Box;
use skein_lib::{Env, Id, Map, Queue, ReplyTo, Time, Token};

#[derive(Debug)]
pub(crate) struct Jobs {
    held: Map<u32, Job>,
    reads: Map<u64, Read>,
    highest: u32,
    next_read: u64,
    cancelling: bool,
}

#[derive(Debug)]
pub(crate) struct Job {
    owner: Id<Flight>,
    reader: Option<u64>,
    call: Token,
    started: Time,
    state: State,
}

#[derive(Debug)]
enum State {
    Running,
    Cancelling,
    Ended { head: Box<[u8]>, tail: Box<[u8]>, dropped: u64, end: JobEnd, at: Time },
}

#[derive(Debug)]
pub(crate) struct Read {
    job: JobNumber,
    reply_to: ReplyTo,
    observation: Option<CallInfo>,
    timed: Timed,
    kept: Kept,
    deadline: Time,
    stop: bool,
    hastened: bool,
}

impl Jobs {
    pub(crate) fn new(limits: &Limits, highest: u32) -> Self {
        Self {
            held: Map::with_capacity(limits.jobs),
            reads: Map::with_capacity(limits.calls),
            highest,
            next_read: 0,
            cancelling: false,
        }
    }

    pub(crate) fn full(&self) -> bool {
        self.held.len() >= self.held.capacity()
    }

    pub(crate) fn empty(&self) -> bool {
        self.held.is_empty()
    }

    pub(crate) fn calls(&self) -> u32 {
        self.reads.len()
    }

    pub(crate) fn held(&mut self, owner: Id<Flight>, observation: Option<CallInfo>, started: Time) -> JobNumber {
        self.highest = self.highest.checked_add(1).expect("a conversation's job numbers fit u32");
        let number = JobNumber(self.highest);
        let call = match observation {
            Some(call) => call.owner,
            None => owner.token(),
        };
        self.held
            .insert(number.0, Job { owner, reader: None, call, started, state: State::Running })
            .expect("the session serializes yielding shell writes");
        number
    }

    pub(crate) fn close(
        &mut self,
        session: Token,
        facts: &mut facts::Facts,
        limits: &Limits,
        out: &mut Queue<Request>,
    ) {
        let numbers = self.numbers();
        for number in &numbers {
            let job = self.held.get_mut(number).expect("retained job");
            match &job.state {
                State::Ended { .. } => {
                    tell(session, JobNumber(*number), job, facts);
                    self.held.remove(number).expect("ended job retained until close");
                }
                State::Running | State::Cancelling => cancel_one(job, limits, out),
            }
        }
    }

    fn numbers(&self) -> skein_lib::List<u32> {
        let mut numbers = skein_lib::List::with_capacity(self.held.capacity());
        for (number, _) in &self.held {
            numbers.push(*number).expect("bounded jobs");
        }
        numbers
    }
}

/// Enqueue a read; another read of this number must consume its answer first.
pub(crate) fn call(domain: &mut Domain, env: &Env<Limits>, id: Id<Kit>, read: Read, out: &mut Queue<Request>) {
    let kit = domain.kits.get_mut(id).expect("the session owns its kit");
    let job = read.job;
    let problem = if kit.jobs.held.contains_key(&job.0) { None } else { Some(JobProblem::Unknown { job }) };
    if let Some(problem) = problem {
        out.push(Request::Invalid { to: read.reply_to, problem });
        return;
    }
    if let Err(outcome) = kit::ready(kit, read.deadline, env) {
        domain.facts.push_call(
            facts::answered(
                kit::session(kit),
                crate::Tool::Job,
                &outcome,
                facts::Observation { omitted_bytes: 0, clamped: false },
            ),
            read.observation,
        );
        out.push(Request::Answer { to: read.reply_to, outcome });
        return;
    }
    domain.facts.push_call(FactKind::Started { session: kit::session(kit), tool: crate::Tool::Job }, read.observation);
    let ended = match &kit.jobs.held.get(&job.0).expect("known job").state {
        State::Ended { .. } => true,
        State::Running | State::Cancelling => false,
    };
    if ended {
        let ended = kit.jobs.held.remove(&job.0).expect("one terminal read");
        match ended.state {
            State::Ended { head, tail, dropped, end, .. } => {
                answer(kit::session(kit), read, Done::Job { head, tail, dropped, end }, &mut domain.facts, out);
            }
            State::Running | State::Cancelling => unreachable!("checked ended"),
        }
        return;
    }
    kit.jobs.next_read = kit.jobs.next_read.checked_add(1).expect("bounded activation calls fit u64");
    let key = kit.jobs.next_read;
    kit.jobs.reads.insert(key, read).expect("the common call admission reserves a read cell");
    advance(&mut kit.jobs, job, env.now, &env.limits, out);
}

pub(crate) fn read(
    job: JobNumber,
    reply_to: ReplyTo,
    observation: Option<CallInfo>,
    timed: Timed,
    kept: Kept,
    deadline: Time,
    stop: bool,
) -> Read {
    Read { job, reply_to, observation, timed, kept, deadline, stop, hastened: false }
}

/// The session names a pending reply; a queued read keeps its shortened wait.
pub(crate) fn end_wait(domain: &mut Domain, kit: Token, call: Token, out: &mut Queue<Request>) {
    let kit = domain.kits.get_mut(Id::from_token(kit)).expect("a session owns its kit");
    let mut found = None;
    for (key, read) in &kit.jobs.reads {
        if read.reply_to == ReplyTo::new(call) && !read.stop && !read.hastened {
            found = Some(*key);
            break;
        }
    }
    if let Some(key) = found {
        let read = kit.jobs.reads.get_mut(&key).expect("a pending read owns this reply");
        read.hastened = true;
        let job = kit.jobs.held.get(&read.job.0).expect("a read names a held job");
        if job.reader == Some(key) {
            out.push(Request::Job { owner: job.owner.token(), act: JobAct::Hasten });
        }
    }
}

fn advance(jobs: &mut Jobs, number: JobNumber, now: Time, limits: &Limits, out: &mut Queue<Request>) {
    let job = jobs.held.get_mut(&number.0).expect("a queued read names a held job");
    if job.reader.is_some() || is_cancelling(&job.state) {
        return;
    }
    for (key, read) in &jobs.reads {
        if read.job == number {
            job.reader = Some(*key);
            let wait = if read.hastened {
                skein_lib::Duration::ZERO
            } else {
                read.timed.timeout.min(read.deadline.saturating_since(now))
            };
            out.push(Request::Job {
                owner: job.owner.token(),
                act: if read.stop {
                    JobAct::Stop { head: limits.shell_head, tail: limits.shell_tail }
                } else {
                    JobAct::Read { head: limits.shell_head, tail: limits.shell_tail, wait }
                },
            });
            return;
        }
    }
}

/// A held owner's next terminal settles its active read, keeping the owner if
/// still running. Close also receives a terminal when no read was outstanding.
pub(crate) fn done(
    domain: &mut Domain,
    env: &Env<Limits>,
    owner: Id<Flight>,
    number: JobNumber,
    done: Done,
    out: &mut Queue<Request>,
) {
    let kit_id = flight::kit(domain.flights.get(owner).expect("a held owner lives until its end"));
    let kit = domain.kits.get_mut(kit_id).expect("a kit owns every held command");
    let (head, tail, dropped, end) = match done {
        Done::Job { head, tail, dropped, end } => (head, tail, dropped, end),
        Done::Loaded { .. }
        | Done::Listed { .. }
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::Exited { .. }
        | Done::Held { .. }
        | Done::Found { .. }
        | Done::Missing
        | Done::NotFile
        | Done::Linked
        | Done::NotDirectory
        | Done::TooLarge { .. }
        | Done::Escapes
        | Done::Failed { .. }
        | Done::TimedOut
        | Done::Cancelled => unreachable!("a held command answers a job act"),
    };
    let job = kit.jobs.held.get_mut(&number.0).expect("the original owner has its job cell");
    let ended = match end {
        JobEnd::Running => false,
        JobEnd::Ended(_) | JobEnd::Stopped | JobEnd::Cancelled => true,
    };
    if ended && job.reader.is_none() {
        for (key, read) in &kit.jobs.reads {
            if read.job == number {
                job.reader = Some(*key);
                break;
            }
        }
    }
    if let Some(reader) = job.reader.take() {
        let read = kit.jobs.reads.remove(&reader).expect("an active read owns its reply right");
        answer(kit::session(kit), read, Done::Job { head, tail, dropped, end }, &mut domain.facts, out);
    } else {
        assert!(ended, "only cancellation answers an unpolled job");
        job.state = State::Ended { head, tail, dropped, end, at: env.now };
        domain.flights.retire(owner);
        if kit::closing(kit) {
            tell(kit::session(kit), number, kit.jobs.held.get(&number.0).expect("ended job"), &mut domain.facts);
            kit.jobs.held.remove(&number.0).expect("close tells the end once");
        }
        cancelled(kit, out);
        kit::settled(&mut domain.kits, &mut domain.facts, kit_id, out);
        return;
    }
    if ended {
        kit.jobs.held.remove(&number.0).expect("the job ends once");
        // Calls queued behind an end now name a job whose end was answered.
        let mut queued = skein_lib::List::with_capacity(env.limits.calls);
        for (key, read) in &kit.jobs.reads {
            if read.job == number {
                queued.push(*key).expect("at most calls reads await output");
            }
        }
        for key in &queued {
            let read = kit.jobs.reads.remove(key).expect("a queued right ends once");
            out.push(Request::Invalid { to: read.reply_to, problem: JobProblem::Unknown { job: number } });
        }
        domain.flights.retire(owner);
        cancelled(kit, out);
        kit::settled(&mut domain.kits, &mut domain.facts, kit_id, out);
    } else if !is_cancelling(&kit.jobs.held.get(&number.0).expect("running job retained").state) {
        advance(&mut kit.jobs, number, env.now, &env.limits, out);
    }
}

/// Cancel every live held owner; retained terminals still count until read.
pub(crate) fn cancel(domain: &mut Domain, kit: Token, limits: &Limits, out: &mut Queue<Request>) {
    let kit = domain.kits.get_mut(Id::from_token(kit)).expect("a session owns its kit");
    assert!(!kit.jobs.cancelling, "one cancellation request at a time");
    kit.jobs.cancelling = true;
    for number in &kit.jobs.numbers() {
        cancel_one(kit.jobs.held.get_mut(number).expect("retained job"), limits, out);
    }
    cancelled(kit, out);
}

fn cancel_one(job: &mut Job, limits: &Limits, out: &mut Queue<Request>) {
    match job.state {
        State::Running => {
            if job.reader.is_none() {
                out.push(Request::Job {
                    owner: job.owner.token(),
                    act: JobAct::Read {
                        head: limits.shell_head,
                        tail: limits.shell_tail,
                        wait: skein_lib::Duration::from_nanos(u64::MAX),
                    },
                });
            }
            job.state = State::Cancelling;
            out.push(Request::CancelIo { owner: job.owner.token() });
        }
        State::Cancelling | State::Ended { .. } => {}
    }
}

fn is_cancelling(state: &State) -> bool {
    match state {
        State::Cancelling => true,
        State::Running | State::Ended { .. } => false,
    }
}

fn cancelled(kit: &mut Kit, out: &mut Queue<Request>) {
    if !kit.jobs.cancelling {
        return;
    }
    for (_, job) in &kit.jobs.held {
        if is_cancelling(&job.state) {
            return;
        }
    }
    kit.jobs.cancelling = false;
    out.push(Request::JobsCancelled { session: kit::session(kit) });
}

fn tell(session: Token, number: JobNumber, job: &Job, facts: &mut facts::Facts) {
    match &job.state {
        State::Ended { head, tail, dropped, end, at } => {
            let unread_bytes = u64::try_from(head.len())
                .expect("bounded head")
                .checked_add(u64::try_from(tail.len()).expect("bounded tail"))
                .expect("bounded kept bytes")
                .checked_add(*dropped)
                .expect("machine's total output fits u64");
            facts.push_at(
                *at,
                FactKind::JobEnded {
                    session,
                    job: number,
                    owner: job.call,
                    end: *end,
                    unread_bytes,
                    started: job.started,
                },
            );
        }
        State::Running | State::Cancelling => unreachable!("only settled jobs have an unread terminal"),
    }
}

fn answer(session: Token, read: Read, done: Done, facts: &mut facts::Facts, out: &mut Queue<Request>) {
    let (head, tail, dropped, end) = match done {
        Done::Job { head, tail, dropped, end } => (head, tail, dropped, end),
        Done::Loaded { .. }
        | Done::Listed { .. }
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::Exited { .. }
        | Done::Held { .. }
        | Done::Found { .. }
        | Done::Missing
        | Done::NotFile
        | Done::Linked
        | Done::NotDirectory
        | Done::TooLarge { .. }
        | Done::Escapes
        | Done::Failed { .. }
        | Done::TimedOut
        | Done::Cancelled => unreachable!("retained terminal is a job"),
    };
    let total = head.len().checked_add(tail.len()).expect("bounded output");
    let first = usize::try_from(read.kept.head).expect("head fits").min(if dropped > 0 { head.len() } else { total });
    let last = usize::try_from(read.kept.tail).expect("tail fits").min(if dropped > 0 {
        tail.len()
    } else {
        total.checked_sub(first).expect("head within kept output")
    });
    let mut leading = skein_lib::List::with_capacity(u32::try_from(first).expect("bounded head"));
    let mut trailing = skein_lib::List::with_capacity(u32::try_from(last).expect("bounded tail"));
    for (at, byte) in head.iter().chain(tail.iter()).enumerate() {
        if at < first {
            leading.push(*byte).expect("selected head room");
        }
        if at >= total.checked_sub(last).expect("tail within kept output") {
            trailing.push(*byte).expect("selected tail room");
        }
    }
    let dropped = dropped
        .checked_add(
            u64::try_from(
                total.checked_sub(first).expect("head within total").checked_sub(last).expect("tail within total"),
            )
            .expect("bounded capture"),
        )
        .expect("bounded total");
    let outcome = Outcome::Job {
        job: read.job,
        head: leading.into_boxed(),
        tail: trailing.into_boxed(),
        dropped,
        end,
        wait: read.timed.clamp,
        output: read.kept.clamp,
    };
    facts.push_call(
        facts::answered(session, crate::Tool::Job, &outcome, facts::Observation { omitted_bytes: 0, clamped: false }),
        read.observation,
    );
    out.push(Request::Answer { to: read.reply_to, outcome });
}
