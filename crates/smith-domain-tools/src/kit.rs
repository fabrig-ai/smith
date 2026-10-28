//! Kits: one session's tools. A session opens its kit when it starts, with the
//! authority its opener gave it, and closes it when it ends.
//!
//! A call is checked at the entrance, where refusing it costs nothing: the
//! family of tools it belongs to must be granted, its path must lie in a
//! repository of the checkout, and for a change in a writable one and outside
//! its `.git`, what it
//! would store must fit the limits, an edit must be of a file its LLM has read
//! and must change something, its deadline must not have passed, and the kit
//! must have room for one more call. A filesystem call that passes becomes a
//! flight; the namespace root's listing answers directly from its authority.
//! A yielded command leaves the batch but keeps its IO owner in the jobs
//! table. Reads of one job queue there; reads of different jobs can proceed.
//!
//! The transition table.
//!
//! ```text
//! state     event                  next      emits
//! Open      call                   Open      answer, or the flight's operation
//!           close, no flights         Closed    closed
//!           close, flights            Closing   a cancel per flight
//!           a flight ends             Open
//! Closing   a flight ends, not last   Closing
//!           the last flight ends      Closed    closed
//! ```
//!
//! A call or a close to a kit that is not open is the session's bug. A kit is
//! retired once it is Closed, after both calls and held owners have settled.
//!
//! Contract: domain/tools.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;

use skein_lib::{Env, Id, List, Queue, ReplyTo, Set, Slab, Time, Token};

use crate::authority::{self, Authority, Checkout, Located, Target};
use crate::boundary::{Expect, Refusal, Request};
use crate::call::{self, Call, Entry, Kind, Outcome};
use crate::call::{Mismatch, Replacement};
use crate::domain::Domain;
use crate::facts::{self, FactKind, Facts};
use crate::flight::{self, Flight, Work};
use crate::knowledge::Knowledge;
use crate::limits::Limits;
use crate::path::{self, Path};
use crate::window::Span;

#[derive(Debug)]
pub(crate) struct Kit {
    /// The session's token, echoed when the kit closes.
    session: Token,
    pub(crate) checkout: Checkout,
    pub(crate) knowledge: Knowledge,
    /// The flights running its calls.
    pub(crate) flights: Set<Id<Flight>>,
    pub(crate) jobs: crate::jobs::Jobs,
    state: State,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum State {
    Open,
    /// Its flights have been cancelled; it ends with the last.
    Closing,
    /// Terminal: retired, until the reclaim point frees it.
    Closed,
}

pub(crate) fn open(
    domain: &mut Domain,
    env: &Env<Limits>,
    session: Token,
    jobs: u32,
    authority: Authority,
    out: &mut Queue<Request>,
) {
    if domain.kits.is_full() {
        refuse(domain, session, Refusal::Busy, out);
        return;
    }
    let Some(checkout) = authority::admit(authority, &env.limits) else {
        refuse(domain, session, Refusal::Invalid, out);
        return;
    };
    let kit = Kit {
        session,
        checkout,
        knowledge: Knowledge::new(env.limits.known_files),
        flights: Set::with_capacity(env.limits.calls),
        jobs: crate::jobs::Jobs::new(&env.limits, jobs),
        state: State::Open,
    };
    let id = domain.kits.insert(kit).expect("checked for room above");
    domain.facts.push(FactKind::Opened { session });
    out.push(Request::Opened { session, kit: id.token() });
}

fn refuse(domain: &mut Domain, session: Token, refusal: Refusal, out: &mut Queue<Request>) {
    domain.facts.push(FactKind::Refused { session, refusal });
    out.push(Request::Refused { session, refusal });
}

#[expect(
    clippy::too_many_arguments,
    reason = "the call boundary carries its affine right and observation context beside its work"
)]
pub(crate) fn call(
    domain: &mut Domain,
    env: &Env<Limits>,
    kit: Token,
    reply_to: ReplyTo,
    observation: Option<crate::CallInfo>,
    call: Call,
    deadline: Time,
    out: &mut Queue<Request>,
) {
    let id = Id::from_token(kit);
    let entry = domain.kits.get(id).expect("the session owns its kit");
    assert!(entry.state == State::Open, "no call follows a close");
    let call = match call {
        Call::Job { job, wait, output, stop } => {
            if !entry.checkout.grants.shell {
                domain.facts.push_call(
                    facts::answered(
                        session(entry),
                        crate::Tool::Job,
                        &Outcome::NotGranted,
                        facts::Observation { omitted_bytes: 0, clamped: false },
                    ),
                    observation,
                );
                out.push(Request::Answer { to: reply_to, outcome: Outcome::NotGranted });
                return;
            }
            let read = crate::jobs::read(
                job,
                reply_to,
                observation,
                crate::command::waited(wait, &env.limits),
                crate::command::kept(output, &env.limits),
                deadline,
                stop,
            );
            crate::jobs::call(domain, env, id, read, out);
            return;
        }
        Call::Shell { after: crate::Yield::Asked(_), .. } if entry.jobs.full() => {
            out.push(Request::Invalid { to: reply_to, problem: crate::JobProblem::Full { jobs: env.limits.jobs } });
            return;
        }
        call @ (Call::Read { .. }
        | Call::List { .. }
        | Call::Search { .. }
        | Call::Write { .. }
        | Call::Edit { .. }
        | Call::Remove { .. }
        | Call::Shell { .. }) => call,
    };
    let Domain { kits, flights, facts } = domain;
    let kit = kits.get_mut(id).expect("a kit lives until its session closes it");
    assert!(kit.state == State::Open, "no call follows a close");
    let tool = call::tool(&call);
    match admit(kit, call, deadline, env) {
        Ok(Admitted::Work(work)) => {
            facts.push_call(FactKind::Started { session: kit.session, tool }, observation);
            let flight = flight::start(flights, id, reply_to, observation, work, deadline, env, out);
            let fresh = kit.flights.insert(flight).expect("checked for room at the entrance");
            assert!(fresh, "a flight is new to its kit");
        }
        Ok(Admitted::Directories) => {
            facts.push_call(FactKind::Started { session: kit.session, tool }, observation);
            let outcome = directories(&kit.checkout, &env.limits);
            facts.push_call(
                facts::answered(kit.session, tool, &outcome, facts::Observation { omitted_bytes: 0, clamped: false }),
                observation,
            );
            out.push(Request::Answer { to: reply_to, outcome });
        }
        Err(outcome) => {
            facts.push_call(
                facts::answered(kit.session, tool, &outcome, facts::Observation { omitted_bytes: 0, clamped: false }),
                observation,
            );
            out.push(Request::Answer { to: reply_to, outcome });
        }
    }
}

pub(crate) fn close(domain: &mut Domain, kit: Token, limits: &Limits, out: &mut Queue<Request>) {
    let id = Id::from_token(kit);
    let kit = domain.kits.get_mut(id).expect("a kit lives until its session closes it");
    assert!(kit.state == State::Open, "a kit is closed once");
    if kit.flights.is_empty() && kit.jobs.empty() {
        end(&mut domain.kits, &mut domain.facts, id, out);
        return;
    }
    kit.state = State::Closing;
    domain.facts.push(FactKind::Closing { session: kit.session, running: kit.flights.len() });
    for flight in &kit.flights {
        flight::cancel(*flight, out);
    }
    kit.jobs.close(kit.session, &mut domain.facts, limits, out);
    settled(&mut domain.kits, &mut domain.facts, id, out);
}

/// The token of the kit's session.
pub(crate) fn session(kit: &Kit) -> Token {
    kit.session
}

/// Whether the kit is closing, its flights cancelled.
pub(crate) fn closing(kit: &Kit) -> bool {
    match kit.state {
        State::Open => false,
        State::Closing => true,
        State::Closed => unreachable!("a closed kit runs no flight"),
    }
}

/// The flight `flight` of the kit `id` has answered: it leaves the kit, which ends
/// with it if it was the last of a closing kit.
pub(crate) fn finished(
    kits: &mut Slab<Kit>,
    facts: &mut Facts,
    id: Id<Kit>,
    flight: Id<Flight>,
    out: &mut Queue<Request>,
) {
    let kit = kits.get_mut(id).expect("a kit lives until its flights have ended");
    let running = kit.flights.remove(&flight);
    assert!(running, "a flight is its kit's until it ends");
    match kit.state {
        State::Open => {}
        State::Closing => {
            if kit.flights.is_empty() && kit.jobs.empty() {
                end(kits, facts, id, out);
            }
        }
        State::Closed => unreachable!("a closed kit runs no flight"),
    }
}

/// Finish a closing kit once both ordinary calls and held owners have settled.
pub(crate) fn settled(kits: &mut Slab<Kit>, facts: &mut Facts, id: Id<Kit>, out: &mut Queue<Request>) {
    let kit = kits.get(id).expect("a kit owns its pending work");
    if kit.state == State::Closing && kit.flights.is_empty() && kit.jobs.empty() {
        end(kits, facts, id, out);
    }
}

/// Ends the kit `id`, which runs nothing: its session learns it has closed.
fn end(kits: &mut Slab<Kit>, facts: &mut Facts, id: Id<Kit>, out: &mut Queue<Request>) {
    let kit = kits.get_mut(id).expect("a kit lives until it is retired");
    kit.state = State::Closed;
    facts.push(FactKind::Closed { session: kit.session });
    out.push(Request::Closed { session: kit.session });
    kits.retire(id);
}

/// An admitted IO flight, or the namespace root's immediate listing.
enum Admitted {
    Work(Work),
    Directories,
}

/// A file-system place; the namespace root has no file behind it.
fn place(checkout: &Checkout, path: &Path, maximum: u32) -> Result<Located, Outcome> {
    match authority::locate(checkout, path, maximum)? {
        Target::Place(located) => Ok(located),
        Target::Directories => Err(Outcome::NotFile),
    }
}

/// Directory names at the namespace root, in name order and within both
/// listing caps. Legacy nested mounts contribute their first name once.
fn directories(checkout: &Checkout, limits: &Limits) -> Outcome {
    let mut names = List::with_capacity(u32::try_from(checkout.mounts.len()).expect("admitted mounts"));
    for mount in &checkout.mounts {
        let name = first_name(&mount.at);
        if !name.is_empty() && !names.as_slice().contains(&name) {
            names.push(name).expect("at most one name per mount");
        }
    }
    let mut names = names.into_boxed();
    names.sort_unstable();
    let mut entries =
        List::with_capacity(limits.list_entries.min(u32::try_from(names.len()).expect("admitted mount count")));
    let mut bytes = 0_u64;
    for name in &names {
        let cell = u64::try_from(size_of::<Entry>()).expect("entry cell fits");
        let cost = cell.checked_add(u64::try_from(name.len()).expect("admitted name fits")).expect("bounded entry");
        let Some(next) = bytes.checked_add(cost) else { break };
        if entries.len() >= limits.list_entries || next > limits.list_bytes {
            break;
        }
        bytes = next;
        entries
            .push(Entry { name: skein_lib::bytes::copy_of(name), kind: Kind::Directory })
            .expect("checked entry cap");
    }
    let more = u64::try_from(
        names.len().checked_sub(usize::try_from(entries.len()).expect("entry count")).expect("kept prefix"),
    )
    .expect("admitted count");
    Outcome::Listed { entries: entries.into_boxed(), more }
}

fn first_name(path: &[u8]) -> &[u8] {
    for (index, byte) in path.iter().enumerate() {
        if *byte == b'/' {
            return path.get(..index).expect("slash index is in path");
        }
    }
    path
}

/// What `call` does if it passes the entrance, or the outcome that refuses it.
fn admit(kit: &Kit, call: Call, deadline: Time, env: &Env<Limits>) -> Result<Admitted, Outcome> {
    let limits = &env.limits;
    if !authority::granted(kit.checkout.grants, &call) {
        return Err(Outcome::NotGranted);
    }
    let work = match call {
        Call::Read { path, offset, window } => {
            let located = place(&kit.checkout, &path, limits.path_bytes)?;
            file(&located.place)?;
            if offset == 0 {
                return Err(Outcome::Failed { fault: crate::Fault::Other });
            }
            match window {
                crate::Budget::Asked(0) => return Err(Outcome::Failed { fault: crate::Fault::Other }),
                crate::Budget::Asked(_) | crate::Budget::Default => {}
            }
            Work::Read { place: located.place, span: Span { offset, window: crate::window::window(window, limits) } }
        }
        Call::List { path, depth, glob } => {
            match depth {
                crate::Depth::Levels(0) => return Err(Outcome::Failed { fault: crate::Fault::Other }),
                crate::Depth::Levels(_) | crate::Depth::Whole => {}
            }
            if let Some(filter) = &glob {
                passable(filter)?;
                if filter.len() > usize::try_from(limits.tool_payload).expect("u32 fits usize") {
                    return Err(Outcome::TooLarge {
                        what: crate::SizePart::ListGlob,
                        size: u64::try_from(filter.len()).unwrap_or(u64::MAX),
                        bound: limits.tool_payload,
                    });
                }
            }
            match authority::locate(&kit.checkout, &path, limits.path_bytes)? {
                Target::Place(located) => Work::List { place: located.place, depth, glob },
                Target::Directories => {
                    ready(kit, deadline, env)?;
                    return Ok(Admitted::Directories);
                }
            }
        }
        Call::Search { path, pattern, glob } => {
            passable(&pattern)?;
            if let Some(glob) = &glob {
                passable(glob)?;
            }
            let located = match authority::locate(&kit.checkout, &path, limits.path_bytes)? {
                Target::Place(located) => located,
                Target::Directories => {
                    return Err(Outcome::NotDirectory { path: path::Resolved::Outside { path: Box::new([]) } });
                }
            };
            Work::Search { place: located.place, pattern, glob }
        }
        Call::Write { path, content } => {
            let located = writable(&kit.checkout, &path, limits)?;
            fits(&content, limits)?;
            // As the kit knows the file: at the version its LLM read, or
            // absent.
            let expect = expected(kit, &located.place);
            Work::Write { place: located.place, content, expect }
        }
        Call::Remove { path } => {
            let located = writable(&kit.checkout, &path, limits)?;
            let expect = expected(kit, &located.place);
            Work::Remove { place: located.place, expect }
        }
        Call::Edit { path, edits } => {
            let located = writable(&kit.checkout, &path, limits)?;
            replacements_fit(&edits, limits)?;
            Work::Edit { place: located.place, edits }
        }
        Call::Shell { command, timeout, output, after } => {
            passable(&command)?;
            // Commands run in the working directory, which must be in the
            // checkout.
            let here = Path { absolute: false, parts: Box::new([]) };
            let located = place(&kit.checkout, &here, limits.path_bytes)?;
            match timeout {
                crate::Timeout::Asked(duration) => {
                    if duration == skein_lib::Duration::ZERO {
                        return Err(Outcome::Failed { fault: crate::Fault::Other });
                    }
                }
                crate::Timeout::Default => {}
            }
            match output {
                crate::Budget::Asked(0) => return Err(Outcome::Failed { fault: crate::Fault::Other }),
                crate::Budget::Asked(_) | crate::Budget::Default => {}
            }
            Work::Shell {
                cwd: located.place,
                command,
                timed: crate::command::timed(timeout, limits),
                kept: crate::command::kept(output, limits),
                yielded: crate::command::yielded(after, limits),
            }
        }
        Call::Job { .. } => unreachable!("job calls go through their numbered table"),
    };
    ready(kit, deadline, env)?;
    Ok(Admitted::Work(work))
}

fn expected(kit: &Kit, place: &path::Place) -> Expect {
    match kit.knowledge.version(place) {
        Some(version) => Expect::Is { version },
        None => Expect::Absent,
    }
}

/// The common entrance deadline and concurrent-call bounds.
pub(crate) fn ready(kit: &Kit, deadline: Time, env: &Env<Limits>) -> Result<(), Outcome> {
    let limits = &env.limits;
    if deadline <= env.now {
        return Err(Outcome::TimedOut);
    }
    if kit.flights.len().checked_add(kit.jobs.calls()).expect("bounded active calls") >= limits.calls {
        return Err(Outcome::Busy);
    }
    Ok(())
}

/// Where `path` is, if the kit may write there: in a writable repository, and
/// not in its `.git`.
fn writable(checkout: &Checkout, path: &Path, limits: &Limits) -> Result<Located, Outcome> {
    let located = place(checkout, path, limits.path_bytes)?;
    file(&located.place)?;
    if !located.writable {
        return Err(Outcome::ReadOnly { path: authority::resolved(checkout, &located.place) });
    }
    if path::in_git(&located.place.path) {
        return Err(Outcome::Protected);
    }
    Ok(located)
}

/// Refuses `bytes` as an argument to a process if they hold a NUL.
fn passable(bytes: &[u8]) -> Result<(), Outcome> {
    for byte in bytes {
        if *byte == 0 {
            return Err(Outcome::NulByte);
        }
    }
    Ok(())
}

/// Refuses `content` if it is larger than one piece.
fn fits(content: &[u8], limits: &Limits) -> Result<(), Outcome> {
    let size = u64::try_from(content.len()).unwrap_or(u64::MAX);
    if size > u64::from(limits.tool_payload) {
        return Err(Outcome::TooLarge { what: crate::SizePart::WriteContent, size, bound: limits.tool_payload });
    }
    Ok(())
}

/// File-only calls cannot name the directory itself (domain/tools.md, section 2).
fn file(place: &path::Place) -> Result<(), Outcome> {
    if place.path.is_empty() { Err(Outcome::NotFile) } else { Ok(()) }
}

/// The aggregate old/new buffers keep the respective per-piece caps; the
/// list cells also fit that bound. No semantic mismatch is decided before load.
fn replacements_fit(edits: &[Replacement], limits: &Limits) -> Result<(), Outcome> {
    if edits.is_empty() {
        return Err(Outcome::EditFailed { edit: 1, why: Mismatch::NoMatch });
    }
    let cells = u64::try_from(edits.len()).or(Err(Outcome::TooLarge {
        what: crate::SizePart::EditItems,
        size: u64::MAX,
        bound: limits.tool_payload,
    }))?;
    let mut old = 0_u64;
    let mut new = 0_u64;
    for replacement in edits {
        old = old
            .checked_add(u64::try_from(replacement.old.len()).or(Err(Outcome::TooLarge {
                what: crate::SizePart::EditOld,
                size: u64::MAX,
                bound: limits.tool_payload,
            }))?)
            .ok_or(Outcome::TooLarge { what: crate::SizePart::EditOld, size: u64::MAX, bound: limits.tool_payload })?;
        new = new
            .checked_add(u64::try_from(replacement.new.len()).or(Err(Outcome::TooLarge {
                what: crate::SizePart::EditNew,
                size: u64::MAX,
                bound: limits.tool_payload,
            }))?)
            .ok_or(Outcome::TooLarge { what: crate::SizePart::EditNew, size: u64::MAX, bound: limits.tool_payload })?;
    }
    let size = old.max(new).max(cells);
    if size > u64::from(limits.tool_payload) {
        let what = if old == size {
            crate::SizePart::EditOld
        } else if new == size {
            crate::SizePart::EditNew
        } else {
            crate::SizePart::EditItems
        };
        return Err(Outcome::TooLarge { what, size, bound: limits.tool_payload });
    }
    Ok(())
}

/// A permission/result snapshot belongs only to its session's root authority.
pub(crate) fn native_version(
    domain: &mut Domain,
    env: &Env<Limits>,
    kit: Token,
    at: path::Place,
    version: Option<crate::Version>,
) {
    if at.path.len() > usize::try_from(env.limits.path_bytes).expect("path ceiling fits usize") {
        return;
    }
    let kit = domain.kits.get_mut(Id::from_token(kit)).expect("a live session owns its kit");
    match kit.state {
        State::Closing | State::Closed => return,
        State::Open => {}
    }
    let mut admitted = false;
    for mount in &kit.checkout.mounts {
        if mount.root == at.root {
            admitted = true;
        }
    }
    assert!(admitted, "program snapshot belongs to an installed root");
    match version {
        Some(version) => kit.knowledge.record(at, version),
        None => kit.knowledge.forget(&at),
    }
}
