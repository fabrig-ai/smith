//! Flights: active calls and the original IO owners of held commands.
//!
//! A call that passes the kit's entrance becomes a flight, which asks io for its
//! operation and answers the call once that operation ends or yields. A yielded
//! owner stays live outside its batch until its job ends. Each call answers once,
//! whether the operation completed, failed, timed out, or lost or won a race
//! with a cancel.
//!
//! A write stores the file if it is as the kit knows it: at the version its
//! LLM read or wrote last, or absent if it knows none. An edit runs in two
//! phases: it loads the file, which must be at the version the kit knows,
//! makes the edit in memory, and stores the result expecting that version.
//! io compares that with the current file just before it stores, so a change
//! made since, by another kit or by anything else, is caught: as `Stale` if
//! the LLM had read the file, and as `NotRead` if it had not and the write
//! would have created it. A store follows no symbolic link, so a change lands
//! only in the repository its path names. A write that may or may not have
//! happened (timed out, failed) leaves the knowledge as it was, and the next
//! write's check settles it.
//!
//! A command runs until it exits or its deadline, the call's or the tools'
//! own limit for commands, whichever is sooner. It may change any file the kit
//! may write; what the kit knows stays as it was, and the version check of
//! the next write or edit catches what the command changed.
//!
//! The transition table. Every other cell is unreachable by the boundary's
//! contract: an operation ends only in its own terminals.
//!
//! ```text
//! state      terminal                    next      answer
//! Reading    loaded                      Done      the window; the version is known
//!            missing                     Done      not found; nothing is known there
//!            any other                   Done      what it says
//! Listing    listed                     Done      the entries
//!            any other                   Done      what it says
//! Searching  found                       Done      the lines found
//!            exited                      Done      how rg failed
//!            any other                   Done      what it says
//! Editing    loaded, as known, edited    Storing   (store the edited file)
//!            loaded, otherwise           Done      stale, no match, ambiguous, too large,
//!                                                  or cancelled if the kit is closing
//!            missing                     Done      not found; nothing is known there
//!            any other                   Done      what it says
//! Storing    stored                      Done      written or edited; the new version is known
//!            conflict, creating          Done      not read
//!            conflict, replacing         Done      stale; a file now absent is forgotten
//!            missing                     Done      not found; nothing is known there
//!            any other                   Done      what it says
//! Running    held                        Held      running, job number and output
//!            exited                      Done      how it ended, and its output
//!            any other                   Done      what it says
//! Held       job running                 Held      the active read, then the next read
//!            job ended                   Done      the read, or owner-close settlement
//! ```
//!
//! A kit that closes while an edit loads stores nothing: whichever way the
//! load's race with its cancel went, the edit answers `Cancelled`.
//!
//! A cell either moves to its next state or answers the call; answering, and
//! telling it as a fact, is done in one place ([`settle`]). A flight is retired
//! once it is Done ([`follow`]), which also ends a closing kit with its last
//! flight and held owner.
//!
//! Contract: domain/tools.md, sections 3, 5 and 6; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;
use core::mem;

use skein_lib::{Env, Id, Queue, ReplyTo, Slab, Time, Token};

use crate::authority::{self, Checkout};
use crate::boundary::{Done, Expect, Op, Request};
use crate::call::Replacement;
use crate::call::{Outcome, Tool};
use crate::domain::Domain;
use crate::edit;
use crate::facts::{self, Facts};
use crate::kit::{self, Kit};
use crate::knowledge::Knowledge;
use crate::limits::Limits;
use crate::path::Place;
use crate::window::{self, Span};

#[derive(Debug)]
pub(crate) struct Flight {
    /// The kit whose call it runs.
    kit: Id<Kit>,
    /// The tool the call is for, to tell of it.
    tool: Tool,
    observation: Option<crate::CallInfo>,
    started: Time,
    state: State,
}

/// The session whose live flight issued the IO token, without exposing flight state.
pub(crate) fn io_session(domain: &Domain, owner: Token) -> Option<Token> {
    let flight = domain.flights.get(Id::from_token(owner))?;
    Some(kit::session(domain.kits.get(flight.kit)?))
}

/// What a cell does: go on in another state, or answer the call.
enum Next {
    Wait(State),
    Answer { reply_to: ReplyTo, outcome: Outcome, omitted_bytes: u64 },
    Held { reply_to: ReplyTo, outcome: Outcome, job: crate::JobNumber },
}

#[derive(Debug)]
enum State {
    /// Loading the file at `place`, to answer with a window of it.
    Reading { reply_to: ReplyTo, place: Place, span: Span },
    /// Waiting for a selected tree listing.
    Listing { reply_to: ReplyTo, place: Place },
    /// Loading the file to edit.
    Editing { reply_to: ReplyTo, editing: Editing },
    /// Storing the file at `place`, for `change`.
    Storing { reply_to: ReplyTo, place: Place, change: Change },
    /// Running a command.
    Running {
        reply_to: ReplyTo,
        place: Place,
        timeout: crate::TimeoutClamp,
        output: crate::BytesClamp,
        after: crate::TimeoutClamp,
    },
    /// Original IO owner of a command answered running, outside the batch.
    Held { job: crate::JobNumber },
    /// Searching files.
    Searching { reply_to: ReplyTo, place: Place },
    /// Terminal: answered, holds nothing.
    Done,
}

/// An edit while it loads the file.
#[derive(Debug)]
pub(crate) struct Editing {
    place: Place,
    edits: Box<[Replacement]>,
    /// The call's, which its store keeps too.
    deadline: Time,
}

/// What a store does to the file, and so what it answers.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Change {
    /// Writes a file the kit knows nothing of.
    Create,
    /// Removes one file or empty directory; known says whether a file version was read.
    Remove { known: bool },
    /// Writes over the file at the version the kit knows.
    Replace,
    /// Writes the file edited, having `replaced` that many occurrences.
    Edit { replaced: u32 },
}

/// What a call that passed its kit's entrance does.
#[derive(Debug)]
pub(crate) enum Work {
    Read {
        place: Place,
        span: Span,
    },
    List {
        place: Place,
        depth: crate::Depth,
        glob: Option<Box<[u8]>>,
    },
    Write {
        place: Place,
        content: Box<[u8]>,
        expect: Expect,
    },
    Remove {
        place: Place,
        expect: Expect,
    },
    Edit {
        place: Place,
        edits: Box<[Replacement]>,
    },
    /// Runs `command` in `cwd` for at most `timeout`, in an uncontained process group.
    Shell {
        cwd: Place,
        command: Box<[u8]>,
        timed: crate::command::Timed,
        kept: crate::command::Kept,
        yielded: (Option<skein_lib::Duration>, crate::TimeoutClamp),
    },
    Search {
        place: Place,
        pattern: Box<[u8]>,
        glob: Option<Box<[u8]>>,
    },
}

/// Starts a flight for `work` in the kit `kit`, which has room for one, asking
/// io for its operation by `deadline`, or sooner if the tools' own limit on
/// file operations, or on commands, falls first.
#[expect(
    clippy::too_many_arguments,
    reason = "the call boundary carries its affine right and observation context beside its work"
)]
pub(crate) fn start(
    flights: &mut Slab<Flight>,
    kit: Id<Kit>,
    reply_to: ReplyTo,
    observation: Option<crate::CallInfo>,
    work: Work,
    deadline: Time,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> Id<Flight> {
    let limits = &env.limits;
    let call_deadline = deadline;
    // A command and a search have limits of their own; any other call is a
    // file operation.
    let (tool, limit) = match &work {
        Work::Read { .. } => (Tool::Read, limits.file_timeout),
        Work::List { .. } => (Tool::List, limits.file_timeout),
        Work::Search { .. } => (Tool::Search, limits.search_timeout),
        Work::Write { .. } => (Tool::Write, limits.file_timeout),
        Work::Remove { .. } => (Tool::Remove, limits.file_timeout),
        Work::Edit { .. } => (Tool::Edit, limits.file_timeout),
        Work::Shell { timed, .. } => (Tool::Shell, timed.timeout),
    };
    let deadline = deadline.min(env.now.saturating_add(limit));
    // The place goes to io and stays with the flight: copy at emission.
    let (state, op) = match work {
        Work::Read { place, span } => {
            let op = Op::Load { at: place.clone(), max: limits.file_bytes };
            (State::Reading { reply_to, place, span }, op)
        }
        Work::List { place, depth, glob } => (
            State::Listing { reply_to, place: place.clone() },
            Op::List { at: place, depth, glob, max: limits.list_entries, max_bytes: limits.list_bytes },
        ),
        Work::Write { place, content, expect } => {
            let change = match expect {
                Expect::Absent => Change::Create,
                Expect::Is { .. } => Change::Replace,
            };
            let op = Op::Store { at: place.clone(), content, expect };
            (State::Storing { reply_to, place, change }, op)
        }
        Work::Remove { place, expect } => {
            let known = match expect {
                Expect::Absent => false,
                Expect::Is { .. } => true,
            };
            let op = Op::Remove { at: place.clone(), expect };
            (State::Storing { reply_to, place, change: Change::Remove { known } }, op)
        }
        Work::Edit { place, edits } => {
            let op = Op::Load { at: place.clone(), max: limits.file_bytes };
            (State::Editing { reply_to, editing: Editing { place, edits, deadline: call_deadline } }, op)
        }
        Work::Shell { cwd, command, timed, kept, yielded } => {
            let place = cwd.clone();
            let op = Op::Spawn { cwd, command, head: kept.head, tail: kept.tail, after: yielded.0 };
            (State::Running { reply_to, place, timeout: timed.clamp, output: kept.clamp, after: yielded.1 }, op)
        }
        Work::Search { place, pattern, glob } => {
            let op =
                Op::Search { at: place.clone(), pattern, glob, hits: limits.search_hits, bytes: limits.search_bytes };
            (State::Searching { reply_to, place }, op)
        }
    };
    // Room: a kit has at most `calls` flights, and the slab twice that many slots
    // per kit, for the flights retired in this iteration, which are at most those
    // running when it began.
    let id = flights
        .insert(Flight { kit, tool, observation, started: env.now, state })
        .expect("the flight slab has room for every kit's flights");
    out.push(Request::Io { owner: id.token(), op, deadline });
    id
}

/// Cancels the flight `id`'s operation, for its kit is closing. The flight answers
/// with whichever terminal comes.
pub(crate) fn cancel(id: Id<Flight>, out: &mut Queue<Request>) {
    out.push(Request::CancelIo { owner: id.token() });
}

// The entry point for terminals: look the flight up, take its state out, run the
// cell's handler, follow the new state.

pub(crate) fn done(domain: &mut Domain, env: &Env<Limits>, owner: Token, done: Done, out: &mut Queue<Request>) {
    let id = Id::from_token(owner);
    let flight = domain.flights.get(id).expect("an IO owner lives until its terminal");
    match flight.state {
        State::Held { job } => {
            crate::jobs::done(domain, env, id, job, done, out);
            return;
        }
        State::Reading { .. }
        | State::Listing { .. }
        | State::Searching { .. }
        | State::Editing { .. }
        | State::Storing { .. }
        | State::Running { .. }
        | State::Done => {}
    }
    let Domain { kits, flights, facts } = domain;
    let flight = flights.get_mut(id).expect("a flight lives until its operation has ended");
    let kit = kits.get_mut(flight.kit).expect("a kit lives until its flights have ended");
    let state = mem::replace(&mut flight.state, State::Done);
    let clamped = state_clamped(&state);
    let next = match state {
        State::Reading { reply_to, place, span } => loaded(kit, reply_to, place, span, done, env.limits.file_bytes),
        State::Listing { reply_to, place } => scanned(&kit.checkout, reply_to, place, done),
        State::Searching { reply_to, place } => found(&kit.checkout, reply_to, place, done),
        State::Editing { reply_to, editing } => to_edit(kit, id, reply_to, editing, done, env, out),
        State::Storing { reply_to, place, change } => {
            stored(&kit.checkout, &mut kit.knowledge, reply_to, place, change, done, env.limits.file_bytes)
        }
        State::Running { reply_to, place, timeout, output, after } => match done {
            Done::Held { head, tail, dropped } => {
                let job = kit.jobs.held(id, flight.observation, flight.started);
                assert!(kit.flights.remove(&id), "a yielding command leaves its batch");
                if kit::closing(kit) {
                    out.push(Request::CancelIo { owner });
                }
                Next::Held {
                    reply_to,
                    outcome: Outcome::Running { job, head, tail, dropped, timeout, output, after },
                    job,
                }
            }
            done @ (Done::Loaded { .. }
            | Done::Listed { .. }
            | Done::Removed
            | Done::NotEmpty
            | Done::Stored { .. }
            | Done::Conflict { .. }
            | Done::Exited { .. }
            | Done::Job { .. }
            | Done::Found { .. }
            | Done::Missing
            | Done::NotFile
            | Done::Linked
            | Done::NotDirectory
            | Done::TooLarge { .. }
            | Done::Escapes
            | Done::Failed { .. }
            | Done::TimedOut
            | Done::Cancelled) => exited(&kit.checkout, reply_to, place, timeout, output, done),
        },
        State::Held { .. } => unreachable!("held terminals use the job table"),
        State::Done => unreachable!("a flight that has answered has nothing in flight"),
    };
    flight.state = settle(next, kit::session(kit), flight, clamped, facts, out);
    follow(kits, flights, facts, id, out);
}

/// The applied request caps survive every terminal, including cancellation and timeout.
fn state_clamped(state: &State) -> bool {
    match state {
        State::Reading { span, .. } => facts::bytes_clamped(span.window.clamp),
        State::Running { timeout, output, after, .. } => {
            facts::timeout_clamped(*timeout) || facts::bytes_clamped(*output) || facts::timeout_clamped(*after)
        }
        State::Listing { .. }
        | State::Searching { .. }
        | State::Editing { .. }
        | State::Storing { .. }
        | State::Held { .. }
        | State::Done => false,
    }
}

/// The state a cell leads to: the one it moves to, or Done once it has
/// answered the call, which is told as a fact.
fn settle(
    next: Next,
    session: Token,
    flight: &Flight,
    clamped: bool,
    facts: &mut Facts,
    out: &mut Queue<Request>,
) -> State {
    match next {
        Next::Wait(state) => state,
        Next::Held { reply_to, outcome, job } => {
            facts.push_call(
                facts::answered(session, flight.tool, &outcome, facts::Observation { omitted_bytes: 0, clamped }),
                flight.observation,
            );
            out.push(Request::Answer { to: reply_to, outcome });
            State::Held { job }
        }
        Next::Answer { reply_to, outcome, omitted_bytes } => {
            facts.push_call(
                facts::answered(session, flight.tool, &outcome, facts::Observation { omitted_bytes, clamped }),
                flight.observation,
            );
            out.push(Request::Answer { to: reply_to, outcome });
            State::Done
        }
    }
}

/// What a flight's state implies, applied after every transition: a flight that is
/// Done is retired, and leaves its kit.
fn follow(
    kits: &mut Slab<Kit>,
    flights: &mut Slab<Flight>,
    facts: &mut Facts,
    id: Id<Flight>,
    out: &mut Queue<Request>,
) {
    let flight = flights.get(id).expect("a flight lives until it is retired");
    match flight.state {
        State::Reading { .. }
        | State::Listing { .. }
        | State::Searching { .. }
        | State::Editing { .. }
        | State::Storing { .. }
        | State::Running { .. }
        | State::Held { .. } => {}
        State::Done => {
            kit::finished(kits, facts, flight.kit, id, out);
            flights.retire(id);
        }
    }
}

// Cell handlers: each takes the source state's data by value and says what
// comes next.

/// Reading, ended: answer with the window, and know the version read.
fn loaded(kit: &mut Kit, reply_to: ReplyTo, place: Place, span: Span, done: Done, bound: u32) -> Next {
    let outcome = match done {
        Done::Loaded { content, version } => {
            kit.knowledge.record(place, version);
            let read = window::read(content, span);
            return Next::Answer { reply_to, outcome: read.outcome, omitted_bytes: read.omitted_bytes };
        }
        Done::Missing => {
            kit.knowledge.forget(&place);
            Outcome::NotFound { path: authority::resolved(&kit.checkout, &place) }
        }
        Done::NotFile => Outcome::NotFile,
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(&kit.checkout, &place) },
        Done::TooLarge { size } => Outcome::TooLarge { what: crate::SizePart::File, size, bound },
        Done::Escapes => Outcome::Outside { path: authority::resolved(&kit.checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Listed { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::Linked
        | Done::Exited { .. }
        | Done::Found { .. } => unreachable!("io ends a load with a load's terminal"),
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

/// Listing, ended: answer with the entries.
fn scanned(checkout: &Checkout, reply_to: ReplyTo, place: Place, done: Done) -> Next {
    let outcome = match done {
        Done::Listed { entries, more } => Outcome::Listed { entries, more },
        Done::Missing => Outcome::NotFound { path: authority::resolved(checkout, &place) },
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(checkout, &place) },
        Done::Escapes => Outcome::Outside { path: authority::resolved(checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Loaded { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::NotFile
        | Done::Linked
        | Done::TooLarge { .. }
        | Done::Exited { .. }
        | Done::Found { .. } => unreachable!("io ends a scan with a scan's terminal"),
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

/// Searching, ended: answer with the lines found, or how rg failed.
fn found(checkout: &Checkout, reply_to: ReplyTo, place: Place, done: Done) -> Next {
    let outcome = match done {
        Done::Found { hits, more, timed_out } => Outcome::Found { hits, more, timed_out },
        Done::Exited { exit, head, tail, dropped } => Outcome::Exited {
            exit,
            head,
            tail,
            dropped,
            timeout: crate::TimeoutClamp::Within,
            output: crate::BytesClamp::Within,
        },
        Done::Missing => Outcome::NotFound { path: authority::resolved(checkout, &place) },
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(checkout, &place) },
        Done::Escapes => Outcome::Outside { path: authority::resolved(checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Loaded { .. }
        | Done::Listed { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::NotFile
        | Done::Linked
        | Done::TooLarge { .. } => unreachable!("io ends a search with a search's terminal"),
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

/// Editing, loaded: match the ordered list and store only over this loaded
/// version, if the kit is not closing.
fn to_edit(
    kit: &mut Kit,
    id: Id<Flight>,
    reply_to: ReplyTo,
    editing: Editing,
    done: Done,
    env: &Env<Limits>,
    out: &mut Queue<Request>,
) -> Next {
    let Editing { place, edits, deadline } = editing;
    let outcome = match done {
        Done::Loaded { content, version } => {
            if kit::closing(kit) {
                return Next::Answer { reply_to, outcome: Outcome::Cancelled, omitted_bytes: 0 };
            }
            if deadline <= env.now {
                return Next::Answer { reply_to, outcome: Outcome::TimedOut, omitted_bytes: 0 };
            }
            match edit::apply_edits(content, &edits, &env.limits) {
                Ok((content, replaced)) => {
                    let deadline = deadline.min(env.now.saturating_add(env.limits.file_timeout));
                    let op = Op::Store { at: place.clone(), content, expect: Expect::Is { version } };
                    out.push(Request::Io { owner: id.token(), op, deadline });
                    return Next::Wait(State::Storing { reply_to, place, change: Change::Edit { replaced } });
                }
                Err(outcome) => outcome,
            }
        }
        Done::Missing => {
            kit.knowledge.forget(&place);
            Outcome::NotFound { path: authority::resolved(&kit.checkout, &place) }
        }
        Done::NotFile => Outcome::NotFile,
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(&kit.checkout, &place) },
        Done::TooLarge { size } => {
            Outcome::TooLarge { what: crate::SizePart::File, size, bound: env.limits.file_bytes }
        }
        Done::Escapes => Outcome::Outside { path: authority::resolved(&kit.checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Listed { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::Linked
        | Done::Exited { .. }
        | Done::Found { .. } => unreachable!("io ends a load with a load's terminal"),
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

/// Storing, ended: answer, and know the version written. `bound` is the
/// whole-file bound a file larger than io would store is named against.
fn stored(
    checkout: &Checkout,
    knowledge: &mut Knowledge,
    reply_to: ReplyTo,
    place: Place,
    change: Change,
    done: Done,
    bound: u32,
) -> Next {
    let creating = match change {
        Change::Create | Change::Remove { known: false } => true,
        Change::Replace | Change::Edit { .. } | Change::Remove { known: true } => false,
    };
    let outcome = match done {
        Done::Stored { version } => {
            knowledge.record(place, version);
            match change {
                Change::Remove { .. } => unreachable!("removal returns Removed"),
                Change::Create => Outcome::Written { created: true },
                Change::Replace => Outcome::Written { created: false },
                Change::Edit { replaced } => Outcome::Edited { replaced },
            }
        }
        Done::Removed => {
            assert!(matches_remove(change), "only a removal returns Removed");
            knowledge.forget(&place);
            Outcome::Removed
        }
        Done::NotEmpty => Outcome::NotEmpty,
        // Something was there that the LLM has not read, even if it is gone
        // by the time io looked for its version.
        Done::Conflict { .. } if creating => Outcome::NotRead,
        Done::Conflict { now: Some(_) } => Outcome::Stale,
        Done::Conflict { now: None } => {
            knowledge.forget(&place);
            Outcome::Stale
        }
        // A directory on the way is missing, or the file replaced is gone.
        Done::Missing => {
            knowledge.forget(&place);
            Outcome::NotFound { path: authority::resolved(checkout, &place) }
        }
        Done::NotFile => Outcome::NotFile,
        Done::Linked => Outcome::Linked,
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(checkout, &place) },
        Done::TooLarge { size } => Outcome::TooLarge { what: crate::SizePart::File, size, bound },
        Done::Escapes => Outcome::Outside { path: authority::resolved(checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Loaded { .. } | Done::Listed { .. } | Done::Exited { .. } | Done::Found { .. } => {
            unreachable!("io ends a store with a store's terminal")
        }
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

fn matches_remove(change: Change) -> bool {
    match change {
        Change::Remove { .. } => true,
        Change::Create | Change::Replace | Change::Edit { .. } => false,
    }
}

/// Running, ended: answer with how the command ended and what it wrote.
fn exited(
    checkout: &Checkout,
    reply_to: ReplyTo,
    place: Place,
    timeout: crate::TimeoutClamp,
    output: crate::BytesClamp,
    done: Done,
) -> Next {
    let outcome = match done {
        Done::Exited { exit, head, tail, dropped } => Outcome::Exited { exit, head, tail, dropped, timeout, output },
        // The working directory.
        Done::Missing => Outcome::NotFound { path: authority::resolved(checkout, &place) },
        Done::NotDirectory => Outcome::NotDirectory { path: authority::resolved(checkout, &place) },
        Done::Escapes => Outcome::Outside { path: authority::resolved(checkout, &place) },
        Done::Failed { fault } => Outcome::Failed { fault },
        Done::TimedOut => Outcome::TimedOut,
        Done::Held { .. } | Done::Job { .. } => unreachable!("the tools issue no yield or job act yet"),
        Done::Cancelled => Outcome::Cancelled,
        Done::Loaded { .. }
        | Done::Listed { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::NotFile
        | Done::Linked
        | Done::TooLarge { .. }
        | Done::Found { .. } => unreachable!("io ends a spawn with a spawn's terminal"),
    };
    Next::Answer { reply_to, outcome, omitted_bytes: 0 }
}

/// A held IO owner's kit, retained after its shell call has answered.
pub(crate) fn kit(flight: &Flight) -> Id<Kit> {
    flight.kit
}

/// The slots the flight slab needs under `limits`, or `None` past a `u32`.
pub(crate) fn slots(limits: &Limits) -> Option<u32> {
    limits.kits.checked_mul(limits.calls.checked_add(limits.jobs)?)?.checked_mul(2)
}

/// What a running flight holds beyond its slot, or `None` past a `u64`: at most
/// a place, and an edit's snippets.
pub(crate) fn held(limits: &Limits) -> Option<u64> {
    let cells = u64::try_from(size_of::<Replacement>()).ok()?.checked_mul(u64::from(limits.tool_payload))?;
    u64::from(limits.tool_payload).checked_mul(2)?.checked_add(cells)?.checked_add(u64::from(limits.path_bytes))
}
