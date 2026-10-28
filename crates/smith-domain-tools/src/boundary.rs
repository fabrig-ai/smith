//! The records that cross the boundary with the tools' parent, the session
//! child domain (programming-model.md, section 4.5), which hands the tools its LLM's calls and routes the
//! file operations they ask for out to io and the terminal events back. The
//! tools define them; the session depends on the tools.
//! Contracts: domain/tools.md, sections 3–6.
//! This module retains no runtime state and knows no provider schema, file
//! contents or process state; [`Event`] and [`Request`] carry the typed calls
//! and terminal rights of the tools domain.
//!
//! Three shapes cross it:
//!
//! - A kit opens and closes: an [`Event::Open`] is answered by exactly one
//!   [`Request::Opened`] or [`Request::Refused`], and an [`Event::Close`] by
//!   exactly one [`Request::Closed`], once every call the kit was running has
//!   been answered. The session names its kit by the token `Opened` gives it,
//!   and the tools echo the session's own token.
//! - Calls in, answers out: each [`Event::Call`] is answered by exactly one
//!   [`Request::Answer`], which consumes its `ReplyTo`.
//! - Operations out, one terminal event in: each [`Request::Io`] is ended by
//!   exactly one [`Event::Done`], after a [`Request::CancelIo`] too. Its
//!   `owner` is the tools' token, echoed on the terminal. A spawn answered
//!   `Held` retains that owner; each read or stop returns one `Done::Job`, and
//!   the binding ends when that answer reports the command's end.
//!
//! The tools arm no timers. A call comes with its deadline, which bounds every
//! operation it asks for; io runs the race (programming-model.md, section 5.3) and reports a lost one as
//! [`Done::TimedOut`].

use alloc::boxed::Box;

use skein_lib::{Duration, ReplyTo, Time, Token};

use crate::authority::Authority;
use crate::call::{Call, Depth, Entry, Exit, Fault, Hit, Outcome};
use crate::path::Place;

/// session -> tools
#[derive(PartialEq, Eq, Debug)]
pub enum Event {
    /// The program bridge supplies a read/own-write version; absence forgets that path.
    NativeVersion { kit: Token, at: Place, version: Option<Version> },
    /// Open a kit for the session `session`, with `authority`.
    Open {
        session: Token,
        /// Highest job number recorded by the conversation, zero when fresh.
        jobs: u32,
        /// Explicit roots, grants and working directory admitted by the kit.
        authority: Authority,
    },
    /// Run `call` with the kit `kit`, and answer it by `deadline`.
    Call {
        /// Tools-issued kit identity, valid until its close terminal.
        kit: Token,
        /// Single-use right to answer this call; exactly one terminal consumes it.
        reply_to: ReplyTo,
        /// Content-free provider-parent observation context; never affects admission.
        observation: Option<crate::CallInfo>,
        /// Parent-issued tool-call token, echoed on the single outcome terminal.
        call: Call,
        deadline: Time,
    },
    /// Close the kit `kit`: cancel what it is running and, once every call
    /// has been answered, end it. No call follows.
    Close {
        /// Tools-issued kit identity, valid until its close terminal.
        kit: Token,
    },
    /// Cancel this kit's held jobs; `JobsCancelled` follows their settlement.
    CancelJobs { kit: Token },
    /// End this call's wait early; its existing answer right remains owed.
    EndWait { kit: Token, call: Token },
    /// Terminal for `Io`.
    Done { owner: Token, done: Done },
}

/// tools -> session
#[derive(PartialEq, Eq, Debug)]
pub enum Request {
    /// The answer to an `Open`: the kit is `kit` from now on.
    Opened {
        session: Token,
        /// Tools-issued kit identity, valid until its close terminal.
        kit: Token,
    },
    /// The answer to an `Open`: no kit was opened.
    Refused {
        session: Token,
        /// Typed admission refusal; no work runs for a refused entrance.
        refusal: Refusal,
    },
    /// The answer to a `Call`: exactly one per call.
    Answer {
        /// Single-use reply right returned to the layer that issued it.
        to: ReplyTo,
        /// Declared or accepted result, validated against the host's contract.
        outcome: Outcome,
    },
    /// A call refused for its job number or the session's job bound; consumes its reply right.
    Invalid { to: ReplyTo, problem: JobProblem },
    /// The answer to a `Close`: the kit has ended.
    Closed { session: Token },
    /// Every job cancelled by the session's request has settled.
    JobsCancelled { session: Token },
    /// Ask io for `op`, giving up at `deadline`.
    Io {
        owner: Token,
        /// Typed IO operation carrying only the authority and bounds required for it.
        op: Op,
        deadline: Time,
    },
    /// Abandon the `Io` in flight for `owner`. Its terminal event still comes:
    /// `Cancelled`, or whichever outcome won the race.
    CancelIo { owner: Token },
    /// Act on a held command; reads and stops end in one `Event::Done`.
    Job { owner: Token, act: JobAct },
}

/// A job call's invalid input, reported by the kit to its session.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum JobProblem {
    /// A yielding shell call would exceed the session's held-job bound.
    Full { jobs: u32 },
    /// No running answer named this number, or its end was already answered.
    Unknown { job: crate::JobNumber },
}

/// One act on the command held under its original spawn token.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JobAct {
    /// Read unread output, answering at its end or once the wait passes.
    Read { head: u32, tail: u32, wait: Duration },
    /// End an active read's wait now; owes no terminal of its own.
    Hasten,
    /// Stop its group, then answer its end and unread output.
    Stop { head: u32, tail: u32 },
}

/// Why a kit was not opened.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Refusal {
    /// Every kit slot is taken.
    Busy,
    /// The authority does not fit the limits.
    Invalid,
}

/// A file operation, asked of io. io resolves every place beneath its root
/// and refuses one that a symbolic link leads out of (`Escapes`). A store
/// follows no link at all.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Op {
    /// Read the regular file at `at`, following symbolic links, if it holds
    /// at most `max` bytes. Ends in `Loaded`, `Missing`, `NotFile`,
    /// `NotDirectory`, `TooLarge` or a common terminal.
    Load {
        at: Place,
        /// Inclusive maximum count allowed by this contract.
        max: u32,
    },
    /// List the directory at `at`, following symbolic links: at most `max`
    /// matching files and their ancestors within `depth` and `max_bytes`,
    /// the first in relative path order. Glob filters files, never ancestors. Ends in
    /// `Listed`, `Missing`, `NotDirectory` or a common terminal.
    List {
        at: Place,
        depth: Depth,
        glob: Option<Box<[u8]>>,
        /// Inclusive maximum count allowed by this contract.
        max: u32,

        /// Maximum owned listing bytes: entry array cells plus every name's bytes.
        /// The terminal obeys this cap even when it wins cancellation.
        /// An unfit first entry returns an empty prefix; `Listed.more` counts
        /// all omitted entries, including those excluded by the byte cap.
        max_bytes: u64,
    },
    /// Make the regular file at `at` hold `content`, if it is as `expect`
    /// says. No part of `at` may be a symbolic link (`Linked`): io resolves
    /// it beneath the root following none (`RESOLVE_NO_SYMLINKS`), so a store
    /// lands in the repository its place names and in no other mounted
    /// beneath it. The content is written beside the file and renamed into
    /// place, so that the file is replaced whole or not at all.
    ///
    /// To replace (`Is`), io compares the file's version with `expect` just
    /// before the rename, and makes no directory: a missing one on the way
    /// means the file is missing, a conflict. A change made before the
    /// comparison is caught; one made by a writer outside the agent between
    /// the comparison and the rename is not, for the two are not one atomic
    /// step. (io runs one store at a time per root, so the agent's own kits
    /// do not race each other there.) To create (`Absent`), io makes the
    /// missing directories on the way, then renames the file into place
    /// without replacing one: a file that appeared meanwhile is a conflict,
    /// and the directories stay.
    ///
    /// Ends in `Stored`, `Conflict`, `Linked`, `NotFile`, `NotDirectory` or a
    /// common terminal.
    Store {
        at: Place,
        /// Owned content blocks or file bytes, bounded by the enclosing record's byte cap.
        content: Box<[u8]>,
        /// Expected file version or absence checked before the atomic replacement.
        expect: Expect,
    },
    /// Remove one current regular file or empty directory, following no links; ends in
    /// `Removed`, `Conflict`, `NotEmpty`, `Missing`, `NotFile`, `Linked` or a common terminal.
    Remove { at: Place, expect: Expect },
    /// Run `command` with `sh` in `cwd`, leading its own process group. The
    /// machine signals the whole group on a deadline or cancel, but the command
    /// is not contained: it has the agent user's permissions and filesystem.
    /// The configured environment is supplied by the machine. Kept output is
    /// the first `head` bytes, the last `tail`, and bytes dropped between them.
    /// Ends in `Exited`, `Missing`, `NotDirectory`, `Escapes`, `Failed` or
    /// `Cancelled`, once the leader is reaped and both output pipes close.
    Spawn {
        /// IO-confined working directory supplied with explicit root authority.
        cwd: Place,
        /// Owned shell command interpreted by the process-group layer.
        command: Box<[u8]>,
        /// First output bytes retained under the caller's cap.
        head: u32,
        /// Last output bytes retained under the caller's cap.
        tail: u32,
        /// Yield after this span; `None` waits for the command's end.
        after: Option<Duration>,
    },
    /// Search the files at and beneath `at` for lines matching `pattern`, in
    /// those whose names match `glob` if given, with rg leading its own
    /// process group, invoked as
    ///
    /// ```text
    /// rg --no-config --json --regexp=PATTERN [--glob=GLOB] -- PATH
    /// ```
    ///
    /// so that neither the pattern nor the glob can be read as an option, and
    /// no configuration file is: with an empty environment, and every root
    /// read-only. rg follows no link beneath `at`, and skips what it skips by
    /// default (hidden and ignored files, binary ones).
    ///
    /// The protocol layer decodes rg's output into hits, in path order: at
    /// most `hits` of them, with at most `bytes` between them of their paths
    /// and their text, the last one's text cut to fit; the hits beyond are
    /// counted. rg's exits: 0 is `Found`; 1, nothing found, is `Found` with no
    /// hits; 2, an error, is `Found` if it found lines all the same (an
    /// unreadable file among many hides nothing found in the others), and
    /// otherwise `Exited`, with at most `bytes` of what it wrote to standard
    /// error in `head` (as for a pattern it cannot read). At the deadline io
    /// kills rg and ends the search as `Found`, timed out, with what it had
    /// found. Ends in `Found`, `Exited`, `Missing`, `NotDirectory`, `Escapes`,
    /// or a common terminal.
    Search {
        at: Place,
        /// Search expression passed as data to the process-group search.
        pattern: Box<[u8]>,
        /// Optional search filename filter, passed as data rather than an option.
        glob: Option<Box<[u8]>>,
        /// Bounded search results, or the caller's maximum retained count.
        hits: u32,
        /// Owned payload bytes charged against the enclosing session limit.
        bytes: u32,
    },
}

/// What a `Store` expects to replace.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Expect {
    /// Nothing: the file is created.
    Absent,
    /// The file at `version`.
    Is {
        /// IO-issued file version or record encoding version, as named by the enclosing type.
        version: Version,
    },
}

/// io's terminal for an operation. Each operation ends in those its
/// documentation names, or in one of the common terminals: `Escapes`,
/// `Failed`, `TimedOut` and `Cancelled`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Done {
    /// Load: the file holds `content`, at `version`.
    Loaded {
        /// Owned content blocks or file bytes, bounded by the enclosing record's byte cap.
        content: Box<[u8]>,
        /// IO-issued file version or record encoding version, as named by the enclosing type.
        version: Version,
    },
    /// List: the selected tree's first `entries` in name order, and how many
    /// `more` it has.
    Listed {
        /// First bounded directory entries in name order.
        entries: Box<[Entry]>,
        /// Problems or matches omitted after the retained bound.
        more: u64,
    },
    /// Store: the file holds the content, at `version`.
    Stored {
        /// IO-issued file version or record encoding version, as named by the enclosing type.
        version: Version,
    },
    /// Remove: one file or empty directory was removed.
    Removed,
    /// Remove: the directory contains entries; nothing was removed.
    NotEmpty,
    /// Store or Remove: the file is not as expected. It is at `now`, or absent.
    Conflict {
        /// Current optional file version observed by IO after an optimistic-write conflict.
        now: Option<Version>,
    },
    /// Spawn: the command ended so, with the output captured. Search: rg
    /// failed so.
    Exited {
        /// Terminal process classification; only a zero exit code passes checks.
        exit: Exit,
        /// First output bytes retained under the caller's cap.
        head: Box<[u8]>,
        /// Last output bytes retained under the caller's cap.
        tail: Box<[u8]>,
        /// Bytes omitted between the retained process output head and tail.
        dropped: u64,
    },
    /// Spawn: still running at its yield, now held under its owner token.
    Held { head: Box<[u8]>, tail: Box<[u8]>, dropped: u64 },
    /// A job act's answer, draining unread output and reporting its end.
    Job { head: Box<[u8]>, tail: Box<[u8]>, dropped: u64, end: crate::JobEnd },
    /// Search: the lines found, and how many more matched; `timed_out` if
    /// the deadline cut it short, and these are what it had found.
    Found {
        /// Bounded search results, or the caller's maximum retained count.
        hits: Box<[Hit]>,
        /// Problems or matches omitted after the retained bound.
        more: u64,
        /// Whether IO ended the process or search at its deadline.
        timed_out: bool,
    },
    /// Load, List, Store: nothing is at the place.
    Missing,
    /// Load, Store: what is at the place is not a regular file.
    NotFile,
    /// Store: a part of the place is a symbolic link, which a store does not
    /// follow.
    Linked,
    /// List: what is at the place is not a directory. Any operation: a
    /// directory on the way to the place is not one.
    NotDirectory,
    /// Load: the file is `size` bytes, more than asked for; Store: the content is.
    TooLarge {
        /// Observed file size which exceeds the receiving byte cap.
        size: u64,
    },
    /// A symbolic link on the way leads out of the root.
    Escapes,
    /// io failed with `fault`.
    Failed {
        /// Typed IO failure; this boundary carries no OS error text.
        fault: Fault,
    },
    /// The deadline passed first.
    TimedOut,
    /// The cancel won the race.
    Cancelled,
}

/// Which version of a file io found, or made by storing it. io makes versions
/// so that changed content has a new one; the domain compares versions and
/// never looks inside. The machine uses a SHA-256 content digest, represented
/// as four big-endian words (protocol/agent.md, section 2).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Version([u64; 4]);

impl Version {
    /// Made below the domain, from what io found.
    #[must_use]
    pub const fn new(raw: [u64; 4]) -> Version {
        Version(raw)
    }

    /// What it was made from, for the layer that made it.
    #[must_use]
    pub const fn raw(self) -> [u64; 4] {
        self.0
    }
}
