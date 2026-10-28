//! What the tools tell whoever watches the agent (domain/tools.md, section 4):
//! a fact for each thing that happened, content-free (tokens, counts and
//! classifications, never a path, a file or what a command wrote), in a
//! bounded queue the parent drains at its own pace.
//!
//! Facts are a separate step output. The parent reserves [`crate::max_facts`]
//! slots before each entrance, holding work while its drain is behind; every
//! observation is preserved and none changes a tool's decision. Applied tool
//! caps and omitted suffix bytes are retained at the terminal, before rendering
//! (protocol/events.md, section 3.6); a later render cut is never known here.

use skein_lib::{Queue, Time, Token};

use crate::boundary::Refusal;
use crate::call::{Effect, Exit, Fault, Outcome, Tool};

/// An observation stamped with the injected time of its emitting step.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Fact {
    /// Emission time, preserved when parents drain this observation later.
    pub at: Time,
    /// The content-free observation.
    pub kind: FactKind,
    /// Parent call context when the caller has a provider observation.
    pub call: Option<CallInfo>,
}

/// Something that happened to a kit, emitted by the tools for its parent.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum FactKind {
    /// The kit opened.
    Opened { session: Token },
    /// No kit was opened.
    Refused {
        session: Token,
        /// Typed admission refusal; no work runs for a refused entrance.
        refusal: Refusal,
    },
    /// A call passed the entrance, and runs.
    Started {
        session: Token,
        /// Content-free classification of the checkout tool called.
        tool: Tool,
    },
    /// A call was answered, so, with an outcome carrying `bytes` of payload
    /// (what was read, listed, found, or kept of a command's output). A call
    /// refused at the entrance is answered without having started.
    Answered {
        session: Token,
        /// Content-free classification of the checkout tool called.
        tool: Tool,
        /// Content-free classification of the checkout call terminal.
        verdict: Verdict,
        /// Owned payload bytes charged against the enclosing session limit.
        bytes: u64,
        /// Number named by a running shell or a valid job answer.
        job: Option<crate::JobNumber>,
        /// Bytes omitted by the tool's own read window or command capture, before rendering.
        omitted_bytes: u64,
        /// Whether the tool lowered a requested byte budget or deadline to its ceiling.
        clamped: bool,
    },
    /// The kit is closing, with `running` calls to settle.
    Closing {
        session: Token,
        /// Number of pending calls whose terminals the closing kit still awaits.
        running: u32,
    },
    /// The kit closed.
    Closed { session: Token },
    /// A job's end went unread; the kit reports it before closing.
    JobEnded {
        session: Token,
        job: crate::JobNumber,
        /// Parent identity of the shell call, retained after its answer.
        owner: Token,
        end: crate::JobEnd,
        unread_bytes: u64,
        started: Time,
    },
}

/// How a call ended, without what it said: an outcome's kind.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Verdict {
    /// Read-only call or successful read fact, according to the enclosing classification.
    Read,
    /// The directory listing completed.
    Listed,
    /// The search completed with bounded matches.
    Found,
    /// The version-aware write completed.
    Written {
        /// Whether this write created a previously absent file.
        created: bool,
    },
    /// The version-aware edit completed.
    Edited {
        /// Number of edit occurrences successfully replaced.
        replaced: u32,
    },
    /// The process ended with its classified exit.
    Exited {
        /// Terminal process classification; only a zero exit code passes checks.
        exit: Exit,
    },
    /// A shell or job call left its command running.
    Running,
    /// A job call stopped its command's group.
    Stopped,
    /// The kit has no authority for this tool family.
    NotGranted,
    /// The path is outside the granted repository roots.
    Outside,
    /// The resolved repository forbids writes.
    ReadOnly,
    /// The resolved path exceeds the kit path cap.
    TooLong,
    /// No entry exists at the requested path.
    NotFound,
    /// The entry is not a regular file.
    NotFile,
    /// A current file or empty directory was removed.
    Removed,
    /// A removal refused a directory with entries.
    NotEmpty,
    /// A write path traverses a symbolic link and is refused.
    Linked,
    /// A write would touch protected git metadata.
    Protected,
    /// A path prefix or requested listing is not a directory.
    NotDirectory,
    /// A configured ownership, count or encoded-byte cap would be exceeded.
    TooLarge,
    /// A write or edit lacks a previously observed file version.
    NotRead,
    /// The file version changed since it was read.
    Stale,
    /// The edit snippet has no occurrence.
    NoMatch {
        /// First failed replacement, counting from one.
        edit: u32,
    },
    /// A single-occurrence edit found more than one match.
    Ambiguous {
        /// First failed replacement, counting from one.
        edit: u32,
        /// Number of matched occurrences which made a single-occurrence edit ambiguous.
        count: u32,
    },
    /// The edit or write would change no file bytes.
    Unchanged {
        /// First failed replacement, counting from one.
        edit: u32,
    },
    /// One failed terminal with bounded diagnostics.
    Failed {
        /// Typed IO failure; this boundary carries no OS error text.
        fault: Fault,
    },
    /// The injected operation deadline won the race.
    TimedOut,
    /// The caller cancelled and the terminal settled.
    Cancelled,
    /// No capacity is currently available; a later call may fit.
    Busy,
    /// The path or process input contains a forbidden NUL byte.
    NulByte,
}

/// Scalar observations retained by the flight until its tool terminal.
#[derive(Clone, Copy)]
pub(crate) struct Observation {
    pub(crate) omitted_bytes: u64,
    pub(crate) clamped: bool,
}

/// Whether the effective byte cap was lowered from the caller's request.
pub(crate) fn bytes_clamped(clamp: crate::BytesClamp) -> bool {
    match clamp {
        crate::BytesClamp::Within => false,
        crate::BytesClamp::Clamped { .. } => true,
    }
}

/// Whether the effective deadline was lowered from the caller's request.
pub(crate) fn timeout_clamped(clamp: crate::TimeoutClamp) -> bool {
    match clamp {
        crate::TimeoutClamp::Within => false,
        crate::TimeoutClamp::Clamped { .. } => true,
    }
}

/// The fact of `outcome` answering a call of `tool` from `session`.
pub(crate) fn answered(session: Token, tool: Tool, outcome: &Outcome, observation: Observation) -> FactKind {
    let (verdict, bytes) = match outcome {
        Outcome::Read { content, .. } => (Verdict::Read, len(content)),
        Outcome::Listed { entries, .. } => {
            let mut bytes: u64 = 0;
            for entry in entries {
                bytes = bytes.checked_add(len(entry.name.as_ref())).expect("bounded listing bytes fit u64");
            }
            (Verdict::Listed, bytes)
        }
        Outcome::Found { hits, .. } => {
            let mut bytes: u64 = 0;
            for hit in hits {
                bytes = bytes.checked_add(len(&hit.path)).expect("bounded search paths fit u64");
                bytes = bytes.checked_add(len(&hit.text)).expect("bounded search bytes fit u64");
            }
            (Verdict::Found, bytes)
        }
        Outcome::Written { created } => (Verdict::Written { created: *created }, 0),
        Outcome::Edited { replaced } => (Verdict::Edited { replaced: *replaced }, 0),
        Outcome::Exited { exit, head, tail, dropped: _, timeout: _, output: _ } => (
            Verdict::Exited { exit: *exit },
            len(head).checked_add(len(tail)).expect("bounded command output fits u64"),
        ),
        Outcome::Running { head, tail, .. } => {
            (Verdict::Running, len(head).checked_add(len(tail)).expect("bounded job output"))
        }
        Outcome::Job { head, tail, end, .. } => {
            let verdict = match end {
                crate::JobEnd::Running => Verdict::Running,
                crate::JobEnd::Ended(exit) => Verdict::Exited { exit: *exit },
                crate::JobEnd::Stopped => Verdict::Stopped,
                crate::JobEnd::Cancelled => Verdict::Cancelled,
            };
            (verdict, len(head).checked_add(len(tail)).expect("bounded job output"))
        }
        Outcome::NotGranted => (Verdict::NotGranted, 0),
        Outcome::Outside { .. } => (Verdict::Outside, 0),
        Outcome::ReadOnly { .. } => (Verdict::ReadOnly, 0),
        Outcome::TooLong => (Verdict::TooLong, 0),
        Outcome::NotFound { .. } => (Verdict::NotFound, 0),
        Outcome::Removed => (Verdict::Removed, 0),
        Outcome::NotEmpty => (Verdict::NotEmpty, 0),
        Outcome::NotFile => (Verdict::NotFile, 0),
        Outcome::Linked => (Verdict::Linked, 0),
        Outcome::Protected => (Verdict::Protected, 0),
        Outcome::NotDirectory { .. } => (Verdict::NotDirectory, 0),
        Outcome::TooLarge { .. } => (Verdict::TooLarge, 0),
        Outcome::NotRead => (Verdict::NotRead, 0),
        Outcome::Stale => (Verdict::Stale, 0),
        Outcome::EditFailed { edit, why } => {
            let verdict = match why {
                crate::Mismatch::NoMatch => Verdict::NoMatch { edit: *edit },
                crate::Mismatch::Ambiguous { count, .. } => Verdict::Ambiguous { edit: *edit, count: *count },
                crate::Mismatch::Unchanged => Verdict::Unchanged { edit: *edit },
            };
            (verdict, 0)
        }
        Outcome::Failed { fault } => (Verdict::Failed { fault: *fault }, 0),
        Outcome::TimedOut => (Verdict::TimedOut, 0),
        Outcome::Cancelled => (Verdict::Cancelled, 0),
        Outcome::Busy => (Verdict::Busy, 0),
        Outcome::NulByte => (Verdict::NulByte, 0),
    };
    let job = outcome_job(outcome);
    let observed = observed(outcome, observation);
    FactKind::Answered {
        session,
        tool,
        verdict,
        bytes,
        job,
        omitted_bytes: observed.omitted_bytes,
        clamped: observed.clamped,
    }
}

fn outcome_job(outcome: &Outcome) -> Option<crate::JobNumber> {
    match outcome {
        Outcome::Running { job, .. } | Outcome::Job { job, .. } => Some(*job),
        Outcome::Exited { .. }
        | Outcome::Read { .. }
        | Outcome::Listed { .. }
        | Outcome::Found { .. }
        | Outcome::Removed
        | Outcome::NotEmpty
        | Outcome::Written { .. }
        | Outcome::Edited { .. }
        | Outcome::NotGranted
        | Outcome::Outside { .. }
        | Outcome::ReadOnly { .. }
        | Outcome::TooLong
        | Outcome::NotFound { .. }
        | Outcome::NotFile
        | Outcome::Linked
        | Outcome::Protected
        | Outcome::NotDirectory { .. }
        | Outcome::TooLarge { .. }
        | Outcome::NotRead
        | Outcome::Stale
        | Outcome::EditFailed { .. }
        | Outcome::Failed { .. }
        | Outcome::TimedOut
        | Outcome::Cancelled
        | Outcome::Busy
        | Outcome::NulByte => None,
    }
}

fn observed(outcome: &Outcome, observation: Observation) -> Observation {
    match outcome {
        Outcome::Exited { dropped, timeout, output, .. } => Observation {
            omitted_bytes: *dropped,
            clamped: observation.clamped || timeout_clamped(*timeout) || bytes_clamped(*output),
        },
        Outcome::Running { dropped, timeout, after, output, .. } => Observation {
            omitted_bytes: *dropped,
            clamped: observation.clamped
                || timeout_clamped(*timeout)
                || timeout_clamped(*after)
                || bytes_clamped(*output),
        },
        Outcome::Job { dropped, wait: after, output, .. } => Observation {
            omitted_bytes: *dropped,
            clamped: observation.clamped || timeout_clamped(*after) || bytes_clamped(*output),
        },
        Outcome::Read { window, .. } => Observation {
            omitted_bytes: observation.omitted_bytes,
            clamped: observation.clamped || bytes_clamped(*window),
        },
        Outcome::Listed { .. }
        | Outcome::Found { .. }
        | Outcome::Removed
        | Outcome::NotEmpty
        | Outcome::Written { .. }
        | Outcome::Edited { .. }
        | Outcome::NotGranted
        | Outcome::Outside { .. }
        | Outcome::ReadOnly { .. }
        | Outcome::TooLong
        | Outcome::NotFound { .. }
        | Outcome::NotFile
        | Outcome::Linked
        | Outcome::Protected
        | Outcome::NotDirectory { .. }
        | Outcome::TooLarge { .. }
        | Outcome::NotRead
        | Outcome::Stale
        | Outcome::EditFailed { .. }
        | Outcome::Failed { .. }
        | Outcome::TimedOut
        | Outcome::Cancelled
        | Outcome::Busy
        | Outcome::NulByte => observation,
    }
}

fn len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).expect("bounded tool bytes fit u64")
}

/// Reserved step observations not yet drained by the parent.
#[derive(Debug)]
pub(crate) struct Facts {
    queue: Queue<Fact>,
    now: Time,
}

impl Facts {
    pub(crate) fn with_capacity(capacity: u32) -> Facts {
        Facts { queue: Queue::with_capacity(capacity), now: Time::ZERO }
    }

    pub(crate) fn begin(&mut self, now: Time) {
        self.now = now;
    }

    /// Keeps one fact in the room the parent reserved for this entry point.
    pub(crate) fn push(&mut self, kind: FactKind) {
        let fact = Fact { at: self.now, kind, call: None };
        self.queue.push(fact);
    }

    pub(crate) fn push_call(&mut self, kind: FactKind, call: Option<CallInfo>) {
        self.queue.push(Fact { at: self.now, kind, call });
    }

    pub(crate) fn push_at(&mut self, at: Time, kind: FactKind) {
        self.queue.push(Fact { at, kind, call: None });
    }

    pub(crate) fn pop(&mut self) -> Option<Fact> {
        self.queue.pop()
    }

    pub(crate) fn room(&self) -> u32 {
        self.queue.room()
    }
}

/// Provider-parent identity and measurements, carried without owning content.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct CallInfo {
    /// The parent's call token, distinct from the affine answer right.
    pub owner: Token,
    pub effect: Effect,
    pub deadline: Time,
    /// Exact raw provider argument bytes, measured before decoding.
    pub input_bytes: u64,
}
