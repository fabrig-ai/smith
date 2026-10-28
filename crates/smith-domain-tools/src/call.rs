//! The calls a session's LLM makes and what comes of them: the vocabulary the
//! session and the protocol layer share with the tools.
//!
//! The protocol layer owns each tool's schema, decodes the JSON the LLM wrote
//! into a [`Call`], valid by construction, and renders an [`Outcome`] as the
//! text the LLM reads. A malformed call never gets here: the session answers
//! it. What an outcome holds, such as how much of a file comes back, is the
//! tools' decision.
//!
//! Contract: domain/tools.md, sections 3 and 4; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;

use skein_lib::Duration;

use crate::path::{Part, Path, Resolved};

/// A call the LLM made to one of the tools.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Call {
    /// Read whole lines from the one-based offset within the chosen byte window; ends in Read or a file terminal.
    Read { path: Path, offset: u32, window: Budget },
    /// List matching files and their directories beneath `path`, within the requested depth.
    List { path: Path, depth: Depth, glob: Option<Box<[u8]>> },
    /// Search the files at and beneath `path` for `pattern`, a regular
    /// expression as rg reads it, in those whose names match `glob` if given.
    Search {
        path: Path,
        /// Search expression passed as data to the process-group search.
        pattern: Box<[u8]>,
        /// Optional search filename filter, passed as data rather than an option.
        glob: Option<Box<[u8]>>,
    },
    /// Make the file at `path` hold `content`, creating it if there is none.
    Write {
        path: Path,
        /// Owned content blocks or file bytes, bounded by the enclosing record's byte cap.
        content: Box<[u8]>,
    },
    /// Apply replacements in order, store once over the loaded version, or apply none.
    /// No earlier read is required; ends in `Edited`, `EditFailed` or a file terminal.
    Edit { path: Path, edits: Box<[Replacement]> },
    /// Remove a current file or empty directory for the LLM; ends in Removed or a file refusal.
    Remove { path: Path },
    /// Run the session's command with selected time and output budgets; ends in Exited or a process terminal.
    Shell {
        /// Owned shell command interpreted only by the process-group layer.
        command: Box<[u8]>,
        /// Caller-selected deadline, within the configured tool ceiling.
        timeout: Timeout,
        /// Caller-selected retained output, within the configured capture ceiling.
        output: Budget,
        after: Yield,
    },
    /// Read a session's held command, optionally stopping it before answering.
    Job { job: JobNumber, wait: Wait, output: Budget, stop: bool },
}

/// One ordered replacement sent by the protocol layer with an edit call.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Replacement {
    pub old: Box<[u8]>,
    pub new: Box<[u8]>,
    pub occurrences: Occurrences,
}

/// The matching rule sent with one replacement; the tools enforce it on the current bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Occurrences {
    /// Exactly one match is required; ends in a replacement or an ambiguity refusal.
    Once,
    /// Every non-overlapping match is replaced; at least one must exist.
    All,
}

/// The depth a listing caller asks for, counting direct children as one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Depth {
    /// List the whole matching tree.
    Whole,
    /// List at most this many levels; the caller supplies at least one.
    Levels(u32),
}

/// A byte budget the caller asks for, or the configured default.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Budget {
    /// Use the configured default.
    Default,
    /// Apply the requested byte budget within its ceiling.
    Asked(u32),
}

/// A command's requested duration, or the deployment's default.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Timeout {
    /// Use the deployment's command deadline.
    Default,
    /// Apply this duration within the tool deadline.
    Asked(Duration),
}

/// How long a shell call waits before answering with a running job.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Yield {
    /// Wait until the command ends.
    Never,
    /// Answer after this duration, clamped to the configured job wait.
    Asked(Duration),
}

/// How long a job call waits for its held command to end.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Wait {
    /// Use the configured job wait.
    Default,
    /// Apply this wait within the configured ceiling.
    Asked(Duration),
}

/// How the domain applied a requested command deadline, returned with its terminal.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum TimeoutClamp {
    /// The duration fits the tool deadline.
    Within,
    /// The duration was reduced to the tool deadline.
    Clamped { asked: Duration, ceiling: Duration },
}

/// How the domain applied a requested byte budget, returned with the outcome.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum BytesClamp {
    /// The request fits the configured ceiling.
    Within,
    /// The requested bytes were reduced to the ceiling.
    Clamped { asked: u32, ceiling: u32 },
}

/// Where the session can begin its next read window.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Next {
    /// Continue at this one-based line.
    Line(u32),
    /// This window reached the file's end.
    End,
}

/// Whether the read's last line fit its byte window.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Cut {
    /// Every returned line is whole.
    Whole,
    /// The first line was cut at the window.
    Line,
}

/// Why the tools refused one replacement without storing any part of the edit.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Mismatch {
    /// The old text is absent, including an empty snippet.
    NoMatch,
    /// A unique replacement matched count times; retained lines count from one.
    Ambiguous { count: u32, lines: Box<[u32]> },
    /// The old and new bytes are identical.
    Unchanged,
}

/// Which tool a call is for, without its arguments.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Tool {
    /// Read-only call or successful read fact, according to the enclosing classification.
    Read,
    /// Directory-listing tool.
    List,
    /// Contained-search tool.
    Search,
    /// A call that may modify state and must run alone.
    Write,
    /// Version-aware file-edit tool.
    Edit,
    /// Version-aware file or empty-directory removal tool.
    Remove,
    /// Contained shell-command tool.
    Shell,
    /// Read or stop a command the session left running.
    Job,
}

/// The tool `call` is for.
#[must_use]
pub const fn tool(call: &Call) -> Tool {
    match call {
        Call::Read { .. } => Tool::Read,
        Call::List { .. } => Tool::List,
        Call::Search { .. } => Tool::Search,
        Call::Write { .. } => Tool::Write,
        Call::Edit { .. } => Tool::Edit,
        Call::Remove { .. } => Tool::Remove,
        Call::Shell { .. } => Tool::Shell,
        Call::Job { .. } => Tool::Job,
    }
}

/// What a call does to the checkout. Calls that only read may run side by
/// side; a call that writes runs alone.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Effect {
    /// Read-only call or successful read fact, according to the enclosing classification.
    Read,
    /// A call that may modify state and must run alone.
    Write,
}

/// The material whose size the tools refused; carried by a `TooLarge` terminal.
/// Contract: domain/tools.md, section 4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum SizePart {
    /// A loaded file exceeded the whole-file byte bound.
    File,
    /// A Write's content exceeded the per-piece byte bound.
    WriteContent,
    /// An Edit's old snippets exceeded their aggregate byte bound.
    EditOld,
    /// An Edit's new snippets exceeded their aggregate byte bound.
    EditNew,
    /// An Edit's replacement count exceeded its item bound.
    EditItems,
    /// An Edit's resulting file exceeded the whole-file byte bound.
    EditResult,
    /// An Edit's total matched replacements exceeded its count bound.
    EditReplacements,
    /// A List's glob exceeded its byte bound.
    ListGlob,
}

/// The effect of `call`. A command may do anything, so it writes.
#[must_use]
pub const fn effect(call: &Call) -> Effect {
    match call {
        Call::Read { .. } | Call::List { .. } | Call::Search { .. } | Call::Job { stop: false, .. } => Effect::Read,
        Call::Write { .. }
        | Call::Edit { .. }
        | Call::Remove { .. }
        | Call::Shell { .. }
        | Call::Job { stop: true, .. } => Effect::Write,
    }
}

/// What came of a call: exactly one per call.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    /// The read's lines, next offset, cut and applied window clamp, returned to the session.
    Read {
        content: Box<[u8]>,
        /// First line of this window, counting from one, including an empty window at EOF.
        first: u32,
        lines: u32,
        total: u32,
        next: Next,
        cut: Cut,
        window: BytesClamp,
    },
    /// A listing: the selected tree's first entries in path order, and how many
    /// `more` it has.
    Listed {
        /// First bounded directory entries in name order.
        entries: Box<[Entry]>,
        /// Problems or matches omitted after the retained bound.
        more: u64,
    },
    /// A search: the first lines that matched, in path order, and how many
    /// `more` did; `timed_out` if the search ran out of time, and these are
    /// what it had found by then.
    Found {
        /// Bounded search results, or the caller's maximum retained count.
        hits: Box<[Hit]>,
        /// Problems or matches omitted after the retained bound.
        more: u64,
        /// Whether IO ended the process or search at its deadline.
        timed_out: bool,
    },
    /// The file holds what was written; it was `created` if there was none.
    Written {
        /// Whether this write created a previously absent file.
        created: bool,
    },
    /// The edit `replaced` that many occurrences.
    Edited {
        /// Number of edit occurrences successfully replaced.
        replaced: u32,
    },
    /// The requested file or empty directory was removed for the session.
    Removed,
    /// The removal target contains entries; nothing was removed.
    NotEmpty,
    /// The machine's command terminal, its retained output, omitted bytes and applied clamps, returned to the session.
    Exited {
        /// Terminal process classification; only a zero exit code passes checks.
        exit: Exit,
        /// First output bytes retained under the caller's cap.
        head: Box<[u8]>,
        /// Last output bytes retained under the caller's cap.
        tail: Box<[u8]>,
        /// Bytes omitted between the retained process output head and tail.
        dropped: u64,
        timeout: TimeoutClamp,
        output: BytesClamp,
    },

    /// A shell command still running at its yield, returned to its session.
    Running {
        job: JobNumber,
        head: Box<[u8]>,
        tail: Box<[u8]>,
        dropped: u64,
        timeout: TimeoutClamp,
        output: BytesClamp,
        after: TimeoutClamp,
    },
    /// A job call's unread output and observed end, returned to its session.
    Job {
        job: JobNumber,
        head: Box<[u8]>,
        tail: Box<[u8]>,
        dropped: u64,
        output: BytesClamp,
        wait: TimeoutClamp,
        end: JobEnd,
    },

    /// The kit was not granted the call's family of tools.
    NotGranted,
    /// The path is outside every repository of the checkout, or a symbolic
    /// link on the way to it leads out of its repository.
    Outside { path: Resolved },
    /// The path is in a repository the kit may not write.
    ReadOnly { path: Resolved },
    /// The path is longer than the tools take.
    TooLong,
    /// Nothing is at the path.
    NotFound { path: Resolved },
    /// What is at the path is not a regular file: a directory, a device.
    NotFile,
    /// The path of a write or an edit goes through a symbolic link, which
    /// they do not follow, so that a change lands only in the repository its
    /// path names. Reads follow links that stay inside the path's own
    /// repository.
    Linked,
    /// The path of a write or an edit is in a repository's git directory: it
    /// has `.git`, in any ASCII case, among its names. The tools do not know
    /// git directories under other names (a `.git` file pointing elsewhere, a
    /// separate git directory, hooks configured to live in the tree), so the
    /// worker must not trust a repository's own configuration either.
    Protected,
    /// What is at the path, or a directory on the way to it, is not a
    /// directory.
    NotDirectory { path: Resolved },
    /// The tools refused `what` at its size bound and returned both quantities.
    TooLarge {
        what: SizePart,
        /// Observed bytes, or items for `EditItems` and `EditReplacements`.
        size: u64,
        /// Exceeded bound in the same unit as size.
        bound: u32,
    },
    /// A write names an existing file whose version the LLM has not read.
    NotRead,
    /// A write or edit lost the version it read or matched before its store.
    Stale,
    /// The first failed replacement, counting from one; no edit was stored.
    EditFailed { edit: u32, why: Mismatch },
    /// io failed.
    Failed {
        /// Typed IO failure; this boundary carries no OS error text.
        fault: Fault,
    },
    /// The call did not finish by its deadline.
    TimedOut,
    /// The kit closed while the call was running.
    Cancelled,
    /// The kit has as many calls running as it may.
    Busy,
    /// The command, the pattern or the glob holds a NUL byte, which no
    /// argument to a process can.
    NulByte,
}

/// A line a search found.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Hit {
    /// The file it is in, beneath the path searched: names joined by `/`,
    /// empty if the path searched is the file.
    pub path: Box<[u8]>,
    /// Its number, counting from 1.
    pub line: u32,
    /// The line, without its end, cut where the search's byte limit fell.
    pub text: Box<[u8]>,
}

/// A selected file or its ancestor, relative to the directory listed.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Entry {
    /// A nonempty relative path beneath the listed directory, joined by `/`.
    pub name: Box<[u8]>,
    /// Typed entry classification or byte label required by the enclosing contract.
    pub kind: Kind,
}

/// What an entry is, as io found it, without following symbolic links.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Kind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Link,
    /// A device, a socket, a pipe.
    Other,
}

/// How a command ended.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Exit {
    /// It exited with `code`.
    Code { code: u8 },
    /// A signal killed it.
    Signal {
        /// Signal number which ended the process.
        signal: u8,
    },
    /// It ran past its deadline, and io killed it, with every process it
    /// started.
    TimedOut,
}

/// A job's number in its conversation, assigned from one and never reused.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct JobNumber(pub u32);

/// How a held command stands when the machine answers an act on it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum JobEnd {
    /// Still running; the machine keeps it for another act.
    Running,
    /// Exited, signalled or timed out; the machine releases it.
    Ended(Exit),
    /// Its group was stopped by a job act; the machine releases it.
    Stopped,
    /// Its group was cancelled by its owner; the machine releases it.
    Cancelled,
}

/// Why io failed, as the protocol layer classifies the error.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Fault {
    /// Permission denied.
    Denied,
    /// No space left, or over a quota.
    NoSpace,
    /// Anything else.
    Other,
}

impl Call {
    /// Checked complete ownership of this decoded call: inline cell plus every
    /// Path/Part/Name wrapper and byte payload, content, pattern, glob or command.
    /// The protocol/root reuse this before effects; None is arithmetic overflow.
    #[must_use]
    pub fn owned_bytes(&self) -> Option<u64> {
        let payload = match self {
            Call::Read { path, offset: _, window: _ } | Call::Remove { path } => owned_path(path),
            Call::List { path, depth: _, glob } => {
                let filter = match glob {
                    Some(filter) => owned_len(filter)?,
                    None => 0,
                };
                owned_path(path)?.checked_add(filter)
            }
            Call::Search { path, pattern, glob } => {
                let glob = match glob {
                    Some(glob) => owned_len(glob)?,
                    None => 0,
                };
                owned_path(path)?.checked_add(owned_len(pattern)?)?.checked_add(glob)
            }
            Call::Write { path, content } => owned_path(path)?.checked_add(owned_len(content)?),
            Call::Edit { path, edits } => {
                let mut cost = owned_path(path)?.checked_add(
                    u64::try_from(size_of::<Replacement>()).ok()?.checked_mul(u64::try_from(edits.len()).ok()?)?,
                )?;
                for replacement in edits {
                    cost = cost.checked_add(owned_len(&replacement.old)?)?.checked_add(owned_len(&replacement.new)?)?;
                }
                Some(cost)
            }
            Call::Shell { command, .. } => owned_len(command),
            Call::Job { .. } => Some(0),
        };
        u64::try_from(size_of::<Self>()).ok()?.checked_add(payload?)
    }
}

fn owned_path(path: &Path) -> Option<u64> {
    let parts = u64::try_from(size_of::<Part>()).ok()?.checked_mul(u64::try_from(path.parts.len()).ok()?)?;
    let mut cost = parts;
    for part in &path.parts {
        match part {
            Part::Name { name } => cost = cost.checked_add(owned_len(name.as_bytes())?)?,
            Part::Current | Part::Parent => {}
        }
    }
    Some(cost)
}

fn owned_len(bytes: &[u8]) -> Option<u64> {
    u64::try_from(bytes.len()).ok()
}
