//! The mapping between the tools' vocabulary and the fake checkout's: what the
//! protocol layer (splitting the paths the LLM wrote) and io (running file
//! operations, naming roots and versions) do, without the bytes and the
//! kernel.

use skein_fake_checkout as fake;
use skein_lib::Token;
use smith_domain_tools::{Done, Exit, Expect, Fault, Hit, Name, Op, Part, Path, Place, Version};

/// The path the LLM wrote, split at its slashes, as the protocol layer does.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn path(text: &[u8]) -> Path {
    if text.is_empty() {
        return Path { absolute: false, parts: Box::new([Part::Current]) };
    }
    let mut parts = Vec::new();
    for part in text.split(|byte| *byte == b'/') {
        match part {
            b"" => {}
            b"." => parts.push(Part::Current),
            b".." => parts.push(Part::Parent),
            name => parts.push(Part::Name { name: name_of(name) }),
        }
    }
    Path { absolute: text.first() == Some(&b'/'), parts: parts.into() }
}

/// The names of the absolute path `text`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn names(text: &[u8]) -> Box<[Name]> {
    let mut names = Vec::new();
    for part in path(text).parts {
        match part {
            Part::Name { name } => names.push(name),
            Part::Current | Part::Parent => panic!("{:?} is not normalised", String::from_utf8_lossy(text)),
        }
    }
    names.into()
}

fn name_of(bytes: &[u8]) -> Name {
    Name::new(bytes.into()).expect("the protocol layer refuses what is not a name")
}

/// The token io gives the fake's root `root`, and back.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn token(root: u64) -> Token {
    Token::new(root)
}

fn root(token: Token) -> u64 {
    token.raw()
}

/// io's version for the fake's, and back.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn version(version: u64) -> Version {
    Version::new([version, 0, 0, 0])
}

fn fake_version(version: Version) -> u64 {
    let [version, ..] = version.raw();
    version
}

/// A command io starts for a spawn, with how much of its output to keep:
/// what runs, its first bytes and its last.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[derive(Debug)]
pub struct Started {
    /// Contained fake process handle, consumed only by its one terminal.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub process: fake::Process,
    /// First output bytes retained under the caller's cap.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub head: u32,
    /// Last output bytes retained under the caller's cap.
    ///
    /// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
    pub tail: u32,
}

/// Starts the command of a spawn on the checkout, as io would, or the
/// terminal that says why it did not start.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub fn spawn(
    checkout: &fake::Checkout,
    cwd: &Place,
    command: &[u8],
    env: &[(Vec<u8>, Vec<u8>)],
    (head, tail): (u32, u32),
) -> Result<Started, Done> {
    match checkout.spawn(root(cwd.root), &cwd.path, command, env, &[]) {
        Ok(process) => Ok(Started { process, head, tail }),
        Err(failure) => Err(done(failure)),
    }
}

/// Apply this world's plain-command fixture changes. File-tool grants do not
/// confine commands before contained trees exist (protocol/agent.md, section 3).
pub fn finish(checkout: &mut fake::Checkout, process: &fake::Process) {
    for (path, content) in &process.program.changes {
        match content {
            Some(content) => drop(checkout.write(path, content)),
            None => checkout.remove(path),
        }
    }
}

/// How io ends a command that ran: with `exit`, or timed out if `None`, having
/// written `output`, of which it keeps the head and the tail.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn exited(exit: Option<fake::Exit>, output: &[u8], head: u32, tail: u32) -> Done {
    let exit = match exit {
        Some(fake::Exit::Code(code)) => Exit::Code { code },
        Some(fake::Exit::Signal(signal)) => Exit::Signal { signal },
        None => Exit::TimedOut,
    };
    let head = usize::try_from(head).expect("a small head").min(output.len());
    let tail = usize::try_from(tail).expect("a small tail").min(output.len() - head);
    let dropped = u64::try_from(output.len() - head - tail).expect("a small output");
    let (kept, rest) = output.split_at(head);
    Done::Exited { exit, head: kept.into(), tail: rest[rest.len() - tail..].into(), dropped }
}

/// Runs `op` on the checkout, as io would, and returns its terminal. A spawn
/// is started instead ([`spawn`]).
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub fn perform(checkout: &mut fake::Checkout, op: Op) -> Done {
    match op {
        Op::Load { at, max } => match checkout.load(root(at.root), &at.path, u64::from(max)) {
            Ok((content, found)) => Done::Loaded { content: content.into(), version: version(found) },
            Err(failure) => done(failure),
        },
        Op::List { at, depth, glob, max, max_bytes } => {
            let command = listing_command(at.root, &at.path, glob.as_deref());
            match checkout.spawn(root(at.root), &at.path, &command, &[], &[]) {
                Ok(process) => {
                    let line = u32::try_from(process.program.output.len()).expect("finite fixture output");
                    let mut listing = smith_protocol_machine::Listing::new(max, max_bytes, depth, line);
                    for byte in process.program.output {
                        listing.push(byte);
                    }
                    let exit = match process.program.exit {
                        fake::Exit::Code(code) => skein_io::kernel::Exit::Code(code),
                        fake::Exit::Signal(signal) => skein_io::kernel::Exit::Signal(u32::from(signal)),
                    };
                    listing.finish(Some(exit), false)
                }
                Err(failure) => done(failure),
            }
        }
        Op::Remove { at, expect } => {
            let expect = match expect {
                Expect::Absent => fake::Expect::Absent,
                Expect::Is { version } => fake::Expect::Is(fake_version(version)),
            };
            match checkout.remove_current(root(at.root), &at.path, expect) {
                Ok(()) => Done::Removed,
                Err(failure) => done(failure),
            }
        }
        Op::Store { at, content, expect } => {
            let expect = match expect {
                Expect::Absent => fake::Expect::Absent,
                Expect::Is { version } => fake::Expect::Is(fake_version(version)),
            };
            match checkout.store(root(at.root), &at.path, &content, expect) {
                Ok(stored) => Done::Stored { version: version(stored) },
                Err(failure) => done(failure),
            }
        }
        Op::Search { at, pattern, glob, hits, bytes } => {
            let limits = (usize::try_from(hits).expect("small"), usize::try_from(bytes).expect("small"));
            match checkout.search(root(at.root), &at.path, &pattern, glob.as_deref(), limits) {
                Ok(found) => {
                    Done::Found { hits: found.hits.into_iter().map(hit).collect(), more: found.more, timed_out: false }
                }
                // rg exits with 2 for a pattern it cannot read.
                Err(fake::Searched::Unreadable(stderr)) => {
                    let head = usize::try_from(bytes).expect("small").min(stderr.len());
                    let dropped = u64::try_from(stderr.len() - head).expect("small");
                    let head = stderr[..head].into();
                    Done::Exited { exit: Exit::Code { code: 2 }, head, tail: Box::new([]), dropped }
                }
                Err(fake::Searched::Failed(failure)) => done(failure),
            }
        }
        Op::Spawn { .. } => panic!("a spawn is started, not run"),
    }
}

/// What a search that would have ended in `done` had found when it was
/// killed, `ran` nanoseconds into the `whole` it would have taken: the share
/// of its hits found by then, and no count of more.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn cut(done: Done, ran: u128, whole: u128) -> Done {
    match done {
        Done::Found { hits, more: _, timed_out: _ } => {
            let kept = usize::try_from(hits.len() as u128 * ran / whole.max(1)).expect("no more than the hits");
            let hits = hits.into_vec().into_iter().take(kept).collect();
            Done::Found { hits, more: 0, timed_out: true }
        }
        // rg could not read the pattern, or the path is not there: that is
        // said at once, before any deadline.
        done @ (Done::Held { .. }
        | Done::Job { .. }
        | Done::Exited { .. }
        | Done::Missing
        | Done::NotDirectory
        | Done::Escapes
        | Done::Failed { .. }
        | Done::Loaded { .. }
        | Done::Listed { .. }
        | Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::NotFile
        | Done::Linked
        | Done::TooLarge { .. }
        | Done::TimedOut
        | Done::Cancelled) => done,
    }
}

fn hit((path, line, text): (Vec<u8>, usize, Vec<u8>)) -> Hit {
    Hit { path: path.into(), line: u32::try_from(line).expect("a short file"), text: text.into() }
}

fn done(failure: fake::Failure) -> Done {
    match failure {
        fake::Failure::Missing => Done::Missing,
        fake::Failure::NotEmpty => Done::NotEmpty,
        fake::Failure::NotFile => Done::NotFile,
        fake::Failure::NotDirectory => Done::NotDirectory,
        fake::Failure::TooLarge { size } => Done::TooLarge { size },
        fake::Failure::Escapes => Done::Escapes,
        fake::Failure::Linked => Done::Linked,
        fake::Failure::Loop => Done::Failed { fault: Fault::Other },
        fake::Failure::Conflict { now } => Done::Conflict { now: now.map(version) },
    }
}

/// Register a finite selected-file output, using Skein's existing command fixture.
/// Paths are relative to the requested directory and already selected by rg's
/// ignore/glob rules; this adapter interprets neither rules nor the filesystem.
pub fn listing_fixture(
    checkout: &mut fake::Checkout,
    root: Token,
    directory: &[u8],
    glob: Option<&[u8]>,
    selected: &[&[u8]],
) {
    let command = listing_command(root, directory, glob);
    let mut output = Vec::new();
    for path in selected {
        output.extend_from_slice(path);
        output.push(0);
    }
    checkout.program(
        &command,
        fake::Program {
            duration: std::time::Duration::from_millis(1),
            output,
            exit: fake::Exit::Code(0),
            changes: Vec::new(),
        },
    );
}

fn listing_command(root: Token, directory: &[u8], glob: Option<&[u8]>) -> Vec<u8> {
    let mut command = b"rg --files fixture\0".to_vec();
    command.extend_from_slice(&root.raw().to_le_bytes());
    command.extend_from_slice(directory);
    command.push(0);
    match glob {
        Some(glob) => {
            command.push(1);
            command.extend_from_slice(glob);
        }
        None => command.push(0),
    }
    command
}
