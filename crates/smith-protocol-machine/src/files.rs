//! Whole files and conditional stores over skein's file io
//! (protocol/agent.md, section 2; domain/tools.md, sections 4 and 6).

use alloc::boxed::Box;

use skein_io::{digest, file, kernel};
use skein_lib::{List, Time, Token};
use smith_domain::tools;

use crate::component::{Kind, Pending, Root};
use crate::limits::Limits;

/// Translate a root-relative domain path once: the directory itself is `.`
/// at io (domain/tools.md, section 2), and every nested path stays unchanged.
pub(crate) fn io_path(path: Box<[u8]>) -> Box<[u8]> {
    if path.is_empty() { Box::from(&b"."[..]) } else { path }
}

/// Prepare one file request, or refuse before io sees an unsafe path. A
/// create whose path names a directory first probes that directory: see
/// [`Create`].
pub(crate) fn request(
    roots: &List<Root>,
    limits: &Limits,
    owner: Token,
    op: tools::Op,
    deadline: Time,
) -> Result<(Pending, file::Request), tools::Done> {
    match op {
        tools::Op::Load { at, max } => {
            check(roots, limits, at.root, &at.path, false)?;
            if max > limits.file_bytes {
                return Err(other());
            }
            Ok((
                Pending::One { kind: Kind::Load },
                file::Request::Load { owner, root: at.root, path: io_path(at.path), max, no_follow: false },
            ))
        }
        tools::Op::Store { at, content, expect } => {
            check(roots, limits, at.root, &at.path, true)?;
            if content.len() > usize::try_from(limits.file_bytes).expect("u32 fits usize") {
                return Err(tools::Done::TooLarge { size: u64::try_from(content.len()).unwrap_or(u64::MAX) });
            }
            let expected = match expect {
                tools::Expect::Absent => match directory_end(&at.path) {
                    Some(parent) => return Ok(Create::start(owner, at.root, at.path, content, parent, deadline)),
                    None => file::Expect::Absent,
                },
                tools::Expect::Is { version } => file::Expect::Digest(digest::Digest(version.raw())),
            };
            Ok((Pending::One { kind: Kind::Store }, store(owner, at.root, io_path(at.path), content, expected)))
        }
        tools::Op::Remove { .. } | tools::Op::Spawn { .. } | tools::Op::Search { .. } | tools::Op::List { .. } => {
            Err(other())
        }
    }
}

fn store(owner: Token, root: Token, path: Box<[u8]>, bytes: Box<[u8]>, expected: file::Expect) -> file::Request {
    file::Request::Store { owner, root, path, bytes, expected, no_follow: true }
}

/// The index of the `/` that ends the directory a nested path's file is in,
/// or `None` for a file directly in its root.
fn directory_end(path: &[u8]) -> Option<u32> {
    let mut end = None;
    for (index, byte) in path.iter().enumerate() {
        if *byte == b'/' && index > 0 {
            end = Some(u32::try_from(index).expect("a checked path's length fits u32"));
        }
    }
    end
}

/// A create in a directory, kept by the component across its steps until
/// its store (domain/tools.md, section 3: `write` first makes the
/// directories its path names that do not exist). It loads nothing from the
/// file's directory, without following a link, to learn whether that is a
/// directory; when nothing is there it makes each directory of the path from
/// the top, an existing one counting as made, then stores. The directories
/// follow the store's rules (section 4, Confined): the probe refuses a
/// symbolic link on any part before anything is made, each is made beneath
/// the same writable root, and its path, a prefix of the file's, was checked
/// for a `.git` part with it. A directory made stays if a later step fails.
#[derive(Debug)]
pub(crate) struct Create {
    root: Token,
    path: Box<[u8]>,
    content: Box<[u8]>,
    deadline: Time,
    /// The index of the `/` that ends the directory the file is in.
    parent: u32,
    step: Making,
    cancelled: bool,
}

/// The step a create's request in flight is.
#[derive(Clone, Copy, Debug)]
enum Making {
    /// Whether the file's directory is one.
    Probing,
    /// The directory whose path ends before byte `end`.
    Making { end: u32 },
}

/// A create's step after one of its terminals.
#[derive(Debug)]
pub(crate) enum Next {
    /// The next request, owed to `pending`.
    Request { pending: Pending, request: file::Request, deadline: Time },
    /// The create's answer.
    Done(tools::Done),
}

/// What one terminal leads a create to.
enum After {
    Store,
    Make { end: u32 },
    Answer(tools::Done),
}

impl Create {
    fn start(
        owner: Token,
        root: Token,
        path: Box<[u8]>,
        content: Box<[u8]>,
        parent: u32,
        deadline: Time,
    ) -> (Pending, file::Request) {
        let probe = file::Request::Load {
            owner,
            root,
            path: Box::from(path.get(..index(parent)).expect("the directory ends within the path")),
            max: 0,
            no_follow: true,
        };
        (
            Pending::Create(Create { root, path, content, deadline, parent, step: Making::Probing, cancelled: false }),
            probe,
        )
    }

    /// The domain cancelled: the request in flight answers, and no other
    /// is sent.
    pub(crate) const fn cancel(&mut self) {
        self.cancelled = true;
    }

    /// Take the terminal of the step in flight.
    pub(crate) fn event(mut self, owner: Token, now: Time, event: file::Event) -> Next {
        let after = match self.step {
            Making::Probing => self.probed(event),
            Making::Making { end } => self.made(end, event),
        };
        match after {
            After::Answer(done) => Next::Done(done),
            After::Store | After::Make { .. } if self.cancelled => Next::Done(tools::Done::Cancelled),
            After::Store | After::Make { .. } if now >= self.deadline => Next::Done(tools::Done::TimedOut),
            After::Store => {
                let Create { root, path, content, deadline, .. } = self;
                Next::Request {
                    pending: Pending::One { kind: Kind::Store },
                    request: store(owner, root, path, content, file::Expect::Absent),
                    deadline,
                }
            }
            After::Make { end } => {
                self.step = Making::Making { end };
                let request = file::Request::MakeDirectory {
                    owner,
                    root: self.root,
                    path: Box::from(self.path.get(..index(end)).expect("a directory ends within the path")),
                    mode: 0o777,
                };
                Next::Request { deadline: self.deadline, pending: Pending::Create(self), request }
            }
        }
    }

    /// The probe found a directory, nothing, or what a store refuses.
    #[expect(clippy::wildcard_enum_match_arm, reason = "skein event and kernel error vocabularies are external")]
    fn probed(&self, event: file::Event) -> After {
        match event {
            file::Event::Failed { error: kernel::Error::IsADirectory, .. } => After::Store,
            file::Event::Failed { error: kernel::Error::NotFound, .. } => After::Make { end: self.part(0) },
            // A file, empty or not, or a FIFO, device or socket where the
            // directory should be.
            file::Event::Loaded { .. }
            | file::Event::TooLarge { .. }
            | file::Event::Failed { error: kernel::Error::NotAFile, .. } => After::Answer(tools::Done::NotDirectory),
            file::Event::Failed { error, .. } => After::Answer(failure(Kind::Store, error)),
            file::Event::Cancelled { .. } => After::Answer(tools::Done::Cancelled),
            _ => unreachable!("a probe is a load"),
        }
    }

    /// The directory ending at `end` was made, or is there.
    #[expect(clippy::wildcard_enum_match_arm, reason = "skein's event vocabulary is external")]
    fn made(&self, end: u32, event: file::Event) -> After {
        match event {
            file::Event::Made { .. } | file::Event::Failed { error: kernel::Error::Exists, .. } => {
                if end == self.parent {
                    After::Store
                } else {
                    After::Make { end: self.part(end.checked_add(1).expect("a path index fits u32")) }
                }
            }
            file::Event::Failed { error, .. } => After::Answer(failure(Kind::Store, error)),
            file::Event::Cancelled { .. } => After::Answer(tools::Done::Cancelled),
            _ => unreachable!("a directory maker answers made or failed"),
        }
    }

    /// The end of the next directory from byte `from`: its `/`, at most the
    /// one that ends the file's directory.
    fn part(&self, from: u32) -> u32 {
        let start = index(from);
        let slice = self.path.get(start..index(self.parent)).expect("a part starts within the file's directory");
        for (offset, byte) in slice.iter().enumerate() {
            if *byte == b'/' {
                return from
                    .checked_add(u32::try_from(offset).expect("a checked path's length fits u32"))
                    .expect("a path index fits u32");
            }
        }
        self.parent
    }
}

fn index(at: u32) -> usize {
    usize::try_from(at).expect("u32 fits usize")
}

pub(crate) fn check(
    roots: &List<Root>,
    limits: &Limits,
    root: Token,
    path: &[u8],
    write: bool,
) -> Result<(), tools::Done> {
    if path.len() > usize::try_from(limits.path_bytes).expect("u32 fits usize") {
        return Err(other());
    }
    if in_git(path) {
        return Err(tools::Done::Failed { fault: tools::Fault::Denied });
    }
    for admitted in roots {
        if admitted.token == root {
            if write && !admitted.writable {
                return Err(tools::Done::Failed { fault: tools::Fault::Denied });
            }
            return Ok(());
        }
    }
    Err(tools::Done::Escapes)
}

fn in_git(path: &[u8]) -> bool {
    let mut start = 0_usize;
    for (index, byte) in path.iter().enumerate() {
        if *byte == b'/' {
            if path.get(start..index).expect("path index is in range").eq_ignore_ascii_case(b".git") {
                return true;
            }
            start = index.checked_add(1).expect("index of a path byte fits");
        }
    }
    path.get(start..).expect("path index is in range").eq_ignore_ascii_case(b".git")
}

fn other() -> tools::Done {
    tools::Done::Failed { fault: tools::Fault::Other }
}

/// Convert the one terminal expected for the admitted operation.
#[expect(clippy::manual_map, reason = "the strict step subset does not use closures")]
pub(crate) fn terminal(kind: Kind, event: file::Event) -> tools::Done {
    match event {
        file::Event::Loaded { bytes, .. } => {
            assert_eq!(kind, Kind::Load, "only a load returns file bytes");
            tools::Done::Loaded { version: tools::Version::new(digest::digest(&bytes).0), content: bytes }
        }
        file::Event::Stored { digest, .. } => {
            assert_eq!(kind, Kind::Store, "only a store returns a digest");
            tools::Done::Stored { version: tools::Version::new(digest.0) }
        }
        file::Event::Conflict { now, .. } => {
            assert_eq!(kind, Kind::Store, "only a store returns conflict");
            let now = match now {
                Some(digest) => Some(tools::Version::new(digest.0)),
                None => None,
            };
            tools::Done::Conflict { now }
        }
        file::Event::TooLarge { size, .. } => {
            assert_eq!(kind, Kind::Load, "only a load reports observed excess");
            tools::Done::TooLarge { size }
        }
        file::Event::Cancelled { .. } => tools::Done::Cancelled,
        file::Event::Failed { error, .. } => failure(kind, error),
        file::Event::Locked { .. }
        | file::Event::Busy { .. }
        | file::Event::Scanned { .. }
        | file::Event::Opened { .. }
        | file::Event::Stated { .. }
        | file::Event::Written { .. }
        | file::Event::Read { .. }
        | file::Event::Cut { .. }
        | file::Event::Synced { .. }
        | file::Event::Closed { .. }
        | file::Event::Renamed { .. }
        | file::Event::Removed { .. }
        | file::Event::Listed { .. }
        | file::Event::Made { .. }
        | file::Event::Refused { .. } => unreachable!("machine asks io only for whole-file operations"),
    }
}

fn failure(kind: Kind, error: kernel::Error) -> tools::Done {
    match error {
        kernel::Error::NotFound => tools::Done::Missing,
        kernel::Error::NotADirectory => tools::Done::NotDirectory,
        kernel::Error::IsADirectory | kernel::Error::NotAFile => match kind {
            Kind::Load | Kind::Store => tools::Done::NotFile,
        },
        kernel::Error::TooManyLinks => match kind {
            Kind::Store => tools::Done::Linked,
            Kind::Load => other(),
        },
        kernel::Error::Escape => tools::Done::Escapes,
        kernel::Error::Permission | kernel::Error::ReadOnly => tools::Done::Failed { fault: tools::Fault::Denied },
        kernel::Error::NoSpace => tools::Done::Failed { fault: tools::Fault::NoSpace },
        kernel::Error::TimedOut => tools::Done::TimedOut,
        kernel::Error::Cancelled => tools::Done::Cancelled,
        kernel::Error::Refused
        | kernel::Error::Busy
        | kernel::Error::Reset
        | kernel::Error::BrokenPipe
        | kernel::Error::NotConnected
        | kernel::Error::AddressInUse
        | kernel::Error::AddressNotAvailable
        | kernel::Error::Unreachable
        | kernel::Error::TooManyOpenFiles
        | kernel::Error::NoBufferSpace
        | kernel::Error::TooLate
        | kernel::Error::Exists
        | kernel::Error::NotEmpty
        | kernel::Error::NameTooLong
        | kernel::Error::InvalidArgument
        | kernel::Error::Other(_) => other(),
    }
}
