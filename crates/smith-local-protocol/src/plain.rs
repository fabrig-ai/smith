//! Bounded plain-directory snapshots (protocol/hosts.md, section 5.5;
//! domain/host.md, section 8). A scan keeps ordered paths and content digests,
//! never file handles or delivery policy. `new`, `start` and `from_file` walk
//! one directory or file at a time. Errors, omitted entries, unsupported
//! nodes and bounds refuse the scan, making the tree unknown. Unknown counts
//! as changed, never unchanged, and refuses neither a start nor a delivery.
//!
//! | State | Input | Output |
//! |---|---|---|
//! | Scanning | directory entries | append bounded paths, visit next |
//! | Scanning | file bytes | retain digest, visit next |
//! | Scanning | last node | completed snapshot |
//! | Scanning | refusal | unknown tree, counted as changed |

#![expect(clippy::manual_let_else, reason = "explicit bounded transition matches")]

use alloc::boxed::Box;
use core::mem::size_of;
use skein_io::{digest, file, kernel};
use skein_lib::{List, Time, Token, Writer};

/// Fixed admission bounds for a plain tree requested by the local host.
#[derive(Clone, Copy, Debug)]
pub struct PlainLimits {
    pub entries: u32,
    pub path_bytes: u32,
    pub file_bytes: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Entry {
    path: Box<[u8]>,
    kind: kernel::Kind,
    digest: Option<digest::Digest>,
}

/// A complete tree snapshot retained by the host until the next run starts.
#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    entries: List<Entry>,
}

/// One bounded load/scan demand or terminal returned to the local service.
#[derive(Debug)]
pub enum PlainAction {
    /// Visit one node under the run's deadline.
    File { request: file::Request, deadline: Time },
    /// The whole admitted tree has been inspected.
    Done,
    /// The local host could not snapshot the whole tree: unknown, never unchanged.
    Failed,
}

/// One bounded tree walk requested by the local host.
#[derive(Debug)]
pub struct Plain {
    owner: Token,
    root: Token,
    limits: PlainLimits,
    deadline: Time,
    snapshot: Snapshot,
    next: u32,
}

/// Bound for one complete snapshot and a scan's transient path and file bytes.
#[must_use]
pub fn plain_worst_case(limits: &PlainLimits) -> Option<u64> {
    u64::try_from(size_of::<Plain>())
        .ok()?
        .checked_add(List::<Entry>::worst_case(limits.entries)?)?
        .checked_add(u64::from(limits.entries).checked_mul(u64::from(limits.path_bytes))?)?
        .checked_add(u64::from(limits.path_bytes))?
        .checked_add(u64::from(limits.file_bytes))
}

impl Plain {
    /// Admit a root scan with checked, nonzero bounds.
    #[must_use]
    pub fn new(owner: Token, root: Token, limits: PlainLimits, deadline: Time) -> Option<Self> {
        if limits.entries == 0 || limits.path_bytes == 0 || limits.file_bytes == 0 {
            return None;
        }
        let mut entries = List::with_capacity(limits.entries);
        entries.push(Entry { path: Box::from(b".".as_slice()), kind: kernel::Kind::Directory, digest: None }).ok()?;
        Some(Self { owner, root, limits, deadline, snapshot: Snapshot { entries }, next: 0 })
    }

    /// Issue the first directory scan.
    #[must_use]
    pub fn start(&self) -> PlainAction {
        self.request()
    }

    /// Settle one file demand, refusing incomplete or unsupported tree observations.
    #[must_use]
    pub fn from_file(&mut self, event: file::Event) -> PlainAction {
        assert_eq!(event.owner(), self.owner, "file terminal echoes scan owner");
        match event {
            file::Event::Scanned { entries, more: 0, .. } => {
                let parent = self.snapshot.entries.get(self.next).expect("active scan node").path.clone();
                for entry in entries {
                    match entry.kind {
                        kernel::Kind::File | kernel::Kind::Directory => {}
                        kernel::Kind::Symlink | kernel::Kind::Other => return PlainAction::Failed,
                    }
                    let path = match join(&parent, &entry.name, self.limits.path_bytes) {
                        Some(path) => path,
                        None => return PlainAction::Failed,
                    };
                    if self.snapshot.entries.push(Entry { path, kind: entry.kind, digest: None }).is_err() {
                        return PlainAction::Failed;
                    }
                }
            }
            file::Event::Loaded { bytes, .. } => {
                if bytes.len() > usize::try_from(self.limits.file_bytes).expect("file limit fits") {
                    return PlainAction::Failed;
                }
                self.snapshot.entries.get_mut(self.next).expect("active file node").digest =
                    Some(digest::digest(&bytes));
            }
            file::Event::Scanned { .. }
            | file::Event::TooLarge { .. }
            | file::Event::Failed { .. }
            | file::Event::Cancelled { .. } => return PlainAction::Failed,
            file::Event::Locked { .. }
            | file::Event::Busy { .. }
            | file::Event::Opened { .. }
            | file::Event::Stored { .. }
            | file::Event::Conflict { .. }
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
            | file::Event::Refused { .. } => unreachable!("scan or load terminal"),
        }
        self.next = self.next.checked_add(1).expect("bounded tree position");
        self.request()
    }

    fn request(&self) -> PlainAction {
        let entry = match self.snapshot.entries.get(self.next) {
            Some(entry) => entry,
            None => return PlainAction::Done,
        };
        let request = match entry.kind {
            kernel::Kind::Directory => file::Request::Scan {
                owner: self.owner,
                root: self.root,
                path: entry.path.clone(),
                max: self.limits.entries,
                max_bytes: u64::from(self.limits.entries)
                    .checked_mul(u64::from(self.limits.path_bytes))
                    .expect("checked scan bound"),
                no_follow: true,
            },
            kernel::Kind::File => file::Request::Load {
                owner: self.owner,
                root: self.root,
                path: entry.path.clone(),
                max: self.limits.file_bytes,
                no_follow: true,
            },
            kernel::Kind::Symlink | kernel::Kind::Other => unreachable!("unsupported entries refused"),
        };
        PlainAction::File { request, deadline: self.deadline }
    }

    /// Move the completed snapshot out after its Done terminal.
    #[must_use]
    pub fn finish(self) -> Snapshot {
        assert_eq!(self.next, self.snapshot.entries.len(), "every node inspected");
        self.snapshot
    }
}

fn join(parent: &[u8], name: &[u8], maximum: u32) -> Option<Box<[u8]>> {
    if name.is_empty() || name.contains(&0) || name.contains(&b'/') || name == b"." || name == b".." {
        return None;
    }
    let prefix = if parent == b"." { 0 } else { parent.len().checked_add(1)? };
    let length = prefix.checked_add(name.len())?;
    if length > usize::try_from(maximum).ok()? {
        return None;
    }
    let mut writer = Writer::new(length);
    if prefix > 0 {
        writer.put(parent).expect("measured path");
        writer.put(b"/").expect("measured separator");
    }
    writer.put(name).expect("measured name");
    Some(writer.finish())
}
