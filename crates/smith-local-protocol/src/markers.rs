//! Original merge-conflict files for a local delivery (protocol/hosts.md,
//! section 5.5; domain/host.md, section 8). The adapter retains the bounded
//! original paths and one read position. It never surveys unrelated files,
//! decides a delivery, or owns a descriptor. `new`, `start` and `from_file`
//! emit one bounded file load at a time; each load settles before the next.
//!
//! | State | Input | Output |
//! |---|---|---|
//! | Reading | clean file or missing path | load next original path |
//! | Reading | marker line | first marker path |
//! | Reading | failed or oversized load | failed git terminal |
//! | Reading | last clean file | no marker terminal |

use alloc::boxed::Box;
use core::mem::size_of;
use skein_io::{file, kernel};
use skein_lib::{Time, Token};
use smith_domain::run;
use smith_local_domain::GitResult;

use crate::GitLimits;

/// One original-conflict load or the terminal sent to the local domain.
#[derive(Debug)]
pub enum MarkerAction {
    /// Load one original path with its delivery deadline.
    File { request: file::Request, deadline: Time },
    /// Finish marker inspection once all admitted reads settled.
    Done(GitResult),
}

/// A bounded sequence of original-conflict file reads requested by the host.
#[derive(Debug)]
pub struct Markers {
    owner: Token,
    root: Token,
    paths: Box<[Box<[u8]>]>,
    next: u32,
    deadline: Time,
    limits: GitLimits,
}

/// Checked bound for original paths and one whole-file delivery.
#[must_use]
pub fn markers_worst_case(limits: &GitLimits) -> Option<u64> {
    u64::try_from(size_of::<Markers>())
        .ok()?
        .checked_add(u64::from(limits.conflicts).checked_mul(u64::try_from(size_of::<Box<[u8]>>()).ok()?)?)?
        .checked_add(u64::from(limits.conflicts).checked_mul(u64::from(limits.path_bytes))?)?
        .checked_add(u64::from(limits.path_bytes))?
        .checked_add(u64::from(limits.output_bytes))
}

impl Markers {
    /// Admit original paths only within the configured count and byte bounds.
    #[must_use]
    pub fn new(owner: Token, root: Token, paths: Box<[Box<[u8]>]>, deadline: Time, limits: GitLimits) -> Option<Self> {
        if paths.len() > usize::try_from(limits.conflicts).ok()? {
            return None;
        }
        for path in &paths {
            if path.len() > usize::try_from(limits.path_bytes).ok()? || path.is_empty() || path.contains(&0) {
                return None;
            }
        }
        Some(Self { owner, root, paths, next: 0, deadline, limits })
    }

    /// Issue the first load, or finish an empty original-conflict list.
    #[must_use]
    pub fn start(&self) -> MarkerAction {
        self.request()
    }

    /// Translate one settled load, treating a deleted conflict file as resolved.
    pub fn from_file(&mut self, event: file::Event) -> MarkerAction {
        match event {
            file::Event::Loaded { owner, bytes } => {
                assert_eq!(owner, self.owner, "file terminal echoes marker owner");
                if has_markers(&bytes) {
                    let path = self
                        .paths
                        .get(usize::try_from(self.next).expect("bounded position"))
                        .expect("original path")
                        .clone();
                    return MarkerAction::Done(GitResult::Markers { first: Some(path) });
                }
            }
            file::Event::Failed { owner, error: kernel::Error::NotFound, .. } => {
                assert_eq!(owner, self.owner, "file terminal echoes marker owner");
            }
            file::Event::Failed { owner, error: _, .. } => {
                assert_eq!(owner, self.owner, "file terminal echoes marker owner");
                return MarkerAction::Done(failed(run::DeliveryReason::Broken));
            }
            file::Event::TooLarge { owner, .. } => {
                assert_eq!(owner, self.owner, "file terminal echoes marker owner");
                return MarkerAction::Done(failed(run::DeliveryReason::TooLarge));
            }
            file::Event::Cancelled { owner } => {
                assert_eq!(owner, self.owner, "file terminal echoes marker owner");
                return MarkerAction::Done(failed(run::DeliveryReason::TimedOut));
            }
            file::Event::Locked { .. }
            | file::Event::Busy { .. }
            | file::Event::Opened { .. }
            | file::Event::Scanned { .. }
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
            | file::Event::Refused { .. } => unreachable!("marker loads have one whole-file terminal"),
        }
        self.next = self.next.checked_add(1).expect("bounded original paths");
        self.request()
    }

    fn request(&self) -> MarkerAction {
        match self.paths.get(usize::try_from(self.next).expect("bounded position")) {
            Some(path) => MarkerAction::File {
                request: file::Request::Load {
                    owner: self.owner,
                    root: self.root,
                    path: path.clone(),
                    max: self.limits.output_bytes,
                    no_follow: true,
                },
                deadline: self.deadline,
            },
            None => MarkerAction::Done(GitResult::Markers { first: None }),
        }
    }
}

fn failed(reason: run::DeliveryReason) -> GitResult {
    GitResult::Failed { reason, diagnostic: Box::new(run::Diagnostic::empty()) }
}

fn has_markers(bytes: &[u8]) -> bool {
    let mut start = 0_usize;
    for end in 0..bytes.len() {
        if bytes.get(end) == Some(&b'\n') {
            if marker_line(bytes.get(start..end).expect("line offsets are bounded")) {
                return true;
            }
            start = end.checked_add(1).expect("bounded file offset");
        }
    }
    marker_line(bytes.get(start..).expect("last line is bounded"))
}

fn marker_line(line: &[u8]) -> bool {
    line.starts_with(b"<<<<<<< ") || line == b"=======" || line.starts_with(b">>>>>>> ")
}
