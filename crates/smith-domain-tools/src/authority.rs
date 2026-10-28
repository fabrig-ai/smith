//! What a kit may do and where: the checkout's repositories, which of them
//! may be written, and the families of tools granted. The run decides it from
//! its charter, and it is copied into the session, and from there into its
//! kit, when the session opens.
//!
//! Contract: domain/tools.md, sections 2 and 4; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;

use skein_lib::bytes::copy_of;
use skein_lib::{List, Token, Writer};

use crate::call::{Call, Outcome};
use crate::limits::Limits;
use crate::path::{self, Name, Namespace, Path, Place, Resolved};

/// What a kit is given when it opens.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Authority {
    /// Where relative paths start: an absolute path, as names.
    pub cwd: Box<[Name]>,
    /// The repositories of the checkout. Paths outside them all are refused.
    pub repos: Box<[Repo]>,
    /// Explicit authority supplied by the opener, never inferred from role or text.
    pub grants: Grants,
}

impl Authority {
    /// Dynamic bytes of the namespace, measured before copying its working name.
    #[must_use]
    pub fn namespace_bytes(&self) -> Option<u64> {
        match self.repos.len() {
            0 | 1 => Some(0),
            _ => match self.cwd.first() {
                Some(name) => u64::try_from(name.as_bytes().len()).ok(),
                None => Some(0),
            },
        }
    }

    /// The conversation's path presentation, derived without reading files.
    #[must_use]
    pub fn namespace(&self) -> Namespace {
        match self.repos.len() {
            0 => Namespace::NoWorkspace,
            1 => Namespace::One,
            _ => match self.cwd.first() {
                Some(working) => Namespace::Several { working: working.clone() },
                // Legacy authorities may mount the filesystem root without a
                // working name. Keep their relative presentation; run-owned
                // authorities always name the first directory.
                None => Namespace::One,
            },
        }
    }
}

/// A repository of the checkout.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Repo {
    /// Absolute host path components; whole-component prefixes alias this root.
    pub host: Box<[Name]>,
    /// Where it is: the absolute path of its root, as names. A repository
    /// mounted inside another holds what is beneath its own mount.
    pub mount: Box<[Name]>,
    /// io's name for its root directory, beneath which io resolves every
    /// place in it.
    pub root: Token,
    /// Whether this mount permits modification; protected git paths remain unwritable.
    pub writable: bool,
}

/// The families of tools a kit may use.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Grants {
    /// Read, list and search.
    pub inspect: bool,
    /// Write and edit, in the writable repositories. Without it, nothing the
    /// kit does writes a file: its commands see every repository read-only.
    pub modify: bool,
    /// Run commands. A command may write only where the kit may write, which
    /// needs modify too, and never in a repository's git directory.
    pub shell: bool,
}

/// An authority as a kit keeps it, with its mounts joined for choosing among
/// them.
#[derive(PartialEq, Eq, Hash, Debug)]
pub(crate) struct Checkout {
    pub(crate) cwd: Box<[Name]>,
    pub(crate) mounts: Box<[Mount]>,
    pub(crate) grants: Grants,
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub(crate) struct Mount {
    pub(crate) host: Box<[u8]>,
    /// The repository's mount, its names joined by `/`.
    pub(crate) at: Box<[u8]>,
    pub(crate) root: Token,
    pub(crate) writable: bool,
}

/// What the normalized path names, before any IO is asked.
#[derive(PartialEq, Eq, Debug)]
pub(crate) enum Target {
    /// A place below an admitted directory and its write authority.
    Place(Located),
    /// The namespace root: listing it answers directory names without IO.
    Directories,
}

/// Where a path is, and whether the kit may write there.
#[derive(PartialEq, Eq, Debug)]
pub(crate) struct Located {
    pub(crate) place: Place,
    pub(crate) writable: bool,
}

/// The checkout for `authority`, or `None` if it does not fit `limits` or
/// cannot be one: too many repositories, two at one mount (which would make
/// writability depend on their order) or with one root, a path longer than
/// the tools take.
pub(crate) fn admit(authority: Authority, limits: &Limits) -> Option<Checkout> {
    let repos = u32::try_from(authority.repos.len()).ok()?;
    if repos > limits.repos {
        return None;
    }
    // Each component needs at least one byte plus its separating slash.
    // Refuse before allocating references to an overlong input.
    let names = limits.path_bytes.checked_add(1)? / 2;
    if u32::try_from(authority.cwd.len()).ok()? > names {
        return None;
    }
    let cwd = path::refs(&authority.cwd)?;
    if path::joined(cwd.as_slice())? > usize::try_from(limits.path_bytes).ok()? {
        return None;
    }
    let mut mounts: List<Mount> = List::with_capacity(repos);
    for repo in &authority.repos {
        if u32::try_from(repo.mount.len()).ok()? > names {
            return None;
        }
        let names = path::refs(&repo.mount)?;
        let at = path::join(names.as_slice(), limits.path_bytes)?;
        for other in mounts.as_slice() {
            if other.at == at || other.root == repo.root {
                return None;
            }
        }
        let host_names = limits.host_path_bytes.checked_add(1)? / 2;
        if u32::try_from(repo.host.len()).ok()? > host_names {
            return None;
        }
        let host = path::join(path::refs(&repo.host)?.as_slice(), limits.host_path_bytes)?;
        let mount = Mount { at, host, root: repo.root, writable: repo.writable };
        mounts.push(mount).expect("room for every repository");
    }
    Some(Checkout { cwd: authority.cwd, mounts: mounts.into_boxed(), grants: authority.grants })
}

/// Whether `grants` cover `call`.
pub(crate) const fn granted(grants: Grants, call: &Call) -> bool {
    match call {
        Call::Read { .. } | Call::List { .. } | Call::Search { .. } => grants.inspect,
        Call::Write { .. } | Call::Edit { .. } | Call::Remove { .. } => grants.modify,
        Call::Shell { .. } | Call::Job { .. } => grants.shell,
    }
}

/// Where `path` is in `checkout`, or the outcome that refuses it: `TooLong`
/// past `max` bytes, `Outside` beyond every repository. The normalized
/// absolute root names the directories themselves, without a filesystem place.
pub(crate) fn locate(checkout: &Checkout, path: &Path, max: u32) -> Result<Target, Outcome> {
    let Some(at) = path::normalise(&checkout.cwd, path, max) else {
        return Err(Outcome::TooLong);
    };
    if path.absolute && at.is_empty() {
        return Ok(Target::Directories);
    }
    if path.absolute {
        let mut chosen = None;
        let mut longest = 0;
        for (index, mount) in checkout.mounts.iter().enumerate() {
            if beneath(&mount.host, &at).is_some() && (chosen.is_none() || mount.host.len() > longest) {
                chosen = Some(index);
                longest = mount.host.len();
            }
        }
        if let Some(index) = chosen {
            let mount = checkout.mounts.get(index).expect("chosen host alias");
            let start = beneath(&mount.host, &at).expect("alias holds this path");
            let path = copy_of(at.get(start..).expect("alias tail"));
            // Failure paths are reconstructed in the namespace, so that alias
            // spelling never widens the normalized result bound.
            let namespace_bytes =
                match mount.at.len().checked_add(usize::from(!mount.at.is_empty() && !path.is_empty())) {
                    Some(bytes) => bytes.checked_add(path.len()),
                    None => None,
                };
            let Some(namespace_bytes) = namespace_bytes else {
                return Err(Outcome::TooLong);
            };
            if namespace_bytes > usize::try_from(max).expect("u32 path bound") {
                return Err(Outcome::TooLong);
            }
            return Ok(Target::Place(Located { place: Place { root: mount.root, path }, writable: mount.writable }));
        }
    }
    let Some(index) = choose(&checkout.mounts, &at) else {
        return Err(Outcome::Outside { path: Resolved::Outside { path: at } });
    };
    let mount = checkout.mounts.get(index).expect("chosen among the mounts");
    let start = beneath(&mount.at, &at).expect("the chosen mount holds the path");
    let path = copy_of(at.get(start..).expect("the path beneath a mount is within it"));
    Ok(Target::Place(Located { place: Place { root: mount.root, path }, writable: mount.writable }))
}

/// The normalized namespace path of a place retained by a flight. Legacy nested
/// mounts keep their remaining components after the directory's first name.
pub(crate) fn resolved(checkout: &Checkout, place: &Place) -> Resolved {
    for mount in &checkout.mounts {
        if mount.root == place.root {
            let separator = usize::from(!mount.at.is_empty() && !place.path.is_empty());
            let size = mount
                .at
                .len()
                .checked_add(separator)
                .expect("bounded mount and separator")
                .checked_add(place.path.len())
                .expect("admitted normalized place");
            let mut writer = Writer::new(size);
            writer.put(&mount.at).expect("measured mount");
            if separator != 0 {
                writer.put(b"/").expect("measured separator");
            }
            writer.put(&place.path).expect("measured place");
            let absolute = writer.finish();
            if mount.at.is_empty() {
                return Resolved::Working { path: absolute };
            }
            let mut end = absolute.len();
            for (index, byte) in absolute.iter().enumerate() {
                if *byte == b'/' {
                    end = index;
                    break;
                }
            }
            let first = absolute.get(..end).expect("first namespace component");
            let start = end.checked_add(usize::from(end < absolute.len())).expect("bounded component");
            let path = copy_of(absolute.get(start..).expect("remaining components"));
            match checkout.cwd.first() {
                Some(working) if working.as_bytes() == first => return Resolved::Working { path },
                Some(_) | None => {
                    let directory = Name::new(copy_of(first)).expect("mount starts with a name");
                    return Resolved::Other { directory, path };
                }
            }
        }
    }
    unreachable!("flights retain places in their admitted checkout")
}

/// The repository that holds `at`, an absolute path's names joined: the one
/// with the longest mount that is `at` or a directory above it, and the first
/// of those mounted there; `None` if `at` is outside them all.
pub(crate) fn choose(mounts: &[Mount], at: &[u8]) -> Option<usize> {
    let mut chosen: Option<(usize, usize)> = None;
    for (index, mount) in mounts.iter().enumerate() {
        if beneath(&mount.at, at).is_none() {
            continue;
        }
        let longer = match chosen {
            Some((_, len)) => mount.at.len() > len,
            None => true,
        };
        if longer {
            chosen = Some((index, mount.at.len()));
        }
    }
    let (index, _) = chosen?;
    Some(index)
}

/// Where the path beneath `mount` starts in `at`, or `None` if `mount` does not
/// hold `at`.
pub(crate) fn beneath(mount: &[u8], at: &[u8]) -> Option<usize> {
    // The root holds everything.
    if mount.is_empty() {
        return Some(0);
    }
    let rest = at.strip_prefix(mount)?;
    match rest.first() {
        None => Some(mount.len()),
        Some(b'/') => mount.len().checked_add(1),
        Some(_) => None,
    }
}
