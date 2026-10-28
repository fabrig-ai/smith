//! The tree each session's tools work on, seeded in the fake checkout, and
//! what may come of a call there.
//!
//! The fake provider writes its calls' arguments from a small menu, and each
//! path on it names something different: a file whose content starts with its
//! own path, another, a directory, a path outside the checkout, and a file that
//! is missing until a write makes it. Every write and edit on the menu keeps
//! that first line's prefix. So what came of a call tells which call it was,
//! and a result put in the wrong slot of a batch is caught ([`fits`]).

use std::time::Duration;

use skein_fake_checkout::{self as fake, Checkout, Program};
use smith_domain_tools::{Call, Exit, Outcome, Part, Path};

/// The files each session's repository starts with: their content starts
/// with their path.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const FILES: [(&[u8], &[u8]); 3] = [
    (b"README.md", b"README.md: hello, world\n"),
    (b"src/lib.rs", b"src/lib.rs: pub fn answer() -> u32 { 42 }\n"),
    (b"docs/guide.md", b"docs/guide.md: the guide\n"),
];

/// The directory on the menu, and an entry it always has.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const DIRECTORY: (&[u8], &[u8]) = (b"docs", b"guide.md");

/// The file on the menu that is missing until a write makes it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const MISSING: &[u8] = b"notes.md";

/// The commands on the menu, and how each ends. What each writes starts with
/// the command. (The world draws how long a command runs, as it does for every
/// operation.)
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const COMMANDS: [(&[u8], u8); 5] = [(b"cargo test", 0), (b"ls", 0), (b"true", 0), (b"cat README.md", 0), (b"false", 1)];

/// Scripts the commands on the menu, once for the whole checkout.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub fn script(checkout: &mut Checkout) {
    for (command, code) in COMMANDS {
        let output = output(command);
        let program =
            Program { duration: Duration::from_millis(50), output, exit: fake::Exit::Code(code), changes: Vec::new() };
        checkout.program(command, program);
    }
}

/// What `command` writes.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
fn output(command: &[u8]) -> Vec<u8> {
    [command, b": done\n"].concat()
}

/// Seeds a repository at `at` in the checkout, and makes it a root: io's name
/// for it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub fn seed(checkout: &mut Checkout, at: &[u8]) -> u64 {
    checkout.mkdir(at);
    for (path, content) in FILES {
        checkout.write(&[at, b"/", path].concat(), content);
    }
    checkout.root(at)
}

/// Whether `text` holds `pattern` whole.
///
/// World contract: domain/session.md, section 8.1; testing-strategy.md, section 2.2.
fn holds(text: &[u8], pattern: &[u8]) -> bool {
    text.windows(pattern.len()).any(|window| window == pattern)
}

/// Where `path` is, relative to the working directory at the repository's
/// root, or `None` if it climbs out of it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
fn place(path: &Path) -> Option<Vec<u8>> {
    let mut names: Vec<&[u8]> = Vec::new();
    for part in &path.parts {
        match part {
            Part::Current => {}
            Part::Parent => {
                names.pop()?;
            }
            Part::Name { name } => names.push(name.as_bytes()),
        }
    }
    Some(names.join(&b'/'))
}

/// Whether `outcome` may come of `call` on a session's tree, as it is or as
/// the menu's writes and edits leave it. Failures any call may meet (a fault,
/// a deadline, a cancel, a family not granted, a full kit) fit every call.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn fits(call: &Call, outcome: &Outcome) -> bool {
    if matches!(
        outcome,
        Outcome::Failed { .. } | Outcome::TimedOut | Outcome::Cancelled | Outcome::Busy | Outcome::NotGranted
    ) {
        return true;
    }
    let path = match call {
        Call::Read { path, .. }
        | Call::List { path, .. }
        | Call::Search { path, .. }
        | Call::Remove { path }
        | Call::Write { path, .. }
        | Call::Edit { path, .. } => path,
        Call::Job { .. } => return matches!(outcome, Outcome::Job { .. }),
        Call::Shell { command, .. } => {
            let Outcome::Exited { exit, head, .. } = outcome else {
                return false;
            };
            // What it wrote by its end, or by its deadline.
            return if *exit == Exit::TimedOut { output(command).starts_with(head) } else { head.starts_with(command) };
        }
    };
    let Some(place) = place(path) else {
        return matches!(outcome, Outcome::Outside { .. });
    };
    let (directory, entry) = DIRECTORY;
    let is_directory = place == directory;
    let is_file = !is_directory && (place == MISSING || FILES.iter().any(|(file, _)| *file == place.as_slice()));
    let may_be_missing = place == MISSING;
    match (call, outcome) {
        (Call::Read { .. }, Outcome::Read { content, .. }) => content.starts_with(&[&place[..], b":"].concat()),
        (Call::List { .. }, Outcome::Listed { entries, .. }) => {
            is_directory && entries.iter().any(|listed| listed.name.as_ref() == entry)
        }
        // Each search on the menu has a pattern of its own. A share below the
        // line may cut the last hit to the start of a matching line.
        (Call::Search { pattern, .. }, Outcome::Found { hits, .. }) => hits.iter().enumerate().all(|(index, hit)| {
            holds(&hit.text, pattern)
                || (index + 1 == hits.len()
                    && FILES
                        .iter()
                        .flat_map(|(_, content)| content.split(|byte| *byte == b'\n'))
                        .any(|line| line.starts_with(&hit.text) && holds(line, pattern)))
        }),
        (Call::Remove { .. }, Outcome::Removed) => is_file || is_directory,
        (Call::Write { .. }, Outcome::Written { created }) => is_file && (!*created || may_be_missing),
        // What a change to a file, or a listing of one, comes to; and an edit
        // that does not apply.
        (Call::Edit { .. }, Outcome::Edited { .. } | Outcome::EditFailed { .. })
        | (Call::List { .. }, Outcome::NotDirectory { .. }) => is_file,
        (Call::Remove { .. }, Outcome::NotEmpty)
        | (Call::Read { .. } | Call::Write { .. } | Call::Edit { .. }, Outcome::NotFile) => is_directory,
        (
            Call::Read { .. } | Call::List { .. } | Call::Search { .. } | Call::Edit { .. } | Call::Remove { .. },
            Outcome::NotFound { .. },
        ) => may_be_missing,
        // What the tools refuse before a change: a path not read first, or
        // changed since.
        (Call::Write { .. } | Call::Edit { .. } | Call::Remove { .. }, Outcome::NotRead | Outcome::Stale) => true,
        _ => false,
    }
}
