//! Calls as the protocol layer would decode them from what an LLM wrote.

use skein_lib::Duration;
use smith_domain_tools::Call;

use crate::translate::path;

/// Reads the file at `at` whole, as far as a read goes.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn read(at: &[u8]) -> Call {
    Call::Read { path: path(at), offset: 1, window: smith_domain_tools::Budget::Default }
}

/// Reads a byte window at a one-based offset.
/// World contract: domain/tools.md, sections 3 and 7.
#[must_use]
pub fn read_window(at: &[u8], offset: u32, bytes: u32) -> Call {
    Call::Read { path: path(at), offset, window: smith_domain_tools::Budget::Asked(bytes) }
}

/// Lists the directory at `at`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn list(at: &[u8]) -> Call {
    Call::List { path: path(at), depth: smith_domain_tools::Depth::Whole, glob: None }
}

/// Makes the file at `at` hold `content`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn write(at: &[u8], content: &[u8]) -> Call {
    Call::Write { path: path(at), content: content.into() }
}

/// Remove one current file or empty directory (domain/tools.md, section 3).
#[must_use]
pub fn remove(at: &[u8]) -> Call {
    Call::Remove { path: path(at) }
}

/// Replaces `old` with `new` in the file at `at`: its one occurrence, or every
/// one if `all`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn edit(at: &[u8], old: &[u8], new: &[u8], all: bool) -> Call {
    edits(at, &[(old, new, all)])
}

/// One ordered replacement list in the existing tools world.
#[must_use]
pub fn edits(at: &[u8], replacements: &[(&[u8], &[u8], bool)]) -> Call {
    let edits = replacements
        .iter()
        .map(|(old, new, all)| smith_domain_tools::Replacement {
            old: (*old).into(),
            new: (*new).into(),
            occurrences: if *all {
                smith_domain_tools::Occurrences::All
            } else {
                smith_domain_tools::Occurrences::Once
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Call::Edit { path: path(at), edits }
}

/// Runs `command` with the shell, for at most `timeout` if given.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn shell(command: &[u8], timeout: Option<Duration>) -> Call {
    Call::Shell {
        after: smith_domain_tools::Yield::Never,
        command: command.into(),
        timeout: match timeout {
            Some(duration) => smith_domain_tools::Timeout::Asked(duration),
            None => smith_domain_tools::Timeout::Default,
        },
        output: smith_domain_tools::Budget::Default,
    }
}

/// Searches the files at and beneath `at` for `pattern`, in those whose names
/// match `glob` if given.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn search(at: &[u8], pattern: &[u8], glob: Option<&[u8]>) -> Call {
    Call::Search { path: path(at), pattern: pattern.into(), glob: glob.map(Into::into) }
}

/// Leave a command running after the asked seconds.
#[must_use]
pub fn shell_after(command: &[u8], seconds: u64) -> Call {
    let mut call = shell(command, None);
    if let Call::Shell { after, .. } = &mut call {
        *after = smith_domain_tools::Yield::Asked(Duration::from_secs(seconds));
    }
    call
}

/// Read the job's unread output, waiting the asked seconds.
#[must_use]
pub fn job(number: u32, seconds: u64) -> Call {
    Call::Job {
        job: smith_domain_tools::JobNumber(number),
        wait: smith_domain_tools::Wait::Asked(Duration::from_secs(seconds)),
        output: smith_domain_tools::Budget::Default,
        stop: false,
    }
}
