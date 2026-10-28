//! What the memory tests build (programming-model.md, section 6.3): small limits, and
//! the largest of each thing they take, an authority, a path and a file, so
//! that a domain fills its configured admission limits.

use skein_lib::{Duration, Token};
use smith_domain_tools::{Authority, Call, Grants, Limits, Name, Part, Path, Repo};

/// Small bounded checkout-tool limits used to exercise the memory envelope.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub const LIMITS: Limits = Limits {
    shell_output: 64,
    kits: 2,
    calls: 3,
    repos: 2,
    path_bytes: 63,
    host_path_bytes: 63,
    known_files: 4,
    file_bytes: 256,
    tool_payload: 256,
    read_bytes: 64,
    list_entries: 4,
    list_bytes: 4096,
    match_lines: 4,
    file_timeout: Duration::from_secs(10),

    jobs: 4,
    job_poll: Duration::from_secs(30),
    job_wait: Duration::from_secs(30),
    shell_timeout: Duration::from_secs(60),
    tool_deadline: Duration::from_secs(600),
    shell_head: 64,
    shell_tail: 128,
    search_hits: 8,
    search_bytes: 256,
    search_timeout: Duration::from_secs(30),
    facts: 64,
};

/// Explicit inspect, modify and shell grants for the memory fixture.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
pub const GRANTS: Grants = Grants { inspect: true, modify: true, shell: true };

/// Builds a validated path component from fixture bytes, which must already satisfy name constraints.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn name(bytes: &[u8]) -> Name {
    Name::new(bytes.into()).expect("a test name")
}

/// The longest authority `limits` take: a working directory of as many names
/// as fit, a repository at the root, so that a place's path is as long as an
/// absolute one, and the others at mounts as long as they may be.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn authority(limits: &Limits) -> Authority {
    let len = usize::try_from(limits.path_bytes).expect("a small limit");
    let names = len.div_ceil(2);
    let cwd: Box<[Name]> = (0..names).map(|_| name(b"a")).collect();
    let mut repos = vec![Repo {
        host: Box::new([name(&vec![b'h'; usize::try_from(limits.host_path_bytes).expect("bounded host path")])]),
        mount: Box::new([]),
        root: Token::new(0),
        writable: true,
    }];
    for repo in 1..limits.repos {
        let mount = format!("{repo:0>len$}");
        repos.push(Repo {
            host: Box::new([name(&vec![b'h'; usize::try_from(limits.host_path_bytes).expect("bounded host path")])]),
            mount: Box::new([name(mount.as_bytes())]),
            root: Token::new(repo.into()),
            writable: false,
        });
    }
    Authority { cwd, repos: repos.into(), grants: GRANTS }
}

/// The path of a file whose absolute path is exactly `path_bytes` long,
/// distinct for each `file`.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn path(limits: &Limits, file: u64) -> Path {
    let len = usize::try_from(limits.path_bytes - 1).expect("a small limit");
    let name = format!("f{file:0>len$}");
    let parts = Box::new([Part::Name { name: Name::new(name.into_bytes().into()).expect("a name") }]);
    Path { absolute: true, parts }
}

/// Builds a read for the fixture file under the supplied path limits.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn read(limits: &Limits, file: u64) -> Call {
    Call::Read { path: path(limits, file), offset: 1, window: smith_domain_tools::Budget::Asked(limits.tool_payload) }
}

/// The most a file holds.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn full(limits: &Limits, byte: u8) -> Box<[u8]> {
    vec![byte; usize::try_from(limits.file_bytes).expect("a small limit")].into()
}

/// A write of a file the kit read, so that it has a version to expect.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn write(limits: &Limits, file: u64) -> Call {
    Call::Write {
        path: path(limits, file),
        content: vec![b'x'; usize::try_from(limits.tool_payload).expect("a small piece")].into(),
    }
}

/// An edit of a file the kit read, with snippets as long as they may be,
/// which the job holds while it loads the file.
///
/// World contract: domain/tools.md, sections 7 and 9; testing-strategy.md, section 2.2.
#[must_use]
pub fn edit(limits: &Limits, file: u64) -> Call {
    let edits = (0..limits.tool_payload)
        .map(|_| smith_domain_tools::Replacement {
            old: b"x".as_slice().into(),
            new: b"y".as_slice().into(),
            occurrences: smith_domain_tools::Occurrences::All,
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Call::Edit { path: path(limits, file), edits }
}
