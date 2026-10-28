//! Feed the domain events, inspect the requests that come out; and the pure
//! functions on paths, by tables.

use alloc::boxed::Box;

use skein_lib::{Duration, Env, Id, List, Queue, ReplyTo, Time, Token, Wall};

use crate::Replacement as Edit;
use crate::authority::{self, Located, Target};
use crate::edit;
use crate::kit::Kit;
use crate::knowledge::Knowledge;
use crate::path;
use crate::window::{self, Span};
use crate::{
    Authority, Call, Domain, Done, Effect, Entry, Event, Exit, Expect, Fault, Grants, Hit, Kind, Limits, Name, Op,
    Outcome, Part, Path, Place, Refusal, Repo, Request, Version, effect, max_out, step, valid,
};
use crate::{FactKind as Fact, Tool, Verdict};

const LIMITS: Limits = Limits {
    shell_output: 192,
    kits: 2,
    calls: 4,
    repos: 4,
    path_bytes: 64,
    host_path_bytes: 4096,
    known_files: 8,
    file_bytes: 1024,
    tool_payload: 1024,
    read_bytes: 256,
    list_entries: 16,
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

const ALL: Grants = Grants { inspect: true, modify: true, shell: true };

/// The domain, its environment, and room for one step's output.
struct Harness {
    observed: Queue<crate::facts::Fact>,
    domain: Domain,
    env: Env<Limits>,
    out: Queue<Request>,
}

impl Harness {
    fn keep_facts(&mut self) {
        while let Some(fact) = self.domain.pop_fact() {
            self.observed.push(fact);
        }
    }

    fn pop_fact(&mut self) -> Option<crate::facts::Fact> {
        match self.observed.pop() {
            Some(fact) => Some(fact),
            None => self.domain.pop_fact(),
        }
    }

    fn new(limits: Limits) -> Harness {
        Harness {
            observed: Queue::with_capacity(4096),
            domain: Domain::new(&limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            out: Queue::with_capacity(max_out(&limits)),
        }
    }

    fn step(&mut self, event: Event) -> Option<Request> {
        self.keep_facts();
        step(&mut self.domain, &self.env, event, &mut self.out);
        let request = self.out.pop();
        assert!(self.out.is_empty(), "one request at most");
        request
    }

    /// Every request one event emits.
    fn emit(&mut self, event: Event) -> Box<[Request]> {
        self.keep_facts();
        step(&mut self.domain, &self.env, event, &mut self.out);
        let max_out = max_out(&self.env.limits);
        let mut requests = List::with_capacity(max_out);
        for _ in 0..max_out {
            if let Some(request) = self.out.pop() {
                requests.push(request).expect("no more than max_out");
            }
        }
        assert!(self.out.is_empty(), "no more than max_out");
        requests.into_boxed()
    }

    /// Sends `call` to `kit`, as call `name`, with a minute to run.
    fn send(&mut self, kit: Token, name: u64, call: Call) -> Option<Request> {
        let reply_to = ReplyTo::new(Token::new(name));
        let deadline = self.env.now.saturating_add(Duration::from_secs(60));
        self.step(Event::Call { kit, observation: None, reply_to, call, deadline })
    }

    /// Sends `call` to `kit`, which asks io for an operation, and returns it.
    fn start(&mut self, kit: Token, name: u64, call: Call) -> (Token, Op) {
        match self.send(kit, name, call) {
            Some(Request::Io { owner, op, deadline }) => {
                let limit = self.env.now.saturating_add(self.env.limits.file_timeout);
                assert_eq!(deadline, limit, "a file operation has the tools' own deadline, the sooner");
                (owner, op)
            }
            other => panic!("expected an operation, not {other:?}"),
        }
    }

    /// Ends the operation of `owner` with `done`, and returns the answer.
    fn end(&mut self, owner: Token, done: Done) -> (u64, Outcome) {
        match self.step(Event::Done { owner, done }) {
            Some(Request::Answer { to, outcome }) => (to.into_token().raw(), outcome),
            other => panic!("expected an answer, not {other:?}"),
        }
    }

    /// Ends the operation of `owner` with `done`, and returns the operation
    /// its flight asks for next.
    fn next(&mut self, owner: Token, done: Done) -> Op {
        match self.step(Event::Done { owner, done }) {
            Some(Request::Io { owner: again, op, .. }) => {
                assert_eq!(again, owner, "a flight asks for one operation at a time");
                op
            }
            other => panic!("expected another operation, not {other:?}"),
        }
    }

    fn knowledge(&self, kit: Token) -> &Knowledge {
        let kit: &Kit = self.domain.kits.get(Id::from_token(kit)).expect("the kit is open");
        &kit.knowledge
    }

    /// Opens a kit for `session` with `authority`, and returns its token.
    fn open(&mut self, session: u64, authority: Authority) -> Token {
        let session = Token::new(session);
        match self.step(Event::Open { jobs: 0, session, authority }) {
            Some(Request::Opened { session: echoed, kit }) => {
                assert_eq!(echoed, session);
                kit
            }
            other => panic!("expected the kit to open, not {other:?}"),
        }
    }

    /// Runs `call` with `kit`, and returns its answer.
    fn call(&mut self, kit: Token, call: Call) -> Outcome {
        let reply_to = ReplyTo::new(Token::new(99));
        let deadline = Time::ZERO.saturating_add(Duration::from_secs(60));
        match self.step(Event::Call { kit, observation: None, reply_to, call, deadline }) {
            Some(Request::Answer { to, outcome }) => {
                assert_eq!(to.into_token(), Token::new(99));
                outcome
            }
            other => panic!("expected an answer, not {other:?}"),
        }
    }
}

/// `text` split at its slashes, as the protocol layer would split it.
fn path(text: &[u8]) -> Path {
    let mut parts = List::with_capacity(u32::try_from(text.len()).expect("a short path"));
    let mut start = 0;
    for (index, byte) in text.iter().enumerate() {
        if *byte == b'/' {
            push(&mut parts, &text[start..index]);
            start = index.checked_add(1).expect("within the text");
        }
    }
    push(&mut parts, &text[start..]);
    Path { absolute: text.first() == Some(&b'/'), parts: parts.into_boxed() }
}

fn push(parts: &mut List<Part>, part: &[u8]) {
    let part = match part {
        b"" => return,
        b"." => Part::Current,
        b".." => Part::Parent,
        name => Part::Name { name: Name::new(Box::from(name)).expect("a test name") },
    };
    parts.push(part).expect("room for every part");
}

/// The names of an absolute path.
fn names(text: &[u8]) -> Box<[Name]> {
    let path = path(text);
    assert!(path.absolute, "names are of absolute paths");
    let mut names = List::with_capacity(u32::try_from(path.parts.len()).expect("a short path"));
    for part in path.parts {
        match part {
            Part::Name { name } => names.push(name).expect("room for every name"),
            Part::Current | Part::Parent => panic!("names are normalised"),
        }
    }
    names.into_boxed()
}

/// A checkout at /work, where relative paths start in temper, which may be
/// written; a library vendored in it and the docs beside it may not.
fn authority(grants: Grants) -> Authority {
    Authority {
        cwd: names(b"/work/temper"),
        repos: Box::new([
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/work/temper"),
                root: Token::new(1),
                writable: true,
            },
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/work/temper/vendor/lib"),
                root: Token::new(2),
                writable: false,
            },
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/work/docs"),
                root: Token::new(3),
                writable: false,
            },
        ]),
        grants,
    }
}

fn bytes(text: &[u8]) -> Box<[u8]> {
    Box::from(text)
}

fn located(root: u64, path: &[u8], writable: bool) -> Located {
    Located { place: Place { root: Token::new(root), path: Box::from(path) }, writable }
}

fn read(text: &[u8]) -> Call {
    Call::Read { path: path(text), offset: 1, window: crate::Budget::Default }
}

fn write(text: &[u8], size: usize) -> Call {
    Call::Write { path: path(text), content: Box::from(&[b'x'; 2048][..size]) }
}

fn edit(text: &[u8]) -> Call {
    Call::Edit {
        path: path(text),
        edits: Box::new([crate::Replacement {
            old: bytes(b"a"),
            new: bytes(b"b"),
            occurrences: crate::Occurrences::Once,
        }]),
    }
}

/// Opening a kit with `authority` under `limits` is refused as invalid.
fn invalid(limits: Limits, authority: Authority) {
    let mut h = Harness::new(limits);
    let refused = h.step(Event::Open { jobs: 0, session: Token::new(1), authority });
    assert_eq!(refused, Some(Request::Refused { session: Token::new(1), refusal: Refusal::Invalid }));
    assert_eq!(h.domain.kits(), 0);
}

#[test]
fn names_are_never_empty_dots_or_slashes() {
    let table: [(&[u8], bool); 9] = [
        (b"lib.rs", true),
        (b".git", true),
        (b"...", true),
        (b"", false),
        (b".", false),
        (b"..", false),
        (b"src/lib.rs", false),
        (b"/", false),
        (b"a\0b", false),
    ];
    for (bytes, valid) in table {
        let made = Name::new(Box::from(bytes));
        assert_eq!(made.is_some(), valid, "{bytes:?}");
        if let Some(made) = made {
            assert_eq!(made.as_bytes(), bytes);
        }
    }
}

#[test]
fn reading_calls_have_the_read_effect_and_the_rest_write() {
    let table = [
        (Call::Read { path: path(b"a"), offset: 1, window: crate::Budget::Default }, Effect::Read),
        (Call::List { path: path(b"."), depth: crate::Depth::Whole, glob: None }, Effect::Read),
        (Call::Search { path: path(b"."), pattern: bytes(b"fn main"), glob: None }, Effect::Read),
        (Call::Write { path: path(b"a"), content: bytes(b"") }, Effect::Write),
        (
            Call::Edit {
                path: path(b"a"),
                edits: Box::new([crate::Replacement {
                    old: bytes(b"x"),
                    new: bytes(b"y"),
                    occurrences: crate::Occurrences::Once,
                }]),
            },
            Effect::Write,
        ),
        (
            Call::Shell {
                after: crate::Yield::Never,
                command: bytes(b"ls"),
                timeout: crate::Timeout::Default,
                output: crate::Budget::Default,
            },
            Effect::Write,
        ),
    ];
    for (call, expected) in table {
        assert_eq!(effect(&call), expected, "{call:?}");
    }
}

#[test]
fn a_path_normalises_to_the_absolute_names_it_stands_for() {
    let cwd = names(b"/work/temper");
    let table: [(&[u8], Option<&[u8]>); 16] = [
        (b"src/lib.rs", Some(b"work/temper/src/lib.rs")),
        (b"./src/./lib.rs", Some(b"work/temper/src/lib.rs")),
        (b"src//lib.rs/", Some(b"work/temper/src/lib.rs")),
        (b"src/../Cargo.toml", Some(b"work/temper/Cargo.toml")),
        (b"", Some(b"work/temper")),
        (b".", Some(b"work/temper")),
        (b"..", Some(b"work")),
        (b"a/b/../../..", Some(b"work")),
        // `..` at the root stays at the root.
        (b"../../..", Some(b"")),
        (b"../../../etc/passwd", Some(b"etc/passwd")),
        (b"a/../../../../x/..", Some(b"")),
        (b"/etc/passwd", Some(b"etc/passwd")),
        (b"/", Some(b"")),
        (b"/..", Some(b"")),
        (b"/work/temper/../docs/x", Some(b"work/docs/x")),
        // Past `path_bytes` (64) joined.
        (b"0123456789/0123456789/0123456789/0123456789/0123456789", None),
    ];
    for (text, expected) in table {
        let normalised = path::normalise(&cwd, &path(text), LIMITS.path_bytes);
        assert_eq!(normalised.as_deref(), expected, "{text:?}");
    }
    // Exactly at the limit is within it.
    let at_limit = path::normalise(&[], &path(b"/abcd"), 4);
    assert_eq!(at_limit.as_deref(), Some(&b"abcd"[..]));
    assert_eq!(path::normalise(&[], &path(b"/abcde"), 4), None);
}

#[test]
fn a_mount_holds_itself_and_what_is_beneath_it() {
    let table: [(&[u8], &[u8], Option<usize>); 7] = [
        (b"", b"", Some(0)),
        (b"", b"etc/passwd", Some(0)),
        (b"work", b"work", Some(4)),
        (b"work", b"work/temper", Some(5)),
        (b"work", b"workshop", None),
        (b"work/temper", b"work", None),
        (b"work/temper", b"etc", None),
    ];
    for (mount, at, expected) in table {
        assert_eq!(authority::beneath(mount, at), expected, "{mount:?} {at:?}");
    }
}

#[test]
fn a_path_is_in_the_repository_with_the_longest_mount_that_holds_it() {
    let checkout = authority::admit(authority(ALL), &LIMITS).expect("the authority fits");
    let table: [(&[u8], Result<Located, Outcome>); 13] = [
        (b"src/lib.rs", Ok(located(1, b"src/lib.rs", true))),
        (b".", Ok(located(1, b"", true))),
        (b"/work/temper/src", Ok(located(1, b"src", true))),
        (b"vendor/lib/x.rs", Ok(located(2, b"x.rs", false))),
        (b"vendor/lib", Ok(located(2, b"", false))),
        (b"vendor/lib/../lib2", Ok(located(1, b"vendor/lib2", true))),
        (b"vendor/library/x.rs", Ok(located(1, b"vendor/library/x.rs", true))),
        (b"../docs/guide.md", Ok(located(3, b"guide.md", false))),
        (b"..", Err(Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"work") } })),
        (
            b"../temperance/x",
            Err(Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"work/temperance/x") } }),
        ),
        (b"/etc/passwd", Err(Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc/passwd") } })),
        (
            b"../../../../etc/passwd",
            Err(Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc/passwd") } }),
        ),
        (b"a/very/long/path/that/does/not/fit/in/the/sixty/four/bytes/allowed", Err(Outcome::TooLong)),
    ];
    for (text, expected) in table {
        let located = match authority::locate(&checkout, &path(text), LIMITS.path_bytes) {
            Ok(Target::Place(located)) => Ok(located),
            Ok(Target::Directories) => panic!("a place row"),
            Err(outcome) => Err(outcome),
        };
        assert_eq!(located, expected, "{text:?}");
    }
}

#[test]
fn a_repository_mounted_at_the_root_holds_what_no_other_does() {
    let mut authority = authority(ALL);
    authority.repos = Box::new([
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/"),
            root: Token::new(7),
            writable: false,
        },
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/work/temper"),
            root: Token::new(1),
            writable: true,
        },
    ]);
    let checkout = authority::admit(authority, &LIMITS).expect("the authority fits");
    let table: [(&[u8], Result<Located, Outcome>); 3] = [
        (b"/etc/passwd", Ok(located(7, b"etc/passwd", false))),
        (b"src", Ok(located(1, b"src", true))),
        (b".", Ok(located(1, b"", true))),
    ];
    for (text, expected) in table {
        assert_eq!(
            authority::locate(&checkout, &path(text), LIMITS.path_bytes),
            match expected {
                Ok(located) => Ok(Target::Place(located)),
                Err(outcome) => Err(outcome),
            },
            "{text:?}"
        );
    }
}

#[test]
fn a_kit_opens_and_closes() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(5, authority(ALL));
    assert_eq!(h.domain.kits(), 1);
    assert_eq!(h.step(Event::Close { kit }), Some(Request::Closed { session: Token::new(5) }));
    assert_eq!(h.domain.kits(), 1, "a closed kit stays until the reclaim point");
    h.domain.reclaim();
    assert_eq!(h.domain.kits(), 0);
}

#[test]
fn opens_beyond_the_kit_slots_are_refused_as_busy() {
    let mut h = Harness::new(Limits { kits: 1, ..LIMITS });
    let _: Token = h.open(1, authority(ALL));
    let refused = h.step(Event::Open { jobs: 0, session: Token::new(2), authority: authority(ALL) });
    assert_eq!(refused, Some(Request::Refused { session: Token::new(2), refusal: Refusal::Busy }));
}

#[test]
fn authorities_beyond_the_limits_are_refused_as_invalid() {
    invalid(Limits { repos: 2, ..LIMITS }, authority(ALL));
    // Two repositories at one mount, or with one root.
    let mut twice = authority(ALL);
    twice.repos = Box::new([
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/work/temper"),
            root: Token::new(1),
            writable: false,
        },
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/work/temper"),
            root: Token::new(2),
            writable: true,
        },
    ]);
    invalid(LIMITS, twice);
    let mut shared = authority(ALL);
    shared.repos = Box::new([
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/work/temper"),
            root: Token::new(1),
            writable: true,
        },
        Repo {
            host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
            mount: names(b"/work/docs"),
            root: Token::new(1),
            writable: false,
        },
    ]);
    invalid(LIMITS, shared);
    // The vendored library's mount, work/temper/vendor/lib, is 22 bytes.
    invalid(Limits { path_bytes: 21, ..LIMITS }, authority(ALL));
    let mut deep = authority(ALL);
    deep.cwd = names(b"/a/very/deep/working/directory/that/is/longer/than/the/limit/allows");
    invalid(LIMITS, deep);
}

#[test]
fn calls_are_refused_at_the_entrance() {
    let inspect = Grants { inspect: true, modify: false, shell: false };
    let modify = Grants { inspect: false, modify: true, shell: false };
    let shell = Call::Shell {
        after: crate::Yield::Never,
        command: bytes(b"cargo test"),
        timeout: crate::Timeout::Default,
        output: crate::Budget::Default,
    };
    let table = [
        (modify, read(b"src/lib.rs"), Outcome::NotGranted),
        (modify, Call::List { path: path(b"."), depth: crate::Depth::Whole, glob: None }, Outcome::NotGranted),
        (inspect, write(b"src/lib.rs", 1), Outcome::NotGranted),
        (inspect, edit(b"src/lib.rs"), Outcome::NotGranted),
        (inspect, shell.clone(), Outcome::NotGranted),
        (
            inspect,
            read(b"/etc/passwd"),
            Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc/passwd") } },
        ),
        (
            inspect,
            Call::List { path: path(b".."), depth: crate::Depth::Whole, glob: None },
            Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"work") } },
        ),
        (
            modify,
            write(b"../../tmp/x", 1),
            Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"tmp/x") } },
        ),
        (
            modify,
            write(b"../docs/guide.md", 1),
            Outcome::ReadOnly { path: path::Resolved::Working { path: bytes(b"docs/guide.md") } },
        ),
        (
            modify,
            edit(b"vendor/lib/x.rs"),
            Outcome::ReadOnly { path: path::Resolved::Working { path: bytes(b"temper/vendor/lib/x.rs") } },
        ),
        (
            modify,
            write(b"src/big.rs", 1025),
            Outcome::TooLarge { what: crate::SizePart::WriteContent, size: 1025, bound: 1024 },
        ),
        (modify, write(b".git/config", 1), Outcome::Protected),
        (modify, write(b"src/../.git/hooks/pre-commit", 1), Outcome::Protected),
        (modify, edit(b".git/HEAD"), Outcome::Protected),
        // A repository mounted inside another has its own .git.
        (
            modify,
            write(b"vendor/lib/.git/config", 1),
            Outcome::ReadOnly { path: path::Resolved::Working { path: bytes(b"temper/vendor/lib/.git/config") } },
        ),
        (inspect, read(b"a/very/long/path/that/does/not/fit/in/the/sixty/four/bytes/allowed"), Outcome::TooLong),
    ];
    let mut h = Harness::new(Limits { kits: 32, ..LIMITS });
    for (index, (grants, call, expected)) in table.into_iter().enumerate() {
        let kit = h.open(u64::try_from(index).expect("a small index"), authority(grants));
        assert_eq!(h.call(kit, call.clone()), expected, "{call:?}");
    }
}

#[test]
fn the_most_a_step_emits_covers_closing_a_busy_kit() {
    assert_eq!(max_out(&LIMITS), 12);
}

#[test]
fn a_small_call_limit_reserves_held_job_outputs() {
    assert_eq!(max_out(&Limits { calls: 1, ..LIMITS }), 9);
}

#[test]
fn invalid_limits_are_refused() {
    assert!(valid(&LIMITS));
    // One shell capture's head and tail share a u32 byte budget.
    assert!(!valid(&Limits { shell_head: u32::MAX, ..LIMITS }));
    // Every native entrance can emit its reserved observations.
    assert!(!valid(&Limits { facts: crate::max_facts(&LIMITS) - 1, ..LIMITS }));
    // One listing entry and its nonempty name must fit.
    assert!(!valid(&Limits { list_bytes: u64::try_from(size_of::<Entry>()).expect("entry fits"), ..LIMITS }));
    // A kit's active and retained flight counts must add without overflow.
    assert!(!valid(&Limits { kits: 0, calls: u32::MAX, jobs: 1, ..LIMITS }));
    // Every kit's flights must fit the slab, before retiring slots are added.
    assert!(!valid(&Limits { kits: u32::MAX, calls: 2, jobs: 0, ..LIMITS }));
    // Retiring flights need a second generation of slots.
    assert!(!valid(&Limits { kits: u32::MAX / 2 + 1, calls: 1, jobs: 0, ..LIMITS }));
    // Path normalisation's component count must be representable.
    assert!(!valid(&Limits { path_bytes: u32::MAX, ..LIMITS }));
    // Host-path normalisation has an independent component count.
    assert!(!valid(&Limits { host_path_bytes: u32::MAX, ..LIMITS }));
}

fn place(root: u64, path: &[u8]) -> Place {
    Place { root: Token::new(root), path: Box::from(path) }
}

fn version(n: u64) -> Version {
    Version::new([n, 0, 0, 0])
}

fn entry(name: &[u8], kind: Kind) -> Entry {
    Entry { name: Box::from(name), kind }
}

fn span(offset: u32, bytes: u32) -> Span {
    Span { offset, window: window::Window { bytes, clamp: crate::BytesClamp::Within } }
}

fn read_of(content: &[u8], skipped: u32, lines: u32, total: u32, cut: bool) -> Outcome {
    let after = skipped.checked_add(lines).expect("fixture line count");
    Outcome::Read {
        content: Box::from(content),
        first: (skipped).saturating_add(1),
        lines,
        total,
        cut: if cut { crate::Cut::Line } else { crate::Cut::Whole },
        next: if after < total {
            crate::Next::Line(after.checked_add(1).expect("fixture next line"))
        } else {
            crate::Next::End
        },
        window: crate::BytesClamp::Within,
    }
}

#[test]
fn a_read_answers_with_whole_lines_within_its_limit() {
    let table = [
        (b"a\nb\nc\n".as_slice(), span(1, 100), read_of(b"a\nb\nc\n", 0, 3, 3, false)),
        (b"a\nb\nc", span(1, 100), read_of(b"a\nb\nc", 0, 3, 3, false)),
        (b"a\nb\nc\n", span(2, 100), read_of(b"b\nc\n", 1, 2, 3, false)),
        (b"a\nb\nc\n", span(2, 2), read_of(b"b\n", 1, 1, 3, false)),
        (b"a\nb\nc\n", span(4, 100), read_of(b"", 3, 0, 3, false)),
        (b"a\nb\nc\n", span(10, 100), read_of(b"", 3, 0, 3, false)),
        (b"aa\nbb\ncc\n", span(1, 6), read_of(b"aa\nbb\n", 0, 2, 3, false)),
        (b"aa\nbb\ncc\n", span(1, 5), read_of(b"aa\n", 0, 1, 3, false)),
        (b"", span(1, 4), read_of(b"", 0, 0, 0, false)),
        (b"\n\n", span(1, 4), read_of(b"\n\n", 0, 2, 2, false)),
        (b"aaaaaa\nb\n", span(1, 4), read_of(b"aaaa", 0, 1, 2, true)),
        (b"aaaaaa", span(1, 4), read_of(b"aaaa", 0, 1, 1, true)),
        (b"a\nbbbbbb\n", span(2, 4), read_of(b"bbbb", 1, 1, 2, true)),
    ];
    for (content, span, expected) in table {
        assert_eq!(window::read(Box::from(content), span).outcome, expected, "{content:?} {span:?}");
    }
}

#[test]
fn a_kit_forgets_the_file_read_longest_ago() {
    let mut knowledge = Knowledge::new(2);
    knowledge.record(place(1, b"a"), version(1));
    knowledge.record(place(1, b"b"), version(2));
    knowledge.record(place(1, b"a"), version(3));
    knowledge.record(place(1, b"c"), version(4));
    assert_eq!(knowledge.version(&place(1, b"a")), Some(version(3)));
    assert_eq!(knowledge.version(&place(1, b"b")), None, "b was read longest ago");
    assert_eq!(knowledge.version(&place(1, b"c")), Some(version(4)));
    assert_eq!(knowledge.version(&place(2, b"c")), None, "places are per repository");
    knowledge.forget(&place(1, b"a"));
    assert_eq!(knowledge.version(&place(1, b"a")), None);

    let mut amnesiac = Knowledge::new(0);
    amnesiac.record(place(1, b"a"), version(1));
    assert_eq!(amnesiac.version(&place(1, b"a")), None);
}

#[test]
fn a_read_loads_the_file_and_answers_with_its_window() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, op) =
        h.start(kit, 7, Call::Read { path: path(b"src/lib.rs"), offset: 2, window: crate::Budget::Asked(4) });
    assert_eq!(op, Op::Load { at: place(1, b"src/lib.rs"), max: LIMITS.file_bytes });
    assert_eq!(h.domain.flights(), 1);
    let content = Box::from(&b"one\ntwo\nthree\n"[..]);
    let (call, outcome) = h.end(owner, Done::Loaded { content, version: version(5) });
    let expected = read_of(b"two\n", 1, 1, 3, false);
    assert_eq!((call, outcome), (7, expected));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(5)));
    h.domain.reclaim();
    assert_eq!(h.domain.flights(), 0);
}

#[test]
fn a_read_of_nothing_is_not_found_and_the_kit_knows_it() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, _) = h.start(kit, 1, read(b"src/lib.rs"));
    drop(h.end(owner, Done::Loaded { content: bytes(b""), version: version(5) }));
    let (owner, _) = h.start(kit, 2, read(b"src/lib.rs"));
    assert_eq!(
        h.end(owner, Done::Missing),
        (2, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper/src/lib.rs") } })
    );
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), None);
}

#[test]
fn every_other_end_of_a_load_answers_for_itself() {
    let table = [
        (Done::NotFile, Outcome::NotFile),
        (Done::NotDirectory, Outcome::NotDirectory { path: path::Resolved::Working { path: bytes(b"temper/link") } }),
        (Done::TooLarge { size: 4096 }, Outcome::TooLarge { what: crate::SizePart::File, size: 4096, bound: 1024 }),
        (Done::Escapes, Outcome::Outside { path: path::Resolved::Working { path: bytes(b"temper/link") } }),
        (Done::Failed { fault: Fault::Denied }, Outcome::Failed { fault: Fault::Denied }),
        (Done::TimedOut, Outcome::TimedOut),
        (Done::Cancelled, Outcome::Cancelled),
    ];
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    for (done, expected) in table {
        let (owner, _) = h.start(kit, 1, read(b"link"));
        assert_eq!(h.end(owner, done), (1, expected));
    }
    assert_eq!(h.knowledge(kit).version(&place(1, b"link")), None);
}

#[test]
fn a_listing_scans_the_directory() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, op) = h.start(kit, 1, Call::List { path: path(b"vendor/lib"), depth: crate::Depth::Whole, glob: None });
    assert_eq!(
        op,
        Op::List {
            at: place(2, b""),
            depth: crate::Depth::Whole,
            glob: None,
            max: LIMITS.list_entries,
            max_bytes: LIMITS.list_bytes
        }
    );
    let entries: Box<[Entry]> = Box::new([entry(b"Cargo.toml", Kind::File), entry(b"src", Kind::Directory)]);
    let (_, outcome) = h.end(owner, Done::Listed { entries: entries.clone(), more: 3 });
    assert_eq!(outcome, Outcome::Listed { entries, more: 3 });

    let table = [
        (Done::Missing, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper/src") } }),
        (Done::NotDirectory, Outcome::NotDirectory { path: path::Resolved::Working { path: bytes(b"temper/src") } }),
        (Done::Escapes, Outcome::Outside { path: path::Resolved::Working { path: bytes(b"temper/src") } }),
        (Done::Failed { fault: Fault::Other }, Outcome::Failed { fault: Fault::Other }),
        (Done::TimedOut, Outcome::TimedOut),
        (Done::Cancelled, Outcome::Cancelled),
    ];
    for (done, expected) in table {
        let (owner, _) = h.start(kit, 1, Call::List { path: path(b"src"), depth: crate::Depth::Whole, glob: None });
        assert_eq!(h.end(owner, done), (1, expected));
    }
}

#[test]
fn calls_beyond_a_kits_room_are_busy() {
    let mut h = Harness::new(Limits { calls: 2, ..LIMITS });
    let kit = h.open(1, authority(ALL));
    let (first, _) = h.start(kit, 1, read(b"a"));
    drop(h.start(kit, 2, read(b"b")));
    assert_eq!(h.call(kit, read(b"c")), Outcome::Busy);
    // Another kit has room of its own.
    let other = h.open(2, authority(ALL));
    drop(h.start(other, 3, read(b"c")));
    // An answered call makes room at once.
    drop(h.end(first, Done::Missing));
    drop(h.start(kit, 4, read(b"c")));
}

#[test]
fn a_call_past_its_deadline_times_out_at_the_entrance() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    h.env.now = Time::ZERO.saturating_add(Duration::from_secs(5));
    let reply_to = ReplyTo::new(Token::new(1));
    let call = Event::Call { kit, observation: None, reply_to, call: read(b"a"), deadline: h.env.now };
    let answer = h.step(call);
    assert_eq!(answer, Some(Request::Answer { to: ReplyTo::new(Token::new(1)), outcome: Outcome::TimedOut }));
    // A call due sooner than the tools' own limit keeps its own deadline.
    let soon = h.env.now.saturating_add(Duration::from_secs(1));
    let call =
        Event::Call { kit, observation: None, reply_to: ReplyTo::new(Token::new(2)), call: read(b"a"), deadline: soon };
    match h.step(call) {
        Some(Request::Io { deadline, .. }) => assert_eq!(deadline, soon),
        other => panic!("expected an operation, not {other:?}"),
    }
}

#[test]
fn closing_a_kit_cancels_its_calls_and_ends_with_the_last() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(9, authority(ALL));
    let (first, _) = h.start(kit, 1, read(b"a"));
    let (second, _) = h.start(kit, 2, Call::List { path: path(b"."), depth: crate::Depth::Whole, glob: None });
    let cancels = h.emit(Event::Close { kit });
    let mut cancelled = [first, second];
    cancelled.sort_unstable();
    let expected: Box<[Request]> =
        Box::new([Request::CancelIo { owner: cancelled[0] }, Request::CancelIo { owner: cancelled[1] }]);
    assert_eq!(cancels, expected);
    assert_eq!(h.end(first, Done::Cancelled), (1, Outcome::Cancelled));
    // The listing won its race with the cancel.
    let last = h.emit(Event::Done { owner: second, done: Done::Listed { entries: Box::new([]), more: 0 } });
    let expected: Box<[Request]> = Box::new([
        Request::Answer {
            to: ReplyTo::new(Token::new(2)),
            outcome: Outcome::Listed { entries: Box::new([]), more: 0 },
        },
        Request::Closed { session: Token::new(9) },
    ]);
    assert_eq!(last, expected);
    h.domain.reclaim();
    assert_eq!((h.domain.kits(), h.domain.flights()), (0, 0));
}

#[test]
fn a_kit_whose_calls_end_and_start_in_one_iteration_has_room() {
    let mut h = Harness::new(Limits { kits: 1, calls: 1, ..LIMITS });
    let kit = h.open(1, authority(ALL));
    let (mut owner, _) = h.start(kit, 1, read(b"a"));
    for name in 2..10 {
        h.domain.reclaim();
        // The call ends, and the next starts before the reclaim point.
        drop(h.end(owner, Done::Missing));
        let (next, _) = h.start(kit, name, read(b"a"));
        assert_eq!(h.domain.flights(), 2, "the answered flight waits for the reclaim point");
        owner = next;
    }
}

fn store(at: Place, content: &[u8], expect: Expect) -> Op {
    Op::Store { at, content: Box::from(content), expect }
}

#[test]
fn a_write_creates_a_file_the_kit_knows_nothing_of() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, op) = h.start(kit, 1, write(b"src/new.rs", 3));
    assert_eq!(op, store(place(1, b"src/new.rs"), b"xxx", Expect::Absent));
    assert_eq!(h.end(owner, Done::Stored { version: version(4) }), (1, Outcome::Written { created: true }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/new.rs")), Some(version(4)));
    // Its LLM wrote it, so it may write it again.
    let (owner, op) = h.start(kit, 2, write(b"src/new.rs", 1));
    assert_eq!(op, store(place(1, b"src/new.rs"), b"x", Expect::Is { version: version(4) }));
    assert_eq!(h.end(owner, Done::Stored { version: version(5) }), (2, Outcome::Written { created: false }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/new.rs")), Some(version(5)));
}

#[test]
fn a_write_replaces_a_file_at_the_version_its_llm_read() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, _) = h.start(kit, 1, read(b"src/lib.rs"));
    drop(h.end(owner, Done::Loaded { content: bytes(b"old\n"), version: version(7) }));
    let (owner, op) = h.start(kit, 2, write(b"./src/../src/lib.rs", 2));
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"xx", Expect::Is { version: version(7) }), "places are normalised");
    assert_eq!(h.end(owner, Done::Stored { version: version(8) }), (2, Outcome::Written { created: false }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(8)));
}

#[test]
fn a_write_that_conflicts_with_the_current_file_is_refused() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    // Creating over a file the LLM never read, even one removed before io
    // could tell its version.
    let (owner, _) = h.start(kit, 1, write(b"src/lib.rs", 1));
    assert_eq!(h.end(owner, Done::Conflict { now: Some(version(3)) }), (1, Outcome::NotRead));
    let (owner, _) = h.start(kit, 1, write(b"src/lib.rs", 1));
    assert_eq!(h.end(owner, Done::Conflict { now: None }), (1, Outcome::NotRead));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), None, "the LLM still has not read it");

    // Replacing a file changed since the LLM read it.
    let (owner, _) = h.start(kit, 2, read(b"src/lib.rs"));
    drop(h.end(owner, Done::Loaded { content: bytes(b""), version: version(3) }));
    let (owner, _) = h.start(kit, 3, write(b"src/lib.rs", 1));
    assert_eq!(h.end(owner, Done::Conflict { now: Some(version(4)) }), (3, Outcome::Stale));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(3)), "what the LLM read");

    // Replacing a file removed since: the kit knows it is gone, and the next
    // write creates it.
    let (owner, _) = h.start(kit, 4, write(b"src/lib.rs", 1));
    assert_eq!(h.end(owner, Done::Conflict { now: None }), (4, Outcome::Stale));
    let (_, op) = h.start(kit, 5, write(b"src/lib.rs", 1));
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"x", Expect::Absent));
}

#[test]
fn a_write_over_a_file_moved_away_is_stale_and_the_next_write_creates_it() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    // A command moved the file the LLM read: the store finds nothing there.
    known(&mut h, kit, b"src/lib.rs", b"a", 2);
    let (owner, op) = h.start(kit, 1, write(b"src/lib.rs", 1));
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"x", Expect::Is { version: version(2) }));
    assert_eq!(h.end(owner, Done::Conflict { now: None }), (1, Outcome::Stale));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), None, "the kit forgets the file");
    let (owner, op) = h.start(kit, 2, write(b"src/lib.rs", 1));
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"x", Expect::Absent));
    assert_eq!(h.end(owner, Done::Stored { version: version(3) }), (2, Outcome::Written { created: true }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(3)));
    // An edit whose file went between its load and its store forgets it too.
    let (owner, _) = h.start(kit, 3, change(b"src/lib.rs", b"x", b"y"));
    let op = h.next(owner, Done::Loaded { content: bytes(b"x"), version: version(3) });
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"y", Expect::Is { version: version(3) }));
    assert_eq!(h.end(owner, Done::Conflict { now: None }), (3, Outcome::Stale));
    let (_, op) = h.start(kit, 4, write(b"src/lib.rs", 1));
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"x", Expect::Absent));
}

#[test]
fn every_other_end_of_a_store_answers_for_itself() {
    let table = [
        (Done::NotFile, Outcome::NotFile),
        (Done::Linked, Outcome::Linked),
        (Done::NotDirectory, Outcome::NotDirectory { path: path::Resolved::Working { path: bytes(b"temper/link") } }),
        // More than io stores, named against the tools' own bound on a file.
        (Done::TooLarge { size: 4096 }, Outcome::TooLarge { what: crate::SizePart::File, size: 4096, bound: 1024 }),
        (Done::Escapes, Outcome::Outside { path: path::Resolved::Working { path: bytes(b"temper/link") } }),
        (Done::Failed { fault: Fault::NoSpace }, Outcome::Failed { fault: Fault::NoSpace }),
        (Done::TimedOut, Outcome::TimedOut),
        (Done::Cancelled, Outcome::Cancelled),
    ];
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, _) = h.start(kit, 1, read(b"link"));
    drop(h.end(owner, Done::Loaded { content: bytes(b""), version: version(2) }));
    for (done, expected) in table {
        let (owner, op) = h.start(kit, 1, write(b"link", 1));
        assert_eq!(op, store(place(1, b"link"), b"x", Expect::Is { version: version(2) }));
        assert_eq!(h.end(owner, done), (1, expected));
        // Whether or not the store happened, the next one's check settles it.
        assert_eq!(h.knowledge(kit).version(&place(1, b"link")), Some(version(2)));
    }
}

#[test]
fn a_store_that_finds_nothing_on_its_path_is_not_found_and_the_kit_forgets_the_file() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let gone = (1, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper/src/new_dir/file.rs") } });
    // A create whose directory io could not make (temper-073).
    let (owner, op) = h.start(kit, 1, write(b"src/new_dir/file.rs", 1));
    assert_eq!(op, store(place(1, b"src/new_dir/file.rs"), b"x", Expect::Absent));
    assert_eq!(h.end(owner, Done::Missing), gone);
    // A replace or an edit of a file whose directory went since it was read.
    known(&mut h, kit, b"src/new_dir/file.rs", b"a", 2);
    let (owner, op) = h.start(kit, 1, write(b"src/new_dir/file.rs", 1));
    assert_eq!(op, store(place(1, b"src/new_dir/file.rs"), b"x", Expect::Is { version: version(2) }));
    assert_eq!(h.end(owner, Done::Missing), gone);
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/new_dir/file.rs")), None);
    known(&mut h, kit, b"src/new_dir/file.rs", b"a", 3);
    let (owner, op) = h.start(kit, 1, change(b"src/new_dir/file.rs", b"a", b"b"));
    assert_eq!(op, Op::Load { at: place(1, b"src/new_dir/file.rs"), max: LIMITS.file_bytes });
    let op = h.next(owner, Done::Loaded { content: bytes(b"a"), version: version(3) });
    assert_eq!(op, store(place(1, b"src/new_dir/file.rs"), b"b", Expect::Is { version: version(3) }));
    assert_eq!(h.end(owner, Done::Missing), gone);
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/new_dir/file.rs")), None);
    // The kit lives on: the next write creates the file.
    let (_, op) = h.start(kit, 2, write(b"src/new_dir/file.rs", 1));
    assert_eq!(op, store(place(1, b"src/new_dir/file.rs"), b"x", Expect::Absent));
}

#[test]
fn a_session_keeps_its_first_file_after_seventeen_reads() {
    // One more file than the old product limit (domain/tools.md, section 4).
    const READ_FILES: u8 = 17;
    let mut h = Harness::new(derived_limits());
    let kit = h.open(1, authority(ALL));
    for file in 0..READ_FILES {
        known(&mut h, kit, &[b'a' + file], b"old\n", 1);
    }
    let (owner, op) = h.start(kit, 2, write(b"a", 2));
    assert_eq!(op, store(place(1, b"a"), b"xx", Expect::Is { version: version(1) }));
    assert_eq!(h.end(owner, Done::Stored { version: version(18) }), (2, Outcome::Written { created: false }));
}

fn derived_limits() -> Limits {
    crate::derive(&crate::Derivation {
        conversations: LIMITS.kits,
        calls_per_response: LIMITS.calls,
        jobs: LIMITS.jobs,
        parallel_tools: LIMITS.calls,
        tool_payload: LIMITS.tool_payload,
        file_bytes: LIMITS.file_bytes,
        read_window: LIMITS.read_bytes,
        shell_output: LIMITS.shell_output,
        shell_head: LIMITS.shell_head,
        shell_tail: LIMITS.shell_tail,
        search_hits: LIMITS.search_hits,
        search_bytes: LIMITS.search_bytes,
        list_entries: LIMITS.list_entries,
        tool_deadline: LIMITS.tool_deadline,
        shell_timeout: LIMITS.shell_timeout,
        job_poll: LIMITS.job_poll,
        job_wait: LIMITS.job_wait,
    })
    .expect("fixture declarations derive")
}

#[test]
fn a_kit_that_forgot_a_file_must_read_it_again_to_change_it() {
    let mut h = Harness::new(Limits { known_files: 1, ..LIMITS });
    let kit = h.open(1, authority(ALL));
    for (name, file) in [(1, &b"a"[..]), (2, b"b")] {
        let (owner, _) = h.start(kit, name, read(file));
        drop(h.end(owner, Done::Loaded { content: bytes(b""), version: version(name) }));
    }
    let (_, op) = h.start(kit, 3, write(b"a", 1));
    assert_eq!(op, store(place(1, b"a"), b"x", Expect::Absent), "a was forgotten for b");
}

fn snippet(old: &[u8], new: &[u8], all: bool) -> Edit {
    Edit {
        old: Box::from(old),
        new: Box::from(new),
        occurrences: if all { crate::Occurrences::All } else { crate::Occurrences::Once },
    }
}

fn edited(content: &[u8], replaced: u32) -> (Box<[u8]>, u32) {
    (Box::from(content), replaced)
}

/// What an edit makes of a file: its new content and how many occurrences it
/// replaced, or the outcome that refuses it.
type Applied = Result<(Box<[u8]>, u32), Outcome>;

fn ambiguous(count: u32, lines: &[u32]) -> Applied {
    Err(Outcome::EditFailed { edit: 1, why: crate::Mismatch::Ambiguous { count, lines: Box::from(lines) } })
}

#[test]
fn an_edit_replaces_its_one_match_or_all_of_them() {
    let small = Limits { file_bytes: 8, tool_payload: 8, match_lines: 2, ..LIMITS };
    let table: [(&[u8], Edit, &Limits, Applied); 10] = [
        (b"a b a", snippet(b"b", b"c", false), &LIMITS, Ok(edited(b"a c a", 1))),
        (b"a b a", snippet(b"a", b"c", false), &LIMITS, ambiguous(2, &[1, 1])),
        (b"x\na\ny\na\na\n", snippet(b"a", b"b", false), &LIMITS, ambiguous(3, &[2, 4, 5])),
        (b"x\na\ny\na\na\n", snippet(b"a", b"b", false), &small, ambiguous(3, &[2, 4])),
        (b"x\na\ny\na\na\n", snippet(b"a", b"bb", true), &LIMITS, Ok(edited(b"x\nbb\ny\nbb\nbb\n", 3))),
        (
            b"a b a",
            snippet(b"z", b"c", true),
            &LIMITS,
            Err(Outcome::EditFailed { edit: 1, why: crate::Mismatch::NoMatch }),
        ),
        // Matches do not overlap.
        (b"aaa", snippet(b"aa", b"b", false), &LIMITS, Ok(edited(b"ba", 1))),
        (b"abc", snippet(b"abc", b"", false), &LIMITS, Ok(edited(b"", 1))),
        (
            b"abc",
            snippet(b"b", b"0123456789", false),
            &small,
            Err(Outcome::TooLarge { what: crate::SizePart::EditResult, size: 12, bound: 8 }),
        ),
        (b"abc", snippet(b"b", b"012345", false), &small, Ok(edited(b"a012345c", 1))),
    ];
    for (content, edit, limits, expected) in table {
        assert_eq!(edit::apply(content, &edit, 1, limits), expected, "{content:?} {edit:?}");
    }
}

/// Reads `file` with `kit`, which finds it holding `content` at `version`.
fn known(h: &mut Harness, kit: Token, file: &[u8], content: &[u8], at: u64) {
    let (owner, _) = h.start(kit, 1, read(file));
    drop(h.end(owner, Done::Loaded { content: bytes(content), version: version(at) }));
}

fn change(text: &[u8], old: &[u8], new: &[u8]) -> Call {
    Call::Edit {
        path: path(text),
        edits: Box::new([crate::Replacement {
            old: bytes(old),
            new: bytes(new),
            occurrences: crate::Occurrences::Once,
        }]),
    }
}

#[test]
fn an_edit_without_an_earlier_read_stores_over_the_version_it_loaded() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, op) = h.start(kit, 2, change(b"src/lib.rs", b"one", b"uno"));
    assert_eq!(op, Op::Load { at: place(1, b"src/lib.rs"), max: LIMITS.file_bytes });
    let op = h.next(owner, Done::Loaded { content: bytes(b"fn one() {}\n"), version: version(3) });
    assert_eq!(op, store(place(1, b"src/lib.rs"), b"fn uno() {}\n", Expect::Is { version: version(3) }));
    assert_eq!(h.end(owner, Done::Stored { version: version(4) }), (2, Outcome::Edited { replaced: 1 }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(4)));
}

#[test]
fn an_edit_matches_the_current_file_and_catches_a_change_after_its_load() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    known(&mut h, kit, b"src/lib.rs", b"fn one() {}\n", 3);
    // The current bytes no longer match the requested old text.
    let (owner, _) = h.start(kit, 2, change(b"src/lib.rs", b"one", b"uno"));
    let loaded = Done::Loaded { content: bytes(b"fn two() {}\n"), version: version(5) };
    assert_eq!(h.end(owner, loaded), (2, Outcome::EditFailed { edit: 1, why: crate::Mismatch::NoMatch }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(3)), "what the LLM read");
    // Changed between the load and the store.
    let (owner, _) = h.start(kit, 3, change(b"src/lib.rs", b"one", b"uno"));
    let loaded = Done::Loaded { content: bytes(b"fn one() {}\n"), version: version(3) };
    drop(h.next(owner, loaded));
    assert_eq!(h.end(owner, Done::Conflict { now: Some(version(6)) }), (3, Outcome::Stale));
    // Removed since: not found, and forgotten.
    let (owner, _) = h.start(kit, 4, change(b"src/lib.rs", b"one", b"uno"));
    assert_eq!(
        h.end(owner, Done::Missing),
        (4, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper/src/lib.rs") } })
    );
    let (owner, _) = h.start(kit, 5, change(b"src/lib.rs", b"one", b"uno"));
    assert_eq!(h.end(owner, Done::Missing).0, 5);
}

#[test]
fn an_edit_that_matches_nothing_or_too_much_stores_nothing() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    known(&mut h, kit, b"src/lib.rs", b"a\nb\na\n", 3);
    let table = [
        (change(b"src/lib.rs", b"zzz", b"y"), Outcome::EditFailed { edit: 1, why: crate::Mismatch::NoMatch }),
        (
            change(b"src/lib.rs", b"a", b"y"),
            Outcome::EditFailed { edit: 1, why: crate::Mismatch::Ambiguous { count: 2, lines: Box::new([1, 3]) } },
        ),
    ];
    for (call, expected) in table {
        let (owner, _) = h.start(kit, 2, call);
        let loaded = Done::Loaded { content: bytes(b"a\nb\na\n"), version: version(3) };
        assert_eq!(h.end(owner, loaded), (2, expected));
    }
    let all = Call::Edit {
        path: path(b"src/lib.rs"),
        edits: Box::new([crate::Replacement {
            old: bytes(b"a"),
            new: bytes(b"y"),
            occurrences: crate::Occurrences::All,
        }]),
    };
    let (owner, _) = h.start(kit, 3, all);
    let loaded = Done::Loaded { content: bytes(b"a\nb\na\n"), version: version(3) };
    match h.next(owner, loaded) {
        Op::Store { content, .. } => assert_eq!(&*content, b"y\nb\ny\n"),
        op @ (Op::Load { .. } | Op::List { .. } | Op::Spawn { .. } | Op::Search { .. }) => {
            panic!("expected the store, not {op:?}")
        }
    }
}

#[test]
fn edit_shape_bounds_are_refused_before_io_but_semantics_follow_the_loaded_bytes() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let table = [
        (change(b"src/lib.rs", b"", b"b"), crate::Mismatch::NoMatch),
        (change(b"src/lib.rs", b"a", b"a"), crate::Mismatch::Unchanged),
    ];
    for (call, why) in table {
        let (owner, _) = h.start(kit, 1, call);
        assert_eq!(
            h.end(owner, Done::Loaded { content: bytes(b"a"), version: version(3) }),
            (1, Outcome::EditFailed { edit: 1, why })
        );
    }
    h.domain.reclaim();
    let big = Call::Edit { path: path(b"src/lib.rs"), edits: Box::new([snippet(b"a", &[b'x'; 1025], false)]) };
    assert_eq!(h.call(kit, big), Outcome::TooLarge { what: crate::SizePart::EditNew, size: 1025, bound: 1024 });
    assert_eq!(
        h.call(kit, change(b"vendor/lib/x.rs", b"a", b"b")),
        Outcome::ReadOnly { path: path::Resolved::Working { path: bytes(b"temper/vendor/lib/x.rs") } }
    );
    assert_eq!(h.domain.flights(), 0);
}

#[test]
fn a_kit_closing_while_an_edit_loads_stores_nothing() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(9, authority(ALL));
    known(&mut h, kit, b"src/lib.rs", b"a\n", 3);
    let (owner, _) = h.start(kit, 2, change(b"src/lib.rs", b"a", b"b"));
    assert_eq!(&*h.emit(Event::Close { kit }), &[Request::CancelIo { owner }]);
    // The load won its race with the cancel; the edit is cancelled all the same.
    let loaded = Done::Loaded { content: bytes(b"a\n"), version: version(3) };
    let expected = [
        Request::Answer { to: ReplyTo::new(Token::new(2)), outcome: Outcome::Cancelled },
        Request::Closed { session: Token::new(9) },
    ];
    assert_eq!(&*h.emit(Event::Done { owner, done: loaded }), &expected);
}

#[test]
fn a_kit_closing_while_an_edit_stores_answers_with_whichever_end_came() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(9, authority(ALL));
    known(&mut h, kit, b"src/lib.rs", b"a\n", 3);
    let (owner, _) = h.start(kit, 2, change(b"src/lib.rs", b"a", b"b"));
    let loaded = Done::Loaded { content: bytes(b"a\n"), version: version(3) };
    drop(h.next(owner, loaded));
    assert_eq!(&*h.emit(Event::Close { kit }), &[Request::CancelIo { owner }]);
    // The store won its race: it happened, and the kit knows it.
    let ended = h.emit(Event::Done { owner, done: Done::Stored { version: version(4) } });
    let expected = [
        Request::Answer { to: ReplyTo::new(Token::new(2)), outcome: Outcome::Edited { replaced: 1 } },
        Request::Closed { session: Token::new(9) },
    ];
    assert_eq!(&*ended, &expected);
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(4)));
}

#[test]
fn an_edit_whose_deadline_passed_while_it_loaded_stores_nothing() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    known(&mut h, kit, b"src/lib.rs", b"a\n", 3);
    let (owner, _) = h.start(kit, 2, change(b"src/lib.rs", b"a", b"b"));
    h.env.now = Time::ZERO.saturating_add(Duration::from_secs(60));
    let loaded = Done::Loaded { content: bytes(b"a\n"), version: version(3) };
    assert_eq!(h.end(owner, loaded), (2, Outcome::TimedOut));
}

#[test]
fn a_path_is_in_git_when_one_of_its_names_is_dot_git() {
    let table: [(&[u8], bool); 13] = [
        (b".git", true),
        (b".GIT/config", true),
        (b"src/.Git", true),
        (b"src/.gitx/.gIt/HEAD", true),
        (b".git/config", true),
        (b"src/.git", true),
        (b"vendor/lib/.git/hooks/post-checkout", true),
        (b"", false),
        (b"src/lib.rs", false),
        (b".github/workflows/ci.yml", false),
        (b"x.git", false),
        (b"src/.gitignore", false),
        (b"a.git/.gitx", false),
    ];
    for (path, expected) in table {
        assert_eq!(path::in_git(path), expected, "{path:?}");
    }
}

/// Sends a command to `kit` with an hour to run, and returns the spawn it asks
/// for and its deadline.
fn spawned(h: &mut Harness, kit: Token, timeout: Option<Duration>) -> (Token, Op, Time) {
    let reply_to = ReplyTo::new(Token::new(1));
    let call = Call::Shell {
        after: crate::Yield::Never,
        command: bytes(b"cargo test"),
        timeout: match timeout {
            Some(duration) => crate::Timeout::Asked(duration),
            None => crate::Timeout::Default,
        },
        output: crate::Budget::Default,
    };
    let deadline = h.env.now.saturating_add(Duration::from_secs(3600));
    match h.step(Event::Call { kit, observation: None, reply_to, call, deadline }) {
        Some(Request::Io { owner, op, deadline }) => (owner, op, deadline),
        other => panic!("expected a spawn, not {other:?}"),
    }
}

fn at(secs: u64) -> Time {
    Time::ZERO.saturating_add(Duration::from_secs(secs))
}

#[test]
fn a_command_runs_in_the_working_directory_with_the_kits_output_bounds() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let (owner, op, deadline) = spawned(&mut h, kit, None);
    let expected = Op::Spawn {
        cwd: place(1, b""),
        command: bytes(b"cargo test"),
        head: LIMITS.shell_head,
        tail: LIMITS.shell_tail,
        after: None,
    };
    assert_eq!((op, deadline), (expected, at(60)), "the tools' default limit for a command");
    let exited =
        Done::Exited { exit: Exit::Code { code: 101 }, head: bytes(b"running"), tail: bytes(b"FAILED"), dropped: 7 };
    let outcome = Outcome::Exited {
        exit: Exit::Code { code: 101 },
        head: bytes(b"running"),
        tail: bytes(b"FAILED"),
        dropped: 7,
        timeout: crate::TimeoutClamp::Within,
        output: crate::BytesClamp::Within,
    };
    assert_eq!(h.end(owner, exited), (1, outcome));
}

#[test]
fn a_command_runs_as_long_as_it_asks_within_the_tools_limit() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    for (asked, deadline) in [(Some(Duration::from_secs(5)), at(5)), (Some(Duration::from_secs(7200)), at(600))] {
        let (owner, _, given) = spawned(&mut h, kit, asked);
        assert_eq!(given, deadline, "{asked:?}");
        drop(h.end(owner, Done::Cancelled));
    }
    // The call's own deadline holds over both.
    let reply_to = ReplyTo::new(Token::new(1));
    let call = Call::Shell {
        after: crate::Yield::Never,
        command: bytes(b"sleep 9"),
        timeout: crate::Timeout::Asked(Duration::from_secs(5)),
        output: crate::Budget::Default,
    };
    match h.step(Event::Call { kit, observation: None, reply_to, call, deadline: at(2) }) {
        Some(Request::Io { deadline, .. }) => assert_eq!(deadline, at(2)),
        other => panic!("expected a spawn, not {other:?}"),
    }
}

#[test]
fn every_end_of_a_command_answers_for_itself() {
    let table = [
        (
            Done::Exited { exit: Exit::TimedOut, head: bytes(b"h"), tail: bytes(b""), dropped: 0 },
            Outcome::Exited {
                exit: Exit::TimedOut,
                head: bytes(b"h"),
                tail: bytes(b""),
                dropped: 0,
                timeout: crate::TimeoutClamp::Within,
                output: crate::BytesClamp::Within,
            },
        ),
        (Done::Missing, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::NotDirectory, Outcome::NotDirectory { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::Escapes, Outcome::Outside { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::Failed { fault: Fault::Other }, Outcome::Failed { fault: Fault::Other }),
        (Done::TimedOut, Outcome::TimedOut),
        (Done::Cancelled, Outcome::Cancelled),
    ];
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    for (done, expected) in table {
        let (owner, _, _) = spawned(&mut h, kit, None);
        assert_eq!(h.end(owner, done), (1, expected));
    }
}

#[test]
fn commands_need_the_shell_grant_and_a_working_directory_in_the_checkout() {
    let mut h = Harness::new(LIMITS);
    let modify = h.open(1, authority(Grants { inspect: true, modify: true, shell: false }));
    assert_eq!(
        h.call(
            modify,
            Call::Shell {
                after: crate::Yield::Never,
                command: bytes(b"ls"),
                timeout: crate::Timeout::Default,
                output: crate::Budget::Default
            }
        ),
        Outcome::NotGranted
    );
    let mut outside = authority(ALL);
    outside.cwd = names(b"/work");
    let outside = h.open(2, outside);
    assert_eq!(
        h.call(
            outside,
            Call::Shell {
                after: crate::Yield::Never,
                command: bytes(b"ls"),
                timeout: crate::Timeout::Default,
                output: crate::Budget::Default
            }
        ),
        Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"work") } }
    );
}

#[test]
fn a_search_asks_io_for_bounded_hits_and_answers_with_them() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let reply_to = ReplyTo::new(Token::new(1));
    let call = Call::Search { path: path(b"src"), pattern: bytes(b"fn"), glob: Some(bytes(b"*.rs")) };
    let (owner, op, deadline) = match h.step(Event::Call { kit, observation: None, reply_to, call, deadline: at(3600) })
    {
        Some(Request::Io { owner, op, deadline }) => (owner, op, deadline),
        other => panic!("expected a search, not {other:?}"),
    };
    let expected = Op::Search {
        at: place(1, b"src"),
        pattern: bytes(b"fn"),
        glob: Some(bytes(b"*.rs")),
        hits: LIMITS.search_hits,
        bytes: LIMITS.search_bytes,
    };
    assert_eq!((op, deadline), (expected, at(30)), "the tools' limit for a search");
    let hits: Box<[Hit]> = Box::new([Hit { path: bytes(b"lib.rs"), line: 3, text: bytes(b"pub fn three() {}") }]);
    let (_, outcome) = h.end(owner, Done::Found { hits: hits.clone(), more: 4, timed_out: true });
    assert_eq!(outcome, Outcome::Found { hits, more: 4, timed_out: true });

    let unreadable =
        Done::Exited { exit: Exit::Code { code: 2 }, head: bytes(b"regex parse error"), tail: bytes(b""), dropped: 0 };
    let table = [
        (
            unreadable.clone(),
            Outcome::Exited {
                exit: Exit::Code { code: 2 },
                head: bytes(b"regex parse error"),
                tail: bytes(b""),
                dropped: 0,
                timeout: crate::TimeoutClamp::Within,
                output: crate::BytesClamp::Within,
            },
        ),
        (Done::Missing, Outcome::NotFound { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::NotDirectory, Outcome::NotDirectory { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::Escapes, Outcome::Outside { path: path::Resolved::Working { path: bytes(b"temper") } }),
        (Done::TimedOut, Outcome::TimedOut),
        (Done::Cancelled, Outcome::Cancelled),
    ];
    for (done, expected) in table {
        let call = Call::Search { path: path(b"."), pattern: bytes(b"fn ("), glob: None };
        let (owner, _) = match h.send(kit, 2, call) {
            Some(Request::Io { owner, op, .. }) => (owner, op),
            other => panic!("expected a search, not {other:?}"),
        };
        assert_eq!(h.end(owner, done), (2, expected));
    }
    let outside = Call::Search { path: path(b"/etc"), pattern: bytes(b"root"), glob: None };
    assert_eq!(h.call(kit, outside), Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc") } });
    // No NUL can go in an argument.
    let table = [
        Call::Search { path: path(b"."), pattern: bytes(b"a\0b"), glob: None },
        Call::Search { path: path(b"."), pattern: bytes(b"a"), glob: Some(bytes(b"*.rs\0")) },
        Call::Shell {
            after: crate::Yield::Never,
            command: bytes(b"ls\0-la"),
            timeout: crate::Timeout::Default,
            output: crate::Budget::Default,
        },
    ];
    for call in table {
        assert_eq!(h.call(kit, call.clone()), Outcome::NulByte, "{call:?}");
    }
}

/// Drains the facts told so far.
fn told(h: &mut Harness) -> List<Fact> {
    let mut facts = List::with_capacity(LIMITS.facts);
    for _ in 0..LIMITS.facts {
        if let Some(fact) = h.pop_fact() {
            facts.push(fact.kind).expect("room for every fact");
        }
    }
    facts
}

#[test]
fn a_kit_tells_what_happens_as_facts() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(5, authority(ALL));
    let session = Token::new(5);
    assert_eq!(
        h.call(kit, read(b"/etc/passwd")),
        Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc/passwd") } }
    );
    let (owner, _) = h.start(kit, 1, read(b"src/lib.rs"));
    drop(h.end(owner, Done::Loaded { content: bytes(b"one\ntwo\n"), version: version(1) }));
    let (_, _) = h.start(kit, 2, Call::List { path: path(b"src"), depth: crate::Depth::Whole, glob: None });
    drop(h.emit(Event::Close { kit }));
    let expected = [
        Fact::Opened { session },
        // Refused at the entrance: answered without having started.
        Fact::Answered {
            session,
            tool: Tool::Read,
            verdict: Verdict::Outside,
            bytes: 0,
            omitted_bytes: 0,
            job: None,
            clamped: false,
        },
        Fact::Started { session, tool: Tool::Read },
        Fact::Answered {
            session,
            tool: Tool::Read,
            verdict: Verdict::Read,
            bytes: 8,
            omitted_bytes: 0,
            job: None,
            clamped: false,
        },
        Fact::Started { session, tool: Tool::List },
        Fact::Closing { session, running: 1 },
    ];
    assert_eq!(told(&mut h).as_slice(), &expected);
    let mut bad = authority(ALL);
    bad.repos[1].root = bad.repos[0].root;
    drop(h.step(Event::Open { jobs: 0, session: Token::new(6), authority: bad }));
    let refused = Fact::Refused { session: Token::new(6), refusal: Refusal::Invalid };
    assert_eq!(told(&mut h).as_slice(), &[refused]);
}

#[test]
fn a_minimum_fact_reserve_keeps_every_native_entrance_observation() {
    assert!(!valid(&Limits { facts: 1, ..LIMITS }));
    let mut h = Harness::new(Limits { facts: crate::max_facts(&LIMITS), ..LIMITS });
    let kit = h.open(5, authority(ALL));
    assert!(h.domain.facts_room() < crate::max_facts(&LIMITS));
    assert_eq!(
        h.call(kit, read(b"/etc/passwd")),
        Outcome::Outside { path: path::Resolved::Outside { path: bytes(b"etc/passwd") } }
    );
    assert_eq!(h.step(Event::Close { kit }), Some(Request::Closed { session: Token::new(5) }));
    assert_eq!(h.observed.len() + h.env.limits.facts - h.domain.facts_room(), 3);
    assert_eq!(told(&mut h).len(), 3);
}

#[test]
fn the_last_cancelled_operation_fills_the_complete_two_fact_reserve() {
    let mut h = Harness::new(Limits { jobs: 0, facts: 2, ..LIMITS });
    let kit = h.open(5, authority(ALL));
    let (owner, _) = h.start(kit, 1, read(b"src/lib.rs"));
    drop(h.emit(Event::Close { kit }));
    h.keep_facts();
    let before = h.observed.len();
    drop(h.emit(Event::Done { owner, done: Done::Cancelled }));
    assert_eq!(h.domain.facts_room(), 0, "the terminal uses its complete reserved output");
    h.keep_facts();
    assert_eq!(h.observed.len() - before, 2);
    let mut tail = List::with_capacity(2);
    for fact in h.observed.iter().skip(usize::try_from(before).expect("bounded observed history")) {
        tail.push(fact.kind).expect("two terminal observations");
    }
    assert_eq!(
        tail.as_slice(),
        &[
            Fact::Answered {
                session: Token::new(5),
                tool: Tool::Read,
                verdict: Verdict::Cancelled,
                bytes: 0,
                omitted_bytes: 0,
                job: None,
                clamped: false
            },
            Fact::Closed { session: Token::new(5) },
        ]
    );
}

#[test]
fn a_command_grant_does_not_depend_on_a_file_modify_grant() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(Grants { inspect: true, modify: false, shell: true }));
    let (_, op, _) = spawned(&mut h, kit, None);
    match op {
        Op::Spawn { cwd, command, head, tail, after: _ } => {
            assert_eq!(cwd, place(1, b""));
            assert_eq!(command, bytes(b"cargo test"));
            assert_eq!((head, tail), (LIMITS.shell_head, LIMITS.shell_tail));
        }
        op @ (Op::Load { .. } | Op::List { .. } | Op::Store { .. } | Op::Search { .. }) => {
            panic!("expected a spawn, not {op:?}")
        }
    }
}

#[test]
fn raw_paths_are_bounded_before_normalisation_erases_components() {
    let dotted = Path { absolute: true, parts: Box::new([const { Part::Current }; 33]) };
    assert_eq!(path::normalise(&[], &dotted, 64), None);
    let parents = Path { absolute: true, parts: Box::new([const { Part::Parent }; 22]) };
    assert_eq!(path::normalise(&[], &parents, 64), None);
    let exact = Path { absolute: true, parts: Box::new([const { Part::Current }; 32]) };
    assert_eq!(path::normalise(&[], &exact, 63), Some(Box::from(&b""[..])));
}

#[test]
fn fact_emission_times_and_call_context_survive_a_delayed_drain() {
    let mut h = Harness::new(LIMITS);
    h.env.now = Time::from_nanos(7);
    let kit = h.open(5, authority(ALL));
    h.env.now = Time::from_nanos(11);
    let info = crate::CallInfo { owner: Token::new(71), effect: Effect::Read, deadline: at(20), input_bytes: 19 };
    let request = h.step(Event::Call {
        kit,
        reply_to: ReplyTo::new(info.owner),
        observation: Some(info),
        call: read(b"a"),
        deadline: info.deadline,
    });
    let Some(Request::Io { owner, .. }) = request else { panic!("read starts") };
    h.env.now = Time::from_nanos(13);
    drop(h.emit(Event::Close { kit }));
    h.env.now = Time::from_nanos(17);
    drop(h.emit(Event::Done { owner, done: Done::Cancelled }));
    h.env.now = Time::from_nanos(99);
    let facts = observed_facts(&mut h);
    assert_eq!(fact_times(&facts).as_ref(), [7, 11, 13, 17, 17].map(Time::from_nanos));
    assert_eq!(facts[1].call, Some(info));
    assert_eq!(facts[3].call, Some(info));
    assert_eq!(
        facts[3].kind,
        Fact::Answered {
            session: Token::new(5),
            tool: Tool::Read,
            verdict: Verdict::Cancelled,
            bytes: 0,
            omitted_bytes: 0,
            job: None,
            clamped: false
        }
    );
    let mut invalid = authority(ALL);
    invalid.repos[1].root = invalid.repos[0].root;
    drop(h.step(Event::Open { jobs: 0, session: Token::new(6), authority: invalid }));
    assert_eq!(h.pop_fact().expect("refused fact").at, h.env.now);
}

fn observed_facts(harness: &mut Harness) -> Box<[crate::Fact]> {
    let mut facts = List::with_capacity(LIMITS.facts);
    for _ in 0..LIMITS.facts {
        if let Some(fact) = harness.pop_fact() {
            facts.push(fact).expect("room for all observations");
        }
    }
    facts.into_boxed()
}

fn fact_times(facts: &[crate::Fact]) -> Box<[Time]> {
    let mut times = List::with_capacity(LIMITS.facts);
    for fact in facts {
        times.push(fact.at).expect("room for all observations");
    }
    times.into_boxed()
}

#[test]
fn file_calls_on_the_directory_itself_answer_without_io() {
    for spelling in [&b"."[..], &b""[..], &b"/work/temper"[..], &b"vendor/lib"[..], &b"/work/docs"[..]] {
        let mut h = Harness::new(LIMITS);
        let kit = h.open(1, authority(ALL));
        for call in [read(spelling), write(spelling, 3), edit(spelling)] {
            assert_eq!(h.call(kit, call), Outcome::NotFile);
        }
    }
}

#[test]
fn list_slash_answers_sorted_authority_names_without_io_and_obeys_both_caps() {
    let named = Authority {
        cwd: names(b"/zeta"),
        repos: Box::new([
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/zeta"),
                root: Token::new(1),
                writable: true,
            },
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/alpha"),
                root: Token::new(2),
                writable: false,
            },
            Repo {
                host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                mount: names(b"/beta"),
                root: Token::new(3),
                writable: false,
            },
        ]),
        grants: ALL,
    };
    let alpha = entry(b"alpha", Kind::Directory);
    let cost = u64::try_from(size_of::<Entry>()).unwrap() + 5;
    let cases: [(u32, u64, Box<[Entry]>, u64); 4] = [
        (3, 4096, Box::new([alpha.clone(), entry(b"beta", Kind::Directory), entry(b"zeta", Kind::Directory)]), 0),
        (1, 4096, Box::new([alpha.clone()]), 2),
        (3, cost, Box::new([alpha.clone()]), 2),
        (3, cost - 1, Box::new([]), 3),
    ];
    for (count, bytes, expected, more) in cases {
        let mut limits = LIMITS;
        limits.list_entries = count;
        limits.list_bytes = bytes;
        let mut h = Harness::new(limits);
        let kit = h.open(1, named.clone());
        assert_eq!(
            h.call(kit, Call::List { path: path(b"/"), depth: crate::Depth::Whole, glob: None }),
            Outcome::Listed { entries: expected, more }
        );
        assert!(h.domain.flights.is_empty(), "the answer owns no IO flight");
        assert_eq!(h.step(Event::Close { kit }), Some(Request::Closed { session: Token::new(1) }));
    }
    let mut h = Harness::new(LIMITS);
    let mut denied = named.clone();
    denied.grants.inspect = false;
    let kit = h.open(1, denied);
    assert_eq!(
        h.call(kit, Call::List { path: path(b"/"), depth: crate::Depth::Whole, glob: None }),
        Outcome::NotGranted
    );
    let kit = h.open(2, named);
    let reply_to = ReplyTo::new(Token::new(99));
    assert_eq!(
        h.step(Event::Call {
            kit,
            observation: None,
            reply_to,
            call: Call::List { path: path(b"/"), depth: crate::Depth::Whole, glob: None },
            deadline: Time::ZERO
        }),
        Some(Request::Answer { to: ReplyTo::new(Token::new(99)), outcome: Outcome::TimedOut })
    );
}

#[test]
fn failed_paths_are_normalized_and_stay_at_the_admitted_path_bound() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(
        77,
        Authority {
            cwd: names(b"/w"),
            repos: Box::new([
                Repo {
                    host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                    mount: names(b"/w"),
                    root: Token::new(1),
                    writable: true,
                },
                Repo {
                    host: Box::new([Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
                    mount: names(b"/d"),
                    root: Token::new(2),
                    writable: false,
                },
            ]),
            grants: ALL,
        },
    );
    let file = [b'x'; 62];
    let (owner, _) = h.start(kit, 1, read(&file));
    let (_, outcome) = h.end(owner, Done::Missing);
    let expected = path::Resolved::Working { path: bytes(&file) };
    assert_eq!(outcome, Outcome::NotFound { path: expected.clone() });
    assert_eq!(expected.owned_bytes(), Some(62));
    let outside = [b'z'; 64];
    let absolute = Path {
        absolute: true,
        parts: Box::new([Part::Name { name: Name::new(bytes(&outside)).expect("bounded name") }]),
    };
    let call = Call::Read { path: absolute, offset: 1, window: crate::Budget::Default };
    assert_eq!(h.call(kit, call.clone()), Outcome::Outside { path: path::Resolved::Outside { path: bytes(&outside) } });
    assert!(crate::result_worst_case(&call, &LIMITS).expect("checked credit") >= 64);
    assert_eq!(h.call(kit, read(&[b'x'; 63])), Outcome::TooLong, "normalized mount plus file exceeds 64");
    let call = Call::Write { path: path(b"/d/a/../guide.md"), content: bytes(b"x") };
    assert_eq!(
        h.call(kit, call),
        Outcome::ReadOnly {
            path: path::Resolved::Other {
                directory: Name::new(bytes(b"d")).expect("directory name"),
                path: bytes(b"guide.md")
            }
        }
    );
}

#[test]
fn host_aliases_choose_whole_names_and_keep_the_namespace_root_pure() {
    let mounted = Authority {
        cwd: names(b"/w"),
        repos: Box::new([
            Repo { host: names(b"/tmp/work"), mount: names(b"/w"), root: Token::new(1), writable: true },
            Repo { host: names(b"/tmp/work/vendor"), mount: names(b"/d"), root: Token::new(2), writable: false },
            Repo { host: names(b"/elsewhere"), mount: names(b"/tmp"), root: Token::new(3), writable: true },
        ]),
        grants: ALL,
    };
    let mut h = Harness::new(LIMITS);
    let kit = h.open(10, mounted);
    assert_eq!(h.call(kit, read(b"/tmp/work")), Outcome::NotFile);
    for (spelling, root, tail) in [
        (b"/tmp/work/src/lib.rs".as_slice(), Token::new(1), b"src/lib.rs".as_slice()),
        (b"/tmp/work/src/../missing".as_slice(), Token::new(1), b"missing".as_slice()),
        (b"/tmp/work/vendor/guide.md".as_slice(), Token::new(2), b"guide.md".as_slice()),
        (b"/tmp/worked/src/lib.rs".as_slice(), Token::new(3), b"worked/src/lib.rs".as_slice()),
    ] {
        let (owner, op) = h.start(kit, 20, read(spelling));
        match op {
            Op::Load { at, .. } => assert_eq!(at, Place { root, path: bytes(tail) }),
            Op::List { .. } | Op::Search { .. } | Op::Store { .. } | Op::Spawn { .. } => panic!("read loads"),
        }
        let (_, outcome) = h.end(owner, Done::Missing);
        let path = if root == Token::new(1) {
            path::Resolved::Working { path: bytes(tail) }
        } else {
            let directory = if root == Token::new(2) { b"d".as_slice() } else { b"tmp".as_slice() };
            path::Resolved::Other { directory: Name::new(bytes(directory)).unwrap(), path: bytes(tail) }
        };
        assert_eq!(outcome, Outcome::NotFound { path });
    }
    assert_eq!(
        h.call(kit, write(b"/tmp/work/vendor/guide.md", 1)),
        Outcome::ReadOnly {
            path: path::Resolved::Other { directory: Name::new(bytes(b"d")).unwrap(), path: bytes(b"guide.md") }
        }
    );
    let mut root = authority(ALL);
    root.repos[0].host = Box::new([]);
    let kit = h.open(11, root);
    assert_eq!(
        h.call(kit, Call::List { path: path(b"/"), depth: crate::Depth::Whole, glob: None }),
        Outcome::Listed { entries: Box::new([entry(b"work", Kind::Directory)]), more: 0 }
    );
}

#[test]
fn ordered_edit_batches_use_previous_results_and_report_the_first_failure() {
    let edits = Box::new([snippet(b"a", b"b", true), snippet(b"bb", b"c", false)]);
    assert_eq!(edit::apply_edits(bytes(b"aa"), &edits[..], &LIMITS), Ok(edited(b"c", 3)));
    let fails = Box::new([snippet(b"a", b"b", false), snippet(b"z", b"c", false), snippet(b"c", b"c", false)]);
    assert_eq!(
        edit::apply_edits(bytes(b"a"), &fails[..], &LIMITS),
        Err(Outcome::EditFailed { edit: 2, why: crate::Mismatch::NoMatch })
    );
    let ambiguous = Box::new([snippet(b"a", b"bb", false), snippet(b"b", b"c", false)]);
    assert_eq!(
        edit::apply_edits(bytes(b"a"), &ambiguous[..], &LIMITS),
        Err(Outcome::EditFailed { edit: 2, why: crate::Mismatch::Ambiguous { count: 2, lines: Box::new([1, 1]) } })
    );
}

#[test]
fn an_edit_batch_without_a_read_stores_once_and_a_failed_batch_stores_nothing() {
    let mut h = Harness::new(LIMITS);
    let kit = h.open(1, authority(ALL));
    let call = Call::Edit {
        path: path(b"src/lib.rs"),
        edits: Box::new([snippet(b"one", b"two", false), snippet(b"two", b"three", false)]),
    };
    let (owner, _) = h.start(kit, 1, call);
    assert_eq!(
        h.next(owner, Done::Loaded { content: bytes(b"one"), version: version(7) }),
        store(place(1, b"src/lib.rs"), b"three", Expect::Is { version: version(7) })
    );
    assert_eq!(h.end(owner, Done::Stored { version: version(8) }), (1, Outcome::Edited { replaced: 2 }));
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(8)));
    let call = Call::Edit {
        path: path(b"src/lib.rs"),
        edits: Box::new([snippet(b"three", b"four", false), snippet(b"missing", b"bad", false)]),
    };
    let (owner, _) = h.start(kit, 2, call);
    assert_eq!(
        h.end(owner, Done::Loaded { content: bytes(b"three"), version: version(8) }),
        (2, Outcome::EditFailed { edit: 2, why: crate::Mismatch::NoMatch })
    );
    assert_eq!(h.knowledge(kit).version(&place(1, b"src/lib.rs")), Some(version(8)));
}

#[test]
fn list_controls_cross_once_and_zero_depth_is_refused_before_io() {
    let mut harness = Harness::new(LIMITS);
    let kit = harness.open(1, authority(ALL));
    let (_, op) = harness.start(
        kit,
        1,
        Call::List { path: path(b"src"), depth: crate::Depth::Levels(2), glob: Some(bytes(b"*.rs")) },
    );
    assert_eq!(
        op,
        Op::List {
            at: place(1, b"src"),
            depth: crate::Depth::Levels(2),
            glob: Some(bytes(b"*.rs")),
            max: LIMITS.list_entries,
            max_bytes: LIMITS.list_bytes
        }
    );
    assert_eq!(
        harness.call(kit, Call::List { path: path(b"src"), depth: crate::Depth::Levels(0), glob: None }),
        Outcome::Failed { fault: Fault::Other }
    );
}

#[test]
fn an_edit_can_grow_to_the_whole_file_bound_and_refuses_one_byte_past_it() {
    let limits = Limits { file_bytes: 128, tool_payload: 64, ..LIMITS };
    let mut content = [b'a'; 125];
    content[124] = b'x';
    for (new, size) in [(b"bbbb".as_slice(), 128_u64), (b"bbbbb".as_slice(), 129_u64)] {
        let result = edit::apply_edits(
            content.as_slice().into(),
            &[Edit { old: b"x".as_slice().into(), new: new.into(), occurrences: crate::Occurrences::Once }],
            &limits,
        );
        if size == 128_u64 {
            let (changed, count) = result.expect("whole-file ceiling");
            assert_eq!(changed.len(), 128);
            assert_eq!(count, 1);
        } else {
            assert_eq!(result, Err(Outcome::TooLarge { what: crate::SizePart::EditResult, size: 129, bound: 128 }));
        }
    }
}

#[test]
fn a_read_uses_its_default_or_reports_the_clamped_payload_window() {
    let limits = Limits { read_bytes: 4, tool_payload: 8, ..LIMITS };
    assert_eq!(window::window(crate::Budget::Default, &limits).bytes, 4);
    let within = window::window(crate::Budget::Asked(6), &limits);
    assert_eq!(within.bytes, 6);
    assert_eq!(within.clamp, crate::BytesClamp::Within);
    let clamp = window::window(crate::Budget::Asked(99), &limits);
    assert_eq!(clamp.bytes, 8);
    assert_eq!(clamp.clamp, crate::BytesClamp::Clamped { asked: 99, ceiling: 8 });
    let mut h = Harness::new(limits);
    let kit = h.open(1, authority(ALL));
    for call in [
        Call::Read { path: path(b"x"), offset: 0, window: crate::Budget::Default },
        Call::Read { path: path(b"x"), offset: 1, window: crate::Budget::Asked(0) },
    ] {
        assert_eq!(h.call(kit, call), Outcome::Failed { fault: Fault::Other });
    }
    assert_eq!(h.domain.flights(), 0);
}

#[test]
fn a_command_retains_its_requested_deadline_and_output_clamps_at_the_terminal() {
    let limits = Limits {
        shell_output: 8,
        shell_head: 9,
        shell_tail: 3,
        shell_timeout: Duration::from_secs(6),
        tool_deadline: Duration::from_secs(10),
        ..LIMITS
    };
    let mut h = Harness::new(limits);
    let kit = h.open(1, authority(ALL));
    let call = Call::Shell {
        after: crate::Yield::Never,
        command: bytes(b"test"),
        timeout: crate::Timeout::Asked(Duration::from_secs(15)),
        output: crate::Budget::Asked(20),
    };
    let reply_to = ReplyTo::new(Token::new(1));
    let request = h.step(Event::Call { kit, observation: None, reply_to, call, deadline: at(30) });
    let Some(Request::Io { owner, op, deadline }) = request else { panic!("command spawn") };
    assert_eq!(deadline, at(10));
    assert_eq!(op, Op::Spawn { cwd: place(1, b""), command: bytes(b"test"), head: 9, tail: 3, after: None });
    let terminal = Done::Exited { exit: Exit::TimedOut, head: bytes(b"first"), tail: bytes(b"end"), dropped: 7 };
    let expected = Outcome::Exited {
        exit: Exit::TimedOut,
        head: bytes(b"first"),
        tail: bytes(b"end"),
        dropped: 7,
        timeout: crate::TimeoutClamp::Clamped { asked: Duration::from_secs(15), ceiling: Duration::from_secs(10) },
        output: crate::BytesClamp::Clamped { asked: 20, ceiling: 12 },
    };
    assert_eq!(h.end(owner, terminal), (1, expected));
}

#[test]
fn a_requested_command_capture_is_split_in_the_deployments_proportion() {
    let limits = Limits { shell_output: 8, shell_head: 9, shell_tail: 3, ..LIMITS };
    let mut h = Harness::new(limits);
    let kit = h.open(1, authority(ALL));
    for (output, head, tail) in
        [(crate::Budget::Default, 6, 2), (crate::Budget::Asked(5), 3, 2), (crate::Budget::Asked(1), 0, 1)]
    {
        let call = Call::Shell {
            after: crate::Yield::Never,
            command: bytes(b"test"),
            timeout: crate::Timeout::Default,
            output,
        };
        let reply_to = ReplyTo::new(Token::new(1));
        let request = h.step(Event::Call { kit, observation: None, reply_to, call, deadline: at(3600) });
        let Some(Request::Io { owner, op, .. }) = request else { panic!("command spawn") };
        assert_eq!(op, Op::Spawn { cwd: place(1, b""), command: bytes(b"test"), head, tail, after: None });
        drop(h.end(owner, Done::Cancelled));
    }
}

#[test]
fn read_terminal_facts_preserve_window_omission_and_applied_clamps() {
    let limits = Limits { read_bytes: 4, tool_payload: 8, ..LIMITS };
    let cases = [
        (1, crate::Budget::Asked(99), b"a\nb\nc\nd\ne\n".as_slice(), 2, true),
        (2, crate::Budget::Asked(4), b"a\nb\nc\nd\ne\n".as_slice(), 4, false),
        (2, crate::Budget::Asked(8), b"a\nb\nc\n".as_slice(), 0, false),
        (1, crate::Budget::Default, b"abcdef".as_slice(), 2, false),
        (1, crate::Budget::Asked(99), b"abc".as_slice(), 0, true),
        (1, crate::Budget::Default, b"abc".as_slice(), 0, false),
        (20, crate::Budget::Default, b"a\nb\n".as_slice(), 0, false),
    ];
    for (offset, window, content, omitted_bytes, clamped) in cases {
        let mut h = Harness::new(limits);
        let kit = h.open(5, authority(ALL));
        drop(told(&mut h));
        let (owner, _) = h.start(kit, 1, Call::Read { path: path(b"x"), offset, window });
        let started = told(&mut h);
        assert_eq!(started.as_slice(), &[Fact::Started { session: Token::new(5), tool: Tool::Read }]);
        let (_, outcome) = h.end(owner, Done::Loaded { content: bytes(content), version: version(1) });
        let Outcome::Read { content, .. } = outcome else { panic!("read answer") };
        assert_eq!(
            told(&mut h).as_slice(),
            &[Fact::Answered {
                job: None,
                session: Token::new(5),
                tool: Tool::Read,
                verdict: Verdict::Read,
                bytes: u64::try_from(content.len()).unwrap(),
                omitted_bytes,
                clamped,
            }],
            "offset {offset}, window {window:?}",
        );
    }
}

#[test]
fn command_terminal_facts_preserve_capture_loss_and_clamps_across_all_terminals() {
    let limits = Limits {
        shell_output: 8,
        shell_head: 9,
        shell_tail: 3,
        shell_timeout: Duration::from_secs(6),
        tool_deadline: Duration::from_secs(10),
        ..LIMITS
    };
    for (timeout, output, clamped) in [
        (crate::Timeout::Default, crate::Budget::Default, false),
        (crate::Timeout::Asked(Duration::from_secs(15)), crate::Budget::Default, true),
        (crate::Timeout::Default, crate::Budget::Asked(20), true),
    ] {
        for (terminal, verdict, kept, omitted_bytes) in [
            (
                Done::Exited { exit: Exit::Code { code: 0 }, head: bytes(b"first"), tail: bytes(b"end"), dropped: 7 },
                Verdict::Exited { exit: Exit::Code { code: 0 } },
                8,
                7,
            ),
            (
                Done::Exited { exit: Exit::Code { code: 0 }, head: bytes(b"ok"), tail: bytes(b""), dropped: 0 },
                Verdict::Exited { exit: Exit::Code { code: 0 } },
                2,
                0,
            ),
            (Done::TimedOut, Verdict::TimedOut, 0, 0),
            (Done::Cancelled, Verdict::Cancelled, 0, 0),
            (Done::Failed { fault: Fault::Denied }, Verdict::Failed { fault: Fault::Denied }, 0, 0),
        ] {
            let mut h = Harness::new(limits);
            let kit = h.open(5, authority(ALL));
            drop(told(&mut h));
            let call = Call::Shell { after: crate::Yield::Never, command: bytes(b"test"), timeout, output };
            let request = h.step(Event::Call {
                kit,
                observation: None,
                reply_to: ReplyTo::new(Token::new(1)),
                call,
                deadline: at(30),
            });
            let Some(Request::Io { owner, .. }) = request else { panic!("command spawn") };
            assert_eq!(told(&mut h).as_slice(), &[Fact::Started { session: Token::new(5), tool: Tool::Shell }]);
            drop(h.end(owner, terminal));
            assert_eq!(
                told(&mut h).as_slice(),
                &[Fact::Answered {
                    job: None,
                    session: Token::new(5),
                    tool: Tool::Shell,
                    verdict,
                    bytes: kept,
                    omitted_bytes,
                    clamped,
                }]
            );
        }
    }
}

#[test]
fn a_clamped_read_retains_its_applied_cap_when_cancelled_or_timed_out() {
    let limits = Limits { read_bytes: 4, tool_payload: 8, ..LIMITS };
    for (done, verdict) in [(Done::TimedOut, Verdict::TimedOut), (Done::Cancelled, Verdict::Cancelled)] {
        let mut h = Harness::new(limits);
        let kit = h.open(5, authority(ALL));
        drop(told(&mut h));
        let (owner, _) = h.start(kit, 1, Call::Read { path: path(b"x"), offset: 1, window: crate::Budget::Asked(99) });
        drop(told(&mut h));
        drop(h.end(owner, done));
        assert_eq!(
            told(&mut h).as_slice(),
            &[Fact::Answered {
                session: Token::new(5),
                tool: Tool::Read,
                verdict,
                bytes: 0,
                omitted_bytes: 0,
                job: None,
                clamped: true,
            }]
        );
    }
}

#[test]
#[expect(
    clippy::disallowed_macros,
    reason = "parent-compatible diagnostic regression is test code, programming-model.md section 10.2"
)]
fn read_caps_are_observable_at_the_tools_parent_boundary() {
    let mut h = Harness::new(Limits { read_bytes: 4, tool_payload: 8, ..LIMITS });
    let kit = h.open(5, authority(ALL));
    drop(told(&mut h));
    let (owner, _) = h.start(kit, 1, Call::Read { path: path(b"x"), offset: 2, window: crate::Budget::Asked(99) });
    drop(told(&mut h));
    drop(h.end(owner, Done::Loaded { content: bytes(b"a\nb\nc\nd\ne\nf\n"), version: version(1) }));
    let terminal = told(&mut h);
    assert_eq!(terminal.len(), 1, "one actual terminal reaches the parent");
    // Use the common parent's existing Debug surface to run the same test
    // before the new fields exist; the typed assertions above check the fixed API.
    let reported = format!("{:?}", terminal.as_slice()[0]);
    assert!(reported.contains("omitted_bytes: 2"), "missing actual window observation: {reported}");
    assert!(reported.contains("clamped: true"), "missing actual lowered-cap observation: {reported}");
}

mod jobs;

/// An edit batch waiting for its file's loaded bytes.
fn pending_edit_batch(edits: Box<[Edit]>) -> (Harness, Token) {
    let mut harness = Harness::new(LIMITS);
    let kit = harness.open(1, authority(ALL));
    let (owner, _) = harness.start(kit, 1, Call::Edit { path: path(b"src/lib.rs"), edits });
    (harness, owner)
}

#[test]
fn matching_no_ops_beside_real_edits_are_stored_and_counted() {
    let cases = [
        (b"a b".as_slice(), [snippet(b"a", b"a", false), snippet(b"b", b"c", false)], b"a c".as_slice(), 2),
        (b"a b", [snippet(b"b", b"c", false), snippet(b"a", b"a", false)], b"a c", 2),
        (b"a a b", [snippet(b"a", b"a", true), snippet(b"b", b"c", false)], b"a a c", 3),
    ];
    for (loaded, edits, expected, matches) in cases {
        let (mut harness, owner) = pending_edit_batch(Box::new(edits));
        assert_eq!(
            harness.next(owner, Done::Loaded { content: bytes(loaded), version: version(3) }),
            store(place(1, b"src/lib.rs"), expected, Expect::Is { version: version(3) })
        );
        // Replacements count matches, including those whose bytes stay the same.
        assert_eq!(
            harness.end(owner, Done::Stored { version: version(4) }),
            (1, Outcome::Edited { replaced: matches })
        );
    }
}

#[test]
fn unchanged_edit_batches_are_refused_at_the_last_item() {
    let cases = [
        (
            Box::<[Edit]>::from([snippet(b"a", b"a", false)]),
            Outcome::EditFailed { edit: 1, why: crate::Mismatch::Unchanged },
        ),
        (
            Box::<[Edit]>::from([snippet(b"a", b"b", false), snippet(b"b", b"a", false)]),
            Outcome::EditFailed { edit: 2, why: crate::Mismatch::Unchanged },
        ),
    ];
    for (edits, expected) in cases {
        assert_eq!(refused_edit_batch(b"a", edits), (1, expected));
    }
}

/// Complete a batch's load, expecting a terminal answer instead of a store.
fn refused_edit_batch(loaded: &[u8], edits: Box<[Edit]>) -> (u64, Outcome) {
    let (mut harness, owner) = pending_edit_batch(edits);
    harness.end(owner, Done::Loaded { content: bytes(loaded), version: version(3) })
}

#[test]
fn absent_or_empty_no_ops_are_refused_as_no_match() {
    for old in [b"z".as_slice(), b""] {
        assert_eq!(
            refused_edit_batch(b"a", Box::new([snippet(old, old, false)])),
            (1, Outcome::EditFailed { edit: 1, why: crate::Mismatch::NoMatch })
        );
    }
}

#[test]
fn a_repeated_no_op_requires_all_occurrences() {
    assert_eq!(
        refused_edit_batch(b"a\na", Box::new([snippet(b"a", b"a", false)])),
        (1, Outcome::EditFailed { edit: 1, why: crate::Mismatch::Ambiguous { count: 2, lines: Box::new([1, 2]) } })
    );
}

#[test]
fn a_no_op_item_counts_each_match_without_changing_bytes() {
    for (content, all, matches) in [(b"a".as_slice(), false, 1), (b"a a", true, 2)] {
        assert_eq!(edit::apply(content, &snippet(b"a", b"a", all), 1, &LIMITS), Ok(edited(content, matches)));
    }
}
