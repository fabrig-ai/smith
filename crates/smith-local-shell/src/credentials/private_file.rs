//! Durable private credential writes (shell.md, 7.2; io.md, 5.2–5.3).
//! One startup operation owns its directory through the terminal and close.
use skein_io::{file, file_layer, kernel};
use skein_lib::{Duration, Queue, Time, Token, Wall};
use skein_shell::{Clock, Host};
use std::{os::unix::fs::DirBuilderExt, path::Path};

const OWNER: Token = Token::new(1);

pub(super) fn store(path: &Path, bytes: Box<[u8]>, expected: file::Expect, entropy: &[u8; 32]) -> Result<bool, String> {
    let clock = Clock::new();
    let parent = path.parent().ok_or("private file needs a parent")?;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(parent)
        .map_err(|_| "cannot make private directory")?;
    smith_agent_shell::program::private_directory(parent).map_err(|_| "unreadable private directory")?;
    let maximum = u32::try_from(bytes.len()).map_err(|_| "private file exceeds bound")?.max(1);
    let mut io = file_layer::FileIo::with_whole_limit(2, 4096, 1, maximum, Duration::from_secs(30))
        .with_effective_user(skein_shell::effective_user());
    io.seed_randomness(u64::from_be_bytes(entropy.get(..8).expect("entropy prefix").try_into().expect("eight bytes")));
    let root = io
        .adopt_root(
            skein_shell::open_root(parent.parent().ok_or("private directory needs a parent")?)
                .map_err(|_| "cannot open private directory")?,
        )
        .expect("one root");
    let request = file::Request::Store {
        owner: OWNER,
        root,
        path: path.file_name().ok_or("private file needs a name")?.as_encoded_bytes().into(),
        bytes,
        expected,
        no_follow: true,
    };
    let mut host = Writing {
        io,
        roots: vec![root],
        opening: Some(file::Request::OpenPrivate {
            owner: OWNER,
            root,
            path: parent.file_name().ok_or("private directory needs a name")?.as_encoded_bytes().into(),
        }),
        request: Some(request),
        terminal: false,
        closed: false,
        failure: None,
        kept: false,
        events: Queue::with_capacity(4),
        completions: Queue::with_capacity(4),
        submissions: Queue::with_capacity(4),
    };
    let mut kernel = skein_shell::Kernel::open(skein_shell::Config { operations: 8 })
        .map_err(|_| "private file kernel unavailable")?;
    let _exit = skein_shell::drive(&mut kernel, &clock, &mut host);
    host.failure.map_or(Ok(host.kept), Err)
}

struct Writing {
    io: file_layer::FileIo,
    roots: Vec<Token>,
    opening: Option<file::Request>,
    request: Option<file::Request>,
    terminal: bool,
    closed: bool,
    kept: bool,
    failure: Option<String>,
    events: Queue<file::Event>,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
}

impl Host for Writing {
    fn iterate(&mut self, now: Time, _wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            file_layer::up(&mut self.io, complete, &mut self.events, &mut self.submissions);
        }
        if self.io.is_due(now) {
            file_layer::expire(&mut self.io, now, &mut self.submissions);
        }
        while let Some(event) = self.events.pop() {
            match event {
                file::Event::Stored { .. } => {
                    self.kept = true;
                    self.terminal = true;
                }
                file::Event::Conflict { .. } => self.terminal = true,
                file::Event::Opened { file, .. } => {
                    self.roots.push(file);
                    match &mut self.request {
                        Some(file::Request::Store { root, .. }) => *root = file,
                        _ => unreachable!("one private store"),
                    }
                }
                file::Event::Closed { .. } => self.closed = self.roots.is_empty(),
                file::Event::Failed { .. } | file::Event::Refused { .. } | file::Event::Cancelled { .. } => {
                    self.failure = Some("private file operation failed".into());
                    self.terminal = true;
                    if self.io.open_files() == 0 {
                        self.closed = true;
                    }
                }
                file::Event::Made { .. }
                | file::Event::Locked { .. }
                | file::Event::Busy { .. }
                | file::Event::Loaded { .. }
                | file::Event::Scanned { .. }
                | file::Event::TooLarge { .. }
                | file::Event::Stated { .. }
                | file::Event::Written { .. }
                | file::Event::Read { .. }
                | file::Event::Cut { .. }
                | file::Event::Synced { .. }
                | file::Event::Renamed { .. }
                | file::Event::Removed { .. }
                | file::Event::Listed { .. } => unreachable!("private store vocabulary"),
            }
        }
        if self.io.takes() && !self.closed {
            let request = if self.terminal {
                self.request = None;
                self.roots.pop().map(|file| file::Request::Close { owner: OWNER, file })
            } else {
                self.opening.take().or_else(|| self.request.take())
            };
            if let Some(request) = request {
                file_layer::down(&mut self.io, now, request, &mut self.events, &mut self.submissions);
            }
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        !self.events.is_empty()
            || !self.completions.is_empty()
            || self.io.is_due(now)
            || (self.io.takes() && !self.closed && (self.request.is_some() || self.terminal))
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.closed && self.submissions.is_empty() && self.completions.is_empty() && self.events.is_empty()
    }
    fn operations(&self) -> u32 {
        8
    }
}
