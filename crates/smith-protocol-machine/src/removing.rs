//! One confined removal: an open parent, a current file digest, then unlink.
//! Keeps the parent until its close terminal; knows no session history.
//! Contract: domain/tools.md, sections 3–4; protocol/agent.md, section 2.

use crate::boundary::Below;
use alloc::boxed::Box;
use skein_io::{digest, file, kernel};
use skein_lib::{Queue, Time, Token};
use smith_domain::tools;

#[derive(Debug)]
pub(crate) struct Removing {
    name: Box<[u8]>,
    expect: tools::Expect,
    max: u32,
    deadline: Time,
    cancelled: bool,
    state: State,
}

#[derive(Debug)]
enum State {
    Opening,
    Loading { parent: Token },
    Removing { parent: Token },
    Closing { done: tools::Done },
}

pub(crate) enum Next {
    Wait(Removing),
    Done(tools::Done),
}

impl Removing {
    pub(crate) fn start(
        owner: Token,
        at: tools::Place,
        expect: tools::Expect,
        max: u32,
        deadline: Time,
    ) -> (Self, Below) {
        let mut split = 0_usize;
        for (index, byte) in at.path.iter().enumerate() {
            if *byte == b'/' {
                split = index.checked_add(1).expect("bounded path");
            }
        }
        let parent = if split == 0 {
            b".".as_slice()
        } else {
            at.path.get(..split.checked_sub(1).expect("nonzero split")).expect("path prefix")
        };
        let request = Below::OpenDirectory { owner, root: at.root, path: parent.into(), no_follow: true, deadline };
        (
            Self {
                name: at.path.get(split..).expect("path suffix").into(),
                expect,
                max,
                deadline,
                cancelled: false,
                state: State::Opening,
            },
            request,
        )
    }

    pub(crate) fn cancel(&mut self) -> bool {
        self.cancelled = true;
        match self.state {
            State::Closing { .. } => false,
            State::Opening | State::Loading { .. } | State::Removing { .. } => true,
        }
    }

    #[expect(clippy::wildcard_enum_match_arm, reason = "skein's event and kernel vocabularies are external")]
    pub(crate) fn event(mut self, owner: Token, now: Time, event: file::Event, below: &mut Queue<Below>) -> Next {
        match self.state {
            State::Opening => match event {
                file::Event::Opened { file: parent, .. } => {
                    if let Some(done) = self.stopped(now) {
                        return self.close(owner, parent, done, below);
                    }
                    below.push(Below::File {
                        request: file::Request::Load {
                            owner,
                            root: parent,
                            path: self.name.clone(),
                            max: self.max,
                            no_follow: true,
                        },
                        deadline: self.deadline,
                    });
                    self.state = State::Loading { parent };
                    Next::Wait(self)
                }
                file::Event::Failed { error, .. } => Next::Done(failure(error)),
                file::Event::Cancelled { .. } => Next::Done(tools::Done::Cancelled),
                _ => unreachable!("parent open terminal"),
            },
            State::Loading { parent } => {
                let directory = match event {
                    file::Event::Loaded { bytes, .. } => {
                        let version = tools::Version::new(digest::digest(&bytes).0);
                        if self.expect != (tools::Expect::Is { version }) {
                            return self.close(owner, parent, tools::Done::Conflict { now: Some(version) }, below);
                        }
                        false
                    }
                    file::Event::Failed { error: kernel::Error::IsADirectory, .. } => true,
                    file::Event::Failed { error, .. } => return self.close(owner, parent, failure(error), below),
                    file::Event::TooLarge { size, .. } => {
                        return self.close(owner, parent, tools::Done::TooLarge { size }, below);
                    }
                    file::Event::Cancelled { .. } => return self.close(owner, parent, tools::Done::Cancelled, below),
                    _ => unreachable!("removal load terminal"),
                };
                if let Some(done) = self.stopped(now) {
                    return self.close(owner, parent, done, below);
                }
                below.push(Below::Remove {
                    owner,
                    parent,
                    name: self.name.clone(),
                    directory,
                    deadline: self.deadline,
                });
                self.state = State::Removing { parent };
                Next::Wait(self)
            }
            State::Removing { parent } => {
                let done = match event {
                    file::Event::Removed { .. } => tools::Done::Removed,
                    file::Event::Failed { error, .. } => failure(error),
                    file::Event::Cancelled { .. } => tools::Done::Cancelled,
                    _ => unreachable!("unlink terminal"),
                };
                self.close(owner, parent, done, below)
            }
            State::Closing { done } => {
                match event {
                    file::Event::Closed { .. } | file::Event::Failed { .. } | file::Event::Cancelled { .. } => {}
                    _ => unreachable!("parent close terminal"),
                }
                Next::Done(done)
            }
        }
    }

    fn stopped(&self, now: Time) -> Option<tools::Done> {
        if self.cancelled {
            Some(tools::Done::Cancelled)
        } else if now >= self.deadline {
            Some(tools::Done::TimedOut)
        } else {
            None
        }
    }

    fn close(mut self, owner: Token, parent: Token, done: tools::Done, below: &mut Queue<Below>) -> Next {
        below.push(Below::File {
            request: file::Request::Close { owner, file: parent },
            deadline: Time::from_nanos(u64::MAX),
        });
        self.state = State::Closing { done };
        Next::Wait(self)
    }
}

#[expect(clippy::wildcard_enum_match_arm, reason = "skein kernel errors are external")]
fn failure(error: kernel::Error) -> tools::Done {
    match error {
        kernel::Error::NotFound => tools::Done::Missing,
        kernel::Error::NotADirectory => tools::Done::NotDirectory,
        kernel::Error::NotEmpty => tools::Done::NotEmpty,
        kernel::Error::TooManyLinks => tools::Done::Linked,
        kernel::Error::Escape => tools::Done::Escapes,
        kernel::Error::IsADirectory | kernel::Error::NotAFile => tools::Done::NotFile,
        kernel::Error::Permission | kernel::Error::ReadOnly => tools::Done::Failed { fault: tools::Fault::Denied },
        kernel::Error::NoSpace => tools::Done::Failed { fault: tools::Fault::NoSpace },
        kernel::Error::TimedOut => tools::Done::TimedOut,
        kernel::Error::Cancelled => tools::Done::Cancelled,
        _ => tools::Done::Failed { fault: tools::Fault::Other },
    }
}
