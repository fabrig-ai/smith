//! Native permission/result snapshots (domain/tools.md 10; programs.md 5.4).
//! One bounded load is retained at a time. Stream parsing waits for its version
//! observation; no file content or version is inferred from program result text.

use super::{Backend, Below, Mount, Phase, policy};
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_io::{digest, file};
use skein_lib::{Env, Queue, Token};
use smith_domain::tools;
use smith_protocol_llm::{self as protocol, ToDomain};

/// One native call holds its owner and confined file until its bounded load settles.
pub(super) struct Snapshot {
    owner: Token,
    at: tools::Place,
    after: After,
    issued: bool,
}

/// The permission or result that resumes once its file version is observed.
pub(super) enum After {
    /// A whole Read receives its allowance after the version observation.
    Allow { call: mcp::CallId, input: Box<[u8]> },
    /// A native Write/Edit result follows its resulting version observation.
    Written { slot: u32 },
}

impl Snapshot {
    pub(super) fn new(owner: Token, at: tools::Place, after: After) -> Self {
        Self { owner, at, after, issued: false }
    }
}

pub(super) fn whole(input: &[u8]) -> bool {
    match json::collect(input) {
        Some(document) => {
            json::field(&document, 0, b"offset").is_none() && json::field(&document, 0, b"limit").is_none()
        }
        None => false,
    }
}

pub(super) fn target(mounts: &[Mount], input: &[u8], maximum: u32) -> Option<tools::Place> {
    let document = json::collect(input)?;
    let path = json::text_in(&document, json::field(&document, 0, b"file_path"))?;
    let cwd = &mounts.first()?.host;
    let resolved = policy::resolve(cwd, path, maximum);
    let mut selected: Option<&Mount> = None;
    for mount in mounts {
        if policy::contains(&mount.host, &resolved) {
            selected = match selected {
                Some(previous) if previous.host.len() >= mount.host.len() => Some(previous),
                Some(_) | None => Some(mount),
            };
        }
    }
    let mount = selected?;
    let relative = resolved.strip_prefix(mount.host.as_ref())?;
    let relative = relative.strip_prefix(b"/").unwrap_or(relative);
    Some(tools::Place { root: mount.root, path: relative.into() })
}

impl Backend {
    pub(super) fn native_advance(&mut self, env: &Env<protocol::ComponentLimits>, below: &mut Queue<Below>) {
        let owner = self.token(6);
        if let Some(snapshot) = &mut self.native
            && !snapshot.issued
        {
            snapshot.issued = true;
            below.push(Below::File {
                request: file::Request::Load {
                    owner,
                    root: snapshot.at.root,
                    path: snapshot.at.path.clone(),
                    max: self.settings.adapter.stated.file_bytes,
                    no_follow: false,
                },
                deadline: env.now.saturating_add(self.settings.startup),
            });
        }
    }

    #[expect(clippy::wildcard_enum_match_arm, reason = "skein file events are external; this owner only loads")]
    pub(super) fn native_done(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        event: file::Event,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let snapshot = self.native.take().expect("one native file snapshot");
        let version = match event {
            file::Event::Loaded { bytes, .. } => Some(tools::Version::new(digest::digest(&bytes).0)),
            file::Event::Failed { .. } | file::Event::TooLarge { .. } | file::Event::Cancelled { .. } => None,
            _ => unreachable!("native snapshot is a bounded load"),
        };
        if self.phase == Phase::Running && self.stop == super::stop::Stop::None {
            up.push(ToDomain::NativeVersion { owner: snapshot.owner, at: snapshot.at, version });
            match snapshot.after {
                After::Allow { call, input } => self.server.as_mut().expect("permission server").verdict(
                    call,
                    mcp::Verdict::Allow { updated_input: input },
                    &mut self.io,
                ),
                After::Written { slot } => {
                    self.ledger.get_mut(slot).expect("native write ledger").native_write = false;
                    self.flush_builtins(up);
                }
            }
            self.match_calls();
            self.consume_stdout(env, up, below);
        }
    }
}
