//! A process follows one window (protocol/programs.md 5.8; domain/session.md 2).
//! Keeps the last recorded id, never the program's session contents.
//! Affinity and the first rendered message identify the window.

use super::{Backend, Below, Phase, compact, turns};
use skein_lib::{Env, List, Queue, sha256::Sha256};
use smith_domain::llm::{self, Affinity, Prompt};
use smith_protocol_llm::{self as protocol, FromDomain, ToDomain};

/// A completion keeps its process or opens the identified window's process.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Opening {
    /// The process already holds this window.
    Running,
    /// No process holds the window; start its deterministic session.
    New { id: [u8; 16] },
    /// Resume a persisted window after its process stopped.
    Resume { id: [u8; 16] },
}

/// UUID v4 bits over sha256(domain, affinity, sha256(first rendered message)).
pub(crate) fn session_id(affinity: Affinity, first: &[u8]) -> [u8; 16] {
    let mut message = Sha256::new();
    message.update(first);
    identify(affinity, message.finish())
}

fn identify(affinity: Affinity, first: [u8; 32]) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"smith program session");
    hash.update(&affinity.key);
    hash.update(&affinity.thread.to_le_bytes());
    hash.update(&first);
    let mut id: [u8; 16] = hash.finish().get(..16).expect("hash prefix").try_into().expect("UUID prefix");
    id[6] = (id[6] & 15) | 64;
    id[8] = (id[8] & 63) | 128;
    id
}

pub(crate) fn opening(prompt: &Prompt, running: Option<[u8; 16]>, recorded: Option<[u8; 16]>) -> Opening {
    let mut first = Sha256::new();
    first.update(b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[");
    let mut separator = false;
    if let Some(message) = prompt.messages.first() {
        for block in &message.content {
            match block {
                llm::Block::Text { text, .. } => {
                    if separator {
                        first.update(b",");
                    }
                    separator = true;
                    first.update(b"{\"type\":\"text\",\"text\":\"");
                    quoted(&mut first, text);
                    first.update(b"\"}");
                }
                llm::Block::ToolCall { .. }
                | llm::Block::ToolResult { .. }
                | llm::Block::Opaque { .. }
                | llm::Block::Refusal { .. } => {}
            }
        }
    }
    first.update(b"]}}\n");
    let id = identify(prompt.affinity, first.finish());
    if running == Some(id) {
        return Opening::Running;
    }
    if running.is_none() {
        if recorded == Some(id) {
            return Opening::Resume { id };
        }
        for message in &prompt.messages {
            if message.role == llm::Role::Assistant {
                return Opening::Resume { id };
            }
        }
    }
    Opening::New { id }
}

fn quoted(hash: &mut Sha256, text: &[u8]) {
    for byte in text {
        match byte {
            b'"' => hash.update(br#"\""#),
            b'\\' => hash.update(br"\\"),
            b'\n' => hash.update(br"\n"),
            b'\r' => hash.update(br"\r"),
            b'\t' => hash.update(br"\t"),
            8 => hash.update(br"\b"),
            12 => hash.update(br"\f"),
            0..=31 => {
                let hex = b"0123456789abcdef";
                hash.update(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    *hex.get(usize::from(byte / 16)).expect("high nibble"),
                    *hex.get(usize::from(byte % 16)).expect("low nibble"),
                ]);
            }
            _ => hash.update(&[*byte]),
        }
    }
}

pub(super) fn uuid(id: [u8; 16]) -> [u8; 36] {
    let mut result = [b'-'; 36];
    let hex = b"0123456789abcdef";
    let mut at = 0_usize;
    for byte in id {
        if [8, 13, 18, 23].contains(&at) {
            at = at.checked_add(1).expect("UUID");
        }
        *result.get_mut(at).expect("UUID position") = *hex.get(usize::from(byte / 16)).expect("high nibble");
        *result.get_mut(at.checked_add(1).expect("UUID")).expect("UUID position") =
            *hex.get(usize::from(byte % 16)).expect("low nibble");
        at = at.checked_add(2).expect("UUID");
    }
    result
}

impl Backend {
    pub(super) fn remember_session(&mut self) {
        if self.phase == Phase::Running && !self.switching && !self.compaction() && self.outgoing.is_none() {
            self.recorded = self.running;
        }
    }

    pub(super) fn window_request(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        request: FromDomain,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let (owner, prompt) = match &request {
            FromDomain::Complete { owner, prompt, .. } => (*owner, prompt),
            FromDomain::Cancel { .. } | FromDomain::Yield { .. } => unreachable!("completion route"),
        };
        let keep = !compact::is_compaction(prompt)
            && !self.switching
            && match opening(prompt, self.running, self.recorded) {
                Opening::Running => true,
                Opening::New { .. } | Opening::Resume { .. } => false,
            };
        if keep {
            self.resume(env, request, up, below);
            return;
        }
        let active = self.active_turn();
        let stopping = self.switching;
        self.switching = true;
        self.owner = Some(owner);
        self.request = Some(request);
        self.outgoing = None;
        if !stopping {
            self.stop(env.now, active, false);
        }
    }

    pub(super) fn switch_progress(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        if self.phase == Phase::Ending
            || !self.switching
            || self.native.is_some()
            || self.child.is_some()
            || self.spawning
            || self.pipes != [None; 3]
        {
            return;
        }
        if !self.server_ended
            && let Some(server) = &mut self.server
        {
            server.close(&mut self.io);
            return;
        }
        self.server = None;
        self.server_ended = false;
        self.switching = false;
        self.running = None;
        self.stop = super::stop::Stop::None;
        if self.winding != super::stop::WindDown::YieldRequested {
            self.winding = super::stop::WindDown::Ordinary;
        }
        self.stopping = 0;
        self.exit = None;
        self.eof_deadline = None;
        self.drain_deadline = None;
        self.deadline = None;
        self.stdout_demanding = false;
        self.intake = List::with_capacity(4096);
        self.intake_at = 0;
        self.intake_due = None;
        self.line = List::with_capacity(self.settings.line_bytes);
        self.stderr = List::with_capacity(self.settings.receiving.max_failure_bytes);
        self.tail = 0;
        self.stderr_wrapped = false;
        self.written = 0;
        self.ledger =
            List::with_capacity(self.settings.receiving.max_completion_blocks.checked_mul(5).expect("ledger"));
        self.turns = turns::Turns::new(&self.settings.receiving, self.settings.adapter.client.answer);
        self.permitted = 1;
        if self.owner.is_some() {
            self.phase = Phase::Starting;
            self.prepare_invocation(env, up, below);
        } else {
            self.request = None;
            self.phase = Phase::Idle;
        }
    }

    pub(super) fn prepare_invocation(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let prompt = match &self.request {
            Some(FromDomain::Complete { prompt, .. }) => prompt,
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => unreachable!("startup request"),
        };
        if compact::is_compaction(prompt) {
            self.resuming = false;
            self.names.session = self.compaction_id();
            self.store_instructions(env.now, 0, below);
        } else {
            match opening(prompt, None, self.recorded) {
                kind @ (Opening::New { .. } | Opening::Resume { .. }) => {
                    let id = match kind {
                        Opening::New { id } => {
                            self.resuming = false;
                            id
                        }
                        Opening::Resume { id } => {
                            self.resuming = true;
                            id
                        }
                        Opening::Running => unreachable!("opening a process"),
                    };
                    self.running = Some(id);
                    self.names.session = uuid(id);
                }
                Opening::Running => unreachable!("no process"),
            }
            let entropy: [u8; 32] = self.names.mcp.expose().try_into().expect("MCP entropy");
            match crate::mcp::Server::new(self.first, 0, &entropy, self.tools(), self.settings.mcp, &mut self.io) {
                Ok(server) => self.server = Some(server),
                Err(_) => self.fail(
                    env,
                    llm::Failure::Program { exit: None },
                    b"smith: MCP inventory exceeds bounds",
                    up,
                    below,
                ),
            }
        }
    }

    fn compaction_id(&mut self) -> [u8; 36] {
        self.compactions = self.compactions.checked_add(1).expect("bounded run turns");
        let key = self.names.mcp.expose().get(..16).expect("entropy prefix").try_into().expect("entropy prefix");
        let id = session_id(Affinity { key, thread: self.compactions }, b"compaction");
        uuid(id)
    }
}
