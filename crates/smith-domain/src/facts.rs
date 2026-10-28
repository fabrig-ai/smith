//! What the agent tells whoever watches it (domain/run.md, section 11): the
//! child domains' facts, content-free, gathered after every entry point into one
//! bounded queue the loop drains at its own pace. The owner reserves room
//! before each entrance; every observation is preserved until it is drained.
//!
//! The run names a conversation by its token for it, and the sessions name
//! theirs by their opener's, which is the same token: the facts of a
//! conversation and of its session go together.

use alloc::boxed::Box;
use skein_lib::{Time, Token};
use smith_domain_run::facts as run;
use smith_domain_session as session;

/// Content the protocol projects into the channel's fact payload. The engine
/// applies capture policy; the agent's queue drops and counts overflow.
/// Provider observations carry the issued completion snapshot alongside its
/// session owner token. Neither later completions nor lost facts can rename
/// queued observations. `Request::Complete` supplies each start independently
/// of capture, including retries that produce no text (domain/session.md,
/// sections 3–4). Tool observations keep their operation owner instead.
#[derive(PartialEq, Eq, Debug)]
pub enum Content {
    /// Owned bounded text in its original provider position.
    Text { owner: Token, completion: crate::llm::CompletionName, text: Box<[u8]> },
    /// Provider tool-call content observed at the root boundary; capture never authorizes or executes it.
    Call {
        owner: Token,
        /// The issued completion snapshot, independent of fact delivery and later session state.
        completion: crate::llm::CompletionName,
        id: Box<[u8]>,
        name: Box<[u8]>,
        /// Provider-written tool-argument bytes, retained exactly for replay.
        input: Box<[u8]>,
    },
    /// Content observation of a typed IO terminal; capture may drop it without changing decisions.
    Tool { owner: Token, done: crate::tools::Done },
    /// Content observation of provider-reported usage; charging does not depend on capture.
    Usage { owner: Token, completion: crate::llm::CompletionName, usage: crate::llm::Usage },
}

pub(crate) fn done_bytes(done: &crate::tools::Done) -> u64 {
    use crate::tools::Done;
    match done {
        Done::Loaded { content, .. } => bytes(content),
        Done::Listed { entries, .. } => {
            let mut held = fixed(entries.len(), size_of::<crate::tools::Entry>());
            for entry in entries {
                held = held.saturating_add(bytes(entry.name.as_ref()));
            }
            held
        }
        Done::Exited { head, tail, .. } | Done::Held { head, tail, .. } | Done::Job { head, tail, .. } => {
            bytes(head).saturating_add(bytes(tail))
        }
        Done::Found { hits, .. } => {
            let mut held = fixed(hits.len(), size_of::<crate::tools::Hit>());
            for hit in hits {
                held = held.saturating_add(bytes(&hit.path)).saturating_add(bytes(&hit.text));
            }
            held
        }
        Done::Removed
        | Done::NotEmpty
        | Done::Stored { .. }
        | Done::Conflict { .. }
        | Done::Missing
        | Done::NotFile
        | Done::Linked
        | Done::NotDirectory
        | Done::TooLarge { .. }
        | Done::Escapes
        | Done::Failed { .. }
        | Done::TimedOut
        | Done::Cancelled => 0,
    }
}

pub(crate) fn bytes(value: &[u8]) -> u64 {
    u64::try_from(value.len()).unwrap_or(u64::MAX)
}

fn fixed(count: usize, size: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX).saturating_mul(u64::try_from(size).unwrap_or(u64::MAX))
}

/// Something that happened in a child domain.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Fact {
    /// In the run child domain.
    Run {
        /// Content-free child observation preserved in the owner's reserved room.
        fact: run::Fact,
        /// Actual opening metadata, copied before the request passes to its session.
        opening: Option<Opening>,
    },
    /// In the session child domain, or its tools.
    Session {
        /// Content-free child observation preserved in the owner's reserved room.
        fact: session::Fact,
    },
}

impl Fact {
    /// The injected time of the child step, independent of drain time.
    #[must_use]
    pub const fn emitted(&self) -> Time {
        match self {
            Fact::Run { fact, opening: _ } => fact.at,
            Fact::Session { fact } => fact.at,
        }
    }
}

/// The run's actual conversation opening, sent with its opened fact to the parent's sinks.
/// The model name is one of the root's checked configured declarations; no prompt is retained.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Opening {
    pub activation: u64,
    pub ordinal: u32,
    pub llm: smith_domain_run::charter::Llm,
}
