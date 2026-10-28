//! Provider identities of commands whose calls ended running (domain/session.md,
//! section 5; domain/tools.md, section 5). The kit owns execution and output;
//! this table knows only job numbers and the original shell call's metadata.
//! Result terminals release identities; close facts consume the remaining ones.

use super::{Domain, Limits, Request, addressed, tools_step};
use crate::{FactKind, ToolCall, llm::Returned};
use skein_lib::{Env, Map, Queue, Token};
use smith_domain_tools::{self as tools, JobEnd, Outcome};

#[derive(Debug)]
pub(super) struct Jobs {
    pub(super) highest: u32,
    calls: Map<u32, ToolCall>,
}

impl Jobs {
    pub(super) fn new(capacity: u32) -> Self {
        Self { highest: 0, calls: Map::with_capacity(capacity) }
    }

    pub(super) fn answered(&mut self, result: &Returned, metadata: ToolCall) {
        match result {
            Returned::Owned { outcome } => match outcome {
                Outcome::Running { job, .. } => {
                    self.highest = job.0;
                    let previous = self.calls.insert(job.0, metadata).expect("one identity per bounded job");
                    assert!(previous.is_none(), "a running shell names a fresh job");
                }
                Outcome::Job { job, end, .. } => match end {
                    JobEnd::Running => {}
                    JobEnd::Ended(_) | JobEnd::Stopped | JobEnd::Cancelled => {
                        self.calls.remove(&job.0).expect("a terminal read names a retained job");
                    }
                },
                Outcome::Read { .. }
                | Outcome::Listed { .. }
                | Outcome::Found { .. }
                | Outcome::Removed
                | Outcome::Written { .. }
                | Outcome::Edited { .. }
                | Outcome::Exited { .. }
                | Outcome::NotGranted
                | Outcome::TooLong
                | Outcome::Outside { .. }
                | Outcome::ReadOnly { .. }
                | Outcome::NotFound { .. }
                | Outcome::NotDirectory { .. }
                | Outcome::NotEmpty
                | Outcome::NotFile
                | Outcome::Linked
                | Outcome::Protected
                | Outcome::TooLarge { .. }
                | Outcome::NotRead
                | Outcome::Stale
                | Outcome::EditFailed { .. }
                | Outcome::Failed { .. }
                | Outcome::TimedOut
                | Outcome::Cancelled
                | Outcome::Busy
                | Outcome::NulByte => {}
            },
            Returned::Text { .. } | Returned::Invalid { .. } | Returned::NotRun { .. } | Returned::Withdrawn => {}
        }
    }
}

pub(crate) fn cancel(domain: &mut Domain, env: &Env<Limits>, session: Token, out: &mut Queue<Request>) {
    let Some(id) = addressed(&domain.sessions, session) else {
        return;
    };
    let kit = domain.sessions.get(id).expect("addressed session").conversation.kit;
    let heard = tools_step(&mut domain.calls, env, tools::Event::CancelJobs { kit }, out);
    assert!(heard.answer.is_none() && heard.kit.is_none(), "cancellation neither opens nor answers a call");
    cancelled(domain, heard.cancelled, out);
}

pub(super) fn cancelled(domain: &Domain, session: Option<Token>, out: &mut Queue<Request>) {
    if let Some(session) = session {
        let session = domain.sessions.get(skein_lib::Id::from_token(session)).expect("kit retains its session");
        out.push(Request::JobsCancelled { opener: session.conversation.opener });
    }
}

pub(super) fn pass_on(domain: &mut Domain, opener: Token, fact: &tools::Fact) -> bool {
    match fact.kind {
        tools::FactKind::JobEnded { session, job, owner, end, unread_bytes, started } => {
            let session = domain.sessions.get_mut(skein_lib::Id::from_token(session)).expect("kit retains its session");
            let metadata = session.conversation.jobs.calls.remove(&job.0).expect("unreported job retains its identity");
            assert_eq!(metadata.owner, owner, "job retains its original shell identity");
            let kind = FactKind::JobEnded { opener, job, end, unread_bytes, started };
            domain.facts.push_call(fact.at, kind, &metadata);
            true
        }
        tools::FactKind::Opened { .. }
        | tools::FactKind::Refused { .. }
        | tools::FactKind::Started { .. }
        | tools::FactKind::Answered { .. }
        | tools::FactKind::Closing { .. }
        | tools::FactKind::Closed { .. } => false,
    }
}
