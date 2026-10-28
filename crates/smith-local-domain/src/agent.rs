//! Agent composition and typed parent commands (domain/host.md, sections 8
//! and 9.4). This module retains the child and its receiving bounds; it knows
//! no process descriptors, credentials, wire bytes or durable store format.
//! Commands and terminals enter either child; its outputs remain ordered
//! for the local policy to consume.

use crate::{AgentLimits, Invalid};
use alloc::boxed::Box;
use skein_lib::List;
use skein_lib::{Env, Queue, Time};
use smith_domain as root;
use smith_domain::{Answered, run};
use smith_host_domain::{self as host, channel};
use smith_inline_agent as inline;

/// A sealed local terminal did not fit the host's matching sealed vocabulary.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BridgeError {
    /// A sealed terminal could not be constructed.
    Terminal,
    /// The charter exceeds the receiving bound.
    Charter,
    /// The concrete transcript exceeds its receiving bound.
    Transcript,
    /// A count does not fit the receiving vocabulary.
    Values,
}

/// Bound the typed local policy and history while moving them into the host Start.
pub(crate) fn prepare_start(
    source: RunStart,
    charter_bytes: u64,
    transcript_bytes: u64,
) -> Result<host::Start, BridgeError> {
    let (workspace, directories) = match source.workspace {
        Some(workspace) => {
            let Ok(count) = u32::try_from(workspace.directories.len()) else {
                return Err(BridgeError::Values);
            };
            let mut directories = List::with_capacity(count);
            for directory in workspace.directories {
                let item = channel::Directory {
                    host: directory.host,
                    root: directory.root,
                    name: directory.name,
                    writable: directory.writable,
                    git: directory.git,
                    conflicts: directory.conflicts,
                };
                if directories.push(item).is_err() {
                    return Err(BridgeError::Values);
                }
            }
            (Some(skein_lib::Token::new(1)), directories.into_boxed())
        }
        None => (None, Box::<[channel::Directory]>::default()),
    };
    let charter = host::Charter::new(source.charter, charter_bytes).ok_or(BridgeError::Charter)?;
    let transcript = match source.transcript {
        Some(history) => Some(host::Transcript::new(history, transcript_bytes).ok_or(BridgeError::Transcript)?),
        None => None,
    };
    let Ok(count) = u32::try_from(source.answered.len()) else {
        return Err(BridgeError::Terminal);
    };
    let mut answered = List::with_capacity(count);
    for call in source.answered {
        let record = channel::AnsweredCall {
            name: name_to_host(call.name),
            tool: call.tool,
            reply: saved_reply_to_host(call.answer)?,
        };
        if answered.push(record).is_err() {
            return Err(BridgeError::Terminal);
        }
    }
    Ok(host::Start {
        live: if source.live_text {
            Some(channel::LiveAsk { text: true, reasoning: false, input: false })
        } else {
            None
        },
        messages: source.messages,
        logical_run: skein_lib::Token::new(1),
        identity: Some(source.identity),
        activation: source.activation,
        workspace,
        charter,
        transcript,
        answered: answered.into_boxed(),
        directories,
        grants: source.grants,
    })
}

/// Preserve one transcript-derived name across the local and host domains.
#[must_use]
pub(crate) const fn name_to_host(name: run::CallName) -> channel::CallName {
    channel::CallName { activation: name.activation, completion: name.completion, position: name.position }
}

/// Carry a settled local answer into the host's saved-call record.
pub(crate) fn saved_reply_to_host(answer: Answered) -> Result<channel::SavedReply, BridgeError> {
    match answer {
        Answered::Host(answer) => {
            Ok(channel::SavedReply::Host { error: answer.error(), body: Box::from(answer.text()) })
        }
        Answered::Delivery(delivery) => Ok(channel::SavedReply::Delivery(Box::new(delivery_to_host(*delivery)?))),
        Answered::TooLarge => Ok(channel::SavedReply::TooLarge),
    }
}

/// Preserve a durable local delivery terminal at the spawned-agent boundary.
pub(crate) fn delivery_to_host(delivery: run::Delivery) -> Result<host::Delivery, BridgeError> {
    match delivery {
        run::Delivery::Delivered(delivered) => {
            let Ok(count) = u32::try_from(delivered.receipts().len()) else {
                return Err(BridgeError::Terminal);
            };
            let mut receipts = List::with_capacity(count);
            for receipt in delivered.receipts() {
                let item =
                    host::Receipt::new(receipt.directory(), Box::from(receipt.text())).ok_or(BridgeError::Terminal)?;
                if receipts.push(item).is_err() {
                    return Err(BridgeError::Terminal);
                }
            }
            let value = host::Delivered::new(receipts.into_boxed()).ok_or(BridgeError::Terminal)?;
            Ok(host::Delivery::Delivered(value))
        }
        run::Delivery::Nothing => Ok(host::Delivery::Nothing),
        run::Delivery::Refused(refused) => {
            let marker = match refused.marker() {
                Some(marker) => {
                    Some(host::Marker::new(marker.directory(), Box::from(marker.path())).ok_or(BridgeError::Terminal)?)
                }
                None => None,
            };
            let value =
                host::DeliveryRefusal::new(marker, Box::from(refused.explanation())).ok_or(BridgeError::Terminal)?;
            Ok(host::Delivery::Refused(value))
        }
        run::Delivery::Failed(failed) => {
            let reason = match failed.reason {
                run::DeliveryReason::Unreachable => host::DeliveryReason::Unreachable,
                run::DeliveryReason::RefusedByTarget => host::DeliveryReason::RefusedByTarget,
                run::DeliveryReason::TimedOut => host::DeliveryReason::TimedOut,
                run::DeliveryReason::Broken => host::DeliveryReason::Broken,
                run::DeliveryReason::TooLarge => host::DeliveryReason::TooLarge,
                run::DeliveryReason::Missing => host::DeliveryReason::Missing,
                run::DeliveryReason::Busy => host::DeliveryReason::Busy,
                run::DeliveryReason::Unavailable => host::DeliveryReason::Unavailable,
                run::DeliveryReason::Cancelled => host::DeliveryReason::Cancelled,
                run::DeliveryReason::Unknown => host::DeliveryReason::Unknown,
            };
            Ok(host::Delivery::Failed(host::DeliveryFailure {
                directory: failed.directory,
                reason,
                diagnostic: host::Diagnostic::new(failed.diagnostic.output(), failed.diagnostic.cut()),
            }))
        }
        run::Delivery::Stale => Ok(host::Delivery::Stale),
    }
}

/// Local-owned child behind the common parent face (domain/host.md, sections 8–9).
#[derive(Debug)]
#[expect(clippy::large_enum_variant, reason = "bounded child state is priced inline in the ownership bound")]
pub(crate) enum Agent {
    /// Native Smith roots with LLM and machine effects delegated to the owner.
    Inline { domain: inline::Domain, limits: inline::Limits, out: Queue<inline::Output> },
    /// The host kit supervising an agent process.
    Spawned { domain: host::Domain, limits: host::Limits, out: Queue<host::Output> },
}

/// One child notice or an operation whose terminal belongs to that child.
#[expect(clippy::large_enum_variant, reason = "typed lower payloads move once through the bounded child output")]
pub(crate) enum Output {
    Parent(host::parent::Request),
    Lower { agent: skein_lib::Token, request: inline::Lower },
    Process(host::process::Request),
}

impl Agent {
    pub(crate) fn new(limits: &AgentLimits, configuration: root::Config, seed: u64) -> Result<Self, Invalid> {
        match limits {
            AgentLimits::Inline(limits) => {
                if !inline::valid(limits) {
                    return Err(Invalid::Limits);
                }
                Ok(Self::Inline {
                    domain: inline::Domain::new(limits, configuration, seed),
                    limits: *limits,
                    out: Queue::uncapped(),
                })
            }
            AgentLimits::Spawned(limits) => {
                if !host::valid(limits) {
                    return Err(Invalid::Limits);
                }
                Ok(Self::Spawned { domain: host::Domain::new(limits), limits: *limits, out: Queue::uncapped() })
            }
        }
    }

    pub(crate) fn prepare(&self, source: RunStart) -> Result<host::Start, BridgeError> {
        match self {
            Self::Inline { limits, .. } => {
                prepare_start(source, limits.smith.run.run_bytes, limits.smith.session.history_bytes)
            }
            Self::Spawned { limits, .. } => prepare_start(source, limits.charter_bytes, limits.transcript_bytes),
        }
    }

    /// Child facts and unrouted output counted by the local service (protocol/limits.md 3.8).
    pub(crate) fn backlog(&self) -> u32 {
        match self {
            Self::Inline { domain, out, .. } => {
                domain.backlog().checked_add(out.len()).expect("local agent backlog fits u32")
            }
            Self::Spawned { domain, out, .. } => {
                domain.backlog().checked_add(out.len()).expect("local agent backlog fits u32")
            }
        }
    }

    pub(crate) fn ready(&self) -> bool {
        match self {
            Self::Inline { domain, .. } => domain.is_ready(),
            Self::Spawned { .. } => false,
        }
    }

    pub(crate) fn deadline(&self) -> Option<Time> {
        match self {
            Self::Inline { domain, .. } => domain.next_deadline(),
            Self::Spawned { domain, .. } => domain.next_deadline(),
        }
    }

    pub(crate) fn process(&mut self, now: Time, wall: skein_lib::Wall, event: host::process::Event) {
        match self {
            Self::Inline { .. } => unreachable!("process terminals belong to the spawned kind"),
            Self::Spawned { domain, limits, out } => {
                host::step(domain, &Env { now, wall, limits: *limits }, host::Input::Process(event), out);
            }
        }
    }

    pub(crate) fn below(&mut self, now: Time, wall: skein_lib::Wall, agent: skein_lib::Token, terminal: inline::Below) {
        match self {
            Self::Inline { domain, limits, out } => {
                inline::step(
                    domain,
                    &Env { now, wall, limits: *limits },
                    inline::Input::Below { agent, terminal },
                    out,
                );
            }
            Self::Spawned { .. } => unreachable!("native terminals belong to the inline kind"),
        }
    }

    pub(crate) fn fire(&mut self, now: Time, wall: skein_lib::Wall) {
        match self {
            Self::Inline { domain, limits, out } => inline::fire(domain, &Env { now, wall, limits: *limits }, out),
            Self::Spawned { domain, limits, out } => host::fire(domain, &Env { now, wall, limits: *limits }, out),
        }
    }

    pub(crate) fn resume(&mut self, now: Time, wall: skein_lib::Wall) {
        match self {
            Self::Inline { domain, limits, out } => inline::resume(domain, &Env { now, wall, limits: *limits }, out),
            Self::Spawned { .. } => {}
        }
    }

    pub(crate) fn pop(&mut self) -> Option<Output> {
        match self {
            Self::Inline { out, .. } => match out.pop() {
                Some(inline::Output::Parent(request)) => Some(Output::Parent(request)),
                Some(inline::Output::Lower { agent, request }) => Some(Output::Lower { agent, request }),
                None => None,
            },
            Self::Spawned { out, .. } => match out.pop() {
                Some(host::Output::Parent(request)) => Some(Output::Parent(request)),
                Some(host::Output::Process(request)) => Some(Output::Process(request)),
                None => None,
            },
        }
    }

    pub(crate) fn reclaim(&mut self) {
        match self {
            Self::Inline { domain, .. } => domain.reclaim(),
            Self::Spawned { domain, .. } => domain.reclaim(),
        }
    }
}

pub(crate) fn command(agent: &mut Agent, now: Time, wall: skein_lib::Wall, event: host::parent::Event) {
    match agent {
        Agent::Inline { domain, limits, out } => {
            inline::step(domain, &Env { now, wall, limits: *limits }, inline::Input::Parent(event), out);
        }
        Agent::Spawned { domain, limits, out } => {
            host::step(domain, &Env { now, wall, limits: *limits }, host::Input::Parent(event), out);
        }
    }
}

/// The local policy's owned run values before sealing at the typed parent face.
#[derive(Debug)]
pub(crate) struct RunStart {
    pub(crate) live_text: bool,
    pub(crate) messages: Box<[host::Message]>,
    pub(crate) identity: run::Identity,
    pub(crate) activation: u64,
    pub(crate) charter: run::charter::Charter,
    pub(crate) workspace: Option<run::Workspace>,
    pub(crate) transcript: Option<root::Transcript>,
    pub(crate) answered: Box<[root::AnsweredCall]>,
    pub(crate) grants: Box<[host::Grant]>,
}

/// Separate accepted text from the typed failure notice the terminal renders.
pub(crate) fn answer_text(answer: &host::Answer) -> Box<[u8]> {
    match &answer.result {
        host::RunResult::Accepted { outcome } => match outcome.value() {
            run::outcome::Declared::Report(report) => report.text.clone(),
            run::outcome::Declared::Failure(failure) => failure.reason.clone(),
            run::outcome::Declared::Verdict(verdict) => verdict.text.clone(),
            run::outcome::Declared::Change(_) => Box::from(&b"Change delivered"[..]),
        },
        host::RunResult::Parked => Box::from(&b"Chat parked"[..]),
        host::RunResult::Refused { .. } | host::RunResult::Failed { .. } => Box::new([]),
    }
}

/// Name the bounded value that prevented preparing the common Start.
pub(crate) fn preparation_refusal(error: BridgeError) -> host::Invalid {
    match error {
        BridgeError::Terminal => host::Invalid::Answered,
        BridgeError::Charter => host::Invalid::Charter,
        BridgeError::Transcript => host::Invalid::Transcript,
        BridgeError::Values => host::Invalid::Limits,
    }
}
