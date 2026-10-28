//! Issued provider and command notices (domain/host.md, section 4; protocol/channel.md,
//! section 4). The root's bounded map retains IO owners and logical runs.
//! These entry points announce actual provider/Spawn issuance and actual terminals,
//! never a cancellation request or provider/tool syntax.

use crate::{Domain, Operation, Request, domain::IssuedCompletion, run, tools};
use skein_lib::{Queue, Time, Token};

pub(crate) fn command(domain: &mut Domain, owner: Token, op: &tools::Op, deadline: Time, out: &mut Queue<Request>) {
    let spawn = match op {
        tools::Op::Spawn { .. } => true,
        tools::Op::Remove { .. }
        | tools::Op::Load { .. }
        | tools::Op::List { .. }
        | tools::Op::Store { .. }
        | tools::Op::Search { .. } => false,
    };
    if spawn {
        let conversation = domain.session.io_opener(owner).expect("issued command retains its session");
        let host_run = run::host_name(&domain.run, conversation).expect("issued command retains its run");
        let old = domain.commands.insert(owner, host_run).expect("one command per live tools call");
        assert!(old.is_none(), "the previous operation has actually ended");
        out.push(Request::Long { host_run, operation: Operation::Command, until: deadline });
    }
}

pub(crate) fn command_done(domain: &mut Domain, owner: Token) {
    if let Some(host_run) = domain.commands.remove(&owner) {
        domain.notices.push(Request::LongDone { host_run });
    }
}

pub(crate) fn begin_completion(
    domain: &mut Domain,
    owner: Token,
    issued: IssuedCompletion,
    conversation: Token,
    until: Time,
    out: &mut Queue<Request>,
) {
    let inserted = domain.completions.insert(owner, issued).expect("one completion per session");
    assert!(inserted.is_none(), "the previous completion ended");
    let host_run = run::host_name(&domain.run, conversation).expect("issued completion retains its run");
    out.push(Request::Long { host_run, operation: Operation::Completion, until });
}

/// A provider terminal ends the actual issued span before session work proceeds.
pub(crate) fn completion_done(domain: &mut Domain, owner: Token) -> IssuedCompletion {
    let issued = domain.completions.remove(&owner).expect("provider terminal belongs to an emitted call");
    let id = *domain.sessions.get(&owner).expect("session lives through its provider terminal");
    let conversation = domain.peers.get(id).expect("peer lives through its provider terminal").conversation;
    let host_run = run::host_name(&domain.run, conversation).expect("run lives through its session's terminal");
    domain.notices.push(Request::LongDone { host_run });
    issued
}

/// A held command's read or stop owns a new bounded long-operation span.
pub(crate) fn job(
    domain: &mut Domain,
    env: &skein_lib::Env<crate::Limits>,
    owner: Token,
    act: tools::JobAct,
    out: &mut Queue<Request>,
) {
    let wait = match act {
        tools::JobAct::Hasten => return,
        tools::JobAct::Read { wait, .. } => wait,
        tools::JobAct::Stop { .. } => env.limits.session.tools.tool_deadline,
    };
    let conversation = domain.session.io_opener(owner).expect("job owner retains its session");
    let host_run = run::host_name(&domain.run, conversation).expect("job retains its run");
    let old = domain.commands.insert(owner, host_run).expect("one active job act per owner");
    assert!(old.is_none(), "the previous job act ended");
    out.push(Request::Long {
        host_run,
        operation: Operation::Command,
        until: env.now.saturating_add(wait.min(env.limits.session.tools.tool_deadline)),
    });
}
