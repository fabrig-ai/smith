//! Mounted-root routing and retained machine rights (domain/host.md, section
//! 9.1, The workspace, and section 9.2). This module keeps each lower family
//! separate: equal callback tokens never consume another family's terminal.
//! It knows no paths outside the typed mounts, descriptors or delivery policy.
//! Routing forwards operations; settlement requests cancellation once and
//! waits for every machine and parent terminal before the slot can be gone.

#![expect(clippy::manual_let_else, reason = "checked mounts use exhaustive results")]

use alloc::boxed::Box;
use skein_lib::{List, Queue, Time, Token};
use smith_domain::{self as smith, run};
use smith_host_domain as host;

use crate::domain::Slot;
use crate::{Below, Limits, Lower, Output, translate};

/// The original root callback retained behind a unique parent call token.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Call {
    /// A run host-tool relay; parent Answer ends its exact attempt.
    Host(run::RelayName),
    /// A checked delivery; parent Answer ends the submitted delivery.
    Delivery(Token),
}

/// One run-owned machine right, whose variant selects its one terminal.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Machine {
    /// A guide read; only Read consumes it.
    Read { cancelled: bool },
    /// An executable probe; only Probed consumes it.
    Probe { cancelled: bool },
    /// A contained check; Checked or Aborted consumes it after optional Abort.
    Check { cancelled: bool },
}

pub(crate) fn io_capacity(limits: &Limits) -> u32 {
    limits.smith.session.tools.kits.checked_mul(limits.smith.session.tools.calls).expect("valid limits")
}

pub(crate) fn call_capacity(limits: &Limits) -> u32 {
    limits.smith.run.calls.checked_add(limits.smith.run.runs).expect("valid limits")
}

pub(crate) fn call(slot: &mut Slot) -> Token {
    slot.next_call = slot.next_call.checked_add(1).expect("finite root completion/call bounds fit unique parent names");
    Token::new(slot.next_call)
}

pub(crate) fn start(
    workspace: Option<Token>,
    directories: Box<[host::Directory]>,
    limits: &Limits,
) -> Result<Option<run::Workspace>, host::Invalid> {
    match workspace {
        None => {
            if !directories.is_empty() {
                return Err(host::Invalid::Directories);
            }
            Ok(None)
        }
        Some(_) => {
            let count = match u32::try_from(directories.len()) {
                Ok(count) => count,
                Err(_) => return Err(host::Invalid::Directories),
            };
            if count == 0 || count > limits.smith.run.directories {
                return Err(host::Invalid::Directories);
            }
            let mut mounted = List::with_capacity(count);
            for directory in directories {
                mounted
                    .push(run::Directory {
                        host: directory.host,
                        name: directory.name,
                        root: directory.root,
                        writable: directory.writable,
                        git: directory.git,
                        conflicts: directory.conflicts,
                    })
                    .expect("checked mount count");
            }
            Ok(Some(run::Workspace { directories: mounted.into_boxed() }))
        }
    }
}

pub(crate) fn parent_answer(call: Call, reply: host::Reply) -> Option<smith::Event> {
    match call {
        Call::Host(relay) => Some(smith::Event::HostReturned { relay, reply: translate::host_reply(reply)? }),
        Call::Delivery(owner) => match reply {
            host::Reply::Delivery(delivery) => {
                Some(smith::Event::Delivered { owner, delivery: translate::delivery(delivery)? })
            }
            host::Reply::Host { .. }
            | host::Reply::Busy
            | host::Reply::Unavailable
            | host::Reply::Withdrawn
            | host::Reply::TooLarge => None,
        },
    }
}

pub(crate) fn terminal(slot: &mut Slot, terminal: Below) -> Option<smith::Event> {
    match terminal {
        Below::Completed { owner, completion } => {
            slot.completions.remove(&owner)?;
            Some(smith::Event::Completed { owner, completion })
        }
        Below::Failed { owner, failure, evidence, detail, report } => {
            slot.completions.remove(&owner)?;
            Some(smith::Event::Failed { owner, failure, evidence, detail, report })
        }
        Below::Cancelled { owner } => {
            slot.completions.remove(&owner)?;
            Some(smith::Event::Cancelled { owner })
        }
        Below::Done { owner, done } => {
            slot.io.remove(&owner)?;
            Some(smith::Event::Done { owner, done })
        }
        Below::Read { owner, read } => match slot.machine.get(&owner)? {
            Machine::Read { .. } => {
                slot.machine.remove(&owner);
                Some(smith::Event::Read { owner, read })
            }
            Machine::Probe { .. } | Machine::Check { .. } => None,
        },
        Below::Probed { owner, executable } => match slot.machine.get(&owner)? {
            Machine::Probe { .. } => {
                slot.machine.remove(&owner);
                Some(smith::Event::Probed { owner, executable })
            }
            Machine::Read { .. } | Machine::Check { .. } => None,
        },
        Below::Checked { owner, ran } => match slot.machine.get(&owner)? {
            Machine::Check { .. } => {
                slot.machine.remove(&owner);
                Some(smith::Event::Checked { owner, ran })
            }
            Machine::Read { .. } | Machine::Probe { .. } => None,
        },
        Below::Aborted { owner } => match slot.machine.get(&owner)? {
            Machine::Check { .. } => {
                slot.machine.remove(&owner);
                Some(smith::Event::Aborted { owner })
            }
            Machine::Read { .. } | Machine::Probe { .. } => None,
        },
    }
}

pub(crate) fn io(
    slot: &mut Slot,
    agent: Token,
    owner: Token,
    op: smith::tools::Op,
    deadline: Time,
    out: &mut Queue<Output>,
) {
    assert!(
        slot.io.insert(owner, false).expect("bounded root tool rights").is_none(),
        "root machine requests own one callback right"
    );
    out.push(Output::Lower { agent, request: Lower::Io { owner, op, deadline } });
}

pub(crate) fn job(slot: &mut Slot, agent: Token, owner: Token, act: smith::tools::JobAct, out: &mut Queue<Output>) {
    match act {
        smith::tools::JobAct::Hasten => {
            out.push(Output::Lower { agent, request: Lower::Job { owner, act } });
            return;
        }
        smith::tools::JobAct::Read { .. } | smith::tools::JobAct::Stop { .. } => {}
    }
    assert!(
        slot.io.insert(owner, false).expect("bounded root tool rights").is_none(),
        "root machine requests own one callback right"
    );
    out.push(Output::Lower { agent, request: Lower::Job { owner, act } });
}

pub(crate) fn cancel_io(slot: &mut Slot, agent: Token, owner: Token, out: &mut Queue<Output>) {
    if let Some(cancelled) = slot.io.get_mut(&owner)
        && !*cancelled
    {
        *cancelled = true;
        out.push(Output::Lower { agent, request: Lower::CancelIo { owner } });
    }
}

pub(crate) fn read(
    slot: &mut Slot,
    agent: Token,
    owner: Token,
    at: run::Place,
    max: u32,
    deadline: Time,
    out: &mut Queue<Output>,
) {
    assert!(
        slot.machine.insert(owner, Machine::Read { cancelled: false }).expect("bounded run discovery rights").is_none(),
        "root machine requests own one callback right"
    );
    out.push(Output::Lower { agent, request: Lower::Read { owner, at, max, deadline } });
}

pub(crate) fn probe(
    slot: &mut Slot,
    agent: Token,
    owner: Token,
    at: run::Place,
    deadline: Time,
    out: &mut Queue<Output>,
) {
    assert!(
        slot.machine
            .insert(owner, Machine::Probe { cancelled: false })
            .expect("bounded run discovery rights")
            .is_none(),
        "root machine requests own one callback right"
    );
    out.push(Output::Lower { agent, request: Lower::Probe { owner, at, deadline } });
}

pub(crate) fn check(
    slot: &mut Slot,
    agent: Token,
    owner: Token,
    program: run::Place,
    deadline: Time,
    tail: u32,
    out: &mut Queue<Output>,
) {
    assert!(
        slot.machine.insert(owner, Machine::Check { cancelled: false }).expect("bounded run check rights").is_none(),
        "root machine requests own one callback right"
    );
    out.push(Output::Lower { agent, request: Lower::Check { owner, program, deadline, tail } });
}

pub(crate) fn abort(slot: &mut Slot, agent: Token, owner: Token, out: &mut Queue<Output>) {
    if let Some(pending) = slot.machine.get_mut(&owner) {
        match pending {
            Machine::Check { cancelled } => {
                if !*cancelled {
                    *cancelled = true;
                    out.push(Output::Lower { agent, request: Lower::Abort { owner } });
                }
            }
            Machine::Read { cancelled } | Machine::Probe { cancelled } => {
                if !*cancelled {
                    *cancelled = true;
                    out.push(Output::Lower { agent, request: Lower::CancelIo { owner } });
                }
            }
        }
    }
}

pub(crate) fn cancel(slot: &mut Slot, agent: Token, out: &mut Queue<Output>) {
    let mut owners = List::with_capacity(slot.io.capacity());
    for (owner, _) in &slot.io {
        owners.push(*owner).expect("one name per IO right");
    }
    for owner in &owners {
        cancel_io(slot, agent, *owner, out);
    }
    let mut owners = List::with_capacity(slot.machine.capacity());
    for (owner, _) in &slot.machine {
        owners.push(*owner).expect("one name per run machine right");
    }
    for owner in &owners {
        abort(slot, agent, *owner, out);
    }
}
