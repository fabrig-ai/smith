//! Owned requests and terminals at the machine boundary. Only the caller
//! retains domain state; io owns descriptors (protocol/agent.md, section 2).

use alloc::boxed::Box;

use skein_io::file;
use skein_lib::{Time, Token};
use smith_domain::{run, tools};

/// A domain request sent by the agent service to the machine.
#[derive(Clone, Debug)]
pub enum FromDomain {
    /// One tools operation, ending in `ToDomain::Done`.
    Op { owner: Token, op: tools::Op, deadline: Time },
    /// Withdraw one operation; its terminal remains owed.
    Cancel { owner: Token },
    /// Read or stop a held command; ends in one `ToDomain::Done`.
    Job { owner: Token, act: tools::JobAct },
    /// Read a bounded UTF-8 guide prefix; ends in `ToDomain::Read`.
    Read { owner: Token, at: run::Place, max: u32, deadline: Time },
    /// Check whether one root-relative path names an executable file.
    Probe { owner: Token, at: run::Place, deadline: Time },
    /// Run the selected check executable and retain its output tail.
    Check { owner: Token, program: run::Place, deadline: Time, tail: u32 },
    /// Stop one running check; its terminal remains owed.
    Abort { owner: Token },
}

/// One terminal returned to the agent service for the domain.
#[derive(Debug, PartialEq, Eq)]
pub enum ToDomain {
    /// The tools operation's single terminal.
    Done { owner: Token, done: tools::Done },
    /// A bounded guide read's single terminal.
    Read { owner: Token, read: run::Read },
    /// A check path's single executable probe terminal.
    Probed { owner: Token, executable: bool },
    /// One check process's terminal, after its pipes and child close.
    Checked { owner: Token, ran: run::Ran },
    /// A stopped check's terminal, after the process is gone.
    Aborted { owner: Token },
}

/// A request to skein's file driver, in domain emission order.
#[derive(Debug, PartialEq, Eq)]
pub enum Below {
    /// One bounded operation with its absolute domain deadline; the service
    /// calls `file_layer::down_until` after earlier requests are submitted.
    File { request: file::Request, deadline: Time },
    /// Withdraw this active file owner through `file_layer::cancel`.
    CancelFile { owner: Token },
    /// Open a path through its root token; the service resolves the root
    /// with `FileIo::descriptor` before calling `file_layer::down_until`.
    OpenRead { owner: Token, root: Token, path: Box<[u8]>, deadline: Time },
    /// Open a listing directory through the installed root, closed before rg starts.
    OpenDirectory { owner: Token, root: Token, path: Box<[u8]>, no_follow: bool, deadline: Time },
    /// Remove one name beneath an open parent; the service resolves its descriptor.
    Remove { owner: Token, parent: Token, name: Box<[u8]>, directory: bool, deadline: Time },
    /// Spawn below io with its root resolved by the service.
    Spawn { owner: Token, root: Token, spawn: Spawn },
    /// One request to the child or its pipes.
    Process(skein_io::Request),
}

/// Process fields independent of the root descriptor owned by io.
#[derive(Debug, PartialEq, Eq)]
pub struct Spawn {
    /// Executable path.
    pub program: Box<[u8]>,
    /// Arguments after argv zero.
    pub args: Box<[Box<[u8]>]>,
    /// Complete explicit environment entries, each `name=value`.
    pub env: Box<[Box<[u8]>]>,
    /// Working directory beneath the root.
    pub dir: Box<[u8]>,
    /// Standard output and error pipe descriptors requested from io.
    pub pipes: Box<[skein_io::kernel::Pipe]>,
}

/// IO progress whose variant preserves the token's namespace (programming-model.md, section 4.2).
#[derive(Debug)]
pub enum BelowEvent {
    /// One file operation ended.
    File(file::Event),
    /// Child progress sent by the service with its domain owner; `Closed` ends the child.
    Process(skein_io::Event),
    /// Pipe progress sent by the service with its io token; `Closed` ends the pipe binding.
    Pipe(skein_io::Event),
}
