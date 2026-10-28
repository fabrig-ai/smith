//! The local host's terminal boundary (protocol/hosts.md, section 5.1;
//! domain/host.md, section 8). It retains one bounded, possibly continued
//! line and whether an interrupt has already cancelled the current run. It
//! never knows the chat, the agent, or how the terminal is opened. `feed`,
//! `interrupt`, `answer_finished`, and `show` are its entry points. Their
//! caller reserves `max_out` and settles each output before feeding more.
//!
//! | State | Input | Output |
//! |---|---|---|
//! | Collecting | newline without trailing backslash | one person line |
//! | Collecting | newline with trailing backslash | retain continued line |
//! | Any | first interrupt | cancel the run |
//! | Cancelled | second interrupt | drop the selected child |
//! | Any | answer finished | clear interrupt count |

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod borrowed;
mod delivery_store;
pub mod format;

#[rustfmt::skip]
#[path = "generated/local_store_v1.rs"]
pub mod local_store_v1;
mod git;
mod git_child;
mod markers;
mod plain;
mod store;
mod terminal;

#[cfg(test)]
mod tests;

pub use borrowed::Borrowed;
pub use delivery_store::{decode_delivery, save_delivery};
pub use git::{Git, GitAction, GitCompletion, GitLimits};
pub use git_child::GitChild;
pub use markers::{MarkerAction, Markers};
pub use plain::{Plain, PlainAction, PlainLimits, Snapshot};
pub use store::{
    Action, CHAT_STORE_VERSION, File, History, RecordName, Store, StoreError, StoreFault, StoreLimits, StoreRequest,
    decode_state, decode_turns, save_state, save_turn, turn_name, turn_opens_window,
};
pub use terminal::{
    Entered, Event as TerminalEvent, Limits as TerminalLimits, Slash, Terminal, entered, max_out as terminal_max_out,
    notice_text, run_failure_text, valid as terminal_valid,
};
