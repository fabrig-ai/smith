//! Composed Smith runs behind the agent parent face (domain/host.md, sections
//! 9.1, 9.2 and 9.4). The agent keeps bounded slots, generation-safe handles,
//! provider and parent operation rights, exact turn acknowledgement metadata,
//! and the root domains' facts and content until the owner drains them.
//! It knows no codec, process handle, address, credential value or host policy.
//! `step`, `fire` and `resume` produce ordered parent notices and
//! lower requests. The owner routes every lower terminal, drains observations
//! each pass, and calls `reclaim` only after handing the outputs onward.
//! Workspace operations keep exact mounted IO roots and machine terminal rights.
//! Stop cancels and waits within the wall bound; the later stop increment adds
//! the host's cancel grace and explicit drop.
//!
//! | State | Input | Next state | Emits |
//! |---|---|---|---|
//! | Empty | Spawn | Live | Started; admission or answer |
//! | Live | Message, Answer, Grant, Ack | Live | Root requests |
//! | Live | Stop or wall expiry | Cancelling | Root cancellation |
//! | Live/Cancelling | Root answer | Settling | Answered; cleanup |
//! | Settling | Last terminal and ACK | Gone | Gone |
//! | Gone | Drains empty, Reclaim | Empty | Nothing |

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod domain;
mod facts;
mod limits;
mod stop;
mod translate;
mod workspace;

#[cfg(test)]
mod tests;

pub use boundary::{Below, Input, Lower, Output};
pub use domain::{Domain, fire, resume, step};
pub use facts::{Fact, FactKind};
pub use limits::{Limits, valid};
