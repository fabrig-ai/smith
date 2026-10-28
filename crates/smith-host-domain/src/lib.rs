//! Host supervision (domain/host.md, sections 2–7 and 10).
//! Keeps process proof, channel order, bounded call/turn metadata and terminal rights.
//! Charter, transcript, post-transcript answers and host policy are opaque; credentials
//! are names only. No IO, frame codec, durable policy record or agent internal state.
//! `step` and `fire` consume typed events and injected clocks; the parent reserves
//! `max_out` output slots and settles every issued operation, including during shutdown.
//! Exact agent message refusals settle issued names even while draining; pending
//! Send names protect reuse, and only Waiting establishes a watchdog claim.
//! Turns move to the parent, with exact commitment ACK metadata retained here.
//! Content-free facts are retained losslessly until the parent drains them.
//! Actual deliveries survive EOF and tree cleanup; `Gone` is the separate process
//! containment terminal, after every outstanding right. Reclaim at the iteration boundary
//! (programming-model.md, sections 4.5, 5 and 6; domain/host.md, sections 4 and 6).
#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]
extern crate alloc;

mod boundary;

pub mod parent;
pub mod process;

pub mod channel;

mod delivery;

mod domain;

mod facts;

mod inbox;

mod limits;

mod long;

mod values;

mod source_name;

mod program;

#[cfg(test)]
mod tests;

pub use boundary::{End, Exit, Fault, Forced, Input, Invalid, MessageRefusal, Output, Signal};

pub use channel::{
    Answer, AnsweredCall, Ask, BudgetRefusal, CallName, Cap, Ceiling, CompletionEvidence, CompletionFailure, Directory,
    Down, Effect, Exhausted, Form, Grant, Limit, Message, ModelFault, Overflow, Part, Phase, Policy, Rate,
    ReceivingLimit, Refusal, Reply, Report, RunFailure, RunInvalid, RunResult, SavedReply, Scheme, SourceName, Start,
    TranscriptRefusal, Turn, Up, Window,
};

pub use delivery::{
    Delivered, Delivery, DeliveryFailure, DeliveryReason, DeliveryRefusal, DeliveryStatus, Diagnostic, MAX_DIRECTORIES,
    Marker, Receipt,
};

pub use domain::{Domain, fire, max_out, step};

pub use facts::Fact;

pub use program::{FailureReason, ProgramCheck, ProgramFailure, ProgramMethod, ProgramSignOut};

pub use inbox::Inbox;

pub use limits::{Check, Limits, check, valid, worst_case};

pub use values::{Charter, Declared, Fields, Transcript, TurnValue};

// The common face uses the root vocabulary for bounded work.
pub use smith_domain::Operation;
