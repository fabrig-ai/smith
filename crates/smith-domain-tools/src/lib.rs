//! The smith agent's tools child domain
//! (programming-model.md, section 4.5; domain/tools.md, sections 1–5): what a session does to the
//! checkout. It reads, lists, searches, writes and edits files, and runs
//! commands, for the calls a session's LLM makes.
//!
//! Sans-io: [`step`] turns events into requests and changes nothing but the
//! [`Domain`] it is given. Its parent is the session child domain, which opens
//! a kit for each session, hands it the LLM's calls, and routes the file
//! operations the tools ask for out to io and their terminal events back. The
//! tools arm no timers: every operation carries its deadline as data, and io
//! runs the race (programming-model.md, section 5.3).
//!
//! Both directions are typed: a [`Call`] arrives decoded by the protocol layer
//! from the JSON the LLM wrote, its paths split into parts ([`Path`]), and an
//! [`Outcome`] goes back for the protocol layer to render as the text the LLM
//! reads.
//!
//! What happens is also told as content-free [`Fact`]s, kept in a bounded
//! queue the parent drains ([`Domain::pop_fact`]).
//!
//! The checkout is shared, knowledge is not. Files are world state, reached
//! through io and shared by every session; what a session's LLM has read, and
//! at which version, belongs to its kit, along with the [`Authority`] the
//! session was opened with. A write needs its current version
//! read; an edit matches the version it loads, then stores only over that version.

//! It keeps bounded kits, per-kit observed file versions, active calls and
//! pending IO terminals. It never knows model text, provider dialects, run
//! results, authentication, forge state or CI. The parent reserves [`max_out`]
//! output slots before every entrance and returns each started operation's
//! one terminal, including a cancellation terminal when cancellation wins
//! (domain/tools.md, sections 4–6; programming-model.md, sections 5.2
//! and 7).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod authority;
mod boundary;
mod call;
mod command;
mod domain;
mod edit;
mod facts;
mod flight;
mod jobs;
mod kit;
mod knowledge;
mod limits;
mod path;
#[cfg(test)]
mod tests;
mod window;

pub use authority::{Authority, Grants, Repo};
pub use boundary::{Done, Event, Expect, JobAct, JobProblem, Op, Refusal, Request, Version};
pub use call::{
    Budget, BytesClamp, Call, Cut, Depth, Effect, Entry, Exit, Fault, Hit, JobEnd, JobNumber, Kind, Mismatch, Next,
    Occurrences, Outcome, Replacement, SizePart, Timeout, TimeoutClamp, Tool, Wait, Yield, effect, tool,
};
pub use domain::{Domain, max_facts, max_out, step};
pub use facts::{CallInfo, Fact, FactKind, Verdict};
pub use limits::{Derivation, Field, Limits, batch_result_worst_case, derive, result_worst_case, valid};
pub use path::{Name, Namespace, Part, Path, Place, Resolved};
