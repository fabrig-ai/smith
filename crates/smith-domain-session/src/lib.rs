//! The smith agent's session child domain
//! (programming-model.md, section 4.5): one conversation with an LLM, driven turn by
//! turn, running the tools the LLM asks for, its own or, delegated, those its
//! opener serves. When the LLM stops calling tools the session yields to its
//! opener, which continues it with a new message or closes it; a failure, a
//! limit or its budget (turns, tokens and time, given by the opener) ends it
//! on its own.
//!
//! Sans-io: [`step`] and [`fire`] turn events into requests and change nothing
//! but the [`Domain`] they are given. Time and randomness are inputs; every
//! effect, from calling an LLM to telling the opener the session has ended, is
//! a [`Request`] that its parent, the root domain (`smith-domain`),
//! routes on, and its outcome comes back later through the parent as an
//! [`Event`].
//!
//! The session owns the tools child domain (`smith-domain-tools`), which
//! runs the LLM's calls to its own tools: it opens a kit for each session,
//! hands it the calls within its own step, and passes the file and process
//! operations the tools ask of io out as they are, and their ends back.
//!
//! The conversation is provider-neutral ([`llm`]): the protocol layer speaks
//! each provider's wire format.
//!
//! What happens is also told as bounded [`Fact`]s. The parent reserves
//! [`max_facts`] slots before each entrance and drains [`Domain::pop_fact`]
//! before more work; every native observation is preserved.
//!
//! One concrete opening admits fresh or restored bounded messages. It keeps
//! live call/descriptor tickets, completion/retry state,
//! outstanding tool terminals, injected deadlines and accepted usage/pricing.
//! It never knows the host's charter, outcome meanings, credential secrets,
//! provider wire grammar, forge state or CI. [`resume`] drains bounded deferred
//! tool handoffs; every entrance reserves [`max_out`] slots, then the caller
//! delivers or discards owned records before reclaiming at iteration end
//! (domain/session.md, sections 3–6; programming-model.md, sections
//! 4.5 and 7).
//! Exact own and inclusive activation spend are retained separately. The root's
//! `BudgetDenied` and `UnsentClosed` entrances release only
//! the current provider reservation; this domain never knows global prices or
//! whether another session may start (domain/session.md, section 6;
//! domain/run.md, section 9).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod domain;
mod facts;
mod final_turn;
mod limits;
pub mod llm;
pub mod record;
mod session;
#[cfg(test)]
mod tests;
mod window;

pub use boundary::{
    Budget, BudgetDenial, Deadlines, Dimension, End, Event, FinalTurn, NoSummary, Progress, Request, Spec, Yield,
};
pub use domain::{Domain, fire, max_facts, max_out, max_to_opener, resume, step};
pub use facts::{Choice, Compacted, Fact, FactKind, ResponseInfo, ToolCall, ToolSource};
pub use limits::{
    Derivation, ENCODED_BYTES, Field, Limits, MAX_PARALLEL, completion_reserve, derive, valid, worst_case,
};
pub use session::{preview_completion, preview_reservation, reserve_completion};
