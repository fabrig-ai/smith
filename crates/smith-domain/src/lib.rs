//! The agent root (domain/run.md, sections 2, 3 and 10;
//! programming-model.md, sections 4.4 and 4.5). It owns the run and session
//! children, opaque credential names and validity, conversation bindings,
//! tool tickets, deferred handoffs, uncapped facts and bounded content.
//! Bounded Start contexts retain the original reply rights and optional history;
//! single-use concrete Turn handoffs own records crossing the sibling seam
//! (domain/run.md, section 13).
//!
//! [`step`], [`fire`] and [`resume`] accept typed host, provider and IO events
//! with injected clocks and seed. They emit owned [`Request`] values within
//! [`max_out`]; the caller reserves output room and returns each request's
//! one typed terminal. Iteration-end reclamation follows output delivery.
//! The root translates run conversations into sessions; siblings share no
//! state or vocabulary, and tools remain beneath sessions.
//!
//! The `host_run` fields carry opaque logical run identities supplied by the host.
//! A host process or byte protocol is not required
//! to call this typed domain. The domain never knows credential secrets,
//! authentication, channel bytes, forge state, CI, posting or merge policy.
//! IO owns confined paths and process effects; the host owns delivery.
//! Native facts accumulate without a cap until the owner drains them.
//! Owned content observations are separately drained or discarded by the caller.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod config;
mod domain;
mod facts;

mod feedback;
mod limits;
pub mod llm;
mod operation;
mod peer;
mod route;
#[cfg(test)]
mod tests;
mod translate;
#[cfg(test)]
mod translation_tests;
mod waking;

pub use boundary::{Answered, AnsweredCall, Event, Grant, GrantName, Operation, Request, Window};
pub use config::{Config, ConfiguredModel, Program};
pub use domain::{Domain, fire, max_facts, max_out, resume, step};
pub use facts::{Content, Fact, Opening};

pub use feedback::{Feedback, FeedbackRefusal, feedback, feedback_worst_case};
pub use limits::{Limits, max_turn_bytes, valid};
// The payloads are the children's: a parent may use its children's types.
pub use smith_domain_run as run;
pub use smith_domain_session as session;
pub use smith_domain_tools as tools;

// Concrete history names belong to session; the root parent may name child records.
pub use smith_domain_session::record::{Transcript, Turn};
