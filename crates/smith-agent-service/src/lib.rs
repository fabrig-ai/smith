//! One agent run composed from its domain, host channel, LLM and machine.
//! The service keeps the four machines, bounded routing tables, and the queues
//! between them. It never knows provider wire syntax, charter policy decisions,
//! or the meaning of a host's bearer token. [`iterate`] is its only progression
//! entry point; [`Service::completions`] and [`Service::submissions`] meet a
//! shell or simulated kernel. Contracts: protocol/agent.md, sections 4–6;
//! protocol/README.md, sections 5–6.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod answers;
mod calls;
mod children;
pub mod events;
mod inputs;
mod live;
mod messages;
mod observations;
mod responses;
mod service;

pub mod profile;
pub mod sinks;

#[cfg(test)]
mod tests;

pub use observations::{
    ChildShare, budget_record, check_exit, child_completed, child_share, opening_model, reported_usage,
    request_tool_choice,
};

pub use service::teardown::{Answer, Close, Step, Teardown};

pub use events::EventLimits;

pub use sinks::{Destination, GivenUp, Why};

pub use answers::{AnswerMeta, Answered, Last, accepted, answer_record, last};

pub use service::{
    AdoptError, Config, ConfigError, Effects, Failure, Limits, ProgramBackend, Service, done, effects_valid, failure,
    io_valid, iterate, next_deadline, next_policy_deadline, program_operations, program_valid, valid, work_pending,
};
