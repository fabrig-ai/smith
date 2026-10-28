//! Workspace file and process effects for the agent (protocol/agent.md,
//! sections 2 and 3; domain/tools.md, sections 4 and 6).
//!
//! The component keeps admitted root names and in-flight owner records.
//! It never knows file descriptors, filesystem contents before io
//! reports them, credentials, or the domain's read-before-write knowledge.
//! [`Component::from_domain`] sends bounded requests below; `from_below`
//! translates one terminal back. `FileIo` owns file deadlines and their
//! cancellation race; the service routes `Below::File` with `down_until`,
//! `Below::CancelFile` with `cancel`, and seeds randomness before stores.
//!
//! Until Skein's contained process trees exist, searches, commands and checks
//! lead plain process groups. Their writes have only
//! the permissions of the machine process (protocol/agent.md, section 3;
//! protocol plan `later.md`).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod boundary;
mod component;
mod environment;
mod files;
mod guide;
mod limits;
mod listing;
mod process;
mod search;
#[cfg(test)]
mod tests;

pub use boundary::{Below, BelowEvent, FromDomain, Spawn, ToDomain};
pub use component::Component;
pub use environment::{Environment, Unencodable, Variable};
pub use limits::{Derivation, Field, Limits, MaxOut, derive, max_out, valid, worst_case};

pub use listing::Listing;
