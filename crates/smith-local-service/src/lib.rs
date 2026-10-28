//! One local chat and its spawned or colocated agent (protocol/hosts.md,
//! sections 5–6; protocol/README.md, sections 5–6). The service keeps local
//! policy and the selected lower components, a credential-value
//! table, and internal routing queues. The process adapter owns terminal and
//! delivery descriptors; each lower layer has its own operation tokens.
//! It never knows OAuth provider responses or token-file paths. [`iterate`]
//! runs the up, domain and down passes, including the durable store; the shell
//! settles credential requests in later iterations. Exit waits for lower closes.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod credentials;
mod live;
mod process;
mod service;
pub mod stream;

pub mod profile;

#[cfg(test)]
mod tests;

pub use credentials::{ACCOUNTS_MAX_OUT, Accounts, CredentialRequest, Settled, Source, accounts_valid};
pub use live::{LIVE_BYTES, LIVE_SLOTS, LivePiece, RemoteTurn};
pub use process::{Launch, ProcessLimits, native_io_valid, to_host_exit};
pub use service::{AgentConfig, Config, Error, Limits, Service, StartValues, iterate, valid};
