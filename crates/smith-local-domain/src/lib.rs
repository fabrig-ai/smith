//! Local host for one person and one chat (domain/host.md, sections 8–10).
//! It owns its inline or spawned host child, typed chat metadata, queued person lines,
//! credential names and turns awaiting a durable store terminal. It never
//! knows terminal IO, file encoding, provider secrets or provider syntax.
//! Uncertain writes retain their intent and inspect their stable delivery
//! trailer before answering (protocol/hosts.md, section 5.5).
//!
//! `step`, `fire` and `resume` take the injected clock and limits. The caller
//! reserves `max_out` slots and settles each store, credential and agent IO
//! request once. `resume` first asks for the saved chat and later advances a
//! ready child. A run answer is shown only after its turns are saved.
//!
//! ```text
//! state     event or child request       next       emits
//! Loading   Loaded                       Idle       start queued line
//! Loading   intent Loaded                Loading   inspect git heads, save reconciled answer
//! Idle      Line                         Starting   SaveState, Credential
//! Starting  StateSaved + all grants      Running    agent Start
//! Running   Line                         Running    SaveState, then Message
//! Running   agent Turn                   Running    SaveTurn, Show
//! Running   agent Deliver                Running    survey, save intent, commit, save answer
//! Running   uncertain commit             Running    inspect saved delivery trailer
//! Running   unknown inspection           Running    fail unknown, retain intent
//! Loading   unavailable inspection       Done       Exit failed, retain intent
//! Running   TurnSaved                    Running    release held answer
//! Running   Interrupt                    Ending     agent Cancel
//! Running   agent Answer                 Loading    Show after saves, Load
//! Idle      Interrupt or Closed          Done       Exit
//! any       StoreFailed                  Done       Show, Exit after child settles
//! ```

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod agent;
mod boundary;
mod chat;
mod choices;
mod config;
mod credentials;
mod delivery;
mod domain;
mod end;
mod facts;
mod limits;
mod long;
mod person;
mod turns;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod agent_tests;

pub use boundary::{
    AccountFailure, AgentFailure, ChatRefusal, ChatState, Checks, CredentialFailure, DeliveryIntent, DeliveryRecord,
    DeliveryState, Event, ExitStatus, Failure, GitOp, GitResult, IntentDirectory, Notice, Older, Reading, Request,
    StoreFailure, StoreFamily, Undelivered, Warning,
};
pub use choices::Slash;
pub use config::{Config, Contract, Invalid, Mode, PushTarget, charter};
pub use domain::{Domain, fire, max_out, resume, step};
pub use end::EndCause;
pub use facts::Fact;
pub use limits::{AgentLimits, Limits, max_line_bytes, valid};

// The credential event exposes these forms to its callers.
pub use smith_host_domain::{Form, Scheme, SourceName};
