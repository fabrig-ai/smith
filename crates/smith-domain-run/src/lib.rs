//! The smith agent's run child domain
//! (programming-model.md, section 4.5; domain/run.md, section 2): one agent instance.
//! A run takes its charter from the host, opens the conversation that does
//! the work, accounts what it spends against one budget, and answers once.
//!
//! Sans-io: [`step`] and [`fire`] turn events into requests and change nothing
//! but the [`Domain`] they are given. Time is an input; every effect, from
//! opening a conversation to answering the host, is a [`Request`] that its
//! parent, the root domain (`smith-domain`), routes on, and its
//! outcome comes back later through the parent as an [`Event`].
//!
//! A run has three faces, all through its parent: the host's and io's, which
//! the parent routes to and from the protocol layer, and its conversations',
//! which the parent translates to and from the session child domain's
//! vocabulary. The run names no session type: siblings share none (programming-model.md, section 4.5).
//!
//! What a run is given is policy as data ([`charter`]): the run interprets no
//! workflow vocabulary, and compares the labels in it byte for byte. So is what
//! it may finish with ([`outcome`]), which [`outcome::judge`] checks a declared
//! outcome against (domain/run.md, sections 7 and 8).

//! The retained state is each admitted charter, conversation binding, shared
//! usage, preparation/check/delivery phase, the write lease and its pending terminal rights.
//! The lease names the exclusive call schedule; each writer first settles session jobs
//! (domain/run.md, sections 5.3 and 8.1).
//! It also keeps the bounded labelled-message FIFO, current/offered read fence,
//! settled wait/park state and activation-local turn numbering
//! (domain/run.md, sections 6 and 13).
//! The run never knows authentication, provider dialect bytes,
//! credential secrets, forge state, CI or the host's delivery policy. Entrances
//! reserve [`MAX_OUT`] output slots, and the caller delivers pending terminals
//! even while cancellation is settling (domain/run.md, sections 2, 10;
//! programming-model.md, sections 5.2, 5.3 and 7).

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod agent;
mod boundary;
mod budget;
mod call;
pub mod charter;
mod conventions;
mod delivery;
mod domain;
pub mod facts;
mod host;
mod identity;
mod inbox;
mod land;
mod lease;
mod limits;
mod opening;
pub mod outcome;
mod prepare;
mod prompt;
pub use prompt::budget_prompt_bytes;
mod run;
mod schema_costs;
#[cfg(test)]
mod tests;
mod wind_down;
mod workspace;

pub use boundary::{
    Answer, Ask, AskRefusal, Cap, CompletionEvidence, CompletionFailure, End, Event, Exit, Failure, Fault, FinalTurn,
    Invalid, Limit, Message, MessageRefusal, Opening, Phase, Place, Policy, Ran, Read, Refusal, Report, Request,
    Returned, Stop, TranscriptRefusal, Window,
};
pub use budget::{
    Budget, BudgetRefusal, Ceiling, CompletionPermit, Exhausted, Granted, Overflow, Part, Prices, Rate, ReceivingLimit,
    Set, Share, Spend, Stage, admit, most_costly, reserve_spend,
};
pub use charter::{Brief, Charter, Conventions, Mode, Section};
pub use domain::{
    Domain, MAX_OUT, completion_overflow, completion_permit, conversation_charter, fire, host_name, max_facts, owner,
    reserve, resume, settle_reservation, step,
};
pub use identity::Identity;
pub use inbox::rendered_bytes;
pub use limits::{Derivation, Field, Limits, derive, valid, worst_case};
pub use prompt::{fixed_prompt, fixed_prompt_bytes, role_host_bytes, summary_bytes};
pub use run::Most;
pub use schema_costs::SchemaCosts;
pub use workspace::{Directory, Workspace};

pub use delivery::{
    CallName, Delivered, Delivery, DeliveryFailure, DeliveryReason, DeliveryRefusal, DeliveryStatus, Diagnostic,
    MAX_DIRECTORIES, Marker, Receipt,
};

pub use host::{HostAnswer, HostEffect, HostInput, HostProblem, HostReply, HostTool, RelayName, Unanswered};
