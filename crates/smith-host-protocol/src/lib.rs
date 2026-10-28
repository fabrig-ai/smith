//! Host's agent channel opening, Start and final answer (protocol/channel.md, sections 2, 3 and 5;
//! domain/host.md, section 3).
//!
//! The component keeps Skein's framed channel, its run phase, transcript
//! position and configured endpoint names. It never decides host policy or
//! knows credential values or stream handles. `new`, `open`,
//! `send_start`, `from_below` and `fire` accept stream events and return lower
//! demands, admission and a decoded last word. A service attaches one component per agent.
//!
//! | State | Input | Output |
//! |---|---|---|
//! | Idle | Open | Send Open with empty pipe credential |
//! | Opening | Ready | Check agent receive terms, then Opened or Refuse limits |
//! | Opened | End/Closed | Hangup |
//! | Started | Admitted or refused Answer | Notify the host domain |
//! | Admitted | Answer | Notify the host domain and read to end |

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod answer;
mod component;
mod limits;
mod live;
mod process;
mod program;
mod translate;

#[cfg(test)]
mod tests;

pub use component::{Component, OpenEvent};
pub use limits::{Error, Limits, MaxOut, max_out, output_bytes};
pub use process::{Launch, Process, ProcessEvent};
pub use program::{ProgramSource, SourceAnswer, SourceAsk, program_source_worst_case};
pub use translate::{Values, encode_start, grant_form};
