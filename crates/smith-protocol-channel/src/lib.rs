//! Agent's host channel opening and Start translation (protocol/channel.md,
//! sections 2, 3 and 5; protocol/charter.md, sections 2 and 5).
//!
//! The component keeps Skein's framed channel, the run's channel phase and configured
//! endpoint names. It never knows host policy, credential values, addresses or
//! stream handles. `new`, `from_below`, `fire`, `send_admitted` and `send_answer`
//! return lower demands and a decoded Start with owned domain charter policy.
//!
//! | State | Input | Output |
//! |---|---|---|
//! | Opening | Open | Accept the highest shared version |
//! | Opening | Ready | Check host receive terms, then Opened or Refuse limits |
//! | Opened | Start | Decode its charter and resolve endpoint names, or answer invalid |
//! | Started | Admitted | Send the domain's admission notice once |
//! | Started/Admitted | Answer | Send the final answer and finish writes |
//! | Opened | End/Closed | Ended |

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

mod calls;
mod charter;
mod component;
mod credentials;
mod facts;
mod limits;
mod live;
mod result;
mod transcript;
mod translate;
mod turn;

#[cfg(test)]
mod tests;

pub use charter::encode_charter;
pub use component::{Component, OpenEvent};
pub use credentials::{GrantValue, program_name};
pub use limits::{Error, Limits, MaxOut, max_out, schema, schema_for_start, valid, worst_case};
pub use live::{Piece, PieceKind};
pub use result::decode_declared;
pub use transcript::{decode_transcript, decode_turn, transcript_piece};
pub use translate::{
    DecodedStart, Endpoint, Endpoints, Grant, Mount, answer_record, decode_charter, encode_result, host_path,
    start_context,
};
pub use turn::encode_turn;
