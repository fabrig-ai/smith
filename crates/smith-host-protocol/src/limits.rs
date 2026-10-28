//! Host-side opening limits (protocol/channel.md, sections 2 and 7).
use skein_channel::SchemaError;

/// Codec, framing and queue allowances fixed before the channel opens.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub bodies: smith_channel::Limits,
    pub charter: smith_charter::v4::Limits,
    pub transcript: smith_transcript::v6::Limits,
    /// Maximum configured endpoint names retained by this component.
    pub endpoints: u32,
    pub channel: skein_channel::Limits,
    /// Aggregate sent Start body bytes, including the whole saved window.
    pub start_bytes: u32,
    /// Maximum live calls awaiting one host terminal.
    pub calls: u32,
}

/// A checked channel cannot be built from the supplied limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// A codec body exceeds the frame range.
    Codec(smith_channel::Overflow),
    /// Skein rejected the schema or framing limits.
    Channel(SchemaError),
    /// A service omitted a workspace path or credential value.
    MissingValue,
    /// A lent value cannot form its grant, or disagrees with the grant's form.
    Grants,
    /// A live call name is repeated, unknown, or beyond capacity.
    Calls,
    /// The service sent a record before or after its permitted channel phase.
    Order,
    /// A host record does not fit the configured channel body limits.
    Body(smith_channel::v5::Problem),
    /// The measured frame could not be written.
    Frame(skein_channel::FrameError),
    /// A measured body could not be written into its allocation.
    Write(skein_lib::Overflow),
    /// An answer's decoded fields do not form a typed host-domain result.
    InvalidAnswer,
    /// A charter or transcript could not be represented by its codec.
    Value,
}

impl From<smith_channel::v5::Problem> for Error {
    fn from(problem: smith_channel::v5::Problem) -> Error {
        Error::Body(problem)
    }
}

impl From<skein_channel::FrameError> for Error {
    fn from(problem: skein_channel::FrameError) -> Error {
        Error::Frame(problem)
    }
}

impl From<skein_lib::Overflow> for Error {
    fn from(problem: skein_lib::Overflow) -> Error {
        Error::Write(problem)
    }
}

/// Slots reserved by the caller at each entry point during opening.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxOut {
    pub to_domain: u32,
    pub below: u32,
}

/// Opening emits at most one owner event and six lower operations per step.
#[must_use]
pub const fn max_out(_limits: &Limits) -> MaxOut {
    MaxOut { to_domain: 1, below: 6 }
}

/// Largest frame sent by the host, including its fixed framing header.
#[must_use]
pub fn output_bytes(limits: &Limits) -> Option<u32> {
    let schema = smith_protocol_channel::schema_for_start(&limits.bodies, limits.start_bytes).ok()?;
    let mut largest = 0;
    for version in &schema.versions {
        for kind in &version.kinds {
            match kind.direction {
                skein_channel::Direction::FromInitiator => largest = largest.max(kind.largest),
                skein_channel::Direction::FromResponder => (),
            }
        }
    }
    largest.checked_add(8)
}
