//! Bounded turn records; this crate retains no runtime state and knows no
//! session, provider configuration or host policy. Generated constructors,
//! encoders and decoders are the entry points. Contract:
//! `protocol/transcript.md`, sections 2-7.

#![cfg_attr(not(test), no_std)]
#![forbid(unsafe_code)]

extern crate alloc;

#[rustfmt::skip]
#[path = "generated/v6.rs"]
pub mod v6;

pub use v6::{
    Affinity, AffinityParts, AfterFailedWrite, AfterFailedWriteParts, AllowedCalls, AllowedCallsParts, Ambiguous,
    AmbiguousParts, Backend, Block, ByteClamp, ByteClampParts, BytesClamp, CEILINGS, Call, CallName, CallNameParts,
    CallParts, CallProblem, CommandEnd, CommandEndParts, CommandRunning, CommandRunningParts, CutProblem,
    CutProblemParts, DurationClamp, DurationClampParts, EditFailed, EditFailedParts, Edited, EditedParts, Entry,
    EntryParts, Exit, ExitCode, ExitCodeParts, ExitSignal, ExitSignalParts, Failed, FailedParts, Fault, FieldProblem,
    FieldProblemParts, Found, FoundParts, Hit, HitParts, Invalid, InvalidParts, JobEnd, JobExit, JobExitParts, JobRead,
    JobReadParts, JobsFullProblem, JobsFullProblemParts, Kind, LineNumber, LineNumberParts, Listed, ListedParts,
    Message, MessageParts, Mismatch, Next, NotRun, NotRunParts, NotRunWhy, Opaque, OpaqueParts, Opening, OpeningParts,
    OtherPath, OtherPathParts, Outcome, OutsideChoice, OutsideChoiceParts, OutsidePath, OutsidePathParts,
    OversizeProblem, OversizeProblemParts, OwnedOutcome, OwnedOutcomeParts, Program, ProgramParts, Read, ReadCut,
    ReadParts, Replay, ReplayParts, Resolved, ResolvedFailure, ResolvedFailureParts, Returned, Role, Said, SaidParts,
    SizePart, Text, TextParts, TimeoutClamp, TooLarge, TooLargeParts, ToolResult, ToolResultParts, Turn, TurnKind,
    TurnParts, UnknownJobProblem, UnknownJobProblemParts, Usage, UsageParts, WorkingPath, WorkingPathParts, Written,
    WrittenParts,
};

impl Turn {
    /// The wire format version this codec writes and reads.
    pub const FORMAT_VERSION: u64 = 6;
}

/// The owner of smith's patches, as their headers spell it (transcript.md, 12).
pub const OWNER: &str = "smith";

/// The turn's family, as its kept texts spell it.
pub const FAMILY: &str = "smith-transcript";

/// The declaration a kept turn is, where a host's machine starts.
pub const ROOT: &str = "Turn";

/// The mark of a smith patch across which a host closes each live window
/// and starts the next run afresh from its brief (transcript.md, 12).
pub const FRESH_WINDOWS: &str = "fresh-windows";

#[path = "../patches/mod.rs"]
pub mod patches;
