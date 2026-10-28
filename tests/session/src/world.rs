use std::collections::{BTreeMap, BTreeSet, VecDeque};

use skein_fake_checkout::Checkout;
use skein_fake_llm_domain as provider;
use skein_lib::{Duration, ReplyTo, Rng, Time, Token};
use skein_world::domain::{Key, Ledger, Schedule, Span, Stage, Trace};
use smith_domain_session as agent;
use smith_domain_session::llm::{Block, Decoded, Descriptor, Endpoint, Failure, Prompt, Returned, Usage};

use smith_domain_tools::{self as tools, Authority, Done, Effect, Fault, Grants, Op, Repo};
use smith_tools_world::translate as io;

use crate::fixture;
use crate::tickets::{SERVED, Ticketed, Tickets};
use crate::translate;

mod llm;
#[path = "window_referee.rs"]
mod window_referee;

pub use window_referee::{assert_prompt_bound, assert_prompt_boundary, assert_prompt_extends};

/// Room in each domain's output queue beyond what one step may emit. Small, so
/// the loop's flow control (take an event only while there is room for what
/// it may produce) is exercised.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const SLACK: u32 = 3;

/// What the fake opener says when it nudges a session on.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
const NUDGE: &[u8] = b"You have not finished: carry on.";

/// The budget [`spec`] asks for: what the calm limits allow at most.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub const BUDGET: agent::Budget = agent::Budget {
    turns: 16,
    input: 1 << 20,
    output: 1 << 20,
    cache_read: 1 << 20,
    cache_write: 1 << 20,
    time: Duration::from_secs(1800),
};

/// The tools' limits in the calm world: a kit for each session, as many calls
/// a kit as the session runs at once, and room for the fixture's files.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub const TOOLS: tools::Limits = tools::Limits {
    shell_output: 512,
    kits: 4,
    calls: 4,
    repos: 1,
    path_bytes: 256,
    host_path_bytes: 4096,
    known_files: 16,
    file_bytes: 1 << 16,
    tool_payload: 1 << 16,
    read_bytes: 4096,
    list_entries: 64,
    list_bytes: 4096,
    match_lines: 8,
    file_timeout: Duration::from_secs(30),
    jobs: 4,
    job_poll: Duration::from_secs(30),
    job_wait: Duration::from_secs(30),
    shell_timeout: Duration::from_secs(30),
    tool_deadline: Duration::from_secs(120),
    shell_head: 256,
    shell_tail: 256,
    search_hits: 16,
    search_bytes: 1024,
    search_timeout: Duration::from_secs(30),
    facts: 256,
};

/// Checked credit envelope around the original retained-payload pressure.
/// The provider and a result batch own their credits successively, so the larger
/// envelope suffices. All six owned result families and concrete delegated results
/// are included; record arrays and caller/emitted copies remain independently
/// counted by `worst_case` and the shared heap meter.
/// World contract: domain/session.md, sections 3, 5, 6 and 12;
/// domain/tools.md, sections 4, 5 and 9; programming-model.md, section 6.3.
#[must_use]
pub fn reservation_envelope(limits: &agent::Limits) -> Option<u64> {
    let tools = &limits.tools;
    let search = u64::try_from(std::mem::size_of::<tools::Hit>())
        .ok()?
        .checked_mul(u64::from(tools.search_hits))?
        .checked_add(u64::from(tools.search_bytes))?;
    let edit = u64::try_from(std::mem::size_of::<u32>()).ok()?.checked_mul(u64::from(tools.match_lines))?;
    let shell = u64::from(tools.shell_head).checked_add(u64::from(tools.shell_tail))?;
    let largest =
        [u64::from(tools.read_bytes), tools.list_bytes, search, 0, edit, shell, limits.delegated_result_bytes]
            .into_iter()
            .max()?;
    let batch = largest.checked_mul(u64::from(limits.parallel_tools))?;
    Some(agent::completion_reserve(limits)?.max(batch))
}

/// Counts drawn uniformly from `min..=max`.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Count {
    /// Inclusive minimum count required by this contract.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub min: u32,
    /// Inclusive maximum count allowed by this contract.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub max: u32,
}

/// Immutable seeded peer behavior and component limits used to construct this world.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Settings {
    /// Seeds the world, which seeds both domains.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub seed: u64,
    /// Immutable session limits for the component under test.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub agent: agent::Limits,
    /// Model deadlines supplied by the scripted opener in randomized worlds.
    pub progress: agent::Progress,
    /// Neutral fake-provider behavior for this world.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub provider: provider::Config,
    /// One-way latency between the agent and the provider.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub network: Span,
    /// How long io takes over an operation of the tools, a command's
    /// included.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub tool: Span,
    /// The chance, per mille, that io fails an operation of the tools.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub tool_errors: u32,
    /// How many times the opener nudges a session that yields before it
    /// closes it, drawn for each session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub nudges: Count,
    /// How long the opener takes to answer a yield.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub think: Span,
    /// The chance, per mille, that the opener closes a session at a moment of
    /// its own, whatever the session is doing then.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub abandon: u32,
    /// When such a close comes, after the session opens.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub abandon_after: Span,
    /// The chance, per mille, that a cancel loses its race: the call, the
    /// tools' operation or the delegated call it was for ends of itself, and
    /// that is its terminal event. A cancel that wins is told after a network
    /// draw.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub cancels_lost: u32,
    /// The chance, per mille, that the opener sends a close twice, the second
    /// a network draw after the first.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub double_close: u32,
    /// The chance, per mille, that the opener serves its own tools, a finish
    /// and a lookup, to a session it opens.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub serve: u32,
    /// How long the opener takes to answer a call it serves.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub serving: Span,
    /// The grid every delivery is rounded up to, so that some come at the
    /// same instant; zero for none.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub granule: Duration,
}

impl Settings {
    /// A world where nothing goes wrong: no failures, answers well within
    /// every deadline, room for a few sessions, and an opener that closes a
    /// session when it first yields.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub const fn calm(seed: u64) -> Settings {
        Settings {
            progress: agent::Progress { head: Duration::from_secs(30), idle: Duration::from_secs(30) },
            seed,
            agent: agent::Limits {
                threshold: [4, 5],
                token_bytes: skein_llm::TOKEN_BYTES,
                sessions: 4,
                spend: 1,
                protocol_allowance: 0,
                history_blocks: u32::MAX,
                history_messages: 32,
                history_bytes: 1 << 20,
                completion_bytes: 4096,
                answer_bytes: 4096,
                completion_blocks: 16,
                failure_bytes: 512,
                delegated_result_bytes: 16_384,
                budget: BUDGET,

                retries: 3,
                backoff_base: Duration::from_millis(200),
                backoff_max: Duration::from_secs(5),

                facts: 256,
                parallel_tools: 4,
                tools: TOOLS,
            },
            provider: provider::Config {
                calls: 16,
                query_bytes: 1 << 20,
                script_bytes: 1 << 20,
                answer_bytes: 1 << 20,
                latency_min: Duration::from_millis(100),
                latency_max: Duration::from_millis(2000),
                overloaded: 0,
                rate_limited: 0,
                retry_after: Duration::from_secs(1),
                unavailable: 0,
                too_long: 0,
                unauthorized: 0,
                refused: 0,
                no_calls: 0,
                outside_choice: 0,
                answer_tokens: 1,
                calls_per_answer: 1,
                malformed: 0,
                tool_rounds: 2,
                cache_lifetime: Duration::from_secs(300),
                cache_entries: 8,
                unscoped_reads: 0,
            },
            network: Span::millis(1, 20),
            tool: Span::millis(10, 500),
            tool_errors: 0,
            nudges: Count { min: 0, max: 0 },
            think: Span::millis(0, 50),
            abandon: 0,
            abandon_after: Span::millis(0, 10_000),
            cancels_lost: 0,
            double_close: 0,
            serve: 0,
            serving: Span::millis(10, 2_000),
            granule: Duration::ZERO,
        }
    }
}

/// A spec that grants every family of tools, with the calm budget, in one
/// writable repository at `/work`, also the working directory. The world
/// gives each session a repository of its own, seeded with the fixture, and
/// names it in the spec as io names its root.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[must_use]
pub fn spec(prompt: &[u8]) -> agent::Spec {
    let repo = Repo {
        host: Box::new([smith_domain_tools::Name::new(Box::from(b"host".as_slice())).expect("fixture host name")]),
        mount: io::names(b"/work"),
        root: Token::new(0),
        writable: true,
    };
    let grants = Grants { inspect: true, modify: true, shell: true };
    let authority = Authority { cwd: io::names(b"/work"), repos: Box::new([repo]), grants };
    agent::Spec {
        backend: smith_domain_session::record::Backend::Direct,
        account: Some(0),
        effort: None,
        final_turn: None,
        window: 65_536,
        line: 64_512,
        summary: b"Summarise the work so far.".as_slice().into(),
        preamble: b"Earlier window summary:".as_slice().into(),
        affinity: smith_domain_session::llm::Affinity { key: [0; 16], thread: 0 },
        protocol_allowance: 0,
        progress: agent::Progress { head: Duration::from_secs(30), idle: Duration::from_secs(30) },
        endpoint: Endpoint(0),
        model: b"fake-1"[..].into(),
        system: b"You are a coding agent."[..].into(),
        authority,
        delegated: Box::new([]),
        prompt: prompt.into(),
        output: 1024,
        budget: BUDGET,
    }
}

/// What the world counted.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Stats {
    /// Calls the agent made.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub calls: u32,
    /// Calls that reached the provider.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub provider_calls: u32,
    /// Calls that ran out of time.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub timeouts: u32,
    /// Calls the agent cancelled.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub cancels: u32,
    /// Answers that arrived after their call had ended, and were dropped.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub late_answers: u32,
    /// Operations the tools asked of io; those a cancel ended; those that ran
    /// out of time; those io failed.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub ops: u32,
    /// Count of op cancels observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub op_cancels: u32,
    /// Count of op timeouts observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub op_timeouts: u32,
    /// Count of op faults observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub op_faults: u32,
    /// Results of calls to the tools that went back to the LLM, each checked
    /// against its call.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub results: u32,
    /// Tool calls the session answered as not run, as the LLM stopped
    /// before it could use them.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub not_run: u32,
    /// Times a session yielded.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub yields: u32,
    /// Messages the opener sent to yielded sessions.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub continues: u32,
    /// Closes the opener sent.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub closes: u32,
    /// Continues and closes that reached a session after it had ended.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub stale: u32,
    /// The most operations and delegated calls one session had in flight at
    /// once.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub most_parallel: u32,
    /// The most tool runs the sessions held at the end of an iteration,
    /// before its reclaim point.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub most_runs: u32,
    /// Cancels of calls and of operations that lost their race.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub cancels_lost: u32,
    /// Count of op cancels lost observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub op_cancels_lost: u32,
    /// What ended a call or an operation whose cancel lost: an answer, a
    /// failure (a deadline included), the operation's own end.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub answered_after_cancel: u32,
    /// Count of failed after cancel observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub failed_after_cancel: u32,
    /// Count of done after cancel observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub done_after_cancel: u32,
    /// Closes that reached a session already closing.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub closed_while_closing: u32,
    /// Cancels and withdraws for operations and delegated calls that ended in
    /// the iteration they were sent.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub cancels_crossed: u32,
    /// Calls delegated to the opener; withdrawn, or whose answer won the race
    /// with the withdraw; answered as timed out by the opener itself.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegates: u32,
    /// Count of withdraws observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub withdraws: u32,
    /// Count of withdraws lost observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub withdraws_lost: u32,
    /// Count of answered after withdraw observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub answered_after_withdraw: u32,
    /// Count of delegate timeouts observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegate_timeouts: u32,
    /// Finishes the opener refused, and accepted.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub finishes_refused: u32,
    /// Count of finishes accepted observed at this scripted boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub finishes_accepted: u32,
}

/// The facts the sessions told, by kind, as the loop drained them.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Told {
    /// Number of opted-in reasoning drops observed, without retained content.
    pub reasoning_dropped: u32,
    /// Count of opened facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub opened: u32,
    /// Count of completions started facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completions_started: u32,
    /// Count of completions answered facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completions_answered: u32,
    /// Count of completions failed facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completions_failed: u32,
    /// Count of completions cancelled facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completions_cancelled: u32,
    /// Count of completions retried facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub completions_retried: u32,
    /// What the session's tools told: kits opened and closed, calls started
    /// (those that asked io for something) and answered.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub kits_opened: u32,
    /// Count of kits closed facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub kits_closed: u32,
    /// Count of tools started facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub tools_started: u32,
    /// Count of tools answered facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub tools_answered: u32,
    /// Count of yielded facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub yielded: u32,
    /// Count of used facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub used: u32,
    /// Count of ended facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub ended: u32,
    /// Count of delegates started facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegates_started: u32,
    /// Count of delegates answered facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegates_answered: u32,
    /// Count of delegates cancelled facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub delegates_cancelled: u32,
    /// Tool calls the completions made, and those that could not be decoded.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub calls: u32,
    /// Count of invalid calls facts drained from the component.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub invalid_calls: u32,
}

/// A session as its opener saw it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Debug)]
pub struct Session {
    previous_prompt: Option<Prompt>,
    counting: crate::provider::Counting,
    /// Every rendered request, including those the guard refused unsent.
    pub measures: Vec<crate::provider::MeasuredPrompt>,
    line: u64,
    compacting: Option<u32>,
    opening: Option<Box<[agent::llm::Message]>>,
    window: u32,
    ordinary: Option<(Usage, usize)>,
    summary: Option<(u64, u64)>,
    summary_instruction: Box<[u8]>,
    /// The session's name, once it opened.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub session: Option<Token>,
    /// What it was opened with: its budget, and the most tokens an answer
    /// may take.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub budget: agent::Budget,
    /// Output-token cap supplied to each generated completion.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub max_tokens: u32,
    /// Each time it yielded: why, and what the LLM said.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub yields: Vec<(agent::Yield, Box<[u8]>)>,
    /// What its `Used` added up to: completions, and their tokens.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub turns: u32,
    /// Accepted token usage observed for this fixture session or its terminal.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub usage: Usage,
    /// Concrete history emitted in sequence, with historical calls and actual results.
    /// World contract: domain/session.md, sections 3, 5, 6 and 12.
    pub records: Vec<agent::record::Turn>,
    /// Exact accepted completion usage, independently matched against each emitted Turn.
    /// World contract: domain/session.md, sections 3, 6 and 12.
    accepted: Vec<Usage>,
    /// Actual provider payload awaiting the corresponding Used acceptance.
    /// World contract: domain/session.md, sections 3, 5 and 12.
    received: Option<(Box<[Block]>, agent::llm::Stop, bool)>,
    /// Original accepted provider payloads before historical call conversion.
    /// World contract: domain/session.md, sections 3, 5 and 12.
    accepted_content: Vec<Box<[Block]>>,
    /// Actual provider stop determines whether calls executed or remained a yielded tail.
    /// World contract: domain/session.md, sections 3, 5 and 12.
    accepted_stop: Vec<agent::llm::Stop>,
    /// Actual provider closing witnesses, kept independently of tool timeout race bookkeeping.
    /// World contract: domain/session.md, sections 5 and 12.
    provider_closing: ProviderClosing,
    /// Actual late Closing winners settle every call as `NotRun` for any provider stop.
    /// World contract: domain/session.md, sections 5 and 12.
    accepted_late: Vec<bool>,
    /// Actual delegated terminal values, indexed by their immutable origin.
    /// World contract: domain/session.md, sections 3, 5 and 12.
    delegated_results: BTreeMap<(u32, u32), Returned>,
    /// Exact inclusive and own price reports observed before terminal handoff.
    /// World contract: domain/session.md, sections 6 and 12.
    priced: u32,
    /// When its time budget runs out, once it opened.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    expires: Option<Time>,
    /// How it ended, once it has.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub ended: Option<Ended>,
    /// Nudges the opener has left to give.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    nudges: u32,
    /// Whether the opener closes it at a moment of its own.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    abandon: bool,
    /// It yielded, and the opener has not continued it yet.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    waiting: bool,
    /// The opener has closed it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    closed: bool,
    /// Finishes it has called.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    finishes: u32,
}

/// Actual provider lifecycle evidence; ordinary IO/withdraw race bookkeeping
/// cannot turn a yielded provider completion into a late Closing winner.
#[derive(Default, Debug)]
struct ProviderClosing {
    close_seen: bool,
    cancel_seen: bool,
}

/// How a session ended.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Ended {
    /// The single settled session terminal, or none while work remains.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub end: agent::End,
    /// Concrete transcript turns emitted by the session, in order.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub turns: u32,
    /// Accepted token usage observed for this fixture session or its terminal.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub usage: Usage,
}

/// Something on its way, delivered at its time.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
enum Delivery {
    /// The opener opens a session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Open {
        opener: u64,
        spec: agent::Spec,
    },
    /// The opener continues a yielded session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Continue {
        opener: u64,
        content: Box<[u8]>,
    },
    /// The opener closes a session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Close {
        opener: u64,
    },
    /// A call arrives at the provider.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Query {
        call: u64,
        query: provider::api::Query,
    },
    /// The provider's answer arrives back at the agent's side.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Answer {
        call: u64,
        result: Result<provider::api::Answer, provider::api::Error>,
    },
    /// The agent's side gives up on a call.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Deadline {
        call: u64,
        phase: smith_domain_session::llm::Phase,
    },
    /// A cancel that won its race is told.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Cancelled {
        owner: Token,
    },
    AnswerCancelled {
        owner: Token,
    },
    /// The opener's answer to a delegated call arrives.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Answered {
        owner: Token,
        text: Box<[u8]>,
        error: bool,
    },
    /// An operation of the tools ends.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Ran {
        owner: Token,
    },
}

/// A delegated call in flight, as the opener keeps it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
struct Running {
    /// The answer's delivery, withdrawn if the call is withdrawn.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    delivery: Key,
    /// Immutable position used to prove exact concrete delegated result retention.
    /// World contract: domain/session.md, sections 3 and 5.
    origin: agent::record::Origin,
    /// The session that started it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    session: Token,
    effect: Effect,
    /// A finish the opener accepts, after which it closes the session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    accepts: bool,
}

/// An operation of the tools in flight, as io keeps it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
struct Pending {
    /// Its end's delivery, moved up when a cancel wins the race.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    delivery: Key,
    /// The session whose tools asked for it, by the repository it is in.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    session: Token,
    /// A store or a command, which only a call that writes asks for.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    writes: bool,
    work: Work,
}

/// What an operation does when it ends.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
enum Work {
    /// A file operation, run on the checkout then.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    File(Op),
    /// A command started, finished then.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Command(io::Started),
    /// Nothing more: it ends so (it failed to start, timed out, or was
    /// cancelled).
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    Ending(Done),
}

/// A call of the agent in flight, as its protocol layer would keep it.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
struct Call {
    owner: Token,
    /// The deadline's delivery, withdrawn when the call ends first.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    deadline: Key,
    /// The session's opener, and the tools it served in the query.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    opener: u64,
    served: Box<[Descriptor]>,
    input: u64,
}

/// Real domain plus scripted typed peers, pending terminals and external observations for this component story.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
pub struct World {
    now: Time,
    rng: Rng,
    settings: Settings,

    agent: agent::Domain,
    agent_stage: Stage<agent::Limits, agent::Event, agent::Request>,

    provider: provider::Domain,
    provider_stage: Stage<provider::Config, provider::Event, provider::Request>,

    /// Deliveries in flight, whose count names openers and calls too.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    wire: Schedule<Delivery>,
    /// The agent's calls in flight, and the call each session has in flight.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    calls: Ledger<u64, Call>,
    calling: BTreeMap<Token, u64>,
    /// The delegated calls in flight, by their tokens; and the session of
    /// each the agent has started or has yet to hear the end of.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    tools: Ledger<Token, Running>,
    runs: BTreeMap<Token, Token>,
    /// The checkout the sessions' tools work on, a repository for each
    /// session, and the opener of each by io's name for its root.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    checkout: Checkout,
    roots: BTreeMap<u64, u64>,
    /// The tools' operations in flight, by their tokens, and every token an
    /// operation has had (a call's, for each operation it asks for).
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    ops: Ledger<Token, Pending>,
    owners: BTreeSet<Token>,
    /// The tools of the calls each session's tools started, as they told,
    /// until their first operation; and whether the call of each operation's
    /// token writes (an edit loads before it stores).
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    starting: BTreeMap<Token, VecDeque<tools::Tool>>,
    writing: BTreeMap<Token, bool>,
    /// How many messages of each session's transcript have had their results
    /// counted.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    counted: BTreeMap<Token, usize>,
    /// The sessions whose call, and the delegated calls and operations, whose
    /// cancel lost its race; and the sessions the agent is closing, with a
    /// cancel sent and no end yet.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    cancel_lost: BTreeSet<Token>,
    run_cancel_lost: BTreeSet<Token>,
    op_cancel_lost: BTreeSet<Token>,
    closing: BTreeSet<Token>,
    /// The opener's tickets, as the top level would keep them.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    tickets: Tickets,
    /// Calls the provider has not answered yet.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    serving: Ledger<u64, ()>,
    /// The sessions opened, by the opener's name for each, and the opener's
    /// name for each session by the session's own while it lives.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    sessions: BTreeMap<u64, Session>,
    openers: BTreeMap<Token, u64>,

    stats: Stats,
    told: Told,
    fact_holds: u64,
    trace: Trace,
}

impl World {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn new(settings: Settings) -> World {
        Self::with_scripts(settings, Box::new([]))
    }

    /// Construct the same world with explicit application tool turns for focused stories.
    #[must_use]
    pub fn with_scripts(settings: Settings, scripts: Box<[provider::api::Script]>) -> World {
        assert!(agent::worst_case(&settings.agent).is_some(), "the shell refuses limits it cannot provision");
        let mut rng = Rng::new(settings.seed);
        let agent = agent::Domain::new(&settings.agent, rng.next_u64());
        let provider =
            provider::Domain::configured(&settings.provider, rng.next_u64(), scripts, crate::provider::menu())
                .expect("application menu obeys provider admission");
        let max_out = agent::max_out(&settings.agent);
        let mut checkout = Checkout::new();
        fixture::script(&mut checkout);
        World {
            now: Time::ZERO,
            rng,
            settings,
            agent,
            agent_stage: Stage::new(settings.agent, max_out, max_out + SLACK),
            provider,
            provider_stage: Stage::new(settings.provider, provider::MAX_OUT, provider::MAX_OUT + SLACK),
            wire: Schedule::new(),
            calls: Ledger::new("call to the provider"),
            calling: BTreeMap::new(),
            tools: Ledger::new("delegated call"),
            runs: BTreeMap::new(),
            checkout,
            roots: BTreeMap::new(),
            ops: Ledger::new("operation"),
            owners: BTreeSet::new(),
            starting: BTreeMap::new(),
            writing: BTreeMap::new(),
            counted: BTreeMap::new(),
            cancel_lost: BTreeSet::new(),
            run_cancel_lost: BTreeSet::new(),
            op_cancel_lost: BTreeSet::new(),
            closing: BTreeSet::new(),
            tickets: Tickets::default(),
            serving: Ledger::new("call the provider serves"),
            sessions: BTreeMap::new(),
            openers: BTreeMap::new(),
            stats: Stats::default(),
            told: Told::default(),
            fact_holds: 0,
            trace: Trace::default(),
        }
    }

    /// Current injected monotonic time in this world.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn now(&self) -> Time {
        self.now
    }

    /// Observed boundary counters.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Parent passes that left pending work untouched until native facts drained.
    #[must_use]
    pub fn fact_holds(&self) -> u64 {
        self.fact_holds
    }

    /// Every native fact the sessions told, counted by kind.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn told(&self) -> Told {
        self.told
    }

    /// What crossed between the domains and the world, in order, with times.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        self.trace.lines()
    }

    /// Has the opener open a session for `spec` at `at`. Returns the opener's
    /// name for it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn submit(&mut self, at: Time, mut spec: agent::Spec) -> u64 {
        let opener = self.wire.name();
        if self.rng.chance(self.settings.serve) {
            let mut served = Vec::new();
            for (name, effect, schema) in SERVED {
                let ticket = self.tickets.issue(opener, Ticketed::Tool { name, effect, schema });
                served.push(Descriptor { ticket, effect });
            }
            spec.delegated = served.into();
        }
        self.schedule(at, Delivery::Open { opener, spec });
        opener
    }

    /// The session the opener named `opener`, once the open has been sent.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn session(&self, opener: u64) -> &Session {
        self.sessions.get(&opener).expect("the session was submitted and its open sent")
    }

    /// Every session opened so far, by the opener's name for it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn sessions(&self) -> impl Iterator<Item = (u64, &Session)> {
        self.sessions.iter().map(|(opener, session)| (*opener, session))
    }

    /// Runs until nothing is left to happen, then checks the invariants of a
    /// settled world. Panics if it takes more than `iterations`.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    pub fn run(&mut self, iterations: u32) {
        for _ in 0..iterations {
            self.iterate();
            if self.has_work_now() {
                continue;
            }
            let Some(next) = self.next_time() else {
                self.assert_settled();
                return;
            };
            assert!(next > self.now, "time moves forward");
            self.now = next;
        }
        panic!("the world did not settle in {iterations} iterations");
    }

    /// One iteration of the loop, as the shell would run it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn iterate(&mut self) {
        self.agent_stage.tick(self.now);
        self.provider_stage.tick(self.now);
        self.deliver();

        // Each stage resumes what is ready, then takes its events, then fires
        // its alarms, while it has room for what one more may produce.
        while self.agent.facts_room() >= agent::max_facts(&self.agent_stage.env.limits)
            && self.agent_stage.has_room()
            && self.agent.is_ready()
        {
            self.log("agent ready");
            let from = self.agent_stage.out.len();
            agent::resume(&mut self.agent, &self.agent_stage.env, &mut self.agent_stage.out);
            self.check_sent(from);
        }
        while self.agent.facts_room() >= agent::max_facts(&self.agent_stage.env.limits)
            && let Some(event) = self.agent_stage.next_event()
        {
            self.log(&format!("agent <- {}", describe_agent_event(&event)));
            if let agent::Event::Close { session } = &event
                && let Some(opener) = self.openers.get(session)
            {
                self.sessions.get_mut(opener).expect("the addressed live opener").provider_closing.close_seen = true;
            }
            if let agent::Event::Completed { owner, completion } = &event {
                let opener = self.openers[owner];
                let session = self.sessions.get_mut(&opener).expect("a provider terminal names its opener");
                let late = session.provider_closing.close_seen || session.provider_closing.cancel_seen;
                session.received = Some((completion.content.clone(), completion.stop, late));
            }
            let ended = ended_run(&event);
            let from = self.agent_stage.out.len();
            agent::step(&mut self.agent, &self.agent_stage.env, event, &mut self.agent_stage.out);
            self.check_sent(from);
            if let Some(run) = ended {
                self.runs.remove(&run);
            }
        }
        while self.agent.facts_room() >= agent::max_facts(&self.agent_stage.env.limits)
            && self.agent_stage.has_room()
            && self.agent.is_due(self.now)
        {
            self.log("agent alarm");
            let from = self.agent_stage.out.len();
            agent::fire(&mut self.agent, &self.agent_stage.env, &mut self.agent_stage.out);
            self.check_sent(from);
        }
        if self.agent.facts_room() < agent::max_facts(&self.agent_stage.env.limits)
            && (self.agent_stage.has_events() || self.agent.is_due(self.now))
        {
            self.fact_holds += 1;
        }
        // The facts, drained as the shell would write them out.
        while let Some(fact) = self.agent.pop_fact() {
            self.tell(&fact);
        }
        while let Some(event) = self.provider_stage.next_event() {
            provider::step(&mut self.provider, &self.provider_stage.env, event, &mut self.provider_stage.out);
        }
        while self.provider_stage.has_room() && self.provider.is_due(self.now) {
            provider::fire(&mut self.provider, &self.provider_stage.env, &mut self.provider_stage.out);
        }

        // What the steps asked for, submitted at the end of the iteration.
        while let Some(request) = self.agent_stage.out.pop() {
            self.agent_request(request);
        }
        while let Some(request) = self.provider_stage.out.pop() {
            self.provider_request(request);
        }

        // What the iteration held at its most, before the reclaim point
        // frees what ended in it.
        assert!(self.agent.sessions() <= self.settings.agent.sessions, "sessions stay within their slots");
        let runs = self.settings.agent.sessions * self.settings.agent.parallel_tools * 2;
        assert!(self.agent.runs() <= runs, "runs stay within two batches a session");
        assert!(self.agent.kits() <= self.settings.agent.sessions, "a kit at most for each session");
        assert!(self.provider.calls() <= self.settings.provider.calls, "calls stay within their slots");
        self.stats.most_runs = self.stats.most_runs.max(self.agent.runs());
        self.agent.reclaim();
        self.provider.reclaim();
    }

    /// The agent's requests, carried out the way its opener, its protocol
    /// layer and the tools would.
    /// Checks what the entry point that emitted the requests past `from`
    /// sent the opener, which a parent counts on: the records that name it,
    /// and the delegated calls and their withdraws.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn check_sent(&self, from: u32) {
        let mut sent = 0;
        for request in self.agent_stage.out.iter().skip(usize::try_from(from).expect("small")) {
            match request {
                agent::Request::Turn { .. }
                | agent::Request::Priced { .. }
                | agent::Request::JobsCancelled { .. }
                | agent::Request::Opened { .. }
                | agent::Request::Yielded { .. }
                | agent::Request::Used { .. }
                | agent::Request::Ended { .. }
                | agent::Request::Delegate { .. }
                | agent::Request::Withdraw { .. } => sent += 1,
                agent::Request::Complete { .. }
                | agent::Request::Cancel { .. }
                | agent::Request::Yield { .. }
                | agent::Request::Io { .. }
                | agent::Request::Job { .. }
                | agent::Request::CancelIo { .. } => {}
            }
        }
        let most = agent::max_to_opener(&self.agent_stage.env.limits);
        assert!(sent <= most, "an entry point sent its opener {sent} requests, more than {most}");
    }

    fn agent_request(&mut self, request: agent::Request) {
        self.log(&format!("agent -> {}", describe_agent_request(&request)));
        match request {
            agent::Request::Turn { opener, turn } => self.recorded(opener.raw(), turn),
            agent::Request::Priced { opener, spent, own_spent } => {
                let session = self.sessions.get_mut(&opener.raw()).expect("a price names its opener");
                assert!(session.ended.is_none(), "prices precede the terminal");
                assert_eq!(
                    (spent, own_spent),
                    (0, 0),
                    "zero-rate provider and zero-bill fake children have exact zero inclusive and own prices"
                );
                session.priced += 1;
            }
            agent::Request::JobsCancelled { .. } | agent::Request::Yield { .. } => {}
            agent::Request::Opened { opener, session } => self.opened(opener.raw(), session),
            agent::Request::Yielded { opener, stop, text } => self.yielded(opener.raw(), stop, text),
            agent::Request::Used { opener, usage } => {
                self.used(opener.raw(), usage);
            }
            agent::Request::Ended { opener, end, turns, usage, .. } => {
                self.ended(opener.raw(), Ended { end, turns, usage });
            }
            agent::Request::Complete { owner, prompt, deadlines, .. } => {
                self.complete(owner, prompt, deadlines);
            }
            agent::Request::Cancel { owner } => {
                let opener = self.openers[&owner];
                self.sessions
                    .get_mut(&opener)
                    .expect("actual provider cancellation names a live session")
                    .provider_closing
                    .cancel_seen = true;
                self.closing.insert(owner);
                // A call that has already ended has its terminal event on the
                // way: the cancel lost the race and changes nothing. One still
                // in flight may end of itself all the same.
                if let Some(&call) = self.calling.get(&owner) {
                    if self.rng.chance(self.settings.cancels_lost) {
                        self.cancel_lost.insert(owner);
                        self.stats.cancels_lost += 1;
                    } else {
                        self.end_call(call);
                        self.send(Delivery::Cancelled { owner });
                        self.stats.cancels += 1;
                    }
                }
            }
            agent::Request::Io { owner, op, deadline } => self.start_op(owner, op, deadline),
            agent::Request::Job { .. } => unreachable!("the tools issue no job act yet"),
            agent::Request::CancelIo { owner } => self.cancel_op(owner),
            agent::Request::Delegate { owner, opener, call, deadline, origin } => {
                let session = self.sessions.get(&opener.raw()).and_then(|session| session.session);
                let session = session.expect("a session delegates once it has opened");
                assert!(self.runs.insert(owner, session).is_none(), "each delegated call has a token of its own");
                self.serve(owner, opener.raw(), call, deadline, origin);
            }
            agent::Request::Withdraw { owner } => {
                let Some(&session) = self.runs.get(&owner) else {
                    self.stats.cancels_crossed += 1;
                    return;
                };
                self.closing.insert(session);
                if self.tools.contains(owner) && self.rng.chance(self.settings.cancels_lost) {
                    self.run_cancel_lost.insert(owner);
                    self.stats.withdraws_lost += 1;
                } else if let Some(Running { delivery, origin, .. }) = self.tools.take(owner) {
                    let opener = self.openers[&session];
                    self.sessions
                        .get_mut(&opener)
                        .expect("delegation belongs to its opener")
                        .delegated_results
                        .insert((origin.sequence, origin.position), Returned::Withdrawn);
                    self.wire.withdraw(delivery).expect("a served call in flight has its answer on the way");
                    self.send(Delivery::AnswerCancelled { owner });
                    self.stats.withdraws += 1;
                }
            }
        }
    }

    /// The opener learns its session's name, and plans a close of its own if
    /// it is to abandon the session.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn opened(&mut self, opener: u64, name: Token) {
        let session = self.sessions.get_mut(&opener).expect("a session opens for an open that was sent");
        assert!(session.session.is_none() && session.ended.is_none(), "a session opens once, before it ends");
        session.session = Some(name);
        session.expires = Some(self.now.saturating_add(session.budget.time));
        self.openers.insert(name, opener);
        if session.abandon {
            let at = self.now.saturating_add(self.draw(self.settings.abandon_after));
            self.schedule(at, Delivery::Close { opener });
        }
    }

    /// The opener answers a yield: it nudges the session on while it has
    /// nudges left, and closes it after.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn yielded(&mut self, opener: u64, stop: agent::Yield, text: Box<[u8]>) {
        let session = self.sessions.get_mut(&opener).expect("a session yields to its opener");
        let name = session.session.expect("a session yields once it has opened");
        assert!(session.ended.is_none(), "a session yields before it ends");
        assert!(!session.waiting, "a session yields once for each message");
        session.yields.push((stop, text));
        self.stats.yields += 1;
        assert!(self.idle(name), "a session yields with nothing in flight");
        let session = self.sessions.get_mut(&opener).expect("looked up above");
        if session.closed {
            // A close of the opener's own crossed the yield.
            return;
        }
        let at = self.now.saturating_add(self.draw(self.settings.think));
        let session = self.sessions.get_mut(&opener).expect("looked up above");
        if session.nudges > 0 {
            session.nudges -= 1;
            session.waiting = true;
            self.schedule(at, Delivery::Continue { opener, content: NUDGE.into() });
        } else {
            self.schedule(at, Delivery::Close { opener });
        }
    }

    /// A completion came back: the opener adds up what it used.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn used(&mut self, opener: u64, usage: Usage) {
        let session = self.sessions.get_mut(&opener).expect("a session reports to its opener");
        assert!(session.session.is_some() && session.ended.is_none(), "a session uses tokens while it lives");
        session.turns += 1;
        session.usage = add_usage(session.usage, usage);
        session.accepted.push(usage);
        let (content, stop, late) = session.received.take().expect("usage accepts an actual provider payload");
        session.accepted_content.push(content);
        session.accepted_stop.push(stop);
        session.accepted_late.push(late);
        if session.compacting.is_none() {
            session.ordinary =
                Some((usage, session.previous_prompt.as_ref().expect("accepted request").messages.len() + 1));
            session.summary = None;
        }
    }

    /// Check every concrete handoff against accepted usage and its historical representation.
    /// World contract: domain/session.md, sections 3, 5, 6 and 12.
    fn recorded(&mut self, opener: u64, turn: agent::record::Turn) {
        let session = self.sessions.get_mut(&opener).expect("a Turn names its opener");
        assert!(session.ended.is_none(), "concrete history precedes terminal handoff");
        assert_eq!((turn.version, turn.endpoint, turn.dialect), (agent::record::VERSION, Some(Endpoint(0)), 2));
        let index = session.records.len();
        assert_eq!(turn.sequence, u32::try_from(index + 1).expect("bounded sequence"));
        assert_eq!(turn.usage, session.accepted[index], "each Turn records its actual accepted provider usage");
        assert_eq!(turn.spent, 0, "zero-price exact bill");
        if matches!(turn.kind, agent::record::Kind::Window(_)) {
            window_referee::recorded_window(session, &turn, index);
            session.records.push(turn);
            return;
        }
        let assistant_index = turn
            .messages
            .iter()
            .position(|message| message.role == agent::llm::Role::Assistant)
            .expect("every settled Turn contains the actual assistant payload");
        let assistant = &turn.messages[assistant_index];
        assert_resumed_tail(session, index, &turn.messages[..assistant_index]);
        let original = &session.accepted_content[index];
        assert_eq!(assistant.content.len(), original.len(), "every provider block retains its position");
        for (position, (block, original)) in assistant.content.iter().zip(original.iter()).enumerate() {
            match original {
                Block::ToolCall { id, name, input, replay, call } => {
                    assert_eq!(
                        *block,
                        Block::ToolCall {
                            id: id.clone(),
                            name: name.clone(),
                            input: input.clone(),
                            replay: replay.clone(),
                            call: Decoded::Historical,
                        },
                        "concrete calls preserve exact provider bytes and shed only live authority"
                    );
                    let result = recorded_result(&turn.messages[assistant_index + 1..], id);
                    if session.accepted_stop[index] != agent::llm::Stop::ToolUse {
                        if session.accepted_late[index] {
                            assert_eq!(
                                result,
                                Some(&Returned::NotRun { why: smith_domain_session::llm::NotRun::Stopped }),
                                "actual late Closing winners settle calls without effects"
                            );
                        } else {
                            assert!(result.is_none(), "a yielded call retains its unanswered tail until a Continue");
                        }
                        continue;
                    }
                    let result = result.expect("every callable ToolUse block has its actual settled result");
                    match call {
                        Decoded::Delegated { .. } => {
                            let position = u32::try_from(position).expect("bounded provider position");
                            if let Some(expected) = session.delegated_results.get(&(turn.sequence, position)) {
                                assert_eq!(
                                    result, expected,
                                    "exact delegated text/error or withdrawal is retained once"
                                );
                            } else {
                                unstarted_delegate(result, position);
                            }
                        }
                        Decoded::Owned { call } => match result {
                            Returned::Owned { outcome } => {
                                assert!(fixture::fits(call, outcome), "actual result belongs to its call");
                            }
                            Returned::NotRun { .. } => {}
                            Returned::Text { .. } | Returned::Invalid { .. } | Returned::Withdrawn => {
                                panic!("an owned call retains its actual owned result or never ran")
                            }
                        },
                        Decoded::Invalid { problem, .. } => match result {
                            Returned::Invalid { problem: actual } => assert_eq!(actual, problem),
                            Returned::NotRun { .. } => {}
                            Returned::Text { .. } | Returned::Owned { .. } | Returned::Withdrawn => {
                                panic!("an invalid call retains its exact problem or never ran")
                            }
                        },
                        Decoded::Builtin { .. } | Decoded::Historical => {
                            panic!("the provider must classify every live call")
                        }
                    }
                }
                Block::Opaque { .. } | Block::Text { .. } | Block::Refusal { .. } | Block::ToolResult { .. } => {
                    assert_eq!(block, original, "concrete payloads preserve actual provider bytes");
                }
            }
        }
        session.records.push(turn);
    }

    /// The session that owns `owner` starts a completion: it must have turns,
    /// input and output tokens and time left, no tokens past their budget,
    /// and an answer that may take no more than the output budget left.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn affordable(&self, owner: Token, prompt: &Prompt) {
        let opener = self.openers.get(&owner).expect("a session calls the LLM while it lives");
        let session = &self.sessions[opener];
        let (budget, usage) = (&session.budget, &session.usage);
        assert!(session.turns < budget.turns, "session {opener} starts no turn past its budget");
        assert!(usage.input_tokens.unwrap_or(0) < budget.input, "session {opener} has input tokens left");
        assert!(usage.output_tokens.unwrap_or(0) < budget.output, "session {opener} has output tokens left");
        assert!(
            usage.cache_read_tokens.unwrap_or(0) <= budget.cache_read,
            "session {opener} is within its cache reads"
        );
        assert!(
            usage.cache_write_tokens.unwrap_or(0) <= budget.cache_write,
            "session {opener} is within its cache writes"
        );
        assert!(Some(self.now) < session.expires, "session {opener} has time left");
        let messages = u32::try_from(prompt.messages.len()).expect("a small transcript");
        assert!(
            match prompt.purpose {
                agent::llm::Purpose::Ordinary { .. } => messages < self.settings.agent.history_messages,
                agent::llm::Purpose::Compaction { .. } => messages <= self.settings.agent.history_messages,
            },
            "session {opener}'s transcript has room for the answer"
        );
        let left = budget.output - usage.output_tokens.unwrap_or(0);
        let most = u32::try_from(left).unwrap_or(u32::MAX).min(session.max_tokens);
        assert_eq!(prompt.max_tokens, most, "session {opener}'s answer takes no more than the output budget left");
    }

    fn ended(&mut self, opener: u64, ended: Ended) {
        let session = self.sessions.get(&opener).expect("a session ends for an open that was sent");
        assert!(session.ended.is_none(), "a session ends once");
        match ended.end {
            agent::End::TranscriptRefused { .. } | agent::End::PriceOverflow | agent::End::UsageOverflow => {
                unreachable!("bounded scheduled peers neither restore nor overflow; zero prices cannot spend the unit")
            }
            agent::End::Busy | agent::End::Invalid => {
                assert!(session.session.is_none(), "a session refused at the entrance never opened");
            }
            agent::End::Closed
            | agent::End::NoSummary { .. }
            | agent::End::Failed { .. }
            | agent::End::Budget { .. } => {
                let name = session.session.expect("a session that ran had opened");
                assert!(self.idle(name), "a session ends once nothing it asked for is in flight");
            }
        }
        if ended.end == agent::End::Closed {
            assert!(session.closed, "a session ends as closed only when its opener closed it");
        }
        if let agent::End::Budget { spent } = ended.end {
            assert!(self.spent(session, spent), "session {opener} ended when its {spent:?} budget was spent");
        }
        assert_eq!((ended.turns, ended.usage), (session.turns, session.usage), "an end adds up what was used");
        if let Some(name) = session.session {
            self.openers.remove(&name);
            self.closing.remove(&name);
        }
        self.tickets.free(opener);
        self.sessions.get_mut(&opener).expect("looked up above").ended = Some(ended);
    }

    /// Whether `session` has run out of its budget in `dimension` now: used it
    /// up, for what gates a completion; gone past it, for the cache.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn spent(&self, session: &Session, dimension: agent::Dimension) -> bool {
        let (budget, usage) = (&session.budget, &session.usage);
        match dimension {
            agent::Dimension::Unit => {
                unreachable!("bounded scheduled peers neither restore nor overflow; zero prices cannot spend the unit")
            }
            agent::Dimension::Turns => session.turns >= budget.turns,
            agent::Dimension::Input => usage.input_tokens.unwrap_or(0) >= budget.input,
            agent::Dimension::Output => usage.output_tokens.unwrap_or(0) >= budget.output,
            agent::Dimension::CacheRead => usage.cache_read_tokens.unwrap_or(0) > budget.cache_read,
            agent::Dimension::CacheWrite => usage.cache_write_tokens.unwrap_or(0) > budget.cache_write,
            agent::Dimension::Time => Some(self.now) >= session.expires,
        }
    }

    /// The opener serves the delegated call `call` for the session of `opener`,
    /// by `deadline`: a finish it refuses the first time, as its tests fail,
    /// and accepts after; a lookup, which is slow. It runs the race with the
    /// deadline itself.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn serve(&mut self, owner: Token, opener: u64, call: Token, deadline: Time, origin: agent::record::Origin) {
        let session = *self.runs.get(&owner).expect("a run is worked out from the step that started it");
        assert_eq!(
            self.sessions[&opener].accepted_stop.last(),
            Some(&agent::llm::Stop::ToolUse),
            "only actual callable ToolUse completions delegate effects"
        );
        let Ticketed::Call { tool, arguments: _ } = self.tickets.resolve(call).clone() else {
            panic!("a delegated call's ticket names a call");
        };
        let effect = SERVED.iter().find(|(name, ..)| *name == tool).expect("a call to a served tool").1;
        self.batched(session, effect == Effect::Write);
        let state = self.sessions.get_mut(&opener).expect("the opener serves the sessions it opened");
        let (mut text, mut error, mut accepts): (&[u8], bool, bool) = match tool {
            b"finish" => {
                state.finishes += 1;
                if state.finishes == 1 {
                    (b"not finished: the tests fail", true, false)
                } else {
                    (b"accepted", false, true)
                }
            }
            _ => (b"the forge says: fine", false, false),
        };
        let mut at = self.now.saturating_add(self.draw(self.settings.serving));
        if at > deadline {
            (text, error, accepts, at) = (b"timed out", true, false, deadline);
            self.stats.delegate_timeouts += 1;
        } else if tool == b"finish" {
            if accepts {
                self.stats.finishes_accepted += 1;
            } else {
                self.stats.finishes_refused += 1;
            }
        }
        let delivery = self.schedule(at, Delivery::Answered { owner, text: text.into(), error });
        let running = Running { delivery, origin, session, effect, accepts };
        self.tools.open(owner, running);
        self.stats.delegates += 1;
    }

    /// io starts the tools' operation `op` for `owner`, to end by
    /// `deadline`: after a draw, or at the deadline if that comes first.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn start_op(&mut self, owner: Token, op: Op, deadline: Time) {
        // A call's operations, one after another, carry its token.
        self.owners.insert(owner);
        let at = match &op {
            Op::Remove { at, .. }
            | Op::Load { at, .. }
            | Op::List { at, .. }
            | Op::Store { at, .. }
            | Op::Search { at, .. } => at.root,
            Op::Spawn { cwd, .. } => cwd.root,
        };
        let opener = *self.roots.get(&at.raw()).expect("an operation is in a session's repository");
        let session = self.sessions.get(&opener).and_then(|session| session.session);
        let session = session.expect("a session's tools ask io for something once it has opened");
        assert_eq!(
            self.sessions[&opener].accepted_stop.last(),
            Some(&agent::llm::Stop::ToolUse),
            "yielded and late Closing call tails never start owned IO effects"
        );
        let writes = self.writes(owner, session, &op);
        self.batched(session, writes);
        let mut ends = self.now.saturating_add(self.draw(self.settings.tool));
        let mut work = if self.rng.chance(self.settings.tool_errors) {
            self.stats.op_faults += 1;
            Work::Ending(Done::Failed { fault: Fault::Other })
        } else {
            match op {
                Op::Spawn { cwd, command, head, tail, after: _ } => {
                    match io::spawn(&self.checkout, &cwd, &command, &[], (head, tail)) {
                        Ok(started) => Work::Command(started),
                        Err(done) => Work::Ending(done),
                    }
                }
                op @ (Op::Load { .. } | Op::List { .. } | Op::Remove { .. } | Op::Store { .. } | Op::Search { .. }) => {
                    Work::File(op)
                }
            }
        };
        // io runs the race with the deadline, and says it lost a moment
        // after the deadline passes: a command killed then tells what it wrote
        // by then (nothing, here).
        if ends > deadline {
            let done = match work {
                Work::Command(started) => io::exited(None, &[], started.head, started.tail),
                Work::File(_) | Work::Ending(_) => Done::TimedOut,
            };
            let noticed = deadline.saturating_add(self.draw(self.settings.network));
            (work, ends) = (Work::Ending(done), noticed);
            self.stats.op_timeouts += 1;
        }
        let delivery = self.schedule(ends, Delivery::Ran { owner });
        self.ops.open(owner, Pending { delivery, session, writes, work });
        self.stats.ops += 1;
    }

    /// io is asked to cancel the operation of `owner`: it ends cancelled
    /// after a network draw, unless it ends of itself first.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn cancel_op(&mut self, owner: Token) {
        assert!(self.owners.contains(&owner), "a cancel names an operation io was asked for");
        let Some(pending) = self.ops.get(owner) else {
            // It ended in the iteration the cancel was sent.
            self.stats.cancels_crossed += 1;
            return;
        };
        self.closing.insert(pending.session);
        if self.rng.chance(self.settings.cancels_lost) {
            self.op_cancel_lost.insert(owner);
            self.stats.op_cancels_lost += 1;
            return;
        }
        let at = self.now.saturating_add(self.draw(self.settings.network));
        let delivery = self.schedule(at, Delivery::Ran { owner });
        let pending = self.ops.get_mut(owner).expect("looked up above");
        self.wire.withdraw(pending.delivery).expect("an operation in flight has its end on the way");
        (pending.delivery, pending.work) = (delivery, Work::Ending(Done::Cancelled));
        self.stats.op_cancels += 1;
    }

    /// The operation of `owner` ends, and io tells the tools how.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn ran(&mut self, owner: Token) {
        let pending = self.ops.end(owner);
        let done = match pending.work {
            Work::File(op) => io::perform(&mut self.checkout, op),
            Work::Command(started) => {
                io::finish(&mut self.checkout, &started.process);
                let program = &started.process.program;
                io::exited(Some(program.exit), &program.output, started.head, started.tail)
            }
            Work::Ending(done) => done,
        };
        if self.op_cancel_lost.remove(&owner) {
            self.stats.done_after_cancel += 1;
        }
        self.agent_stage.push(agent::Event::Done { owner, done });
    }

    /// Checks every result of the session's own tools in `prompt` against
    /// the call it answers, and counts those of its last message once.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn check_results(&mut self, owner: Token, prompt: &Prompt) {
        for pair in prompt.messages.windows(2) {
            let calls = pair[0].content.iter().filter_map(|block| {
                let Block::ToolCall { call: Decoded::Owned { call }, id, .. } = block else { return None };
                Some((id, call))
            });
            for (id, call) in calls {
                let result = pair[1].content.iter().find_map(|block| {
                    let Block::ToolResult { id: answered, result: Returned::Owned { outcome } } = block else {
                        return None;
                    };
                    (answered == id).then_some(outcome)
                });
                if let Some(outcome) = result {
                    assert!(fixture::fits(call, outcome), "{outcome:?} is not what comes of {call:?}");
                }
            }
        }
        let counted = self.counted.entry(owner).or_default();
        if prompt.messages.len() > *counted {
            *counted = prompt.messages.len();
            let last = prompt.messages.last().expect("a prompt has a message");
            let answers = last
                .content
                .iter()
                .filter(|block| matches!(block, Block::ToolResult { result: Returned::Owned { .. }, .. }));
            self.stats.results += u32::try_from(answers.count()).expect("a small message");
        }
    }

    /// Whether the session `name` has nothing in flight.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn idle(&self, name: Token) -> bool {
        !self.calling.contains_key(&name)
            && self.tools.values().all(|run| run.session != name)
            && self.ops.values().all(|op| op.session != name)
    }

    /// Whether the call `owner`'s operation `op` is for, of `session`'s tools,
    /// writes: as the tools told when it started, matched with its first
    /// operation, so that an edit writes as it loads. Once facts have been
    /// lost, the start may not have been told: the operation's own effect.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn writes(&mut self, owner: Token, session: Token, op: &Op) -> bool {
        let stores = matches!(op, Op::Remove { .. } | Op::Store { .. } | Op::Spawn { .. });
        if let Some(&writes) = self.writing.get(&owner) {
            assert!(writes || !stores, "only a call that writes stores or runs a command");
            return writes;
        }
        let told = self.starting.get_mut(&session).and_then(VecDeque::pop_front);
        let writes = match told {
            Some(tool) => match tool {
                tools::Tool::Remove | tools::Tool::Write | tools::Tool::Edit | tools::Tool::Shell => true,
                tools::Tool::Read | tools::Tool::List | tools::Tool::Search | tools::Tool::Job => false,
            },
            None => panic!("a call's first operation follows its native start observation"),
        };
        assert!(writes || !stores, "only a call that writes stores or runs a command");
        self.writing.insert(owner, writes);
        writes
    }

    /// Something starts for `session`, an operation of its tools or a
    /// delegated call, which `writes` or only reads: a write runs alone, and
    /// reads run together, as many as the limits allow. (The tools ask io for
    /// one operation at a time for each call, and only a call that writes
    /// stores or runs a command.)
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn batched(&mut self, session: Token, writes: bool) {
        let runs = self.tools.values().filter(|run| run.session == session).map(|run| run.effect == Effect::Write);
        let ops = self.ops.values().filter(|op| op.session == session).map(|op| op.writes);
        let others: Vec<bool> = runs.chain(ops).collect();
        assert!(!others.contains(&true), "nothing runs beside a write");
        if writes {
            assert!(others.is_empty(), "a write runs alone");
        }
        let running = u32::try_from(others.len()).expect("a small batch") + 1;
        assert!(running <= self.settings.agent.parallel_tools, "no more runs at once than the limits allow");
        self.stats.most_parallel = self.stats.most_parallel.max(running);
    }

    /// The provider's requests, carried back the way its protocol layer would.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn provider_request(&mut self, request: provider::Request) {
        match request {
            provider::Request::Reply { to, result } => {
                let call = to.into_token().raw();
                self.serving.end(call);
                self.send(Delivery::Answer { call, result: result.map_err(|failure| failure.error) });
            }
            other @ (provider::Request::Piece { .. } | provider::Request::Hold { .. }) => {
                panic!("typed provider expected a terminal reply, got {other:?}")
            }
        }
    }

    /// Hands every delivery that is due to its destination.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    #[expect(clippy::too_many_lines, reason = "one exhaustive delivery table preserves actual terminal ownership")]
    fn deliver(&mut self) {
        while let Some(delivery) = self.wire.next(self.now) {
            match delivery {
                Delivery::Open { opener, spec } => self.open(opener, spec),
                Delivery::Continue { opener, content } => {
                    let session = self.sessions.get_mut(&opener).expect("the opener continues what it opened");
                    assert!(session.waiting, "the opener continues a session only while it is yielded");
                    session.waiting = false;
                    if session.closed {
                        // It closed the session while it was making up its mind.
                        continue;
                    }
                    let name = session.session.expect("a yielded session has opened");
                    if session.ended.is_some() {
                        self.stats.stale += 1;
                    }
                    self.agent_stage.push(agent::Event::Continue { session: name, content });
                    self.stats.continues += 1;
                }
                Delivery::Close { opener } => {
                    let session = self.sessions.get_mut(&opener).expect("the opener closes what it opened");
                    let again = !session.closed && self.rng.chance(self.settings.double_close);
                    let session = self.sessions.get_mut(&opener).expect("looked up above");
                    session.closed = true;
                    let name = session.session.expect("the opener closes a session once it has opened");
                    if session.ended.is_some() {
                        self.stats.stale += 1;
                    } else if self.closing.contains(&name) {
                        self.stats.closed_while_closing += 1;
                    }
                    self.agent_stage.push(agent::Event::Close { session: name });
                    self.stats.closes += 1;
                    if again {
                        self.send(Delivery::Close { opener });
                    }
                }
                Delivery::Query { call, query } => {
                    self.serving.open(call, ());
                    self.provider_stage.push(provider::Event::Call {
                        reply_to: ReplyTo::new(Token::new(call)),
                        call: Token::new(call),
                        query,
                    });
                    self.stats.provider_calls += 1;
                }
                Delivery::Answer { call, result } => {
                    // The fake refuses a transcript a provider would:
                    // a call without its result, a result without its call.
                    assert!(result != Err(provider::api::Error::InvalidRequest), "the agent sends well-formed queries");
                    if let Some(Call { owner, opener, served, input, .. }) = self.end_call(call) {
                        if self.cancel_lost.remove(&owner) {
                            match result {
                                Ok(_) => self.stats.answered_after_cancel += 1,
                                Err(_) => self.stats.failed_after_cancel += 1,
                            }
                        }
                        let mut result = result;
                        if let Ok(answer) = &mut result {
                            crate::provider::report(&mut answer.usage, input);
                        }
                        let event = translate::outcome(owner, result, &mut self.tickets, opener, &served);
                        self.agent_stage.push(event);
                    } else {
                        self.stats.late_answers += 1;
                    }
                }
                Delivery::Deadline { call, phase } => {
                    let Call { owner, .. } =
                        self.end_call(call).expect("a deadline is withdrawn when its call ends first");
                    if self.cancel_lost.remove(&owner) {
                        self.stats.failed_after_cancel += 1;
                    }
                    self.agent_stage.push(agent::Event::Failed {
                        retry_ok: true,
                        owner,
                        failure: Failure::TimedOut { phase },
                        evidence: smith_domain_session::llm::Evidence::Unknown,
                        detail: Box::default(),
                    });
                    self.stats.timeouts += 1;
                }
                Delivery::Cancelled { owner } => self.agent_stage.push(agent::Event::Cancelled { owner }),
                Delivery::AnswerCancelled { owner } => {
                    self.agent_stage.push(agent::Event::AnswerCancelled { owner, spent: 0 });
                }
                Delivery::Answered { owner, text, error } => {
                    // A withdrawn call's answer is withdrawn.
                    let run = self.tools.end(owner);
                    let opener = self.openers[&run.session];
                    self.sessions.get_mut(&opener).expect("delegation belongs to its opener").delegated_results.insert(
                        (run.origin.sequence, run.origin.position),
                        Returned::Text { text: text.clone(), error, replay: None },
                    );
                    if self.run_cancel_lost.remove(&owner) {
                        self.stats.answered_after_withdraw += 1;
                    }
                    self.agent_stage.push(agent::Event::Answered { owner, text, error, spent: 0 });
                    // The opener has its finish, and closes the session.
                    if run.accepts {
                        let opener = *self.openers.get(&run.session).expect("a session lives while its calls run");
                        self.sessions.get_mut(&opener).expect("the opener's session").nudges = 0;
                        self.send(Delivery::Close { opener });
                    }
                }
                Delivery::Ran { owner } => self.ran(owner),
            }
        }
    }

    /// The opener opens a session, deciding how it will treat it.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn open(&mut self, opener: u64, spec: agent::Spec) {
        let Count { min, max } = self.settings.nudges;
        let nudges = u32::try_from(self.rng.between(min.into(), max.into())).expect("drawn between two u32s");
        let abandon = self.rng.chance(self.settings.abandon);
        let session = Session {
            previous_prompt: None,
            counting: crate::provider::Counting::new(self.settings.seed ^ opener),
            measures: Vec::new(),
            line: spec.line,
            compacting: None,
            opening: None,
            window: spec.window,
            ordinary: None,
            summary: None,
            summary_instruction: spec.summary.clone(),
            session: None,
            budget: spec.budget,
            max_tokens: spec.output,
            yields: Vec::new(),
            turns: 0,
            usage: Usage::ZERO,
            accepted: Vec::new(),
            received: None,
            accepted_content: Vec::new(),
            accepted_stop: Vec::new(),
            provider_closing: ProviderClosing::default(),
            accepted_late: Vec::new(),
            delegated_results: BTreeMap::new(),
            records: Vec::new(),
            priced: 0,
            expires: None,
            ended: None,
            nudges,
            abandon,
            waiting: false,
            closed: false,
            finishes: 0,
        };
        assert!(self.sessions.insert(opener, session).is_none(), "openers have distinct names");
        // A repository of its own, which io names as its root.
        let root = fixture::seed(&mut self.checkout, format!("s{opener}").as_bytes());
        self.roots.insert(root, opener);
        let mut spec = spec;
        for repo in &mut spec.authority.repos {
            repo.root = io::token(root);
        }
        self.agent_stage.push(agent::Event::Open {
            opener: Token::new(opener),
            opening: Box::new(agent::record::Opening {
                window_llm: None,
                spec,
                dialect: 2,
                prices: agent::record::Prices { input: 0, cached: 0, cache_write: None, output: 0, unit: 1 },
                budget: 1,
                transcript: None,
                answered: Box::default(),
            }),
        });
    }

    /// Ends the agent's call `call` if it is still in flight, withdrawing its
    /// deadline, and returns its owner.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn end_call(&mut self, call: u64) -> Option<Call> {
        let ended = self.calls.take(call)?;
        self.calling.remove(&ended.owner);
        self.wire.withdraw(ended.deadline);
        Some(ended)
    }

    fn has_work_now(&self) -> bool {
        self.agent_stage.has_events()
            || self.provider_stage.has_events()
            || self.agent.is_ready()
            || self.agent.is_due(self.now)
            || self.provider.is_due(self.now)
            || self.wire.is_due(self.now)
    }

    fn next_time(&self) -> Option<Time> {
        [self.wire.next_time(), self.agent.next_deadline(), self.provider.next_deadline()].into_iter().flatten().min()
    }

    fn tell(&mut self, fact: &agent::Fact) {
        let fact = fact.kind.clone();
        if let agent::FactKind::Tools { opener, fact } = fact {
            let (tools::FactKind::Opened { session }
            | tools::FactKind::Refused { session, .. }
            | tools::FactKind::Started { session, .. }
            | tools::FactKind::Answered { session, .. }
            | tools::FactKind::JobEnded { session, .. }
            | tools::FactKind::Closing { session, .. }
            | tools::FactKind::Closed { session }) = fact;
            assert!(self.sessions.contains_key(&opener.raw()), "a kit's fact names an opener that opened");
            if let Some(&known) = self.openers.get(&session) {
                assert_eq!(known, opener.raw(), "a kit's fact names the opener of its session");
            }
            if let tools::FactKind::Started { session, tool } = fact {
                self.starting.entry(session).or_default().push_back(tool);
            }
        }
        let told = &mut self.told;
        if let agent::FactKind::CompletionAnswered { calls, invalid, .. } = fact {
            told.calls += calls;
            told.invalid_calls += invalid;
        }
        let count = match fact {
            agent::FactKind::CompactionStarted { opener, window, .. } => {
                self.sessions.get_mut(&opener.raw()).expect("known opener").compacting = Some(window);
                return;
            }
            agent::FactKind::BuiltinStarted { .. }
            | agent::FactKind::BuiltinAnswered { .. }
            | agent::FactKind::CompactionEnded { .. } => return,
            agent::FactKind::Opened { .. } => &mut told.opened,
            agent::FactKind::JobEnded { .. } | agent::FactKind::ReasoningDropped { .. } => &mut told.reasoning_dropped,
            agent::FactKind::CompletionStarted { .. } => &mut told.completions_started,
            agent::FactKind::CompletionAnswered { .. } => &mut told.completions_answered,
            agent::FactKind::CompletionFailed { .. } => &mut told.completions_failed,
            agent::FactKind::CompletionCancelled { .. } => &mut told.completions_cancelled,
            agent::FactKind::CompletionRetried { .. } => &mut told.completions_retried,
            agent::FactKind::Tools { opener: _, fact } => match fact {
                tools::FactKind::Opened { .. } => &mut told.kits_opened,
                tools::FactKind::Closed { .. } => &mut told.kits_closed,
                tools::FactKind::Started { .. } => &mut told.tools_started,
                tools::FactKind::Answered { .. } => &mut told.tools_answered,
                tools::FactKind::Refused { .. }
                | tools::FactKind::Closing { .. }
                | tools::FactKind::JobEnded { .. } => return,
            },
            agent::FactKind::DelegateStarted { .. } => &mut told.delegates_started,
            agent::FactKind::DelegateAnswered { .. } => &mut told.delegates_answered,
            agent::FactKind::DelegateCancelled { .. } | agent::FactKind::DelegateWithheld { .. } => {
                &mut told.delegates_cancelled
            }
            agent::FactKind::Yielded { .. } => &mut told.yielded,
            agent::FactKind::Used { .. } => &mut told.used,
            agent::FactKind::Ended { .. } => &mut told.ended,
        };
        *count += 1;
    }

    /// What every native fact must add up to: what the world
    /// saw cross the boundary.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn assert_told(&self) {
        let (told, stats) = (&self.told, &self.stats);
        let count = |n: usize| u32::try_from(n).expect("a small world");
        let opened = count(self.sessions.values().filter(|session| session.session.is_some()).count());
        let turns: u32 = self.sessions.values().map(|session| session.turns).sum();
        assert_eq!((told.opened, told.ended), (opened, count(self.sessions.len())), "an open and an end each");
        assert_eq!(told.completions_started, stats.calls, "a fact for every call");
        let ended = told.completions_answered + told.completions_failed + told.completions_cancelled;
        assert_eq!(ended, stats.calls, "a fact for the end of every call");
        assert_eq!(told.completions_cancelled, stats.cancels, "a fact for every cancelled call");
        assert_eq!(told.used, turns, "a fact for every completion's usage");
        assert_eq!((told.kits_opened, told.kits_closed), (opened, opened), "a kit opened and closed per session");
        assert!(told.tools_started <= stats.ops, "a call the tools start asks io for something");
        assert!(told.tools_answered >= told.tools_started, "a fact for the answer to every call started");
        assert!(told.tools_answered >= stats.results, "a fact for every result that went back");
        assert_eq!(told.yielded, stats.yields, "a fact for every yield");
        assert_eq!(told.delegates_started, stats.delegates, "a fact for every delegated call");
        let delegates_ended = told.delegates_answered + told.delegates_cancelled;
        assert_eq!(delegates_ended, stats.delegates, "a fact for the end of every delegated call");
        assert_eq!(told.delegates_cancelled, stats.withdraws, "a fact for every withdrawn call");
    }

    /// The invariants of a world where nothing is left to happen.
    ///
    /// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
    fn assert_settled(&self) {
        assert_eq!(self.agent.sessions(), 0, "every session has ended and been reclaimed");
        assert_eq!(self.agent.runs(), 0, "every run has ended and been reclaimed");
        assert_eq!((self.agent.kits(), self.agent.flights()), (0, 0), "every kit has closed, its calls answered");
        assert!(self.ops.is_empty() && self.op_cancel_lost.is_empty(), "every operation has ended, once");
        assert_eq!(self.agent.next_deadline(), None, "no alarm outlives its session");
        assert_eq!(self.provider.calls(), 0, "the provider holds no call");
        assert!(self.calls.is_empty() && self.calling.is_empty(), "no call is in flight");
        assert!(self.tools.is_empty() && self.runs.is_empty(), "no tool is running, and every run's end was heard");
        assert!(self.tickets.is_empty(), "every session's tickets were freed when it ended");
        assert!(self.cancel_lost.is_empty() && self.run_cancel_lost.is_empty(), "every lost cancel's race ended");
        assert!(self.serving.is_empty(), "the provider answered every call");
        assert!(
            self.wire.is_empty() && !self.agent_stage.has_events() && !self.provider_stage.has_events(),
            "nothing is on its way"
        );
        for (opener, session) in &self.sessions {
            assert!(session.ended.is_some(), "session {opener} has ended");
            assert_eq!(
                session.records.len(),
                session.accepted.len()
                    - usize::from(matches!(
                        session.ended.as_ref().map(|ended| ended.end.clone()),
                        Some(agent::End::NoSummary { .. })
                    )),
                "every accepted completion except a failed summary emits concrete history"
            );
            if session.session.is_some() {
                assert!(session.priced >= session.turns, "every actual usage and final terminal is priced");
            }
        }
        self.assert_told();
    }

    fn send(&mut self, delivery: Delivery) {
        let at = self.now.saturating_add(self.draw(self.settings.network));
        self.schedule(at, delivery);
    }

    fn schedule(&mut self, at: Time, delivery: Delivery) -> Key {
        // On a coarse grid, if the world has one, so that deliveries coincide.
        let granule = self.settings.granule.as_nanos();
        let at =
            if granule > 0 { Time::from_nanos(at.as_nanos().div_ceil(granule).saturating_mul(granule)) } else { at };
        self.wire.send(at, delivery)
    }

    fn draw(&mut self, span: Span) -> Duration {
        span.draw(&mut self.rng)
    }

    fn log(&mut self, line: &str) {
        self.trace.log(self.now, line);
    }
}

/// The delegated call whose end `event` is, if it is one.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
fn ended_run(event: &agent::Event) -> Option<Token> {
    match event {
        agent::Event::Answered { owner, .. }
        | agent::Event::AnsweredAndYield { owner, .. }
        | agent::Event::AnswerWithheld { owner }
        | agent::Event::AnswerCancelled { owner, .. } => Some(*owner),
        agent::Event::NativeVersion { .. }
        | agent::Event::BuiltinResult { .. }
        | agent::Event::Open { .. }
        | agent::Event::Continue { .. }
        | agent::Event::CancelJobs { .. }
        | agent::Event::Yield { .. }
        | agent::Event::Close { .. }
        | agent::Event::Completed { .. }
        | agent::Event::Overflowed { .. }
        | agent::Event::BudgetDenied { .. }
        | agent::Event::UnsentClosed { .. }
        | agent::Event::Narrowed { .. }
        | agent::Event::Failed { .. }
        | agent::Event::Cancelled { .. }
        | agent::Event::Done { .. } => None,
    }
}

fn describe_agent_event(event: &agent::Event) -> String {
    match event {
        agent::Event::NativeVersion { session, at, version } => {
            format!("native version {} {at:?} {version:?}", session.raw())
        }
        agent::Event::BuiltinResult { session, id, text, error } => {
            format!("builtin result {} {id:?} {text:?} {error}", session.raw())
        }
        agent::Event::Yield { session } => format!("yield completion {}", session.raw()),
        agent::Event::CancelJobs { session } => format!("cancel jobs {}", session.raw()),
        agent::Event::AnswerWithheld { owner } => format!("answer withheld {}", owner.raw()),
        agent::Event::AnswerCancelled { owner, spent, .. } => format!("answer cancelled {} {spent}", owner.raw()),
        agent::Event::Open { opener, opening } => format!("open {} {opening:?}", opener.raw()),
        agent::Event::Answered { owner, text, error, spent, .. } => {
            format!("answered {} {text:?} {error} {spent}", owner.raw())
        }
        agent::Event::AnsweredAndYield { owner, text, error, spent } => {
            format!("answered and yield {} {text:?} {error} {spent}", owner.raw())
        }
        agent::Event::Continue { session, content } => {
            format!("continue {} {:?}", session.raw(), String::from_utf8_lossy(content))
        }
        agent::Event::Close { session } => format!("close {}", session.raw()),
        agent::Event::Completed { owner, completion } => {
            format!("completed {} {:?} with {} blocks", owner.raw(), completion.stop, completion.content.len())
        }
        agent::Event::Overflowed { owner, end } => format!("overflowed {} {end:?}", owner.raw()),
        agent::Event::Narrowed { owner, note, choice, names } => {
            format!("narrowed {} {note:?} {choice:?} {names:?}", owner.raw())
        }
        agent::Event::UnsentClosed { owner } => format!("unsent closed {}", owner.raw()),
        agent::Event::BudgetDenied { owner, reason } => format!("budget denied {} {reason:?}", owner.raw()),
        agent::Event::Failed { owner, failure, .. } => format!("failed {} {failure:?}", owner.raw()),
        agent::Event::Cancelled { owner } => format!("cancelled {}", owner.raw()),
        agent::Event::Done { owner, done } => format!("done {} {done:?}", owner.raw()),
    }
}

fn describe_agent_request(request: &agent::Request) -> String {
    match request {
        agent::Request::JobsCancelled { opener } => format!("jobs cancelled {}", opener.raw()),
        agent::Request::Turn { opener, turn } => format!("turn {} {turn:?}", opener.raw()),
        agent::Request::Priced { opener, spent, own_spent } => {
            format!("priced {} {spent} own {own_spent}", opener.raw())
        }
        agent::Request::Opened { opener, session } => format!("opened {} as {}", opener.raw(), session.raw()),
        agent::Request::Yielded { opener, stop, text } => {
            format!("yielded {} {stop:?} {:?}", opener.raw(), String::from_utf8_lossy(text))
        }
        agent::Request::Used { opener, usage, .. } => format!("used {} {usage:?}", opener.raw()),
        agent::Request::Ended { opener, end, turns, usage, .. } => {
            format!("ended {} {end:?} after {turns} turns, {usage:?}", opener.raw())
        }
        agent::Request::Complete { owner, prompt, deadlines, .. } => {
            let (messages, most) = (prompt.messages.len(), prompt.max_tokens);
            format!("complete {} with {messages} messages, at most {most} tokens, deadlines {deadlines:?}", owner.raw())
        }
        agent::Request::Yield { owner } => format!("yield completion {}", owner.raw()),
        agent::Request::Cancel { owner } => format!("cancel {}", owner.raw()),
        agent::Request::Io { owner, op, deadline } => format!("io {} {op:?} by {}", owner.raw(), deadline.as_nanos()),
        agent::Request::Job { .. } => unreachable!("the tools issue no job act yet"),
        agent::Request::CancelIo { owner } => format!("cancel io {}", owner.raw()),
        agent::Request::Delegate { owner, opener, call, deadline, origin } => {
            format!(
                "delegate {} for {} {call:?} origin {origin:?} by {}",
                owner.raw(),
                opener.raw(),
                deadline.as_nanos()
            )
        }
        agent::Request::Withdraw { owner } => format!("withdraw {}", owner.raw()),
    }
}

/// The results answered as not run in the last message of `prompt`.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
fn not_run(prompt: &Prompt) -> u32 {
    let Some(last) = prompt.messages.last() else { return 0 };
    let unrun = last.content.iter().filter(|block| {
        matches!(
            block,
            Block::ToolResult { result: Returned::NotRun { why: smith_domain_session::llm::NotRun::Stopped }, .. }
        )
    });
    u32::try_from(unrun.count()).expect("a small message")
}

/// Checks that the results at the end of `prompt` answer the calls of the
/// message before them, in their order.
///
/// World contract: domain/session.md, sections 10 and 12; testing-strategy.md, section 2.2.
fn in_call_order(prompt: &Prompt) {
    let [.., asked, answered] = &*prompt.messages else { return };
    let calls: Vec<&[u8]> = asked
        .content
        .iter()
        .filter_map(|block| if let Block::ToolCall { id, .. } = block { Some(&**id) } else { None })
        .collect();
    let results: Vec<&[u8]> = answered
        .content
        .iter()
        .filter_map(|block| if let Block::ToolResult { id, .. } = block { Some(&**id) } else { None })
        .collect();
    assert_eq!(calls, results, "the results go back in call order");
}

/// Every genuinely resumed yielded tail settles before the waking assistant.
fn assert_resumed_tail(session: &Session, index: usize, preceding: &[agent::llm::Message]) {
    if index > 0 && session.accepted_stop[index - 1] != agent::llm::Stop::ToolUse && !session.accepted_late[index - 1] {
        for block in &session.accepted_content[index - 1] {
            if let Block::ToolCall { id, .. } = block {
                let result =
                    recorded_result(preceding, id).expect("continuing a yielded call supplies its next-Turn result");
                assert_eq!(
                    *result,
                    Returned::NotRun { why: smith_domain_session::llm::NotRun::Stopped },
                    "yielded calls did not execute and precede the waking assistant"
                );
            }
        }
    }
}

/// The concrete result after/before the actual assistant, scoped by the caller
/// to avoid matching a reused provider id in a different Turn segment.
fn recorded_result<'a>(messages: &'a [agent::llm::Message], id: &[u8]) -> Option<&'a Returned> {
    messages.iter().flat_map(|message| message.content.iter()).find_map(|block| {
        if let Block::ToolResult { id: result_id, result } = block {
            (id == result_id.as_ref()).then_some(result)
        } else {
            None
        }
    })
}

/// Checked raw-prefix addition: the scripted peer cannot overflow these bounded counters.
fn add_usage(left: Usage, right: Usage) -> Usage {
    Usage {
        input_tokens: Some(
            left.input_tokens
                .unwrap_or(0)
                .checked_add(right.input_tokens.unwrap_or(0))
                .expect("bounded scheduled input"),
        ),
        output_tokens: Some(
            left.output_tokens
                .unwrap_or(0)
                .checked_add(right.output_tokens.unwrap_or(0))
                .expect("bounded scheduled output"),
        ),
        cache_read_tokens: Some(
            left.cache_read_tokens
                .unwrap_or(0)
                .checked_add(right.cache_read_tokens.unwrap_or(0))
                .expect("bounded scheduled cache reads"),
        ),
        cache_write_tokens: Some(
            left.cache_write_tokens
                .unwrap_or(0)
                .checked_add(right.cache_write_tokens.unwrap_or(0))
                .expect("bounded scheduled cache writes"),
        ),
        reasoning_tokens: match (left.reasoning_tokens, right.reasoning_tokens) {
            (Some(left), Some(right)) => Some(left.checked_add(right).expect("bounded scheduled reasoning")),
            (None, _) | (_, None) => None,
        },
    }
}

fn unstarted_delegate(result: &Returned, position: u32) {
    match result {
        Returned::NotRun { why: smith_domain_session::llm::NotRun::Stopped } => {}
        Returned::NotRun { why: smith_domain_session::llm::NotRun::AfterFailedWrite { call } } => {
            assert!(u32::from(*call) <= position + 1, "failure precedes unstarted call");
        }
        Returned::Owned { .. }
        | Returned::Text { .. }
        | Returned::Invalid { .. }
        | Returned::Withdrawn
        | Returned::NotRun { why: smith_domain_session::llm::NotRun::OutsideChoice { .. } } => {
            panic!("an unstarted delegated call has no terminal right");
        }
    }
}
