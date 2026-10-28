use std::collections::{BTreeMap, BTreeSet};

use skein_lib::{Duration, ReplyTo, Rng, Time, Token};
use skein_world::domain::{Key, Ledger, Schedule, Span, Stage, Trace};
use smith_domain_run as run;

use crate::host::{self, Host};
use crate::partner::{Out, Partner, Script, Tally};

/// Room in each domain's output queue. Small, so the loop's flow control (take
/// an event only while there is room for what it may produce) is exercised.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
const OUT: u32 = 4;

/// Immutable seeded peer behavior and component limits used to construct this world.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Settings {
    /// Seeds the world, which seeds the host and the partner.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub seed: u64,
    /// Immutable run limits for the component under test.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub run: run::Limits,
    /// Seeded host behavior for this world.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub host: host::Script,
    /// Seeded conversation-peer behavior for this world.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub partner: Script,
    /// One-way latency between the host and the agent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub network: Span,
    /// One-way latency between the run and its conversations. Within the
    /// agent, the top level hands records over in the step that makes them;
    /// a latency here lets them cross in every order.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub hop: Span,
    /// What the checkouts hold, and how io works on them.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub checkout: Checkouts,
    /// The chance, per mille, that a check in flight wins the race with its
    /// abort. Submitted host delivery remains owed through cancellation.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub races: u32,
    /// The chance, per mille, that as the run is handed an event, the host's
    /// cancel of that event's run comes right before it or right after it.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub inject: u32,
}

/// The checkouts the world makes for runs, and io's way with them.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Checkouts {
    /// The chance, per mille, that a repository has an `AGENTS.md`, of a
    /// length drawn from `1..=guide_max`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub guides: u32,
    /// Inclusive largest generated guide byte length.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub guide_max: u32,
    /// The chance, per mille, that a repository has checks, and how many
    /// times they fail before they pass: drawn from `0..=check_failures`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub checks: u32,
    /// Chance per mille that a scripted check process exits unsuccessfully.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub check_failures: u32,
    /// The chance, per mille, that a guide is not UTF-8 text.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub not_text: u32,
    /// How long io takes for each read or probe, and the chance, per mille,
    /// that it fails.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub io: Span,
    /// Chance per mille that a scripted IO request fails.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub io_failures: u32,
    /// How long checks run.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub check: Span,
}

impl Settings {
    /// A world where nothing goes wrong at the agent's side: room for every
    /// run, charters well within the limits, an LLM that works and yields now
    /// and then but never finishes, and no cancels.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub const fn calm(seed: u64) -> Settings {
        Settings {
            seed,
            run: calm_run_limits(),
            host: host::Script {
                reserve: run::charter::Reserve { turns: 0, time: Duration::ZERO },
                checks: Duration::from_secs(600),
                jobs: 4,
                window: Duration::from_secs(10),
                cancels: 0,
                cancel: Span::millis(1_000, 60_000),
                recancels: 0,
                late_cancels: 0,
                brief_min: 100,
                brief_max: 2000,
                turns_min: 20,
                turns_max: 40,
                tokens_min: 1 << 20,
                tokens_max: 1 << 21,
                time: Span { min: Duration::from_secs(3600), max: Duration::from_secs(7200) },
                max_tokens: 4096,
                writable: 500,
                changes: 700,
                reports: 0,
                failures: 0,
                verdicts: 700,
                agents: 0,
                deliveries: 0,
                host_tools: 500,
                relay: Span::millis(10, 500),
                relay_busy: 250,
                relay_lost: 250,
                push: Span::millis(10, 500),
                moved: 0,
                push_failures: 0,
            },
            partner: Script {
                prices: None,
                conversations: 16,
                invalid: 0,
                turn: Span::millis(100, 5_000),
                input: 4_000,
                output: 1_000,
                cache: 2_000,
                faults: 0,
                finishes: 0,
                asks: 0,
                host_calls: 0,
                deliveries: 0,
                waits: 0,
                yields: 200,
                bad_asks: 0,
                shares: 0,
                parallel: 1,
                changes: 500,
                reports: 0,
                failures: 0,
                good: 1000,
                odd_stops: 0,
                settle: Span::millis(1, 500),
                races: 500,
            },
            network: Span::millis(1, 20),
            hop: Span::millis(0, 2),
            checkout: Checkouts {
                guides: 500,
                guide_max: 2000,
                checks: 500,
                check_failures: 0,
                not_text: 0,
                io: Span::millis(0, 50),
                io_failures: 0,
                check: Span::millis(100, 5_000),
            },
            races: 500,
            inject: 0,
        }
    }
}

/// Immutable admission/ownership configuration for the calm component world.
/// Contract: domain/run.md, sections 3 and 13.
const fn calm_run_limits() -> run::Limits {
    run::Limits {
        runs: 4,
        conversations: 16,
        run_bytes: 1 << 16,
        brief_sections: 4,
        host_part: 1 << 20,
        room: 0,
        compaction_threshold: [4, 5],
        compaction_material: 0,
        schema_costs: smith_domain_run::SchemaCosts::NONE,
        directories: 4,
        directory_name_bytes: 256,
        host_path_bytes: 4096,
        conflicts: 64,
        conflict_path_bytes: 4096,
        host_tools: 4,
        host_input_bytes: 65_536,
        host_reply_bytes: 65_536,
        answered_calls: 16,
        answered_bytes: 4096,
        host_timeout: Duration::from_secs(60),
        host_timeout_max: Duration::from_secs(60),
        host_backoff: Duration::from_millis(50),
        verdicts: 4,
        calls: 16,
        budget: run::Budget { turns: 1000, spend: 1 << 32, time: Duration::from_secs(24 * 3600) },

        models: 4,
        run_conversations: 4,
        answer_bytes: 512,
        nudges: 2,
        guide_bytes: 1024,
        io_timeout: Duration::from_secs(5),
        outcome_bytes: 4096,
        delivery_timeout: Duration::from_secs(600),
        check_timeout: Duration::from_secs(600),
        wrap_up: Duration::from_secs(60),
        check_tail: 256,
        facts: 64,
        messages: 8,
        message_bytes: 4096,
        offer_messages: 8,
        offer_bytes: 32_782,
        waiting: skein_lib::Duration::from_secs(300),
    }
}

/// What the world counted.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub struct Stats {
    /// Runs the host started, and cancels it sent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub starts: u32,
    /// Count of cancels observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub cancels: u32,
    /// Reads, probes and checks the run asked io for, and checks it aborted.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub reads: u32,
    /// Count of probes observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub probes: u32,
    /// Count of checks observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub checks: u32,
    /// Count of aborts observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub aborts: u32,
    /// Actual deliveries the run submitted to its host.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub pushes: u32,
    /// Scripted host relay terminals by decision class.
    pub relay_busy: u32,
    pub relay_lost: u32,
    pub relay_answered: u32,
    /// Conversations the run opened, sub-agents among them, nudges it said,
    /// and closes it sent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub opens: u32,
    /// Count of children observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub children: u32,
    /// Count of says observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub says: u32,
    /// Count of closes observed at this scripted boundary.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub closes: u32,
    /// The most conversations a run had live at once.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub peak: u32,
    /// What the host and the partner counted.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub host: host::Tally,
    /// Observed partner counters collected by this world.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub partner: Tally,
}

/// Something on its way, delivered at its time.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
enum Delivery {
    /// The host's start reaches the agent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Start {
        reply_to: ReplyTo,
        host_run: Token,
        charter: run::Charter,
        workspace: Option<run::Workspace>,
    },
    /// The host's cancel reaches the agent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Cancel {
        run: Token,
    },
    /// The end of a host call reaches the run.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Host(run::Event),
    /// The run's word reaches the host.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Admitted {
        host_run: Token,
        run: Token,
    },
    Answered {
        host_run: Token,
    },
    Checking {
        host_run: Token,
    },
    ChecksEnded {
        host_run: Token,
    },
    Submit {
        host_run: Token,
        owner: Token,
        deadline: Time,
    },
    /// The run's requests reach its conversations.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Open {
        conversation: Token,
        opening: run::Opening,
    },
    Say {
        peer: Token,
    },
    Close {
        peer: Token,
    },
    Return {
        call: Token,
        spent: u64,
        result: run::Returned,
    },
    /// A conversation's event reaches the run.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Event(run::Event),
    /// Price and raw usage share their one actual completion callback hop.
    /// Price steps first; no extra random transport latency is invented.
    /// Scripted-world contract: domain/run.md, sections 9 and 13.
    CompletionUsage {
        conversation: Token,
        own_spent: u64,
        subtree_spent: u64,
        usage: run::Spend,
    },
    /// An unsent completion waits behind its actual Started/Priced notices.
    /// Scripted-world contract: domain/run.md, sections 9 and 13.
    Permit {
        peer: Token,
        conversation: Token,
    },
    /// io's answer reaches the run.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Io(run::Event),
    /// The partner's own timer.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    Wake {
        peer: Token,
        wake: u64,
    },
}

/// The four one-way channels, named by where they lead, each delivering in the
/// order it was given.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Lane {
    Agent,
    Host,
    Conversations,
    Run,
}

/// A start, as the world tracks it: the budget its run keeps to, and the
/// answer once it has come.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct Start {
    reserved_spend: u64,
    budget: run::Budget,
    reserve: run::charter::Reserve,
    /// The roots whose checks a change must pass, once its checkout is made.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    checks: BTreeSet<Token>,
    answer: Option<run::Answer>,
}

/// An open, as the world tracks it.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct Open {
    started: bool,
    ended: bool,
    opened_at: Time,
    ended_at: Option<Time>,
    spent: run::Spend,
}

/// A run as the world sees it from outside, to tell what state a cancel finds
/// it in.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct RunView {
    reserved_spend: u64,
    admitted_at: Time,
    budget: run::Budget,
    reserve: run::charter::Reserve,
    deadline: Time,
    /// Its main conversation, once opened.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    main: Option<Token>,
    started: bool,
    waiting: bool,
    /// Whether it decided how it ends: it closed main, or a cancel or its
    /// deadline came while it prepared or worked.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    decided: bool,
    spent: run::Spend,
    /// Actual main Turn output count and last reported read fence.
    turns: u32,
    read: Option<Token>,
    /// Its conversations opened and not ended, and the most it had at once.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    live: u32,
    peak: u32,
    /// The roots whose checks a change must pass before it is pushed: of the
    /// writable repositories, when the spec wants checks, those with checks,
    /// less those io failed to look in.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    checks: BTreeSet<Token>,
    /// The iteration it answered in.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    answered: Option<u64>,
}

/// What one step or alarm of the run made, for the world to attribute: the run
/// it was about, if known, how many requests it made, and the state a cancel
/// it took found the run in.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct Made {
    run: Option<Token>,
    requests: u32,
    cancel: Option<(Token, &'static str)>,
    /// The call, if the step took an ask for a sub-agent, and what its run had
    /// left as it took it.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    asked: Option<(Token, run::Budget, run::Share, Duration)>,
}

/// A conversation's call, as the world tracks it.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct Call {
    conversation: Token,
    wait: bool,
    workspace: bool,
    returned: bool,
}

/// A push, as the world tracks it: for which job.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
struct Pushing {
    job: Token,
}

/// Real domain plus scripted typed peers, pending terminals and external observations for this component story.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
pub struct World {
    lease_seen: Vec<(Time, Token, crate::lease_referee::Seen)>,
    wind_down_seen: Vec<(Time, Token, crate::wind_down_referee::Seen)>,
    share_seen: Vec<(Time, crate::share_referee::Seen)>,
    workspace_of_call: BTreeMap<Token, Token>,
    now: Time,
    rng: Rng,
    settings: Settings,

    run: run::Domain,
    run_stage: Stage<run::Limits, run::Event, run::Request>,

    host: Host,
    partner: Partner,
    permits: Vec<(Token, Token)>,
    held_permits: BTreeMap<Token, Token>,

    /// Deliveries in flight.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    wire: Schedule<Delivery>,
    /// When each lane delivers its latest, which the next may not overtake.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    lanes: [Time; 4],
    /// Every start, by the host's name for it; every open, by the run's
    /// name for the conversation; every conversation's call, by its own name.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    starts: BTreeMap<Token, Start>,
    opens: BTreeMap<Token, Open>,
    reservations: BTreeSet<Token>,
    child_completions: u32,
    main_in_reserve: u32,
    calls: BTreeMap<Token, Call>,
    /// The checkouts' files, by their roots and paths, which of them are
    /// executable, and how many more times each repository's checks fail.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    files: BTreeMap<(Token, Vec<u8>), Vec<u8>>,
    executables: BTreeSet<(Token, Vec<u8>)>,
    failures: BTreeMap<Token, u64>,
    /// io's operations in flight, by their owners: the runs' looks and the
    /// calls' checks (with the root each runs in), apart, as their tokens are
    /// of different kinds; and where the result of each check in flight is on
    /// the wire.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    looks: BTreeSet<Token>,
    checking: BTreeMap<Token, Token>,
    checks: BTreeMap<Token, Key>,
    /// Pushes in flight, by the run's owner; and the jobs whose runs pushed a
    /// change.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pushes: Ledger<Token, Pushing>,
    pushed: BTreeSet<Token>,
    /// Outstanding attempts and first decisions, kept by the scripted host.
    relays: BTreeSet<(Token, u32)>,
    relay_owner: BTreeMap<Token, Token>,
    relay_stage: BTreeMap<Token, &'static str>,
    host_decisions: BTreeMap<(Token, u64, u32, u32), run::HostAnswer>,
    /// The roots whose checks passed, for each landing call, until it pushes
    /// or its run answers.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    passed: BTreeMap<Token, BTreeSet<Token>>,
    /// The runs as the world sees them, by the run's names for them; which run
    /// each host name, conversation and landing call is of; the
    /// conversation each peer is; and the landing calls with a check or push
    /// in flight.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    views: BTreeMap<Token, RunView>,
    run_of_owner: BTreeMap<Token, Token>,
    run_of_conversation: BTreeMap<Token, Token>,
    run_of_call: BTreeMap<Token, Token>,
    conversation_of_peer: BTreeMap<Token, Token>,
    landing: BTreeSet<Token>,
    /// The states cancels and deadlines found runs in, and how many times.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    cancel_cells: BTreeMap<&'static str, u32>,
    /// The facts the run told, by kind.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    facts: BTreeMap<&'static str, u32>,
    facts_seen: Vec<run::facts::Fact>,
    charges_seen: Vec<(Time, Token, crate::facts_referee::Charged)>,
    empty_brief_wait: Option<bool>,
    waiting_time: Option<Duration>,
    awaiting_message: Option<run::Message>,
    carried_messages: Box<[run::Message]>,
    opening_prompts: Vec<Box<[u8]>>,
    message_chance: u32,
    message_name: u64,
    message_cells: BTreeMap<&'static str, u32>,
    message_seen: Vec<(Time, Token, crate::messages_referee::Seen)>,
    message_answers: Vec<(Token, Option<Token>)>,
    deadline_cells: BTreeMap<&'static str, u32>,
    iteration: u64,
    fact_holds: u64,
    /// The run the last request answered, for the iteration's attribution;
    /// the call the step being attributed took, if it asked for a sub-agent,
    /// with what its run had left then; and the sub-agent each such call
    /// opened, until it returns.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    just_answered: Option<Token>,
    asked: Option<(Token, run::Budget, run::Share, Duration)>,
    pending_asks: BTreeMap<Token, (Token, run::Budget, run::Share, Duration)>,
    child_of_call: BTreeMap<Token, Token>,

    stats: Stats,
    trace: Trace,
}

impl World {
    /// Constructs empty bounded state under the supplied immutable limits; no IO or clocks are consulted.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn new(settings: Settings) -> World {
        assert!(run::valid(&settings.run), "the shell refuses invalid limits");
        let mut rng = Rng::new(settings.seed);
        let host = Host::new(host::Script { checks: settings.run.check_timeout, ..settings.host }, rng.next_u64());
        let partner = Partner::new(settings.partner, rng.next_u64());
        World {
            now: Time::ZERO,
            rng,
            settings,
            lease_seen: Vec::new(),
            wind_down_seen: Vec::new(),
            share_seen: Vec::new(),
            workspace_of_call: BTreeMap::new(),
            run: run::Domain::new(&settings.run, settings.seed),
            run_stage: Stage::new(settings.run, run::MAX_OUT, OUT),
            host,
            partner,
            permits: Vec::new(),
            held_permits: BTreeMap::new(),
            wire: Schedule::new(),
            lanes: [Time::ZERO; 4],
            starts: BTreeMap::new(),
            opens: BTreeMap::new(),
            reservations: BTreeSet::new(),
            child_completions: 0,
            main_in_reserve: 0,
            calls: BTreeMap::new(),
            files: BTreeMap::new(),
            executables: BTreeSet::new(),
            failures: BTreeMap::new(),
            looks: BTreeSet::new(),
            checking: BTreeMap::new(),
            checks: BTreeMap::new(),
            pushes: Ledger::new("push"),
            pushed: BTreeSet::new(),
            relays: BTreeSet::new(),
            relay_owner: BTreeMap::new(),
            relay_stage: BTreeMap::new(),
            host_decisions: BTreeMap::new(),
            passed: BTreeMap::new(),
            views: BTreeMap::new(),
            run_of_owner: BTreeMap::new(),
            run_of_conversation: BTreeMap::new(),
            run_of_call: BTreeMap::new(),
            conversation_of_peer: BTreeMap::new(),
            landing: BTreeSet::new(),
            cancel_cells: BTreeMap::new(),
            facts: BTreeMap::new(),
            facts_seen: Vec::new(),
            charges_seen: Vec::new(),
            empty_brief_wait: None,
            waiting_time: None,
            awaiting_message: None,
            carried_messages: Box::default(),
            opening_prompts: Vec::new(),
            message_chance: 0,
            message_name: 0,
            message_cells: BTreeMap::new(),
            message_seen: Vec::new(),
            message_answers: Vec::new(),
            deadline_cells: BTreeMap::new(),
            iteration: 0,
            fact_holds: 0,
            just_answered: None,
            asked: None,
            pending_asks: BTreeMap::new(),
            child_of_call: BTreeMap::new(),
            stats: Stats::default(),
            trace: Trace::default(),
        }
    }

    /// Give each host Start an empty brief and this explicit waiting authority.
    pub fn empty_brief(&mut self, wait: bool) {
        self.empty_brief_wait = Some(wait);
    }

    /// Use this host-selected idle deadline in every actual Start.
    pub fn waiting_time(&mut self, waiting: Duration) {
        self.waiting_time = Some(waiting);
    }

    /// Relay one scripted first message only after the actual awaiting notice.
    pub fn message_when_awaiting(&mut self, message: run::Message) {
        self.awaiting_message = Some(message);
        self.partner.tell_turns();
    }

    /// Carry this ordered batch in each host Start and observe actual told turns.
    pub fn carry_messages(&mut self, messages: Box<[run::Message]>) {
        self.carried_messages = messages;
        self.partner.tell_turns();
    }

    /// Every actual emitted fact, retained independently of its drain timing.
    #[must_use]
    pub fn facts_seen(&self) -> &[run::facts::Fact] {
        &self.facts_seen
    }

    /// Host ceilings and cumulative real callbacks, observed before each usage ingress.
    #[must_use]
    pub fn charges_seen(&self) -> &[(Time, Token, crate::facts_referee::Charged)] {
        &self.charges_seen
    }

    /// Settled own usage from outside callback accounting and the actual opening clock.
    #[must_use]
    pub fn child_usage_seen(&self, conversation: Token) -> Option<(Time, run::facts::Used)> {
        let opened = self.opens.get(&conversation)?;
        let ended_at = opened.ended_at?;
        Some((
            ended_at,
            run::facts::Used {
                turns: opened.spent.turns,
                time: ended_at.saturating_since(opened.opened_at),
                spend: opened.spent.units,
            },
        ))
    }

    /// Actual child opening shares and external ordinary credit when asked.
    #[must_use]
    pub fn share_seen(&self) -> &[(Time, crate::share_referee::Seen)] {
        &self.share_seen
    }

    /// Actual child completions admitted and main completions begun after ordinary turns ran out.
    #[must_use]
    pub fn reserve_progress(&self) -> (u32, u32) {
        (self.child_completions, self.main_in_reserve)
    }

    /// Admitted reserves, actual final provider starts and answers, observed outside the run.
    #[must_use]
    pub fn wind_down_seen(&self) -> &[(Time, Token, crate::wind_down_referee::Seen)] {
        &self.wind_down_seen
    }

    /// Actual user opening bytes at the conversation boundary.
    #[must_use]
    pub fn opening_prompts(&self) -> &[Box<[u8]>] {
        &self.opening_prompts
    }

    /// Actual child authority, workspace operations and their call terminals.
    #[must_use]
    pub fn lease_seen(&self) -> &[(Time, Token, crate::lease_referee::Seen)] {
        &self.lease_seen
    }

    /// Inject named person messages before or after actual callbacks, in per mille.
    pub fn inject_messages(&mut self, chance: u32) {
        self.message_chance = chance;
        self.partner.tell_turns();
    }

    /// Actual received/read/unread facts and final fences, for replay and mutation.
    #[must_use]
    pub fn message_seen(&self) -> &[(Time, Token, crate::messages_referee::Seen)] {
        &self.message_seen
    }

    /// Externally observed callback cells where a person message arrived.
    #[must_use]
    pub fn message_cells(&self) -> &BTreeMap<&'static str, u32> {
        &self.message_cells
    }

    /// Current injected monotonic time in this world.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn now(&self) -> Time {
        self.now
    }

    /// Observed boundary counters.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn stats(&self) -> Stats {
        Stats { host: self.host.tally(), partner: self.partner.tally(), ..self.stats }
    }

    /// Parent passes that left pending work untouched until native facts drained.
    #[must_use]
    pub fn fact_holds(&self) -> u64 {
        self.fact_holds
    }

    /// What crossed between the domains and the world, in order, with times.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        self.trace.lines()
    }

    /// The states cancels found runs in: preparing, stopping, opening,
    /// working, landing, over (its budget), winding, answered (earlier in the
    /// same iteration) or gone; and how many times each.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn cancel_cells(&self) -> &BTreeMap<&'static str, u32> {
        &self.cancel_cells
    }

    /// The states runs' deadlines found them in, as for cancels.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn deadline_cells(&self) -> &BTreeMap<&'static str, u32> {
        &self.deadline_cells
    }

    /// Every run started, with the run's answer once it has come.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    pub fn answers(&self) -> impl Iterator<Item = Option<&run::Answer>> {
        self.starts.values().map(|start| start.answer.as_ref())
    }

    /// Runs until nothing is left to happen, then checks the invariants of a
    /// settled world. Panics if it takes more than `iterations`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
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
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn iterate(&mut self) {
        self.run_stage.tick(self.now);
        self.deliver();

        // Each stage takes its events, then fires its alarms, while it has room
        // for what one more may produce.
        self.iteration += 1;
        let mut made = Vec::new();
        while self.run.facts_room() >= run::max_facts(&self.run_stage.env.limits)
            && let Some(event) = self.run_stage.next_event()
        {
            self.log(&format!("run <- {event:?}"));
            let run = self.run_of(&event);
            let cancel = self.note(&event, run);
            // What the run has left as it takes an ask, before what else it
            // takes in this iteration is counted.
            let asked =
                if let run::Event::Delegated { call, ask: run::Ask::SubAgent { share, .. }, deadline, .. } = &event {
                    let left = run.map(|run| self.views[&run].budget_left(self.now));
                    left.map(|left| (*call, left, *share, deadline.saturating_since(self.now)))
                } else if let run::Event::JobsCancelled { conversation } = &event {
                    self.pending_asks.remove(conversation)
                } else {
                    None
                };
            let before = self.run_stage.out.len();
            run::step(&mut self.run, &self.run_stage.env, event, &mut self.run_stage.out);
            made.push(Made { run, requests: self.run_stage.out.len() - before, cancel, asked });
        }
        while self.run.facts_room() >= run::max_facts(&self.run_stage.env.limits)
            && self.run_stage.has_room()
            && self.run.is_due(self.now)
        {
            self.log("run alarm");
            let run = self.deadline_due();
            let before = self.run_stage.out.len();
            run::fire(&mut self.run, &self.run_stage.env, &mut self.run_stage.out);
            made.push(Made { run, requests: self.run_stage.out.len() - before, cancel: None, asked: None });
        }

        if !self.run_stage.has_events()
            && self.run.is_ready()
            && self.run_stage.has_room()
            && self.run.facts_room() >= run::max_facts(&self.run_stage.env.limits)
        {
            let before = self.run_stage.out.len();
            run::resume(&mut self.run, &self.run_stage.env, &mut self.run_stage.out);
            made.push(Made { run: None, requests: self.run_stage.out.len() - before, cancel: None, asked: None });
        }
        if self.run.facts_room() < run::max_facts(&self.run_stage.env.limits)
            && (self.run_stage.has_events() || self.run.is_due(self.now))
        {
            self.fact_holds += 1;
        }
        // What the steps asked for, submitted at the end of the iteration, each
        // attributed to the run its step was about.
        let mut answered = BTreeMap::new();
        for (index, made) in made.iter().enumerate() {
            let mut current = made.run;
            self.asked = made.asked;
            for _ in 0..made.requests {
                let request = self.run_stage.out.pop().expect("a step's requests are queued");
                current = self.run_request(request, current);
                if let Some(run) = self.just_answered.take() {
                    answered.insert(run, index);
                }
            }
        }
        // A cancel stepped after its run answered in this iteration found it
        // answered, not yet reclaimed.
        for (index, made) in made.iter().enumerate() {
            if let Some((run, mut cell)) = made.cancel {
                if answered.get(&run).is_some_and(|at| *at < index) {
                    cell = "answered";
                }
                *self.cancel_cells.entry(cell).or_insert(0) += 1;
            }
        }
        self.provider_permits();
        // The host's alarms.
        if self.host.is_due(self.now) {
            let mut out = Vec::new();
            self.host.fire(self.now, &mut out);
            self.host_out(out);
        }

        // The facts the run told, drained at the world's pace.
        while let Some(fact) = self.run.pop_fact() {
            self.fact(&fact);
        }

        for (run, read) in std::mem::take(&mut self.message_answers) {
            self.message_seen.push((self.now, run, crate::messages_referee::Seen::Answer { read }));
        }

        // The reclaim point.
        self.run.reclaim();
        assert!(self.run.runs() <= self.settings.run.runs, "runs stay within their slots");
        assert!(self.run.conversations() <= self.settings.run.conversations, "conversations stay within their slots");
        assert!(self.run.calls() <= self.settings.run.calls, "calls stay within their slots");
    }

    /// The run's requests, carried out the way the top level, the protocol
    /// layer, io and the conversations would. `current` is the run the step
    /// that made it was about, if known; what it is after the request is
    /// returned.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn completion_ready(&mut self, conversation: Token) {
        if let Some(peer) = self.held_permits.remove(&conversation) {
            self.permits.push((peer, conversation));
        }
    }

    fn close_conversation(&mut self, peer: Token) {
        let conversation = self.conversation_of_peer[&peer];
        let run = self.run_of_conversation[&conversation];
        if self.views[&run].main == Some(conversation) {
            self.views.get_mut(&run).expect("a run outlives its conversations").decided = true;
        }
        self.stats.closes += 1;
        self.send(Lane::Conversations, Delivery::Close { peer });
    }

    fn run_request(&mut self, request: run::Request, current: Option<Token>) -> Option<Token> {
        self.log(&format!("run -> {request:?}"));
        let mut current = current;
        match request {
            run::Request::Turn { host_run, number, position: _, read, spent, opens_window: _, record: _ } => {
                current = Some(self.observed_turn(host_run, number, read, spent));
            }
            run::Request::CompletionReady { conversation } => self.completion_ready(conversation),
            run::Request::MessageRefused { .. } | run::Request::Yield { .. } => {}
            run::Request::Waiting { host_run, read } => {
                current = Some(self.observed_waiting(host_run, read));
            }
            request @ run::Request::HostCall { .. } => {
                self.host_call(request);
            }
            run::Request::WithdrawHost { relay } => {
                self.withdraw_host(relay);
            }
            run::Request::Admitted { host_run, run } => {
                self.admitted(host_run, run);
                current = Some(run);
                self.send(Lane::Host, Delivery::Admitted { host_run, run });
            }
            run::Request::Answer { to, answer, read, detail: _ } => {
                let owner = self.answer(to, answer);
                if let Some(&run) = self.run_of_owner.get(&owner) {
                    assert_eq!(read, self.views[&run].read, "Answer keeps the last actual told fence");
                    self.message_answers.push((run, read));
                    // What its landings that did not push had passed.
                    self.passed.retain(|call, _| self.run_of_call.get(call) != Some(&run));
                    self.views.get_mut(&run).expect("a view per admitted run").answered = Some(self.iteration);
                    self.just_answered = Some(run);
                }
            }
            run::Request::Open { conversation, opening } => self.open(conversation, opening, current),
            run::Request::Say { peer, text: _ } => {
                self.stats.says += 1;
                self.send(Lane::Conversations, Delivery::Say { peer });
            }
            run::Request::CancelJobs { peer } => self.cancel_jobs(peer),
            run::Request::Close { peer } => self.close_conversation(peer),
            run::Request::Return { call, result, spent } => {
                let ledger = self.calls.get_mut(&call).expect("a return is of a call that was made");
                assert!(!ledger.returned, "a call returns once");
                ledger.returned = true;
                if matches!(
                    result,
                    run::Returned::HostAnswered(_)
                        | run::Returned::HostTooLarge { .. }
                        | run::Returned::HostUnknown
                        | run::Returned::HostRejected(_)
                ) {
                    let run = self.run_of_conversation[&ledger.conversation];
                    self.relay_stage.remove(&run);
                }
                if let Some(child) = self.child_of_call.remove(&call) {
                    assert!(self.opens[&child].ended, "a sub-agent has ended before its call returns");
                    let run = self.run_of_conversation[&child];
                    self.lease_seen.push((self.now, run, crate::lease_referee::Seen::ChildReturned { call }));
                }
                if let Some(owner) = self.workspace_of_call.remove(&call) {
                    let run = self.run_of_call[&owner];
                    self.lease_seen.push((self.now, run, crate::lease_referee::Seen::WorkspaceReturned { owner }));
                }
                self.send(Lane::Conversations, Delivery::Return { call, result, spent });
            }
            request @ (run::Request::Read { .. } | run::Request::Probe { .. } | run::Request::Abort { .. }) => {
                self.io_request(request);
            }
            run::Request::Check { owner, program, deadline, tail } => {
                let run = current.expect("a check is made in a step about its run");
                self.assert_alone(run);
                self.workspace_lease(run, owner);
                self.lease_seen.push((self.now, run, crate::lease_referee::Seen::Check { owner }));
                self.run_of_call.insert(owner, run);
                self.landing.insert(owner);
                self.check(owner, &program, deadline, tail);
            }
            run::Request::Checking { host_run, deadline: _ } => self.send(Lane::Host, Delivery::Checking { host_run }),
            run::Request::ChecksEnded { host_run } => self.send(Lane::Host, Delivery::ChecksEnded { host_run }),
            run::Request::Deliver { host_run, owner, change: _, name: _, deadline } => {
                let run = current.expect("a push is made in a step about its run");
                self.assert_alone(run);
                self.workspace_lease(run, owner);
                self.lease_seen.push((self.now, run, crate::lease_referee::Seen::Delivery { owner }));
                self.run_of_call.insert(owner, run);
                self.landing.insert(owner);
                self.pushes.open(owner, Pushing { job: host_run });
                // Checked is pushed: every repository whose checks the change
                // must pass passed them, for this landing.
                let passed = self.passed.remove(&owner).unwrap_or_default();
                let run = &self.views[&self.run_of_call[&owner]];
                assert!(run.checks.is_subset(&passed), "a change is pushed once every repository's checks passed it");
                self.stats.pushes += 1;
                self.send(Lane::Host, Delivery::Submit { host_run, owner, deadline });
            }
        }
        current
    }

    fn cancel_jobs(&mut self, peer: Token) {
        let conversation = self.conversation_of_peer[&peer];
        if let Some(asked) = self.asked.take() {
            assert!(self.pending_asks.insert(conversation, asked).is_none(), "one exclusive child ask awaits jobs");
        }
        self.run_stage.push(run::Event::JobsCancelled { conversation });
    }

    fn host_call(&mut self, request: run::Request) {
        let run::Request::HostCall { host_run, relay, name, tool, effect, input, deadline } = request else {
            unreachable!("only a host call reaches this handler")
        };
        assert_eq!(tool.as_ref(), b"comment");
        assert_eq!(effect, run::HostEffect::Read);
        assert_eq!(input.bytes(), b"{}");
        assert!(deadline <= self.now.saturating_add(self.settings.run.host_timeout));
        let run = self.run_of_owner[&host_run];
        assert!(self.relays.insert((relay.owner, relay.attempt)), "one terminal per host relay");
        self.relay_owner.insert(relay.owner, run);
        self.relay_stage.insert(run, "relay sending");
        let key = (host_run, name.activation, name.completion, name.position);
        let reply = if let Some(answer) = self.host_decisions.get(&key) {
            run::HostReply::Answered(answer.clone())
        } else if self.rng.chance(self.settings.host.relay_busy) {
            run::HostReply::Busy
        } else {
            let answer = run::HostAnswer::new(b"recorded".as_slice().into(), false).expect("bounded host text");
            self.host_decisions.insert(key, answer.clone());
            if self.rng.chance(self.settings.host.relay_lost) {
                run::HostReply::Unanswered(run::Unanswered::Lost)
            } else {
                run::HostReply::Answered(answer)
            }
        };
        let delay = self.draw(self.settings.host.relay);
        self.schedule(self.now.saturating_add(delay), Delivery::Host(run::Event::HostReturned { relay, reply }));
    }

    /// Admit scripted provider turns after earlier Started/Priced notices.
    fn provider_permits(&mut self) {
        // The provider gate runs after all earlier Started/Priced notices.
        for (peer, conversation) in std::mem::take(&mut self.permits) {
            if self.run_stage.has_events() {
                self.permits.push((peer, conversation));
                continue;
            }
            assert_eq!(self.conversation_of_peer.get(&peer), Some(&conversation), "Started really stepped first");
            let mut permit = run::completion_permit(
                &mut self.run,
                conversation,
                self.now,
                run::Most::Bounded(self.partner.turn_max().units),
            );
            if matches!(permit, run::CompletionPermit::Allowed | run::CompletionPermit::Final { .. }) {
                let owner = self.run_of_conversation[&conversation];
                let view = &self.views[&owner];
                let pending = self
                    .reservations
                    .iter()
                    .filter(|conversation| self.run_of_conversation[conversation] == owner)
                    .count();
                let child = view.main != Some(conversation);
                let ordinary_turns = view.budget.turns.saturating_sub(view.reserve.turns);
                match run::reserve(
                    &mut self.run,
                    conversation,
                    conversation,
                    run::Most::Bounded(self.partner.turn_max().units),
                ) {
                    Ok(()) => {
                        if matches!(permit, run::CompletionPermit::Final { .. }) {
                            self.wind_down_seen.push((
                                self.now,
                                owner,
                                crate::wind_down_referee::Seen::Final {
                                    main: !child,
                                    live_children: view.live.saturating_sub(1),
                                },
                            ));
                        }
                        if child && !view.decided {
                            assert!(
                                u64::from(view.spent.turns) + u64::try_from(pending).expect("bounded pending turns")
                                    < u64::from(ordinary_turns),
                                "a child completion cannot take a reserved turn"
                            );
                            self.child_completions += 1;
                        } else if !child && view.spent.turns >= ordinary_turns {
                            self.main_in_reserve += 1;
                        }
                        assert!(self.reservations.insert(conversation), "one pending completion per conversation");
                    }
                    Err(exhausted) => permit = run::CompletionPermit::Denied(exhausted),
                }
            }
            if permit == run::CompletionPermit::Held {
                self.held_permits.insert(conversation, peer);
            }
            let mut out = Vec::new();
            self.partner.permitted(self.now, peer, permit, &mut out);
            self.partner_out(out);
        }
    }

    fn withdraw_host(&mut self, relay: run::RelayName) {
        assert!(self.relays.contains(&(relay.owner, relay.attempt)), "withdraw only a live relay");
        let run = self.relay_owner[&relay.owner];
        self.relay_stage.insert(run, "relay withdrawing");
    }

    /// Retain the independent start budget and checks at actual admission.
    /// Contract: domain/run.md, sections 3, 9 and 13.
    fn admitted(&mut self, host_run: Token, run: Token) {
        let Start { budget, reserve, checks, reserved_spend, .. } = &self.starts[&host_run];
        let view = RunView {
            reserved_spend: *reserved_spend,
            admitted_at: self.now,
            budget: *budget,
            reserve: *reserve,
            checks: checks.clone(),
            deadline: self.now.saturating_add(budget.time),
            main: None,
            started: false,
            waiting: false,
            decided: false,
            spent: run::Spend::ZERO,
            turns: 0,
            read: None,
            live: 0,
            peak: 0,
            answered: None,
        };
        self.wind_down_seen.push((self.now, run, crate::wind_down_referee::Seen::Admitted { reserve: reserve.turns }));
        self.views.insert(run, view);
        self.run_of_owner.insert(host_run, run);
    }

    /// Observe waiting after preparation or the settlement of an actual turn.
    fn observed_waiting(&mut self, host_run: Token, read: Option<Token>) -> Token {
        let run = self.run_of_owner[&host_run];
        let view = self.views.get_mut(&run).expect("admitted run");
        assert_eq!(read, view.read, "Waiting carries the actual last Turn fence");
        assert!(
            (view.started || (view.main.is_none() && view.turns == 0 && view.read.is_none()))
                && view.answered.is_none(),
            "Waiting follows preparation or an actual started main"
        );
        assert!(
            self.calls.values().all(|call| {
                self.run_of_conversation[&call.conversation] != run
                    || call.returned
                    || (call.wait && Some(call.conversation) == view.main)
            }),
            "Waiting keeps only its actual main wait call outstanding"
        );
        assert!(
            self.calls
                .values()
                .filter(|call| !call.returned && call.wait && Some(call.conversation) == view.main)
                .count()
                <= 1,
            "at most one main wait remains outstanding"
        );
        view.waiting = true;
        if view.main.is_none()
            && let Some(message) = self.awaiting_message.take()
        {
            self.run_stage.push(run::Event::Message {
                finish_requested: message.finish_requested,
                run,
                name: message.name,
                label: message.label,
                text: message.text,
            });
        }
        run
    }

    /// Count only actual main output; this component neighbour owns opaque records.
    /// Contract: domain/run.md, sections 6 and 13.
    fn observed_turn(&mut self, host_run: Token, number: u32, read: Option<Token>, spent: run::Spend) -> Token {
        let run = self.run_of_owner[&host_run];
        let view = self.views.get_mut(&run).expect("Turn belongs to an admitted run");
        assert_eq!(number, view.turns + 1, "actual main outputs are consecutive");
        assert_eq!(spent, view.spent, "Turn carries the observed cumulative spend");
        view.turns = number;
        view.waiting = false;
        view.read = read;
        run
    }

    /// The run's reads, probes and aborts, carried out the way io would.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn io_request(&mut self, request: run::Request) {
        match request {
            run::Request::Read { owner, at, max, deadline } => {
                let (at_time, read) = match self.io_result(owner, deadline) {
                    Some(at_time) => {
                        let read = match self.files.get(&(at.root, at.path.to_vec())) {
                            // io reads text, cut where a character ends.
                            Some(content) => match std::str::from_utf8(content) {
                                Ok(text) => {
                                    let mut cut = text.len().min(usize::try_from(max).expect("a u32 fits"));
                                    while !text.is_char_boundary(cut) {
                                        cut -= 1;
                                    }
                                    run::Read::Text { text: text.as_bytes()[..cut].into(), whole: cut == text.len() }
                                }
                                Err(_) => run::Read::NotText,
                            },
                            None => run::Read::Missing,
                        };
                        (at_time, read)
                    }
                    None => (deadline, run::Read::Failed),
                };
                self.stats.reads += 1;
                self.schedule(at_time, Delivery::Io(run::Event::Read { owner, read }));
            }
            run::Request::Probe { owner, at, deadline } => {
                let (at_time, executable) = if let Some(at_time) = self.io_result(owner, deadline) {
                    (at_time, self.executables.contains(&(at.root, at.path.to_vec())))
                } else {
                    // The run cannot know of checks io failed to find.
                    self.views.get_mut(&owner).expect("a run looks once admitted").checks.remove(&at.root);
                    (deadline, false)
                };
                self.stats.probes += 1;
                self.schedule(at_time, Delivery::Io(run::Event::Probed { owner, executable }));
            }
            run::Request::Abort { owner } => {
                self.stats.aborts += 1;
                // A check whose result is on its way already won the race.
                if let Some(key) = self.checks.get(&owner).copied()
                    && !self.rng.chance(self.settings.races)
                {
                    self.wire.withdraw(key).expect("a check in flight has its result on the wire");
                    self.checks.remove(&owner);
                    let at = self.now.saturating_add(self.draw(self.settings.checkout.io));
                    self.schedule(at, Delivery::Io(run::Event::Aborted { owner }));
                }
            }
            run::Request::HostCall { .. }
            | run::Request::WithdrawHost { .. }
            | run::Request::Turn { .. }
            | run::Request::MessageRefused { .. }
            | run::Request::Waiting { .. }
            | run::Request::Admitted { .. }
            | run::Request::Answer { .. }
            | run::Request::Open { .. }
            | run::Request::Say { .. }
            | run::Request::CancelJobs { .. }
            | run::Request::Yield { .. }
            | run::Request::Close { .. }
            | run::Request::Return { .. }
            | run::Request::Check { .. }
            | run::Request::Checking { .. }
            | run::Request::ChecksEnded { .. }
            | run::Request::CompletionReady { .. }
            | run::Request::Deliver { .. } => unreachable!("not one of io's requests"),
        }
    }

    /// The run an event is about, if the world knows it yet.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn run_of(&self, event: &run::Event) -> Option<Token> {
        match event {
            run::Event::HostReturned { relay, .. } => self.relay_owner.get(&relay.owner).copied(),
            run::Event::Start { .. } => None,
            run::Event::Cancel { run } | run::Event::Acknowledge { run, .. } | run::Event::Message { run, .. } => {
                Some(*run)
            }
            run::Event::Read { owner, .. } | run::Event::Probed { owner, .. } => Some(*owner),
            run::Event::JobsCancelled { conversation }
            | run::Event::Started { conversation, .. }
            | run::Event::Turn { conversation, .. }
            | run::Event::Yielded { conversation, .. }
            | run::Event::Priced { conversation, .. }
            | run::Event::Used { conversation, .. }
            | run::Event::Ended { conversation, .. }
            | run::Event::Delegated { conversation, .. }
            | run::Event::Withdraw { conversation, .. } => self.run_of_conversation.get(conversation).copied(),
            run::Event::Checked { owner, .. } | run::Event::Aborted { owner } | run::Event::Delivered { owner, .. } => {
                self.run_of_call.get(owner).copied()
            }
        }
    }

    /// What an event the run is about to take tells the world of it; for a
    /// cancel, the state it finds the run in.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn note(&mut self, event: &run::Event, run: Option<Token>) -> Option<(Token, &'static str)> {
        let owner = run?;
        if let run::Event::Used { conversation, .. } | run::Event::Ended { conversation, .. } = event
            && self.reservations.remove(conversation)
        {
            let charge = if let run::Event::Used { spend, .. } = event { spend.units } else { 0 };
            assert!(
                run::settle_reservation(&mut self.run, *conversation, *conversation, charge),
                "the scripted provider charges within its actual reserved maximum"
            );
        }
        let view = self.views.get_mut(&owner)?;
        match event {
            run::Event::Started { conversation, peer } => {
                self.conversation_of_peer.insert(*peer, *conversation);
                view.started = true;
            }
            run::Event::Used { conversation, spend, .. } => {
                let opened = self.opens.get_mut(conversation).expect("actual opened conversation");
                opened.spent = opened.spent.accumulate(*spend).expect("finite own child usage");
                view.spent = view.spent.accumulate(*spend).expect("bounded scripted usage");
                self.charges_seen.push((
                    self.now,
                    owner,
                    crate::facts_referee::Charged {
                        spent: view.spent,
                        budget: view.budget,
                        reserve: view.reserve,
                        reserved_spend: view.reserved_spend,
                        time: self.now.saturating_since(view.admitted_at),
                    },
                ));
            }
            run::Event::Checked { owner, .. } | run::Event::Aborted { owner } | run::Event::Delivered { owner, .. } => {
                self.landing.remove(owner);
            }
            run::Event::Cancel { run } => {
                let cell = self.cell(*run);
                let view = self.views.get_mut(run).expect("looked up above");
                if matches!(cell, "awaiting" | "preparing" | "opening" | "working" | "landing" | "over") {
                    view.decided = true;
                }
                return Some((*run, cell));
            }
            run::Event::Ended { conversation, spend, .. } => {
                let opened = self.opens.get_mut(conversation).expect("actual opened conversation");
                opened.ended_at = Some(self.now);
                opened.spent.turns = opened.spent.turns.max(spend.turns);
            }
            run::Event::Priced { .. }
            | run::Event::HostReturned { .. }
            | run::Event::Start { .. }
            | run::Event::Message { .. }
            | run::Event::Acknowledge { .. }
            | run::Event::Turn { .. }
            | run::Event::Yielded { .. }
            | run::Event::Delegated { .. }
            | run::Event::Withdraw { .. }
            | run::Event::Read { .. }
            | run::Event::Probed { .. }
            | run::Event::JobsCancelled { .. } => {}
        }
        None
    }

    /// The state the world sees the run `run` in.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn cell(&self, run: Token) -> &'static str {
        let view = &self.views[&run];
        let landing = self.landing.iter().any(|owner| self.run_of_call.get(owner) == Some(&run));
        match view.answered {
            Some(at) if at < self.iteration => "gone",
            Some(_) => "answered",
            None if view.main.is_none() && view.decided => "stopping",
            None if view.main.is_none() && view.waiting => "awaiting",
            None if view.main.is_none() => "preparing",
            None if view.decided => "winding",
            None if !view.started => "opening",
            None if landing => "landing",
            None if self.facts_seen.iter().rev().any(|fact| {
                matches!(fact.kind,
                run::facts::FactKind::Budget { run: owner, standing, .. }
                    if owner == run && standing.stage == run::Stage::WindDown)
            }) =>
            {
                "wind-down"
            }
            None if self.relay_stage.contains_key(&run) => self.relay_stage[&run],
            None if view.waiting => "waiting",
            None => "working",
        }
    }

    /// The run whose deadline the run's alarm is about to fire, if one alone
    /// is due; its state, counted.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn deadline_due(&mut self) -> Option<Token> {
        let mut due =
            self.views.iter().filter(|(_, view)| view.answered.is_none() && !view.decided && view.deadline <= self.now);
        let (run, view) = due.next()?;
        let (run, deadline) = (*run, view.deadline);
        let tied = due.any(|(_, other)| other.deadline <= deadline);
        let earliest = self
            .views
            .iter()
            .all(|(other, view)| *other == run || view.answered.is_some() || view.decided || view.deadline > deadline);
        if tied || !earliest {
            return None;
        }
        let cell = self.cell(run);
        *self.deadline_cells.entry(cell).or_insert(0) += 1;
        self.views.get_mut(&run).expect("looked up above").decided = true;
        Some(run)
    }

    /// The run opens a conversation: main, or the sub-agent of the call the
    /// step took.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn open(&mut self, conversation: Token, opening: run::Opening, current: Option<Token>) {
        self.opening_prompts.push(opening.prompt.clone());
        let fresh = self
            .opens
            .insert(
                conversation,
                Open { started: false, ended: false, opened_at: self.now, ended_at: None, spent: run::Spend::ZERO },
            )
            .is_none();
        assert!(fresh, "conversations have distinct names");
        let run = current.expect("a run opens a conversation in a step about it");
        self.run_of_conversation.insert(conversation, run);
        // Checked is pushed: nothing opens while a change is checked or pushed.
        let landing = self.landing.iter().any(|owner| self.run_of_call.get(owner) == Some(&run));
        assert!(!landing, "a run opens no conversation while its change is checked or pushed");
        let view = self.views.get_mut(&run).expect("a run is admitted before it opens a conversation");
        view.live += 1;
        view.peak = view.peak.max(view.live);
        self.stats.peak = self.stats.peak.max(view.peak);
        match self.asked.take() {
            None => {
                assert!(view.main.is_none(), "a run opens main once, and sub-agents when asked");
                view.main = Some(conversation);
            }
            Some((call, left, asked, call_left)) => {
                // A sub-agent's share is within what its run had left as it
                // took the ask.
                let share = opening.budget;
                self.share_seen.push((
                    self.now,
                    crate::share_referee::Seen { ordinary: left, effective: share, asked, call_left },
                ));
                assert!(
                    share.turns <= left.turns && share.spend <= left.spend && share.time <= left.time,
                    "a sub-agent's share {share:?} is within what is left above the reserve, {left:?}"
                );
                assert!(!opening.finish, "a sub-agent may not finish");
                self.child_of_call.insert(call, conversation);
                let writing = opening.tools.modify || opening.tools.shell;
                self.lease_seen.push((self.now, run, crate::lease_referee::Seen::ChildOpened { call, writing }));
                self.stats.children += 1;
            }
        }
        self.stats.opens += 1;
        self.send(Lane::Conversations, Delivery::Open { conversation, opening });
    }

    /// Checked is pushed: from a run's first check to its push, nothing but
    /// main is live in it, so nothing else may write to the checkout.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn assert_alone(&self, run: Token) {
        let view = &self.views[&run];
        assert_eq!(view.live, 1, "main is its run's only conversation while its change is checked and pushed");
    }

    fn workspace_lease(&mut self, run: Token, owner: Token) {
        let mut pending = self.calls.iter().filter(|(_, call)| {
            call.workspace && !call.returned && self.run_of_conversation[&call.conversation] == run
        });
        let (&call, _) = pending.next().expect("a workspace operation serves a live main call");
        assert!(pending.next().is_none(), "one exclusive workspace call");
        match self.workspace_of_call.insert(call, owner) {
            Some(first) => assert_eq!(first, owner, "checks and submission keep the same owner"),
            None => self.lease_seen.push((self.now, run, crate::lease_referee::Seen::WorkspaceOpened { owner })),
        }
    }

    /// The run's answer, checked against its budget and what it did: the
    /// host's name for the run.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn answer(&mut self, to: ReplyTo, answer: run::Answer) -> Token {
        let owner = to.into_token();
        let start = self.starts.get_mut(&owner).expect("an answer is to a start that was made");
        assert!(start.answer.is_none(), "a start is answered once");
        let turns = match &answer {
            run::Answer::Accepted { turns, .. }
            | run::Answer::Failed { turns, .. }
            | run::Answer::Parked { turns, .. } => *turns,
            run::Answer::Refused(_) => 0,
        };
        let observed = self.run_of_owner.get(&owner).map_or(0, |run| self.views[run].turns);
        assert_eq!(turns, observed, "final count matches actual main outputs");
        let peak = self.run_of_owner.get(&owner).map_or(0, |run| self.views[run].peak);
        assert_within(&start.budget, &answer, self.partner.turn_max(), peak);
        // A final Change needs a settled push. A separate mid-run delivery
        // may already have pushed before another kind of final answer.
        let change = matches!(&answer, run::Answer::Accepted { outcome: run::outcome::Declared::Change(_), .. });
        assert!(!change || self.pushed.contains(&owner), "an accepted Change was pushed");
        // A run answers once its main conversation, if it opened one, and
        // every other conversation it opened have ended.
        if let Some(run) = self.run_of_owner.get(&owner) {
            for (conversation, of) in &self.run_of_conversation {
                assert!(of != run || self.opens[conversation].ended, "no conversation outlives its run");
            }
            self.lease_seen.push((self.now, *run, crate::lease_referee::Seen::Answer));
            self.wind_down_seen.push((
                self.now,
                *run,
                crate::wind_down_referee::Seen::Answer {
                    turns_failure: matches!(
                        answer,
                        run::Answer::Failed {
                            failure: run::Failure::Budget { exhausted: run::Exhausted::Turns, .. },
                            ..
                        }
                    ),
                    time_failure: matches!(
                        answer,
                        run::Answer::Failed {
                            failure: run::Failure::Budget { exhausted: run::Exhausted::Time, .. },
                            ..
                        }
                    ),
                    time_expired: self.now >= self.views[run].deadline,
                },
            ));
        }
        start.answer = Some(answer);
        self.send(Lane::Host, Delivery::Answered { host_run: owner });
        owner
    }

    /// Runs the checks at `program` as io would: they fail as many times as
    /// their repository was given, then pass, unless they outlast `deadline`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn check(&mut self, owner: Token, program: &run::Place, deadline: Time, tail: u32) {
        assert!(self.checking.insert(owner, program.root).is_none(), "a call has one check in flight at a time");
        assert!(self.executables.contains(&(program.root, program.path.to_vec())), "checks are run where found");
        self.stats.checks += 1;
        let at = self.now.saturating_add(self.draw(self.settings.checkout.check));
        let failures = self.failures.entry(program.root).or_insert(0);
        let (at, exit, written) = if at > deadline {
            (deadline, run::Exit::TimedOut, 100)
        } else if *failures > 0 {
            *failures -= 1;
            (at, run::Exit::Code { code: 1 }, self.rng.between(0, u64::from(tail) * 3))
        } else {
            (at, run::Exit::Code { code: 0 }, 20)
        };
        // io keeps the tail of what the checks wrote.
        let kept = written.min(u64::from(tail));
        let output =
            b"test parse_tabs ... FAILED\n".iter().copied().cycle().take(usize::try_from(kept).expect("small"));
        let ran = run::Ran { exit, output: output.collect(), cut: written - kept };
        let key = self.schedule(at, Delivery::Io(run::Event::Checked { owner, ran }));
        self.checks.insert(owner, key);
    }

    /// When io answers a read or a probe of `owner`'s due by `deadline`, or
    /// `None` if it fails or runs out of time.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn io_result(&mut self, owner: Token, deadline: Time) -> Option<Time> {
        assert!(self.looks.insert(owner), "a run has one look in flight at a time");
        let at = self.now.saturating_add(self.draw(self.settings.checkout.io));
        let failed = self.rng.chance(self.settings.checkout.io_failures);
        (!failed && at <= deadline).then_some(at)
    }

    /// Lays out what each repository of a checkout the host made holds, as
    /// io finds it.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn checkout(&mut self, directories: &[run::Directory]) {
        let settings = self.settings.checkout;
        for &run::Directory { root, .. } in directories {
            if self.rng.chance(settings.guides) {
                let len = usize::try_from(self.rng.between(1, u64::from(settings.guide_max))).expect("small");
                // Text with characters of more than one byte, so a cut may
                // fall inside one; or, now and then, not text at all.
                let mut guide = String::new();
                for c in "Keep changes small — and run the tests, où qu'ils soient. ".chars().cycle() {
                    if guide.len() + c.len_utf8() > len {
                        break;
                    }
                    guide.push(c);
                }
                let mut guide = guide.into_bytes();
                if self.rng.chance(settings.not_text) {
                    guide.insert(0, 0xff);
                }
                self.files.insert((root, b"AGENTS.md".to_vec()), guide);
            }
            if self.rng.chance(settings.checks) {
                self.executables.insert((root, b".temper/pre-pr".to_vec()));
                self.failures.insert(root, self.rng.between(0, u64::from(settings.check_failures)));
            }
        }
    }

    /// What the host asked for: events on their way to the agent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn host_out(&mut self, out: Vec<run::Event>) {
        for event in out {
            match event {
                run::Event::Start { reply_to, host_run, mut charter, workspace, .. } => {
                    if let Some(prices) = self.settings.partner.prices {
                        charter.llm.prices = prices;
                        for model in &mut charter.models {
                            model.prices = prices;
                        }
                    }
                    let input_rate = charter
                        .llm
                        .prices
                        .input
                        .max(charter.llm.prices.cached)
                        .max(charter.llm.prices.cache_write.unwrap_or(charter.llm.prices.input));
                    let numerator = u128::from(charter.llm.window) * u128::from(input_rate)
                        + u128::from(charter.llm.output) * u128::from(charter.llm.prices.output);
                    let unit = u128::from(charter.llm.prices.unit);
                    let maximum = numerator.div_ceil(unit);
                    let reserved_spend =
                        u64::try_from(maximum * u128::from(charter.reserve.turns)).expect("finite scripted reserve");
                    let start = Start {
                        reserved_spend,
                        budget: charter.budget,
                        reserve: charter.reserve,
                        checks: BTreeSet::new(),
                        answer: None,
                    };
                    assert!(self.starts.insert(host_run, start).is_none(), "jobs have distinct names");
                    self.stats.starts += 1;
                    self.send(Lane::Agent, Delivery::Start { reply_to, host_run, charter, workspace });
                }
                event @ run::Event::Message { .. } => self.send(Lane::Agent, Delivery::Host(event)),
                run::Event::Cancel { run } => {
                    self.stats.cancels += 1;
                    self.send(Lane::Agent, Delivery::Cancel { run });
                }
                run::Event::Delivered { owner, delivery: push } => {
                    let Pushing { job } = self.pushes.end(owner);
                    if matches!(push, run::Delivery::Delivered(_)) {
                        self.pushed.insert(job);
                    }
                    self.send(Lane::Agent, Delivery::Host(run::Event::Delivered { owner, delivery: push }));
                }
                run::Event::HostReturned { .. }
                | run::Event::Turn { .. }
                | run::Event::Acknowledge { .. }
                | run::Event::JobsCancelled { .. }
                | run::Event::Started { .. }
                | run::Event::Yielded { .. }
                | run::Event::Priced { .. }
                | run::Event::Used { .. }
                | run::Event::Ended { .. }
                | run::Event::Read { .. }
                | run::Event::Probed { .. }
                | run::Event::Delegated { .. }
                | run::Event::Withdraw { .. }
                | run::Event::Checked { .. }
                | run::Event::Aborted { .. } => unreachable!("the host starts and cancels runs, and ends their pushes"),
            }
        }
    }

    /// Hands every delivery that is due to its destination.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[expect(clippy::too_many_lines, reason = "the scripted world's finite delivery routing stays in one match")]
    fn deliver(&mut self) {
        while let Some(delivery) = self.wire.next(self.now) {
            match delivery {
                Delivery::Start { reply_to, host_run, charter, workspace } => {
                    self.start_delivery(reply_to, host_run, charter, workspace);
                }
                Delivery::Cancel { run } => self.run_stage.push(run::Event::Cancel { run }),
                Delivery::Host(event) => {
                    if let run::Event::HostReturned { relay, reply } = &event {
                        assert!(self.relays.remove(&(relay.owner, relay.attempt)), "relay returns once");
                        let run = self.relay_owner[&relay.owner];
                        match reply {
                            run::HostReply::Answered(_) | run::HostReply::TooLarge => {
                                self.stats.relay_answered += 1;
                                self.relay_stage.remove(&run);
                            }
                            run::HostReply::Busy => {
                                self.stats.relay_busy += 1;
                                self.relay_stage.insert(run, "relay backoff");
                            }
                            run::HostReply::Unanswered(_) | run::HostReply::Withdrawn => {
                                self.stats.relay_lost += 1;
                                self.relay_stage.insert(run, "relay backoff");
                            }
                        }
                    }
                    self.hand(event);
                }
                Delivery::Admitted { host_run, run } => self.host.admitted(self.now, host_run, run),
                Delivery::Answered { host_run } => self.host.answered(self.now, host_run),
                Delivery::Checking { host_run } => self.host.checking(host_run),
                Delivery::ChecksEnded { host_run } => self.host.checks_ended(host_run),
                Delivery::Submit { host_run, owner, deadline } => self.host.push(self.now, host_run, owner, deadline),
                Delivery::Open { conversation, opening } => {
                    let mut out = Vec::new();
                    self.partner.open(self.now, conversation, &opening, &mut out);
                    self.partner_out(out);
                }
                Delivery::Say { peer } => {
                    let mut out = Vec::new();
                    self.partner.say(peer, &mut out);
                    self.partner_out(out);
                }
                Delivery::Close { peer } => {
                    let mut out = Vec::new();
                    self.partner.close(self.now, peer, &mut out);
                    self.partner_out(out);
                }
                Delivery::Return { call, result, spent } => {
                    let mut out = Vec::new();
                    self.partner.returned(self.now, call, &result, spent, &mut out);
                    self.partner_out(out);
                }
                Delivery::Wake { peer, wake } => {
                    let mut out = Vec::new();
                    self.partner.woken(self.now, peer, wake, &mut out);
                    self.partner_out(out);
                }
                Delivery::Permit { peer, conversation } => self.permits.push((peer, conversation)),
                Delivery::CompletionUsage { conversation, own_spent, subtree_spent, usage } => {
                    self.completion_usage(conversation, own_spent, subtree_spent, usage);
                }
                Delivery::Event(event) => {
                    if let run::Event::Turn { conversation, .. } = &event {
                        let owner = self.run_of_conversation[conversation];
                        if self.views[&owner].main != Some(*conversation) {
                            continue;
                        }
                    }
                    self.check_conversation(&event);
                    self.hand(event);
                }
                Delivery::Io(event) => {
                    let answered = match &event {
                        run::Event::Read { owner, .. } | run::Event::Probed { owner, .. } => self.looks.remove(owner),
                        run::Event::Checked { owner, ran } => {
                            self.checks.remove(owner);
                            let root = self.checking.remove(owner);
                            if let Some(root) = root
                                && matches!(ran.exit, run::Exit::Code { code: 0 })
                            {
                                self.passed.entry(*owner).or_default().insert(root);
                            }
                            root.is_some()
                        }
                        run::Event::Aborted { owner } => {
                            self.checks.remove(owner);
                            self.checking.remove(owner).is_some()
                        }
                        run::Event::HostReturned { .. }
                        | run::Event::Message { .. }
                        | run::Event::Acknowledge { .. }
                        | run::Event::Turn { .. }
                        | run::Event::Start { .. }
                        | run::Event::Cancel { .. }
                        | run::Event::JobsCancelled { .. }
                        | run::Event::Started { .. }
                        | run::Event::Yielded { .. }
                        | run::Event::Priced { .. }
                        | run::Event::Used { .. }
                        | run::Event::Ended { .. }
                        | run::Event::Delegated { .. }
                        | run::Event::Withdraw { .. }
                        | run::Event::Delivered { .. } => unreachable!("io answers reads, probes and checks"),
                    };
                    assert!(answered, "io answers each operation once");
                    self.hand(event);
                }
            }
        }
    }

    /// Discover actual checkout checks before delivering the original host Start.
    /// Caller-selected Charter and Workspace remain owning boundary values.
    /// Scripted-world contract: domain/run.md, sections 3 and 13.
    fn start_delivery(
        &mut self,
        reply_to: ReplyTo,
        host_run: Token,
        mut charter: run::Charter,
        workspace: Option<run::Workspace>,
    ) {
        if let Some(wait) = self.empty_brief_wait {
            charter.brief.sections = Box::default();
            charter.grants.wait = wait;
        }
        if let Some(waiting) = self.waiting_time {
            charter.waiting = waiting;
        }
        self.checkout(workspace.as_ref().map_or(&[][..], |workspace| &workspace.directories));
        let wants = charter.outcome.change.is_some() || charter.grants.deliver.is_some();
        let mut checks = BTreeSet::new();
        for repository in workspace.as_ref().map_or(&[][..], |workspace| workspace.directories.as_ref()) {
            let executable = (repository.root, b".temper/pre-pr".to_vec());
            if wants && repository.writable && self.executables.contains(&executable) {
                checks.insert(repository.root);
            }
        }
        self.starts.get_mut(&host_run).expect("a start is tracked").checks = checks;
        self.run_stage.push(run::Event::Start {
            messages: self.carried_messages.clone(),
            reply_to,
            host_run,
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter,
            workspace,
            transcript: None,
            resumed: false,
        });
    }

    /// Reconstruct one accepted completion's exact accounting notices in order.
    /// Both share the original callback's one cancellation-injection draw.
    /// Scripted-world contract: domain/run.md, sections 9 and 13.
    fn completion_usage(&mut self, conversation: Token, own_spent: u64, subtree_spent: u64, usage: run::Spend) {
        let price = run::Event::Priced { conversation, own_spent, subtree_spent };
        self.check_conversation(&price);
        let used = run::Event::Used { conversation, spend: usage };
        self.check_conversation(&used);
        self.hand_callback(Some(price), used);
    }

    /// A fact the run told: of a run it admitted, and a conversation it
    /// opened.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn fact(&mut self, fact: &run::facts::Fact) {
        use run::facts::FactKind as Fact;
        self.facts_seen.push(*fact);
        let fact = &fact.kind;
        let (Fact::Admitted { run, .. }
        | Fact::MessageRead { run, .. }
        | Fact::MessageFence { run, .. }
        | Fact::MessageUnread { run, .. }
        | Fact::MessageReceived { run, .. }
        | Fact::MessageRefused { run, .. }
        | Fact::Budget { run, .. }
        | Fact::Prepared { run, .. }
        | Fact::Opened { run, .. }
        | Fact::Ended { run, .. }
        | Fact::Called { run, .. }
        | Fact::Returned { run, .. }
        | Fact::CheckStarted { run, .. }
        | Fact::CheckFinished { run, .. }
        | Fact::Delivered { run, .. }
        | Fact::Answered { run, .. }) = fact;
        assert!(self.views.contains_key(run), "a fact is of a run that was admitted");
        if let Fact::Opened { conversation, .. } | Fact::Ended { conversation, .. } = fact {
            assert!(self.opens.contains_key(conversation), "a fact is of a conversation that was opened");
        }
        let message = match fact {
            Fact::MessageReceived { name, .. } => Some(crate::messages_referee::Seen::Received { name: *name }),
            Fact::MessageRead { name, .. } => Some(crate::messages_referee::Seen::Read { name: *name }),
            Fact::MessageUnread { name, .. } => Some(crate::messages_referee::Seen::Unread { name: *name }),
            Fact::MessageFence { read, .. } => Some(crate::messages_referee::Seen::Fence { read: *read }),
            Fact::MessageRefused { .. }
            | Fact::Admitted { .. }
            | Fact::Budget { .. }
            | Fact::Prepared { .. }
            | Fact::Opened { .. }
            | Fact::Ended { .. }
            | Fact::Called { .. }
            | Fact::Returned { .. }
            | Fact::CheckStarted { .. }
            | Fact::CheckFinished { .. }
            | Fact::Delivered { .. }
            | Fact::Answered { .. } => None,
        };
        if let Some(message) = message {
            self.message_seen.push((self.now, *run, message));
        }
        *self.facts.entry(kind(fact)).or_insert(0) += 1;
    }

    /// Every actual fact the run told, counted by kind.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    #[must_use]
    pub fn facts(&self) -> &BTreeMap<&'static str, u32> {
        &self.facts
    }

    /// Hands the run `event`, and with the configured chance, the host's
    /// cancel of its run right before or right after it.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn hand(&mut self, event: run::Event) {
        self.hand_callback(None, event);
    }

    /// One physical callback owns one before/after cancellation injection.
    /// A completion's price precedes its raw usage without an invented race.
    /// Scripted-world contract: domain/run.md, sections 9 and 13.
    fn hand_callback(&mut self, price: Option<run::Event>, event: run::Event) {
        let run = self.run_of(&event);
        let inject = match run {
            Some(_) => self.rng.chance(self.settings.inject),
            None => false,
        };
        let before = inject && self.rng.chance(500);
        let message_run = run
            .filter(|run| self.views[run].answered.is_none())
            .filter(|_| self.message_chance > 0 && self.rng.chance(self.message_chance));
        let message_before = message_run.is_some() && self.rng.chance(500);
        if let Some(run) = message_run.filter(|_| message_before) {
            self.person_message(run);
        }
        if let Some(run) = run.filter(|_| before) {
            self.run_stage.push(run::Event::Cancel { run });
        }
        if let Some(price) = price {
            self.run_stage.push(price);
        }
        self.run_stage.push(event);
        if let Some(run) = message_run.filter(|_| !message_before) {
            self.person_message(run);
        }
        if let Some(run) = run.filter(|_| inject && !before) {
            self.run_stage.push(run::Event::Cancel { run });
        }
    }

    fn person_message(&mut self, run: Token) {
        *self.message_cells.entry(self.cell(run)).or_insert(0) += 1;
        // Deliberately opaque, decreasing names include zero; arrival order is external.
        let name = Token::new(u64::MAX.wrapping_sub(self.message_name));
        self.message_name += 1;
        self.run_stage.push(run::Event::Message {
            finish_requested: false,
            run,
            name,
            label: b"person".as_slice().into(),
            text: b"follow up".as_slice().into(),
        });
    }

    /// What the partner asked for: events on their way to the run, and wakes.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn partner_out(&mut self, out: Vec<Out>) {
        let mut items = out.into_iter().peekable();
        while let Some(item) = items.next() {
            match item {
                Out::Event(run::Event::Priced { conversation, own_spent, subtree_spent })
                    if matches!(items.peek(), Some(Out::Event(run::Event::Used { .. }))) =>
                {
                    let Some(Out::Event(run::Event::Used { conversation: used, spend: usage })) = items.next() else {
                        panic!("checked adjacent raw usage from the same actual callback")
                    };
                    assert_eq!(conversation, used, "one completion callback owns both notices");
                    self.send(Lane::Run, Delivery::CompletionUsage { conversation, own_spent, subtree_spent, usage });
                }
                Out::Permit { peer, conversation } => {
                    // Preserve the Run lane's order without inventing another
                    // randomized protocol hop for this local gate query.
                    let at = self.now.max(self.lanes[Lane::Run as usize]);
                    self.schedule(at, Delivery::Permit { peer, conversation });
                }
                Out::Event(event) => self.send(Lane::Run, Delivery::Event(event)),
                Out::Wake { at, peer, wake } => {
                    self.schedule(at, Delivery::Wake { peer, wake });
                }
            }
        }
    }

    /// The conversations' contract, as the run receives it: `Started` at most
    /// once and first, one `Ended` per open once every call it made has
    /// returned, nothing after it; a withdraw only of a call it made.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn check_conversation(&mut self, event: &run::Event) {
        let (conversation, started, ended) = match event {
            run::Event::Started { conversation, .. } => (conversation, true, false),
            run::Event::Ended { conversation, .. } => (conversation, false, true),
            run::Event::JobsCancelled { conversation }
            | run::Event::Yielded { conversation, .. }
            | run::Event::Turn { conversation, .. }
            | run::Event::Priced { conversation, .. }
            | run::Event::Used { conversation, .. } => (conversation, false, false),
            run::Event::Delegated { conversation, call, ask, .. } => {
                let fresh = self
                    .calls
                    .insert(
                        *call,
                        Call {
                            conversation: *conversation,
                            wait: matches!(ask, run::Ask::Wait),
                            workspace: matches!(
                                ask,
                                run::Ask::Deliver { .. }
                                    | run::Ask::Finish { outcome: run::outcome::Declared::Change(_) }
                            ),
                            returned: false,
                        },
                    )
                    .is_none();
                assert!(fresh, "calls have distinct names");
                (conversation, false, false)
            }
            run::Event::Withdraw { conversation, call } => {
                let ledger = self.calls.get(call).expect("a withdraw is of a call that was made");
                assert_eq!(ledger.conversation, *conversation, "a conversation withdraws its own calls");
                (conversation, false, false)
            }
            run::Event::HostReturned { .. }
            | run::Event::Start { .. }
            | run::Event::Message { .. }
            | run::Event::Acknowledge { .. }
            | run::Event::Cancel { .. }
            | run::Event::Read { .. }
            | run::Event::Probed { .. }
            | run::Event::Checked { .. }
            | run::Event::Aborted { .. }
            | run::Event::Delivered { .. } => unreachable!("not a conversation's event"),
        };
        let open = self.opens.get_mut(conversation).expect("events are about conversations the run opened");
        assert!(!open.ended, "nothing comes after a conversation's end");
        if started {
            assert!(!open.started, "a conversation starts once");
            open.started = true;
        }
        if !started && !ended {
            assert!(open.started, "a conversation starts before anything else");
        }
        open.ended = ended;
        if ended {
            let run = self.run_of_conversation[conversation];
            self.views.get_mut(&run).expect("a run outlives its conversations").live -= 1;
            for ledger in self.calls.values() {
                assert!(
                    ledger.conversation != *conversation || ledger.returned,
                    "a conversation ends once its calls have returned"
                );
            }
        }
    }

    fn has_work_now(&self) -> bool {
        !self.permits.is_empty()
            || self.run_stage.has_events()
            || self.run.is_ready()
            || self.run.is_due(self.now)
            || self.host.is_due(self.now)
            || self.wire.is_due(self.now)
    }

    fn next_time(&self) -> Option<Time> {
        [self.wire.next_time(), self.run.next_deadline(), self.host.next_deadline()].into_iter().flatten().min()
    }

    /// The invariants of a world where nothing is left to happen.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn assert_settled(&self) {
        assert!(
            crate::wind_down_referee::judge(&self.wind_down_seen)
                .iter()
                .all(|verdict| *verdict == skein_world::domain::Verdict::Passed),
            "final requests follow the independent admitted reserve: {:?}",
            crate::wind_down_referee::judge(&self.wind_down_seen)
        );
        assert_eq!(crate::share_referee::judge(&self.share_seen), skein_world::domain::Verdict::Passed);
        assert!(self.reservations.is_empty(), "every pending provider turn settled");
        assert!(
            self.views.values().all(|view| !view.budget_spent()),
            "honest reserved callbacks never cross a scalar ceiling"
        );
        assert!(
            crate::lease_referee::judge(&self.lease_seen)
                .iter()
                .all(|verdict| *verdict == skein_world::domain::Verdict::Passed),
            "lease observations follow the external write-holder contract"
        );
        assert!(self.workspace_of_call.is_empty(), "every workspace holder's call returned");
        assert_eq!(self.run.runs(), 0, "every run has answered and been reclaimed");
        assert_eq!(self.run.conversations(), 0, "every conversation has ended and been reclaimed");
        assert_eq!(self.run.calls(), 0, "every call has returned and been reclaimed");
        assert_eq!(self.run.next_deadline(), None, "no alarm outlives its run");
        self.host.assert_settled();
        assert_eq!(self.partner.live(), 0, "every conversation has ended");
        assert!(
            self.looks.is_empty() && self.checking.is_empty() && self.checks.is_empty(),
            "io answered every operation"
        );
        assert!(self.pushes.is_empty(), "every push was answered");
        assert!(self.relays.is_empty(), "every host relay was answered");
        assert!(self.passed.is_empty(), "every landing call returned");
        assert!(self.child_of_call.is_empty(), "every sub-agent's call returned");
        assert!(self.wire.is_empty() && !self.run_stage.has_events(), "nothing is on its way");
        let mut answered = run::Spend::ZERO;
        for (owner, start) in &self.starts {
            let answer = start.answer.as_ref().unwrap_or_else(|| panic!("start {owner:?} was answered"));
            match answer {
                run::Answer::Parked { spent, .. }
                | run::Answer::Failed { spent, .. }
                | run::Answer::Accepted { spent, .. } => {
                    answered = answered.accumulate(*spent).expect("bounded scripted usage");
                }
                run::Answer::Refused(_) => {}
            }
        }
        for (conversation, open) in &self.opens {
            assert!(open.ended, "conversation {conversation:?} ended");
        }
        for (call, ledger) in &self.calls {
            assert!(ledger.returned, "call {call:?} returned");
        }
        assert_eq!(answered, self.partner.spent(), "every turn spent is in exactly one answer");
    }

    /// Sends on `lane`, after its latency, and after whatever it carries
    /// already.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn send(&mut self, lane: Lane, delivery: Delivery) {
        let span = match lane {
            Lane::Agent | Lane::Host => self.settings.network,
            Lane::Conversations | Lane::Run => self.settings.hop,
        };
        let latency = self.draw(span);
        let last = &mut self.lanes[lane as usize];
        let at = self.now.saturating_add(latency).max(*last);
        *last = at;
        self.schedule(at, delivery);
    }

    fn schedule(&mut self, at: Time, delivery: Delivery) -> Key {
        self.wire.send(at, delivery)
    }

    fn draw(&mut self, span: Span) -> Duration {
        span.draw(&mut self.rng)
    }

    fn log(&mut self, line: &str) {
        self.trace.log(self.now, line);
    }
}

/// A fact's kind, to count it by.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
fn kind(fact: &run::facts::FactKind) -> &'static str {
    use run::facts::FactKind as Fact;
    match fact {
        Fact::MessageRead { .. } => "message_read",
        Fact::MessageFence { .. } => "message_fence",
        Fact::MessageUnread { .. } => "message_unread",
        Fact::MessageReceived { .. } => "message_received",
        Fact::MessageRefused { .. } => "message_refused",
        Fact::Admitted { .. } => "admitted",
        Fact::Budget { .. } => "budget",
        Fact::Prepared { .. } => "prepared",
        Fact::Opened { .. } => "opened",
        Fact::Ended { .. } => "ended",
        Fact::Called { .. } => "called",
        Fact::Returned { .. } => "returned",
        Fact::CheckStarted { .. } => "check started",
        Fact::CheckFinished { .. } => "check finished",
        Fact::Delivered { .. } => "pushed",
        Fact::Answered { .. } => "answered",
    }
}

impl RunView {
    /// What the run has left of its budget at `now`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn budget_left(&self, now: Time) -> run::Budget {
        let (spent, budget) = (self.spent, self.budget);
        run::Budget {
            turns: budget.turns.saturating_sub(self.reserve.turns).saturating_sub(spent.turns),
            spend: budget.spend.saturating_sub(spent.units).saturating_sub(self.reserved_spend),
            time: Duration::from_nanos(
                self.deadline.saturating_since(now).as_nanos().saturating_sub(self.reserve.time.as_nanos()),
            ),
        }
    }

    /// Whether its conversations spent past any part of its budget.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
    fn budget_spent(&self) -> bool {
        let (spent, budget) = (self.spent, self.budget);
        spent.turns > budget.turns || spent.units > budget.spend
    }
}

/// A run spends within its budget plus the existing conservative completion
/// slack: one turn per conversation live at the peak, and two. The pure run
/// gate denies every next unsent completion once global turns or scalar units
/// reach the ceiling. Already-admitted completions still report exact usage,
/// settle their calls and may win a race with time or cancellation. The bound
/// retains its original slack independently of those scheduling choices.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 2.2.
fn assert_within(budget: &run::Budget, answer: &run::Answer, turn: run::Spend, peak: u32) {
    let (run::Answer::Parked { spent, .. } | run::Answer::Failed { spent, .. } | run::Answer::Accepted { spent, .. }) =
        answer
    else {
        return;
    };
    let turns = u64::from(peak) + 2;
    let over = |spent: u64, budget: u64, turn: u64| spent <= budget.saturating_add(turn.saturating_mul(turns));
    assert!(
        over(u64::from(spent.turns), u64::from(budget.turns), u64::from(turn.turns))
            && over(spent.units, budget.spend, turn.units),
        "{spent:?} is within {budget:?} and {turns} turns of at most {turn:?} between them"
    );
}
