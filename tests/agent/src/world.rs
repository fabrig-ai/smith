//! Scripted agent domain world (domain/run.md, sections 13 and 14).
//! It keeps schedules, pending boundary requests, checkout bytes and observations.
//! It never knows provider grammar or transport bytes. Entry points start a
//! charter, drive the typed fake LLM, inject parent input, and return host
//! terminals. Only public counters are read for final quiescence checks.

mod jobs;

use std::collections::{BTreeMap, BTreeSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use skein_fake_checkout::Checkout;
use skein_fake_llm_domain as provider;
use skein_lib::{Duration, ReplyTo, Rng, Time, Token, Wall};
use skein_world::domain::{Key, Ledger, Referee, Schedule, Span, Stage, Trace};
use smith_domain::session::llm::Block as SavedBlock;
use smith_domain::{self as agent, Event, Fact, Grant, GrantName, Limits, Request, llm, run, tools};
use smith_tools_world::translate as io;

use crate::{
    BUDGET, LIMITS, fixture,
    referee::{Meeting, Seen},
    script::{self, Job},
    translate,
};

/// Scripted host choice; materialized as a sealed actual terminal at submission.
/// It is scenario data, not a second protocol or delivery implementation.
/// Contract: domain/run.md, sections 8.2 and 13; testing-strategy.md, section 2.2.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "scenario choices retain the sealed fixed diagnostic inline and stay Copy"
)]
pub enum HostReply {
    /// Real changed-directory receipt. Contract: domain/run.md, section 8.2.
    Delivered,
    /// Maximum sealed non-UTF8 receipt, supplied as an actual successful terminal.
    /// Contract: domain/run.md, sections 8.2 and 13; testing-strategy.md, section 7.
    OpaqueDelivered,
    /// No changed directory. Contract: domain/run.md, section 8.2.
    Nothing,
    /// Named conflict marker. Contract: domain/run.md, sections 8.1 and 8.2.
    Refused,
    /// Fixed generic failed operation. Contract: domain/run.md, section 8.2.
    Failed(
        /// Sealed fixed-size reason and diagnostic emitted at the actual host
        /// deadline. Contract: domain/run.md, section 8.2.
        run::DeliveryFailure,
    ),
    /// Moved host context. Contract: domain/run.md, section 8.2.
    Stale,
}

/// One sealed scripted-host receipt, used only by boundary fixtures.
/// Its opaque text has no domain interpretation; mount zero is writable.
/// Contract: domain/run.md, sections 8.2 and 13; testing-strategy.md, section 2.2.
#[must_use]
pub fn delivered() -> run::Delivery {
    run::Delivery::Delivered(
        run::Delivered::new(Box::new([
            run::Receipt::new(0, b"scripted receipt".as_slice().into()).expect("bounded receipt")
        ]))
        .expect("one unique receipt"),
    )
}

impl HostReply {
    fn terminal(self) -> run::Delivery {
        match self {
            Self::Delivered => delivered(),
            Self::OpaqueDelivered => run::Delivery::Delivered(
                run::Delivered::new(Box::new([run::Receipt::new(0, vec![0xff; run::Receipt::CAPACITY].into())
                    .expect("maximum sealed opaque receipt")]))
                .expect("one actual changed directory"),
            ),
            Self::Nothing => run::Delivery::Nothing,
            Self::Refused => run::Delivery::Refused(
                run::DeliveryRefusal::new(
                    Some(run::Marker::new(0, b"src/answer.rs".as_slice().into()).expect("relative marker")),
                    b"conflict markers remain".as_slice().into(),
                )
                .expect("bounded named refusal"),
            ),
            Self::Failed(failure) => run::Delivery::Failed(failure),
            Self::Stale => run::Delivery::Stale,
        }
    }
}

/// Immutable deterministic scenario inputs for the composed agent on a
/// scripted typed host; terminal choices are sealed only at actual submission.
/// Contract: domain/run.md, sections 8 and 13; testing-strategy.md, section 7.
#[derive(Clone, Copy, Debug)]
pub struct Settings {
    /// Usable context window shared by the model declaration and charter.
    pub window: u32,
    /// Host selects supplied history explicitly; false starts fresh even if present.
    /// Contract: domain/run.md, sections 3 and 13.
    pub resume: bool,
    /// Positive idle threshold; independent run wall time never pauses.
    /// Contract: domain/run.md, sections 6 and 10.
    pub waiting: Duration,

    /// Generic-host recovery schedule. Contract: domain/run.md, sections 5.2 and 13.
    pub host: HostSchedule,
    /// Replay seed for the host, agent and provider, independently derived.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub seed: u64,
    /// Script selected by the charter's brief.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub job: Job,
    /// The agent's immutable admission and ownership limits.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub limits: Limits,
    /// The run's token and wall-time allowance, shared across conversations.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub budget: run::Budget,
    /// Whether the scripted host grants checkout modification.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub writable: bool,
    /// The fake provider's latency, failures and ownership limits.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub provider: provider::Config,
    /// Latency of host and IO terminals; zero is permitted.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub network: Span,
    /// Time spent by the fixture's checks, capped by their request deadline.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub check: Span,
    /// The host's reply to each push; refusal feedback remains typed and bounded.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub push: HostReply,
    /// Explicit host cancellation time; `None` sends no cancel.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub cancel_at: Option<Duration>,
    /// A cancel loses to its existing terminal with this chance per mille.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub races: u32,
    /// Whether the scripted parent retains drained facts; false drains without a sink.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub drain_facts: bool,
}

impl Settings {
    /// A coding charter, ample bounds and a successful typed host, from `seed`.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn calm(seed: u64) -> Settings {
        let provider = smith_session_world::Settings::calm(seed).provider;
        Settings {
            window: 65_536,
            resume: false,
            waiting: Duration::from_secs(30),
            seed,
            job: Job::Coding,
            limits: LIMITS,
            budget: BUDGET,
            writable: true,
            provider,
            network: Span::millis(1, 20),
            check: Span::millis(100, 500),
            push: HostReply::Delivered,
            host: HostSchedule::Answer,
            cancel_at: None,
            races: 0,
            drain_facts: true,
        }
    }
}

/// Finite typed host choices; responses settle actual attempts before recovery.
/// Contract: domain/run.md, sections 5.2 and 13.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HostSchedule {
    /// Immediate first recorded answer. Contract: domain/run.md, section 5.2.
    Answer,
    /// One decided write answer exceeds the receiving cap.
    TooLarge,
    /// Busy, lost committed answer, then replay first recorded answer. Contract: domain/run.md, section 5.2.
    Replay,
    /// Wait beyond declared deadline, settle withdrawn, then recover. Contract: domain/run.md, section 5.2.
    Withdraw,
    /// Wait for withdrawal, but replay actual recorded answer. Contract: domain/run.md, section 5.2.
    LateAnswer,
}

/// Outside record of an actual root delivery handed to an opt-in parent bridge.
/// The World retains at most 256 bounded submission records. They expose no private state.
/// The existing flight ledger retains each operation until its actual terminal.
/// Contract: domain/run.md, sections 8.2, 10 and 14; domain/host.md, sections 2 and 9.
#[derive(Clone, Debug)]
pub struct DeliverySubmission {
    /// Original parent logical scope, echoed unchanged. Contract: domain/run.md, section 8.2.
    pub host_run: Token,

    /// Actual callback right, distinct from the durable name. Contract: domain/run.md, section 8.2.
    pub owner: Token,

    /// Actual transcript-derived operation identity. Contract: domain/run.md, section 8.2.
    pub name: run::CallName,

    /// Whole validated opaque metadata, bounded by root outcome limits.

    /// Contract: domain/run.md, sections 7.1 and 8.2; domain/host.md, section 2.
    pub change: run::outcome::Change,

    /// Actual operation deadline; withdrawal never consumes this right.

    /// Contract: domain/run.md, sections 8.2 and 10.
    pub deadline: Time,

    /// Observed root submission clock. Contract: domain/run.md, section 14.
    pub at: Time,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Family {
    Completion,
    Io,
    Read,
    Probe,
    Check,
    Delivery,
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "scheduled copied terminal records retain their fixed diagnostic tails without another allocation"
)]
enum Delivery {
    Message { finish_requested: bool, name: Token, label: Box<[u8]>, text: Box<[u8]> },
    Acknowledge { turn: u32 },
    Terminal { family: Family, owner: Token, event: Event },
    Io { owner: Token, op: tools::Op, deadline: Time },
    Command { owner: Token, process: skein_fake_checkout::Process, head: u32, tail: u32, timed_out: bool },
    Check { owner: Token, passed: bool, output: Vec<u8>, timed_out: bool, tail: u32 },
    Host { relay: run::RelayName, reply: run::HostReply },
    Job { owner: Token },
    Cancel,
    Grant { grant: Grant },
}

#[derive(Debug)]
struct Flight {
    key: Option<Key>,
    cancelled: bool,
    completion_message: Option<u32>,
    completion_index: Option<usize>,
}

#[derive(Debug)]
enum Backend {
    Typed {
        provider: provider::Domain,
        stage: Stage<provider::Config, provider::Event, provider::Request>,
        calls: Ledger<Token, (tools::Grants, Box<[llm::Served]>, llm::Asker)>,
    },
}

impl Backend {
    fn typed(settings: &Settings) -> Self {
        Self::Typed {
            provider: crate::scripted_provider(&settings.provider, settings.seed ^ 0x25),
            stage: Stage::new(settings.provider, provider::MAX_OUT, provider::MAX_OUT + 3),
            calls: Ledger::new("fake provider call"),
        }
    }

    fn has_events(&self) -> bool {
        match self {
            Self::Typed { stage, .. } => stage.has_events(),
        }
    }

    fn is_settled(&self) -> bool {
        match self {
            Self::Typed { provider, .. } => provider.calls() == 0,
        }
    }

    fn next_deadline(&self) -> Option<Time> {
        match self {
            Self::Typed { provider, .. } => provider.next_deadline(),
        }
    }
}

/// Actual lower requests observed without inspecting domain state.
/// Contract: domain/run.md, sections 3.2 and 14; testing-strategy.md, section 7.
#[derive(Clone, Debug)]
pub enum Boundary {
    /// Actual guide discovery. Contract: domain/run.md, section 3.3.
    Read {
        /// Accepted lower place. Contract: domain/run.md, section 3.3.
        at: run::Place,
    },
    /// Actual writable check discovery. Contract: domain/run.md, section 8.1.
    Probe {
        /// Accepted lower place. Contract: domain/run.md, section 8.1.
        at: run::Place,
    },
    /// Actual checked snapshot. Contract: domain/run.md, section 8.1.
    Check {
        /// Accepted lower program. Contract: domain/run.md, section 8.1.
        program: run::Place,
    },
    /// Actual session filesystem/program operation. Contract: domain/tools.md, section 2.
    Io {
        /// Existing tools boundary. Contract: domain/tools.md, section 2.
        op: tools::Op,
    },
}

/// One actual provider admission and terminal, observed outside accounting.
/// A finite turn/conversation/block/retry bound caps this fixture activation; model bytes are
/// copied from the original Complete, never inferred from a Turn.
/// Contract: domain/run.md, sections 9 and 14; testing-strategy.md, section 7.
#[derive(Clone, Debug)]
pub struct CompletionObservation {
    /// Actual callback owner. Contract: domain/run.md, section 14.
    pub owner: Token,

    /// Caller-selected model from actual Complete. Contract: domain/run.md, section 9.
    pub model: Box<[u8]>,

    /// Grant selected at actual dispatch, including its generation.
    pub grant: Option<GrantName>,

    /// Actual completion start clock. Contract: domain/run.md, section 10.
    pub started: Time,

    /// Actual terminal clock and SDK-shaped usage, or failure/cancellation.
    /// Pending is None; failure/cancellation has no invented usage.
    /// Contract: domain/run.md, sections 9 and 10.
    pub terminal: Option<(Time, CompletionTerminal)>,
}

/// Actual completion terminal class, independent of Session's price calculation.
/// Contract: domain/run.md, sections 9 and 10; testing-strategy.md, section 7.
#[derive(Clone, Copy, Debug)]
pub enum CompletionTerminal {
    /// All four exact accepted provider counters.
    /// Contract: domain/run.md, section 9; scratch/client.md, section 4.
    Completed(
        /// SDK usage shape copied at the terminal.
        /// Contract: scratch/client.md, section 4.
        llm::Usage,
    ),

    /// Genuine provider failure. Contract: scratch/client.md, section 4.
    Failed,

    /// Genuine settled cancellation. Contract: scratch/client.md, section 5.
    Cancelled,
}

/// The agent on a typed scripted host or actual wire backend. [`World::run`]
/// drives every boundary to settlement, then checks its ledgers, referee and facts.
/// The selected backend owns only its corresponding provider state and queues.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; scratch/client.md,
/// sections 1 and 5; testing-strategy.md, section 7.
#[derive(Debug)]
pub struct World {
    jobs: BTreeMap<Token, jobs::Held>,
    writing_opened: BTreeSet<u32>,
    settings: Settings,
    failure: Option<(agent::llm::Failure, agent::llm::Evidence)>,
    failure_report: agent::llm::Report,
    failure_count: Option<u32>,
    unauthorized: Vec<(Time, GrantName)>,
    now: Time,
    rng: Rng,
    agent: agent::Domain,
    stage: Stage<Limits, Event, Request>,
    backend: Backend,
    parent_deliveries: bool,
    parent_host_calls: bool,
    host_run: Token,
    reply_to: Token,
    delivery_submissions: Vec<DeliverySubmission>,
    schedule: Schedule<Delivery>,
    flights: Ledger<(Family, Token), Flight>,
    disk: Checkout,
    root: Option<u64>,
    boundaries: Vec<(Time, Boundary)>,
    admitted: Option<Token>,
    answer: Option<run::Answer>,
    answered: Option<Time>,
    checked: Vec<bool>,
    check_notices: Vec<(Time, bool)>,
    check_terminals: Vec<Time>,
    pushes: Vec<run::Delivery>,
    delivery_names: Vec<(run::CallName, Time)>,
    host_history: crate::host_referee::History,
    host_pending: BTreeMap<(u64, u32), Option<Key>>,
    host_decision: Option<run::HostAnswer>,
    host_decisions: u32,
    host_terminals: Vec<(run::RelayName, Time, run::HostReply)>,
    snapshots: BTreeMap<Token, Vec<u8>>,
    landed: Vec<u8>,
    prompts: Vec<provider::api::Query>,
    completions: Vec<CompletionObservation>,
    model_prices: BTreeMap<Box<[u8]>, run::Prices>,
    turns: Vec<agent::Turn>,
    messages_seen: Vec<(Time, crate::messages_referee::Seen)>,
    turn_metadata: Vec<(u32, Option<Token>, run::Spend)>,
    auto_ack: bool,
    waiting: Vec<(Time, Option<Token>)>,
    facts: Vec<Fact>,
    agent_facts_not_written: u64,
    trace: Trace,
    referee: Referee<Meeting>,
    stimuli: Vec<()>,
    terminals: u32,
}

fn choose_start(
    settings: &Settings,
    transcript: Option<agent::Transcript>,
    answered: Box<[agent::AnsweredCall]>,
    workspace: Option<run::Workspace>,
    selected_charter: Option<run::Charter>,
    selected_start: Option<Event>,
) -> (Event, Token) {
    match selected_start {
        Some(Event::Start {
            reply_to,
            host_run,
            activation,
            identity,
            window,
            messages,
            charter,
            workspace,
            grants,
            transcript,
            answered,
        }) => {
            let token = reply_to.into_token();
            (
                Event::Start {
                    reply_to: ReplyTo::new(token),
                    host_run,
                    activation,
                    identity,
                    window,
                    messages,
                    charter,
                    workspace,
                    grants,
                    transcript,
                    answered,
                },
                token,
            )
        }
        Some(_) => panic!("the caller supplies a complete Start event"),
        None => (
            Event::Start {
                messages: Box::default(),
                reply_to: ReplyTo::new(Token::new(1)),
                host_run: Token::new(1),
                identity: None,
                activation: if settings.resume { 2 } else { 1 },
                window: agent::Window { turns: u32::MAX, bytes: u64::MAX },
                charter: selected_charter.unwrap_or_else(|| charter(settings)),
                workspace,
                grants: Box::new([Grant {
                    renewable: true,
                    name: GrantName { account: 0, generation: 1 },
                    valid: Duration::from_secs(7200),
                }]),
                transcript,
                answered,
            },
            Token::new(1),
        ),
    }
}

impl World {
    /// Queue a host's grant before the pending start, which names no new grant.
    pub fn hold_grant(&mut self, account: u32) {
        let start = self.stage.next_event().expect("pending start");
        self.stage.push(Event::Grant {
            grant: Grant {
                renewable: true,
                name: GrantName { account, generation: 1 },
                valid: Duration::from_secs(7200),
            },
        });
        self.stage.push(start);
    }

    /// Creates one host request and a fresh agent tree. Fixed queue slack
    /// exercises output pressure; finite trace and delivery caps fail fast.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn new(settings: Settings) -> World {
        Self::with_history(settings, None)
    }

    /// A real fake-provider failure with the selected typed terminal metadata.
    #[must_use]
    pub fn with_failure(mut settings: Settings, failure: agent::llm::Failure, evidence: agent::llm::Evidence) -> Self {
        settings.provider.overloaded = 1000;
        let mut world = Self::new(settings);
        world.failure = Some((failure, evidence));
        world
    }

    /// Script this many genuine fake-provider failures before its ordinary successful script.
    /// Uses the same typed terminal metadata as `with_failure`; the referee still sees every call.
    pub fn fail_calls(&mut self, count: u32, failure: agent::llm::Failure, evidence: agent::llm::Evidence) {
        self.failure_count = Some(count);
        self.failure = Some((failure, evidence));
    }

    /// Schedule an existing host grant independently of an in-flight provider call.
    pub fn grant_at(&mut self, at: Time, grant: Grant) {
        self.schedule.send(at, Delivery::Grant { grant });
    }

    /// Actual unauthorized notices, retaining their rejected generations.
    #[must_use]
    pub fn unauthorized(&self) -> &[(Time, GrantName)] {
        &self.unauthorized
    }

    /// The root's remaining alarm after its actual boundary observations.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.agent.next_deadline()
    }

    /// Installs the bounded report accompanying scripted provider failures.
    pub fn failure_report(&mut self, report: agent::llm::Report) {
        self.failure_report = report;
    }

    /// Typed V2 entrance from saved turns, with no later answers supplied.
    /// History is host-owned; root/session admission decides before provider work.
    /// Contract: domain/run.md, sections 3 and 13; domain/session.md, section 3.
    #[must_use]
    pub fn with_history(settings: Settings, transcript: Option<agent::Transcript>) -> World {
        Self::with_backend(&settings, transcript, Box::default(), Backend::typed(&settings))
    }

    /// Starts from saved turns and host decisions made after their last turn.
    #[must_use]
    pub fn with_history_answers(
        settings: Settings,
        transcript: Option<agent::Transcript>,
        answered: Box<[agent::AnsweredCall]>,
    ) -> World {
        Self::with_backend(&settings, transcript, answered, Backend::typed(&settings))
    }

    /// Starts with the caller's complete host event and the usual fixture
    /// checkout and typed provider. The caller may enable the host-call bridge
    /// before driving this world.
    /// Contract: domain/run.md, sections 3.1, 5.2 and 14; domain/host.md, section 9.
    #[must_use]
    pub fn with_start(settings: Settings, start: Event) -> World {
        let mut disk = Checkout::new();
        fixture::seed(&mut disk);
        Self::with_selected_backend(
            &settings,
            None,
            Box::default(),
            None,
            disk,
            Backend::typed(&settings),
            None,
            Some(start),
        )
    }

    /// Start before any work arrives, with the host's waiting authority.
    #[must_use]
    pub fn with_empty_brief(settings: Settings) -> World {
        let mut charter = charter(&settings);
        charter.brief.sections = Box::default();
        charter.instructions = script::cue(settings.job).unwrap_or(b"Look into the code.").into();
        charter.grants.wait = true;
        let start = Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: agent::Window { turns: 100, bytes: u64::MAX },
            charter,
            workspace: None,
            transcript: None,
            answered: Box::default(),
            grants: Box::new([Grant {
                renewable: true,
                name: GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(7200),
            }]),
        };
        Self::with_start(settings, start)
    }

    /// Start a scripted run with the host's specified acknowledgement credit.
    #[must_use]
    pub fn with_window(settings: Settings, window: agent::Window) -> World {
        let start = Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window,
            charter: charter(&settings),
            workspace: None,
            transcript: None,
            answered: Box::default(),
            grants: Box::new([Grant {
                renewable: true,
                name: GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(7200),
            }]),
        };
        Self::with_start(settings, start)
    }

    fn with_backend(
        settings: &Settings,
        transcript: Option<agent::Transcript>,
        answered: Box<[agent::AnsweredCall]>,
        backend: Backend,
    ) -> World {
        let mut disk = Checkout::new();
        let root = fixture::seed(&mut disk);
        let workspace = Some(run::Workspace {
            directories: Box::new([run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"work".as_slice().into(),
                root: Token::new(root),
                writable: settings.writable,
                git: true,
                conflicts: Box::new([]),
            }]),
        });
        Self::with_selected_backend(settings, transcript, answered, workspace, disk, backend, None, None)
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the scripted world accepts each independent Start and backend fixture"
    )]
    fn with_selected_backend(
        settings: &Settings,
        transcript: Option<agent::Transcript>,
        answered: Box<[agent::AnsweredCall]>,
        workspace: Option<run::Workspace>,
        disk: Checkout,
        backend: Backend,
        selected_charter: Option<run::Charter>,
        selected_start: Option<Event>,
    ) -> World {
        let max_out = agent::max_out(&settings.limits);
        let mut stage = Stage::new(settings.limits, max_out, max_out + 3);
        let supplied_start = selected_start.is_some();
        let (start, reply_to) =
            choose_start(settings, transcript, answered, workspace, selected_charter, selected_start);
        let Event::Start { host_run, charter, workspace, messages, .. } = &start else {
            unreachable!("the selected event is Start")
        };
        let root = workspace
            .as_ref()
            .and_then(|workspace| workspace.directories.first())
            .map(|directory| directory.root.raw());
        let observed_contract = charter.outcome.clone();
        let delivery = charter.grants.deliver.is_some();
        let checks = !supplied_start
            && matches!(
                settings.job,
                Job::Coding | Job::Delegating | Job::Wandering | Job::MidReport | Job::MidChange | Job::MarkerReport
            );
        let within = charter.budget.time.saturating_add(Duration::from_secs(120));
        let mut model_prices = BTreeMap::new();
        model_prices.insert(charter.llm.model.clone(), charter.llm.prices);
        for model in &charter.models {
            model_prices.insert(model.model.clone(), model.prices);
        }
        let messages_seen = opening_observations(charter, messages);
        let host_run = *host_run;
        stage.push(start);
        let mut schedule = Schedule::new();
        if let Some(after) = settings.cancel_at {
            schedule.send(Time::ZERO.saturating_add(after), Delivery::Cancel);
        }
        let mut referee = Referee::new(Meeting::default());
        let mut stimuli = Vec::new();
        referee.observe(
            Time::ZERO,
            Seen::Started {
                contract: observed_contract,
                outcome_bytes: settings.limits.run.outcome_bytes,
                checks,
                delivery,
                within,
            },
            &mut stimuli,
        );
        World {
            settings: *settings,
            now: Time::ZERO,
            rng: Rng::new(settings.seed ^ 0x0b),
            agent: agent::Domain::new(&settings.limits, configuration(settings.window), settings.seed ^ 0x17),
            stage,
            backend,
            parent_deliveries: false,
            parent_host_calls: false,
            host_run,
            reply_to,
            delivery_submissions: Vec::new(),
            schedule,
            flights: Ledger::new("agent request"),
            disk,
            root,
            boundaries: Vec::new(),
            admitted: None,
            answer: None,
            answered: None,
            checked: Vec::new(),
            check_notices: Vec::new(),
            check_terminals: Vec::new(),
            pushes: Vec::new(),
            delivery_names: Vec::new(),
            host_history: crate::host_referee::History::with_reply_cap(settings.limits.run.host_reply_bytes),
            host_pending: BTreeMap::new(),
            host_decision: None,
            host_decisions: 0,
            host_terminals: Vec::new(),
            snapshots: BTreeMap::new(),
            landed: Vec::new(),
            prompts: Vec::new(),
            completions: Vec::new(),
            model_prices,
            turns: Vec::new(),
            messages_seen,
            turn_metadata: Vec::new(),
            auto_ack: false,
            waiting: Vec::new(),
            facts: Vec::new(),
            agent_facts_not_written: 0,
            jobs: BTreeMap::new(),
            writing_opened: BTreeSet::new(),
            trace: Trace::default(),
            referee,
            stimuli,
            terminals: 0,
            failure: None,
            failure_report: agent::llm::Report::none(),
            failure_count: None,
            unauthorized: Vec::new(),
        }
    }

    /// Caller-selected workspace/disk and actual typed scripts; no default roots
    /// are seeded. All accepted metadata and lower terminals use the existing world.
    /// Contract: domain/run.md, sections 3.2, 8.3 and 14; testing-strategy.md, section 2.3.
    #[must_use]
    pub fn with_workspace_scripts(
        settings: Settings,
        transcript: Option<agent::Transcript>,
        workspace: Option<run::Workspace>,
        disk: Checkout,
        scripts: Box<[provider::api::Script]>,
    ) -> World {
        let mut backend = Backend::typed(&settings);
        match &mut backend {
            Backend::Typed { provider, .. } => {
                *provider = provider::Domain::configured(
                    &settings.provider,
                    settings.seed ^ 0x25,
                    scripts,
                    smith_session_world::provider::menu(),
                )
                .expect("caller scripts obey provider admission");
            }
        }
        Self::with_selected_backend(&settings, transcript, Box::default(), workspace, disk, backend, None, None)
    }

    /// Caller supplies the complete typed Charter before the original Start;
    /// sections and instructions are never derived from the job settings. The
    /// admission path decides refusal, or one terminal Answer after settling.
    /// Existing receiving bounds, typed backend exclusivity and history apply.
    /// Contract: domain/run.md, sections 3.1, 3.3, 5.3, 13 and 14.
    #[must_use]
    pub fn with_workspace_scripts_charter(
        settings: Settings,
        transcript: Option<agent::Transcript>,
        workspace: Option<run::Workspace>,
        disk: Checkout,
        scripts: Box<[provider::api::Script]>,
        charter: run::Charter,
    ) -> World {
        let mut backend = Backend::typed(&settings);
        match &mut backend {
            Backend::Typed { provider, .. } => {
                *provider = provider::Domain::configured(
                    &settings.provider,
                    settings.seed ^ 0x25,
                    scripts,
                    smith_session_world::provider::menu(),
                )
                .expect("caller scripts obey provider admission");
            }
        }
        Self::with_selected_backend(
            &settings,
            transcript,
            Box::default(),
            workspace,
            disk,
            backend,
            Some(charter),
            None,
        )
    }

    /// Starts with the caller's complete host event, checkout and provider
    /// scripts. The world does not replace any Start field from the settings.
    /// Contract: domain/run.md, sections 3.1, 5.2 and 14; domain/host.md, section 9.
    #[must_use]
    pub fn with_workspace_scripts_start(
        settings: Settings,
        disk: Checkout,
        scripts: Box<[provider::api::Script]>,
        start: Event,
    ) -> World {
        let mut backend = Backend::typed(&settings);
        match &mut backend {
            Backend::Typed { provider, .. } => {
                *provider = provider::Domain::configured(
                    &settings.provider,
                    settings.seed ^ 0x25,
                    scripts,
                    smith_session_world::provider::menu(),
                )
                .expect("caller scripts obey provider admission");
            }
        }
        Self::with_selected_backend(&settings, None, Box::default(), None, disk, backend, None, Some(start))
    }

    /// Route subsequent checked submissions to the outside parent.
    /// Contract: domain/run.md, sections 8.2 and 10.
    pub fn enable_parent_deliveries(&mut self) {
        self.parent_deliveries = true;
    }

    /// Hands host calls to the caller instead of the fixture's canned host.
    /// Call before driving the world.
    /// Contract: domain/run.md, sections 5.2 and 14; domain/host.md, section 9.
    pub fn enable_parent_host_calls(&mut self) {
        self.parent_host_calls = true;
    }

    /// Host calls awaiting a caller terminal, in submission order. Each record
    /// contains the complete emitted call, including relay, input and deadline.
    /// Contract: domain/run.md, sections 5.2 and 14; testing-strategy.md, section 2.3.
    #[must_use]
    pub fn pending_host_calls(&self) -> Vec<&crate::host_referee::Submission> {
        self.host_history
            .submissions()
            .iter()
            .filter(|call| self.host_pending.get(&(call.relay.owner.raw(), call.relay.attempt)) == Some(&None))
            .collect()
    }

    /// Queues the caller's terminal for a pending host relay. Withdrawal keeps
    /// this right alive; the scheduled terminal settles it in the next drive.
    /// Contract: domain/run.md, sections 5.2 and 10; domain/host.md, section 9.
    ///
    /// # Errors
    /// Refuses non-bridge worlds, unknown relays and duplicate replies.
    pub fn return_host_reply(&mut self, relay: run::RelayName, reply: run::HostReply) -> Result<(), &'static str> {
        if !self.parent_host_calls {
            return Err("parent host-call bridge is disabled");
        }
        let Some(pending) = self.host_pending.get_mut(&(relay.owner.raw(), relay.attempt)) else {
            return Err("no pending host relay");
        };
        if pending.is_some() {
            return Err("host relay terminal is already queued");
        }
        *pending = Some(self.schedule.send(self.now, Delivery::Host { relay, reply }));
        Ok(())
    }

    /// Actual fake filesystem state, with no domain-private authority inspection.
    /// Contract: domain/run.md, section 14; testing-strategy.md, section 4.3.
    #[must_use]
    pub fn disk(&self) -> &Checkout {
        &self.disk
    }

    /// A parent submission grants access to its current exclusive snapshot.
    /// The actual host terminal remains owed and is supplied by `return_delivery`.
    /// Contract: domain/run.md, sections 8.2 and 10; testing-strategy.md, section 4.3.
    pub fn delivery_checkout(&mut self, owner: Token) -> &mut Checkout {
        assert!(
            self.parent_deliveries && self.snapshots.contains_key(&owner),
            "only an actual pending parent submission exposes its checkout"
        );
        &mut self.disk
    }

    /// Actual lower boundary observations, in emission order at injected time.
    /// Contract: domain/run.md, sections 3.2 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn boundaries(&self) -> &[(Time, Boundary)] {
        &self.boundaries
    }

    /// Set the externally injected wall clock before the next iteration.
    /// Monotonic deadlines keep running independently; the same wall is handed
    /// to root, shared Client, scripted domain and byte peer.
    /// Contract: scratch/client.md, section 1; programming-model.md, section 9.
    pub fn wall_at(&mut self, wall: Wall) {
        self.stage.env.wall = wall;
        match &mut self.backend {
            Backend::Typed { stage, .. } => stage.env.wall = wall,
        }
    }

    /// Original Start/discovery with actual delivery rights routed to the outside
    /// parent rather than automatically answered. Existing schedule and ledger
    /// retain ownership; drive yields at the current clock while a parent reply
    /// is owed. Caller must supply that actual terminal through `return_delivery`.
    /// At most 256 bounded submission records are retained by this fixture.
    /// Contract: domain/run.md, sections 8.2, 10 and 14; domain/host.md, section 9.
    #[must_use]
    pub fn with_parent_deliveries(settings: Settings) -> World {
        let mut world = Self::new(settings);
        world.parent_deliveries = true;
        world
    }

    /// Actual root submissions observed by the opt-in parent; default worlds
    /// expose none. Records carry the whole bounded request and no extra right.
    /// Contract: domain/run.md, sections 8.2 and 14; testing-strategy.md, section 2.3.
    #[must_use]
    pub fn delivery_submissions(&self) -> &[DeliverySubmission] {
        &self.delivery_submissions
    }

    /// Parent supplies one actual sealed operation terminal for a previously
    /// observed root submission. It enters the existing shared schedule at now;
    /// only its actual delivery consumes the existing flight/snapshot/referee.
    /// Cancellation never substitutes a reply or removes the operation right.
    /// Contract: domain/run.md, sections 8.2 and 10; domain/host.md, sections 2 and 9.
    ///
    /// # Errors
    /// Refuses non-bridge worlds, unknown callbacks and already queued terminals
    /// before scheduling any effect. The refused supplied value is dropped.
    pub fn return_delivery(&mut self, owner: Token, push: run::Delivery) -> Result<(), &'static str> {
        if !self.parent_deliveries {
            return Err("parent delivery bridge is disabled");
        }
        let Some(flight) = self.flights.get_mut((Family::Delivery, owner)) else {
            return Err("no actual parent delivery right");
        };
        if flight.key.is_some() {
            return Err("actual parent terminal is already queued");
        }
        flight.key = Some(self.schedule.send(
            self.now,
            Delivery::Terminal { family: Family::Delivery, owner, event: Event::Delivered { owner, delivery: push } },
        ));
        Ok(())
    }

    /// Queue a parent cancellation for the admitted original run. Its
    /// Start reply and all actual lower terminals remain owed through close.
    /// Contract: domain/run.md, sections 10 and 13; scratch/client.md, section 5.
    pub fn cancel_run(&mut self) {
        assert!(self.admitted.is_some(), "parent cancellation follows actual admission");
        self.schedule.send(self.now, Delivery::Cancel);
    }

    /// Current injected monotonic clock, for outside chronology controls.
    /// Contract: domain/run.md, sections 6 and 10; programming-model.md, section 9.
    #[must_use]
    pub const fn now(&self) -> Time {
        self.now
    }

    /// Script an actual parent input; delivery requires prior Admitted, as the host does.
    /// Contract: domain/run.md, section 6; testing-strategy.md, section 7.
    pub fn message_at(&mut self, at: Time, name: Token, label: Box<[u8]>, text: Box<[u8]>) {
        self.schedule.send(at, Delivery::Message { finish_requested: false, name, label, text });
    }

    /// Script durable host acknowledgement of this turn and its prefix.
    pub fn acknowledge_at(&mut self, at: Time, turn: u32) {
        self.schedule.send(at, Delivery::Acknowledge { turn });
    }

    /// Make a host with no transcript commit each turn as soon as it is told.
    pub fn acknowledge_each_turn(&mut self) {
        self.auto_ack = true;
    }

    /// Runs at most `iterations` deterministic shell rounds. Success means the
    /// one start answered, every boundary settled, and every expectation passed.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    pub fn run(&mut self, iterations: u32) {
        assert!(
            self.drive(iterations),
            "seed {}: world did not settle within {iterations} rounds\n{}",
            self.settings.seed,
            self.trace.lines().join("\n")
        );
    }

    /// Advance up to this many loop rounds, allowing another domain
    /// to translate the emitted boundary records before the next round. Returns
    /// true only after the original root/provider/IO ledgers actually settle.
    /// An opt-in parent delivery awaiting its external terminal keeps the current
    /// clock and returns control; it never advances a deadline in place of that
    /// parent. The caller schedules the actual reply before driving settlement.
    /// Contract: domain/host.md, section 9; testing-strategy.md, section 2.3.
    pub fn drive(&mut self, iterations: u32) -> bool {
        for _ in 0..iterations {
            self.stage.tick(self.now);
            match &mut self.backend {
                Backend::Typed { stage, .. } => stage.tick(self.now),
            }
            while let Some(delivery) = self.schedule.next(self.now) {
                self.deliver(delivery);
            }
            while self.agent.facts_room() >= agent::max_facts(&self.stage.env.limits)
                && self.stage.has_room()
                && self.agent.is_ready()
            {
                agent::resume(&mut self.agent, &self.stage.env, &mut self.stage.out);
                self.gather();
            }
            while self.agent.facts_room() >= agent::max_facts(&self.stage.env.limits)
                && let Some(mut event) = self.stage.next_event()
            {
                self.log_event(&mut event);
                if matches!(&event, Event::Cancel { .. }) {
                    self.messages_seen.push((self.now, crate::messages_referee::Seen::ParentCancelled));
                }
                agent::step(&mut self.agent, &self.stage.env, event, &mut self.stage.out);
                self.gather();
            }
            while self.agent.facts_room() >= agent::max_facts(&self.stage.env.limits)
                && self.stage.has_room()
                && self.agent.is_due(self.now)
            {
                agent::fire(&mut self.agent, &self.stage.env, &mut self.stage.out);
                self.gather();
            }
            while let Some(request) = self.stage.out.pop() {
                self.trace.log(self.now, format!("agent -> {request:?}"));
                self.request(request);
            }
            self.advance_provider();
            self.agent.reclaim();
            match &mut self.backend {
                Backend::Typed { provider, .. } => provider.reclaim(),
            }
            if self.referee.is_due(self.now) {
                self.referee.fire(self.now, &mut self.stimuli);
            }
            self.referee.assert_holding(self.settings.seed);
            assert!(
                self.trace.lines().len() <= 20_000 && self.schedule.len() <= 256,
                "seed {}: fixture trace and deliveries stay bounded",
                self.settings.seed
            );
            if self.answer.is_some()
                && self.schedule.is_empty()
                && !self.stage.has_events()
                && !self.backend.has_events()
                && !self.agent.is_ready()
                && self.backend.is_settled()
            {
                self.settled();
                return true;
            }
            if self.parent_host_calls && self.host_pending.values().any(Option::is_none) {
                return false;
            }
            let immediate = (self.parent_deliveries
                && self.flights.keys().any(|key| {
                    key.0 == Family::Delivery && self.flights.get(*key).is_some_and(|flight| flight.key.is_none())
                }))
                || self.stage.has_events()
                || self.backend.has_events()
                || self.agent.is_ready();
            if !immediate {
                self.now = [
                    self.schedule.next_time(),
                    self.agent.next_deadline(),
                    self.backend.next_deadline(),
                    self.referee.next_deadline(),
                ]
                .into_iter()
                .flatten()
                .min()
                .expect("an unsettled world has a next boundary");
            }
        }
        false
    }

    fn log_event(&mut self, event: &mut Event) {
        let saved = if let Event::Start { transcript, .. } = event { transcript.clone() } else { None };
        let mut lengths = Vec::new();
        let mut digest = DefaultHasher::new();
        if let Event::Start { transcript: Some(transcript), .. } = event {
            for block in
                transcript.turns.iter_mut().flat_map(|turn| &mut turn.messages).flat_map(|message| &mut message.content)
            {
                let payload = match block {
                    SavedBlock::Text { text, .. }
                    | SavedBlock::Refusal { text, .. }
                    | SavedBlock::Opaque { bytes: text }
                    | SavedBlock::ToolCall { input: text, .. } => Some(text),
                    SavedBlock::ToolResult { .. } => None,
                };
                let Some(payload) = payload.filter(|payload| payload.len() > 1024) else { continue };
                (payload.len(), &payload[..512], &payload[payload.len() - 512..]).hash(&mut digest);
                lengths.push(payload.len());
                *payload = Box::new([]);
            }
            transcript.hash(&mut digest);
        }
        if lengths.is_empty() {
            self.trace.log(self.now, format!("agent <- {event:?}"));
        } else {
            self.trace.log(self.now, format!("agent <- {event:?}; large payload lengths {lengths:?}, envelope and first/last 512-byte fingerprint {:016x}", digest.finish()));
        }
        if let Event::Start { transcript, .. } = event {
            *transcript = saved;
        }
    }

    fn advance_provider(&mut self) -> bool {
        match &mut self.backend {
            Backend::Typed { provider, stage, .. } => {
                while let Some(event) = stage.next_event() {
                    if matches!(&event, provider::Event::Call { .. })
                        && let Some(remaining) = self.failure_count.as_mut()
                    {
                        stage.env.limits.overloaded = if *remaining > 0 { 1000 } else { 0 };
                        *remaining = remaining.saturating_sub(1);
                    }
                    provider::step(provider, &stage.env, event, &mut stage.out);
                }
                while stage.has_room() && provider.is_due(self.now) {
                    provider::fire(provider, &stage.env, &mut stage.out);
                }
            }
        }
        loop {
            let reply = match &mut self.backend {
                Backend::Typed { stage, calls, .. } => stage.out.pop().map(|request| {
                    let (to, result) = match request {
                        provider::Request::Reply { to, result } => (to, result),
                        other @ (provider::Request::Piece { .. } | provider::Request::Hold { .. }) => {
                            panic!("typed provider expected a terminal reply, got {other:?}")
                        }
                    };
                    let owner = to.into_token();
                    let (grants, served, asker) = calls.end(owner);
                    (owner, grants, served, asker, result)
                }),
            };
            let Some((owner, grants, served, asker, result)) = reply else { break };
            if self.flights.get((Family::Completion, owner)).is_some_and(|flight| !flight.cancelled) {
                let event = match result {
                    Ok(answer) => {
                        Event::Completed { owner, completion: translate::completion(answer, grants, &served, asker) }
                    }
                    Err(error) => {
                        let (failure, evidence) =
                            self.failure.unwrap_or((translate::failure(error.error), agent::llm::Evidence::Unknown));
                        Event::Failed {
                            owner,
                            failure,
                            evidence,
                            detail: Box::default(),
                            report: self.failure_report.clone(),
                        }
                    }
                };
                self.send(Family::Completion, owner, event);
            }
        }
        false
    }

    fn observe(&mut self, seen: Seen) {
        self.referee.observe(self.now, seen, &mut self.stimuli);
    }

    fn gather(&mut self) {
        while let Some(fact) = self.agent.pop_fact() {
            match &fact {
                Fact::Session { fact } => self.observe(Seen::Jobs { fact: fact.kind.clone() }),
                Fact::Run { .. } => {}
            }
            if self.settings.drain_facts {
                self.facts.push(fact);
            } else {
                self.agent_facts_not_written += 1;
            }
        }
        while self.agent.pop_content().is_some() {}
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive fixture routing table keeps each copied request and its terminal ownership together"
    )]
    fn request(&mut self, request: Request) {
        match request {
            Request::Turn { host_run, number, position, read, spent, opens_window, turn } => {
                assert_eq!(host_run, self.host_run);
                assert_eq!(usize::try_from(number).expect("bounded output number"), self.turns.len() + 1);
                assert_eq!(position, turn.sequence);
                self.observe(Seen::Turn { number });
                if let smith_domain::session::record::Kind::Window(opener) = &turn.kind {
                    self.observe(Seen::Window {
                        caching: skein_fake_llm_domain::api::Caching::Scope(opener.affinity.key),
                        messages: turn
                            .messages
                            .iter()
                            .map(|message| skein_fake_llm_domain::api::Message {
                                role: skein_fake_llm_domain::api::Role::User,
                                parts: crate::messages_referee::recorded_parts(&message.content).into(),
                            })
                            .collect(),
                    });
                }
                self.messages_seen.push((
                    self.now,
                    crate::messages_referee::Seen::Turn {
                        number,
                        read,
                        spent,
                        opens_window,
                        turn: Box::new(turn.clone()),
                    },
                ));
                self.turn_metadata.push((number, read, spent));
                self.turns.push(turn);
                if self.auto_ack {
                    let run = self.admitted.expect("turn follows admission");
                    self.stage.push(Event::Acknowledge { run, turn: number });
                }
            }
            Request::Waiting { host_run, read } => {
                assert_eq!(host_run, self.host_run);
                self.messages_seen.push((self.now, crate::messages_referee::Seen::Waiting { read }));
                self.waiting.push((self.now, read));
            }
            Request::Admitted { host_run, run } => {
                assert_eq!(host_run, self.host_run, "the host's admitted identity is echoed");
                assert!(self.admitted.replace(run).is_none(), "a start is admitted at most once");
                self.messages_seen.push((self.now, crate::messages_referee::Seen::Admitted));
            }
            Request::Answer { to, answer, read, detail: _ } => {
                if matches!(&answer, run::Answer::Failed { .. }) {
                    self.host_history.shutdown(self.now);
                }
                assert_eq!(to, ReplyTo::new(self.reply_to), "the host receives its own answer");
                self.observe(Seen::Answered {
                    answer: copy_answer(&answer),
                    pending: self.flights.keys().count() + self.host_pending.len(),
                });
                let (turns, parked) = match &answer {
                    run::Answer::Parked { turns, .. } => (*turns, true),
                    run::Answer::Accepted { turns, .. } | run::Answer::Failed { turns, .. } => (*turns, false),
                    run::Answer::Refused(_) => (0, false),
                };
                let spent = match &answer {
                    run::Answer::Refused(_) => run::Spend::ZERO,
                    run::Answer::Parked { spent, .. }
                    | run::Answer::Accepted { spent, .. }
                    | run::Answer::Failed { spent, .. } => *spent,
                };
                assert!(self.answer.replace(answer).is_none(), "one answer per host start");
                self.messages_seen
                    .push((self.now, crate::messages_referee::Seen::Answer { turns, parked, spent, read }));
                self.answered = Some(self.now);
            }
            Request::Long { host_run, operation, .. } => {
                assert_eq!(host_run, self.host_run, "long notice echoes the host identity");
                match operation {
                    smith_domain::Operation::Checks => self.check_notices.push((self.now, true)),
                    smith_domain::Operation::Completion | smith_domain::Operation::Command => {}
                }
            }
            Request::LongDone { host_run } => {
                assert_eq!(host_run, self.host_run, "ended notice echoes the host identity");
                if self.check_notices.last().is_some_and(|(_, open)| *open) {
                    self.check_notices.push((self.now, false));
                }
            }
            Request::Complete { owner, prompt, grant, .. } => {
                if prompt.asker == llm::Asker::Child
                    && (prompt.tools.modify || prompt.tools.shell)
                    && self.writing_opened.insert(prompt.affinity.thread)
                {
                    self.observe(Seen::WritingChild);
                }
                self.host_history.prompt(&prompt).expect("exact feedback for observed provider host call");
                self.observe(Seen::Completing { owner });
                let starts = u64::from(self.settings.limits.run.budget.turns)
                    .checked_add(u64::from(self.settings.limits.run.conversations))
                    .expect("fixture concurrent turns")
                    .checked_mul(u64::from(self.settings.limits.session.completion_blocks) + 1)
                    .expect("fixture child openings across all delegate batches")
                    .checked_add(1)
                    .expect("fixture initial main completion")
                    .checked_mul(u64::from(self.settings.limits.session.retries) + 1)
                    .expect("fixture retry attempts");
                assert!(
                    u64::try_from(self.completions.len()).expect("fixture observation count") < starts,
                    "bounded actual provider chronology from receiving turn/concurrency/retry limits"
                );
                let completion_index = self.completions.len();
                self.completions.push(CompletionObservation {
                    owner,
                    model: prompt.model.clone(),
                    grant,
                    started: self.now,
                    terminal: None,
                });
                let completion_message =
                    u32::try_from(prompt.messages.len()).expect("bounded actual prompt message count");
                self.flights.open(
                    (Family::Completion, owner),
                    Flight {
                        key: None,
                        cancelled: false,
                        completion_message: Some(completion_message),
                        completion_index: Some(completion_index),
                    },
                );
                match &mut self.backend {
                    Backend::Typed { stage, calls, .. } => {
                        calls.open(owner, (prompt.tools, prompt.served.clone(), prompt.asker));
                        let query = translate::query(prompt);
                        self.messages_seen
                            .push((self.now, crate::messages_referee::Seen::Prompt { query: query.clone() }));
                        self.prompts.push(query.clone());
                        stage.push(provider::Event::Call { reply_to: ReplyTo::new(owner), call: owner, query });
                    }
                }
                self.observe(Seen::Prompt { owner, query: self.prompts.last().expect("actual typed request").clone() });
            }
            Request::Cancel { owner } => match &mut self.backend {
                Backend::Typed { .. } => self.cancel(Family::Completion, owner, Event::Cancelled { owner }),
            },
            Request::Io { owner, op, deadline } => {
                if self.start_job(owner, &op, deadline) {
                    return;
                }
                self.boundaries.push((self.now, Boundary::Io { op: op.clone() }));
                self.flights.open(
                    (Family::Io, owner),
                    Flight { key: None, cancelled: false, completion_message: None, completion_index: None },
                );
                let delivery = match op {
                    tools::Op::Spawn { cwd, command, head, tail, after: _ } => {
                        match io::spawn(&self.disk, &cwd, &command, &[], (head, tail)) {
                            Ok(started) => {
                                let duration = Duration::from_nanos(
                                    u64::try_from(started.process.program.duration.as_nanos())
                                        .expect("fixture command duration fits"),
                                );
                                let complete = self.now.saturating_add(duration);
                                let key = self.schedule.send(
                                    complete.min(deadline),
                                    Delivery::Command {
                                        owner,
                                        process: started.process,
                                        head,
                                        tail,
                                        timed_out: complete > deadline,
                                    },
                                );
                                self.flights.get_mut((Family::Io, owner)).expect("the command is pending").key =
                                    Some(key);
                                return;
                            }
                            Err(done) => {
                                Delivery::Terminal { family: Family::Io, owner, event: Event::Done { owner, done } }
                            }
                        }
                    }
                    op @ (tools::Op::Load { .. }
                    | tools::Op::List { .. }
                    | tools::Op::Remove { .. }
                    | tools::Op::Store { .. }
                    | tools::Op::Search { .. }) => Delivery::Io { owner, op, deadline },
                };
                self.schedule_flight(Family::Io, owner, delivery);
            }
            Request::Job { owner, act } => self.job_act(owner, act),
            Request::CancelIo { owner } => {
                if self.cancel_job(owner) {
                    return;
                }
                self.cancel(Family::Io, owner, Event::Done { owner, done: tools::Done::Cancelled });
            }
            Request::Read { owner, at, max, deadline } => {
                self.boundaries.push((self.now, Boundary::Read { at: at.clone() }));
                let complete = self.now.saturating_add(self.settings.network.draw(&mut self.rng));
                let read = if complete > deadline {
                    run::Read::Failed
                } else {
                    match self.disk.load(at.root.raw(), &at.path, u64::from(max)) {
                        Ok((text, _)) => run::Read::Text { text: text.into(), whole: true },
                        Err(_) => run::Read::Missing,
                    }
                };
                let key = self.schedule.send(
                    complete.min(deadline),
                    Delivery::Terminal { family: Family::Read, owner, event: Event::Read { owner, read } },
                );
                self.flights.open(
                    (Family::Read, owner),
                    Flight { key: Some(key), cancelled: false, completion_message: None, completion_index: None },
                );
            }
            Request::Probe { owner, at, deadline } => {
                self.boundaries.push((self.now, Boundary::Probe { at: at.clone() }));
                let complete = self.now.saturating_add(self.settings.network.draw(&mut self.rng));
                let executable = complete <= deadline
                    && self
                        .disk
                        .load(at.root.raw(), &at.path, 4096)
                        .is_ok_and(|(content, _)| content.starts_with(b"#!"));
                let key = self.schedule.send(
                    complete.min(deadline),
                    Delivery::Terminal { family: Family::Probe, owner, event: Event::Probed { owner, executable } },
                );
                self.flights.open(
                    (Family::Probe, owner),
                    Flight { key: Some(key), cancelled: false, completion_message: None, completion_index: None },
                );
            }
            Request::Check { owner, program, deadline, tail } => {
                self.boundaries.push((self.now, Boundary::Check { program: program.clone() }));
                self.observe(Seen::Checking { owner, tree: self.code() });
                assert_eq!(&*program.path, fixture::CHECKS, "the host explicitly selected its Temper fixture checks");
                let (passed, output) = fixture::check(&self.disk, program.root.raw());
                self.flights.open(
                    (Family::Check, owner),
                    Flight { key: None, cancelled: false, completion_message: None, completion_index: None },
                );
                let duration = self.settings.check.draw(&mut self.rng);
                let complete = self.now.saturating_add(duration);
                let key = self.schedule.send(
                    complete.min(deadline),
                    Delivery::Check { owner, passed, output, timed_out: complete > deadline, tail },
                );
                self.flights.get_mut((Family::Check, owner)).expect("the check is pending").key = Some(key);
            }
            Request::Abort { owner } => self.cancel(Family::Check, owner, Event::Aborted { owner }),
            Request::Deliver { host_run, owner, change, name, deadline } => {
                // Title/body are this fixture host's final-Change policy only.
                // The Report-only mid-run fixture instead requires opaque ticket.
                let required: &[&[u8]] = if change.fields.iter().any(|field| field.name.as_ref() == b"ticket") {
                    &[b"ticket"]
                } else {
                    &[b"title", b"body"]
                };
                for &name in required {
                    let value = change
                        .fields
                        .iter()
                        .find(|field| &*field.name == name)
                        .expect("the scripted host requires this field");
                    assert!(!value.value.is_empty(), "the host receives the metadata its own contract required");
                }
                assert_eq!(host_run, self.host_run, "the push names the scripted host's request");
                self.delivery_names.push((name, self.now));
                let tree = self.code();
                let finishing = !change.fields.iter().any(|field| field.name.as_ref() == b"ticket");
                self.observe(Seen::Pushing { owner, name, tree: tree.clone(), finishing });
                self.snapshots.insert(owner, tree);
                self.flights.open(
                    (Family::Delivery, owner),
                    Flight { key: None, cancelled: false, completion_message: None, completion_index: None },
                );
                if self.parent_deliveries {
                    assert!(self.delivery_submissions.len() < 256, "finite actual parent delivery story ceiling");
                    self.delivery_submissions.push(DeliverySubmission {
                        host_run,
                        owner,
                        name,
                        change,
                        deadline,
                        at: self.now,
                    });
                    return;
                }
                let after = self.settings.network.draw(&mut self.rng);
                let complete = self.now.saturating_add(after);
                let push = if complete > deadline {
                    run::Delivery::Failed(run::DeliveryFailure::new(0, run::DeliveryReason::TimedOut))
                } else if self.settings.job == Job::MarkerReport
                    && self
                        .disk
                        .load(self.root.expect("marker scenario has a workspace"), b"conflict.txt", u64::MAX)
                        .is_ok_and(|(bytes, _)| bytes.windows(7).any(|part| part == b"<<<<<<<"))
                {
                    run::Delivery::Refused(
                        run::DeliveryRefusal::new(
                            Some(
                                run::Marker::new(0, b"conflict.txt".as_slice().into()).expect("relative fixture path"),
                            ),
                            b"conflict markers remain".as_slice().into(),
                        )
                        .expect("bounded feedback"),
                    )
                } else {
                    self.settings.push.terminal()
                };
                let key = self.schedule.send(
                    complete.min(deadline),
                    Delivery::Terminal {
                        family: Family::Delivery,
                        owner,
                        event: Event::Delivered { owner, delivery: push },
                    },
                );
                self.flights.get_mut((Family::Delivery, owner)).expect("submitted delivery").key = Some(key);
            }
            Request::HostCall { host_run, relay, name, tool, effect, input, deadline } => {
                assert_eq!(host_run, self.host_run, "the host call keeps the caller's logical run scope");
                self.host_history
                    .submit(crate::host_referee::Submission {
                        host_run,
                        relay,
                        name,
                        tool,
                        effect,
                        input,
                        at: self.now,
                        deadline,
                    })
                    .expect("valid recovery history");
                if self.parent_host_calls {
                    assert!(self.host_pending.insert((relay.owner.raw(), relay.attempt), None).is_none());
                    return;
                }
                let answer = match self.settings.host {
                    HostSchedule::TooLarge => {
                        run::HostAnswer::new(vec![b'x'; 128].into(), false).expect("bounded oversized host text")
                    }
                    HostSchedule::Answer | HostSchedule::Replay | HostSchedule::Withdraw | HostSchedule::LateAnswer => {
                        run::HostAnswer::new(b"opaque host answer: first decision".as_slice().into(), false)
                            .expect("bounded host text")
                    }
                };
                let reply = match self.settings.host {
                    HostSchedule::Answer | HostSchedule::LateAnswer | HostSchedule::TooLarge => {
                        run::HostReply::Answered(answer)
                    }
                    HostSchedule::Replay => match relay.attempt {
                        1 => run::HostReply::Busy,
                        2 => run::HostReply::Unanswered(run::Unanswered::Lost),
                        _ => run::HostReply::Answered(answer),
                    },
                    HostSchedule::Withdraw => {
                        if relay.attempt == 1 {
                            run::HostReply::Unanswered(run::Unanswered::Withdrawn)
                        } else {
                            run::HostReply::Answered(answer)
                        }
                    }
                };
                let reply = match reply {
                    run::HostReply::Answered(answer) => {
                        let recorded = self.record_host(answer);
                        run::HostReply::Answered(recorded)
                    }
                    run::HostReply::Unanswered(run::Unanswered::Lost) => {
                        self.record_host(
                            run::HostAnswer::new(b"opaque host answer: first decision".as_slice().into(), false)
                                .expect("bounded first record"),
                        );
                        run::HostReply::Unanswered(run::Unanswered::Lost)
                    }
                    run::HostReply::Busy => run::HostReply::Busy,
                    run::HostReply::Withdrawn => run::HostReply::Withdrawn,
                    run::HostReply::TooLarge => run::HostReply::TooLarge,
                    run::HostReply::Unanswered(run::Unanswered::Withdrawn) => {
                        run::HostReply::Unanswered(run::Unanswered::Withdrawn)
                    }
                };
                let at = if matches!(self.settings.host, HostSchedule::LateAnswer | HostSchedule::Withdraw)
                    && relay.attempt == 1
                {
                    deadline.saturating_add(Duration::from_millis(20))
                } else {
                    self.now.saturating_add(self.settings.network.draw(&mut self.rng))
                };
                let key = self.schedule.send(at, Delivery::Host { relay, reply });
                assert!(self.host_pending.insert((relay.owner.raw(), relay.attempt), Some(key)).is_none());
            }
            Request::WithdrawHost { relay } => {
                self.host_history.withdraw(relay).expect("withdraw retains actual terminal");
                assert!(self.host_pending.contains_key(&(relay.owner.raw(), relay.attempt)));
            }
            Request::Unauthorized { grant } => self.unauthorized.push((self.now, grant)),
            Request::MessageRefused { .. }
            | Request::Refused { .. }
            | Request::Limited { .. }
            | Request::Yield { .. } => {}
        }
    }

    fn record_host(&mut self, answer: run::HostAnswer) -> run::HostAnswer {
        if let Some(recorded) = &self.host_decision {
            return recorded.clone();
        }
        self.host_decisions = self.host_decisions.checked_add(1).expect("one immutable host decision");
        self.host_decision = Some(answer.clone());
        answer
    }

    fn send(&mut self, family: Family, owner: Token, event: Event) {
        self.schedule_flight(family, owner, Delivery::Terminal { family, owner, event });
    }

    fn schedule_flight(&mut self, family: Family, owner: Token, delivery: Delivery) {
        let at = self.now.saturating_add(self.settings.network.draw(&mut self.rng));
        let key = self.schedule.send(at, delivery);
        self.flights.get_mut((family, owner)).expect("the terminal is for a pending request").key = Some(key);
    }

    fn cancel(&mut self, family: Family, owner: Token, event: Event) {
        let Some(flight) = self.flights.get((family, owner)) else { return };
        if flight.cancelled {
            return;
        }
        if flight.key.is_some() && self.rng.chance(self.settings.races) {
            return;
        }
        if let Some(key) = flight.key {
            self.schedule.withdraw(key).expect("a pending terminal can be withdrawn");
        }
        self.flights.get_mut((family, owner)).expect("the cancellation owns the pending request").cancelled = true;
        self.send(family, owner, event);
    }

    fn deliver(&mut self, delivery: Delivery) {
        match delivery {
            Delivery::Grant { grant } => self.stage.push(Event::Grant { grant }),
            Delivery::Message { finish_requested, name, label, text } => {
                let mut rendered = label.to_vec();
                rendered.extend_from_slice(b": ");
                rendered.extend_from_slice(&text);
                self.messages_seen
                    .push((self.now, crate::messages_referee::Seen::Input { name, text: rendered.into_boxed_slice() }));
                let run = self.admitted.expect("parent sends only after actual admission");
                self.stage.push(Event::Message { finish_requested, label, run, name, text });
            }
            Delivery::Acknowledge { turn } => {
                let run = self.admitted.expect("parent acknowledges only an admitted run");
                self.stage.push(Event::Acknowledge { run, turn });
            }
            Delivery::Host { relay, reply } => {
                self.host_terminals.push((relay, self.now, reply.clone()));
                self.host_history.terminal(self.now, relay, &reply).expect("one actual host terminal");
                self.host_pending.remove(&(relay.owner.raw(), relay.attempt)).expect("host relay pending");
                self.stage.push(Event::HostReturned { relay, reply });
                self.terminals += 1;
            }
            Delivery::Cancel => {
                if let Some(run) = self.admitted {
                    self.host_history.shutdown(self.now);
                    self.stage.push(Event::Cancel { run });
                }
            }
            Delivery::Terminal { family, owner, event } => self.terminal(family, owner, event),
            Delivery::Io { owner, op, deadline } => {
                let done = if self.now > deadline { tools::Done::TimedOut } else { io::perform(&mut self.disk, op) };
                self.terminal(Family::Io, owner, Event::Done { owner, done });
            }
            Delivery::Command { owner, process, head, tail, timed_out } => {
                if !timed_out {
                    io::finish(&mut self.disk, &process);
                }
                let exit = if timed_out { None } else { Some(process.program.exit) };
                let done = io::exited(exit, &process.program.output, head, tail);
                self.terminal(Family::Io, owner, Event::Done { owner, done });
            }
            Delivery::Job { owner } => self.job_tick(owner),
            Delivery::Check { owner, passed, output, timed_out, tail } => {
                let exit = if timed_out { run::Exit::TimedOut } else { run::Exit::Code { code: u8::from(!passed) } };
                let keep = usize::try_from(tail).expect("fixture output bound fits").min(output.len());
                let cut = output.len() - keep;
                let ran = run::Ran {
                    exit,
                    output: output[cut..].into(),
                    cut: u64::try_from(cut).expect("fixture output fits"),
                };
                self.terminal(Family::Check, owner, Event::Checked { owner, ran });
            }
        }
    }

    fn observe_host_call(&mut self, completion: &llm::Completion, message: Option<u32>) {
        for (position, part) in completion.content.iter().enumerate() {
            match part {
                llm::Said::ToolCall {
                    id,
                    name,
                    input,
                    call: llm::Decoded::Served { ask: run::Ask::Host { .. } },
                    ..
                } if completion.stop == llm::Stop::ToolUse => {
                    self.host_history
                        .called(
                            message.expect("actual completion flight retains its prompt origin"),
                            u32::try_from(position).expect("bounded actual completion block position"),
                            id.clone(),
                            name.clone(),
                            input.clone(),
                        )
                        .expect("observed single executable provider host operation");
                }
                llm::Said::Text { .. }
                | llm::Said::Refusal { .. }
                | llm::Said::Opaque { .. }
                | llm::Said::ToolCall { .. } => {}
            }
        }
    }

    /// Observe the original owner's terminal before forwarding its event.
    /// Native usage comes directly from the matching SDK binding; typed usage
    /// is the exact callback value. No event or completion body is cloned.
    /// Contract: scratch/client.md, section 4; domain/run.md, sections 9 and 13.
    fn observe_completion_terminal(&mut self, index: usize, event: &Event) {
        let outcome = match event {
            Event::Completed { completion, .. } => {
                let usage = match &self.backend {
                    Backend::Typed { .. } => completion.usage,
                };
                CompletionTerminal::Completed(usage)
            }
            Event::Failed { .. } => CompletionTerminal::Failed,
            Event::Cancelled { .. } => CompletionTerminal::Cancelled,
            Event::NativeVersion { .. }
            | Event::BuiltinResult { .. }
            | Event::HostReturned { .. }
            | Event::Start { .. }
            | Event::Message { .. }
            | Event::Acknowledge { .. }
            | Event::Grant { .. }
            | Event::Cancel { .. }
            | Event::Delivered { .. }
            | Event::Done { .. }
            | Event::Read { .. }
            | Event::Probed { .. }
            | Event::Checked { .. }
            | Event::Aborted { .. } => panic!("completion flight receives only actual provider terminals"),
        };
        assert!(self.completions[index].terminal.replace((self.now, outcome)).is_none());
    }

    fn terminal(&mut self, family: Family, owner: Token, event: Event) {
        let flight = self.flights.end((family, owner));
        self.terminals += 1;
        if let Some(index) = flight.completion_index {
            self.observe_completion_terminal(index, &event);
        }
        match &event {
            Event::Completed { completion, .. } => {
                self.observe_host_call(completion, flight.completion_message);
                let index = flight.completion_index.expect("actual completion observation");
                let prices = self.model_prices[&self.completions[index].model];
                let usage = completion.usage;
                let units = observed_price(prices, usage).unwrap_or(0);
                self.observe(Seen::Completed {
                    owner,
                    spent: run::Spend {
                        units,
                        turns: 1,
                        input: completion.usage.input_tokens,
                        output: completion.usage.output_tokens,
                        cache_read: completion.usage.cache_read_tokens,
                        cache_write: completion.usage.cache_write_tokens,
                        reasoning: completion.usage.reasoning_tokens,
                    },
                });
            }
            Event::Failed { .. } | Event::Cancelled { .. } => self.observe(Seen::CompletionEnded { owner }),
            Event::Checked { ran, .. } => {
                self.checked.push(ran.exit == (run::Exit::Code { code: 0 }));
                self.check_terminals.push(self.now);
                self.observe(Seen::Checked { owner, exit: Some(ran.exit) });
            }
            Event::Delivered { delivery: push, .. } => {
                self.pushes.push(push.clone());
                let tree = self.snapshots.remove(&owner).expect("a push snapshots its checkout");
                let landed = if matches!(push, run::Delivery::Delivered(_)) { tree } else { Vec::new() };
                if !landed.is_empty() {
                    self.landed.clone_from(&landed);
                }
                self.observe(Seen::Delivered { owner, push: push.clone(), tree: landed });
            }
            Event::Aborted { .. } => {
                self.check_terminals.push(self.now);
                self.observe(Seen::Checked { owner, exit: None });
            }
            Event::NativeVersion { .. }
            | Event::BuiltinResult { .. }
            | Event::HostReturned { .. }
            | Event::Message { .. }
            | Event::Acknowledge { .. }
            | Event::Start { .. }
            | Event::Grant { .. }
            | Event::Cancel { .. }
            | Event::Done { .. }
            | Event::Read { .. }
            | Event::Probed { .. } => {}
        }
        match &event {
            Event::Completed { completion, .. } => self.messages_seen.push((
                self.now,
                crate::messages_referee::Seen::Completed {
                    parts: crate::messages_referee::completion_parts(&completion.content),
                },
            )),
            Event::Failed { .. } | Event::Cancelled { .. } => {
                self.messages_seen.push((self.now, crate::messages_referee::Seen::CompletionEnded));
            }
            Event::NativeVersion { .. }
            | Event::BuiltinResult { .. }
            | Event::Cancel { .. }
            | Event::Start { .. }
            | Event::Message { .. }
            | Event::Acknowledge { .. }
            | Event::Grant { .. }
            | Event::HostReturned { .. }
            | Event::Delivered { .. }
            | Event::Read { .. }
            | Event::Probed { .. }
            | Event::Checked { .. }
            | Event::Aborted { .. }
            | Event::Done { .. } => {}
        }
        self.stage.push(event);
    }

    fn settled(&self) {
        let continuation = match self.answer() {
            run::Answer::Accepted { .. } | run::Answer::Parked { .. } => true,
            run::Answer::Failed { .. } | run::Answer::Refused(_) => false,
        };
        self.host_history
            .finish(continuation)
            .expect("normal continuation owes exact host feedback; shutdown settles actual relays");
        assert!(self.host_pending.is_empty());
        self.flights.assert_settled();
        match &self.backend {
            Backend::Typed { calls, .. } => calls.assert_settled(),
        }
        self.referee.assert_passed(self.settings.seed);
        assert_eq!(
            (self.agent.run().runs(), self.agent.run().conversations(), self.agent.run().calls()),
            (0, 0, 0),
            "the run tree settled"
        );
        assert_eq!(
            (
                self.agent.session().sessions(),
                self.agent.session().runs(),
                self.agent.session().kits(),
                self.agent.session().flights()
            ),
            (0, 0, 0, 0),
            "sessions and tools settled"
        );
        assert_eq!(
            (self.agent.peers(), self.agent.flights(), self.agent.tickets()),
            (0, 0, 0),
            "root routing holds nothing"
        );
        if self.settings.drain_facts {
            self.check_facts();
        }
    }

    fn check_facts(&self) {
        use agent::{run::facts as rf, session::Fact as sf};
        let mut used = run::Spend::ZERO;
        let (mut opened, mut ended, mut answered) = (0_u32, 0_u32, 0_u32);
        for fact in &self.facts {
            match fact {
                Fact::Session { fact: sf { kind: smith_domain::session::FactKind::Used { usage, .. }, .. } } => {
                    used = used
                        .accumulate(run::Spend {
                            units: 0,
                            turns: 1,
                            input: usage.input_tokens,
                            output: usage.output_tokens,
                            cache_read: usage.cache_read_tokens,
                            cache_write: usage.cache_write_tokens,
                            reasoning: usage.reasoning_tokens,
                        })
                        .expect("bounded observed usage");
                }
                Fact::Run { fact: rf::Fact { kind: rf::FactKind::Opened { .. }, .. }, opening: _ } => opened += 1,
                Fact::Run { fact: rf::Fact { kind: rf::FactKind::Ended { .. }, .. }, opening: _ } => ended += 1,
                Fact::Run { fact: rf::Fact { kind: rf::FactKind::Answered { .. }, .. }, opening: _ } => answered += 1,
                Fact::Run { .. } | Fact::Session { .. } => {}
            }
        }
        let spent = match self.answer() {
            run::Answer::Refused(_) => run::Spend::ZERO,
            run::Answer::Parked { spent, .. }
            | run::Answer::Accepted { spent, .. }
            | run::Answer::Failed { spent, .. } => *spent,
        };
        assert_eq!(
            used,
            run::Spend { units: 0, ..spent },
            "facts match independently accepted provider raw usage and the host answer"
        );
        assert_eq!(opened, ended, "every conversation fact has one terminal");
        assert_eq!(answered, u32::from(self.admitted.is_some()), "every admitted run answers once in its facts");
    }

    /// Exact time-ordered outside observations for the independent chat oracle.
    /// Contract: domain/run.md, sections 6 and 13; testing-strategy.md, section 7.
    #[must_use]
    pub fn messages_seen(&self) -> &[(Time, crate::messages_referee::Seen)] {
        &self.messages_seen
    }

    /// Actual main concrete turns, in emitted order. Contract: domain/run.md, section 13.
    #[must_use]
    pub fn turns(&self) -> &[agent::Turn] {
        &self.turns
    }

    /// Actual activation numbers/read fences/token usage. Contract: domain/run.md, sections 6 and 13.
    #[must_use]
    pub fn turn_metadata(&self) -> &[(u32, Option<Token>, run::Spend)] {
        &self.turn_metadata
    }

    /// Actual settled Waiting notices. Contract: domain/run.md, section 6.
    #[must_use]
    pub fn waiting(&self) -> &[(Time, Option<Token>)] {
        &self.waiting
    }

    /// Actual final root output after every lower terminal and main Turn.
    /// Contract: domain/run.md, section 13; testing-strategy.md, section 7.
    #[must_use]
    pub fn answer(&self) -> &run::Answer {
        self.answer.as_ref().expect("the world has settled")
    }

    /// Check outcomes actually delivered to the agent, in request order.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn checked(&self) -> &[bool] {
        &self.checked
    }

    /// Time-ordered host check notices; true starts a span and false ends it.
    #[must_use]
    pub fn check_notices(&self) -> &[(Time, bool)] {
        &self.check_notices
    }

    /// Actual check and abort terminal times, for outside span checks.
    #[must_use]
    pub fn check_terminals(&self) -> &[Time] {
        &self.check_terminals
    }

    /// Typed push replies actually delivered by the scripted host.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn pushes(&self) -> &[run::Delivery] {
        &self.pushes
    }

    /// Bytes of the fixture code in the shared checkout, independent of the agent.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn code(&self) -> Vec<u8> {
        self.root
            .map_or_else(Vec::new, |root| self.disk.load(root, fixture::CODE, u64::MAX).expect("fixture code exists").0)
    }

    /// Bytes retained by a successful host push; empty when nothing landed.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn landed(&self) -> &[u8] {
        &self.landed
    }

    /// Native observations drained and counted when this parent has no retained sink.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn agent_facts_not_written(&self) -> u64 {
        self.agent_facts_not_written
    }

    /// Actual native observations retained by this scripted parent.
    #[must_use]
    pub fn facts(&self) -> &[Fact] {
        &self.facts
    }

    /// Optional content observations omitted by the capture buffer.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn lost(&self) -> u64 {
        self.agent.content_lost()
    }

    /// Provider queries observed at its boundary, including returned tool IDs.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn prompts(&self) -> &[provider::api::Query] {
        &self.prompts
    }

    /// Host-owned recorded decisions, distinct from relay attempts.
    /// Contract: domain/run.md, sections 5.2 and 13.
    #[must_use]
    pub const fn host_decisions(&self) -> u32 {
        self.host_decisions
    }

    /// Actual host relay terminals, retained only by this outside world.
    /// Contract: domain/run.md, sections 5.2 and 13.
    #[must_use]
    pub fn host_terminals(&self) -> &[(run::RelayName, Time, run::HostReply)] {
        &self.host_terminals
    }

    /// Actual observed cancellation/failed-run boundary time.
    /// Contract: domain/run.md, sections 5.2 and 10.
    #[must_use]
    pub const fn host_shutdown_at(&self) -> Option<Time> {
        self.host_history.shutdown_at()
    }

    /// Immutable opaque relay observations for replay and recovery checks.
    /// Contract: domain/run.md, sections 5.2 and 14.
    #[must_use]
    pub fn host_submissions(&self) -> &[crate::host_referee::Submission] {
        self.host_history.submissions()
    }

    /// Stable host names and submission times observed from actual delivery.
    /// Contract: domain/run.md, sections 8.2 and 13; testing-strategy.md, section 7.
    #[must_use]
    pub fn delivery_names(&self) -> &[(run::CallName, Time)] {
        &self.delivery_names
    }

    /// Ordered boundary trace for same-seed replay comparisons.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        self.trace.lines()
    }

    /// Injected monotonic time when the host received the answer.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn answered_at(&self) -> Time {
        self.answered.expect("the world has settled")
    }

    /// Actual safety checks and met liveness obligations from the shared referee.
    ///
    /// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
    #[must_use]
    pub fn judged(&self) -> (u64, u64) {
        self.referee.judged()
    }
}

/// The scripted host's complete charter, supplied to root admission by its world.
#[must_use]
pub fn charter(settings: &Settings) -> run::Charter {
    use run::charter::{Grants, Llm, Tools};

    use run::outcome::{ChangeSpec, FieldRule, ItemRule, ItemSpec, OutcomeSpec, TextSpec, VerdictRule};
    let change = matches!(settings.job, Job::Coding | Job::Delegating | Job::Wandering | Job::MidChange);
    let review = settings.job == Job::Review;
    let rule = VerdictRule {
        name: b"request-changes".as_slice().into(),
        text_max: 1024,
        fields: Box::new([]),
        items: ItemSpec {
            min: 1,
            max: 4,
            kinds: Box::new([ItemRule {
                kind: b"blocking".as_slice().into(),
                fields: Box::new([
                    FieldRule { name: b"path".as_slice().into(), max: 1024 },
                    FieldRule { name: b"body".as_slice().into(), max: 1024 },
                ]),
            }]),
        },
    };
    let llm = main_model(settings.window);
    run::Charter {
        mode: run::Mode::Finite,
        reserve: finishing_reserve(settings, change),
        instructions: Box::new([]),
        brief: run::Brief {
            sections: Box::new([run::Section {
                title: b"Task".as_slice().into(),
                text: script::cue(settings.job).unwrap_or(b"Look into the code.").into(),
            }]),
        },

        grants: Grants {
            wait: true,
            deliver: if matches!(settings.job, Job::MidReport | Job::MidChange | Job::MarkerReport) {
                Some(ChangeSpec {
                    checks_must_pass: true,
                    fields: Box::new([FieldRule { name: b"ticket".as_slice().into(), max: 128 }]),
                })
            } else {
                None
            },
            tools: Tools { inspect: true, modify: settings.writable, shell: settings.writable },

            agents: true,
            host_tools: if settings.job == Job::HostTools {
                Box::new([run::HostTool {
                    name: b"host_action".as_slice().into(),
                    description: b"Opaque host write".as_slice().into(),
                    schema: br#"{"type":"object"}"#.as_slice().into(),
                    effect: run::HostEffect::Write,
                    timeout: Duration::from_millis(300),
                }])
            } else {
                Box::new([])
            },
        },
        outcome: OutcomeSpec {
            change: if change {
                Some(ChangeSpec {
                    checks_must_pass: true,
                    fields: Box::new([
                        smith_domain::run::outcome::FieldRule { name: b"title".as_slice().into(), max: 1024 },
                        smith_domain::run::outcome::FieldRule { name: b"body".as_slice().into(), max: 1024 },
                    ]),
                })
            } else {
                None
            },
            verdicts: if review { Box::new([rule]) } else { Box::new([]) },
            report: if !change && !review && settings.job != Job::Failing {
                Some(TextSpec {
                    max: 1024,
                    fields: Box::new([FieldRule { name: b"source".as_slice().into(), max: 128 }]),
                })
            } else {
                None
            },
            failure: if settings.job == Job::Failing {
                Some(TextSpec {
                    max: 1024,
                    fields: Box::new([FieldRule { name: b"cause".as_slice().into(), max: 128 }]),
                })
            } else {
                None
            },
        },
        budget: settings.budget,
        models: Box::new([Llm { effort: None, model: b"fake-2".as_slice().into(), ..llm.clone() }]),
        llm,
        conventions: Some(smith_domain::run::Conventions {
            guide: b"AGENTS.md".as_slice().into(),
            checks: b".temper/pre-pr".as_slice().into(),
        }),
        resume: settings.resume,
        waiting: settings.waiting,
    }
}

fn copy_answer(answer: &run::Answer) -> run::Answer {
    use run::outcome::Declared;
    match answer {
        run::Answer::Refused(refusal) => run::Answer::Refused(*refusal),
        run::Answer::Parked { spent, turns } => run::Answer::Parked { spent: *spent, turns: *turns },
        run::Answer::Failed { failure, spent, turns } => {
            run::Answer::Failed { failure: failure.clone(), spent: *spent, turns: *turns }
        }
        run::Answer::Accepted { outcome, spent, turns } => {
            let outcome = match outcome {
                Declared::Change(change) => Declared::Change(change.clone()),
                Declared::Verdict(verdict) => Declared::Verdict(verdict.clone()),
                Declared::Report(report) => Declared::Report(report.clone()),
                Declared::Failure(failure) => Declared::Failure(failure.clone()),
            };
            run::Answer::Accepted { outcome, spent: *spent, turns: *turns }
        }
    }
}

impl World {
    /// Bounded actual start/terminal chronology from Complete and callbacks.
    /// Native exact SDK usage is also retained on each physical wire Binding.
    /// Contract: domain/run.md, sections 9 and 14; scratch/client.md, section 4.
    #[must_use]
    pub fn completions(&self) -> &[CompletionObservation] {
        &self.completions
    }
}

// Independent outside scalar oracle from caller data and actual provider counts.
fn observed_price(prices: run::Prices, usage: llm::Usage) -> Option<u64> {
    let denominator = u128::from(prices.unit);
    let fresh = u128::from(usage.input_tokens.expect("scripted peer supplies count"));
    let numerator = fresh
        .checked_mul(u128::from(prices.input))?
        .checked_add(
            u128::from(usage.cache_write_tokens.expect("scripted peer supplies count"))
                .checked_mul(u128::from(prices.cache_write.unwrap_or(prices.input)))?,
        )?
        .checked_add(
            u128::from(usage.cache_read_tokens.expect("scripted peer supplies count"))
                .checked_mul(u128::from(prices.cached))?,
        )?
        .checked_add(
            u128::from(usage.output_tokens.expect("scripted peer supplies count"))
                .checked_mul(u128::from(prices.output))?,
        )?;
    let rounded = (numerator / denominator).checked_add(u128::from(numerator % denominator != 0))?;
    u64::try_from(rounded).ok()
}

/// Observe the actual Start batch before its admission and first provider query.
fn opening_observations(
    charter: &run::Charter,
    messages: &[run::Message],
) -> Vec<(Time, crate::messages_referee::Seen)> {
    let mut messages_seen = vec![(
        Time::ZERO,
        crate::messages_referee::Seen::Started {
            brief: charter.brief.sections.iter().any(|section| !section.title.is_empty() || !section.text.is_empty()),
            wait: charter.grants.wait,
            time: charter.budget.time,
        },
    )];
    for message in messages {
        let mut rendered = message.label.to_vec();
        rendered.extend_from_slice(b": ");
        rendered.extend_from_slice(&message.text);
        messages_seen.push((
            Time::ZERO,
            crate::messages_referee::Seen::Input { name: message.name, text: rendered.into_boxed_slice() },
        ));
    }
    messages_seen
}

fn configuration(window: u32) -> agent::Config {
    agent::Config {
        programs: Box::new([]),
        endpoints: Box::new([run::charter::Endpoint(0)]),
        models: [b"fake-1".as_slice(), b"fake-2".as_slice(), b"fake-3".as_slice()]
            .iter()
            .map(|name| smith_domain::ConfiguredModel {
                efforts: Box::new([]),
                prices: smith_domain::run::Prices {
                    input: 1,
                    cached: 1,
                    cache_write: None,
                    output: 1,
                    unit: 1_000_000,
                },
                protocol_allowance: 0,
                endpoint: run::charter::Endpoint(0),
                model: (*name).into(),
                window,
                output: 4096,
                head: Duration::from_secs(60),
                idle: Duration::from_secs(30),
            })
            .collect(),
    }
}

fn main_model(window: u32) -> run::charter::Llm {
    use run::charter::{Endpoint, Llm};
    Llm {
        effort: None,
        prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
        account: Some(0),
        endpoint: Endpoint(0),
        model: b"fake-1".as_slice().into(),
        window,
        output: 4096,
        dialect: 1,
    }
}

/// Host fixture allowance for every contract that can run checks.
fn finishing_reserve(settings: &Settings, change: bool) -> run::charter::Reserve {
    run::charter::Reserve {
        turns: 0,
        time: if change || matches!(settings.job, Job::MidReport | Job::MarkerReport) {
            settings.limits.run.check_timeout
        } else {
            Duration::ZERO
        },
    }
}

/// Build a reporting world with exactly these host account grants.
#[must_use]
pub fn world(settings: &Settings, charter: run::Charter, granted: &[u32]) -> World {
    let grants = granted
        .iter()
        .map(|account| Grant {
            renewable: true,
            name: GrantName { account: *account, generation: 1 },
            valid: Duration::from_secs(7200),
        })
        .collect();
    World::with_start(
        *settings,
        Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: agent::Window { turns: u32::MAX, bytes: u64::MAX },
            charter,
            workspace: None,
            grants,
            transcript: None,
            answered: Box::new([]),
        },
    )
}

#[cfg(test)]
mod bridge_tests {
    use super::*;
    use provider::api::{Finish, Line, Script, Turn};

    #[test]
    fn a_caller_start_and_host_reply_cross_the_parent_bridge() {
        let settings = Settings { job: Job::HostTools, ..Settings::calm(811) };
        let start = Event::Start {
            messages: Box::default(),
            answered: Box::default(),
            reply_to: ReplyTo::new(Token::new(71)),
            host_run: Token::new(73),
            identity: None,
            activation: 4,
            window: agent::Window { turns: u32::MAX, bytes: u64::MAX },
            charter: charter(&settings),
            workspace: None,
            grants: Box::new([Grant {
                renewable: true,
                name: GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(7200),
            }]),
            transcript: None,
        };
        let mut world = World::with_start(settings, start);
        world.enable_parent_host_calls();
        assert!(!world.drive(20_000), "the world yields before answering the host call");
        let pending = world.pending_host_calls();
        let [call] = pending.as_slice() else {
            panic!("one pending host call");
        };
        assert_eq!(call.host_run, Token::new(73));
        assert_eq!(call.name.activation, 4);
        assert_eq!(world.host_submissions().len(), 1);
        let relay = call.relay;
        let answer = run::HostAnswer::new(b"parent decision".as_slice().into(), false).expect("bounded answer");
        assert_eq!(
            world.return_host_reply(run::RelayName { owner: Token::new(999), attempt: 1 }, run::HostReply::Busy),
            Err("no pending host relay")
        );
        world.return_host_reply(relay, run::HostReply::Answered(answer.clone())).expect("pending relay");
        assert_eq!(world.return_host_reply(relay, run::HostReply::Busy), Err("host relay terminal is already queued"));
        assert!(world.pending_host_calls().is_empty());
        assert!(world.drive(20_000));
        assert!(matches!(world.answer(), run::Answer::Accepted { .. }));
        assert_eq!(world.host_terminals().len(), 1);
        assert_eq!(world.host_terminals()[0].2, run::HostReply::Answered(answer));
    }

    #[test]
    fn a_caller_named_message_host_tool_keeps_its_feedback() {
        let settings = Settings { job: Job::HostTools, ..Settings::calm(819) };
        let mut charter = charter(&settings);
        charter.grants.host_tools[0].name = b"message".as_slice().into();
        let start = Event::Start {
            messages: Box::default(),
            answered: Box::default(),
            reply_to: ReplyTo::new(Token::new(71)),
            host_run: Token::new(73),
            identity: None,
            activation: 4,
            window: agent::Window { turns: u32::MAX, bytes: u64::MAX },
            charter,
            workspace: None,
            grants: Box::new([Grant {
                renewable: true,
                name: GrantName { account: 0, generation: 1 },
                valid: Duration::from_secs(7200),
            }]),
            transcript: None,
        };
        let scripts = Box::new([Script {
            cue: b"@hosttools".as_slice().into(),
            turns: Box::new([
                Turn {
                    lines: Box::new([Line::Call {
                        name: b"message".as_slice().into(),
                        arguments: b"{}".as_slice().into(),
                    }]),
                    finish: Finish::ToolCalls,
                    tokens: 8,
                },
                Turn {
                    lines: Box::new([Line::Call {
                        name: b"finish".as_slice().into(),
                        arguments: br#"{"report":"Host completed.","source":"host"}"#.as_slice().into(),
                    }]),
                    finish: Finish::ToolCalls,
                    tokens: 8,
                },
            ]),
        }]);
        let mut world = World::with_workspace_scripts_start(settings, Checkout::new(), scripts, start);
        world.enable_parent_host_calls();
        assert!(!world.drive(20_000), "the world yields before answering the host call");
        let pending = world.pending_host_calls();
        let [call] = pending.as_slice() else { panic!("one pending host call") };
        assert_eq!(call.tool.as_ref(), b"message");
        let relay = call.relay;
        let answer = run::HostAnswer::new(b"parent decision".as_slice().into(), false).expect("bounded answer");
        world.return_host_reply(relay, run::HostReply::Answered(answer.clone())).expect("pending relay");
        assert!(world.drive(20_000));
        assert!(matches!(world.answer(), run::Answer::Accepted { .. }));
        assert_eq!(world.host_terminals()[0].2, run::HostReply::Answered(answer));
    }
}
