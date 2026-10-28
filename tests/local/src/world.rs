//! A typed local host over a durable fake store and the shared scripted
//! agent-provider translation (domain/host.md, sections 8–10).

mod headless;

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use skein_fake_checkout::git::Remote;
use skein_fake_checkout::{Checkout, Exit as FakeExit, Program, git};
use skein_fake_llm_domain as provider;
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};
use skein_world::domain::{Referee, Trace};
use smith_agent_world::{self as agent_world, checkout_io, translate};
use smith_domain::{self as agent, run, tools};
use smith_inline_agent::{Below, Lower};
use smith_local_domain::{
    self as local, ChatState, Contract, DeliveryRecord, DeliveryState, Event, ExitStatus, GitOp, GitResult, Request,
    StoreFailure,
};

use crate::git::History;
use crate::referee::{Meeting, Seen};

/// Typed fake transcript store, movable across independent invocations.
#[derive(Debug, Default)]
pub struct Store {
    state: Option<ChatState>,
    turns: Vec<agent::Turn>,
    window: usize,
    history_override: Option<agent::Transcript>,
    deliveries: BTreeMap<run::CallName, DeliveryRecord>,
    disk: Option<Checkout>,
    history: Option<History>,
    git_head: Option<u64>,
    git_expected: Option<u64>,
    commits: Vec<(run::CallName, u32)>,
}

impl Store {
    /// Replay the first durable terminal for the same delivery call name.
    #[must_use]
    pub fn delivery_answer(&self, name: run::CallName) -> Option<run::Delivery> {
        self.deliveries.get(&name)?.answer(name)
    }

    /// The last durable delivery call name.
    #[must_use]
    pub fn delivery_name(&self) -> Option<run::CallName> {
        Some(*self.deliveries.last_key_value()?.0)
    }

    /// Number of durable delivery answers still missing from a saved turn.
    #[must_use]
    pub fn delivery_answers(&self) -> usize {
        self.deliveries.values().filter(|record| matches!(record.state, DeliveryState::Answer(_))).count()
    }

    fn history(&self) -> Option<agent::Transcript> {
        if let Some(history) = &self.history_override {
            return Some(history.clone());
        }
        let turns = self.turns.get(self.window..)?;
        let first = turns.first()?;
        Some(agent::Transcript {
            version: agent::session::record::VERSION,
            endpoint: first.endpoint,
            dialect: first.dialect,
            turns: turns.to_vec().into_boxed_slice(),
        })
    }

    /// Last durable activation number, including a start whose agent never ran.
    #[must_use]
    pub fn activation(&self) -> u64 {
        self.state.map_or(0, |state| state.activation)
    }

    /// Opaque chat identity in the last durably acknowledged metadata.
    #[must_use]
    pub fn identity(&self) -> Option<run::Identity> {
        self.state.map(|state| state.identity)
    }

    /// Number of durable turns across activations.
    #[must_use]
    pub fn turns(&self) -> usize {
        self.turns.len()
    }

    /// Last person message named by a durable turn.
    #[must_use]
    pub fn read(&self) -> Option<Token> {
        self.state.and_then(|state| state.read)
    }

    /// Inject an unsupported transcript version for refusal stories.
    pub fn refuse_version(&mut self) {
        self.history_override = Some(agent::Transcript {
            version: agent::session::record::VERSION.saturating_add(1),
            endpoint: Some(agent::session::llm::Endpoint(0)),
            dialect: 1,
            turns: Box::new([]),
        });
    }
}

/// A boundary at which the world drops its domain without settling more work.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cut {
    /// The next activation metadata is durable; its terminal is not delivered.
    AfterSaveState,
    /// A numbered turn request reached the store but was not written.
    BeforeSaveTurn(u32),
    /// A numbered turn is durable and its terminal reached the domain.
    AfterTurnSaved(u32),
    /// A commit and its delivery record are durable, but its child terminal is lost.
    AfterSaveDelivery,
    /// The second delivery answer is durable, but its child terminal is lost.
    AfterSecondSaveDelivery,
    /// An intent is durable and no commit has begun.
    AfterIntent,
    /// A commit landed, but its result has not reached the host domain.
    AfterCommit(u32),
    /// A push landed, but its terminal has not reached the host domain.
    BeforePushTerminal,
    /// The push terminal reached the host, but its answer has not been saved.
    AfterPushTerminal,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Goal {
    Waiting,
    Parked,
    Cut,
    Cancelled,
    Exit,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Scenario {
    Chat,
    Finishing,
    Workspace,
    Delegating,
    PlainChange,
    PlainNothing,
    GitChange,
    GitMarker,
    PushLands,
    PushStale,
    SecondFails,
    MidReport,
    TwoDeliveries,
}

#[derive(Clone, Copy, Debug)]
enum FirstFailure {
    None,
    Reject,
    Exhaust,
    Refused,
    AccountRefused,
    Used,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PushFault {
    None,
    Next,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// One scripted uncertainty at the git boundary, supplied by a local-world story.
pub enum GitFault {
    /// The deadline passes before a commit makes any effect.
    NoEffect,
    /// The commit lands as its child deadline passes.
    Deadline,
    /// The commit lands but its receipt head read fails.
    HeadUnreadable,
    /// The commit and all later inspections are unreadable this invocation.
    InspectionUnreadable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeadChange {
    None,
    BeforeDelivery,
}

/// Which store request should fail once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreFault {
    /// Loading the chat fails.
    Load,
    /// Saving activation metadata fails.
    State,
    /// Saving a turn fails.
    Turn,
}

/// One invocation; the store can be taken into a later invocation.
#[derive(Debug)]
pub struct World {
    domain: local::Domain,
    env: Env<local::Limits>,
    out: Queue<Request>,
    provider: provider::Domain,
    provider_env: Env<provider::Config>,
    provider_out: Queue<provider::Request>,
    disk: Checkout,
    disk_start: skein_fake_checkout::git::Tree,
    history: Option<History>,
    git_head: Option<u64>,
    git_expected: Option<u64>,
    commits: Vec<(run::CallName, u32)>,
    lower_agent: Option<Token>,
    pending: BTreeMap<Token, (tools::Grants, Box<[agent::llm::Served]>, agent::llm::Asker)>,
    cancelled: BTreeSet<Token>,
    events: VecDeque<Event>,
    store: Store,
    last_delivery: Option<run::Delivery>,
    shown: Vec<Box<[u8]>>,
    notices: Vec<smith_local_domain::Notice>,
    readings: Vec<local::Reading>,
    person: Option<crate::person::Person>,
    reached: BTreeSet<Goal>,
    exit: Option<ExitStatus>,
    completions: u32,
    prompt_assistants: Vec<usize>,
    prompt_affinities: Vec<agent::llm::Affinity>,
    child_prompts: Vec<(tools::Grants, Box<[agent::llm::Served]>)>,
    prompt_texts: Vec<Vec<u8>>,
    tool_results: Vec<Vec<u8>>,
    activation_turns: u32,
    cut: Option<Cut>,
    slow_store: bool,
    delayed_turns: VecDeque<u32>,
    slow_git: bool,
    push_fault: PushFault,
    head_change: HeadChange,
    git_fault: Option<GitFault>,
    held_git: VecDeque<(Token, GitResult)>,
    first_failure: FirstFailure,
    credential_requests: u32,
    missing_credential: Option<u32>,
    referee: Referee<Meeting>,
    trace: Trace,
    seed: u64,
    collect_facts: bool,
    store_fault: Option<StoreFault>,
}

impl World {
    /// A chat with one account, no workspace and a scripted fake provider.
    #[must_use]
    pub fn new(seed: u64) -> World {
        Self::with_store(seed, Store::default())
    }

    /// A chat whose first provider turn finishes before reading the follow-up.
    #[must_use]
    pub fn finishing(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::Finishing)
    }

    /// A workspace chat whose main conversation may delegate a read-only reading.
    #[must_use]
    pub fn with_delegation(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::Delegating)
    }

    /// Same chat with no observation capacity; domain behavior must agree.
    #[must_use]
    pub fn without_facts(seed: u64) -> World {
        let mut world = Self::with_capacity(seed, Store::default(), true, 2, 0, Scenario::Chat);
        world.collect_facts = false;
        world
    }

    /// A one-turn unsaved window for store-pressure stories.
    #[must_use]
    pub fn tight_unsaved(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 1, 64, Scenario::Chat)
    }

    /// A fresh local domain over an existing durable fake store.
    #[must_use]
    pub fn with_store(seed: u64, store: Store) -> World {
        Self::with_resume(seed, store, true)
    }

    /// Start fresh despite saved history the agent would refuse.
    #[must_use]
    pub fn with_store_fresh(seed: u64, store: Store) -> World {
        Self::with_resume(seed, store, false)
    }

    fn with_resume(seed: u64, store: Store, resume: bool) -> World {
        Self::with_capacity(seed, store, resume, 2, 64, Scenario::Chat)
    }

    /// A writable plain directory with a source file and contained test command.
    #[must_use]
    pub fn with_workspace(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::Workspace)
    }

    /// A writable plain directory under a Change contract.
    #[must_use]
    pub fn with_plain_change(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::PlainChange)
    }

    /// A change declaration over an untouched plain directory.
    #[must_use]
    pub fn with_plain_nothing(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::PlainNothing)
    }

    /// A writable git checkout under a Change contract.
    #[must_use]
    pub fn with_git_change(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::GitChange)
    }

    /// A merge with one unresolved marker in its original conflict path.
    #[must_use]
    pub fn with_git_marker(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::GitMarker)
    }

    /// Two ordered git mounts with a commit fault in the second.
    #[must_use]
    pub fn with_second_commit_failure(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::SecondFails)
    }

    /// Resume the two-directory fixture after a crash cut.
    #[must_use]
    pub fn with_second_commit_failure_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::SecondFails)
    }

    /// A Report run separately granted checked mid-run delivery.
    #[must_use]
    pub fn with_mid_report(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::MidReport)
    }

    /// Resume a mid-run delivery fixture after a crash cut.
    #[must_use]
    pub fn with_mid_report_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::MidReport)
    }

    /// Two delivery calls in one turn, before that turn is saved.
    #[must_use]
    pub fn with_two_deliveries(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::TwoDeliveries)
    }

    /// Resume two delivery calls after both answers are durable.
    #[must_use]
    pub fn with_two_deliveries_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::TwoDeliveries)
    }

    /// Resume over the same transcript, checkout, graph and delivery record.
    #[must_use]
    pub fn with_git_change_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::GitChange)
    }

    /// A configured git change whose push reaches the remote.
    #[must_use]
    pub fn with_push(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::PushLands)
    }

    /// Resume a configured push after a crash cut.
    #[must_use]
    pub fn with_push_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::PushLands)
    }

    /// A configured git change whose branch moved before push.
    #[must_use]
    pub fn with_moved_remote(seed: u64) -> World {
        Self::with_capacity(seed, Store::default(), true, 2, 64, Scenario::PushStale)
    }

    /// Resume a configured push whose target had moved.
    #[must_use]
    pub fn with_moved_remote_store(seed: u64, store: Store) -> World {
        Self::with_capacity(seed, store, true, 2, 64, Scenario::PushStale)
    }

    /// Head of the remote branch in this fixture.
    #[must_use]
    pub fn remote_head(&self) -> Option<u64> {
        Some(self.history.as_ref()?.remote_head())
    }

    /// Provider prompts, including the next waking message.
    #[must_use]
    pub fn prompt_texts(&self) -> &[Vec<u8>] {
        &self.prompt_texts
    }

    /// Rendered host tool results reaching provider prompts.
    #[must_use]
    pub fn tool_results(&self) -> &[Vec<u8>] {
        &self.tool_results
    }

    /// The last durable delivery decision, if one was made.
    #[must_use]
    pub fn delivery(&self) -> Option<&run::Delivery> {
        self.last_delivery.as_ref()
    }

    /// Message stored by the fake git graph for its latest local commit.
    #[must_use]
    pub fn commit_message(&self) -> Option<&[u8]> {
        let history = self.history.as_ref()?;
        let head = self.git_head?;
        Some(history.commit_message(head))
    }

    /// Number of commits made under delivery names across invocations.
    #[must_use]
    pub fn delivery_commits(&self) -> usize {
        self.commits.len()
    }

    /// The fake disk observed after the agent has settled its file operations.
    #[must_use]
    pub fn disk(&self) -> &Checkout {
        &self.disk
    }

    fn with_capacity(seed: u64, store: Store, resume: bool, unsaved: u32, facts: u32, scenario: Scenario) -> World {
        Self::with_mode(
            seed,
            store,
            resume,
            (unsaved, facts),
            scenario,
            local::Mode::Chat { waiting: Duration::from_secs(30) },
            None,
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one constructor keeps scenario fixtures and their typed configuration together"
    )]
    fn with_mode(
        seed: u64,
        mut store: Store,
        resume: bool,
        capacities: (u32, u32),
        scenario: Scenario,
        mode: local::Mode,
        budget: Option<run::Budget>,
    ) -> World {
        let (unsaved, facts) = capacities;
        let meeting = Meeting::after(store.activation(), store.state.map_or(0, |state| state.next_message))
            .prior_delivery(store.deliveries.values(), &store.commits);
        let meeting = match scenario {
            Scenario::Chat | Scenario::Finishing => meeting,
            Scenario::SecondFails => meeting.writable(&[0, 1], &[1, 2]),
            Scenario::Workspace
            | Scenario::Delegating
            | Scenario::PlainChange
            | Scenario::PlainNothing
            | Scenario::GitChange
            | Scenario::GitMarker
            | Scenario::PushLands
            | Scenario::PushStale
            | Scenario::MidReport
            | Scenario::TwoDeliveries => meeting.writable(&[0], &[1]),
        };
        let referee = Referee::new(meeting);
        let limits = local::Limits {
            smith: agent_world::LIMITS,
            agent: local::AgentLimits::inline(agent_world::LIMITS, 2, Duration::from_secs(60), Duration::from_secs(5))
                .expect("bounded inline child"),
            endpoints: Box::new([run::charter::Endpoint(0)]),
            chat_bytes: 64,
            text_bytes: 4096,
            models: 1,
            line_bytes: 1024,
            show_bytes: 4096,
            unsaved,
            facts,
        };
        let saved_disk = store.disk.take();
        let restored = saved_disk.is_some();
        let mut disk = saved_disk.unwrap_or_default();
        let mut history = store.history.take();
        let mut git_head = store.git_head.take();
        let git_expected = store.git_expected.take().or(git_head).or(Some(1));
        let workspace = if matches!(scenario, Scenario::Chat | Scenario::Finishing) {
            None
        } else {
            if matches!(
                scenario,
                Scenario::GitChange
                    | Scenario::GitMarker
                    | Scenario::SecondFails
                    | Scenario::MidReport
                    | Scenario::TwoDeliveries
                    | Scenario::PushLands
                    | Scenario::PushStale
            ) && !restored
            {
                let mut tree = BTreeMap::from([(b"src/lib.rs".to_vec(), b"pub fn answer() -> u32 { 42 }\n".to_vec())]);
                if matches!(
                    scenario,
                    Scenario::GitChange
                        | Scenario::SecondFails
                        | Scenario::MidReport
                        | Scenario::TwoDeliveries
                        | Scenario::PushLands
                        | Scenario::PushStale
                ) {
                    tree.insert(b".temper/pre-pr".to_vec(), b"#!checks\nsrc/lib.rs 43\n".to_vec());
                }
                let mut graph = History::new(tree);
                git::clone_repository(&mut graph, &mut disk, b"repo", b"work").expect("fixture git clone");
                git::check_out(&graph, &mut disk, b"work", 1).expect("fixture initial checkout");
                if scenario == Scenario::PushStale {
                    graph.move_remote();
                }
                if scenario == Scenario::GitMarker {
                    disk.write(b"work/src/lib.rs", b"<<<<<<< ours\nleft\n=======\nright\n>>>>>>> theirs\n");
                    disk.write(b"work/.git/MERGE_HEAD", &1_u64.to_le_bytes());
                    disk.write(b"work/.git/temper-conflicts/0", b"src/lib.rs");
                }
                history = Some(graph);
                git_head = Some(1);
            } else if !restored {
                disk.write(b"work/src/lib.rs", b"pub fn answer() -> u32 { 42 }\n");
                if scenario != Scenario::PlainNothing {
                    disk.write(b"work/.temper/pre-pr", b"#!checks\nsrc/lib.rs 43\n");
                }
            }
            disk.program(
                b"cargo test",
                Program {
                    duration: std::time::Duration::from_millis(1),
                    output: b"test result: ok. 1 passed\n".to_vec(),
                    exit: FakeExit::Code(0),
                    changes: Vec::new(),
                },
            );
            let root = if restored { 1 } else { disk.root(b"work") };
            let first = run::Directory {
                host: Box::from(b"/host".as_slice()),
                name: b"work".as_slice().into(),
                root: checkout_io::token(root),
                writable: true,
                git: matches!(
                    scenario,
                    Scenario::GitChange
                        | Scenario::GitMarker
                        | Scenario::SecondFails
                        | Scenario::MidReport
                        | Scenario::TwoDeliveries
                        | Scenario::PushLands
                        | Scenario::PushStale
                ),
                conflicts: if scenario == Scenario::GitMarker {
                    Box::new([b"src/lib.rs".as_slice().into()])
                } else {
                    Box::new([])
                },
            };
            let directories = if scenario == Scenario::SecondFails {
                if !restored {
                    disk.write(b"work2/other.txt", b"changed");
                }
                let second_root = if restored { 2 } else { disk.root(b"work2") };
                Box::new([
                    first,
                    run::Directory {
                        host: Box::from(b"/host".as_slice()),
                        name: b"work2".as_slice().into(),
                        root: checkout_io::token(second_root),
                        writable: true,
                        git: true,
                        conflicts: Box::new([]),
                    },
                ]) as Box<[run::Directory]>
            } else {
                Box::new([first]) as Box<[run::Directory]>
            };
            Some(run::Workspace { directories })
        };
        let change = matches!(
            scenario,
            Scenario::PlainChange
                | Scenario::PlainNothing
                | Scenario::GitChange
                | Scenario::GitMarker
                | Scenario::SecondFails
                | Scenario::PushLands
                | Scenario::PushStale
        );
        let instructions: &[u8] = if matches!(mode, local::Mode::Chat { .. }) && scenario == Scenario::Chat {
            b"@local-chat Assist the person."
        } else if scenario == Scenario::Finishing {
            b"@local-finish"
        } else if scenario == Scenario::Delegating {
            b"@local-delegate Delegate a reading, then answer."
        } else if scenario == Scenario::TwoDeliveries {
            b"@local-two-deliveries Deliver twice, then report."
        } else if scenario == Scenario::MidReport {
            b"@midreport Deliver then report."
        } else if scenario == Scenario::PlainNothing {
            b"@local-nothing Declare an unchanged result."
        } else if scenario == Scenario::GitMarker {
            b"@local-marker Resolve the merge."
        } else if change {
            b"@local-change Make a checked change."
        } else if workspace.is_some() {
            b"@local-workspace Assist the person."
        } else {
            b"@chat Assist the person."
        };
        let contract = if change {
            Contract::Change(run::outcome::ChangeSpec {
                checks_must_pass: true,
                fields: Box::new([
                    run::outcome::FieldRule { name: b"title".as_slice().into(), max: 256 },
                    run::outcome::FieldRule { name: b"body".as_slice().into(), max: 1024 },
                ]),
            })
        } else {
            Contract::Report(run::outcome::TextSpec { max: 2048, fields: Box::new([]) })
        };
        let config = local::Config {
            live_text: false,
            protocol_allowances: Box::new([0]),
            reserve: run::charter::Reserve { turns: 2, time: Duration::from_secs(300) },
            chat: b"main".as_slice().into(),
            instructions: instructions.into(),
            // This persistence fixture asks for a greeting before waiting.
            // Its charter therefore supplies actual initial work.
            brief: run::charter::Brief {
                sections: Box::new([run::charter::Section {
                    title: Box::default(),
                    text: b"Greet the person, then wait for their questions.".as_slice().into(),
                }]),
            },
            progress: Box::new([smith_domain::session::Progress {
                head: Duration::from_secs(60),
                idle: Duration::from_secs(30),
            }]),
            efforts: Box::new([Box::new([])]),
            models: Box::new([run::charter::Llm {
                effort: None,
                prices: run::Prices { input: 1, cached: 1, cache_write: None, output: 1, unit: 1_000_000 },
                dialect: 1,
                account: Some(0),
                endpoint: run::charter::Endpoint(0),
                model: b"scripted".as_slice().into(),
                window: 8192,
                output: 256,
            }]),
            sub_agents: scenario == Scenario::Delegating,
            budget: budget.unwrap_or(agent_world::BUDGET),
            conventions: None,
            contract,
            deliver: if matches!(scenario, Scenario::MidReport | Scenario::TwoDeliveries) {
                Some(run::outcome::ChangeSpec {
                    checks_must_pass: true,
                    fields: Box::new([run::outcome::FieldRule { name: b"ticket".as_slice().into(), max: 128 }]),
                })
            } else {
                None
            },
            title_field: if matches!(scenario, Scenario::MidReport | Scenario::TwoDeliveries) {
                b"ticket".as_slice().into()
            } else {
                b"title".as_slice().into()
            },
            mode,
            resume,
            accounts: Box::new([0]),
            workspace,
            push: if matches!(scenario, Scenario::PushLands | Scenario::PushStale) {
                Some(Box::new([Some(local::PushTarget {
                    remote: b"repo".as_slice().into(),
                    branch: b"main".as_slice().into(),
                })]))
            } else {
                None
            },
        };
        let provider_config = smith_session_world::Settings::calm(seed).provider;
        let disk_start = disk.tree(b"work");
        let provider = agent_world::scripted_provider(&provider_config, seed ^ 0x25);
        let out = Queue::with_capacity(local::max_out(&limits));
        let domain = local::Domain::new(config, &limits, seed ^ 0x17).expect("world config fits local limits");
        World {
            domain,
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            out,
            provider,
            provider_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: provider_config },
            provider_out: Queue::uncapped(),
            disk,
            disk_start,
            history,
            git_head,
            git_expected,
            commits: store.commits.clone(),
            lower_agent: None,
            pending: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            events: VecDeque::new(),
            last_delivery: store.deliveries.values().rev().find_map(|record| match &record.state {
                DeliveryState::Answer(delivery) => Some(delivery.clone()),
                DeliveryState::Intent(_) => None,
            }),
            store,
            shown: Vec::new(),
            notices: Vec::new(),
            readings: Vec::new(),
            person: None,
            reached: BTreeSet::new(),
            exit: None,
            completions: 0,
            prompt_assistants: Vec::new(),
            prompt_affinities: Vec::new(),
            child_prompts: Vec::new(),
            prompt_texts: Vec::new(),
            tool_results: Vec::new(),
            activation_turns: 0,
            cut: None,
            slow_store: false,
            delayed_turns: VecDeque::new(),
            slow_git: false,
            push_fault: PushFault::None,
            head_change: HeadChange::None,
            git_fault: None,
            held_git: VecDeque::new(),
            first_failure: FirstFailure::None,
            credential_requests: 0,
            missing_credential: None,
            referee,
            trace: Trace::default(),
            seed,
            collect_facts: true,
            store_fault: None,
        }
    }

    /// Take the fake store after ending or dropping this invocation.
    #[must_use]
    pub fn into_store(mut self) -> Store {
        self.store.disk = Some(self.disk);
        self.store.history = self.history;
        self.store.git_head = self.git_head;
        self.store.git_expected = self.git_expected;
        self.store.commits = self.commits;
        self.store
    }

    /// Make the first completion reject its initial credential generation.
    pub fn reject_first_credential(&mut self) {
        self.first_failure = FirstFailure::Reject;
    }

    /// Make the first completion report account exhaustion.
    pub fn exhaust_first_account(&mut self) {
        self.first_failure = FirstFailure::Exhaust;
    }

    /// Let the first completion permanently refuse its model, keeping its diagnostic.
    pub fn refuse_first_model(&mut self) {
        self.first_failure = FirstFailure::Refused;
    }

    /// Make the provider refuse the first completion's account.
    pub fn refuse_first_account(&mut self) {
        self.first_failure = FirstFailure::AccountRefused;
    }

    /// Reject a failure diagnostic found in a durable turn or any provider prompt.
    #[must_use]
    pub fn diagnostic_in_history(&self, diagnostic: &[u8]) -> bool {
        self.prompt_texts.iter().any(|text| text.windows(diagnostic.len()).any(|part| part == diagnostic))
            || self.store.turns.iter().any(|turn| {
                turn.messages.iter().any(|message| {
                    message.content.iter().any(|block| {
                        matches!(block, agent::session::llm::Block::Text { text, .. } | agent::session::llm::Block::Refusal { text, .. }
                    if text.windows(diagnostic.len()).any(|part| part == diagnostic))
                    })
                })
            })
    }

    /// Hold store turn acknowledgements until released by the story.
    pub fn slow_store(&mut self) {
        self.slow_store = true;
    }

    /// Delay typed git terminals until the story releases them.
    pub fn slow_git(&mut self) {
        self.slow_git = true;
    }

    /// Make a landed commit's terminal uncertain without issuing another commit.
    pub fn uncertain_git(&mut self, fault: GitFault) {
        self.git_fault = Some(fault);
    }

    /// Fail the next typed push operation.
    pub fn fail_next_push(&mut self) {
        self.push_fault = PushFault::Next;
    }

    /// Make an outside local commit after the run starts, before delivery surveys it.
    pub fn commit_meanwhile(&mut self) {
        self.head_change = HeadChange::BeforeDelivery;
    }

    /// Release one git terminal already computed by the fake checkout.
    pub fn release_git(&mut self) -> bool {
        let Some((owner, result)) = self.held_git.pop_front() else { return false };
        self.events.push_back(Event::Git { owner, result });
        true
    }

    /// Fail the next store request of the selected kind.
    pub fn fail_store(&mut self, fault: StoreFault) {
        self.store_fault = Some(fault);
    }

    /// Whether the local host exited and with what status.
    #[must_use]
    pub fn exit(&self) -> Option<ExitStatus> {
        self.exit
    }

    /// Release the oldest delayed turn acknowledgement.
    pub fn release_turn(&mut self) -> bool {
        let Some(number) = self.delayed_turns.pop_front() else { return false };
        self.events.push_back(Event::TurnSaved { number });
        true
    }

    /// Number of delayed turn acknowledgements.
    #[must_use]
    pub fn delayed_turns(&self) -> usize {
        self.delayed_turns.len()
    }

    /// Number of credential requests actually made by the local host.
    #[must_use]
    pub fn credential_requests(&self) -> u32 {
        self.credential_requests
    }

    /// The person interrupts the live run.
    pub fn interrupt(&mut self) {
        self.events.push_back(Event::Interrupt);
    }

    /// Leave content-free facts undrained to show they cannot change behavior.
    pub fn ignore_facts(&mut self) {
        self.collect_facts = false;
    }

    /// Number of facts dropped because the caller did not drain them.
    #[must_use]
    pub fn facts_lost(&self) -> u64 {
        self.domain.facts_lost()
    }

    /// Boundary chronology for deterministic replay.
    #[must_use]
    pub fn trace(&self) -> &[String] {
        self.trace.lines()
    }

    /// Number of independent referee checks performed.
    #[must_use]
    pub fn judged(&self) -> (u64, u64) {
        self.referee.judged()
    }

    /// Attach a person whose actions follow visible observations.
    pub fn person(&mut self, person: crate::person::Person) {
        self.person = Some(person);
    }

    /// End input while draining the live activation.
    pub fn close(&mut self) {
        self.events.push_back(Event::Closed);
    }

    /// Drive through the run's terminal and input closure to host exit.
    pub fn drive_to_exit(&mut self, iterations: u32) -> bool {
        self.drive_until(iterations, Goal::Exit)
    }

    fn act(&mut self, act: Option<crate::person::Act>) {
        match act {
            Some(crate::person::Act::Type(text)) => self.line(&text),
            Some(crate::person::Act::Interrupt) => self.interrupt(),
            Some(crate::person::Act::Close) => self.close(),
            None => {}
        }
    }

    fn saw(&mut self, shown: crate::person::Shown) {
        let act = self.person.as_mut().and_then(|person| person.saw(shown));
        self.act(act);
    }

    /// Drive until a cancelled answer is shown.
    pub fn drive_to_cancelled(&mut self, iterations: u32) -> bool {
        self.drive_until(iterations, Goal::Cancelled)
    }

    /// Notices shown to the person by kind.
    #[must_use]
    pub fn readings(&self) -> &[local::Reading] {
        &self.readings
    }

    #[must_use]
    pub fn notices(&self) -> &[smith_local_domain::Notice] {
        &self.notices
    }

    /// Type a person line; its name becomes durable before agent delivery.
    pub fn line(&mut self, text: &[u8]) {
        self.events.push_back(Event::Line { text: text.into() });
    }

    /// Text the local host asked to show, in order.
    #[must_use]
    pub fn shown(&self) -> &[Box<[u8]>] {
        &self.shown
    }

    /// Number of durable turns in the fake store.
    #[must_use]
    pub fn saved_turns(&self) -> usize {
        self.store.turns.len()
    }

    /// Whether the child reported that it waits for another person message.
    #[must_use]
    pub fn waiting(&self) -> bool {
        self.reached.contains(&Goal::Waiting)
    }

    /// Number of actual fake provider completion calls.
    #[must_use]
    pub fn completions(&self) -> u32 {
        self.completions
    }

    /// Actual child completion prompts, with their workspace authority and served tools.
    #[must_use]
    pub fn child_prompts(&self) -> &[(tools::Grants, Box<[agent::llm::Served]>)] {
        &self.child_prompts
    }

    /// Affinity in every actual native child completion prompt.
    #[must_use]
    pub fn prompt_affinities(&self) -> &[agent::llm::Affinity] {
        &self.prompt_affinities
    }

    /// The chat identity after its metadata reaches the durable store.
    #[must_use]
    pub fn identity(&self) -> Option<run::Identity> {
        self.store.identity()
    }

    /// Number of assistant messages in each provider request.
    #[must_use]
    pub fn prompt_assistants(&self) -> &[usize] {
        &self.prompt_assistants
    }

    /// Last activation saved by this invocation.
    #[must_use]
    pub fn activation(&self) -> u64 {
        self.store.activation()
    }

    /// Run through the waiting interval until the agent parks.
    pub fn drive_to_park(&mut self, iterations: u32) -> bool {
        self.reached.remove(&Goal::Parked);
        self.drive_until(iterations, Goal::Parked)
    }

    /// Drop at a chosen durable-store boundary.
    pub fn drive_to_cut(&mut self, iterations: u32, cut: Cut) -> bool {
        self.cut = Some(cut);
        self.drive_until(iterations, Goal::Cut)
    }

    /// Drive at most `iterations` rounds until the first answered question waits.
    pub fn drive(&mut self, iterations: u32) -> bool {
        self.drive_until(iterations, Goal::Waiting)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the world drives one full boundary round with the crash cut beside each event"
    )]
    fn drive_until(&mut self, iterations: u32, goal: Goal) -> bool {
        for _ in 0..iterations {
            let act = self.person.as_mut().and_then(crate::person::Person::step);
            self.act(act);
            if self.domain.is_ready() {
                local::resume(&mut self.domain, &self.env, &mut self.out);
                self.gather_facts();
            }
            if let Some(event) = self.events.pop_front() {
                let pushed = matches!(&event, Event::Git { result: GitResult::Pushed, .. });
                let saved = match &event {
                    Event::TurnSaved { number } => {
                        self.observe(Seen::TurnSaved { number: *number });
                        Some(*number)
                    }
                    Event::DeliverySaved { name } => {
                        let intent = matches!(
                            self.store.deliveries.get(name).map(|record| &record.state),
                            Some(DeliveryState::Intent(_))
                        );
                        self.observe(Seen::DeliverySaved { name: *name, intent });
                        None
                    }
                    Event::Below {
                        terminal:
                            Below::Completed { owner, .. } | Below::Failed { owner, .. } | Below::Cancelled { owner },
                        ..
                    } => {
                        self.observe(Seen::Completed { owner: *owner });
                        None
                    }
                    Event::Line { .. }
                    | Event::Slash(_)
                    | Event::Interrupt
                    | Event::Drop
                    | Event::Closed
                    | Event::ChatSelected { .. }
                    | Event::SelectChat { .. }
                    | Event::Loaded { .. }
                    | Event::StateSaved
                    | Event::Git { .. }
                    | Event::PlainStatus { .. }
                    | Event::StoreFailed { .. }
                    | Event::Credential { .. }
                    | Event::NoCredential { .. }
                    | Event::Below { .. }
                    | Event::Process(_) => None,
                };
                local::step(&mut self.domain, &self.env, event, &mut self.out);
                self.gather_facts();
                if pushed && self.cut == Some(Cut::AfterPushTerminal) {
                    self.reached.insert(Goal::Cut);
                    return true;
                }
                if let (Some(Cut::AfterTurnSaved(target)), Some(number)) = (self.cut, saved)
                    && target == number
                {
                    self.reached.insert(Goal::Cut);
                }
            }
            if self.domain.is_due(self.env.now) {
                local::fire(&mut self.domain, &self.env, &mut self.out);
                self.gather_facts();
            }
            while let Some(request) = self.out.pop() {
                self.request(request);
            }
            if self.provider.is_due(self.provider_env.now) {
                provider::fire(&mut self.provider, &self.provider_env, &mut self.provider_out);
            }
            while let Some(reply) = self.provider_out.pop() {
                let (to, result) = match reply {
                    provider::Request::Reply { to, result } => (to, result),
                    other @ (provider::Request::Piece { .. } | provider::Request::Hold { .. }) => {
                        panic!("typed provider expected a terminal reply, got {other:?}")
                    }
                };
                let owner = to.into_token();
                let (grants, served, asker) = self.pending.remove(&owner).expect("one provider terminal per call");
                let event = if self.cancelled.remove(&owner) {
                    self.below(Below::Cancelled { owner })
                } else {
                    match result {
                        Ok(answer) => self.below(Below::Completed {
                            owner,
                            completion: translate::completion(answer, grants, &served, asker),
                        }),
                        Err(error) => self.below(Below::Failed {
                            owner,
                            failure: translate::failure(error.error),
                            evidence: agent::llm::Evidence::Unknown,
                            detail: Box::new([]),
                            report: smith_domain::llm::Report::none(),
                        }),
                    }
                };
                self.events.push_back(event);
            }
            self.domain.reclaim();
            self.provider.reclaim();
            if self.collect_facts
                && let skein_world::domain::Verdict::Failed(failure) = self.referee.verdict()
            {
                panic!("seed {}: {failure}; trace: {:?}", self.seed, self.trace.lines());
            }
            let reached = self.reached.contains(&goal)
                && (goal != Goal::Waiting
                    || (self.activation_turns > 0
                        && !self.events.iter().any(|event| matches!(event, Event::TurnSaved { .. }))));
            if reached {
                if goal == Goal::Exit && self.collect_facts {
                    self.referee.assert_passed(self.seed);
                }
                return true;
            }
            if !self.domain.is_ready() && self.events.is_empty() && self.out.is_empty() {
                if self.person.is_some() && self.slow_store && !self.delayed_turns.is_empty() && self.pending.is_empty()
                {
                    self.release_turn();
                    continue;
                }
                if (self.slow_store && !self.delayed_turns.is_empty() && self.pending.is_empty())
                    || (self.slow_git && !self.held_git.is_empty())
                {
                    return false;
                }
                let next = [self.domain.next_deadline(), self.provider.next_deadline()].into_iter().flatten().min();
                let Some(next) = next else {
                    if self.person.is_some() {
                        continue;
                    }
                    return false;
                };
                self.env.now = next;
                self.provider_env.now = next;
            }
        }
        false
    }

    #[expect(clippy::too_many_lines, reason = "the test adapter settles each local request beside its terminal")]
    fn request(&mut self, request: Request) {
        match request {
            Request::Input { reading } => self.readings.push(reading),
            Request::Notice { notice } => {
                if notice == smith_local_domain::Notice::Waiting {
                    self.observe(Seen::Waiting);
                    self.reached.insert(Goal::Waiting);
                }
                if let local::Notice::NotDelivered { line: Some(name), .. } = notice {
                    self.observe(Seen::NotDelivered { name });
                }
                let text = smith_local_protocol::notice_text(
                    &notice,
                    self.domain.chat_name(),
                    self.domain.models(),
                    self.env.limits.show_bytes,
                );
                if text.as_ref() == b"interrupted: the run was cancelled\n" {
                    self.reached.insert(Goal::Cancelled);
                }
                self.shown.push(text);
                self.notices.push(notice.clone());
                self.saw(crate::person::Shown::Notice(notice));
            }
            Request::Show { text } => {
                if text.as_ref() == b"Chat parked" {
                    self.reached.insert(Goal::Parked);
                }
                if text.as_ref() == b"Run cancelled" {
                    self.reached.insert(Goal::Cancelled);
                }
                self.shown.push(text);
            }
            Request::Load => {
                if self.store_fault == Some(StoreFault::Load) {
                    self.store_fault = None;
                    self.events.push_back(Event::StoreFailed { turn: None, reason: StoreFailure::Read });
                } else {
                    self.events.push_back(Event::Loaded {
                        state: self.store.state,
                        transcript: self.store.history(),
                        deliveries: self.store.deliveries.values().cloned().collect::<Vec<_>>().into_boxed_slice(),
                    });
                }
            }
            Request::SaveState { state, fresh } => {
                self.observe(Seen::Activation { number: state.activation, message: state.next_message });
                if self.store_fault == Some(StoreFault::State) {
                    self.store_fault = None;
                    self.events.push_back(Event::StoreFailed { turn: None, reason: StoreFailure::Write });
                    return;
                }
                if self.store.activation() < state.activation {
                    self.activation_turns = 0;
                }
                if fresh {
                    self.store.turns.clear();
                    self.store.window = 0;
                    self.store.history_override = None;
                    self.store.deliveries.clear();
                }
                self.store.state = Some(state);
                self.events.push_back(Event::StateSaved);
                if self.cut == Some(Cut::AfterSaveState) {
                    self.reached.insert(Goal::Cut);
                }
            }
            Request::SaveTurn { number, read, opens_window, turn } => {
                self.observe(Seen::SaveTurn { number, read });
                if self.store_fault == Some(StoreFault::Turn) {
                    self.store_fault = None;
                    self.events
                        .push_back(Event::StoreFailed { turn: Some(turn.sequence), reason: StoreFailure::Write });
                    return;
                }
                if self.cut == Some(Cut::BeforeSaveTurn(number)) {
                    self.reached.insert(Goal::Cut);
                    return;
                }
                assert_eq!(number, self.activation_turns.saturating_add(1));
                self.activation_turns = number;
                let state = self.store.state.as_mut().expect("activation state saved before any turn");
                state.read = read;
                let activation = state.activation;
                let sequence = turn.sequence;
                if opens_window {
                    self.store.window = self.store.turns.len();
                }
                self.store.turns.push(turn);
                self.store.deliveries.retain(|name, record| {
                    matches!(record.state, DeliveryState::Intent(_))
                        || (name.activation == activation && name.completion > sequence)
                });
                self.observe(Seen::StoredAnswers {
                    activation,
                    sequence,
                    names: self
                        .store
                        .deliveries
                        .iter()
                        .filter(|(_, record)| matches!(record.state, DeliveryState::Answer(_)))
                        .map(|(name, _)| *name)
                        .collect(),
                });
                if self.slow_store {
                    self.delayed_turns.push_back(number);
                } else {
                    self.events.push_back(Event::TurnSaved { number });
                }
            }
            Request::Credential { account } => {
                self.credential_requests = self.credential_requests.saturating_add(1);
                if self.missing_credential == Some(account) {
                    self.events.push_back(Event::NoCredential { account, reason: local::CredentialFailure::Missing });
                    return;
                }
                self.events.push_back(Event::Credential {
                    form: local::Form::Credential(local::Scheme::Bearer),
                    grant: agent::Grant {
                        renewable: true,
                        name: agent::GrantName { account, generation: u64::from(self.credential_requests) },
                        valid: Duration::from_secs(7200),
                    },
                });
            }
            Request::SaveDelivery { record } => {
                let name = record.name;
                let intent = matches!(record.state, DeliveryState::Intent(_));
                let receipts = record.landed.iter().map(run::Receipt::directory).collect();
                self.observe(Seen::DeliveryRecorded { name, intent, receipts });
                if matches!(&record.state, DeliveryState::Answer(run::Delivery::Delivered(_))) {
                    let targeted = self.store.deliveries.get(&name).is_some_and(|prior| match &prior.state {
                        DeliveryState::Intent(intent) => {
                            intent.directories.iter().any(|entry| entry.push.is_some() && entry.changed)
                        }
                        DeliveryState::Answer(_) => false,
                    });
                    if targeted {
                        self.observe(Seen::DeliveredTarget {
                            remote_matches: self.history.as_ref().map(History::remote_head) == self.git_head,
                        });
                    }
                }
                if let DeliveryState::Answer(delivery) = &record.state {
                    self.last_delivery = Some(delivery.clone());
                }
                self.store.deliveries.insert(name, *record);
                self.events.push_back(Event::DeliverySaved { name });
                if self.cut == Some(Cut::AfterIntent)
                    && matches!(
                        self.store.deliveries.get(&name).map(|record| &record.state),
                        Some(DeliveryState::Intent(_))
                    )
                {
                    self.reached.insert(Goal::Cut);
                }
                if self.cut == Some(Cut::AfterSaveDelivery)
                    && matches!(
                        self.store.deliveries.get(&name).map(|record| &record.state),
                        Some(DeliveryState::Answer(_))
                    )
                {
                    self.reached.insert(Goal::Cut);
                }
                if self.cut == Some(Cut::AfterSecondSaveDelivery)
                    && self
                        .store
                        .deliveries
                        .values()
                        .filter(|record| matches!(record.state, DeliveryState::Answer(_)))
                        .count()
                        == 2
                {
                    self.reached.insert(Goal::Cut);
                }
            }
            Request::PlainStatus { owner, directory: _, deadline } => {
                assert!(self.env.now <= deadline, "plain status observes its deadline");
                let changed = self.disk.tree(b"work") != self.disk_start;
                self.events.push_back(Event::PlainStatus { owner, changed });
            }
            Request::Git { owner, directory, op, deadline } => {
                if self.head_change == HeadChange::BeforeDelivery && directory == 0 && matches!(&op, GitOp::Status) {
                    self.disk.write(b"work/outside.txt", b"outside commit\n");
                    let head = self.git_head.expect("local head exists");
                    let moved = git::commit(
                        self.history.as_mut().expect("git graph exists"),
                        &mut self.disk,
                        b"work",
                        head,
                        b"outside",
                    )
                    .expect("outside commit can be made")
                    .expect("outside tree changed");
                    self.git_head = Some(moved);
                    self.head_change = HeadChange::None;
                }
                let pushing = matches!(op, GitOp::Push { .. });
                let committing = matches!(op, GitOp::Commit { .. });
                let inspecting = matches!(op, GitOp::Inspect { .. });
                let mut result = if committing && self.git_fault == Some(GitFault::NoEffect) {
                    GitResult::Uncertain {
                        reason: run::DeliveryReason::TimedOut,
                        diagnostic: Box::new(run::Diagnostic::empty()),
                    }
                } else if inspecting && self.git_fault == Some(GitFault::InspectionUnreadable) {
                    GitResult::Failed {
                        reason: run::DeliveryReason::Broken,
                        diagnostic: Box::new(run::Diagnostic::empty()),
                    }
                } else if self.env.now > deadline {
                    GitResult::Failed {
                        reason: run::DeliveryReason::TimedOut,
                        diagnostic: Box::new(run::Diagnostic::empty()),
                    }
                } else if pushing && self.push_fault == PushFault::Next {
                    self.push_fault = PushFault::None;
                    GitResult::Failed {
                        reason: run::DeliveryReason::Broken,
                        diagnostic: Box::new(run::Diagnostic::new(b"simulated push failure", 0)),
                    }
                } else if directory == 1 {
                    match op {
                        GitOp::Head => GitResult::Head { head: b"1".as_slice().into() },
                        GitOp::Status => {
                            GitResult::Status { changed: true, merging: None, head: b"1".as_slice().into() }
                        }
                        GitOp::Inspect { .. } => GitResult::Inspected { head: b"1".as_slice().into(), named: false },
                        GitOp::Commit { .. } => GitResult::Failed {
                            reason: run::DeliveryReason::Broken,
                            diagnostic: Box::new(run::Diagnostic::new(b"simulated git commit failure", 0)),
                        },
                        GitOp::Markers { .. } | GitOp::Push { .. } => unreachable!("second directory has no merge"),
                    }
                } else {
                    assert_eq!(directory, 0, "fixture has at most two git directories");
                    self.git_operation(op)
                };
                if matches!(result, GitResult::Committed { .. }) {
                    let mut tree = self.disk.tree(b"work");
                    tree.retain(|path, _| !skein_fake_checkout::in_git(path));
                    let name = *self
                        .store
                        .deliveries
                        .iter()
                        .find(|(_, record)| matches!(record.state, DeliveryState::Intent(_)))
                        .expect("intent saved before commit")
                        .0;
                    self.commits.push((name, directory));
                    self.observe(Seen::Committed { name, directory, tree });
                    if self.cut == Some(Cut::AfterCommit(directory)) {
                        self.reached.insert(Goal::Cut);
                    }
                }
                if matches!(result, GitResult::Committed { .. })
                    && let Some(fault) = self.git_fault
                {
                    let reason = match fault {
                        GitFault::Deadline | GitFault::NoEffect => run::DeliveryReason::TimedOut,
                        GitFault::HeadUnreadable | GitFault::InspectionUnreadable => run::DeliveryReason::Broken,
                    };
                    result = GitResult::Uncertain { reason, diagnostic: Box::new(run::Diagnostic::empty()) };
                }
                if pushing && matches!(result, GitResult::Pushed) && self.cut == Some(Cut::BeforePushTerminal) {
                    self.reached.insert(Goal::Cut);
                }
                if self.slow_git && !matches!(result, GitResult::Head { .. }) {
                    self.held_git.push_back((owner, result));
                } else {
                    self.events.push_back(Event::Git { owner, result });
                }
            }
            Request::Lower { agent, request } => {
                self.lower_agent = Some(agent);
                self.agent_request(request);
            }
            Request::Process(_) => panic!("in-process local world cannot spawn an external agent"),
            Request::SelectChat { .. } => panic!("this world's stories keep their configured chat"),
            Request::Answered { .. } | Request::Unanswered { .. } => {}
            Request::Exit { status } => {
                self.exit = Some(status);
                self.reached.insert(Goal::Exit);
            }
        }
    }

    fn git_operation(&mut self, op: GitOp) -> GitResult {
        let head = self.git_head.expect("git operation has a checkout head");
        let history = self.history.as_mut().expect("git operation has a graph");
        match op {
            GitOp::Head => GitResult::Head { head: head.to_string().into_bytes().into() },
            GitOp::Status => {
                let mut tree = self.disk.tree(b"work");
                tree.retain(|path, _| !skein_fake_checkout::in_git(path));
                let merging = if self.disk.exists(b"work/.git/MERGE_HEAD") {
                    let mut paths = Vec::new();
                    for path in self.disk.tree(b"work/.git/temper-conflicts").into_values() {
                        paths.push(path.into_boxed_slice());
                    }
                    Some(paths.into_boxed_slice())
                } else {
                    None
                };
                GitResult::Status {
                    changed: tree != history.tree(head),
                    merging,
                    head: head.to_string().into_bytes().into(),
                }
            }
            GitOp::Inspect { name } => {
                let trailer = format!("Smith-Delivery: {}/{}/{}", name.activation, name.completion, name.position);
                GitResult::Inspected {
                    head: head.to_string().into_bytes().into(),
                    named: history.commit_message(head).ends_with(trailer.as_bytes()),
                }
            }
            GitOp::Markers { paths } => {
                let mut first = None;
                for path in paths {
                    let at = [b"work/".as_slice(), path.as_ref()].concat();
                    if self.disk.content(&at).is_some_and(|text| text.windows(7).any(|part| part == b"<<<<<<<")) {
                        first = Some(path);
                        break;
                    }
                }
                GitResult::Markers { first }
            }
            GitOp::Commit { message } => {
                let committed = if let Some(raw) = self.disk.content(b"work/.git/MERGE_HEAD") {
                    let merging = u64::from_le_bytes(raw.try_into().expect("fixture merge head is u64"));
                    git::commit_merging(history, &mut self.disk, b"work", head, merging, &message).ok().map(Some)
                } else {
                    git::commit(history, &mut self.disk, b"work", head, &message).ok()
                };
                match committed {
                    Some(Some(commit)) => {
                        self.git_head = Some(commit);
                        GitResult::Committed {
                            receipt: format!("commit {commit}").into_bytes().into(),
                            head: commit.to_string().into_bytes().into(),
                        }
                    }
                    Some(None) => GitResult::Failed {
                        reason: run::DeliveryReason::Broken,
                        diagnostic: Box::new(run::Diagnostic::empty()),
                    },
                    None => GitResult::Failed {
                        reason: run::DeliveryReason::Missing,
                        diagnostic: Box::new(run::Diagnostic::empty()),
                    },
                }
            }
            GitOp::Push { remote, branch } => {
                let expected = self.git_expected.expect("checkout kept the remote head it started from");
                match git::push_expected(history, &self.disk, &remote, b"work", head, &branch, expected) {
                    Ok(git::Pushed::Pushed) => {
                        self.git_expected = Some(head);
                        GitResult::Pushed
                    }
                    Ok(git::Pushed::Rejected) => GitResult::Stale,
                    Err(fault) => GitResult::Failed {
                        reason: run::DeliveryReason::Broken,
                        diagnostic: Box::new(run::Diagnostic::new(format!("{fault:?}").as_bytes(), 0)),
                    },
                }
            }
        }
    }

    fn gather_facts(&mut self) {
        drop(self.domain.pop_run_message());
        while self.domain.pop_run_fact().is_some() {}
        if !self.collect_facts {
            return;
        }
        while let Some(fact) = self.domain.pop_fact() {
            let seen = match fact {
                local::Fact::Started { activation: _ } => None,
                local::Fact::Message { name } => Some(Seen::Message { name }),
                local::Fact::Turn { number } => {
                    self.saw(crate::person::Shown::Turn);
                    Some(Seen::Turn { number })
                }
                local::Fact::Answered { activation, read } => Some(Seen::Answered { activation, read }),
                local::Fact::Unread { name } => Some(Seen::Unread { name }),
                local::Fact::Shown { activation } => {
                    self.saw(crate::person::Shown::Answer);
                    Some(Seen::Shown { activation })
                }
                local::Fact::DeliveryReturned { name } => Some(Seen::DeliveryReturned { name }),
            };
            if let Some(seen) = seen {
                self.observe(seen);
            }
        }
    }

    fn observe(&mut self, seen: Seen) {
        self.trace.log(self.env.now, format!("{seen:?}"));
        self.referee.observe(self.env.now, seen, &mut Vec::new());
    }

    fn below(&self, terminal: Below) -> Event {
        Event::Below { agent: self.lower_agent.expect("inline lower callback"), terminal }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one exhaustive typed adapter keeps each agent request beside its terminal"
    )]
    fn agent_request(&mut self, request: Lower) {
        match request {
            Lower::Complete { owner, prompt, .. } => {
                self.observe(Seen::Complete { owner });
                self.completions += 1;
                self.prompt_affinities.push(prompt.affinity);
                self.prompt_assistants
                    .push(prompt.messages.iter().filter(|message| message.role == agent::llm::Role::Assistant).count());
                let mut user_text = Vec::new();
                for message in &prompt.messages {
                    if message.role == agent::llm::Role::User {
                        for block in &message.content {
                            match block {
                                agent::llm::Block::Text { text, .. } => user_text.extend_from_slice(text),
                                agent::llm::Block::ToolResult {
                                    result: agent::llm::Returned::Text { text, .. },
                                    ..
                                } => {
                                    self.tool_results.push(text.to_vec());
                                }
                                agent::llm::Block::ToolResult {
                                    result:
                                        agent::llm::Returned::Owned { outcome: tools::Outcome::Read { content, .. } },
                                    ..
                                } => self.tool_results.push(content.to_vec()),
                                _ => {}
                            }
                        }
                    }
                }
                self.prompt_texts.push(user_text);
                if !prompt.served.contains(&agent::llm::Served::Finish) {
                    self.child_prompts.push((prompt.tools, prompt.served.clone()));
                }
                let asker = prompt.asker;
                let grants = prompt.tools;
                let served = prompt.served.clone();
                if matches!(self.first_failure, FirstFailure::Refused) {
                    self.first_failure = FirstFailure::Used;
                    self.events.push_back(self.below(Below::Failed {
                        owner,
                        failure: agent::llm::Failure::Invalid,
                        evidence: agent::llm::Evidence::Response { status: Some(400) },
                        detail: b"model does not exist\nsecond line is private".as_slice().into(),
                        report: smith_domain::llm::Report::none(),
                    }));
                    return;
                }
                if matches!(self.first_failure, FirstFailure::AccountRefused) {
                    self.first_failure = FirstFailure::Used;
                    self.events.push_back(self.below(Below::Failed {
                        owner,
                        failure: agent::llm::Failure::Refused,
                        evidence: agent::llm::Evidence::Response { status: Some(403) },
                        detail: b"contact support\nprivate".as_slice().into(),
                        report: agent::llm::Report {
                            code: b"billing".as_slice().into(),
                            request: Box::default(),
                            field: Box::default(),
                        },
                    }));
                    return;
                }
                if matches!(self.first_failure, FirstFailure::Exhaust) {
                    self.first_failure = FirstFailure::Used;
                    self.events.push_back(self.below(Below::Failed {
                        owner,
                        failure: agent::llm::Failure::Exhausted { retry_after: Some(Duration::from_secs(60)) },
                        evidence: agent::llm::Evidence::Response { status: None },
                        detail: Box::new([]),
                        report: smith_domain::llm::Report::none(),
                    }));
                    return;
                }
                if matches!(self.first_failure, FirstFailure::Reject) {
                    self.first_failure = FirstFailure::Used;
                    self.events.push_back(self.below(Below::Failed {
                        owner,
                        failure: agent::llm::Failure::Unauthorized,
                        evidence: agent::llm::Evidence::Response { status: None },
                        detail: Box::new([]),
                        report: smith_domain::llm::Report::none(),
                    }));
                    return;
                }
                assert!(self.pending.insert(owner, (grants, served, asker)).is_none());
                let query = translate::query(prompt);
                provider::step(
                    &mut self.provider,
                    &self.provider_env,
                    provider::Event::Call { reply_to: ReplyTo::new(owner), call: owner, query },
                    &mut self.provider_out,
                );
            }
            Lower::Cancel { owner } => {
                if self.pending.contains_key(&owner) {
                    assert!(self.cancelled.insert(owner), "one cancellation per provider right");
                } else {
                    assert!(self.events.iter().any(|event| matches!(event,
                        Event::Below { terminal: Below::Completed { owner: pending, .. } | Below::Failed { owner: pending, .. }, .. }
                            if *pending == owner
                    )), "an already completed provider right retains its queued terminal");
                }
            }
            Lower::Io { owner, op, deadline: _ } => {
                let store_root = match &op {
                    tools::Op::Store { at, .. } => Some(at.root.raw()),
                    tools::Op::Load { .. }
                    | tools::Op::List { .. }
                    | tools::Op::Spawn { .. }
                    | tools::Op::Search { .. } => None,
                };
                let done = match op {
                    tools::Op::Spawn { cwd, command, head, tail, after: _ } => {
                        match checkout_io::spawn(&self.disk, &cwd, &command, &[], (head, tail)) {
                            Ok(started) => {
                                checkout_io::finish(&mut self.disk, &started.process);
                                checkout_io::exited(
                                    Some(started.process.program.exit),
                                    &started.process.program.output,
                                    head,
                                    tail,
                                )
                            }
                            Err(done) => done,
                        }
                    }
                    op => checkout_io::perform(&mut self.disk, op),
                };
                if matches!(done, tools::Done::Stored { .. }) {
                    self.observe(Seen::Wrote { root: store_root.expect("stored operation names a root") });
                }
                self.events.push_back(self.below(Below::Done { owner, done }));
            }
            // Fake checkout operations already settled synchronously; their one terminal is queued above.
            Lower::Job { owner, act: _ } => {
                let done = tools::Done::Failed { fault: tools::Fault::Other };
                self.events.push_back(self.below(Below::Done { owner, done }));
            }
            Lower::CancelIo { .. } | Lower::Abort { .. } => {}
            Lower::Read { owner, at, max, deadline: _ } => {
                let read = match self.disk.load(at.root.raw(), &at.path, u64::from(max)) {
                    Ok((text, _)) => run::Read::Text { text: text.into(), whole: true },
                    Err(_) => run::Read::Missing,
                };
                self.events.push_back(self.below(Below::Read { owner, read }));
            }
            Lower::Probe { owner, at, deadline: _ } => {
                let executable =
                    self.disk.load(at.root.raw(), &at.path, 4096).is_ok_and(|(text, _)| text.starts_with(b"#!"));
                self.events.push_back(self.below(Below::Probed { owner, executable }));
            }
            Lower::Check { owner, program, deadline: _, tail } => {
                let passed = self
                    .disk
                    .load(program.root.raw(), b"src/lib.rs", 4096)
                    .is_ok_and(|(text, _)| text.windows(2).any(|part| part == b"43"));
                if passed {
                    let directory =
                        u32::try_from(program.root.raw().checked_sub(1).expect("fixture roots start at one"))
                            .expect("few fixture roots");
                    let mut tree = self.disk.tree(self.disk.root_path(program.root.raw()));
                    tree.retain(|path, _| !skein_fake_checkout::in_git(path));
                    self.observe(Seen::Checked { directory, tree });
                }
                let output = if passed { &b"ok: src/lib.rs\n"[..] } else { &b"FAILED: src/lib.rs\n"[..] };
                let keep = usize::try_from(tail).expect("small check output").min(output.len());
                let ran = run::Ran {
                    exit: run::Exit::Code { code: u8::from(!passed) },
                    output: output[output.len() - keep..].into(),
                    cut: u64::try_from(output.len() - keep).expect("small output"),
                };
                self.events.push_back(self.below(Below::Checked { owner, ran }));
            }
        }
    }
}
