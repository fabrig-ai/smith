//! Independent policy over terminal bytes, decoded peer requests and service
//! facts. Repository access is only head/message/files; the referee never
//! reads Sim or Machine (protocol/hosts.md, section 7).

use skein_fake_checkout::git::Tree;
use skein_fake_llm_domain::api::Query;
use skein_io::kernel;
use skein_lib::{Duration, Time};
use skein_world::domain::{Expectations, Judge, Referee};
use smith_local_domain::Fact;

/// Checkout observations and the scenario's expected token-exchange count.
pub trait CheckoutRead {
    /// Token requests expected by the scenario when an issuer is present.
    fn expected_token_posts(&self) -> u64 {
        1
    }
    fn head(&self) -> Option<Vec<u8>>;
    fn message(&self, commit: &[u8]) -> Vec<u8>;
    fn files(&self, commit: &[u8]) -> Tree;
    fn borrowed_notice(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Concrete observations collected independently of process state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Seen {
    pub facts: Vec<Fact>,
    pub shown: Vec<u8>,
    pub errors: Vec<u8>,
    pub queries: Vec<Query>,
    pub exit: Option<kernel::Exit>,
    pub pushed: bool,
    pub oauth: Option<OAuthSeen>,
}

/// Facts observed by the fake issuer, browser and first provider request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthSeen {
    pub posts: u64,
    pub pages: u32,
    pub browser_replied: bool,
    pub saved_before_query: bool,
}

/// One expected story ending supplied by its author.
#[derive(Clone, Debug)]
pub enum Ending {
    Report(Vec<u8>),
    Cancelled,
    /// A headless answer failed because its turn budget ran out.
    Budget,
    /// The second interrupt ends the invocation before any answer.
    Dropped,
    /// The spawned child exits before answering or contacting the provider.
    AgentEnded,
    Change {
        before: Vec<u8>,
    },
    Unavailable,
    HostFailure,
    /// The host drops a run whose workspace read completed before its turn save failed.
    HostFailureBeforeAnswer,
}

/// Evidence handed to the shared judgment kit, never internal process cells.
#[derive(Clone, Debug)]
pub enum Observation {
    Fact(Fact),
    Peer { queries: Vec<Query>, oauth: Option<OAuthSeen> },
    Terminal { shown: Vec<u8>, errors: Vec<u8>, exit: Option<kernel::Exit>, queries: usize },
    Commit { head: Option<Vec<u8>>, message: Vec<u8>, files: Tree, pushed: bool },
}

/// Obligations and ordering from one invocation's visible facts.
pub struct Meeting {
    ending: Ending,
    require_facts: bool,
    expected_token_posts: u64,
    activation: Option<u64>,
    next_turn: u32,
    answered: bool,
    shown: bool,
    borrowed_notice: Option<Vec<u8>>,
}

impl Expectations for Meeting {
    type Seen = Observation;
    type Name = &'static str;
    type Stimulus = ();

    fn observe(&mut self, observation: Observation, judge: &mut Judge<Self::Name, Self::Stimulus>) {
        match observation {
            Observation::Fact(fact) => match fact {
                Fact::Started { activation } => {
                    judge.check(
                        self.activation.is_none() || self.answered,
                        "an activation starts only after the one before it answered",
                    );
                    self.activation = Some(activation);
                    self.next_turn = 1;
                    self.answered = false;
                    self.shown = false;
                    judge.rearm("terminal", Duration::from_secs(60));
                }
                Fact::Message { .. } => {
                    judge.check(self.activation.is_some() && !self.answered, "message belongs to a live activation")
                }
                Fact::Turn { number } => {
                    judge.check(
                        self.activation.is_some() && !self.answered && number == self.next_turn,
                        "consecutive activation-local turns before answer",
                    );
                    self.next_turn += 1;
                }
                Fact::Unread { .. } => {}
                Fact::Answered { activation, .. } => {
                    judge
                        .check(self.activation == Some(activation) && !self.answered, "one answer for this activation");
                    self.answered = true;
                }
                Fact::Shown { activation } => {
                    judge.check(
                        self.activation == Some(activation) && self.answered && !self.shown,
                        "one shown answer follows its answer fact",
                    );
                    self.shown = true;
                }
                Fact::DeliveryReturned { name } => {
                    judge.check(self.activation == Some(name.activation), "delivery belongs to this activation")
                }
            },
            Observation::Terminal { shown, errors, exit, queries } => {
                let borrowed = self.borrowed_notice.as_deref();
                let borrowed_text = borrowed.and_then(|line| line.strip_prefix(b"smith: "));
                let expected = match &self.ending {
                    Ending::Report(text) => text.as_slice(),
                    Ending::Cancelled => b"interrupted: the run was cancelled",
                    Ending::Budget => b"run failed: out of turns",
                    Ending::Dropped => b"not delivered",
                    Ending::AgentEnded => b"run failed: the agent ended without answering",
                    Ending::Change { .. } => b"Change delivered",
                    Ending::Unavailable => {
                        borrowed_text.unwrap_or(b"Line 1 not delivered: the run ended without reading it")
                    }
                    Ending::HostFailure | Ending::HostFailureBeforeAnswer => b"could not be kept: write failed",
                };
                if matches!(self.ending, Ending::Unavailable) {
                    judge.check(
                        self.activation.is_none() && !self.answered && !self.shown,
                        "a refused account starts no run",
                    );
                } else if matches!(self.ending, Ending::HostFailure | Ending::HostFailureBeforeAnswer) {
                    judge.check(self.activation.is_some() && !self.shown, "failed store owns the terminal failure");
                } else if matches!(self.ending, Ending::Dropped | Ending::AgentEnded) {
                    judge.check(
                        self.activation.is_some() && !self.answered && !self.shown,
                        "a dropped run never answers",
                    );
                } else if self.require_facts {
                    judge.check(self.shown && self.answered, "the invocation actually answered and showed it");
                }
                let code = match self.ending {
                    Ending::HostFailure | Ending::HostFailureBeforeAnswer | Ending::AgentEnded => 1,
                    Ending::Dropped => 130,
                    Ending::Budget => 4,
                    _ => 0,
                };
                judge.check(exit == Some(kernel::Exit::Code(code)), "shell exit matches the host terminal");
                judge.check(
                    if matches!(self.ending, Ending::Unavailable | Ending::AgentEnded) {
                        queries == 0
                    } else {
                        queries > 0
                    },
                    "provider work matches account availability",
                );
                let start = b"smith: local host started\n";
                let diagnostic = if matches!(self.ending, Ending::HostFailure | Ending::HostFailureBeforeAnswer) {
                    errors.strip_suffix(b"smith: run failed: chat main: turn 2 could not be kept: write failed\n")
                } else if matches!(self.ending, Ending::Dropped) {
                    errors.strip_suffix(b"smith: interrupted: the run was dropped before it answered\n")
                } else if matches!(self.ending, Ending::AgentEnded) {
                    errors.strip_suffix(b"smith: run failed: the agent ended without answering (exit 7); terminated\n")
                } else if matches!(self.ending, Ending::Budget) {
                    errors
                        .strip_suffix(
                            b"smith: run failed: out of turns: used 3 of 3: iled: out of turns: used 3 of 3\n",
                        )
                        .or_else(|| errors.strip_suffix(b"smith: run failed: out of turns: used 3 of 3\n"))
                } else if matches!(self.ending, Ending::Cancelled) {
                    errors.strip_suffix(b"smith: interrupted: the run was cancelled\n").or_else(|| {
                        errors
                            .strip_suffix(
                                b"smith: interrupted: the run was cancelled: n failed: the run was cancelled\n",
                            )
                            .or_else(|| {
                                errors.strip_suffix(
                                    b"smith: interrupted: the run was cancelled: call aborted by its owner\n",
                                )
                            })
                    })
                } else if matches!(self.ending, Ending::Unavailable) {
                    errors
                        .strip_suffix(b"smith: account 0 has no credential: run `smith login 0`\n")
                        .or_else(|| {
                            errors
                                .strip_suffix(b"smith: account 0 could not renew its credential: run `smith login 0`\n")
                        })
                        .or_else(|| borrowed.and_then(|line| errors.strip_suffix(line)))
                } else {
                    Some(errors.as_slice())
                };
                let diagnostic = diagnostic.map(|bytes| {
                    let bytes = if let Some(at) = bytes.windows(19).rposition(|part| part == b"smith: accepted in ") {
                        &bytes[..at]
                    } else {
                        bytes
                    };
                    bytes
                        .strip_suffix(b"smith: warning: agent forced to exit after answering (signal 9); killed\n")
                        .or_else(|| {
                            bytes.strip_suffix(
                                b"smith: warning: agent forced to exit after answering (signal 15); terminated\n",
                            )
                        })
                        .or_else(|| {
                            bytes.strip_suffix(
                                b"smith: warning: agent forced to exit after answering (exit 0); terminated\n",
                            )
                        })
                        .unwrap_or(bytes)
                });
                let expected_diagnostic = diagnostic.is_some_and(|bytes| {
                    bytes.is_empty()
                        || bytes.strip_prefix(b"smith: chat main\n").is_some_and(|tail| {
                            tail.split_inclusive(|byte| *byte == b'\n')
                                .all(|line| line == b"smith: tool finish\n" || line == b"smith: tool read\n")
                        })
                        || bytes == start
                });
                judge.check(expected_diagnostic, "only the expected local operator start and failure lines");
                judge.check(
                    shown.windows(expected.len()).any(|part| part == expected)
                        || (matches!(self.ending, Ending::Budget)
                            && errors.starts_with(b"smith: chat main\n")
                            && (errors.ends_with(b"smith: run failed: out of turns: used 3 of 3\n")
                                || errors.ends_with(
                                    b"smith: run failed: out of turns: used 3 of 3: iled: out of turns: used 3 of 3\n",
                                ))),
                    "expected terminal text",
                );
                let private = borrowed_text.and_then(|line| shown.strip_suffix(line)).unwrap_or(&shown);
                judge.check(
                    !private.windows(7).any(|part| part == b"refresh")
                        && !private.windows(5).any(|part| part == b"token"),
                    "terminal never reveals credentials",
                );
                if self.require_facts && !matches!(self.ending, Ending::Unavailable) {
                    let met = judge.meet(&"terminal");
                    judge.check(met, "terminal settles the activation");
                }
            }
            Observation::Peer { queries, oauth } => {
                judge.check(queries.len() <= 64, "bounded observed provider requests");
                for query in &queries {
                    judge.check(
                        (query.model.as_ref() == b"fake" || query.model.as_ref() == b"alternate")
                            && query.max_tokens > 0,
                        "configured model reaches the provider and its declared ceiling is positive",
                    );
                    judge.check(
                        query.system.windows(12).any(|part| part == b"@local-shell"),
                        "configured instructions reach the provider",
                    );
                }
                if let Some(oauth) = oauth {
                    judge.check(
                        oauth.posts == self.expected_token_posts && oauth.pages <= 1,
                        "the expected token exchanges and at most one page visit",
                    );
                    judge.check(oauth.pages == 0 || oauth.browser_replied, "the browser heard the loopback reply");
                    judge.check(
                        queries.is_empty() || oauth.saved_before_query,
                        "token saved before the peer saw grant use",
                    );
                }
            }
            Observation::Commit { head, message, files, pushed } => {
                if let Ending::Change { before } = &self.ending {
                    judge.check(head.as_ref().is_some_and(|head| head != before), "a new local commit exists");
                    judge.check(
                        message.starts_with(b"Updated result\n\nCreated the result file"),
                        "result fields form the message",
                    );
                    judge.check(
                        message.windows(15).any(|part| part == b"Smith-Delivery:"),
                        "commit carries its delivery identity",
                    );
                    judge.check(
                        files.get(b"result.txt".as_slice()).is_some_and(|bytes| bytes == b"new\n"),
                        "commit contains the agent's change",
                    );
                } else if matches!(self.ending, Ending::HostFailureBeforeAnswer | Ending::Budget)
                    || matches!(&self.ending, Ending::Report(text) if text.as_slice() == b"After three tools")
                {
                    judge.check(
                        head.as_deref() == Some(b"0000000000000000000000000000000000000001"),
                        "read leaves the repository head unchanged",
                    );
                    judge.check(
                        files.len() == 1
                            && files.get(b"original.txt".as_slice()).is_some_and(|bytes| bytes == b"original\n"),
                        "read leaves the repository files unchanged",
                    );
                } else {
                    judge.check(head.is_none(), "report or cancellation has no repository effect");
                }
                judge.check(!pushed, "no push without configuration");
            }
        }
    }
}

/// Judge only supplied observations and the checkout's narrow read interface.
#[must_use]
pub fn review(seen: &Seen, checkout: &impl CheckoutRead, ending: Ending) -> Referee<Meeting> {
    review_with_facts(seen, checkout, ending, true)
}

/// Judge binary observations without requiring in-process service facts.
#[must_use]
pub fn review_binary(seen: &Seen, checkout: &impl CheckoutRead, ending: Ending) -> Referee<Meeting> {
    review_with_facts(seen, checkout, ending, false)
}

fn review_with_facts(
    seen: &Seen,
    checkout: &impl CheckoutRead,
    ending: Ending,
    require_facts: bool,
) -> Referee<Meeting> {
    let mut referee = Referee::new(Meeting {
        ending,
        require_facts,
        expected_token_posts: checkout.expected_token_posts(),
        activation: None,
        next_turn: 1,
        answered: false,
        shown: false,
        borrowed_notice: checkout.borrowed_notice(),
    });
    let mut stimuli = Vec::new();
    for fact in &seen.facts {
        referee.observe(Time::ZERO, Observation::Fact(*fact), &mut stimuli);
    }
    referee.observe(
        Time::ZERO,
        Observation::Terminal {
            shown: seen.shown.clone(),
            errors: seen.errors.clone(),
            exit: seen.exit,
            queries: seen.queries.len(),
        },
        &mut stimuli,
    );
    referee.observe(
        Time::ZERO,
        Observation::Peer { queries: seen.queries.clone(), oauth: seen.oauth.clone() },
        &mut stimuli,
    );
    let head = checkout.head();
    referee.observe(
        Time::ZERO,
        Observation::Commit {
            message: head.as_ref().map_or_else(Vec::new, |head| checkout.message(head)),
            files: head.as_ref().map_or_else(Tree::new, |head| checkout.files(head)),
            head,
            pushed: seen.pushed,
        },
        &mut stimuli,
    );
    referee
}

/// Capture only terminal, emitted facts and independent peer observations.
#[must_use]
pub fn seen(procs: &[crate::process::Proc], saved_before_query: bool, pushed: bool) -> Seen {
    use crate::process::Proc;
    let terminal =
        procs.iter().find_map(|p| if let Proc::Terminal(p) = p { Some(p.as_ref()) } else { None }).expect("terminal");
    let peer =
        procs.iter().find_map(|p| if let Proc::Peer(p) = p { Some(p.as_ref()) } else { None }).expect("provider");
    let facts = procs
        .iter()
        .find_map(|p| if let Proc::Local(p) = p { Some(p.facts().to_vec()) } else { None })
        .unwrap_or_default();
    let browser = procs.iter().find_map(|p| if let Proc::Browser(p) = p { Some(p.as_ref()) } else { None });
    let issuer = procs.iter().find_map(|p| if let Proc::Issuer(p) = p { Some(p.as_ref()) } else { None });
    Seen {
        facts,
        shown: terminal.shown().to_vec(),
        errors: terminal.errors().to_vec(),
        queries: crate::llm::queries(peer).cloned().collect(),
        exit: terminal.exit(),
        pushed,
        oauth: issuer.map(|issuer| OAuthSeen {
            posts: crate::oauth::posts(issuer.peer()),
            pages: browser.map_or(0, crate::process::BrowserProcess::pages),
            browser_replied: browser.is_some_and(crate::process::BrowserProcess::replied),
            saved_before_query,
        }),
    }
}

/// Incremental sign-in URL observation from the scripted terminal's append bytes.
#[derive(Default)]
pub struct SignIn {
    received: usize,
    search: usize,
    start: Option<usize>,
    url: Option<Vec<u8>>,
}

impl SignIn {
    /// Inspect only appended bytes, retaining a split prefix or unfinished URL.
    pub fn observe(&mut self, shown: &[u8]) {
        if self.url.is_some() || shown.len() == self.received {
            return;
        }
        if self.start.is_none() {
            let suffix = shown.get(self.search..).expect("terminal observations append");
            if let Some(at) = suffix.windows(9).position(|bytes| bytes == b"Sign in: ") {
                self.start = Some(self.search + at + 9);
            } else {
                self.search = shown.len().saturating_sub(8);
            }
        }
        if let Some(start) = self.start {
            let appended = start.max(self.received);
            if let Some(end) = shown.get(appended..).expect("appended URL bytes").iter().position(|byte| *byte == b'\n')
            {
                self.url = Some(shown.get(start..appended + end).expect("complete URL").trim_ascii_end().to_vec());
            }
        }
        self.received = shown.len();
    }

    /// The first complete URL, kept for the browser's one visit.
    pub fn url(&self) -> Option<&[u8]> {
        self.url.as_deref()
    }
}

/// The same outside referee driven beside processes in either kernel loop.
pub struct Run {
    seed: u64,
    meeting: Referee<Meeting>,
    checkout: Box<dyn CheckoutRead>,
    token_directory: std::path::PathBuf,
    interrupt_on_query: bool,
    drop_on_query: bool,
    controls: Option<skein_world::Controls>,
    interrupted: bool,
    facts: usize,
    sign_in: SignIn,
    observations: usize,
    queries: usize,
    saved_before_query: bool,
    reviewed: bool,
    next_act: Option<Time>,
    shutdown: bool,
    end: Option<Time>,
    failed_turn: Option<std::path::PathBuf>,
    line_on_wait: Option<Box<[u8]>>,
    /// The provider's scripted reply delay, which an interrupted turn leaves
    /// to run out: the scenario's last word (skein's testing-strategy.md, 6).
    delay: Option<Time>,
}
impl Run {
    /// Prepare the scenario's policy, repository face and observed-token check.
    #[must_use]
    pub fn new(
        seed: u64,
        ending: Ending,
        checkout: Box<dyn CheckoutRead>,
        token_directory: std::path::PathBuf,
        interrupt_on_query: bool,
        keep_facts: bool,
        expected_token_posts: u64,
    ) -> Self {
        let drop_on_query = matches!(ending, Ending::Dropped);
        Self {
            seed,
            meeting: Referee::new(Meeting {
                ending,
                require_facts: keep_facts,
                expected_token_posts,
                activation: None,
                next_turn: 1,
                answered: false,
                shown: false,
                borrowed_notice: checkout.borrowed_notice(),
            }),
            checkout,
            token_directory,
            interrupt_on_query,
            drop_on_query,
            controls: None,
            interrupted: false,
            facts: 0,
            sign_in: SignIn::default(),
            observations: 0,
            queries: 0,
            saved_before_query: false,
            reviewed: false,
            next_act: None,
            shutdown: false,
            end: None,
            failed_turn: None,
            line_on_wait: None,
            delay: None,
        }
    }

    /// Type this line when the process emits its first wait event.
    pub fn line_on_wait(&mut self, line: Box<[u8]>) {
        self.line_on_wait = Some(line);
    }

    /// Make this turn's rename fail only after the provider observes the saved activation.
    pub fn fail_turn_at_query(&mut self, directory: std::path::PathBuf, number: u32) {
        self.failed_turn = Some(directory.join(format!("{number:010}.turn")));
    }

    /// Use the world's signal source for the two-interrupt story.
    #[must_use]
    pub fn controlled(mut self, controls: skein_world::Controls) -> Self {
        self.controls = Some(controls);
        self
    }
}
impl skein_world::Referee<crate::process::Proc> for Run {
    fn act(&mut self, now: Time, procs: &mut [crate::process::Proc]) {
        use crate::process::Proc;
        self.end.get_or_insert_with(|| now.saturating_add(Duration::from_secs(120)));
        self.next_act = None;
        let query = self.queries > 0;
        if query && let Some(path) = self.failed_turn.take() {
            std::fs::create_dir(path).expect("turn rename fault after the initial transcript load");
        }
        let wait = procs.iter().any(|proc| match proc {
            Proc::Local(local) => local.wait_seen(),
            Proc::Agent(_, _, wait) => *wait,
            Proc::Terminal(_)
            | Proc::Peer(_)
            | Proc::Issuer(_)
            | Proc::Browser(_)
            | Proc::Ended(_)
            | Proc::Git(_)
            | Proc::LingeringAgent(_) => false,
        });
        let line = if wait { self.line_on_wait.take() } else { None };
        let exited = procs.iter().any(|p| matches!(p, Proc::Terminal(t) if t.exit().is_some()));
        let url = self.sign_in.url();
        // Copy into the referee's stack; process storage was allocated at startup.
        let mut bytes = [0; 8192];
        let length = url.map_or(0, |url| {
            assert!(url.len() <= bytes.len());
            bytes[..url.len()].copy_from_slice(url);
            url.len()
        });
        if exited {
            self.shutdown = true;
        }
        let mut line = line;
        for (host, proc) in procs.iter_mut().enumerate() {
            match proc {
                Proc::Terminal(t) => {
                    if let Some(line) = line.take() {
                        t.type_line(line);
                    }
                    if self.interrupt_on_query && !self.drop_on_query && query && !self.interrupted {
                        t.interrupt();
                        self.interrupted = true;
                    }
                }
                Proc::Local(_) if self.drop_on_query && query && !self.interrupted => {
                    let controls = self.controls.as_ref().expect("dropped story uses the world's signal source");
                    controls.signal(host, kernel::ServiceSignal::Interrupt);
                    controls.signal(host, kernel::ServiceSignal::Interrupt);
                    self.interrupted = true;
                }
                Proc::Peer(peer) => {
                    if exited {
                        peer.shutdown();
                    }
                }
                Proc::Issuer(issuer) => {
                    if exited {
                        issuer.shutdown();
                    }
                }
                Proc::Browser(browser) => {
                    if browser.pages() == 0 && length > 0 {
                        browser.visit(&bytes[..length]);
                    }
                    if exited {
                        browser.shutdown();
                    }
                }
                Proc::Local(_) | Proc::Agent(_, _, _) | Proc::Ended(_) | Proc::Git(_) | Proc::LingeringAgent(_) => {}
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[crate::process::Proc]) {
        use crate::process::Proc;
        use skein_world::Host;
        let mut stimuli = Vec::new();
        if let Some(local) = procs.iter().find_map(|p| if let Proc::Local(p) = p { Some(p.as_ref()) } else { None }) {
            for fact in &local.facts()[self.facts..] {
                self.meeting.observe(now, Observation::Fact(*fact), &mut stimuli);
            }
            self.facts = local.facts().len();
        }
        let peer =
            procs.iter().find_map(|p| if let Proc::Peer(p) = p { Some(p.as_ref()) } else { None }).expect("provider");
        let observations = peer.observations();
        for observation in observations.get(self.observations..).expect("peer observations append") {
            if matches!(observation, skein_fake_peers::llm::Observation::Query { .. }) {
                self.queries += 1;
            }
        }
        self.observations = observations.len();
        assert!(self.queries <= 64, "bounded provider work");
        if self.interrupt_on_query
            && let Some(delay) = peer.next_policy_deadline()
        {
            self.delay = Some(self.delay.map_or(delay, |known| known.max(delay)));
        }
        let authenticated = procs.iter().any(|p| matches!(p, Proc::Issuer(_)));
        if authenticated && !self.saved_before_query && self.queries > 0 {
            let tokens = crate::credential_fixture::Tokens::new(
                &self.token_directory,
                smith_local_shell::local_host::token_limits(),
            )
            .expect("private store");
            assert_eq!(
                tokens.load(0).expect("record").expect("saved before grant use").access_token.as_ref(),
                b"access-new"
            );
            self.saved_before_query = true;
        }
        let terminal = procs
            .iter()
            .find_map(|p| if let Proc::Terminal(p) = p { Some(p.as_ref()) } else { None })
            .expect("terminal");
        self.sign_in.observe(terminal.shown());
        let visit = self.sign_in.url().is_some()
            && procs.iter().any(|p| matches!(p, Proc::Browser(browser) if browser.pages() == 0));
        let wait = procs.iter().any(|proc| match proc {
            Proc::Local(local) => local.wait_seen(),
            Proc::Agent(_, _, wait) => *wait,
            Proc::Terminal(_)
            | Proc::Peer(_)
            | Proc::Issuer(_)
            | Proc::Browser(_)
            | Proc::Ended(_)
            | Proc::Git(_)
            | Proc::LingeringAgent(_) => false,
        });
        if (self.line_on_wait.is_some() && wait)
            || (self.interrupt_on_query && !self.interrupted && self.queries > 0)
            || visit
            || (terminal.exit().is_some() && !self.shutdown)
        {
            self.next_act = Some(now);
        }
        let local_settled = procs.iter().any(|p| matches!(p,Proc::Local(local) if local.is_empty()));
        let browser_settled = procs.iter().filter(|p| matches!(p, Proc::Browser(_))).all(Host::is_empty);
        if !self.reviewed && terminal.is_empty() && local_settled && browser_settled {
            let observed = seen(procs, self.saved_before_query, false);
            self.meeting.observe(
                now,
                Observation::Terminal {
                    shown: observed.shown,
                    errors: observed.errors,
                    exit: observed.exit,
                    queries: observed.queries.len(),
                },
                &mut stimuli,
            );
            self.meeting.observe(
                now,
                Observation::Peer { queries: observed.queries, oauth: observed.oauth },
                &mut stimuli,
            );
            let head = self.checkout.head();
            self.meeting.observe(
                now,
                Observation::Commit {
                    message: head.as_ref().map_or_else(Vec::new, |head| self.checkout.message(head)),
                    files: head.as_ref().map_or_else(Tree::new, |head| self.checkout.files(head)),
                    head,
                    pushed: false,
                },
                &mut stimuli,
            );
            self.reviewed = true;
        }
        if self.meeting.is_due(now) {
            self.meeting.fire(now, &mut stimuli);
        }
        if let skein_world::domain::Verdict::Failed(why) = self.meeting.verdict() {
            panic!(
                "seed {}: {why:?}; shown {:?}; errors {:?}; exit {:?}",
                self.seed,
                String::from_utf8_lossy(terminal.shown()),
                String::from_utf8_lossy(terminal.errors()),
                terminal.exit(),
            );
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.next_act, self.meeting.next_deadline(), self.end.filter(|_| !self.reviewed)].into_iter().flatten().min()
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (self.end.is_some_and(|end| now >= end) && !self.reviewed)
            .then(|| "local invocation did not settle its observed terminal".into())
    }
    fn passed(&self) -> bool {
        self.reviewed && matches!(self.meeting.verdict(), skein_world::domain::Verdict::Passed)
    }
    fn last_word(&self) -> skein_world::LastWord {
        self.delay.map_or(skein_world::LastWord::LastExpectation, skein_world::LastWord::After)
    }
}

#[test]
fn sign_in_observation_keeps_a_split_prefix() {
    let bytes = b"unrelated terminal bytes\nSign in: http://127.0.0.1/authorize?code=fixture\r\n";
    let mut sign_in = SignIn::default();
    for end in 0..bytes.len() {
        sign_in.observe(&bytes[..end]);
        sign_in.observe(&bytes[..end]);
        assert!(sign_in.url().is_none(), "a partial URL cannot start the browser");
    }
    sign_in.observe(bytes);
    assert_eq!(sign_in.url(), Some(b"http://127.0.0.1/authorize?code=fixture".as_slice()));
    sign_in.observe(b"unrelated terminal bytes\nSign in: http://127.0.0.1/authorize?code=fixture\r\nafter");
    assert_eq!(sign_in.url(), Some(b"http://127.0.0.1/authorize?code=fixture".as_slice()));
}
