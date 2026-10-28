//! Local choices translated into one agent charter (domain/host.md,
//! sections 5 and 8; domain/run.md, sections 3.1 and 5.3). Configuration owns text,
//! model names and result policy, but neither credential secrets nor files.
//! `charter` copies those choices without consulting time or the workspace.
//! One-turn reserve admission stays with the real run entrance (domain/run.md,
//! section 9.1); local startup retains budget ceilings and completion affordability.

use alloc::boxed::Box;
use skein_lib::List;
use smith_domain::run::{self, charter, outcome};

use crate::Limits;

/// The person's chosen result form for this chat.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Contract {
    /// Finish with a report of at most the configured size.
    Report(outcome::TextSpec),
    /// Finish with a checked change in writable directories.
    Change(outcome::ChangeSpec),
}

/// Where a git workspace directory sends its new commits.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PushTarget {
    /// Named remote for this directory.
    pub remote: Box<[u8]>,
    /// Branch on that remote.
    pub branch: Box<[u8]>,
}

/// Whether the local host accepts person lines or carries one prompt in Start.
#[derive(Debug)]
pub enum Mode {
    /// A chat's run waits for the person for this duration before parking.
    Chat { waiting: skein_lib::Duration },
    /// One prompt starts one run; its answer ends the invocation.
    Headless { prompt: Box<[u8]> },
}

impl Mode {
    /// The charter's waiting allowance; a headless wait parks immediately.
    #[must_use]
    pub const fn waiting(&self) -> skein_lib::Duration {
        match self {
            Self::Chat { waiting } => *waiting,
            Self::Headless { .. } => skein_lib::Duration::ZERO,
        }
    }
}

/// One configured chat and the policy for each activation.
#[derive(Debug)]
pub struct Config {
    /// Store key for this chat.
    pub chat: Box<[u8]>,
    /// Host-authored role text.
    pub instructions: Box<[u8]>,
    /// Ordered host-authored context.
    pub brief: charter::Brief,
    /// Main model first, followed by models available to sub-agents.
    pub models: Box<[charter::Llm]>,
    /// Accepted effort names in the same order as the startup models.
    pub efforts: Box<[Box<[charter::Effort]>]>,
    /// Whether main may open sub-agents; children cannot delegate.
    pub sub_agents: bool,
    /// Declared deadlines in the same order as `models`, kept outside charters.
    pub progress: Box<[smith_domain::session::Progress]>,
    /// Startup-measured provider/schema bytes, in the same order as models.
    pub protocol_allowances: Box<[u64]>,
    /// Activation budget in the host's unit.
    pub budget: run::Budget,
    /// Main's finishing turns and time, kept outside child-opening budgets.
    pub reserve: charter::Reserve,
    /// Optional guide and check paths.
    pub conventions: Option<run::Conventions>,
    /// Result form; a report is the usual choice.
    pub contract: Contract,
    /// Separately granted checked mid-run delivery, if configured.
    pub deliver: Option<outcome::ChangeSpec>,
    /// Required change field used as the first commit-message paragraph.
    pub title_field: Box<[u8]>,
    /// Interactive waiting policy or the headless prompt carried at startup.
    pub mode: Mode,
    /// Ask the spawned agent for text for the terminal presentation.
    pub live_text: bool,
    /// Whether a saved transcript is supplied on start.
    pub resume: bool,
    /// Accounts for which grants must be fetched before a start.
    pub accounts: Box<[u32]>,
    /// Workspace authority; absent for a chat without files.
    pub workspace: Option<run::Workspace>,
    /// Optional push target by workspace position; a missing entry stays local.
    pub push: Option<Box<[Option<PushTarget>]>>,
}

/// Why configuration cannot be built within its receiving limits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Invalid {
    /// The chat budget was refused before the agent starts, naming its part and bound.
    Budget(run::BudgetRefusal),
    /// A local model is unpriced at this rate, refused before agent startup.
    Price(run::Rate),
    /// Chat name is empty or too large.
    Chat,
    /// Instructions or brief exceed their receiving bound.
    Text,
    /// There is no main model, or models exceed the count bound.
    Models,
    /// An account is missing, repeated or beyond capacity.
    Accounts,
    /// A model's endpoint is absent from the agent configuration.
    Endpoint,
    /// The agent cannot fit the chosen limits.
    Limits,
    /// A configured delivery has no bounded title field.
    Contract,
    /// Push targets do not match the configured git directories.
    Push,
}

impl Config {
    /// Validate owned local policy before constructing the agent.
    pub fn validate(&self, limits: &Limits) -> Result<(), Invalid> {
        self.check_budget(limits)?;
        self.check_mode(limits)?;
        if self.chat.is_empty() || self.chat.len() > usize::try_from(limits.chat_bytes).expect("u32 fits usize") {
            return Err(Invalid::Chat);
        }
        if self.instructions.len() > usize::try_from(limits.text_bytes).expect("u32 fits usize") {
            return Err(Invalid::Text);
        }
        if self.title_field.is_empty()
            || self.title_field.len() > usize::try_from(limits.text_bytes).expect("u32 fits usize")
        {
            return Err(Invalid::Text);
        }
        match crate::max_line_bytes(limits.smith.run.message_bytes) {
            Some(max) if limits.line_bytes <= max => {}
            Some(_) | None => return Err(Invalid::Limits),
        }
        match &self.contract {
            Contract::Change(spec) => {
                if !has_title(spec, &self.title_field) {
                    return Err(Invalid::Contract);
                }
            }
            Contract::Report(_) => {}
        }
        if let Some(spec) = &self.deliver
            && !has_title(spec, &self.title_field)
        {
            return Err(Invalid::Contract);
        }
        let mut brief_bytes = 0_u64;
        for section in &self.brief.sections {
            let Ok(title) = u64::try_from(section.title.len()) else {
                return Err(Invalid::Text);
            };
            let Ok(text) = u64::try_from(section.text.len()) else {
                return Err(Invalid::Text);
            };
            brief_bytes = brief_bytes.checked_add(title).ok_or(Invalid::Text)?;
            brief_bytes = brief_bytes.checked_add(text).ok_or(Invalid::Text)?;
        }
        if brief_bytes > u64::from(limits.text_bytes) {
            return Err(Invalid::Text);
        }
        if self.progress.len() != self.models.len()
            || self.models.is_empty()
            || self.models.len() > usize::try_from(limits.models).expect("u32 fits usize")
            || self.models.len() > usize::try_from(limits.smith.run.models.saturating_add(1)).expect("u32 fits usize")
        {
            return Err(Invalid::Models);
        }
        if limits.endpoints.len() > usize::try_from(limits.smith.endpoints).expect("u32 fits usize") {
            return Err(Invalid::Endpoint);
        }
        for (index, endpoint) in limits.endpoints.iter().enumerate() {
            if limits.endpoints.get(index.saturating_add(1)..).unwrap_or_default().contains(endpoint) {
                return Err(Invalid::Endpoint);
            }
        }
        if self.accounts.len() > usize::try_from(limits.smith.accounts).expect("u32 fits usize") {
            return Err(Invalid::Accounts);
        }
        if let Some(workspace) = &self.workspace
            && (workspace.directories.is_empty()
                || workspace.directories.len() > usize::try_from(limits.smith.run.directories).expect("u32 fits usize"))
        {
            return Err(Invalid::Limits);
        }
        if let Some(push) = &self.push {
            let Some(workspace) = &self.workspace else { return Err(Invalid::Push) };
            if push.len() != workspace.directories.len() {
                return Err(Invalid::Push);
            }
            for (target, directory) in push.iter().zip(workspace.directories.iter()) {
                if let Some(target) = target
                    && (!directory.git
                        || !directory.writable
                        || target.remote.is_empty()
                        || target.branch.is_empty()
                        || target.remote.len() > usize::try_from(limits.text_bytes).expect("u32 fits usize")
                        || target.branch.len() > usize::try_from(limits.text_bytes).expect("u32 fits usize"))
                {
                    return Err(Invalid::Push);
                }
            }
        }
        for (index, account) in self.accounts.iter().enumerate() {
            if self.accounts.get(index.saturating_add(1)..).unwrap_or_default().contains(account) {
                return Err(Invalid::Accounts);
            }
        }
        self.check_models(limits)?;
        if !crate::valid(limits) {
            return Err(Invalid::Limits);
        }
        Ok(())
    }

    fn check_mode(&self, limits: &Limits) -> Result<(), Invalid> {
        match &self.mode {
            Mode::Headless { prompt }
                if prompt.len() > usize::try_from(limits.line_bytes).expect("line bound fits") =>
            {
                Err(Invalid::Text)
            }
            Mode::Chat { .. } | Mode::Headless { .. } => Ok(()),
        }
    }

    fn check_budget(&self, limits: &Limits) -> Result<(), Invalid> {
        let checks = match &self.contract {
            Contract::Change(_) => Some(limits.smith.run.check_timeout),
            Contract::Report(_) => {
                if self.deliver.is_some() {
                    Some(limits.smith.run.check_timeout)
                } else {
                    None
                }
            }
        };
        let main = self.models.first().ok_or(Invalid::Models)?;
        match main.prices.check() {
            Ok(()) => {}
            Err(rate) => return Err(Invalid::Price(rate)),
        }
        if main.window == 0 || main.output == 0 || main.prices.unit == 0 {
            return Err(Invalid::Models);
        }
        match self.budget.check(&limits.smith.run.budget) {
            Ok(()) => {}
            Err(reason) => return Err(Invalid::Budget(reason)),
        }
        let most = run::most_costly(main).ok_or(Invalid::Budget(run::BudgetRefusal::Reserve(run::Part::Spend)))?;
        if most > self.budget.spend {
            return Err(Invalid::Budget(run::BudgetRefusal::Completion { most }));
        }
        match run::admit(&self.budget, self.reserve, main, &limits.smith.run.budget, checks) {
            Ok(()) => {}
            Err(reason) => match reason {
                run::BudgetRefusal::Reserve(part) => match part {
                    run::Part::Turns => {}
                    run::Part::Spend | run::Part::Time => return Err(Invalid::Budget(reason)),
                },
                run::BudgetRefusal::Ceiling(_)
                | run::BudgetRefusal::Empty(_)
                | run::BudgetRefusal::Completion { .. }
                | run::BudgetRefusal::Checks { .. } => return Err(Invalid::Budget(reason)),
            },
        }
        if self.mode.waiting() > limits.smith.run.waiting {
            return Err(Invalid::Budget(run::BudgetRefusal::Ceiling(run::Ceiling::Time(limits.smith.run.waiting))));
        }
        Ok(())
    }

    #[expect(clippy::manual_let_else, reason = "strict subset keeps checked construction exhaustive")]
    fn check_models(&self, limits: &Limits) -> Result<(), Invalid> {
        if self.protocol_allowances.len() != self.models.len() || self.efforts.len() != self.models.len() {
            return Err(Invalid::Models);
        }
        for allowance in &self.protocol_allowances {
            if *allowance > limits.smith.session.protocol_allowance {
                return Err(Invalid::Models);
            }
        }
        let mut model_bytes = 0_u64;
        for (index, model) in self.models.iter().enumerate() {
            match model.prices.check() {
                Ok(()) => {}
                Err(rate) => return Err(Invalid::Price(rate)),
            }
            if model.model.is_empty() || model.window == 0 || model.output == 0 {
                return Err(Invalid::Models);
            }
            for other in self.models.get(index.saturating_add(1)..).unwrap_or_default() {
                if model.endpoint == other.endpoint && model.model == other.model {
                    return Err(Invalid::Models);
                }
            }
            let owned = match smith_domain::ConfiguredModel::worst_case(
                model.model.len(),
                self.efforts.get(index).expect("matched effort inventory"),
            ) {
                Some(owned) => owned,
                None => return Err(Invalid::Limits),
            };
            model_bytes = match model_bytes.checked_add(owned) {
                Some(bytes) => bytes,
                None => return Err(Invalid::Limits),
            };
            if model_bytes > limits.smith.configured_model_bytes {
                return Err(Invalid::Limits);
            }
            if let Some(account) = model.account
                && !self.accounts.contains(&account)
            {
                return Err(Invalid::Accounts);
            }
            if !limits.endpoints.contains(&model.endpoint) {
                return Err(Invalid::Endpoint);
            }
        }
        Ok(())
    }
}

fn has_title(spec: &outcome::ChangeSpec, title: &[u8]) -> bool {
    for field in &spec.fields {
        if field.name.as_ref() == title && field.max > 0 {
            return true;
        }
    }
    false
}

/// Translate local choices into the agent's charter for one start.
#[must_use]
pub fn charter(config: &Config) -> charter::Charter {
    let workspace = config.workspace.as_ref();
    let mut writable = false;
    if let Some(workspace) = workspace {
        for directory in &workspace.directories {
            if directory.writable {
                writable = true;
            }
        }
    }
    let report = match &config.contract {
        Contract::Report(spec) => Some(spec.clone()),
        Contract::Change(_) => None,
    };
    let change = match &config.contract {
        Contract::Report(_) => None,
        Contract::Change(spec) => Some(spec.clone()),
    };
    let main = config.models.first().expect("validated configuration has a main model").clone();
    let models: Box<[charter::Llm]> =
        if config.sub_agents { Box::from(config.models.get(1..).unwrap_or_default()) } else { Box::new([]) };
    let mut sections = List::with_capacity(u32::try_from(config.brief.sections.len()).expect("validated brief count"));
    for section in &config.brief.sections {
        sections
            .push(charter::Section { title: section.title.clone(), text: section.text.clone() })
            .expect("validated brief capacity");
    }
    charter::Charter {
        mode: charter::Mode::Finite,
        resume: config.resume,
        waiting: config.mode.waiting(),
        instructions: config.instructions.clone(),
        brief: charter::Brief { sections: sections.into_boxed() },
        conventions: config.conventions.clone(),
        grants: charter::Grants {
            wait: true,
            deliver: config.deliver.clone(),
            tools: charter::Tools { inspect: workspace.is_some(), modify: writable, shell: workspace.is_some() },
            agents: config.sub_agents,
            host_tools: Box::new([]),
        },
        outcome: outcome::OutcomeSpec { change, verdicts: Box::new([]), report, failure: None },
        budget: config.budget,
        reserve: config.reserve,
        llm: main,
        models,
    }
}
