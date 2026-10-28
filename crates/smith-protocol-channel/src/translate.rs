//! Charter v4 translation into the run's policy data (protocol/charter.md,
//! sections 2, 5 and 7; protocol/agent.md, section 4).
//!
//! The caller supplies configured endpoint names and their domain identities.
//! This module keeps no state, knows no addresses or credential values, and
//! returns either owned domain policy or a typed invalid-start reason.

use alloc::boxed::Box;
use skein_lib::{List, Reader, Token, Writer};
use smith_channel as channel;
use smith_charter as wire;
use smith_domain::run;

use crate::Error;

/// One directory whose root path the agent service attaches to io.
#[derive(Debug)]
pub struct Mount {
    pub name: Box<[u8]>,
    pub path: Box<[u8]>,
    pub writable: bool,
    pub git: bool,
    pub conflicts: Box<[Box<[u8]>]>,
}

/// One grant name and validity; the agent channel keeps its credential value.
#[derive(Debug)]
pub struct Grant {
    pub name: smith_domain::GrantName,
    pub valid: skein_lib::Duration,
    /// A key is never renewed by its host (protocol/channel.md, section 6).
    pub renewable: bool,
}

/// Start context after channel and charter decoding, before io root attachment.
#[derive(Debug)]
pub struct DecodedStart {
    pub live: Option<channel::LiveAsk>,
    /// Opaque host identity preserved through framing and charter decoding.
    pub identity: Option<run::Identity>,
    pub messages: Box<[run::Message]>,
    pub activation: u64,
    pub charter: run::Charter,
    pub mounts: Option<Box<[Mount]>>,
    pub transcript: Option<smith_domain_session::record::Transcript>,
    pub answered: Box<[smith_domain::AnsweredCall]>,
    pub grants: Box<[Grant]>,
    pub window: smith_domain::Window,
}

/// Move decoded channel fields into bounded start context, keeping values below the domain.
#[expect(clippy::manual_map, reason = "strict subset keeps optional boundary conversion as an explicit match")]
pub fn start_context(
    start: channel::Start,
    charter: run::Charter,
    transcript: Option<smith_domain_session::record::Transcript>,
) -> Result<DecodedStart, Error> {
    let parts = start.into_parts();
    let mounts = match parts.workspace {
        Some(workspace) => {
            let mut mounts = List::with_capacity(workspace.directories().len());
            for directory in workspace.directories() {
                let mount = Mount {
                    name: Box::from(directory.name()),
                    path: Box::from(directory.path()),
                    writable: directory.writable(),
                    git: directory.git(),
                    conflicts: directory.conflicts().to_boxed(),
                };
                if mounts.push(mount).is_err() {
                    return Err(Error::ResultCapacity);
                }
            }
            Some(mounts.into_boxed())
        }
        None => None,
    };
    let mut grants = List::with_capacity(parts.grants.len());
    for grant in &parts.grants {
        let item = Grant {
            name: smith_domain::GrantName { account: grant.account(), generation: grant.generation() },
            valid: grant.valid(),
            renewable: renewable(grant.value()),
        };
        if grants.push(item).is_err() {
            return Err(Error::ResultCapacity);
        }
    }
    let mut messages = List::with_capacity(parts.messages.len());
    for message in parts.messages.into_boxed() {
        let parts = message.into_parts();
        if messages
            .push(run::Message {
                finish_requested: parts.finish_requested,
                name: Token::new(parts.name),
                label: parts.label,
                text: parts.text,
            })
            .is_err()
        {
            return Err(Error::ResultCapacity);
        }
    }
    Ok(DecodedStart {
        live: parts.live,
        identity: match parts.identity {
            Some(bytes) => Some(run::Identity::new(bytes)),
            None => None,
        },
        messages: messages.into_boxed(),
        activation: parts.activation,
        charter,
        mounts,
        transcript,
        answered: saved_answers(parts.answered.as_slice())?,
        grants: grants.into_boxed(),
        window: smith_domain::Window { turns: parts.window.turns(), bytes: parts.window.bytes() },
    })
}

/// Move post-transcript host decisions into the domain's settled vocabulary.
fn saved_answers(source: &[channel::AnsweredCall]) -> Result<Box<[smith_domain::AnsweredCall]>, Error> {
    let Ok(count) = u32::try_from(source.len()) else {
        return Err(Error::InvalidSavedAnswer);
    };
    let mut answered = List::with_capacity(count);
    for call in source {
        let name = call.name();
        let reply = match call.reply() {
            channel::SavedReply::Host(host) => {
                let answer =
                    run::HostAnswer::new(Box::from(host.text()), host.error()).ok_or(Error::InvalidSavedAnswer)?;
                smith_domain::Answered::Host(answer)
            }
            channel::SavedReply::Delivery(delivery) => {
                smith_domain::Answered::Delivery(Box::new(saved_delivery(delivery.value())?))
            }
            channel::SavedReply::TooLarge => smith_domain::Answered::TooLarge,
        };
        let item = smith_domain::AnsweredCall {
            name: run::CallName {
                activation: name.activation(),
                completion: name.completion(),
                position: name.position(),
            },
            tool: Box::from(call.tool()),
            answer: reply,
        };
        if answered.push(item).is_err() {
            return Err(Error::InvalidSavedAnswer);
        }
    }
    Ok(answered.into_boxed())
}

pub(crate) fn saved_delivery(source: &channel::Delivery) -> Result<run::Delivery, Error> {
    let delivery = match source {
        channel::Delivery::Delivered(delivered) => {
            let mut receipts = List::with_capacity(delivered.receipts().len());
            for receipt in delivered.receipts() {
                let receipt = run::Receipt::new(receipt.directory(), Box::from(receipt.text()))
                    .ok_or(Error::InvalidSavedAnswer)?;
                if receipts.push(receipt).is_err() {
                    return Err(Error::InvalidSavedAnswer);
                }
            }
            let evidence = run::Delivered::new(receipts.into_boxed()).ok_or(Error::InvalidSavedAnswer)?;
            run::Delivery::Delivered(evidence)
        }
        channel::Delivery::Nothing => run::Delivery::Nothing,
        channel::Delivery::Refused(refused) => {
            let marker = match refused.marker() {
                Some(marker) => Some(
                    run::Marker::new(marker.directory(), Box::from(marker.path())).ok_or(Error::InvalidSavedAnswer)?,
                ),
                None => None,
            };
            let refusal =
                run::DeliveryRefusal::new(marker, Box::from(refused.explanation())).ok_or(Error::InvalidSavedAnswer)?;
            run::Delivery::Refused(refusal)
        }
        channel::Delivery::Failed(failed) => {
            let reason = match failed.reason() {
                channel::DeliveryReason::Unreachable => run::DeliveryReason::Unreachable,
                channel::DeliveryReason::RefusedByTarget => run::DeliveryReason::RefusedByTarget,
                channel::DeliveryReason::TimedOut => run::DeliveryReason::TimedOut,
                channel::DeliveryReason::Broken => run::DeliveryReason::Broken,
                channel::DeliveryReason::TooLarge => run::DeliveryReason::TooLarge,
                channel::DeliveryReason::Missing => run::DeliveryReason::Missing,
                channel::DeliveryReason::Busy => run::DeliveryReason::Busy,
                channel::DeliveryReason::Unavailable => run::DeliveryReason::Unavailable,
                channel::DeliveryReason::Cancelled => run::DeliveryReason::Cancelled,
                channel::DeliveryReason::Unknown => run::DeliveryReason::Unknown,
            };
            run::Delivery::Failed(run::DeliveryFailure {
                directory: failed.directory(),
                reason,
                diagnostic: run::Diagnostic::new(failed.diagnostic(), failed.dropped()),
            })
        }
        channel::Delivery::Stale => run::Delivery::Stale,
    };
    Ok(delivery)
}

/// One configured endpoint name and its domain-visible identities.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoint {
    /// Byte name carried by charters and transcripts.
    pub name: Box<[u8]>,
    /// Domain-visible endpoint identity.
    pub number: u32,
    /// Opaque replay dialect identity.
    pub dialect: u32,
}

/// Bounded endpoint names resolved before an agent starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoints {
    entries: List<Endpoint>,
}

impl Endpoints {
    /// Retain the checked startup endpoint table.
    #[must_use]
    pub fn new(entries: List<Endpoint>) -> Endpoints {
        Endpoints { entries }
    }

    /// Number of names retained by the agent.
    #[must_use]
    pub(crate) fn len(&self) -> u32 {
        self.entries.len()
    }

    /// Whether the configured names fit the channel's endpoint allowance.
    #[must_use]
    pub fn fits(&self, count: u32, name_bytes: u32) -> bool {
        if self.len() > count {
            return false;
        }
        for endpoint in &self.entries {
            match u32::try_from(endpoint.name.len()) {
                Ok(length) if length <= name_bytes => {}
                Ok(_) | Err(_) => return false,
            }
        }
        true
    }

    /// Find a charter name without exposing endpoint addresses.
    #[must_use]
    #[expect(clippy::manual_find, reason = "production steps do not use closures")]
    pub fn resolve(&self, name: &[u8]) -> Option<&Endpoint> {
        for endpoint in &self.entries {
            if endpoint.name.as_ref() == name {
                return Some(endpoint);
            }
        }
        None
    }

    /// Find the startup wire name by its unique configured endpoint number.
    #[must_use]
    pub fn name_for(&self, number: u32) -> Option<&[u8]> {
        for endpoint in &self.entries {
            if endpoint.number == number {
                return Some(&endpoint.name);
            }
        }
        None
    }

    /// Find the configured wire name for one domain endpoint and dialect.
    #[must_use]
    pub fn name_of(&self, number: u32, dialect: u32) -> Option<&[u8]> {
        for endpoint in &self.entries {
            if endpoint.number == number && endpoint.dialect == dialect {
                return Some(&endpoint.name);
            }
        }
        None
    }
}

/// Decode one charter and resolve all LLM endpoint names before admission.
#[expect(clippy::manual_let_else, reason = "explicit exhaustive result handling")]
pub fn decode_charter(
    bytes: &[u8],
    limits: &wire::v4::Limits,
    endpoints: &Endpoints,
) -> Result<run::Charter, run::Invalid> {
    if bytes.len() < 2 {
        return Err(run::Invalid::MalformedCharter);
    }
    if bytes.get(..2) != Some(&[0, 4][..]) {
        return Err(run::Invalid::CharterVersion);
    }
    let charter = match wire::Charter::decode(limits, &mut Reader::new(bytes)) {
        Ok(charter) => charter,
        Err(_) => return Err(run::Invalid::MalformedCharter),
    };
    translate_charter(&charter, endpoints)
}

#[expect(clippy::manual_map, reason = "production steps do not use closures")]
fn translate_charter(charter: &wire::Charter, endpoints: &Endpoints) -> Result<run::Charter, run::Invalid> {
    let mut sections = List::with_capacity(charter.brief().len());
    for section in charter.brief() {
        let item = run::Section { title: Box::from(section.title()), text: Box::from(section.body()) };
        if sections.push(item).is_err() {
            return Err(run::Invalid::TooLarge);
        }
    }

    let tools = charter.tools();
    let families = tools.families();
    let mut host_tools = List::with_capacity(tools.host().len());
    for tool in tools.host() {
        let effect = match tool.effect() {
            wire::Effect::Read => run::HostEffect::Read,
            wire::Effect::Write => run::HostEffect::Write,
        };
        let item = run::HostTool {
            name: Box::from(tool.name()),
            description: Box::from(tool.description()),
            schema: Box::from(tool.input()),
            effect,
            timeout: tool.deadline(),
        };
        if host_tools.push(item).is_err() {
            return Err(run::Invalid::TooLarge);
        }
    }

    let deliver = match tools.deliver() {
        Some(rule) => Some(change_rule(rule)?),
        None => None,
    };
    let conventions = match charter.conventions() {
        Some(paths) => Some(run::Conventions { guide: Box::from(paths.guide()), checks: Box::from(paths.checks()) }),
        None => None,
    };
    let mut models = List::with_capacity(charter.models().len());
    for model in charter.models() {
        if models.push(llm(model, endpoints)?).is_err() {
            return Err(run::Invalid::TooLarge);
        }
    }
    let budget = charter.budget();
    Ok(run::Charter {
        mode: match charter.mode() {
            wire::Mode::Finite => run::Mode::Finite,
            wire::Mode::Open => run::Mode::Open,
        },
        resume: charter.resume(),
        waiting: charter.waiting(),
        instructions: Box::from(charter.instructions()),
        brief: run::Brief { sections: sections.into_boxed() },
        conventions,
        grants: run::charter::Grants {
            wait: tools.wait(),
            deliver,
            tools: run::charter::Tools {
                inspect: families.inspect(),
                modify: families.modify(),
                shell: families.shell(),
            },
            agents: families.agents(),
            host_tools: host_tools.into_boxed(),
        },
        outcome: contract(charter.contract())?,
        budget: run::Budget { turns: budget.turns(), spend: budget.spend(), time: budget.time() },
        reserve: run::charter::Reserve { turns: budget.reserve().turns(), time: budget.reserve().time() },
        llm: llm(charter.main(), endpoints)?,
        models: models.into_boxed(),
    })
}

#[expect(clippy::manual_let_else, reason = "explicit exhaustive option handling")]
fn llm(value: &wire::Llm, endpoints: &Endpoints) -> Result<run::charter::Llm, run::Invalid> {
    let endpoint = match endpoints.resolve(value.endpoint()) {
        Some(endpoint) => endpoint,
        None => return Err(run::Invalid::Endpoint),
    };
    let prices = value.prices();
    Ok(run::charter::Llm {
        prices: run::Prices {
            input: prices.input(),
            cached: prices.cached(),
            cache_write: *prices.cache_write(),
            output: prices.output(),
            unit: prices.unit(),
        },
        dialect: endpoint.dialect,
        account: *value.account(),
        endpoint: run::charter::Endpoint(endpoint.number),
        model: Box::from(value.model()),
        effort: value.effort().clone(),
        window: value.window(),
        output: value.output(),
    })
}

fn contract(value: &wire::Contract) -> Result<run::outcome::OutcomeSpec, run::Invalid> {
    let report = match value.report() {
        Some(rule) => Some(text_rule(rule)?),
        None => None,
    };
    let change = match value.change() {
        Some(rule) => Some(change_rule(rule)?),
        None => None,
    };
    let failure = match value.failure() {
        Some(rule) => Some(text_rule(rule)?),
        None => None,
    };
    let mut verdicts = List::with_capacity(value.verdicts().len());
    for rule in value.verdicts() {
        let mut kinds = List::with_capacity(rule.kinds().len());
        for kind in rule.kinds() {
            let item = run::outcome::ItemRule { kind: Box::from(kind.kind()), fields: field_rules(kind.fields())? };
            if kinds.push(item).is_err() {
                return Err(run::Invalid::TooLarge);
            }
        }
        let item = run::outcome::VerdictRule {
            name: Box::from(rule.label()),
            text_max: rule.text_max(),
            fields: field_rules(rule.fields())?,
            items: run::outcome::ItemSpec { min: rule.least(), max: rule.most(), kinds: kinds.into_boxed() },
        };
        if verdicts.push(item).is_err() {
            return Err(run::Invalid::TooLarge);
        }
    }
    Ok(run::outcome::OutcomeSpec { change, verdicts: verdicts.into_boxed(), report, failure })
}

fn text_rule(value: &wire::TextRule) -> Result<run::outcome::TextSpec, run::Invalid> {
    Ok(run::outcome::TextSpec { max: value.max(), fields: field_rules(value.fields())? })
}

fn change_rule(value: &wire::ChangeRule) -> Result<run::outcome::ChangeSpec, run::Invalid> {
    Ok(run::outcome::ChangeSpec { checks_must_pass: value.checks_must_pass(), fields: field_rules(value.fields())? })
}

fn field_rules(value: &List<wire::FieldRule>) -> Result<Box<[run::outcome::FieldRule]>, run::Invalid> {
    let mut fields = List::with_capacity(value.len());
    for field in value {
        let item = run::outcome::FieldRule { name: Box::from(field.name()), max: field.max() };
        if fields.push(item).is_err() {
            return Err(run::Invalid::TooLarge);
        }
    }
    Ok(fields.into_boxed())
}

/// Write an accepted domain result in the current charter family.
pub fn encode_result(result: &run::outcome::Declared, limits: &wire::v4::Limits) -> Result<Box<[u8]>, Error> {
    let mut fields = List::with_capacity(limits.run_result_fields);
    let mut items = List::with_capacity(limits.run_result_items);
    let (form, label, text, values) = match result {
        run::outcome::Declared::Change(change) => (wire::Form::Change, None, &[][..], &change.fields[..]),
        run::outcome::Declared::Verdict(verdict) => {
            for item in &verdict.items {
                let mut item_fields = List::with_capacity(limits.item_fields);
                copy_fields(&item.fields, &mut item_fields, limits)?;
                let item = wire::Item::new(limits, wire::ItemParts { kind: item.kind.clone(), fields: item_fields })?;
                if items.push(item).is_err() {
                    return Err(Error::ResultCapacity);
                }
            }
            (wire::Form::Verdict, Some(verdict.name.clone()), &verdict.text[..], &verdict.fields[..])
        }
        run::outcome::Declared::Report(report) => (wire::Form::Report, None, &report.text[..], &report.fields[..]),
        run::outcome::Declared::Failure(failure) => {
            (wire::Form::Failure, None, &failure.reason[..], &failure.fields[..])
        }
    };
    copy_fields(values, &mut fields, limits)?;
    let record =
        wire::RunResult::new(limits, wire::RunResultParts { form, label, text: Box::from(text), fields, items })?;
    let Ok(length) = usize::try_from(record.measure()) else {
        return Err(Error::ResultCapacity);
    };
    let mut writer = Writer::new(length);
    record.encode(&mut writer)?;
    Ok(writer.finish())
}

fn copy_fields(
    values: &[run::outcome::Field],
    target: &mut List<wire::Field>,
    limits: &wire::v4::Limits,
) -> Result<(), Error> {
    for field in values {
        let value = wire::Field::new(limits, wire::FieldParts { name: field.name.clone(), text: field.value.clone() })?;
        if target.push(value).is_err() {
            return Err(Error::ResultCapacity);
        }
    }
    Ok(())
}

/// Translate the domain's single Start terminal into the channel's last word.
#[expect(clippy::manual_map, reason = "the protocol subset keeps option projection exhaustive")]
pub fn answer_record(
    now: skein_lib::Time,
    answer: run::Answer,
    read: Option<Token>,
    bodies: &channel::Limits,
    charter: &wire::v4::Limits,
) -> Result<channel::Answer, Error> {
    let (turns, spent, result) = match answer {
        run::Answer::Refused(refusal) => {
            let reason = match refusal {
                run::Refusal::Busy => channel::StartRefusal::Busy,
                run::Refusal::Invalid(invalid) => {
                    let value = channel::InvalidStartValue::new(
                        bodies,
                        channel::InvalidStartValueParts { value: invalid_start(invalid, bodies)? },
                    )?;
                    channel::StartRefusal::Invalid(value)
                }
            };
            let refused = channel::Refused::new(bodies, channel::RefusedParts { reason })?;
            (0, 0, channel::RunResult::Refused(refused))
        }
        run::Answer::Parked { spent, turns } => (turns, spent.units, channel::RunResult::Parked),
        run::Answer::Accepted { outcome, spent, turns } => {
            let result = encode_result(&outcome, charter)?;
            let accepted = channel::Accepted::new(bodies, channel::AcceptedParts { result })?;
            (turns, spent.units, channel::RunResult::Accepted(accepted))
        }
        run::Answer::Failed { failure, spent, turns } => {
            let reason = failure_record(failure, bodies, now)?;
            let failed = channel::Failed::new(bodies, channel::FailedParts { reason })?;
            (turns, spent.units, channel::RunResult::Failed(failed))
        }
    };
    let last_read = match read {
        Some(name) => Some(name.raw()),
        None => None,
    };
    Ok(channel::Answer::new(bodies, channel::AnswerParts { turns, spent, last_read, result })?)
}

pub(crate) fn invalid_start(invalid: run::Invalid, limits: &channel::Limits) -> Result<channel::InvalidStart, Error> {
    Ok(match invalid {
        run::Invalid::CharterVersion => channel::InvalidStart::CharterVersion,
        run::Invalid::MalformedCharter => channel::InvalidStart::MalformedCharter,
        run::Invalid::Endpoint => channel::InvalidStart::Endpoint,
        run::Invalid::Messages => channel::InvalidStart::Messages,
        run::Invalid::Activation => channel::InvalidStart::Activation,
        run::Invalid::Window => channel::InvalidStart::Window,
        run::Invalid::Conventions => channel::InvalidStart::Conventions,
        run::Invalid::TooLarge => channel::InvalidStart::TooLarge,
        run::Invalid::Workspace => channel::InvalidStart::Workspace,
        run::Invalid::Grants => channel::InvalidStart::Grants,
        run::Invalid::Outcome => channel::InvalidStart::Outcome,
        run::Invalid::Budget(refusal) => channel::InvalidStart::Budget(channel::BudgetRefusalValue::new(
            limits,
            channel::BudgetRefusalValueParts { value: budget_refusal(refusal, limits)? },
        )?),
        run::Invalid::Price(rate) => channel::InvalidStart::Price(channel::PriceRateValue::new(
            limits,
            channel::PriceRateValueParts { value: price_rate(rate) },
        )?),
        run::Invalid::Llm => channel::InvalidStart::Llm,
        run::Invalid::Effort => channel::InvalidStart::Effort,
        run::Invalid::Conversation => channel::InvalidStart::Conversation,
        run::Invalid::HostPart { bound } => {
            channel::InvalidStart::HostPart(channel::WindowBound::new(limits, channel::WindowBoundParts { bound })?)
        }
        run::Invalid::Room { bound } => {
            channel::InvalidStart::Room(channel::WindowBound::new(limits, channel::WindowBoundParts { bound })?)
        }
    })
}

fn failure_record(
    failure: run::Failure,
    limits: &channel::Limits,
    now: skein_lib::Time,
) -> Result<channel::RunFailure, Error> {
    let record = match failure {
        run::Failure::Transcript(refusal) => {
            let value = match refusal {
                run::TranscriptRefusal::Version => channel::TranscriptRefusal::Version,
                run::TranscriptRefusal::Malformed => channel::TranscriptRefusal::Malformed,
                run::TranscriptRefusal::Unresolved => channel::TranscriptRefusal::Unresolved,
                run::TranscriptRefusal::TooLarge => channel::TranscriptRefusal::TooLarge,
            };
            channel::RunFailure::Transcript(channel::TranscriptRefusalValue::new(
                limits,
                channel::TranscriptRefusalValueParts { value },
            )?)
        }
        run::Failure::Model(fault) => {
            let value = model_fault(fault, now, limits)?;
            channel::RunFailure::Model(channel::ModelFaultValue::new(limits, channel::ModelFaultValueParts { value })?)
        }
        run::Failure::Budget { exhausted, used, bound } => {
            let value = budget_failure(exhausted, limits)?;
            channel::RunFailure::Budget(channel::BudgetFailureValue::new(
                limits,
                channel::BudgetFailureValueParts { value, used, bound },
            )?)
        }
        run::Failure::Policy(run::Policy::Unfinished { nudges, rejected }) => channel::RunFailure::Policy(
            channel::PolicyFailure::new(limits, channel::PolicyFailureParts { nudges, rejected })?,
        ),
        run::Failure::Cancelled => channel::RunFailure::Cancelled,
        run::Failure::Stale => channel::RunFailure::Stale,
    };
    Ok(record)
}

#[expect(clippy::manual_map, reason = "the protocol subset projects the optional cooldown without a closure")]
fn model_fault(
    fault: run::Fault,
    now: skein_lib::Time,
    limits: &channel::Limits,
) -> Result<channel::ModelFault, Error> {
    let value = match fault {
        run::Fault::Completion { failure, evidence, model, attempts, report } => {
            let (failure, retry_after) = match failure {
                run::CompletionFailure::Limit { which, bound } => (
                    channel::CompletionFailure::Limit(channel::LimitFailure::new(
                        limits,
                        channel::LimitFailureParts { which: completion_limit(which, limits)?, bound },
                    )?),
                    None,
                ),
                run::CompletionFailure::ApplicationLimit => (channel::CompletionFailure::ApplicationLimit, None),
                run::CompletionFailure::Protocol => (channel::CompletionFailure::Protocol, None),
                run::CompletionFailure::Cancelled => (channel::CompletionFailure::Cancelled, None),
                run::CompletionFailure::Overloaded => (channel::CompletionFailure::Overloaded, None),
                run::CompletionFailure::Unavailable => (channel::CompletionFailure::Unavailable, None),
                run::CompletionFailure::TimedOut { phase } => (
                    channel::CompletionFailure::TimedOut(channel::TimeoutFailure::new(
                        limits,
                        channel::TimeoutFailureParts { phase: completion_phase(phase) },
                    )?),
                    None,
                ),
                run::CompletionFailure::ContextTooLong => (channel::CompletionFailure::ContextTooLong, None),
                run::CompletionFailure::Invalid => (channel::CompletionFailure::Invalid, None),
                run::CompletionFailure::Refused => (channel::CompletionFailure::Refused, None),
                run::CompletionFailure::Unsupported => (channel::CompletionFailure::Unsupported, None),
                run::CompletionFailure::Unauthorized => (channel::CompletionFailure::Unauthorized, None),
                run::CompletionFailure::RateLimited { retry_after } => {
                    (channel::CompletionFailure::RateLimited, Some(retry_after))
                }
                run::CompletionFailure::Exhausted { retry_after } => {
                    (channel::CompletionFailure::Exhausted, retry_after)
                }
            };
            let evidence = match evidence {
                run::CompletionEvidence::Unsent => channel::CompletionEvidence::Unsent,
                run::CompletionEvidence::Unknown => channel::CompletionEvidence::Unknown,
                run::CompletionEvidence::Response { status } => channel::CompletionEvidence::Response(
                    channel::ResponseEvidence::new(limits, channel::ResponseEvidenceParts { status })?,
                ),
            };
            channel::ModelFault::Completion(channel::CompletionFault::new(
                limits,
                channel::CompletionFaultParts {
                    failure,
                    evidence,
                    retry_after,
                    model,
                    attempts,
                    code: report.code,
                    request_id: report.request,
                    field: report.field,
                },
            )?)
        }
        run::Fault::Limited { account, reset_at } => channel::ModelFault::Limited(channel::LimitedFault::new(
            limits,
            channel::LimitedFaultParts {
                account,
                reset: match reset_at {
                    Some(at) => Some(at.saturating_since(now)),
                    None => None,
                },
            },
        )?),
        run::Fault::Refused { account, code } => channel::ModelFault::Refused(channel::RefusedFault::new(
            limits,
            channel::RefusedFaultParts { account, code },
        )?),
        run::Fault::Unauthorized { account } => channel::ModelFault::Unauthorized(channel::UnauthorizedFault::new(
            limits,
            channel::UnauthorizedFaultParts { account },
        )?),
        run::Fault::Program { name, exit, detail } => channel::ModelFault::Program(channel::ProgramFault::new(
            limits,
            channel::ProgramFaultParts { name, exit, detail },
        )?),
        run::Fault::Provider => channel::ModelFault::Provider,
        run::Fault::Declined => channel::ModelFault::Declined,
        run::Fault::Truncated => channel::ModelFault::Truncated,
        run::Fault::Malformed => channel::ModelFault::Malformed,
    };
    Ok(value)
}

fn budget_failure(exhausted: run::Exhausted, limits: &channel::Limits) -> Result<channel::BudgetFailure, Error> {
    let value = match exhausted {
        run::Exhausted::Turns => channel::BudgetFailure::Turns,
        run::Exhausted::Spend => channel::BudgetFailure::Spend,
        run::Exhausted::Time => channel::BudgetFailure::Time,
        run::Exhausted::Tokens(receiving) => {
            let value = match receiving {
                run::ReceivingLimit::Input => channel::ReceivingLimit::Input,
                run::ReceivingLimit::Output => channel::ReceivingLimit::Output,
                run::ReceivingLimit::CacheRead => channel::ReceivingLimit::CacheRead,
                run::ReceivingLimit::CacheWrite => channel::ReceivingLimit::CacheWrite,
            };
            channel::BudgetFailure::Tokens(channel::ReceivingLimitValue::new(
                limits,
                channel::ReceivingLimitValueParts { value },
            )?)
        }
        run::Exhausted::Overflow(overflow) => {
            let value = match overflow {
                run::Overflow::Spend => channel::BudgetOverflow::Spend,
                run::Overflow::Usage => channel::BudgetOverflow::Usage,
            };
            channel::BudgetFailure::Overflow(channel::OverflowValue::new(
                limits,
                channel::OverflowValueParts { value },
            )?)
        }
    };
    Ok(value)
}

fn price_rate(rate: run::Rate) -> channel::PriceRate {
    match rate {
        run::Rate::Input => channel::PriceRate::Input,
        run::Rate::Cached => channel::PriceRate::Cached,
        run::Rate::CacheWrite => channel::PriceRate::CacheWrite,
        run::Rate::Output => channel::PriceRate::Output,
    }
}

fn budget_part(part: run::Part) -> channel::BudgetPart {
    match part {
        run::Part::Turns => channel::BudgetPart::Turns,
        run::Part::Spend => channel::BudgetPart::Spend,
        run::Part::Time => channel::BudgetPart::Time,
    }
}

fn budget_refusal(refusal: run::BudgetRefusal, limits: &channel::Limits) -> Result<channel::BudgetRefusal, Error> {
    Ok(match refusal {
        run::BudgetRefusal::Ceiling(ceiling) => {
            let value = match ceiling {
                run::Ceiling::Turns(value) => channel::BudgetCeiling::Turns(channel::BudgetTurns::new(
                    limits,
                    channel::BudgetTurnsParts { value },
                )?),
                run::Ceiling::Spend(value) => channel::BudgetCeiling::Spend(channel::BudgetSpend::new(
                    limits,
                    channel::BudgetSpendParts { value },
                )?),
                run::Ceiling::Time(value) => {
                    channel::BudgetCeiling::Time(channel::BudgetTime::new(limits, channel::BudgetTimeParts { value })?)
                }
            };
            channel::BudgetRefusal::Ceiling(channel::BudgetCeilingValue::new(
                limits,
                channel::BudgetCeilingValueParts { value },
            )?)
        }
        run::BudgetRefusal::Empty(part) => channel::BudgetRefusal::Empty(channel::BudgetPartValue::new(
            limits,
            channel::BudgetPartValueParts { value: budget_part(part) },
        )?),
        run::BudgetRefusal::Reserve(part) => channel::BudgetRefusal::Reserve(channel::BudgetPartValue::new(
            limits,
            channel::BudgetPartValueParts { value: budget_part(part) },
        )?),
        run::BudgetRefusal::Completion { most } => channel::BudgetRefusal::Completion(channel::BudgetCompletion::new(
            limits,
            channel::BudgetCompletionParts { most },
        )?),
        run::BudgetRefusal::Checks { reserve, checks } => channel::BudgetRefusal::Checks(channel::BudgetChecks::new(
            limits,
            channel::BudgetChecksParts { reserve, checks },
        )?),
    })
}

/// Normalize an absolute host path before its root is attached; no filesystem lookup.
/// The shell attests its absolute spelling, and the run retains only bounded bytes.
#[must_use]
pub fn host_path(path: &[u8], maximum: u32) -> Option<Box<[u8]>> {
    if path.first() != Some(&b'/') || path.len() > usize::try_from(maximum).ok()? {
        return None;
    }
    let capacity = maximum.checked_add(1)? / 2;
    let mut names = List::<&[u8]>::with_capacity(capacity);
    let mut kept = 0_u32;
    let mut start = 1_usize;
    for end in 1..=path.len() {
        if end == path.len() || path.get(end) == Some(&b'/') {
            let part = path.get(start..end)?;
            if part == b".." {
                kept = kept.saturating_sub(1);
            } else if !part.is_empty() && part != b"." {
                for byte in part {
                    if *byte == 0 {
                        return None;
                    }
                }
                match names.get_mut(kept) {
                    Some(previous) => *previous = part,
                    None => names.push(part).ok()?,
                }
                kept = kept.checked_add(1)?;
            }
            start = end.checked_add(1)?;
        }
    }
    let names = names.as_slice().get(..usize::try_from(kept).ok()?)?;
    let mut bytes = 1_usize;
    for (index, name) in names.iter().enumerate() {
        bytes = bytes.checked_add(name.len())?.checked_add(usize::from(index != 0))?;
    }
    let mut writer = Writer::new(bytes);
    writer.put(b"/").ok()?;
    for (index, name) in names.iter().enumerate() {
        if index != 0 {
            writer.put(b"/").ok()?;
        }
        writer.put(name).ok()?;
    }
    Some(writer.finish())
}

fn completion_limit(which: run::Limit, limits: &channel::Limits) -> Result<channel::CompletionLimit, Error> {
    let which = match which {
        run::Limit::Pool { reservation } => {
            channel::CompletionLimit::Pool(channel::PoolLimit::new(limits, channel::PoolLimitParts { reservation })?)
        }
        run::Limit::Path(cap) => channel::CompletionLimit::Path(channel::PathCap::new(
            limits,
            channel::PathCapParts { value: completion_cap(cap) },
        )?),
        run::Limit::History => channel::CompletionLimit::History,
        run::Limit::Window => channel::CompletionLimit::Window,
        run::Limit::Call => channel::CompletionLimit::Call,
        run::Limit::Message => channel::CompletionLimit::Message,
    };
    Ok(which)
}

const fn completion_cap(cap: run::Cap) -> channel::Cap {
    match cap {
        run::Cap::Request => channel::Cap::Request,
        run::Cap::RequestHead => channel::Cap::RequestHead,
        run::Cap::ResponseHead => channel::Cap::ResponseHead,
        run::Cap::ResponseFields => channel::Cap::ResponseFields,
        run::Cap::Skip => channel::Cap::Skip,
        run::Cap::Field => channel::Cap::Field,
        run::Cap::Retained => channel::Cap::Retained,
        run::Cap::Tokens => channel::Cap::Tokens,
        run::Cap::Depth => channel::Cap::Depth,
        run::Cap::Strings => channel::Cap::Strings,
        run::Cap::Number => channel::Cap::Number,
        run::Cap::Reasoning => channel::Cap::Reasoning,
        run::Cap::OutputItems => channel::Cap::OutputItems,
        run::Cap::Metadata => channel::Cap::Metadata,
        run::Cap::Tools => channel::Cap::Tools,
        run::Cap::HistoryItems => channel::Cap::HistoryItems,
        run::Cap::Output => channel::Cap::Output,
        run::Cap::Answer => channel::Cap::Answer,
        run::Cap::ErrorBody => channel::Cap::ErrorBody,
        run::Cap::AccessToken => channel::Cap::AccessToken,
        run::Cap::AccountId => channel::Cap::AccountId,
    }
}

const fn completion_phase(phase: run::Phase) -> channel::CompletionPhase {
    match phase {
        run::Phase::Connect => channel::CompletionPhase::Connect,
        run::Phase::Handshake => channel::CompletionPhase::Handshake,
        run::Phase::Head => channel::CompletionPhase::Head,
        run::Phase::Idle => channel::CompletionPhase::Idle,
        run::Phase::Whole => channel::CompletionPhase::Whole,
    }
}

/// Renewal policy already carried by the existing wire form (protocol/channel.md, section 6).
pub(crate) fn renewable(value: &channel::GrantValue) -> bool {
    match value {
        channel::GrantValue::Credential(credential) => match credential.scheme() {
            channel::Scheme::Key => false,
            channel::Scheme::Bearer => true,
        },
        channel::GrantValue::Program(_) | channel::GrantValue::Withdrawn(_) => true,
    }
}

/// Refresh conversion used after the channel retains the value (protocol/channel.md, section 6).
pub(crate) fn refreshed_grant(value: &channel::Grant) -> smith_domain::Grant {
    smith_domain::Grant {
        name: smith_domain::GrantName { account: value.account(), generation: value.generation() },
        valid: value.valid(),
        renewable: renewable(value.value()),
    }
}
