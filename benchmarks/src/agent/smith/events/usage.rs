//! Conversation scopes and immutable response ids feed the shared token ledger.
//! Aggregated conversation, child-tool and run totals never charge a response.
//! Contract: benchmarks.md, section8.2; protocol/events.md, sections3.5/3.10.

use std::collections::{BTreeMap, BTreeSet};

use smith_events::{ConversationKind, Event, Evidence, Outcome, ResponseCompleted, ResponseStarted};

use crate::{Convention, Counts, Measure, ScopeKind, TokenLedger, TokenRecord, Usage};

use super::Session;

pub(super) fn incomplete(session: &Session) -> Option<String> {
    match &session.ended {
        Some(ended) if ended.loss.events == 0 => None,
        Some(ended) => Some(format!("session reports {} lost event records", ended.loss.events)),
        None => Some("session.ended and its event loss are unavailable".into()),
    }
}

fn loss(metric: &mut Measure<u64>, reason: &str) {
    match metric {
        Measure::Observed { value } => *metric = Measure::LowerBound { value: *value, missing: vec![reason.into()] },
        Measure::LowerBound { missing, .. } => missing.push(reason.into()),
        Measure::Unavailable { .. } => {}
    }
}

fn run_tokens(
    session: &Session,
    index: usize,
    run: u64,
    priced: &mut Vec<crate::ResponseTokens>,
) -> Result<Vec<TokenRecord>, String> {
    let prefix = format!("session-{index}/run-{run}");
    let mut ledger = TokenLedger::default();
    let mut scopes = BTreeMap::new();
    let mut models = BTreeMap::new();
    for record in &session.records {
        if let Event::ConversationOpened(conversation) = &record.event
            && conversation.run == run
        {
            let role = match &conversation.kind {
                ConversationKind::Main => ScopeKind::Root,
                ConversationKind::Child => match conversation.parent {
                    Some(parent) => ScopeKind::Child { parent: format!("{prefix}/conversation-{parent}") },
                    None => ScopeKind::UnlinkedChild,
                },
                ConversationKind::Compaction => match conversation.parent {
                    Some(parent) => ScopeKind::Compaction { parent: format!("{prefix}/conversation-{parent}") },
                    None => return Err("compaction scope has no native parent".into()),
                },
                ConversationKind::Unknown(value) => {
                    return Err(format!("unknown conversation kind {}", String::from_utf8_lossy(value)));
                }
            };
            models.insert(
                conversation.conversation,
                format!(
                    "{}/{}",
                    String::from_utf8_lossy(&conversation.model.endpoint),
                    String::from_utf8_lossy(&conversation.model.model)
                ),
            );
            match scopes.insert(conversation.conversation, role.clone()) {
                Some(previous) if previous != role => return Err("conversation scope changed its role".into()),
                Some(_) | None => {}
            }
        }
    }
    if scopes.is_empty() {
        return Ok(Vec::new());
    }
    for (conversation, role) in &scopes {
        ledger.declare(
            &format!("{prefix}/conversation-{conversation}"),
            role.clone(),
            Convention::SmithEvents,
            "conversation has no retained per-response usage",
        )?;
    }
    let mut responses = BTreeMap::new();
    let mut requests = BTreeMap::new();
    for (position, record) in session.records.iter().enumerate() {
        if let Event::ResponseStarted(request) = &record.event {
            requests.insert((request.run, request.conversation, request.response), request);
        }
        if let Event::ResponseCompleted(response) = &record.event
            && response.run == run
        {
            match responses.insert(response.response, response.conversation) {
                Some(previous) if previous != response.conversation => {
                    return Err("response changed its conversation scope".into());
                }
                Some(_) | None => {}
            }
            let reason = requests
                .get(&(response.run, response.conversation, response.response))
                .filter(|request| retried_failure(session, position, response, request))
                .map(|_| "retried failure");
            ledger.record_with_reason(
                &format!("{prefix}/conversation-{}", response.conversation),
                &format!("{prefix}/response-{}", response.response),
                models.get(&response.conversation).map_or("", String::as_str),
                response_usage(response),
                reason,
            )?;
        }
    }
    priced.extend_from_slice(ledger.responses());
    let mut records = ledger.records()?;
    if let Some(reason) = incomplete(session) {
        for record in &mut records {
            for metric in [
                &mut record.usage.fresh,
                &mut record.usage.cache_read,
                &mut record.usage.cache_write,
                &mut record.usage.output,
                &mut record.usage.reasoning,
            ] {
                loss(metric, &reason);
            }
        }
    }
    Ok(records)
}

fn response_usage(response: &ResponseCompleted) -> Usage {
    let unsent = response.failure.as_ref().is_some_and(|failure| failure.evidence == Evidence::Unsent);
    if unsent {
        Usage::SmithEvents {
            input: Some(0),
            cache_read: Some(0),
            cache_write: Some(0),
            output: Some(0),
            reasoning: Some(0),
        }
    } else {
        Usage::SmithEvents {
            input: response.usage.input_tokens,
            cache_read: response.usage.cache_read_tokens,
            cache_write: response.usage.cache_write_tokens,
            output: response.usage.output_tokens,
            reasoning: response.usage.reasoning_tokens,
        }
    }
}

fn retried_failure(
    session: &Session,
    position: usize,
    response: &ResponseCompleted,
    request: &ResponseStarted,
) -> bool {
    response.outcome == Outcome::Failed
        && response.blocks == 0
        && response.failure.as_ref().is_some_and(|failure| failure.evidence == Evidence::Response)
        && [
            response.usage.input_tokens,
            response.usage.cache_read_tokens,
            response.usage.cache_write_tokens,
            response.usage.output_tokens,
            response.usage.reasoning_tokens,
        ]
        .iter()
        .all(Option::is_none)
        && session.records[position + 1..].iter().any(|record| {
            matches!(&record.event,
            Event::ResponseStarted(retry) if retry.run == response.run && retry.conversation == response.conversation
                && retry.turn == request.turn && retry.attempt > request.attempt)
        })
}

pub(super) fn tokens(
    sessions: &[Session],
    priced: &mut Vec<crate::ResponseTokens>,
) -> Result<Vec<TokenRecord>, String> {
    let mut result = Vec::new();
    let mut total: Option<crate::TokenCounts> = None;
    for (index, session) in sessions.iter().enumerate() {
        let runs: BTreeSet<u64> = session
            .records
            .iter()
            .filter_map(|record| match &record.event {
                Event::ConversationOpened(conversation) => Some(conversation.run),
                Event::MessageReceived(_)
                | Event::MessageRead(_)
                | Event::MessageUnread(_)
                | Event::Budget(_)
                | Event::SessionStarted(_)
                | Event::SessionEnded(_)
                | Event::RunStarted(_)
                | Event::RunCompleted(_)
                | Event::ConversationClosed(_)
                | Event::ResponseStarted(_)
                | Event::ResponseCompleted(_)
                | Event::TextDelta(_)
                | Event::ToolStarted(_)
                | Event::JobEnded(_)
                | Event::ToolCompleted(_)
                | Event::CheckStarted(_)
                | Event::CheckCompleted(_)
                | Event::Notice(_) => None,
            })
            .collect();
        for run in runs {
            for record in run_tokens(session, index, run, priced)? {
                if record.role == ScopeKind::Total {
                    total = Some(match total {
                        Some(previous) => previous.add(&record.usage)?,
                        None => record.usage,
                    });
                } else {
                    result.push(record);
                }
            }
        }
    }
    if let Some(usage) = total {
        result.push(TokenRecord {
            scope: "total".into(),
            role: ScopeKind::Total,
            convention: Convention::SmithEvents,
            usage,
        });
    }
    Ok(result)
}

fn measured(value: u64, missing: &[String]) -> Measure<u64> {
    if missing.is_empty() {
        Measure::Observed { value }
    } else {
        Measure::LowerBound { value, missing: missing.to_vec() }
    }
}

pub(super) fn counts(sessions: &[Session]) -> Counts {
    let mut missing: Vec<String> = sessions.iter().filter_map(incomplete).collect();
    if sessions.is_empty() {
        missing.push("no event session was observed".into());
    }
    let mut attempts = BTreeSet::new();
    let mut responses = BTreeSet::new();
    let mut completions = BTreeSet::new();
    let mut retries = BTreeSet::new();
    let mut conversations = BTreeSet::new();
    let mut children = BTreeSet::new();
    let mut compactions = BTreeSet::new();
    let mut calls = BTreeMap::<String, BTreeSet<_>>::new();
    let mut failures = BTreeMap::<String, BTreeSet<_>>::new();
    for (index, session) in sessions.iter().enumerate() {
        for record in &session.records {
            match &record.event {
                Event::ResponseStarted(value) => {
                    attempts.insert((index, value.run, value.response));
                    completions.insert((index, value.run, value.conversation, value.turn));
                    if value.attempt > 0 {
                        retries.insert((index, value.run, value.response));
                    }
                }
                Event::ResponseCompleted(value) => {
                    responses.insert((index, value.run, value.response));
                }
                Event::ConversationOpened(value) => {
                    let key = (index, value.run, value.conversation);
                    conversations.insert(key);
                    match &value.kind {
                        ConversationKind::Main | ConversationKind::Unknown(_) => {}
                        ConversationKind::Child => {
                            children.insert(key);
                        }
                        ConversationKind::Compaction => {
                            compactions.insert(key);
                        }
                    }
                }
                Event::ToolStarted(value) => {
                    calls.entry(String::from_utf8_lossy(&value.tool).into_owned()).or_default().insert((
                        index,
                        value.run,
                        value.conversation,
                        value.call.clone(),
                    ));
                }
                Event::ToolCompleted(value) => {
                    if tool_failed(&value.verdict) {
                        failures.entry(String::from_utf8_lossy(&value.tool).into_owned()).or_default().insert((
                            index,
                            value.run,
                            value.conversation,
                            value.call.clone(),
                        ));
                    }
                }
                Event::MessageReceived(_)
                | Event::MessageRead(_)
                | Event::MessageUnread(_)
                | Event::Budget(_)
                | Event::SessionStarted(_)
                | Event::SessionEnded(_)
                | Event::RunStarted(_)
                | Event::RunCompleted(_)
                | Event::ConversationClosed(_)
                | Event::TextDelta(_)
                | Event::CheckStarted(_)
                | Event::CheckCompleted(_)
                | Event::JobEnded(_)
                | Event::Notice(_) => {}
            }
        }
    }
    let count = |length: usize| measured(u64::try_from(length).expect("record count fits u64"), &missing);
    Counts {
        responses: count(responses.len()),
        provider_attempts: count(attempts.len()),
        retries: count(retries.len()),
        completions: count(completions.len()),
        conversations: count(conversations.len()),
        children: count(children.len()),
        compactions: count(compactions.len()),
        tool_calls: calls.into_iter().map(|(tool, ids)| (tool, count(ids.len()))).collect(),
        tool_failures: failures.into_iter().map(|(tool, ids)| (tool, count(ids.len()))).collect(),
        message_terminals: super::messages::counts(sessions),
    }
}

fn tool_failed(verdict: &smith_events::Verdict) -> bool {
    use smith_events::Verdict;
    match verdict {
        Verdict::Removed
        | Verdict::Read
        | Verdict::Listed
        | Verdict::Found
        | Verdict::Written
        | Verdict::Edited
        | Verdict::Exited
        | Verdict::Result
        | Verdict::NoMatch
        | Verdict::Unchanged
        | Verdict::Running
        | Verdict::Stopped
        | Verdict::Unknown(_) => false,
        Verdict::NotEmpty
        | Verdict::Failed
        | Verdict::TimedOut
        | Verdict::Cancelled
        | Verdict::Invalid
        | Verdict::NotRun
        | Verdict::Withdrawn
        | Verdict::Conflict
        | Verdict::Missing
        | Verdict::TooLarge
        | Verdict::Error
        | Verdict::NotFound
        | Verdict::Stale
        | Verdict::Ambiguous
        | Verdict::NotGranted
        | Verdict::Outside
        | Verdict::ReadOnly
        | Verdict::TooLong
        | Verdict::NotFile
        | Verdict::Linked
        | Verdict::Protected
        | Verdict::NotDirectory
        | Verdict::NotRead
        | Verdict::Busy
        | Verdict::NulByte => true,
    }
}
