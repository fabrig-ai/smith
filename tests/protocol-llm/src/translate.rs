//! Typed fixture wiring between the copied scripts and the agent conversation
//! (domain/session.md, section 12; domain/run.md, section 14). This test-only
//! adapter recognizes the scripts' finite arguments. It owns no production
//! provider schema adapter; migration 05s5 supplies those. Opaque host inputs
//! use the skein JSON tokenizer to attest complete object syntax. Tool results keep
//! call IDs and diagnostic bytes, allowing the provider to check transcript
//! pairing and the tests to inspect feedback on subsequent requests.

use crate::adapter;
use skein_fake_llm_domain::api as provider;
use smith_domain::{llm as agent, run, tools};

pub(crate) fn query(prompt: agent::Prompt, stated: &adapter::Stated) -> provider::Query {
    let mut tools = Vec::new();
    for specification in adapter::schemas(&prompt, stated) {
        tools.push(provider::ToolSpec {
            name: specification.name,
            description: specification.description,
            parameters: specification.schema,
        });
    }
    for served in prompt.served {
        let specification = match served {
            agent::Served::Host(tool) => {
                provider::ToolSpec { name: tool.name, description: tool.description, parameters: tool.schema }
            }
            agent::Served::Deliver | agent::Served::Finish | agent::Served::SubAgent | agent::Served::Wait => {
                continue;
            }
        };
        tools.push(specification);
    }
    let messages = prompt
        .messages
        .into_iter()
        .map(|message| provider::Message {
            role: match message.role {
                agent::Role::User => provider::Role::User,
                agent::Role::Assistant => provider::Role::Assistant,
            },
            parts: message.content.into_iter().map(part).collect(),
        })
        .collect();
    provider::Query {
        credential: None,
        caching: provider::Caching::Scope(prompt.affinity.key),
        model: prompt.model,
        system: prompt.system,
        tools: tools.into(),
        choice: provider::ToolChoice::Auto,
        messages,
        max_tokens: prompt.max_tokens,
    }
}

fn part(block: agent::Block) -> provider::Part {
    match block {
        agent::Block::Text { text, .. } | agent::Block::Refusal { text, .. } => provider::Part::Text { text },
        agent::Block::Opaque { bytes } => provider::Part::Opaque { bytes },
        agent::Block::ToolCall { id, name, input, .. } => provider::Part::ToolCall { id, name, arguments: input },
        agent::Block::ToolResult { id, result } => {
            let error = match &result {
                agent::Returned::Text { error, .. } | agent::Returned::Served { error, .. } => *error,
                agent::Returned::Withdrawn | agent::Returned::Invalid { .. } | agent::Returned::NotRun { .. } => true,
                agent::Returned::Owned { outcome } => !matches!(
                    outcome,
                    tools::Outcome::Read { .. }
                        | tools::Outcome::Listed { .. }
                        | tools::Outcome::Found { .. }
                        | tools::Outcome::Removed
                        | tools::Outcome::Written { .. }
                        | tools::Outcome::Edited { .. }
                        | tools::Outcome::Exited { exit: tools::Exit::Code { code: 0 }, .. }
                ),
            };
            let output = match &result {
                agent::Returned::Text { text, .. }
                | agent::Returned::Served { returned: run::Returned::Answered { text, .. }, .. } => text.clone(),
                agent::Returned::Withdrawn => b"withdrawn".as_slice().into(),
                agent::Returned::Served { returned: run::Returned::DeliveryFailed { failure }, .. } => {
                    failure.diagnostic.output().into()
                }
                agent::Returned::Served { returned: run::Returned::HostAnswered(answer), .. } => answer.text().into(),
                agent::Returned::Owned { .. }
                | agent::Returned::Served { .. }
                | agent::Returned::Invalid { .. }
                | agent::Returned::NotRun { .. } => format!("{result:?}").into_bytes().into(),
            };
            provider::Part::ToolOutput { id, output, is_error: error }
        }
    }
}

pub(crate) fn completion(
    answer: provider::Answer,
    grants: tools::Grants,
    served: &[agent::Served],
    asker: agent::Asker,
) -> agent::Completion {
    let stop = match answer.finish {
        provider::Finish::Stop => agent::Stop::EndTurn,
        provider::Finish::ToolCalls => agent::Stop::ToolUse,
        provider::Finish::Length => agent::Stop::MaxTokens,
        provider::Finish::ContentFilter => agent::Stop::Refusal,
    };
    let usage = agent::Usage {
        input_tokens: answer.usage.input,
        output_tokens: answer.usage.output,
        cache_read_tokens: answer.usage.cache_read,
        cache_write_tokens: answer.usage.cache_write,
        reasoning_tokens: answer.usage.reasoning,
    };
    let content = answer
        .parts
        .into_iter()
        .map(|part| match part {
            provider::Part::Text { text } => agent::Said::Text { text, replay: None },
            provider::Part::Opaque { bytes } => agent::Said::Opaque { bytes },
            provider::Part::ToolCall { id, name, arguments } => {
                let call = decode(&name, &arguments, grants, served, asker);
                agent::Said::ToolCall { id, name, input: arguments, call, replay: None }
            }
            provider::Part::ToolOutput { .. } => panic!("a fake completion contains no tool result"),
        })
        .collect();
    agent::Completion { reasoning_dropped: Box::default(), content, stop, usage }
}

pub(crate) fn decode(
    name: &[u8],
    arguments: &[u8],
    grants: tools::Grants,
    served: &[agent::Served],
    asker: agent::Asker,
) -> agent::Decoded {
    let invalid = || agent::Decoded::Invalid { problem: agent::Problem::UnknownTool };
    if name == b"sub_agent" && (asker == agent::Asker::Child || arguments.windows(8).any(|part| part == b"\"agents\""))
    {
        return agent::Decoded::Invalid { problem: agent::Problem::Nesting };
    }
    for tool in served {
        match tool {
            agent::Served::Host(tool) if tool.name.as_ref() == name => {
                return object(arguments).and_then(|()| run::HostInput::attested(arguments.into())).map_or_else(
                    invalid,
                    |input| agent::Decoded::Served {
                        ask: run::Ask::Host { tool: name.into(), effect: tool.effect, input },
                    },
                );
            }
            agent::Served::Host(_)
            | agent::Served::Deliver
            | agent::Served::Finish
            | agent::Served::SubAgent
            | agent::Served::Wait => {}
        }
    }
    if name == b"deliver" && served.contains(&agent::Served::Deliver) {
        let Some(value) = field(arguments, b"ticket") else {
            return invalid();
        };
        return agent::Decoded::Served {
            ask: run::Ask::Deliver {
                change: run::outcome::Change {
                    fields: Box::new([run::outcome::Field { name: b"ticket".as_slice().into(), value }]),
                },
            },
        };
    }
    if name == b"finish" && served.contains(&agent::Served::Finish) {
        return finish(arguments)
            .map_or_else(invalid, |outcome| agent::Decoded::Served { ask: run::Ask::Finish { outcome } });
    }
    if name == b"wait" && served.contains(&agent::Served::Wait) {
        let no_arguments: Vec<u8> = arguments.iter().copied().filter(|byte| !byte.is_ascii_whitespace()).collect();
        return if no_arguments == b"{}" { agent::Decoded::Served { ask: run::Ask::Wait } } else { invalid() };
    }
    if name == b"sub_agent" && served.contains(&agent::Served::SubAgent) {
        let Some(brief) = field(arguments, b"brief") else { return invalid() };
        let has = |word: &[u8]| arguments.windows(word.len()).any(|part| part == word);
        let families = run::charter::Families {
            tools: run::charter::Tools {
                inspect: has(b"\"inspect\""),
                modify: has(b"\"modify\""),
                shell: has(b"\"shell\""),
            },

            agents: false,
        };
        return agent::Decoded::Served {
            ask: run::Ask::SubAgent { brief, families, llm: field(arguments, b"llm"), share: run::Share::UNASKED },
        };
    }
    let limits = adapter::Limits {
        client: skein_llm_world::limits(),
        tool_bytes: 32768,
        history_bytes: u64::from(skein_llm_world::limits().request),
        rendered_result: skein_llm_world::limits().strings,
        stated: crate::adapter::stated(),
    };
    adapter::decode(name, arguments, grants, served, asker, &limits)
}

fn finish(arguments: &[u8]) -> Option<run::outcome::Declared> {
    use run::outcome::{Change, Declared, DeclaredFailure, Field, Item, Report, Verdict};
    if !arguments.starts_with(b"{") || !arguments.ends_with(b"}") {
        return None;
    }
    if let Some(text) = field(arguments, b"report") {
        let fields: Box<[Field]> = match field(arguments, b"source") {
            Some(value) => Box::new([Field { name: b"source".as_slice().into(), value }]),
            None => Box::new([]),
        };
        return Some(Declared::Report(Report { text, fields }));
    }
    if let Some(reason) = field(arguments, b"failure") {
        let fields: Box<[Field]> = match field(arguments, b"cause") {
            Some(value) => Box::new([Field { name: b"cause".as_slice().into(), value }]),
            None => Box::new([]),
        };
        return Some(Declared::Failure(DeclaredFailure { reason, fields }));
    }
    let body = field(arguments, b"body")?;
    if let Some(title) = field(arguments, b"title") {
        return Some(Declared::Change(Change {
            fields: Box::new([
                smith_domain::run::outcome::Field { name: b"title".as_slice().into(), value: title },
                smith_domain::run::outcome::Field { name: b"body".as_slice().into(), value: body },
            ]),
        }));
    }
    let name = field(arguments, b"verdict")?;
    let children: Box<[Item]> = if arguments.windows(10).any(|part| part == b"\"children\"") {
        let child = Item {
            kind: field(arguments, b"kind")?,
            fields: Box::new([
                Field { name: b"path".as_slice().into(), value: field(arguments, b"path")? },
                Field { name: b"body".as_slice().into(), value: b"43".as_slice().into() },
            ]),
        };
        Box::new([child])
    } else {
        Box::new([])
    };
    Some(Declared::Verdict(Verdict { name, text: body, items: children, fields: Box::new([]) }))
}

/// The copied scripts use unescaped string fields. This recognizes that finite
/// fixture language, never arbitrary JSON or production provider documents.
///
/// Scripted-world contract: domain/run.md, sections 13 and 14; testing-strategy.md, section 7.
fn field(arguments: &[u8], name: &[u8]) -> Option<Box<[u8]>> {
    let prefix = [b"\"", name, b"\":\""].concat();
    let start = arguments.windows(prefix.len()).position(|part| part == prefix)? + prefix.len();
    let rest = &arguments[start..];
    let end = rest.iter().position(|byte| *byte == b'"')?;
    Some(rest[..end].into())
}

pub(crate) fn failure(error: provider::Error) -> agent::Failure {
    match error {
        provider::Error::Overloaded => agent::Failure::Overloaded,
        provider::Error::RateLimited { retry_after } => agent::Failure::RateLimited { retry_after },
        provider::Error::Unavailable => agent::Failure::Unavailable,
        provider::Error::ContextTooLong => agent::Failure::ContextTooLong,
        provider::Error::Unauthorized => agent::Failure::Unauthorized,
        provider::Error::Exhausted { retry_after } => agent::Failure::Exhausted { retry_after },
        provider::Error::Refused => agent::Failure::Refused,
        provider::Error::Unsupported => agent::Failure::Unsupported,
        provider::Error::InvalidRequest => agent::Failure::Invalid,
    }
}

/// This protocol face uses the JSON tokenizer for syntax and object shape;
/// the domain receives exactly the original bytes, without field extraction.
fn object(document: &[u8]) -> Option<()> {
    use skein_json::{Token, tokenizer as json};
    use skein_lib::{
        Env, Intake, Queue, Time, Wall,
        stream::{Down, Up},
    };
    let length = u32::try_from(document.len()).ok()?;
    if document.len() > run::HostInput::CAPACITY {
        return None;
    }
    let limits = json::Limits { depth: 32, string: length, number: length, chunk: 1024, length };
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut tokenizer = json::Tokenizer::new(&limits);
    let mut above = Queue::with_capacity(json::UP_MAX_OUT.above.max(json::DOWN_MAX_OUT.above));
    let mut below = Queue::with_capacity(json::UP_MAX_OUT.below.max(json::DOWN_MAX_OUT.below));
    let mut intake = Intake::with_capacity(length.max(json::largest_demand(&limits)));
    intake.append(document).ok()?;
    let mut demand = None;
    let mut first = true;
    for _ in 0..document.len().checked_mul(4)?.checked_add(8)? {
        match demand.take() {
            None => json::down(&mut tokenizer, &env, json::Request::Next, &mut above, &mut below),
            Some(read) => {
                let event = intake.meet(read).map_or(Up::End, Up::Bytes);
                json::up(&mut tokenizer, &env, event, &mut above, &mut below);
            }
        }
        match below.pop() {
            Some(Down::Demand { read, room: 0 }) => demand = Some(read),
            None => {}
            Some(Down::Demand { .. } | Down::Send(_) | Down::Finish) => {
                panic!("JSON tokenizer only demands bounded reads")
            }
        }
        match above.pop() {
            Some(json::Event::Token(token)) => {
                if first && token != Token::ObjectStart {
                    return None;
                }
                first = false;
            }
            Some(json::Event::Done) => return (!first).then_some(()),
            Some(json::Event::Failed(_)) => return None,
            Some(json::Event::Long(_) | json::Event::Skipped(_)) => {
                unreachable!("fixture decoder requests only Next tokens")
            }
            Some(json::Event::Closed) => panic!("parser is not closed by this translator"),
            None => {}
        }
        assert!(above.is_empty() && below.is_empty());
    }
    panic!("bounded JSON completes within linear steps");
}

#[cfg(test)]
mod host_tests {
    use super::*;
    #[test]
    fn only_a_complete_json_object_is_attested_and_bytes_are_opaque() {
        let tool = agent::Served::Host(run::HostTool {
            name: b"opaque".as_slice().into(),
            description: b"Opaque operation".as_slice().into(),
            schema: b"{}".as_slice().into(),
            effect: run::HostEffect::Write,
            timeout: skein_lib::Duration::from_secs(1),
        });
        let granted = tools::Grants { inspect: false, modify: false, shell: false };
        let raw = br#" {"nested":[1,true,{"unchanged":"\u0041"}],"policy":"unknown"} "#;
        let decoded = decode(b"opaque", raw, granted, std::slice::from_ref(&tool), agent::Asker::Main);
        let agent::Decoded::Served { ask: run::Ask::Host { input, effect, .. } } = decoded else {
            panic!("valid opaque object is relayed");
        };
        assert_eq!(input.bytes(), raw);
        assert_eq!(effect, run::HostEffect::Write);
        for malformed in [b"{bad}".as_slice(), b"{}{}", b"[]", b"{\"key\":}", b"{\"key\":NaN}", b"{\"key\":\"\xff\"}"] {
            assert!(matches!(
                decode(b"opaque", malformed, granted, std::slice::from_ref(&tool), agent::Asker::Main),
                agent::Decoded::Invalid { .. }
            ));
        }
    }
}
