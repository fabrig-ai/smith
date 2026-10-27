//! Bounded JSON collection and spelling for protocol/programs.md, section 5.3.
//! The collector retains complete values; subtree spelling preserves numbers
//! and decoded strings without guessing spans in escaped source text.

use super::{Call, CallId, PROMPT_TOOL, PROTOCOL_VERSIONS, Refused, Tool, Up};
use alloc::boxed::Box;
use skein_json::{Document, Kind, collector, tokenizer, writer::Encoder};
use skein_lib::{Env, Intake, List, Queue, Time, Wall, Writer, stream};

const FILTER: collector::Filter = collector::Filter { root: collector::Keep::Value };

fn limits(bound: u32) -> collector::Limits {
    collector::Limits {
        tokenizer: tokenizer::Limits { depth: bound, string: bound, number: bound, chunk: bound, length: bound },
        tokens: bound.checked_add(2).expect("JSON token sentinel fits"),
        text: bound,
        skip: u64::from(bound),
    }
}

pub(crate) fn collect(bytes: &[u8]) -> Option<Document> {
    let bound = u32::try_from(bytes.len()).ok()?;
    if bound == 0 || bound > u32::MAX.checked_sub(2)? {
        return None;
    }
    collect_limited(bytes, limits(bound))
}

pub(crate) fn collect_limited(bytes: &[u8], limits: collector::Limits) -> Option<Document> {
    let bound = u32::try_from(bytes.len()).ok()?;
    let mut collector = collector::Collector::new(FILTER, &limits, &[]).ok()?;
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(1);
    let mut below = Queue::with_capacity(1);
    let mut intake = Intake::with_capacity(bound);
    intake.append(bytes).ok()?;
    collector::down(&mut collector, &env, collector::Request::Collect, &mut above, &mut below);
    for _ in 0..u64::from(bound).checked_mul(4)?.checked_add(16)? {
        match above.pop() {
            Some(collector::Event::Collected(document)) => return Some(document),
            Some(collector::Event::Failed(_) | collector::Event::Closed) => return None,
            None => {}
        }
        match below.pop() {
            Some(stream::Down::Demand { read, room: 0 }) => {
                let event = match intake.meet(read) {
                    Some(bytes) => stream::Up::Bytes(bytes),
                    None => stream::Up::End,
                };
                collector::up(&mut collector, &env, event, &mut above, &mut below);
            }
            Some(stream::Down::Demand { .. } | stream::Down::Send(_) | stream::Down::Finish) | None => return None,
        }
    }
    None
}

pub(crate) fn worst_case(bound: u32) -> Option<u64> {
    bound.checked_add(2)?;
    worst_case_limited(limits(bound))
}

pub(crate) fn worst_case_limited(limits: collector::Limits) -> Option<u64> {
    let bound = limits.tokenizer.length;
    collector::worst_case(&limits, &[], &FILTER)?
        .checked_add(Intake::worst_case(bound)?)?
        .checked_add(Queue::<collector::Event>::worst_case(1)?)?
        .checked_add(Queue::<stream::Down>::worst_case(1)?)?
        .checked_add(skein_json::writer::worst_case(&skein_json::writer::Limits {
            depth: limits.tokenizer.depth,
            length: bound,
        })?)
}

pub(crate) fn after(document: &Document, start: u32) -> Option<u32> {
    let mut depth = 0_u32;
    for index in start..document.len() {
        match document.token(index)?.kind {
            Kind::ObjectStart | Kind::ArrayStart => depth = depth.checked_add(1)?,
            Kind::ObjectEnd | Kind::ArrayEnd => depth = depth.checked_sub(1)?,
            Kind::Key | Kind::String | Kind::Number | Kind::True | Kind::False | Kind::Null | Kind::Long => {}
        }
        if depth == 0 {
            return index.checked_add(1);
        }
    }
    None
}

pub(crate) fn field(document: &Document, object: u32, name: &[u8]) -> Option<u32> {
    if document.token(object)?.kind != Kind::ObjectStart {
        return None;
    }
    let mut found = None;
    let mut next = object.checked_add(1)?;
    for _ in 0..document.len() {
        let key = document.token(next)?;
        if key.kind == Kind::ObjectEnd {
            return found;
        }
        if key.kind != Kind::Key {
            return None;
        }
        let value = next.checked_add(1)?;
        if document.text(key)? == name {
            if found.is_some() {
                return None;
            }
            found = Some(value);
        }
        next = after(document, value)?;
    }
    None
}

pub(crate) fn text(document: &Document, index: u32) -> Option<&[u8]> {
    let record = document.token(index)?;
    if record.kind == Kind::String { document.text(record) } else { None }
}

fn spell(encoder: &mut Encoder, document: &Document, start: u32, end: u32) {
    for index in start..end {
        let record = document.token(index).expect("validated subtree record");
        let text = document.text(record).expect("validated document text");
        match record.kind {
            Kind::ObjectStart => encoder.object_start(),
            Kind::ObjectEnd => encoder.object_end(),
            Kind::ArrayStart => encoder.array_start(),
            Kind::ArrayEnd => encoder.array_end(),
            Kind::Key => encoder.key(text),
            Kind::String => encoder.string(text),
            Kind::Number => encoder.number(text),
            Kind::True => encoder.boolean(true),
            Kind::False => encoder.boolean(false),
            Kind::Null => encoder.null(),
            Kind::Long => unreachable!("complete value filter retains every string"),
        }
    }
}

pub(crate) fn raw(document: &Document, index: u32, bound: u32) -> Option<Box<[u8]>> {
    let end = after(document, index)?;
    let limits = skein_json::writer::Limits { depth: document.len(), length: bound };
    let mut measure = Encoder::measure(&limits);
    spell(&mut measure, document, index, end);
    let length = measure.measured().ok()?;
    let mut write = Encoder::write(length, &limits);
    spell(&mut write, document, index, end);
    Some(write.finish())
}

pub(crate) fn quote(bytes: &[u8], bound: u32) -> Option<Box<[u8]>> {
    let limits = skein_json::writer::Limits { depth: 1, length: bound };
    let mut measure = Encoder::measure(&limits);
    measure.string(bytes);
    let length = measure.measured().ok()?;
    let mut write = Encoder::write(length, &limits);
    write.string(bytes);
    Some(write.finish())
}

pub(crate) fn join(parts: &[&[u8]]) -> Box<[u8]> {
    let mut length = 0_usize;
    for part in parts {
        length = length.checked_add(part.len()).expect("admitted JSON size");
    }
    let mut writer = Writer::new(length);
    for part in parts {
        writer.put(part).expect("measured JSON parts");
    }
    writer.finish()
}

pub(crate) fn reply(id: &[u8], result: &[u8]) -> Box<[u8]> {
    join(&[b"{\"jsonrpc\":\"2.0\",\"id\":", id, b",\"result\":", result, b"}\n"])
}

pub(crate) fn error(id: &[u8], code: &[u8], message: &[u8]) -> Box<[u8]> {
    join(&[b"{\"jsonrpc\":\"2.0\",\"id\":", id, b",\"error\":{\"code\":", code, b",\"message\":\"", message, b"\"}}\n"])
}

pub(crate) fn result(text: &[u8], error: bool, bound: u32) -> Option<Box<[u8]>> {
    let quoted = quote(text, bound)?;
    let flag: &[u8] = if error { b"true" } else { b"false" };
    Some(join(&[b"{\"content\":[{\"type\":\"text\",\"text\":", &quoted, b"}],\"isError\":", flag, b"}"]))
}

pub(crate) fn field_in(document: &Document, object: Option<u32>, name: &[u8]) -> Option<u32> {
    field(document, object?, name)
}

pub(crate) fn text_in(document: &Document, index: Option<u32>) -> Option<&[u8]> {
    text(document, index?)
}

// An absent type is narrowed to the object inputs smith already accepts.
// Explicit non-object types (including type arrays) are refused, not overwritten.
fn object_type(document: &Document) -> Option<bool> {
    if document.token(0)?.kind != Kind::ObjectStart {
        return None;
    }
    let mut typed = false;
    let mut next = 1;
    for _ in 0..document.len() {
        let key = document.token(next)?;
        if key.kind == Kind::ObjectEnd {
            return Some(typed);
        }
        let value = next.checked_add(1)?;
        if document.text(key)? == b"type" {
            if typed || text(document, value) != Some(b"object".as_slice()) {
                return None;
            }
            typed = true;
        }
        next = after(document, value)?;
    }
    None
}

fn spell_object(encoder: &mut Encoder, document: &Document) {
    encoder.object_start();
    encoder.key(b"type");
    encoder.string(b"object");
    spell(encoder, document, 1, document.len().checked_sub(1).expect("schema object end"));
    encoder.object_end();
}

fn object_schema(document: &Document, bound: u32) -> Option<Box<[u8]>> {
    let limits = skein_json::writer::Limits { depth: document.len(), length: bound };
    let mut measure = Encoder::measure(&limits);
    spell_object(&mut measure, document);
    let length = measure.measured().ok()?;
    let mut write = Encoder::write(length, &limits);
    spell_object(&mut write, document);
    Some(write.finish())
}

pub(crate) fn tool_list(tools: &[Tool], bound: u32, schema_bytes: u32) -> Result<Box<[u8]>, Refused> {
    let mut bytes = List::with_capacity(bound);
    for byte in b"{\"tools\":[" {
        match bytes.push(*byte) {
            Ok(()) => {}
            Err(_) => return Err(Refused::TooLarge),
        }
    }
    for tool in tools {
        let name = quote(&tool.name, bound).ok_or(Refused::TooLarge)?;
        let description = quote(&tool.description, bound).ok_or(Refused::TooLarge)?;
        if tool.input_schema.len() > usize::try_from(schema_bytes.min(bound)).expect("u32 schema bound") {
            return Err(Refused::TooLarge);
        }
        let schema = collect(&tool.input_schema).ok_or(Refused::Schema { tool: tool.name.clone() })?;
        let typed = object_type(&schema).ok_or(Refused::Schema { tool: tool.name.clone() })?;
        if tool.name.as_ref() == PROMPT_TOOL {
            return Err(Refused::Schema { tool: tool.name.clone() });
        }
        let normalized;
        let input_schema = if typed {
            tool.input_schema.as_ref()
        } else {
            normalized = object_schema(&schema, schema_bytes.min(bound)).ok_or(Refused::TooLarge)?;
            &normalized
        };
        let record = join(&[
            b"{\"name\":",
            &name,
            b",\"description\":",
            &description,
            b",\"inputSchema\":",
            input_schema,
            b"},",
        ]);
        for byte in &record {
            match bytes.push(*byte) {
                Ok(()) => {}
                Err(_) => return Err(Refused::TooLarge),
            }
        }
    }
    let prompt = b"{\"name\":\"permission_prompt\",\"description\":\"Ask smith before a built-in tool\",\"inputSchema\":{\"type\":\"object\",\"properties\":{\"tool_name\":{\"type\":\"string\"},\"input\":{\"type\":\"object\"},\"tool_use_id\":{\"type\":\"string\"}},\"required\":[\"tool_name\",\"input\"]}}]}";
    for byte in prompt {
        match bytes.push(*byte) {
            Ok(()) => {}
            Err(_) => return Err(Refused::TooLarge),
        }
    }
    let result = bytes.into_boxed();
    // Every legal ID (including a 64-byte one) must fit at startup.
    if reply(&[b'0'; 64], &result).len() > usize::try_from(bound).expect("u32 answer bound") {
        return Err(Refused::TooLarge);
    }
    Ok(result)
}

impl super::Server {
    pub(crate) fn message(&mut self, line: &[u8]) {
        let Some(document) = collect(line) else {
            self.output.push(error(b"null", b"-32700", b"parse error"));
            return;
        };
        if document.token(0).expect("collected value").kind != Kind::ObjectStart {
            self.output.push(error(b"null", b"-32600", b"invalid request"));
            return;
        }
        let Some(method_index) = field(&document, 0, b"method") else {
            return;
        };
        let Some(id_index) = field(&document, 0, b"id") else {
            return;
        };
        let Some(id) = raw(&document, id_index, 64) else {
            self.output.push(error(b"null", b"-32600", b"invalid request"));
            return;
        };
        if text_in(&document, field(&document, 0, b"jsonrpc")) != Some(b"2.0".as_slice()) {
            self.output.push(error(&id, b"-32600", b"invalid request"));
            return;
        }
        let method = text(&document, method_index);
        let params = field(&document, 0, b"params");
        match method {
            Some(b"initialize") => {
                let offered = text_in(&document, field_in(&document, params, b"protocolVersion"));
                let mut version = PROTOCOL_VERSIONS.first().expect("MCP versions").as_bytes();
                for known in PROTOCOL_VERSIONS {
                    if offered == Some(known.as_bytes()) {
                        version = known.as_bytes();
                    }
                }
                #[expect(clippy::disallowed_macros, reason = "MCP serverInfo reports the package version required by the protocol")]
                let result = join(&[b"{\"protocolVersion\":\"", version,
                    b"\",\"capabilities\":{\"tools\":{\"listChanged\":false}},\"serverInfo\":{\"name\":\"smith\",\"version\":\"",
                    env!("CARGO_PKG_VERSION").as_bytes(), b"\"}}"]);
                self.output.push(reply(&id, &result));
            }
            Some(b"ping") => self.output.push(reply(&id, b"{}")),
            Some(b"tools/list") => self.output.push(reply(&id, &self.tool_list)),
            Some(b"tools/call") => self.call(&document, params, id),
            Some(_) => self.output.push(error(&id, b"-32601", b"unknown method")),
            None => self.output.push(error(&id, b"-32600", b"invalid request")),
        }
    }

    #[expect(clippy::manual_map, reason = "the protocol subset uses exhaustive matches without callbacks")]
    fn call(&mut self, document: &Document, params: Option<u32>, id: Box<[u8]>) {
        let name = text_in(document, field_in(document, params, b"name"));
        let arguments = field_in(document, params, b"arguments");
        let input = match arguments {
            Some(index) => raw(document, index, self.limits.message_bytes),
            None => Some(Box::from(b"{}".as_slice())),
        };
        let event = match name {
            Some(PROMPT_TOOL) => {
                let tool_name = text_in(document, field_in(document, arguments, b"tool_name"));
                let input_index = field_in(document, arguments, b"input");
                let tool_use_id = text_in(document, field_in(document, arguments, b"tool_use_id"));
                match tool_name {
                    Some(tool_name) => match input_index {
                        Some(index) if document.token(index).expect("field token").kind == Kind::ObjectStart => {
                            let input = raw(document, index, self.limits.message_bytes).expect("bounded subtree");
                            Some(Up::Prompt {
                                call: CallId(0),
                                tool_name: Box::from(tool_name),
                                input,
                                tool_use_id: match tool_use_id {
                                    Some(id) => Some(Box::from(id)),
                                    None => None,
                                },
                            })
                        }
                        Some(_) | None => None,
                    },
                    None => None,
                }
            }
            Some(name) => {
                let mut known = false;
                for tool in &self.tools {
                    if tool.name.as_ref() == name {
                        known = true;
                    }
                }
                match input {
                    Some(input) if known => Some(Up::Call { call: CallId(0), tool: Box::from(name), input }),
                    Some(_) | None => None,
                }
            }
            None => None,
        };
        match event {
            Some(mut event) => {
                let Some(next) = self.sequence.checked_add(1) else {
                    self.output.push(error(&id, b"-32603", b"call identifiers exhausted"));
                    return;
                };
                self.sequence = next;
                let call = CallId(next);
                match &mut event {
                    Up::Call { call: target, .. } | Up::Prompt { call: target, .. } => *target = call,
                    Up::Listening { .. } | Up::Connected | Up::Ended(_) => unreachable!("call event"),
                }
                for index in 0..self.calls.len() {
                    if self.calls.get(index).expect("call slot").is_none() {
                        *self.calls.get_mut(index).expect("free call slot") = Some(Call { call, id });
                        self.above.push(event);
                        return;
                    }
                }
                unreachable!("reading pauses at the call bound");
            }
            None => self.output.push(error(&id, b"-32602", b"invalid tool parameters")),
        }
    }
}
