//! Bounded decoding of provider-written tool inputs into the domain vocabulary.
//! No call starts here. The original bytes stay in the completion for replay;
//! invalid inputs become typed problems for the domain to answer.
//! Contract: protocol/llm.md, section 4; domain/tools.md, sections 3 and 4.

use alloc::boxed::Box;

use skein_json::{Compact, Document, Kind as JsonKind};
use skein_lib::{Duration, List};
use skein_llm::{DocumentError, Json};
use smith_domain::{llm, run, tools};

use crate::Limits;

/// Decode one offered Smith tool. Host tools are checked and relayed by the
/// completion adapter; finish and deliver use their contract decoder.
#[must_use]
pub fn decode(
    name: &[u8],
    input: &[u8],
    grants: tools::Grants,
    served: &[llm::Served],
    asker: llm::Asker,
    limits: &Limits,
) -> llm::Decoded {
    if name == b"sub_agent" {
        match asker {
            llm::Asker::Child => return invalid(llm::Problem::Nesting),
            llm::Asker::Main => {}
        }
    }
    let kind = match name {
        b"read" if grants.inspect => Kind::Read,
        b"list" if grants.inspect => Kind::List,
        b"search" if grants.inspect => Kind::Search,
        b"write" if grants.modify => Kind::Write,
        b"edit" if grants.modify => Kind::Edit,
        b"remove" if grants.modify => Kind::Remove,
        b"shell" if grants.shell => Kind::Shell,
        b"job" if grants.shell => Kind::Job,
        b"wait" if offered(served, Kind::Wait) => Kind::Wait,
        b"sub_agent" if offered(served, Kind::SubAgent) => Kind::SubAgent,
        _ => return llm::Decoded::Invalid { problem: llm::Problem::UnknownTool },
    };
    let document = match Json::from_bytes(
        input,
        &crate::limits::document_limits(
            &limits.client,
            u32::try_from(input.len()).unwrap_or(u32::MAX).min(limits.client.input),
        ),
    ) {
        Ok(document) => document,
        Err(DocumentError::TooLarge { which: _, bound: _ }) => return invalid(llm::Problem::TooLarge),
        Err(DocumentError::Malformed | DocumentError::Missing | DocumentError::WrongType) => {
            return invalid(llm::Problem::NotAnObject);
        }
    };
    let tokens = document.document();
    if match tokens.token(0) {
        Some(record) => record.kind != JsonKind::ObjectStart,
        None => true,
    } {
        return invalid(llm::Problem::NotAnObject);
    }
    match kind {
        Kind::SubAgent if field(tokens, b"agents").is_some() => return invalid(llm::Problem::Nesting),
        Kind::SubAgent
        | Kind::Read
        | Kind::List
        | Kind::Search
        | Kind::Write
        | Kind::Remove
        | Kind::Edit
        | Kind::Shell
        | Kind::Job
        | Kind::Wait => {}
    }
    match call(kind, tokens) {
        Ok(value) => value,
        Err(problem) => invalid(problem),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Read,
    List,
    Search,
    Write,
    Edit,
    Remove,
    Shell,
    Job,
    Wait,
    SubAgent,
}

fn offered(served: &[llm::Served], kind: Kind) -> bool {
    for declaration in served {
        match declaration {
            llm::Served::Wait if kind == Kind::Wait => return true,
            llm::Served::SubAgent if kind == Kind::SubAgent => return true,
            llm::Served::Wait
            | llm::Served::SubAgent
            | llm::Served::Host(_)
            | llm::Served::Deliver
            | llm::Served::Finish => {}
        }
    }
    false
}

fn call(kind: Kind, tokens: &Document) -> Result<llm::Decoded, llm::Problem> {
    let result = match kind {
        Kind::Read => llm::Decoded::Owned {
            call: tools::Call::Read {
                path: path(&required_string(tokens, b"path")?)?,
                offset: match optional_u32(tokens, b"offset")? {
                    Some(0) => return Err(bad_value(b"offset")),
                    Some(offset) => offset,
                    None => 1,
                },
                window: match optional_u32(tokens, b"window")? {
                    Some(0) => return Err(bad_value(b"window")),
                    Some(bytes) => tools::Budget::Asked(bytes),
                    None => tools::Budget::Default,
                },
            },
        },
        Kind::List => {
            let depth = match optional_u32(tokens, b"depth")? {
                Some(0) => return Err(bad_value(b"depth")),
                Some(levels) => tools::Depth::Levels(levels),
                None => tools::Depth::Whole,
            };
            llm::Decoded::Owned {
                call: tools::Call::List {
                    path: path(&required_string(tokens, b"path")?)?,
                    depth,
                    glob: optional_string(tokens, b"glob")?,
                },
            }
        }
        Kind::Search => {
            let spelling = match optional_string(tokens, b"path")? {
                Some(path) => path,
                None => b".".as_slice().into(),
            };
            llm::Decoded::Owned {
                call: tools::Call::Search {
                    path: path(&spelling)?,
                    pattern: required_string(tokens, b"pattern")?,
                    glob: optional_string(tokens, b"glob")?,
                },
            }
        }
        Kind::Write => llm::Decoded::Owned {
            call: tools::Call::Write {
                path: path(&required_string(tokens, b"path")?)?,
                content: required_string(tokens, b"content")?,
            },
        },
        Kind::Remove => {
            llm::Decoded::Owned { call: tools::Call::Remove { path: path(&required_string(tokens, b"path")?)? } }
        }
        Kind::Edit => llm::Decoded::Owned {
            call: tools::Call::Edit { path: path(&required_string(tokens, b"path")?)?, edits: replacements(tokens)? },
        },
        Kind::Shell => llm::Decoded::Owned { call: shell(tokens)? },
        Kind::Job => llm::Decoded::Owned { call: job(tokens)? },
        Kind::Wait => llm::Decoded::Served { ask: run::Ask::Wait },
        Kind::SubAgent => llm::Decoded::Served {
            ask: run::Ask::SubAgent {
                brief: required_string(tokens, b"brief")?,
                families: families(tokens)?,
                llm: optional_string(tokens, b"llm")?,
                share: run::Share {
                    turns: optional_u32(tokens, b"max_turns")?,
                    time: optional_seconds(tokens, b"max_seconds")?,
                },
            },
        },
    };
    Ok(result)
}

fn shell(tokens: &Document) -> Result<tools::Call, llm::Problem> {
    let timeout = match optional_seconds(tokens, b"timeout_seconds")? {
        Some(duration) => tools::Timeout::Asked(duration),
        None => tools::Timeout::Default,
    };
    let after = match job_seconds(tokens, b"yield_seconds")? {
        Some(duration) => tools::Yield::Asked(duration),
        None => tools::Yield::Never,
    };
    Ok(tools::Call::Shell {
        after,
        command: required_string(tokens, b"command")?,
        timeout,
        output: output_budget(tokens)?,
    })
}

fn job(tokens: &Document) -> Result<tools::Call, llm::Problem> {
    let number = optional_u32(tokens, b"job")?.ok_or(missing(b"job"))?;
    let wait = match job_seconds(tokens, b"wait_seconds")? {
        Some(duration) => tools::Wait::Asked(duration),
        None => tools::Wait::Default,
    };
    let stop = match field(tokens, b"stop") {
        Some(record) => match record.kind {
            JsonKind::True => true,
            JsonKind::False => false,
            JsonKind::ObjectStart
            | JsonKind::ObjectEnd
            | JsonKind::ArrayStart
            | JsonKind::ArrayEnd
            | JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::Null
            | JsonKind::Long => {
                return Err(wrong_type(b"stop"));
            }
        },
        None => false,
    };
    Ok(tools::Call::Job { job: tools::JobNumber(number), wait, output: output_budget(tokens)?, stop })
}

fn output_budget(tokens: &Document) -> Result<tools::Budget, llm::Problem> {
    match optional_u32(tokens, b"output_bytes")? {
        Some(bytes) => Ok(tools::Budget::Asked(bytes)),
        None => Ok(tools::Budget::Default),
    }
}

fn job_seconds(tokens: &Document, name: &[u8]) -> Result<Option<Duration>, llm::Problem> {
    match field(tokens, name) {
        Some(record) if record.kind == JsonKind::Number => {
            let seconds = decimal(tokens.text(&record).ok_or(llm::Problem::NotAnObject)?).ok_or(bad_value(name))?;
            Ok(Some(Duration::from_secs(u64::from(seconds))))
        }
        Some(_) => Err(wrong_type(name)),
        None => Ok(None),
    }
}

fn invalid(problem: llm::Problem) -> llm::Decoded {
    llm::Decoded::Invalid { problem }
}

fn missing(name: &[u8]) -> llm::Problem {
    llm::Problem::Missing { field: name.into() }
}

fn wrong_type(name: &[u8]) -> llm::Problem {
    llm::Problem::WrongType { field: name.into() }
}

fn bad_value(name: &[u8]) -> llm::Problem {
    llm::Problem::BadValue { field: name.into() }
}

// Skein admits the grammar. Inspect only direct application members through
// borrowed compact records; no token tree or string copy is reconstructed.
fn field(document: &Document, name: &[u8]) -> Option<Compact> {
    let mut depth = 0_u32;
    for index in 0..document.len() {
        let record = document.token(index)?;
        match record.kind {
            JsonKind::ObjectStart | JsonKind::ArrayStart => depth = depth.checked_add(1)?,
            JsonKind::ObjectEnd | JsonKind::ArrayEnd => depth = depth.checked_sub(1)?,
            JsonKind::Key if depth == 1 => {
                if document.text(record)? == name {
                    return Some(*document.token(index.checked_add(1)?)?);
                }
            }
            JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::True
            | JsonKind::False
            | JsonKind::Null
            | JsonKind::Long => {}
        }
    }
    None
}

/// A direct member's value index in one admitted compact object.
fn member(document: &Document, start: u32, name: &[u8]) -> Option<u32> {
    let mut depth = 0_u32;
    for index in start..document.len() {
        let record = document.token(index)?;
        match record.kind {
            JsonKind::ObjectStart | JsonKind::ArrayStart => depth = depth.checked_add(1)?,
            JsonKind::ObjectEnd | JsonKind::ArrayEnd => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return None;
                }
            }
            JsonKind::Key if depth == 1 && document.text(record)? == name => return index.checked_add(1),
            JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::True
            | JsonKind::False
            | JsonKind::Null
            | JsonKind::Long => {}
        }
    }
    None
}

fn replacement_string(document: &Document, start: u32, name: &[u8]) -> Result<Box<[u8]>, llm::Problem> {
    let index = member(document, start, name).ok_or(missing(name))?;
    let record = document.token(index).ok_or(llm::Problem::NotAnObject)?;
    match record.kind {
        JsonKind::String => Ok(document.text(record).ok_or(llm::Problem::NotAnObject)?.into()),
        JsonKind::ObjectStart
        | JsonKind::ObjectEnd
        | JsonKind::ArrayStart
        | JsonKind::ArrayEnd
        | JsonKind::Key
        | JsonKind::Number
        | JsonKind::True
        | JsonKind::False
        | JsonKind::Null
        | JsonKind::Long => Err(wrong_type(name)),
    }
}

fn replacement(document: &Document, start: u32) -> Result<tools::Replacement, llm::Problem> {
    let occurrences = match member(document, start, b"all") {
        Some(index) => match document.token(index).ok_or(llm::Problem::NotAnObject)?.kind {
            JsonKind::True => tools::Occurrences::All,
            JsonKind::False => tools::Occurrences::Once,
            JsonKind::ObjectStart
            | JsonKind::ObjectEnd
            | JsonKind::ArrayStart
            | JsonKind::ArrayEnd
            | JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::Null
            | JsonKind::Long => return Err(wrong_type(b"all")),
        },
        None => tools::Occurrences::Once,
    };
    Ok(tools::Replacement {
        old: replacement_string(document, start, b"old")?,
        new: replacement_string(document, start, b"new")?,
        occurrences,
    })
}

fn replacements(document: &Document) -> Result<Box<[tools::Replacement]>, llm::Problem> {
    let start = member(document, 0, b"edits").ok_or(missing(b"edits"))?;
    if document.token(start).ok_or(llm::Problem::NotAnObject)?.kind != JsonKind::ArrayStart {
        return Err(wrong_type(b"edits"));
    }
    let count = replacement_count(document, start)?;
    if count == 0 {
        return Err(bad_value(b"edits"));
    }
    let mut edits = List::with_capacity(count);
    let mut depth = 0_u32;
    for index in start..document.len() {
        let record = document.token(index).ok_or(llm::Problem::NotAnObject)?;
        match record.kind {
            JsonKind::ObjectStart => {
                if depth == 1 {
                    edits.push(replacement(document, index)?).or(Err(llm::Problem::TooLarge))?;
                }
                depth = depth.checked_add(1).ok_or(llm::Problem::TooLarge)?;
            }
            JsonKind::ArrayStart => depth = depth.checked_add(1).ok_or(llm::Problem::TooLarge)?,
            JsonKind::ObjectEnd | JsonKind::ArrayEnd => {
                depth = depth.checked_sub(1).ok_or(llm::Problem::NotAnObject)?;
                if depth == 0 {
                    break;
                }
            }
            JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::True
            | JsonKind::False
            | JsonKind::Null
            | JsonKind::Long => {}
        }
    }
    Ok(edits.into_boxed())
}

fn replacement_count(document: &Document, start: u32) -> Result<u32, llm::Problem> {
    let mut depth = 0_u32;
    let mut count = 0_u32;
    for index in start..document.len() {
        let record = document.token(index).ok_or(llm::Problem::NotAnObject)?;
        match record.kind {
            JsonKind::ObjectStart => {
                if depth == 1 {
                    count = count.checked_add(1).ok_or(llm::Problem::TooLarge)?;
                }
                depth = depth.checked_add(1).ok_or(llm::Problem::TooLarge)?;
            }
            JsonKind::ArrayStart => {
                if depth == 1 {
                    return Err(wrong_type(b"edits"));
                }
                depth = depth.checked_add(1).ok_or(llm::Problem::TooLarge)?;
            }
            JsonKind::ObjectEnd | JsonKind::ArrayEnd => {
                depth = depth.checked_sub(1).ok_or(llm::Problem::NotAnObject)?;
                if depth == 0 {
                    break;
                }
            }
            JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::True
            | JsonKind::False
            | JsonKind::Null
            | JsonKind::Long => {
                if depth == 1 {
                    return Err(wrong_type(b"edits"));
                }
            }
        }
    }
    Ok(count)
}

fn required_string(document: &Document, name: &[u8]) -> Result<Box<[u8]>, llm::Problem> {
    match field(document, name) {
        Some(record) if record.kind == JsonKind::String => {
            Ok(document.text(&record).ok_or(llm::Problem::NotAnObject)?.into())
        }
        Some(_) => Err(wrong_type(name)),
        None => Err(missing(name)),
    }
}

fn optional_string(document: &Document, name: &[u8]) -> Result<Option<Box<[u8]>>, llm::Problem> {
    match field(document, name) {
        Some(record) if record.kind == JsonKind::String => {
            Ok(Some(document.text(&record).ok_or(llm::Problem::NotAnObject)?.into()))
        }
        Some(_) => Err(wrong_type(name)),
        None => Ok(None),
    }
}

fn optional_u32(document: &Document, name: &[u8]) -> Result<Option<u32>, llm::Problem> {
    match field(document, name) {
        Some(record) if record.kind == JsonKind::Number => {
            let number = decimal(document.text(&record).ok_or(llm::Problem::NotAnObject)?).ok_or(bad_value(name))?;
            if number == 0 {
                return Err(bad_value(name));
            }
            Ok(Some(number))
        }
        Some(_) => Err(wrong_type(name)),
        None => Ok(None),
    }
}

fn optional_seconds(document: &Document, name: &[u8]) -> Result<Option<Duration>, llm::Problem> {
    match optional_u32(document, name)? {
        Some(seconds) => Ok(Some(Duration::from_secs(u64::from(seconds)))),
        None => Ok(None),
    }
}

fn decimal(text: &[u8]) -> Option<u32> {
    if text.is_empty() {
        return None;
    }
    let mut number = 0_u32;
    for byte in text {
        if !byte.is_ascii_digit() {
            return None;
        }
        number = number.checked_mul(10)?.checked_add(u32::from(*byte).checked_sub(u32::from(b'0'))?)?;
    }
    Some(number)
}

fn path(text: &[u8]) -> Result<tools::Path, llm::Problem> {
    if text.is_empty() {
        return Ok(tools::Path { absolute: false, parts: Box::new([tools::Part::Current]) });
    }
    let absolute = text.first() == Some(&b'/');
    let capacity = u32::try_from(text.len()).or(Err(llm::Problem::TooLarge))?;
    let mut parts = List::with_capacity(capacity);
    let mut start = 0_usize;
    for (index, byte) in text.iter().enumerate() {
        if *byte == b'/' {
            append_part(&mut parts, text.get(start..index).ok_or(llm::Problem::TooLarge)?)?;
            start = index.checked_add(1).ok_or(llm::Problem::TooLarge)?;
        }
    }
    append_part(&mut parts, text.get(start..).ok_or(llm::Problem::TooLarge)?)?;
    Ok(tools::Path { absolute, parts: parts.into_boxed() })
}

fn append_part(parts: &mut List<tools::Part>, piece: &[u8]) -> Result<(), llm::Problem> {
    if piece.is_empty() {
        return Ok(());
    }
    let part = match piece {
        b"." => tools::Part::Current,
        b".." => tools::Part::Parent,
        name => {
            let name = tools::Name::new(name.into()).ok_or(bad_value(b"path"))?;
            tools::Part::Name { name }
        }
    };
    parts.push(part).or(Err(llm::Problem::TooLarge))
}

fn families(document: &Document) -> Result<run::charter::Families, llm::Problem> {
    let mut inspect = false;
    let mut modify = false;
    let mut shell = false;
    match field(document, b"tools") {
        Some(record) if record.kind == JsonKind::ArrayStart => {}
        Some(_) => return Err(wrong_type(b"tools")),
        None => return Err(missing(b"tools")),
    }
    let mut in_tools = false;
    let mut depth = 0_u32;
    for index in 0..document.len() {
        let record = document.token(index).expect("index within admitted document");
        match record.kind {
            JsonKind::ObjectEnd => depth = depth.checked_sub(1).ok_or(llm::Problem::NotAnObject)?,
            JsonKind::Key if depth == 1 => in_tools = document.text(record) == Some(b"tools".as_slice()),
            JsonKind::ObjectStart | JsonKind::ArrayStart => {
                depth = depth.checked_add(1).ok_or(llm::Problem::TooLarge)?;
            }
            JsonKind::ArrayEnd => {
                depth = depth.checked_sub(1).ok_or(llm::Problem::NotAnObject)?;
                if depth == 1 {
                    in_tools = false;
                }
            }
            JsonKind::String if in_tools && depth == 2 => {
                match document.text(record).ok_or(llm::Problem::NotAnObject)? {
                    b"inspect" => inspect = true,
                    b"modify" => modify = true,
                    b"shell" => shell = true,
                    _ => return Err(bad_value(b"tools")),
                }
            }
            JsonKind::Number | JsonKind::True | JsonKind::False | JsonKind::Null if in_tools && depth == 2 => {
                return Err(wrong_type(b"tools"));
            }
            JsonKind::Key
            | JsonKind::String
            | JsonKind::Number
            | JsonKind::True
            | JsonKind::False
            | JsonKind::Null
            | JsonKind::Long => {}
        }
    }
    Ok(run::charter::Families { tools: run::charter::Tools { inspect, modify, shell }, agents: false })
}
