//! Allowed workspace built-ins (protocol/programs.md, sections 5.3, 5.4 and 8).
//! Keeps one operation and its bounded edit operands, never permission policy.
//! `start` and `file` return file effects or a terminal tool result; paths are
//! resolved from the launch directory in the shared invocation file root.

use std::path::Path;

use serde_json::Value;
use skein_io::file;
use skein_lib::Token;

use crate::{commands::FILE_OWNER, fake::FILE_BYTES};

#[derive(Debug)]
pub(crate) struct Outcome {
    pub text: String,
    pub error: bool,
}

impl Outcome {
    fn failed(message: &str) -> Self {
        Self { text: message.into(), error: true }
    }
}

#[derive(Debug)]
enum Operation {
    Read,
    Write,
    Edit { old: String, new: String, replace_all: bool },
}

#[derive(Debug)]
pub(crate) enum Progress {
    Request(file::Request),
    Done(Outcome),
}

#[derive(Debug)]
pub(crate) struct Act {
    pub path: String,
    root: Token,
    resolved: Box<[u8]>,
    operation: Operation,
    saving: bool,
}

impl Act {
    pub fn start(tool: &str, input: &Value, root: Token, directory: &Path) -> Result<(Self, file::Request), Outcome> {
        let path = string(input, "file_path")?;
        if path.is_empty() {
            return Err(Outcome::failed("file_path must not be empty"));
        }
        let operation = match tool {
            "Read" => Operation::Read,
            "Write" => Operation::Write,
            "Edit" => {
                let old = string(input, "old_string")?;
                if old.is_empty() {
                    return Err(Outcome::failed("old_string must not be empty"));
                }
                let new = string(input, "new_string")?;
                let replace_all = match input.get("replace_all") {
                    None => false,
                    Some(value) => value.as_bool().ok_or_else(|| Outcome::failed("replace_all must be a boolean"))?,
                };
                Operation::Edit { old: old.into(), new: new.into(), replace_all }
            }
            _ => unreachable!("Act tools are validated before the permission call"),
        };
        let resolved = directory.join(path);
        let resolved = resolved.strip_prefix("/").unwrap_or(&resolved);
        let mut state = Self {
            path: path.into(),
            root,
            resolved: resolved.as_os_str().as_encoded_bytes().into(),
            operation,
            saving: false,
        };
        let request = if matches!(state.operation, Operation::Write) {
            let content = string(input, "content")?;
            if content.len() > FILE_BYTES as usize {
                return Err(Outcome::failed("write exceeds the file byte limit"));
            }
            state.store(content.as_bytes().into())
        } else {
            file::Request::Load {
                owner: FILE_OWNER,
                root,
                path: state.resolved.clone(),
                max: FILE_BYTES,
                no_follow: true,
            }
        };
        Ok((state, request))
    }

    fn store(&mut self, bytes: Box<[u8]>) -> file::Request {
        self.saving = true;
        file::Request::Store {
            owner: FILE_OWNER,
            root: self.root,
            path: self.resolved.clone(),
            bytes,
            expected: file::Expect::Any,
            no_follow: true,
        }
    }

    pub fn file(&mut self, event: file::Event) -> Progress {
        if self.saving {
            return Progress::Done(match event {
                file::Event::Stored { .. } => Outcome {
                    text: if matches!(self.operation, Operation::Write) { "Wrote file." } else { "Edited file." }
                        .into(),
                    error: false,
                },
                file::Event::Failed { error, .. } => Outcome::failed(&format!("could not write file: {error:?}")),
                _ => Outcome::failed("could not write file"),
            });
        }
        let bytes = match event {
            file::Event::Loaded { bytes, .. } => bytes,
            file::Event::TooLarge { .. } => return Progress::Done(Outcome::failed("read exceeds the file byte limit")),
            file::Event::Failed { error, .. } => {
                return Progress::Done(Outcome::failed(&format!("could not read file: {error:?}")));
            }
            _ => return Progress::Done(Outcome::failed("could not read file")),
        };
        let Ok(text) = String::from_utf8(bytes.into_vec()) else {
            return Progress::Done(Outcome::failed("file is not UTF-8 text"));
        };
        match &self.operation {
            Operation::Read => Progress::Done(Outcome { text, error: false }),
            Operation::Edit { old, new, replace_all } => {
                let Some(first) = text.find(old.as_str()) else {
                    return Progress::Done(Outcome::failed("old_string was not found"));
                };
                // Advance one character, rather than the whole match, so an
                // overlapping second match also makes a single edit ambiguous.
                let after_first = first + old.chars().next().expect("nonempty old_string").len_utf8();
                if !replace_all && text[after_first..].contains(old.as_str()) {
                    return Progress::Done(Outcome::failed("old_string occurs more than once"));
                }
                let count = if *replace_all { text.matches(old.as_str()).count() } else { 1 };
                let size =
                    old.len().checked_mul(count).and_then(|removed| text.len().checked_sub(removed)).and_then(
                        |remaining| new.len().checked_mul(count).and_then(|added| remaining.checked_add(added)),
                    );
                if size.is_none_or(|size| size > FILE_BYTES as usize) {
                    return Progress::Done(Outcome::failed("edit exceeds the file byte limit"));
                }
                let edited = if *replace_all { text.replace(old, new) } else { text.replacen(old, new, 1) };
                Progress::Request(self.store(edited.into_bytes().into()))
            }
            Operation::Write => unreachable!("writes start in the saving phase"),
        }
    }
}

fn string<'a>(input: &'a Value, name: &str) -> Result<&'a str, Outcome> {
    input.get(name).and_then(Value::as_str).ok_or_else(|| Outcome::failed(&format!("{name} must be a string")))
}
