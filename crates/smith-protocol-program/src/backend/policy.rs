//! Built-in permission policy (protocol/programs.md 5.3–5.4; domain/tools.md 10).
//! Keeps no filesystem knowledge: admission uses only grants and lexical mounts.

use super::Mount;
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_lib::List;
use smith_domain::tools::{Effect, Grants};

const NOT_GRANTED: &[u8] = b"tool not granted";
const TOO_LONG: &[u8] = b"path is too long";
const OUTSIDE: &[u8] = b"path is outside the workspace: ";
const PROTECTED: &[u8] = b"git directory is protected";
const READ_ONLY: &[u8] = b"path is read-only: ";
const MISSING: &[u8] = b"missing field ";
const WRONG_TYPE: &[u8] = b"wrong field type ";
const TOOLS_FLAG_BYTES: u32 = 30;

/// A Claude built-in, checked before the program executes it (programs.md 5.4).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Builtin {
    /// Inspect one file.
    Read,
    /// List matching paths.
    Glob,
    /// Search file contents.
    Grep,
    /// Replace a file.
    Write,
    /// Edit a file.
    Edit,
    /// Execute a command without parsing its text.
    Bash,
}

pub(super) const BUILTINS: [Builtin; 6] =
    [Builtin::Read, Builtin::Glob, Builtin::Grep, Builtin::Write, Builtin::Edit, Builtin::Bash];

impl Builtin {
    pub(crate) fn named(name: &[u8]) -> Option<Self> {
        match name {
            b"Read" => Some(Self::Read),
            b"Glob" => Some(Self::Glob),
            b"Grep" => Some(Self::Grep),
            b"Write" => Some(Self::Write),
            b"Edit" => Some(Self::Edit),
            b"Bash" => Some(Self::Bash),
            _ => None,
        }
    }

    pub(crate) const fn name(self) -> &'static [u8] {
        match self {
            Self::Read => b"Read",
            Self::Glob => b"Glob",
            Self::Grep => b"Grep",
            Self::Write => b"Write",
            Self::Edit => b"Edit",
            Self::Bash => b"Bash",
        }
    }

    pub(crate) const fn effect(self) -> Effect {
        match self {
            Self::Read | Self::Glob | Self::Grep => Effect::Read,
            Self::Write | Self::Edit | Self::Bash => Effect::Write,
        }
    }

    pub(crate) const fn granted(self, grants: Grants) -> bool {
        match self {
            Self::Read | Self::Glob | Self::Grep => grants.inspect,
            Self::Write | Self::Edit => grants.modify,
            Self::Bash => grants.shell,
        }
    }
}

pub(crate) fn tools_flag(grants: Grants) -> Box<[u8]> {
    let mut bytes = List::with_capacity(TOOLS_FLAG_BYTES);
    for tool in BUILTINS {
        if tool.granted(grants) {
            if !bytes.is_empty() {
                bytes.push(b',').expect("six names");
            }
            for byte in tool.name() {
                bytes.push(*byte).expect("six names");
            }
        }
    }
    bytes.into_boxed()
}

fn deny(message: &[u8]) -> mcp::Verdict {
    mcp::Verdict::Deny { message: message.into() }
}

pub(crate) fn judge(tool: &[u8], input: &[u8], grants: Grants, mounts: &[Mount], path_bytes: u32) -> mcp::Verdict {
    let tool = match Builtin::named(tool) {
        Some(tool) if tool.granted(grants) => tool,
        Some(_) | None => return deny(NOT_GRANTED),
    };
    let field = match tool {
        Builtin::Bash => return mcp::Verdict::Allow { updated_input: input.into() },
        Builtin::Read | Builtin::Write | Builtin::Edit => b"file_path".as_slice(),
        Builtin::Glob | Builtin::Grep => b"path".as_slice(),
    };
    let reading = path_field(input, field);
    let cwd = match mounts.first() {
        Some(mount) => mount.host.as_ref(),
        None => b"/",
    };
    let path = match &reading {
        PathField::Text(path) => path.as_ref(),
        PathField::Absent if field == b"path" => cwd,
        PathField::Absent => return deny(&json::join(&[MISSING, b"\"", field, b"\""])),
        PathField::NotText | PathField::Unreadable => return deny(&json::join(&[WRONG_TYPE, b"\"", field, b"\""])),
    };
    if path.len() > usize::try_from(path_bytes).expect("path bound") {
        return deny(TOO_LONG);
    }
    let resolved = resolve(cwd, path, path_bytes);
    let mut selected: Option<&Mount> = None;
    for mount in mounts {
        if contains(&mount.host, &resolved) {
            selected = match selected {
                Some(previous) if previous.host.len() >= mount.host.len() => Some(previous),
                Some(_) | None => Some(mount),
            };
        }
    }
    let Some(mount) = selected else {
        return deny(&json::join(&[OUTSIDE, path]));
    };
    let mut cursor = 0;
    for _ in 0..=resolved.len() {
        let Some(name) = next_name(&resolved, &mut cursor) else {
            break;
        };
        if name.eq_ignore_ascii_case(b".git") {
            return deny(PROTECTED);
        }
    }
    match tool {
        Builtin::Write | Builtin::Edit if !mount.writable => return deny(&json::join(&[READ_ONLY, path])),
        Builtin::Read | Builtin::Glob | Builtin::Grep | Builtin::Write | Builtin::Edit | Builtin::Bash => {}
    }
    mcp::Verdict::Allow { updated_input: input.into() }
}

pub(super) fn contains(mount: &[u8], path: &[u8]) -> bool {
    mount == b"/" || path == mount || (path.starts_with(mount) && path.get(mount.len()) == Some(&b'/'))
}

pub(super) fn resolve(cwd: &[u8], path: &[u8], maximum: u32) -> Box<[u8]> {
    let mut resolved =
        List::with_capacity(maximum.checked_mul(2).expect("two paths").checked_add(1).expect("joining slash"));
    for part in [if path.starts_with(b"/") { b"".as_slice() } else { cwd }, path] {
        let mut cursor = 0;
        for _ in 0..=part.len() {
            let Some(name) = next_name(part, &mut cursor) else {
                break;
            };
            match name {
                b"" | b"." => {}
                b".." => {
                    let mut end = 0;
                    for (index, byte) in resolved.iter().enumerate() {
                        if *byte == b'/' {
                            end = u32::try_from(index).expect("path cursor");
                        }
                    }
                    resolved.truncate(end);
                }
                _ => {
                    resolved.push(b'/').expect("two paths");
                    for byte in name {
                        resolved.push(*byte).expect("two paths");
                    }
                }
            }
        }
    }
    if resolved.is_empty() {
        resolved.push(b'/').expect("root path");
    }
    resolved.into_boxed()
}

// The cursor advances by at least one byte per name, including empty names.
fn next_name<'input>(bytes: &'input [u8], cursor: &mut usize) -> Option<&'input [u8]> {
    if *cursor > bytes.len() {
        return None;
    }
    let start = *cursor;
    let mut end = bytes.len();
    for index in start..bytes.len() {
        if bytes.get(index) == Some(&b'/') {
            end = index;
            break;
        }
    }
    *cursor = end.checked_add(1).expect("bounded input cursor");
    Some(bytes.get(start..end).expect("bounded name"))
}

/// Distinguish an absent field from an input that cannot be judged safely.
enum PathField {
    Absent,
    Text(Box<[u8]>),
    NotText,
    Unreadable,
}

fn path_field(input: &[u8], field: &[u8]) -> PathField {
    // MCP admits input with this same collector and byte-derived nesting bound.
    let Some(document) = json::collect(input) else {
        return PathField::Unreadable;
    };
    let Some(root) = document.token(0) else {
        return PathField::Unreadable;
    };
    if root.kind != skein_json::Kind::ObjectStart {
        return PathField::Unreadable;
    }
    let mut found = PathField::Absent;
    let mut index = 1;
    for _ in 0..document.len() {
        let Some(key) = document.token(index) else {
            return PathField::Unreadable;
        };
        if key.kind == skein_json::Kind::ObjectEnd {
            return found;
        }
        if key.kind != skein_json::Kind::Key {
            return PathField::Unreadable;
        }
        let Some(value) = index.checked_add(1) else {
            return PathField::Unreadable;
        };
        if document.text(key) == Some(field) {
            match found {
                PathField::Absent => {}
                PathField::Text(_) | PathField::NotText | PathField::Unreadable => return PathField::Unreadable,
            }
            found = match json::text(&document, value) {
                Some(text) => PathField::Text(text.into()),
                None => PathField::NotText,
            };
        }
        let Some(next) = json::after(&document, value) else {
            return PathField::Unreadable;
        };
        index = next;
    }
    PathField::Unreadable
}

#[cfg(test)]
#[path = "policy_tests.rs"]
mod tests;
