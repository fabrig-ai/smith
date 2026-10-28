//! Measured schemas and words from service-supplied effective numbers.
//! It stores no conversation state and never chooses an admission limit.
//! Contract: protocol/llm.md, section 3; protocol/limits.md, section 3.

use crate::Stated;
use alloc::boxed::Box;
use skein_lib::{Decimal, Writer};
use smith_domain::tools::{Namespace, Tool};

struct Text {
    length: usize,
    writer: Option<Writer>,
}

impl Text {
    fn put(&mut self, bytes: &[u8]) {
        self.length = self.length.checked_add(bytes.len()).expect("fixed descriptions and u32 decimals fit");
        if let Some(writer) = &mut self.writer {
            writer.put(bytes).expect("measured description room");
        }
    }

    fn number(&mut self, number: u64) {
        self.put(Decimal::of(number).as_bytes());
    }

    fn finish(self) -> Box<[u8]> {
        self.writer.expect("writing pass owns its measured buffer").finish()
    }
}

pub(crate) fn description(tool: Tool, stated: &Stated, namespace: &Namespace) -> Box<[u8]> {
    let mut measure = Text { length: 0, writer: None };
    words(tool, stated, namespace, &mut measure);
    let mut output = Text { length: 0, writer: Some(Writer::new(measure.length)) };
    words(tool, stated, namespace, &mut output);
    output.finish()
}

pub(crate) fn schema(tool: Tool, stated: &Stated, namespace: &Namespace) -> Box<[u8]> {
    let mut measure = Text { length: 0, writer: None };
    properties(tool, stated, namespace, &mut measure);
    let mut output = Text { length: 0, writer: Some(Writer::new(measure.length)) };
    properties(tool, stated, namespace, &mut output);
    output.finish()
}

fn words(tool: Tool, stated: &Stated, namespace: &Namespace, output: &mut Text) {
    if tool == Tool::Remove {
        output.put(b"Remove a file you have read at its current version, or an empty directory; never a directory with files in it.");
        return;
    }
    match tool {
        Tool::Read => {
            output.put(b"Read whole lines in byte windows; default ");
            output.number(u64::from(stated.read_window));
            output.put(b" bytes, ceiling ");
            output.number(u64::from(stated.tool_payload));
            output.put(b" bytes. Next line, cuts and clamps are reported. ");
        }
        Tool::List => {
            output.put(b"List matching files and their directory tree; depth defaults to whole, direct children 1. Glob selects files. Max ");
            output.number(u64::from(stated.list_entries));
            output.put(b" entries; omitted files and directories counted once. ");
            match namespace {
                Namespace::One => output.put(b"List . for the working directory. "),
                Namespace::Several { .. } => output.put(b"List / for directory names. "),
                Namespace::NoWorkspace => {}
            }
        }
        Tool::Search => {
            output.put(b"Search files with rg using a regular expression; max ");
            output.number(u64::from(stated.search_hits));
            output.put(b" matches, ");
            output.number(u64::from(stated.search_bytes));
            output.put(b" bytes; cuts are marked. ");
        }
        Tool::Remove => unreachable!("remove has its own description"),
        Tool::Write => output.put(b"Write a file; read its current version first. "),
        Tool::Edit => output.put(
            b"Apply ordered edits atomically; no earlier read. Each old must match once, or at least once with all. ",
        ),
        Tool::Job => {
            output.put(b"Output since last answer; running or end. Max ");
            output.number(u64::from(stated.jobs));
            output.put(b" jobs/session, held until end is answered. Without stop, read beside reads, lists and searches; beside other calls it waits no longer than they run, so such a poll costs nothing; only with job calls alone does it wait as asked; with stop, write in call order. A poll is best asked to wait about as long as the job has left, so that one poll sees its end; it needs no deliberation of its own. Running jobs hold no later batch slot. Cuts marked.");
        }
        Tool::Shell => {
            output.put(b"Run a command; files may change. Max ");
            output.number(u64::from(stated.output_ceiling));
            output.put(b" output bytes; cuts are marked. Without yield_seconds, wait for command end. At yield, an unfinished command answers running with job number; timeout counts from command start. ");
        }
    }
    match tool {
        Tool::Read | Tool::Write | Tool::Edit => {
            output.put(b"Whole-file limit ");
            output.number(u64::from(stated.file_bytes));
            output.put(b" bytes. ");
        }
        Tool::Remove | Tool::List | Tool::Search | Tool::Shell | Tool::Job => {}
    }
    match tool {
        Tool::Write | Tool::Edit => output.put(b"For a larger file, write its first part within the byte bound, then extend it with edits; put as many edits in one call as fit the bound. "),
        Tool::Read | Tool::Remove | Tool::List | Tool::Search | Tool::Shell | Tool::Job => {}
    }
    match tool {
        Tool::Read | Tool::List | Tool::Search => {
            output.put(b"Batch independent reads, lists and searches side by side.");
        }
        Tool::Remove | Tool::Job => {}
        Tool::Write | Tool::Edit | Tool::Shell => {
            output.put(b"Batch a change's edits across its files, then the command that checks them; they run one at a time in call order, and a write that fails, or any call answered invalid, leaves the writes and commands after it not run.");
        }
    }
}

fn path(stated: &Stated, namespace: &Namespace, output: &mut Text) {
    output.put(br#""path":{"type":"string","description":""#);
    match namespace {
        Namespace::NoWorkspace => output.put(b"No workspace path; max "),
        Namespace::One => output.put(b"Relative to the working directory, e.g. src/lib.rs; max "),
        Namespace::Several { .. } => output.put(b"Relative to the working directory or /name/...; max "),
    }
    output.number(u64::from(stated.path_bytes));
    output.put(br#" bytes. The dot (.) and the empty path both name the working directory. Count each name, . and .., with one separator between each two; repeated, leading and trailing separators count nothing. Keep the same bound once resolved, . and .. removed, from the workspace root."}"#);
}

fn input(stated: &Stated, output: &mut Text) {
    output.put(b" Call JSON max ");
    output.number(u64::from(stated.call_input_bytes));
    output.put(b" bytes.");
}

fn payload(stated: &Stated, output: &mut Text) {
    output.number(u64::from(stated.tool_payload));
    output.put(b" bytes.");
    input(stated, output);
}

fn properties(tool: Tool, stated: &Stated, namespace: &Namespace, output: &mut Text) {
    output.put(br#"{"type":"object","properties":{"#);
    match tool {
        Tool::Read => {
            path(stated, namespace, output);
            output.put(br#", "offset":{"type":"integer","minimum":1,"maximum":4294967295,"description":"First line, counting from 1; default 1."},"window":{"type":"integer","minimum":1,"description":"Requested byte window; default "#);
            output.number(u64::from(stated.read_window));
            output.put(b" bytes; max ");
            output.number(u64::from(stated.tool_payload));
            output.put(br#" bytes. Larger requests are clamped and reported."}},"required":["path"]}"#);
        }
        Tool::List => {
            path(stated, namespace, output);
            output.put(br#", "depth":{"type":"integer","minimum":1,"maximum":4294967295,"description":"Optional depth; absent lists the whole tree. Direct children are at depth 1."},"glob":{"type":"string","description":"Optional rg file glob; directories holding a matching file remain even when that file is below depth."#);
            input(stated, output);
            output.put(br#""}},"required":["path"]}"#);
        }
        Tool::Search => {
            path(stated, namespace, output);
            output.put(br#", "pattern":{"type":"string","description":"Regular expression for rg."#);
            input(stated, output);
            output.put(br#""},"glob":{"type":"string","description":"Optional file glob."#);
            input(stated, output);
            output.put(br#""}},"required":["pattern"]}"#);
        }
        Tool::Remove => {
            output.put(br#""path":{"type":"string","description":"The path, inside the workspace, as write takes it."}},"required":["path"]}"#);
        }
        Tool::Write => {
            path(stated, namespace, output);
            output.put(br#", "content":{"type":"string","description":"Content for this write; max "#);
            payload(stated, output);
            output.put(br#""}},"required":["path","content"]}"#);
        }
        Tool::Edit => {
            path(stated, namespace, output);
            output.put(br#", "edits":{"type":"array","minItems":1,"maxItems":"#);
            output.number(u64::from(stated.tool_payload));
            output.put(br#", "description":"Apply in order and store once; none apply if one fails.","items":{"type":"object","properties":{"old":{"type":"string","description":"Text to match; aggregate old snippets max "#);
            payload(stated, output);
            output.put(br#""},"new":{"type":"string","description":"Replacement text; aggregate new snippets max "#);
            payload(stated, output);
            output.put(br#""},"all":{"type":"boolean","description":"False requires one unique match; true replaces every match."}},"required":["old","new"]}}},"required":["path","edits"]}"#);
        }
        Tool::Job => {
            output.put(br#""job":{"type":"integer","minimum":1,"description":"Job number answered running by this session."},"wait_seconds":{"type":"integer","minimum":0,"description":"Wait; default "#);
            output.number(stated.job_poll.as_nanos() / 1_000_000_000);
            output.put(b" seconds; max ");
            job_wait(stated, output);
            output.put(br#""},"output_bytes":{"type":"integer","minimum":1,"description":""#);
            command_output(stated, output);
            output.put(br#""},"stop":{"type":"boolean","description":"Stop, then report end. Default false."}},"required":["job"]}"#);
        }
        Tool::Shell => {
            output.put(br#""command":{"type":"string","description":"Shell command text."#);
            input(stated, output);
            output.put(
                br#""},"timeout_seconds":{"type":"integer","minimum":1,"description":"Seconds; default deadline "#,
            );
            output.number(stated.shell_timeout.as_nanos() / 1_000_000_000);
            output.put(b" seconds; effective maximum ");
            output.number(stated.tool_deadline.as_nanos() / 1_000_000_000);
            output.put(br#" seconds, within time left; clamped."},"output_bytes":{"type":"integer","minimum":1,"description":""#);
            command_output(stated, output);
            output.put(br#""},"yield_seconds":{"type":"integer","minimum":0,"description":"Yield; no default; max "#);
            job_wait(stated, output);
            output.put(br#""}},"required":["command"]}"#);
        }
    }
}

fn command_output(stated: &Stated, output: &mut Text) {
    output.put(b"Bytes; default ");
    output.number(u64::from(stated.shell_output));
    output.put(b"; max ");
    output.number(u64::from(stated.output_ceiling));
    output.put(b"; head/tail, clamped.");
}

fn job_wait(stated: &Stated, output: &mut Text) {
    output.number(stated.job_wait.as_nanos() / 1_000_000_000);
    output.put(b" seconds; clamped.");
}
