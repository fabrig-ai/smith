//! Workspace outcomes into bounded text for a live or resumed LLM prompt.
//! The domain chooses retained content; this module chooses only its words.
//! It holds no state across calls and never decides whether an effect ran.
//! Contract: protocol/llm.md, section 5; protocol/transcript.md, section 5;
//! domain/tools.md, section 4.

use alloc::boxed::Box;

use skein_lib::{Decimal, Writer};
use smith_domain::tools::{self, Outcome};

use crate::Error;

/// Room for a newline, brackets, a u64 byte count and the cut's words.
/// Derived result bounds always hold this marker (protocol/limits.md, section 6).
pub const CUT_MARKER_BYTES: u32 = 37;

/// Render workspace feedback, cutting its prefix with the exact omitted bytes.
/// The caller derives a bound large enough for the marker before admitting a
/// prompt. Cutting preserves the outcome's error flag and every UTF-8 boundary.
pub fn render_outcome(
    outcome: &Outcome,
    namespace: &tools::Namespace,
    maximum: u32,
) -> Result<(Box<[u8]>, bool), Error> {
    assert!(maximum >= CUT_MARKER_BYTES, "derived render bound holds the cut marker");
    let mut measure = Sink { length: 0, kept: 0, maximum: u64::MAX, cut: false, writer: None };
    let error = render(outcome, namespace, &mut measure)?;
    let prefix_cap = if measure.length > u64::from(maximum) {
        u64::from(maximum.checked_sub(CUT_MARKER_BYTES).expect("marker room checked"))
    } else {
        u64::from(maximum)
    };
    let mut prefix = Sink { length: 0, kept: 0, maximum: prefix_cap, cut: false, writer: None };
    let again = render(outcome, namespace, &mut prefix)?;
    assert_eq!(error, again, "render classification is stable");
    let omitted = measure.length.checked_sub(prefix.kept).expect("prefix within full rendering");
    let marker_length = if omitted > 0 {
        u64::try_from(Decimal::of(omitted).as_bytes().len())
            .or(Err(Error::Limit))?
            .checked_add(17)
            .ok_or(Error::Limit)?
    } else {
        0
    };
    let length = usize::try_from(prefix.kept.checked_add(marker_length).ok_or(Error::Limit)?).or(Err(Error::Limit))?;
    let mut output = Sink { length: 0, kept: 0, maximum: prefix_cap, cut: false, writer: Some(Writer::new(length)) };
    let rendered = render(outcome, namespace, &mut output)?;
    assert_eq!(error, rendered, "render classification is stable");
    assert_eq!(output.kept, prefix.kept, "render prefix is stable");
    let mut writer = output.writer.expect("writing pass owns its measured output");
    if omitted > 0 {
        writer.put(b"\n[").expect("measured marker room");
        writer.put(Decimal::of(omitted).as_bytes()).expect("measured byte-count room");
        writer.put(b" bytes omitted]").expect("measured marker room");
    }
    Ok((writer.finish(), error))
}

struct Sink {
    length: u64,
    kept: u64,
    maximum: u64,
    cut: bool,
    writer: Option<Writer>,
}

impl Sink {
    fn put(&mut self, text: &[u8]) -> Result<(), Error> {
        self.length = self.length.checked_add(u64::try_from(text.len()).or(Err(Error::Limit))?).ok_or(Error::Limit)?;
        if self.cut {
            return Ok(());
        }
        let available = self.maximum.checked_sub(self.kept).expect("kept prefix within cap");
        let wanted = usize::try_from(available).unwrap_or(usize::MAX).min(text.len());
        let mut boundary = wanted;
        // At most three continuation bytes precede a UTF-8 boundary. No byte
        // beyond the cap is copied, including when a multi-byte scalar straddles it.
        for _byte in 0_u8..3 {
            match text.get(boundary) {
                Some(byte) if byte & 0xc0 == 0x80 && boundary > 0 => {
                    boundary = boundary.checked_sub(1).expect("positive boundary");
                }
                Some(_) | None => break,
            }
        }
        let retained = text.get(..boundary).expect("boundary within text");
        self.kept = self.kept.checked_add(u64::try_from(retained.len()).or(Err(Error::Limit))?).ok_or(Error::Limit)?;
        self.cut = boundary < text.len();
        match &mut self.writer {
            Some(writer) => writer.put(retained).or(Err(Error::Limit)),
            None => Ok(()),
        }
    }

    fn number(&mut self, number: u64) -> Result<(), Error> {
        self.put(Decimal::of(number).as_bytes())
    }

    fn text(&mut self, bytes: &[u8]) -> Result<(), Error> {
        let mut next = 0_usize;
        for position in 0..bytes.len() {
            if position < next {
                continue;
            }
            let remaining = bytes.get(position..).expect("position within input");
            let width = scalar_width(remaining);
            let advance = width.unwrap_or(1);
            let scalar = remaining.get(..advance).expect("validated scalar or one invalid byte");
            match width {
                Some(_) => match scalar {
                    [b'\n' | b'\t'] => self.put(scalar)?,
                    [0..=31 | 127] | [0xc2, 0x80..=0x9f] => self.escaped(scalar)?,
                    _ => self.put(scalar)?,
                },
                None => self.escaped(scalar)?,
            }
            next = position.checked_add(advance).expect("advance remains within input");
        }
        Ok(())
    }

    fn escaped(&mut self, bytes: &[u8]) -> Result<(), Error> {
        for byte in bytes {
            let high = hex(byte >> 4_u8);
            let low = hex(byte & 15_u8);
            self.put(&[b'\\', b'x', high, low])?;
        }
        Ok(())
    }
}

// RFC 3629 scalar prefixes exclude overlong forms, surrogates and values
// beyond U+10FFFF. The complete result buffer needs no streaming decoder state.
fn scalar_width(bytes: &[u8]) -> Option<usize> {
    match bytes {
        [0..=0x7f, ..] => Some(1),
        [0xc2..=0xdf, 0x80..=0xbf, ..] => Some(2),
        [0xe0, 0xa0..=0xbf, 0x80..=0xbf, ..]
        | [0xe1..=0xec | 0xee..=0xef, 0x80..=0xbf, 0x80..=0xbf, ..]
        | [0xed, 0x80..=0x9f, 0x80..=0xbf, ..] => Some(3),
        [0xf0, 0x90..=0xbf, 0x80..=0xbf, 0x80..=0xbf, ..]
        | [0xf1..=0xf3, 0x80..=0xbf, 0x80..=0xbf, 0x80..=0xbf, ..]
        | [0xf4, 0x80..=0x8f, 0x80..=0xbf, 0x80..=0xbf, ..] => Some(4),
        _ => None,
    }
}

fn hex(nibble: u8) -> u8 {
    *b"0123456789abcdef".get(usize::from(nibble)).expect("four-bit byte")
}

#[expect(clippy::too_many_lines, reason = "one exhaustive rendering cell per outcome variant")]
fn render(outcome: &Outcome, namespace: &tools::Namespace, output: &mut Sink) -> Result<bool, Error> {
    let error = match outcome {
        Outcome::Read { content, first, lines, total, next, cut, window } => {
            read(content, *first, *lines, *total, *cut, output)?;
            read_following(*next, *window, output)?;
            false
        }
        Outcome::Listed { entries, more } => {
            for entry in entries {
                output.text(entry.name.as_ref())?;
                output.put(b"\t")?;
                let kind = match entry.kind {
                    tools::Kind::File => b"file".as_slice(),
                    tools::Kind::Directory => b"directory",
                    tools::Kind::Link => b"link",
                    tools::Kind::Other => b"other",
                };
                output.put(kind)?;
                output.put(b"\n")?;
            }
            if *more > 0 {
                output.put(b"... ")?;
                output.number(*more)?;
                output.put(b" more entries")?;
            }
            false
        }
        Outcome::Found { hits, more, timed_out } => {
            for hit in hits {
                output.text(&hit.path)?;
                output.put(b":")?;
                output.number(u64::from(hit.line))?;
                output.put(b": ")?;
                output.text(&hit.text)?;
                output.put(b"\n")?;
            }
            if *more > 0 {
                output.put(b"... ")?;
                output.number(*more)?;
                output.put(b" more matches\n")?;
            }
            if *timed_out {
                output.put(b"search timed out")?;
            }
            *timed_out
        }
        Outcome::Written { created } => {
            if *created {
                output.put(b"file created")?;
            } else {
                output.put(b"file written")?;
            }
            false
        }
        Outcome::Removed => {
            output.put(b"removed")?;
            false
        }
        Outcome::NotEmpty => {
            output.put(b"directory is not empty")?;
            true
        }
        Outcome::Edited { replaced } => {
            output.put(b"replaced ")?;
            output.number(u64::from(*replaced))?;
            output.put(b" occurrence(s)")?;
            false
        }
        Outcome::Exited { exit, head, tail, dropped, timeout, output: kept } => {
            let failure = command_end(*exit, output)?;
            command_output(head, tail, *dropped, output)?;
            command_following(*timeout, *kept, output)?;
            failure
        }
        Outcome::Running { job, head, tail, dropped, timeout, after, output: kept } => {
            job_number(*job, output)?;
            output.put(b" running; use job to read the rest")?;
            command_output(head, tail, *dropped, output)?;
            command_following(*timeout, tools::BytesClamp::Within, output)?;
            job_following(b"yield", *after, *kept, output)?;
            false
        }
        Outcome::Job { job, head, tail, dropped, wait, output: kept, end } => {
            job_number(*job, output)?;
            output.put(b" output since its last answer:")?;
            command_output(head, tail, *dropped, output)?;
            output.put(b"\n")?;
            let failure = match end {
                tools::JobEnd::Running => {
                    output.put(b"running")?;
                    false
                }
                tools::JobEnd::Ended(exit) => command_end(*exit, output)?,
                tools::JobEnd::Stopped => {
                    output.put(b"stopped")?;
                    true
                }
                tools::JobEnd::Cancelled => {
                    output.put(b"cancelled")?;
                    true
                }
            };
            job_following(b"wait", *wait, *kept, output)?;
            failure
        }
        Outcome::NotGranted => {
            output.put(b"tool not granted")?;
            true
        }
        Outcome::Outside { path } => {
            output.put(b"path is outside the workspace: ")?;
            resolved_path(path, namespace, output)?;
            true
        }
        Outcome::ReadOnly { path } => {
            output.put(b"path is read-only: ")?;
            resolved_path(path, namespace, output)?;
            true
        }
        Outcome::TooLong => {
            output.put(b"path is too long")?;
            true
        }
        Outcome::NotFound { path } => {
            output.put(b"path not found: ")?;
            resolved_path(path, namespace, output)?;
            true
        }
        Outcome::NotFile => {
            output.put(b"path is not a file")?;
            true
        }
        Outcome::Linked => {
            output.put(b"write path crosses a symbolic link")?;
            true
        }
        Outcome::Protected => {
            output.put(b"git directory is protected")?;
            true
        }
        Outcome::NotDirectory { path } => {
            output.put(b"path is not a directory: ")?;
            resolved_path(path, namespace, output)?;
            true
        }
        Outcome::TooLarge { what, size, bound } => {
            size_refusal(*what, *size, *bound, output)?;
            true
        }
        Outcome::NotRead => {
            output.put(b"read the whole file without offset or limit before changing it")?;
            true
        }
        Outcome::Stale => {
            output.put(b"file changed or is gone since it was read; read it again, or, if it is gone, write it again to create it")?;
            true
        }
        Outcome::EditFailed { edit, why } => {
            output.put(b"edit ")?;
            output.number(u64::from(*edit))?;
            output.put(b" failed: ")?;
            match why {
                tools::Mismatch::NoMatch => output.put(b"text to replace was not found")?,
                tools::Mismatch::Unchanged => output.put(b"edits leave the file unchanged")?,
                tools::Mismatch::Ambiguous { count, lines } => {
                    output.put(b"text matches ")?;
                    output.number(u64::from(*count))?;
                    output.put(b" times, starting on lines ")?;
                    for (index, line) in lines.iter().enumerate() {
                        if index > 0 {
                            output.put(b", ")?;
                        }
                        output.number(u64::from(*line))?;
                    }
                }
            }
            true
        }
        Outcome::Failed { fault } => {
            let text = match fault {
                tools::Fault::Denied => b"operation failed: permission denied".as_slice(),
                tools::Fault::NoSpace => b"operation failed: no space",
                tools::Fault::Other => b"operation failed: io error",
            };
            output.put(text)?;
            true
        }
        Outcome::TimedOut => {
            output.put(b"operation timed out")?;
            true
        }
        Outcome::Cancelled => {
            output.put(b"operation cancelled")?;
            true
        }
        Outcome::Busy => {
            output.put(b"tool is busy")?;
            true
        }
        Outcome::NulByte => {
            output.put(b"input contains a NUL byte")?;
            true
        }
    };
    Ok(error)
}

fn read(content: &[u8], first: u32, lines: u32, total: u32, cut: tools::Cut, output: &mut Sink) -> Result<(), Error> {
    let skipped = first.checked_sub(1).ok_or(Error::Invalid)?;
    let mut start = 0_usize;
    let mut index = 0_u64;
    for (position, byte) in content.iter().enumerate() {
        if *byte == b'\n' {
            output.number(
                u64::from(skipped).checked_add(index).ok_or(Error::Limit)?.checked_add(1).ok_or(Error::Limit)?,
            )?;
            output.put(b": ")?;
            output.text(content.get(start..position).ok_or(Error::Limit)?)?;
            output.put(b"\n")?;
            start = position.checked_add(1).ok_or(Error::Limit)?;
            index = index.checked_add(1).ok_or(Error::Limit)?;
        }
    }
    if start < content.len() {
        output
            .number(u64::from(skipped).checked_add(index).ok_or(Error::Limit)?.checked_add(1).ok_or(Error::Limit)?)?;
        output.put(b": ")?;
        output.text(content.get(start..).ok_or(Error::Limit)?)?;
        output.put(b"\n")?;
    }
    output.put(b"(")?;
    output.number(u64::from(lines))?;
    output.put(b" of ")?;
    output.number(u64::from(total))?;
    output.put(b" lines; skipped ")?;
    output.number(u64::from(skipped))?;
    output.put(b")")?;
    match cut {
        tools::Cut::Line => output.put(b" [line cut]")?,
        tools::Cut::Whole => {}
    }
    Ok(())
}

fn read_following(next: tools::Next, window: tools::BytesClamp, output: &mut Sink) -> Result<(), Error> {
    match next {
        tools::Next::Line(line) => {
            output.put(b" [next line ")?;
            output.number(u64::from(line))?;
            output.put(b"]")?;
        }
        tools::Next::End => output.put(b" [end of file]")?,
    }
    match window {
        tools::BytesClamp::Within => {}
        tools::BytesClamp::Clamped { asked, ceiling } => {
            output.put(b" [window clamped: ")?;
            output.number(u64::from(asked))?;
            output.put(b" bytes to ")?;
            output.number(u64::from(ceiling))?;
            output.put(b" bytes]")?;
        }
    }
    Ok(())
}

fn command_end(exit: tools::Exit, output: &mut Sink) -> Result<bool, Error> {
    match exit {
        tools::Exit::Code { code } => {
            output.put(b"exit code ")?;
            output.number(u64::from(code))?;
            Ok(code != 0)
        }
        tools::Exit::Signal { signal } => {
            output.put(b"signal ")?;
            output.number(u64::from(signal))?;
            Ok(true)
        }
        tools::Exit::TimedOut => {
            output.put(b"command timed out")?;
            Ok(true)
        }
    }
}

fn command_output(head: &[u8], tail: &[u8], dropped: u64, output: &mut Sink) -> Result<(), Error> {
    if !head.is_empty() {
        output.put(b"\n")?;
        output.text(head)?;
    }
    if dropped > 0 {
        output.put(b"\n... ")?;
        output.number(dropped)?;
        output.put(b" bytes dropped ...")?;
    }
    if !tail.is_empty() {
        output.put(b"\n")?;
        output.text(tail)?;
    }
    Ok(())
}

fn job_number(job: tools::JobNumber, output: &mut Sink) -> Result<(), Error> {
    output.put(b"job ")?;
    output.number(u64::from(job.0))
}

fn job_following(
    label: &[u8],
    wait: tools::TimeoutClamp,
    kept: tools::BytesClamp,
    output: &mut Sink,
) -> Result<(), Error> {
    match wait {
        tools::TimeoutClamp::Within => {}
        tools::TimeoutClamp::Clamped { asked, ceiling } => {
            output.put(b"\n[")?;
            output.put(label)?;
            output.put(b" clamped: ")?;
            output.number(asked.as_nanos() / 1_000_000)?;
            output.put(b" ms to ")?;
            output.number(ceiling.as_nanos() / 1_000_000)?;
            output.put(b" ms]")?;
        }
    }
    command_following(tools::TimeoutClamp::Within, kept, output)
}

fn command_following(timeout: tools::TimeoutClamp, kept: tools::BytesClamp, output: &mut Sink) -> Result<(), Error> {
    match timeout {
        tools::TimeoutClamp::Within => {}
        tools::TimeoutClamp::Clamped { asked, ceiling } => {
            output.put(b"\n[timeout clamped: ")?;
            output.number(asked.as_nanos() / 1_000_000)?;
            output.put(b" ms to ")?;
            output.number(ceiling.as_nanos() / 1_000_000)?;
            output.put(b" ms]")?;
        }
    }
    match kept {
        tools::BytesClamp::Within => {}
        tools::BytesClamp::Clamped { asked, ceiling } => {
            output.put(b"\n[output clamped: ")?;
            output.number(u64::from(asked))?;
            output.put(b" bytes to ")?;
            output.number(u64::from(ceiling))?;
            output.put(b" bytes]")?;
        }
    }
    Ok(())
}

/// Cut domain-written text without changing its UTF-8 or allocating its full size.
pub(crate) fn cut_text(text: Box<[u8]>, maximum: u32) -> Box<[u8]> {
    assert!(maximum >= CUT_MARKER_BYTES, "derived render bound holds the cut marker");
    if text.len() <= usize::try_from(maximum).expect("u32 fits usize") {
        return text;
    }
    let cap = u64::from(maximum.checked_sub(CUT_MARKER_BYTES).expect("marker room checked"));
    let mut measure = Sink { length: 0, kept: 0, maximum: cap, cut: false, writer: None };
    measure.put(&text).expect("owned text length fits u64");
    let omitted = measure.length.checked_sub(measure.kept).expect("prefix within full text");
    let length = usize::try_from(measure.kept)
        .expect("bounded prefix fits usize")
        .checked_add(Decimal::of(omitted).as_bytes().len())
        .expect("marker within derived cap")
        .checked_add(17)
        .expect("marker within derived cap");
    let mut output = Sink { length: 0, kept: 0, maximum: cap, cut: false, writer: Some(Writer::new(length)) };
    output.put(&text).expect("measured prefix room");
    let mut writer = output.writer.expect("writing pass owns its measured output");
    writer.put(b"\n[").expect("measured marker room");
    writer.put(Decimal::of(omitted).as_bytes()).expect("measured byte-count room");
    writer.put(b" bytes omitted]").expect("measured marker room");
    writer.finish()
}

/// Name the normalized descriptor without showing a single directory's mount.
fn resolved_path(path: &tools::Resolved, namespace: &tools::Namespace, output: &mut Sink) -> Result<(), Error> {
    match path {
        tools::Resolved::Working { path } => match namespace {
            tools::Namespace::NoWorkspace | tools::Namespace::One => {
                if path.is_empty() {
                    output.put(b".")
                } else {
                    output.text(path)
                }
            }
            tools::Namespace::Several { working } => named_path(working, path, output),
        },
        tools::Resolved::Other { directory, path } => named_path(directory, path, output),
        tools::Resolved::Outside { path } => {
            output.put(b"/")?;
            output.text(path)
        }
    }
}

fn named_path(directory: &tools::Name, path: &[u8], output: &mut Sink) -> Result<(), Error> {
    output.put(b"/")?;
    output.text(directory.as_bytes())?;
    if !path.is_empty() {
        output.put(b"/")?;
    }
    output.text(path)
}

fn size_refusal(what: tools::SizePart, size: u64, bound: u32, output: &mut Sink) -> Result<(), Error> {
    let prefix: &[u8] = match what {
        tools::SizePart::File => b"file too large: ",
        tools::SizePart::WriteContent => b"write content too large: ",
        tools::SizePart::EditOld => b"edit old snippets too large: ",
        tools::SizePart::EditNew => b"edit new snippets too large: ",
        tools::SizePart::EditItems => b"edit list too large: ",
        tools::SizePart::EditResult => b"edit result too large: ",
        tools::SizePart::EditReplacements => b"edit replacement count too large: ",
        tools::SizePart::ListGlob => b"list glob too large: ",
    };
    let unit: &[u8] = match what {
        tools::SizePart::EditItems => b" edits",
        tools::SizePart::EditReplacements => b" replacements",
        tools::SizePart::File
        | tools::SizePart::WriteContent
        | tools::SizePart::EditOld
        | tools::SizePart::EditNew
        | tools::SizePart::EditResult
        | tools::SizePart::ListGlob => b" bytes",
    };
    output.put(prefix)?;
    output.number(size)?;
    output.put(unit)?;
    output.put(b", bound ")?;
    output.number(u64::from(bound))?;
    match what {
        tools::SizePart::WriteContent => output.put(crate::failure::WRITE_SIZE_SUFFIX),
        tools::SizePart::File
        | tools::SizePart::EditOld
        | tools::SizePart::EditNew
        | tools::SizePart::EditItems
        | tools::SizePart::EditResult
        | tools::SizePart::EditReplacements
        | tools::SizePart::ListGlob => output.put(unit),
    }
}

// Measure the actual longest refusal at both integer maxima, using the same
// words as live and resumed rendering. No independent word-cap constant.
pub(crate) fn size_refusal_worst_case() -> Option<u64> {
    let mut largest = 0;
    for what in [
        tools::SizePart::File,
        tools::SizePart::WriteContent,
        tools::SizePart::EditOld,
        tools::SizePart::EditNew,
        tools::SizePart::EditItems,
        tools::SizePart::EditResult,
        tools::SizePart::EditReplacements,
        tools::SizePart::ListGlob,
    ] {
        let mut measure = Sink { length: 0, kept: 0, maximum: u64::MAX, cut: false, writer: None };
        size_refusal(what, u64::MAX, u32::MAX, &mut measure).ok()?;
        largest = largest.max(measure.length);
    }
    Some(largest)
}
