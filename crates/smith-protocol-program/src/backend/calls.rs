//! Stream/MCP correlation and replies (protocol/programs.md, section 5.5).
//! Keeps the open/last message's unanswered calls, never guesses by RPC order.

use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_json::{Document, Kind};
use skein_lib::List;
use smith_domain::llm;

#[expect(
    clippy::struct_excessive_bools,
    reason = "answered, permission prompted, eligible and built-in are independent ledger facts"
)]
pub(super) struct Call {
    pub id: Box<[u8]>,
    pub name: Box<[u8]>,
    pub input: Box<[u8]>,
    pub rpc: Option<mcp::CallId>,
    pub answer: Option<mcp::Answer>,
    pub answered: bool,
    pub prompted: bool,
    pub eligible: bool,
    pub message: u64,
    pub builtin: bool,
    pub native_write: bool,
    pub owner: Option<skein_lib::Token>,
}

pub(super) fn find(calls: &List<Call>, name: &[u8], input: &[u8], depth: u32, tokens: u32) -> Option<u32> {
    let mut fallback = None;
    for (index, call) in calls.iter().enumerate() {
        if !call.builtin && call.eligible && call.name.as_ref() == name && call.rpc.is_none() && !call.answered {
            let index = u32::try_from(index).ok()?;
            if fallback.is_none() {
                fallback = Some(index);
            }
            if equal(&call.input, input, depth, tokens) {
                return Some(index);
            }
        }
    }
    fallback
}

pub(super) fn equal(left: &[u8], right: &[u8], depth: u32, tokens: u32) -> bool {
    equal_value(left, right, depth, tokens) == Some(())
}

fn equal_value(left: &[u8], right: &[u8], depth: u32, tokens: u32) -> Option<()> {
    let left = super::stream::collect(left, depth, tokens)?;
    let right = super::stream::collect(right, depth, tokens)?;
    let mut pending = List::with_capacity(tokens);
    pending.push((0_u32, 0_u32)).ok()?;
    for cursor in 0..tokens {
        let (a, b) = match pending.get(cursor) {
            Some(pair) => *pair,
            None => return Some(()),
        };
        let l = left.token(a)?;
        let r = right.token(b)?;
        if l.kind != r.kind {
            return None;
        }
        match l.kind {
            Kind::ObjectStart => {
                let mut index = a.checked_add(1)?;
                for _ in 0..left.len() {
                    let key = left.token(index)?;
                    if key.kind == Kind::ObjectEnd {
                        break;
                    }
                    let value = index.checked_add(1)?;
                    pending.push((value, json::field(&right, b, left.text(key)?)?)).ok()?;
                    index = json::after(&left, value)?;
                }
                if members(&left, a)? != members(&right, b)? {
                    return None;
                }
            }
            Kind::ArrayStart => {
                let mut ai = a.checked_add(1)?;
                let mut bi = b.checked_add(1)?;
                for _ in 0..left.len() {
                    if left.token(ai)?.kind == Kind::ArrayEnd {
                        break;
                    }
                    pending.push((ai, bi)).ok()?;
                    ai = json::after(&left, ai)?;
                    bi = json::after(&right, bi)?;
                }
                if right.token(bi)?.kind != Kind::ArrayEnd {
                    return None;
                }
            }
            Kind::Number => {
                if number(left.text(l)?)? != number(right.text(r)?)? {
                    return None;
                }
            }
            Kind::String | Kind::True | Kind::False | Kind::Null => {
                if left.text(l)? != right.text(r)? {
                    return None;
                }
            }
            Kind::ObjectEnd | Kind::ArrayEnd | Kind::Key | Kind::Long => return None,
        }
    }
    None
}

#[derive(PartialEq, Eq)]
struct Number {
    negative: bool,
    digits: [u8; 32],
    length: usize,
    exponent: i128,
}

// The stream's 32-byte numeral bound fits its decimal exponent in i128.
// Normalize decimal position and zeros without rounding through a float.
fn number(bytes: &[u8]) -> Option<Number> {
    if bytes.len() > 32 {
        return None;
    }
    let mut digits = [0; 32];
    let mut length = 0_usize;
    let mut fraction = 0_i128;
    let mut fractional = false;
    let mut reading_exponent = false;
    let mut exponent = 0_i128;
    let mut negative_exponent = false;
    let mut negative = false;
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'-' if index == 0 => negative = true,
            b'-' => negative_exponent = true,
            b'+' => {}
            b'.' => fractional = true,
            b'e' | b'E' => reading_exponent = true,
            b'0'..=b'9' => {
                if reading_exponent {
                    exponent = exponent.checked_mul(10)?.checked_add(i128::from(byte.checked_sub(b'0')?))?;
                } else {
                    *digits.get_mut(length)? = *byte;
                    length = length.checked_add(1)?;
                    if fractional {
                        fraction = fraction.checked_add(1)?;
                    }
                }
            }
            _ => return None,
        }
    }
    if negative_exponent {
        exponent = exponent.checked_neg()?;
    }
    exponent = exponent.checked_sub(fraction)?;
    let mut start = 0_usize;
    for byte in digits.get(..length)? {
        if *byte != b'0' {
            break;
        }
        start = start.checked_add(1)?;
    }
    if start == length {
        return Some(Number { negative: false, digits: [0; 32], length: 0, exponent: 0 });
    }
    let mut end = length;
    for index in (start..length).rev() {
        if *digits.get(index)? != b'0' {
            break;
        }
        end = end.checked_sub(1)?;
        exponent = exponent.checked_add(1)?;
    }
    let mut normalized = [0; 32];
    for (target, source) in normalized.iter_mut().zip(digits.get(start..end)?) {
        *target = *source;
    }
    Some(Number { negative, digits: normalized, length: end.checked_sub(start)?, exponent })
}

fn members(document: &Document, object: u32) -> Option<u32> {
    let mut next = object.checked_add(1)?;
    let mut count = 0_u32;
    for _ in 0..document.len() {
        if document.token(next)?.kind == Kind::ObjectEnd {
            return Some(count);
        }
        count = count.checked_add(1)?;
        next = json::after(document, next.checked_add(1)?)?;
    }
    None
}

pub(super) fn render(
    result: llm::Returned,
    prompt: &llm::Prompt,
    limits: &smith_protocol_llm::Limits,
) -> Option<mcp::Answer> {
    let (text, error) = match result {
        llm::Returned::Text { text, error, .. } => (text, error),
        llm::Returned::Served { returned, .. } => {
            let result = smith_domain::feedback(returned, u64::from(limits.rendered_result)).ok()?;
            (result.text, result.error)
        }
        result @ (llm::Returned::Owned { .. }
        | llm::Returned::Withdrawn
        | llm::Returned::Invalid { .. }
        | llm::Returned::NotRun { .. }) => return render_unserved(&result, prompt, limits),
    };
    if text.len() > usize::try_from(limits.rendered_result).ok()? {
        return None;
    }
    Some(if error { mcp::Answer::Error(text) } else { mcp::Answer::Text(text) })
}

pub(super) fn render_unserved(
    result: &llm::Returned,
    prompt: &llm::Prompt,
    limits: &smith_protocol_llm::Limits,
) -> Option<mcp::Answer> {
    let (text, error) = match result {
        llm::Returned::Text { text, error, .. } => (text.clone(), *error),
        llm::Returned::Owned { outcome } => {
            smith_protocol_llm::render_outcome(outcome, &prompt.namespace, limits.rendered_result).ok()?
        }
        llm::Returned::Served { .. } => unreachable!("served results are consumed before the tail passes"),
        llm::Returned::Withdrawn => (b"withdrawn".as_slice().into(), true),
        llm::Returned::Invalid { .. } => (b"invalid tool input".as_slice().into(), true),
        llm::Returned::NotRun { why } => match why {
            llm::NotRun::OutsideChoice { choice } => (outside_choice(choice.as_deref(), limits.rendered_result)?, true),
            llm::NotRun::Stopped | llm::NotRun::AfterFailedWrite { .. } => (b"not run".as_slice().into(), true),
        },
    };
    if text.len() > usize::try_from(limits.rendered_result).ok()? {
        return None;
    }
    Some(if error { mcp::Answer::Error(text) } else { mcp::Answer::Text(text) })
}

fn outside_choice(names: Option<&[Box<[u8]>]>, maximum: u32) -> Option<Box<[u8]>> {
    let mut text = List::with_capacity(maximum);
    for byte in b"not run: allowed calls now: " {
        text.push(*byte).ok()?;
    }
    match names {
        Some(names) => {
            for name in names {
                for byte in name {
                    text.push(*byte).ok()?;
                }
                text.push(b' ').ok()?;
            }
        }
        None => {
            for byte in b"none" {
                text.push(*byte).ok()?;
            }
        }
    }
    Some(text.into_boxed())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn calls() -> List<Call> {
        let mut calls = List::with_capacity(3);
        for (id, name, input) in [
            (b"a".as_slice(), b"first".as_slice(), b"{}".as_slice()),
            (b"b", b"second", br#"{"x":1,"nested":{"a":true,"b":[2,3]}}"#),
            (b"c", b"second", br#"{"x":2}"#),
        ] {
            assert!(
                calls
                    .push(Call {
                        id: id.into(),
                        name: name.into(),
                        input: input.into(),
                        rpc: None,
                        answer: None,
                        answered: false,
                        prompted: false,
                        eligible: true,
                        message: 1,
                        builtin: false,
                        native_write: false,
                        owner: None,
                    })
                    .is_ok()
            );
        }
        calls
    }
    #[test]
    fn matching_uses_names_before_position() {
        assert_eq!(find(&calls(), b"second", br#"{"x":2}"#, 32, 256), Some(2));
    }
    #[test]
    fn matching_compares_nested_json_without_object_key_order() {
        assert_eq!(find(&calls(), b"second", br#"{"nested":{"b":[2,3],"a":true},"x":1}"#, 32, 256), Some(1));
    }
    #[test]
    fn unmatched_input_uses_the_first_unanswered_name() {
        assert_eq!(find(&calls(), b"second", br#"{"x":9}"#, 32, 256), Some(1));
    }
    #[test]
    fn a_call_absent_from_the_message_is_unmatched() {
        assert_eq!(find(&calls(), b"absent", b"{}", 32, 256), None);
    }
    #[test]
    fn matching_compares_decimal_values_without_rounding() {
        assert_eq!(find(&calls(), b"second", br#"{"x":2.0e0}"#, 32, 256), Some(2));
        for (left, right) in [
            (b"-0.00".as_slice(), b"0".as_slice()),
            (b"1.2300e2", b"123"),
            (b"0.001", b"1e-3"),
            (b"10e100000000000000000000000000", b"1e100000000000000000000000001"),
        ] {
            assert!(equal(left, right, 32, 256));
        }
        assert!(!equal(b"1.01", b"1.0101", 32, 256));
    }
}
