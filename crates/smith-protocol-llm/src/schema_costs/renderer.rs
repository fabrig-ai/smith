//! Test-only derivation from the production schema renderer.

use crate::{Error, deliver_schema, finish_schema};
use alloc::boxed::Box;
use skein_json::writer::{Encoder, Limits};
use smith_domain::run::{
    SchemaCosts,
    outcome::{ChangeSpec, FieldRule, ItemRule, ItemSpec, OutcomeSpec, TextSpec, VerdictRule},
};

/// Measure fixed and first/subsequent element costs, including all separators.
/// Numeric digits and escaped host text are summed separately by the run.
pub(super) fn measure() -> Result<SchemaCosts, Error> {
    let empty = OutcomeSpec { change: None, verdicts: Box::new([]), report: None, failure: None };
    let finish = length(&finish_schema(&empty, u32::MAX)?)?;
    let change = ChangeSpec { checks_must_pass: false, fields: Box::new([]) };
    let deliver = length(&deliver_schema(&change, u32::MAX)?)?;
    let mut one = empty.clone();
    one.change = Some(change.clone());
    let changed = length(&finish_schema(&one, u32::MAX)?)?.checked_sub(finish).ok_or(Error::Limit)?;
    let text = TextSpec { max: 0, fields: Box::new([]) };
    one = empty.clone();
    one.report = Some(text.clone());
    let reported = length(&finish_schema(&one, u32::MAX)?)?.checked_sub(finish).ok_or(Error::Limit)?;
    one.change = Some(change.clone());
    let form_join =
        difference(difference(difference(length(&finish_schema(&one, u32::MAX)?)?, finish)?, changed)?, reported)?;
    one = empty.clone();
    one.failure = Some(text);
    let failure = difference(difference(length(&finish_schema(&one, u32::MAX)?)?, finish)?, 1)?;
    let field = FieldRule { name: Box::new([]), max: 0 };
    let mut fields = change.clone();
    fields.fields = Box::new([field.clone()]);
    let first_field = length(&deliver_schema(&fields, u32::MAX)?)?;
    fields.fields = Box::new([field.clone(), field]);
    let next_field = length(&deliver_schema(&fields, u32::MAX)?)?;
    let verdict = VerdictRule {
        name: Box::new([]),
        text_max: 0,
        fields: Box::new([]),
        items: ItemSpec { min: 0, max: 0, kinds: Box::new([]) },
    };
    one = empty.clone();
    one.verdicts = Box::new([verdict.clone()]);
    let first_verdict = length(&finish_schema(&one, u32::MAX)?)?;
    one.verdicts = Box::new([verdict.clone(), verdict.clone()]);
    let next_verdict = length(&finish_schema(&one, u32::MAX)?)?;
    let kind = ItemRule { kind: Box::new([]), fields: Box::new([]) };
    let mut with_kind = verdict;
    with_kind.items.kinds = Box::new([kind.clone()]);
    one.verdicts = Box::new([with_kind.clone()]);
    let first_kind = length(&finish_schema(&one, u32::MAX)?)?;
    with_kind.items.kinds = Box::new([kind.clone(), kind]);
    one.verdicts = Box::new([with_kind]);
    let next_kind = length(&finish_schema(&one, u32::MAX)?)?;
    let [finish_text, deliver_text] = crate::tools::contract_text_bytes().ok_or(Error::Limit)?;
    Ok(SchemaCosts {
        finish_text,
        deliver_text,
        finish,
        deliver,
        change: changed,
        form_join,
        report: reported.checked_sub(1).ok_or(Error::Limit)?,
        failure,
        field_first: difference(difference(first_field, deliver)?, 1)?,
        field_next: difference(difference(next_field, first_field)?, 1)?,
        verdict_first: difference(difference(first_verdict, finish)?, 3)?,
        verdict_next: difference(difference(next_verdict, first_verdict)?, 3)?,
        kind_first: first_kind.checked_sub(first_verdict).ok_or(Error::Limit)?,
        kind_next: next_kind.checked_sub(first_kind).ok_or(Error::Limit)?,
        escaped: escaped()?,
    })
}

fn length(bytes: &[u8]) -> Result<u64, Error> {
    u64::try_from(bytes.len()).or(Err(Error::Limit))
}

fn escaped() -> Result<[u8; 256], Error> {
    let limits = Limits { depth: 1, length: 16 };
    let mut measure = Encoder::measure(&limits);
    measure.string("\u{10ffff}".as_bytes());
    // Non-ASCII bytes are from protocol-attested UTF-8 and remain literal.
    let literal = measured(measure)?.checked_sub(2).ok_or(Error::Limit)?.checked_div(4).ok_or(Error::Limit)?;
    let mut widths = [u8::try_from(literal).or(Err(Error::Limit))?; 256];
    for byte in 0_u8..=127 {
        let mut measure = Encoder::measure(&limits);
        measure.string(&[byte]);
        let width = measured(measure)?.checked_sub(2).ok_or(Error::Limit)?;
        *widths.get_mut(usize::from(byte)).expect("ASCII byte indexes the 256-entry width table") =
            u8::try_from(width).or(Err(Error::Limit))?;
    }
    Ok(widths)
}

fn measured(measure: Encoder) -> Result<u32, Error> {
    match measure.measured() {
        Ok(length) => Ok(length),
        Err(refusal) => Err(schema_error(refusal)),
    }
}

fn schema_error(refusal: skein_json::writer::Refusal) -> Error {
    match refusal {
        skein_json::writer::Refusal::Text | skein_json::writer::Refusal::Number => Error::Invalid,
        skein_json::writer::Refusal::TooLong | skein_json::writer::Refusal::TooDeep => Error::Limit,
    }
}

fn difference(left: u64, right: u64) -> Result<u64, Error> {
    left.checked_sub(right).ok_or(Error::Limit)
}
