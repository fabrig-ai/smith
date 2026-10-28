//! Open prompt goldens supplied by the lifecycle design; Finite bounds still dominate.
#![expect(clippy::disallowed_methods, reason = "programming-model.md, section 10.2: prompt goldens use ordinary Rust")]

use super::*;
use crate::tests::charter;

#[test]
fn open_main_is_told_to_reply_without_a_result_contract() {
    let mut selected = charter();
    selected.mode = Mode::Open;
    selected.outcome.report = Some(TextSpec { max: 64, fields: Box::new([]) });
    selected.reserve = Reserve { turns: 2, time: Duration::from_secs(3) };
    let rendered = system(&selected, None, &Found::with_capacity(0), selected.budget, false);
    let rendered = core::str::from_utf8(&rendered).expect("prompt text");
    assert!(rendered.contains("Call `wait` when you need another message;"));
    assert_eq!(rendered.matches(core::str::from_utf8(REPLYING).expect("reply guide")).count(), 1);
    assert!(rendered.ends_with("Keep 2 turns and 3 seconds for a last reply. When the ordinary budget is spent, live children close and you give it then.\n"));
    for absent in ["## Finishing", "`finish`", "`deliver`", "## Delivery", "Report:"] {
        assert!(!rendered.contains(absent), "unoffered mechanism: {absent}");
    }
}

#[test]
fn open_last_reply_and_wind_down_use_the_approved_words() {
    assert_eq!(
        wind_down_note(Mode::Open, crate::wind_down::Cause::Budget(crate::Exhausted::Turns), Reserve { turns: 2, time: Duration::from_millis(750) }).as_ref(),
        b"The ordinary budget is spent. Reply now with what you have. You have at most 2 final completions and less than a second to reply. Use text only; promise no later work."
    );
    assert_eq!(last_reply().as_ref(), b"Reply now with what you have. Use text only; promise no later work.");
}

#[test]
fn open_nudges_say_why_without_naming_terminal_tools() {
    for (stop, reason) in [
        (Stop::EndTurn, "You ended your turn with no text, so you have not replied."),
        (Stop::MaxTokens, "Your answer was cut off: it ran out of tokens."),
        (Stop::Refusal, "You declined to go on."),
        (Stop::NoCalls, "You said you would call a tool, and named none."),
    ] {
        let expected = [reason, " Carry on, and reply with text or call `wait`. This is reminder 1 of 2: if you stop again without replying after the last one, the run ends unfinished."].concat();
        assert_eq!(nudge(Mode::Open, stop, 1, 2).as_ref(), expected.as_bytes());
    }
}

#[test]
fn open_mechanics_fit_the_existing_finite_measurement() {
    let time = Duration::from_nanos(u64::MAX);
    let budget = Budget { turns: u32::MAX, spend: u64::MAX, time };
    let reserve = Reserve { turns: u32::MAX, time };
    let mut finite = Text::measuring();
    let mut open = Text::measuring();
    render_budget(Mode::Finite, &mut finite, budget, reserve, true);
    render_budget(Mode::Open, &mut open, budget, reserve, true);
    assert!(open.len <= finite.len);
    assert!(
        wind_down_note(Mode::Open, crate::wind_down::Cause::Budget(crate::Exhausted::Turns), reserve).len()
            <= wind_down_note(Mode::Finite, crate::wind_down::Cause::Budget(crate::Exhausted::Turns), reserve).len()
    );
    assert!(
        wind_down_note(Mode::Open, crate::wind_down::Cause::Host, reserve).len()
            <= wind_down_note(Mode::Finite, crate::wind_down::Cause::Budget(crate::Exhausted::Turns), reserve).len()
    );
    assert!(last_reply().len() <= finish_now().len());
    let mut finishing = Text::measuring();
    render_finishing(
        &mut finishing,
        &OutcomeSpec { change: None, verdicts: Box::new([]), failure: None, report: None },
        false,
    );
    assert!(REPLYING.len() <= finishing.len);
}
