//! Main receives literal host instructions; a fresh Open main receives its
//! ordered titled Brief as the first user message, a Finite main in system text;
//! a child receives only its caller's raw task (domain/run.md, sections 3.1,
//! 3.3, 3.4, 5.3 and 9.2). Both then receive selected guides, run mechanics and
//! budget prose fixed at their actual opening.
//! `opening` composes a fresh main brief or begin instruction and its bounded message offer;
//! with concrete history, it offers only messages or a saved-conversation continuation.
//! No prompt state is retained here; title/text meaning and authority are never
//! inferred. `system` and `child` consume only admitted immutable source data;
//! session receiving bytes still decide whether the rendered opening fits.
//!
//! A text is rendered twice: once to measure it, then into a [`Writer`] of
//! exactly that length (programming-model.md, section 8).

use alloc::boxed::Box;

use crate::budget::{Budget, Granted};
use crate::charter::{Mode, Reserve};
use skein_lib::{Duration, Writer};

use crate::boundary::Stop;
use crate::charter::{Charter, Families, Llm, Tools};
use crate::conventions;
use crate::outcome::{FieldRule, OutcomeSpec, TextSpec, VerdictRule};
use crate::prepare::{Found, Guide};
use crate::workspace::{self, Directory, Workspace};

/// The first user instruction of a Finite main or child conversation.
pub(crate) const BEGIN: &[u8] = b"Begin the work your brief describes.";

/// The summary keeps the work ready to continue in a new window.
pub(crate) const SUMMARY: &[u8] = b"Summarise the work so far. The work is not finished: a new context window will continue it, opening with the task's own words. Answer with text only; call no tool. Write what the next window needs that the task does not say: what was done and found, with exact values, names and paths as they were read; what remains; and the decisions taken. Do not restate the task or its required answer. Keep within 1,000 words.";

/// The text introducing a summary in the new window.
pub(crate) const PREAMBLE: &[u8] =
    b"What follows is your own summary of an earlier context window. The work continues in this window:";

/// The summary instruction's contribution to the compaction headroom.
#[must_use]
pub fn summary_bytes() -> u64 {
    u64::try_from(SUMMARY.len()).expect("fixed summary length fits u64")
}

/// The child's last prompt, independent of provider and host vocabulary.
pub(crate) const CHILD_FINAL: &[u8] =
    b"Your share is ending. This is your final turn. Answer now with what you have. No tool calls will run.";

pub(crate) fn child_final_note() -> Box<[u8]> {
    skein_lib::bytes::copy_of(CHILD_FINAL)
}

const REPLYING: &[u8] = b"## Replying\n\nEnd your turn with text to reply; a reply does not end the conversation. The run then waits as for `wait`, but with no call to answer: the next messages come as a user message, at once if any came while you worked. To wait without replying, call `wait` with no text.\n";

const WAIT: &[u8] = b"Call `wait` when you need another message; its result is withheld until messages arrive and returns with them in one request.\n\n";

/// Measure the largest fixed system mechanics and the opening instruction.
/// Charter forms, names, conventions, guides and their scaffolding are excluded.
/// Contract: protocol/limits.md, section 3.9.
#[must_use]
pub fn fixed_prompt_bytes() -> Option<u64> {
    let mut measured = Text::measuring();
    render_fixed(&mut measured);
    u64::try_from(measured.len).ok()
}

/// Render the largest fixed text for startup's provider framing measurement.
/// No charter text is received or retained by this pass.
#[must_use]
pub fn fixed_prompt() -> Box<[u8]> {
    let mut measured = Text::measuring();
    render_fixed(&mut measured);
    let mut written = measured.writing();
    render_fixed(&mut written);
    written.finish()
}

fn render_fixed(text: &mut Text) {
    render_tools(text, Tools { inspect: true, modify: true, shell: true });
    text.put(b"\n");
    render_checkout(text, &[], &[], b"");
    text.put(b"\n");
    render_agents(text, &[]);
    text.put(b"\n");
    text.put(WAIT);
    render_finishing(text, &OutcomeSpec { change: None, verdicts: Box::new([]), failure: None, report: None }, false);
    render_answering(text);
    text.put(&begin(false, false, None));
    text.put(b"\n\n");
    render_fixed_budget(text);
    render_wind_down(
        Mode::Finite,
        crate::wind_down::Cause::Budget(crate::Exhausted::Turns),
        text,
        Reserve { turns: u32::MAX, time: Duration::from_nanos(u64::MAX) },
    );
    text.put(&finish_now());
    text.put(&child_final_note());
    text.put(SUMMARY);
    text.put(PREAMBLE);
}

fn render_fixed_budget(text: &mut Text) {
    let time = Duration::from_nanos(u64::MAX);
    let budget = Budget { turns: u32::MAX, spend: u64::MAX, time };
    let reserve = Reserve { turns: u32::MAX, time };
    let granted =
        Granted { turns: u32::MAX, time, spend: u64::MAX, turns_set: crate::Set::Pool, time_set: crate::Set::Pool };
    let mut main = Text::measuring();
    render_budget(Mode::Finite, &mut main, budget, reserve, true);
    let mut child = Text::measuring();
    render_share(&mut child, granted, u32::MAX);
    if main.len >= child.len {
        render_budget(Mode::Finite, text, budget, reserve, true);
    } else {
        render_share(text, granted, u32::MAX);
    }
}

fn render_answering(text: &mut Text) {
    text.put(
        b"## Answering\n\nWhen you are done, end your turn with your answer. The run adds your usage to the result.\n",
    );
}

/// The finish result says why the run returns to its queued messages.
pub(crate) fn crossed() -> Box<[u8]> {
    skein_lib::bytes::copy_of(b"Finish was not accepted because messages arrived. Read them before finishing.")
}

/// The retained wait settles only when work has arrived.
pub(crate) fn waited() -> Box<[u8]> {
    skein_lib::bytes::copy_of(b"Messages arrived. Read them before continuing.")
}

/// A brief has work when any literal title or text is present.
pub(crate) fn has_brief(charter: &Charter) -> bool {
    for section in &charter.brief.sections {
        if !section.title.is_empty() || !section.text.is_empty() {
            return true;
        }
    }
    false
}

/// Open with the person's whole brief, or the unchanged Finite/history instruction.
pub(crate) fn opening(charter: &Charter, resumed: bool, messages: Option<Box<[u8]>>) -> Box<[u8]> {
    match charter.mode {
        Mode::Open if !resumed && !charter.brief.sections.is_empty() => {
            let mut measured = Text::measuring();
            render_brief(&mut measured, charter);
            if let Some(messages) = &messages {
                measured.put(messages);
            }
            let mut text = measured.writing();
            render_brief(&mut text, charter);
            if let Some(messages) = &messages {
                text.put(messages);
            }
            text.finish()
        }
        Mode::Finite | Mode::Open => begin(has_brief(charter), resumed, messages),
    }
}

fn begin(brief: bool, resumed: bool, messages: Option<Box<[u8]>>) -> Box<[u8]> {
    match messages {
        Some(messages) if resumed || !brief => messages,
        Some(messages) => {
            let length =
                BEGIN.len().checked_add(2).expect("fixed prefix").checked_add(messages.len()).expect("bounded offer");
            let mut writer = Writer::new(length);
            writer.put(BEGIN).expect("measured instruction");
            writer.put(b"\n\n").expect("measured separator");
            writer.put(&messages).expect("measured offer");
            writer.finish()
        }
        None if resumed => Box::from(b"Continue the saved conversation.".as_slice()),
        None if brief => Box::from(BEGIN),
        None => Box::from(b"Begin the work your instructions describe.".as_slice()),
    }
}

/// The first final completion receives this appended after the latest results.
pub(crate) fn wind_down_note(mode: Mode, cause: crate::wind_down::Cause, reserve: Reserve) -> Box<[u8]> {
    let mut measured = Text::measuring();
    render_wind_down(mode, cause, &mut measured, reserve);
    let mut text = measured.writing();
    render_wind_down(mode, cause, &mut text, reserve);
    text.finish()
}

/// A final completion that yields without finishing asks again within its cap.
pub(crate) fn finish_now() -> Box<[u8]> {
    skein_lib::bytes::copy_of(b"Finish now with what you have. Use finish, or deliver when offered.")
}

/// A final Open completion that has not replied asks again within its cap.
pub(crate) fn last_reply() -> Box<[u8]> {
    skein_lib::bytes::copy_of(b"Reply now with what you have. Use text only; promise no later work.")
}

fn render_wind_down(mode: Mode, cause: crate::wind_down::Cause, text: &mut Text, reserve: Reserve) {
    text.put(match cause {
        crate::wind_down::Cause::Budget(_) => b"The ordinary budget is spent. ",
        crate::wind_down::Cause::Host => b"The host requested wind-down. ",
    });
    text.put(match mode {
        Mode::Finite => b"Finish now with what you have. You have at most ",
        Mode::Open => b"Reply now with what you have. You have at most ",
    });
    text.put_decimal(reserve.turns);
    text.put(b" final completions and ");
    render_duration(text, reserve.time);
    text.put(match mode {
        Mode::Finite => b" to finish. Use only finish, or deliver when offered.",
        Mode::Open => b" to reply. Use text only; promise no later work.",
    });
}

/// Count every charter-dependent system byte, with the selected guide bound.
/// This pass renders into a counter and retains no text or filesystem state.
pub(crate) fn host_bytes(charter: &Charter, mounted: Option<&Workspace>, guide_bytes: u32) -> Option<u64> {
    let repositories = workspace::directories(mounted);
    let mut found = Found::with_capacity(u32::try_from(repositories.len()).ok()?);
    for (index, directory) in repositories.iter().enumerate() {
        if directory.writable {
            found.checks.push(u32::try_from(index).ok()?).expect("one check per directory");
        }
    }
    let mut text = Text::measuring();
    // The brief consumes the same host part regardless of its message role.
    render_main_prefix(&mut text, charter, true);
    render_system(&mut text, charter, mounted, &found, Families::of(&charter.grants), true);
    for directory in repositories {
        let name = if repositories.len() == 1 { None } else { Some(directory.name.as_ref()) };
        count_guide(&mut text, conventions::guide(charter), name, guide_bytes)?;
    }
    text.host_bytes
}

/// Count the bounded shell role and directory guides with their default rendering.
#[must_use]
pub fn role_host_bytes(role_bound: u32, directories: u32, directory_name_bytes: u32, guide_bytes: u32) -> Option<u64> {
    let mut role = Text::measuring();
    role.host = true;
    if role_bound > 0 {
        role.add_host(u64::from(role_bound));
        end_paragraph(&mut role, b"x");
    }
    let mut guide = Text::measuring();
    let name = if directories == 1 { None } else { Some(b"".as_slice()) };
    count_guide(&mut guide, conventions::GUIDE, name, guide_bytes)?;
    if directories > 1 {
        guide.add_host(u64::from(directory_name_bytes));
    }
    role.host_bytes?.checked_add(guide.host_bytes?.checked_mul(u64::from(directories))?)
}

#[cfg(test)]
mod role_tests;

/// Count the caller brief and the charter-dependent common child system text.
pub(crate) fn child_host_bytes(
    charter: &Charter,
    mounted: Option<&Workspace>,
    found: &Found,
    brief: &[u8],
    families: Families,
) -> Option<u64> {
    let mut text = Text::measuring();
    render_child_prefix(&mut text, brief);
    render_system(&mut text, charter, mounted, found, families, false);
    text.host_bytes
}

/// The system text of a run's main conversation, given what the run found in
/// its checkout.
pub(crate) fn system(
    charter: &Charter,
    mounted: Option<&Workspace>,
    found: &Found,
    budget: Budget,
    resumed: bool,
) -> Box<[u8]> {
    let families = workspace::families(mounted, Families::of(&charter.grants));
    let brief = match charter.mode {
        Mode::Finite => true,
        Mode::Open => resumed,
    };
    let mut measured = Text::measuring();
    render_main_prefix(&mut measured, charter, brief);
    render_system(&mut measured, charter, mounted, found, families, true);
    render_budget(charter.mode, &mut measured, budget, charter.reserve, families.agents);
    let mut text = measured.writing();
    render_main_prefix(&mut text, charter, brief);
    render_system(&mut text, charter, mounted, found, families, true);
    render_budget(charter.mode, &mut text, budget, charter.reserve, families.agents);
    text.finish()
}

/// The system text of a sub-agent asked for with `brief` and `families`.
pub(crate) fn child(
    charter: &Charter,
    mounted: Option<&Workspace>,
    found: &Found,
    brief: &[u8],
    families: Families,
    granted: Granted,
    answer_bytes: u32,
) -> Box<[u8]> {
    let mut measured = Text::measuring();
    render_child_prefix(&mut measured, brief);
    render_system(&mut measured, charter, mounted, found, families, false);
    render_share(&mut measured, granted, answer_bytes);
    let mut text = measured.writing();
    render_child_prefix(&mut text, brief);
    render_system(&mut text, charter, mounted, found, families, false);
    render_share(&mut text, granted, answer_bytes);
    text.finish()
}

/// Maximum new immutable budget/share prose, measured at scalar maxima without allocation.
/// The profile adds this to its other fixed prompt bytes (domain/run.md, section 9.2).
#[must_use]
pub fn budget_prompt_bytes() -> Option<u64> {
    let mut main = Text::measuring();
    let time = Duration::from_nanos(u64::MAX);
    render_budget(
        Mode::Finite,
        &mut main,
        Budget { turns: u32::MAX, spend: u64::MAX, time },
        Reserve { turns: u32::MAX, time },
        true,
    );
    let mut child = Text::measuring();
    render_share(
        &mut child,
        Granted { turns: u32::MAX, time, spend: u64::MAX, turns_set: crate::Set::Pool, time_set: crate::Set::Pool },
        u32::MAX,
    );
    u64::try_from(main.len.max(child.len)).ok()
}

fn render_budget(mode: Mode, text: &mut Text, budget: Budget, reserve: Reserve, agents: bool) {
    text.put(b"\n## Budget\n\nAt open, you have ");
    text.put_decimal(budget.turns);
    text.put(b" turns and ");
    render_duration(text, budget.time);
    text.put(b" of wall time. Spend is bounded too.\nKeep ");
    text.put_decimal(reserve.turns);
    text.put(b" turns and ");
    render_duration(text, reserve.time);
    text.put(match mode {
        Mode::Finite => {
            b" to finish. When the ordinary budget is spent, live children close and you finish on that reserve.\n"
        }
        Mode::Open => {
            b" for a last reply. When the ordinary budget is spent, live children close and you give it then.\n"
        }
    });
    if agents {
        text.put(b"Sub-agents share only what is above the reserve. Ask for focused turn or time shares; leaving a part unasked gives the child all of that part left.\n");
    }
}

fn render_share(text: &mut Text, granted: Granted, answer_bytes: u32) {
    text.put(b"\n## Your share\n\nAt open, your share is ");
    text.put_decimal(granted.turns);
    text.put(b" turns and ");
    render_duration(text, granted.time);
    text.put(b". Your answer may hold at most ");
    text.put_decimal(answer_bytes);
    text.put(
        b" bytes; anything past it is cut with a marker. Your last turn has no tools: answer with what you have.\n",
    );
}

fn render_duration(text: &mut Text, duration: Duration) {
    let seconds = duration.as_nanos().checked_div(1_000_000_000).expect("positive seconds divisor");
    if seconds == 0 && duration != Duration::ZERO {
        text.put(b"less than a second");
    } else {
        text.put_count(seconds);
        match seconds {
            1 => text.put(b" second"),
            _ => text.put(b" seconds"),
        }
    }
}

/// What a run says to an LLM that stopped for `stop` without finishing, in its
/// nudge numbered `nudge` of `nudges`.
pub(crate) fn nudge(mode: Mode, stop: Stop, nudge: u32, nudges: u32) -> Box<[u8]> {
    let mut measured = Text::measuring();
    render_nudge(&mut measured, mode, stop, nudge, nudges);
    let mut text = measured.writing();
    render_nudge(&mut text, mode, stop, nudge, nudges);
    text.finish()
}

/// Main's immutable literal instructions, with the brief in Finite or restored system text.
fn render_main_prefix(text: &mut Text, charter: &Charter, brief: bool) {
    text.host = true;
    if !charter.instructions.is_empty() {
        text.put(&charter.instructions);
        end_paragraph(text, &charter.instructions);
    }
    if brief {
        render_brief(text, charter);
    }
    text.host = false;
}

/// Each section keeps its literal title/text and existing paragraph termination.
/// The delimiter bound is seven bytes per section, in either message role.
fn render_brief(text: &mut Text, charter: &Charter) {
    for section in &charter.brief.sections {
        text.put(b"## ");
        text.put(&section.title);
        text.put(b"\n\n");
        text.put(&section.text);
        end_paragraph(text, &section.text);
    }
}

/// A child's caller task, with no parent instructions or structured context.
fn render_child_prefix(text: &mut Text, brief: &[u8]) {
    text.host = true;
    if !brief.is_empty() {
        text.put(brief);
        end_paragraph(text, brief);
    }
    text.host = false;
}

/// Shared guides/mechanics after the distinct main or raw child prefix.
fn render_system(
    text: &mut Text,
    charter: &Charter,
    mounted: Option<&Workspace>,
    found: &Found,
    families: Families,
    main: bool,
) {
    let repositories = workspace::directories(mounted);
    let families = workspace::families(mounted, families);
    for guide in &found.guides {
        render_guide(text, repositories, guide, conventions::guide(charter));
    }
    render_tools(text, families.tools);
    text.put(b"\n");
    render_checkout(text, repositories, found.checks.as_slice(), conventions::checks(charter));
    text.put(b"\n");
    if families.agents {
        render_agents(text, &charter.models);
        text.put(b"\n");
    }
    if main {
        if charter.grants.wait {
            text.put(WAIT);
        }
        match charter.mode {
            Mode::Finite => render_finishing(text, &charter.outcome, !found.checks.is_empty()),
            Mode::Open => text.put(REPLYING),
        }
        if let Some(spec) = &charter.grants.deliver {
            text.host = true;
            text.put(b"\n## Delivery\n\nYou may call `deliver` during the run, then continue. Its opaque fields are: ");
            render_fields(text, &spec.fields);
            if !found.checks.is_empty() {
                match &charter.outcome.change {
                    Some(change) if !change.checks_must_pass => {
                        text.put(
                            b". Discovered writable checks run before delivery; a failing check does not block it.",
                        );
                    }
                    Some(_) | None => {
                        text.put(b". Every discovered writable check must pass the exclusive snapshot.");
                    }
                }
            }
            text.put(b" The host supplies receipts, nothing, named refusal, bounded failure or stale.\n");
            text.host = false;
        }
    } else {
        render_answering(text);
    }
}

fn render_agents(text: &mut Text, models: &[Llm]) {
    text.put(b"## Sub-agents\n\n");
    text.put(
        b"You can ask for a sub-agent: an LLM of its own, working on a brief you write, with tools no wider than ",
    );
    text.put(b"yours and a share of the ordinary budget above the reserve. Its last message comes back with usage.\n");
    text.put(b"A child starts cold: give it a focused brief naming paths, decisions and scope. Inspect-only children may work side by side; a writing child works alone.\nIt runs on the ");
    if models.is_empty() {
        text.put(b"run's main LLM.\n");
        return;
    }
    text.host = true;
    text.put(b"run's main LLM unless you name one of these: ");
    let mut rest = models.len();
    for llm in models {
        text.put(b"`");
        text.put(&llm.model);
        text.put(b"`");
        rest = rest.saturating_sub(1);
        match rest {
            0 => {}
            1 => text.put(b" or "),
            _ => text.put(b", "),
        }
    }
    text.put(b".\n");
    text.host = false;
}

/// One blank line after a text that came as it is, whether or not it ends
/// its last line.
fn end_paragraph(text: &mut Text, came: &[u8]) {
    if !came.ends_with(b"\n") {
        text.put(b"\n");
    }
    text.put(b"\n");
}

fn render_guide(text: &mut Text, repositories: &[Directory], guide: &Guide, guide_path: &[u8]) {
    let directory =
        if repositories.len() == 1 { None } else { Some(name(repositories, guide.repository).name.as_ref()) };
    render_guide_parts(text, guide_path, directory, &guide.text, guide.whole);
}

fn render_guide_parts(text: &mut Text, guide_path: &[u8], directory: Option<&[u8]>, content: &[u8], whole: bool) {
    text.host = true;
    text.put(b"## ");
    text.put(guide_path);
    match directory {
        None => text.put(b" in the working directory\n\n"),
        Some(name) => {
            text.put(b" in `");
            text.put(name);
            text.put(b"`\n\n");
        }
    }
    if !content.is_empty() {
        text.put(content);
        end_paragraph(text, content);
    }
    if !whole {
        text.put(b"(The file goes on: read the rest with your tools.)\n\n");
    }
    text.host = false;
}

fn count_guide(text: &mut Text, guide_path: &[u8], directory: Option<&[u8]>, guide_bytes: u32) -> Option<()> {
    render_guide_parts(text, guide_path, directory, &[], false);
    if guide_bytes > 0 {
        text.add_host(u64::from(guide_bytes).checked_add(2)?);
    }
    Some(())
}

fn render_tools(text: &mut Text, tools: Tools) {
    let Tools { inspect, modify, shell } = tools;
    text.put(b"## Tools\n\n");
    if inspect {
        text.put(b"You can read, list and search the files in the checkout.\n");
    }
    if modify {
        text.put(b"You can write and edit files in its writable repositories.\n");
    }
    if shell {
        text.put(b"You can run shell commands.\n");
    }
    if !inspect && !modify && !shell {
        text.put(b"You have no tools that act on the checkout.\n");
    }
}

fn render_checkout(text: &mut Text, repositories: &[Directory], checks: &[u32], check_path: &[u8]) {
    text.put(b"## Checkout\n\n");
    if repositories.is_empty() {
        text.put(b"There is no checkout.\n");
    }
    text.host = true;
    match repositories.len() {
        0 => {}
        1 => text.put(b"Paths start in your working directory. Use `src/lib.rs`, without a directory-name prefix.\n"),
        _ => {
            text.put(b"Your working directory is `");
            text.put(&repositories.first().expect("several directories").name);
            text.put(b"`. Relative paths start there; reach the other directories as `/name/...`. Use `list /` to list their names.\n");
        }
    }
    let mut index: u32 = 0;
    for Directory { name, host: _, root: _, writable, git, conflicts } in repositories {
        if repositories.len() == 1 {
            text.put(b"- The working directory");
        } else {
            text.put(b"- `");
            text.put(name);
            text.put(b"`");
        }
        text.put(if *writable { b", which you may change" } else { b", which you may only read" });
        text.put(if *git { b", a git working tree" } else { b", a plain directory" });
        if !conflicts.is_empty() {
            text.put(b", with initial merge conflicts: ");
            for path in conflicts {
                text.put(b"`");
                text.put(path);
                text.put(b"` ");
            }
        }
        if checks.contains(&index) {
            text.put(b", with checks (`");
            text.put(check_path);
            text.put(b"`)");
        }
        text.put(b"\n");
        index = index.saturating_add(1);
    }
    text.host = false;
}

fn render_finishing(text: &mut Text, spec: &OutcomeSpec, checks: bool) {
    text.put(b"## Finishing\n\n");
    text.put(b"When the work is done, call `finish` with its result. A result outside the host's contract returns ");
    text.put(b"typed feedback; fix it and call `finish` again. Stopping without `finish` does not finish the run.\n");
    text.host = true;
    if let Some(rule) = &spec.change {
        text.put(b"\nChange: the workspace changes, with these host-required result fields:\n");
        render_fields(text, &rule.fields);
        if checks {
            if rule.checks_must_pass {
                text.put(b"Checks run before the host receives the change. A failed check returns its output so you can fix it.\n");
            } else {
                text.put(b"Checks run before the host receives the change. A failed check does not block delivery.\n");
            }
        }
        text.put(b"The host pushes the checked state. A moved target ends this run; other refusals are feedback.\n");
    }
    if let Some(rule) = &spec.report {
        render_text_rule(text, b"Report", rule);
    }
    if let Some(rule) = &spec.failure {
        render_text_rule(text, b"Declared failure", rule);
    }
    if !spec.verdicts.is_empty() {
        text.put(b"\nVerdict: one exact host label from this closed list:\n");
        for rule in &spec.verdicts {
            render_verdict(text, rule);
        }
    }
    text.host = false;
    text.put(b"Extra fields are allowed within the aggregate result byte limit; no field name may repeat.\n");
}

fn render_text_rule(text: &mut Text, form: &[u8], rule: &TextSpec) {
    text.put(b"\n");
    text.put(form);
    text.put(b": text up to ");
    text.put_decimal(rule.max);
    text.put(b" bytes, with these host-required fields:\n");
    render_fields(text, &rule.fields);
}

fn render_fields(text: &mut Text, fields: &[FieldRule]) {
    if fields.is_empty() {
        text.put(b"(none)\n");
    }
    for field in fields {
        text.put(b"- `");
        text.put(&field.name);
        text.put(b"`: nonempty, at most ");
        text.put_decimal(field.max);
        text.put(b" bytes\n");
    }
}

fn name(repositories: &[Directory], index: u32) -> &Directory {
    let index = usize::try_from(index).expect("a u32 fits in a usize");
    repositories.get(index).expect("a guide is of a repository of the checkout")
}

/// A verdict and its contract, as one item of a list.
fn render_verdict(text: &mut Text, rule: &VerdictRule) {
    text.put(b"- `");
    text.put(&rule.name);
    text.put(b"`: text at most ");
    text.put_decimal(rule.text_max);
    text.put(b" bytes; required result fields:\n");
    render_fields(text, &rule.fields);
    text.put(b"Items: ");
    text.put_decimal(rule.items.min);
    text.put(b" to ");
    text.put_decimal(rule.items.max);
    text.put(b"; allowed kinds and their required fields:\n");
    for kind in &rule.items.kinds {
        text.put(b"`");
        text.put(&kind.kind);
        text.put(b"`:\n");
        render_fields(text, &kind.fields);
    }
}

fn render_nudge(text: &mut Text, mode: Mode, stop: Stop, nudge: u32, nudges: u32) {
    text.put(match stop {
        Stop::Budget => unreachable!("only main is nudged, and it has no session final turn"),
        Stop::EndTurn => match mode {
            Mode::Finite => b"You stopped without calling `finish`, so the work is not done.",
            Mode::Open => b"You ended your turn with no text, so you have not replied.",
        },
        Stop::MaxTokens => b"Your answer was cut off: it ran out of tokens.",
        Stop::Refusal => b"You declined to go on.",
        Stop::NoCalls => b"You said you would call a tool, and named none.",
    });
    text.put(match mode {
        Mode::Finite => b" Carry on with the work, and call `finish` when it is done. This is reminder ",
        Mode::Open => b" Carry on, and reply with text or call `wait`. This is reminder ",
    });
    text.put_decimal(nudge);
    text.put(b" of ");
    text.put_decimal(nudges);
    text.put(match mode {
        Mode::Finite => b": if you stop again after the last one, the run ends unfinished.",
        Mode::Open => b": if you stop again without replying after the last one, the run ends unfinished.",
    });
}

/// A text being rendered: measured first, then written into a box of exactly
/// the length measured.
struct Text {
    /// The bytes put so far.
    len: usize,
    /// Where they go, once the text has been measured.
    writer: Option<Writer>,
    host: bool,
    host_bytes: Option<u64>,
}

impl Text {
    fn measuring() -> Text {
        Text { len: 0, writer: None, host: false, host_bytes: Some(0) }
    }

    /// A text to write what this one measured.
    fn writing(self) -> Text {
        Text { len: 0, writer: Some(Writer::new(self.len)), host: false, host_bytes: Some(0) }
    }

    fn put(&mut self, bytes: &[u8]) {
        self.len = self.len.saturating_add(bytes.len());
        if self.host {
            self.add_host(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        }
        if let Some(writer) = &mut self.writer {
            writer.put(bytes).expect("a text is written as it was measured");
        }
    }

    fn add_host(&mut self, bytes: u64) {
        self.host_bytes = match self.host_bytes {
            Some(current) => current.checked_add(bytes),
            None => None,
        };
    }

    /// `n` in decimal digits.
    fn put_decimal(&mut self, n: u32) {
        self.put_count(u64::from(n));
    }

    fn put_count(&mut self, n: u64) {
        self.put(skein_lib::Decimal::of(n).as_bytes());
    }

    fn finish(self) -> Box<[u8]> {
        self.writer.expect("a text is finished once written").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{Text, child, nudge, system};
    use crate::boundary::Stop;
    use crate::charter::{Charter, Families, Grants, Mode, Tools};
    use crate::outcome::{ChangeSpec, FieldRule, ItemRule, ItemSpec, OutcomeSpec, TextSpec, VerdictRule};
    use crate::prepare::{Found, Guide};
    use crate::tests::{bytes, charter, workspace};
    use alloc::boxed::Box;

    #[expect(clippy::disallowed_methods, reason = "a test reads the exact fixture text it checks")]
    fn text(bytes: &[u8]) -> &str {
        core::str::from_utf8(bytes).expect("fixture text")
    }

    fn found() -> Found {
        let mut found = Found::with_capacity(2);
        found
            .guides
            .push(Guide { repository: 0, text: bytes(b"Run `make test` before you finish."), whole: false })
            .expect("room");
        found.checks.push(1).expect("room");
        found
    }

    fn share(nanos: u64) -> crate::Granted {
        crate::Granted {
            turns: 3,
            time: skein_lib::Duration::from_nanos(nanos),
            spend: 91,
            turns_set: crate::Set::Asked,
            time_set: crate::Set::Asked,
        }
    }

    fn fields(name: &[u8], max: u32) -> Box<[FieldRule]> {
        Box::new([FieldRule { name: bytes(name), max }])
    }

    #[test]
    fn host_names_and_individual_caps_are_rendered_without_builtin_change_fields() {
        let charter = Charter {
            outcome: OutcomeSpec {
                change: Some(ChangeSpec { checks_must_pass: true, fields: fields(b"ticket", 17) }),
                report: Some(TextSpec { max: 32, fields: fields(b"source", 9) }),
                failure: Some(TextSpec { max: 24, fields: Box::new([]) }),
                verdicts: Box::new([VerdictRule {
                    name: bytes(b"triaged"),
                    text_max: 11,
                    fields: fields(b"owner", 6),
                    items: ItemSpec {
                        min: 1,
                        max: 2,
                        kinds: Box::new([
                            ItemRule { kind: bytes(b"risk"), fields: fields(b"severity", 4) },
                            ItemRule { kind: bytes(b"lead"), fields: fields(b"url", 19) },
                        ]),
                    },
                }]),
            },
            ..charter()
        };
        let rendered = system(&charter, Some(&workspace()), &found(), charter.budget, false);
        let rendered = text(&rendered);
        let brief = rendered.find("Review the change.").expect("brief");
        let guide = rendered.find("## AGENTS.md").expect("guide");
        let finish = rendered.find("## Finishing").expect("finish");
        assert!(brief < guide && guide < finish, "brief, guides then mechanics");
        for fragment in [
            "`ticket`: nonempty, at most 17 bytes",
            "Report: text up to 32 bytes",
            "`source`: nonempty, at most 9 bytes",
            "Declared failure: text up to 24 bytes",
            "`triaged`: text at most 11 bytes",
            "`owner`: nonempty, at most 6 bytes",
            "Items: 1 to 2",
            "`risk`:\n- `severity`",
            "`lead`:\n- `url`",
        ] {
            assert!(rendered.contains(fragment), "missing {fragment}: {rendered}");
        }
        assert!(
            !rendered.contains("pull request") && !rendered.contains("`title`") && !rendered.contains("`body`"),
            "host names only"
        );
    }

    #[test]
    fn optional_checks_are_described_for_final_and_mid_run_delivery() {
        let mut charter = charter();
        charter.outcome.change = Some(ChangeSpec { checks_must_pass: false, fields: Box::new([]) });
        charter.grants.deliver = Some(ChangeSpec { checks_must_pass: true, fields: Box::new([]) });
        let rendered = system(&charter, Some(&workspace()), &found(), charter.budget, false);
        assert!(skein_lib::bytes::find(&rendered, b"A failed check does not block delivery.").is_some());
        assert!(skein_lib::bytes::find(&rendered, b"a failing check does not block it.").is_some());
    }

    #[test]
    fn absent_workspace_and_empty_field_rules_are_said_plainly() {
        let charter = Charter {
            grants: Grants {
                wait: true,
                deliver: None,
                tools: Tools { inspect: false, modify: false, shell: false },
                ..charter().grants
            },
            outcome: OutcomeSpec {
                change: None,
                verdicts: Box::new([]),
                report: Some(TextSpec { max: 16, fields: Box::new([]) }),
                failure: None,
            },
            ..charter()
        };
        let rendered = system(&charter, None, &Found::with_capacity(0), charter.budget, false);
        let rendered = text(&rendered);
        assert!(rendered.contains("There is no checkout."));
        assert!(rendered.contains("Report: text up to 16 bytes, with these host-required fields:\n(none)"));
        assert!(!rendered.contains("Change:") && !rendered.contains("Verdict:"));
    }

    #[test]
    fn a_sub_agent_is_told_its_brief_its_tools_its_checkout_and_how_to_answer() {
        let mut charter = charter();
        charter.models =
            Box::new([crate::charter::Llm { effort: None, model: bytes(b"model-b"), ..charter.llm.clone() }]);
        let families = Families { tools: Tools { inspect: true, modify: false, shell: false }, agents: false };
        let expected: &[u8] = b"Find where tabs are parsed.

## Tools

You can read, list and search the files in the checkout.

## Checkout

Paths start in your working directory. Use `src/lib.rs`, without a directory-name prefix.
- The working directory, which you may only read, a git working tree

## Answering

When you are done, end your turn with your answer. The run adds your usage to the result.

## Your share

At open, your share is 3 turns and less than a second. Your answer may hold at most 8192 bytes; anything past it is cut with a marker. Your last turn has no tools: answer with what you have.
";
        let found = Found::with_capacity(1);
        assert_eq!(
            text(&child(
                &charter,
                Some(&workspace()),
                &found,
                b"Find where tabs are parsed.",
                families,
                share(750_000_000),
                8192
            )),
            text(expected)
        );
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "test-only literal golden fragments are joined without domain rendering"
    )]
    fn budget_text_has_exact_open_values_with_and_without_children() {
        for agents in [false, true] {
            let mut policy = charter();
            policy.grants.agents = agents;
            policy.reserve = crate::charter::Reserve { turns: 2, time: skein_lib::Duration::from_nanos(750_000_000) };
            let budget = crate::Budget { turns: 7, time: skein_lib::Duration::from_nanos(1_999_000_000), spend: 123 };
            let rendered = system(&policy, None, &Found::with_capacity(0), budget, false);
            let expected = "\n## Budget\n\nAt open, you have 7 turns and 1 second of wall time. Spend is bounded too.\nKeep 2 turns and less than a second to finish. When the ordinary budget is spent, live children close and you finish on that reserve.\n";
            let delegation = "Sub-agents share only what is above the reserve. Ask for focused turn or time shares; leaving a part unasked gives the child all of that part left.\n";
            let expected = if agents { [expected, delegation].concat() } else { expected.into() };
            assert!(text(&rendered).ends_with(&expected));
            assert_eq!(text(&rendered).matches("## Budget").count(), 1);
            assert_eq!(text(&rendered).contains("## Sub-agents"), agents);
            if agents {
                assert!(
                    text(&rendered)
                        .contains("A child starts cold: give it a focused brief naming paths, decisions and scope.")
                );
                assert!(
                    text(&rendered)
                        .contains("Inspect-only children may work side by side; a writing child works alone.")
                );
            }
        }
    }

    #[test]
    #[expect(
        clippy::disallowed_methods,
        reason = "test-only literal golden fragments are joined without domain rendering"
    )]
    fn child_seconds_round_down_and_fixed_prose_measurement_covers_maxima() {
        for (nanos, seconds) in [(750_000_000_u64, "less than a second"), (1_999_000_000, "1 second")] {
            let rendered = child(
                &charter(),
                None,
                &Found::with_capacity(0),
                b"task",
                Families { tools: Tools { inspect: false, modify: false, shell: false }, agents: false },
                share(nanos),
                8192,
            );
            let expected = ["\n## Your share\n\nAt open, your share is 3 turns and ", seconds, ". Your answer may hold at most 8192 bytes; anything past it is cut with a marker. Your last turn has no tools: answer with what you have.\n"].concat();
            assert!(text(&rendered).ends_with(&expected));
            assert!(!text(&rendered).contains("spend"));
            assert!(
                u64::try_from(expected.len()).expect("small prose")
                    <= super::budget_prompt_bytes().expect("scalar bound")
            );
        }
        let mut measured = Text::measuring();
        super::render_budget(
            Mode::Finite,
            &mut measured,
            crate::Budget { turns: u32::MAX, time: skein_lib::Duration::from_nanos(u64::MAX), spend: u64::MAX },
            crate::charter::Reserve { turns: u32::MAX, time: skein_lib::Duration::from_nanos(u64::MAX) },
            true,
        );
        assert_eq!(u64::try_from(measured.len).ok(), super::budget_prompt_bytes());
    }

    #[test]
    fn main_literal_prefix_preserves_host_order_duplicates_and_empty_payloads() {
        let selected = Charter {
            instructions: bytes(b"literal role\n"),
            brief: crate::Brief {
                sections: Box::new([
                    crate::Section { title: bytes(b"Repeated"), text: bytes(b"parent first body") },
                    crate::Section { title: Box::new([]), text: Box::new([]) },
                    crate::Section { title: bytes(b"Repeated"), text: bytes(b"parent last body\n") },
                ]),
            },
            ..charter()
        };
        let main = system(&selected, None, &Found::with_capacity(0), selected.budget, false);
        let prefix = b"literal role\n\n## Repeated\n\nparent first body\n\n## \n\n\n\n## Repeated\n\nparent last body\n\n## Tools\n\n";
        assert!(main.starts_with(prefix), "host bytes and exact paragraph delimiters: {}", text(&main));
        let child = child(
            &selected,
            None,
            &Found::with_capacity(0),
            b"own task\n",
            Families { tools: Tools { inspect: false, modify: false, shell: false }, agents: false },
            share(1_999_000_000),
            8192,
        );
        assert!(child.starts_with(b"own task\n\n## Tools\n\n"));
        for inherited in [b"literal role".as_slice(), b"Repeated", b"parent first body", b"parent last body"] {
            for window in child.windows(inherited.len()) {
                assert_ne!(window, inherited, "child scope excludes parent text");
            }
        }
    }

    #[test]
    fn empty_role_and_brief_emit_no_prefix_but_empty_sections_are_still_rendered() {
        let selected =
            Charter { instructions: Box::new([]), brief: crate::Brief { sections: Box::new([]) }, ..charter() };
        assert!(system(&selected, None, &Found::with_capacity(0), selected.budget, false).starts_with(b"## Tools\n\n"));
        let selected = Charter {
            instructions: bytes(b"role without LF"),
            brief: crate::Brief { sections: Box::new([crate::Section { title: Box::new([]), text: Box::new([]) }]) },
            ..selected
        };
        assert!(
            system(&selected, None, &Found::with_capacity(0), selected.budget, false)
                .starts_with(b"role without LF\n\n## \n\n\n\n## Tools\n\n")
        );
    }

    #[test]
    fn a_nudge_says_why_the_llm_stopped_and_how_many_nudges_are_left() {
        let rest = " Carry on with the work, and call `finish` when it is done. This is reminder 1 of 2: if you stop \
                    again after the last one, the run ends unfinished.";
        let cases = [
            (Stop::EndTurn, "You stopped without calling `finish`, so the work is not done."),
            (Stop::MaxTokens, "Your answer was cut off: it ran out of tokens."),
            (Stop::Refusal, "You declined to go on."),
            (Stop::NoCalls, "You said you would call a tool, and named none."),
        ];
        for (stop, first) in cases {
            let rendered = nudge(Mode::Finite, stop, 1, 2);
            let rendered = text(&rendered);
            assert!(rendered.starts_with(first) && rendered.ends_with(rest), "{rendered}");
            assert_eq!(rendered.len(), first.len() + rest.len());
        }
    }

    #[test]
    fn numbers_are_written_in_decimal() {
        for (n, expected) in [(0, "0"), (7, "7"), (10, "10"), (305, "305"), (u32::MAX, "4294967295")] {
            let mut measured = Text::measuring();
            measured.put_decimal(n);
            let mut written = measured.writing();
            written.put_decimal(n);
            assert_eq!(text(&written.finish()), expected);
        }
    }
    #[test]
    fn one_directory_hides_its_name_in_the_checkout_and_guide_headings() {
        let charter = charter();
        let mut mounted = workspace();
        mounted.directories = Box::new([mounted.directories[0].clone()]);
        let mut found = Found::with_capacity(1);
        found.guides.push(Guide { repository: 0, text: bytes(b"Guide text."), whole: true }).unwrap();
        let rendered = system(&charter, Some(&mounted), &found, charter.budget, false);
        let rendered = text(&rendered);
        assert!(!rendered.contains(text(&mounted.directories[0].name)), "the one mount's name is hidden");
        assert!(rendered.contains("## AGENTS.md in the working directory\n\nGuide text."));
        assert!(rendered.contains("## Checkout\n\nPaths start in your working directory. Use `src/lib.rs`, without a directory-name prefix.\n- The working directory, which you may only read"));
    }

    #[test]
    fn several_directories_name_the_working_directory_and_the_namespace() {
        let charter = charter();
        let mut mounted = workspace();
        let mut other = mounted.directories[0].clone();
        other.name = bytes(b"docs");
        other.root = skein_lib::Token::new(901);
        mounted.directories = Box::new([mounted.directories[0].clone(), other]);
        let rendered = system(&charter, Some(&mounted), &Found::with_capacity(0), charter.budget, false);
        let rendered = text(&rendered);
        assert!(rendered.contains("Your working directory is `temper`. Relative paths start there; reach the other directories as `/name/...`. Use `list /` to list their names."));
        for directory in &mounted.directories {
            assert!(rendered.contains(text(&directory.name)));
        }
    }
}

#[cfg(test)]
#[path = "prompt/open_tests.rs"]
mod open_tests;
