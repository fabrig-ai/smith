//! Guide reads use the space left after admitting the charter's rendered material.

#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    reason = "programming-model.md, section 10.2: deterministic test harnesses are ordinary Rust"
)]

use super::{
    Answer, Charter, Event, Harness, Invalid, LIMITS, Limits, Read, Refusal, Request, Workspace, charter,
    workspace_directory,
};
use alloc::boxed::Box;

fn opening() -> Charter {
    let mut selected = charter();
    selected.grants.host_tools = Box::default();
    selected
}

fn workspace() -> Workspace {
    Workspace {
        directories: Box::new([
            workspace_directory(b"left", 900, false, false, &[]),
            workspace_directory(b"right", 901, false, false, &[]),
            workspace_directory(b"kit", 902, false, false, &[]),
            workspace_directory(b"log", 903, false, false, &[]),
        ]),
    }
}

fn working_directory() -> Workspace {
    Workspace { directories: Box::new([workspace_directory(b"work", 900, false, false, &[])]) }
}

fn host_part(selected: &Charter, mounted: &Workspace, guide: u32) -> u64 {
    crate::prompt::host_bytes(selected, Some(mounted), guide)
        .expect("rendered host")
        .checked_add(LIMITS.schema_costs.finish(&selected.outcome).expect("finish schema"))
        .expect("small schema and host")
        .checked_add(LIMITS.schema_costs.finish_text)
        .expect("small schema text")
}

#[test]
fn each_guide_is_read_and_retained_within_its_share_of_the_host_part() {
    let selected = opening();
    let mounted = workspace();
    // Four guides get 4 * (16 + 2) = 72 bytes beyond the empty-guide
    // scaffolding: floor(72 / 4) - 2 = 16 bytes each, below guide_bytes 64.
    let host_part = host_part(&selected, &mounted, 16);
    let mut harness = Harness::new(Limits { directories: 4, host_part, ..LIMITS });
    let requests = harness.start_workspace(1, selected, Some(mounted));
    let [Request::Admitted { .. }, Request::Read { owner, max, .. }] = requests.as_ref() else {
        panic!("admitted guide read: {requests:?}")
    };
    assert_eq!(*max, 16);
    let owner = *owner;
    for _ in 0_u32..3 {
        let requests =
            harness.step(Event::Read { owner, read: Read::Text { text: Box::new([b'g'; 16]), whole: false } });
        let [Request::Read { max, .. }] = requests.as_ref() else { panic!("next directory's guide") };
        assert_eq!(*max, 16);
    }
    let requests = harness.step(Event::Read { owner, read: Read::Text { text: Box::new([b'g'; 16]), whole: false } });
    let [Request::Open { opening, .. }] = requests.as_ref() else { panic!("prepared main") };
    let mut guides = 0_u32;
    for piece in opening.system.windows(16) {
        if piece == [b'g'; 16] {
            guides = guides.checked_add(1).expect("four guide payloads");
        }
    }
    assert_eq!(guides, 4);
    assert_eq!(
        opening.system.windows(b"The file goes on".len()).filter(|piece| *piece == b"The file goes on").count(),
        4
    );
}

#[test]
fn a_charter_that_leaves_no_guide_bytes_is_admitted() {
    let selected = opening();
    let mounted = workspace();
    let host_part = host_part(&selected, &mounted, 0);
    let mut harness = Harness::new(Limits { directories: 4, host_part, ..LIMITS });
    let requests = harness.start_workspace(1, selected, Some(mounted));
    let [Request::Admitted { .. }, Request::Read { max, .. }] = requests.as_ref() else {
        panic!("admitted without guide bytes: {requests:?}")
    };
    assert_eq!(*max, 0);
}

#[test]
fn a_charter_that_cannot_fit_without_guide_bytes_is_refused_before_io() {
    let selected = opening();
    let mounted = workspace();
    let host_part = host_part(&selected, &mounted, 0) - 1;
    let mut harness = Harness::new(Limits { directories: 4, host_part, ..LIMITS });
    let requests = harness.start_workspace(1, selected, Some(mounted));
    let [Request::Answer { answer: Answer::Refused(Refusal::Invalid(Invalid::HostPart { bound })), .. }] =
        requests.as_ref()
    else {
        panic!("host part refused before preparation: {requests:?}")
    };
    assert_eq!(*bound, host_part);
}

#[test]
fn one_directory_keeps_a_guide_at_the_bound_whole_when_four_are_available() {
    let selected = opening();
    let mounted = working_directory();
    let host_part = host_part(&selected, &mounted, LIMITS.guide_bytes);
    let mut harness = Harness::new(Limits { directories: 4, host_part, ..LIMITS });
    let requests = harness.start_workspace(1, selected, Some(mounted));
    let [Request::Admitted { .. }, Request::Read { owner, max, .. }] = requests.as_ref() else {
        panic!("one directory admitted: {requests:?}")
    };
    assert_eq!(*max, LIMITS.guide_bytes);
    let requests =
        harness.step(Event::Read { owner: *owner, read: Read::Text { text: Box::new([b'g'; 64]), whole: true } });
    let [Request::Open { opening, .. }] = requests.as_ref() else { panic!("prepared main") };
    assert!(opening.system.windows(64).any(|piece| piece == [b'g'; 64]));
    assert!(!opening.system.windows(b"The file goes on".len()).any(|piece| piece == b"The file goes on"));
}

// A one-directory start may now retain a guide larger than the old profile's
// host / directories allowance. Child admission must count that retained text.
const CHILD_GUIDE_BYTES: usize = 1024;

fn child_with_inherited_guide(extra: usize) -> Box<[Request]> {
    use super::{Ask, EXPIRY, Found, Guide, Token, families};
    let mut selected = opening();
    selected.grants.agents = true;
    let mounted = working_directory();
    let guide_bytes = u32::try_from(CHILD_GUIDE_BYTES).unwrap();
    let host_part = host_part(&selected, &mounted, guide_bytes);
    let mut found = Found::with_capacity(1);
    found.guides.push(Guide { repository: 0, text: Box::new([b'g'; CHILD_GUIDE_BYTES]), whole: true }).unwrap();
    // A one-byte brief measures its paragraph separators too; replace only
    // that byte with the remaining room, then optionally cross the bound.
    let inherited =
        crate::prompt::child_host_bytes(&selected, Some(&mounted), &found, b"b", families(true, false, false)).unwrap();
    let brief_bytes = usize::try_from(host_part.checked_sub(inherited).unwrap())
        .unwrap()
        .checked_add(1)
        .unwrap()
        .checked_add(extra)
        .unwrap();
    let brief = alloc::vec![b'b'; brief_bytes].into_boxed_slice();
    let mut harness = Harness::new(Limits { directories: 4, guide_bytes, host_part, ..LIMITS });
    let requests = harness.start_workspace(1, selected, Some(mounted));
    let [Request::Admitted { .. }, Request::Read { owner, max, .. }] = requests.as_ref() else {
        panic!("parent admitted")
    };
    assert_eq!(*max, guide_bytes);
    let requests = harness.step(Event::Read {
        owner: *owner,
        read: Read::Text { text: Box::new([b'g'; CHILD_GUIDE_BYTES]), whole: true },
    });
    let [Request::Open { conversation, .. }] = requests.as_ref() else { panic!("parent opens") };
    let conversation = *conversation;
    drop(harness.step(Event::Started { conversation, peer: Token::new(100) }));
    harness.step(Event::Delegated {
        name: crate::CallName { activation: 1, completion: 1, position: 0 },
        conversation,
        call: Token::new(7),
        ask: Ask::SubAgent { brief, families: families(true, false, false), llm: None, share: crate::Share::UNASKED },
        deadline: EXPIRY,
    })
}

#[test]
fn a_child_at_the_host_part_keeps_its_parents_whole_guide() {
    let requests = child_with_inherited_guide(0);
    let [Request::Open { opening, .. }] = requests.as_ref() else { panic!("child fits: {requests:?}") };
    assert!(opening.system.windows(CHILD_GUIDE_BYTES).any(|piece| piece == [b'g'; CHILD_GUIDE_BYTES]));
}

#[test]
fn a_child_past_the_host_part_is_refused_with_its_inherited_guide() {
    let requests = child_with_inherited_guide(1);
    let [Request::Return { result: super::Returned::Refused { refusal: super::AskRefusal::TooLarge { .. } }, .. }] =
        requests.as_ref()
    else {
        panic!("child refused before opening: {requests:?}")
    };
}
