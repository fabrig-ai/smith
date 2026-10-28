//! First-user ownership of Open briefs; frozen pre-change Finite and resumed text.
#![expect(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    reason = "programming-model.md, section 10.2: literal prompt fixtures use ordinary Rust"
)]

use super::*;
use crate::{Brief, Mode, Section};

fn policy(mode: Mode) -> Charter {
    Charter {
        mode,
        instructions: bytes(b"Host instructions.\n"),
        brief: Brief {
            sections: Box::new([
                Section { title: bytes(b"Request"), text: bytes("Read café; ask before coding.".as_bytes()) },
                Section { title: Box::default(), text: Box::default() },
                Section { title: bytes(b"Request"), text: bytes(b"Preserve this final line.\n") },
            ]),
        },
        ..charter()
    }
}

// Literal rendering from the pre-change host fixture, including empty and repeated titles.
const BRIEF: &[u8] =
    "## Request\n\nRead café; ask before coding.\n\n## \n\n\n\n## Request\n\nPreserve this final line.\n\n".as_bytes();
const OFFER: &[u8] = b"person: first\n\nperson: second";

fn opened(mode: Mode, resumed: bool, messages: Box<[crate::Message]>) -> Opening {
    let mut start = carried_start(policy(mode), messages, None);
    let Event::Start { resumed: selected, transcript, .. } = &mut start else { panic!("Start fixture") };
    *selected = resumed;
    *transcript = resumed.then_some(Token::new(77));
    let emitted = Harness::new(LIMITS).step(start);
    let Ok(emitted) = Box::<[Request; 2]>::try_from(emitted) else { panic!("two requests") };
    let [Request::Admitted { .. }, Request::Open { opening, .. }] = *emitted else { panic!("main opens") };
    opening
}

fn messages() -> Box<[crate::Message]> {
    Box::new([
        crate::Message { finish_requested: false, name: Token::new(1), label: bytes(b"person"), text: bytes(b"first") },
        crate::Message {
            finish_requested: false,
            name: Token::new(2),
            label: bytes(b"person"),
            text: bytes(b"second"),
        },
    ])
}

#[test]
fn open_first_user_message_is_the_whole_literal_brief() {
    // Pre-change Open system 821 bytes less its 90-byte rendered brief.
    const SYSTEM_BYTES: usize = 731;
    let opening = opened(Mode::Open, false, Box::default());
    assert_eq!(opening.prompt.as_ref(), BRIEF);
    let system = core::str::from_utf8(&opening.system).unwrap();
    assert!(system.starts_with("Host instructions.\n\n## Tools\n\n"));
    for absent in ["## Request", "Read café", "Preserve this final line.", "Begin the work"] {
        assert!(!system.contains(absent));
    }
    for retained in ["## Checkout", "Call `wait`", "## Replying", "## Budget"] {
        assert!(system.contains(retained));
    }
    assert_eq!(opening.system.len(), SYSTEM_BYTES);
}

#[test]
fn open_pre_turn_messages_follow_the_whole_brief() {
    let opening = opened(Mode::Open, false, messages());
    let expected = [BRIEF, OFFER].concat();
    assert_eq!(opening.prompt.as_ref(), expected);
}

#[test]
fn finite_opening_keeps_the_pre_change_bytes() {
    let opening = opened(Mode::Finite, false, messages());
    assert_eq!(opening.system.as_ref(), include_bytes!("finite_system.txt"));
    assert_eq!(opening.prompt.as_ref(), b"Begin the work your brief describes.\n\nperson: first\n\nperson: second");
}

#[test]
fn resumed_open_window_keeps_its_system_and_waking_message() {
    for (messages, expected) in
        [(Box::<[crate::Message]>::default(), b"Continue the saved conversation.".as_slice()), (messages(), OFFER)]
    {
        let opening = opened(Mode::Open, true, messages);
        assert_eq!(opening.system.as_ref(), include_bytes!("open_system.txt"));
        assert_eq!(opening.prompt.as_ref(), expected);
        assert_eq!(opening.transcript, Some(Token::new(77)));
    }
}

#[test]
fn moving_the_brief_keeps_its_host_part_charge() {
    // Pre-change rendered charter charge: 20 instruction bytes and 90 brief bytes.
    const HOST_BYTES: u64 = 110;
    let selected = policy(Mode::Open);
    assert_eq!(crate::prompt::host_bytes(&selected, None, 0), Some(HOST_BYTES));
}

#[test]
fn empty_open_brief_keeps_the_message_driven_opening() {
    for (sections, expected) in [
        (Box::<[Section]>::default(), OFFER),
        (
            Box::from([Section { title: Box::default(), text: Box::default() }]),
            b"## \n\n\n\nperson: first\n\nperson: second".as_slice(),
        ),
    ] {
        let selected = Charter { brief: Brief { sections }, ..policy(Mode::Open) };
        assert_eq!(crate::prompt::opening(&selected, false, Some(bytes(OFFER))).as_ref(), expected);
    }
}
