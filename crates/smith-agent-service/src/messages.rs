//! Host message content waiting for the run's admission or refusal fact.
//! The service copies decoded labels and text immediately before their domain
//! hand-off. Arrival order, including repeated names, joins each fact once;
//! this book knows neither admission policy nor rendered provider content.
//! Contracts: protocol/events.md, sections 3.7 and 5.3; domain/run.md, section 6.

use alloc::boxed::Box;

use skein_lib::{Queue, Token};

use crate::sinks::Capture;

/// One service-decoded message, retained until its actual run receipt.
#[derive(Debug)]
pub(crate) struct Message {
    name: Token,
    pub(crate) label: Box<[u8]>,
    pub(crate) text: Box<[u8]>,
}

/// Decoded content waiting for one actual run receipt, under Everything only.
#[derive(Debug)]
pub(crate) struct Book {
    pending: Queue<Message>,
}

impl Book {
    #[cfg(test)]
    pub(crate) fn disabled() -> Book {
        Book { pending: Queue::with_capacity(0) }
    }

    pub(crate) fn new(limits: &crate::Limits, capture: Capture) -> Book {
        let slots = match capture {
            Capture::None | Capture::Calls => 0,
            Capture::Everything => slots(limits).expect("admitted message observation cells fit"),
        };
        Book { pending: Queue::with_capacity(slots) }
    }

    pub(crate) fn received(&mut self, name: Token, label: &[u8], text: &[u8]) {
        if self.pending.capacity() == 0 {
            return;
        }
        self.pending.push(Message { name, label: Box::from(label), text: Box::from(text) });
    }

    pub(crate) fn take(&mut self, name: Token) -> Option<Message> {
        if self.pending.capacity() == 0 {
            return None;
        }
        let message = self.pending.pop().expect("the service observed the actual message before its receipt");
        assert_eq!(message.name, name, "run receipts retain message arrival order even for repeated names");
        Some(message)
    }

    pub(crate) fn clear(&mut self) {
        for _ in 0..self.pending.capacity() {
            match self.pending.pop() {
                Some(_) => {}
                None => break,
            }
        }
    }
}

fn slots(limits: &crate::Limits) -> Option<u32> {
    limits.channel.bodies.start_messages.checked_add(1)
}

/// Whether captured message slots fit (programming-model.md, 6.3).
#[must_use]
pub(crate) fn valid(limits: &crate::Limits, capture: Capture) -> bool {
    match capture {
        Capture::None | Capture::Calls => true,
        Capture::Everything => slots(limits).is_some(),
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        reason = "programming-model.md section 10.2: deterministic receipt fixtures use ordinary Rust"
    )]

    use super::*;

    #[test]
    fn invalid_limits_are_refused() {
        let mut limits = crate::service::tests::configuration().limits;
        assert!(valid(&limits, Capture::Everything));
        limits.channel.bodies.start_messages = u32::MAX;
        assert!(!valid(&limits, Capture::Everything));
        assert!(valid(&limits, Capture::None));
        assert!(valid(&limits, Capture::Calls));
    }

    #[test]
    fn repeated_names_keep_each_original_label_and_text_until_its_receipt() {
        let mut book = Book { pending: Queue::with_capacity(2) };
        let name = Token::new(4);
        book.received(name, b"task", b"first actual input");
        book.received(name, b"relay", b"second, refused input");
        let first = book.take(name).unwrap();
        assert_eq!(first.label.as_ref(), b"task");
        assert_eq!(first.text.as_ref(), b"first actual input");
        let second = book.take(name).unwrap();
        assert_eq!(second.label.as_ref(), b"relay");
        assert_eq!(second.text.as_ref(), b"second, refused input");
        assert!(book.pending.is_empty());
        book.received(name, b"never admitted", b"start refused");
        book.clear();
        assert!(book.pending.is_empty());
    }

    #[test]
    fn absent_content_selection_owns_no_message_copy() {
        let mut book = Book { pending: Queue::with_capacity(0) };
        book.received(Token::new(4), b"task", b"text");
        assert!(book.take(Token::new(4)).is_none());
        assert_eq!(book.pending.capacity(), 0);
    }
}
