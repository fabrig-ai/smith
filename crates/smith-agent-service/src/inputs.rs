//! Provider arguments awaiting an actual tool start in the same conversation.
//! One bounded completion per conversation supplies inputs; valid calls keep
//! their original arguments after decoding. Invalid and historical calls never
//! acquire a start. `received` replaces the prior batch, `take` moves one input
//! into its event, and `ended` releases anything the session did not start.
//! This book never holds tool results or replay envelopes.
//! Contracts: protocol/events.md, sections 3.6 and 5.3; domain/session.md, section 6.

use alloc::boxed::Box;

use skein_lib::{List, Map, Token};
use smith_domain::{llm, session};

use crate::sinks::Capture;

#[derive(Debug)]
struct Input {
    call: Box<[u8]>,
    tool: Box<[u8]>,
    text: Option<Box<[u8]>>,
}

/// A selected capture's one pending provider-argument batch per conversation.
#[derive(Debug)]
pub(crate) struct Book {
    conversations: Map<Token, List<Input>>,
    blocks: u32,
    bytes: u64,
}

impl Book {
    pub(crate) fn new(limits: &session::Limits, capture: Capture) -> Book {
        let sessions = match capture {
            Capture::None => 0,
            Capture::Calls | Capture::Everything => limits.sessions,
        };
        Book {
            conversations: Map::with_capacity(sessions),
            blocks: limits.completion_blocks,
            bytes: limits.answer_bytes,
        }
    }

    pub(crate) fn received(&mut self, owner: Token, completion: &llm::Completion) {
        if self.conversations.capacity() == 0 {
            return;
        }
        // A session requests its next completion only after its tooling batch
        // settles. Unchosen calls have no start and need no content event.
        drop(self.conversations.remove(&owner));
        let mut inputs = List::with_capacity(self.blocks);
        let mut bytes = 0_u64;
        for block in &completion.content {
            match block {
                llm::Said::ToolCall { id, name, input, call, replay: _ } => {
                    let selected = match call {
                        llm::Decoded::Builtin { .. } | llm::Decoded::Owned { .. } | llm::Decoded::Served { .. } => true,
                        llm::Decoded::Invalid { .. } => false,
                    };
                    if selected {
                        let identity = u64::try_from(id.len())
                            .expect("bounded provider call identity")
                            .checked_add(u64::try_from(name.len()).expect("bounded tool name"))
                            .expect("bounded provider identity sum");
                        let payload = identity
                            .checked_add(u64::try_from(input.len()).expect("bounded arguments"))
                            .expect("bounded provider input sum");
                        bytes = bytes.checked_add(payload).expect("bounded completion payload sum");
                        assert!(
                            bytes <= self.bytes,
                            "captured valid arguments are a subset of the raw provider answer"
                        );
                        inputs
                            .push(Input { call: id.clone(), tool: name.clone(), text: Some(input.clone()) })
                            .expect("one pending input per admitted completion block");
                    }
                }
                llm::Said::Opaque { .. } | llm::Said::Refusal { .. } | llm::Said::Text { .. } => {}
            }
        }
        let old = self.conversations.insert(owner, inputs).expect("actual conversations fit the source session bound");
        assert!(old.is_none(), "the prior pending batch was released above");
    }

    pub(crate) fn take(&mut self, owner: Token, metadata: &session::ToolCall) -> Option<Box<[u8]>> {
        let inputs = self.conversations.get_mut(&owner)?;
        for index in 0..inputs.len() {
            let input = inputs.get_mut(index).expect("within the retained batch");
            if input.call == metadata.id && input.tool == metadata.name {
                let text = input.text.take().expect("an actual provider call starts once in its batch");
                assert_eq!(
                    u64::try_from(text.len()).expect("bounded input length"),
                    metadata.input_bytes,
                    "the actual start keeps its provider arguments"
                );
                return Some(text);
            }
        }
        None
    }

    pub(crate) fn ended(&mut self, owner: Token) {
        drop(self.conversations.remove(&owner));
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::disallowed_macros,
        clippy::disallowed_types,
        clippy::disallowed_methods,
        reason = "programming-model.md section10.2: deterministic input-join fixtures use ordinary Rust"
    )]

    use super::*;

    fn limits() -> session::Limits {
        smith_agent_world::LIMITS.session
    }

    fn completion(input: &[u8]) -> llm::Completion {
        llm::Completion {
            reasoning_dropped: Box::new([]),
            content: Box::new([llm::Said::ToolCall {
                id: b"same-provider-id".to_vec().into(),
                name: b"wait".to_vec().into(),
                input: input.into(),
                call: llm::Decoded::Served { ask: smith_domain::run::Ask::Wait },
                replay: None,
            }]),
            stop: llm::Stop::ToolUse,
            usage: llm::Usage::ZERO,
        }
    }

    fn metadata(input: &[u8]) -> session::ToolCall {
        session::ToolCall {
            owner: Token::new(17),
            id: b"same-provider-id".to_vec().into(),
            name: b"wait".to_vec().into(),
            source: session::ToolSource::Run,
            effect: smith_domain::tools::Effect::Read,
            deadline: skein_lib::Time::ZERO,
            input_bytes: u64::try_from(input.len()).unwrap(),
        }
    }

    #[test]
    fn only_selected_capture_moves_exact_arguments_into_the_matching_conversation_start() {
        for capture in [Capture::None, Capture::Calls, Capture::Everything] {
            let bounds = limits();
            let mut book = Book::new(&bounds, capture);
            let owner = Token::new(11);
            let input = b" { } \n";
            book.received(owner, &completion(input));
            assert!(book.take(Token::new(12), &metadata(input)).is_none());
            let mut wrong = metadata(input);
            wrong.name = b"different-tool".to_vec().into();
            assert!(book.take(owner, &wrong).is_none());
            let captured = book.take(owner, &metadata(input));
            match capture {
                Capture::None => {
                    assert!(captured.is_none());
                    assert_eq!(book.conversations.capacity(), 0);
                }
                Capture::Calls | Capture::Everything => assert_eq!(captured.as_deref(), Some(input.as_slice())),
            }
        }
    }

    #[test]
    fn a_later_completion_reusing_an_id_and_an_unstarted_session_end_release_the_prior_batch() {
        let mut book = Book::new(&limits(), Capture::Calls);
        let owner = Token::new(11);
        let first = b"{}";
        book.received(owner, &completion(first));
        assert_eq!(book.take(owner, &metadata(first)).as_deref(), Some(first.as_slice()));
        let later = b" { } ";
        book.received(owner, &completion(later));
        assert_eq!(book.take(owner, &metadata(later)).as_deref(), Some(later.as_slice()));
        book.received(owner, &completion(first));
        book.ended(owner);
        assert_eq!(book.conversations.len(), 0);
        assert!(book.take(owner, &metadata(first)).is_none());
    }
}
