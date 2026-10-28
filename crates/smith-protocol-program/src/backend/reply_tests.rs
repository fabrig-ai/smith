use super::{calls::Call, lifecycle_tests::backend, lifecycle_tests::request, replies, *};

pub(super) fn call(id: &[u8]) -> Call {
    Call {
        id: id.into(),
        name: b"wait".as_slice().into(),
        input: b"{}".as_slice().into(),
        rpc: None,
        answer: None,
        answered: false,
        prompted: false,
        eligible: true,
        message: 1,
        builtin: false,
        native_write: false,
        owner: None,
    }
}

pub(super) fn result(id: &[u8]) -> llm::Block {
    llm::Block::ToolResult {
        id: id.into(),
        result: llm::Returned::Text { text: b"done".as_slice().into(), error: false, replay: None },
    }
}

#[test]
fn wait_s_messages_join_its_answer() {
    let mut backend = backend();
    assert!(backend.ledger.push(call(b"first")).is_ok());
    assert!(backend.ledger.push(call(b"last")).is_ok());
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("prompt") };
    let messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([
            result(b"first"),
            result(b"last"),
            llm::Block::Text { text: b"message 1".as_slice().into(), replay: None },
            llm::Block::Text { text: b"message 2".as_slice().into(), replay: None },
        ]),
    }]);
    assert!(replies::resume(&mut backend, messages, &mut prompt, false).expect("resumed").line.is_none());
    let mcp::Answer::Text(first) = backend.ledger.get(0).expect("first").answer.as_ref().expect("first reply") else {
        panic!("successful first reply")
    };
    let mcp::Answer::Text(last) = backend.ledger.get(1).expect("last").answer.as_ref().expect("last reply") else {
        panic!("successful last reply")
    };
    assert_eq!(first.as_ref(), b"done");
    assert_eq!(last.as_ref(), b"done\nmessage 1\nmessage 2");
}

#[test]
fn a_joined_reply_past_its_bound_is_refused() {
    let mut backend = backend();
    backend.settings.adapter.rendered_result = 8;
    assert!(backend.ledger.push(call(b"last")).is_ok());
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("prompt") };
    let messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([result(b"last"), llm::Block::Text { text: b"more".as_slice().into(), replay: None }]),
    }]);
    assert!(replies::resume(&mut backend, messages, &mut prompt, false).is_none());
}

#[test]
fn user_text_survives_interleaved_results() {
    let mut backend = backend();
    assert!(backend.ledger.push(call(b"first")).is_ok());
    assert!(backend.ledger.push(call(b"last")).is_ok());
    let FromDomain::Complete { mut prompt, .. } = request(&backend) else { panic!("prompt") };
    let messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([
            llm::Block::Text { text: b"before".as_slice().into(), replay: None },
            result(b"first"),
            llm::Block::Text { text: b"between".as_slice().into(), replay: None },
            result(b"last"),
            llm::Block::Text { text: b"after".as_slice().into(), replay: None },
        ]),
    }]);
    assert!(replies::resume(&mut backend, messages, &mut prompt, false).expect("resumed").line.is_none());
    let mcp::Answer::Text(first) = backend.ledger.get(0).expect("first").answer.as_ref().expect("first reply") else {
        panic!("first success")
    };
    let mcp::Answer::Text(last) = backend.ledger.get(1).expect("last").answer.as_ref().expect("last reply") else {
        panic!("last success")
    };
    assert_eq!(first.as_ref(), b"done");
    assert_eq!(last.as_ref(), b"done\nbefore\nbetween\nafter");
}
