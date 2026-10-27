#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    reason = "ordinary backend harness, programming-model.md 10.2"
)]
use super::lifecycle_tests::{backend, env, request};
use super::*;
use alloc::vec;
use smith_domain::tools::{Effect, Grants};

const READ: &[u8] = br#"{"type":"assistant","message":{"id":"one","content":[{"type":"tool_use","id":"r","name":"Read","input":{"file_path":"a"}}]}}"#;

const RESULT: &[u8] = br#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"r","content":[{"type":"text","text":"one"},{"type":"image"},{"type":"text","text":"two"}],"is_error":true}]}}"#;
fn active() -> Backend {
    let mut backend = backend();
    backend.phase = Phase::Running;
    backend.child = Some(Token::new(30));
    backend.pipes[0] = Some(Token::new(31));
    backend.request = Some(request(&backend));
    backend.owner = Some(Token::new(80));
    backend
}

#[test]
fn a_built_in_tool_use_is_a_builtin_call() {
    for (name, effect) in [
        ("Read", Effect::Read),
        ("Glob", Effect::Read),
        ("Grep", Effect::Read),
        ("Write", Effect::Write),
        ("Edit", Effect::Write),
        ("Bash", Effect::Write),
        ("future", Effect::Write),
        ("finish", Effect::Write),
    ] {
        let mut backend = active();
        let mut up = Queue::with_capacity(32);
        let mut below = Queue::with_capacity(64);
        let bytes = core::str::from_utf8(READ).expect("JSON").replace("Read", name);
        backend.line(&env(0), bytes.as_bytes(), &mut up, &mut below);
        let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("completion") };
        let llm::Said::ToolCall { name: got, call, .. } = &completion.content[0] else { panic!("tool call") };
        assert_eq!(got.as_ref(), name.as_bytes());
        assert_eq!(*call, llm::Decoded::Builtin { effect });
    }
}

#[test]
fn a_built_in_result_follows_its_completion() {
    let mut backend = active();
    backend.owner = None;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), READ, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    assert!(up.is_empty(), "result waits behind queued completion");
    backend.resume(&env(0), request(&backend), &mut up, &mut below);
    completed(up.pop(), Token::new(80));
    let Some(ToDomain::BuiltinResult { owner, id, text, error }) = up.pop() else { panic!("builtin result") };
    assert_eq!(owner, Token::new(80));
    assert_eq!(id.as_ref(), b"r");
    assert_eq!(text.as_ref(), b"one\ntwo");
    assert!(error);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    assert!(up.is_empty(), "repeated result is inert");
}

#[test]
fn a_complete_s_result_for_a_built_in_is_skipped() {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), READ, &mut up, &mut below);
    up.pop();
    while backend.pop_measure().is_some() {}
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("Complete") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::ToolResult {
            id: b"r".as_slice().into(),
            result: llm::Returned::Text { text: Box::from([b'x'; 8192]), error: false, replay: None },
        }]),
    }]);
    backend.resume(&env(0), next, &mut up, &mut below);
    assert!(up.is_empty());
    assert!(backend.outgoing.is_none());
    assert!(backend.ledger.get(0).expect("call").answer.is_none());
    backend.line(&env(0), RESULT, &mut up, &mut below);
    result_id(up.pop(), Token::new(80), b"r");
    let later = core::str::from_utf8(READ).expect("JSON").replace("one", "later").replace("\"r\"", "\"r2\"");
    backend.line(&env(0), later.as_bytes(), &mut up, &mut below);
    completed(up.pop(), Token::new(80));
    let mut next = request(&backend);
    let FromDomain::Complete { prompt, .. } = &mut next else { panic!("Complete") };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::ToolResult {
            id: b"r".as_slice().into(),
            result: llm::Returned::Text { text: Box::from([b'x'; 8192]), error: false, replay: None },
        }]),
    }]);
    backend.resume(&env(0), next, &mut up, &mut below);
    assert!(up.is_empty(), "historical built-in results also bypass MCP rendering");
}

#[test]
fn tools_flag_names_the_granted_families() {
    for (grants, flag) in [
        (Grants { inspect: false, modify: false, shell: false }, b"".as_slice()),
        (Grants { inspect: true, modify: false, shell: false }, b"Read,Glob,Grep"),
        (Grants { inspect: false, modify: true, shell: true }, b"Write,Edit,Bash"),
        (Grants { inspect: true, modify: true, shell: true }, b"Read,Glob,Grep,Write,Edit,Bash"),
    ] {
        assert_eq!(policy::tools_flag(grants).as_ref(), flag);
    }
}

fn init(tools: &[&str], granted: bool) -> (Backend, Queue<ToDomain>) {
    init_contract(tools, granted, false)
}

fn init_contract(tools: &[&str], granted: bool, contract: bool) -> (Backend, Queue<ToDomain>) {
    let mut backend = active();
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    if let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request {
        prompt.tools.inspect = granted;
        if contract {
            prompt.served = Box::new([llm::Served::Finish]);
        }
    }
    let bytes = serde_json::to_vec(&serde_json::json!({"type":"system","subtype":"init","mcp_servers":[{"name":"smith","status":"connected"}],"tools":tools})).expect("init");
    backend.admit_init(&env(0), &bytes, &mut up, &mut below);
    (backend, up)
}

#[test]
fn a_contract_init_may_list_structured_output() {
    let (backend, up) = init_contract(&["mcp__smith__finish", "StructuredOutput"], false, true);
    assert!(up.is_empty());
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn a_contract_init_may_omit_structured_output() {
    let (backend, up) = init_contract(&["mcp__smith__finish"], false, true);
    assert!(up.is_empty());
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn structured_output_without_a_contract_fails_init() {
    let (_, mut up) = init_contract(&["StructuredOutput"], false, false);
    failed(up.pop(), b"smith: the program offers a built-in tool the run did not grant: StructuredOutput");
}

#[test]
fn a_contract_init_refuses_duplicate_structured_output() {
    let (_, mut up) = init_contract(&["mcp__smith__finish", "StructuredOutput", "StructuredOutput"], false, true);
    failed(up.pop(), b"smith: the program offers a built-in tool the run did not grant: StructuredOutput");
}

#[test]
fn structured_output_uses_the_programs_result_before_finish() {
    use smith_domain::run::outcome::{Declared, Report};

    let mut backend = active();
    backend.request = Some(report_request(&backend));
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"type":"assistant","message":{"id":"structured","content":[{"type":"tool_use","id":"s","name":"StructuredOutput","input":{"form":"report","text":"done","fields":{}}}]}}"#, &mut up, &mut below);
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("completion") };
    let [llm::Said::ToolCall { name, input, call, .. }] = completion.content.as_ref() else { panic!("builtin call") };
    assert_eq!(name.as_ref(), b"StructuredOutput");
    assert_eq!(input.as_ref(), br#"{"form":"report","text":"done","fields":{}}"#);
    assert_eq!(*call, llm::Decoded::Builtin { effect: Effect::Write });
    assert!(!backend.awaits_answer(b"s"), "the domain has no call to answer");
    backend.line(&env(0), br#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"s","content":"accepted","is_error":false}]}}"#, &mut up, &mut below);
    let Some(ToDomain::BuiltinResult { id, text, error, .. }) = up.pop() else { panic!("program result") };
    assert_eq!(id.as_ref(), b"s");
    assert_eq!(text.as_ref(), b"accepted");
    assert!(!error);
    while backend.pop_measure().is_some() {}
    let mut next = report_request(&backend);
    let FromDomain::Complete { owner, .. } = &mut next else { panic!("complete") };
    *owner = Token::new(81);
    backend.resume(&env(0), next, &mut up, &mut below);
    backend.line(&env(0), br#"{"type":"result","subtype":"success","is_error":false,"structured_output":{"form":"report","text":"done","fields":{}}}"#, &mut up, &mut below);
    let Some(ToDomain::Completed { owner, completion }) = up.pop() else { panic!("structured finish") };
    assert_eq!(owner, Token::new(81));
    let [llm::Said::ToolCall { name, call, .. }] = completion.content.as_ref() else { panic!("finish") };
    assert_eq!(name.as_ref(), b"finish");
    assert_eq!(
        *call,
        llm::Decoded::Served {
            ask: smith_domain::run::Ask::Finish {
                outcome: Declared::Report(Report { text: b"done".as_slice().into(), fields: Box::default() })
            }
        }
    );
    assert!(up.is_empty());
    assert!(below.is_empty(), "neither a permission prompt nor an MCP answer");
    assert_eq!(backend.phase, Phase::Running);
}

fn report_request(backend: &Backend) -> FromDomain {
    let mut request = request(backend);
    let FromDomain::Complete { prompt, outcome, .. } = &mut request else { panic!("complete") };
    prompt.served = Box::new([llm::Served::Finish]);
    // Same report text cap as the backend harness's max_tokens.
    outcome.report = Some(smith_domain::run::outcome::TextSpec { max: 128, fields: Box::default() });
    request
}

#[test]
fn an_init_with_an_ungranted_built_in_fails() {
    let (_, mut up) = init(&["mcp__smith__permission_prompt", "Read"], false);
    failed(up.pop(), b"smith: the program offers a built-in tool the run did not grant: Read");
}

#[test]
fn an_init_without_a_granted_built_in_fails() {
    let (_, mut up) = init(&["mcp__smith__permission_prompt", "Read", "Glob"], true);
    failed(up.pop(), b"smith: the program lacks a granted built-in tool: Grep");
    let (backend, up) = init(&["mcp__smith__permission_prompt", "Read", "Glob", "Grep"], true);
    assert!(up.is_empty());
    assert_eq!(backend.phase, Phase::Running);
}

#[test]
fn the_server_lists_no_workspace_tool() {
    let mut backend = active();
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else { panic!("request") };
    prompt.tools = Grants { inspect: true, modify: true, shell: true };
    prompt.served = Box::new([llm::Served::Finish, llm::Served::Wait]);
    let tools = backend.tools();
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().all(|tool| [b"finish".as_slice(), b"wait"].contains(&tool.name.as_ref())));
}

fn host_tool(schema: &[u8]) -> llm::Served {
    llm::Served::Host(smith_domain::run::HostTool {
        name: b"host_action".as_slice().into(),
        description: b"Host action".as_slice().into(),
        schema: schema.into(),
        effect: smith_domain::run::HostEffect::Write,
        timeout: Duration::from_secs(1),
    })
}

#[test]
fn every_charter_inventory_serves_only_object_input_schemas() {
    use smith_domain::run::outcome::{ChangeSpec, FieldRule, ItemSpec, OutcomeSpec, TextSpec, VerdictRule};
    // Same text/field cap as the backend harness's max_tokens.
    const CONTRACT_VALUE_BYTES: u32 = 128;
    let empty = OutcomeSpec { change: None, verdicts: Box::default(), report: None, failure: None };
    // Exercise every final form, including the nested verdict oneOf.
    let contract = OutcomeSpec {
        change: Some(ChangeSpec {
            checks_must_pass: true,
            fields: Box::new([FieldRule { name: b"title".as_slice().into(), max: CONTRACT_VALUE_BYTES }]),
        }),
        verdicts: Box::new([VerdictRule {
            name: b"approved".as_slice().into(),
            text_max: CONTRACT_VALUE_BYTES,
            fields: Box::default(),
            items: ItemSpec { min: 0, max: 0, kinds: Box::default() },
        }]),
        report: Some(TextSpec { max: CONTRACT_VALUE_BYTES, fields: Box::default() }),
        failure: Some(TextSpec { max: CONTRACT_VALUE_BYTES, fields: Box::default() }),
    };
    let delivery = Some(ChangeSpec { checks_must_pass: true, fields: Box::default() });
    // Open does not offer finish; Finite does. A delivery grant is independent.
    for (served, outcome, deliver) in [
        (vec![llm::Served::Wait, host_tool(br#"{"anyOf":[{"type":"object"}]}"#)], empty.clone(), None),
        (vec![llm::Served::Wait, llm::Served::Deliver], empty.clone(), delivery.clone()),
        (vec![llm::Served::Wait], contract.clone(), None),
        (vec![llm::Served::Finish, llm::Served::Wait], empty, None),
        (vec![llm::Served::Finish, llm::Served::Wait], contract.clone(), None),
        (vec![llm::Served::Finish, llm::Served::Deliver, host_tool(b"{}")], contract, delivery),
    ] {
        let mut backend = active();
        let Some(FromDomain::Complete { prompt, outcome: result, deliver: grant, .. }) = &mut backend.request else {
            panic!("request")
        };
        prompt.served = served.into_boxed_slice();
        *result = outcome.clone();
        *grant = deliver;
        let tools = backend.tools();
        let list = mcp::json::tool_list(&tools, backend.settings.mcp.answer_bytes, backend.settings.mcp.schema_bytes)
            .expect("inventory");
        let list: serde_json::Value = serde_json::from_slice(&list).expect("MCP list");
        for tool in list["tools"].as_array().expect("tools") {
            assert!(tool["inputSchema"].is_object());
            assert_eq!(tool["inputSchema"]["type"], "object", "{}", tool["name"]);
        }
        let expected: serde_json::Value = serde_json::from_slice(
            &protocol::finish_schema(&outcome, backend.settings.contract_bytes).expect("finish schema"),
        )
        .expect("schema JSON");
        for tool in list["tools"].as_array().expect("tools").iter().filter(|tool| tool["name"] == "finish") {
            assert_eq!(tool["inputSchema"]["oneOf"], expected["oneOf"]);
        }
    }
}

#[test]
fn a_host_schema_refusal_names_the_tool_at_startup() {
    let mut backend = active();
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else { panic!("request") };
    prompt.served = Box::new([host_tool(br#"{"type":"string"}"#)]);
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.prepare_invocation(&env(0), &mut up, &mut below);
    failed(up.pop(), b"smith: MCP tool input schema is not an object: host_action");
    assert!(backend.server.is_none());
    for request in &below {
        if let Below::Io(io::Request::Listen { .. }) = request {
            panic!("schema refusal starts no listener");
        }
    }
}

#[test]
fn queued_built_in_results_keep_their_message_owners() {
    let mut backend = active();
    backend.owner = None;
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), READ, &mut up, &mut below);
    backend.line(&env(0), RESULT, &mut up, &mut below);
    let next_call = core::str::from_utf8(READ).expect("JSON").replace("one", "second").replace("\"r\"", "\"r2\"");
    let next_result = core::str::from_utf8(RESULT).expect("JSON").replace("\"r\"", "\"r2\"");
    backend.line(&env(0), next_call.as_bytes(), &mut up, &mut below);
    backend.line(&env(0), next_result.as_bytes(), &mut up, &mut below);
    backend.resume(&env(0), request(&backend), &mut up, &mut below);
    completed(up.pop(), Token::new(80));
    result_id(up.pop(), Token::new(80), b"r");
    assert!(up.is_empty());
    while backend.pop_measure().is_some() {}
    let mut next = request(&backend);
    let FromDomain::Complete { owner, .. } = &mut next else { panic!("Complete") };
    *owner = Token::new(81);
    backend.resume(&env(0), next, &mut up, &mut below);
    completed(up.pop(), Token::new(81));
    result_id(up.pop(), Token::new(81), b"r2");
    assert!(up.is_empty());
}

fn completed(event: Option<ToDomain>, expected: Token) {
    let Some(ToDomain::Completed { owner, .. }) = event else { panic!("completion") };
    assert_eq!(owner, expected);
}

fn result_id(event: Option<ToDomain>, expected: Token, expected_id: &[u8]) {
    let Some(ToDomain::BuiltinResult { owner, id, .. }) = event else { panic!("builtin result") };
    assert_eq!(owner, expected);
    assert_eq!(id.as_ref(), expected_id);
}

fn failed(event: Option<ToDomain>, words: &[u8]) {
    let Some(ToDomain::Failed { failure, detail, .. }) = event else { panic!("failure") };
    assert_eq!(failure, llm::Failure::Program { exit: None });
    assert_eq!(detail.as_ref(), words);
}

#[test]
fn an_open_catalog_has_wait_and_refuses_unoffered_finish() {
    let mut backend = active();
    let Some(FromDomain::Complete { prompt, .. }) = &mut backend.request else { panic!("request") };
    prompt.served = Box::new([llm::Served::Wait]);
    let tools = backend.tools();
    let [waiting] = tools.as_ref() else { panic!("one offered tool") };
    assert_eq!(waiting.name.as_ref(), b"wait");
    let mut up = Queue::with_capacity(32);
    let mut below = Queue::with_capacity(64);
    backend.line(&env(0), br#"{"type":"assistant","message":{"id":"one","content":[{"type":"tool_use","id":"f","name":"mcp__smith__finish","input":{}}]}}"#, &mut up, &mut below);
    let Some(ToDomain::Completed { completion, .. }) = up.pop() else { panic!("actual completion") };
    let [llm::Said::ToolCall { call, .. }] = completion.content.as_ref() else { panic!("one unoffered call") };
    assert_eq!(*call, llm::Decoded::Invalid { problem: llm::Problem::UnknownTool });
}
