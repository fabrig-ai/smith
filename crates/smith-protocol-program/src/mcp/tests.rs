#![expect(clippy::match_like_matches_macro, reason = "I/O record filters use explicit matches in the test harness")]
#![expect(clippy::wildcard_enum_match_arm, reason = "test record filters ignore unrelated I/O effects")]
#![expect(
    clippy::disallowed_macros,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    reason = "unit test harnesses use ordinary Rust per programming-model.md, section 10.2"
)]

extern crate std;
use super::{
    ACK, Answer, CallId, Ended, Limits, PROMPT_TOOL, PROTOCOL_VERSIONS, Server, Tool, Up, Verdict, json,
    mcp_worst_case, secret_text,
};
use alloc::{boxed::Box, format, vec, vec::Vec};
use serde_json::{Value, json as value};
use skein_io::{self as io, Addr};
use skein_lib::{
    Queue, Time, Token,
    stream::{self, Down, OutputDown, OutputOutcome, OutputUp, Read},
};

const FIRST: Token = Token::new(10);
const LISTENER: Token = Token::new(100);
const OWNER: Token = Token::new(11);
const SOCKET: Token = Token::new(101);

fn limits() -> Limits {
    Limits {
        message_bytes: 4096,
        answer_bytes: 4096,
        schema_bytes: 4096,
        calls: 2,
        unauthenticated: Time::from_nanos(10),
    }
}

fn tool() -> Tool {
    Tool {
        name: b"finish".as_slice().into(),
        description: b"Finish the run".as_slice().into(),
        input_schema: br#"{"type":"object"}"#.as_slice().into(),
    }
}

fn address(port: u16) -> Addr {
    Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], port)))
}

fn drain(queue: &mut Queue<io::Request>) -> Vec<io::Request> {
    let mut requests = Vec::new();
    while let Some(request) = queue.pop() {
        requests.push(request);
    }
    requests
}

struct Harness {
    server: Server,
    below: Queue<io::Request>,
}

impl Harness {
    fn new() -> Self {
        Self::with_limits(limits())
    }
    fn with_limits(limits: Limits) -> Self {
        let mut below = Queue::with_capacity(64);
        let server = Server::new(FIRST, 0, &[7; 32], Box::new([tool()]), limits, &mut below).expect("tool list fits");
        Self { server, below }
    }
    fn event(&mut self, event: io::Event) {
        self.server.from_io(Time::ZERO, event, &mut self.below);
    }
    fn listen(&mut self) {
        self.event(io::Event::Listening { owner: FIRST, listener: LISTENER, addr: address(47100) });
        assert_eq!(self.server.up(), Some(Up::Listening { port: 47100 }));
    }
    fn accept(&mut self, socket: Token) {
        self.event(io::Event::Accepted { owner: FIRST, socket, peer: address(50000) });
    }
    fn connect(&mut self) {
        self.listen();
        self.accept(SOCKET);
        let line = json::join(&[self.server.secret(), b"\n"]);
        self.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(line) });
        self.acknowledge();
    }
    fn acknowledge(&mut self) {
        assert_eq!(self.server.up(), None);
        let records = drain(&mut self.below);
        assert!(!records.iter().any(|record| match record {
            io::Request::Stream { down: Down::Demand { read: Read::Line { max: 4096 }, .. }, .. } => true,
            _ => false,
        }));
        let right = records
            .into_iter()
            .find_map(|record| match record {
                io::Request::Output { down: OutputDown::Room { right, .. }, .. } => Some(right),
                _ => None,
            })
            .expect("acknowledgement room");
        self.event(io::Event::Output {
            owner: OWNER,
            up: OutputUp::Settled { right, outcome: OutputOutcome::Granted },
        });
        assert_eq!(self.server.up(), Some(Up::Connected));
        let records = drain(&mut self.below);
        let bytes = records
            .into_iter()
            .find_map(|record| match record {
                io::Request::Output { down: OutputDown::Send { bytes, .. }, .. } => Some(bytes),
                _ => None,
            })
            .expect("acknowledgement send");
        assert_eq!(bytes.as_ref(), ACK);
    }
    fn message(&mut self, message: &[u8]) {
        self.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(json::join(&[message, b"\n"])) });
    }
    fn response(&mut self) -> Value {
        for _ in 0_u8..20 {
            let records = drain(&mut self.below);
            for request in records {
                match request {
                    io::Request::Output { down: OutputDown::Room { right, .. }, .. } => self.event(io::Event::Output {
                        owner: OWNER,
                        up: OutputUp::Settled { right, outcome: OutputOutcome::Granted },
                    }),
                    io::Request::Output { down: OutputDown::Send { bytes, .. }, .. } => {
                        return serde_json::from_slice(&bytes).expect("reply JSON");
                    }
                    _ => {}
                }
            }
        }
        panic!("server has a response")
    }
    fn call(&mut self) -> CallId {
        self.message(br#"{"jsonrpc":"2.0","id":"opaque","method":"tools/call","params":{"name":"finish","arguments":{"answer":42}}}"#);
        let Some(Up::Call { call, tool, input }) = self.server.up() else { panic!("call upward") };
        assert_eq!(tool.as_ref(), b"finish");
        assert_eq!(serde_json::from_slice::<Value>(&input).expect("input JSON"), value!({"answer":42_u32}));
        call
    }
    fn prompt(&mut self) -> CallId {
        self.message(br#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"permission_prompt","arguments":{"tool_name":"Read","input":{"file_path":"file"},"tool_use_id":"toolu_1"}}}"#);
        let Some(Up::Prompt { call, tool_name, input, tool_use_id }) = self.server.up() else {
            panic!("prompt upward")
        };
        assert_eq!(tool_name.as_ref(), b"Read");
        assert_eq!(input.as_ref(), br#"{"file_path":"file"}"#);
        assert_eq!(tool_use_id.as_deref(), Some(b"toolu_1".as_slice()));
        call
    }
    fn end(&mut self) {
        self.event(io::Event::Closed { owner: FIRST });
        self.event(io::Event::Closed { owner: OWNER });
    }
}

#[test]
fn the_server_listens_on_loopback_only() {
    let mut h = Harness::new();
    assert_eq!(h.below.pop(), Some(io::Request::Listen { owner: FIRST, addr: address(0) }));
    assert!(h.server.owns(FIRST));
    assert!(h.server.owns(OWNER));
    assert!(h.server.owns(Token::new(12)));
    assert!(!h.server.owns(Token::new(13)));
}

#[test]
fn a_resolved_port_is_told_up() {
    Harness::new().listen();
}

#[test]
fn a_connection_without_the_secret_is_aborted() {
    for line in [b"bad\n".as_slice(), &[b'0'; 65], &[b'\n'; 65]] {
        let mut h = Harness::new();
        h.listen();
        h.accept(SOCKET);
        drain(&mut h.below);
        h.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(line.into()) });
        assert!(drain(&mut h.below).contains(&io::Request::Abort { entity: SOCKET }));
        assert_eq!(h.server.up(), None);
    }
}

#[test]
fn a_silent_connection_is_aborted_at_its_deadline() {
    let mut h = Harness::new();
    h.listen();
    h.accept(SOCKET);
    drain(&mut h.below);
    assert_eq!(h.server.next_deadline(), Some(Time::from_nanos(10)));
    h.server.fire(Time::from_nanos(9), &mut h.below);
    assert!(h.below.is_empty());
    h.server.fire(Time::from_nanos(10), &mut h.below);
    assert_eq!(h.below.pop(), Some(io::Request::Abort { entity: SOCKET }));
    assert_eq!(h.server.next_deadline(), None);
}

#[test]
fn a_third_pending_connection_is_rejected() {
    let mut h = Harness::new();
    h.listen();
    h.accept(SOCKET);
    h.accept(Token::new(102));
    drain(&mut h.below);
    h.accept(Token::new(103));
    assert_eq!(h.below.pop(), Some(io::Request::Reject { socket: Token::new(103) }));
}

#[test]
fn the_first_authenticated_connection_closes_the_listener() {
    let mut h = Harness::new();
    h.listen();
    h.accept(SOCKET);
    h.accept(Token::new(102));
    drain(&mut h.below);
    h.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(json::join(&[h.server.secret(), b"\n"])) });
    let requests = drain(&mut h.below);
    assert!(requests.contains(&io::Request::Close { entity: LISTENER }));
    assert!(requests.contains(&io::Request::Abort { entity: Token::new(102) }));
    assert_eq!(h.server.up(), None);
    assert_eq!(h.server.next_deadline(), None);
}

#[test]
fn initialize_echoes_a_supported_version() {
    for version in PROTOCOL_VERSIONS {
        let mut h = Harness::new();
        h.connect();
        h.message(
            format!(
                "{}",
                value!({"jsonrpc":"2.0","id":1_u32,"method":"initialize","params":{"protocolVersion":version}})
            )
            .as_bytes(),
        );
        let response = h.response();
        assert_eq!(response["result"]["protocolVersion"], version);
        assert_eq!(response["result"]["capabilities"]["tools"]["listChanged"], false);
        assert_eq!(response["result"]["serverInfo"]["name"], "smith");
    }
}

#[test]
fn initialize_answers_its_newest_version_to_an_unknown_one() {
    let mut h = Harness::new();
    h.connect();
    h.message(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"future"}}"#);
    assert_eq!(h.response()["result"]["protocolVersion"], PROTOCOL_VERSIONS[0]);
}

#[test]
fn the_tool_list_ends_with_the_prompt_tool() {
    let mut h = Harness::new();
    h.connect();
    h.message(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#);
    let response = h.response();
    let tools = response["result"]["tools"].as_array().expect("tools");
    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0]["name"], "finish");
    assert_eq!(tools[0]["inputSchema"], value!({"type":"object"}));
    assert_eq!(tools[1]["name"].as_str().expect("prompt").as_bytes(), PROMPT_TOOL);
    assert_eq!(tools[1]["inputSchema"]["required"], value!(["tool_name", "input"]));
}

#[test]
fn untyped_tool_schemas_gain_only_the_object_type() {
    for schema in [
        value!({}),
        value!({"properties":{"input":{"type":"string"}},"required":["input"]}),
        value!({"type":"object","properties":{"type":{"type":"string"}}}),
    ] {
        let mut tool = tool();
        tool.input_schema = serde_json::to_vec(&schema).expect("schema JSON").into();
        let bytes = json::tool_list(&[tool], limits().answer_bytes, limits().schema_bytes).expect("object schema");
        let list: Value = serde_json::from_slice(&bytes).expect("tool list");
        let mut expected = schema;
        expected["type"] = value!("object");
        assert_eq!(list["tools"][0]["inputSchema"], expected);
    }
}

#[test]
fn incompatible_tool_schemas_are_named_before_listening() {
    for schema in [
        br#"{"type":"string"}"#.as_slice(),
        br#"{"type":["object","null"]}"#,
        br#"{"type":"object","type":"string"}"#,
        br#"{"oneOf":[{"type":"object"}]}"#,
        br#"{"type":"object","oneOf":[]}"#,
        br#"{"type":"object","allOf":[]}"#,
        br#"{"type":"object","anyOf":[]}"#,
        b"true",
        b"[]",
        b"{",
    ] {
        let mut tool = tool();
        tool.name = b"host_action".as_slice().into();
        tool.input_schema = schema.into();
        let mut below = Queue::with_capacity(8);
        let refusal = Server::new(FIRST, 0, &[7; 32], Box::new([tool]), limits(), &mut below).expect_err("bad schema");
        match refusal {
            super::Refused::Schema { tool } => assert_eq!(tool.as_ref(), b"host_action"),
            super::Refused::TooLarge => panic!("schema refusal names the tool"),
        }
        assert!(below.is_empty());
    }
}

#[test]
fn normalized_schemas_stay_within_the_schema_bound() {
    let normalized = br#"{"type":"object"}"#;
    let schema_bytes = u32::try_from(normalized.len()).expect("normalized empty object length");
    let mut untyped = tool();
    untyped.input_schema = b"{}".as_slice().into();
    let mut below = Queue::with_capacity(8);
    let _server = Server::new(FIRST, 0, &[7; 32], Box::new([untyped]), Limits { schema_bytes, ..limits() }, &mut below)
        .expect("normalized schema at its bound");
    drain(&mut below);
    let mut untyped = tool();
    untyped.input_schema = b"{}".as_slice().into();
    let refusal = Server::new(
        FIRST,
        0,
        &[7; 32],
        Box::new([untyped]),
        Limits { schema_bytes: schema_bytes - 1, ..limits() },
        &mut below,
    )
    .expect_err("normalized schema one byte past its bound");
    match refusal {
        super::Refused::TooLarge => {}
        super::Refused::Schema { .. } => panic!("valid schema exceeds its size bound"),
    }
    assert!(below.is_empty());
}

#[test]
fn a_call_is_told_up_then_answered() {
    let mut h = Harness::new();
    h.connect();
    let call = h.call();
    h.server.answer(call, Answer::Text(b"accepted".as_slice().into()), &mut h.below);
    let response = h.response();
    assert_eq!(response["id"], "opaque");
    assert_eq!(response["result"]["content"][0]["text"], "accepted");
    assert_eq!(response["result"]["isError"], false);
}

#[test]
fn a_prompt_is_told_up_with_its_tool_use_id() {
    let mut h = Harness::new();
    h.connect();
    h.prompt();
}

#[test]
fn an_allowed_prompt_answers_allow_with_its_input() {
    let mut h = Harness::new();
    h.connect();
    let call = h.prompt();
    h.server.verdict(
        call,
        Verdict::Allow { updated_input: br#"{"file_path":"file"}"#.as_slice().into() },
        &mut h.below,
    );
    let response = h.response();
    let text: Value =
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().expect("text")).expect("verdict JSON");
    assert_eq!(text, value!({"behavior":"allow","updatedInput":{"file_path":"file"}}));
    assert_eq!(response["result"]["isError"], false);
}

#[test]
fn a_denied_prompt_answers_deny_with_its_message() {
    let mut h = Harness::new();
    h.connect();
    let call = h.prompt();
    h.server.verdict(call, Verdict::Deny { message: b"denied".as_slice().into() }, &mut h.below);
    let response = h.response();
    let text: Value =
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().expect("text")).expect("verdict JSON");
    assert_eq!(text, value!({"behavior":"deny","message":"denied"}));
    assert_eq!(response["result"]["isError"], false);
}

fn error_response(message: &[u8]) -> Value {
    let mut h = Harness::new();
    h.connect();
    h.message(message);
    h.response()
}

#[test]
fn an_unknown_method_answers_32601() {
    assert_eq!(error_response(br#"{"jsonrpc":"2.0","id":1,"method":"other"}"#)["error"]["code"], -32_601_i32);
}

#[test]
fn an_unknown_tool_answers_32602() {
    for params in [value!({"name":"unknown"}), value!({})] {
        let line = serde_json::to_vec(&value!({"jsonrpc":"2.0","id":1_u32,"method":"tools/call","params":params}))
            .expect("request JSON");
        assert_eq!(error_response(&line)["error"]["code"], -32_602_i32);
    }
}

#[test]
fn an_unreadable_line_answers_32700() {
    for line in [b"not json".as_slice(), br#"{"jsonrpc":"2.0"} true"#] {
        let response = error_response(line);
        assert_eq!(response["error"]["code"], -32_700_i32);
        assert_eq!(response["id"], Value::Null);
    }
}

#[test]
fn a_batch_answers_32600() {
    assert_eq!(error_response(b"[]")["error"]["code"], -32_600_i32);
}

#[test]
fn a_line_past_its_bound_fails_the_connection() {
    let mut h = Harness::with_limits(Limits { message_bytes: 16, ..limits() });
    h.connect();
    h.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(Box::new([b'x'; 16])) });
    assert!(drain(&mut h.below).contains(&io::Request::Abort { entity: SOCKET }));
    h.end();
    assert_eq!(h.server.up(), Some(Up::Ended(Ended::Failed)));
}

#[test]
fn calls_past_their_bound_wait_for_room() {
    let mut h = Harness::with_limits(Limits { calls: 1, ..limits() });
    h.connect();
    let call = h.call();
    assert!(!drain(&mut h.below).iter().any(|request| match request {
        io::Request::Stream { down: Down::Demand { read: Read::Line { .. }, .. }, .. } => true,
        _ => false,
    }));
    h.server.answer(call, Answer::Text(b"done".as_slice().into()), &mut h.below);
    assert!(drain(&mut h.below).iter().any(|request| match request {
        io::Request::Stream { down: Down::Demand { read: Read::Line { .. }, .. }, .. } => true,
        _ => false,
    }));
}

#[test]
fn an_answer_past_its_bound_is_an_error() {
    let mut h = Harness::new();
    h.connect();
    let call = h.call();
    h.server.answer(call, Answer::Text(vec![b'x'; 4096].into_boxed_slice()), &mut h.below);
    let response = h.response();
    assert_eq!(response["result"]["isError"], true);
    assert_eq!(response["result"]["content"][0]["text"], "smith: the answer passed its bound");
}

#[test]
fn a_tool_list_past_its_bound_is_refused() {
    let mut below = Queue::with_capacity(8);
    let result =
        Server::new(FIRST, 0, &[7; 32], Box::new([tool()]), Limits { answer_bytes: 64, ..limits() }, &mut below);
    let _refusal = result.expect_err("tool-list answer exceeds its bound");
    assert!(below.is_empty());
}

#[test]
fn a_tool_schema_is_bounded_before_listening() {
    let schema_bytes = u32::try_from(tool().input_schema.len()).expect("schema length");
    let mut below = Queue::with_capacity(8);
    let _server = Server::new(FIRST, 0, &[7; 32], Box::new([tool()]), Limits { schema_bytes, ..limits() }, &mut below)
        .expect("schema at its bound");
    assert!(match below.pop() {
        Some(io::Request::Listen { .. }) => true,
        _ => false,
    });
    let mut oversized = tool();
    oversized.input_schema = json::join(&[&oversized.input_schema, b" "]);
    let _refusal =
        Server::new(FIRST, 0, &[7; 32], Box::new([oversized]), Limits { schema_bytes, ..limits() }, &mut below)
            .expect_err("valid schema one byte past its bound");
    assert!(below.is_empty(), "refusal starts no listener");
}

#[test]
fn schema_collection_prices_its_ceiling() {
    let larger = Limits { schema_bytes: 8192, ..limits() };
    let increase = json::worst_case(larger.schema_bytes)
        .expect("larger schema scratch")
        .checked_sub(json::worst_case(limits().schema_bytes).expect("schema scratch"))
        .expect("larger scratch");
    assert_eq!(
        mcp_worst_case(&larger).expect("larger memory").checked_sub(mcp_worst_case(&limits()).expect("memory")),
        Some(increase),
    );
}

#[test]
fn collection_scratch_is_shared_between_phases() {
    let smaller_schema = Limits { schema_bytes: 2048, ..limits() };
    assert_eq!(mcp_worst_case(&smaller_schema), mcp_worst_case(&limits()), "request scratch dominates");
    let smaller_message = Limits { message_bytes: 2048, ..limits() };
    assert_eq!(
        mcp_worst_case(&limits()).expect("memory").checked_sub(mcp_worst_case(&smaller_message).expect("memory")),
        Some(2048 * 12),
        "schema scratch dominates; only the retained message allowance changes",
    );
}

#[test]
fn a_ping_answers_empty() {
    assert_eq!(error_response(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)["result"], value!({}));
}

#[test]
fn notifications_are_not_answered() {
    let mut h = Harness::new();
    h.connect();
    for message in [
        br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#.as_slice(),
        br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#,
        br#"{"jsonrpc":"2.0","id":3,"result":{}}"#,
    ] {
        h.message(message);
        assert!(!drain(&mut h.below).iter().any(|r| match r {
            io::Request::Output { .. } => true,
            _ => false,
        }));
        assert_eq!(h.server.up(), None);
    }
}

#[test]
fn a_late_answer_is_ignored() {
    let mut h = Harness::new();
    h.connect();
    let call = h.call();
    h.server.answer(call, Answer::Text(b"done".as_slice().into()), &mut h.below);
    h.response();
    drain(&mut h.below);
    h.server.answer(call, Answer::Text(b"late".as_slice().into()), &mut h.below);
    assert!(h.below.is_empty());
}

#[test]
fn the_secret_is_absent_from_its_debug_view() {
    let h = Harness::new();
    let debug = format!("{:?}", h.server);
    assert!(!debug.contains(core::str::from_utf8(h.server.secret()).expect("hex")));
    assert!(debug.contains("secret_length: 64"));
    assert_eq!(secret_text(&[7; 32]), *b"0707070707070707070707070707070707070707070707070707070707070707");
}

#[test]
fn the_peer_s_end_closes_the_server() {
    let mut h = Harness::new();
    h.connect();
    h.event(io::Event::Stream { owner: OWNER, up: stream::Up::End });
    assert!(drain(&mut h.below).contains(&io::Request::Close { entity: SOCKET }));
    assert_eq!(h.server.up(), None);
    h.end();
    assert_eq!(h.server.up(), Some(Up::Ended(Ended::Closed)));
    h.server.close(&mut h.below);
    assert_eq!(h.server.up(), None);
}

#[test]
fn json_escaping_keeps_utf8() {
    let text = b"quote\" slash\\ controls\x00\x1f\n\r\t \xc3\xa9";
    let quoted = json::quote(text, 1024).expect("quoted");
    let decoded: alloc::string::String = serde_json::from_slice(&quoted).expect("string");
    assert_eq!(decoded.as_bytes(), text);
}

#[test]
fn nested_arguments_keep_their_values() {
    let mut h = Harness::new();
    h.connect();
    h.message(br#"{"jsonrpc":"2.0","id":"a\"b","method":"tools/call","params":{"name":"finish","arguments":{"array":[true,null,2.5,{"text":"\u00e9"}],"method":"ping"}}}"#);
    let Some(Up::Call { call, input, .. }) = h.server.up() else { panic!("call") };
    assert_eq!(
        serde_json::from_slice::<Value>(&input).expect("input"),
        value!({"array":[true,null,2.5_f64,{"text":"é"}],"method":"ping"})
    );
    h.server.answer(call, Answer::Error(b"tool failed".as_slice().into()), &mut h.below);
    let response = h.response();
    assert_eq!(response["id"], "a\"b");
    assert_eq!(response["result"]["isError"], true);
}

#[test]
fn an_id_past_its_bound_answers_null() {
    let message = serde_json::to_vec(&value!({"jsonrpc":"2.0","id":"x".repeat(63),"method":"ping"})).expect("request");
    let response = error_response(&message);
    assert_eq!(response["id"], Value::Null);
    assert_eq!(response["error"]["code"], -32_600_i32);
}

#[test]
fn worst_case_rejects_overflow() {
    assert!(mcp_worst_case(&limits()).expect("memory") > 4096);
    assert_eq!(mcp_worst_case(&Limits { message_bytes: u32::MAX, ..limits() }), None);
}

#[test]
fn the_server_acknowledges_an_authenticated_connection() {
    let mut h = Harness::new();
    h.connect();
}

#[test]
fn a_cancelled_call_still_receives_its_answer() {
    let mut h = Harness::new();
    h.connect();
    let call = h.call();
    h.message(br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"opaque"}}"#);
    assert_eq!(h.server.up(), None);
    h.server.answer(call, Answer::Text(b"completed".as_slice().into()), &mut h.below);
    assert_eq!(h.response()["result"]["content"][0]["text"], "completed");
}

#[test]
fn queued_answers_pause_reading_until_output_room_returns() {
    let mut h = Harness::with_limits(Limits { calls: 1, ..limits() });
    h.connect();
    h.message(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#);
    let records = drain(&mut h.below);
    assert!(records.iter().any(|r| match r {
        io::Request::Stream { down: Down::Demand { read: Read::Line { .. }, .. }, .. } => true,
        _ => false,
    }));
    let right = records
        .into_iter()
        .find_map(|r| match r {
            io::Request::Output { down: OutputDown::Room { right, .. }, .. } => Some(right),
            _ => None,
        })
        .expect("first answer room");
    h.message(br#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#);
    assert!(!drain(&mut h.below).iter().any(|r| match r {
        io::Request::Stream { down: Down::Demand { read: Read::Line { .. }, .. }, .. } => true,
        _ => false,
    }));
    h.event(io::Event::Output { owner: OWNER, up: OutputUp::Settled { right, outcome: OutputOutcome::Granted } });
    assert!(drain(&mut h.below).iter().any(|r| match r {
        io::Request::Stream { down: Down::Demand { read: Read::Line { .. }, .. }, .. } => true,
        _ => false,
    }));
}

#[test]
fn crlf_empty_deliveries_are_skipped() {
    let mut h = Harness::new();
    h.connect();
    for bytes in [b"\r".as_slice(), b"\n"] {
        h.event(io::Event::Stream { owner: OWNER, up: stream::Up::Bytes(bytes.into()) });
        assert!(!drain(&mut h.below).iter().any(|r| match r {
            io::Request::Output { .. } => true,
            _ => false,
        }));
    }
    h.message(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#);
    assert_eq!(h.response()["result"], value!({}));
}
