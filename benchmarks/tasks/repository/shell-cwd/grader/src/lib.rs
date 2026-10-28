//! Independent behavioral oracle. Public entrances/effects only; no feature representation assumed.
use skein_lib::{Duration, Env, Queue, ReplyTo, Time, Token, Wall};
use smith_domain::{llm, run, tools};
use smith_protocol_machine::{Below, Component, FromDomain};

const ALL: tools::Grants = tools::Grants { inspect: true, modify: true, shell: true };
fn limits() -> tools::Limits {
    tools::Limits { kits: 2, calls: 4, repos: 4, path_bytes: 128, known_files: 8,
        file_bytes: 1024, read_bytes: 256, list_entries: 16, list_bytes: 4096,
        match_lines: 4, file_timeout: Duration::from_secs(10), env_bytes: 256,
        shell_timeout: Duration::from_secs(60), shell_timeout_max: Duration::from_secs(600),
        shell_head: 64, shell_tail: 128, search_hits: 8, search_bytes: 256,
        search_timeout: Duration::from_secs(30), facts: 128 }
}
fn decode_value(value: serde_json::Value, grants: tools::Grants) -> llm::Decoded {
    let input = serde_json::to_vec(&value).unwrap();
    let bounds = smith_protocol_llm::Limits { client: skein_llm_world::limits(), tool_bytes: 65536, result_bytes: 65536 };
    smith_protocol_llm::decode(b"shell", &input, grants, &[], &bounds)
}
fn shell(cwd: Option<&str>, timeout: Option<u32>) -> tools::Call {
    let mut value = serde_json::json!({"command":"printf repository_oracle"});
    if let Some(cwd) = cwd { value["cwd"] = serde_json::json!(cwd); }
    if let Some(timeout) = timeout { value["timeout"] = serde_json::json!(timeout); }
    match decode_value(value, ALL) { llm::Decoded::Owned { call } => call, other => panic!("expected owned shell, got {other:?}") }
}
fn names(path: &str) -> Box<[tools::Name]> {
    path.split('/').filter(|part| !part.is_empty()).map(|part| tools::Name::new(part.as_bytes().into()).unwrap()).collect()
}
fn authority(cwd: &str, grants: tools::Grants) -> tools::Authority {
    tools::Authority { cwd: names(cwd), repos: Box::new([
        tools::Repo { mount: names("/work"), root: Token::new(10), writable: true },
        tools::Repo { mount: names("/work/src/vendor"), root: Token::new(20), writable: false },
        tools::Repo { mount: names("/other"), root: Token::new(30), writable: true },
    ]), grants, env: Box::new([tools::Var { name: b"ORACLE_ENV".as_slice().into(), value: b"unchanged".as_slice().into() }]) }
}
struct Harness { domain: tools::Domain, env: Env<tools::Limits>, out: Queue<tools::Request>, grants: Vec<(Token, tools::Grants)> }
impl Harness {
    fn new() -> Self {
        let limits = limits();
        assert!(tools::valid(&limits));
        Self { domain: tools::Domain::new(&limits), env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits }, out: Queue::with_capacity(tools::max_out(&limits)), grants: Vec::new() }
    }
    fn emit(&mut self, event: tools::Event) -> Vec<tools::Request> {
        tools::step(&mut self.domain, &self.env, event, &mut self.out);
        let mut requests = Vec::new();
        while let Some(request) = self.out.pop() { requests.push(request); }
        self.domain.reclaim();
        requests
    }
    fn one(&mut self, event: tools::Event) -> tools::Request {
        let mut output = self.emit(event); assert_eq!(output.len(), 1, "exactly one effect for this entrance"); output.remove(0)
    }
    fn open(&mut self, session: u64, cwd: &str, grants: tools::Grants) -> Token {
        match self.one(tools::Event::Open { session: Token::new(session), authority: authority(cwd, grants) }) {
            tools::Request::Opened { session: echoed, kit } => { assert_eq!(echoed, Token::new(session)); self.grants.push((kit, grants)); kit }, other => panic!("open refused {other:?}") }
    }
    fn call(&mut self, kit: Token, call: tools::Call, deadline: u64) -> tools::Request {
        self.one(tools::Event::Call { kit, reply_to: ReplyTo::new(Token::new(99)), call, deadline: Time::ZERO.saturating_add(Duration::from_secs(deadline)) })
    }
    fn spawn(&mut self, kit: Token, call: tools::Call, root: u64, path: &[u8], duration: u64) -> (Token, tools::Op) {
        match self.call(kit, call, 1000) {
            tools::Request::Io { owner, op, deadline } => {
                assert_eq!(deadline, Time::ZERO.saturating_add(Duration::from_secs(duration)));
                match &op { tools::Op::Spawn { cwd, command, env, roots, head, tail } => {
                    assert_eq!(cwd, &tools::Place { root: Token::new(root), path: path.into() });
                    assert_eq!(command.as_ref(), b"printf repository_oracle");
                    assert_eq!(env.as_ref(), authority("/work/src", ALL).env.as_ref());
                    assert_eq!((*head, *tail), (64, 128));
                    let grants = self.grants.iter().find(|(token, _)| *token == kit).unwrap().1;
                    assert_eq!(roots.as_ref(), [tools::Root { root: Token::new(10), writable: grants.modify }, tools::Root { root: Token::new(20), writable: false }, tools::Root { root: Token::new(30), writable: grants.modify }]);
                }, other => panic!("wrong effect {other:?}") }
                (owner, op)
            }, other => panic!("expected Spawn {other:?}") }
    }
    fn finish(&mut self, owner: Token) {
        match self.one(tools::Event::Done { owner, done: tools::Done::Exited { exit: tools::Exit::Code { code: 0 }, head: b"ok".as_slice().into(), tail: Box::new([]), dropped: 0 } }) {
            tools::Request::Answer { to, outcome: tools::Outcome::Exited { exit, head, tail, dropped } } => {
                assert_eq!(to.into_token(), Token::new(99)); assert_eq!(exit, tools::Exit::Code { code: 0 }); assert_eq!(head.as_ref(), b"ok"); assert!(tail.is_empty()); assert_eq!(dropped, 0);
            }, other => panic!("wrong terminal {other:?}") }
        assert_eq!(self.domain.jobs(), 0);
    }
}
fn schema(grants: tools::Grants) -> Option<serde_json::Value> {
    let prompt = llm::Prompt { endpoint: llm::Endpoint(1), model: b"unused".as_slice().into(), system: Box::new([]), tools: grants, served: Box::new([]), messages: Box::new([]), max_tokens: 1 };
    smith_protocol_llm::schemas(&prompt).iter().find(|tool| tool.name.as_ref() == b"shell").map(|tool| serde_json::from_slice(&tool.schema).unwrap())
}
#[test] fn control_public_shell_constructor_and_omission() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let control = tools::Call::Shell { command: b"printf repository_oracle".as_slice().into(), timeout: None };
    assert_eq!(tools::tool(&control), tools::Tool::Shell); assert_eq!(tools::effect(&control), tools::Effect::Write);
    let (owner, _) = h.spawn(kit, control, 10, b"src", 60); h.finish(owner);
    let (owner, _) = h.spawn(kit, shell(None, None), 10, b"src", 60); h.finish(owner);
}
#[test] fn control_existing_schema_and_grant() {
    let schema = schema(ALL).unwrap(); assert_eq!(schema["properties"]["command"]["type"], "string");
    assert!(schema["required"].as_array().unwrap().iter().any(|v| v == "command"));
    let denied = tools::Grants { shell: false, ..ALL }; assert!(self::schema(denied).is_none());
    assert!(matches!(decode_value(serde_json::json!({"command":"true"}), denied), llm::Decoded::Invalid { .. }));
}
#[test] fn control_timeout_clamping_and_deadline() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    for (timeout, duration) in [(None, 60), (Some(3), 3), (Some(10000), 600)] {
        let (owner, _) = h.spawn(kit, shell(None, timeout), 10, b"src", duration); h.finish(owner);
    }
    match h.call(kit, shell(None, None), 0) { tools::Request::Answer { to, outcome: tools::Outcome::TimedOut } => assert_eq!(to.into_token(), Token::new(99)), other => panic!("deadline {other:?}") }
}
#[test] fn feature_optional_string_schema() {
    let schema = schema(ALL).unwrap(); assert_eq!(schema["properties"]["cwd"]["type"], "string");
    assert!(!schema["required"].as_array().unwrap().iter().any(|v| v == "cwd"));
}
#[test] fn feature_relative_absolute_normalization_and_deepest_mount() {
    let cases = [(".", 10, "src"), ("crates/agent", 10, "src/crates/agent"),
        ("./crates/../tests//", 10, "src/tests"), ("../../work/src", 10, "src"),
        ("/other/pkg", 30, "pkg"), ("vendor/lib", 20, "lib"), ("/work/src/vendor", 20, ""),
        ("/work/src/vendor/../own", 10, "src/own"), ("caf\u{e9}/\u{65e5}\u{672c}", 10, "src/caf\u{e9}/\u{65e5}\u{672c}")];
    for (cwd, root, path) in cases {
        let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
        let (owner, _) = h.spawn(kit, shell(Some(cwd), None), root, path.as_bytes(), 60); h.finish(owner);
    }
}
#[test] fn feature_cwd_does_not_persist_and_kits_are_independent() {
    let mut h = Harness::new(); let first = h.open(1, "/work/src", ALL); let second = h.open(2, "/other/original", ALL);
    for (kit, cwd, root, path) in [(first, Some("/other/changed"), 30, "changed"), (first, None, 10, "src"),
        (second, Some("/work/temp"), 10, "temp"), (second, None, 30, "original"), (first, None, 10, "src")] {
        let (owner, _) = h.spawn(kit, shell(cwd, None), root, path.as_bytes(), 60); h.finish(owner);
    }
}
#[test] fn feature_invalid_types_and_nul_are_rejected_before_io() {
    for cwd in [serde_json::Value::Null, serde_json::json!(true), serde_json::json!(12), serde_json::json!([]), serde_json::json!({})] {
        assert!(matches!(decode_value(serde_json::json!({"command":"true","cwd":cwd}), ALL), llm::Decoded::Invalid { .. }), "malformed cwd must not decode as a valid shell");
    }
}
#[test] fn feature_outside_and_overlong_have_one_refusal_without_spawn() {
    for cwd in ["/outside".to_owned(), "../../../outside".to_owned(), "/workmate/path".to_owned(), format!("/work/{}", "x".repeat(129))] {
        let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
        match decode_value(serde_json::json!({"command":"true","cwd":cwd}), ALL) {
            llm::Decoded::Invalid { .. } => {},
            llm::Decoded::Owned { call } => match h.call(kit, call, 1000) { tools::Request::Answer { to, outcome: tools::Outcome::Outside | tools::Outcome::TooLong | tools::Outcome::TooLarge { .. } } => assert_eq!(to.into_token(), Token::new(99)), other => panic!("must refuse before spawn {other:?}") },
            other => panic!("not a shell {other:?}") }
        assert_eq!(h.domain.jobs(), 0);
    }
}
#[test] fn feature_explicit_cwd_keeps_effect_and_grants() {
    let call = shell(Some("/other"), None); assert_eq!(tools::tool(&call), tools::Tool::Shell); assert_eq!(tools::effect(&call), tools::Effect::Write);
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", tools::Grants { shell: false, ..ALL });
    match h.call(kit, call, 1000) { tools::Request::Answer { to, outcome: tools::Outcome::NotGranted } => assert_eq!(to.into_token(), Token::new(99)), other => panic!("grant {other:?}") }
}
#[test] fn feature_read_only_authority_environment_and_timeout_are_preserved() {
    let mut h = Harness::new(); let grants = tools::Grants { inspect: false, modify: false, shell: true }; let kit = h.open(1, "/work/src", grants);
    for (timeout, duration) in [(None, 60), (Some(2), 2), (Some(10000), 600)] {
        let (owner, op) = h.spawn(kit, shell(Some("vendor/inside"), timeout), 20, b"inside", duration);
        match op { tools::Op::Spawn { roots, .. } => assert!(roots.iter().all(|root| !root.writable)), _ => unreachable!() }; h.finish(owner);
    }
    match h.call(kit, shell(Some("vendor/inside"), None), 4) { tools::Request::Io { owner, deadline, .. } => { assert_eq!(deadline, Time::ZERO.saturating_add(Duration::from_secs(4))); h.finish(owner); }, other => panic!("deadline {other:?}") }
}
#[test] fn feature_machine_receives_selected_root_directory_and_unmodified_command() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let (owner, op) = h.spawn(kit, shell(Some("vendor/lib"), None), 20, b"lib", 60);
    let bounds = smith_protocol_machine::Limits { operations: 4, roots: 4, path_bytes: 128, file_bytes: 1024, entries: 16, entry_bytes: 4096, processes: 4, output_bytes: 256, search_hits: 8, search_bytes: 256, search_line_bytes: 512, env_bytes: 512, stop_grace: Duration::from_millis(10) };
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: bounds }; let mut component = Component::new(&bounds);
    component.workspace(&run::Workspace { directories: [10,20,30].into_iter().map(|root| run::Directory { name: format!("root{root}").into_bytes().into(), root: Token::new(root), writable: root != 20, git: true, conflicts: Box::new([]) }).collect() });
    let mut answers = Queue::with_capacity(8); let mut below = Queue::with_capacity(8);
    component.from_domain(&env, FromDomain::Op { owner, op, deadline: Time::ZERO.saturating_add(Duration::from_secs(60)) }, &mut answers, &mut below);
    assert!(answers.is_empty()); match below.pop() { Some(Below::Spawn { root, spawn, .. }) => { assert_eq!(root, Token::new(20)); assert_eq!(spawn.dir.as_ref(), b"lib"); assert_eq!(spawn.program.as_ref(), b"/bin/sh"); assert_eq!(spawn.args[0].as_ref(), b"-c"); assert_eq!(spawn.args[1].as_ref(), b"printf repository_oracle"); assert_eq!(spawn.env.as_ref(), [Box::from(&b"ORACLE_ENV=unchanged"[..])]); }, other => panic!("machine effect {other:?}") }; assert!(below.is_empty());
    h.finish(owner);
}
#[test] fn feature_new_path_ownership_is_charged() {
    let omitted = shell(None, None).owned_bytes().unwrap();
    let short = shell(Some("x"), None).owned_bytes().unwrap(); let long = shell(Some(&"x".repeat(50)), None).owned_bytes().unwrap();
    assert!(short >= omitted + 1, "new path byte is not free"); assert!(long >= short + 49, "all newly retained name bytes are owned");
    let parts = shell(Some("x/y/z"), None).owned_bytes().unwrap(); assert!(parts >= omitted + 3);
}
#[test] fn feature_cancel_close_waits_for_one_io_terminal_and_reclaims() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let (owner, _) = h.spawn(kit, shell(Some("/other/job"), None), 30, b"job", 60);
    match h.one(tools::Event::Close { kit }) { tools::Request::CancelIo { owner: echoed } => assert_eq!(echoed, owner), other => panic!("cancel {other:?}") }
    let output = h.emit(tools::Event::Done { owner, done: tools::Done::Cancelled }); assert_eq!(output.len(), 2);
    assert_eq!(output.iter().filter(|item| matches!(item, tools::Request::Answer { .. })).count(), 1);
    assert_eq!(output.iter().filter(|item| matches!(item, tools::Request::Closed { .. })).count(), 1);
    for item in output { match item { tools::Request::Answer { to, outcome: tools::Outcome::Cancelled } => assert_eq!(to.into_token(), Token::new(99)), tools::Request::Closed { session } => assert_eq!(session, Token::new(1)), other => panic!("wrong cancellation terminal {other:?}") } }
    assert_eq!(h.domain.jobs(), 0); assert_eq!(h.domain.kits(), 0);
}
#[test] fn feature_shell_cwd_does_not_forge_read_knowledge() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let (owner, _) = h.spawn(kit, shell(Some("/other/readme"), None), 30, b"readme", 60); h.finish(owner);
    let path = tools::Path { absolute: false, parts: Box::new([tools::Part::Name { name: tools::Name::new(b"README".as_slice().into()).unwrap() }]) };
    let call = tools::Call::Edit { path, old: b"old".as_slice().into(), new: b"new".as_slice().into(), all: false };
    match h.call(kit, call, 1000) { tools::Request::Answer { outcome: tools::Outcome::NotRead, .. } => {}, other => panic!("shell cannot attest file contents {other:?}") }
}

#[test] fn feature_normalized_path_limit_is_inclusive() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let tail = "x".repeat(123); let cwd = format!("/work/{tail}");
    let (owner, _) = h.spawn(kit, shell(Some(&cwd), None), 10, tail.as_bytes(), 60); h.finish(owner);
    let cwd = format!("/work/{}", "x".repeat(124));
    match decode_value(serde_json::json!({"command":"true", "cwd":cwd}), ALL) {
        llm::Decoded::Invalid { .. } => {},
        llm::Decoded::Owned { call } => match h.call(kit, call, 1000) { tools::Request::Answer { to, outcome: tools::Outcome::TooLong | tools::Outcome::TooLarge { .. } } => assert_eq!(to.into_token(), Token::new(99)), other => panic!("above normalized path cap {other:?}") },
        other => panic!("not an owned shell {other:?}") }
    assert_eq!(h.domain.jobs(), 0);
}

#[test] fn feature_nul_rejection_can_belong_to_decoder_or_domain() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    match decode_value(serde_json::json!({"command":"true", "cwd":"bad\0dir"}), ALL) {
        llm::Decoded::Invalid { .. } => {},
        llm::Decoded::Owned { call } => match h.call(kit, call, 1000) {
            tools::Request::Answer { to, outcome } => { assert_eq!(to.into_token(), Token::new(99)); assert!(matches!(outcome, tools::Outcome::NulByte | tools::Outcome::TooLong | tools::Outcome::TooLarge { .. } | tools::Outcome::Outside | tools::Outcome::Failed { .. }), "cwd ends with a refusal"); },
            other => panic!("NUL must be rejected before any IO {other:?}") },
        other => panic!("not an owned shell {other:?}") }
    assert_eq!(h.domain.jobs(), 0);
}

#[test] fn feature_existing_read_version_survives_cwd_selection() {
    let mut h = Harness::new(); let kit = h.open(1, "/work/src", ALL);
    let path = tools::Path { absolute: false, parts: Box::new([tools::Part::Name { name: tools::Name::new(b"README".as_slice().into()).unwrap() }]) };
    let owner = match h.call(kit, tools::Call::Read { path: path.clone(), skip: 0, lines: None }, 1000) { tools::Request::Io { owner, op: tools::Op::Load { .. }, .. } => owner, other => panic!("read {other:?}") };
    match h.one(tools::Event::Done { owner, done: tools::Done::Loaded { content: b"old".as_slice().into(), version: tools::Version::new([1,2,3,4]) } }) { tools::Request::Answer { outcome: tools::Outcome::Read { .. }, .. } => {}, other => panic!("read terminal {other:?}") }
    let (owner, _) = h.spawn(kit, shell(Some("/other/job"), None), 30, b"job", 60); h.finish(owner);
    let edit = tools::Call::Edit { path, old: b"old".as_slice().into(), new: b"new".as_slice().into(), all: false };
    let owner = match h.call(kit, edit, 1000) { tools::Request::Io { owner, op: tools::Op::Load { at, .. }, .. } => { assert_eq!(at, tools::Place { root: Token::new(10), path: b"src/README".as_slice().into() }); owner }, other => panic!("existing read version remains known {other:?}") };
    match h.one(tools::Event::Done { owner, done: tools::Done::Cancelled }) { tools::Request::Answer { outcome: tools::Outcome::Cancelled, .. } => {}, other => panic!("edit settles {other:?}") }
}
