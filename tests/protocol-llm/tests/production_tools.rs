//! Smith's production schema and decoder, exercised independently of the
//! scripted provider's finite fixture language.

use smith_domain::{llm, run, tools};
use smith_protocol_llm_world::adapter::{self as adapter, Limits, ToolKind};

fn limits() -> Limits {
    Limits {
        client: skein_llm_world::limits(),
        tool_bytes: 32768,
        history_bytes: u64::from(skein_llm_world::limits().request),
        rendered_result: skein_llm_world::limits().strings,
        stated: smith_protocol_llm_world::adapter::stated(),
    }
}

fn grants() -> tools::Grants {
    tools::Grants { inspect: true, modify: true, shell: true }
}

fn prompt() -> llm::Prompt {
    llm::Prompt {
        effort: None,
        purpose: llm::Purpose::Ordinary { guard: None },
        choice: smith_domain::llm::ToolChoice::Auto,
        namespace: smith_domain::tools::Namespace::One,
        affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
        asker: llm::Asker::Main,
        endpoint: llm::Endpoint(1),
        model: b"test".as_slice().into(),
        system: Box::new([]),
        tools: grants(),
        served: Box::new([llm::Served::Wait, llm::Served::SubAgent]),
        messages: Box::new([]),
        max_tokens: 16,
    }
}

#[test]
fn production_schemas_cover_every_offered_workspace_and_run_tool() {
    let schemas = adapter::schemas(&prompt(), &limits().stated);
    assert_eq!(schemas.len(), 10);
    for kind in [
        ToolKind::Owned(tools::Tool::Read),
        ToolKind::Owned(tools::Tool::List),
        ToolKind::Owned(tools::Tool::Search),
        ToolKind::Owned(tools::Tool::Write),
        ToolKind::Owned(tools::Tool::Edit),
        ToolKind::Owned(tools::Tool::Remove),
        ToolKind::Owned(tools::Tool::Shell),
        ToolKind::Owned(tools::Tool::Job),
        ToolKind::Wait,
        ToolKind::SubAgent,
    ] {
        let schema = schemas.iter().find(|schema| schema.kind == kind).expect("offered schema");
        assert!(schema.description.len() > 20);
        let document = skein_llm::Json::from_bytes(&schema.schema, &limits().client.native().request_document())
            .expect("production schema is bounded valid JSON");
        assert_eq!(document.document().token(0).map(|record| record.kind), Some(skein_json::Kind::ObjectStart));
    }
}

#[test]
fn each_workspace_call_decodes_escaped_input_and_ignores_unknown_members() {
    let limits = limits();
    let cases = [
        (
            b"read".as_slice(),
            br#"{"path":"src/a.rs","offset":3,"window":12,"future":[1,{"x":true}]}"#.as_slice(),
            tools::Tool::Read,
        ),
        (b"remove", br#"{"path":"a"}"#, tools::Tool::Remove),
        (b"list", br#"{"path":".","future":false}"#, tools::Tool::List),
        (b"search", br#"{"pattern":"a\\nb","glob":"*.rs"}"#, tools::Tool::Search),
        (b"write", br#"{"path":"a","content":"line\nnext"}"#, tools::Tool::Write),
        (b"edit", br#"{"path":"a","edits":[{"old":"before","new":"after","all":true}]}"#, tools::Tool::Edit),
        (b"shell", br#"{"command":"printf ok","timeout_seconds":5,"output_bytes":99}"#, tools::Tool::Shell),
    ];
    for (name, input, expected) in cases {
        let decoded = adapter::decode(name, input, grants(), &[], llm::Asker::Main, &limits);
        match decoded {
            llm::Decoded::Owned { call } => {
                assert_eq!(tools::tool(&call), expected, "{}", String::from_utf8_lossy(name));
            }
            other @ (llm::Decoded::Builtin { .. } | llm::Decoded::Served { .. } | llm::Decoded::Invalid { .. }) => {
                panic!("{}: {other:?}", String::from_utf8_lossy(name));
            }
        }
    }
}

#[test]
fn malformed_workspace_calls_have_field_named_typed_problems_then_correct() {
    let limits = limits();
    let cases = [
        (b"read".as_slice(), br#"{"path":12}"#.as_slice(), b"path".as_slice()),
        (b"list", br"{}", b"path"),
        (b"search", br#"{"pattern":false}"#, b"pattern"),
        (b"write", br#"{"path":"a"}"#, b"content"),
        (b"edit", br#"{"path":"a","edits":[{"old":"x"}]}"#, b"new"),
        (b"shell", br#"{"command":"x","timeout_seconds":-1}"#, b"timeout_seconds"),
    ];
    for (name, input, field) in cases {
        let decoded = adapter::decode(name, input, grants(), &[], llm::Asker::Main, &limits);
        match decoded {
            llm::Decoded::Invalid {
                problem:
                    llm::Problem::Missing { field: found }
                    | llm::Problem::WrongType { field: found }
                    | llm::Problem::BadValue { field: found },
            } => assert_eq!(found.as_ref(), field),
            other @ (llm::Decoded::Owned { .. }
            | llm::Decoded::Builtin { .. }
            | llm::Decoded::Served { .. }
            | llm::Decoded::Invalid { .. }) => {
                panic!("{}: {other:?}", String::from_utf8_lossy(name));
            }
        }
    }
    assert!(matches!(
        adapter::decode(b"read", b"{bad}", grants(), &[], llm::Asker::Main, &limits),
        llm::Decoded::Invalid { problem: llm::Problem::NotAnObject }
    ));
    assert!(matches!(
        adapter::decode(b"unknown", b"{}", grants(), &[], llm::Asker::Main, &limits),
        llm::Decoded::Invalid { problem: llm::Problem::UnknownTool }
    ));
}

#[test]
fn wait_and_sub_agent_decode_into_run_asks() {
    let limits = limits();
    let served = [llm::Served::Wait, llm::Served::SubAgent];
    assert!(matches!(
        adapter::decode(b"wait", b"{}", grants(), &served, llm::Asker::Main, &limits),
        llm::Decoded::Served { ask: run::Ask::Wait }
    ));
    let decoded = adapter::decode(
        b"sub_agent",
        br#"{"brief":"look","tools":["inspect","shell"],"llm":"small"}"#,
        grants(),
        &served,
        llm::Asker::Main,
        &limits,
    );
    match decoded {
        llm::Decoded::Served { ask: run::Ask::SubAgent { brief, families, llm, share } } => {
            assert_eq!(share, run::Share::UNASKED);
            assert_eq!(brief.as_ref(), b"look");
            assert!(families.tools.inspect && families.tools.shell && !families.tools.modify && !families.agents);
            assert_eq!(llm.as_deref(), Some(b"small".as_slice()));
        }
        other @ (llm::Decoded::Owned { .. }
        | llm::Decoded::Builtin { .. }
        | llm::Decoded::Served { .. }
        | llm::Decoded::Invalid { .. }) => {
            panic!("{other:?}");
        }
    }
}

#[test]
fn optional_child_turns_and_seconds_decode_without_a_schema_maximum() {
    use skein_lib::Duration;

    let limits = limits();
    for (input, turns, time) in [
        (br#"{"brief":"look","tools":[],"max_turns":2,"max_seconds":3}"#.as_slice(), Some(2), Some(3)),
        (br#"{"brief":"look","tools":[],"max_turns":4294967295}"#, Some(u32::MAX), None),
        (br#"{"brief":"look","tools":[],"max_seconds":4294967295}"#, None, Some(u32::MAX)),
        (br#"{"brief":"look","tools":[]}"#, None, None),
    ] {
        let decoded =
            adapter::decode(b"sub_agent", input, grants(), &[llm::Served::SubAgent], llm::Asker::Main, &limits);
        let llm::Decoded::Served { ask: run::Ask::SubAgent { share, .. } } = decoded else {
            panic!("expected a child ask: {decoded:?}");
        };
        assert_eq!(share, run::Share { turns, time: time.map(|seconds| Duration::from_secs(u64::from(seconds))) });
    }
    for field in ["max_turns", "max_seconds"] {
        for value in ["0", "-1", "1.5", "1e2", "4294967296"] {
            let input = format!("{{\"brief\":\"look\",\"tools\":[],\"{field}\":{value}}}");
            assert_eq!(
                adapter::decode(
                    b"sub_agent",
                    input.as_bytes(),
                    grants(),
                    &[llm::Served::SubAgent],
                    llm::Asker::Main,
                    &limits
                ),
                llm::Decoded::Invalid { problem: llm::Problem::BadValue { field: field.as_bytes().into() } },
                "{input}",
            );
        }
    }
    let schemas = adapter::schemas(&prompt(), &limits.stated);
    let schema = schemas.iter().find(|schema| schema.kind == ToolKind::SubAgent).expect("main tool");
    let document = skein_llm::Json::from_bytes(&schema.schema, &limits.client.native().request_document())
        .expect("bounded schema");
    let compact = document.document();
    let keys: Vec<_> = (0..compact.len())
        .filter_map(|index| compact.token(index))
        .filter(|record| record.kind == skein_json::Kind::Key)
        .filter_map(|record| compact.text(record))
        .collect();
    assert!(keys.contains(&b"max_turns".as_slice()) && keys.contains(&b"max_seconds".as_slice()));
    assert!(!keys.contains(&b"maximum".as_slice()));
    assert!(schema.description.windows(b"finishing reserve".len()).any(|part| part == b"finishing reserve"));
}

#[test]
fn nesting_is_refused_for_any_agents_field_and_any_child_sub_agent_call() {
    let limits = limits();
    for input in [
        br#"{"brief":"look","tools":["inspect"],"agents":true}"#.as_slice(),
        br#"{"agents":true}"#,
        br#"{"brief":null,"agents":false}"#,
        br#"{"brief":"look","tools":[],"agents":false}"#,
        br#"{"brief":"look","tools":[],"agents":null}"#,
        br#"{"brief":"look","tools":[],"agents":{"future":[]}}"#,
    ] {
        assert_eq!(
            adapter::decode(b"sub_agent", input, grants(), &[llm::Served::SubAgent], llm::Asker::Main, &limits),
            llm::Decoded::Invalid { problem: llm::Problem::Nesting },
        );
    }
    for input in [b"{}".as_slice(), b"{invalid", br#"{"brief":"look","tools":[]}"#] {
        assert_eq!(
            adapter::decode(b"sub_agent", input, grants(), &[], llm::Asker::Child, &limits),
            llm::Decoded::Invalid { problem: llm::Problem::Nesting },
        );
    }
    assert_eq!(
        adapter::decode(b"sub_agent", b"{}", grants(), &[], llm::Asker::Main, &limits),
        llm::Decoded::Invalid { problem: llm::Problem::UnknownTool },
    );
    let schemas = adapter::schemas(&prompt(), &limits.stated);
    let child = schemas.iter().find(|schema| schema.kind == ToolKind::SubAgent).expect("main tool");
    let document =
        skein_llm::Json::from_bytes(&child.schema, &limits.client.native().request_document()).expect("valid schema");
    let document = document.document();
    assert!(!(0..document.len()).any(|index| {
        let token = document.token(index).expect("within document");
        token.kind == skein_json::Kind::Key && document.text(token) == Some(b"agents".as_slice())
    }));
}

#[test]
fn edit_replacements_decode_in_order_and_nested_unknown_members_do_not_become_fields() {
    let input = r#"{"path":"a","edits":[{"old":"one\n","new":"two🌍","future":{"old":"ignored"}},{"new":"three","old":"two🌍","all":true}]}"#.as_bytes();
    let llm::Decoded::Owned { call: tools::Call::Edit { edits, .. } } =
        adapter::decode(b"edit", input, grants(), &[], llm::Asker::Main, &limits())
    else {
        panic!("ordered edit")
    };
    assert_eq!(edits.len(), 2);
    assert_eq!(edits[0].old.as_ref(), b"one\n");
    assert_eq!(edits[0].new.as_ref(), "two🌍".as_bytes());
    assert_eq!(edits[0].occurrences, tools::Occurrences::Once);
    assert_eq!(edits[1].old.as_ref(), "two🌍".as_bytes());
    assert_eq!(edits[1].new.as_ref(), b"three");
    assert_eq!(edits[1].occurrences, tools::Occurrences::All);
    for input in [
        br#"{"path":"a","old":"old","new":"new"}"#.as_slice(),
        br#"{"path":"a","edits":[]}"#,
        br#"{"path":"a","edits":{}}"#,
        br#"{"path":"a","edits":[false]}"#,
        br#"{"path":"a","edits":[[]]}"#,
        br#"{"path":"a","edits":[{"old":0,"new":"new"}]}"#,
        br#"{"path":"a","edits":[{"old":"old","new":"new","all":0}]}"#,
    ] {
        assert!(
            matches!(
                adapter::decode(b"edit", input, grants(), &[], llm::Asker::Main, &limits()),
                llm::Decoded::Invalid { .. }
            ),
            "{}",
            String::from_utf8_lossy(input)
        );
    }
}

#[test]
fn list_depth_and_glob_decode_to_one_typed_call_and_invalid_depth_is_refused() {
    let limits = limits();
    for (input, depth, glob) in [
        (br#"{"path":"src"}"#.as_slice(), tools::Depth::Whole, None),
        (br#"{"path":"src","depth":1,"glob":"*.rs"}"#.as_slice(), tools::Depth::Levels(1), Some(b"*.rs".as_slice())),
        (br#"{"path":"src","depth":4294967295,"future":{"depth":0}}"#.as_slice(), tools::Depth::Levels(u32::MAX), None),
    ] {
        let decoded = adapter::decode(b"list", input, grants(), &[], llm::Asker::Main, &limits);
        let llm::Decoded::Owned { call: tools::Call::List { depth: actual, glob: actual_glob, .. } } = decoded else {
            panic!("typed list: {decoded:?}");
        };
        assert_eq!(actual, depth);
        assert_eq!(actual_glob.as_deref(), glob);
    }
    for input in [
        br#"{"path":"src","depth":0}"#.as_slice(),
        br#"{"path":"src","depth":-1}"#,
        br#"{"path":"src","depth":4294967296}"#,
    ] {
        assert_eq!(
            adapter::decode(b"list", input, grants(), &[], llm::Asker::Main, &limits),
            llm::Decoded::Invalid { problem: llm::Problem::BadValue { field: b"depth".as_slice().into() } }
        );
    }
}

#[test]
fn read_offsets_and_byte_windows_decode_as_typed_requests() {
    for (input, first, budget) in [
        (br#"{"path":"file"}"#.as_slice(), 1, tools::Budget::Default),
        (br#"{"path":"file","offset":3,"window":99}"#.as_slice(), 3, tools::Budget::Asked(99)),
    ] {
        let llm::Decoded::Owned { call: tools::Call::Read { offset, window, .. } } =
            adapter::decode(b"read", input, grants(), &[], llm::Asker::Main, &limits())
        else {
            panic!("typed read")
        };
        assert_eq!(offset, first);
        assert_eq!(window, budget);
    }
    for (input, expected) in [
        (br#"{"path":"file","offset":0}"#.as_slice(), b"offset".as_slice()),
        (br#"{"path":"file","window":0}"#.as_slice(), b"window".as_slice()),
    ] {
        let llm::Decoded::Invalid { problem: llm::Problem::BadValue { field } } =
            adapter::decode(b"read", input, grants(), &[], llm::Asker::Main, &limits())
        else {
            panic!("positive fields")
        };
        assert_eq!(field.as_ref(), expected);
    }
}

#[test]
fn shell_budgets_decode_by_unit_and_a_unitless_timeout_is_ignored() {
    for (input, timeout, output) in [
        (br#"{"command":"test","timeout":9}"#.as_slice(), tools::Timeout::Default, tools::Budget::Default),
        (
            br#"{"command":"test","timeout_seconds":9,"output_bytes":99}"#.as_slice(),
            tools::Timeout::Asked(skein_lib::Duration::from_secs(9)),
            tools::Budget::Asked(99),
        ),
    ] {
        assert_eq!(
            adapter::decode(b"shell", input, grants(), &[], llm::Asker::Main, &limits()),
            llm::Decoded::Owned {
                call: tools::Call::Shell {
                    after: smith_domain_tools::Yield::Never,
                    command: b"test".as_slice().into(),
                    timeout,
                    output
                }
            }
        );
    }
    for (input, field) in [
        (br#"{"command":"test","timeout_seconds":0}"#.as_slice(), b"timeout_seconds".as_slice()),
        (br#"{"command":"test","output_bytes":0}"#.as_slice(), b"output_bytes".as_slice()),
    ] {
        let llm::Decoded::Invalid { problem: llm::Problem::BadValue { field: actual }, .. } =
            adapter::decode(b"shell", input, grants(), &[], llm::Asker::Main, &limits())
        else {
            panic!("zero budget refused")
        };
        assert_eq!(actual.as_ref(), field);
    }
}

#[test]
fn the_empty_path_decodes_as_the_working_directory_for_each_path_field() {
    for (name, input) in [
        (b"read".as_slice(), br#"{"path":""}"#.as_slice()),
        (b"list", br#"{"path":""}"#),
        (b"write", br#"{"path":"","content":"text"}"#),
        (b"edit", br#"{"path":"","edits":[{"old":"before","new":"after"}]}"#),
        (b"search", br#"{"path":"","pattern":"text"}"#),
    ] {
        let decoded = adapter::decode(name, input, grants(), &[], llm::Asker::Main, &limits());
        let llm::Decoded::Owned { call } = decoded else { panic!("{name:?}: {decoded:?}") };
        let path = match call {
            tools::Call::Read { path, .. }
            | tools::Call::List { path, .. }
            | tools::Call::Remove { path }
            | tools::Call::Write { path, .. }
            | tools::Call::Edit { path, .. }
            | tools::Call::Search { path, .. } => path,
            tools::Call::Shell { .. } | tools::Call::Job { .. } => panic!("a path field"),
        };
        assert_eq!(path, tools::Path { absolute: false, parts: Box::new([tools::Part::Current]) }, "{name:?}");
    }
}

#[test]
fn each_description_states_its_facts() {
    let schemas = adapter::schemas(&prompt(), &limits().stated);
    assert_path_facts(&schemas);
    for (name, facts) in [
        (
            b"list".as_slice(),
            ["List . for the working directory", "omitted files and directories counted once"].as_slice(),
        ),
        (b"search", ["Search files with rg using a regular expression", "Regular expression for rg"].as_slice()),
        (
            b"shell",
            [
                "Seconds; default deadline 30 seconds",
                "effective maximum 120 seconds",
                "max 30 seconds",
                "Without yield_seconds, wait for command end",
                "timeout counts from command start",
            ]
            .as_slice(),
        ),
        (
            b"job",
            [
                "Output since last answer",
                "default 30 seconds; max 30 seconds",
                "Max 4 jobs/session",
                "held until end is answered",
                "Without stop, read beside reads, lists and searches",
                "beside other calls it waits no longer than they run",
                "such a poll costs nothing",
                "only with job calls alone does it wait as asked",
                "with stop, write in call order",
                "A poll is best asked to wait about as long as the job has left, so that one poll sees its end; it needs no deliberation of its own.",
                "hold no later batch slot",
                "Stop, then report end",
            ]
            .as_slice(),
        ),
    ] {
        let schema = schemas.iter().find(|schema| schema.name.as_ref() == name).expect("fact tool");
        let text =
            format!("{} {}", String::from_utf8_lossy(&schema.description), String::from_utf8_lossy(&schema.schema));
        for fact in facts {
            assert!(text.contains(fact), "{name:?}: {fact}");
        }
    }
}

fn assert_path_facts(schemas: &[adapter::ToolSchema]) {
    for name in [b"read".as_slice(), b"list", b"search", b"write", b"edit"] {
        let schema = schemas.iter().find(|schema| schema.name.as_ref() == name).expect("path tool");
        let text = std::str::from_utf8(&schema.schema).expect("schema text");
        for fact in [
            "The dot (.) and the empty path both name the working directory",
            "Count each name, . and .., with one separator between each two",
            "repeated, leading and trailing separators count nothing",
            "Keep the same bound once resolved, . and .. removed, from the workspace root",
        ] {
            assert!(text.contains(fact), "{name:?}: {fact}");
        }
    }
}

#[test]
fn one_directory_listing_hides_the_directory_names_instruction() {
    let schemas = adapter::schemas(&prompt(), &limits().stated);
    let list = schemas.iter().find(|schema| schema.name.as_ref() == b"list").expect("list tool");
    assert!(!String::from_utf8_lossy(&list.description).contains("List /"));
}

#[test]
fn several_directories_offer_listing_the_directory_names() {
    let mut prompt = prompt();
    prompt.namespace =
        tools::Namespace::Several { working: tools::Name::new(b"work".as_slice().into()).expect("name") };
    let schemas = adapter::schemas(&prompt, &limits().stated);
    let list = schemas.iter().find(|schema| schema.name.as_ref() == b"list").expect("list tool");
    assert!(String::from_utf8_lossy(&list.description).contains("List / for directory names"));
}

#[test]
fn remove_uses_the_owners_words_and_is_offered_only_with_modify() {
    let mut prompt = prompt();
    let schemas = adapter::schemas(&prompt, &limits().stated);
    let remove = schemas.iter().find(|tool| tool.name.as_ref() == b"remove").expect("modify offers remove");
    assert_eq!(remove.description.as_ref(), b"Remove a file you have read at its current version, or an empty directory; never a directory with files in it.");
    assert!(remove.description.len() <= 160);
    assert!(String::from_utf8_lossy(&remove.schema).contains("The path, inside the workspace, as write takes it."));
    prompt.tools.modify = false;
    assert!(!adapter::schemas(&prompt, &limits().stated).iter().any(|tool| tool.name.as_ref() == b"remove"));
    assert_eq!(
        adapter::decode(b"remove", br#"{"path":"a"}"#, prompt.tools, &[], llm::Asker::Main, &limits()),
        llm::Decoded::Invalid { problem: llm::Problem::UnknownTool }
    );
}
