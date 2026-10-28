use super::*;
use smith_domain::{
    llm,
    tools::{Namespace, Outcome, Resolved},
};

const ALL: Grants = Grants { inspect: true, modify: true, shell: true };

fn mounts() -> [Mount; 2] {
    [
        Mount { root: skein_lib::Token::new(1), host: b"/work/repo".as_slice().into(), writable: true },
        Mount { root: skein_lib::Token::new(1), host: b"/library".as_slice().into(), writable: false },
    ]
}

fn allowed(tool: &[u8], input: &[u8]) {
    same(judge(tool, input, ALL, &mounts(), 128), mcp::Verdict::Allow { updated_input: input.into() });
}

fn denied(tool: &[u8], input: &[u8], words: &[u8]) {
    same(judge(tool, input, ALL, &mounts(), 128), deny(words));
}

#[test]
fn a_read_inside_a_directory_is_allowed() {
    allowed(b"Read", br#"{"file_path":"/work/repo/a"}"#);
}

#[test]
fn a_relative_path_resolves_against_the_working_directory() {
    allowed(b"Read", br#"{"file_path":"./a/../b//c"}"#);
    let cwd = [Mount { root: skein_lib::Token::new(1), host: b"/abc".as_slice().into(), writable: true }];
    same(
        judge(b"Read", br#"{"file_path":"defg"}"#, ALL, &cwd, 4),
        mcp::Verdict::Allow { updated_input: br#"{"file_path":"defg"}"#.as_slice().into() },
    );
}

#[test]
fn a_path_climbing_out_is_outside() {
    denied(
        b"Read",
        br#"{"file_path":"../../../../etc/passwd"}"#,
        b"path is outside the workspace: ../../../../etc/passwd",
    );
}

#[test]
fn a_prefix_that_is_not_a_whole_name_is_outside() {
    denied(b"Read", br#"{"file_path":"/work/repo2/a"}"#, b"path is outside the workspace: /work/repo2/a");
}

#[test]
fn a_path_in_git_is_protected() {
    for path in [".git/a", "nested/.GIT/config", "/library/.Git"] {
        let input = serde_json::to_vec(&serde_json::json!({"file_path":path})).expect("bounded fixture");
        denied(b"Read", &input, PROTECTED);
        denied(b"Write", &input, PROTECTED);
    }
}

#[test]
fn a_glob_without_a_path_uses_the_working_directory() {
    allowed(b"Glob", br#"{"pattern":"*.rs"}"#);
    allowed(b"Grep", br#"{"pattern":"x"}"#);
}

#[test]
fn a_write_to_a_read_only_directory_is_denied() {
    for tool in [b"Write".as_slice(), b"Edit"] {
        denied(tool, br#"{"file_path":"/library/a"}"#, b"path is read-only: /library/a");
    }
    allowed(b"Read", br#"{"file_path":"/library/a"}"#);
    let nested = [
        Mount { root: skein_lib::Token::new(1), host: b"/work/repo".as_slice().into(), writable: true },
        Mount { root: skein_lib::Token::new(1), host: b"/work/repo/locked".as_slice().into(), writable: false },
    ];
    same(judge(b"Edit", br#"{"file_path":"locked/a"}"#, ALL, &nested, 128), deny(b"path is read-only: locked/a"));
}

#[test]
fn an_ungranted_tool_is_not_granted() {
    for tool in [b"Read".as_slice(), b"unknown", b"mcp__smith__finish"] {
        same(
            judge(tool, b"{}", Grants { inspect: false, modify: false, shell: false }, &mounts(), 128),
            deny(NOT_GRANTED),
        );
    }
}

#[test]
fn bash_is_allowed_with_shell() {
    same(
        judge(b"Bash", b"unparsed", Grants { inspect: false, modify: false, shell: true }, &[], 1),
        mcp::Verdict::Allow { updated_input: b"unparsed".as_slice().into() },
    );
}

#[test]
fn a_missing_path_field_is_denied() {
    denied(b"Read", b"{}", b"missing field \"file_path\"");
    denied(b"Edit", br#"{"file_path":42}"#, b"wrong field type \"file_path\"");
    denied(b"Glob", br#"{"path":null}"#, b"wrong field type \"path\"");
}

#[test]
fn a_too_long_path_is_denied() {
    same(judge(b"Read", br#"{"file_path":"abcde"}"#, ALL, &mounts(), 4), deny(TOO_LONG));
}

#[test]
fn the_denials_use_the_workspace_words() {
    for (outcome, words) in
        [(Outcome::NotGranted, NOT_GRANTED), (Outcome::TooLong, TOO_LONG), (Outcome::Protected, PROTECTED)]
    {
        assert_eq!(
            smith_protocol_llm::render_outcome(&outcome, &Namespace::One, 256).expect("bounded fixture"),
            (words.into(), true)
        );
    }
    for (outcome, prefix) in [
        (Outcome::Outside { path: Resolved::Outside { path: b"elsewhere".as_slice().into() } }, OUTSIDE),
        (Outcome::ReadOnly { path: Resolved::Working { path: b"a".as_slice().into() } }, READ_ONLY),
    ] {
        assert!(
            smith_protocol_llm::render_outcome(&outcome, &Namespace::One, 256)
                .expect("bounded fixture")
                .0
                .starts_with(prefix)
        );
    }
    let backend = super::super::lifecycle_tests::backend();
    for (problem, prefix) in [
        (llm::Problem::Missing { field: b"file_path".as_slice().into() }, MISSING),
        (llm::Problem::WrongType { field: b"file_path".as_slice().into() }, WRONG_TYPE),
    ] {
        let denied = result_prompt(&backend, llm::Returned::Invalid { problem });
        let expected = result_prompt(
            &backend,
            llm::Returned::Text { text: json::join(&[prefix, b"\"file_path\""]), error: true, replay: None },
        );
        assert_eq!(
            smith_protocol_llm::prompt(denied, &[], &backend.settings.adapter).expect("rendered invalid input"),
            smith_protocol_llm::prompt(expected, &[], &backend.settings.adapter).expect("rendered denial words")
        );
    }
}

fn same(left: mcp::Verdict, right: mcp::Verdict) {
    match left {
        mcp::Verdict::Allow { updated_input: a } => {
            let mcp::Verdict::Allow { updated_input: b } = right else { panic!("expected allow") };
            assert_eq!(a, b);
        }
        mcp::Verdict::Deny { message: a } => {
            let mcp::Verdict::Deny { message: b } = right else { panic!("expected deny") };
            assert_eq!(a, b);
        }
    }
}

fn result_prompt(backend: &super::super::Backend, result: llm::Returned) -> llm::Prompt {
    let smith_protocol_llm::FromDomain::Complete { mut prompt, .. } = super::super::lifecycle_tests::request(backend)
    else {
        panic!("Complete")
    };
    prompt.messages = Box::new([llm::Message {
        role: llm::Role::User,
        content: Box::new([llm::Block::ToolResult { id: b"id".as_slice().into(), result }]),
    }]);
    prompt
}

#[test]
#[expect(clippy::disallowed_methods, reason = "ordinary unit fixture constructs JSON beyond the former depth cap")]
fn a_grep_beside_deep_input_is_denied() {
    let mut input = br#"{"path":"/etc","extra":"#.to_vec();
    input.extend_from_slice(&[b'['; 129]);
    input.push(b'0');
    input.extend_from_slice(&[b']'; 129]);
    input.push(b'}');
    denied(b"Grep", &input, b"path is outside the workspace: /etc");
}

#[test]
fn a_glob_with_a_repeated_path_is_denied() {
    denied(b"Glob", br#"{"path":"/work/repo","path":"/etc"}"#, b"wrong field type \"path\"");
}

#[test]
fn a_glob_with_unreadable_input_is_denied() {
    denied(b"Glob", br#"{"pattern":"x""#, b"wrong field type \"path\"");
}

#[test]
fn every_tools_flag_fits_the_inventory_capacity() {
    let mut largest = 0;
    for inspect in [false, true] {
        for modify in [false, true] {
            for shell in [false, true] {
                let flag = tools_flag(Grants { inspect, modify, shell });
                assert!(flag.len() <= usize::try_from(TOOLS_FLAG_BYTES).expect("flag bound"));
                largest = largest.max(flag.len());
            }
        }
    }
    assert_eq!(largest, usize::try_from(TOOLS_FLAG_BYTES).expect("flag bound"));
}
