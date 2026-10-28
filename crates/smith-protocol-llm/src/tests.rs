//! Rendering properties with deterministic input and the ordinary Rust test kit.
#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "testing-strategy.md, section 4: test harnesses are ordinary Rust"
)]

use skein_lib::{Duration, Rng};
use smith_domain::tools;

use crate::{CUT_MARKER_BYTES, render_outcome, render_worst_case};

#[test]
fn replay_admits_an_ordered_run_beyond_one_reasoning_item() {
    let mut client = skein_llm_world::limits();
    client.reasoning = 64;
    client.output_items = 4;
    let limits = crate::limits::replay_limits(&client).expect("checked share");
    assert_eq!(limits.bytes, 256);
    assert_eq!([limits.strings, limits.tokens, limits.depth], [client.strings, client.tokens, client.depth]);
    let raw =
        br#"[{"type":"reasoning","encrypted_content":"first"},{"type":"reasoning","encrypted_content":"second"}]"#;
    assert!(raw.len() > usize::try_from(client.reasoning).expect("item fits"));
    let value = skein_llm::Json::from_bytes(raw, &limits).expect("ordered run");
    let replay = skein_llm::Replay { provider: skein_llm::Provider::OpenAiCodex, value };
    let bytes = replay.to_bytes(&limits).expect("whole durable run");
    let restored = skein_llm::Replay::from_bytes(&bytes, &limits).expect("whole replay");
    assert_eq!(restored.value.to_bytes(&limits).expect("raw replay").as_ref(), raw);
}

#[test]
fn completion_prices_one_reasoning_share_and_refuses_its_overflow() {
    let mut client = skein_llm_world::limits();
    client.output_items = 4;
    client.reasoning = 32;
    let smaller = crate::completion_worst_case(&client, 4096).expect("one share");
    client.reasoning = 64;
    let larger = crate::completion_worst_case(&client, 4096).expect("larger share");
    assert_eq!(larger - smaller, 128);
    client.output_items = u32::MAX;
    assert_eq!(crate::limits::replay_limits(&client), None);
    assert_eq!(crate::completion_worst_case(&client, 4096), None);
}

fn tools() -> tools::Limits {
    tools::Limits {
        shell_output: 128,
        kits: 1,
        calls: 1,
        repos: 1,
        path_bytes: 32,
        host_path_bytes: 4096,
        known_files: 1,
        file_bytes: 1024,
        tool_payload: 1024,
        read_bytes: 64,
        list_entries: 8,
        list_bytes: 512,
        match_lines: 8,
        file_timeout: Duration::from_secs(30),
        jobs: 4,
        job_poll: Duration::from_secs(30),
        job_wait: Duration::from_secs(30),
        shell_timeout: Duration::from_secs(30),
        tool_deadline: Duration::from_secs(120),
        shell_head: 64,
        shell_tail: 64,
        search_hits: 8,
        search_bytes: 128,
        search_timeout: Duration::from_secs(30),
        facts: 32,
    }
}

#[test]
fn random_outcomes_at_random_caps_fit_their_derived_render_bound() {
    let mut rng = Rng::new(941);
    let mut smallest_bound = u32::MAX;
    let mut read_cuts = Vec::new();
    for case in 0_usize..1 {
        let mut caps = tools();
        caps.read_bytes = u32::try_from(rng.next_u64() % 1024).unwrap();
        caps.shell_head = u32::try_from(rng.next_u64() % 1024).unwrap();
        caps.shell_tail = u32::try_from(rng.next_u64() % 1024).unwrap();
        caps.search_hits = u32::try_from(rng.next_u64() % 32).unwrap();
        caps.search_bytes = u32::try_from(rng.next_u64() % 1024).unwrap();
        let bound = render_worst_case(&caps, 1024, 2048).unwrap();
        smallest_bound = smallest_bound.min(bound);
        if case % 8 == 0 {
            read_cuts.push(CUT_MARKER_BYTES + u32::try_from(rng.next_u64() % 128).unwrap());
        }
        for outcome in random_outcomes(&caps) {
            let cut = (case % 8 == 0).then(|| CUT_MARKER_BYTES + u32::try_from(rng.next_u64() % 128).unwrap());
            assert_rendering(&outcome, bound, cut);
        }
    }
    let caps = tools();
    let read = tools::Outcome::Read {
        content: vec![b'\n'; usize::try_from(caps.tool_payload).unwrap()].into(),
        first: u32::MAX,
        lines: u32::MAX,
        total: u32::MAX,
        cut: tools::Cut::Line,
        next: tools::Next::Line(u32::MAX),
        window: tools::BytesClamp::Clamped { asked: u32::MAX, ceiling: caps.tool_payload },
    };
    assert_rendering(&read, smallest_bound, read_cuts.first().copied());
    for cut in read_cuts.into_iter().skip(1) {
        assert_cut_replays(&read, cut);
    }
}

fn random_outcomes(caps: &tools::Limits) -> [tools::Outcome; 6] {
    let entries = (0..caps.list_entries)
        .map(|_| tools::Entry { name: vec![0xff; 8].into(), kind: tools::Kind::Directory })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let hits = (0..caps.search_hits)
        .map(|_| tools::Hit { path: Box::new([]), line: u32::MAX, text: Box::new([]) })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    [
        tools::Outcome::Listed { entries, more: u64::MAX },
        tools::Outcome::Found { hits, more: u64::MAX, timed_out: true },
        tools::Outcome::EditFailed {
            edit: 1,
            why: tools::Mismatch::Ambiguous {
                count: u32::MAX,
                lines: vec![u32::MAX; usize::try_from(caps.match_lines).unwrap()].into(),
            },
        },
        tools::Outcome::Running {
            timeout: tools::TimeoutClamp::Clamped {
                asked: Duration::from_nanos(u64::MAX),
                ceiling: Duration::from_nanos(1),
            },
            job: tools::JobNumber(u32::MAX),
            head: vec![0xff; usize::try_from(caps.shell_head).unwrap()].into(),
            tail: vec![0; usize::try_from(caps.shell_tail).unwrap()].into(),
            dropped: u64::MAX,
            after: tools::TimeoutClamp::Clamped {
                asked: Duration::from_nanos(u64::MAX),
                ceiling: Duration::from_nanos(1),
            },
            output: tools::BytesClamp::Clamped { asked: u32::MAX, ceiling: 1 },
        },
        tools::Outcome::Job {
            job: tools::JobNumber(u32::MAX),
            head: vec![0xff; usize::try_from(caps.shell_head).unwrap()].into(),
            tail: vec![0; usize::try_from(caps.shell_tail).unwrap()].into(),
            dropped: u64::MAX,
            wait: tools::TimeoutClamp::Clamped {
                asked: Duration::from_nanos(u64::MAX),
                ceiling: Duration::from_nanos(1),
            },
            output: tools::BytesClamp::Clamped { asked: u32::MAX, ceiling: 1 },
            end: tools::JobEnd::Ended(tools::Exit::Signal { signal: u8::MAX }),
        },
        tools::Outcome::Exited {
            exit: tools::Exit::Code { code: u8::MAX },
            head: vec![0xff; usize::try_from(caps.shell_head).unwrap()].into(),
            tail: vec![0; usize::try_from(caps.shell_tail).unwrap()].into(),
            dropped: u64::MAX,
            timeout: tools::TimeoutClamp::Clamped {
                asked: Duration::from_nanos(u64::MAX),
                ceiling: Duration::from_nanos(1),
            },
            output: tools::BytesClamp::Clamped { asked: u32::MAX, ceiling: 1 },
        },
    ]
}

fn assert_rendering(outcome: &tools::Outcome, bound: u32, cut: Option<u32>) {
    let (full, _) = render_outcome(outcome, &tools::Namespace::One, bound).unwrap();
    if let Some(cut) = cut {
        assert_eq!(
            render_outcome(outcome, &tools::Namespace::One, bound).unwrap().0,
            full,
            "same outcome renders identical bytes"
        );
        assert_cut_replays(outcome, cut);
    }
    assert!(full.len() <= usize::try_from(bound).unwrap());
    assert!(!full.ends_with(b" bytes omitted]"));
}

fn assert_cut_replays(outcome: &tools::Outcome, cap: u32) {
    assert_eq!(
        render_outcome(outcome, &tools::Namespace::One, cap),
        render_outcome(outcome, &tools::Namespace::One, cap),
        "cut rendering is deterministic too"
    );
}

#[test]
fn each_tools_framing_starts_from_the_same_empty_object() {
    let limits = skein_llm::DocumentLimits { bytes: 2, strings: 1, tokens: 2, depth: 2 };
    for tool in &crate::framing::offered_tools(3).unwrap() {
        assert_eq!(tool.name.as_ref(), b"a");
        assert_eq!(tool.description.as_ref(), b"");
        assert_eq!(tool.schema.to_bytes(&limits).unwrap().as_ref(), b"{}");
    }
}

#[test]
fn a_cut_marker_counts_the_exact_bytes_left_out_of_the_rendering() {
    let outcome = tools::Outcome::Read {
        content: vec![b'x'; 512].into(),
        first: 1,
        lines: 1,
        total: 1,
        cut: tools::Cut::Whole,
        next: tools::Next::End,
        window: tools::BytesClamp::Within,
    };
    let (full, error) = render_outcome(&outcome, &tools::Namespace::One, 4096).unwrap();
    for cap in CUT_MARKER_BYTES..128 {
        let (cut, cut_error) = render_outcome(&outcome, &tools::Namespace::One, cap).unwrap();
        let marker = cut.windows(2).position(|bytes| bytes == b"\n[").unwrap();
        let count =
            core::str::from_utf8(&cut[marker + 2..]).unwrap().split(' ').next().unwrap().parse::<usize>().unwrap();
        assert_eq!(count, full.len() - marker);
        assert_eq!(&cut[..marker], &full[..marker]);
        assert_eq!(cut_error, error);
        assert!(cut.len() <= usize::try_from(cap).unwrap());
    }
}

#[test]
fn cuts_preserve_every_utf8_character_boundary() {
    let original = "é界🦀".repeat(30);
    for cap in CUT_MARKER_BYTES..128 {
        let cut = crate::render::cut_text(original.as_bytes().into(), cap);
        let text = core::str::from_utf8(&cut).unwrap();
        let (prefix, marker) = text.split_once("\n[").unwrap();
        let omitted = marker.split(' ').next().unwrap().parse::<usize>().unwrap();
        assert!(original.starts_with(prefix));
        assert_eq!(omitted, original.len() - prefix.len());
        assert!(cut.len() <= usize::try_from(cap).unwrap());
    }
}

#[test]
fn rendering_derivation_refuses_arithmetic_overflow() {
    let mut caps = tools();
    caps.list_bytes = u64::MAX;
    assert_eq!(render_worst_case(&caps, 1, 1), None);
}

#[test]
fn valid_utf8_survives_and_only_invalid_bytes_and_controls_are_escaped() {
    let cases = [
        ("é界🦀 –\n\t".as_bytes(), "exit code 0\né界🦀 –\n\t".as_bytes()),
        (
            "\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}".as_bytes(),
            "exit code 0\n\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}\u{10000}\u{10ffff}".as_bytes(),
        ),
        (b"\xc0\xaf\xf4\x90\x80\x80".as_slice(), b"exit code 0\n\\xc0\\xaf\\xf4\\x90\\x80\\x80".as_slice()),
        (b"a\x00\r\x1b\x7fb".as_slice(), b"exit code 0\na\\x00\\x0d\\x1b\\x7fb".as_slice()),
        ("a\u{85}b".as_bytes(), b"exit code 0\na\\xc2\\x85b".as_slice()),
        (b"a\xff\xc3\xa9\xe2\x82".as_slice(), "exit code 0\na\\xffé\\xe2\\x82".as_bytes()),
        (b"\xf0\x80\x80\x80\xed\xa0\x80".as_slice(), b"exit code 0\n\\xf0\\x80\\x80\\x80\\xed\\xa0\\x80".as_slice()),
    ];
    for (input, expected) in cases {
        let outcome = tools::Outcome::Exited {
            exit: tools::Exit::Code { code: 0 },
            head: input.into(),
            tail: Box::new([]),
            dropped: 0,
            timeout: tools::TimeoutClamp::Within,
            output: tools::BytesClamp::Within,
        };
        let first = render_outcome(&outcome, &tools::Namespace::One, 4096).unwrap();
        assert_eq!(first.0.as_ref(), expected);
        assert!(!first.1);
        assert_eq!(first, render_outcome(&outcome, &tools::Namespace::One, 4096).unwrap());
    }
}

#[test]
fn rendered_utf8_cuts_remain_valid_and_count_exact_omitted_bytes() {
    let outcome = tools::Outcome::Exited {
        exit: tools::Exit::Code { code: 0 },
        head: "é界🦀".repeat(30).into_bytes().into(),
        tail: Box::new([]),
        dropped: 0,
        timeout: tools::TimeoutClamp::Within,
        output: tools::BytesClamp::Within,
    };
    let (full, _) = render_outcome(&outcome, &tools::Namespace::One, 4096).unwrap();
    for cap in CUT_MARKER_BYTES..128 {
        let (cut, error) = render_outcome(&outcome, &tools::Namespace::One, cap).unwrap();
        let text = core::str::from_utf8(&cut).unwrap();
        let (prefix, marker) = text.split_once("\n[").unwrap();
        let omitted = marker.split(' ').next().unwrap().parse::<usize>().unwrap();
        assert_eq!(omitted, full.len() - prefix.len());
        assert_eq!(prefix.as_bytes(), &full[..prefix.len()]);
        assert!(cut.len() <= usize::try_from(cap).unwrap());
        assert!(!error);
    }
}

fn described_prompt() -> smith_domain::llm::Prompt {
    use smith_domain::llm;
    llm::Prompt {
        effort: None,
        purpose: llm::Purpose::Ordinary { guard: None },
        choice: llm::ToolChoice::Auto,
        namespace: tools::Namespace::One,
        asker: llm::Asker::Main,
        affinity: llm::Affinity { key: [0; 16], thread: 0 },
        endpoint: llm::Endpoint(1),
        model: b"test".as_slice().into(),
        system: Box::new([]),
        tools: tools::Grants { inspect: true, modify: true, shell: true },
        served: Box::new([]),
        messages: Box::new([]),
        max_tokens: 16,
    }
}

#[test]
fn golden_descriptions_state_batching_and_every_effective_parameter() {
    let stated = crate::Stated {
        host_part: u64::MAX,
        path_bytes: 37,
        read_window: 73,
        tool_payload: 257,
        file_bytes: 257,
        call_input_bytes: 401,
        list_entries: 11,
        search_hits: 13,
        search_bytes: 127,
        job_poll: Duration::from_secs(2),
        job_wait: Duration::from_secs(7),
        jobs: 4,
        shell_timeout: Duration::from_secs(12),
        tool_deadline: Duration::from_secs(43),
        shell_output: 151,
        output_ceiling: 151,
    };
    let golden = [
        (
            "Read whole lines in byte windows; default 73 bytes, ceiling 257 bytes. Next line, cuts and clamps are reported. Whole-file limit 257 bytes. Batch independent reads, lists and searches side by side.",
            3_usize,
        ),
        (
            "List matching files and their directory tree; depth defaults to whole, direct children 1. Glob selects files. Max 11 entries; omitted files and directories counted once. List . for the working directory. Batch independent reads, lists and searches side by side.",
            3,
        ),
        (
            "Search files with rg using a regular expression; max 13 matches, 127 bytes; cuts are marked. Batch independent reads, lists and searches side by side.",
            3,
        ),
        (
            "Write a file; read its current version first. Whole-file limit 257 bytes. For a larger file, write its first part within the byte bound, then extend it with edits; put as many edits in one call as fit the bound. Batch a change's edits across its files, then the command that checks them; they run one at a time in call order, and a write that fails, or any call answered invalid, leaves the writes and commands after it not run.",
            2,
        ),
        (
            "Apply ordered edits atomically; no earlier read. Each old must match once, or at least once with all. Whole-file limit 257 bytes. For a larger file, write its first part within the byte bound, then extend it with edits; put as many edits in one call as fit the bound. Batch a change's edits across its files, then the command that checks them; they run one at a time in call order, and a write that fails, or any call answered invalid, leaves the writes and commands after it not run.",
            5,
        ),
        (
            "Remove a file you have read at its current version, or an empty directory; never a directory with files in it.",
            1,
        ),
        (
            "Run a command; files may change. Max 151 output bytes; cuts are marked. Without yield_seconds, wait for command end. At yield, an unfinished command answers running with job number; timeout counts from command start. Batch a change's edits across its files, then the command that checks them; they run one at a time in call order, and a write that fails, or any call answered invalid, leaves the writes and commands after it not run.",
            4,
        ),
        (
            "Output since last answer; running or end. Max 4 jobs/session, held until end is answered. Without stop, read beside reads, lists and searches; beside other calls it waits no longer than they run, so such a poll costs nothing; only with job calls alone does it wait as asked; with stop, write in call order. A poll is best asked to wait about as long as the job has left, so that one poll sees its end; it needs no deliberation of its own. Running jobs hold no later batch slot. Cuts marked.",
            4,
        ),
    ];
    let schemas = crate::schemas(&described_prompt(), &stated);
    assert_eq!(schemas.len(), golden.len());
    for (schema, (words, parameters)) in schemas.iter().zip(golden) {
        assert_eq!(schema.description.as_ref(), words.as_bytes());
        let text = core::str::from_utf8(&schema.schema).unwrap();
        assert_eq!(
            text.matches("\"description\"").count(),
            parameters,
            "{}",
            core::str::from_utf8(&schema.name).unwrap()
        );
        assert!(!text.contains("runs alone"));
        match schema.name.as_ref() {
            b"read" => assert!(
                text.contains("max 37 bytes")
                    && text.contains("default 73 bytes; max 257 bytes")
                    && text.contains("counting from 1")
            ),
            b"list" => assert!(text.contains("max 37 bytes")),
            b"search" => assert!(text.contains("max 37 bytes") && text.matches("max 401 bytes").count() == 2),
            b"write" => assert!(text.contains("max 257 bytes") && text.contains("max 401 bytes")),
            b"edit" => assert_eq!(text.matches("max 257 bytes").count(), 2),
            b"remove" => assert!(text.contains("The path, inside the workspace, as write takes it.")),
            b"shell" => assert!(
                text.contains("Seconds")
                    && text.contains("12 seconds")
                    && text.contains("43 seconds")
                    && text.contains("Yield; no default; max 7 seconds; clamped.")
                    && text.contains("max 401 bytes")
            ),
            b"job" => assert!(text.contains("Wait; default 2 seconds; max 7 seconds; clamped.")),
            _ => panic!("only the eight workspace tools"),
        }
    }
    assert_eq!(schemas, crate::schemas(&described_prompt(), &stated));
}

#[test]
fn stated_quantities_copy_effective_limits_and_refuse_capture_overflow() {
    let limits = tools();
    let stated = crate::Stated::from_tools(&limits, 401).unwrap();
    assert_eq!(stated.path_bytes, 32);
    assert_eq!(stated.tool_payload, 1024);
    assert_eq!(stated.read_window, 64);
    assert_eq!(stated.search_hits, 8);
    assert_eq!(stated.search_bytes, 128);
    assert_eq!(stated.output_ceiling, 128);
    assert_eq!(stated.shell_output, 128);
    assert_eq!(stated.shell_timeout, Duration::from_secs(30));
    assert_eq!(stated.job_poll, Duration::from_secs(30));
    assert_eq!(stated.job_wait, Duration::from_secs(30));
    assert_eq!(stated.jobs, 4);
    assert_eq!(stated.tool_deadline, Duration::from_secs(120));
    let excessive = tools::Limits { shell_head: u32::MAX, shell_tail: 1, ..limits };
    assert!(crate::Stated::from_tools(&excessive, 401).is_none());
}

#[test]
fn path_descriptions_follow_the_actual_conversation_namespace() {
    use smith_domain::tools::{Name, Namespace};
    for (namespace, wanted, absent) in [
        (Namespace::One, "Relative to the working directory, e.g. src/lib.rs", "/name/..."),
        (
            Namespace::Several {
                working: Name::new(b"private-name".as_slice().into()).expect("valid namespace fixture"),
            },
            "Relative to the working directory or /name/...",
            "private-name",
        ),
    ] {
        let mut prompt = described_prompt();
        prompt.namespace = namespace;
        for schema in crate::schemas(
            &prompt,
            &crate::Stated {
                host_part: u64::MAX,
                path_bytes: 256,
                read_window: 1024,
                tool_payload: 4096,
                file_bytes: 4096,
                call_input_bytes: 2048,
                list_entries: 16,
                search_hits: 8,
                search_bytes: 256,
                job_poll: Duration::from_secs(30),
                job_wait: Duration::from_secs(30),
                jobs: 4,
                shell_timeout: Duration::from_secs(60),
                tool_deadline: Duration::from_secs(600),
                shell_output: 192,
                output_ceiling: 192,
            },
        ) {
            if ![b"shell".as_slice(), b"job", b"remove"].contains(&schema.name.as_ref()) {
                let text = core::str::from_utf8(&schema.schema).expect("valid namespace fixture");
                assert!(
                    text.contains(wanted),
                    "{}",
                    core::str::from_utf8(&schema.name).expect("valid namespace fixture")
                );
                assert!(!text.contains(absent));
            }
        }
    }
}

#[test]
fn resolved_failure_paths_render_in_the_fixed_namespace_live_and_on_replay() {
    let working = tools::Resolved::Working { path: "src/café.rs".as_bytes().into() };
    let other = tools::Resolved::Other {
        directory: tools::Name::new(b"docs".as_slice().into()).expect("name"),
        path: b"guide.md".as_slice().into(),
    };
    let outside = tools::Resolved::Outside { path: b"elsewhere/file".as_slice().into() };
    let several = tools::Namespace::Several { working: tools::Name::new(b"work".as_slice().into()).expect("name") };
    let table: [(tools::Outcome, tools::Namespace, &[u8]); 5] = [
        (
            tools::Outcome::NotFound { path: working.clone() },
            tools::Namespace::One,
            "path not found: src/café.rs".as_bytes(),
        ),
        (tools::Outcome::NotFound { path: working }, several, "path not found: /work/src/café.rs".as_bytes()),
        (tools::Outcome::ReadOnly { path: other }, tools::Namespace::One, b"path is read-only: /docs/guide.md"),
        (
            tools::Outcome::Outside { path: outside },
            tools::Namespace::One,
            b"path is outside the workspace: /elsewhere/file",
        ),
        (
            tools::Outcome::NotDirectory { path: tools::Resolved::Working { path: Box::new([]) } },
            tools::Namespace::One,
            b"path is not a directory: .",
        ),
    ];
    for (outcome, namespace, expected) in table {
        let rendered = render_outcome(&outcome, &namespace, 4096).expect("bounded path feedback");
        assert_eq!(rendered, (expected.into(), true));
        assert_eq!(rendered, render_outcome(&outcome.clone(), &namespace, 4096).expect("replay"));
    }
}

#[test]
fn oversized_call_feedback_states_size_bound_and_batched_recovery_within_its_cap() {
    use smith_domain::llm::Problem;
    let golden = b"input too large: 1024 bytes, bound 256 bytes; make a smaller write or an edit. For a larger file, write its first part within the bound, then extend it with edits; put as many edits in one call as fit the bound";
    let exact = u32::try_from(golden.len()).expect("fixed feedback length");
    assert_eq!(
        crate::failure::problem(Problem::Oversize { bytes: 1024, bound: 256 }, exact).expect("exact room").as_ref(),
        golden.as_slice()
    );
    assert_eq!(
        crate::failure::problem(Problem::Oversize { bytes: 1024, bound: 256 }, exact - 1),
        Err(crate::Error::Limit)
    );
    assert_eq!(
        crate::failure::problem(Problem::CutOff { bytes: 17 }, 4096).expect("cut note").as_ref(),
        b"input cut off after 17 bytes; make a smaller call and try again".as_slice()
    );
}

#[test]
fn fixed_own_schema_measurement_matches_the_complete_fixed_inventory() {
    let stated = crate::Stated::from_tools(&tools(), 2048).expect("bounded quantities");
    let mut prompt = described_prompt();
    prompt.served = Box::new([smith_domain::llm::Served::Wait, smith_domain::llm::Served::SubAgent]);
    let mut largest = 0;
    for namespace in [
        tools::Namespace::One,
        tools::Namespace::Several { working: tools::Name::new(b"work".as_slice().into()).expect("name") },
    ] {
        prompt.namespace = namespace;
        let inventory = crate::schemas(&prompt, &stated);
        let rendered =
            inventory.iter().map(|tool| tool.name.len() + tool.description.len() + tool.schema.len()).sum::<usize>();
        largest = largest.max(rendered);
        assert_eq!(inventory.len(), 10);
    }
    assert_eq!(crate::fixed_schema_bytes(&stated), u64::try_from(largest).ok());
}

#[test]
fn drawn_contract_schema_costs_equal_every_rendered_byte() {
    use smith_domain::run::outcome::{ChangeSpec, FieldRule, ItemRule, ItemSpec, OutcomeSpec, TextSpec, VerdictRule};
    let costs = crate::schema_costs().expect("startup renderer costs");
    let mut rng = Rng::new(46_106);
    let words = [b"plain".as_slice(), b"\\\"", b"\n\t\x01", "é😀".as_bytes()];
    for _ in 0_u32..512 {
        let mut fields = || {
            (0..rng.next_u64() % 5)
                .map(|_| FieldRule {
                    name: words[usize::try_from(rng.next_u64() % 4).expect("word index")].into(),
                    max: u32::try_from(rng.next_u64() & u64::from(u32::MAX)).expect("u32 cap"),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice()
        };
        let change = ChangeSpec { checks_must_pass: false, fields: fields() };
        let report = TextSpec { max: u32::MAX, fields: fields() };
        let failure = TextSpec { max: 0, fields: fields() };
        let kind_fields = fields();
        let verdict_fields = fields();
        let kinds = (0..rng.next_u64() % 4)
            .map(|index| ItemRule {
                kind: words[usize::try_from(index).expect("word index")].into(),
                fields: kind_fields.clone(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let verdicts = (0..rng.next_u64() % 4)
            .map(|index| VerdictRule {
                name: words[usize::try_from(index).expect("word index")].into(),
                text_max: u32::MAX,
                fields: verdict_fields.clone(),
                items: ItemSpec { min: 0, max: 23, kinds: kinds.clone() },
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let outcome = OutcomeSpec {
            change: (rng.next_u64().is_multiple_of(2)).then_some(change.clone()),
            report: (rng.next_u64().is_multiple_of(2)).then_some(report),
            failure: (rng.next_u64().is_multiple_of(2)).then_some(failure),
            verdicts,
        };
        assert_eq!(
            costs.finish(&outcome),
            Some(
                u64::try_from(crate::finish_schema(&outcome, u32::MAX).expect("rendered finish").len())
                    .expect("length")
            )
        );
        assert_eq!(
            costs.deliver(&change),
            Some(
                u64::try_from(crate::deliver_schema(&change, u32::MAX).expect("rendered deliver").len())
                    .expect("length")
            )
        );
    }
}

#[test]
fn every_size_refusal_at_numeric_maxima_fits_the_derived_result_with_its_units() {
    let mut caps = tools();
    caps.tool_payload = 1;
    caps.read_bytes = 1;
    caps.list_bytes = 1;
    caps.list_entries = 1;
    caps.search_bytes = 1;
    caps.search_hits = 1;
    caps.shell_head = 0;
    caps.shell_tail = 0;
    caps.match_lines = 0;
    caps.path_bytes = 1;
    let maximum = render_worst_case(&caps, 0, 0).expect("longest feedback");
    let table = [
        (tools::SizePart::File, "file", "bytes"),
        (tools::SizePart::WriteContent, "write content", "bytes"),
        (tools::SizePart::EditOld, "edit old snippets", "bytes"),
        (tools::SizePart::EditNew, "edit new snippets", "bytes"),
        (tools::SizePart::EditItems, "edit list", "edits"),
        (tools::SizePart::EditResult, "edit result", "bytes"),
        (tools::SizePart::EditReplacements, "edit replacement count", "replacements"),
        (tools::SizePart::ListGlob, "list glob", "bytes"),
    ];
    for (what, subject, unit) in table {
        let outcome = tools::Outcome::TooLarge { what, size: u64::MAX, bound: u32::MAX };
        let (text, error) = render_outcome(&outcome, &tools::Namespace::One, maximum).expect("size feedback");
        let text = String::from_utf8(text.into_vec()).expect("UTF8 words");
        assert!(error);
        assert!(text.starts_with(subject), "subject: {text}");
        assert!(text.contains(&format!("18446744073709551615 {unit}")), "observed size: {text}");
        assert!(text.contains(&format!("bound 4294967295 {unit}")), "exceeded bound: {text}");
        assert!(!text.contains("omitted"), "the longest form must fit: {text}");
    }
}

#[test]
fn owed_results_past_their_room_are_cut_with_markers() {
    let room = 160;
    let original = "é界🦀".repeat(30);
    let outcome = tools::Outcome::Exited {
        exit: tools::Exit::Code { code: 0 },
        head: original.as_bytes().into(),
        tail: Box::new([]),
        dropped: 0,
        timeout: tools::TimeoutClamp::Within,
        output: tools::BytesClamp::Within,
    };
    let (cut, error) = render_outcome(&outcome, &tools::Namespace::One, room / 2).unwrap();
    assert!(!error);
    for result in [cut, crate::render::cut_text(original.as_bytes().into(), room / 2)] {
        let text = core::str::from_utf8(&result).unwrap();
        assert!(result.len() <= 80);
        assert!(text.contains("\n["));
        assert!(text.ends_with(" bytes omitted]"));
    }
}

fn render_job(end: tools::JobEnd) -> (Box<[u8]>, bool) {
    render_outcome(
        &tools::Outcome::Job {
            job: tools::JobNumber(7),
            head: b"new".as_slice().into(),
            tail: b"last".as_slice().into(),
            dropped: 11,
            output: tools::BytesClamp::Within,
            wait: tools::TimeoutClamp::Within,
            end,
        },
        &tools::Namespace::One,
        1024,
    )
    .unwrap()
}

#[test]
fn a_running_command_names_the_job_to_read_next() {
    let outcome = tools::Outcome::Running {
        timeout: tools::TimeoutClamp::Within,
        job: tools::JobNumber(3),
        head: b"start".as_slice().into(),
        tail: Box::default(),
        dropped: 0,
        output: tools::BytesClamp::Within,
        after: tools::TimeoutClamp::Within,
    };
    assert_eq!(
        render_outcome(&outcome, &tools::Namespace::One, 1024).unwrap(),
        (b"job 3 running; use job to read the rest\nstart".as_slice().into(), false)
    );
}

#[test]
fn a_running_jobs_answer_renders_only_its_unread_output() {
    assert_eq!(
        render_job(tools::JobEnd::Running),
        (b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\nrunning".as_slice().into(), false)
    );
}

#[test]
fn a_successful_jobs_answer_renders_its_exit_code() {
    assert_eq!(
        render_job(tools::JobEnd::Ended(tools::Exit::Code { code: 0 })),
        (
            b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\nexit code 0".as_slice().into(),
            false
        )
    );
}

#[test]
fn a_failed_jobs_answer_renders_its_exit_code() {
    assert_eq!(
        render_job(tools::JobEnd::Ended(tools::Exit::Code { code: 2 })),
        (
            b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\nexit code 2".as_slice().into(),
            true
        )
    );
}

#[test]
fn a_signalled_jobs_answer_renders_its_terminal_signal() {
    assert_eq!(
        render_job(tools::JobEnd::Ended(tools::Exit::Signal { signal: 9 })),
        (b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\nsignal 9".as_slice().into(), true)
    );
}

#[test]
fn a_timed_out_jobs_answer_says_its_deadline_passed() {
    assert_eq!(
        render_job(tools::JobEnd::Ended(tools::Exit::TimedOut)),
        (
            b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\ncommand timed out"
                .as_slice()
                .into(),
            true
        )
    );
}

#[test]
fn a_stopped_jobs_answer_says_it_was_stopped() {
    assert_eq!(
        render_job(tools::JobEnd::Stopped),
        (b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\nstopped".as_slice().into(), true)
    );
}

#[test]
fn a_cancelled_jobs_answer_says_it_was_cancelled() {
    assert_eq!(
        render_job(tools::JobEnd::Cancelled),
        (
            b"job 7 output since its last answer:\nnew\n... 11 bytes dropped ...\nlast\ncancelled".as_slice().into(),
            true
        )
    );
}

#[test]
fn a_commands_yield_clamp_names_the_asked_and_ceiling() {
    let outcome = tools::Outcome::Running {
        timeout: tools::TimeoutClamp::Within,
        job: tools::JobNumber(3),
        head: Box::default(),
        tail: Box::default(),
        dropped: 0,
        output: tools::BytesClamp::Within,
        after: tools::TimeoutClamp::Clamped { asked: Duration::from_secs(90), ceiling: Duration::from_secs(30) },
    };
    assert_eq!(
        render_outcome(&outcome, &tools::Namespace::One, 1024).unwrap(),
        (b"job 3 running; use job to read the rest\n[yield clamped: 90000 ms to 30000 ms]".as_slice().into(), false)
    );
}

#[test]
fn a_jobs_wait_clamp_names_the_asked_and_ceiling() {
    let outcome = tools::Outcome::Job {
        job: tools::JobNumber(3),
        head: Box::default(),
        tail: Box::default(),
        dropped: 0,
        output: tools::BytesClamp::Within,
        end: tools::JobEnd::Running,
        wait: tools::TimeoutClamp::Clamped { asked: Duration::from_secs(90), ceiling: Duration::from_secs(30) },
    };
    assert_eq!(
        render_outcome(&outcome, &tools::Namespace::One, 1024).unwrap(),
        (
            b"job 3 output since its last answer:\nrunning\n[wait clamped: 90000 ms to 30000 ms]".as_slice().into(),
            false
        )
    );
}

#[test]
fn a_jobs_output_clamp_names_the_asked_and_ceiling() {
    let outcome = tools::Outcome::Job {
        job: tools::JobNumber(3),
        head: Box::default(),
        tail: Box::default(),
        dropped: 0,
        output: tools::BytesClamp::Clamped { asked: 90, ceiling: 30 },
        end: tools::JobEnd::Running,
        wait: tools::TimeoutClamp::Within,
    };
    assert_eq!(
        render_outcome(&outcome, &tools::Namespace::One, 1024).unwrap(),
        (
            b"job 3 output since its last answer:\nrunning\n[output clamped: 90 bytes to 30 bytes]".as_slice().into(),
            false
        )
    );
}

#[test]
fn a_running_command_says_its_deadline_and_yield_clamps() {
    let outcome = tools::Outcome::Running {
        job: tools::JobNumber(3),
        head: Box::default(),
        tail: Box::default(),
        dropped: 0,
        timeout: tools::TimeoutClamp::Clamped { asked: Duration::from_secs(180), ceiling: Duration::from_secs(120) },
        after: tools::TimeoutClamp::Clamped { asked: Duration::from_secs(90), ceiling: Duration::from_secs(30) },
        output: tools::BytesClamp::Within,
    };
    assert_eq!(render_outcome(&outcome, &tools::Namespace::One, 1024).unwrap(),
        (b"job 3 running; use job to read the rest\n[timeout clamped: 180000 ms to 120000 ms]\n[yield clamped: 90000 ms to 30000 ms]".as_slice().into(), false));
}

#[test]
fn a_full_jobs_problem_names_the_bound_and_recovery() {
    assert_eq!(crate::failure::problem(smith_domain::llm::Problem::JobsFull { jobs: 4 }, 1024).unwrap().as_ref(),
        b"invalid yield_seconds: at most 4 jobs may run at once; job with stop stops one, or reads the end of one that ended");
}

#[test]
fn an_unknown_job_problem_names_which_number_was_invalid() {
    assert_eq!(
        crate::failure::problem(smith_domain::llm::Problem::UnknownJob { job: 17 }, 1024).unwrap().as_ref(),
        b"invalid job 17: no running answer named it, or its end was already answered"
    );
}

#[test]
fn skein_failures_map_to_the_sessions_classes() {
    use skein_llm::{Cap, Failure as Source, Phase};
    use smith_domain::llm::{Failure, Limit};

    let retry = Duration::from_secs(7);
    for (source, expected) in [
        (Source::Unauthorized, Failure::Unauthorized),
        (Source::Exhausted { retry_after: Some(retry) }, Failure::Exhausted { retry_after: Some(retry) }),
        (Source::Exhausted { retry_after: None }, Failure::Exhausted { retry_after: None }),
        (Source::RateLimited { retry_after: retry }, Failure::RateLimited { retry_after: retry }),
        (Source::Refused, Failure::Refused),
        (Source::Unsupported, Failure::Unsupported),
        (Source::Overloaded, Failure::Overloaded),
        (Source::Unavailable, Failure::Unavailable),
        (Source::ContextTooLong, Failure::ContextTooLong),
        (Source::Invalid, Failure::Invalid),
        (
            Source::Limit { which: Cap::Value, bound: 7 },
            Failure::Limit { which: Limit::Path(smith_domain::llm::Cap::AccessToken), bound: 7 },
        ),
        (Source::Protocol, Failure::Protocol),
        (Source::Cancelled, Failure::Cancelled),
        (Source::TimedOut { phase: Phase::Head }, Failure::TimedOut { phase: smith_domain::llm::Phase::Head }),
    ] {
        assert_eq!(crate::failure::class(source), expected, "{source:?}");
    }
}

#[test]
fn each_providers_prompt_allowance_counts_its_tool_inventory() {
    use skein_llm::Provider;

    let client = skein_llm_world::limits();
    for provider in [Provider::OpenAi, Provider::OpenAiCodex, Provider::Anthropic] {
        let empty = crate::prompt_allowance(provider, &client, 0).expect("empty tool inventory");
        let one = crate::prompt_allowance(provider, &client, 1).expect("one tool");
        let three = crate::prompt_allowance(provider, &client, 3).expect("three tools");
        assert!(empty > 0 && one > empty && three > one, "{provider:?}");
    }
}

#[test]
fn a_call_with_no_account_carries_no_credential() {
    let grants = crate::Grants::new(1, 64).expect("bounded grants");
    let missing = smith_domain::GrantName { account: 7, generation: 1 };
    for (name, expected) in [(None, Ok(false)), (Some(missing), Err(crate::GrantError::Missing))] {
        assert_eq!(grants.credential(name, skein_lib::Time::ZERO).map(|credential| credential.is_some()), expected);
    }
}
