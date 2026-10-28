mod credentials;
mod program_check;
mod program_commands;
mod program_directory;
mod program_rechecks;
mod program_runs;
mod relay;
mod sources;
pub mod support;

use smith_local_process_world::{Placement, referee::CheckoutRead};
use smith_real_world::{Scratch, World};

#[test]
fn the_terminal_modes_are_unchanged_after_smith_exits() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let (ran, queries) = support::chat_modes(
        &scratch,
        vec![Action::Wait(b"> ".as_slice().into()), Action::Send(b"Ask first\n".as_slice().into()), Action::Eof],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(queries.len(), 1);
}

#[test]
fn end_of_input_at_the_terminal_lets_the_run_answer_then_exits_0() {
    use smith_local_process_world::terminal::Action;
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new();
    let mut settings: toml::Value =
        toml::from_str(std::str::from_utf8(&support::fake_provider_settings(&scratch)).unwrap()).unwrap();
    settings["accounts"]["codex"] = toml::Value::try_from(serde_json::json!({"source":"key"})).unwrap();
    settings["endpoints"]["codex"].as_table_mut().unwrap().insert("preset".into(), "openai".into());
    let _file = support::user_layer(&scratch, toml::to_string(&settings).unwrap().as_bytes());
    let token: serde_json::Value =
        serde_json::from_slice(&std::fs::read(scratch.path().join("auth.json")).unwrap()).unwrap();
    let key = scratch.path().join("credentials/keys/codex");
    std::fs::create_dir_all(key.parent().unwrap()).unwrap();
    std::fs::write(&key, token["tokens"]["access_token"].as_str().unwrap()).unwrap();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let ran = support::key_chat(
        &scratch,
        vec![Action::Wait(b"> ".as_slice().into()), Action::Send(b"Ask first\n".as_slice().into()), Action::Eof],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(String::from_utf8_lossy(&ran.stdout).contains("API key"));
    assert!(ran.stdout.windows(12).any(|bytes| bytes == b"First answer"));
}

#[test]
fn a_window_size_change_reaches_the_status_line() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let (ran, queries) = support::chat_resized(
        &scratch,
        vec![
            Action::Wait(b"> ".as_slice().into()),
            Action::Send(b"Ask first\n".as_slice().into()),
            Action::Wait(b"codex/gpt-6-luna; API key: in ".as_slice().into()),
            Action::Wait(b"\r\nFinishing now\r\n".as_slice().into()),
            Action::Send(b"Again\n".as_slice().into()),
            Action::Eof,
        ],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(queries.len(), 2);
    let widths: Vec<_> = ran
        .stdout
        .split(|byte| *byte == b'\n')
        .map(|line| line.strip_suffix(b"\r").unwrap_or(line))
        .filter(|line| line.starts_with(b"codex/gpt-6-luna;"))
        .map(<[u8]>::len)
        .collect();
    assert_eq!(widths, [80, 20]);
}

#[test]
fn the_interactive_default_fits_its_memory() {
    use smith_local_shell::{check, command, settings};

    for (model, account, source) in
        [("codex/gpt-6-astra", "codex", "env"), ("anthropic/claude-opus-5-5", "anthropic", "env")]
    {
        let scratch = Scratch::new();
        let token = smith_local_process_world::world::login_at(skein_shell::Clock::new().now().wall, false);
        let login =
            serde_json::json!({"tokens":{"access_token":std::str::from_utf8(&token).unwrap(),"account_id":"acc"}});
        std::fs::write(scratch.path().join("auth.json"), serde_json::to_vec(&login).unwrap()).unwrap();
        let variable = if account == "codex" { "SMITH_FIXTURE_TOKEN" } else { "ANTHROPIC_API_KEY" };
        let _file = support::user_layer(
            &scratch,
            format!(
                "credentials='{}'\nmodel='{model}'\n[accounts.{account}]\nsource='{source}'\nvariable='{variable}'\n",
                scratch.path().join("credentials").display()
            )
            .as_bytes(),
        );
        let ran = support::smith_with_environment(
            &scratch,
            &["check", "--json"],
            b"",
            vec![("ANTHROPIC_API_KEY".into(), "synthetic-unissued".into())],
        );
        assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
        let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("interactive check document");
        let limits = report["limits"].as_array().expect("named limits");
        let memory = limits.iter().find(|limit| limit["name"] == "declared.memory").expect("default memory")["value"]
            .as_u64()
            .expect("memory bytes");
        let settings = settings::Settings {
            paths: settings::Paths::default(),
            workspace: scratch.path().into(),
            values: report["settings"]["values"]
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, sourced)| {
                    (
                        name.clone(),
                        settings::Sourced {
                            value: if name == "credentials" {
                                toml::Value::String(scratch.path().join("credentials").display().to_string())
                            } else {
                                serde_json::from_value(sourced["value"].clone()).expect("checked setting")
                            },
                            origin: settings::Origin::BuiltIn,
                        },
                    )
                })
                .collect(),
        };
        let mut resolved = settings::resolve::resolve(
            &settings,
            &command::Command::Check { shared: command::Shared::default(), json: true, execution: None },
        )
        .unwrap_or_else(|why| panic!("{}", why.line()));
        let (local, _) = check::interactive_limits(&mut resolved).expect("all interactive checks pass");
        let worst = smith_local_service::worst_case(&local).expect("complete service footprint");
        assert_eq!(memory, smith_agent_service::profile::standard().declared.memory);
        assert!(worst <= memory - 134_217_728, "{account}: worst_case={worst}; margin={}", memory - worst);
        println!(
            "{account}: interactive worst_case={worst} bytes; margin={} bytes under the shipped cap",
            memory - worst
        );
        assert!(!scratch.path().join("state").exists(), "check starts no store or sign-in");
    }
}

#[test]
fn the_shipped_local_and_agent_answer_on_a_terminal() {
    let scratch = Scratch::new();
    let scenario = World::new(31, &scratch, Placement::Spawned, b"First answer").scenario;
    let seen = support::run(&scratch, scenario);
    assert_eq!(seen.queries.len(), 1);
}

#[test]
fn a_first_run_signs_in_and_ends_with_a_commit_in_place() {
    let scratch = Scratch::new();
    let mut scenario = World::changed(32, &scratch, Placement::Spawned).scenario;
    scenario.launch.authenticated = true;
    scenario.authentication = Some(smith_local_process_world::world::Authentication::SignIn);
    std::fs::remove_file(scenario.launch.token_directory.join("0.json")).expect("first sign-in has no token");
    let seen = support::run(&scratch, scenario);
    let oauth = seen.oauth.expect("issuer and browser observations");
    assert_eq!(oauth.posts, 1);
    assert_eq!(oauth.pages, 1);
    assert!(oauth.browser_replied && oauth.saved_before_query);
    let checkout = scratch.checkout();
    let head = checkout.head().expect("committed head");
    let files = checkout.files(&head);
    assert_eq!(files[b"original.txt".as_slice()], b"edited\n");
    assert_eq!(files[b"result.txt".as_slice()], b"new\n");
    assert_eq!(files[b"command.txt".as_slice()], b"ran\n");
    assert_eq!(files[b"checks-ran.txt".as_slice()], b"checked\n");
}

#[test]
fn a_second_run_resumes_the_chat_from_its_files() {
    let scratch = Scratch::new();
    let first = support::run(&scratch, World::new(33, &scratch, Placement::Spawned, b"First answer").scenario);
    let second = support::run(&scratch, World::new(34, &scratch, Placement::Spawned, b"Second answer").scenario);
    assert!(second.queries[0].messages.len() > first.queries[0].messages.len());
    assert!(
        second.queries[0].messages.iter().flat_map(|message| message.parts.iter()).any(|part| {
            matches!(part, skein_fake_llm_domain::api::Part::ToolCall { name, arguments, .. }
            if name.as_ref() == b"finish" && arguments.windows(12).any(|bytes| bytes == b"First answer"))
        }),
        "the second actual agent received the first run's durable history"
    );
}

#[test]
fn a_second_smith_resumes_the_chat_from_its_files() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let script = || vec![Action::Send(b"hello\n".as_slice().into()), Action::Eof];
    let (first, first_queries) = support::chat(&scratch, script());
    assert_eq!(first.code, 0, "{}", String::from_utf8_lossy(&first.stderr));
    let (second, second_queries) = support::chat(&scratch, script());
    assert_eq!(second.code, 0, "{}", String::from_utf8_lossy(&second.stderr));
    assert!(second_queries[0].messages.len() > first_queries[0].messages.len());
    assert!(second_queries[0].messages.iter().flat_map(|message| message.parts.iter()).any(|part| {
        matches!(part, skein_fake_llm_domain::api::Part::ToolCall { name, arguments, .. }
            if name.as_ref() == b"finish" && arguments.windows(12).any(|bytes| bytes == b"First answer"))
    }));
}

#[test]
fn an_interrupt_cancels_the_run() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::chat_waiting(
        &scratch,
        vec![
            Action::Send(b"hello\n".as_slice().into()),
            Action::Wait(b"Starting now".as_slice().into()),
            Action::Send(Box::new([3])),
            Action::Wait(b"interrupted: the run was cancelled".as_slice().into()),
            Action::Eof,
        ],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(ran.stdout.windows(34).any(|part| part == b"interrupted: the run was cancelled"));
}

#[test]
fn smith_on_a_terminal_signs_in_and_commits_in_place() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let mut settings: toml::Value =
        toml::from_str(std::str::from_utf8(&support::fake_provider_settings(&scratch)).unwrap()).unwrap();
    let host = smith_real_world::loopback();
    let redirect = smith_real_world::redirect_port();
    settings["accounts"]["codex"] = toml::Value::try_from(serde_json::json!({
        "source":"sign_in", "client_id":"client", "authorization_url":format!("http://{host}:34444/authorize"),
        "token_url":format!("http://{host}:34444/token"), "redirect_uri":format!("http://127.0.0.1:{redirect}/callback"),
        "scope":"read", "token_format":"form"
    }))
    .unwrap();
    settings.as_table_mut().unwrap().insert(
        "workspace".into(),
        toml::Value::try_from(serde_json::json!({
            "directories":[{"name":"repo","path":scratch.path().join("repo"),"writable":true,"git":true}],
            "conventions":{"guide":".smith-test/guide","checks":".smith-test/check"},
            "contract":{"form":"change","checks_must_pass":true,
                "fields":[{"name":"title","max":256},{"name":"body","max":1024}]}
        }))
        .unwrap(),
    );
    let _file = support::user_layer(&scratch, toml::to_string(&settings).unwrap().as_bytes());
    let before = scratch.checkout().head().unwrap();
    let ran = support::chat_signed_in(
        &scratch,
        vec![
            Action::Send(b"change it\n".as_slice().into()),
            Action::Wait(b"Change delivered".as_slice().into()),
            Action::Eof,
        ],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(String::from_utf8_lossy(&ran.stdout).contains(&format!("Sign in: http://{host}:34444/authorize")));
    assert!(scratch.path().join("credentials/tokens/codex").is_file());
    let checkout = scratch.checkout();
    let after = checkout.head().unwrap();
    assert_ne!(before, after);
    let files = checkout.files(&after);
    assert_eq!(files[b"original.txt".as_slice()], b"edited\n");
    assert_eq!(files[b"result.txt".as_slice()], b"new\n");
    assert_eq!(files[b"command.txt".as_slice()], b"ran\n");
    assert_eq!(files[b"checks-ran.txt".as_slice()], b"checked\n");
}

#[test]
fn the_startup_refusals_of_both_commands_are_observed_outside() {
    let scratch = Scratch::new();
    let agent = skein_scratch::paths::binary("smith-agent");
    let errors = support::refusal_program(&agent, &scratch, vec!["".into()]);
    assert!(String::from_utf8_lossy(&errors).contains("configuration metadata"));
    let config = scratch.path().join("config/smith/config.toml");
    std::fs::create_dir_all(&config).expect("settings metadata refusal");
    let errors = support::refusal_code(&scratch, Vec::new(), 2);
    assert!(String::from_utf8_lossy(&errors).contains("regular file"));
    let path = scratch.path().join("agent-config.json");
    std::fs::write(
        &path,
        br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"conversations":1,"calls_per_response":2,"tool_payload":256,"read_window":256},"policy":{"group_stop":10}},"endpoints":[{"name":"fake","number":0,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","transport":"plaintext","connect":1000,"handshake":1000,"models":[{"name":"fake","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":1000,"idle":1000}]}],"environment":[]}"#,
    )
    .expect("valid agent fixture");
    let errors = support::refusal_program(&agent, &scratch, vec![path.clone().into()]);
    assert_eq!(
        String::from_utf8_lossy(&errors).lines().last(),
        Some("smith: the run could not answer: the channel ended before the answer")
    );
    std::fs::write(&path, br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"conversations":1,"calls_per_response":2,"tool_payload":256,"read_window":256},"policy":{"group_stop":10}},"endpoints":[{"name":"invalid","number":1,"dialect":1,"account":0,"provider":"codex","connect":10000,"handshake":10000,"models":[{"name":"fake","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":60000,"idle":30000}],"address":"192.0.2.1:8080","transport":"plaintext"}],"environment":[]}"#).expect("invalid transport fixture");
    let errors = support::refusal_program(&agent, &scratch, vec![path.into()]);
    assert_eq!(
        String::from_utf8_lossy(&errors).lines().last(),
        Some("smith: plaintext endpoint address must be loopback")
    );
    assert!(!scratch.path().join("chat").exists(), "refusals leave no chat");
}

#[test]
fn the_live_outcome_referee_runs_on_the_shared_fake_binary_harness() {
    let report = Scratch::new();
    support::live_run::run_fake(&report, &World::new(35, &report, Placement::Spawned, b"First answer").scenario);
    let change = Scratch::new();
    support::live_run::run_fake(&change, &World::changed(36, &change, Placement::Spawned).scenario);
}

#[test]
fn smith_agent_refuses_a_malformed_configuration_before_its_channel_opens() {
    let scratch = Scratch::new();
    let path = scratch.path().join("malformed-agent.json");
    std::fs::write(&path, br#"{"profile":"standard"}"#).expect("malformed configuration fixture");
    let errors = support::refusal_program(&skein_scratch::paths::binary("smith-agent"), &scratch, vec![path.into()]);
    let line = std::str::from_utf8(&errors).expect("UTF-8 refusal");
    assert!(line.starts_with("smith: configuration JSON:"));
    assert_eq!(line.lines().count(), 1, "startup writes one refusal");
}

#[test]
fn smith_agent_names_a_broken_relationship_and_exits_1_before_its_channel_opens() {
    let scratch = Scratch::new();
    let path = scratch.path().join("read-past-payload.json");
    std::fs::write(&path,
        br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"read_window":1025,"tool_payload":1024}},"endpoints":[{"name":"fake","number":0,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","transport":"plaintext","connect":1000,"handshake":1000,"models":[{"name":"fake","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":1000,"idle":1000}]}],"environment":[]}"#,
    ).expect("broken relationship fixture");
    let errors = support::refusal_program(&skein_scratch::paths::binary("smith-agent"), &scratch, vec![path.into()]);
    assert_eq!(
        std::str::from_utf8(&errors).expect("UTF-8 refusal"),
        "smith: read-in-payload: declared.read_window=1025 bytes; declared.tool_payload=1024 bytes\n"
    );
}

#[test]
fn smith_agent_names_a_repeated_environment_entry_before_its_channel_opens() {
    let scratch = Scratch::new();
    let path = scratch.path().join("repeated-environment.json");
    std::fs::write(&path,
        br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"tool_payload":256,"read_window":256}},"endpoints":[{"name":"fake","number":0,"dialect":1,"account":0,"provider":"codex","address":"127.0.0.1:8080","transport":"plaintext","connect":1000,"handshake":1000,"models":[{"name":"fake","window":65536,"output":512,"reasoning_item":64,"prices":{"input":1,"cached":1,"output":1,"unit":1},"head":1000,"idle":1000}]}],"environment":[{"name":"A","value":"one"},{"name":"A","value":"two"}]}"#,
    ).expect("repeated environment fixture");
    let errors = support::refusal_program(&skein_scratch::paths::binary("smith-agent"), &scratch, vec![path.into()]);
    assert_eq!(
        std::str::from_utf8(&errors).expect("UTF-8 refusal"),
        "smith: environment: entry 1 has an invalid or repeated name\n"
    );
}

#[test]
fn smith_agent_refuses_empty_endpoints_by_name_before_its_channel_opens() {
    let scratch = Scratch::new();
    let path = scratch.path().join("empty-endpoints.json");
    std::fs::write(
        &path,
        br#"{"profile":{"name":"standard","declared":{"memory":1099511627776,"tool_payload":256,"read_window":256}},"endpoints":[],"environment":[]}"#,
    )
    .expect("empty endpoints fixture");
    let errors = support::refusal_program(&skein_scratch::paths::binary("smith-agent"), &scratch, vec![path.into()]);
    assert_eq!(
        std::str::from_utf8(&errors).expect("UTF-8 refusal"),
        "smith: field: declared.endpoints must not be empty\n"
    );
}

#[test]
fn standard_profile_runs_a_large_coding_write_and_the_following_checks() {
    use skein_fake_llm_domain::api;
    use smith_local_process_world::referee::CheckoutRead;

    let scratch = Scratch::new();
    let scenario = World::changed(37, &scratch, Placement::Spawned).scenario;
    let large = serde_json::to_vec(&serde_json::json!({
        "path": "result.txt", "content": "// coding regression\n".repeat(160)
    }))
    .expect("large coding tool arguments");
    assert!(large.len() > 2048, "coding write exceeds the former input limit");
    let mut turns = vec![(b"write".as_slice(), large)];
    for (name, arguments) in [
        ("read", r#"{"path":"result.txt"}"#),
        ("read", r#"{"path":"original.txt"}"#),
        ("edit", r#"{"path":"original.txt","edits":[{"old":"original","new":"edited"}]}"#),
        ("write", r#"{"path":"result.txt","content":"new\n"}"#),
        ("shell", r#"{"command":"printf 'ran\n' > command.txt"}"#),
        (
            "finish",
            r#"{"form":"change","text":"","fields":{"title":"Updated result","body":"Created the result file"}}"#,
        ),
    ] {
        turns.push((name.as_bytes(), arguments.as_bytes().to_vec()));
    }
    let token = support::settings(&scratch, &scenario);
    let peer = smith_agent_process_world::fake::configured_transport(
        Box::new([api::Script {
            cue: b"@local-shell".as_slice().into(),
            turns: turns
                .into_iter()
                .map(|(name, arguments)| api::Turn {
                    lines: Box::new([api::Line::Call { name: name.into(), arguments: arguments.into() }]),
                    finish: api::Finish::ToolCalls,
                    tokens: 1,
                })
                .collect(),
        }]),
        skein_llm::Credential { value: token, account_id: b"acc".as_slice().into(), scheme: skein_llm::Scheme::Bearer },
        skein_lib::Duration::ZERO,
        skein_fake_peers::Transport::Tls,
    );
    let seen = support::run_with_provider(&scratch, scenario, Some(peer));
    assert_eq!(seen.queries.len(), 7, "large write and every subsequent call completed");
    assert!(
        seen.queries.iter().skip(1).any(|query| query
            .messages
            .iter()
            .any(|message| { message.parts.iter().any(|part| matches!(part, api::Part::ToolOutput { .. })) })),
        "follow-up provider prompts contain actual workspace answers"
    );
    let checkout = scratch.checkout();
    let files = checkout.files(&checkout.head().expect("committed head"));
    assert_eq!(files[b"result.txt".as_slice()], b"new\n");
    assert_eq!(files[b"original.txt".as_slice()], b"edited\n");
    assert_eq!(files[b"checks-ran.txt".as_slice()], b"checked\n");
}

fn refusal_settings(output: u32, read_window: u32) -> Vec<u8> {
    format!(
        "model='codex/gpt-6-luna'\nchat='refused'\ninstructions='No run may start'\n\
        [accounts.codex]\nsource='env'\n\
        [models.'codex/gpt-6-luna']\noutput={output}\n\
        [deployment]\nread_window={read_window}\ntool_payload=40960\n"
    )
    .into_bytes()
}

fn local_refusal_output(settings: &[u8]) -> (Vec<u8>, Scratch) {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, settings);
    let ran = support::smith(&scratch, &["check"], b"");
    assert_eq!(ran.code, 2, "product startup refused");
    assert!(!scratch.path().join("state").exists(), "configuration refusal precedes chat/state and a run");
    (ran.stderr, scratch)
}

#[test]
fn local_bad_setting_exits_2_and_names_output_before_a_run() {
    let (output, _scratch) = local_refusal_output(&refusal_settings(0, 32768));
    let stderr = std::str::from_utf8(&output).expect("refusal is UTF-8");
    assert!(stderr.contains("output"), "{stderr}");
    assert!(stderr.contains("positive"), "{stderr}");
}

#[test]
fn local_derived_limit_exits_2_and_names_both_values_before_a_run() {
    let (output, _scratch) = local_refusal_output(&refusal_settings(32000, 40961));
    let stderr = std::str::from_utf8(&output).expect("refusal is UTF-8");
    assert!(stderr.contains("read-in-payload"), "{stderr}");
    assert!(stderr.contains("40961") && stderr.contains("40960"), "{stderr}");
}

#[test]
fn local_host_io_failure_keeps_exit_1() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let state = scratch.path().join("state");
    std::fs::write(&state, b"a file cannot be the state directory").expect("block state directory");
    let ran = support::smith(&scratch, &["exec", "hello", "--chat", "refused"], b"");
    assert_eq!(ran.code, 1);
    assert!(String::from_utf8_lossy(&ran.stderr).contains("Not a directory"));
    assert_eq!(std::fs::read(state).expect("unchanged state file"), b"a file cannot be the state directory");
}

#[test]
fn exec_in_a_repository_without_a_commit_exits_two_with_its_line() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let (ran, requests) = support::exec_without_head(&scratch);
    assert_eq!(ran.code, 2);
    let stderr = std::str::from_utf8(&ran.stderr).expect("operator lines are UTF-8");
    assert_eq!(stderr.lines().last(), Some("smith: run refused: repository workspace: its head cannot be read"));
    assert!(requests.is_empty(), "a refused start sends no provider request");
}

#[test]
fn local_chat_store_io_failure_keeps_exit_1() {
    use std::fmt::Write as _;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let workspace = scratch.path().canonicalize().unwrap();
    let digest = skein_io::digest::digest(workspace.as_os_str().as_encoded_bytes());
    let mut key = String::new();
    for part in digest.0 {
        write!(&mut key, "{part:016x}").expect("fixture workspace key");
    }
    let state = scratch.path().join("state/smith/chats").join(key);
    std::fs::create_dir_all(&state).expect("state root");
    let chat = state.join("refused");
    std::fs::write(&chat, b"a file cannot be the chat directory").expect("block chat directory");
    let ran = support::smith(&scratch, &["exec", "hello", "--chat", "refused"], b"");
    assert_eq!(ran.code, 1);
    assert!(String::from_utf8_lossy(&ran.stderr).contains("File exists"), "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(std::fs::read(chat).expect("unchanged chat file"), b"a file cannot be the chat directory");
}

#[test]
fn a_one_turn_local_run_is_refused_by_the_actor_before_any_completion() {
    let scratch = Scratch::new();
    let scenario = World::new(63, &scratch, Placement::Spawned, b"Unused").scenario;
    support::actor_refusal::configure(&scratch, &scenario);
    let ran = support::smith(&scratch, &["exec", "A refused run"], b"");
    assert_eq!(ran.code, 2);
    let records = support::read_trace(scratch.path(), &[]);
    let completed = support::actor_refusal::completed(&records);
    assert_eq!(completed.len(), 1, "one actor terminal");
    let completed = completed.first().expect("actual refused run");
    assert_eq!(completed.status, smith_events::Status::Refused);
    let failure = completed.failure.as_ref().expect("typed reserve refusal");
    assert_eq!(failure.class, smith_events::AnswerClass::Invalid);
    assert_eq!(failure.which.as_deref(), Some(b"budget_reserve_turns".as_slice()));
    assert_eq!(failure.reason, None, "an admission refusal invents no provider prose");
    assert!(
        !records.iter().any(|record| matches!(&record.event, smith_events::Event::ResponseStarted(_))),
        "reserve refusal starts no completion or provider request"
    );
}

#[test]
fn smith_agent_spawned_by_the_sample_host_answers() {
    support::sample_host::run();
}

#[test]
fn the_agents_trace_header_carries_its_effective_limits() {
    let scratch = Scratch::new();
    let _seen = support::run(&scratch, World::new(81, &scratch, Placement::Spawned, b"First answer").scenario);
    let records = support::read_trace(scratch.path(), &[]);
    let smith_events::Event::SessionStarted(header) = &records[0].event else { panic!("startup header first") };
    assert!(header.limits.iter().any(|limit| limit.name.as_ref() == b"declared.memory"));
    assert!(header.limits.iter().any(|limit| limit.name.as_ref() == b"session.history_bytes"));
}

#[test]
fn a_refusal_in_the_user_layer_names_its_file_line_and_key() {
    let scratch = Scratch::new();
    let file = support::user_layer(&scratch, b"model = \"codex/gpt-6-luna\"\nmodle = 1\n");
    let ran = support::smith(&scratch, &["check"], b"");
    assert_eq!(ran.code, 2);
    let line = String::from_utf8(ran.stderr).expect("UTF-8");
    assert!(line.starts_with("smith: ") && line.contains(&format!("{}:2", file.display())) && line.contains("modle"));
}

#[test]
fn smith_check_json_shows_each_value_with_its_layer() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, b"model='codex/gpt-6-luna'\neffort='low'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\n");
    let directory = scratch.path().join(".smith");
    std::fs::create_dir(&directory).expect("repository layer");
    std::fs::write(directory.join("config.toml"), b"[environment.set]\nREPO='repository-value'\n")
        .expect("repository environment");
    let ran = support::smith(&scratch, &["check", "--json", "-c", "effort='high'"], b"");
    assert_eq!(ran.code, 3, "{}", String::from_utf8_lossy(&ran.stderr));
    let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("one check document");
    assert_eq!(report["version"], 2);
    assert_eq!(report["outside_models"]["anthropic/claude-opus-5-5"], "sub_agents is off");
    assert_eq!(report["settings"]["values"]["model"]["origin"]["layer"], "user");
    assert_eq!(report["settings"]["values"]["effort"]["value"], "high");
    assert_eq!(report["settings"]["values"]["effort"]["origin"]["kind"], "override");
    assert_eq!(report["settings"]["values"]["environment.set.REPO"]["origin"]["layer"], "repository");
    assert!(!String::from_utf8_lossy(&ran.stdout).contains("repository-value"));
    assert_eq!(
        report["formats"],
        serde_json::json!({"channel":5,"charter":4,"transcript":6,"events":4,"chat_store":1,"check":2})
    );
    for tool in ["sh", "rg", "git"] {
        assert!(report["tools"][tool].as_str().expect("tool path").starts_with('/'));
    }
    assert_eq!(report["agent"], serde_json::json!({"name":"smith", "version":env!("CARGO_PKG_VERSION")}));
    assert!(report.get("build").is_none(), "check reports the agent name and version");
}

#[test]
fn a_repository_layer_setting_an_account_is_refused_naming_the_file() {
    let scratch = Scratch::new();
    let directory = scratch.path().join(".smith");
    std::fs::create_dir(&directory).expect("repository layer");
    let file = directory.join("config.toml");
    std::fs::write(&file, b"[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n")
        .expect("account authority");
    let ran = support::smith(&scratch, &["check"], b"");
    assert_eq!(ran.code, 2);
    let line = String::from_utf8(ran.stderr).expect("UTF-8");
    assert!(line.contains(&file.display().to_string()) && line.contains("accounts") && line.contains("authority"));
}

#[test]
fn an_override_past_its_bound_is_refused_naming_the_override() {
    let scratch = Scratch::new();
    let ran = support::smith(&scratch, &["check", "-c", "budget.turns=4294967296"], b"");
    assert_eq!(ran.code, 2);
    let line = String::from_utf8(ran.stderr).expect("UTF-8");
    assert!(line.contains("-c override 1") && line.contains("budget.turns") && line.contains("bound"));
}

#[test]
fn smith_check_names_both_sides_of_a_budget_past_the_profile_ceiling() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, b"model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='cannot-resolve.invalid:443'\n");
    let ran = support::smith(&scratch, &["check", "-c", "budget.turns=5000"], b"");
    assert_eq!(ran.code, 2);
    assert_eq!(
        String::from_utf8(ran.stderr).expect("UTF-8"),
        "smith: budget.turns = 5000 (from -c) exceeds the standard profile's ceiling of 4096 turns\n"
    );
    assert!(ran.stdout.is_empty());
}

#[test]
fn smith_check_json_warns_of_a_wrapper_cache_outside_the_caches() {
    let scratch = Scratch::new();
    let cargo = scratch.path().join("cargo");
    for cache in ["registry", "git"] {
        std::fs::create_dir_all(cargo.join(cache)).expect("existing Rust cache");
    }
    let settings = format!(
        "model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\n[build]\npreset='rust'\n[environment.set]\nCARGO_HOME='{}'\nRUSTC_WRAPPER='sccache'\nSCCACHE_DIR='{}'\n",
        cargo.display(),
        scratch.path().join("wrapper-cache").display()
    );
    let _file = support::user_layer(&scratch, settings.as_bytes());
    let ran = support::smith(&scratch, &["check", "--json"], b"");
    assert_eq!(ran.code, 3, "{}", String::from_utf8_lossy(&ran.stderr));
    let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("one check document");
    assert!(
        report["warnings"]
            .as_array()
            .expect("warnings")
            .iter()
            .any(|warning| warning["kind"] == "wrapper_cache" && warning["configured_by"] == "RUSTC_WRAPPER")
    );
    assert!(!scratch.path().join("wrapper-cache").exists(), "check creates no cache");
}

#[test]
fn smith_check_warns_about_an_absent_cache_and_creates_nothing() {
    let scratch = Scratch::new();
    let cache = scratch.path().join("absent-cache");
    let settings = format!(
        "model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\n[build]\ncaches=['{}']\n",
        cache.display()
    );
    let _file = support::user_layer(&scratch, settings.as_bytes());
    let ran = support::smith(&scratch, &["check", "--json"], b"");
    assert_eq!(ran.code, 3, "{}", String::from_utf8_lossy(&ran.stderr));
    let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("check document");
    assert!(report["warnings"].as_array().expect("warnings").iter().any(|warning| warning["kind"] == "cache_access"
        && warning["cache"] == cache.to_str().expect("UTF-8 path")
        && warning["reason"] == "absent"));
    assert!(!cache.exists(), "check creates nothing");
}

#[test]
fn smith_check_warns_about_a_file_named_as_a_cache_and_keeps_it() {
    let scratch = Scratch::new();
    let cache = scratch.path().join("file-cache");
    std::fs::write(&cache, b"kept").expect("file in place of cache");
    let settings = format!(
        "model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\n[build]\ncaches=['{}']\n",
        cache.display()
    );
    let _file = support::user_layer(&scratch, settings.as_bytes());
    let ran = support::smith(&scratch, &["check", "--json"], b"");
    assert_eq!(ran.code, 3, "{}", String::from_utf8_lossy(&ran.stderr));
    let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("check document");
    assert!(report["warnings"].as_array().expect("warnings").iter().any(|warning| warning["kind"] == "cache_access"
        && warning["cache"] == cache.to_str().expect("UTF-8 path")
        && warning["reason"] == "not a directory"));
    assert_eq!(std::fs::read(cache).expect("cache file remains"), b"kept");
}

#[test]
fn smith_check_json_lists_the_effective_limits_by_name() {
    let scratch = Scratch::new();
    let _file = support::user_layer(
        &scratch,
        b"model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\n",
    );
    let ran = support::smith(&scratch, &["check", "--json"], b"");
    assert_eq!(ran.code, 3, "{}", String::from_utf8_lossy(&ran.stderr));
    let report: serde_json::Value = serde_json::from_slice(&ran.stdout).expect("check document");
    let limits = report["limits"].as_array().expect("effective limits");
    assert!(
        limits.iter().any(|limit| limit["name"] == "declared.tool_payload"
            && limit["value"] == 40_960
            && limit["unit"] == "bytes")
    );
    assert!(limits.iter().any(|limit| limit["name"] == "declared.head[endpoint=codex,model=gpt-6-luna]"
        && limit["value"] == 60_000
        && limit["unit"] == "ms"));
    assert!(
        limits
            .iter()
            .any(|limit| limit["name"] == "service.teardown" && limit["value"] == 40_000 && limit["unit"] == "ms")
    );
    assert!(limits.iter().all(|limit| !limit["name"].as_str().expect("limit name").contains("endpoint=0")));
}

fn dir_turns(path: &str) -> Box<[skein_fake_llm_domain::api::Turn]> {
    use skein_fake_llm_domain::api;
    let write = format!(r#"{{"path":"{path}","content":"noted\n"}}"#);
    let finish = r#"{"form":"report","text":"Wrote it","fields":{}}"#;
    [("write", write.into_bytes()), ("finish", finish.as_bytes().to_vec())]
        .into_iter()
        .map(|(name, arguments)| api::Turn {
            lines: Box::new([api::Line::Call { name: name.as_bytes().into(), arguments: arguments.into() }]),
            finish: api::Finish::ToolCalls,
            tokens: 1,
        })
        .collect()
}

#[test]
fn a_dir_is_a_named_directory_the_run_writes_unless_it_is_read_only() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let plan = Scratch::new();
    let kit = Scratch::new();
    let flag = format!("plan={}", plan.path().display());
    let ran =
        support::exec_with_turns(&scratch, &["exec", "--dir", &flag, "-"], b"Note it\n", dir_turns("/plan/notes.txt"));
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(
        std::fs::read(plan.path().join("notes.txt")).expect("the run wrote into the named directory"),
        b"noted\n"
    );
    assert!(!scratch.path().join("plan").exists(), "the name is the directory's, not a path in the working directory");

    let flag = format!("kit={}:ro", kit.path().display());
    let ran = support::exec_with_turns(
        &scratch,
        &["exec", "--json", "--dir", &flag, "-"],
        b"Note it\n",
        dir_turns("/kit/notes.txt"),
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    let verdicts: Vec<serde_json::Value> = ran
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("event record"))
        .filter(|record| record["type"] == "tool.completed" && record["tool"] == "write")
        .map(|record| record["verdict"].clone())
        .collect();
    assert_eq!(verdicts, ["read_only"], "a read-only directory refuses the write");
    assert!(!kit.path().join("notes.txt").exists(), "nothing was written into the read-only directory");
}

#[test]
fn a_writable_plain_dir_past_snapshot_bounds_starts_and_answers() {
    // One byte past process.rs's plain_limits().file_bytes.
    const UNKNOWN_FILE_BYTES: usize = 65_537;
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let plan = Scratch::new();
    std::fs::write(plan.path().join("large"), vec![b'x'; UNKNOWN_FILE_BYTES]).unwrap();
    let flag = format!("plan={}:rw", plan.path().display());
    let ran =
        support::exec_with_turns(&scratch, &["exec", "--dir", &flag, "-"], b"Note it\n", dir_turns("/plan/notes.txt"));
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(ran.stdout.windows(b"Wrote it".len()).any(|bytes| bytes == b"Wrote it"));
    assert!(!String::from_utf8_lossy(&ran.stderr).contains("could not be saved"));
    assert_eq!(std::fs::read(plan.path().join("notes.txt")).unwrap(), b"noted\n");
}

#[test]
fn a_headless_run_that_would_wait_exits_5_naming_its_chat() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran =
        support::exec_with_provider(&scratch, &["exec", "--chat", "plan", "-"], b"Ask first\n", support::Reply::Wait);
    assert_eq!(ran.code, 5, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(ran.stderr.ends_with(b"smith: needs input: answer with `smith exec --chat plan`\n"));
}

#[test]
fn smith_exec_json_ends_accepted_with_its_events_and_exit_0() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_provider(&scratch, &["exec", "--json", "-"], b"Ask first\n", support::Reply::Accepted);
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    let records: Vec<serde_json::Value> = ran
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("event record"))
        .collect();
    assert_eq!(records.first().unwrap()["type"], "session.started");
    assert_eq!(records.first().unwrap()["mode"], "exec");
    assert_eq!(records.first().unwrap()["versions"]["chat_store"], 1);
    assert_eq!(records.last().unwrap()["type"], "session.ended");
    assert_eq!(records.last().unwrap()["exit"], 0);
    assert!(records.iter().any(|r| r["type"] == "run.completed"));
    assert!(ran.stderr.windows(18).any(|bytes| bytes == b"smith: accepted in"));
}

#[test]
fn an_account_without_a_credential_exits_3_before_any_request() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    std::fs::remove_file(scratch.path().join("auth.json")).unwrap();
    let ran = support::smith(&scratch, &["exec", "Ask first"], b"");
    assert_eq!(ran.code, 3);
    assert!(ran.stdout.is_empty());
    assert!(!scratch.path().join("state/smith/chats").exists());
}

#[test]
fn an_empty_prompt_is_a_usage_error_exiting_2() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::smith(&scratch, &["exec", "-"], b" \n");
    assert_eq!(ran.code, 2);
    assert!(ran.stderr.ends_with(b"smith: usage: the prompt is empty\n"));
}

#[test]
fn the_trace_file_is_read_as_it_grows() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--trace", "events.jsonl", "-"],
        b"Ask first\n",
        support::Reply::Accepted,
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(ran.stdout, b"First answer");
    let bytes = std::fs::read(scratch.path().join("events.jsonl")).unwrap();
    assert!(bytes.ends_with(b"\n"));
    for line in bytes.split(|byte| *byte == b'\n').filter(|line| !line.is_empty()) {
        let _record: serde_json::Value = serde_json::from_slice(line).unwrap();
    }
}

#[test]
fn smith_exec_replaces_the_result_file_with_only_its_text() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let path = scratch.path().join("answer.txt");
    std::fs::write(&path, b"an older and longer result\n").unwrap();
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "-o", "answer.txt", "-"],
        b"Ask first\n",
        support::Reply::Accepted,
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(std::fs::read(path).unwrap(), b"First answer");
    assert_eq!(ran.stdout, b"First answer");
    assert!(
        ran.stderr
            .split(|byte| *byte == b'\n')
            .rfind(|line| !line.is_empty())
            .unwrap()
            .starts_with(b"smith: accepted in ")
    );
}

#[test]
fn smith_exec_json_writes_its_result_file_beside_the_event_stream() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--json", "-o", "answer.txt", "-"],
        b"Ask first\n",
        support::Reply::Accepted,
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(std::fs::read(scratch.path().join("answer.txt")).unwrap(), b"First answer");
    let records = output_records(&ran.stdout);
    assert_eq!(records.last().unwrap()["type"], "session.ended");
    assert_eq!(records.last().unwrap()["exit"], 0);
    assert_eq!(records.iter().find(|record| record["type"] == "run.completed").unwrap()["status"], "accepted");
}

#[test]
fn smith_exec_result_file_failure_exits_1_after_an_accepted_run() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    std::fs::create_dir(scratch.path().join("answer.txt")).unwrap();
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--json", "-o", "answer.txt", "-"],
        b"Ask first\n",
        support::Reply::Accepted,
    );
    assert_eq!(ran.code, 1, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(
        ran.stderr.ends_with(b"smith: run accepted, but its result could not be written to answer.txt: IsADirectory\n")
    );
    let records = output_records(&ran.stdout);
    assert_eq!(records.last().unwrap()["type"], "session.ended");
    assert_eq!(records.last().unwrap()["exit"], 1);
    assert_eq!(records.iter().find(|record| record["type"] == "run.completed").unwrap()["status"], "accepted");
    assert!(scratch.path().join("answer.txt").is_dir());
}

#[test]
fn smith_exec_result_file_failure_warns_before_a_parked_answers_last_line() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    std::fs::create_dir(scratch.path().join("answer.txt")).unwrap();
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--chat", "plan", "--json", "-o", "answer.txt", "-"],
        b"Ask first\n",
        support::Reply::Wait,
    );
    assert_eq!(ran.code, 5, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(ran.stderr.ends_with(b"smith: warning: its result could not be written to answer.txt: IsADirectory\nsmith: needs input: answer with `smith exec --chat plan`\n"));
    let records = output_records(&ran.stdout);
    assert_eq!(records.last().unwrap()["exit"], 5);
    assert_eq!(records.iter().find(|record| record["type"] == "run.completed").unwrap()["status"], "parked");
}

fn output_records(bytes: &[u8]) -> Vec<serde_json::Value> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("event record"))
        .collect()
}

#[test]
fn smith_exec_ephemeral_removes_its_private_files_after_acceptance() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--ephemeral", "-o", "answer.txt", "-"],
        b"Ask first\n",
        support::Reply::Accepted,
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(ran.stdout, b"First answer");
    assert_eq!(std::fs::read(scratch.path().join("answer.txt")).unwrap(), b"First answer");
    assert!(ephemeral_directories(&scratch).is_empty());
    assert!(!String::from_utf8_lossy(&ran.stderr).contains("smith: warning: ephemeral directory "));
}

#[test]
fn smith_exec_ephemeral_removes_its_private_files_after_a_failed_answer() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_provider(
        &scratch,
        &["exec", "--ephemeral", "--json", "-"],
        b"Ask first\n",
        support::Reply::Filtered,
    );
    assert_eq!(ran.code, 1, "{}", String::from_utf8_lossy(&ran.stderr));
    let records = output_records(&ran.stdout);
    assert_eq!(records.last().unwrap()["type"], "session.ended");
    assert_eq!(records.last().unwrap()["exit"], 1);
    assert_eq!(records.iter().find(|record| record["type"] == "run.completed").unwrap()["status"], "failed");
    assert!(ephemeral_directories(&scratch).is_empty());
}

#[test]
fn smith_exec_ephemeral_keeps_a_foreign_file_and_warns_before_the_last_line() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::exec_with_foreign_file(&scratch, &["exec", "--ephemeral", "-"], b"Ask first\n");
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    let entries = ephemeral_directories(&scratch);
    assert_eq!(entries.len(), 1);
    let directory = entries[0].clone();
    assert_eq!(std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777, 0o700);
    assert_eq!(std::fs::read(directory.join("foreign.txt")).unwrap(), b"kept foreign content");
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 1);
    let errors = std::str::from_utf8(&ran.stderr).unwrap();
    let mut last = errors.lines().rev();
    assert!(last.next().unwrap().starts_with("smith: accepted in "));
    assert_eq!(last.next().unwrap(), format!("smith: warning: ephemeral directory {}: NotEmpty", directory.display()));
}

#[test]
fn an_ephemeral_directory_is_removed_when_product_startup_refuses() {
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let ran = support::smith(&scratch, &["exec", "--ephemeral", "--trace", "absent/events.jsonl", "message"], b"");
    assert_eq!(ran.code, 1, "{}", String::from_utf8_lossy(&ran.stderr));
    assert!(ran.stdout.is_empty());
    assert!(ephemeral_directories(&scratch).is_empty());
}

/// What an ephemeral run left in the scratch runtime directory, where its
/// private directory is made; none is ever made under the state directory
/// (shell.md 4.2).
fn ephemeral_directories(scratch: &Scratch) -> Vec<std::path::PathBuf> {
    let state = std::fs::read_dir(scratch.path().join("state")).into_iter().flatten();
    assert!(
        !state
            .map(|entry| entry.expect("scratch state entry").file_name())
            .any(|name| name.as_encoded_bytes().starts_with(b".smith-exec-")),
        "no ephemeral directory under the state directory"
    );
    std::fs::read_dir(scratch.path().join("runtime"))
        .expect("the scratch runtime directory holds the private directory")
        .map(|entry| entry.expect("scratch runtime entry").path())
        .collect()
}

fn login_settings(scratch: &Scratch, registration: &str) {
    let directory = scratch.path().join("config/smith");
    std::fs::create_dir_all(&directory).expect("scratch user layer");
    std::fs::write(
        directory.join("config.toml"),
        format!(
            "credentials='{}'\n[accounts.test]\nsource='sign_in'\n{registration}",
            scratch.path().join("credentials").display()
        ),
    )
    .expect("scratch registration");
}

#[test]
fn smith_login_signs_in_against_the_fake_issuer_and_exits_0() {
    let scratch = Scratch::new();
    login_settings(
        &scratch,
        &format!(
            "client_id='client'\nauthorization_url='http://{host}:34444/authorize'\ntoken_url='http://{host}:34444/token'\nredirect_uri='http://127.0.0.1:{redirect}/callback'\nscope='read'\ntoken_format='form'\n",
            host = smith_real_world::loopback(),
            redirect = smith_real_world::redirect_port()
        ),
    );
    let ran = support::login::sign_in(&scratch);
    assert_eq!(ran.code, 0);
    assert!(String::from_utf8_lossy(&ran.stderr).contains("account test: signed in"));
    let file = scratch.path().join("credentials/tokens/test");
    let secret = skein_shell::read_private_file(
        &file,
        smith_local_shell::local_host::token_limits().record_bytes,
        skein_shell::effective_user(),
    )
    .expect("kept private record");
    let record = skein_oauth::decode_record(secret.expose(), &smith_local_shell::local_host::token_limits())
        .expect("native record");
    assert!(record.access_token.as_ref() == b"access-new", "access credential retained");
    assert!(record.refresh_token.as_deref() == Some(b"refresh-new".as_slice()), "refresh credential retained");
    assert!(!scratch.path().join("state/smith/tokens").exists(), "credentials kept apart from chat state");
}

#[test]
fn smith_login_of_an_unknown_account_exits_2() {
    let scratch = Scratch::new();
    let ran = support::run_product(&scratch, &["login", "missing"], b"", None);
    assert_eq!(ran.code, 2);
    assert!(String::from_utf8_lossy(&ran.stderr).contains("unknown account missing"));
    assert!(!scratch.path().join("state/smith/tokens").exists());
}

#[test]
fn smith_check_and_startup_refuse_missing_sign_in_registration_keys_by_name() {
    let scratch = Scratch::new();
    let directory = scratch.path().join("config/smith");
    std::fs::create_dir_all(&directory).expect("scratch user layer");
    let base = String::from_utf8(support::fake_provider_settings(&scratch))
        .expect("settings")
        .replace("source='env'\nvariable='SMITH_FIXTURE_TOKEN'", "source='sign_in'");
    std::fs::write(directory.join("config.toml"), base).expect("missing registration fixture");
    for arguments in [&["check"][..], &["exec", "hello"][..]] {
        let ran = support::run_product(&scratch, arguments, b"", None);
        assert_eq!(ran.code, 2);
        let line = String::from_utf8_lossy(&ran.stderr);
        assert!(line.contains("accounts.codex.client_id"));
        assert!(line.contains("config.toml"));
    }
    assert!(!scratch.path().join("state/smith/tokens").exists());
}

#[test]
fn slash_effort_reaches_the_next_request() {
    use smith_local_process_world::terminal::Action;
    let scratch = Scratch::new();
    let mut settings = support::fake_provider_settings(&scratch);
    settings.extend_from_slice(b"\n[models.\"codex/gpt-6-luna\"]\nefforts=['low','high']\n");
    let _file = support::user_layer(&scratch, &settings);
    let (ran, queries) = support::chat_traced(
        &scratch,
        vec![
            Action::Send(b"hello\n".as_slice().into()),
            Action::Wait(b"First answer".as_slice().into()),
            Action::Send(b"/effort high\nnext\n".as_slice().into()),
            Action::Eof,
        ],
    );
    assert_eq!(ran.code, 0, "{}", String::from_utf8_lossy(&ran.stderr));
    assert_eq!(queries.len(), 2);
    let trace = std::fs::read(scratch.path().join("effort.jsonl")).unwrap();
    let efforts: Vec<_> = trace
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap())
        .filter(|record| record["type"] == "response.started")
        .map(|record| record["model"]["effort"].clone())
        .collect();
    assert_eq!(efforts, vec![serde_json::json!("low"), serde_json::json!("high")]);
}

#[test]
fn an_older_local_chat_exits_one_with_operator_words_and_unchanged_store() {
    use std::{fmt::Write as _, os::unix::fs::MetadataExt};
    let scratch = Scratch::new();
    let _file = support::user_layer(&scratch, &support::fake_provider_settings(&scratch));
    let workspace = scratch.path().canonicalize().unwrap();
    let mut key = String::new();
    for part in skein_io::digest::digest(workspace.as_os_str().as_encoded_bytes()).0 {
        write!(&mut key, "{part:016x}").expect("fixture workspace key");
    }
    let chat = scratch.path().join("state/smith/chats").join(key).join("old");
    std::fs::create_dir_all(&chat).unwrap();
    let state = smith_local_domain::ChatState {
        identity: smith_domain::run::Identity::new([42; 16]),
        activation: 9,
        next_message: 1,
        read: None,
    };
    for name in ["state", "fresh"] {
        std::fs::write(chat.join(name), smith_local_protocol::save_state(state).bytes).unwrap();
    }
    std::fs::write(chat.join("0000000001.turn"), b"old local transcript").unwrap();
    let snapshot = || {
        let mut files: Vec<_> = std::fs::read_dir(&chat)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name(),
                    std::fs::symlink_metadata(entry.path()).unwrap().ino(),
                    std::fs::read(entry.path()).unwrap(),
                )
            })
            .collect();
        files.sort();
        files
    };
    let before = snapshot();
    let ran = support::smith(&scratch, &["exec", "hello", "--chat", "old"], b"");
    assert_eq!(ran.code, 1, "{}", String::from_utf8_lossy(&ran.stderr));
    let expected = format!("smith: {}", smith_local_domain::Older::Unrecorded.words().join("old"));
    assert_eq!(std::str::from_utf8(&ran.stderr).unwrap().lines().last(), Some(expected.as_str()));
    assert_eq!(before, snapshot());
}
