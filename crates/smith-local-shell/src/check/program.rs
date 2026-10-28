//! Spawned program invocation derivation (shell.md, sections 4.4 and 9).
use super::*;

#[test]
fn program_invocations_select_spawned_placement_and_the_consumed_producer() {
    // Agent program paths are bounded at 4096 bytes; /programs consumes nine.
    const PROGRAM_PATH_BYTES: usize = 4096;
    const PROGRAMS_SUFFIX_BYTES: usize = 9;
    const MEMORY: u64 = 2_281_701_376;
    let credentials = format!(
        "/{}",
        "c".repeat(
            PROGRAM_PATH_BYTES
                .checked_sub(PROGRAMS_SUFFIX_BYTES)
                .and_then(|bytes| bytes.checked_sub(1))
                .expect("programs path prefix")
        )
    );
    let trust = skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der");
    let settings = settings::merge(
        &command::Shared::default(),
        "/workspace".into(),
        &[settings::Source {
            layer: settings::Layer::User,
            path: "program-chat.toml".into(),
            text: format!(
                "credentials='{credentials}'\nmodel='anthropic/claude-opus-5-5'\n\
                 [accounts.anthropic]\nsource='program'\n[endpoints.anthropic]\nprogram='claude_code'\ntrust.system=false\ntrust.der=['{}']\n[programs]\nclaude_code='/bin/true'\n",
                trust.display()
            ),
        }],
    )
    .unwrap_or_else(|why| panic!("{}", why.line()));
    for (interactive, json) in [(false, false), (false, true), (true, false)] {
        let mut resolved =
            settings::resolve::resolve_startup(&settings).unwrap_or_else(|why| panic!("{}", why.line())).0;
        startup_agent(&mut resolved).expect("product placement");
        let invocation = command::Exec {
            shared: command::Shared::default(),
            prompt: command::Prompt::Stdin,
            output: None,
            ephemeral: false,
            deliver: false,
            json,
        };
        let (limits, _) = derive_invocation(&mut resolved, &invocation, skein_lib::Wall::EPOCH, interactive)
            .expect("program invocation checks");
        assert!(matches!(limits.local.agent, smith_local_domain::AgentLimits::Spawned(_)), "program placement");
        assert!(limits.inline.is_none(), "spawned composition has no inline effects");
        assert_eq!(resolved.agent.memory, MEMORY);
        assert_eq!(resolved.agent.service.limits.events.is_some(), interactive || json);
    }
}
