use super::{Layer, Origin, Source, layers::merge};
use crate::command::Shared;
use std::fmt::Write;
use std::path::PathBuf;

pub(super) fn source(layer: Layer, text: &str) -> Source {
    Source { layer, path: PathBuf::from("fixture.toml"), text: text.into() }
}

pub(super) fn settings(sources: &[Source]) -> Result<super::Settings, super::Refusal> {
    let mut settings = merge(&Shared::default(), PathBuf::from("/workspace"), sources)?;
    settings.paths.path = std::env::var_os("PATH");
    Ok(settings)
}

#[test]
fn layers_merge_tables_and_keep_the_winning_origin() {
    let shared =
        Shared { model: Some("flag/model".into()), overrides: vec!["effort='high'".into()], ..Shared::default() };
    let decoded = merge(
        &shared,
        "/workspace".into(),
        &[
            source(Layer::User, "model='user/model'\n[environment.set]\nFIRST='user'\n"),
            source(Layer::Repository, "model='repo/model'\n[environment.set]\nSECOND='repo'\n[build]\npreset='rust'\n"),
        ],
    )
    .expect("merged layers");
    assert_eq!(decoded.values["model"].value.as_str(), Some("flag/model"));
    assert_eq!(decoded.values["model"].origin, Origin::Flag { flag: "-m".into() });
    assert_eq!(decoded.values["effort"].origin, Origin::Override { index: 1 });
    assert_eq!(decoded.values["environment.set.FIRST"].value.as_str(), Some("user"));
    assert_eq!(decoded.values["build.preset"].value.as_str(), Some("rust"));
    assert_eq!(
        decoded.values["environment.set.SECOND"].origin,
        Origin::File { layer: Layer::Repository, path: "fixture.toml".into(), line: 3 }
    );
}

#[test]
fn an_unknown_key_names_its_file_line_and_path() {
    let refusal = settings(&[source(Layer::User, "model='test/model'\nmodle=1\n")]).expect_err("unknown key");
    assert_eq!(refusal.key, "modle");
    assert!(refusal.line().contains("fixture.toml:2"));
    assert_eq!(refusal.problem, super::Problem::UnknownKey);
}

#[test]
fn a_wrong_type_names_the_nested_key_and_its_line() {
    let refusal = settings(&[source(Layer::User, "[accounts.test]\nsource=false\n")]).expect_err("source type");
    assert_eq!(refusal.key, "accounts.test.source");
    assert!(refusal.line().contains("fixture.toml:2"));
}

#[test]
fn an_integer_past_its_record_width_is_refused_at_the_key() {
    let refusal = settings(&[source(Layer::User, "[budget]\nturns=4294967296\n")]).expect_err("count bound");
    assert_eq!(refusal.key, "budget.turns");
    assert!(refusal.line().contains("fixture.toml:2"));
    assert_eq!(refusal.problem, super::Problem::WrongValue);
}

#[test]
fn a_file_past_one_mib_is_refused_before_decoding() {
    let text = " ".repeat(1_048_577);
    let refusal = settings(&[source(Layer::User, &text)]).expect_err("file bound");
    assert!(refusal.line().contains("1 MiB"));
}

#[test]
fn repository_inherited_names_are_authority_and_are_refused() {
    let refusal = settings(&[source(Layer::Repository, "[environment]\ninherit=['TOKEN']\n")]).expect_err("authority");
    assert_eq!(refusal.key, "environment.inherit");
    assert!(refusal.line().contains("fixture.toml:2"));
}

#[test]
fn a_repository_cannot_raise_the_users_budget() {
    let refusal =
        settings(&[source(Layer::User, "[budget]\nturns=12\n"), source(Layer::Repository, "[budget]\nturns=13\n")])
            .expect_err("repository budget");
    assert_eq!(refusal.key, "budget.turns");
    assert_eq!(refusal.problem, super::Problem::RepositoryBudget);
}

#[test]
fn an_unknown_array_field_names_its_element_and_exact_line() {
    let refusal = settings(&[source(Layer::User, "[[workspace.directories]]\nname='main'\npatth='.'\n")])
        .expect_err("directory key");
    assert_eq!(refusal.key, "workspace.directories[0].patth");
    assert!(refusal.line().contains("fixture.toml:3"));
}

#[test]
fn the_built_in_layer_matches_its_golden() {
    let settings = settings(&[]).expect("built-in layer");
    let mut bytes = String::new();
    for (key, sourced) in settings.values {
        assert_eq!(sourced.origin, Origin::BuiltIn);
        writeln!(bytes, "{key}={}", serde_json::to_string(&sourced.value).expect("value JSON")).expect("golden line");
    }
    assert_eq!(bytes, include_str!("built-in.golden"));
}

#[test]
fn haiku_takes_its_long_prompt_rates_only_past_a_100000_token_window() {
    let defaults = settings(&[]).expect("preset");
    let declared = settings(&[source(Layer::User, "[models.'anthropic/claude-haiku-5-5']\nwindow=100000\n")])
        .expect("declared window");
    let key = "models.anthropic/claude-haiku-5-5.input_price";
    assert_eq!(defaults.values["models.anthropic/claude-haiku-5-5.window"].value.as_integer(), Some(200_000));
    assert_eq!(defaults.values[key].value.as_float(), Some(0.5));
    assert_eq!(declared.values[key].value.as_float(), Some(0.1));
    let past = settings(&[source(Layer::User, "[models.'anthropic/claude-haiku-5-5']\nwindow=100001\n")])
        .expect("window past the tier");
    assert_eq!(past.values[key].value.as_float(), Some(0.5));
}

#[test]
fn rates_a_person_sets_stay_as_set_when_the_window_changes() {
    let decoded = settings(&[source(Layer::User,
        "[models.'anthropic/claude-haiku-5-5']\nwindow=100001\ninput_price=0.2\ncached_price=0.03\ncache_write_price=0.4\noutput_price=0.6\n"
    )]).expect("person's prices");
    for (field, expected) in
        [("input_price", 0.2), ("cached_price", 0.03), ("cache_write_price", 0.4), ("output_price", 0.6)]
    {
        let value = &decoded.values[&format!("models.anthropic/claude-haiku-5-5.{field}")];
        assert_eq!(value.value.as_float(), Some(expected));
        assert!(matches!(value.origin, Origin::File { layer: Layer::User, .. }));
    }
}

#[test]
fn a_preset_rate_uses_the_declared_price_unit() {
    let decoded =
        settings(&[source(Layer::User, "[models.'codex/gpt-6-astra']\nprice_unit=1000\n")]).expect("price unit");
    assert_eq!(decoded.values["models.codex/gpt-6-astra.input_price"].value.as_float(), Some(0.01));
}

#[test]
fn codex_takes_long_prompt_rates_only_past_272000_tokens() {
    let decoded =
        settings(&[source(Layer::User, "[models.'codex/gpt-6-astra']\nwindow=272001\n")]).expect("long prompt window");
    assert_eq!(decoded.values["models.codex/gpt-6-astra.input_price"].value.as_float(), Some(20.0));
    assert_eq!(decoded.values["models.codex/gpt-6-astra.output_price"].value.as_float(), Some(75.0));
}

#[test]
fn gpt_six_one_sol_is_declared_with_its_list_prices() {
    for (window, input, cached, cache_write, output) in [(272_000, 2.0, 0.1, 2.5, 10.0), (272_001, 4.0, 0.2, 5.0, 15.0)]
    {
        let decoded = settings(&[source(Layer::User, &format!("[models.'codex/gpt-6.1-sol']\nwindow={window}\n"))])
            .expect("sol preset");
        let prefix = "models.\"codex/gpt-6.1-sol\"";
        for (field, expected) in [
            ("input_price", input),
            ("cached_price", cached),
            ("cache_write_price", cache_write),
            ("output_price", output),
        ] {
            assert_eq!(
                decoded.values[&format!("{prefix}.{field}")].value.as_float(),
                Some(expected),
                "window {window}, {field}"
            );
        }
        for (field, expected) in
            [("window", window), ("output", 32_000), ("reasoning_item", 65_536), ("head", 60), ("idle", 300)]
        {
            assert_eq!(
                decoded.values[&format!("{prefix}.{field}")].value.as_integer(),
                Some(expected),
                "window {window}, {field}"
            );
        }
        assert!(!decoded.values.contains_key(&format!("{prefix}.efforts")));
    }
}

pub(super) fn selection(text: &str) -> Result<super::resolve::Selection, super::Refusal> {
    let mut settings = settings(&[source(Layer::User, text)])?;
    let files = crate::credentials::tests::Files::new();
    settings.values.entry("credentials".into()).or_insert(super::Sourced {
        value: toml::Value::String(files.0.display().to_string()),
        origin: Origin::BuiltIn,
    });
    super::resolve::select(&settings)
}

#[test]
fn a_codex_only_first_run_needs_no_anthropic_credential() {
    let selected =
        selection("model='codex/gpt-6-astra'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n")
            .expect("one account's run");
    assert_eq!(selected.models, ["codex/gpt-6-astra", "codex/gpt-6-luna", "codex/gpt-6.1-sol"]);
    assert_eq!(selected.accounts.len(), 1);
    assert_eq!(selected.accounts[0].name, "codex");
    assert_eq!(selected.accounts[0].fields["source"].value.as_str(), Some("env"));
    assert!(!selected.settings.values.contains_key("accounts.anthropic.source"));
}

#[test]
fn sub_agents_on_adds_the_models_of_every_account_with_a_source() {
    let selected = selection("model='codex/gpt-6-astra'\nsub_agents=true\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[accounts.anthropic]\nsource='env'\n")
        .expect("accounts with sources");
    assert_eq!(
        selected.models,
        [
            "codex/gpt-6-astra",
            "anthropic/claude-haiku-5-5",
            "anthropic/claude-opus-5-5",
            "anthropic/claude-sonnet-5-5",
            "codex/gpt-6-luna",
            "codex/gpt-6.1-sol"
        ]
    );
    assert_eq!(
        selected.accounts.iter().map(|account| (account.name.as_str(), account.number)).collect::<Vec<_>>(),
        [("codex", 1_474_186_484), ("anthropic", 3_339_635_307)]
    );
}

#[test]
fn mains_account_without_a_source_is_refused_naming_the_key() {
    let refusal = selection("model='codex/gpt-6-astra'\n").expect_err("main account has no source");
    assert_eq!(refusal.key, "accounts.codex.source");
    assert_eq!(refusal.line(), "smith: built-in settings: accounts.codex.source: required value is absent");
}

#[test]
fn sub_agents_on_keeps_an_unset_accounts_models_out() {
    let selected = selection(
        "model='codex/gpt-6-astra'\nsub_agents=true\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n",
    )
    .expect("codex delegation");
    assert_eq!(selected.models, ["codex/gpt-6-astra", "codex/gpt-6-luna", "codex/gpt-6.1-sol"]);
    assert_eq!(selected.accounts.len(), 1);
}

#[test]
fn a_custom_endpoint_inherits_its_preset_and_keeps_supplied_values() {
    let selected = selection("model='custom/gpt-6-astra'\n[endpoints.custom]\npreset='codex'\naddress='example.test:443'\n[models.'custom/gpt-6-astra']\nwindow=100000\ninput_price=1.0\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n")
        .expect("custom preset");
    let values = selected.settings.values;
    assert_eq!(values["endpoints.custom.address"].value.as_str(), Some("example.test:443"));
    assert!(matches!(values["endpoints.custom.address"].origin, Origin::File { layer: Layer::User, .. }));
    assert_eq!(values["endpoints.custom.server_name"].value.as_str(), Some("chatgpt.com"));
    assert_eq!(values["endpoints.custom.server_name"].origin, Origin::BuiltIn);
    assert_eq!(values["models.custom/gpt-6-astra.window"].value.as_integer(), Some(100_000));
    assert_eq!(values["models.custom/gpt-6-astra.output"].value.as_integer(), Some(32_000));
    assert_eq!(values["models.custom/gpt-6-astra.input_price"].value.as_float(), Some(1.0));
    assert_eq!(values["models.custom/gpt-6-astra.output_price"].value.as_float(), Some(50.0));
}

#[test]
fn dotted_custom_names_keep_their_account_and_model_bindings() {
    let selected = selection("model='custom.endpoint/gpt-6-luna'\n[endpoints.'custom.endpoint']\npreset='codex'\naccount='named-account'\n[accounts.'named-account']\nsource='env'\nvariable='TEST_CREDENTIAL'\n")
        .expect("dotted names");
    assert_eq!(selected.models, ["custom.endpoint/gpt-6-luna"]);
    assert_eq!(selected.accounts[0].name, "named-account");
    assert_eq!(
        selected.settings.values["models.\"custom.endpoint/gpt-6-luna\".window"].value.as_integer(),
        Some(272_000)
    );
}

#[test]
fn an_unknown_endpoint_preset_is_refused_at_its_source() {
    let refusal =
        selection("model='custom/model'\n[endpoints.custom]\npreset='unknown'\n").expect_err("unknown preset");
    assert_eq!(refusal.key, "endpoints.custom.preset");
    assert_eq!(refusal.origin, Origin::File { layer: Layer::User, path: "fixture.toml".into(), line: 3 });
}

#[test]
fn a_declared_model_with_a_source_is_available_to_sub_agents() {
    let selected = selection("model='codex/gpt-6-astra'\nsub_agents=true\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[models.'codex/declared']\nwindow=100000\noutput=1000\ninput_price=1.0\ncached_price=0.1\noutput_price=2.0\n")
        .expect("declared child model");
    assert_eq!(selected.models, ["codex/gpt-6-astra", "codex/declared", "codex/gpt-6-luna", "codex/gpt-6.1-sol"]);
    assert_eq!(selected.accounts.len(), 1);
}

#[test]
fn choosing_another_preset_replaces_only_the_endpoints_default_values() {
    let selected = selection("model='codex/claude-haiku-5-5'\n[endpoints.codex]\npreset='anthropic'\naddress='proxy.test:443'\n[accounts.anthropic]\nsource='env'\n")
        .expect("preset over defaults");
    assert_eq!(selected.accounts[0].name, "anthropic");
    assert_eq!(selected.settings.values["endpoints.codex.server_name"].value.as_str(), Some("api.anthropic.com"));
    assert_eq!(selected.settings.values["endpoints.codex.address"].value.as_str(), Some("proxy.test:443"));
    assert_eq!(selected.settings.values["models.codex/claude-haiku-5-5.window"].value.as_integer(), Some(200_000));
}

fn composed(extra: &str) -> Result<super::resolve::Resolved, super::Refusal> {
    let certificate =
        skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
    let text = format!(
        "model='codex/gpt-6-luna'\n{extra}\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\ntrust.system=false\ntrust.der=['{certificate}']\n[endpoints.anthropic]\naddress='127.0.0.1:443'\ntrust.system=false\ntrust.der=['{certificate}']\n"
    );
    let decoded = settings(&[source(Layer::User, &text)])?;
    super::resolve::resolve(
        &decoded,
        &crate::command::Command::Check { shared: Shared::default(), json: false, execution: None },
    )
}

#[test]
fn composition_derives_only_the_main_model_with_sub_agents_off() {
    let resolved = composed("[models.'anthropic/claude-opus-5-5']\nhead=0\nreasoning_item=0\n")
        .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.models.len(), 3);
    assert_eq!(resolved.local.models[0].model.as_ref(), b"gpt-6-luna");
    assert_eq!(resolved.accounts.len(), 1);
    assert_eq!(resolved.agent.declarations.endpoints.len(), 1);
    assert_eq!(resolved.agent.declarations.endpoints[0].models.len(), 3);
    assert_eq!(resolved.outside_models["anthropic/claude-opus-5-5"], "sub_agents is off");
}

#[test]
fn composition_adds_every_model_of_accounts_with_a_source() {
    let resolved = composed("sub_agents=true\n[accounts.anthropic]\nsource='env'\n")
        .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.models.len(), 6);
    assert_eq!(resolved.local.models[0].model.as_ref(), b"gpt-6-luna");
    assert_eq!(
        resolved.accounts.iter().map(|account| account.name.as_str()).collect::<Vec<_>>(),
        ["codex", "anthropic"]
    );
    assert!(resolved.outside_models.is_empty());
    assert_eq!(resolved.agent.declarations.endpoints.len(), 2);
    assert_eq!(resolved.local.accounts.as_ref(), [1_474_186_484, 3_339_635_307]);
}

#[test]
fn composition_reports_an_account_without_a_source_outside_the_run() {
    let resolved = composed("sub_agents=true\n").unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.models.len(), 3);
    assert_eq!(resolved.outside_models.len(), 3);
    assert_eq!(resolved.outside_models["anthropic/claude-opus-5-5"], "account has no source");
}

#[test]
fn an_outside_model_is_still_checked_for_a_positive_rate() {
    let refusal = composed("[models.'anthropic/claude-opus-5-5']\ninput_price=0\n").err().expect("zero rate");
    assert_eq!(refusal.key, "models.anthropic/claude-opus-5-5.input_price");
    assert!(refusal.line().contains("model rate must be positive"));
}

#[test]
fn an_outside_models_output_window_refusal_keeps_both_values_and_origins() {
    let refusal = composed("[models.'anthropic/claude-opus-5-5']\noutput=200000\n").err().expect("output in window");
    assert!(refusal.line().contains("output-in-window"));
    assert!(
        refusal.line().contains("models.anthropic/claude-opus-5-5.output = 200000 tokens (from user fixture.toml:3)")
    );
    assert!(refusal.line().contains("models.anthropic/claude-opus-5-5.window = 200000 tokens (from built-in)"));
}

#[test]
fn a_read_window_past_tool_payload_names_both_declared_sides() {
    let refusal = composed("[deployment]\nread_window=40961\n").err().expect("read in payload");
    assert!(
        refusal.line().contains("read-in-payload: deployment.read_window = 40961 bytes (from user fixture.toml:3)")
    );
    assert!(refusal.line().contains("deployment.tool_payload = 40960 bytes (from built-in)"));
}

#[test]
fn a_tool_payload_past_file_bytes_names_both_declared_sides() {
    let refusal = composed("[deployment]\nfile_bytes=40959\n").err().expect("payload in file");
    assert!(refusal.line().contains("payload-in-file: deployment.tool_payload = 40960 bytes (from built-in)"));
    assert!(refusal.line().contains("deployment.file_bytes = 40959 bytes (from user fixture.toml:3)"));
}

#[test]
fn a_budget_ceiling_override_retains_its_source() {
    let refusal = composed("[budget]\nturns=5\n[deployment]\nmax_turns=4\n").err().expect("turn ceiling");
    assert_eq!(
        refusal.line(),
        "smith: budget-ceiling: budget.turns = 5 items (from user fixture.toml:3) exceeds deployment.max_turns = 4 items (from user fixture.toml:5)"
    );
}

#[test]
fn completion_affordability_names_spend_and_mains_cost() {
    let refusal = composed("[budget]\nspend=0.001\n").err().expect("completion cost");
    assert!(refusal.line().contains("completion-affordability: budget.spend = $0.001000 (from user fixture.toml:3)"));
    assert!(refusal.line().contains("main completion = $0.050000"));
}

#[test]
fn reserve_check_time_refusal_keeps_the_two_durations() {
    let refusal = composed(
        "[workspace.contract]\nform='change'\nfields=[{name='title',max=128}]\n[budget.reserve]\nseconds=299\n",
    )
    .err()
    .expect("reserve check time");
    assert!(refusal.line().contains("reserve-over-checks"));
    assert!(refusal.line().contains("budget.reserve.seconds = 299000000000 ns (from user fixture.toml:6)"));
    assert!(refusal.line().contains("deployment.check_timeout = 300000000000 ns (from built-in)"));
}

#[test]
fn waiting_past_the_profile_ceiling_is_refused_at_waiting_seconds() {
    let refusal = composed("waiting_seconds=86401\n").err().expect("waiting ceiling");
    assert_eq!(refusal.key, "waiting_seconds");
    assert!(refusal.line().ends_with("standard profile's ceiling of 86400 seconds"));
}

#[test]
fn a_change_and_mid_run_delivery_keep_their_fields_and_title() {
    let resolved = composed("[workspace.contract]\nform='change'\nfields=[{name='title',max=128}]\n[workspace.delivery]\nfields=[{name='title',max=64}]\ntitle='title'\n").unwrap_or_else(|why| panic!("{}", why.line()));
    assert!(matches!(resolved.local.contract, smith_local_domain::Contract::Change(_)));
    assert_eq!(resolved.local.deliver.expect("mid-run delivery").fields.first().expect("title field").max, 64);
    assert_eq!(resolved.local.title_field.as_ref(), b"title");
}

#[test]
fn an_outside_declared_model_needs_only_its_value_checks() {
    let resolved = composed(
        "[models.'outside/model']\nwindow=1024\noutput=16\ninput_price=1.0\ncached_price=0.1\noutput_price=2.0\n",
    )
    .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.models.len(), 3);
    assert_eq!(resolved.outside_models["outside/model"], "sub_agents is off");
}

#[test]
fn cancellation_grace_refusal_names_the_profile_durations() {
    let refusal = composed("[deployment]\ncancel_grace=9\n").err().expect("cancel over close");
    assert!(refusal.line().contains("cancel-over-close"));
    assert!(refusal.line().contains("deployment.cancel_grace = 9000000000 ns (from user fixture.toml:3)"));
    assert!(refusal.line().contains("io.twice_close = 10000000000 ns (from built-in)"));
}

#[test]
fn trust_paths_are_resolved_from_the_settings_workspace() {
    let decoded = merge(&Shared::default(), skein_scratch::paths::package(), &[
        source(Layer::User,"model='codex/gpt-6-luna'\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\ntrust.system=false\ntrust.der=['../../tests/protocol-llm/fixtures/root.der']\n")
    ]).expect("relative root");
    let resolved = super::resolve::resolve(
        &decoded,
        &crate::command::Command::Check { shared: Shared::default(), json: false, execution: None },
    )
    .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.models.len(), 3);
}

#[test]
fn inherit_takes_named_variables_never_a_pattern() {
    let named = settings(&[source(Layer::User, "[environment]\ninherit=['PATH']\n")]).expect("named inheritance");
    let values = super::environment::values(&named).expect("one inherited name");
    assert_eq!(values.get("PATH"), std::env::var("PATH").ok().as_ref());
    assert!(!values.contains_key("HOME"));
    let pattern = settings(&[source(Layer::User, "[environment]\ninherit=['CARGO_*']\n")]).expect("decoded names");
    let refusal = super::environment::values(&pattern).expect_err("patterns refused");
    assert_eq!(refusal.key, "environment.inherit");
    assert!(refusal.line().contains("fixture.toml:2"));
}

#[test]
fn the_rust_preset_adds_its_homes_and_caches() {
    let decoded = settings(&[source(Layer::User,
        "[build]\npreset='rust'\ncaches=['/cache/extra']\n[environment]\ninherit=[]\n[environment.set]\nCARGO_HOME='/cache/cargo'\nRUSTUP_HOME='/cache/rustup'\n")]).expect("rust preset");
    let values = super::environment::values(&decoded).expect("rust names");
    assert_eq!(values["CARGO_HOME"], "/cache/cargo");
    assert_eq!(values["RUSTUP_HOME"], "/cache/rustup");
    assert_eq!(crate::presets::build_homes("rust"), Some(["CARGO_HOME", "RUSTUP_HOME"].as_slice()));
    assert_eq!(
        super::environment::caches(&decoded, &values),
        [PathBuf::from("/cache/cargo/git"), PathBuf::from("/cache/cargo/registry"), PathBuf::from("/cache/extra")]
    );
}

#[test]
fn a_credential_like_name_is_warned() {
    let decoded = settings(&[source(
        Layer::User,
        "[environment]\ninherit=[]\n[environment.set]\nSERVICE_API_KEY='value-never-shown'\n",
    )])
    .expect("named value");
    let values = super::environment::values(&decoded).expect("command environment");
    let warnings = crate::check::warnings::collect(&decoded, &values, &[]).expect("warnings");
    assert!(warnings.iter().any(|warning| matches!(warning,
        crate::check::Warning::CredentialLikeName { name, origin: Origin::File { line: 4, .. } }
        if name == "SERVICE_API_KEY")));
    assert!(!serde_json::to_string(&warnings).expect("name-only JSON").contains("value-never-shown"));
}

#[global_allocator]
static MEMORY: skein_heap::Counting = skein_heap::Counting;

fn inline_default() -> (super::resolve::Resolved, smith_local_service::Limits) {
    let resolved = composed("").unwrap_or_else(|why| panic!("{}", why.line()));
    let host =
        smith_local_service::profile::host_limits(&resolved.agent.profile, &resolved.agent.service.limits).unwrap();
    let mut limits = crate::startup::service_limits(resolved.limits.clone(), host, &resolved.agent.service).unwrap();
    limits.local.agent = smith_local_domain::AgentLimits::inline(
        limits.local.smith,
        limits.local.unsaved,
        resolved.local.budget.time,
        resolved.agent.profile.policy.cancel_grace,
    )
    .unwrap();
    limits.inline = Some(smith_local_service::InlineLimits::new(&resolved.agent.service).unwrap());
    (resolved, limits)
}

#[test]
fn the_default_inline_product_parts_fit_two_gibibytes() {
    let (resolved, limits) = inline_default();
    let rows =
        crate::Local::memory_parts(&limits, &resolved.agent.service, resolved.agent.service.limits.routes).unwrap();
    let total = crate::Local::required_memory(&limits, resolved.agent.service.limits.routes).unwrap();
    assert_eq!(rows.iter().map(|(_, bytes)| bytes).sum::<u64>(), total);
    assert!(total <= 2_281_701_376, "default product {total}");
    for (name, bytes) in &rows {
        println!("PRODUCT_PART {name}={bytes}");
    }
    measure_domain(resolved.local, &limits.local, &rows);
    measure_effects(resolved.agent.service, &rows);
    measure_common(&limits, &rows);
    measure_product();
    println!("PRODUCT_TOTAL {total}/2281701376");
}

fn measure_domain(config: smith_local_domain::Config, limits: &smith_local_domain::Limits, rows: &[(&str, u64)]) {
    let span = skein_heap::Span::start();
    let domain = smith_local_domain::Domain::new(config, limits, 1).unwrap();
    let measured = span.end();
    let bound = rows.iter().find(|(name, _)| *name == "local_domain").unwrap().1;
    assert!(u64::try_from(measured.peak).unwrap() <= bound);
    println!("PRODUCT_MEASURE local_domain={} bound={bound}", measured.peak);
    drop(domain);
}

fn measure_effects(config: smith_agent_service::Config, rows: &[(&str, u64)]) {
    let span = skein_heap::Span::start();
    let effects = smith_agent_service::Effects::new(config, 1).unwrap();
    let measured = span.end();
    let bound: u64 = rows
        .iter()
        .filter(|(name, _)| ["llm", "machine", "files", "events", "effects_routing"].contains(name))
        .map(|(_, bytes)| bytes)
        .sum();
    assert!(u64::try_from(measured.peak).unwrap() <= bound);
    println!("PRODUCT_MEASURE effects={} bound={bound}", measured.peak);
    drop(effects);
}

fn measure_common(limits: &smith_local_service::Limits, rows: &[(&str, u64)]) {
    let span = skein_heap::Span::start();
    let io = skein_io::Io::new(&limits.process.io);
    let terminal = smith_local_protocol::Terminal::new(smith_local_protocol::TerminalLimits {
        line_bytes: limits.local.line_bytes,
        show_bytes: limits.local.show_bytes,
    })
    .unwrap();
    let measured = span.end();
    let bound: u64 = rows.iter().filter(|(name, _)| ["io", "terminal"].contains(name)).map(|(_, bytes)| bytes).sum();
    assert!(u64::try_from(measured.peak).unwrap() <= bound);
    println!("PRODUCT_MEASURE io_and_terminal={} bound={bound}", measured.peak);
    drop((io, terminal));
}

fn measure_product() {
    use skein_io::kernel::Fd;
    let (resolved, limits) = inline_default();
    let count = resolved.local.workspace.as_ref().unwrap().directories.len();
    let roots = |first: u32| {
        (0..count)
            .map(|index| Fd::new(i32::try_from(first.checked_add(u32::try_from(index).unwrap()).unwrap()).unwrap()))
            .collect::<Box<[_]>>()
    };
    let config = smith_local_service::Config {
        local: resolved.local,
        limits,
        endpoints: resolved.agent.service.channel_endpoints.clone(),
        paths: Box::new([]),
        launch: smith_local_service::Launch {
            program: Box::new([]),
            arguments: Box::new([]),
            environment: Box::new([]),
            root: Fd::new(510),
            directory: b".".as_slice().into(),
        },
        agent: smith_local_service::AgentConfig::Spawned,
    };
    let available = resolved.agent.memory;
    let resources = crate::Resources {
        state_root: Fd::new(511),
        input: Fd::new(512),
        output: Fd::new(513),
        signals: Fd::new(514),
        delivery_roots: roots(520),
        effect_roots: roots(530),
        delivery_environment: Box::new([]),
        seed: 1,
        oauth_entropy: [0; 32],
    };
    let span = skein_heap::Span::start();
    let local = crate::Local::within_memory(
        config,
        Some(resolved.agent.service),
        resources,
        Box::new(Vec::<u8>::new()),
        None,
        None,
    )
    .unwrap();
    let measured = span.end();
    assert!(u64::try_from(measured.peak).unwrap() <= available);
    println!("PRODUCT_MEASURE whole_constructor={} memory={available}", measured.peak);
    drop(local);
}

#[test]
fn the_configuration_holds_each_model_of_mains_endpoint_and_none_of_another() {
    let resolved = composed("effort='high'\n").unwrap_or_else(|why| panic!("{}", why.line()));
    let declared = &resolved.agent.declarations.endpoints;
    assert_eq!(declared.len(), 1);
    assert_eq!(declared[0].models.len(), 3);
    assert!(declared[0].models.iter().any(|model| model.name.as_ref() == b"gpt-6-astra"));
    assert!(declared[0].models.iter().any(|model| model.name.as_ref() == b"gpt-6-luna"));
    let charter = smith_local_domain::charter(&resolved.local);
    assert_eq!(charter.llm.model.as_ref(), b"gpt-6-luna");
    assert_eq!(charter.llm.effort.as_deref(), Some(b"high".as_slice()));
    assert!(charter.models.is_empty());
    assert_eq!(resolved.accounts.len(), 1);
}

fn effort_settings(extra: &str) -> Result<super::resolve::Resolved, super::Refusal> {
    effort_settings_shared(extra, &Shared::default())
}

fn effort_settings_shared(extra: &str, shared: &Shared) -> Result<super::resolve::Resolved, super::Refusal> {
    let decoded = effort_settings_input(extra, shared)?;
    super::resolve::resolve(
        &decoded,
        &crate::command::Command::Check { shared: Shared::default(), json: false, execution: None },
    )
}

fn effort_settings_input(extra: &str, shared: &Shared) -> Result<super::Settings, super::Refusal> {
    let certificate =
        skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
    let base = format!(
        "model='test/model'\n[accounts.test]\nsource='env'\nvariable='SYNTHETIC'\n[endpoints.test]\ndialect='codex'\nconnect=10\nhandshake=10\naccount='test'\naddress='127.0.0.1:443'\nserver_name='example.test'\ntrust.system=false\ntrust.der=['{certificate}']\n[models.\"test/model\"]\nwindow=262144\noutput=32768\ninput_price=1.0\ncached_price=1.0\noutput_price=1.0\n"
    );
    let extra = extra.replace("codex/gpt-6-luna", "test/model");
    merge(shared, PathBuf::from("/workspace"), &[source(Layer::User, &base), source(Layer::User, &extra)])
}

#[test]
fn the_settings_effort_is_refused_with_its_models_declared_names() {
    let why = effort_settings("effort='mystery'\n[models.\"codex/gpt-6-luna\"]\nefforts=['low','high']\n")
        .err()
        .expect("undeclared effort");
    assert_eq!(why.key, "effort");
    assert!(why.line().contains("Effort mystery is not declared; accepted: low, high"));
}

#[test]
fn a_model_with_no_effort_list_passes_any_bounded_name_to_the_provider() {
    let resolved = effort_settings("effort='future'\n").unwrap();
    assert_eq!(resolved.local.models[0].effort.as_deref(), Some(b"future".as_slice()));
    assert!(resolved.agent.service.domain.models.iter().all(|model| model.efforts.is_empty()));
}

#[test]
fn declared_efforts_reach_the_local_and_agent_startup_owners() {
    let resolved = effort_settings("effort='high'\n[models.\"codex/gpt-6-luna\"]\nefforts=['low','high']\n").unwrap();
    assert_eq!(
        resolved.local.efforts[0].as_ref(),
        &[Box::<[u8]>::from(b"low".as_slice()), Box::<[u8]>::from(b"high".as_slice())]
    );
    assert!(
        resolved
            .agent
            .service
            .domain
            .models
            .iter()
            .any(|model| model.model.as_ref() == b"model" && model.efforts.len() == 2)
    );
}

#[test]
fn model_effort_names_are_bounded_at_the_settings_entrance() {
    let why = effort_settings("[models.\"codex/gpt-6-luna\"]\nefforts=['abcdefghijklmnopqrstuvwxyz0123456']\n")
        .err()
        .unwrap();
    assert!(why.key.ends_with("efforts"));
}

#[test]
fn the_effort_flag_is_checked_against_the_models_declared_names() {
    let why = effort_settings_shared(
        "[models.\"test/model\"]\nefforts=['low']\n",
        &Shared { effort: Some("high".into()), ..Shared::default() },
    )
    .err()
    .expect("undeclared flag effort");
    assert_eq!(why.key, "effort");
    assert_eq!(why.origin, Origin::Flag { flag: "--effort".into() });
    assert!(why.line().contains("Effort high is not declared; accepted: low"));
}

#[test]
fn parallel_tools_reaches_the_agents_policy_and_batch_limits() {
    let resolved = composed("[deployment]\nparallel_tools=2\n").unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.agent.profile.policy.parallel_tools, 2);
    assert_eq!(resolved.agent.service.limits.domain.session.parallel_tools, 2);
    assert_eq!(resolved.agent.service.limits.domain.session.tools.calls, 2);
}

#[test]
fn jobs_job_poll_and_job_wait_reach_the_agents_tool_limits() {
    let resolved =
        composed("[deployment]\njobs=2\njob_poll=2\njob_wait=7\n").unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.agent.profile.declared.jobs, 2);
    assert_eq!(resolved.agent.profile.policy.job_poll, skein_lib::Duration::from_secs(2));
    assert_eq!(resolved.agent.profile.policy.job_wait, skein_lib::Duration::from_secs(7));
    assert_eq!(resolved.agent.service.limits.domain.session.tools.jobs, 2);
    assert_eq!(resolved.agent.service.limits.domain.session.tools.job_poll, skein_lib::Duration::from_secs(2));
    assert_eq!(resolved.agent.service.limits.domain.session.tools.job_wait, skein_lib::Duration::from_secs(7));
}

fn refresh_profile(text: &str) -> Result<(smith_agent_service::profile::Profile, smith_agent_service::Limits), String> {
    use skein_lib::Duration;
    use smith_agent_service::profile::{Configuration, Endpoint, Model, OversizedReasoning};
    let decoded = settings(&[source(Layer::User, text)]).map_err(|why| why.line())?;
    let document = super::resolve::document::profile(&decoded).map_err(|why| why.line())?;
    let profile = smith_agent_shell::config::profile_from_value(document)
        .map_err(smith_agent_shell::config::ConfigurationRefusal::detail)?;
    let configuration = Configuration {
        role_bound: None,
        environment_bytes: 0,
        endpoints: Box::new([Endpoint {
            number: 0,
            provider: skein_llm::Provider::OpenAiCodex,
            option_bytes: 0,
            connect: Duration::from_secs(2),
            handshake: Duration::from_secs(3),
            models: Box::new([Model {
                efforts: Box::new([]),
                name: b"test".as_slice().into(),
                window: 1_048_576,
                output: 32_768,
                reasoning_item: 65_536,
                oversized_reasoning: OversizedReasoning::Fail,
                head: Duration::from_secs(5),
                idle: Duration::from_secs(11),
            }]),
        }]),
    };
    let limits = smith_agent_service::profile::derive(&profile, &configuration)
        .map_err(|why| String::from_utf8_lossy(&smith_agent_service::profile::words(&why)).into_owned())?;
    Ok((profile, limits))
}

#[test]
fn refresh_lead_reaches_the_accounts_limits() {
    let (profile, agent) = refresh_profile("[deployment]\nrefresh_lead=7\n").unwrap_or_else(|why| panic!("{why}"));
    assert_eq!(profile.policy.refresh_lead, skein_lib::Duration::from_secs(7));
    let limits = crate::credentials::limits(agent.io, 1, profile.policy.refresh_lead);
    assert_eq!(limits.refresh_lead, skein_lib::Duration::from_secs(7));
}

#[test]
fn an_unset_refresh_lead_keeps_sixty_seconds() {
    let (profile, _) = refresh_profile("").unwrap_or_else(|why| panic!("{why}"));
    assert_eq!(profile.policy.refresh_lead, skein_lib::Duration::from_secs(60));
}

#[test]
fn a_zero_refresh_lead_names_the_setting() {
    let refusal = refresh_profile("[deployment]\nrefresh_lead=0\n").expect_err("zero duration refused");
    assert!(refusal.contains("policy.refresh_lead"));
}

#[test]
fn the_default_task_role_reaches_the_resolved_run() {
    let resolved = composed("").unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.instructions.as_ref(), crate::role::TASK_ROLE.as_bytes());
    assert_eq!(
        resolved.agent.declarations.role_bound,
        Some(u32::try_from(crate::role::TASK_ROLE.len()).expect("the task role fits four kibibytes"))
    );
}

#[test]
fn explicit_instructions_replace_the_shells_default_task_role() {
    let resolved = composed("instructions='Custom instructions'\n").unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(resolved.local.instructions.as_ref(), b"Custom instructions");
    assert_eq!(resolved.agent.declarations.role_bound, None);
}

#[test]
fn an_openai_endpoint_is_written_with_its_number() {
    // shell.md 6.2 gives OpenAI the 300-second idle bound and dialect 3.
    const IDLE_MILLIS: u64 = 300_000;
    #[derive(Debug, PartialEq)]
    struct WrittenEndpoint<'a> {
        provider: Option<&'a str>,
        dialect: Option<u64>,
        idle: Option<u64>,
    }
    let input = effort_settings_input("[endpoints.test]\ndialect='openai'\n", &Shared::default())
        .expect("public endpoint settings");
    let super::resolve::Selection { models, accounts: _, mut settings } =
        super::resolve::select(&input).expect("selected public endpoint");
    let models = super::resolve::models::resolve(&mut settings, &models).expect("public model defaults");
    let document = super::resolve::document::endpoints(&settings, &models).expect("agent endpoints");
    let endpoint = &document[0];
    assert_eq!(
        WrittenEndpoint {
            provider: endpoint["provider"].as_str(),
            dialect: endpoint["dialect"].as_u64(),
            idle: endpoint["models"][0]["idle"].as_u64()
        },
        WrittenEndpoint { provider: Some("openai"), dialect: Some(3), idle: Some(IDLE_MILLIS) }
    );
}

/// A fresh directory under the system's temporary directory, one per test process.
fn scratch(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("smith-dirs-{label}-{}", std::process::id()));
    drop(std::fs::remove_dir_all(&path));
    std::fs::create_dir_all(path.join("work")).expect("scratch working directory");
    path
}

fn dir(name: &str, path: PathBuf, writable: bool) -> crate::command::Dir {
    crate::command::Dir { name: name.into(), path, writable }
}

fn with_dirs(
    root: &std::path::Path,
    extra: &str,
    dirs: Vec<crate::command::Dir>,
) -> Result<super::resolve::Resolved, super::Refusal> {
    let certificate =
        skein_scratch::paths::workspace().join("tests/protocol-llm/fixtures/root.der").display().to_string();
    let text = format!(
        "model='codex/gpt-6-luna'\n{extra}\n[accounts.codex]\nsource='env'\nvariable='SMITH_FIXTURE_TOKEN'\n[endpoints.codex]\naddress='127.0.0.1:443'\ntrust.system=false\ntrust.der=['{certificate}']\n"
    );
    let shared = Shared { dirs, ..Shared::default() };
    let decoded = merge(&shared, root.join("work"), &[source(Layer::User, &text)])?;
    super::resolve::resolve(&decoded, &crate::command::Command::Check { shared, json: false, execution: None })
}

fn rows(resolved: &super::resolve::Resolved) -> Vec<(String, bool, bool)> {
    let workspace = resolved.local.workspace.as_ref().expect("a workspace");
    workspace
        .directories
        .iter()
        .map(|row| (String::from_utf8(row.name.to_vec()).expect("UTF-8 name"), row.writable, row.git))
        .collect()
}

#[test]
fn without_dir_the_workspace_is_one_writable_row_named_workspace() {
    let root = scratch("default");
    let resolved = with_dirs(&root, "", Vec::new()).unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(rows(&resolved), [("workspace".into(), true, false)]);
}

#[test]
fn a_dir_follows_the_working_directory_named_by_its_last_component() {
    let root = scratch("rows");
    std::fs::create_dir_all(root.join("plan")).unwrap();
    let resolved = with_dirs(&root, "", vec![dir("plan", root.join("work/../plan"), false)])
        .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(rows(&resolved), [("work".into(), true, false), ("plan".into(), false, false)]);
    let workspace = resolved.local.workspace.as_ref().unwrap();
    let canonical = root.canonicalize().unwrap();
    assert_eq!(&*workspace.directories[1].host, canonical.join("plan").as_os_str().as_encoded_bytes());
    let roots = workspace.directories.iter().map(|row| row.root).collect::<Vec<_>>();
    assert_eq!(roots, [skein_lib::Token::new(1), skein_lib::Token::new(2)]);
}

#[test]
fn three_dirs_start_beside_the_working_directory() {
    use skein_lib::{Env, Queue, ReplyTo, Time, Token, Wall};
    use smith_domain::run;

    let root = scratch("four");
    for name in ["plan", "kit", "log"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    let mut resolved = with_dirs(
        &root,
        "",
        vec![
            dir("plan", root.join("plan"), false),
            dir("kit", root.join("kit"), false),
            dir("log", root.join("log"), true),
        ],
    )
    .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(
        rows(&resolved),
        [
            ("work".into(), true, false),
            ("plan".into(), false, false),
            ("kit".into(), false, false),
            ("log".into(), true, false)
        ]
    );
    let native = &resolved.agent.service.limits.domain;
    assert_eq!(native.run.directories, 4);
    assert_eq!(native.session.tools.repos, 4);
    resolved.local.mode = smith_local_domain::Mode::Headless { prompt: b"Do the work".as_slice().into() };
    let charter = smith_local_domain::charter(&resolved.local);
    let mut domain = run::Domain::new(&native.run, 0);
    let mut out = Queue::with_capacity(run::MAX_OUT);
    run::step(
        &mut domain,
        &Env { now: Time::ZERO, wall: Wall::EPOCH, limits: native.run },
        run::Event::Start {
            messages: Box::default(),
            reply_to: ReplyTo::new(Token::new(1)),
            host_run: Token::new(1),
            identity: None,
            activation: 1,
            window: run::Window { turns: u32::MAX, bytes: u64::MAX, largest_turn: 1 },
            charter,
            workspace: resolved.local.workspace,
            transcript: None,
            resumed: false,
        },
        &mut out,
    );
    assert!(matches!(out.pop(), Some(run::Request::Admitted { .. })), "four-directory start admitted");
}

#[test]
fn a_dir_inside_a_git_working_tree_is_a_repository() {
    let root = scratch("git");
    std::fs::create_dir_all(root.join("kit/.git")).unwrap();
    std::fs::create_dir_all(root.join("kit/src")).unwrap();
    let resolved = with_dirs(&root, "", vec![dir("kit", root.join("kit/src"), true)])
        .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(rows(&resolved), [("work".into(), true, false), ("kit".into(), true, true)]);
}

#[test]
fn dir_flags_replace_a_workspace_directories_setting() {
    let root = scratch("replace");
    std::fs::create_dir_all(root.join("plan")).unwrap();
    let setting = "[[workspace.directories]]\nname='main'\npath='.'\n";
    let resolved = with_dirs(&root, setting, vec![dir("plan", root.join("plan"), true)])
        .unwrap_or_else(|why| panic!("{}", why.line()));
    assert_eq!(rows(&resolved), [("work".into(), true, false), ("plan".into(), true, false)]);
}

#[test]
fn a_dir_the_workspace_cannot_hold_is_refused_at_the_flag() {
    let root = scratch("refused");
    for name in ["a", "b", "c", "d"] {
        std::fs::create_dir_all(root.join(name)).unwrap();
    }
    std::fs::write(root.join("file"), b"text").unwrap();
    let refused = |dirs: Vec<crate::command::Dir>| {
        let refusal = with_dirs(&root, "", dirs).err().expect("a refusal");
        assert_eq!(refusal.origin, Origin::Flag { flag: "--dir".into() });
        assert_eq!(refusal.key, "workspace.directories");
        refusal.line()
    };
    let line = refused(vec![dir("a", root.join("a"), true), dir("a", root.join("b"), true)]);
    assert!(line.contains("`a` names two directories"), "{line}");
    let line = refused(vec![dir("work", root.join("a"), true)]);
    assert!(line.contains("`work` is the working directory's name"), "{line}");
    let line = refused(vec![dir("a", root.join("file"), true)]);
    assert!(line.contains("is not a directory"), "{line}");
    let line = refused(vec![dir("a", root.join("absent"), true)]);
    assert!(line.contains("is not a directory"), "{line}");
    let limits = &with_dirs(&root, "", Vec::new()).unwrap().agent.service.limits.domain;
    let bound = limits.run.directories.min(limits.session.tools.repos);
    assert_eq!(bound, 4);
    let names = ["a", "b", "c", "d"];
    let line = refused(names.iter().map(|name| dir(name, root.join(name), true)).collect());
    let count = bound + 1;
    let expected = format!("{count} directories with the working directory exceed the profile's workspace of {bound}");
    assert!(line.contains(&expected), "{line}");
    assert!(line.starts_with("smith: --dir: workspace.directories: "), "{line}");
}
