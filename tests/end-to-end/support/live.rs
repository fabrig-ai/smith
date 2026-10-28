//! Live account setup and command policy (testing.md, section 2.3).
//! Keeps only the guarded live root; smith owns all credential reads and writes.

use smith_real_world::Scratch;
use std::path::{Path, PathBuf};

pub const CHATGPT: &str =
    "no record in `$SMITH_LIVE/tokens/chatgpt`: run `XDG_CONFIG_HOME=$SMITH_LIVE/config smith login chatgpt`";
pub const CLAUDE: &str =
    "no record in `$SMITH_LIVE/programs/claude`: run `XDG_CONFIG_HOME=$SMITH_LIVE/config smith login claude`";
pub const KEY: &str = "`ANTHROPIC_API_KEY` is not set";

/// The suite's durable user layer; each story requests only its own accounts.
pub struct Environment {
    pub root: PathBuf,
    pub refresh_lead: Option<u64>,
}

impl Environment {
    #[must_use]
    pub fn load(accounts: &[&str]) -> Self {
        let root = root(std::env::var_os("SMITH_LIVE").as_deref()).unwrap_or_else(|message| panic!("{message}"));
        let root = smith_bench::guard::write_path(&root).expect("guarded $SMITH_LIVE");
        let environment = Self { root, refresh_lead: None };
        let _configuration = environment.configuration();
        for account in accounts {
            let key = *account == "anthropic-key"
                && std::env::var_os("ANTHROPIC_API_KEY").is_some_and(|value| !value.is_empty());
            setup(&environment.root, account, key).unwrap_or_else(|message| panic!("{message}"));
        }
        environment
    }

    /// Read generation through the product's private record reader, without exposing a token.
    #[must_use]
    pub fn generation(&self) -> u64 {
        let _configuration = self.configuration();
        smith_local_shell::credentials::record_generation(&self.root.join("tokens/chatgpt"))
            .expect("a readable ChatGPT record")
            .expect("ChatGPT record exists")
    }

    /// Recheck every live-layer destination before a child can use it.
    #[must_use]
    pub fn configuration(&self) -> PathBuf {
        let mut configuration = self.root.join("config");
        for relative in [
            "config",
            "config/smith",
            "config/smith/config.toml",
            "host-id",
            "tokens",
            "tokens/chatgpt",
            "programs/claude",
        ] {
            let path = smith_bench::guard::write_path(&self.root.join(relative))
                .unwrap_or_else(|reason| panic!("$SMITH_LIVE/{relative}: {reason}"));
            if relative == "config" {
                configuration = path;
            }
        }
        configuration
    }
}

fn root(value: Option<&std::ffi::OsStr>) -> Result<PathBuf, &'static str> {
    value.filter(|value| !value.is_empty()).map(PathBuf::from).ok_or("$SMITH_LIVE unset")
}

fn setup(root: &Path, account: &str, key: bool) -> Result<(), &'static str> {
    let (path, message) = match account {
        "chatgpt" => ("tokens/chatgpt", CHATGPT),
        "claude" => ("programs/claude", CLAUDE),
        "anthropic-key" => return if key { Ok(()) } else { Err(KEY) },
        _ => panic!("unknown live account"),
    };
    std::fs::symlink_metadata(root.join(path)).map(|_| ()).map_err(|_| message)
}

/// Public command settings shared by live stories and their offline controls.
#[must_use]
pub fn arguments(account: &str, directory: &Path, change: bool, push: bool) -> Vec<std::ffi::OsString> {
    let mut arguments = vec![
        "exec".into(),
        "--json".into(),
        "-C".into(),
        directory.join("repo").into(),
        "--chat".into(),
        "chat".into(),
        "-c".into(),
        format!("endpoints.{}.account=\"{account}\"", if account == "chatgpt" { "openai" } else { "anthropic" }).into(),
        "-m".into(),
        if account == "chatgpt" { "openai/gpt-6-luna".into() } else { "anthropic/claude-haiku-5-5".into() },
    ];
    if account == "chatgpt" {
        arguments.extend(["--effort".into(), "low".into()]);
    }
    if change {
        arguments.push("--deliver".into());
    }
    let overrides = serde_json::json!({
        "instructions":"Follow the requested outcome exactly. Use tools and call finish when done.",
        "budget.turns":512, "budget.spend":25, "budget.seconds":3600,
        "deployment.memory":2_281_701_376_u64, "sub_agents":false,
        "workspace.directories":[{"name":"repo","path":directory.join("repo"),"writable":true,"git":change}],
        "workspace.conventions.guide":".smith-test/guide", "workspace.conventions.checks":".smith-test/check",
        "workspace.contract.form":if change {"change"} else {"report"},
        "workspace.contract.checks_must_pass":change,
        "workspace.contract.fields":if change {serde_json::json!([{"name":"title","max":256},{"name":"body","max":4096}])} else {serde_json::json!([])},
        "workspace.contract.max":4096,
        "workspace.delivery.pushes":push,
        "trace.capture":"everything",
        "environment.inherit":[], "environment.set.PATH":std::env::var("PATH").expect("PATH"),
        "environment.set.SMITH_TEST_ENVIRONMENT":"configured",
        "environment.set.HOME":directory.join("home"),
        "environment.set.XDG_STATE_HOME":directory.join("state"),
    });
    for (name, value) in overrides.as_object().expect("overrides") {
        let value: toml::Value = serde_json::from_value(value.clone()).expect("public TOML value");
        arguments.extend(["-c".into(), format!("{name}={value}").into()]);
    }
    if account == "claude" {
        arguments.extend(["-c".into(), "endpoints.anthropic.program=\"claude_code\"".into()]);
    }
    arguments.push("-".into());
    arguments
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_live_root_names_the_variable() {
        assert_eq!(root(None).expect_err("unset root"), "$SMITH_LIVE unset");
        assert_eq!(root(Some(std::ffi::OsStr::new(""))).expect_err("empty root"), "$SMITH_LIVE unset");
    }

    #[test]
    fn missing_chatgpt_names_the_sign_in() {
        assert_eq!(
            setup(Path::new("/dev/null"), "chatgpt", false),
            Err(
                "no record in `$SMITH_LIVE/tokens/chatgpt`: run `XDG_CONFIG_HOME=$SMITH_LIVE/config smith login chatgpt`"
            )
        );
    }

    #[test]
    fn missing_claude_names_the_sign_in() {
        assert_eq!(
            setup(Path::new("/dev/null"), "claude", false),
            Err(
                "no record in `$SMITH_LIVE/programs/claude`: run `XDG_CONFIG_HOME=$SMITH_LIVE/config smith login claude`"
            )
        );
    }

    #[test]
    fn missing_key_names_the_variable() {
        assert_eq!(setup(Path::new("/dev/null"), "anthropic-key", false), Err("`ANTHROPIC_API_KEY` is not set"));
        assert_eq!(setup(Path::new("/dev/null"), "anthropic-key", true), Ok(()));
    }

    fn synthetic_login(root: &Path) -> PathBuf {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        let login = root.join("synthetic-login");
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(login.join("tokens"))
            .expect("private synthetic login");
        let record = skein_oauth::SavedToken {
            key: 0,
            generation: 1,
            access_token: b"unissued-access".as_slice().into(),
            refresh_token: None,
            expires_at: skein_lib::Wall::from_nanos(u64::MAX),
            issued_client_id: None,
            granted_scopes: Some(
                b"openid profile email offline_access resource.invoke chatgpt.tokens.use.direct".as_slice().into(),
            ),
            subject: None,
            id_token: None,
            earliest_refresh_at: None,
            metadata: None,
        };
        let bytes = skein_oauth::encode_record(&record, &smith_local_shell::local_host::token_limits())
            .expect("synthetic native record");
        let file = login.join("tokens/chatgpt");
        std::fs::write(&file, &bytes).expect("synthetic record file");
        std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600)).expect("private record");
        login
    }

    fn pinned_product_check(conversations: i64) -> smith_bench::process::ProcessResult {
        use skein_lib::Duration;
        let scratch = Scratch::new();
        let root = scratch.path();
        let guard = smith_bench::guard::Guard::new(&root.join("user"), &root.join("config"), &root.join("state"), &[])
            .expect("guard");
        let private = smith_bench::isolate::empty(&guard, &root.join("attempt")).expect("private paths");
        let project = skein_scratch::paths::package().join("benchmarks");
        let pin_file = project.join("agents/smith/everything.pin.toml");
        let pin = smith_bench::read_configuration(&pin_file).expect("pin");
        let mut task = smith_bench::read_task(&project.join("tasks/probes/first-request/task.toml")).expect("task");
        task.smith.insert("deployment.conversations".into(), toml::Value::Integer(conversations));
        let model = smith_bench::ModelChoice { model: Some("gpt-6-luna".into()), effort: Some("low".into()) };
        let arm = smith_bench::arm::ArmRef::Installed { program: "/unused".into() };
        let environment = smith_bench::isolate::environment(
            &smith_bench::isolate::Machine { path: "/usr/bin:/bin".into(), rustup_home: None },
            &private,
            &std::collections::BTreeMap::new(),
        )
        .expect("environment");
        let clock = skein_shell::Clock::new();
        let login = synthetic_login(root);
        let context = smith_bench::agent::Context {
            guard: &guard,
            private: &private,
            pin: &pin,
            pin_file: &pin_file,
            task: &task,
            variant: None,
            model: &model,
            arm: &arm,
            installed_version: "unused",
            home: root,
            wall: clock.now().wall,
            lifetime: Duration::from_secs(20),
            environment: &environment,
        };
        let layer: toml::Value =
            toml::from_str(include_str!("../../../benchmarks/agents/smith/everything.toml")).expect("product layer");
        let base = serde_json::to_value(layer).expect("layer values");
        let mut rendered =
            smith_bench::agent::smith::product_settings(&context, &base, &login).expect("product rendering");
        let invocation = rendered.as_object_mut().expect("layer").remove("invocation").expect("product flags");
        let directory = private.config.join("smith");
        std::fs::create_dir(&directory).expect("user layer directory");
        let document: toml::Value = serde_json::from_value(rendered).expect("TOML document");
        std::fs::write(directory.join("config.toml"), toml::to_string(&document).expect("user layer"))
            .expect("scratch settings");
        let result = smith_bench::process::execute(
            &clock,
            skein_world::end_to_end::Command {
                program: skein_scratch::paths::binary("smith"),
                arguments: product_check_arguments(&invocation, &private.workspace, root),
                environment,
                directory: private.workspace.clone(),
            },
            Box::default(),
            smith_bench::process::Capture::Raw,
            smith_bench::process::Bounds {
                hard: Duration::from_secs(1),
                teardown: Duration::ZERO,
                grace: Duration::from_millis(20),
                settlement: Duration::from_secs(1),
                stdout: 1_048_576,
                stderr: 65_536,
                force_walk: true,
            },
        )
        .expect("check settles");
        assert!(!root.join("unopened").exists());
        assert_eq!(std::fs::read_dir(&private.state).expect("scratch state").count(), 0);
        result
    }

    fn product_check_arguments(
        invocation: &serde_json::Value,
        workspace: &Path,
        root: &Path,
    ) -> Vec<std::ffi::OsString> {
        let mut arguments = vec![
            "check".into(),
            "--json".into(),
            "exec".into(),
            "--json".into(),
            "-C".into(),
            workspace.into(),
            "-o".into(),
            root.join("unopened/result").into(),
            "--ephemeral".into(),
        ];
        arguments.extend(
            invocation.as_array().expect("product flags").iter().map(|value| value.as_str().expect("flag").into()),
        );
        arguments
    }

    #[test]
    fn the_product_adapter_user_layer_passes_the_shipped_check() {
        let result = pinned_product_check(5);
        assert_eq!(result.exit, smith_bench::Exit::Code(0), "pinned product settings pass check");
        assert!(
            smith_bench::agent::smith::teardown_from_check(&result.stdout)
                .is_ok_and(|bound| bound == skein_lib::Duration::from_secs(100)),
            "check reports its teardown bound"
        );
    }
}
