//! The real kernel beneath the shared local-process stories (testing.md,
//! sections 2.2, 5 and 6). Scratch files and outside git observations are kept
//! here; service state is never inspected. `World::settle` registers the same
//! local and agent factories over one ring, with TLS peers on loopback.

pub mod settled;

pub use smith_agent_process_world::fake::{loopback, redirect_port};

use skein_fake_checkout::git::Tree;
use skein_lib::Duration;
use skein_shell::{Clock, Host};
use skein_world::{HostedProgram, real};
use smith_local_process_world::{
    Files, Placement, World as Simulated,
    process::{self, Proc},
    referee::{CheckoutRead, Ending, Run, Seen},
    terminal::Terminal,
    world::{Authentication, Scenario},
};
use std::io::Write as _;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Git's actual committed observations, read by the independent referee.
#[derive(Clone, Default)]
pub struct Checkout {
    path: Option<PathBuf>,
}

fn git(path: &Path, arguments: &[&str]) -> Vec<u8> {
    let output = Command::new("/usr/bin/git")
        .arg("-C")
        .arg(path)
        .args(arguments)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .output()
        .expect("real world requires git at /usr/bin/git");
    assert!(output.status.success(), "git {arguments:?}: {}", String::from_utf8_lossy(&output.stderr));
    output.stdout
}

impl CheckoutRead for Checkout {
    fn head(&self) -> Option<Vec<u8>> {
        self.path.as_ref().map(|path| git(path, &["rev-parse", "HEAD"]).trim_ascii_end().to_vec())
    }

    fn message(&self, commit: &[u8]) -> Vec<u8> {
        git(
            self.path.as_ref().expect("mounted repository"),
            &["show", "-s", "--format=%B", std::str::from_utf8(commit).expect("object id")],
        )
    }

    fn files(&self, commit: &[u8]) -> Tree {
        let path = self.path.as_ref().expect("mounted repository");
        let commit = std::str::from_utf8(commit).expect("object id");
        let names = git(path, &["ls-tree", "-r", "--name-only", "-z", commit]);
        names
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(|name| {
                let name = std::str::from_utf8(name).expect("fixture filename");
                (name.as_bytes().to_vec(), git(path, &["show", &format!("{commit}:{name}")]))
            })
            .collect()
    }
}

/// A scratch directory holding durable settings, tokens, chat and a checkout.
pub struct Scratch {
    files: Files,
}

impl Scratch {
    #[must_use]
    pub fn new() -> Self {
        for (tool, path) in [("git", "/usr/bin/git"), ("rg", "/usr/bin/rg"), ("sh", "/bin/sh")] {
            assert!(Path::new(path).is_file(), "real world requires {tool} at {path}");
        }
        smith_agent_process_world::fake::real_loopback();
        let files = Files::new();
        let repo = files.path().join("repo");
        std::fs::create_dir(&repo).expect("scratch repository");
        std::fs::write(repo.join("original.txt"), b"original\n").expect("initial file");
        std::fs::create_dir(repo.join(".smith-test")).expect("check directory");
        let check = repo.join(".smith-test/check");
        std::fs::write(
            &check,
            br#"#!/bin/sh
set -eu
test "$(cat result.txt)" = new
test "$(cat original.txt)" = edited
test "$(cat command.txt)" = ran
printf 'checked\n' > checks-ran.txt
"#,
        )
        .expect("check script");
        std::fs::set_permissions(check, std::fs::Permissions::from_mode(0o700)).expect("executable check");
        std::fs::write(files.path().join("root.der"), skein_tls_world::pki::ROOT).expect("private test trust");
        git(&repo, &["init", "-q"]);
        std::fs::OpenOptions::new()
            .append(true)
            .open(repo.join(".git/config"))
            .expect("initialized repository config")
            .write_all(b"[user]\n\tname = Smith test\n\temail = smith@example.invalid\n")
            .expect("fixture commit identity");
        git(&repo, &["add", "."]);
        git(&repo, &["commit", "-q", "-m", "Initial checkout"]);
        Self { files }
    }

    /// The repository face observed independently by higher tiers.
    #[must_use]
    pub fn checkout(&self) -> Checkout {
        Checkout { path: Some(self.path().join("repo")) }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.files.path()
    }
}

impl Default for Scratch {
    fn default() -> Self {
        Self::new()
    }
}

/// The shared scenario and its terminal/peer outcomes on the real kernel.
pub struct World {
    pub scenario: Scenario,
    checkout: Checkout,
    outcome: Option<real::CheckedOutcome<Proc>>,
}

impl World {
    #[must_use]
    pub fn new(seed: u64, scratch: &Scratch, placement: Placement, answer: &[u8]) -> Self {
        Self::configured(Simulated::new(seed, &scratch.files, placement, answer).scenario, scratch)
    }

    #[must_use]
    pub fn authenticated(seed: u64, scratch: &Scratch, placement: Placement) -> Self {
        Self::configured(
            Simulated::authenticated(seed, &scratch.files, placement, Authentication::SignIn).scenario,
            scratch,
        )
    }

    #[must_use]
    pub fn changed(seed: u64, scratch: &Scratch, placement: Placement) -> Self {
        let mut scenario = Simulated::changed(seed, &scratch.files, placement).scenario;
        let checkout = Checkout { path: Some(scratch.path().join("repo")) };
        scenario.launch.tools = true;
        scenario.ending = Ending::Change { before: checkout.head().expect("initial head") };
        Self::configured(scenario, scratch)
    }

    fn configured(mut scenario: Scenario, scratch: &Scratch) -> Self {
        scenario.launch.tls = true;
        scenario.launch.trust_der = Some(scratch.path().join("root.der"));
        scenario.launch.root_path = scratch.path().to_path_buf();
        Self {
            checkout: Checkout { path: scenario.launch.change.then(|| scratch.path().join("repo")) },
            scenario,
            outcome: None,
        }
    }

    pub fn settle(&mut self) {
        settled::assert_no_children();
        let before = settled::Before::new(&self.scenario.launch.root_path);
        let launch = &self.scenario.launch;
        let referee = Run::new(
            launch.seed,
            self.scenario.ending.clone(),
            Box::new(self.checkout.clone()),
            launch.token_directory.clone(),
            self.scenario.interrupt_on_query,
            launch.keep_facts,
            self.scenario.expected_token_posts(),
        );
        let mut world = real::World::new_checked(referee);
        let lower = process::lower_configuration();
        let local_operations =
            smith_local_shell::local_host::Local::kernel_operations(&lower.limits.io, lower.limits.routes, true)
                .expect("local shell and handed accounts fit")
                .checked_add(5)
                .expect("four result-file operations and diagnostic pipe fit");
        world.host_roots(
            HostedProgram {
                program: b"smith-local".as_slice().into(),
                make: process::make_local,
                instances: 1,
                operations: local_operations,
            },
            process::local_roots,
        );
        world.host_roots(
            HostedProgram {
                program: b"smith-agent".as_slice().into(),
                make: process::make_agent,
                instances: 8,
                operations: lower
                    .limits
                    .routes
                    .checked_add(3)
                    .and_then(|count| count.checked_add(lower.limits.domain.run.directories))
                    .expect("agent shell operations"),
            },
            process::agent_roots,
        );
        let root = skein_shell::open_root(&launch.root_path).expect("open scratch terminal root");
        world.spawn_with_fds(vec![root], || {
            Proc::Terminal(Box::new(Terminal::configured(root, self.scenario.commands.clone(), launch.arguments())))
        });
        world.spawn(|| Proc::Peer(Box::new(self.scenario.provider())));
        if launch.authenticated {
            world
                .spawn(|| Proc::Issuer(Box::new(process::IssuerProcess::configured(skein_fake_peers::Transport::Tls))));
            world.spawn(|| Proc::Browser(Box::default()));
        }
        self.outcome = Some(world.run(&Clock::new(), Duration::from_secs(10)));
        let outcome = self.outcome.as_ref().expect("settled checked world");
        assert!(outcome.procs.iter().all(Host::is_empty), "all real processes have settled");
        assert!(outcome.killed.is_empty(), "normal stories kill no hosted child");
        settled::assert_no_children();
        before.assert_expected(&self.scenario, &self.seen());
        let heaps = outcome.heap.as_ref().expect("per-process checked heap ledger");
        assert_eq!(heaps.len(), outcome.procs.len());
        for (peak, bound) in heaps {
            assert!(*peak > 0 && peak <= bound, "each constructed and iterated process stays within its bound");
        }
    }

    #[must_use]
    pub fn seen(&self) -> Seen {
        smith_local_process_world::referee::seen(
            &self.outcome.as_ref().expect("settled world").procs,
            self.scenario.launch.authenticated,
            false,
        )
    }

    #[must_use]
    pub fn checkout(&self) -> &Checkout {
        &self.checkout
    }

    /// Actual durable transcript loaded through the same outside store face.
    #[must_use]
    pub fn turns(&self) -> usize {
        match smith_local_process_world::world::load_chat(
            &self.scenario.launch.state_directory,
            &process::lower_configuration().channel_endpoints,
        ) {
            smith_local_domain::Event::Loaded { transcript, .. } => transcript.map_or(0, |history| history.turns.len()),
            _ => unreachable!("store load has its declared terminal"),
        }
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        let outcome = self.outcome.as_ref().expect("settled world");
        outcome.end.saturating_since(outcome.start)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn two_processes_never_share_a_loopback_address() {
        let first = smith_agent_process_world::fake::loopback_for(1);
        let second = smith_agent_process_world::fake::loopback_for(4_194_303);
        assert_ne!(first, second, "distinct live process ids have distinct addresses");
        assert!(first.is_loopback() && second.is_loopback(), "both addresses stay within 127/8");
    }
}
