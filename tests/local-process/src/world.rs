//! The shared harness owns every loop, hosted admission and descriptor lifecycle.
//! This module supplies scenario configuration and the fake machine's filesystem
//! and git face, never process routing (testing.md, sections 2.1, 3.2 and 5).
use crate::credential_fixture;
use crate::{
    process::{self, Launch, Proc},
    referee::{CheckoutRead, Ending, Run},
    terminal::{Action, Terminal},
};
use skein_fake_checkout::git::{Remote, Tree};
use skein_fake_checkout::{Checkout, git};
use skein_fake_llm_domain::api;
use skein_fake_machine::{How, Machine as Filesystem, Opened};
use skein_io::kernel;
use skein_lib::{Duration, Queue, Wall};
use skein_world::{HostedProgram, Memory, Outcome};
use smith_local_domain as local;
use smith_local_shell::local_host::token_limits;
use smith_local_world::git::History;
use std::cell::RefCell;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

mod key;
mod lending;
pub use key::provider as key_provider;
pub(crate) use lending::login_token;
pub use lending::{Lending, login_at};

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Both placements enter the same shell and local policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Placement {
    Spawned,
    InProcess,
}

/// Credential state at the next shell startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Authentication {
    SignIn,
    Refresh,
    Refused,
    Proactive,
    /// An existing sign-in record serves the run without a token exchange.
    Current,
}

/// Files surviving separate invocations of the actual shared shell.
pub struct Files {
    path: PathBuf,
}

impl Files {
    #[must_use]
    pub fn new() -> Self {
        let index = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("smith-local-process-{}-{index}", std::process::id()));
        std::fs::DirBuilder::new().recursive(true).mode(0o700).create(&path).expect("fixture directory");
        Self { path }
    }
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
impl Default for Files {
    fn default() -> Self {
        Self::new()
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn scripts() -> Box<[api::Script]> {
    let mut turns = Vec::new();
    for answer in ["First answer", "Second answer", "Third answer"] {
        turns.push(api::Turn {
            lines: Box::new([
                api::Line::Text { text: b"Finishing now".as_slice().into() },
                api::Line::Call {
                    name: b"finish".as_slice().into(),
                    arguments: format!("{{\"form\":\"report\",\"text\":\"{answer}\",\"fields\":{{}}}}")
                        .into_bytes()
                        .into(),
                },
            ]),
            finish: api::Finish::ToolCalls,
            tokens: 1,
        });
    }
    Box::new([api::Script { cue: b"@local-shell".as_slice().into(), turns: turns.into() }])
}

fn waiting_scripts() -> Box<[api::Script]> {
    Box::new([api::Script {
        cue: b"@local-shell".as_slice().into(),
        turns: Box::new([
            api::Turn {
                lines: Box::new([api::Line::Text { text: b"Starting now".as_slice().into() }]),
                finish: api::Finish::Stop,
                tokens: 1,
            },
            api::Turn {
                lines: Box::new([api::Line::Call {
                    name: b"wait".as_slice().into(),
                    arguments: b"{}".as_slice().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            },
            api::Turn {
                lines: Box::new([
                    api::Line::Text { text: b"Read follow-up".as_slice().into() },
                    api::Line::Call {
                        name: b"finish".as_slice().into(),
                        arguments: br#"{"form":"report","text":"Read follow-up","fields":{}}"#.as_slice().into(),
                    },
                ]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            },
        ]),
    }])
}

fn change_scripts() -> Box<[api::Script]> {
    let turns = [
        ("write", "{\"path\":\"result.txt\",\"content\":\"new\\n\"}"),
        (
            "finish",
            "{\"form\":\"change\",\"text\":\"\",\"fields\":{\"title\":\"Updated result\",\"body\":\"Created the result file\"}}",
        ),
        (
            "finish",
            "{\"form\":\"change\",\"text\":\"\",\"fields\":{\"title\":\"Updated result\",\"body\":\"Created the result file\"}}",
        ),
        (
            "finish",
            "{\"form\":\"change\",\"text\":\"\",\"fields\":{\"title\":\"Updated result\",\"body\":\"Created the result file\"}}",
        ),
    ];
    Box::new([api::Script {
        cue: b"@local-shell".as_slice().into(),
        turns: turns
            .into_iter()
            .map(|(name, arguments)| api::Turn {
                lines: Box::new([api::Line::Call {
                    name: name.as_bytes().into(),
                    arguments: arguments.as_bytes().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            })
            .collect(),
    }])
}

// Shared scenario data: any tier can select these same provider turns.
fn tool_scripts() -> Box<[api::Script]> {
    let turns = [
        ("read", r#"{"path":"original.txt"}"#),
        ("search", r#"{"pattern":"original","path":"original.txt"}"#),
        ("edit", r#"{"path":"original.txt","edits":[{"old":"original","new":"edited"}]}"#),
        ("write", r#"{"path":"result.txt","content":"new\n"}"#),
        ("shell", r#"{"command":"printf 'ran\n' > command.txt"}"#),
        (
            "finish",
            r#"{"form":"change","text":"","fields":{"title":"Updated result","body":"Created the result file"}}"#,
        ),
    ];
    Box::new([api::Script {
        cue: b"@local-shell".as_slice().into(),
        turns: turns
            .into_iter()
            .map(|(name, arguments)| api::Turn {
                lines: Box::new([api::Line::Call {
                    name: name.as_bytes().into(),
                    arguments: arguments.as_bytes().into(),
                }]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            })
            .collect(),
    }])
}

/// Checkout observations shared only by the machine and outside referee.
#[derive(Clone)]
pub struct CheckoutView(Rc<RefCell<Option<(Checkout, History, u64)>>>, Option<Vec<u8>>);
impl CheckoutRead for CheckoutView {
    fn borrowed_notice(&self) -> Option<Vec<u8>> {
        self.1.clone()
    }
    fn head(&self) -> Option<Vec<u8>> {
        self.0.borrow().as_ref().map(|(_, _, head)| format!("{head:040x}").into_bytes())
    }
    fn message(&self, commit: &[u8]) -> Vec<u8> {
        let commit = u64::from_str_radix(std::str::from_utf8(commit).expect("head"), 16).expect("object");
        self.0.borrow().as_ref().expect("checkout").1.commit_message(commit).to_vec()
    }
    fn files(&self, commit: &[u8]) -> Tree {
        let commit = u64::from_str_radix(std::str::from_utf8(commit).expect("head"), 16).expect("object");
        self.0.borrow().as_ref().expect("checkout").1.tree(commit)
    }
}
/// The fake disk and checkout, outside every process heap.
pub struct Machine {
    files: Filesystem,
    master: Opened,
    checkout: CheckoutView,
    commands: Vec<Box<[Box<[u8]>]>>,
    chat_path: PathBuf,
}
impl Machine {
    fn current_tree(&mut self) -> Tree {
        let root = self.files.open(self.master, b"repo", How::Directory).expect("repo");
        let mut pending = vec![(root, Vec::<u8>::new())];
        let mut tree = Tree::new();
        while let Some((directory, prefix)) = pending.pop() {
            for (kind, name) in self.files.list(directory, 32, 4096).expect("bounded fixture tree") {
                if name.as_ref() == b".git" {
                    continue;
                }
                let path = [prefix.as_slice(), name.as_ref()].concat();
                match kind {
                    skein_fake_machine::Is::Directory => {
                        let opened = self.files.open(directory, &name, How::Directory).expect("directory");
                        pending.push((opened, [path.as_slice(), b"/"].concat()));
                    }
                    skein_fake_machine::Is::File => {
                        let opened = self.files.open(directory, &name, How::Read).expect("file");
                        tree.insert(path, self.files.read(opened, 0, 8192).expect("fixture bytes"));
                        self.files.close(opened);
                    }
                    _ => panic!("fixture uses regular files only"),
                }
            }
            self.files.close(directory);
        }
        tree
    }

    fn git_reply(&mut self, args: &[Box<[u8]>]) -> (Box<[u8]>, u8) {
        let tree = self.current_tree();
        let mut shared = self.checkout.0.borrow_mut();
        let (checkout, history, head) = shared.as_mut().expect("git fixture");
        checkout.replace_tree(b"work", &tree);
        let verb = args.first().expect("git verb").as_ref();
        let bytes = match verb {
            b"rev-parse" if args.len() == 2 => format!("{head:040x}\n").into_bytes(),
            b"rev-parse" => return (Box::new([]), 1),
            b"status" => {
                let previous = history.tree(*head);
                let mut changed = Vec::new();
                for (path, bytes) in &tree {
                    if previous.get(path) != Some(bytes) {
                        changed.extend_from_slice(b" M ");
                        changed.extend_from_slice(path);
                        changed.push(0);
                    }
                }
                for path in previous.keys() {
                    if !tree.contains_key(path) {
                        changed.extend_from_slice(b" D ");
                        changed.extend_from_slice(path);
                        changed.push(0);
                    }
                }
                changed
            }
            b"add" => Vec::new(),
            b"-c" => {
                *head = git::commit(history, checkout, b"work", *head, args.last().expect("commit message"))
                    .expect("local commit parent")
                    .expect("changed tree");
                Vec::new()
            }
            b"log" => {
                let pattern = &args[3];
                let trailer = &pattern[b"--grep=^".len()..pattern.len() - 1];
                let mut at = Some(*head);
                let mut found = Vec::new();
                while let Some(commit) = at {
                    let message = history.commit_message(commit);
                    if message.split(|byte| *byte == b'\n').any(|line| line == trailer) {
                        found = format!("{commit:040x}\0").into_bytes();
                        found.extend_from_slice(message);
                        found.push(b'\n');
                        break;
                    }
                    at = history.parent(commit);
                }
                found
            }
            b"push" => panic!("push was not configured"),
            _ => panic!("unexpected git command {args:?}"),
        };
        (bytes.into(), 0)
    }
}
impl skein_world::Machine for Machine {
    fn open_root(&mut self, path: &[u8]) -> Result<skein_sim::Handle, kernel::Error> {
        if let Some(arguments) = path.strip_prefix(b".smith-test/git/") {
            assert!(self.commands.len() < 128, "bounded git commands");
            let arguments: Box<[Box<[u8]>]> = serde_json::from_slice(arguments).expect("exact git arguments");
            let (bytes, code) = self.git_reply(&arguments);
            self.commands.push(arguments);
            let name = format!("git-{}", self.commands.len()).into_bytes();
            let temporary =
                self.files.open(self.master, b".smith-test", How::Directory).expect("fake result namespace");
            self.files.make_directory(temporary, &name, 0o700).expect("fresh result directory");
            let root = self.files.open(temporary, &name, How::Directory).expect("result root");
            self.files.close(temporary);
            let file = self.files.open(root, b"result", How::Create { mode: 0o600 }).expect("result file");
            let mut result = vec![code];
            result.extend_from_slice(&bytes);
            self.files.write(file, 0, &result).expect("bounded result");
            self.files.close(file);
            Ok(skein_sim::Handle::new(root.raw()))
        } else {
            let path = if path == self.chat_path.as_os_str().as_encoded_bytes() { b"chat" } else { path };
            self.files
                .open(self.master, path, How::Directory)
                .map(|root| skein_sim::Handle::new(root.raw()))
                .map_err(|_| kernel::Error::NotFound)
        }
    }
    fn open_append(
        &mut self,
        root: skein_sim::Handle,
        path: &[u8],
        mode: u32,
    ) -> Result<skein_sim::Handle, kernel::Error> {
        self.files
            .open(Opened::new(root.raw()), path, How::Create { mode })
            .map(|file| skein_sim::Handle::new(file.raw()))
            .map_err(|_| kernel::Error::NotFound)
    }
    fn close_root(&mut self, root: skein_sim::Handle) {
        self.files.close(Opened::new(root.raw()));
    }
    fn step(&mut self, call: skein_sim::Call, answers: &mut Queue<skein_sim::Answer>) {
        let root = self.files.open(self.master, b"chat", How::Directory).expect("chat fixture");
        for entry in std::fs::read_dir(&self.chat_path).expect("outside chat faults") {
            let entry = entry.expect("fixture entry");
            if entry.path().is_dir() {
                let _ = self.files.make_directory(root, entry.file_name().as_encoded_bytes(), 0o700);
            }
        }
        self.files.close(root);
        skein_fake_machine::step(&mut self.files, call, answers);
        if let Ok(root) = self.files.open(self.master, b"tokens", How::Directory) {
            if let Ok(file) = self.files.open(root, b"0.json", How::Read) {
                let bytes = self.files.read(file, 0, token_limits().record_bytes).expect("native kept record");
                self.files.close(file);
                let directory = self.chat_path.parent().expect("fixture home").join("tokens");
                credential_fixture::Tokens::new(&directory, token_limits())
                    .expect("fixture directory")
                    .save(0, &bytes)
                    .expect("outside native record observation");
            }
            self.files.close(root);
        }
    }
}

/// A reusable story and its immutable hosted-launch settings.
pub struct Scenario {
    pub launch: Launch,
    pub commands: Vec<Action>,
    pub ending: Ending,
    pub interrupt_on_query: bool,
    pub terminate_on_output_end: bool,
    /// Keep the answered spawned process alive until its host sends a signal.
    pub linger_agent: bool,
    pub authentication: Option<Authentication>,
}
/// Simulated configuration and the settled shared-harness result.
pub struct World {
    pub scenario: Scenario,
    machine: Option<Machine>,
    checkout: CheckoutView,
    memory: Memory,
    outcome: Option<Outcome<Proc, Machine>>,
    commit_message: Vec<u8>,
    first_post: Option<Wall>,
    saved_before_query: bool,
}
impl World {
    /// Adopt the workspace's chats root with its initial private chat.
    pub fn interactive(&mut self) {
        self.scenario.launch.interactive = true;
        let machine = self.machine.as_mut().expect("before the invocation");
        let root = machine.files.open(machine.master, b"chat", How::Directory).unwrap();
        machine.files.make_directory(root, b"main", 0o700).unwrap();
        machine.files.close(root);
        std::fs::create_dir_all(machine.chat_path.join("main")).unwrap();
    }

    /// The settled private records of one selected chat, observed from the fake disk.
    pub fn chat_files(&mut self, name: &[u8]) -> Vec<Box<[u8]>> {
        let machine = &mut self.outcome.as_mut().expect("settled").machine;
        let path = [b"chat/".as_slice(), name].concat();
        let root = machine.files.open(machine.master, &path, How::Directory).unwrap();
        let entries = machine.files.list(root, 16384, 1 << 20).unwrap();
        machine.files.close(root);
        entries.into_iter().map(|(_, name)| name).collect()
    }
    #[must_use]
    pub fn new(seed: u64, files: &Files, placement: Placement, answer: &[u8]) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![Action::Send(b"hello\n".as_slice().into()), Action::Wait(answer.into()), Action::Eof],
            Ending::Report(answer.to_vec()),
            (false, true, false),
        )
    }
    /// Three workspace reads followed by an accepted report in the same process.
    #[must_use]
    pub fn reading(seed: u64, files: &Files, placement: Placement) -> Self {
        let mut world = Self::configured(
            seed,
            files,
            placement,
            vec![
                Action::Send(b"read then report\n".as_slice().into()),
                Action::Wait(b"After three tools".as_slice().into()),
                Action::Eof,
            ],
            Ending::Report(b"After three tools".to_vec()),
            (false, true, true),
        );
        world.scenario.launch.traced = true;
        world.scenario.launch.read_report = true;
        world
    }

    /// Name the product's event destination while retaining the ordinary chat story.
    #[must_use]
    pub fn traced(seed: u64, files: &Files, placement: Placement, answer: &[u8]) -> Self {
        let mut world = Self::new(seed, files, placement, answer);
        world.scenario.launch.traced = true;
        world
    }

    /// Read the settled append bytes independently of the local service.
    #[must_use]
    pub fn records(&mut self) -> Vec<smith_events::Record> {
        self.trace_records(b"events.jsonl")
    }

    /// The agent process's own stream, not its host's product observations.
    #[must_use]
    pub fn agent_records(&mut self) -> Vec<smith_events::Record> {
        self.trace_records(b"events.jsonl.agent")
    }

    fn trace_records(&mut self, path: &[u8]) -> Vec<smith_events::Record> {
        let machine = &mut self.outcome.as_mut().expect("settled world").machine;
        let trace = machine.files.open(machine.master, path, How::Read).expect("trace file");
        let size = u32::try_from(machine.files.stat(trace).size).expect("finite trace size");
        let bytes = machine.files.read(trace, 0, size).expect("trace bytes");
        machine.files.close(trace);
        let lower = process::lower_configuration();
        let mut events = smith_agent_service::EventLimits::derive(
            &lower.limits,
            smith_agent_service::sinks::Capture::Everything,
            1,
            0,
            &[],
            Duration::from_secs(1),
        )
        .expect("trace limits");
        events.sinks.records.items = events.sinks.records.items.max(size);
        smith_agent_process_world::events::read(&bytes, &events)
    }

    #[must_use]
    pub fn finishing(seed: u64, files: &Files, placement: Placement) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![
                Action::Send(b"hello\n".as_slice().into()),
                Action::Wait(b"Finishing now".as_slice().into()),
                Action::Send(b"follow-up\n".as_slice().into()),
                Action::Wait(b"Second answer".as_slice().into()),
                Action::Eof,
            ],
            Ending::Report(b"Second answer".to_vec()),
            (false, true, false),
        )
    }

    /// Send a follow-up across the first turn's wait fence.
    #[must_use]
    pub fn waiting(seed: u64, files: &Files, placement: Placement) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![
                Action::Send(b"hello\n".as_slice().into()),
                Action::Wait(b"Starting now".as_slice().into()),
                Action::Send(b"follow-up\n".as_slice().into()),
                Action::Wait(b"Read follow-up".as_slice().into()),
                Action::Eof,
            ],
            Ending::Report(b"Read follow-up".to_vec()),
            (false, true, false),
        )
    }

    #[must_use]
    pub fn interrupted(seed: u64, files: &Files, placement: Placement) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![
                Action::Send(b"hello\n".as_slice().into()),
                Action::Wait(b"interrupted: the run was cancelled".as_slice().into()),
                Action::Eof,
            ],
            Ending::Cancelled,
            (true, true, false),
        )
    }

    /// Deliver both interrupts from the world's controls while the provider holds its response.
    #[must_use]
    pub fn dropped(seed: u64, files: &Files, placement: Placement) -> Self {
        let mut world = Self::interrupted(seed, files, placement);
        world.scenario.ending = Ending::Dropped;
        world.scenario.commands = vec![Action::Send(b"hello\n".as_slice().into())];
        world
    }

    /// Fail the second durable turn rename after the provider receives its first request.
    #[must_use]
    pub fn failed_store(seed: u64, files: &Files, placement: Placement) -> Self {
        let mut world = Self::new(seed, files, placement, b"could not be kept: write failed");
        world.scenario.ending = Ending::HostFailure;
        world
    }

    /// Fail the turn save while its read call keeps the run from answering.
    #[must_use]
    pub fn failed_store_before_answer(seed: u64, files: &Files, placement: Placement) -> Self {
        let mut world = Self::changed(seed, files, placement);
        world.scenario.ending = Ending::HostFailureBeforeAnswer;
        world
    }
    #[must_use]
    pub fn without_facts(seed: u64, files: &Files, placement: Placement, answer: &[u8]) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![Action::Send(b"hello\n".as_slice().into()), Action::Wait(answer.into()), Action::Eof],
            Ending::Report(answer.to_vec()),
            (false, false, false),
        )
    }
    #[must_use]
    pub fn changed(seed: u64, files: &Files, placement: Placement) -> Self {
        Self::configured(
            seed,
            files,
            placement,
            vec![
                Action::Send(b"change it\n".as_slice().into()),
                Action::Wait(b"Change delivered".as_slice().into()),
                Action::Eof,
            ],
            Ending::Change { before: format!("{:040x}", 1).into_bytes() },
            (false, true, true),
        )
    }
    fn configured(
        seed: u64,
        files: &Files,
        placement: Placement,
        commands: Vec<Action>,
        ending: Ending,
        behavior: (bool, bool, bool),
    ) -> Self {
        let (interrupt_on_query, keep_facts, change) = behavior;
        let mut disk = Filesystem::for_user(skein_shell::effective_user());
        std::fs::create_dir_all(files.path.join("chat")).expect("chat fixture");
        let mut items =
            vec![skein_fake_machine::Item::directory(b".smith-test"), skein_fake_machine::Item::directory(b"chat")];
        for entry in std::fs::read_dir(files.path.join("chat")).expect("previous chat") {
            let entry = entry.expect("chat entry");
            let path = [b"chat/".as_slice(), entry.file_name().as_encoded_bytes()].concat();
            items.push(if entry.path().is_dir() {
                skein_fake_machine::Item::directory(&path)
            } else {
                skein_fake_machine::Item::file(&path, &std::fs::read(entry.path()).expect("chat record"))
            });
        }
        if change {
            items.push(skein_fake_machine::Item::directory(b"repo"));
            items.push(skein_fake_machine::Item::file(b"repo/original.txt", b"original\n"));
        }
        let master = disk.lay(&items);
        let checkout = CheckoutView(
            Rc::new(RefCell::new(if change {
                let mut history = History::new(Tree::from([(b"original.txt".to_vec(), b"original\n".to_vec())]));
                let mut checkout = Checkout::new();
                git::clone_repository(&mut history, &mut checkout, b"repo", b"work").expect("clone fixture");
                git::check_out(&history, &mut checkout, b"work", 1).expect("checkout fixture");
                Some((checkout, history, 1))
            } else {
                None
            })),
            None,
        );
        let token_directory = files.path.join("tokens");
        let tokens = credential_fixture::Tokens::new(&token_directory, token_limits()).expect("token directory");
        if tokens.load(0).expect("token record").is_none() {
            let saved = skein_oauth::SavedToken {
                issued_client_id: None,
                granted_scopes: None,
                subject: None,
                id_token: None,
                earliest_refresh_at: None,
                key: 0,
                generation: 1,
                access_token: b"token".as_slice().into(),
                refresh_token: Some(b"refresh".as_slice().into()),
                metadata: None,
                expires_at: Wall::from_nanos(
                    skein_tls_world::pki::VALID.as_nanos() + Duration::from_secs(7200).as_nanos(),
                ),
            };
            tokens
                .save(0, &skein_oauth::encode_record(&saved, &token_limits()).expect("token fixture"))
                .expect("durable token");
        }
        Self {
            scenario: Scenario {
                launch: Launch {
                    seed,
                    tls: false,
                    trust_der: None,
                    change,
                    tools: false,
                    read_report: false,
                    headless: false,
                    interactive: false,
                    choices: false,
                    budget_failure: false,
                    placement,
                    keep_facts,
                    traced: false,
                    authenticated: false,
                    lending: None,
                    state_directory: files.path.join("chat"),
                    token_directory,
                    root_path: ".".into(),
                },
                commands,
                ending,
                interrupt_on_query,
                terminate_on_output_end: false,
                linger_agent: false,
                authentication: None,
            },
            machine: Some(Machine {
                files: disk,
                master,
                checkout: checkout.clone(),
                commands: Vec::new(),
                chat_path: files.path.join("chat"),
            }),
            checkout,
            memory: Memory::Unchecked,
            outcome: None,
            commit_message: Vec::new(),
            first_post: None,
            saved_before_query: false,
        }
    }
    #[must_use]
    pub fn authenticated(seed: u64, files: &Files, placement: Placement, mode: Authentication) -> Self {
        let mut world = Self::new(
            seed,
            files,
            placement,
            if mode == Authentication::Refused { b"not delivered" } else { b"First answer" },
        );
        world.scenario.launch.authenticated = true;
        world.scenario.authentication = Some(mode);
        if mode == Authentication::Refused {
            world.scenario.ending = Ending::Unavailable;
        }
        let tokens = credential_fixture::Tokens::new(&world.scenario.launch.token_directory, token_limits())
            .expect("private store");
        if mode == Authentication::SignIn {
            std::fs::remove_file(world.scenario.launch.token_directory.join("0.json")).expect("no saved token");
        } else {
            let saved = skein_oauth::SavedToken {
                issued_client_id: None,
                granted_scopes: None,
                subject: None,
                id_token: None,
                earliest_refresh_at: None,
                key: 0,
                generation: 1,
                access_token: if matches!(mode, Authentication::Proactive | Authentication::Current) {
                    b"access-new".as_slice().into()
                } else {
                    b"old".as_slice().into()
                },
                refresh_token: Some(if mode == Authentication::Refused {
                    b"wrong".as_slice().into()
                } else {
                    b"refresh-old".as_slice().into()
                }),
                metadata: None,
                expires_at: Wall::from_nanos(
                    skein_tls_world::pki::VALID.as_nanos()
                        + match mode {
                            Authentication::Proactive => Duration::from_secs(61).as_nanos(),
                            Authentication::Current => Duration::from_secs(3600).as_nanos(),
                            Authentication::SignIn | Authentication::Refresh | Authentication::Refused => 0,
                        },
                ),
            };
            tokens
                .save(0, &skein_oauth::encode_record(&saved, &token_limits()).expect("prior token"))
                .expect("expired grant");
        }
        world
    }
    /// Use the shared per-process construction, iteration and drop meter.
    pub fn check_memory(&mut self) {
        self.memory = Memory::Checked;
    }
    pub fn settle(&mut self) {
        let launch = &self.scenario.launch;
        let mut config = skein_sim::Config::calm();
        config.wall = skein_tls_world::pki::VALID;
        if launch.seed >= 100 {
            config.faults.latency = 300;
            config.faults.latency_max = Duration::from_millis(1);
            config.faults.short_recv = 500;
            config.faults.short_send = 500;
            config.faults.short_read = 500;
            config.faults.short_write = 500;
            config.faults.cancel_race = 500;
        }
        let mut machine = self.machine.take().expect("one invocation");
        if launch.authenticated {
            machine.files.make_directory(machine.master, b"tokens", 0o700).expect("native keeper directory");
            if let Ok(bytes) = std::fs::read(launch.token_directory.join("0.json")) {
                let file = machine
                    .files
                    .open(machine.master, b"tokens/0.json", How::Create { mode: 0o600 })
                    .expect("native input record");
                machine.files.write(file, 0, &bytes).expect("native fixture bytes");
                machine.files.close(file);
            }
        }
        let opened = machine.files.open_handles();
        assert_eq!(opened, 1);
        let terminal_root = machine.files.open(machine.master, b".", How::Directory).expect("terminal root");
        let mut referee = Run::new(
            launch.seed,
            self.scenario.ending.clone(),
            Box::new(self.checkout.clone()),
            launch.token_directory.clone(),
            self.scenario.interrupt_on_query,
            launch.keep_facts,
            self.scenario.expected_token_posts(),
        );
        if matches!(&self.scenario.ending, Ending::Report(text) if text.as_slice() == b"Crossed wait") {
            referee.line_on_wait(b"follow-up\n".as_slice().into());
        }
        if matches!(self.scenario.ending, Ending::HostFailure | Ending::HostFailureBeforeAnswer) {
            referee.fail_turn_at_query(launch.state_directory.clone(), 2);
        }
        let mut world = skein_world::World::new_controlled(launch.seed, config, self.memory, |controls| {
            referee.controlled(controls)
        })
        .with_machine(machine);
        let lower = process::lower_configuration();
        let local_operations =
            smith_local_shell::local_host::Local::kernel_operations(&lower.limits.io, lower.limits.routes, true)
                .expect("local shell and handed accounts fit")
                .checked_add(5)
                .expect("four result-file operations and diagnostic pipe fit");
        world.host_startup(
            HostedProgram {
                program: b"smith-local".as_slice().into(),
                make: process::make_local,
                instances: 1,
                operations: local_operations,
            },
            process::local_roots,
            process::local_appends,
        );
        if matches!(self.scenario.ending, Ending::AgentEnded) {
            world.host(HostedProgram {
                program: b"smith-agent".as_slice().into(),
                make: process::make_exited_agent,
                instances: 1,
                operations: 1,
            });
        } else {
            world.host_startup(
                HostedProgram {
                    program: b"smith-agent".as_slice().into(),
                    make: if self.scenario.linger_agent { process::make_lingering_agent } else { process::make_agent },
                    instances: 8,
                    operations: process::lower_configuration().limits.routes
                        + 3
                        + process::lower_configuration().limits.domain.run.directories,
                },
                process::agent_roots,
                process::agent_appends,
            );
        }
        world.host_roots(
            HostedProgram {
                program: b"/usr/bin/git".as_slice().into(),
                make: crate::git::make,
                instances: 1,
                operations: 4,
            },
            crate::git::roots,
        );
        // The terminal owns a fresh handle, never the machine's master.
        world.spawn_root(skein_sim::Handle::new(terminal_root.raw()), |root| {
            let mut environment = Vec::new();
            if launch.lending.is_some() {
                let home = launch.token_directory.parent().expect("scratch home");
                for (name, path) in [
                    ("HOME", home.to_path_buf()),
                    ("XDG_CONFIG_HOME", home.join("config")),
                    ("XDG_STATE_HOME", home.join("state")),
                ] {
                    environment.push(format!("{name}={}", path.display()).into_bytes().into_boxed_slice());
                }
                if launch.lending == Some(Lending::Env) {
                    let mut value = b"SMITH_FIXTURE_TOKEN=".to_vec();
                    value.extend_from_slice(&login_token(false));
                    environment.push(value.into());
                }
            }
            let mut terminal = Terminal::with_environment(
                root,
                self.scenario.commands.clone(),
                launch.arguments(),
                environment.into(),
            );
            if self.scenario.terminate_on_output_end {
                terminal.terminate_when_output_ends();
            }
            Proc::Terminal(Box::new(terminal))
        });
        world.spawn(|| Proc::Peer(Box::new(self.scenario.provider())));
        if launch.authenticated {
            world.spawn(|| {
                Proc::Issuer(Box::new(process::IssuerProcess::configured(if launch.tls {
                    skein_fake_peers::Transport::Tls
                } else {
                    skein_fake_peers::Transport::Plaintext
                })))
            });
            world.spawn(|| Proc::Browser(Box::default()));
        }
        self.outcome = Some(world.run());
        let machine = &mut self.outcome.as_mut().expect("settled").machine;
        let (path, outside) = if launch.interactive {
            (b"chat/main".as_slice(), machine.chat_path.join("main"))
        } else {
            (b"chat".as_slice(), machine.chat_path.clone())
        };
        let root = machine.files.open(machine.master, path, How::Directory).expect("settled chat");
        for entry in std::fs::read_dir(&outside).expect("outside fixture") {
            let entry = entry.expect("chat entry");
            if entry.path().is_file() {
                std::fs::remove_file(entry.path()).expect("replace outside fixture");
            }
        }
        for (kind, name) in machine.files.list(root, 16384, 1 << 20).expect("chat fixture entries") {
            if kind == skein_fake_machine::Is::File {
                let file = machine.files.open(root, &name, How::Read).expect("chat file");
                let bytes = machine.files.read(file, 0, 64 << 20).expect("chat bytes");
                std::fs::write(outside.join(std::str::from_utf8(&name).expect("chat record name")), bytes)
                    .expect("outside record");
                machine.files.close(file);
            }
        }
        machine.files.close(root);
        let head = self.checkout.head();
        self.commit_message = head.as_ref().map_or_else(Vec::new, |head| self.checkout.message(head));
        // The shared referee's observed values are retained by the peers.
        self.first_post = self.issuer().and_then(process::IssuerProcess::first_post);
        self.saved_before_query = self.scenario.launch.authenticated && !self.queries().is_empty();
    }
    fn procs(&self) -> &[Proc] {
        &self.outcome.as_ref().expect("settled world").procs
    }
    fn terminal(&self) -> &Terminal {
        self.procs()
            .iter()
            .find_map(|p| if let Proc::Terminal(p) = p { Some(p.as_ref()) } else { None })
            .expect("terminal")
    }

    /// The settled terminal's operator output.
    #[must_use]
    pub fn errors(&self) -> &[u8] {
        self.terminal().errors()
    }
    fn peer(&self) -> &crate::llm::Peer {
        self.procs().iter().find_map(|p| if let Proc::Peer(p) = p { Some(p.as_ref()) } else { None }).expect("peer")
    }
    fn issuer(&self) -> Option<&process::IssuerProcess> {
        self.procs().iter().find_map(|p| if let Proc::Issuer(p) = p { Some(p.as_ref()) } else { None })
    }
    fn browser(&self) -> Option<&process::BrowserProcess> {
        self.procs().iter().find_map(|p| if let Proc::Browser(p) = p { Some(p.as_ref()) } else { None })
    }
    #[must_use]
    pub fn commit_message(&self) -> &[u8] {
        &self.commit_message
    }
    #[must_use]
    pub fn commit_files(&self) -> Tree {
        self.checkout.head().as_ref().map_or_else(Tree::new, |head| self.checkout.files(head))
    }
    #[must_use]
    pub fn git_commands(&self) -> &[Box<[Box<[u8]>]>] {
        &self.outcome.as_ref().expect("settled world").machine.commands
    }
    #[must_use]
    pub fn first_token_post(&self) -> Option<Wall> {
        self.first_post
    }
    #[must_use]
    pub fn oauth_posts(&self) -> u64 {
        self.issuer().map_or(0, |issuer| crate::oauth::posts(issuer.peer()))
    }
    #[must_use]
    pub fn page_visits(&self) -> u32 {
        self.browser().map_or(0, process::BrowserProcess::pages)
    }
    #[must_use]
    pub fn browser_replied(&self) -> bool {
        self.browser().is_some_and(process::BrowserProcess::replied)
    }
    #[must_use]
    pub fn saved_before_query(&self) -> bool {
        self.saved_before_query
    }
    #[must_use]
    pub fn shown(&self) -> &[u8] {
        self.terminal().shown()
    }
    #[must_use]
    pub fn queries(&self) -> Vec<api::Query> {
        crate::llm::queries(self.peer()).cloned().collect()
    }
    #[must_use]
    pub fn facts(&self) -> &[local::Fact] {
        self.procs().iter().find_map(|p| if let Proc::Local(p) = p { Some(p.facts()) } else { None }).expect("local")
    }
    #[must_use]
    pub fn exit(&self) -> Option<kernel::Exit> {
        self.terminal().exit()
    }
    #[must_use]
    pub fn trace(&self) -> Vec<String> {
        self.outcome.as_ref().expect("settled world").trace.iter().map(|entry| format!("{entry:?}")).collect()
    }
    #[must_use]
    pub fn steps(&self) -> u32 {
        self.outcome.as_ref().expect("settled world").iterations
    }
    #[must_use]
    pub fn heaps(&self) -> &[(u64, u64)] {
        self.outcome.as_ref().expect("settled world").heap.as_ref().expect("memory checked")
    }
    #[must_use]
    pub fn seen(&self) -> crate::referee::Seen {
        crate::referee::seen(
            self.procs(),
            self.saved_before_query,
            self.git_commands().iter().any(|args| args[0].as_ref() == b"push"),
        )
    }
    #[must_use]
    pub fn turns(&mut self) -> usize {
        let local::Event::Loaded { transcript, .. } =
            load_chat(&self.scenario.launch.state_directory, &process::lower_configuration().channel_endpoints)
        else {
            unreachable!("chat codec terminal")
        };
        transcript.map_or(0, |history| history.turns.len())
    }
}
impl CheckoutRead for World {
    fn expected_token_posts(&self) -> u64 {
        self.scenario.expected_token_posts()
    }
    fn borrowed_notice(&self) -> Option<Vec<u8>> {
        self.checkout.borrowed_notice()
    }
    fn head(&self) -> Option<Vec<u8>> {
        self.checkout.head()
    }
    fn message(&self, commit: &[u8]) -> Vec<u8> {
        self.checkout.message(commit)
    }
    fn files(&self, commit: &[u8]) -> Tree {
        self.checkout.files(commit)
    }
}
impl Scenario {
    /// Token requests this scenario expects its issuer to observe.
    #[must_use]
    pub fn expected_token_posts(&self) -> u64 {
        match self.authentication {
            Some(Authentication::Current) => 0,
            Some(
                Authentication::SignIn | Authentication::Refresh | Authentication::Refused | Authentication::Proactive,
            )
            | None => 1,
        }
    }

    /// Provider scripts and credentials are scenario data, shared across tiers.
    #[must_use]
    pub fn provider(&self) -> crate::llm::Peer {
        if self.launch.lending == Some(Lending::Key) {
            return self.provider_with(skein_llm::Credential {
                value: b"key-for-work".as_slice().into(),
                account_id: Box::new([]),
                scheme: skein_llm::Scheme::Key,
            });
        }
        let token = if self.launch.lending.is_some() {
            login_token(false)
        } else if self.launch.authenticated {
            b"access-new".as_slice().into()
        } else {
            b"token".as_slice().into()
        };
        self.provider_with(skein_llm::Credential {
            value: token,
            account_id: b"acc".as_slice().into(),
            scheme: skein_llm::Scheme::Bearer,
        })
    }

    /// Use the same scripts with the synthetic credential selected by the binary tier.
    #[must_use]
    pub fn provider_with(&self, credential: skein_llm::Credential) -> crate::llm::Peer {
        let mut provider_scripts = if matches!(&self.ending, Ending::Report(text) if text.as_slice() == b"After three tools")
            || matches!(self.ending, Ending::Budget)
        {
            let mut turns = Vec::new();
            for _ in 0..3 {
                turns.push(api::Turn {
                    lines: Box::new([api::Line::Call {
                        name: b"read".as_slice().into(),
                        arguments: br#"{"path":"original.txt"}"#.as_slice().into(),
                    }]),
                    finish: api::Finish::ToolCalls,
                    tokens: 1,
                });
            }
            turns.push(api::Turn {
                lines: Box::new([
                    api::Line::Text { text: b"Finishing after the reads".as_slice().into() },
                    api::Line::Call {
                        name: b"finish".as_slice().into(),
                        arguments: br#"{"form":"report","text":"After three tools","fields":{}}"#.as_slice().into(),
                    },
                ]),
                finish: api::Finish::ToolCalls,
                tokens: 1,
            });
            Box::new([api::Script { cue: b"@local-shell".as_slice().into(), turns: turns.into() }])
        } else if self.launch.tools {
            tool_scripts()
        } else if self.launch.change {
            change_scripts()
        } else if let Ending::Report(text) = &self.ending
            && (text.as_slice() == b"Crossed wait" || text.as_slice() == b"Queued while starting")
        {
            let mut scripts = waiting_scripts();
            let mut turns = scripts[0].turns.to_vec();
            let wait = turns.remove(1).lines[0].clone();
            turns[0].lines = Box::new([api::Line::Text { text: b"Starting now".as_slice().into() }, wait]);
            turns[0].finish = api::Finish::ToolCalls;
            turns[1].lines = Box::new([api::Line::Call {
                name: b"finish".as_slice().into(),
                arguments: format!(
                    "{{\"form\":\"report\",\"text\":\"{}\",\"fields\":{{}}}}",
                    String::from_utf8_lossy(text)
                )
                .into_bytes()
                .into(),
            }]);
            scripts[0].turns = turns.into();
            scripts
        } else if matches!(&self.ending, Ending::Report(text) if text.as_slice() == b"Read follow-up") {
            waiting_scripts()
        } else {
            scripts()
        };
        if self.launch.choices {
            let mut combined = vec![api::Script {
                cue: b"Summarise the work so far".as_slice().into(),
                turns: Box::new([api::Turn {
                    lines: Box::new([api::Line::Text { text: b"The task is kept".as_slice().into() }]),
                    finish: api::Finish::Stop,
                    tokens: 1,
                }]),
            }];
            combined.extend(Vec::from(provider_scripts));
            provider_scripts = combined.into();
        }
        if matches!(self.ending, Ending::HostFailureBeforeAnswer) {
            for turn in &mut provider_scripts[0].turns {
                turn.lines = Box::new([api::Line::Call {
                    name: b"read".as_slice().into(),
                    arguments: br#"{"path":"original.txt"}"#.as_slice().into(),
                }]);
            }
        }
        if self.launch.lending == Some(Lending::Key) {
            return smith_agent_process_world::fake::configured_anthropic(provider_scripts, credential);
        }
        smith_agent_process_world::fake::configured_transport(
            provider_scripts,
            credential,
            if self.interrupt_on_query {
                Duration::from_secs(3600)
            } else if self.authentication == Some(Authentication::Proactive) {
                Duration::from_secs(2)
            } else {
                Duration::ZERO
            },
            if self.launch.tls { skein_fake_peers::Transport::Tls } else { skein_fake_peers::Transport::Plaintext },
        )
    }
}

/// Decode the settled files outside every process, through the shared record codecs.
pub fn load_chat(path: &Path, endpoints: &smith_protocol_channel::Endpoints) -> local::Event {
    let state = std::fs::read(path.join("state"))
        .ok()
        .map(|bytes| smith_local_protocol::decode_state(&bytes).expect("state record"));
    let mut entries = std::fs::read_dir(path)
        .expect("chat directory")
        .map(|entry| entry.expect("chat entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    let turns = entries
        .iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "turn"))
        .map(|path| std::fs::read(path).expect("turn file").into_boxed_slice())
        .collect::<Vec<_>>();
    let (transcript, read) =
        smith_local_protocol::decode_turns(&turns, &smith_transcript::CEILINGS, endpoints).expect("chat transcript");
    let state = state.map(|mut state| {
        if transcript.is_some() {
            state.read = read;
        }
        state
    });
    let deliveries = entries
        .iter()
        .filter(|path| path.file_name().is_some_and(|name| name.as_encoded_bytes().len() == 42))
        .map(|path| {
            smith_local_protocol::decode_delivery(&std::fs::read(path).expect("delivery file"), 1 << 20)
                .expect("delivery record")
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    local::Event::Loaded { state, transcript, deliveries }
}

/// Existing report or waiting script, with the product story's synthetic credential.
#[must_use]
pub fn headless_provider(wait: bool, credential: skein_llm::Credential) -> crate::llm::Peer {
    smith_agent_process_world::fake::configured_transport(
        if wait { waiting_scripts() } else { scripts() },
        credential,
        Duration::ZERO,
        skein_fake_peers::Transport::Tls,
    )
}

/// The existing checked change script with the product story's synthetic grant.
pub fn change_provider(credential: skein_llm::Credential) -> crate::llm::Peer {
    smith_agent_process_world::fake::configured_transport(
        tool_scripts(),
        credential,
        Duration::ZERO,
        skein_fake_peers::Transport::Tls,
    )
}
