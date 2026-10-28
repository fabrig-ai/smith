//! Private startup files and ordered cleanup (programs.md, sections 4.2, 5.1).
//! Keeps only created-file flags and directory handles; never scans old runs.

use super::{Backend, Below, Phase};
use crate::mcp::{self, json};
use alloc::boxed::Box;
use skein_io::{file, kernel};
use skein_lib::{Decimal, Env, List, Queue, Time, Token};
use smith_domain::llm;
use smith_protocol_llm::{self as protocol, FromDomain, ToDomain};

pub(super) const SETTINGS: &[u8] =
    br#"{"permissions":{"allow":["mcp__smith"],"ask":["Read","Glob","Grep","Write","Edit","Bash"]}}"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Runs,
    Run,
    Instructions,
    Mcp,
    Settings,
    RemoveResidue,
    RemoveMcp,
    RemoveInstructions,
    CloseRun,
    RemoveRun,
    CloseRuns,
    CloseRoot,
}

pub(super) struct Files {
    runs: Option<Token>,
    runs_descriptor: Option<kernel::Fd>,
    run_descriptor: Option<kernel::Fd>,
    run: Option<Token>,
    pending: Option<Step>,
    instructions: bool,
    mcp: bool,
    remove_run: bool,
    port: u16,
    residue: Option<file::Residue>,
    residue_source: Option<Step>,
}

impl Files {
    pub fn new() -> Self {
        Self {
            runs: None,
            runs_descriptor: None,
            run_descriptor: None,
            run: None,
            pending: None,
            instructions: false,
            mcp: false,
            remove_run: false,
            port: 0,
            residue: None,
            residue_source: None,
        }
    }
    pub(super) fn prepared(&self) -> bool {
        self.run.is_some()
    }

    fn accept(&mut self, step: Step, event: file::Event, descriptor: Option<kernel::Fd>) -> bool {
        match event {
            file::Event::Opened { file, .. } => match step {
                Step::Runs => {
                    self.runs = Some(file);
                    self.runs_descriptor = descriptor;
                    true
                }
                Step::Run => {
                    self.run = Some(file);
                    self.run_descriptor = descriptor;
                    self.remove_run = true;
                    true
                }
                Step::Instructions
                | Step::Mcp
                | Step::Settings
                | Step::RemoveResidue
                | Step::RemoveMcp
                | Step::RemoveInstructions
                | Step::CloseRun
                | Step::RemoveRun
                | Step::CloseRuns
                | Step::CloseRoot => false,
            },
            file::Event::Stored { .. } => {
                match step {
                    Step::Instructions => self.instructions = true,
                    Step::Mcp => self.mcp = true,
                    Step::Runs
                    | Step::Run
                    | Step::Settings
                    | Step::RemoveResidue
                    | Step::RemoveMcp
                    | Step::RemoveInstructions
                    | Step::CloseRun
                    | Step::RemoveRun
                    | Step::CloseRuns
                    | Step::CloseRoot => {}
                }
                true
            }
            file::Event::Removed { .. } | file::Event::Closed { .. } => {
                self.cleaned(step);
                true
            }
            file::Event::Failed { error, committed, residue, .. } => {
                if committed {
                    self.stored(step);
                }
                if let Some(residue) = residue {
                    self.residue = Some(residue);
                    self.residue_source = Some(step);
                }
                if error == kernel::Error::NotFound
                    && match step {
                        Step::RemoveResidue | Step::RemoveMcp | Step::RemoveInstructions | Step::RemoveRun => true,
                        Step::Runs
                        | Step::Run
                        | Step::Instructions
                        | Step::Mcp
                        | Step::Settings
                        | Step::CloseRun
                        | Step::CloseRuns
                        | Step::CloseRoot => false,
                    }
                {
                    self.cleaned(step);
                    true
                } else {
                    false
                }
            }
            file::Event::Locked { .. }
            | file::Event::Busy { .. }
            | file::Event::Made { .. }
            | file::Event::Refused { .. }
            | file::Event::Loaded { .. }
            | file::Event::Scanned { .. }
            | file::Event::Conflict { .. }
            | file::Event::TooLarge { .. }
            | file::Event::Stated { .. }
            | file::Event::Written { .. }
            | file::Event::Read { .. }
            | file::Event::Cut { .. }
            | file::Event::Synced { .. }
            | file::Event::Renamed { .. }
            | file::Event::Listed { .. }
            | file::Event::Cancelled { .. } => false,
        }
    }
    fn stored(&mut self, step: Step) {
        match step {
            Step::Instructions => self.instructions = true,
            Step::Mcp => self.mcp = true,
            Step::Runs
            | Step::Run
            | Step::Settings
            | Step::RemoveResidue
            | Step::RemoveMcp
            | Step::RemoveInstructions
            | Step::CloseRun
            | Step::RemoveRun
            | Step::CloseRuns
            | Step::CloseRoot => {}
        }
    }
    fn cleaned(&mut self, step: Step) {
        match step {
            Step::RemoveResidue => self.residue = None,
            Step::RemoveMcp => self.mcp = false,
            Step::RemoveInstructions => self.instructions = false,
            Step::CloseRun => self.run = None,
            Step::RemoveRun => self.remove_run = false,
            Step::CloseRuns => self.runs = None,
            Step::Runs | Step::Run | Step::Instructions | Step::Mcp | Step::Settings | Step::CloseRoot => {}
        }
    }
}

impl Backend {
    pub(super) fn run_relative(&self) -> Box<[u8]> {
        json::join(&[b".runs/", &self.names.run])
    }

    pub(super) fn run_path(&self, name: &[u8]) -> Box<[u8]> {
        let separator: &[u8] = if self.settings.program.directory.ends_with(b"/") { b"" } else { b"/" };
        json::join(&[&self.settings.program.directory, separator, &self.run_relative(), b"/", name])
    }

    pub(super) fn open_runs(&mut self, now: Time, below: &mut Queue<Below>) {
        self.file(
            Step::Runs,
            file::Request::OpenPrivate {
                owner: self.token(5),
                root: self.settings.directory.expect("checked programs root").file,
                path: b".runs".as_slice().into(),
            },
            now,
            below,
        );
    }

    #[expect(
        clippy::match_same_arms,
        reason = "remove has its own workspace exception arm under protocol/programs.md 5.3"
    )]
    pub(super) fn tools(&self) -> Box<[mcp::Tool]> {
        let (prompt, outcome, deliver) = match &self.request {
            Some(FromDomain::Complete { prompt, outcome, deliver, .. }) => (prompt, outcome, deliver),
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => return Box::default(),
        };
        let Ok(schemas) = protocol::schemas_for_contract(
            prompt,
            &self.settings.adapter.stated,
            outcome,
            deliver.as_ref(),
            self.settings.contract_bytes,
        ) else {
            return Box::default();
        };
        let count = schemas.len().checked_add(prompt.served.len()).expect("admitted tool count");
        let mut tools = List::with_capacity(u32::try_from(count).expect("admitted tool count"));
        for schema in schemas {
            match schema.kind {
                protocol::ToolKind::Owned(smith_domain::tools::Tool::Remove) => {
                    tools
                        .push(mcp::Tool {
                            name: schema.name,
                            description: schema.description,
                            input_schema: schema.schema,
                        })
                        .expect("inventory capacity");
                }
                protocol::ToolKind::SubAgent
                | protocol::ToolKind::Owned(
                    smith_domain::tools::Tool::Read
                    | smith_domain::tools::Tool::List
                    | smith_domain::tools::Tool::Search
                    | smith_domain::tools::Tool::Write
                    | smith_domain::tools::Tool::Edit
                    | smith_domain::tools::Tool::Shell
                    | smith_domain::tools::Tool::Job,
                ) => {}
                protocol::ToolKind::Wait | protocol::ToolKind::Deliver | protocol::ToolKind::Finish => {
                    tools
                        .push(mcp::Tool {
                            name: schema.name,
                            description: schema.description,
                            input_schema: schema.schema,
                        })
                        .expect("inventory capacity");
                }
            }
        }
        for descriptor in &prompt.served {
            match descriptor {
                llm::Served::Host(tool) => tools
                    .push(mcp::Tool {
                        name: tool.name.clone(),
                        description: tool.description.clone(),
                        input_schema: tool.schema.clone(),
                    })
                    .expect("host inventory capacity"),
                llm::Served::Wait | llm::Served::SubAgent | llm::Served::Deliver | llm::Served::Finish => {}
            }
        }
        tools.into_boxed()
    }

    pub(super) fn store_instructions(&mut self, now: Time, port: u16, below: &mut Queue<Below>) {
        self.files.port = port;
        let bytes = match &self.request {
            Some(FromDomain::Complete { prompt, .. }) => prompt.system.clone(),
            Some(FromDomain::Cancel { .. } | FromDomain::Yield { .. }) | None => {
                unreachable!("startup retains its prompt")
            }
        };
        self.file(
            Step::Instructions,
            file::Request::Store {
                owner: self.token(5),
                root: self.files.run.expect("private run root"),
                path: b"instructions.md".as_slice().into(),
                bytes,
                expected: file::Expect::Any,
                no_follow: true,
            },
            now,
            below,
        );
    }

    pub(super) fn file_event(
        &mut self,
        env: &Env<protocol::ComponentLimits>,
        event: file::Event,
        descriptor: Option<kernel::Fd>,
        up: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let step = self.files.pending.take().expect("one pending private file request");
        let success = self.files.accept(step, event, descriptor);
        if self.phase == Phase::Ending {
            if success && step == Step::CloseRoot {
                self.settings.directory = None;
            }
            return;
        }
        if !success {
            self.fail(
                env,
                llm::Failure::Program { exit: None },
                b"smith: the private program files could not be prepared",
                up,
                below,
            );
            return;
        }
        match step {
            Step::Runs => self.file(
                Step::Run,
                file::Request::OpenPrivate {
                    owner: self.token(5),
                    root: self.files.runs.expect("runs root"),
                    path: self.names.run.as_slice().into(),
                },
                env.now,
                below,
            ),
            Step::Run => self.prepare_invocation(env, up, below),
            Step::Instructions if self.compaction() => self.launch(env, up, below),
            Step::Instructions => {
                let bytes = self.mcp_configuration();
                self.file(
                    Step::Mcp,
                    file::Request::Store {
                        owner: self.token(5),
                        root: self.files.run.expect("run root"),
                        path: b"mcp.json".as_slice().into(),
                        bytes,
                        expected: file::Expect::Any,
                        no_follow: true,
                    },
                    env.now,
                    below,
                );
            }
            Step::Mcp => {
                let path = json::join(&[&self.grant.as_ref().expect("installed source").source, b"/settings.json"]);
                self.file(
                    Step::Settings,
                    file::Request::Store {
                        owner: self.token(5),
                        root: self.settings.directory.expect("programs root").file,
                        path,
                        bytes: SETTINGS.into(),
                        expected: file::Expect::Any,
                        no_follow: true,
                    },
                    env.now,
                    below,
                );
            }
            Step::Settings => self.launch(env, up, below),
            Step::RemoveResidue
            | Step::RemoveMcp
            | Step::RemoveInstructions
            | Step::CloseRun
            | Step::RemoveRun
            | Step::CloseRuns
            | Step::CloseRoot => {}
        }
    }

    fn mcp_configuration(&self) -> Box<[u8]> {
        mcp_configuration(
            &self.settings.relay,
            self.files.port,
            self.server.as_ref().expect("listening server").secret(),
        )
    }

    fn file(&mut self, step: Step, request: file::Request, now: Time, below: &mut Queue<Below>) {
        assert!(self.files.pending.is_none(), "one private file operation at a time");
        self.files.pending = Some(step);
        below.push(Below::File { request, deadline: now.saturating_add(self.settings.startup) });
    }

    pub(super) fn cleanup(&mut self, now: Time, below: &mut Queue<Below>) {
        if self.native.is_some() || self.files.pending.is_some() {
            return;
        }
        let Some(root) = self.settings.directory else {
            self.phase = Phase::Ended;
            return;
        };
        if let Some(residue) = &self.files.residue {
            let (root, name) = if self.files.residue_source == Some(Step::Settings) {
                (root.descriptor, json::join(&[&self.grant.as_ref().expect("source").source, b"/", &residue.name]))
            } else {
                (self.files.run_descriptor.expect("run descriptor"), residue.name.clone())
            };
            self.file(
                Step::RemoveResidue,
                file::Request::Remove { owner: self.token(5), root, name, directory: false },
                now,
                below,
            );
        } else if self.files.mcp {
            self.file(
                Step::RemoveMcp,
                file::Request::Remove {
                    owner: self.token(5),
                    root: self.files.run_descriptor.expect("opened run descriptor"),
                    name: b"mcp.json".as_slice().into(),
                    directory: false,
                },
                now,
                below,
            );
        } else if self.files.instructions {
            self.file(
                Step::RemoveInstructions,
                file::Request::Remove {
                    owner: self.token(5),
                    root: self.files.run_descriptor.expect("opened run descriptor"),
                    name: b"instructions.md".as_slice().into(),
                    directory: false,
                },
                now,
                below,
            );
        } else if let Some(run) = self.files.run {
            self.file(Step::CloseRun, file::Request::Close { owner: self.token(5), file: run }, now, below);
        } else if self.files.remove_run {
            self.file(
                Step::RemoveRun,
                file::Request::Remove {
                    owner: self.token(5),
                    root: self.files.runs_descriptor.expect("opened runs descriptor"),
                    name: self.names.run.as_slice().into(),
                    directory: true,
                },
                now,
                below,
            );
        } else if let Some(runs) = self.files.runs {
            self.file(Step::CloseRuns, file::Request::Close { owner: self.token(5), file: runs }, now, below);
        } else {
            self.file(Step::CloseRoot, file::Request::Close { owner: self.token(5), file: root.file }, now, below);
        }
    }
}

pub(super) fn mcp_configuration(relay_path: &[u8], server_port: u16, secret: &[u8]) -> Box<[u8]> {
    let relay = json::quote(relay_path, 32_768).expect("validated relay path");
    let port = Decimal::of(u64::from(server_port));
    json::join(&[
        b"{\"mcpServers\":{\"smith\":{\"type\":\"stdio\",\"command\":",
        &relay,
        b",\"args\":[\"--mcp-relay\",\"127.0.0.1:",
        port.as_bytes(),
        b"\"],\"env\":{\"SMITH_MCP_SECRET\":\"",
        secret,
        b"\"}}}}",
    ])
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
