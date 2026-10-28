//! The scripted MCP peer for protocol/programs.md, sections 5 and 8. Keeps
//! a script and verdict cursor; knows no login. `served` and `server_main`
//! drive the same line machine, which settles on input EOF or termination.

use crate::output::Output;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use skein_io::{
    Event, Io, Request,
    kernel::{self, Complete, Fd, Spawn, Submit},
};
use skein_lib::{
    Env, Queue, Time, Token, Wall,
    stream::{Delimiter, Down, Read, Up},
};
use skein_shell::Host;
use skein_world::{HostedProgram, Inherited};

/// Script supplied by a test to the peer, ending at its client's EOF.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerScript {
    #[serde(default)]
    pub tools: Vec<Tool>,
    /// Test-supplied schemas by tool name; unlisted tools use an object schema.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub input_schemas: std::collections::BTreeMap<String, Value>,
    #[serde(default = "prompt")]
    pub prompt: String,
    #[serde(default)]
    pub verdicts: Vec<Verdict>,
    #[serde(default = "connects")]
    pub connects: bool,
}

fn prompt() -> String {
    "prompt".into()
}

fn connects() -> bool {
    true
}

impl Default for ServerScript {
    fn default() -> Self {
        Self {
            tools: Vec::new(),
            input_schemas: std::collections::BTreeMap::new(),
            prompt: prompt(),
            verdicts: Vec::new(),
            connects: true,
        }
    }
}

impl ServerScript {
    /// The peer's one command-line argument, with no filesystem configuration.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("server script JSON")
    }
}

/// A named tool offered to the client, answered from this script alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tool {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub answer: Answer,
}

/// A tool's scripted response to its client, including a held call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Answer {
    /// Returns this text as a successful tool result.
    Text(String),
    /// Returns this text as an error tool result.
    Error(String),
    /// Withholds the answer while continuing to read requests and EOF.
    Never,
}

impl Default for Answer {
    fn default() -> Self {
        Self::Text(String::new())
    }
}

/// A permission answer consumed in order; past the script, calls are allowed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Allows the client's input unchanged.
    #[default]
    Allow,
    /// Denies the call with this message for the model.
    Deny(String),
}

/// Scripted stdio server hosted by a world or the real kernel loop.
#[derive(Debug)]
pub struct Server {
    io: Io,
    script: ServerScript,
    verdict: usize,
    input: Token,
    signal: Token,
    output: Output,
    bytes: Vec<u8>,
    initialized: bool,
    ending: bool,
    calls: Vec<String>,
    completions: Queue<Complete>,
    submissions: Queue<Submit>,
    events: Queue<Event>,
    requests: Queue<Request>,
}

impl Server {
    fn new(script: ServerScript, input: Fd, output: Fd, signal: Fd) -> Self {
        let mut io = Io::new(&crate::fake::limits());
        let input = io.adopt_read_pipe(input).expect("server input");
        let output = Output::new(Some(io.adopt_write_pipe(output, None).expect("server output")));
        let signal = io.adopt_signals(signal).expect("server signals");
        let mut server = Self {
            io,
            script,
            verdict: 0,
            input,
            signal,
            output,
            bytes: Vec::new(),
            initialized: false,
            ending: false,
            calls: Vec::new(),
            completions: Queue::with_capacity(64),
            submissions: Queue::with_capacity(64),
            events: Queue::with_capacity(64),
            requests: Queue::with_capacity(64),
        };
        server.read();
        server
    }

    /// Tool names in wire order, for a test referee; arguments are not retained.
    pub fn calls(&self) -> &[String] {
        &self.calls
    }

    fn read(&mut self) {
        self.requests.push(Request::Stream {
            stream: self.input,
            down: Down::Demand { read: Read::Scan { until: Delimiter::LF, max: crate::fake::FILE_BYTES }, room: 0 },
        });
    }

    fn end(&mut self) {
        if !self.ending {
            self.ending = true;
            self.requests.push(Request::Abort { entity: self.input });
            self.requests.push(Request::Abort { entity: self.signal });
        }
    }

    fn message(&mut self, value: Value) -> Option<Value> {
        let id = value.get("id")?.clone();
        assert_eq!(value["jsonrpc"], "2.0");
        let result = match value["method"].as_str() {
            Some("initialize") => {
                assert_eq!(value["params"]["protocolVersion"], "2025-06-18");
                if !self.script.connects {
                    return None;
                }
                json!({"protocolVersion":"2025-06-18", "capabilities":{"tools":{}}, "serverInfo":{"name":"scripted", "version":"1"}})
            }
            Some("tools/list") => {
                assert!(self.initialized, "initialized notification precedes listing");
                let tools: Vec<_> = self
                    .script
                    .tools
                    .iter()
                    .map(|tool| tool.name.as_str())
                    .chain([self.script.prompt.as_str()])
                    .map(|name| json!({"name":name, "inputSchema":self.script.input_schemas.get(name).cloned().unwrap_or_else(|| json!({"type":"object"}))}))
                    .collect();
                json!({"tools":tools})
            }
            Some("tools/call") => {
                let tool = value["params"]["name"].as_str().expect("tool name");
                self.calls.push(tool.into());
                if tool == self.script.prompt {
                    let input = &value["params"]["arguments"];
                    assert!(
                        input["tool_name"].is_string()
                            && input["input"].is_object()
                            && input["tool_use_id"].is_string(),
                        "permission input shape"
                    );
                    let verdict = self.script.verdicts.get(self.verdict).cloned().unwrap_or_default();
                    self.verdict = self.verdict.checked_add(1).expect("verdict count fits");
                    let answer = match verdict {
                        Verdict::Allow => json!({"behavior":"allow", "updatedInput":input["input"]}),
                        Verdict::Deny(message) => json!({"behavior":"deny", "message":message}),
                    };
                    text(answer.to_string(), false)
                } else {
                    match self.script.tools.iter().find(|entry| entry.name == tool).map(|entry| &entry.answer) {
                        Some(Answer::Text(answer)) => text(answer.clone(), false),
                        Some(Answer::Error(answer)) => text(answer.clone(), true),
                        Some(Answer::Never) => return None,
                        None => text("unknown tool".into(), true),
                    }
                }
            }
            _ => return Some(json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32601, "message":"unknown method"}})),
        };
        Some(json!({"jsonrpc":"2.0", "id":id, "result":result}))
    }

    fn event(&mut self, event: Event) {
        match event {
            Event::Stream { owner, up } if Some(owner) == self.output.token() => {
                self.output.event(up, &mut self.requests)
            }
            Event::Closed { owner } if Some(owner) == self.output.token() => self.output.closed(),
            Event::Stream { owner, up } if owner == self.input && !self.ending => match up {
                Up::Bytes(bytes) => {
                    self.bytes.extend_from_slice(&bytes);
                    if self.bytes.len() > crate::fake::FILE_BYTES as usize {
                        self.end();
                    } else if self.bytes.last() == Some(&b'\n') {
                        let value: Value = serde_json::from_slice(&self.bytes).expect("MCP request JSON");
                        self.bytes.clear();
                        if value["method"] == "notifications/initialized" {
                            self.initialized = true;
                        }
                        if let Some(answer) = self.message(value) {
                            let mut bytes = serde_json::to_vec(&answer).expect("MCP answer JSON");
                            bytes.push(b'\n');
                            self.output.push(&bytes);
                        }
                    }
                    if !self.ending {
                        self.read();
                    }
                }
                Up::End | Up::Failed(_) => self.end(),
                Up::Room => {}
            },
            Event::Shutdown { .. } | Event::Failed { .. } => self.end(),
            _ => {}
        }
    }
}

pub(crate) fn text(answer: String, error: bool) -> Value {
    json!({"content":[{"type":"text", "text":answer}], "isError":error})
}

impl Host for Server {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: crate::fake::limits() };
        while self.io.is_ready() {
            skein_io::resume(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            skein_io::up(&mut self.io, &env, complete, &mut self.events, &mut self.submissions);
        }
        while self.io.is_due(now) {
            skein_io::fire(&mut self.io, &env, &mut self.events, &mut self.submissions);
        }
        while let Some(event) = self.events.pop() {
            self.event(event);
        }
        self.output.progress(self.ending, &mut self.requests);
        while let Some(request) = self.requests.pop() {
            skein_io::down(&mut self.io, &env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }
    fn completions(&mut self) -> &mut Queue<Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        self.io.is_ready() || !self.completions.is_empty() || !self.events.is_empty() || !self.requests.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.ending
            && self.io.is_empty()
            && self.output.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.is_empty().then_some(kernel::Exit::Code(0))
    }
    fn operations(&self) -> u32 {
        crate::fake::operations()
    }
}

/// Hosts the scripted peer under this program path; its one argument is JSON.
pub fn served(program: &[u8]) -> HostedProgram<Server> {
    HostedProgram { program: program.into(), make, instances: 1, operations: crate::fake::operations() }
}

fn make(spawn: &Spawn, inherited: &Inherited) -> Server {
    let pipe = |number| inherited.pipes.iter().find(|(child, _)| *child == number).expect("server pipe").1;
    let script =
        serde_json::from_slice(spawn.args.first().expect("server script argument")).expect("server script JSON");
    Server::new(script, pipe(0), pipe(1), inherited.signal)
}

/// Runs the scripted peer on the kernel; downstream binaries call this directly.
pub fn server_main() -> std::process::ExitCode {
    let script = match std::env::args().nth(1).and_then(|argument| serde_json::from_str(&argument).ok()) {
        Some(script) => script,
        None => return std::process::ExitCode::from(2),
    };
    let signal = match skein_shell::open_termination_signals() {
        Ok(signal) => signal,
        Err(_) => return std::process::ExitCode::FAILURE,
    };
    let mut server = Server::new(script, Fd::new(0), Fd::new(1), signal);
    let mut kernel = match skein_shell::Kernel::open(skein_shell::Config { operations: server.operations() }) {
        Ok(kernel) => kernel,
        Err(_) => return std::process::ExitCode::FAILURE,
    };
    match skein_shell::drive(&mut kernel, &skein_shell::Clock::new(), &mut server) {
        kernel::Exit::Code(code) => std::process::ExitCode::from(code),
        kernel::Exit::Signal(_) => std::process::ExitCode::FAILURE,
    }
}
