//! One stdio MCP child for protocol/programs.md, sections 5 and 8. Keeps
//! framing and request IDs, never credentials or observations. The driver
//! routes owned I/O events here; notices settle startup or the current call.

use crate::output::Output;
use serde_json::{Value, json};
use skein_io::{
    Event, Request,
    kernel::{Fd, Pipe, Spawn, Way},
};
use skein_lib::{
    Queue, Token,
    stream::{Delimiter, Down, Read, Up},
};
use std::collections::{BTreeMap, VecDeque};

const OWNER: Token = Token::new(100);

#[derive(Debug)]
pub(crate) enum Notice {
    Settled { connected: bool, tools: Vec<String> },
    Reply(Result<Value, String>),
}

#[derive(Debug)]
enum Pending {
    Initialize,
    List,
    Call,
}

#[derive(Debug)]
pub(crate) struct Mcp {
    child: Option<Token>,
    input: Option<Token>,
    output: Output,
    bytes: Vec<u8>,
    sequence: u64,
    pending: Option<(u64, Pending)>,
    spawning: bool,
    settled: bool,
    connected: bool,
    ending: bool,
    pub notices: VecDeque<Notice>,
}

impl Mcp {
    pub fn new() -> Self {
        Self {
            child: None,
            input: None,
            output: Output::new(None),
            bytes: Vec::new(),
            sequence: 0,
            pending: None,
            spawning: false,
            settled: false,
            connected: false,
            ending: false,
            notices: VecDeque::new(),
        }
    }

    pub fn start(
        &mut self,
        configuration: Value,
        root: Fd,
        directory: &[u8],
        environment: &[Box<[u8]>],
        requests: &mut Queue<Request>,
    ) {
        let mut env: BTreeMap<Vec<u8>, Vec<u8>> = environment
            .iter()
            .filter_map(|entry| {
                let split = entry.iter().position(|byte| *byte == b'=')?;
                Some((entry[..split].to_vec(), entry[split + 1..].to_vec()))
            })
            .collect();
        if let Some(extra) = configuration["env"].as_object() {
            for (name, value) in extra {
                env.insert(name.as_bytes().to_vec(), value.as_str().expect("checked environment").as_bytes().to_vec());
            }
        }
        self.spawning = true;
        requests.push(Request::Spawn {
            owner: OWNER,
            spawn: Spawn {
                program: configuration["command"].as_str().expect("checked command").as_bytes().into(),
                args: configuration["args"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|argument| argument.as_str().expect("checked argument").as_bytes().into())
                    .collect(),
                env: env
                    .into_iter()
                    .map(|(name, value)| [name.as_slice(), b"=", value.as_slice()].concat().into_boxed_slice())
                    .collect(),
                root,
                dir: directory.into(),
                pipes: vec![
                    Pipe { child: 0, way: Way::In, parent: None },
                    Pipe { child: 1, way: Way::Out, parent: None },
                ]
                .into_boxed_slice(),
            },
        });
    }

    pub fn owns(&self, event: &Event) -> bool {
        match event {
            Event::Spawned { owner, .. }
            | Event::Exited { owner, .. }
            | Event::Failed { owner, .. }
            | Event::Closed { owner } => {
                *owner == OWNER || Some(*owner) == self.input || Some(*owner) == self.output.token()
            }
            Event::Stream { owner, .. } => Some(*owner) == self.input || Some(*owner) == self.output.token(),
            _ => false,
        }
    }

    pub fn event(&mut self, event: Event, requests: &mut Queue<Request>) {
        match event {
            Event::Spawned { child, pipes, .. } => {
                self.spawning = false;
                self.child = Some(child);
                self.output = Output::new(Some(pipes[0]));
                self.input = Some(pipes[1]);
                if !self.ending {
                    self.request("initialize", json!({"protocolVersion":"2025-06-18", "capabilities":{}, "clientInfo":{"name":"fake-claude", "version":"1"}}), Pending::Initialize);
                }
                self.read(requests);
            }
            Event::Stream { owner, up } if Some(owner) == self.output.token() => {
                let failed = matches!(up, Up::Failed(_));
                self.output.event(up, requests);
                if failed {
                    self.failed();
                }
            }
            Event::Stream { up, .. } => match up {
                Up::Bytes(bytes) => {
                    self.bytes.extend_from_slice(&bytes);
                    if self.bytes.len() > super::fake::FILE_BYTES as usize {
                        self.failed();
                        self.bytes.clear();
                    } else if self.bytes.last() == Some(&b'\n') {
                        let value = serde_json::from_slice::<Value>(&self.bytes);
                        self.bytes.clear();
                        match value {
                            Ok(value) => self.message(value),
                            Err(_) => self.failed(),
                        }
                    }
                    self.read(requests);
                }
                Up::End | Up::Failed(_) => {
                    if let Some(input) = self.input {
                        requests.push(Request::Abort { entity: input });
                    }
                    self.failed();
                }
                Up::Room => {}
            },
            Event::Exited { .. } => {
                self.failed();
                if let Some(child) = self.child {
                    requests.push(Request::Close { entity: child });
                }
                // The relay's final response may still be in the pipe. Keep
                // the outstanding read, draining through EOF before closure.
            }
            Event::Failed { .. } => {
                self.spawning = false;
                self.failed();
            }
            Event::Closed { owner } if owner == OWNER => {
                self.child = None;
                self.spawning = false;
            }
            Event::Closed { owner } if Some(owner) == self.output.token() => self.output.closed(),
            Event::Closed { .. } => self.input = None,
            _ => {}
        }
    }

    fn read(&self, requests: &mut Queue<Request>) {
        if let Some(input) = self.input {
            requests.push(Request::Stream {
                stream: input,
                down: Down::Demand { read: Read::Scan { until: Delimiter::LF, max: crate::fake::FILE_BYTES }, room: 0 },
            });
        }
    }

    fn send(&mut self, value: Value) {
        let mut bytes = serde_json::to_vec(&value).expect("MCP JSON");
        bytes.push(b'\n');
        self.output.push(&bytes);
    }

    fn request(&mut self, method: &str, params: Value, pending: Pending) {
        self.sequence = self.sequence.checked_add(1).expect("MCP request ID fits");
        self.pending = Some((self.sequence, pending));
        self.send(json!({"jsonrpc":"2.0", "id":self.sequence, "method":method, "params":params}));
    }

    fn message(&mut self, value: Value) {
        if self.ending || self.pending.as_ref().is_none_or(|(id, _)| value["id"].as_u64() != Some(*id)) {
            return;
        }
        let (_, pending) = self.pending.take().expect("matching request");
        match pending {
            Pending::Initialize => {
                if value.get("error").is_some() || !value["result"].is_object() {
                    self.failed();
                    return;
                }
                self.send(json!({"jsonrpc":"2.0", "method":"notifications/initialized"}));
                self.request("tools/list", json!({}), Pending::List);
            }
            Pending::List => {
                let tools = value["result"]["tools"].as_array().and_then(|tools| {
                    let names = tools
                        .iter()
                        .map(|tool| tool["name"].as_str().map(str::to_owned))
                        .collect::<Option<Vec<_>>>()?;
                    // Claude Code drops the whole server's inventory if even one
                    // schema lacks the MCP-required top-level object type.
                    Some(
                        if tools
                            .iter()
                            .all(|tool| tool["inputSchema"].is_object() && tool["inputSchema"]["type"] == "object")
                        {
                            names
                        } else {
                            Vec::new()
                        },
                    )
                });
                match tools {
                    Some(tools) if value.get("error").is_none() => {
                        self.settled = true;
                        self.connected = true;
                        self.notices.push_back(Notice::Settled { connected: true, tools });
                    }
                    _ => self.failed(),
                }
            }
            Pending::Call => {
                let reply = match value.get("result") {
                    Some(result) if value.get("error").is_none() => Ok(result.clone()),
                    _ => Err(value["error"]["message"].as_str().unwrap_or("MCP call failed").into()),
                };
                self.notices.push_back(Notice::Reply(reply));
            }
        }
    }

    fn failed(&mut self) {
        if matches!(self.pending, Some((_, Pending::Call))) {
            self.notices.push_back(Notice::Reply(Err("MCP server ended".into())));
        }
        self.pending = None;
        self.connected = false;
        self.ending = true;
        if !self.settled {
            self.settled = true;
            self.notices.push_back(Notice::Settled { connected: false, tools: Vec::new() });
        }
    }

    pub fn call(&mut self, tool: String, input: Value) {
        if !self.connected {
            self.notices.push_back(Notice::Reply(Err("MCP server unavailable".into())));
        } else {
            self.request("tools/call", json!({"name":tool, "arguments":input}), Pending::Call);
        }
    }

    pub fn cancel(&mut self) {
        self.pending = None;
    }

    pub fn stop(&mut self) {
        self.ending = true;
        self.pending = None;
    }

    pub fn progress(&mut self, requests: &mut Queue<Request>) {
        self.output.progress(self.ending, requests);
    }

    pub fn is_empty(&self) -> bool {
        !self.spawning && self.child.is_none() && self.input.is_none() && self.output.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::{Mcp, Notice, Pending};
    use serde_json::json;
    use skein_io::{Event, Request};
    use skein_lib::{
        Queue, Token,
        stream::{Down, Held, Up},
    };

    // T1's measured relay tools/list wire size, including LF.
    const RELAY_LIST_BYTES: usize = 25_566;
    // One complete JSON line should require one delivery, regardless of its size.
    const MAX_DELIVERIES: usize = 1;

    #[test]
    fn a_relay_sized_tool_list_arrives_in_one_delivery() {
        let input = Token::new(1);
        let mut mcp = Mcp::new();
        mcp.input = Some(input);
        mcp.pending = Some((1, Pending::List));
        let mut value = json!({"jsonrpc":"2.0", "id":1, "result":{"tools":[{"name":"finish","inputSchema":{"type":"object"}}]}, "padding":""});
        let envelope = serde_json::to_vec(&value).expect("JSON").len() + 1;
        value["padding"] = json!("x".repeat(RELAY_LIST_BYTES - envelope));
        let mut bytes = serde_json::to_vec(&value).expect("JSON");
        bytes.push(b'\n');
        let mut stream = Held::new(bytes.into_boxed_slice());
        let mut requests = Queue::with_capacity(8);
        mcp.read(&mut requests);
        let mut deliveries = 0;
        while mcp.notices.is_empty() {
            let Request::Stream { down: Down::Demand { read, .. }, .. } = requests.pop().expect("read demand") else {
                panic!("MCP reads its response");
            };
            let up = stream.answer(read).expect("response available");
            assert!(matches!(up, Up::Bytes(_)), "a complete response is delivered");
            deliveries += 1;
            mcp.event(Event::Stream { owner: input, up }, &mut requests);
        }
        assert!(deliveries <= MAX_DELIVERIES, "tool-list deliveries: {deliveries}");
        assert!(
            matches!(mcp.notices.pop_front(), Some(Notice::Settled { connected: true, tools }) if tools == ["finish"])
        );
    }
}
