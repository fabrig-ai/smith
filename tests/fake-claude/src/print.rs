//! One print conversation (protocol/programs.md, sections 5 and 8). Keeps
//! parsed flags, session and request cursors, a system-cue selection and file
//! phase; matches cues without otherwise interpreting or retaining prompt text.
//! Session state survives a process resume.
//! The shared driver supplies file/input completions and service signals.

use std::collections::BTreeMap;

use serde_json::{Value, json};
use skein_io::file;

use crate::{
    Decision, End, LOGIN_FILE, Resume, SETTINGS_FILE, Script, Seen, Selection, Step, Story, Turn,
    commands::{Config, Effect, FILE_OWNER, error},
    stream,
};

#[derive(Debug)]
struct Flags {
    values: BTreeMap<String, String>,
    tools: Vec<String>,
    session: String,
}

impl Flags {
    fn parse(arguments: &[String]) -> Option<Self> {
        let mut values = BTreeMap::new();
        let mut arguments = arguments.iter();
        while let Some(flag) = arguments.next() {
            let value = match flag.as_str() {
                "-p" | "--verbose" | "--strict-mcp-config" | "--include-partial-messages" => String::new(),
                "--input-format"
                | "--output-format"
                | "--setting-sources"
                | "--model"
                | "--append-system-prompt-file"
                | "--mcp-config"
                | "--permission-prompt-tool"
                | "--tools"
                | "--session-id"
                | "--resume"
                | "--effort"
                | "--json-schema"
                | "--max-turns" => arguments.next()?.clone(),
                _ => return None,
            };
            if value.starts_with('-')
                || (value.is_empty()
                    && !matches!(
                        flag.as_str(),
                        "-p" | "--verbose" | "--strict-mcp-config" | "--include-partial-messages" | "--tools"
                    ))
                || values.insert(flag.clone(), value).is_some()
            {
                return None;
            }
        }
        for flag in ["-p", "--verbose", "--model", "--append-system-prompt-file", "--tools"] {
            values.get(flag)?;
        }
        for (flag, value) in
            [("--input-format", "stream-json"), ("--output-format", "stream-json"), ("--setting-sources", "user")]
        {
            if values.get(flag)?.as_str() != value {
                return None;
            }
        }
        if values.contains_key("--session-id") == values.contains_key("--resume") {
            return None;
        }
        let session = values.get("--session-id").or_else(|| values.get("--resume"))?.clone();
        if session.len() != 36
            || !session.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) { byte == b'-' } else { byte.is_ascii_hexdigit() }
            })
        {
            return None;
        }
        if let Some(turns) = values.get("--max-turns")
            && (!turns.bytes().all(|byte| byte.is_ascii_digit()) || turns.parse::<u64>().ok()? == 0)
        {
            return None;
        }
        if values.contains_key("--mcp-config") {
            values.get("--strict-mcp-config")?;
            let (server, tool) = values.get("--permission-prompt-tool")?.strip_prefix("mcp__")?.split_once("__")?;
            if server.is_empty() || tool.is_empty() {
                return None;
            }
        } else if values.get("--tools")?.as_str() != ""
            || values.contains_key("--permission-prompt-tool")
            || values.contains_key("--json-schema")
            || values.contains_key("--resume")
            || values.get("--max-turns")?.as_str() != "1"
        {
            return None;
        }
        if let Some(schema) = values.get("--json-schema") {
            serde_json::from_str::<Value>(schema).ok()?;
        }
        let list = values.get("--tools")?;
        let tools: Vec<String> =
            if list.is_empty() { Vec::new() } else { list.split(',').map(str::to_owned).collect() };
        for (index, tool) in tools.iter().enumerate() {
            if !["Read", "Glob", "Grep", "Write", "Edit", "Bash"].contains(&tool.as_str())
                || tools[..index].contains(tool)
            {
                return None;
            }
        }
        Some(Self { values, tools, session })
    }
}

#[derive(Debug)]
enum Phase {
    Session,
    Prompt,
    Saving,
    Configuration,
    Settings,
    Connecting,
    Calling,
    Acting,
    Idle,
    Login,
    Removing,
    Hanging,
    Deaf,
    Done,
}

#[derive(Debug)]
pub(crate) struct Print {
    flags: Flags,
    story: Story,
    config: Config,
    phase: Phase,
    server: String,
    init: Option<Value>,
    input: Vec<u8>,
    session: Session,
    next_turn: usize,
    cued: Option<usize>,
    turn: Turn,
    step: usize,
    texts: u64,
    last_text: String,
    interrupted: bool,
    cancelling: bool,
    server_config: Value,
    pending: Option<ToolCall>,
    acting: Option<crate::act::Act>,
    directory: std::path::PathBuf,
    next_tool: u64,
    expected_reply: Option<String>,
    together: std::collections::VecDeque<(ToolCall, Effect)>,
}

impl Print {
    pub(crate) fn active_turn(&self) -> Option<u32> {
        matches!(self.phase, Phase::Calling | Phase::Hanging | Phase::Deaf)
            .then(|| u32::try_from(self.next_turn).expect("scripted turn count fits"))
    }

    pub(crate) fn pending_tool(&self) -> Option<&str> {
        self.pending.as_ref().map(|call| call.tool.as_str())
    }

    pub fn start(
        arguments: &[String],
        story: Story,
        config: Config,
        directory: &std::path::Path,
    ) -> Result<(Self, Vec<Effect>), Vec<Effect>> {
        let flags = Flags::parse(arguments).ok_or_else(|| error("unknown command or flag", 2))?;
        if flags.values.contains_key("--resume") && story.resume == Resume::Missing {
            return Err(vec![
                Effect::Error(format!("No conversation found with session ID: {}\n", flags.session).into_bytes()),
                Effect::End(1),
            ]);
        }
        let mut state = Self {
            flags,
            story,
            config,
            phase: Phase::Session,
            server: String::new(),
            init: None,
            input: Vec::new(),
            session: Session::default(),
            next_turn: 0,
            cued: None,
            turn: Turn::default(),
            step: 0,
            texts: 0,
            last_text: String::new(),
            interrupted: false,
            cancelling: false,
            server_config: Value::Null,
            pending: None,
            acting: None,
            directory: directory.to_path_buf(),
            next_tool: 0,
            expected_reply: None,
            together: std::collections::VecDeque::new(),
        };
        let effects = match config.root {
            Some(root) => vec![load(root, &state.session_file())],
            None => state.prompt(),
        };
        Ok((state, effects))
    }

    fn session_file(&self) -> String {
        format!("fake-claude.session.{}", self.flags.session)
    }

    fn prompt(&mut self) -> Vec<Effect> {
        if self.story.cued.is_empty() {
            return self.begin();
        }
        let Some(root) = self.config.mcp else { return error("cannot read system prompt", 1) };
        self.phase = Phase::Prompt;
        vec![load(root, &self.flags.values["--append-system-prompt-file"])]
    }

    fn begin(&mut self) -> Vec<Effect> {
        if !self.flags.values.contains_key("--mcp-config") {
            self.phase = Phase::Idle;
            let mut init = stream::init(&self.story.init, &[], "", &self.flags.values["--model"]);
            init["mcp_servers"] = json!([]);
            self.init = Some(init.clone());
            vec![self.emit(init), self.emit(stream::status()), Effect::ReadCode]
        } else {
            let Some(root) = self.config.mcp else { return error("cannot read MCP configuration", 1) };
            self.phase = Phase::Configuration;
            vec![load(root, &self.flags.values["--mcp-config"])]
        }
    }

    fn login(&mut self) -> Vec<Effect> {
        self.phase = Phase::Login;
        match self.config.root {
            Some(root) => vec![load(root, LOGIN_FILE)],
            None => self.finish(stream::failure(Some(401), None, vec!["not signed in".into()])),
        }
    }

    fn emit(&self, value: Value) -> Effect {
        Effect::Output(stream::line(value, &self.flags.session))
    }

    pub fn file(&mut self, event: file::Event) -> Vec<Effect> {
        match self.phase {
            Phase::Session => {
                match event {
                    file::Event::Loaded { .. } if self.flags.values.contains_key("--session-id") => {
                        return vec![
                            Effect::Error(
                                format!("Error: Session ID {} is already in use.\n", self.flags.session).into_bytes(),
                            ),
                            Effect::End(1),
                        ];
                    }
                    file::Event::Loaded { bytes, .. } => {
                        // The original marker meant only that this ID was used.
                        self.session = if bytes.len() == 1 {
                            Session::default()
                        } else {
                            match serde_json::from_slice(&bytes) {
                                Ok(session) => session,
                                Err(_) => return error("invalid session state", 1),
                            }
                        };
                    }
                    file::Event::Failed { error: skein_io::kernel::Error::NotFound, .. } => {}
                    _ => return error("cannot read session marker", 1),
                }
                self.prompt()
            }
            Phase::Prompt => match event {
                file::Event::Loaded { bytes, .. } => {
                    self.cued = select(&self.story.cued, &[&bytes]);
                    self.begin()
                }
                _ => error("cannot read system prompt", 1),
            },
            Phase::Saving => match event {
                file::Event::Stored { .. } => {
                    if self.interrupted {
                        self.interrupt_result()
                    } else {
                        self.login()
                    }
                }
                _ => error("cannot record session", 1),
            },
            Phase::Configuration => {
                let bytes = match event {
                    file::Event::Loaded { bytes, .. } => bytes,
                    _ => return error("cannot read MCP configuration", 1),
                };
                let Some((server, configuration)) =
                    configuration(&bytes, &self.flags.values["--permission-prompt-tool"])
                else {
                    return error("invalid MCP configuration", 1);
                };
                self.server = server;
                self.server_config = configuration;
                self.phase = Phase::Settings;
                match self.config.root {
                    Some(root) => vec![load(root, SETTINGS_FILE)],
                    None => self.initialized(Seen::UnreadableSettings),
                }
            }
            Phase::Settings => {
                let seen = match event {
                    file::Event::Loaded { bytes, .. } => settings(&bytes),
                    _ => Seen::UnreadableSettings,
                };
                self.initialized(seen)
            }
            Phase::Login => {
                if self.interrupted {
                    return self.interrupt_result();
                }
                match event {
                    file::Event::Loaded { .. } => self.steps(),
                    _ => self.finish(stream::failure(Some(401), None, vec!["not signed in".into()])),
                }
            }
            Phase::Acting => {
                let progress = self.acting.as_mut().expect("active built-in operation").file(event);
                match progress {
                    crate::act::Progress::Request(request) => vec![Effect::File(request)],
                    crate::act::Progress::Done(outcome) => {
                        let path = self.acting.take().expect("completed operation").path;
                        self.acted(path, outcome)
                    }
                }
            }
            Phase::Removing => {
                if self.interrupted {
                    self.interrupt_result()
                } else {
                    self.steps()
                }
            }
            Phase::Idle | Phase::Hanging | Phase::Deaf | Phase::Connecting | Phase::Calling | Phase::Done => Vec::new(),
        }
    }

    fn initialized(&mut self, seen: Seen) -> Vec<Effect> {
        self.phase = Phase::Connecting;
        let directory = std::path::Path::new(&self.flags.values["--mcp-config"])
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(std::path::Path::new("."))
            .to_string_lossy()
            .into_owned();
        vec![Effect::Seen(seen), Effect::McpStart { configuration: self.server_config.take(), directory }]
    }

    pub fn input(&mut self, bytes: Option<&[u8]>) -> Vec<Effect> {
        match bytes {
            None if matches!(
                self.phase,
                Phase::Saving
                    | Phase::Login
                    | Phase::Removing
                    | Phase::Hanging
                    | Phase::Deaf
                    | Phase::Calling
                    | Phase::Acting
                    | Phase::Connecting
            ) =>
            {
                Vec::new()
            }
            None if self.input.is_empty() => {
                self.phase = Phase::Done;
                vec![Effect::End(0)]
            }
            None => error("incomplete input line", 1),
            Some(bytes) => {
                let remaining =
                    (super::fake::FILE_BYTES as usize).checked_sub(self.input.len()).expect("bounded input");
                if bytes.len() > remaining {
                    return error("input line too long", 1);
                }
                self.input.extend_from_slice(bytes);
                if self.input.last() != Some(&b'\n') {
                    return vec![Effect::ReadCode];
                }
                let value = serde_json::from_slice::<Value>(&self.input).ok();
                if let Some(value) = value
                    && value["type"] == "control_request"
                    && value["request"]["subtype"] == "interrupt"
                    && value["request_id"].is_string()
                {
                    self.input.clear();
                    let mut response = serde_json::to_vec(&json!({"type":"control_response", "response":{
                        "subtype":"success", "request_id":value["request_id"], "response":{"still_queued":[]}
                    }}))
                    .expect("control response JSON");
                    response.push(b'\n');
                    let mut effects = vec![Effect::Output(response), Effect::Seen(Seen::Interrupted)];
                    effects.extend(self.interrupt_turn());
                    effects.push(Effect::ReadCode);
                    return effects;
                }
                if !stream::user(&self.input) {
                    return error("invalid user message", 1);
                }
                let received: Value = serde_json::from_slice(&self.input).expect("validated user message");
                self.input.clear();
                let selection = self.select_turn(&received);
                self.session.messages = self.session.messages.checked_add(1).expect("session count fits");
                self.next_turn = self.next_turn.checked_add(1).expect("turn count fits");
                self.step = 0;
                self.texts = 0;
                self.last_text.clear();
                self.interrupted = false;
                let mut effects = vec![Effect::Received(received)];
                if self.next_turn > 1 {
                    effects.push(self.emit(self.init.clone().expect("initialized conversation")));
                    effects.push(self.emit(stream::status()));
                }
                if let Some(selection) = selection {
                    effects.push(Effect::Seen(selection));
                }
                if let Some(root) = self.config.root {
                    self.phase = Phase::Saving;
                    effects.push(Effect::File(file::Request::Store {
                        owner: FILE_OWNER,
                        root,
                        path: self.session_file().as_bytes().into(),
                        bytes: serde_json::to_vec(&self.session).expect("session is JSON data").into(),
                        expected: file::Expect::Any,
                        no_follow: true,
                    }));
                } else {
                    effects.extend(self.login());
                }
                effects
            }
        }
    }

    fn select_turn(&mut self, received: &Value) -> Option<Seen> {
        if !self.flags.values.contains_key("--mcp-config") {
            self.turn = Turn { steps: vec![Step::Text(self.story.summary.clone())], ..Turn::default() };
            return None;
        }
        let content = &received["message"]["content"];
        let parts: Vec<&[u8]> = match content {
            Value::String(text) => vec![text.as_bytes()],
            Value::Array(blocks) => {
                blocks.iter().filter_map(|block| block["text"].as_str().map(str::as_bytes)).collect()
            }
            _ => unreachable!("validated user content"),
        };
        if let Some(index) = select(&self.story.asked, &parts) {
            self.session.asked = Some(RequestCue { index, start: self.session.messages });
        }
        let (script, turns, turn) = if let Some(index) = self.cued {
            (Selection::Cued(index), &self.story.cued[index].turns[..], self.session.messages)
        } else if let Some(request) = &self.session.asked {
            let turns = self.story.asked.get(request.index).map(|script| script.turns.as_slice()).unwrap_or_default();
            (
                Selection::Asked(request.index),
                turns,
                self.session.messages.checked_sub(request.start).expect("request precedes turn"),
            )
        } else {
            // Legacy stories restarted their positional turns in each process.
            (Selection::Otherwise, &self.story.turns[..], self.next_turn)
        };
        self.turn = turns.get(turn).cloned().unwrap_or_default();
        Some(Seen::Selected { script, turn })
    }

    fn steps(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        while let Some(step) = self.turn.steps.get(self.step).cloned() {
            self.step = self.step.checked_add(1).expect("step count fits");
            match step {
                Step::Compact => effects.push(self.emit(json!({"type":"system", "subtype":"compact_boundary"}))),
                Step::Text(text) => {
                    if self.texts > 0 {
                        effects.push(self.emit(stream::status()));
                    }
                    self.texts = self.texts.checked_add(1).expect("text count fits");
                    let id = self
                        .session.messages
                        .checked_mul(super::fake::FILE_BYTES as usize)
                        .and_then(|base| base.checked_add(self.step))
                        .expect("message id fits");
                    let assistant = stream::assistant(&text, &self.flags.values["--model"], id, &self.story.usage);
                    if self.flags.values.contains_key("--include-partial-messages") {
                        for (index, value) in stream::partials(&text, &self.flags.values["--model"], id).into_iter().enumerate() {
                            // The complete assistant precedes the block/message stops.
                            if index == 3 {
                                effects.push(self.emit(assistant.clone()));
                            }
                            effects.push(self.emit(value));
                        }
                    } else {
                        effects.push(self.emit(assistant));
                    }
                    self.last_text = text;
                }
                Step::Partial(text) => {
                    effects.extend(stream::partials(&text, &self.flags.values["--model"], self.step)
                        .into_iter().take(3).map(|value| self.emit(value)));
                }
                Step::RateLimit { resets_at } => {
                    let mut value = json!({"type":"rate_limit_event", "rate_limit_info":{"status":"rejected"}});
                    if let Some(reset) = resets_at {
                        value["rate_limit_info"]["resetsAt"] = json!(reset);
                    }
                    effects.push(self.emit(value));
                }
                Step::Retry { attempt, status, delay_ms } => {
                    effects.push(self.emit(
                        json!({"type":"system", "subtype":"api_retry", "attempt":attempt, "error_status":status,"retry_delay_ms":delay_ms}),
                    ))
                }
                Step::LoseLogin => {
                    if let Some(root) = self.config.directory {
                        self.phase = Phase::Removing;
                        effects.push(Effect::File(file::Request::Remove {
                            owner: FILE_OWNER,
                            root,
                            name: LOGIN_FILE.as_bytes().into(),
                            directory: false,
                        }));
                        return effects;
                    }
                }
                Step::Call { tool, input } => {
                    effects.extend(self.tool(tool, input, ToolAction::Server));
                    return effects;
                }
                Step::CallChecked { tool, input, contains } => {
                    self.expected_reply = Some(contains);
                    effects.extend(self.tool(tool, input, ToolAction::Server));
                    return effects;
                }
                Step::Builtin { tool, input, result } => {
                    effects.extend(self.tool(tool, input, ToolAction::Scripted(result)));
                    return effects;
                }
                Step::Act { tool, input } => {
                    effects.extend(self.tool(tool, input, ToolAction::Act));
                    return effects;
                }
                Step::Together(steps) => {
                    effects.extend(self.together(steps));
                    return effects;
                }
                Step::Deaf => {
                    self.phase = Phase::Deaf;
                    effects.push(Effect::ReadCode);
                    return effects;
                }
                Step::Hang => {
                    self.phase = Phase::Hanging;
                    effects.push(Effect::ReadCode);
                    return effects;
                }
            }
        }
        match self.turn.end.clone() {
            End::Success { structured_output } => {
                let mut value = json!({"type":"result", "subtype":"success", "is_error":false, "terminal_reason":"completed", "stop_reason":"end_turn", "result":self.last_text, "usage":stream::usage(&self.story.usage, self.texts)});
                if self.flags.values.contains_key("--json-schema")
                    && let Some(output) = structured_output
                {
                    value["structured_output"] = output;
                }
                effects.extend(self.finish(value));
            }
            End::Error { api_error_status, terminal_reason, errors, resets_at } => {
                let mut value = stream::failure(api_error_status, terminal_reason, errors);
                if let Some(reset) = resets_at {
                    value["rate_limit_info"] = json!({"resetsAt":reset});
                }
                effects.extend(self.finish(value))
            }
            End::Exit(code) => effects.push(Effect::End(code)),
            End::Undecodable => effects.extend([Effect::Output(b"not json\n".to_vec()), Effect::End(1)]),
        }
        effects
    }

    fn tool(&mut self, tool: String, input: Value, action: ToolAction) -> Vec<Effect> {
        if matches!(action, ToolAction::Act) && !["Read", "Write", "Edit"].contains(&tool.as_str()) {
            return error("Act requires Read, Write or Edit", 1);
        }
        let builtin = action.builtin();
        self.next_tool = self.next_tool.checked_add(1).expect("tool ID fits");
        self.texts = self.texts.checked_add(1).expect("message count fits");
        let id = format!("toolu_{}", self.next_tool);
        let bare = tool.strip_prefix(&format!("mcp__{}__", self.server)).unwrap_or(&tool).to_owned();
        let name = if builtin { tool.clone() } else { format!("mcp__{}__{}", self.server, bare) };
        let message_id = self
            .session
            .messages
            .checked_mul(super::fake::FILE_BYTES as usize)
            .and_then(|base| base.checked_add(self.step))
            .expect("message ID fits");
        let mut assistant = stream::assistant("", &self.flags.values["--model"], message_id, &self.story.usage);
        assistant["message"]["content"] = json!([{"type":"tool_use", "id":id, "name":name, "input":input}]);
        let (call, arguments) = if builtin {
            (
                self.flags.values["--permission-prompt-tool"]
                    .strip_prefix(&format!("mcp__{}__", self.server))
                    .expect("checked prompt tool")
                    .to_owned(),
                json!({"tool_name":tool,"input":input,"tool_use_id":id}),
            )
        } else {
            (bare.clone(), input.clone())
        };
        self.pending = Some(ToolCall { id, tool: if builtin { tool } else { bare }, input, action });
        self.phase = Phase::Calling;
        let mut effects = Vec::new();
        if self.texts > 1 {
            effects.push(self.emit(stream::status()));
        }
        if self.flags.values.contains_key("--include-partial-messages") {
            let message = &assistant["message"];
            for (index, event) in [
                json!({"type":"message_start","message":{"id":message["id"],"role":"assistant","content":[]}}),
                json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":message["content"][0]["id"],"name":message["content"][0]["name"],"input":{}}}),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":serde_json::to_string(&message["content"][0]["input"]).expect("tool input JSON")}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":stream::usage(&self.story.usage,1)}),
                json!({"type":"message_stop"}),
            ].into_iter().enumerate() {
                if index == 3 {
                    effects.push(self.emit(assistant.clone()));
                }
                effects.push(self.emit(json!({"type":"stream_event","event":event})));
            }
        } else {
            effects.push(self.emit(assistant));
        }
        effects.extend([self.emit(assistant), Effect::McpCall { tool: call, input: arguments }, Effect::ReadCode]);
        effects
    }

    pub fn mcp(&mut self, notice: crate::mcp::Notice) -> Vec<Effect> {
        match notice {
            crate::mcp::Notice::Settled { connected, tools } => {
                if !matches!(self.phase, Phase::Connecting) {
                    return Vec::new();
                }
                self.phase = Phase::Idle;
                let mut init =
                    stream::init(&self.story.init, &self.flags.tools, &self.server, &self.flags.values["--model"]);
                init["mcp_servers"][0]["status"] = json!(if connected { "connected" } else { "failed" });
                let mut all_tools = self.flags.tools.clone();
                let mut effects = if self.story.observe_tools {
                    vec![Effect::Seen(Seen::Tools { names: tools.clone() })]
                } else {
                    Vec::new()
                };
                all_tools.extend(
                    tools
                        .into_iter()
                        .map(|tool| format!("mcp__{}__{tool}", self.server))
                        .filter(|tool| tool != &self.flags.values["--permission-prompt-tool"]),
                );
                all_tools.extend(self.story.init.extra_tools.clone());
                init["tools"] = json!(all_tools);
                self.init = Some(init.clone());
                effects.extend(match self.story.init.delay_ms {
                    Some(after_ms) => {
                        let mut bytes = stream::line(init, &self.flags.session);
                        bytes.extend(stream::line(stream::status(), &self.flags.session));
                        vec![Effect::DelayedInit { after_ms, bytes }]
                    }
                    None => vec![self.emit(init), self.emit(stream::status()), Effect::ReadCode],
                });
                effects
            }
            crate::mcp::Notice::Reply(reply) => {
                let Some(pending) = self.pending.take() else { return Vec::new() };
                let (text, mut failed) = match reply {
                    Ok(reply) => match reply["content"].as_array().and_then(|blocks| {
                        blocks.iter().map(|block| block["text"].as_str()).collect::<Option<Vec<_>>>()
                    }) {
                        Some(blocks) => (blocks.join("\n"), reply["isError"].as_bool().unwrap_or(false)),
                        None => ("unreadable MCP answer".into(), true),
                    },
                    Err(error) => (error, true),
                };
                if let Some(expected) = self.expected_reply.take() {
                    failed |= !text.contains(&expected);
                }
                let mut effects = Vec::new();
                let text = match &pending.action {
                    ToolAction::Server => {
                        effects.push(Effect::Seen(Seen::Called { tool: pending.tool.clone(), error: failed }));
                        text
                    }
                    ToolAction::Scripted(_) | ToolAction::Act => {
                        let decision = if failed { Decision::Unreadable } else { decision(&text, &pending.input) };
                        effects.push(Effect::Seen(Seen::Prompted {
                            tool: pending.tool.clone(),
                            decision: decision.clone(),
                        }));
                        match decision {
                            Decision::Allow { .. } => match &pending.action {
                                ToolAction::Scripted(result) => result.clone(),
                                ToolAction::Act => {
                                    let path = pending.input["file_path"].as_str().unwrap_or("").to_owned();
                                    let started = crate::act::Act::start(
                                        &pending.tool,
                                        &pending.input,
                                        self.config.mcp.expect("MCP invocation has its file root"),
                                        &self.directory,
                                    );
                                    self.pending = Some(pending);
                                    self.phase = Phase::Acting;
                                    match started {
                                        Ok((act, request)) => {
                                            self.acting = Some(act);
                                            effects.push(Effect::File(request));
                                        }
                                        Err(outcome) => effects.extend(self.acted(path, outcome)),
                                    }
                                    return effects;
                                }
                                ToolAction::Server => unreachable!("server calls do not ask permission"),
                            },
                            Decision::Deny(message) => {
                                failed = true;
                                message
                            }
                            Decision::Unreadable => {
                                failed = true;
                                "unreadable permission answer".into()
                            }
                        }
                    }
                };
                effects.extend(self.tool_result(pending.id, text, failed));
                effects
            }
        }
    }

    fn acted(&mut self, path: String, outcome: crate::act::Outcome) -> Vec<Effect> {
        let pending = self.pending.take().expect("acted built-in call");
        let mut effects = vec![Effect::Seen(Seen::Acted { tool: pending.tool, path, error: outcome.error })];
        if self.interrupted {
            effects.extend(self.interrupt_result());
        } else {
            effects.extend(self.tool_result(pending.id, outcome.text, outcome.error));
        }
        effects
    }

    fn tool_result(&mut self, id: String, text: String, failed: bool) -> Vec<Effect> {
        let mut effects = vec![self.emit(json!({"type":"user", "message":{"role":"user", "content":[{"type":"tool_result", "tool_use_id":id, "content":text, "is_error":failed}]}}))];
        if let Some((pending, call)) = self.together.pop_front() {
            self.pending = Some(pending);
            self.phase = Phase::Calling;
            effects.push(call);
        } else {
            effects.extend(self.steps());
        }
        effects
    }

    fn together(&mut self, steps: Vec<Step>) -> Vec<Effect> {
        let mut blocks = Vec::new();
        for step in steps {
            let effects = match step {
                Step::Call { tool, input } => self.tool(tool, input, ToolAction::Server),
                Step::Builtin { tool, input, result } => self.tool(tool, input, ToolAction::Scripted(result)),
                Step::Act { tool, input } => {
                    if !["Read", "Write", "Edit"].contains(&tool.as_str()) {
                        return error("Act requires Read, Write or Edit", 1);
                    }
                    self.tool(tool, input, ToolAction::Act)
                }
                _ => return error("Together requires Call, Builtin or Act", 1),
            };
            let pending = self.pending.take().expect("tool prepared");
            let name = if pending.action.builtin() {
                pending.tool.clone()
            } else {
                format!("mcp__{}__{}", self.server, pending.tool)
            };
            blocks.push(json!({"type":"tool_use","id":pending.id,"name":name,"input":pending.input}));
            let call = effects.into_iter().find(|effect| matches!(effect, Effect::McpCall { .. })).expect("tool RPC");
            self.together.push_back((pending, call));
        }
        if blocks.is_empty() {
            return self.steps();
        }
        // The group is one API message, regardless of its number of calls.
        self.texts = self
            .texts
            .checked_sub(u64::try_from(blocks.len()).expect("group size"))
            .and_then(|count| count.checked_add(1))
            .expect("message count");
        let mut assistant = stream::assistant(
            "",
            &self.flags.values["--model"],
            usize::try_from(self.next_tool).expect("message ID fits"),
            &self.story.usage,
        );
        assistant["message"]["content"] = json!(blocks);
        let (pending, call) = self.together.pop_front().expect("nonempty group");
        self.pending = Some(pending);
        self.phase = Phase::Calling;
        vec![self.emit(assistant), call, Effect::ReadCode]
    }

    fn finish(&mut self, value: Value) -> Vec<Effect> {
        self.phase = Phase::Idle;
        vec![self.emit(value), Effect::ReadCode]
    }

    fn interrupt_result(&mut self) -> Vec<Effect> {
        self.interrupted = false;
        self.together.clear();
        self.expected_reply = None;
        let mut result = stream::failure(None, Some("aborted_streaming".into()), Vec::new());
        result["stop_reason"] = Value::Null;
        if self.cancelling {
            self.phase = Phase::Done;
            vec![self.emit(result), Effect::End(0)]
        } else {
            self.finish(result)
        }
    }

    pub fn stop(&mut self) -> Vec<Effect> {
        match self.pending.take() {
            Some(pending) => {
                if let Some(act) = self.acting.take() {
                    return vec![Effect::Seen(Seen::Acted { tool: pending.tool, path: act.path, error: true })];
                }
                let seen = if pending.action.builtin() {
                    Seen::Prompted { tool: pending.tool, decision: Decision::Unreadable }
                } else {
                    Seen::Called { tool: pending.tool, error: true }
                };
                vec![Effect::Seen(seen)]
            }
            None => Vec::new(),
        }
    }

    pub fn interrupt(&mut self) -> Vec<Effect> {
        self.cancelling = true;
        if matches!(self.phase, Phase::Idle | Phase::Done) {
            self.phase = Phase::Done;
            return vec![Effect::End(0)];
        }
        self.interrupt_turn()
    }

    fn interrupt_turn(&mut self) -> Vec<Effect> {
        match self.phase {
            Phase::Hanging => self.interrupt_result(),
            Phase::Calling => {
                let mut effects = self.stop();
                effects.push(Effect::McpCancel);
                effects.extend(self.interrupt_result());
                effects
            }
            Phase::Saving | Phase::Login | Phase::Removing | Phase::Acting => {
                self.interrupted = true;
                Vec::new()
            }
            Phase::Session
            | Phase::Prompt
            | Phase::Configuration
            | Phase::Settings
            | Phase::Connecting
            | Phase::Idle
            | Phase::Deaf
            | Phase::Done => Vec::new(),
        }
    }
}

fn load(root: skein_lib::Token, name: &str) -> Effect {
    Effect::File(file::Request::Load {
        owner: FILE_OWNER,
        root,
        path: name.as_bytes().into(),
        max: super::fake::FILE_BYTES,
        no_follow: true,
    })
}

fn configuration(bytes: &[u8], prompt: &str) -> Option<(String, Value)> {
    let value: Value = serde_json::from_slice(bytes).ok()?;
    let servers = value["mcpServers"].as_object()?;
    if servers.len() != 1 {
        return None;
    }
    let (name, server) = servers.iter().next()?;
    if name.is_empty()
        || server.get("type").is_some_and(|kind| kind != "stdio")
        || server["command"].as_str()?.is_empty()
    {
        return None;
    }
    if let Some(args) = server.get("args")
        && !args.as_array()?.iter().all(Value::is_string)
    {
        return None;
    }
    if let Some(env) = server.get("env")
        && !env.as_object()?.values().all(Value::is_string)
    {
        return None;
    }
    if prompt.strip_prefix(&format!("mcp__{name}__"))?.is_empty() {
        return None;
    }
    Some((name.clone(), server.clone()))
}

fn settings(bytes: &[u8]) -> Seen {
    let Ok(Value::Object(value)) = serde_json::from_slice::<Value>(bytes) else { return Seen::UnreadableSettings };
    if value.get("permissions").is_some_and(|permissions| !permissions.is_object()) {
        return Seen::UnreadableSettings;
    }
    let rules = |name| -> Option<Vec<String>> {
        match value.get("permissions").and_then(|permissions| permissions.get(name)) {
            None => Some(Vec::new()),
            Some(rules) => rules.as_array()?.iter().map(|rule| rule.as_str().map(str::to_owned)).collect(),
        }
    };
    match (rules("allow"), rules("ask"), rules("deny")) {
        (Some(allow), Some(ask), Some(deny)) => {
            Seen::Settings { allow, ask, deny, keys: value.keys().cloned().collect() }
        }
        _ => Seen::UnreadableSettings,
    }
}

#[derive(Debug)]
struct ToolCall {
    id: String,
    tool: String,
    input: Value,
    action: ToolAction,
}

#[derive(Debug)]
enum ToolAction {
    Server,
    Scripted(String),
    Act,
}

impl ToolAction {
    fn builtin(&self) -> bool {
        match self {
            Self::Server => false,
            Self::Scripted(_) | Self::Act => true,
        }
    }
}

fn decision(text: &str, input: &Value) -> Decision {
    let Ok(value) = serde_json::from_str::<Value>(text) else { return Decision::Unreadable };
    match value["behavior"].as_str() {
        Some("allow") if value["updatedInput"].is_object() => {
            Decision::Allow { unchanged: value["updatedInput"] == *input }
        }
        Some("deny") => match value["message"].as_str() {
            Some(message) => Decision::Deny(message.into()),
            None => Decision::Unreadable,
        },
        _ => Decision::Unreadable,
    }
}

/// Only selection state is saved; message and prompt contents are not retained.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Session {
    messages: usize,
    asked: Option<RequestCue>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct RequestCue {
    index: usize,
    start: usize,
}

// Positions are ordered by text part, then byte offset, then script index.
fn select(scripts: &[Script], parts: &[&[u8]]) -> Option<usize> {
    parts.iter().find_map(|part| {
        scripts
            .iter()
            .enumerate()
            .filter_map(|(index, script)| {
                let cue = script.cue.as_bytes();
                let offset =
                    if cue.is_empty() { Some(0) } else { part.windows(cue.len()).position(|window| window == cue) };
                offset.map(|offset| (offset, index))
            })
            .min()
            .map(|(_, index)| index)
    })
}
