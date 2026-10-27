//! The test's scripts and recorded invocation names (protocol/programs.md,
//! sections 5 and 8; testing.md, section 4); no environment values are retained.

use serde::{Deserialize, Serialize};

/// What a test scripts; defaults to a truthful subscription login and logout.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Story {
    pub status: Status,
    pub login: Login,
    pub logout: Logout,
    #[serde(default)]
    pub init: Init,
    /// Legacy fallback turns, counted from this process's first user message.
    #[serde(default)]
    pub turns: Vec<Turn>,
    /// Record the MCP tool inventory received at startup for admission stories.
    #[serde(default)]
    pub observe_tools: bool,
    /// System-cued conversations; turns count every user message, including resumes.
    #[serde(default)]
    pub cued: Vec<Script>,
    /// Request-cued conversations; turns restart at the latest message holding a cue.
    #[serde(default)]
    pub asked: Vec<Script>,
    #[serde(default)]
    pub usage: Usage,
    #[serde(default)]
    pub resume: Resume,
    /// Text returned by the tool-free summary invocation.
    #[serde(default)]
    pub summary: String,
}

/// One conversation selected by a cue in the system prompt or a user message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Script {
    /// Earliest position wins, then the first listed; a cue never spans text parts.
    pub cue: String,
    pub turns: Vec<Turn>,
}

/// The script selected for a user turn, recorded by the fake for its test.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Selection {
    /// The system prompt selected this zero-based index in `Story::cued`.
    Cued(usize),
    /// A user message selected this zero-based index in `Story::asked`.
    Asked(usize),
    /// Neither cue selected a script, so `Story::turns` answers.
    Otherwise,
}

/// A status answer selected by the story, ending on output or a signal.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    /// Reports the login file's method, or not signed in when absent.
    #[default]
    Truthful,
    /// Reports bytes that do not decode as JSON, then exits successfully.
    Unreadable,
    /// Waits without output until a termination signal ends the invocation.
    Silent,
    /// Reports signed out without reading a login file.
    NotSignedIn,
    /// Answers across invocations, retaining its position beside observations.
    Sequence(Vec<Status>),
}

/// A sign-in flow selected by the story, ending on a saved login or refusal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Login {
    /// Saves the specified method without asking for input.
    Immediate(String),
    /// Saves the method after a nonempty pasted line, refusing empty input or EOF.
    Code(String),
    /// Refuses sign-in and keeps the previous login.
    Refused,
}

impl Default for Login {
    fn default() -> Self {
        Self::Immediate("claude.ai".into())
    }
}

/// A sign-out answer selected by the story, ending with removal or failure.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Logout {
    /// Removes a login, accepting an already absent file.
    #[default]
    Succeeds,
    /// Refuses removal and keeps the login.
    Fails,
    /// Refuses removal with the supplied stderr detail.
    Detail(String),
}

impl Story {
    /// The bytes a test writes to `fake-claude.story`.
    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("story contains only JSON data")
    }
}

/// One invocation as the fake saw it, recorded without environment values.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Observation {
    /// The invocation's arguments, excluding the program name.
    pub arguments: Vec<String>,
    /// The invocation's environment variable names, without their values.
    pub environment: Vec<String>,
}

/// Reads the newline-separated records in `fake-claude.observations`.
/// Malformed records fail the test that reads them.
pub fn observations(bytes: &[u8]) -> Vec<Observation> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("fake observation record"))
        .filter(|value| value.get("arguments").is_some())
        .map(|value| serde_json::from_value(value).expect("invocation observation"))
        .collect()
}

/// Additional init claims sent by the scripted program to its parent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Init {
    /// Simulated delay before publishing init; never sleeps the test process.
    #[serde(default)]
    pub delay_ms: Option<u64>,
    #[serde(default)]
    pub extra_tools: Vec<String>,
    #[serde(default)]
    pub extra_servers: Vec<String>,
    #[serde(default = "capabilities")]
    pub capabilities: Vec<String>,
}

impl Default for Init {
    fn default() -> Self {
        Self { delay_ms: None, extra_tools: Vec::new(), extra_servers: Vec::new(), capabilities: capabilities() }
    }
}

// Owner's Claude Code 2.1.311 probe: reports/live-claude-shapes.md, system/init.
fn capabilities() -> Vec<String> {
    [
        "interrupt_receipt_v1",
        "interrupt_cancel_queued_v1",
        "interrupt_send_now_v1",
        "interrupt_requeue_taken_v1",
        "msg_lifecycle_v1",
        "request_marker_lists_v1",
        "lifecycle_wakeup_source_v1",
        "sdk_mcp_tools_list_changed",
        "sdk_mcp_manifests",
        "mcp_read_resource_v1",
        "mcp_tool_ui_meta_v1",
        "ui_surface_v1",
        "mcp_call_result_not_for_model_v1",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

/// The n-th user message runs this turn; beyond the story it says "Done.".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Turn {
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub end: End,
}

impl Default for Turn {
    fn default() -> Self {
        Self { steps: vec![Step::Text("Done.".into())], end: End::default() }
    }
}

/// A scripted action sent as stream messages or held until a signal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    /// The model supplies StructuredOutput and ends this turn with that outcome.
    Outcome { input: serde_json::Value },
    /// Thinks for these estimated tokens before the wrapped text or tool message.
    Think { tokens: u64, step: Box<Step> },
    /// Announces the program's own window compaction.
    Compact,
    /// Sends text and, when requested, its partial messages.
    Text(String),
    /// Opens a text block and leaves its attempt unfinished for a retry.
    Partial(String),
    /// Reports a rejected usage limit with an optional Unix-seconds reset.
    RateLimit { resets_at: Option<u64> },
    /// Reports the program's own retry to its parent.
    Retry { attempt: u32, status: u16, delay_ms: Option<u64> },
    /// Removes the login before the next turn checks it.
    LoseLogin,
    /// Waits until an interrupt or termination.
    Hang,
    /// Waits through interrupts until termination.
    Deaf,
    /// Emits one assistant message, then runs its Call, Builtin or Act steps in order.
    Together(Vec<Step>),
    /// Calls the named server tool, returning its answer to the model.
    Call { tool: String, input: serde_json::Value },
    /// Calls a server tool and checks its reply against scripted text.
    CallChecked { tool: String, input: serde_json::Value, contains: String },
    /// Asks permission before returning this built-in tool's scripted result.
    Builtin { tool: String, input: serde_json::Value, result: String },
    /// Asks permission, then performs Read, Write or Edit in the launch directory;
    /// a denial returns its message without touching the file.
    Act { tool: String, input: serde_json::Value },
}

/// A turn's terminal message, or its process exit without a result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum End {
    /// Completes successfully, optionally supplying the requested outcome.
    Success { structured_output: Option<serde_json::Value> },
    /// Reports the model's error with its available evidence.
    Error {
        api_error_status: Option<u16>,
        terminal_reason: Option<String>,
        errors: Vec<String>,
        /// Optional Unix-seconds account reset, as in the rate event.
        #[serde(default)]
        resets_at: Option<u64>,
    },
    /// Exits with this code without sending a result.
    Exit(u8),
    /// Sends an undecodable line and exits one.
    Undecodable,
}

impl Default for End {
    fn default() -> Self {
        Self::Success { structured_output: None }
    }
}

/// Token counts per assistant message; the parent receives their turn sum.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default = "default_input")]
    pub input_tokens: u64,
    #[serde(default = "default_output")]
    pub output_tokens: u64,
}

fn default_input() -> u64 {
    10
}

fn default_output() -> u64 {
    5
}

impl Default for Usage {
    fn default() -> Self {
        Self { input_tokens: 10, output_tokens: 5 }
    }
}

/// Whether the scripted program can resume the parent's named session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resume {
    /// Resumes the conversation and accepts user messages.
    #[default]
    Found,
    /// Reports an absent conversation and exits one.
    Missing,
}

/// Turn selections, signals and settings recorded by the fake for its test.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Seen {
    /// The script answering this user message and its zero-based turn index.
    Selected { script: Selection, turn: usize },
    /// The parent interrupted the program.
    Interrupted,
    /// The parent terminated the program.
    Terminated,
    /// The settings read at startup, sent only to the observation file.
    Settings { allow: Vec<String>, ask: Vec<String>, deny: Vec<String>, keys: Vec<String> },
    /// The server's tool inventory, when the story requests its observation.
    Tools { names: Vec<String> },
    /// The settings could not be read or decoded.
    UnreadableSettings,
    /// An allowed built-in operation completed or failed, without file contents.
    Acted { tool: String, path: String, error: bool },
    /// A server tool call and whether its result failed, without its input.
    Called { tool: String, error: bool },
    /// A built-in tool's permission decision, without its input.
    Prompted { tool: String, decision: Decision },
}

/// Reads control and settings observations, skipping invocations and selections.
/// Use [`selections`] for the per-turn script observations.
pub fn seen(bytes: &[u8]) -> Vec<Seen> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("observation JSON"))
        .filter_map(|value| value.get("seen").cloned())
        .map(|value| serde_json::from_value(value).expect("seen record"))
        .collect()
}

/// Reads per-turn script selections, skipping invocation and control records.
pub fn selections(bytes: &[u8]) -> Vec<Seen> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("observation JSON"))
        .filter_map(|value| value.get("selection").cloned())
        .map(|value| serde_json::from_value(value).expect("selection record"))
        .collect()
}

/// A permission decision observed by a test, without the tool's input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    /// The prompt allowed the call; records whether the input stayed unchanged.
    Allow { unchanged: bool },
    /// The prompt denied the call with this explanation.
    Deny(String),
    /// The prompt did not return a readable permission decision.
    Unreadable,
}

/// User messages independently decoded when the fake accepts each input line.
pub fn received(bytes: &[u8]) -> Vec<serde_json::Value> {
    bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).expect("observation JSON"))
        .filter_map(|mut value| value.get_mut("received").map(serde_json::Value::take))
        .collect()
}
