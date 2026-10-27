//! Command machines for sign-in (protocol/programs.md, sections 3, 4.1, 5 and
//! 8; testing.md, section 4). Effects are driven by the shared fake host.

use skein_io::{file, kernel};
use skein_lib::Token;

use crate::{LOGIN_FILE, Login, Logout, Story};
mod status;
use status::StatusCommand;

pub(crate) const FILE_OWNER: Token = Token::new(1);

#[derive(Debug)]
pub(crate) enum Effect {
    Output(Vec<u8>),
    Error(Vec<u8>),
    File(file::Request),
    ReadCode,
    DelayedInit { after_ms: u64, bytes: Vec<u8> },
    Seen(crate::Seen),
    Received(serde_json::Value),
    McpStart { configuration: serde_json::Value, directory: String },
    McpCall { tool: String, input: serde_json::Value },
    McpCancel,
    End(u8),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Config {
    pub root: Option<Token>,
    pub directory: Option<kernel::Fd>,
    pub mcp: Option<Token>,
    pub mcp_directory: Option<kernel::Fd>,
}

#[derive(Debug)]
pub(crate) enum Command {
    Status(StatusCommand),
    Login(LoginCommand),
    Logout(LogoutCommand),
    Print(Box<crate::print::Print>),
    Unknown,
}

impl Command {
    pub fn start(arguments: &[String], story: Story, config: Config) -> (Self, Vec<Effect>) {
        match arguments {
            [auth, command] if auth == "auth" => match command.as_str() {
                "status" => {
                    let (state, effects) = StatusCommand::start(story.status, config);
                    (Self::Status(state), effects)
                }
                "login" => {
                    let (state, effects) = LoginCommand::start(story.login, config);
                    (Self::Login(state), effects)
                }
                "logout" => {
                    let (state, effects) = LogoutCommand::start(story.logout, config);
                    (Self::Logout(state), effects)
                }
                _ => (Self::Unknown, error("unknown command", 2)),
            },
            _ => match crate::print::Print::start(arguments, story, config) {
                Ok((state, effects)) => (Self::Print(Box::new(state)), effects),
                Err(effects) => (Self::Unknown, effects),
            },
        }
    }

    pub fn file(&mut self, event: file::Event) -> Vec<Effect> {
        match self {
            Self::Status(state) => state.file(event),
            Self::Login(state) => state.file(event),
            Self::Logout(state) => state.file(event),
            Self::Print(state) => state.file(event),
            Self::Unknown => Vec::new(),
        }
    }

    pub fn input(&mut self, bytes: Option<&[u8]>, config: Config) -> Vec<Effect> {
        match self {
            Self::Login(state) => state.input(bytes, config),
            Self::Print(state) => state.input(bytes),
            Self::Status(_) | Self::Logout(_) | Self::Unknown => Vec::new(),
        }
    }

    pub fn mcp(&mut self, notice: crate::mcp::Notice) -> Vec<Effect> {
        match self {
            Self::Print(state) => state.mcp(notice),
            Self::Status(_) | Self::Login(_) | Self::Logout(_) | Self::Unknown => Vec::new(),
        }
    }

    pub fn stop(&mut self) -> Vec<Effect> {
        match self {
            Self::Print(state) => state.stop(),
            Self::Status(_) | Self::Login(_) | Self::Logout(_) | Self::Unknown => Vec::new(),
        }
    }

    pub fn interrupt(&mut self) -> Vec<Effect> {
        match self {
            Self::Print(state) => state.interrupt(),
            Self::Status(_) | Self::Login(_) | Self::Logout(_) | Self::Unknown => vec![Effect::End(143)],
        }
    }
}

#[derive(Debug)]
pub(crate) enum LoginCommand {
    Code { method: String, nonempty: bool },
    Saving { announce: bool },
    Done,
}

impl LoginCommand {
    fn start(login: Login, config: Config) -> (Self, Vec<Effect>) {
        if config.root.is_none() {
            return (Self::Done, error("configuration directory does not exist", 1));
        }
        match login {
            Login::Immediate(method) => (Self::Saving { announce: true }, vec![save(method, config)]),
            Login::Code(method) => (
                Self::Code { method, nonempty: false },
                vec![Effect::Output(b"Paste the code from the browser:\n".to_vec()), Effect::ReadCode],
            ),
            Login::Refused => (Self::Done, error("sign-in refused", 1)),
        }
    }

    fn input(&mut self, bytes: Option<&[u8]>, config: Config) -> Vec<Effect> {
        let Self::Code { method, nonempty } = self else { return Vec::new() };
        match bytes {
            Some(b"\n" | b"\r") if *nonempty => {
                let method = std::mem::take(method);
                *self = Self::Saving { announce: false };
                vec![save(method, config)]
            }
            Some(b"\n" | b"\r") | None => {
                *self = Self::Done;
                vec![Effect::End(1)]
            }
            Some(_) => {
                *nonempty = true;
                vec![Effect::ReadCode]
            }
        }
    }

    fn file(&mut self, event: file::Event) -> Vec<Effect> {
        let announce = matches!(self, Self::Saving { announce: true });
        *self = Self::Done;
        match event {
            file::Event::Stored { .. } if announce => reply(b"Signed in.\n".to_vec(), 0),
            file::Event::Stored { .. } => vec![Effect::End(0)],
            _ => error("cannot save login", 1),
        }
    }
}

fn save(method: String, config: Config) -> Effect {
    Effect::File(file::Request::Store {
        owner: FILE_OWNER,
        root: config.root.expect("login checked its configuration root"),
        path: LOGIN_FILE.as_bytes().into(),
        bytes: method.into_bytes().into_boxed_slice(),
        expected: file::Expect::Any,
        no_follow: true,
    })
}

#[derive(Debug)]
pub(crate) enum LogoutCommand {
    Removing,
    Done,
}

impl LogoutCommand {
    fn start(logout: Logout, config: Config) -> (Self, Vec<Effect>) {
        match logout {
            Logout::Fails => (Self::Done, error("sign-out refused", 1)),
            Logout::Detail(detail) => (Self::Done, error(&detail, 1)),
            Logout::Succeeds => match config.directory {
                Some(root) => (
                    Self::Removing,
                    vec![Effect::File(file::Request::Remove {
                        owner: FILE_OWNER,
                        root,
                        name: LOGIN_FILE.as_bytes().into(),
                        directory: false,
                    })],
                ),
                None => (Self::Done, reply(b"Signed out.\n".to_vec(), 0)),
            },
        }
    }

    fn file(&mut self, event: file::Event) -> Vec<Effect> {
        *self = Self::Done;
        match event {
            file::Event::Removed { .. } | file::Event::Failed { error: kernel::Error::NotFound, .. } => {
                reply(b"Signed out.\n".to_vec(), 0)
            }
            _ => error("cannot remove login", 1),
        }
    }
}

fn reply(bytes: Vec<u8>, code: u8) -> Vec<Effect> {
    vec![Effect::Output(bytes), Effect::End(code)]
}

pub(crate) fn error(message: &str, code: u8) -> Vec<Effect> {
    vec![Effect::Error(format!("fake claude: {message}\n").into_bytes()), Effect::End(code)]
}
