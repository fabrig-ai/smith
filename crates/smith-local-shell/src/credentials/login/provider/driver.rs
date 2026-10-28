//! Drive the account owner's terminals through close (protocol/hosts.md, 5.4).
//! Subject comparisons emit account names only; no subject enters diagnostics.
use super::super::super::{registration, sign_in};
use super::super::LoginOutcome;
use skein_io::kernel;
use skein_lib::{Queue, Time, Wall};
use skein_shell::Host;
use smith_local_service::{Accounts, CredentialRequest, Settled};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Stage {
    SignIn,
    SignOut,
    Rollback,
}

pub(super) struct Login {
    pub driver: Accounts,
    pub name: String,
    pub outcome: Option<LoginOutcome>,
    pub operation: Stage,
    pub closing: bool,
    pub notice: bool,
    pub others: Vec<(String, std::path::PathBuf)>,
    pub ssh: Option<String>,
    pub operations: u32,
    pub out: Queue<CredentialRequest>,
    pub completions: Queue<kernel::Complete>,
}

impl Login {
    fn terminal(&mut self, terminal: Settled) {
        match terminal {
            Settled::SignedIn { subject, missing } => {
                if let Some(subject) = subject {
                    for (name, file) in &self.others {
                        match sign_in::record(file) {
                            Ok(Some(record)) if record.subject.as_deref() == Some(subject.as_ref()) => {
                                eprintln!("smith: account {}: already signed in as account {name}", self.name);
                                self.operation = Stage::Rollback;
                                self.driver.sign_out(0);
                                return;
                            }
                            Err(_) => {
                                eprintln!("smith: account {name}: unreadable sign-in record");
                                self.operation = Stage::Rollback;
                                self.driver.sign_out(0);
                                return;
                            }
                            Ok(Some(_) | None) => {}
                        }
                    }
                }
                if missing.is_empty() {
                    if self.notice {
                        eprintln!("{}", registration::NOTICE);
                    }
                    eprintln!("account {}: signed in", self.name);
                    self.outcome = Some(LoginOutcome::Usable);
                } else {
                    eprintln!(
                        "Signed in without plan usage: run `smith login {}` again and allow it, or use a key",
                        self.name
                    );
                    self.outcome = Some(LoginOutcome::Unavailable);
                }
            }
            Settled::SignedOut { revoked } => {
                if !revoked {
                    eprintln!("account {}: disconnect smith at {}", self.name, registration::DISCONNECT);
                }
                eprintln!("account {}: signed out", self.name);
                self.outcome =
                    Some(if self.operation == Stage::Rollback { LoginOutcome::Refused } else { LoginOutcome::Usable });
            }
            Settled::Failed { failure, .. } => {
                let refused = match failure {
                    skein_oauth_accounts::Failure::Exchange(skein_oauth::Failure::ClientRejected) => {
                        eprintln!("smith: account {}: the provider refused smith's registration", self.name);
                        true
                    }
                    skein_oauth_accounts::Failure::AnotherSubject => {
                        eprintln!("smith: account {}: another subject was refused", self.name);
                        true
                    }
                    skein_oauth_accounts::Failure::Busy
                    | skein_oauth_accounts::Failure::NotGranted
                    | skein_oauth_accounts::Failure::NotKept
                    | skein_oauth_accounts::Failure::Expired
                    | skein_oauth_accounts::Failure::Unloaded { .. }
                    | skein_oauth_accounts::Failure::Exchange(_) => {
                        eprintln!(
                            "account {}: {}",
                            self.name,
                            if self.operation == Stage::SignOut { "not signed out" } else { "not signed in" }
                        );
                        false
                    }
                };
                self.outcome = Some(if refused || self.operation == Stage::Rollback {
                    LoginOutcome::Refused
                } else {
                    LoginOutcome::Unavailable
                });
            }
        }
    }
}

impl Host for Login {
    fn iterate(&mut self, now: Time, wall: Wall) {
        while let Some(complete) = self.completions.pop() {
            self.driver.complete(complete);
        }
        self.driver.advance(now, wall, &mut self.out);
        while let Some(request) = self.out.pop() {
            match request {
                CredentialRequest::Visit { url } => {
                    eprintln!("Sign in: {}", String::from_utf8_lossy(&url));
                    if let Some(host) = &self.ssh
                        && let Some(port) = redirect_port(&url)
                    {
                        eprintln!("ssh -L {port}:127.0.0.1:{port} {host}");
                    }
                }
                CredentialRequest::SignedIn { .. }
                | CredentialRequest::Failed { .. }
                | CredentialRequest::Closed
                | CredentialRequest::Notice { .. }
                | CredentialRequest::Ready { .. } => {}
            }
        }
        while let Some((_, terminal)) = self.driver.settled() {
            self.terminal(terminal);
        }
        if self.outcome.is_some() && !self.closing {
            self.driver.cancel(0);
            self.driver.close(smith_agent_service::Close::Graceful, now, wall);
            self.closing = true;
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        self.driver.submissions()
    }
    fn work_pending(&self, now: Time) -> bool {
        self.driver.work_pending(now)
    }
    fn next_deadline(&self) -> Option<Time> {
        self.driver.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        self.driver.next_deadline()
    }
    fn is_empty(&self) -> bool {
        self.outcome.is_some() && self.driver.closed()
    }
    fn operations(&self) -> u32 {
        self.operations
    }
    fn exit(&self) -> Option<kernel::Exit> {
        self.outcome.map(|outcome| kernel::Exit::Code(outcome.code()))
    }
}

fn redirect_port(url: &[u8]) -> Option<u16> {
    let url = std::str::from_utf8(url).ok()?;
    let redirect = url.split(['?', '&']).find_map(|field| field.strip_prefix("redirect_uri="))?;
    let port = redirect.split("%3A").nth(2)?.split("%2F").next()?;
    port.parse().ok()
}
