//! Account grants, visits and source operations in the service's kernel namespace.
//! The source driver owns expiry; the service owns tagged values and local-domain events.
use super::{Close, Error, Service, local};
use alloc::boxed::Box;
use skein_lib::{List, Queue, Time, Wall, Writer};

impl Service {
    /// Install the selected account sources before the first service pass.
    pub fn handed_accounts(
        &mut self,
        sources: List<crate::Source>,
        limits: skein_oauth_accounts::Limits,
        entropy: [u8; 32],
    ) -> Result<(), Error> {
        assert!(self.accounts.is_none(), "one account driver");
        if self.limits.queue < crate::ACCOUNTS_MAX_OUT {
            return Err(Error::Queue);
        }
        if sources.len() > self.limits.local.smith.accounts {
            return Err(Error::Memory);
        }
        self.accounts = Some(crate::Accounts::shared(sources, limits, entropy)?);
        Ok(())
    }

    pub(super) fn accounts_closed(&self) -> bool {
        match &self.accounts {
            Some(accounts) => accounts.closed(),
            None => true,
        }
    }

    pub(super) fn release_accounts(&mut self, now: Time) {
        if let Some(accounts) = &mut self.accounts {
            accounts.release_all(now, self.wall);
        }
    }

    pub(super) fn accounts_pass(&mut self, now: Time, wall: Wall) {
        if self.shell.room() < crate::ACCOUNTS_MAX_OUT || self.output.room() < crate::ACCOUNTS_MAX_OUT {
            return;
        }
        let Some(mut accounts) = self.accounts.take() else { return };
        if self.closing() {
            accounts.close(if self.aborted() { Close::Aborted } else { Close::Graceful }, now, wall);
        }
        let mut out = Queue::with_capacity(crate::ACCOUNTS_MAX_OUT);
        accounts.advance(now, wall, &mut out);
        for _ in 0..crate::ACCOUNTS_MAX_OUT {
            match out.pop() {
                Some(crate::CredentialRequest::Ready { event: local::Event::Credential { grant, .. }, value }) => {
                    let identifier = accounts.account_id(grant.name.account).expect("selected account");
                    // accounts-sources moves the presets to their own sources.
                    let value = if identifier.is_empty() {
                        smith_protocol_channel::GrantValue::Bearer(value)
                    } else {
                        smith_protocol_channel::GrantValue::Withdrawn {
                            account_id: Box::from(identifier),
                            bearer: value,
                        }
                    };
                    self.credential(grant, value);
                }
                Some(crate::CredentialRequest::Notice { account, reason }) => {
                    self.notice(now, local::Notice::Failure(local::Failure::Credential { account, reason }));
                }
                Some(crate::CredentialRequest::Failed { event }) => self.local_events.push(event),
                Some(crate::CredentialRequest::Closed) | None => break,
                Some(crate::CredentialRequest::Visit { url, .. }) => {
                    let mut line = Writer::new(url.len().checked_add(10).expect("bounded sign-in address"));
                    line.put(b"Sign in: ").expect("visit prefix");
                    line.put(&url).expect("visit address");
                    line.put(b"\n").expect("visit newline");
                    self.output.push(line.finish());
                }
                Some(crate::CredentialRequest::SignedIn { .. }) => {}
                Some(crate::CredentialRequest::Ready { .. }) => {
                    unreachable!("the native driver owns its exchange and keeper")
                }
            }
        }
        self.accounts = Some(accounts);
    }

    pub(super) fn account_request(&mut self, account: u32) {
        if self.interrupted_program_request(account) {
            return;
        }
        if self.below.process_mut().program_request(account, self.now, &mut self.shell) {
            return;
        }
        let handled = match &mut self.accounts {
            Some(owner) => owner.request(account),
            None => false,
        };
        if !handled {
            self.shell.push(local::Request::Credential { account });
        }
    }
}
