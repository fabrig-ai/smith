//! Explicit account operations and last terminals (protocol/hosts.md, 5.4).
//! Values remain private; hosts take the subject only to enforce account identity.
use super::{Accounts, Origin, oauth};
use alloc::boxed::Box;

/// A requested account operation's last terminal, taken by its shell owner.
#[expect(missing_debug_implementations, reason = "subjects must never enter diagnostics")]
pub enum Settled {
    /// Sign-in kept a record, with its subject and any ungranted required scopes.
    SignedIn { subject: Option<Box<[u8]>>, missing: Box<[u8]> },
    /// Sign-out removed the record, whether or not revocation was confirmed.
    SignedOut { revoked: bool },
    /// The named operation failed before its successful terminal.
    Failed { ends: oauth::Ends, failure: oauth::Failure },
}

impl Accounts {
    /// Revoke and remove a named sign-in account at the next advance.
    pub fn sign_out(&mut self, number: u32) {
        for index in 0..self.entries.len() {
            let account = self.entries.get_mut(index).expect("account index");
            if account.number == number && self.closing.is_none() {
                account.pending = Some(oauth::Request::SignOut { account: index });
                account.startup = true;
            }
        }
    }

    /// Require this subject for the next explicit sign-in of the account.
    pub fn expect_subject(&mut self, number: u32, subject: Box<[u8]>) {
        for index in 0..self.entries.len() {
            let account = self.entries.get_mut(index).expect("account index");
            if account.number == number {
                account.subject = Some(subject);
                return;
            }
        }
    }

    /// Cancel the account's waiting sign-in before closing its driver.
    pub fn cancel(&mut self, number: u32) {
        for index in 0..self.entries.len() {
            let account = self.entries.get_mut(index).expect("account index");
            if account.number == number && !self.closed {
                match &mut account.source {
                    Origin::SignIn { sign_in, .. } => *sign_in = false,
                    Origin::Env { .. } => {}
                }
                account.pending = Some(oauth::Request::Cancel { account: index });
                account.loading = false;
                account.startup = true;
            }
        }
    }

    /// Take one account's last terminal without changing the grant boundary.
    pub fn settled(&mut self) -> Option<(u32, Settled)> {
        for index in 0..self.settled.len() {
            if let Some(terminal) = self.settled.get_mut(index).expect("terminal index").take() {
                return Some(terminal);
            }
        }
        None
    }

    pub(super) fn remember(&mut self, index: u32, terminal: Settled) {
        let number = self.entries.get(index).expect("account index").number;
        let mut empty = None;
        for index in 0..self.settled.len() {
            match self.settled.get_mut(index).expect("terminal index") {
                Some((prior, value)) if *prior == number => {
                    *value = terminal;
                    return;
                }
                None => empty = Some(index),
                Some(_) => {}
            }
        }
        if let Some(index) = empty {
            *self.settled.get_mut(index).expect("empty slot") = Some((number, terminal));
        } else {
            self.settled.push(Some((number, terminal))).ok().expect("one terminal per account");
        }
    }
}
