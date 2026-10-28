//! Per-request program checks (domain/host.md, section 8; protocol/hosts.md,
//! section 2). Keeps startup coverage, one child and an arrival-ordered queue;
//! knows no login contents. Requests, I/O, deadlines and cancellation settle
//! through the process adapter before a credential is lent or refused.
use alloc::boxed::Box;
use skein_io::{self as io, kernel};
use skein_lib::{Duration, List, Queue, Time, Token};
use smith_host_domain::{ProgramCheck, SourceName};
use smith_host_protocol::{ProgramSource, SourceAnswer, SourceAsk};
use smith_local_domain as local;
use smith_protocol_program::Programs;

mod binding;
mod driver;
mod lifecycle;
mod pending;

#[derive(Debug)]
struct Account {
    number: u32,
    source: SourceName,
    covered: bool,
}

#[derive(Debug)]
struct Active {
    account: u32,
    source: ProgramSource,
    cancelled: bool,
}

pub(crate) struct Checks {
    accounts: List<Account>,
    waiting: Queue<u32>,
    programs: Programs,
    base: Box<[Box<[u8]>]>,
    root: Option<kernel::Fd>,
    root_closed: bool,
    detail_bytes: u32,
    duration: Duration,
    next_owner: u64,
    active: Option<Active>,
    interrupted: u32,
}

impl core::fmt::Debug for Checks {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("Checks")
            .field("accounts", &self.accounts.len())
            .field("waiting", &self.waiting.len())
            .field("base_entries", &self.base.len())
            .finish_non_exhaustive()
    }
}

impl Checks {
    pub(crate) fn request(
        &mut self,
        account: u32,
        now: Time,
        below: &mut Queue<io::Request>,
        shell: &mut Queue<local::Request>,
    ) -> bool {
        if self.account_index(account).is_none() {
            return false;
        }
        if self.active.is_some() || !self.waiting.is_empty() {
            self.waiting.push(account);
        } else {
            self.start(account, now, below, shell);
        }
        true
    }

    fn account_index(&self, account: u32) -> Option<u32> {
        let mut found = None;
        for index in 0..self.accounts.len() {
            if self.accounts.get(index).expect("account slot").number == account {
                found = Some(index);
            }
        }
        found
    }

    fn start(&mut self, account: u32, now: Time, below: &mut Queue<io::Request>, shell: &mut Queue<local::Request>) {
        let index = self.account_index(account).expect("program account");
        let entry = self.accounts.get_mut(index).expect("program account slot");
        if entry.covered {
            entry.covered = false;
            shell.push(local::Request::Credential { account });
            return;
        }
        let owner = Token::new(self.next_owner);
        self.next_owner = self.next_owner.checked_add(1).expect("program check owners never wrap");
        let source = ProgramSource::new(
            owner,
            SourceAsk::Check,
            &self.programs,
            entry.source,
            &self.base,
            self.root.expect("program root stays open through checks"),
            b".".as_slice().into(),
            now.saturating_add(self.duration),
            self.detail_bytes,
            below,
        )
        .expect("admitted program binding");
        self.active = Some(Active { account, source, cancelled: false });
    }
}

fn failure(answer: ProgramCheck) -> Option<local::CredentialFailure> {
    match answer {
        ProgramCheck::Usable => None,
        ProgramCheck::NotSignedIn => Some(local::CredentialFailure::ProgramNotSignedIn),
        ProgramCheck::SignedInOtherwise(method) => Some(local::CredentialFailure::ProgramSignedInOtherwise { method }),
        ProgramCheck::Failed(failure) => Some(local::CredentialFailure::ProgramFailed(failure)),
    }
}

#[cfg(test)]
mod tests;
