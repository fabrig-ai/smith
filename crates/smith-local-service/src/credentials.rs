//! Account sources and private keeper routing (protocol/hosts.md, 5.4).
//! The driver owns records, exchanges and holds. This owner keeps source
//! descriptors, account identifiers, socket and private file routing. Env
//! records are handed in once; refresh bytes stay in private sign-in records.
use alloc::boxed::Box;
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::{CryptoRng, Env, List, Queue, Secret, Time, Token, Wall};
use skein_oauth_accounts as oauth;
use smith_local_domain as local;
use smith_local_protocol::Borrowed;
mod adopt;
mod boundary;
pub use boundary::CredentialRequest;

mod lock;
mod private;
mod sign_out;
mod subject;
mod terminals;
pub use sign_out::Settled;
mod transport;

const ROOT_FILE: u64 = 1 << 63;

/// One account source handed to the local service before its loop.
#[expect(missing_debug_implementations, reason = "environment records contain credentials")]
#[expect(clippy::large_enum_variant, reason = "source registrations move once through the account adoption boundary")]
pub enum Source {
    /// A variable read once by the shell, with its endpoint's account identifier.
    Env { account: u32, borrowed: Borrowed },
    /// An issuer account whose private record and exchanges the driver owns.
    SignIn {
        account: u32,
        root: kernel::Fd,
        user: u32,
        directory: Box<[u8]>,
        file: Box<[u8]>,
        registration: skein_oauth::Registration,
        endpoint: oauth::Endpoint,
        account_id: Box<[u8]>,
        interactive: bool,
    },
}

impl Source {
    /// Maximum account identifier accepted by this driver's handed-in boundary.
    #[must_use]
    pub const fn account_id_bytes() -> usize {
        1024
    }
}

struct Account {
    number: u32,
    source: Origin,
    identifier: Box<[u8]>,
    generation: u64,
    expires: Wall,
    startup: bool,
    loading: bool,
    wanted: bool,
    subject: Option<Box<[u8]>>,
    subject_failure: Option<oauth::Failure>,
    pending: Option<oauth::Request>,
}
enum Origin {
    Env {
        initial: Option<Borrowed>,
    },
    SignIn {
        root: Option<Token>,
        claim: Option<skein_oauth::ClaimSelector>,
        interactive: bool,
        sign_in: bool,
        signing: bool,
    },
}

/// The service's account driver, socket pass and source file owner.
#[expect(missing_debug_implementations, reason = "the driver contains credentials")]
pub struct Accounts {
    driver: oauth::Component,
    sockets: io::Io,
    socket_events: Queue<io::Event>,
    socket_requests: Queue<io::Request>,
    socket_completions: Queue<kernel::Complete>,
    socket_submissions: Queue<kernel::Submit>,
    file_requests: Queue<oauth::FileLower>,
    limits: oauth::Limits,
    entries: List<Account>,
    files: file_layer::FileIo,
    events: Queue<file::Event>,
    failed: Option<(u32, local::CredentialFailure)>,
    submissions: Queue<kernel::Submit>,
    out: Queue<oauth::Event>,
    closed: bool,
    closing: Option<smith_agent_service::Close>,
    close_sent: bool,
    locks: List<lock::Lock>,
    reads: List<subject::Read>,
    settled: List<Option<(u32, Settled)>>,
}

/// Whether account file routing, socket tables and OAuth documents can be represented.
#[must_use]
pub fn accounts_valid(limits: &oauth::Limits) -> bool {
    let Some(file_requests) = limits.accounts.checked_mul(2) else { return false };
    limits.accounts.checked_mul(4).is_some()
        && file_requests.checked_add(1).is_some()
        && crate::native_io_valid(&limits.io)
        // Skein's client owns the document/record codec and parser checks.
        && skein_oauth::Client::new(limits.client).is_ok()
        && limits.http.head >= 2
        && limits.http.read > 0
        && limits.http.send > 0
        && limits.tls.read > 0
        && limits.tls.send > 0
        && limits.tls.records >= skein_tls::client::MAX_RECORD
        && limits.tls.read.checked_add(skein_tls::client::MAX_PLAINTEXT).is_some()
        && skein_tls::client::room_for(limits.tls.send).is_some()
}

fn number(source: &Source) -> u32 {
    match source {
        Source::Env { account, .. } | Source::SignIn { account, .. } => *account,
    }
}

fn validate_sources(sources: &List<Source>, limits: &oauth::Limits) -> Result<Option<u32>, crate::Error> {
    if !accounts_valid(limits) {
        return Err(crate::Error::Memory);
    }
    if limits.file_stall == skein_lib::Duration::ZERO || limits.accounts == 0 {
        return Err(crate::Error::Process);
    }
    if sources.len() > limits.accounts || limits.client.document.document_bytes == 0 {
        return Err(crate::Error::Memory);
    }
    for index in 0..sources.len() {
        let source = sources.get(index).expect("source index");
        for prior in 0..index {
            if number(source) == number(sources.get(prior).expect("prior source")) {
                return Err(crate::Error::Process);
            }
        }
        match source {
            Source::Env { borrowed, .. }
                if borrowed.record.refresh_token.is_some()
                    || borrowed.account_id.len() > 1024
                    || skein_oauth::encode_record(&borrowed.record, &limits.client.document).is_err() =>
            {
                return Err(crate::Error::Process);
            }
            Source::Env { .. } | Source::SignIn { .. } => {}
        }
    }
    let mut effective_user = None;
    for source in sources {
        match source {
            Source::SignIn { user, .. } => {
                if effective_user.is_some() && effective_user != Some(*user) {
                    return Err(crate::Error::Process);
                }
                effective_user = Some(*user);
            }
            Source::Env { .. } => {}
        }
    }
    Ok(effective_user)
}

fn configured_driver(
    accounts: List<oauth::Account>,
    limits: &oauth::Limits,
    entropy: [u8; 32],
) -> Result<oauth::Component, crate::Error> {
    let crypto = CryptoRng::new(&Secret::new(Box::from(entropy)), 0).ok_or(crate::Error::Memory)?;
    match oauth::Component::new(accounts, limits, crypto) {
        Ok(driver) => Ok(driver),
        Err(_) => Err(crate::Error::Process),
    }
}

fn account_form(identifier: &[u8]) -> smith_host_domain::Form {
    if identifier.is_empty() {
        smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer)
    } else {
        smith_host_domain::Form::Withdrawn
    }
}

fn claim_identifier(account: &mut Account, token: &[u8], wall: Wall, limits: &skein_oauth::Limits) -> bool {
    match &account.source {
        Origin::SignIn { claim: Some(claim), .. } => match skein_oauth::read_claims(token, claim, wall, limits) {
            Ok(claims) if claims.metadata.len() <= Source::account_id_bytes() => {
                account.identifier = claims.metadata;
                true
            }
            Ok(_) | Err(_) => false,
        },
        Origin::SignIn { claim: None, .. } | Origin::Env { .. } => true,
    }
}

impl Accounts {
    /// Validate sources and adopt their source directory descriptors.
    pub fn new(sources: List<Source>, limits: oauth::Limits, entropy: [u8; 32]) -> Result<Self, crate::Error> {
        Self::build(sources, &limits, entropy, false)
    }

    /// Adopt sources whose private writes take the record's directory lock.
    pub fn shared(sources: List<Source>, limits: oauth::Limits, entropy: [u8; 32]) -> Result<Self, crate::Error> {
        Self::build(sources, &limits, entropy, true)
    }

    fn build(
        sources: List<Source>,
        limits: &oauth::Limits,
        entropy: [u8; 32],
        shared: bool,
    ) -> Result<Self, crate::Error> {
        let mut locks = List::with_capacity(limits.accounts);
        let effective_user = validate_sources(&sources, limits)?;
        let mut handed = List::with_capacity(limits.accounts);
        let mut accounts = List::<Account>::with_capacity(limits.accounts);
        let mut files = file_layer::FileIo::with_whole_limit(
            limits.accounts.checked_mul(4).ok_or(crate::Error::Memory)?,
            4096,
            1,
            limits.client.document.document_bytes.max(limits.client.document.record_bytes),
            limits.file_stall,
        );
        let random =
            u64::from_be_bytes(entropy.get(..8).expect("entropy prefix").try_into().expect("eight entropy bytes"));
        files.seed_randomness(random);
        if let Some(user) = effective_user {
            files = files.with_effective_user(user);
        }
        for source in sources.into_boxed() {
            let bound = adopt::source(source, &mut files, &mut locks, shared)?;
            assert!(handed.push(bound.configured).is_ok(), "validated account capacity");
            assert!(accounts.push(bound.account).is_ok(), "validated account capacity");
        }
        let driver = configured_driver(handed, limits, entropy)?;
        Ok(Self {
            driver,
            locks,
            reads: List::with_capacity(limits.accounts),
            settled: List::with_capacity(limits.accounts),
            sockets: io::Io::new(&limits.io),
            socket_events: Queue::with_capacity(3),
            socket_requests: Queue::with_capacity(3),
            socket_completions: Queue::with_capacity(io::operations(&limits.io).ok_or(crate::Error::Memory)?),
            socket_submissions: Queue::with_capacity(8),
            file_requests: Queue::with_capacity(
                limits
                    .accounts
                    .checked_mul(2)
                    .ok_or(crate::Error::Memory)?
                    .checked_add(1)
                    .ok_or(crate::Error::Memory)?,
            ),
            limits: *limits,
            entries: accounts,
            files,
            events: Queue::with_capacity(3),
            failed: None,
            submissions: Queue::with_capacity(8),
            out: Queue::with_capacity(3),
            closed: false,
            closing: None,
            close_sent: false,
        })
    }

    fn down(&mut self, request: oauth::Request, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits };
        self.driver.down(&env, request, &mut self.out, &mut self.socket_requests, &mut self.file_requests);
    }

    /// Request this account's grant; the next advance emits its terminal.
    pub fn request(&mut self, number: u32) -> bool {
        if self.closing.is_some() || self.closed {
            return false;
        }
        for index in 0..self.entries.len() {
            let account = self.entries.get_mut(index).expect("account index");
            if account.number == number {
                assert!(!account.wanted, "one grant request per account");
                account.wanted = true;
                return true;
            }
        }
        false
    }

    fn hand_in(&mut self, index: u32, mut borrowed: Borrowed, now: Time, wall: Wall) -> bool {
        let account = self.entries.get_mut(index).expect("account index");
        if borrowed.account_id.len() > 1024 {
            self.fail(index, local::CredentialFailure::Missing);
            return false;
        }
        let Some(generation) = account.generation.checked_add(1) else {
            self.fail(index, local::CredentialFailure::Missing);
            return false;
        };
        borrowed.record.key = index;
        borrowed.record.generation = generation;
        account.generation = generation;
        account.expires = borrowed.record.expires_at;
        account.identifier = borrowed.account_id;
        self.down(oauth::Request::HandIn { account: index, record: borrowed.record }, now, wall);
        self.out.is_empty()
    }

    fn fail(&mut self, index: u32, reason: local::CredentialFailure) {
        let account = self.entries.get_mut(index).expect("account index");
        if account.wanted {
            assert!(self.failed.is_none(), "one serial file failure");
            self.failed = Some((account.number, reason));
            account.wanted = false;
        }
    }

    fn begin(&mut self, index: u32, now: Time, wall: Wall) {
        let account = self.entries.get_mut(index).expect("account index");
        account.startup = false;
        if let Some(request) = account.pending.take() {
            account.loading = true;
            self.down(request, now, wall);
            return;
        }
        match &mut account.source {
            Origin::SignIn { sign_in, signing, .. } => {
                let request = if *sign_in {
                    *sign_in = false;
                    *signing = true;
                    oauth::Request::SignIn { account: index, subject: account.subject.take() }
                } else if account.wanted {
                    oauth::Request::Grant { account: index }
                } else {
                    return;
                };
                account.loading = true;
                self.down(request, now, wall);
            }
            Origin::Env { initial } => {
                let initial = initial.take();
                if let Some(borrowed) = initial
                    && !self.hand_in(index, borrowed, now, wall)
                {
                    return;
                }
                if self.entries.get(index).expect("account index").wanted {
                    self.down(oauth::Request::Release { account: index }, now, wall);
                    self.down(oauth::Request::Grant { account: index }, now, wall);
                }
            }
        }
    }

    fn progress(&mut self, now: Time, wall: Wall) {
        if !self.files.takes() || !self.out.is_empty() {
            return;
        }
        for index in 0..self.entries.len() {
            let account = self.entries.get_mut(index).expect("account index");
            if self.closing.is_some() {
                match &mut account.source {
                    Origin::SignIn { root, .. } if self.closed => {
                        if let Some(root) = root.take() {
                            file_layer::down(
                                &mut self.files,
                                now,
                                file::Request::Close { owner: Token::new(u64::from(index) | ROOT_FILE), file: root },
                                &mut self.events,
                                &mut self.submissions,
                            );
                            return;
                        }
                    }
                    Origin::SignIn { .. } | Origin::Env { .. } => {}
                }
            } else if !account.loading && (account.startup || account.wanted) {
                self.begin(index, now, wall);
                return;
            }
        }
    }

    fn filed(&mut self, event: file::Event, now: Time, wall: Wall) {
        if event.owner().raw() & ROOT_FILE != 0 {
            return;
        }
        if event.owner().raw() & lock::LOCK_FILE != 0 {
            self.lock_event(event, now, wall);
            return;
        }
        self.record_event(&event);
        let env = Env { now, wall, limits: self.limits };
        self.driver.filed(&env, event, &mut self.out, &mut self.socket_requests, &mut self.file_requests);
    }

    /// Route one terminal of this driver's kernel operations.
    pub fn complete(&mut self, complete: kernel::Complete) {
        if let Some(complete) = self.socket_completion(complete) {
            file_layer::up(&mut self.files, complete, &mut self.events, &mut self.submissions);
        }
    }

    /// The earliest exchange, socket or source-file deadline.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut deadline = self.driver.next_deadline();
        for candidate in
            [self.files.next_deadline(), self.sockets.next_deadline(), self.lock_deadline()].into_iter().flatten()
        {
            deadline = Some(match deadline {
                Some(prior) => prior.min(candidate),
                None => candidate,
            });
        }
        for lower in &self.file_requests {
            match lower {
                oauth::FileLower::Request { deadline: candidate, .. }
                | oauth::FileLower::Remove { deadline: candidate, .. } => {
                    deadline = Some(match deadline {
                        Some(prior) => prior.min(*candidate),
                        None => *candidate,
                    });
                }
                oauth::FileLower::Cancel { .. } => {}
            }
        }
        deadline
    }

    /// The driver and every adopted directory have reported closed.
    #[must_use]
    pub fn closed(&self) -> bool {
        self.closed
            && self.files.open_files() == 0
            && self.events.is_empty()
            && self.submissions.is_empty()
            && self.file_requests.is_empty()
            && self.socket_empty()
    }

    /// Whether an advance can make progress before another kernel completion.
    #[must_use]
    pub fn work_pending(&self, now: Time) -> bool {
        if self.closed() {
            return false;
        }
        if self.files.takes() && (self.lock_pending(now) || !self.file_requests.is_empty()) {
            return true;
        }
        if self.socket_pending(now) || self.driver.has_work() || (self.closing.is_some() && !self.close_sent) {
            return true;
        }
        if !self.events.is_empty() || !self.out.is_empty() || self.failed.is_some() || !self.submissions.is_empty() {
            return true;
        }
        if match self.next_deadline() {
            Some(deadline) => deadline <= now,
            None => false,
        } {
            return true;
        }
        if self.files.takes() {
            for account in &self.entries {
                if self.closing.is_none() && !account.loading && (account.startup || account.wanted) {
                    return true;
                }
                match account.source {
                    Origin::SignIn { root: Some(_), .. } if self.closed => {
                        return true;
                    }
                    Origin::Env { .. } | Origin::SignIn { .. } => {}
                }
            }
        }
        false
    }
}

/// Caller-reserved room for one driver advance, including its file failures.
pub const ACCOUNTS_MAX_OUT: u32 = 6;

impl Accounts {
    /// Directory identifiers accompany grants without putting them in traces.
    #[must_use]
    pub fn account_id(&self, number: u32) -> Option<&[u8]> {
        for account in &self.entries {
            if account.number == number {
                return Some(&account.identifier);
            }
        }
        None
    }

    /// Kernel requests retain this owner's tokens until their terminals arrive.
    pub fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    /// End the run's holds while keeping the source records for the next run.
    pub fn release_all(&mut self, now: Time, wall: Wall) {
        for index in 0..self.entries.len() {
            self.down(oauth::Request::Release { account: index }, now, wall);
        }
    }

    /// Close the driver before closing its adopted private roots.
    pub fn close(&mut self, close: smith_agent_service::Close, _now: Time, _wall: Wall) {
        if self.closing.is_none() && !self.closed {
            self.closing = Some(close);
        }
        if close == smith_agent_service::Close::Aborted && self.closing == Some(smith_agent_service::Close::Graceful) {
            self.closing = Some(close);
            self.close_sent = false;
        }
    }

    /// Advance serial file reads and translate driver events, reserving `ACCOUNTS_MAX_OUT`.
    pub fn advance(&mut self, now: Time, wall: Wall, out: &mut Queue<CredentialRequest>) {
        assert!(out.room() >= ACCOUNTS_MAX_OUT, "caller reserves a complete account advance");
        if self.closed() {
            return;
        }
        if self.files.is_due(now) {
            file_layer::expire(&mut self.files, now, &mut self.submissions);
        }
        for _ in 0..3_u32 {
            if self.out.room() < 3 || self.socket_requests.room() < 3 || self.file_requests.room() == 0 {
                break;
            }
            match self.events.pop() {
                Some(event) => self.filed(event, now, wall),
                None => break,
            }
            if !self.out.is_empty() {
                break;
            }
        }
        self.socket_up(now, wall);
        if self.out.is_empty() && self.socket_requests.room() >= 3 && self.file_requests.room() > 0 {
            self.settle_close(now, wall);
            self.progress(now, wall);
            if self.out.is_empty() {
                let env = Env { now, wall, limits: self.limits };
                self.driver.fire(&env, &mut self.out, &mut self.socket_requests, &mut self.file_requests);
            }
        }
        self.lock_progress(now);
        self.private_files(now, wall);
        self.socket_down(now, wall);
        self.emit(now, wall, out);
        if self.closed() && self.closing.is_some() {
            self.closing = None;
            out.push(CredentialRequest::Closed);
        }
    }
}

#[cfg(test)]
mod tests;
