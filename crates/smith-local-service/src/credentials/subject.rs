//! Guard a shared sign-in's write using its keeper's last locked read.
//! Keeps only the subject per private root; no credential enters a diagnostic
//! (shell.md, section 7.3; oauth.md, section 6.4).
use super::{Accounts, Origin, oauth};
use alloc::boxed::Box;
use skein_io::file;
use skein_lib::{Time, Token, Wall};

pub(super) struct Read {
    root: Token,
    owner: Token,
    subject: Option<Box<[u8]>>,
}

impl Accounts {
    // Only private keeper requests reach this boundary. The component reloads
    // under the held lock immediately before constructing each store.
    pub(super) fn record_request(&mut self, request: &file::Request, now: Time, wall: Wall) -> bool {
        match request {
            file::Request::Load { root, owner, .. } => {
                for index in 0..self.reads.len() {
                    let read = self.reads.get_mut(index).expect("read index");
                    if read.root == *root {
                        read.owner = *owner;
                        read.subject = None;
                        return false;
                    }
                }
                self.reads.push(Read { root: *root, owner: *owner, subject: None }).ok().expect("one read per account");
            }
            file::Request::Store { root, owner, bytes, .. } => {
                let candidate =
                    skein_oauth::decode_record(bytes, &self.limits.client.document).expect("keeper encoding");
                let mut different = false;
                for read in &self.reads {
                    if read.root == *root && read.subject.is_some() && read.subject != candidate.subject {
                        different = true;
                    }
                }
                if !different || !self.lock_held(candidate.key) {
                    return false;
                }
                for index in 0..self.entries.len() {
                    let account = self.entries.get_mut(index).expect("account index");
                    if account.number == candidate.key {
                        match account.source {
                            Origin::SignIn { signing: true, .. } => {
                                account.subject_failure = Some(oauth::Failure::AnotherSubject);
                                self.down(oauth::Request::Cancel { account: index }, now, wall);
                                self.events.push(file::Event::Cancelled { owner: *owner });
                                return true;
                            }
                            Origin::SignIn { .. } | Origin::Env { .. } => {}
                        }
                    }
                }
            }
            file::Request::OpenPrivate { .. } | file::Request::Close { .. } => {}
            file::Request::Lock { .. }
            | file::Request::MakeDirectory { .. }
            | file::Request::Create { .. }
            | file::Request::CreateNoFollow { .. }
            | file::Request::OpenRead { .. }
            | file::Request::OpenReadNoFollow { .. }
            | file::Request::OpenWrite { .. }
            | file::Request::OpenWriteNoFollow { .. }
            | file::Request::OpenDirectory { .. }
            | file::Request::Scan { .. }
            | file::Request::Stat { .. }
            | file::Request::WriteAt { .. }
            | file::Request::ReadAt { .. }
            | file::Request::Cut { .. }
            | file::Request::Sync { .. }
            | file::Request::SyncDirectory { .. }
            | file::Request::Rename { .. }
            | file::Request::Remove { .. }
            | file::Request::List { .. }
            | file::Request::ListPage { .. } => unreachable!("private keeper file vocabulary"),
        }
        false
    }

    pub(super) fn record_event(&mut self, event: &file::Event) {
        if let file::Event::Loaded { owner, bytes } = event {
            for index in 0..self.reads.len() {
                let read = self.reads.get_mut(index).expect("read index");
                if read.owner == *owner {
                    read.subject = match skein_oauth::decode_record(bytes, &self.limits.client.document) {
                        Ok(record) => record.subject,
                        Err(_) => None,
                    };
                }
            }
        }
    }
}
