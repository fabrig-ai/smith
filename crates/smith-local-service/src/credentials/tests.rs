use super::*;
use crate::CredentialRequest;
use skein_lib::Duration;

fn limits() -> oauth::Limits {
    oauth::Limits {
        accounts: 2,
        file_stall: Duration::from_secs(30),
        lock_wait: Duration::from_secs(1),
        exchanges: 1,
        listeners: 0,
        server: skein_http::server::Limits { head: 1024, headers: 8, body: 1024, read: 256, response: 1024, send: 256 },
        http: skein_http::client::Limits { request: 1024, head: 1024, headers: 8, read: 256, send: 256 },
        tls: skein_tls::client::Limits { read: 1024, send: 1024, records: 131_072 },
        io: skein_io::Limits {
            sockets: 1,
            names: 0,
            refusals: 1,
            intake: 32_768,
            receive: 1024,
            output: 32_768,
            sends: 1,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        },
        refresh_lead: Duration::from_secs(10),
        client: skein_oauth::ClientLimits {
            parameter_bytes: 0,
            resource_bytes: 0,
            nonce_bytes: 0,
            metadata_bytes: 0,
            key_set_bytes: 0,
            keys: 0,
            modulus_bits: 2048,
            skew: Duration::ZERO,
            document: skein_oauth::Limits {
                issued_client_bytes: 0,
                granted_bytes: 64,
                subject_bytes: 0,
                id_token_bytes: 256,
                document_bytes: 1024,
                string_bytes: 512,
                token_bytes: 256,
                client_bytes: 64,
                detail_bytes: 64,
                record_bytes: 1024,
                depth: 8,
                tokens: 64,
            },
            uri_bytes: 256,
            scope_bytes: 64,
            state_bytes: 64,
            code_bytes: 64,
            url_bytes: 1024,
            request_bytes: 1024,
            sign_in_time: Duration::from_secs(120),
            request_time: Duration::from_secs(20),
            backoff_base: Duration::from_secs(1),
            backoff_ceiling: Duration::from_secs(8),
            max_attempts: 3,
        },
    }
}

fn wall() -> Wall {
    Wall::from_nanos(100_000_000_000)
}
fn borrowed() -> Borrowed {
    Borrowed {
        record: skein_oauth::SavedToken {
            key: 0,
            generation: 1,
            access_token: b"header.eyJleHAiOjEyMH0.signature".as_slice().into(),
            refresh_token: None,
            issued_client_id: None,
            granted_scopes: None,
            subject: None,
            id_token: None,
            earliest_refresh_at: None,
            metadata: None,
            expires_at: Wall::from_nanos(120_000_000_000),
        },
        account_id: b"acc".as_slice().into(),
    }
}
fn source() -> Source {
    Source::Env { account: 7, borrowed: borrowed() }
}
fn accounts(source: Source) -> Accounts {
    let mut sources = List::with_capacity(1);
    assert!(sources.push(source).is_ok());
    Accounts::new(sources, limits(), [7; 32]).expect("handed-in owner")
}
fn settle(owner: &mut Accounts, now: Time, out: &mut Queue<CredentialRequest>) {
    for _ in 0..32_u32 {
        owner.advance(now, wall(), out);
        assert!(owner.submissions().is_empty(), "env accounts perform no file operations");
        if !owner.work_pending(now) {
            return;
        }
    }
    panic!("bounded account transitions settle");
}
fn ready(
    out: &mut Queue<CredentialRequest>,
    generation: u64,
    seconds: u64,
    value: &[u8],
    expected: smith_host_domain::Form,
) {
    let Some(CredentialRequest::Ready { event: local::Event::Credential { grant, form }, value: token }) = out.pop()
    else {
        panic!("one ready grant")
    };
    assert_eq!(form, expected);
    assert_eq!(grant.name, smith_domain::GrantName { account: 7, generation });
    assert_eq!(grant.valid, Duration::from_secs(seconds));
    assert!(token.as_ref() == value, "the lent access value matches the fixture");
}

#[test]
fn an_env_account_lends_the_handed_in_value_without_a_file_operation() {
    for (identifier, form) in [
        (b"acc".as_slice(), smith_host_domain::Form::Withdrawn),
        (b"".as_slice(), smith_host_domain::Form::Credential(smith_host_domain::Scheme::Bearer)),
    ] {
        let mut borrowed = borrowed();
        borrowed.account_id = Box::from(identifier);
        let mut owner = accounts(Source::Env { account: 7, borrowed });
        let mut out = Queue::with_capacity(ACCOUNTS_MAX_OUT);
        assert!(owner.request(7));
        settle(&mut owner, Time::ZERO, &mut out);
        ready(&mut out, 1, 20, b"header.eyJleHAiOjEyMH0.signature", form);
        assert!(owner.submissions().is_empty());
    }
}

#[test]
fn an_env_grant_announces_expiry() {
    let mut owner = accounts(source());
    let mut out = Queue::with_capacity(ACCOUNTS_MAX_OUT);
    assert!(owner.request(7));
    settle(&mut owner, Time::ZERO, &mut out);
    ready(&mut out, 1, 20, b"header.eyJleHAiOjEyMH0.signature", smith_host_domain::Form::Withdrawn);
    owner.advance(Time::from_nanos(10_000_000_000), wall(), &mut out);
    let Some(CredentialRequest::Notice { account: 7, reason: local::CredentialFailure::BorrowedExpiring { .. } }) =
        out.pop()
    else {
        panic!("expiry lead notice");
    };
    owner.advance(Time::from_nanos(20_000_000_000), wall(), &mut out);
    let Some(CredentialRequest::Failed {
        event: local::Event::NoCredential { account: 7, reason: local::CredentialFailure::Missing },
    }) = out.pop()
    else {
        panic!("expiry ends the held grant");
    };
}

#[test]
fn releasing_a_finished_runs_grants_prevents_a_later_grant_failure() {
    let mut owner = accounts(source());
    let mut out = Queue::with_capacity(ACCOUNTS_MAX_OUT);
    assert!(owner.request(7));
    settle(&mut owner, Time::ZERO, &mut out);
    ready(&mut out, 1, 20, b"header.eyJleHAiOjEyMH0.signature", smith_host_domain::Form::Withdrawn);
    owner.release_all(Time::ZERO, wall());
    owner.advance(Time::from_nanos(20_000_000_000), wall(), &mut out);
    assert!(out.is_empty());
    assert!(owner.next_deadline().is_none());
}

#[test]
fn a_handed_in_refresh_token_is_refused_before_the_owner_starts() {
    let mut borrowed = borrowed();
    borrowed.record.refresh_token = Some(b"refresh".as_slice().into());
    let mut sources = List::with_capacity(1);
    assert!(sources.push(Source::Env { account: 7, borrowed }).is_ok());
    assert!(Accounts::new(sources, limits(), [7; 32]).is_err());
}

#[test]
fn zero_file_stall_is_refused_before_a_file_owner_is_constructed() {
    let mut sources = List::with_capacity(1);
    assert!(sources.push(source()).is_ok());
    let mut bounds = limits();
    bounds.file_stall = Duration::ZERO;
    assert!(Accounts::new(sources, bounds, [7; 32]).is_err());
}

mod private;

#[test]
fn invalid_limits_are_refused() {
    let base = limits();
    assert!(accounts_valid(&base));
    let changes: &[fn(&mut oauth::Limits)] = &[
        |limits| limits.accounts = u32::MAX,
        |limits| limits.accounts = u32::MAX / 4 + 1,
        |limits| limits.io.intake = 0,
        |limits| limits.io.sockets = u32::MAX,
        |limits| limits.client.parameter_bytes = u32::MAX,
        |limits| limits.client.document.token_bytes = u32::MAX,
        |limits| limits.http.head = 1,
        |limits| limits.http.read = 0,
        |limits| limits.http.send = 0,
        |limits| limits.tls.read = 0,
        |limits| limits.tls.send = 0,
        |limits| limits.tls.records = skein_tls::client::MAX_RECORD - 1,
        |limits| limits.tls.read = u32::MAX,
        |limits| limits.tls.send = u32::MAX,
    ];
    for change in changes {
        let mut invalid = base;
        change(&mut invalid);
        assert!(!accounts_valid(&invalid), "invalid: {invalid:?}");
        assert_eq!(validate_sources(&List::with_capacity(0), &invalid), Err(crate::Error::Memory));
    }
}
