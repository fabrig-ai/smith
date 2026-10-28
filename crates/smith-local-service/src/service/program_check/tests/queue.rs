use super::*;

#[test]
fn the_queue_at_its_bound_preserves_arrival_order() {
    let mut fixture = Fixture::new();
    for account in 1..=3 {
        fixture.request(account);
        fixture.shell.pop().unwrap();
    }
    fixture.request(1);
    fixture.spawn();
    fixture.checks.cancel(&mut fixture.below);
    fixture.request(1);
    fixture.request(3);
    fixture.request(2);
    assert_eq!(fixture.checks.waiting.len(), 3);
    fixture.finish(b"", 1);
    fixture.refused(1, local::CredentialFailure::ProgramInterrupted);
    fixture.spawn();
    for account in [1, 3, 2] {
        fixture.finish(br#"{"loggedIn":true,"authMethod":"claude.ai"}"#, 0);
        fixture.shell_account(account);
        if account != 2 {
            fixture.spawn();
        }
    }
    assert!(fixture.checks.active.is_none());
    assert!(fixture.checks.waiting.is_empty());
}

#[test]
fn interruption_answers_the_queue_before_the_live_child_settles() {
    let mut fixture = Fixture::new();
    for account in 1..=2 {
        fixture.request(account);
        fixture.shell.pop().unwrap();
    }
    fixture.request(1);
    fixture.spawn();
    fixture.request(2);
    fixture.checks.cancel(&mut fixture.below);
    fixture.advance();
    fixture.refused(2, local::CredentialFailure::ProgramInterrupted);
    assert!(fixture.events.is_empty());
    assert!(fixture.checks.closing_root().is_none());
    let Some(io::Request::Signal { child: CHILD, signal: kernel::Signal::Kill, .. }) = fixture.below.pop() else {
        panic!("cancel kill")
    };
    fixture.finish(b"", 1);
    fixture.refused(1, local::CredentialFailure::ProgramInterrupted);
    assert!(fixture.shell.is_empty());
    assert_eq!(fixture.checks.closing_root(), Some(kernel::Fd::new(3)));
    assert!(!fixture.checks.closed());
    fixture.checks.root_closed();
    assert!(fixture.checks.closed());
}

#[test]
fn a_settling_check_leaves_room_for_every_live_account() {
    let mut service = service();
    let mut fixture = Fixture::new();
    fixture.checks = *service.below.process_mut().programs.take().unwrap();
    fixture.request(7);
    fixture.spawn();
    fixture.checks.cancel(&mut fixture.below);
    fixture.request(7);
    assert_eq!(fixture.checks.waiting.len(), 1);
    fixture.finish(b"", 1);
    fixture.refused(7, local::CredentialFailure::ProgramInterrupted);
    fixture.spawn();
    fixture.finish(b"", 1);
    fixture.refused(7, local::CredentialFailure::ProgramNotSignedIn);
    assert!(fixture.events.is_empty());
}

#[test]
fn an_older_waiter_runs_first_after_shell_backpressure() {
    let mut fixture = Fixture::new();
    for account in 1..=3 {
        fixture.request(account);
        fixture.shell.pop().unwrap();
    }
    fixture.request(1);
    fixture.spawn();
    fixture.request(2);
    for _ in 1..fixture.shell.capacity() {
        fixture.shell.push(local::Request::Credential { account: 99 });
    }
    fixture.finish(br#"{"loggedIn":true,"authMethod":"claude.ai"}"#, 0);
    assert!(fixture.checks.active.is_none());
    assert_eq!(fixture.shell.room(), 0);
    fixture.request(3);
    fixture.shell.pop().unwrap();
    fixture.advance();
    assert_eq!(fixture.checks.active.as_ref().unwrap().account, 2);
    assert_eq!(fixture.checks.waiting.pop(), Some(3));
}

#[test]
fn interruption_leaves_later_waiters_live() {
    let mut fixture = Fixture::new();
    for account in 1..=3 {
        fixture.request(account);
        fixture.shell.pop().unwrap();
    }
    fixture.request(1);
    fixture.spawn();
    fixture.request(2);
    fixture.checks.cancel(&mut fixture.below);
    fixture.request(3);
    fixture.events = Queue::uncapped();
    fixture.events.push(local::Event::Closed);
    assert!(fixture.checks.drain_interrupted(&mut fixture.events, true));
    fixture.events.pop().unwrap();
    fixture.advance();
    fixture.refused(2, local::CredentialFailure::ProgramInterrupted);
    fixture.finish(b"", 1);
    fixture.refused(1, local::CredentialFailure::ProgramInterrupted);
    fixture.spawn();
    fixture.finish(b"", 1);
    fixture.refused(3, local::CredentialFailure::ProgramNotSignedIn);
}
