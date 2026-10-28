use crate::{Git, GitAction, GitCompletion, GitLimits};
use alloc::boxed::Box;
use smith_domain::run;
use smith_local_domain::{GitOp, GitResult};

fn limits() -> GitLimits {
    GitLimits { argument_bytes: 4096, output_bytes: 64, conflicts: 2, path_bytes: 64, detail_bytes: 32 }
}

fn command(code: Option<u8>, stdout: &[u8], timed_out: bool) -> GitCompletion {
    GitCompletion::Command { code, stdout: stdout.into(), stderr: Box::new([]), timed_out }
}

#[test]
fn a_commit_deadline_or_output_overflow_is_uncertain_and_never_reissued() {
    for (stdout, timed_out) in [(b"".as_slice(), true), ([b'x'; 65].as_slice(), false)] {
        let (mut git, _) = Git::new(GitOp::Commit { message: b"Title".as_slice().into() }, limits()).unwrap();
        let GitAction::Command { .. } = git.complete(command(Some(0), b"", false)) else { panic!("commit command") };
        let GitAction::Done(GitResult::Uncertain { .. }) = git.complete(command(Some(0), stdout, timed_out)) else {
            panic!("uncertain terminal")
        };
    }
}

#[test]
fn an_unreadable_head_after_a_successful_commit_is_uncertain() {
    let (mut git, _) = Git::new(GitOp::Commit { message: b"Title".as_slice().into() }, limits()).unwrap();
    git.complete(command(Some(0), b"", false));
    git.complete(command(Some(0), b"", false));
    let GitAction::Done(GitResult::Uncertain { .. }) = git.complete(command(Some(1), b"", false)) else {
        panic!("uncertain terminal")
    };
}

#[test]
fn failed_staging_has_no_commit_effect() {
    let (mut git, _) = Git::new(GitOp::Commit { message: b"Title".as_slice().into() }, limits()).unwrap();
    let GitAction::Done(GitResult::NoEffect { .. }) = git.complete(command(Some(1), b"", false)) else {
        panic!("no effect terminal")
    };
}

#[test]
fn an_absent_trailer_reads_the_unchanged_head_as_no_named_commit() {
    let name = run::CallName { activation: 7, completion: 3, position: 1 };
    let (mut git, _) = Git::new(GitOp::Inspect { name }, limits()).unwrap();
    let GitAction::Command { args } = git.complete(command(Some(0), b"", false)) else { panic!("head inspection") };
    assert_eq!(args[0].as_ref(), b"rev-parse");
    let GitAction::Done(GitResult::Inspected { head, named: false }) =
        git.complete(command(Some(0), b"01234567\n", false))
    else {
        panic!("absent inspection")
    };
    assert_eq!(head.as_ref(), b"01234567");
}

#[test]
fn a_trailer_with_a_longer_position_does_not_name_this_delivery() {
    let name = run::CallName { activation: 7, completion: 3, position: 1 };
    let output = b"01234567\0Title\n\nSmith-Delivery: 7/3/10\n";
    let (mut git, _) = Git::new(GitOp::Inspect { name }, limits()).unwrap();
    let GitAction::Done(GitResult::Inspected { named: false, .. }) = git.complete(command(Some(0), output, false))
    else {
        panic!("longer name is not this delivery")
    };
}

#[test]
fn marker_reads_visit_original_paths_in_order_and_stop_at_the_first_marker() {
    let owner = skein_lib::Token::new(17);
    let root = skein_lib::Token::new(19);
    let paths = Box::new([
        Box::from(b"first.rs".as_slice()),
        Box::from(b"second.rs".as_slice()),
        Box::from(b"third.rs".as_slice()),
    ]);
    let mut marker_limits = limits();
    marker_limits.conflicts = 3;
    let mut markers = crate::Markers::new(owner, root, paths, skein_lib::Time::from_nanos(50), marker_limits).unwrap();
    let crate::MarkerAction::File { request: skein_io::file::Request::Load { path, no_follow: true, .. }, .. } =
        markers.start()
    else {
        panic!("first original path")
    };
    assert_eq!(path.as_ref(), b"first.rs");
    let crate::MarkerAction::File { request: skein_io::file::Request::Load { path, .. }, .. } =
        markers.from_file(skein_io::file::Event::Loaded { owner, bytes: b"clean\n".as_slice().into() })
    else {
        panic!("second original path")
    };
    assert_eq!(path.as_ref(), b"second.rs");
    let crate::MarkerAction::Done(GitResult::Markers { first: Some(path) }) =
        markers.from_file(skein_io::file::Event::Loaded {
            owner,
            bytes: b"<<<<<<< ours\nchanged\n=======\n".as_slice().into(),
        })
    else {
        panic!("first remaining marker")
    };
    assert_eq!(path.as_ref(), b"second.rs");
}

#[test]
fn a_deleted_original_conflict_file_is_resolved() {
    let owner = skein_lib::Token::new(17);
    let mut markers = crate::Markers::new(
        owner,
        skein_lib::Token::new(19),
        Box::new([Box::from(b"removed.rs".as_slice())]),
        skein_lib::Time::from_nanos(50),
        limits(),
    )
    .unwrap();
    drop(markers.start());
    let crate::MarkerAction::Done(GitResult::Markers { first: None }) =
        markers.from_file(skein_io::file::Event::Failed {
            owner,
            error: skein_io::kernel::Error::NotFound,
            committed: false,
            residue: None,
        })
    else {
        panic!("deleted conflict resolved")
    };
}

#[test]
fn a_marker_read_over_its_bound_is_failed_instead_of_declared_clean() {
    let owner = skein_lib::Token::new(17);
    let mut markers = crate::Markers::new(
        owner,
        skein_lib::Token::new(19),
        Box::new([Box::from(b"large.rs".as_slice())]),
        skein_lib::Time::from_nanos(50),
        limits(),
    )
    .unwrap();
    drop(markers.start());
    let crate::MarkerAction::Done(GitResult::Failed { reason: run::DeliveryReason::TooLarge, .. }) =
        markers.from_file(skein_io::file::Event::TooLarge { owner, size: 65 })
    else {
        panic!("oversized file is not clean")
    };
}

fn plain_snapshot(files: &[(&[u8], &[u8])]) -> crate::Snapshot {
    let owner = skein_lib::Token::new(17);
    let mut scan = crate::Plain::new(
        owner,
        skein_lib::Token::new(19),
        crate::PlainLimits { entries: 8, path_bytes: 64, file_bytes: 128 },
        skein_lib::Time::from_nanos(50),
    )
    .unwrap();
    let crate::PlainAction::File { request: skein_io::file::Request::Scan { no_follow: true, .. }, .. } = scan.start()
    else {
        panic!("root scan")
    };
    let mut entries = skein_lib::List::with_capacity(u32::try_from(files.len()).unwrap());
    for (name, _) in files {
        entries.push(skein_io::file::Entry { name: Box::from(*name), kind: skein_io::kernel::Kind::File }).unwrap();
    }
    let mut action = scan.from_file(skein_io::file::Event::Scanned { owner, entries: entries.into_boxed(), more: 0 });
    for (name, bytes) in files {
        let crate::PlainAction::File { request: skein_io::file::Request::Load { path, no_follow: true, .. }, .. } =
            action
        else {
            panic!("one whole file load")
        };
        assert_eq!(path.as_ref(), *name);
        action = scan.from_file(skein_io::file::Event::Loaded { owner, bytes: Box::from(*bytes) });
    }
    let crate::PlainAction::Done = action else { panic!("complete snapshot") };
    scan.finish()
}

#[test]
fn plain_snapshots_compare_contents_additions_and_deletions() {
    let original = plain_snapshot(&[(b"a", b"old"), (b"b", b"same")]);
    assert_eq!(original, plain_snapshot(&[(b"a", b"old"), (b"b", b"same")]));
    assert_ne!(original, plain_snapshot(&[(b"a", b"new"), (b"b", b"same")]));
    assert_ne!(original, plain_snapshot(&[(b"a", b"old")]));
    assert_ne!(original, plain_snapshot(&[(b"a", b"old"), (b"b", b"same"), (b"c", b"new")]));
}

#[test]
fn plain_snapshots_walk_nested_directories() {
    let owner = skein_lib::Token::new(17);
    let mut scan = crate::Plain::new(
        owner,
        skein_lib::Token::new(19),
        crate::PlainLimits { entries: 8, path_bytes: 64, file_bytes: 128 },
        skein_lib::Time::from_nanos(50),
    )
    .unwrap();
    drop(scan.start());
    let action = scan.from_file(skein_io::file::Event::Scanned {
        owner,
        entries: Box::new([skein_io::file::Entry {
            name: b"src".as_slice().into(),
            kind: skein_io::kernel::Kind::Directory,
        }]),
        more: 0,
    });
    let crate::PlainAction::File { request: skein_io::file::Request::Scan { path, .. }, .. } = action else {
        panic!("nested scan")
    };
    assert_eq!(path.as_ref(), b"src");
    let action = scan.from_file(skein_io::file::Event::Scanned {
        owner,
        entries: Box::new([skein_io::file::Entry {
            name: b"lib.rs".as_slice().into(),
            kind: skein_io::kernel::Kind::File,
        }]),
        more: 0,
    });
    let crate::PlainAction::File { request: skein_io::file::Request::Load { path, .. }, .. } = action else {
        panic!("nested load")
    };
    assert_eq!(path.as_ref(), b"src/lib.rs");
    let crate::PlainAction::Done = scan.from_file(skein_io::file::Event::Loaded { owner, bytes: Box::new([]) }) else {
        panic!("complete snapshot")
    };
}

#[test]
fn an_incomplete_or_unsupported_plain_tree_makes_the_snapshot_unknown() {
    let owner = skein_lib::Token::new(17);
    let empty: Box<[skein_io::file::Entry]> = Box::new([]);
    for (entries, more) in [
        (empty, 1_u64),
        (
            Box::new([skein_io::file::Entry {
                name: b"link".as_slice().into(),
                kind: skein_io::kernel::Kind::Symlink,
            }]),
            0,
        ),
        (
            Box::new([skein_io::file::Entry {
                name: b"../escape".as_slice().into(),
                kind: skein_io::kernel::Kind::File,
            }]),
            0,
        ),
    ] {
        let mut scan = crate::Plain::new(
            owner,
            skein_lib::Token::new(19),
            crate::PlainLimits { entries: 2, path_bytes: 64, file_bytes: 128 },
            skein_lib::Time::from_nanos(50),
        )
        .unwrap();
        drop(scan.start());
        let crate::PlainAction::Failed = scan.from_file(skein_io::file::Event::Scanned { owner, entries, more }) else {
            panic!("incomplete tree must be unknown, never unchanged")
        };
    }
}

mod store;

#[test]
fn invalid_limits_are_refused() {
    let owner = skein_lib::Token::new(1);
    let root = skein_lib::Token::new(2);
    let deadline = skein_lib::Time::from_nanos(100);
    assert!(Git::new(GitOp::Head, limits()).is_some(), "valid Git limits");
    for check in 0_u8..3 {
        let mut invalid = limits();
        match check {
            0 => invalid.output_bytes = 0,
            1 => invalid.conflicts = 0,
            2 => invalid.path_bytes = 0,
            _ => unreachable!(),
        }
        assert!(Git::new(GitOp::Head, invalid).is_none(), "positive Git limit {check}");
    }
    let plain = crate::PlainLimits { entries: 2, path_bytes: 32, file_bytes: 1024 };
    assert!(crate::Plain::new(owner, root, plain, deadline).is_some(), "valid scan limits");
    for check in 0_u8..3 {
        let mut invalid = plain;
        match check {
            0 => invalid.entries = 0,
            1 => invalid.path_bytes = 0,
            2 => invalid.file_bytes = 0,
            _ => unreachable!(),
        }
        assert!(crate::Plain::new(owner, root, invalid, deadline).is_none(), "positive scan limit {check}");
    }
    let terminal = crate::TerminalLimits { line_bytes: 32, show_bytes: 64 };
    assert!(crate::terminal_valid(&terminal));
    assert!(crate::Terminal::new(terminal).is_some(), "valid terminal limits");
    assert!(crate::Terminal::new(crate::TerminalLimits { line_bytes: 0, ..terminal }).is_none(), "positive line limit");
    assert!(
        crate::Terminal::new(crate::TerminalLimits { show_bytes: 0, ..terminal }).is_none(),
        "positive display limit"
    );
    let store = crate::StoreLimits { file_bytes: 4096, turns: 2, deliveries: 2, scan_bytes: 4096 };
    let construct = |limits| {
        crate::Store::new(
            owner,
            root,
            skein_io::kernel::Fd::new(3),
            limits,
            smith_protocol_channel::Endpoints::new(skein_lib::List::with_capacity(0)),
        )
    };
    assert!(crate::Store::valid(&store));
    assert!(construct(store).is_some(), "valid store limits");
    for invalid in [
        crate::StoreLimits { file_bytes: 0, ..store },
        crate::StoreLimits { turns: 0, ..store },
        crate::StoreLimits { deliveries: 0, ..store },
        crate::StoreLimits { scan_bytes: 0, ..store },
        crate::StoreLimits { turns: u32::MAX - 5, ..store },
        crate::StoreLimits { turns: u32::MAX, ..store },
    ] {
        assert!(!crate::Store::valid(&invalid));
        assert!(construct(invalid).is_none(), "store file, scan and record limits are checked");
    }
    assert!(
        construct(crate::StoreLimits { scan_bytes: u64::MAX, ..store }).is_some(),
        "aggregate scan ownership is not an edge overflow"
    );
}
