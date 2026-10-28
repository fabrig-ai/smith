//! Git operations for a local delivery (protocol/hosts.md, section 5.5;
//! domain/host.md, section 8). This adapter retains one typed operation and
//! bounded stdout, never a child handle. The service runs each command as a
//! child of the host in the named workspace directory, without a shell. Its
//! command completion enters `complete`; `start` yields the first request.
//! The local domain decides stale heads, intents, recovery and commit policy.
//!
//! | Operation | Commands | Terminal |
//! |---|---|---|
//! | Head | rev-parse HEAD | Head |
//! | Status | rev-parse HEAD, status, verify MERGE_HEAD | Status |
//! | Inspect | log by trailer, rev-parse HEAD if absent | Inspected |
//! | Markers | read named conflict files | Markers |
//! | Commit | add, commit, rev-parse HEAD | Committed |
//! | Push | push | Pushed, Stale or Failed |

use alloc::boxed::Box;
use skein_lib::{List, bytes};
use smith_domain::run;
use smith_local_domain::{GitOp, GitResult};

/// Bounds for one git child adapter.
#[derive(Clone, Copy, Debug)]
pub struct GitLimits {
    /// Maximum total owned message or push-target argument bytes.
    pub argument_bytes: u32,
    /// Maximum status or inspection stdout bytes.
    pub output_bytes: u32,
    /// Maximum conflicted paths to retain.
    pub conflicts: u32,
    /// Maximum bytes in one path.
    pub path_bytes: u32,
    /// Maximum git diagnostic tail bytes.
    pub detail_bytes: u32,
}

/// A child command or filesystem marker read for the local service.
#[derive(Debug)]
pub enum GitAction {
    /// Run git in the requested workspace directory, with separate arguments.
    Command { args: Box<[Box<[u8]>]> },
    /// Inspect only these original conflict paths for remaining markers.
    Markers { paths: Box<[Box<[u8]>]> },
    /// One terminal for the local domain's typed operation.
    Done(GitResult),
}

/// One terminal for the child or marker read requested by this adapter.
#[derive(Debug)]
pub enum GitCompletion {
    /// Child exit with bounded stdout/stderr; `code` is absent after a signal.
    Command { code: Option<u8>, stdout: Box<[u8]>, stderr: Box<[u8]>, timed_out: bool },
    /// First original conflicted file still containing a marker.
    Markers { first: Option<Box<[u8]>> },
}

#[derive(Debug)]
enum Step {
    Head,
    StatusHead,
    StatusBody { head: Box<[u8]> },
    StatusMerge { head: Box<[u8]>, changed: bool, paths: Box<[Box<[u8]>]> },
    Inspect { name: run::CallName },
    InspectHead,
    Markers { paths: Box<[Box<[u8]>]> },
    Add { message: Box<[u8]> },
    Commit { message: Box<[u8]> },
    CommitHead,
    Push { remote: Box<[u8]>, branch: Box<[u8]> },
    Done,
}

/// One typed git operation run through bounded child commands.
#[derive(Debug)]
pub struct Git {
    limits: GitLimits,
    step: Step,
}

impl Git {
    /// Begin one operation; the service must settle each returned child request.
    #[must_use]
    pub fn new(op: GitOp, limits: GitLimits) -> Option<(Self, GitAction)> {
        if limits.output_bytes == 0 || limits.conflicts == 0 || limits.path_bytes == 0 {
            return None;
        }
        let arguments = match &op {
            GitOp::Commit { message } => message.len(),
            GitOp::Push { remote, branch } => remote.len().checked_add(branch.len())?,
            GitOp::Head | GitOp::Status | GitOp::Inspect { .. } | GitOp::Markers { .. } => 0,
        };
        if arguments > usize::try_from(limits.argument_bytes).ok()? {
            return None;
        }
        let step = match op {
            GitOp::Head => Step::Head,
            GitOp::Status => Step::StatusHead,
            GitOp::Inspect { name } => Step::Inspect { name },
            GitOp::Markers { paths } => Step::Markers { paths },
            GitOp::Commit { message } => Step::Add { message },
            GitOp::Push { remote, branch } => Step::Push { remote, branch },
        };
        let git = Self { limits, step };
        let action = git.request();
        Some((git, action))
    }

    /// Take a child terminal and either issue the next command or return one domain terminal.
    pub fn complete(&mut self, completion: GitCompletion) -> GitAction {
        let step = core::mem::replace(&mut self.step, Step::Done);
        match completion {
            GitCompletion::Markers { first } => match step {
                Step::Markers { .. } => GitAction::Done(GitResult::Markers { first }),
                Step::Head
                | Step::StatusHead
                | Step::StatusBody { .. }
                | Step::StatusMerge { .. }
                | Step::Inspect { .. }
                | Step::InspectHead
                | Step::Add { .. }
                | Step::Commit { .. }
                | Step::CommitHead
                | Step::Push { .. }
                | Step::Done => unreachable!("marker terminal answers marker request"),
            },
            GitCompletion::Command { code, stdout, stderr, timed_out } => {
                if timed_out {
                    return GitAction::Done(step_failure(&step, run::DeliveryReason::TimedOut, &stderr, &self.limits));
                }
                if stdout.len() > usize::try_from(self.limits.output_bytes).expect("u32 fits usize") {
                    return GitAction::Done(step_failure(&step, run::DeliveryReason::TooLarge, &stderr, &self.limits));
                }
                self.command_done(step, code, &stdout, &stderr)
            }
        }
    }

    fn request(&self) -> GitAction {
        let args: Box<[Box<[u8]>]> = match &self.step {
            Step::Head | Step::StatusHead | Step::CommitHead | Step::InspectHead => {
                Box::new([Box::from(&b"rev-parse"[..]), Box::from(&b"HEAD"[..])])
            }
            Step::StatusBody { .. } => Box::new([
                Box::from(&b"status"[..]),
                Box::from(&b"--porcelain=v1"[..]),
                Box::from(&b"-z"[..]),
                Box::from(&b"--untracked-files=all"[..]),
            ]),
            Step::StatusMerge { .. } => Box::new([
                Box::from(&b"rev-parse"[..]),
                Box::from(&b"-q"[..]),
                Box::from(&b"--verify"[..]),
                Box::from(&b"MERGE_HEAD"[..]),
            ]),
            Step::Inspect { name } => {
                let mut pattern = List::with_capacity(96);
                for byte in b"--grep=^Smith-Delivery: " {
                    pattern.push(*byte).expect("trailer pattern bound");
                }
                decimal(&mut pattern, name.activation);
                pattern.push(b'/').expect("trailer pattern bound");
                decimal(&mut pattern, u64::from(name.completion));
                pattern.push(b'/').expect("trailer pattern bound");
                decimal(&mut pattern, u64::from(name.position));
                pattern.push(b'$').expect("trailer pattern bound");
                Box::new([
                    Box::from(&b"log"[..]),
                    Box::from(&b"-1"[..]),
                    Box::from(&b"--format=%H%x00%B"[..]),
                    pattern.into_boxed(),
                    Box::from(&b"HEAD"[..]),
                ])
            }
            Step::Add { .. } => Box::new([Box::from(&b"add"[..]), Box::from(&b"-A"[..])]),
            Step::Commit { message } => Box::new([
                Box::from(&b"-c"[..]),
                Box::from(&b"core.hooksPath=/dev/null"[..]),
                Box::from(&b"commit"[..]),
                Box::from(&b"--no-gpg-sign"[..]),
                Box::from(&b"-m"[..]),
                message.clone(),
            ]),
            Step::Push { remote, branch } => {
                Box::new([Box::from(&b"push"[..]), Box::from(&b"--porcelain"[..]), remote.clone(), target(branch)])
            }
            Step::Markers { paths } => return GitAction::Markers { paths: paths.clone() },
            Step::Done => unreachable!("a finished git operation has no request"),
        };
        GitAction::Command { args }
    }

    fn command_done(&mut self, step: Step, code: Option<u8>, stdout: &[u8], stderr: &[u8]) -> GitAction {
        match step {
            Step::Head => match clean_head(code, stdout) {
                Some(head) => GitAction::Done(GitResult::Head { head }),
                None => GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits)),
            },
            Step::StatusHead => match clean_head(code, stdout) {
                Some(head) => self.advance(Step::StatusBody { head }),
                None => GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits)),
            },
            Step::StatusBody { head } => {
                if code != Some(0) {
                    return GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits));
                }
                let Ok(paths) = conflicts(stdout, &self.limits) else {
                    return GitAction::Done(failed(run::DeliveryReason::TooLarge, stderr, &self.limits));
                };
                self.advance(Step::StatusMerge { head, changed: !stdout.is_empty(), paths })
            }
            Step::StatusMerge { head, changed, paths } => {
                let merging = match code {
                    Some(0) => Some(paths),
                    Some(1) => None,
                    Some(_) | None => {
                        return GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits));
                    }
                };
                GitAction::Done(GitResult::Status { changed, merging, head })
            }
            Step::InspectHead => match clean_head(code, stdout) {
                Some(head) => GitAction::Done(GitResult::Inspected { head, named: false }),
                None => GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits)),
            },
            Step::Inspect { name: _ } if code == Some(0) && stdout.is_empty() => self.advance(Step::InspectHead),
            Step::Inspect { name } => match inspect(code, stdout, name) {
                Some((head, named)) => GitAction::Done(GitResult::Inspected { head, named }),
                None => GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits)),
            },
            Step::Add { message } => {
                if code == Some(0) {
                    self.advance(Step::Commit { message })
                } else {
                    GitAction::Done(no_effect(run::DeliveryReason::Broken, stderr, &self.limits))
                }
            }
            Step::Commit { .. } => {
                if code == Some(0) {
                    self.advance(Step::CommitHead)
                } else {
                    GitAction::Done(uncertain(run::DeliveryReason::Broken, stderr, &self.limits))
                }
            }
            Step::CommitHead => match clean_head(code, stdout) {
                Some(head) => {
                    let mut text = List::with_capacity(512);
                    for byte in b"commit ".iter().chain(head.iter()) {
                        text.push(*byte).expect("bounded hash receipt");
                    }
                    GitAction::Done(GitResult::Committed { receipt: text.into_boxed(), head })
                }
                None => GitAction::Done(uncertain(run::DeliveryReason::Broken, stderr, &self.limits)),
            },
            Step::Push { .. } => {
                if code == Some(0) {
                    GitAction::Done(GitResult::Pushed)
                } else if bytes::find(stderr, b"non-fast-forward").is_some()
                    || bytes::find(stderr, b"rejected").is_some()
                {
                    GitAction::Done(GitResult::Stale)
                } else {
                    GitAction::Done(failed(run::DeliveryReason::Broken, stderr, &self.limits))
                }
            }
            Step::Markers { .. } | Step::Done => unreachable!("command terminal answers an issued git child"),
        }
    }

    fn advance(&mut self, next: Step) -> GitAction {
        self.step = next;
        self.request()
    }
}

fn target(branch: &[u8]) -> Box<[u8]> {
    let mut out = List::with_capacity(u32::try_from(branch.len()).unwrap_or(u32::MAX).saturating_add(16));
    for byte in b"HEAD:refs/heads/".iter().chain(branch.iter()) {
        out.push(*byte).expect("target capacity includes branch");
    }
    out.into_boxed()
}

fn clean_head(code: Option<u8>, stdout: &[u8]) -> Option<Box<[u8]>> {
    if code != Some(0) || stdout.len() < 8 || stdout.len() > 65 {
        return None;
    }
    let head = stdout.strip_suffix(b"\n")?;
    if head.is_empty() || !is_hex(head) {
        return None;
    }
    Some(Box::from(head))
}

fn inspect(code: Option<u8>, stdout: &[u8], name: run::CallName) -> Option<(Box<[u8]>, bool)> {
    if code != Some(0) {
        return None;
    }
    let split = bytes::find(stdout, b"\0")?;
    let head = stdout.get(..split)?;
    if head.is_empty() || head.len() > 64 || !is_hex(head) {
        return None;
    }
    let message = stdout.get(split.saturating_add(1)..)?;
    let mut trailer = List::with_capacity(80);
    for byte in b"Smith-Delivery: " {
        trailer.push(*byte).expect("trailer bound");
    }
    decimal(&mut trailer, name.activation);
    trailer.push(b'/').expect("trailer bound");
    decimal(&mut trailer, u64::from(name.completion));
    trailer.push(b'/').expect("trailer bound");
    decimal(&mut trailer, u64::from(name.position));
    let mut named = false;
    let mut start = 0_usize;
    for end in 0..message.len() {
        if message.get(end) == Some(&b'\n') {
            if message.get(start..end) == Some(trailer.as_slice()) {
                named = true;
            }
            start = end.checked_add(1).expect("bounded message offset");
        }
    }
    if message.get(start..) == Some(trailer.as_slice()) {
        named = true;
    }
    Some((Box::from(head), named))
}

fn is_hex(bytes: &[u8]) -> bool {
    for byte in bytes {
        if !byte.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

fn decimal(out: &mut List<u8>, mut number: u64) {
    let mut digits = [0_u8; 20];
    let mut count = 0_usize;
    for _ in 0_u8..20_u8 {
        *digits.get_mut(count).expect("twenty decimal digits fit") =
            b'0'.checked_add(u8::try_from(number % 10).expect("digit")).expect("ASCII digit");
        count = count.saturating_add(1);
        number /= 10;
        if number == 0 {
            break;
        }
    }
    for digit in digits.get(..count).expect("twenty decimal digits fit").iter().rev() {
        out.push(*digit).expect("trailer bound");
    }
}

fn conflicts(stdout: &[u8], limits: &GitLimits) -> Result<Box<[Box<[u8]>]>, ()> {
    let mut paths = List::with_capacity(limits.conflicts);
    let mut rest = stdout;
    for _ in 0..limits.output_bytes {
        if rest.is_empty() {
            return Ok(paths.into_boxed());
        }
        let end = bytes::find(rest, b"\0").ok_or(())?;
        let entry = rest.get(..end).ok_or(())?;
        if entry.len() < 4 || entry.get(2) != Some(&b' ') {
            return Err(());
        }
        let status = entry.get(..2).ok_or(())?;
        let path = entry.get(3..).ok_or(())?;
        let conflicted = status.contains(&b'U') || status == b"AA" || status == b"DD";
        if conflicted {
            if path.len() > usize::try_from(limits.path_bytes).expect("u32 fits usize") {
                return Err(());
            }
            paths.push(Box::from(path)).or(Err(()))?;
        }
        rest = rest.get(end.saturating_add(1)..).ok_or(())?;
        if status.contains(&b'R') || status.contains(&b'C') {
            let original_end = bytes::find(rest, b"\0").ok_or(())?;
            rest = rest.get(original_end.saturating_add(1)..).ok_or(())?;
        }
    }
    Err(())
}

fn step_failure(step: &Step, reason: run::DeliveryReason, stderr: &[u8], limits: &GitLimits) -> GitResult {
    match step {
        Step::Add { .. } => no_effect(reason, stderr, limits),
        Step::Commit { .. } | Step::CommitHead | Step::Push { .. } => uncertain(reason, stderr, limits),
        Step::Head
        | Step::StatusHead
        | Step::StatusBody { .. }
        | Step::StatusMerge { .. }
        | Step::Inspect { .. }
        | Step::InspectHead
        | Step::Markers { .. }
        | Step::Done => failed(reason, stderr, limits),
    }
}

fn no_effect(reason: run::DeliveryReason, stderr: &[u8], limits: &GitLimits) -> GitResult {
    GitResult::NoEffect { reason, diagnostic: diagnostic(stderr, limits) }
}

fn uncertain(reason: run::DeliveryReason, stderr: &[u8], limits: &GitLimits) -> GitResult {
    GitResult::Uncertain { reason, diagnostic: diagnostic(stderr, limits) }
}

fn failed(reason: run::DeliveryReason, stderr: &[u8], limits: &GitLimits) -> GitResult {
    GitResult::Failed { reason, diagnostic: diagnostic(stderr, limits) }
}

fn diagnostic(stderr: &[u8], limits: &GitLimits) -> Box<run::Diagnostic> {
    let tail = if stderr.len() > usize::try_from(limits.detail_bytes).expect("u32 fits usize") {
        stderr
            .get(stderr.len().saturating_sub(usize::try_from(limits.detail_bytes).expect("u32 fits usize"))..)
            .expect("tail starts within diagnostic")
    } else {
        stderr
    };
    let dropped = stderr.len().saturating_sub(tail.len());
    Box::new(run::Diagnostic::new(tail, u64::try_from(dropped).unwrap_or(u64::MAX)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &[u8] = b"0123456789abcdef0123456789abcdef01234567";

    fn limits() -> GitLimits {
        GitLimits { argument_bytes: 4096, output_bytes: 4096, conflicts: 4, path_bytes: 128, detail_bytes: 128 }
    }

    fn command(code: Option<u8>, stdout: &[u8], stderr: &[u8]) -> GitCompletion {
        GitCompletion::Command { code, stdout: Box::from(stdout), stderr: Box::from(stderr), timed_out: false }
    }

    fn head_line() -> Box<[u8]> {
        let mut out = List::with_capacity(41);
        for byte in HASH.iter().chain(b"\n".iter()) {
            out.push(*byte).expect("hash line");
        }
        out.into_boxed()
    }

    #[test]
    fn status_checks_head_and_merge_conflicts_before_the_domain_sees_it() {
        let Some((mut git, GitAction::Command { .. })) = Git::new(GitOp::Status, limits()) else {
            panic!("head command")
        };
        let GitAction::Command { args } = git.complete(command(Some(0), &head_line(), b"")) else {
            panic!("status command")
        };
        assert_eq!(args.first().expect("git verb").as_ref(), b"status");
        let GitAction::Command { args } = git.complete(command(Some(0), b"UU src/main.rs\0?? extra\0", b"")) else {
            panic!("merge-head query")
        };
        assert_eq!(args.first().expect("git verb").as_ref(), b"rev-parse");
        let GitAction::Done(GitResult::Status { changed, merging: Some(paths), head }) =
            git.complete(command(Some(0), &head_line(), b""))
        else {
            panic!("typed status")
        };
        assert!(changed);
        assert_eq!(head.as_ref(), HASH);
        assert_eq!(paths.as_ref(), &[Box::from(&b"src/main.rs"[..])]);
    }

    #[test]
    fn commit_runs_add_then_commit_then_reads_its_new_head() {
        let Some((mut git, GitAction::Command { args })) =
            Git::new(GitOp::Commit { message: Box::from(&b"Title\n\nSmith-Delivery: 7/3/1"[..]) }, limits())
        else {
            panic!("add command")
        };
        assert_eq!(args.first().expect("git verb").as_ref(), b"add");
        let GitAction::Command { args } = git.complete(command(Some(0), b"", b"")) else { panic!("commit command") };
        assert_eq!(args.get(2).expect("git verb").as_ref(), b"commit");
        let GitAction::Command { args } = git.complete(command(Some(0), b"", b"")) else { panic!("new head command") };
        assert_eq!(args.first().expect("git verb").as_ref(), b"rev-parse");
        let GitAction::Done(GitResult::Committed { receipt, head }) = git.complete(command(Some(0), &head_line(), b""))
        else {
            panic!("commit result")
        };
        assert_eq!(head.as_ref(), HASH);
        assert!(receipt.starts_with(b"commit "));
    }

    #[test]
    fn recovery_inspection_matches_the_stable_call_trailer() {
        let name = run::CallName { activation: 7, completion: 3, position: 1 };
        let Some((mut git, GitAction::Command { .. })) = Git::new(GitOp::Inspect { name }, limits()) else {
            panic!("inspection command")
        };
        let mut output = List::with_capacity(128);
        for byte in HASH.iter().chain(b"\0Title\n\nSmith-Delivery: 7/3/1\n".iter()) {
            output.push(*byte).expect("inspection output bound");
        }
        let GitAction::Done(GitResult::Inspected { head, named }) =
            git.complete(command(Some(0), &output.into_boxed(), b""))
        else {
            panic!("inspection result")
        };
        assert_eq!(head.as_ref(), HASH);
        assert!(named);
    }

    #[test]
    fn moved_remote_is_stale_and_other_push_failure_is_failed() {
        let op = GitOp::Push { remote: Box::from(&b"origin"[..]), branch: Box::from(&b"main"[..]) };
        let Some((mut git, GitAction::Command { args })) = Git::new(op, limits()) else { panic!("push command") };
        assert_eq!(args.last().expect("push target").as_ref(), b"HEAD:refs/heads/main");
        let GitAction::Done(GitResult::Stale) = git.complete(command(Some(1), b"", b"non-fast-forward")) else {
            panic!("stale push")
        };

        let op = GitOp::Push { remote: Box::from(&b"origin"[..]), branch: Box::from(&b"main"[..]) };
        let Some((mut git, _)) = Git::new(op, limits()) else { panic!("push command") };
        let GitAction::Done(GitResult::Failed { reason, .. }) =
            git.complete(command(Some(1), b"", b"authentication failed"))
        else {
            panic!("failed push")
        };
        assert_eq!(reason, run::DeliveryReason::Broken);
    }
}
