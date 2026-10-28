//! Live outcome policy beside Smith 04's binary and scripted terminal on the
//! shared real ring (testing.md, sections 2.3 and 5). No service state is read.

use super::{Process, start};
use skein_io::kernel::Exit;
use skein_lib::{Duration, Time};
use skein_shell::{Clock, Host};
use skein_world::{
    Referee,
    end_to_end::{Mode, Streams},
    real,
};
use smith_local_process_world::{
    process::Proc,
    referee::CheckoutRead,
    terminal::{Action, Terminal},
};
use smith_real_world::{Checkout, Scratch, settled::assert_no_children};
use std::path::PathBuf;

pub struct Expectation {
    pub before: Option<Vec<u8>>,
    pub files: Vec<(Vec<u8>, Vec<u8>)>,
}

struct Judge {
    next_act: Option<Time>,
    shutdown: bool,
    deadline: Time,
    checkout: Checkout,
    expectation: Expectation,
    directory: PathBuf,
    trace_prefix: Vec<u8>,
    reviewed: bool,
}

impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, procs: &mut [Process]) {
        let exit =
            procs.iter().find_map(|proc| if let Process::Binary(binary) = proc { binary.exit_status() } else { None });
        self.next_act = None;
        self.shutdown |= exit.is_some();
        for proc in procs {
            match proc {
                Process::Script(Proc::Terminal(person)) => {
                    if let Some(exit) = exit {
                        person.finished(exit);
                    }
                }
                Process::Script(Proc::Peer(peer)) if exit.is_some() => peer.shutdown(),
                Process::Binary(_) | Process::Script(_) | Process::Sample(_) => {}
            }
        }
    }
    fn observe(&mut self, now: Time, procs: &[Process]) {
        if !self.shutdown
            && procs.iter().any(|proc| matches!(proc, Process::Binary(binary) if binary.exit_status().is_some()))
        {
            self.next_act = Some(now);
        }
        for proc in procs {
            if let Process::Binary(binary) = proc
                && let Some(exit) = binary.exit_status()
            {
                assert_eq!(exit, Exit::Code(0), "live Smith must exit successfully; diagnostic bytes are withheld");
                assert!(
                    std::str::from_utf8(binary.stderr()).is_ok_and(|text| text.lines().all(|line| line
                        .starts_with("smith: local host started; worst case ")
                        || line.starts_with("smith: agent started; worst case ")
                        || line == "smith: commands run in process groups and are not contained"
                        || line == "smith: chat chat"
                        || trust_notice(line)
                        || unused_preset_notice(line))),
                    "live Smith emitted an unexpected diagnostic; credential-bearing bytes are withheld"
                );
            }
        }
        if self.reviewed || !procs.iter().all(Host::is_empty) {
            return;
        }
        let records = super::read_trace(&self.directory, &self.trace_prefix);
        assert!(
            records.iter().any(|record| matches!(&record.event,
                smith_events::Event::ToolStarted(started) if started.tool.as_ref() == b"finish")),
            "the shipped live agent actually called finish"
        );
        assert!(
            records.iter().any(|record| matches!(&record.event,
                smith_events::Event::ToolCompleted(completed)
                if completed.tool.as_ref() == b"finish"
                    && completed.verdict == smith_events::Verdict::Result
                    && (self.expectation.before.is_none()
                        || completed.delivery == Some(smith_events::Delivered::Delivered)))),
            "the live agent received its accepted or delivered finish terminal"
        );
        if let Some(before) = &self.expectation.before {
            let head = self.checkout.head().expect("live committed head");
            assert!(head != *before, "live run creates a new commit");
            let message = self.checkout.message(&head);
            assert!(
                message.windows(15).any(|bytes| bytes == b"Smith-Delivery:"),
                "commit carries the delivery identity"
            );
            let files = self.checkout.files(&head);
            for (path, expected) in &self.expectation.files {
                assert!(
                    files.get(path).is_some_and(|bytes| bytes == expected),
                    "the actual committed file has its prescribed content"
                );
                assert!(
                    std::fs::read(self.directory.join("repo").join(std::str::from_utf8(path).expect("test path")))
                        .is_ok_and(|bytes| bytes == *expected),
                    "the delivered checkout keeps the prescribed content"
                );
            }
            let person = procs
                .iter()
                .find_map(|proc| if let Process::Script(Proc::Terminal(person)) = proc { Some(person) } else { None })
                .expect("live person");
            assert!(
                person.shown().windows(16).any(|bytes| bytes == b"Change delivered"),
                "terminal observed actual delivery"
            );
        }
        self.reviewed = true;
    }
    fn next_deadline(&self) -> Option<Time> {
        self.next_act.into_iter().chain([self.deadline]).min()
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= self.deadline).then(|| "live binary story did not settle before its deadline; output withheld".into())
    }
    fn passed(&self) -> bool {
        self.reviewed
    }
}

pub fn run(scratch: &Scratch, prompt: &str, change: bool, expectation: Expectation) {
    run_with_peer(scratch, prompt, change, expectation, None);
}

/// Offline control of exactly the live outcome policy and shared binary driver.
pub fn run_fake(scratch: &Scratch, scenario: &smith_local_process_world::world::Scenario) {
    let token = super::settings(scratch, scenario);
    let change = scenario.launch.change;
    let files = if change {
        [
            ("original.txt", "edited\n"),
            ("result.txt", "new\n"),
            ("command.txt", "ran\n"),
            ("checks-ran.txt", "checked\n"),
        ]
        .into_iter()
        .map(|(path, content)| (path.as_bytes().to_vec(), content.as_bytes().to_vec()))
        .collect()
    } else {
        vec![]
    };
    run_with_peer(
        scratch,
        if change { "change it" } else { "hello" },
        change,
        Expectation { before: change.then(|| scratch.checkout().head().expect("initial head")), files },
        Some(scenario.provider_with(skein_llm::Credential {
            value: token,
            account_id: b"acc".as_slice().into(),
            scheme: skein_llm::Scheme::Bearer,
        })),
    );
}

fn run_with_peer(
    scratch: &Scratch,
    prompt: &str,
    change: bool,
    expectation: Expectation,
    peer: Option<smith_local_process_world::llm::Peer>,
) {
    assert_no_children();
    let clock = Clock::new();
    let now = clock.now().now;
    let budget = if peer.is_some() { Duration::from_secs(10) } else { Duration::from_secs(260) };
    // Snapshot only the prior sessions before the child can append its header.
    let trace_prefix = std::fs::read(super::trace_path(scratch.path())).unwrap_or_default();
    let mut binary = start(Vec::new(), scratch.path(), Mode::Terminal);
    let streams = binary.take_streams().expect("live controlling terminal");
    let descriptors = streams.descriptors();
    let Streams::Terminal { stream } = streams else { unreachable!("live person on terminal") };
    let mut world = real::World::new(Judge {
        next_act: None,
        shutdown: false,
        deadline: now.saturating_add(budget),
        checkout: scratch.checkout(),
        expectation,
        directory: scratch.path().into(),
        trace_prefix,
        reviewed: false,
    });
    world.spawn_with_fds(binary.descriptors(), || Process::Binary(Box::new(binary)));
    world.spawn_with_fds(descriptors, || {
        Process::Script(Proc::Terminal(Box::new(Terminal::attached(
            stream,
            vec![
                Action::Send(format!("{prompt}\n").into_bytes().into()),
                Action::Wait(if change {
                    b"Change delivered".as_slice().into()
                } else {
                    b"tool finish".as_slice().into()
                }),
                Action::Eof,
            ],
        ))))
    });
    if let Some(peer) = peer {
        world.spawn(|| Process::Script(Proc::Peer(Box::new(peer))));
    }
    let outcome = world.run(&clock, budget.saturating_add(Duration::from_secs(2)));
    assert!(outcome.procs.iter().all(Host::is_empty), "live binary and terminal fully drained");
    assert_no_children();
}

// Startup's content-free root counts (skein shell.md, section 6.2).
fn trust_notice(line: &str) -> bool {
    line.strip_prefix("smith: trust roots: ").is_some_and(|counts| {
        let fields: Vec<_> = counts.split(", ").collect();
        fields.len() == 4
            && fields
                .iter()
                .zip([" taken", " skipped", " malformed blocks", " other blocks"])
                .all(|(field, suffix)| field.strip_suffix(suffix).is_some_and(|count| count.parse::<u32>().is_ok()))
    })
}

fn unused_preset_notice(line: &str) -> bool {
    ["codex", "anthropic"].iter().any(|account| {
        line == format!("smith: warning: {}", smith_local_shell::credentials::Credential::NotSignedIn.line(account))
    })
}
