//! Local policy consumes each typed parent variant; process ownership is
//! exercised separately by the spawned lifecycle unit and composed worlds.
use super::{AgentLife, Domain, Phase, parent_request};
use crate::{
    AccountFailure, AgentFailure, Checks, Config, Contract, Event, Failure, Limits, Notice, Request, Undelivered,
};
use alloc::boxed::Box;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use smith_domain::{run, session};
use smith_host_domain::{self as host, parent, process};

fn running(config: Config) -> (Domain, Env<Limits>, Queue<Request>) {
    let mut limits = crate::tests::limits();
    limits.agent = crate::AgentLimits::Spawned(crate::tests::host_limits());
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits.clone() };
    let mut domain = Domain::new(config, &limits, 71).expect("bounded local parent");
    domain.chat.phase = Phase::Running;
    domain.run = Some(Token::new(1));
    let out = Queue::uncapped();
    (domain, env, out)
}

#[test]
fn started_binds_the_handle_and_admission_keeps_an_empty_person_queue() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Started { client: Token::new(1), agent: Token::new(9) },
        &mut out,
    );
    parent_request(&mut domain, &env, parent::Request::Admitted { client: Token::new(1) }, &mut out);
    assert_eq!(domain.run, Some(Token::new(9)));
    assert!(out.is_empty());
}

#[test]
fn the_first_drop_cause_survives_store_failure_and_the_childs_later_fault() {
    for store_first in [false, true] {
        let (mut domain, env, mut out) = running(crate::tests::config());
        let failed_store = Event::StoreFailed { turn: Some(2), reason: crate::StoreFailure::Write };
        if store_first {
            crate::step(&mut domain, &env, failed_store, &mut out);
            crate::step(&mut domain, &env, Event::Drop, &mut out);
        } else {
            crate::step(&mut domain, &env, Event::Drop, &mut out);
            crate::step(&mut domain, &env, failed_store, &mut out);
        }
        let cause = if store_first {
            crate::EndCause::Host { turn: Some(2), reason: crate::StoreFailure::Write }
        } else {
            crate::EndCause::Cancelled
        };
        assert_eq!(domain.end_cause(), Some(cause));
        parent_request(
            &mut domain,
            &env,
            parent::Request::Faulted { client: Token::new(1), fault: host::Fault::Exited },
            &mut out,
        );
        parent_request(
            &mut domain,
            &env,
            parent::Request::Gone {
                client: Token::new(1),
                end: host::End::Exited {
                    exit: Some(smith_host_domain::Exit::Code(0)),
                    forced: smith_host_domain::Forced::No,
                },
                detail: Box::default(),
            },
            &mut out,
        );
        assert_eq!(domain.end_cause(), Some(cause), "later child terminals cannot replace the cause");
        let mut status = None;
        for _ in 0..out.len() {
            let Some(request) = out.pop() else { break };
            if let Request::Exit { status: value } = request {
                status = Some(value);
            }
        }
        assert_eq!(
            status,
            Some(if store_first { crate::ExitStatus::RunFailed } else { crate::ExitStatus::Interrupted })
        );
    }
}

#[test]
fn waiting_and_both_long_notices_reach_the_person_by_kind() {
    let notices = [
        (parent::Request::Waiting { client: Token::new(1), read: None }, Notice::Waiting),
        (
            parent::Request::Long {
                client: Token::new(1),
                operation: host::Operation::Checks,
                span: Duration::from_secs(9),
            },
            Notice::Checking(Checks::Started),
        ),
        (parent::Request::LongDone { client: Token::new(1) }, Notice::Checking(Checks::Ended)),
    ];
    let (mut domain, env, mut out) = running(crate::tests::config());
    for (notice, expected) in notices {
        parent_request(&mut domain, &env, notice, &mut out);
        let Some(Request::Notice { notice }) = out.pop() else { panic!("one person notice") };
        assert_eq!(notice, expected);
        assert!(out.is_empty());
    }
}

#[test]
fn completion_and_command_spans_do_not_claim_checks_are_running() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    for operation in [host::Operation::Completion, host::Operation::Command] {
        parent_request(
            &mut domain,
            &env,
            parent::Request::Long { client: Token::new(1), operation, span: Duration::from_secs(9) },
            &mut out,
        );
    }
    assert!(out.is_empty());
    for _ in 0_u32..2 {
        parent_request(&mut domain, &env, parent::Request::LongDone { client: Token::new(1) }, &mut out);
        assert!(out.is_empty());
    }
}

#[test]
fn each_account_notice_keeps_its_account() {
    let (mut domain, mut env, mut out) = running(crate::tests::config());
    env.wall = Wall::from_nanos(36_000_000_000_000);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Unauthorized { client: Token::new(1), account: 7, generation: 4 },
        &mut out,
    );
    let Some(Request::Credential { account }) = out.pop() else { panic!("refresh the named account") };
    assert_eq!(account, 7);
    for (request, expected) in account_notices() {
        parent_request(&mut domain, &env, request, &mut out);
        let Some(Request::Notice { notice }) = out.pop() else { panic!("account notice") };
        assert_eq!(notice, Notice::Failure(Failure::Account(expected.clone())), "{expected:?}");
        assert!(out.is_empty());
    }
}

fn account_notices() -> [(parent::Request, AccountFailure); 4] {
    let client = Token::new(1);
    [
        (
            parent::Request::Limited { client, account: 7, reset: Some(Duration::from_secs(2)) },
            AccountFailure::Limited { account: 7, reset_at: Some(Wall::from_nanos(36_002_000_000_000)) },
        ),
        (
            parent::Request::Limited { client, account: 8, reset: Some(Duration::ZERO) },
            AccountFailure::Limited { account: 8, reset_at: Some(Wall::from_nanos(36_000_000_000_000)) },
        ),
        (
            parent::Request::Limited { client, account: 9, reset: None },
            AccountFailure::Limited { account: 9, reset_at: None },
        ),
        (
            parent::Request::Refused {
                client,
                account: 10,
                code: b"billing".as_slice().into(),
                reason: b"contact support".as_slice().into(),
            },
            AccountFailure::Refused {
                account: 10,
                code: b"billing".as_slice().into(),
                reason: b"contact support".as_slice().into(),
            },
        ),
    ]
}

#[test]
fn told_and_withdrawn_do_not_abandon_a_durable_delivery() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    domain.delivery_owner = Some(Token::new(55));
    parent_request(
        &mut domain,
        &env,
        parent::Request::Told { client: Token::new(1), body: b"fact".as_slice().into() },
        &mut out,
    );
    parent_request(
        &mut domain,
        &env,
        parent::Request::Withdrawn { client: Token::new(1), call: Token::new(55) },
        &mut out,
    );
    assert_eq!(domain.delivery_owner, Some(Token::new(55)));
    assert!(out.is_empty());
}

#[test]
fn a_parent_turn_moves_its_concrete_value_to_the_durable_store() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    let turn = smith_domain::Turn {
        model: b"model".as_slice().into(),
        effort: None,
        kind: session::record::Kind::First(Box::new(session::record::Opener {
            affinity: smith_domain::llm::Affinity { key: [0; 16], thread: 0 },
            account: None,
            backend: session::record::Backend::Direct,
        })),
        version: session::record::VERSION,
        endpoint: Some(session::llm::Endpoint(4)),
        dialect: 1,
        sequence: 1,
        usage: session::llm::Usage::NONE,
        spent: 29,
        jobs: 0,
        messages: Box::default(),
    };
    let expected = turn.clone();
    parent_request(
        &mut domain,
        &env,
        parent::Request::Turn {
            client: Token::new(1),
            turn: host::Turn {
                opens_window: true,
                number: 1,
                read: None,
                spent: 29,
                body: host::TurnValue::new(turn, crate::tests::host_limits().turn_bytes).expect("bounded native turn"),
            },
        },
        &mut out,
    );
    let Some(Request::SaveTurn { turn, number: 1, .. }) = out.pop() else { panic!("durable native turn") };
    assert_eq!(turn, expected);
}

#[test]
fn the_parent_answer_keeps_its_host_spend_until_gone() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer { read: None, turns: 3, spent: 29, result: host::RunResult::Parked },
            detail: Box::default(),
        },
        &mut out,
    );
    assert!(out.is_empty());
    let Some(answer) = &domain.turns.answer else { panic!("typed host answer retained") };
    assert_eq!(answer.spent, 29);
    assert_eq!(answer.turns, 3);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Exited {
                exit: Some(smith_host_domain::Exit::Code(0)),
                forced: smith_host_domain::Forced::No,
            },
            detail: Box::default(),
        },
        &mut out,
    );
    let Some(Request::Show { text }) = out.pop() else { panic!("answer shown after Gone") };
    assert_eq!(text.as_ref(), b"Chat parked");
    assert_eq!(domain.agent_life, AgentLife::Gone);
}

#[test]
fn an_agent_fault_keeps_the_current_failure_class() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Faulted { client: Token::new(1), fault: host::Fault::Rules },
        &mut out,
    );
    assert!(domain.agent_failed.is_some());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone { client: Token::new(1), end: host::End::Settled, detail: Box::new([]) },
        &mut out,
    );
    let Some(Request::Unanswered { .. }) = out.pop() else { panic!("unanswered terminal") };
    let Some(Request::Notice { notice }) = out.pop() else { panic!("failure notice") };
    assert_eq!(
        notice,
        Notice::Failure(Failure::Agent {
            failure: AgentFailure::Fault(host::Fault::Rules),
            end: host::End::Settled,
            detail: Box::new([])
        })
    );
}

#[test]
fn every_message_refusal_ends_the_message_without_failing_the_agent() {
    for reason in [
        host::MessageRefusal::Full,
        host::MessageRefusal::TooLarge,
        host::MessageRefusal::NameInUse,
        host::MessageRefusal::Ending,
    ] {
        let (mut domain, env, mut out) = running(crate::tests::config());
        parent_request(
            &mut domain,
            &env,
            parent::Request::MessageRefused { client: Token::new(1), name: Token::new(3), reason },
            &mut out,
        );
        assert!(domain.agent_failed.is_none());
        assert_eq!(domain.chat.phase, Phase::Running);
        let Some(Request::Notice { notice }) = out.pop() else { panic!("message terminal notice") };
        assert_eq!(notice, Notice::NotDelivered { line: Some(Token::new(3)), why: Undelivered::Refused(reason) });
        assert!(out.is_empty());
    }
}

#[test]
fn unread_names_reach_the_parent_without_an_agent_failure_or_another_activation() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Unread { client: Token::new(1), names: Box::from([Token::new(3)]) },
        &mut out,
    );
    assert!(domain.agent_failed.is_none());
    assert_eq!(domain.chat.phase, Phase::Running);
    assert!(out.is_empty());
}

#[test]
fn a_message_refused_after_the_last_word_keeps_the_single_transcript_reload() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer {
                read: None,
                turns: 0,
                spent: 0,
                result: host::RunResult::Failed {
                    failure: host::RunFailure::Transcript(host::TranscriptRefusal::Version),
                },
            },
            detail: Box::default(),
        },
        &mut out,
    );
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Exited {
                exit: Some(smith_host_domain::Exit::Code(0)),
                forced: smith_host_domain::Forced::No,
            },
            detail: Box::default(),
        },
        &mut out,
    );
    parent_request(
        &mut domain,
        &env,
        parent::Request::MessageRefused {
            client: Token::new(1),
            name: Token::new(3),
            reason: host::MessageRefusal::Ending,
        },
        &mut out,
    );
    let Some(Request::Notice { notice }) = out.pop() else { panic!("actual transcript refusal") };
    assert_eq!(
        notice,
        Notice::Failure(Failure::Run {
            failure: host::RunFailure::Transcript(host::TranscriptRefusal::Version),
            detail: Box::new([])
        })
    );
    let Some(Request::Answered { .. }) = out.pop() else { panic!("answer reaches its event writer") };
    let Some(Request::Load) = out.pop() else { panic!("one transcript reload") };
    let Some(Request::Notice { notice }) = out.pop() else { panic!("late message terminal") };
    assert_eq!(
        notice,
        Notice::NotDelivered { line: Some(Token::new(3)), why: Undelivered::Refused(host::MessageRefusal::Ending) }
    );
    assert!(out.is_empty());
    assert_eq!(domain.chat.phase, Phase::Loading);
    assert!(domain.agent_failed.is_none());
}

#[test]
fn gone_without_an_answer_fails_after_retained_turns_are_saved() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Unspawned,
            detail: b"unavailable".as_slice().into(),
        },
        &mut out,
    );
    let Some(Request::Unanswered { cause: crate::EndCause::Agent { ended: None }, turns: 0, spent: 0, read: None }) =
        out.pop()
    else {
        panic!("unanswered run reaches its event writer")
    };
    let Some(Request::Notice { notice }) = out.pop() else { panic!("missing answer notice") };
    assert_eq!(
        notice,
        Notice::Failure(Failure::Agent {
            failure: AgentFailure::NotStarted,
            end: host::End::Unspawned,
            detail: b"unavailable".as_slice().into()
        })
    );
    let Some(Request::Exit { status: crate::ExitStatus::RunFailed }) = out.pop() else { panic!("failed invocation") };
}

#[test]
fn a_delivery_call_keeps_its_fields_owner_and_deadline_in_local_policy() {
    let mut config = crate::tests::config();
    config.reserve.time = crate::tests::limits().smith.run.check_timeout;
    config.budget.time = Duration::from_secs(120);
    config.contract = Contract::Change(run::outcome::ChangeSpec {
        checks_must_pass: false,
        fields: Box::from([run::outcome::FieldRule { name: b"title".as_slice().into(), max: 64 }]),
    });
    config.workspace = Some(run::Workspace {
        directories: Box::from([run::Directory {
            host: Box::from(b"/host".as_slice()),
            name: b"work".as_slice().into(),
            root: Token::new(9),
            writable: true,
            git: false,
            conflicts: Box::default(),
        }]),
    });
    let (mut domain, env, mut out) = running(config);
    let change = run::outcome::Change {
        fields: Box::from([run::outcome::Field {
            name: b"title".as_slice().into(),
            value: b"keep the change".as_slice().into(),
        }]),
    };
    let deadline = Time::from_nanos(99);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Called {
            client: Token::new(1),
            logical_run: Token::new(1),
            call: Token::new(55),
            name: host::CallName { activation: 1, completion: 2, position: 1 },
            deadline,
            ask: host::Ask::Deliver { fields: host::Fields::new(change, 1024).expect("bounded fields") },
        },
        &mut out,
    );
    let Some(Request::PlainStatus { owner, directory, deadline: received }) = out.pop() else {
        panic!("survey typed delivery")
    };
    assert_eq!(owner, Token::new(55));
    assert_eq!(directory, 0);
    assert_eq!(received, deadline);
}

#[test]
fn an_unconfigured_host_call_receives_one_unavailable_parent_answer() {
    let (mut domain, env, mut out, owner) = crate::tests::spawned_chat_with_saved_hello();
    crate::step(
        &mut domain,
        &env,
        Event::Process(process::Event::Received {
            owner,
            message: host::Up::Call {
                call: Token::new(55),
                name: host::CallName { activation: 1, completion: 2, position: 1 },
                deadline: Time::from_nanos(99),
                ask: host::Ask::Host {
                    tool: b"missing".as_slice().into(),
                    effect: host::Effect::Read,
                    body: Box::default(),
                },
            },
        }),
        &mut out,
    );
    assert!(domain.is_ready(), "the routed call keeps its generated answer ready");
    crate::resume(&mut domain, &env, &mut out);
    assert!(!domain.is_ready(), "the answer drained without another external event");
    let mut replies = 0_u32;
    while let Some(request) = out.pop() {
        if let Request::Process(process::Request::Send {
            message: host::Down::Answer { call, reply: host::Reply::Unavailable },
            ..
        }) = request
        {
            assert_eq!(call, Token::new(55));
            replies += 1;
        }
    }
    assert_eq!(replies, 1);
}

#[test]
fn closing_input_during_a_refused_start_exits_after_the_notice() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    domain.chat.phase = Phase::Starting;
    domain.chat.start_saved = true;
    domain.grants.awaiting = 1;
    crate::step(&mut domain, &env, Event::Closed, &mut out);
    assert!(out.is_empty());
    crate::step(
        &mut domain,
        &env,
        Event::NoCredential { account: 7, reason: crate::CredentialFailure::Missing },
        &mut out,
    );
    let Some(Request::Notice { notice }) = out.pop() else { panic!("failed grant notice") };
    assert_eq!(notice, Notice::Failure(Failure::Credential { account: 7, reason: crate::CredentialFailure::Missing }));
    let Some(Request::Exit { status: crate::ExitStatus::Accepted }) = out.pop() else { panic!("closed input exits") };
    assert!(out.is_empty());
}

fn answer_with_a_late_state_save(gone_first: bool) {
    let (mut domain, env, mut out) = running(crate::tests::config());
    domain.chat.state_pending = true;
    domain.lines.receive(Box::from(&b"follow-up"[..])).expect("room");
    domain.lines.name(Token::new(1));
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer { read: None, turns: 0, spent: 0, result: host::RunResult::Parked },
            detail: Box::default(),
        },
        &mut out,
    );
    assert_eq!(domain.chat.phase, Phase::Answered { gone: super::Gone::Owed });
    let gone = parent::Request::Gone {
        client: Token::new(1),
        end: host::End::Exited { exit: Some(host::Exit::Code(0)), forced: host::Forced::No },
        detail: Box::new([]),
    };
    if gone_first {
        parent_request(&mut domain, &env, gone, &mut out);
        assert_eq!(domain.chat.phase, Phase::Answered { gone: super::Gone::Heard });
        super::state_saved(&mut domain, &env, &mut out);
    } else {
        super::state_saved(&mut domain, &env, &mut out);
        assert_eq!(domain.chat.phase, Phase::Answered { gone: super::Gone::Owed });
        parent_request(&mut domain, &env, gone, &mut out);
    }
    assert_eq!(domain.chat.phase, Phase::Loading);
    assert_eq!(domain.lines.carry()[0].name, Token::new(1));
    assert!(!domain.chat.state_pending);
}

#[test]
fn a_late_state_save_after_the_answer_is_handled_before_gone() {
    answer_with_a_late_state_save(false);
}

#[test]
fn gone_before_a_late_state_save_keeps_the_line_until_its_save_finishes() {
    answer_with_a_late_state_save(true);
}

#[test]
fn gone_after_a_late_turn_save_shows_the_answer_once() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    domain.turns.unsaved.push((1, 1));
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer { read: None, turns: 1, spent: 0, result: host::RunResult::Parked },
            detail: Box::default(),
        },
        &mut out,
    );
    domain.run = None;
    super::turn_saved(&mut domain, &env, 1, &mut out);
    assert_eq!(domain.chat.phase, Phase::Answered { gone: super::Gone::Owed });
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Exited { exit: Some(host::Exit::Code(0)), forced: host::Forced::No },
            detail: Box::new([]),
        },
        &mut out,
    );
    assert_eq!(domain.chat.phase, Phase::Loading);
    assert_eq!(shown_requests(&mut out), 1);
}

fn shown_requests(out: &mut Queue<Request>) -> u32 {
    let mut shown = 0_u32;
    for _ in 0..out.len() {
        match out.pop() {
            Some(Request::Show { .. }) => shown = shown.checked_add(1).expect("bounded output"),
            Some(
                Request::Input { .. }
                | Request::Notice { .. }
                | Request::Answered { .. }
                | Request::Unanswered { .. }
                | Request::Load
                | Request::SelectChat { .. }
                | Request::SaveState { .. }
                | Request::SaveTurn { .. }
                | Request::SaveDelivery { .. }
                | Request::Git { .. }
                | Request::PlainStatus { .. }
                | Request::Credential { .. }
                | Request::Exit { .. }
                | Request::Lower { .. }
                | Request::Process(_),
            ) => {}
            None => break,
        }
    }
    shown
}

#[test]
fn gone_fills_the_agents_first_cause_with_the_exit_the_kit_reported() {
    let (mut domain, env, mut out) = running(crate::tests::config());
    parent_request(
        &mut domain,
        &env,
        parent::Request::Faulted { client: Token::new(1), fault: host::Fault::Exited },
        &mut out,
    );
    assert_eq!(domain.end_cause(), Some(crate::EndCause::Agent { ended: None }));
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Exited { exit: Some(host::Exit::Signal(9)), forced: host::Forced::Killed },
            detail: Box::default(),
        },
        &mut out,
    );
    assert_eq!(domain.end_cause(), Some(crate::EndCause::Agent { ended: Some(smith_events::AgentExit::Signal(9)) }));
    let mut notices = 0_u32;
    let mut completed = 0_u32;
    for _ in 0..out.len() {
        match out.pop() {
            Some(Request::Notice {
                notice:
                    Notice::Failure(Failure::Agent {
                        end: host::End::Exited { exit: Some(host::Exit::Signal(9)), forced: host::Forced::Killed },
                        ..
                    }),
            }) => {
                notices = notices.checked_add(1).expect("bounded output");
            }
            Some(Request::Unanswered {
                cause: crate::EndCause::Agent { ended: Some(smith_events::AgentExit::Signal(9)) },
                ..
            }) => {
                completed = completed.checked_add(1).expect("bounded output");
            }
            _ => {}
        }
    }
    assert_eq!(notices, 1);
    assert_eq!(completed, 1);
}

#[test]
fn gone_without_observed_process_evidence_keeps_ended_absent() {
    for end in [host::End::Unspawned, host::End::Settled, host::End::Exited { exit: None, forced: host::Forced::No }] {
        let (mut domain, env, mut out) = running(crate::tests::config());
        parent_request(
            &mut domain,
            &env,
            parent::Request::Gone { client: Token::new(1), end, detail: Box::default() },
            &mut out,
        );
        assert_eq!(domain.end_cause(), Some(crate::EndCause::Agent { ended: None }));
        let mut completed = 0_u32;
        for _ in 0..out.len() {
            if let Some(Request::Unanswered { cause: crate::EndCause::Agent { ended: None }, .. }) = out.pop() {
                completed = completed.checked_add(1).expect("bounded output");
            }
        }
        assert_eq!(completed, 1);
    }
}

fn after_an_answered_agents_forced_exit(result: host::RunResult) -> Queue<Request> {
    let mut config = crate::tests::config();
    config.mode = crate::Mode::Headless { prompt: b"hello".as_slice().into() };
    let (mut domain, env, mut out) = running(config);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer { read: None, turns: 0, spent: 0, result },
            detail: Box::new([]),
        },
        &mut out,
    );
    while out.pop().is_some() {}
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone {
            client: Token::new(1),
            end: host::End::Exited { exit: Some(host::Exit::Signal(9)), forced: host::Forced::Killed },
            detail: Box::new([]),
        },
        &mut out,
    );
    out
}

#[test]
fn a_forced_exit_after_a_parked_or_cancelled_answer_keeps_its_terminal() {
    for (result, expected) in [
        (host::RunResult::Parked, crate::ExitStatus::NeedsInput),
        (host::RunResult::Failed { failure: host::RunFailure::Cancelled }, crate::ExitStatus::Interrupted),
    ] {
        let mut out = after_an_answered_agents_forced_exit(result);
        let Some(Request::Notice {
            notice: Notice::Warning(crate::Warning::ForcedExit { forced: host::Forced::Killed, .. }),
        }) = out.pop()
        else {
            panic!("warning comes before the answer")
        };
        let mut exit = None;
        while let Some(request) = out.pop() {
            if let Request::Exit { status } = request {
                exit = Some(status);
            }
        }
        assert_eq!(exit, Some(expected));
    }
}

#[test]
fn the_host_answer_clock_precedes_teardown_and_durable_saves() {
    let (mut domain, mut env, mut out) = running(crate::tests::config());
    env.wall = Wall::from_nanos(36_000_000_000_000);
    domain.chat.state_pending = true;
    assert_eq!(domain.answer_wall(), None);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Answered {
            client: Token::new(1),
            answer: host::Answer { read: None, turns: 3, spent: 29, result: host::RunResult::Parked },
            detail: Box::default(),
        },
        &mut out,
    );
    assert!(out.is_empty());
    let answered_at = Some(env.wall);
    env.wall = Wall::from_nanos(39_600_000_000_000);
    parent_request(
        &mut domain,
        &env,
        parent::Request::Gone { client: Token::new(1), end: host::End::Settled, detail: Box::default() },
        &mut out,
    );
    assert_eq!(domain.answer_wall(), answered_at);
    assert!(out.is_empty());
    crate::step(&mut domain, &env, Event::StateSaved, &mut out);
    assert_eq!(domain.answer_wall(), answered_at);
    let Some(Request::Show { .. }) = out.pop() else { panic!("settled answer is shown") };
    let Some(Request::Answered { .. }) = out.pop() else { panic!("settled answer reaches the shell") };
}

#[expect(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "verbatim contract tests are ordinary Rust (programming-model.md, section 10.2)"
)]
mod older {
    use super::*;
    #[test]
    fn older_store_words_match_all_three_shell_contract_blockquotes_verbatim() {
        let documented = include_str!("../../../docs/design/shell.md");
        let quotes: Vec<_> = documented
            .lines()
            .filter_map(|line| line.strip_prefix("> Chat "))
            .map(|line| format!("Chat {line}"))
            .collect();
        let reasons = [
            crate::Older::Unrecorded,
            crate::Older::Layout(crate::StoreFamily::Store),
            crate::Older::Layout(crate::StoreFamily::Transcript),
        ];
        assert_eq!(quotes.len(), reasons.len());
        for (why, quoted) in reasons.iter().zip(quotes) {
            assert_eq!(why.words().join("<chat>"), quoted);
        }
    }

    #[test]
    fn an_older_load_before_any_run_preserves_its_reason_through_notice_cause_and_exit_one() {
        for why in [
            crate::Older::Unrecorded,
            crate::Older::Layout(crate::StoreFamily::Store),
            crate::Older::Layout(crate::StoreFamily::Transcript),
        ] {
            let (mut domain, env, mut out) = running(crate::tests::config());
            domain.run = None;
            domain.chat.phase = Phase::Loading;
            let reason = crate::StoreFailure::OlderBuild { why };
            crate::step(&mut domain, &env, Event::StoreFailed { turn: None, reason }, &mut out);
            assert_eq!(domain.end_cause, Some(crate::EndCause::Host { turn: None, reason }));
            let Some(Request::Notice { notice: Notice::Failure(Failure::Store { turn: None, reason: found }) }) =
                out.pop()
            else {
                panic!("older notice before exit");
            };
            assert_eq!(found, reason);
            let Some(Request::Exit { status }) = out.pop() else {
                panic!("older-load exit");
            };
            assert_eq!(status, crate::ExitStatus::RunFailed);
            assert!(out.is_empty());
        }
    }
}
