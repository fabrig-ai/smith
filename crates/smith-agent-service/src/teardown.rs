//! One end phase for the framed agent and its host-owned effects.
//! It keeps the answer disposition and the ordered close reports, never a run
//! outcome. Each pass advances what its children have reported, through usage
//! and the event stream. Contract: protocol/agent.md, section 6.

use skein_lib::{Time, Token};
use smith_domain as domain;

use super::{Failure, Operation, Service, USAGE_OWNER};
use skein_io::{self as io, file, file_layer};

/// Where the service is in its end, advanced after each routing pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Teardown {
    /// The run still owes its answer.
    Live,
    /// The run has answered; its closes follow any retained LLM measures.
    Answered { at: Time, answer: Answer, close: Close },
    /// Every owned close has been requested; its reports arrive in order.
    Closing { at: Time, answer: Answer, close: Close, step: Step },
    /// Every child reported closed and io is empty; the shell may exit.
    Closed { answer: Answer, close: Close },
}

/// Whether the service sent its answer or its effects owner holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Answer {
    /// The answer crossed the channel or is held by the effects owner.
    Sent,
    /// The channel could not carry an answer, for the kept first cause.
    Unsent(Failure),
}

impl Answer {
    pub(super) const fn sent(self) -> bool {
        match self {
            Self::Sent => true,
            Self::Unsent(_) => false,
        }
    }
}

/// How the service asks its children to close.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Close {
    /// Close drains the work already admitted.
    Graceful,
    /// A termination signal asks remaining closes to abort.
    Aborted,
}

/// The close reports the service waits for, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// The LLM pool, roots and channel still owe their close reports.
    Components,
    /// Io owes its requested process resource usage.
    Usage,
    /// Event destinations still owe their close reports.
    Stream,
    /// The retained signal reader still owes its close report.
    Signals,
}

pub(super) fn answered(service: &mut Service) {
    match service.teardown {
        Teardown::Live => {
            let answer = match service.failure {
                Some(failure) => Answer::Unsent(failure),
                None => Answer::Sent,
            };
            service.teardown = Teardown::Answered { at: service.io_env.now, answer, close: Close::Graceful };
        }
        Teardown::Answered { .. } | Teardown::Closing { .. } | Teardown::Closed { .. } => {}
    }
}

pub(super) fn failed(service: &mut Service, failure: Failure) {
    match service.teardown {
        Teardown::Live => {}
        Teardown::Answered { at, close, .. } => {
            service.teardown = Teardown::Answered { at, answer: Answer::Unsent(failure), close };
        }
        Teardown::Closing { at, close, step, .. } => {
            service.teardown = Teardown::Closing { at, answer: Answer::Unsent(failure), close, step };
        }
        Teardown::Closed { .. } => unreachable!("the closed service has no remaining events"),
    }
}

fn close_room(service: &Service) -> bool {
    service.llm.measures_drained()
}

pub(super) fn components_closed(service: &Service) -> bool {
    let streams = match &service.events {
        Some(events) => events.sinks.open_streams(),
        None => 0,
    };
    let streams = streams.checked_add(u32::from(service.signals.is_some())).expect("bounded live signals");
    super::program::closed(service)
        && service.llm.closed()
        && service.live.deadline().is_none()
        && settled(service, streams)
}

fn stream_closed(service: &Service) -> bool {
    let destinations = match &service.events {
        Some(events) => events.sinks.closed(),
        None => true,
    };
    destinations
        && settled(service, u32::from(service.signals.is_some()))
        && (service.signals.is_some()
            || match &service.io {
                Some(io) => io.is_empty(),
                None => true,
            })
}

pub(super) fn shutdown(service: &mut Service) {
    let before_terminal = match service.teardown {
        Teardown::Live => {
            if let Some(run) = service.admitted {
                service.domain_events.push(domain::Event::Cancel { run });
            } else {
                service.cancel_pending = true;
            }
            return;
        }
        Teardown::Answered { at, answer, close: Close::Graceful } => {
            service.teardown = Teardown::Answered { at, answer, close: Close::Aborted };
            true
        }
        Teardown::Closing { at, answer, step: step @ (Step::Stream | Step::Signals), .. } => {
            service.teardown = Teardown::Closing { at, answer, close: Close::Aborted, step };
            if let Some(events) = &mut service.events {
                events.sinks.abort(&mut service.io_requests);
            }
            return;
        }
        Teardown::Closing { at, answer, close: Close::Graceful, step } => {
            service.teardown = Teardown::Closing { at, answer, close: Close::Aborted, step };
            true
        }
        Teardown::Answered { close: Close::Aborted, .. }
        | Teardown::Closing { close: Close::Aborted, .. }
        | Teardown::Closed { .. } => false,
    };
    if before_terminal {
        if !service.llm.closed() {
            service.llm.abort(&service.llm_env, &mut service.llm_events, &mut service.llm_below);
        }
        if let Some(entity) = service.input {
            service.io_requests.push(io::Request::Abort { entity });
        }
        if let Some(entity) = service.output {
            service.io_requests.push(io::Request::Abort { entity });
        }
        if let Some(events) = &mut service.events {
            events.notice(
                service.io_env.now,
                smith_events::Notice {
                    code: None,
                    level: smith_events::Level::Warning,
                    kind: smith_events::NoticeKind::TeardownAborted,
                    run: None,
                    account: None,
                    reset_ms: None,
                    bytes: None,
                    exit: None,
                    forced: None,
                },
            );
        }
    }
}

pub(super) fn work_pending(service: &Service) -> bool {
    match service.teardown {
        Teardown::Live | Teardown::Closed { .. } | Teardown::Closing { step: Step::Usage, .. } => false,
        Teardown::Answered { .. } => close_room(service),
        Teardown::Closing { step: Step::Components, .. } => {
            (components_closed(service) && owner_ended(service))
                || (!service.roots.is_empty()
                    && service.owners.is_empty()
                    && service.pipe_routes.is_empty()
                    && service.files.takes()
                    && !service.root_close_pending)
        }
        Teardown::Closing { step: Step::Stream, .. } => stream_closed(service),
        Teardown::Closing { step: Step::Signals, .. } => service.signals.is_none() && stream_closed(service),
    }
}

fn owner_ended(service: &Service) -> bool {
    service.domain.is_some() || service.product_exit.is_some() || service.events.is_none()
}

pub(super) fn advance(service: &mut Service) {
    match service.teardown {
        Teardown::Live | Teardown::Closed { .. } | Teardown::Closing { step: Step::Usage, .. } => {}
        Teardown::Answered { at, answer, close } => {
            if close_room(service) {
                match close {
                    Close::Graceful => {
                        service.llm.close(&service.llm_env, &mut service.llm_events, &mut service.llm_below);
                    }
                    Close::Aborted => {}
                }
                if close == Close::Graceful
                    && let Some(entity) = service.input
                {
                    service.io_requests.push(io::Request::Close { entity });
                }
                // A sent answer already finishes the channel's output after its
                // queued frames drain. Closing that pipe here would retire the
                // output rights the channel still needs to send those frames.
                match answer {
                    Answer::Sent => {}
                    Answer::Unsent(_) => {
                        if close == Close::Graceful
                            && let Some(entity) = service.output
                        {
                            service.io_requests.push(io::Request::Close { entity });
                        }
                    }
                }
                close_roots(service);
                service.teardown = Teardown::Closing { at, answer, close, step: Step::Components };
            }
        }
        Teardown::Closing { at, answer, close, step: Step::Components } => {
            close_roots(service);
            if components_closed(service) && owner_ended(service) {
                let step = match &service.events {
                    Some(_) => {
                        service.io_requests.push(io::Request::Usage { owner: USAGE_OWNER });
                        Step::Usage
                    }
                    None => Step::Stream,
                };
                service.teardown = Teardown::Closing { at, answer, close, step };
            }
        }
        Teardown::Closing { at, answer, close, step: Step::Stream } => {
            if stream_closed(service) {
                match service.signals {
                    Some(entity) => {
                        service.io_requests.push(io::Request::Close { entity });
                        service.teardown = Teardown::Closing { at, answer, close, step: Step::Signals };
                    }
                    None => service.teardown = Teardown::Closed { answer, close },
                }
            }
        }
        Teardown::Closing { answer, close, step: Step::Signals, .. } => {
            if service.signals.is_none() && stream_closed(service) {
                service.teardown = Teardown::Closed { answer, close };
            }
        }
    }
}

fn close_roots(service: &mut Service) {
    service.machine.close(&mut service.machine_below);
    if service.machine.closed()
        && service.owners.is_empty()
        && service.pipe_routes.is_empty()
        && service.files.takes()
        && !service.root_close_pending
    {
        if service.root_close_next > 0 {
            service.root_close_next = service.root_close_next.checked_sub(1).expect("positive root count");
            let root = *service.roots.get(service.root_close_next).expect("closed roots in reverse order");
            service.root_close_pending = true;
            file_layer::down(
                &mut service.files,
                service.io_env.now,
                file::Request::Close { owner: Token::new(u64::MAX), file: root },
                &mut service.file_events,
                &mut service.file_submissions,
            );
        } else {
            service.roots.clear();
        }
    }
}

/// All non-destination children have reported closed; only these streams may remain.
fn operations_settled(service: &Service) -> bool {
    for (_, operation) in &service.operations {
        match operation {
            Operation::Signals(_) if service.signals.is_some() => {}
            Operation::Signals(_) | Operation::Io(_) | Operation::File(_) => return false,
        }
    }
    true
}

fn settled(service: &Service, streams: u32) -> bool {
    use super::program;

    (match &service.channel {
        Some(channel) => channel.ended(),
        None => true,
    }) && service.channel_events.is_empty()
        && service.channel_below.is_empty()
        && service.llm_events.is_empty()
        && service.llm.measures_drained()
        && program::closed(service)
        && service.llm_below.is_empty()
        && service.machine_events.is_empty()
        && service.machine_below.is_empty()
        && service.domain_events.is_empty()
        && service.domain_requests.is_empty()
        && match &service.domain {
            Some(domain) => !domain.has_facts(),
            None => true,
        }
        && service.io_events.is_empty()
        && service.io_requests.is_empty()
        && service.file_events.is_empty()
        && service.io_submissions.is_empty()
        && service.file_submissions.is_empty()
        && service.completions.is_empty()
        && match &service.io {
            Some(io) => io.sockets() == streams,
            None => true,
        }
        && service.files.takes()
        && service.files.open_files() == 0
        && service.roots.is_empty()
        && service.owners.is_empty()
        && service.pipe_routes.is_empty()
        && operations_settled(service)
        && service.submissions.is_empty()
}
