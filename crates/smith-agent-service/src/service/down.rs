//! Downward passes from queued domain requests to lower components.
//! Facts, messages and lower requests stay in the service; this stage never
//! chooses their domain meaning. Contract: protocol/agent.md, sections 4–6.

use super::request::domain_request;
use super::routing::component_owner;
use super::{Failure, Owner, PendingMessage, Service, program};
use skein_channel::Lower as ChannelLower;
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::stream;
use smith_domain as domain;
use smith_protocol_channel as channel;
use smith_protocol_machine as machine;

pub(super) fn domain_down(service: &mut Service) {
    drain_facts(service);
    match &service.domain {
        Some(domain) if domain.has_facts() => return,
        Some(_) | None => {}
    }
    let channel_max = channel::max_out(&service.limits.channel);
    if let Some(run) = service.admitted {
        for _ in 0..service.pending_messages.capacity() {
            if !super::admits(service) {
                break;
            }
            let PendingMessage { finish_requested, name, label, text } = match service.pending_messages.pop() {
                Some(message) => message,
                None => break,
            };
            service.domain_events.push(domain::Event::Message { finish_requested, run, name, label, text });
        }
    }
    for _ in 0..service.limits.queue {
        match &service.events {
            Some(events) if !events.sinks.takes() => break,
            Some(_) | None => {}
        }
        if !channel_takes(service)
            || !service.llm.measures_drained()
            || (service.channel.is_some()
                && (service.channel_events.room() < channel_max.to_domain
                    || service.channel_below.room() < channel_max.below))
        {
            break;
        }
        if program::wait_for_close(service) {
            break;
        }
        let request = match service.domain_requests.pop() {
            Some(request) => request,
            None => break,
        };
        domain_request(service, request);
    }
    match service.admitted {
        Some(run) if service.cancel_pending => {
            service.domain_events.push(domain::Event::Cancel { run });
            service.cancel_pending = false;
        }
        Some(_) | None => {}
    }
    drain_facts(service);
}

/// A queued request waits on kernel output credit; a colocated runtime has no channel.
pub(super) fn channel_takes(service: &Service) -> bool {
    match &service.channel {
        Some(channel) => channel.takes(),
        None => true,
    }
}

/// A single take offers each observation to the independent sinks. The trace
/// continues through answer and settlement; channel projection owns its phase.
pub(super) fn drain_facts(service: &mut Service) {
    for _ in 0..service.limits.queue {
        match &service.events {
            Some(events) if !events.sinks.takes() => break,
            Some(_) | None => {}
        }
        let fact = match &mut service.domain {
            Some(domain) => match domain.pop_fact() {
                Some(fact) => fact,
                None => break,
            },
            None => break,
        };
        let token = service.next_send();
        if let Some(channel) = &mut service.channel
            && channel
                .send_fact(&fact, service.run_started, token, &mut service.channel_events, &mut service.channel_below)
                .is_err()
        {
            service.mark_failed(Failure::Send);
        }
        if let Some(events) = &mut service.events {
            events.fact(&fact);
        }
    }
    let drained = match &service.domain {
        Some(domain) => !domain.has_facts(),
        None => true,
    };
    if drained && let Some(events) = &mut service.events {
        for _ in 0..service.limits.domain.run.conversations {
            if !events.sinks.takes() || !events.sinks.facts_drained(events.origin()) {
                break;
            }
        }
    }
}

#[expect(clippy::too_many_lines, reason = "bounded lower routing stays in one down-pass stage")]
pub(super) fn component_down(service: &mut Service) {
    for _ in 0..service.channel_below.capacity() {
        let request = match service.channel_below.pop() {
            Some(request) => request,
            None => break,
        };
        match request {
            ChannelLower::Read(down) => {
                if let Some(input) = service.input {
                    service.io_requests.push(io::Request::Stream { stream: input, down });
                }
            }
            ChannelLower::Write(down) => {
                if let Some(output) = service.output {
                    service.io_requests.push(io::Request::Output { stream: output, down });
                }
            }
            ChannelLower::FinishWrite => {
                if let Some(output) = service.output {
                    service.io_requests.push(io::Request::Stream { stream: output, down: stream::Down::Finish });
                }
            }
        }
    }
    program::down(service);
    for _ in 0..service.limits.queue {
        let request = match service.llm_below.pop() {
            Some(request) => request,
            None => break,
        };
        match request {
            io::Request::Connect { owner, addr } => {
                let route = service.owners.insert(Owner::Llm(owner)).expect("LLM connection route capacity");
                service.io_requests.push(io::Request::Connect { owner: component_owner(route.token()), addr });
            }
            other @ (io::Request::Listen { .. }
            | io::Request::Bind { .. }
            | io::Request::Reject { .. }
            | io::Request::Stream { .. }
            | io::Request::Output { .. }
            | io::Request::Spawn { .. }
            | io::Request::Signal { .. }
            | io::Request::Window { .. }
            | io::Request::Usage { .. }
            | io::Request::Close { .. }
            | io::Request::Abort { .. }) => service.io_requests.push(other),
        }
    }
    for _ in 0..service.limits.queue {
        let waits_for_file = match service.machine_below.iter().next() {
            Some(
                machine::Below::Remove { .. }
                | machine::Below::File { .. }
                | machine::Below::OpenRead { .. }
                | machine::Below::OpenDirectory { .. },
            ) => !service.files.takes(),
            Some(machine::Below::CancelFile { .. } | machine::Below::Spawn { .. } | machine::Below::Process(_))
            | None => false,
        };
        if waits_for_file {
            break;
        }
        let request = match service.machine_below.pop() {
            Some(request) => request,
            None => break,
        };
        match request {
            machine::Below::File { request, deadline } => {
                service.files.seed_randomness(service.next_operation);
                file_layer::down_until(
                    &mut service.files,
                    deadline,
                    request,
                    &mut service.file_events,
                    &mut service.file_submissions,
                );
            }
            machine::Below::CancelFile { owner } => {
                file_layer::cancel(&mut service.files, owner, &mut service.file_submissions);
            }
            machine::Below::OpenRead { owner, root, path, deadline } => {
                let descriptor = service.files.descriptor(root).expect("machine admitted an installed root");
                file_layer::down_until(
                    &mut service.files,
                    deadline,
                    file::Request::OpenRead { owner, root: descriptor, name: path },
                    &mut service.file_events,
                    &mut service.file_submissions,
                );
            }
            machine::Below::Remove { owner, parent, name, directory, deadline } => {
                let descriptor = service.files.descriptor(parent).expect("removal owns its parent");
                file_layer::down_until(
                    &mut service.files,
                    deadline,
                    file::Request::Remove { owner, root: descriptor, name, directory },
                    &mut service.file_events,
                    &mut service.file_submissions,
                );
            }
            machine::Below::OpenDirectory { owner, root, path, no_follow, deadline } => {
                let descriptor = service.files.descriptor(root).expect("machine admitted an installed root");
                file_layer::down_until(
                    &mut service.files,
                    deadline,
                    file::Request::OpenDirectory { owner, root: descriptor, name: path, no_follow },
                    &mut service.file_events,
                    &mut service.file_submissions,
                );
            }
            machine::Below::Spawn { owner, root, spawn } => {
                let descriptor = service.files.descriptor(root).expect("machine admitted an installed root");
                let spawn = kernel::Spawn {
                    program: spawn.program,
                    args: spawn.args,
                    env: spawn.env,
                    root: descriptor,
                    dir: spawn.dir,
                    pipes: spawn.pipes,
                };
                let route = service.owners.insert(Owner::Machine(owner)).expect("process route capacity");
                service.io_requests.push(io::Request::Spawn { owner: component_owner(route.token()), spawn });
            }
            machine::Below::Process(request) => service.io_requests.push(request),
        }
    }
}
