//! The exec prompt's bounded stdin phase, before local policy is constructed.
//! The caller keeps one kernel for this phase and the run; IO owns stdin's close.
use crate::command::Usage;
use skein_io::{self as io, kernel};
use skein_lib::{Env, List, Queue, Time, Token, Wall, stream};
use skein_shell::{Clock, Host, Kernel};

const PROMPT_READ: u32 = 4096;

struct Input {
    io: io::Io,
    env: Env<io::Limits>,
    owner: Token,
    bytes: List<u8>,
    failure: Option<Usage>,
    requests: Queue<io::Request>,
    events: Queue<io::Event>,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
}

pub(crate) fn read_prompt(
    kernel: &mut Kernel,
    clock: &Clock,
    limits: io::Limits,
    maximum: u32,
) -> Result<Box<[u8]>, Usage> {
    if io::worst_case(&limits).is_none() {
        return Err(Usage("the prompt phase's IO limits are unusable".into()));
    }
    let mut io = io::Io::new(&limits);
    let owner = io.adopt_read_pipe(kernel::Fd::new(0)).map_err(|_| Usage("standard input cannot be adopted".into()))?;
    let mut input = Input {
        io,
        env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
        owner,
        bytes: List::with_capacity(maximum),
        failure: None,
        requests: Queue::with_capacity(64),
        events: Queue::with_capacity(64),
        completions: Queue::with_capacity(64),
        submissions: Queue::with_capacity(64),
    };
    input.demand();
    let _exit = skein_shell::drive(kernel, clock, &mut input);
    if let Some(failure) = input.failure {
        return Err(failure);
    }
    if input.bytes.as_slice().trim_ascii().is_empty() {
        return Err(Usage("the prompt is empty".into()));
    }
    Ok(input.bytes.into_boxed())
}

impl Input {
    fn demand(&mut self) {
        self.requests.push(io::Request::Stream {
            stream: self.owner,
            down: stream::Down::Demand { read: stream::Read::Arrived { max: PROMPT_READ }, room: 0 },
        });
    }
}

impl Host for Input {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        while let Some(complete) = self.completions.pop() {
            io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
        }
        io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
        io::fire(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
        while let Some(event) = self.events.pop() {
            match event {
                io::Event::Stream { up: stream::Up::Bytes(bytes), .. } => {
                    if append_prompt(&mut self.bytes, &bytes) {
                        self.demand();
                    } else {
                        self.failure = Some(Usage("the prompt exceeds the message bound".into()));
                        self.requests.push(io::Request::Close { entity: self.owner });
                    }
                }
                io::Event::Stream { up: stream::Up::End, .. } => {
                    self.requests.push(io::Request::Close { entity: self.owner });
                }
                io::Event::Stream { up: stream::Up::Failed(_), .. } => {
                    self.failure = Some(Usage("standard input failed before its end".into()));
                    self.requests.push(io::Request::Close { entity: self.owner });
                }
                io::Event::Closed { .. } => {}
                io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Connecting { .. }
                | io::Event::Connected { .. }
                | io::Event::Stream { up: stream::Up::Room, .. }
                | io::Event::Output { .. }
                | io::Event::Spawned { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Resized
                | io::Event::Window { .. }
                | io::Event::Shutdown { .. }
                | io::Event::Failed { .. } => unreachable!("the prompt owns only an input pipe"),
            }
        }
        while let Some(request) = self.requests.pop() {
            io::down(&mut self.io, &self.env, request, &mut self.submissions);
        }
        self.io.reclaim();
    }

    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, _now: Time) -> bool {
        !self.completions.is_empty() || !self.requests.is_empty() || !self.events.is_empty() || self.io.is_ready()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.io.is_empty() && self.submissions.is_empty() && self.completions.is_empty() && self.events.is_empty()
    }
    fn operations(&self) -> u32 {
        io::operations(&self.env.limits).expect("resolved IO operations")
    }
}

fn append_prompt(prompt: &mut List<u8>, bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| prompt.push(*byte).is_ok())
}

#[cfg(test)]
mod tests {
    use super::append_prompt;
    use skein_lib::List;

    #[test]
    fn a_prompt_read_as_it_arrives_is_the_same_whatever_its_cuts() {
        for cut in [20, 1] {
            let mut prompt = List::with_capacity(32);
            for delivery in b"abcdefghijklmnopqrst".chunks(cut) {
                assert!(append_prompt(&mut prompt, delivery), "prompt fits its bound");
            }
            assert_eq!(prompt.as_slice(), b"abcdefghijklmnopqrst");
        }
    }
}
