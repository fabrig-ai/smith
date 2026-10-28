//! One spawned agent's IO, pipe channel and host-domain terminals
//! (protocol/hosts.md, sections 3 and 5.6). The adapter keeps IO tokens,
//! process resources, channel buffers, and bounded plain-tree snapshots or
//! unknown marks.
//! `capture_plain` retains no snapshot for trees it cannot inspect whole;
//! `plain_status` counts them as changed (protocol/hosts.md, section 5.5).
//! File demands settle serially before markers or a snapshot become a terminal.
//! Internal handoffs are uncapped; shell and kernel edges retain credit
//! (protocol/limits.md, section 3.8).
//! It never knows local delivery
//! policy, a credential's meaning, or the agent's private domain state.
//! `up` converts kernel completions to host observations; `down` translates
//! host effects and submits IO. Gone remains the host domain's decision after
//! the process adapter has reported child exit, pipe EOF and reap.

use alloc::boxed::Box;
use skein_io::{self as io, file, file_layer, kernel};
use skein_lib::{Env, List, Map, Queue, Time, Token, Wall, stream};
use smith_host_domain as host;
use smith_host_protocol as protocol;
use smith_local_domain as local;
use smith_local_protocol as local_protocol;

use crate::StartValues;

/// Reusable child executable and credential-free environment.
#[derive(Clone, Debug)]
pub struct Launch {
    pub program: Box<[u8]>,
    pub arguments: Box<[Box<[u8]>]>,
    pub environment: Box<[Box<[u8]>]>,
    pub root: kernel::Fd,
    pub directory: Box<[u8]>,
}

impl Launch {
    fn for_spawn(&self) -> protocol::Launch {
        protocol::Launch {
            program: self.program.clone(),
            arguments: self.arguments.clone(),
            environment: self.environment.clone(),
            root: self.root,
            directory: self.directory.clone(),
        }
    }
}

/// Lower limits for one agent child and its channel, supplied by the service.
#[derive(Clone, Copy, Debug)]
pub struct ProcessLimits {
    pub io: io::Limits,
    pub channel: protocol::Limits,
    pub detail_bytes: u32,
    /// Kernel and shell edge capacity, and internal drain work per iteration.
    pub queue: u32,
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Io(Token),
    File(Token),
    ProgramRoot,
}

#[derive(Debug)]
enum PlainPurpose {
    Capture { directory: u32 },
    Compare { owner: Token, directory: u32 },
}

#[derive(Debug)]
struct PlainScan {
    purpose: PlainPurpose,
    scan: local_protocol::Plain,
}

#[derive(Debug)]
struct Closing {
    roots: u32,
    shutdown_pending: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum WindowRead {
    Idle,
    Requested,
    Pending,
    Again,
}

#[derive(Debug)]
pub(crate) struct ProcessAdapter {
    pub(crate) live: crate::live::Live,
    limits: ProcessLimits,
    pub(crate) programs: Option<Box<crate::service::program_check::Checks>>,
    launch: Launch,
    endpoints: smith_protocol_channel::Endpoints,
    io: io::Io,
    shell_output: Option<Token>,
    shell_events: Queue<io::Event>,
    process: Option<protocol::Process>,
    git: Option<local_protocol::GitChild>,
    markers: Option<(Token, local_protocol::Markers)>,
    plain: Option<PlainScan>,
    snapshots: Map<u32, Option<local_protocol::Snapshot>>,
    capture_directories: List<u32>,
    capture_next: u32,
    capture_deadline: Time,
    capture_done: Option<bool>,
    files: file_layer::FileIo,
    store: Option<local_protocol::Store>,
    cleanup_warning: Option<kernel::Error>,
    store_requests: Queue<local_protocol::StoreRequest>,
    store_actions: Queue<local_protocol::Action>,
    file_roots: Box<[Token]>,
    file_requests: Queue<(file::Request, Time)>,
    file_events: Queue<file::Event>,
    io_submissions: Queue<kernel::Submit>,
    file_submissions: Queue<kernel::Submit>,
    operations: Map<Token, Operation>,
    next_operation: u64,
    delivery_roots: Box<[kernel::Fd]>,
    git_environment: Box<[Box<[u8]>]>,
    io_events: Queue<io::Event>,
    pub(crate) io_requests: Queue<io::Request>,
    process_events: Queue<protocol::ProcessEvent>,
    submissions: Queue<kernel::Submit>,
    completions: Queue<kernel::Complete>,
    send: u64,
    reading: bool,
    failed: bool,
    terminal_input: Option<Token>,
    terminal_reading: local::Reading,
    window_read: WindowRead,
    signals: Option<Token>,
    closing: Option<Closing>,
    signal_operation: Option<Token>,
    close_root_pending: bool,
}

/// IO table capacities and the selected process channel's framing edges.
pub(crate) fn valid(limits: &ProcessLimits, framed: bool) -> bool {
    if !native_io_valid(&limits.io) || !local_protocol::Store::valid(&store_limits()) {
        return false;
    }
    if framed {
        let Ok(schema) = smith_protocol_channel::schema_for_start(&limits.channel.bodies, limits.channel.start_bytes)
        else {
            return false;
        };
        return schema.check(skein_channel::Role::Initiator, &limits.channel.channel).is_ok();
    }
    true
}

/// Whether native IO is usable and its flight and timer capacities fit (protocol/limits.md, section 3).
#[must_use]
pub fn native_io_valid(limits: &io::Limits) -> bool {
    let Some(operations) = io::operations(limits) else { return false };
    limits.is_usable() && operations.checked_mul(2).is_some() && limits.sockets.checked_mul(2).is_some()
}

impl ProcessAdapter {
    pub(crate) fn new(
        limits: &ProcessLimits,
        launch: Launch,
        endpoints: smith_protocol_channel::Endpoints,
        seed: u64,
    ) -> Option<Self> {
        if !limits.io.is_usable() {
            return None;
        }
        let mut files =
            file_layer::FileIo::with_whole_limit(66, 4096, 8196, 64 << 20, skein_lib::Duration::from_secs(30));
        files.seed_randomness(seed);
        Some(Self {
            io: io::Io::new(&limits.io),
            programs: None,
            shell_output: None,
            shell_events: Queue::with_capacity(limits.queue),
            live: crate::live::Live::new(),
            limits: *limits,
            launch,
            endpoints,
            process: None,
            git: None,
            markers: None,
            plain: None,
            snapshots: Map::with_capacity(64),
            capture_directories: List::with_capacity(64),
            capture_next: 0,
            capture_deadline: Time::ZERO,
            capture_done: None,
            files,
            store: None,
            cleanup_warning: None,
            store_requests: Queue::uncapped(),
            store_actions: Queue::uncapped(),
            file_roots: Box::new([]),
            file_requests: Queue::uncapped(),
            file_events: Queue::uncapped(),
            io_submissions: Queue::uncapped(),
            file_submissions: Queue::uncapped(),
            operations: Map::with_capacity(limits.queue),
            next_operation: 1,
            delivery_roots: Box::new([]),
            git_environment: Box::new([]),
            io_events: Queue::uncapped(),
            io_requests: Queue::uncapped(),
            process_events: Queue::uncapped(),
            submissions: Queue::with_capacity(limits.queue),
            completions: Queue::with_capacity(limits.queue),
            send: 1,
            reading: false,
            failed: false,
            terminal_input: None,
            terminal_reading: local::Reading::Open,
            window_read: WindowRead::Idle,
            signals: None,
            closing: None,
            signal_operation: None,
            close_root_pending: false,
        })
    }

    // Internal handoffs count toward admission; edges never contribute.
    pub(crate) fn backlog(&self) -> u32 {
        let mut total = 0_u32;
        for entries in [
            self.io_events.len(),
            self.io_requests.len(),
            self.io_submissions.len(),
            self.file_requests.len(),
            self.file_events.len(),
            self.file_submissions.len(),
            self.store_requests.len(),
            self.store_actions.len(),
            self.process_events.len(),
        ] {
            total = total.checked_add(entries).expect("process backlog entries fit u32");
        }
        total
    }

    // O2: dispatch a store transition's actions before consuming the next file
    // terminal, so its answer precedes a later marker or plain-scan result.
    fn store_actions_dispatched(&self) -> bool {
        self.store_actions.is_empty()
    }

    pub(crate) fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }

    pub(crate) fn io(&mut self) -> &mut io::Io {
        &mut self.io
    }

    pub(crate) fn adopt_writer(&mut self, fd: kernel::Fd) -> Result<Token, kernel::Fd> {
        let token = self.io.adopt_write_pipe(fd, None)?;
        self.shell_output = Some(token);
        Ok(token)
    }

    pub(crate) fn io_requests(&mut self) -> &mut Queue<io::Request> {
        &mut self.io_requests
    }

    pub(crate) fn adopt_terminal(
        &mut self,
        input: Option<kernel::Fd>,
        signals: kernel::Fd,
        read_input: bool,
    ) -> Result<(), kernel::Fd> {
        if let Some(input) = input {
            let stream = self.io.adopt_read_pipe(input)?;
            self.terminal_input = Some(stream);
            if read_input {
                self.io_requests.push(io::Request::Stream {
                    stream,
                    down: stream::Down::Demand { read: stream::Read::Fill(1), room: 0 },
                });
            }
        }
        let signal = self.io.adopt_signals(signals)?;
        self.signals = Some(signal);
        Ok(())
    }

    pub(crate) fn read_window(&mut self) {
        if self.closing.is_none() && self.terminal_input.is_some() {
            self.window_read = match self.window_read {
                WindowRead::Idle | WindowRead::Requested => WindowRead::Requested,
                WindowRead::Pending | WindowRead::Again => WindowRead::Again,
            };
        }
    }

    pub(crate) fn input(&mut self, reading: local::Reading) {
        if self.terminal_reading == reading {
            return;
        }
        self.terminal_reading = reading;
        if self.closing.is_none()
            && let Some(stream) = self.terminal_input
        {
            let read = match reading {
                local::Reading::Open => stream::Read::Fill(1),
                local::Reading::Held => stream::Read::Nothing,
            };
            self.io_requests.push(io::Request::Stream { stream, down: stream::Down::Demand { read, room: 0 } });
        }
    }

    pub(crate) fn adopt_delivery_roots(&mut self, roots: Box<[kernel::Fd]>, environment: Box<[Box<[u8]>]>) {
        let mut file_roots = List::with_capacity(u32::try_from(roots.len()).expect("admitted root count"));
        for root in &roots {
            file_roots
                .push(self.files.adopt_root(*root).expect("bounded workspace roots"))
                .expect("one token per root");
        }
        self.file_roots = file_roots.into_boxed();
        self.delivery_roots = roots;
        self.git_environment = environment;
    }

    pub(crate) fn at(&self, now: Time, wall: Wall) -> Env<io::Limits> {
        Env { now, wall, limits: self.limits.io }
    }

    pub(crate) fn adopt_trace(
        &mut self,
        fd: kernel::Fd,
        write_deadline: skein_lib::Duration,
    ) -> Result<Token, kernel::Fd> {
        self.io.adopt_append(fd, write_deadline)
    }

    pub(crate) fn adopt_output(
        &mut self,
        descriptor: kernel::Fd,
        append: bool,
        write_deadline: skein_lib::Duration,
    ) -> Result<Token, kernel::Fd> {
        if append {
            self.io.adopt_append(descriptor, write_deadline)
        } else {
            self.io.adopt_write_pipe(descriptor, Some(write_deadline))
        }
    }

    pub(crate) fn adopt_store(&mut self, root: kernel::Fd) -> Option<()> {
        let token = self.files.adopt_root(root)?;
        self.store = Some(local_protocol::Store::new(
            Token::new(u64::MAX - 1),
            token,
            root,
            store_limits(),
            self.endpoints.clone(),
        )?);
        Some(())
    }

    pub(crate) fn ephemeral(&mut self, parent: kernel::Fd, name: Box<[u8]>) -> Result<(), kernel::Fd> {
        let token = self.files.adopt_root(parent).ok_or(parent)?;
        self.store.as_mut().expect("adopted private store").ephemeral(token, parent, name);
        Ok(())
    }

    pub(crate) fn endpoint_name(&self, number: u32) -> Option<&[u8]> {
        self.endpoints.name_for(number)
    }

    pub(crate) fn cleanup_warning(&mut self) -> Option<kernel::Error> {
        self.cleanup_warning.take()
    }

    #[cfg(test)]
    pub(crate) fn store_requests(&mut self) -> &mut Queue<local_protocol::StoreRequest> {
        &mut self.store_requests
    }

    pub(crate) fn store_request(&mut self, request: local_protocol::StoreRequest) {
        self.store_requests.push(request);
    }

    fn store_pass(&mut self, now: Time, local_events: &mut Queue<local::Event>) {
        for _ in 0_u8..3_u8 {
            let Some(action) = self.store_actions.pop() else { break };
            match action {
                local_protocol::Action::Io { request, deadline } => self.file_requests.push((request, deadline)),
                local_protocol::Action::Domain(event) => local_events.push(event),
                local_protocol::Action::Fault(_) => {}
                local_protocol::Action::CleanupWarning { error } => self.cleanup_warning = Some(error),
                local_protocol::Action::Closed => self.store = None,
            }
        }
        if self.store_actions.is_empty()
            && let Some(store) = &mut self.store
        {
            if self.closing.is_some() {
                store.close(now, &mut self.store_actions);
            } else if store.takes()
                && let Some(request) = self.store_requests.pop()
            {
                store.request(request, now, &mut self.store_actions);
            }
        }
    }

    pub(crate) fn close(&mut self) {
        if self.closing.is_some() {
            return;
        }
        self.cancel_programs();
        self.closing = Some(Closing {
            roots: u32::try_from(self.file_roots.len()).expect("admitted root count"),
            shutdown_pending: false,
        });
        if let Some(entity) = self.terminal_input {
            self.io_requests.push(io::Request::Close { entity });
        }
    }

    fn closing_roots(&self) -> u32 {
        match &self.closing {
            Some(closing) => closing.roots,
            None => 0,
        }
    }

    pub(crate) fn signal_streams(&self) -> u32 {
        u32::from(self.signals.is_some())
    }

    pub(crate) fn close_signals(&mut self) {
        if let Some(entity) = self.signals {
            self.io_requests.push(io::Request::Close { entity });
        }
    }

    pub(crate) fn take_shutdown(&mut self) -> bool {
        match &mut self.closing {
            Some(closing) => core::mem::replace(&mut closing.shutdown_pending, false),
            None => false,
        }
    }

    pub(crate) fn abort_stream(&mut self, trace: &mut Option<crate::stream::Spawned>) {
        if let Some(trace) = trace {
            trace.producer.sinks().abort(&mut self.io_requests);
        }
    }

    pub(crate) fn abort(&mut self) {
        if let Some(entity) = self.terminal_input {
            self.io_requests.push(io::Request::Abort { entity });
        }
    }

    pub(crate) fn closed(&self) -> bool {
        self.settled(0) && self.io.is_empty()
    }

    pub(crate) fn settled(&self, streams: u32) -> bool {
        self.closing.is_some()
            && match &self.programs {
                Some(checks) => checks.closed(),
                None => true,
            }
            && self.closing_roots() == 0
            && self.io.sockets() == streams
            && self.store.is_none()
            && self.store_requests.is_empty()
            && self.store_actions.is_empty()
            && self.files.takes()
            && self.files.open_files() == 0
            && self.operations.len() == u32::from(self.signal_operation.is_some())
            && self.file_requests.is_empty()
            && self.file_events.is_empty()
            && self.io_requests.is_empty()
            && self.io_events.is_empty()
            && self.io_submissions.is_empty()
            && self.file_submissions.is_empty()
            && self.submissions.is_empty()
            && self.completions.is_empty()
    }

    pub(crate) fn capture_plain(&mut self, directories: List<u32>, deadline: Time) {
        assert!(self.plain.is_none() && self.markers.is_none(), "file inspection is serial");
        self.snapshots = Map::with_capacity(64);
        self.capture_directories = directories;
        self.capture_next = 0;
        self.capture_deadline = deadline;
        self.capture_done = None;
        self.next_capture();
    }

    pub(crate) fn take_capture_done(&mut self) -> Option<bool> {
        self.capture_done.take()
    }

    fn next_capture(&mut self) {
        match self.capture_directories.get(self.capture_next) {
            Some(directory) => {
                let directory = *directory;
                let root = match self.file_roots.get(usize::try_from(directory).expect("bounded directory")) {
                    Some(root) => *root,
                    None => {
                        self.capture_done = Some(false);
                        return;
                    }
                };
                let scan = local_protocol::Plain::new(Token::new(0), root, plain_limits(), self.capture_deadline)
                    .expect("checked plain limits");
                let action = scan.start();
                self.plain = Some(PlainScan { purpose: PlainPurpose::Capture { directory }, scan });
                match action {
                    local_protocol::PlainAction::File { request, deadline } => {
                        self.file_requests.push((request, deadline));
                    }
                    local_protocol::PlainAction::Done | local_protocol::PlainAction::Failed => {
                        unreachable!("root is first scan")
                    }
                }
            }
            None => self.capture_done = Some(true),
        }
    }

    pub(crate) fn plain_status(
        &mut self,
        owner: Token,
        directory: u32,
        deadline: Time,
        events: &mut Queue<local::Event>,
    ) {
        assert!(self.plain.is_none() && self.markers.is_none(), "file inspection is serial");
        let root = match self.file_roots.get(usize::try_from(directory).expect("bounded directory")) {
            Some(root) => *root,
            None => {
                events.push(local::Event::StoreFailed { turn: None, reason: local::StoreFailure::Read });
                return;
            }
        };
        match self.snapshots.get(&directory) {
            Some(Some(_)) => {}
            Some(None) => {
                events.push(local::Event::PlainStatus { owner, changed: true });
                return;
            }
            None => {
                events.push(local::Event::StoreFailed { turn: None, reason: local::StoreFailure::Read });
                return;
            }
        }
        let scan = local_protocol::Plain::new(owner, root, plain_limits(), deadline).expect("checked plain limits");
        let action = scan.start();
        self.plain = Some(PlainScan { purpose: PlainPurpose::Compare { owner, directory }, scan });
        self.plain_action(action, events);
    }

    fn plain_action(&mut self, action: local_protocol::PlainAction, events: &mut Queue<local::Event>) {
        match action {
            local_protocol::PlainAction::File { request, deadline } => self.file_requests.push((request, deadline)),
            local_protocol::PlainAction::Done => {
                let active = self.plain.take().expect("active plain scan");
                let snapshot = active.scan.finish();
                match active.purpose {
                    PlainPurpose::Capture { directory } => {
                        self.snapshots.insert(directory, Some(snapshot)).expect("bounded snapshots");
                        self.capture_next = self.capture_next.checked_add(1).expect("bounded directory position");
                        self.next_capture();
                    }
                    PlainPurpose::Compare { owner, directory } => {
                        let before = self
                            .snapshots
                            .get(&directory)
                            .expect("capture precedes compare")
                            .as_ref()
                            .expect("unknown capture answers without a scan");
                        events.push(local::Event::PlainStatus { owner, changed: *before != snapshot });
                    }
                }
            }
            local_protocol::PlainAction::Failed => {
                let active = self.plain.take().expect("active plain scan");
                match active.purpose {
                    PlainPurpose::Capture { directory } => {
                        self.snapshots.insert(directory, None).expect("bounded snapshots");
                        self.capture_next = self.capture_next.checked_add(1).expect("bounded directory position");
                        self.next_capture();
                    }
                    PlainPurpose::Compare { owner, .. } => {
                        events.push(local::Event::PlainStatus { owner, changed: true });
                    }
                }
            }
        }
    }

    pub(crate) fn start_markers(
        &mut self,
        owner: Token,
        directory: u32,
        paths: Box<[Box<[u8]>]>,
        deadline: Time,
        events: &mut Queue<local::Event>,
    ) {
        assert!(self.markers.is_none() && self.plain.is_none(), "one local file inspection at a time");
        let root = self.file_roots.get(usize::try_from(directory).expect("admitted directory"));
        let markers = match root {
            Some(root) => local_protocol::Markers::new(owner, *root, paths, deadline, git_limits(&self.limits)),
            None => None,
        };
        match markers {
            Some(markers) => {
                let action = markers.start();
                self.markers = Some((owner, markers));
                self.marker_action(owner, action, events);
            }
            None => events.push(local::Event::Git {
                owner,
                result: local::GitResult::Failed {
                    reason: smith_domain::run::DeliveryReason::TooLarge,
                    diagnostic: Box::new(smith_domain::run::Diagnostic::empty()),
                },
            }),
        }
    }

    fn marker_action(&mut self, owner: Token, action: local_protocol::MarkerAction, events: &mut Queue<local::Event>) {
        match action {
            local_protocol::MarkerAction::File { request, deadline } => self.file_requests.push((request, deadline)),
            local_protocol::MarkerAction::Done(result) => {
                self.markers = None;
                events.push(local::Event::Git { owner, result });
            }
        }
    }

    pub(crate) fn start_git(
        &mut self,
        owner: Token,
        directory: u32,
        op: smith_local_domain::GitOp,
        deadline: Time,
        events: &mut Queue<smith_local_domain::Event>,
    ) {
        assert!(self.git.is_none(), "the local domain serializes git operations");
        let root = match usize::try_from(directory) {
            Ok(index) => self.delivery_roots.get(index).copied(),
            Err(_) => None,
        };
        self.git = match root {
            Some(root) => local_protocol::GitChild::new(
                owner,
                Token::new(1 << 62),
                root,
                self.git_environment.clone(),
                op,
                deadline,
                git_limits(&self.limits),
                &mut self.io_requests,
            ),
            None => None,
        };
        if self.git.is_none() {
            events.push(smith_local_domain::Event::Git {
                owner,
                result: smith_local_domain::GitResult::Failed {
                    reason: smith_domain::run::DeliveryReason::Broken,
                    diagnostic: Box::new(smith_domain::run::Diagnostic::empty()),
                },
            });
        }
    }

    pub(crate) fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }

    pub(crate) fn next_deadline(&self) -> Option<Time> {
        let git_deadline = match &self.git {
            Some(git) => git.next_deadline(),
            None => None,
        };
        let process_deadline = match &self.process {
            Some(process) => process.next_deadline(),
            None => None,
        };
        let mut next: Option<Time> = None;
        for deadline in [
            self.io.next_deadline(),
            process_deadline,
            git_deadline,
            self.files.next_deadline(),
            match &self.programs {
                Some(checks) => checks.next_deadline(),
                None => None,
            },
        ]
        .into_iter()
        .flatten()
        {
            next = Some(match next {
                Some(current) => current.min(deadline),
                None => deadline,
            });
        }
        next
    }

    pub(crate) fn work_pending(&self, now: Time) -> bool {
        match &self.programs {
            Some(checks) if checks.work_pending(now, self.closing.is_some()) => return true,
            Some(_) | None => {}
        }
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.io_events.is_empty()
            || !self.shell_events.is_empty()
            || !self.io_requests.is_empty()
            || self.window_read == WindowRead::Requested
            || self.process_work_pending()
            || !self.completions.is_empty()
            || (self.store_actions_dispatched() && !self.file_events.is_empty())
            || !self.store_actions.is_empty()
            || (!self.store_requests.is_empty()
                && match &self.store {
                    Some(store) => store.takes(),
                    None => false,
                })
            || (!self.file_requests.is_empty() && self.files.takes())
            || !self.io_submissions.is_empty()
            || !self.file_submissions.is_empty()
            || self.files.is_due(now)
            || (self.closing_roots() > 0 && self.files.takes() && !self.close_root_pending)
    }

    pub(crate) const fn failed(&self) -> bool {
        self.failed
    }

    fn reap(&mut self, env: &Env<io::Limits>) {
        for _ in 0..self.completions.capacity() {
            let Some(complete) = self.completions.pop() else { break };
            if self.signal_operation == Some(complete.op) {
                self.signal_operation = None;
            }
            let route = self.operations.remove(&complete.op).expect("one route per submitted kernel operation");
            match route {
                Operation::ProgramRoot => self.programs.as_mut().expect("program root close").root_closed(),
                Operation::Io(op) => io::up(
                    &mut self.io,
                    env,
                    kernel::Complete { op, kind: complete.kind, result: complete.result },
                    &mut self.io_events,
                    &mut self.io_submissions,
                ),
                Operation::File(op) => file_layer::up(
                    &mut self.files,
                    kernel::Complete { op, kind: complete.kind, result: complete.result },
                    &mut self.file_events,
                    &mut self.file_submissions,
                ),
            }
        }
    }

    fn markers_up(&mut self, now: Time, local_events: &mut Queue<local::Event>) {
        if self.files.is_due(now) {
            file_layer::expire(&mut self.files, now, &mut self.file_submissions);
        }
        self.store_pass(now, local_events);
        for _ in 0..self.limits.queue {
            if !self.store_actions_dispatched() {
                break;
            }
            let Some(event) = self.file_events.pop() else { break };
            if event.owner() == Token::new(u64::MAX - 1) {
                if let file::Event::Opened { file, .. } = &event {
                    let descriptor = self.files.descriptor(*file).expect("opened directory is owned by the file layer");
                    self.store.as_mut().expect("store owns its directory open").opened_directory_descriptor(descriptor);
                }
                self.store.as_mut().expect("store owner remains until Closed").from_io(
                    event,
                    now,
                    &mut self.store_actions,
                );
                continue;
            }
            if self.close_root_pending && event.owner() == Token::new(u64::MAX) {
                self.close_root_pending = false;
                continue;
            }
            match &mut self.plain {
                Some(active) => {
                    let action = active.scan.from_file(event);
                    self.plain_action(action, local_events);
                }
                None => {
                    let (owner, markers) = self.markers.as_mut().expect("file terminal belongs to marker inspection");
                    let owner = *owner;
                    let action = markers.from_file(event);
                    self.marker_action(owner, action, local_events);
                }
            }
        }
    }

    pub(crate) fn io_up(&mut self, env: &Env<io::Limits>) {
        let now = env.now;
        for _ in 0..self.limits.io.sockets {
            if !self.io.is_ready() {
                break;
            }
            io::resume(&mut self.io, env, &mut self.io_events, &mut self.io_submissions);
        }
        self.reap(env);
        for _ in 0..self.limits.io.sockets {
            if self.io.is_ready() {
                io::resume(&mut self.io, env, &mut self.io_events, &mut self.io_submissions);
            }
            if self.io.is_due(now) {
                io::fire(&mut self.io, env, &mut self.io_events, &mut self.io_submissions);
            }
        }
    }

    pub(crate) fn shell_events(&mut self) -> &mut Queue<io::Event> {
        &mut self.shell_events
    }

    pub(crate) fn effects_up(&mut self, effects: &mut smith_agent_service::Effects) {
        for _ in 0..self.io_events.len() {
            let event = self.io_events.pop().expect("bounded native terminals");
            if effects.owns_io(&event) {
                effects.io_event(event);
            } else {
                self.io_events.push(event);
            }
        }
    }

    pub(crate) fn up(
        &mut self,
        env: &Env<io::Limits>,
        host_events: &mut Queue<host::Input>,
        terminal: &mut local_protocol::Terminal,
        terminal_events: &mut Queue<local_protocol::TerminalEvent>,
        local_events: &mut Queue<smith_local_domain::Event>,
        trace: &mut Option<crate::stream::Spawned>,
    ) {
        let now = env.now;
        self.markers_up(now, local_events);
        for _ in 0..self.limits.queue {
            if terminal_events.room() < local_protocol::terminal_max_out() || self.shell_events.room() == 0 {
                break;
            }
            let Some(event) = self.io_events.pop() else { break };
            let Some(event) = trace_up(trace, now, event) else { continue };
            if self.shell_output.is_some() && io_owner(&event) == self.shell_output {
                self.writer_up(event);
                continue;
            }

            if self.program_owns(io_owner(&event)) {
                self.programs.as_mut().expect("program route").receive(event, &mut self.io_requests);
                continue;
            }
            let git_owned = match &self.git {
                Some(git) => match io_owner(&event) {
                    Some(owner) => git.owns(owner),
                    None => false,
                },
                None => false,
            };
            if git_owned {
                self.git.as_mut().expect("git route exists").from_io(event, local_events, &mut self.io_requests);
                continue;
            }
            match event {
                io::Event::Resized => self.shell_events.push(io::Event::Resized),
                event @ io::Event::Window { .. } => {
                    self.window_read = match self.window_read {
                        WindowRead::Again => WindowRead::Requested,
                        WindowRead::Idle | WindowRead::Requested | WindowRead::Pending => WindowRead::Idle,
                    };
                    self.shell_events.push(event);
                }
                io::Event::Shutdown { .. } => {
                    if let Some(closing) = &mut self.closing {
                        closing.shutdown_pending = true;
                    } else {
                        terminal.interrupt(terminal_events);
                    }
                }
                io::Event::Stream { owner, up } if Some(owner) == self.terminal_input => match up {
                    stream::Up::Bytes(bytes) => {
                        for byte in bytes {
                            terminal.feed(byte, terminal_events);
                        }
                        if self.closing.is_none() && self.terminal_reading == local::Reading::Open {
                            self.io_requests.push(io::Request::Stream {
                                stream: owner,
                                down: stream::Down::Demand { read: stream::Read::Fill(1), room: 0 },
                            });
                        }
                    }
                    stream::Up::End | stream::Up::Failed(_) => terminal.closed(terminal_events),
                    stream::Up::Room => unreachable!("read-only terminal asks for no output room"),
                },
                io::Event::Closed { owner } if Some(owner) == self.terminal_input => self.terminal_input = None,
                io::Event::Closed { owner } if Some(owner) == self.signals => self.signals = None,
                other @ (io::Event::Listening { .. }
                | io::Event::Accepted { .. }
                | io::Event::Connecting { .. }
                | io::Event::Connected { .. }
                | io::Event::Stream { .. }
                | io::Event::Output { .. }
                | io::Event::Spawned { .. }
                | io::Event::Exited { .. }
                | io::Event::Usage { .. }
                | io::Event::Failed { .. }
                | io::Event::Closed { .. }) => match &mut self.process {
                    Some(process) => process.from_io(now, other, &mut self.process_events, &mut self.io_requests),
                    None => self.shell_events.push(other),
                },
            }
        }
        if let Some(git) = &mut self.git {
            git.fire(now, &mut self.io_requests);
        }
        let git_done = match &self.git {
            Some(git) => git.done(),
            None => false,
        };
        if git_done {
            self.git = None;
        }
        if let Some(process) = &mut self.process {
            process.fire(now, &mut self.process_events, &mut self.io_requests);
        }
        self.drain_process(host_events);
    }

    fn writer_up(&mut self, event: io::Event) {
        match &event {
            io::Event::Closed { .. } => self.shell_output = None,
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Stream { .. }
            | io::Event::Output { .. }
            | io::Event::Spawned { .. }
            | io::Event::Exited { .. }
            | io::Event::Usage { .. }
            | io::Event::Resized
            | io::Event::Window { .. }
            | io::Event::Shutdown { .. }
            | io::Event::Failed { .. } => {}
        }
        self.shell_events.push(event);
    }

    fn process_work_pending(&self) -> bool {
        for event in &self.process_events {
            if self.reading || !read_terminal(event) {
                return true;
            }
        }
        false
    }

    fn drain_process(&mut self, host_events: &mut Queue<host::Input>) {
        for _ in 0..self.process_events.len() {
            let event = self.process_events.pop().expect("bounded queued events");
            match &event {
                protocol::ProcessEvent::Channel { event, .. } => match event.as_ref() {
                    protocol::OpenEvent::WriteFailed => continue,
                    protocol::OpenEvent::Opened { .. }
                    | protocol::OpenEvent::Sent { .. }
                    | protocol::OpenEvent::Unsent { .. }
                    | protocol::OpenEvent::Hangup { .. }
                    | protocol::OpenEvent::Answer { .. }
                    | protocol::OpenEvent::Admitted
                    | protocol::OpenEvent::MessageRefused { .. }
                    | protocol::OpenEvent::Waiting { .. }
                    | protocol::OpenEvent::Long { .. }
                    | protocol::OpenEvent::LongDone
                    | protocol::OpenEvent::Turn { .. }
                    | protocol::OpenEvent::Fact { .. }
                    | protocol::OpenEvent::Live { .. }
                    | protocol::OpenEvent::Unauthorized { .. }
                    | protocol::OpenEvent::Limited { .. }
                    | protocol::OpenEvent::Refused { .. }
                    | protocol::OpenEvent::Call { .. }
                    | protocol::OpenEvent::Withdraw { .. } => {}
                },
                protocol::ProcessEvent::Spawned { .. }
                | protocol::ProcessEvent::Unspawned { .. }
                | protocol::ProcessEvent::Exited { .. }
                | protocol::ProcessEvent::Reaped { .. } => {}
            }
            if read_terminal(&event) {
                if !self.reading {
                    self.process_events.push(event);
                    continue;
                }
                self.reading = false;
            }
            host_events.push(to_host_event(event, &mut self.live));
        }
    }

    pub(crate) fn down(
        &mut self,
        env: &Env<io::Limits>,
        requests: &mut Queue<host::process::Request>,
        start_values: &mut Option<StartValues>,
        grant_values: &Map<u32, smith_protocol_channel::GrantValue>,
        host_events: &mut Queue<host::Input>,
        trace: &mut Option<crate::stream::Spawned>,
    ) {
        let now = env.now;
        if self.window_read == WindowRead::Requested {
            self.window_read = WindowRead::Idle;
            if self.closing.is_none()
                && let Some(stream) = self.terminal_input
            {
                self.window_read = WindowRead::Pending;
                self.io_requests.push(io::Request::Window { owner: stream, stream });
            }
        }
        if let Some(trace) = trace {
            if self.settled(trace.producer.streams().checked_add(self.signal_streams()).expect("bounded signal stream"))
            {
                trace.measure(&mut self.io_requests);
            }
            trace.producer.sinks().down(now, &mut self.io_requests);
        }
        for _ in 0..self.limits.queue {
            let Some(request) = requests.pop() else { break };
            self.request(request, start_values, grant_values, host_events);
        }
        if let Some(process) = &mut self.process {
            for _ in 0..self.limits.queue {
                let Some(request) = process.next_channel_down() else { break };
                process.channel_down(request, &mut self.io_requests);
            }
        }
        for _ in 0..self.limits.queue {
            if !self.io.takes() {
                break;
            }
            let Some(request) = self.io_requests.pop() else { break };
            io::down(&mut self.io, env, request, &mut self.io_submissions);
        }
        if self.files.takes()
            && let Some((request, deadline)) = self.file_requests.pop()
        {
            file_layer::down_until(
                &mut self.files,
                deadline,
                request,
                &mut self.file_events,
                &mut self.file_submissions,
            );
        }
        if self.closing_roots() > 0 && !self.close_root_pending && self.files.takes() {
            let position = self.closing_roots().checked_sub(1).expect("positive roots to close");
            self.closing.as_mut().expect("closing roots").roots = position;
            let root =
                *self.file_roots.get(usize::try_from(position).expect("root position fits")).expect("adopted root");
            self.close_root_pending = true;
            file_layer::down(
                &mut self.files,
                now,
                file::Request::Close { owner: Token::new(u64::MAX), file: root },
                &mut self.file_events,
                &mut self.file_submissions,
            );
        }
        if self.closing.is_some() && self.submissions.room() > 0 && self.operations.len() < self.operations.capacity() {
            let root = match &mut self.programs {
                Some(checks) => checks.closing_root(),
                None => None,
            };
            if let Some(fd) = root {
                let op = Token::new(self.next_operation);
                self.next_operation = self.next_operation.checked_add(1).expect("root close operation");
                self.operations.insert(op, Operation::ProgramRoot).expect("root close route");
                self.submissions.push(kernel::Submit { op, kind: kernel::Op::Close { fd } });
            }
        }
        self.flush_submissions();
        self.io.reclaim();
    }

    fn flush_submissions(&mut self) {
        for _ in 0..self.limits.queue {
            if self.submissions.room() == 0 || self.operations.len() == self.operations.capacity() {
                break;
            }
            let Some(submit) = self.io_submissions.pop() else { break };
            self.route_submission(submit, false);
        }
        for _ in 0..self.limits.queue {
            if self.submissions.room() == 0 || self.operations.len() == self.operations.capacity() {
                break;
            }
            let Some(submit) = self.file_submissions.pop() else { break };
            self.route_submission(submit, true);
        }
    }

    #[expect(
        clippy::wildcard_enum_match_arm,
        reason = "kernel operations are external; only Cancel references another operation"
    )]
    fn route_submission(&mut self, submit: kernel::Submit, file: bool) {
        let global = Token::new(self.next_operation);
        self.next_operation = self.next_operation.checked_add(1).expect("kernel operation tokens never wrap");
        let kind = match submit.kind {
            kernel::Op::Cancel { target } => {
                let mut found = None;
                for (global, route) in &self.operations {
                    match route {
                        Operation::File(original) if file && *original == target => found = Some(*global),
                        Operation::Io(original) if !file && *original == target => found = Some(*global),
                        Operation::File(_) | Operation::Io(_) | Operation::ProgramRoot => {}
                    }
                }
                kernel::Op::Cancel { target: found.unwrap_or(Token::new(u64::MAX)) }
            }
            other => other,
        };
        if let kernel::Op::ReadSignal { .. } = &kind {
            assert!(self.signal_operation.is_none(), "one retained signal read");
            self.signal_operation = Some(global);
        }
        let route = if file { Operation::File(submit.op) } else { Operation::Io(submit.op) };
        self.operations.insert(global, route).expect("reserved kernel route");
        self.submissions.push(kernel::Submit { op: global, kind });
    }

    fn request(
        &mut self,
        request: host::process::Request,
        start_values: &mut Option<StartValues>,
        grant_values: &Map<u32, smith_protocol_channel::GrantValue>,
        host_events: &mut Queue<host::Input>,
    ) {
        match request {
            host::process::Request::Spawn { owner, deadline, .. } => {
                let Ok(mut process) = protocol::Process::new(
                    owner,
                    &self.limits.channel,
                    self.limits.detail_bytes,
                    self.endpoints.clone(),
                ) else {
                    self.failed = true;
                    host_events.push(host::Input::Process(host::process::Event::Unspawned {
                        owner,
                        detail: Box::from(&b"invalid channel"[..]),
                    }));
                    return;
                };
                process.spawn(self.launch.for_spawn(), deadline, &mut self.io_requests);
                self.process = Some(process);
            }
            host::process::Request::Send { owner, message, .. } => {
                let send_token = Token::new(self.send);
                self.send = self.send.checked_add(1).expect("bounded send tokens");
                let process = self.process.as_mut().expect("Spawn precedes Send");
                let sent = match message {
                    host::Down::Start { start, window } => {
                        let asked = match start.live {
                            Some(ask) => ask.text,
                            None => false,
                        };
                        self.live.start(owner, asked);
                        let values = start_values.take().expect("Start values paired before Spawn");
                        process.send_start(
                            start,
                            window,
                            protocol::Values { paths: values.paths, credentials: values.credentials },
                            send_token,
                            &mut self.process_events,
                            &mut self.io_requests,
                        )
                    }
                    host::Down::Message { finish_requested, name, label, text } => process.send_message(
                        host::Message { finish_requested, name, label, text },
                        send_token,
                        &mut self.process_events,
                        &mut self.io_requests,
                    ),
                    host::Down::Answer { call, reply } => {
                        process.send_reply(call, reply, send_token, &mut self.process_events, &mut self.io_requests)
                    }
                    host::Down::Acknowledge { turn } => {
                        process.send_acknowledge(turn, send_token, &mut self.process_events, &mut self.io_requests)
                    }
                    host::Down::Grant { grant } => {
                        let value = grant_values.get(&grant.account).expect("grant value supplied before send");
                        process.send_grant(
                            grant,
                            value.clone(),
                            send_token,
                            &mut self.process_events,
                            &mut self.io_requests,
                        )
                    }
                    host::Down::Cancel => {
                        process.send_cancel(send_token, &mut self.process_events, &mut self.io_requests)
                    }
                };
                if sent.is_err() {
                    self.failed = true;
                    host_events.push(host::Input::Process(host::process::Event::Unsent { owner }));
                }
            }
            host::process::Request::Signal { owner, signal, .. } => {
                let signal = match signal {
                    host::Signal::Terminate => kernel::Signal::Terminate,
                    host::Signal::Kill => kernel::Signal::Kill,
                };
                self.process.as_ref().expect("spawned child").signal(signal, &mut self.io_requests);
                host_events.push(host::Input::Process(host::process::Event::Signalled { owner }));
            }
            host::process::Request::Read { .. } => {
                assert!(!self.reading, "one host read at a time");
                self.reading = true;
                self.process
                    .as_mut()
                    .expect("Spawn precedes Read")
                    .read(&mut self.process_events, &mut self.io_requests);
            }
            host::process::Request::Wait { .. } | host::process::Request::Reap { .. } => {}
        }
    }
}

fn read_terminal(event: &protocol::ProcessEvent) -> bool {
    match event {
        protocol::ProcessEvent::Channel { event, .. } => match event.as_ref() {
            protocol::OpenEvent::Opened { .. }
            | protocol::OpenEvent::Sent { .. }
            | protocol::OpenEvent::Unsent { .. }
            | protocol::OpenEvent::WriteFailed => false,
            protocol::OpenEvent::Hangup { .. }
            | protocol::OpenEvent::Answer { .. }
            | protocol::OpenEvent::Admitted
            | protocol::OpenEvent::MessageRefused { .. }
            | protocol::OpenEvent::Waiting { .. }
            | protocol::OpenEvent::Long { .. }
            | protocol::OpenEvent::LongDone
            | protocol::OpenEvent::Turn { .. }
            | protocol::OpenEvent::Fact { .. }
            | protocol::OpenEvent::Live { .. }
            | protocol::OpenEvent::Unauthorized { .. }
            | protocol::OpenEvent::Limited { .. }
            | protocol::OpenEvent::Refused { .. }
            | protocol::OpenEvent::Call { .. }
            | protocol::OpenEvent::Withdraw { .. } => true,
        },
        protocol::ProcessEvent::Spawned { .. }
        | protocol::ProcessEvent::Unspawned { .. }
        | protocol::ProcessEvent::Exited { .. }
        | protocol::ProcessEvent::Reaped { .. } => false,
    }
}

fn to_host_event(event: protocol::ProcessEvent, live: &mut crate::live::Live) -> host::Input {
    match event {
        protocol::ProcessEvent::Spawned { agent, process } => {
            host::Input::Process(host::process::Event::Spawned { owner: agent, process })
        }
        protocol::ProcessEvent::Unspawned { agent, detail } => {
            host::Input::Process(host::process::Event::Unspawned { owner: agent, detail })
        }
        protocol::ProcessEvent::Exited { agent, exit } => {
            host::Input::Process(host::process::Event::Exited { owner: agent, exit: to_host_exit(exit) })
        }
        protocol::ProcessEvent::Reaped { agent, detail } => {
            host::Input::Process(host::process::Event::Reaped { owner: agent, detail })
        }
        protocol::ProcessEvent::Channel { agent, event } => live.receive(agent, *event),
    }
}

/// Preserve io's observed exit without inferring it from the signals sent.
#[must_use]
pub const fn to_host_exit(exit: Option<kernel::Exit>) -> Option<host::Exit> {
    match exit {
        Some(kernel::Exit::Code(code)) => Some(host::Exit::Code(code)),
        Some(kernel::Exit::Signal(signal)) => Some(host::Exit::Signal(signal)),
        None => None,
    }
}

pub(crate) fn channel_event(agent: Token, event: protocol::OpenEvent) -> host::Input {
    use protocol::OpenEvent;
    match event {
        OpenEvent::Opened { .. } => unreachable!("opened is emitted as Spawned"),
        OpenEvent::Hangup { .. } => host::Input::Process(host::process::Event::Hangup { owner: agent }),
        OpenEvent::Sent { .. } => host::Input::Process(host::process::Event::Sent { owner: agent }),
        OpenEvent::Unsent { .. } => host::Input::Process(host::process::Event::Unsent { owner: agent }),
        OpenEvent::Answer { answer, .. } => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Answer { answer } })
        }
        OpenEvent::MessageRefused { name, reason } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::MessageRefused { name, reason },
        }),
        OpenEvent::Admitted => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Admitted })
        }
        OpenEvent::Waiting { read } => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Waiting { read } })
        }
        OpenEvent::Long { operation, span } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::Long { operation, span },
        }),
        OpenEvent::LongDone => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::LongDone })
        }
        OpenEvent::Turn { turn } => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Turn { turn } })
        }
        OpenEvent::Live { .. } => host::Input::Process(host::process::Event::Progress { owner: agent }),
        OpenEvent::Fact { body } => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Fact { body } })
        }
        OpenEvent::Unauthorized { account, generation } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::Unauthorized { account, generation },
        }),
        OpenEvent::Limited { account, reset } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::Limited { account, reset },
        }),
        OpenEvent::Refused { account, code, reason } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::Refused { account, code, reason },
        }),
        OpenEvent::WriteFailed => unreachable!("write failure does not end a host read"),
        OpenEvent::Call { call, name, deadline, ask } => host::Input::Process(host::process::Event::Received {
            owner: agent,
            message: host::Up::Call { call, name, deadline, ask },
        }),
        OpenEvent::Withdraw { call } => {
            host::Input::Process(host::process::Event::Received { owner: agent, message: host::Up::Withdraw { call } })
        }
    }
}

fn git_limits(limits: &ProcessLimits) -> local_protocol::GitLimits {
    local_protocol::GitLimits {
        argument_bytes: 262_144,
        output_bytes: 65_536,
        conflicts: 64,
        path_bytes: 4096,
        detail_bytes: limits.detail_bytes,
    }
}

fn io_owner(event: &io::Event) -> Option<Token> {
    match event {
        io::Event::Listening { owner, .. }
        | io::Event::Accepted { owner, .. }
        | io::Event::Connecting { owner, .. }
        | io::Event::Connected { owner }
        | io::Event::Stream { owner, .. }
        | io::Event::Output { owner, .. }
        | io::Event::Spawned { owner, .. }
        | io::Event::Exited { owner, .. }
        | io::Event::Window { owner, .. }
        | io::Event::Usage { owner, .. }
        | io::Event::Failed { owner, .. }
        | io::Event::Closed { owner } => Some(*owner),
        io::Event::Resized | io::Event::Shutdown { .. } => None,
    }
}

fn plain_limits() -> local_protocol::PlainLimits {
    local_protocol::PlainLimits { entries: 256, path_bytes: 4096, file_bytes: 65_536 }
}

fn trace_up(trace: &mut Option<crate::stream::Spawned>, now: Time, event: io::Event) -> Option<io::Event> {
    if let Some(trace) = trace {
        match io_owner(&event) {
            Some(owner) if trace.owns(owner) => {
                trace.up(now, event);
                return None;
            }
            Some(_) | None => {}
        }
    }
    Some(event)
}

fn store_limits() -> local_protocol::StoreLimits {
    local_protocol::StoreLimits { file_bytes: 64 << 20, turns: 4096, deliveries: 4096, scan_bytes: 16 << 20 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_limits_are_refused() {
        let base = crate::tests::config().limits.process.io;
        assert!(native_io_valid(&base));
        let changes: &[fn(&mut io::Limits)] = &[
            |limits| limits.refusals = 0,
            |limits| limits.intake = 0,
            |limits| limits.receive = 0,
            |limits| limits.output = 0,
            |limits| limits.sends = 0,
            |limits| limits.accepts = 0,
            |limits| limits.retry = skein_lib::Duration::ZERO,
            |limits| limits.sockets = u32::MAX,
            |limits| limits.sockets = u32::MAX / 8 + 1,
        ];
        for change in changes {
            let mut invalid = base;
            change(&mut invalid);
            assert!(!native_io_valid(&invalid), "invalid native IO: {invalid:?}");
        }
        for sockets in [0, u32::MAX / 8] {
            assert!(native_io_valid(&io::Limits { sockets, ..base }));
        }
    }

    #[test]
    #[expect(
        clippy::disallowed_types,
        reason = "unit-test observations use ordinary Rust (programming-model.md, section 10.2)"
    )]
    fn a_store_answer_reaches_the_chat_before_a_later_file_terminal() {
        const STORE_OWNER: Token = Token::new(u64::MAX - 1);
        const MARKER_OWNER: Token = Token::new(7);
        // Consume the store terminal, dispatch Fault + Domain, then the marker.
        const DISPATCH_PASSES: u32 = 3;
        let config = crate::tests::config();
        let mut adapter =
            ProcessAdapter::new(&config.limits.process, config.launch, config.endpoints, 1).expect("adapter");
        adapter.adopt_store(kernel::Fd::new(10)).expect("store root");
        adapter.adopt_delivery_roots(Box::new([kernel::Fd::new(11)]), Box::new([]));
        let mut events = Queue::uncapped();
        adapter.store_request(local_protocol::StoreRequest::Load);
        adapter.markers_up(Time::ZERO, &mut events);
        adapter.markers_up(Time::ZERO, &mut events);
        adapter.start_markers(
            MARKER_OWNER,
            0,
            Box::new([Box::from(b"source.rs".as_slice())]),
            Time::from_nanos(1_000_000_000),
            &mut events,
        );
        // The file driver releases its serial slot when it queues a terminal.
        // Accumulate both completed requests before the next markers_up pass;
        // the chat handoff is now uncapped (protocol/limits.md, section 3.8).
        match adapter.file_requests.pop().expect("store load") {
            (file::Request::Load { owner: STORE_OWNER, path, .. }, _) => {
                assert_eq!(path.as_ref(), b"format");
            }
            other => panic!("store's first file request: {other:?}"),
        }
        adapter.file_events.push(file::Event::Failed {
            owner: STORE_OWNER,
            error: kernel::Error::Permission,
            committed: false,
            residue: None,
        });
        match adapter.file_requests.pop().expect("marker load") {
            (file::Request::Load { owner: MARKER_OWNER, .. }, _) => {}
            other => panic!("marker's file request: {other:?}"),
        }
        adapter.file_events.push(file::Event::Loaded { owner: MARKER_OWNER, bytes: b"clean\n".as_slice().into() });

        // Collect both terminals in dispatch order across the bounded passes.
        // The assertion is about the chat's stream, not queue room.
        let mut terminals = Vec::new();
        for _ in 0..DISPATCH_PASSES {
            adapter.markers_up(Time::ZERO, &mut events);
            while let Some(event) = events.pop() {
                terminals.push(event);
            }
        }
        let [
            local::Event::StoreFailed { turn: None, reason: local::StoreFailure::Read },
            local::Event::Git { owner: MARKER_OWNER, result: local::GitResult::Markers { first: None } },
        ] = terminals.as_slice()
        else {
            panic!("the store answer must reach the chat before the later marker terminal: {terminals:?}");
        };
        assert!(adapter.file_events.is_empty(), "both file terminals settle once");
        assert!(adapter.store_actions.is_empty(), "the store answer settles once");
        assert!(adapter.markers.is_none(), "the inspection settles once");
    }

    // One entry is the smallest routing queue: terminals are handed to the
    // shell one per pass. Two ready pipes exceed that estimate.
    const ROUTING_QUEUE: u32 = 1;
    const BURST: u32 = 2;
    const KERNEL_QUEUE: u32 = 1;

    fn adapter(queue: u32, sockets: u32) -> ProcessAdapter {
        let mut config = crate::tests::config();
        config.limits.process.queue = queue;
        config.limits.process.io.sockets = sockets;
        ProcessAdapter::new(&config.limits.process, config.launch, config.endpoints, 1).expect("adapter")
    }

    fn close_submission(number: u64) -> kernel::Submit {
        kernel::Submit { op: Token::new(number), kind: kernel::Op::Close { fd: kernel::Fd::new(90) } }
    }

    #[test]
    fn a_process_adapter_drains_more_io_terminals_than_its_queue_in_one_burst() {
        let mut adapter = adapter(ROUTING_QUEUE, BURST);
        let env = adapter.at(Time::ZERO, Wall::EPOCH);
        let mut expected = Queue::with_capacity(BURST);
        for number in 0..BURST {
            let owner = adapter
                .io
                .adopt_write_pipe(kernel::Fd::new(i32::try_from(number).expect("fixture descriptor")), None)
                .expect("one socket per pipe");
            io::down(
                &mut adapter.io,
                &env,
                io::Request::Stream {
                    stream: owner,
                    down: stream::Down::Demand { read: stream::Read::Nothing, room: 1 },
                },
                &mut adapter.io_submissions,
            );
            expected.push(io::Event::Stream { owner, up: stream::Up::Room });
        }
        adapter.io.reclaim();
        adapter.io_up(&env);
        assert_eq!(adapter.io_events.len(), BURST, "one metered IO pass exceeds the internal queue estimate");
        let config = crate::tests::config();
        let mut terminal = local_protocol::Terminal::new(local_protocol::TerminalLimits {
            line_bytes: config.limits.local.line_bytes,
            show_bytes: config.limits.local.show_bytes,
        })
        .expect("fixture terminal");
        let mut host_events = Queue::with_capacity(ROUTING_QUEUE);
        let mut terminal_events = Queue::with_capacity(ROUTING_QUEUE);
        let mut local_events = Queue::with_capacity(ROUTING_QUEUE);
        for remaining in [1, 0] {
            adapter.up(&env, &mut host_events, &mut terminal, &mut terminal_events, &mut local_events, &mut None);
            assert_eq!(adapter.io_events.len(), remaining, "each drain keeps its queue work bound");
            for _ in 0..ROUTING_QUEUE {
                let Some(event) = adapter.shell_events.pop() else { break };
                assert_eq!(Some(event), expected.pop(), "every IO terminal reaches the shell in order");
            }
        }
        assert!(expected.is_empty(), "no terminal lost within the bounded iterations");
        assert!(adapter.io_events.is_empty());
        assert!(adapter.shell_events.is_empty());
    }

    fn requests_down(adapter: &mut ProcessAdapter) {
        let env = adapter.at(Time::ZERO, Wall::EPOCH);
        adapter.down(
            &env,
            &mut Queue::uncapped(),
            &mut None,
            &Map::with_capacity(0),
            &mut Queue::uncapped(),
            &mut None,
        );
    }

    fn internal_io_burst() -> ProcessAdapter {
        use skein_lib::Duration;
        use smith_host_protocol::{ProgramSource, SourceAsk};
        use smith_protocol_program::Programs;

        // Two routing entries, four occupied pipes, one refusal: the check,
        // stderr read, window and channel opening exceed the queue together.
        const QUEUE: u32 = 2;
        const PIPES: u32 = 4;
        let mut adapter = adapter(QUEUE, PIPES);
        let env = adapter.at(Time::ZERO, Wall::EPOCH);
        let input = adapter.io.adopt_write_pipe(kernel::Fd::new(10), None).unwrap();
        let output = adapter.io.adopt_read_pipe(kernel::Fd::new(11)).unwrap();
        let error = adapter.io.adopt_read_pipe(kernel::Fd::new(12)).unwrap();
        let terminal = adapter.io.adopt_read_pipe(kernel::Fd::new(13)).unwrap();
        // Adopted read pipes prefetch independently of their producer demands.
        adapter.io.reclaim();
        adapter.io_up(&env);
        while adapter.io_submissions.pop().is_some() {}
        adapter.terminal_input = Some(terminal);
        adapter.read_window();
        // Filling IO's one refusal slot holds its actual credit independently
        // of the service handoff. No kernel operation is required to release it.
        io::down(
            &mut adapter.io,
            &env,
            io::Request::Connect {
                owner: Token::new(90),
                addr: io::Addr::Ip(core::net::SocketAddr::from(([127, 0, 0, 1], 1_u16))),
            },
            &mut adapter.io_submissions,
        );
        let _check = ProgramSource::new(
            Token::new(91),
            SourceAsk::Check,
            &Programs { directory: b"/programs".as_slice().into(), claude: b"/claude".as_slice().into() },
            host::SourceName::new(b"work").unwrap(),
            &[],
            kernel::Fd::new(3),
            b".".as_slice().into(),
            Time::ZERO.saturating_add(Duration::from_secs(60)),
            adapter.limits.detail_bytes,
            &mut adapter.io_requests,
        )
        .unwrap();
        let mut process = protocol::Process::new(
            Token::new(7),
            &adapter.limits.channel,
            adapter.limits.detail_bytes,
            adapter.endpoints.clone(),
        )
        .unwrap();
        process.spawn(
            adapter.launch.for_spawn(),
            Time::ZERO.saturating_add(Duration::from_secs(60)),
            &mut Queue::uncapped(),
        );
        process.from_io(
            Time::ZERO,
            io::Event::Spawned {
                owner: process.io_owner(),
                child: Token::new(8),
                pipes: Box::new([input, output, error]),
            },
            &mut adapter.process_events,
            &mut adapter.io_requests,
        );
        adapter.process = Some(process);
        adapter
    }

    #[test]
    fn a_burst_of_internal_io_requests_past_the_queue_goes_down_in_order() {
        // The check spawn, stderr demand, window, and two channel requests.
        const BURST_REQUESTS: u32 = 5;
        // Two refusals each emit Failed/Closed; the channel gets one grant.
        const IO_TERMINALS: u32 = 5;
        let mut adapter = internal_io_burst();
        let env = adapter.at(Time::ZERO, Wall::EPOCH);
        requests_down(&mut adapter);
        assert!(!adapter.io.takes(), "only IO refusal credit holds the burst");
        let mut burst = List::with_capacity(BURST_REQUESTS);
        for request in &adapter.io_requests {
            burst.push(request).unwrap();
        }
        let [
            io::Request::Spawn { owner, .. },
            io::Request::Stream { down: stream::Down::Demand { read: stream::Read::Arrived { .. }, room: 0 }, .. },
            io::Request::Window { .. },
            io::Request::Stream { down: stream::Down::Demand { read: stream::Read::Fill(1), room: 0 }, .. },
            io::Request::Output { down: stream::OutputDown::Room { .. }, .. },
        ] = burst.as_slice()
        else {
            panic!("all three producers enqueue in FIFO order: {burst:?}");
        };
        assert_eq!(*owner, Token::new(91), "the check starts the burst");
        assert!(adapter.io_submissions.is_empty(), "no request crosses exhausted IO credit");

        // The full entity table also refuses the check. One pass takes that
        // spawn, the next stderr and window, and the last both channel requests.
        for remaining in [4, 2, 0] {
            adapter.io_up(&env);
            requests_down(&mut adapter);
            assert_eq!(adapter.io_requests.len(), remaining, "IO credit and the work bound alone pace FIFO drains");
        }
        adapter.io_up(&env);
        let Some(kernel::Submit { kind: kernel::Op::Window { fd }, .. }) = adapter.submissions.pop() else {
            panic!("the window request reaches IO after the check refusal");
        };
        assert_eq!(fd, kernel::Fd::new(13));
        assert!(adapter.submissions.is_empty(), "only the window needs a new kernel operation");
        let mut events = List::with_capacity(IO_TERMINALS);
        for event in &adapter.io_events {
            events.push(event).unwrap();
        }
        let [
            io::Event::Failed { owner: blocker, .. },
            io::Event::Closed { owner: blocker_closed },
            io::Event::Failed { owner: check, .. },
            io::Event::Closed { owner: check_closed },
            io::Event::Output { up: stream::OutputUp::Settled { outcome: stream::OutputOutcome::Granted, .. }, .. },
        ] = events.as_slice()
        else {
            panic!("IO answers both refusals before granting the channel output: {events:?}");
        };
        assert_eq!(
            [*blocker, *blocker_closed, *check, *check_closed],
            [Token::new(90), Token::new(90), Token::new(91), Token::new(91)]
        );
    }

    #[test]
    fn the_kernel_edge_stays_capped_at_its_queue() {
        let mut adapter = adapter(KERNEL_QUEUE, KERNEL_QUEUE);
        adapter.submissions.push(close_submission(10));
        adapter.io_submissions.push(close_submission(11));
        adapter.io_submissions.push(close_submission(12));
        adapter.file_submissions.push(close_submission(13));
        adapter.flush_submissions();
        assert_eq!(adapter.submissions.capacity(), KERNEL_QUEUE);
        assert_eq!(adapter.completions.capacity(), KERNEL_QUEUE);
        assert_eq!(adapter.submissions.len(), KERNEL_QUEUE);
        assert_eq!(adapter.io_submissions.len(), 2, "full edge holds IO");
        assert_eq!(adapter.file_submissions.len(), 1, "full edge holds files");
        assert!(adapter.operations.is_empty(), "no route crosses the full kernel edge");
        adapter.submissions.pop().expect("shell drains the occupied edge");
        for original in [11, 12, 13] {
            adapter.flush_submissions();
            assert_eq!(adapter.submissions.len(), KERNEL_QUEUE, "only one submission crosses returned credit");
            let submit = adapter.submissions.pop().expect("shell takes the routed submission");
            adapter.flush_submissions();
            assert!(adapter.submissions.is_empty(), "the operation table also waits for completion credit");
            // Model the route retirement performed by reap, without sending
            // these synthetic operations into the real IO/file machines.
            match adapter.operations.remove(&submit.op).expect("one route per kernel operation") {
                Operation::Io(token) => {
                    assert!(original == 11 || original == 12, "IO retains priority over files");
                    assert_eq!(token, Token::new(original));
                }
                Operation::File(token) => {
                    assert_eq!(original, 13, "files follow both IO submissions");
                    assert_eq!(token, Token::new(original));
                }
                Operation::ProgramRoot => panic!("fixture has no program root"),
            }
        }
        assert!(adapter.io_submissions.is_empty());
        assert!(adapter.file_submissions.is_empty());
        assert!(adapter.operations.is_empty());
    }

    #[test]
    fn the_process_backlog_counts_only_internal_handoffs() {
        let mut adapter = adapter(KERNEL_QUEUE, KERNEL_QUEUE);
        adapter.submissions.push(close_submission(1));
        adapter.completions.push(kernel::Complete {
            op: Token::new(1),
            kind: kernel::Op::Close { fd: kernel::Fd::new(90) },
            result: Ok(kernel::Done::Nothing),
        });
        adapter.shell_events.push(io::Event::Resized);
        adapter.operations.insert(Token::new(1), Operation::Io(Token::new(1))).expect("one edge route");
        assert_eq!(adapter.backlog(), 0, "kernel and shell edges are outside the backlog");
        adapter.io_events.push(io::Event::Resized);
        adapter.io_submissions.push(close_submission(2));
        adapter.file_requests.push((file::Request::Close { owner: Token::new(1), file: Token::new(2) }, Time::ZERO));
        adapter.file_events.push(file::Event::Closed { owner: Token::new(1) });
        adapter.file_submissions.push(close_submission(3));
        adapter.store_requests.push(local_protocol::StoreRequest::Load);
        adapter.store_actions.push(local_protocol::Action::Closed);
        adapter.process_events.push(protocol::ProcessEvent::Exited { agent: Token::new(1), exit: None });
        adapter.io_requests.push(io::Request::Close { entity: Token::new(1) });
        // Each of the nine internal handoffs holds one entry.
        assert_eq!(adapter.backlog(), 9);
    }

    fn prefetched_adapter(owner: Token) -> ProcessAdapter {
        let config = crate::tests::config();
        let mut adapter =
            ProcessAdapter::new(&config.limits.process, config.launch, config.endpoints, 1).expect("adapter");
        adapter
            .process_events
            .push(protocol::ProcessEvent::Channel { agent: owner, event: Box::new(protocol::OpenEvent::WriteFailed) });
        adapter.process_events.push(protocol::ProcessEvent::Channel {
            agent: owner,
            event: Box::new(protocol::OpenEvent::Fact { body: b"first".as_slice().into() }),
        });
        adapter.process_events.push(protocol::ProcessEvent::Exited { agent: owner, exit: None });
        adapter.process_events.push(protocol::ProcessEvent::Channel {
            agent: owner,
            event: Box::new(protocol::OpenEvent::Fact { body: b"second".as_slice().into() }),
        });
        adapter
    }

    fn take_exit(events: &mut Queue<host::Input>) {
        match events.pop() {
            Some(host::Input::Process(host::process::Event::Exited { .. })) => {}
            other @ (Some(host::Input::Process(_) | host::Input::Parent(_)) | None) => {
                panic!("exit terminal: {other:?}")
            }
        }
    }

    #[test]
    fn a_write_failure_does_not_spend_the_hosts_pending_read() {
        let owner = Token::new(7);
        let mut adapter = prefetched_adapter(owner);
        let mut events = Queue::with_capacity(1);
        adapter.drain_process(&mut events);
        take_exit(&mut events);
        assert!(events.is_empty(), "no record without a held Read");
        assert!(!adapter.work_pending(Time::ZERO), "prefetched records wait without spinning");
        for text in [b"first".as_slice(), b"second".as_slice()] {
            // This fixture tests the outer terminal ledger with already queued events,
            // below request forwarding: a real Read now also requires an opened Process.
            adapter.reading = true;
            adapter.drain_process(&mut events);
            match events.pop().expect("one read terminal") {
                host::Input::Process(host::process::Event::Received {
                    owner: actual,
                    message: host::Up::Fact { body },
                }) => {
                    assert_eq!(actual, owner);
                    assert_eq!(&*body, text);
                }
                other @ (host::Input::Parent(_) | host::Input::Process(_)) => panic!("fact terminal: {other:?}"),
            }
            assert!(events.is_empty(), "one terminal per Read");
        }
        assert!(adapter.process_events.is_empty());
        adapter.reading = true;
        adapter
            .process_events
            .push(protocol::ProcessEvent::Channel { agent: owner, event: Box::new(protocol::OpenEvent::WriteFailed) });
        adapter.drain_process(&mut events);
        assert!(adapter.reading, "an independent write failure leaves the read owed");
        assert!(events.is_empty());
    }
}
