use std::path::PathBuf;

use skein_lib::{Duration, Wall};
use skein_shell::Host;
use smith_local_domain as local;
use smith_local_process_world::credential_fixture;
use smith_local_service as service;
use smith_local_shell::local_host::{Local, Resources, token_limits};

fn configuration(root: skein_io::kernel::Fd) -> (service::Config, smith_agent_service::Config) {
    smith_local_process_world::process::configuration(root, false)
}

struct Invocation {
    sim: skein_sim::Sim,
    machine: skein_fake_machine::Machine,
    state: skein_fake_machine::Opened,
    pid: skein_sim::Pid,
    peer_pid: skein_sim::Pid,
    peer: skein_fake_peers::llm::Peer,
    local: Local,
    input: skein_io::kernel::Fd,
    output: skein_io::kernel::Fd,
    seen: Vec<u8>,
    closed: bool,
    directory: PathBuf,
}

impl Invocation {
    fn new(name: &str, seed: u64) -> Self {
        let directory = std::env::temp_dir().join(format!("smith-library-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("test directory");
        let mut config = skein_sim::Config::calm();
        config.wall = skein_tls_world::pki::VALID;
        let mut sim = skein_sim::Sim::new(seed, config);
        let pid = sim.spawn_process();
        let peer_pid = sim.spawn_process();
        let mut machine = skein_fake_machine::Machine::new();
        let root = machine.lay(&[]);
        let root = sim.root(pid, skein_sim::Handle::new(root.raw()));
        let input = sim.open_inherited_read(pid);
        let output = sim.open_inherited_write(pid);
        let signals = sim.open_signal_source(pid);
        let token_directory = directory.join("tokens");
        let tokens = credential_fixture::Tokens::new(&token_directory, token_limits()).expect("private tokens");
        let token = skein_oauth::encode_record(
            &skein_oauth::SavedToken {
                issued_client_id: None,
                granted_scopes: None,
                subject: None,
                id_token: None,
                earliest_refresh_at: None,
                key: 0,
                generation: 1,
                access_token: b"token".as_slice().into(),
                refresh_token: Some(b"refresh".as_slice().into()),
                metadata: None,
                expires_at: Wall::from_nanos(
                    config.wall.as_nanos().saturating_add(Duration::from_secs(7200).as_nanos()),
                ),
            },
            &token_limits(),
        )
        .expect("saved token");
        tokens.save(0, &token).expect("token durable");
        let (config, lower) = configuration(root);
        let state = machine.lay(&[]);
        let prepared = smith_local_process_world::process::prepared_accounts(
            &token_directory,
            false,
            false,
            None,
            config.limits.process.io,
        );
        let local = Local::within_memory(
            config,
            Some(lower),
            Resources {
                state_root: { sim.root(pid, skein_sim::Handle::new(state.raw())) },
                input,
                output,
                signals,
                delivery_roots: Box::new([]),
                effect_roots: Box::new([]),
                delivery_environment: Box::new([]),
                seed,
                oauth_entropy: [17; 32],
            },
            Box::new(std::io::sink()),
            Some(prepared),
            None,
        )
        .expect("shared local shell");
        Self {
            sim,
            machine,
            state,
            pid,
            peer_pid,
            peer: smith_agent_process_world::fake::peer(),
            local,
            input,
            output,
            seen: Vec::new(),
            closed: false,
            directory,
        }
    }

    fn step(&mut self) {
        self.sim.reap(self.peer_pid, self.peer.completions());
        self.peer.iterate(self.sim.now(), self.sim.wall());
        self.sim.submit(self.peer_pid, self.peer.submissions());
        self.sim.reap(self.pid, self.local.completions());
        self.local.iterate(self.sim.now(), self.sim.wall());
        self.local.drain();
        self.sim.submit(self.pid, self.local.submissions());
        skein_fake_machine::serve(&mut self.machine, &mut self.sim);
        self.seen.extend(self.sim.peer_drain(self.pid, self.output, 4096));
        if !self.local.work_pending(self.sim.now())
            && !self.peer.work_pending(self.sim.now())
            && self.sim.ready(self.pid) == 0
            && self.sim.ready(self.peer_pid) == 0
            && !self.sim.deferred(self.pid)
            && !self.sim.deferred(self.peer_pid)
            && let Some(at) =
                [self.sim.next_due(), self.local.next_deadline(), self.peer.next_deadline()].into_iter().flatten().min()
        {
            self.sim.advance_to(at);
        }
    }

    fn turn_saved(&mut self) -> bool {
        if let Ok(file) = self.machine.open(self.state, b"0000000001.turn", skein_fake_machine::How::Read) {
            self.machine.close(file);
            true
        } else {
            false
        }
    }

    fn load_chat(&mut self) -> local::Event {
        let root =
            self.machine.open(self.state, b".", skein_fake_machine::How::Directory).expect("bounded chat fixture");
        let mut turns = Vec::new();
        for (_, name) in self.machine.list(root, 4096, 65536).expect("bounded chat fixture") {
            if name.ends_with(b".turn") {
                let file = self.machine.open(root, &name, skein_fake_machine::How::Read).expect("bounded chat fixture");
                turns.push(self.machine.read(file, 0, 64 << 20).expect("bounded chat fixture").into_boxed_slice());
                self.machine.close(file);
            }
        }
        self.machine.close(root);
        let endpoints = configuration(skein_io::kernel::Fd::new(0)).0.endpoints;
        local::Event::Loaded {
            state: None,
            transcript: smith_local_protocol::decode_turns(&turns, &smith_transcript::CEILINGS, &endpoints)
                .expect("bounded chat fixture")
                .0,
            deliveries: Box::new([]),
        }
    }

    fn finish(&mut self) {
        if !self.closed {
            self.sim.peer_close(self.pid, self.input);
            self.closed = true;
        }
        for _ in 0..2000 {
            self.step();
            if self.local.is_empty() {
                break;
            }
        }
        assert!(
            matches!(self.local.result(), Some(Ok(local::ExitStatus::Accepted))),
            "local invocation finishes successfully"
        );
        self.sim.assert_quiescent(self.pid);
        self.sim.assert_no_open_fds(self.pid);
    }
}

impl Drop for Invocation {
    fn drop(&mut self) {
        drop(std::fs::remove_dir_all(&self.directory));
    }
}

#[test]
fn the_shared_library_saves_a_plaintext_turn_and_flushes_terminal_output_before_exiting() {
    let mut invocation = Invocation::new("turn", 17);
    assert_eq!(invocation.sim.peer_feed(invocation.pid, invocation.input, b"hello\n"), 6);
    for _ in 0..2000 {
        invocation.step();
        if invocation.turn_saved() {
            break;
        }
    }
    assert!(
        smith_agent_process_world::fake::replied(&invocation.peer),
        "the shared service reached the plaintext peer"
    );
    let local::Event::Loaded { transcript: Some(transcript), .. } = invocation.load_chat() else {
        panic!("the shell saved the typed turn");
    };
    assert_eq!(transcript.turns.len(), 1);
    invocation.finish();
    assert!(!invocation.seen.is_empty(), "output crossed the inherited terminal descriptor");
}

#[test]
fn an_empty_shared_invocation_closes_its_terminal_signals_and_launch_directory() {
    let mut invocation = Invocation::new("empty", 18);
    invocation.finish();
}
