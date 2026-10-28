//! The scripted browser follows actual HTTP redirects; Skein owns the issuer.
//! It keeps GET and callback state and received observations, never local
//! service state. Contract: testing.md, sections 4 and 5.
pub use skein_fake_peers::oauth::Peer;
use skein_io::{self as io, kernel};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_tls::client as tls;
use skein_tls_world::pki;
use skein_world::Host;

/// Outside token POST count, with no issuer state access.
#[must_use]
pub fn posts(peer: &Peer) -> u64 {
    u64::try_from(
        peer.observations()
            .iter()
            .filter(|observation| matches!(observation, skein_fake_peers::oauth::Observation::Post { .. }))
            .count(),
    )
    .expect("bounded observations")
}

/// A browser's HTTP or HTTPS GET, ending after the loopback callback reply.
pub struct Browser {
    io: io::Io,
    env: Env<io::Limits>,
    request: Option<Box<[u8]>>,
    socket: Option<Token>,
    body: bool,
    ended: bool,
    seen: bool,
    redirect: Option<Box<[u8]>>,
    tls: Option<tls::Client>,
    tls_up: Queue<tls::Event>,
    cipher_down: Queue<Down>,
    closing: bool,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
}

impl Browser {
    #[must_use]
    pub fn new(url: &[u8]) -> Self {
        let (address, request) = get(url);
        let limits = browser_limits();
        let mut requests = Queue::with_capacity(64);
        requests.push(io::Request::Connect { owner: Token::new(1), addr: io::Addr::Ip(kernel::Addr::from(address)) });
        Self {
            io: io::Io::new(&limits),
            env: Env { now: Time::ZERO, wall: pki::VALID, limits },
            request: Some(request),
            socket: None,
            body: false,
            ended: false,
            seen: false,
            redirect: None,
            tls: tls_for(url),
            tls_up: Queue::with_capacity(64),
            cipher_down: Queue::with_capacity(64),
            closing: false,
            completions: Queue::with_capacity(64),
            submissions: Queue::with_capacity(64),
            events: Queue::with_capacity(64),
            requests,
        }
    }
    #[must_use]
    pub fn saw_reply(&self) -> bool {
        self.seen
    }
    fn begin(&mut self) {
        let length = u32::try_from(self.request.as_ref().expect("one GET").len()).expect("bounded GET");
        self.stream_down(Down::Demand { read: Read::Nothing, room: length });
    }
    fn stream_down(&mut self, down: Down) {
        if let Some(client) = &mut self.tls {
            tls::down(
                client,
                &Env { now: self.env.now, wall: self.env.wall, limits: tls_limits() },
                tls::Request::Stream(down),
                &mut self.tls_up,
                &mut self.cipher_down,
            );
        } else {
            self.requests.push(io::Request::Stream { stream: self.socket.expect("browser socket"), down });
        }
    }
    fn close(&mut self) {
        if !self.closing {
            self.closing = true;
            self.requests.push(io::Request::Close { entity: self.socket.expect("connected browser") });
        }
    }
    fn plain_up(&mut self, up: Up) {
        match up {
            Up::Room => {
                let bytes = self.request.take().expect("one GET output");
                self.stream_down(Down::Send(bytes));
                self.stream_down(Down::Demand {
                    read: Read::Scan {
                        until: skein_lib::stream::Delimiter::new(b"\r\n\r\n").expect("headers"),
                        max: 4096,
                    },
                    room: 0,
                });
            }
            Up::Bytes(bytes) => {
                if self.body {
                    self.seen = bytes.as_ref() == b"Redirect received\n"
                        || bytes.as_ref() == b"Sign-in received. You may close this page.\n";
                    self.close();
                } else {
                    if bytes.starts_with(b"HTTP/1.1 302") {
                        let head = std::str::from_utf8(&bytes).expect("redirect head");
                        let location = head
                            .split("\r\n")
                            .find_map(|line| {
                                line.strip_prefix("Location: ").or_else(|| line.strip_prefix("location: "))
                            })
                            .expect("issuer Location");
                        self.redirect = Some(location.as_bytes().into());
                        self.close();
                    } else {
                        assert!(bytes.starts_with(b"HTTP/1.1 200"));
                        self.body = true;
                        let head = std::str::from_utf8(&bytes).expect("callback head");
                        let length = head
                            .split("\r\n")
                            .find_map(|line| {
                                line.strip_prefix("Content-Length: ").or_else(|| line.strip_prefix("content-length: "))
                            })
                            .expect("callback body length")
                            .parse::<u32>()
                            .expect("bounded callback page");
                        assert!(length == 18 || length == 43, "the legacy or native callback page");
                        self.stream_down(Down::Demand { read: Read::Fill(length), room: 0 });
                    }
                }
            }
            Up::End | Up::Failed(_) => self.close(),
        }
    }
    fn route(&mut self) {
        for _ in 0..128 {
            if let Some(event) = self.tls_up.pop() {
                match event {
                    tls::Event::Ready(_) => self.begin(),
                    tls::Event::Stream(up) if !self.closing => self.plain_up(up),
                    tls::Event::Failed(why) if !self.closing => panic!("scripted browser TLS failed: {why:?}"),
                    tls::Event::Stream(_) | tls::Event::Failed(_) | tls::Event::Closed => {}
                }
            } else if let Some(down) = self.cipher_down.pop() {
                if !self.closing {
                    self.requests.push(io::Request::Stream { stream: self.socket.expect("TLS socket"), down });
                }
            } else {
                break;
            }
        }
    }
}

impl Host for Browser {
    fn iterate(&mut self, now: Time, wall: Wall) {
        self.env.now = now;
        self.env.wall = wall;
        while self.io.is_ready() {
            io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
        }
        while let Some(complete) = self.completions.pop() {
            io::up(&mut self.io, &self.env, complete, &mut self.events, &mut self.submissions);
        }
        for _ in 0..8 {
            if self.io.is_ready() {
                io::resume(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
            if self.io.is_due(now) {
                io::fire(&mut self.io, &self.env, &mut self.events, &mut self.submissions);
            }
        }
        while let Some(event) = self.events.pop() {
            match event {
                io::Event::Connecting { socket, .. } => self.socket = Some(socket),
                io::Event::Connected { .. } => {
                    if let Some(client) = &mut self.tls {
                        tls::down(
                            client,
                            &Env { now, wall, limits: tls_limits() },
                            tls::Request::Handshake,
                            &mut self.tls_up,
                            &mut self.cipher_down,
                        );
                    } else {
                        self.begin();
                    }
                }
                io::Event::Stream { up, .. } if !self.closing => {
                    if let Some(client) = &mut self.tls {
                        tls::up(
                            client,
                            &Env { now, wall, limits: tls_limits() },
                            up,
                            &mut self.tls_up,
                            &mut self.cipher_down,
                        );
                    } else {
                        self.plain_up(up);
                    }
                }
                io::Event::Closed { .. } => {
                    self.socket = None;
                    self.tls = None;
                    self.closing = false;
                    while self.tls_up.pop().is_some() {}
                    while self.cipher_down.pop().is_some() {}
                    if let Some(url) = self.redirect.take() {
                        let (address, request) = get(&url);
                        self.tls = tls_for(&url);
                        self.request = Some(request);
                        self.requests.push(io::Request::Connect {
                            owner: Token::new(1),
                            addr: io::Addr::Ip(kernel::Addr::from(address)),
                        });
                    } else {
                        self.ended = true;
                    }
                }
                io::Event::Stream { up: Up::End | Up::Failed(_), .. } | io::Event::Failed { .. } => {
                    self.close();
                }
                _ => {}
            }
        }
        self.route();
        while self.io.takes() {
            let Some(request) = self.requests.pop() else { break };
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
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || self.io.is_due(now)
            || !self.requests.is_empty()
            || !self.events.is_empty()
            || !self.tls_up.is_empty()
            || !self.cipher_down.is_empty()
            || !self.completions.is_empty()
    }
    fn next_deadline(&self) -> Option<Time> {
        self.io.next_deadline()
    }
    fn next_policy_deadline(&self) -> Option<Time> {
        None
    }
    fn is_empty(&self) -> bool {
        self.ended
            && self.io.is_empty()
            && self.requests.is_empty()
            && self.events.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn operations(&self) -> u32 {
        io::operations(&self.env.limits).expect("browser operations")
    }
}

fn get(url: &[u8]) -> (std::net::SocketAddr, Box<[u8]>) {
    let text = std::str::from_utf8(url).expect("loopback URI");
    let text = text.strip_prefix("http://").or_else(|| text.strip_prefix("https://")).expect("loopback HTTP");
    let (authority, path) = text.split_once('/').expect("GET path");
    let address = authority.parse().expect("loopback address");
    let request = format!("GET /{path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n")
        .into_bytes()
        .into_boxed_slice();
    (address, request)
}

fn tls_limits() -> tls::Limits {
    tls::Limits { read: 4096, send: 8192, records: tls::MAX_RECORD }
}
fn tls_for(url: &[u8]) -> Option<tls::Client> {
    url.starts_with(b"https://").then(|| tls::Client::new(&pki::client(&[]), pki::name(), &tls_limits()))
}

fn browser_limits() -> io::Limits {
    io::Limits {
        // The redirect connects before the retired authorization socket is reclaimed.
        sockets: 2,
        names: 0,
        refusals: 1,
        intake: 32_768,
        receive: 4096,
        output: 65_536,
        sends: 2,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(1),
    }
}
