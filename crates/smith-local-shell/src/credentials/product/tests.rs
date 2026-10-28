//! The callback's product limits admit a browser navigation (shell.md, 6.2).
use skein_http::server::{self, Call, Error, Event, Limits, Request, Server};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Duration, Env, Intake, Queue, Time, Wall};

fn browser_head() -> Vec<u8> {
    let target = format!("/auth/callback?code={}&state=synthetic-state", "c".repeat(900));
    let fields = [
        "Host: 127.0.0.1:1455",
        "Connection: keep-alive",
        "Cache-Control: max-age=0",
        "sec-ch-ua: \"Chromium\";v=\"140\", \"Not=A?Brand\";v=\"24\", \"Google Chrome\";v=\"140\"",
        "sec-ch-ua-mobile: ?0",
        "sec-ch-ua-platform: \"Linux\"",
        "Upgrade-Insecure-Requests: 1",
        "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
        "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Sec-Fetch-Site: cross-site",
        "Sec-Fetch-Mode: navigate",
        "Sec-Fetch-User: ?1",
        "Sec-Fetch-Dest: document",
        "Accept-Encoding: gzip, deflate, br, zstd",
        "Accept-Language: en-GB,en-US;q=0.9,en;q=0.8",
        "Priority: u=0, i",
        "Referer: https://auth.example.test/",
        "DNT: 1",
        "Sec-GPC: 1",
        "sec-ch-ua-full-version-list: \"Chromium\";v=\"140.0.7339.0\", \"Google Chrome\";v=\"140.0.7339.0\"",
    ];
    format!("GET {target} HTTP/1.1\r\n{}\r\n\r\n", fields.join("\r\n")).into_bytes()
}

fn read_head(head: &[u8], limits: Limits) -> Result<Call, Error> {
    let mut server = Server::new(&limits);
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::uncapped();
    let mut below = Queue::uncapped();
    let mut intake = Intake::with_capacity(limits.head.max(u32::try_from(head.len()).expect("fixture length")));
    intake.append(head).expect("fixture fits intake");
    server::down(&mut server, &env, Request::Next, &mut above, &mut below);
    assert_eq!(below.pop(), Some(Down::Demand { read: Read::Nothing, room: limits.response }), "response reserve");
    server::up(&mut server, &env, Up::Room, &mut above, &mut below);
    let result = loop {
        let Some(Down::Demand { read, room: 0 }) = below.pop() else { panic!("head read demanded") };
        let bytes = intake.meet(read).expect("fixture meets the head read");
        server::up(&mut server, &env, Up::Bytes(bytes), &mut above, &mut below);
        if let Some(event) = above.pop() {
            break match event {
                Event::Call(call) => Ok(call),
                Event::Failed(error) => Err(error),
                other @ (Event::Ended
                | Event::Body(_)
                | Event::Reply(_)
                | Event::Refused(_)
                | Event::Done(_)
                | Event::Closed) => panic!("head terminal: {other:?}"),
            };
        }
    };
    while below.pop().is_some() {}
    server::down(&mut server, &env, Request::Close, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Closed), "server closes after the head");
    assert!(above.is_empty(), "one head terminal and one close terminal");
    result
}

#[test]
fn the_login_callback_accepts_a_browser_navigation() {
    let (_, configuration) = smith_local_process_world::process::configuration(skein_io::kernel::Fd::new(70), false);
    let limits = super::limits(configuration.limits.io, 1, Duration::from_secs(60)).server;
    let head = browser_head();
    assert!((1800..2600).contains(&head.len()), "browser-shaped head is about 2 KiB: {}", head.len());
    let call = read_head(&head, limits).expect("browser callback admitted by product bounds");
    assert_eq!(call.headers.len(), 20, "browser metadata fields survive parsing");
    assert_eq!(call.header(b"Host"), Some(b"127.0.0.1:1455".as_slice()), "callback host");
    assert!(call.target.starts_with(b"/auth/callback?code="), "authorization callback target");
    assert_eq!(
        read_head(&head, Limits { head: 1024, ..limits }).err(),
        Some(Error::Rejected(server::Rejection::HeadTooLong)),
        "a 1 KiB head refuses this browser"
    );
    assert_eq!(
        read_head(&head, Limits { headers: 8, ..limits }).err(),
        Some(Error::Rejected(server::Rejection::TooManyHeaders)),
        "eight fields refuse this browser"
    );
}
