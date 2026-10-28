//! Shell driver of the host half's source operations (protocol/hosts.md, 2).
//! Owns the programs root through each terminal, reading no login file.
use runner::Runner;
use skein_lib::{Duration, Queue, Token};
use skein_shell::Clock;
use smith_host_domain::SourceName;
use smith_host_protocol::{ProgramSource, SourceAnswer, SourceAsk};
use smith_protocol_program::Programs;

mod runner;

pub(crate) fn run(
    ask: SourceAsk,
    program: &std::path::Path,
    source: &str,
    directory: &super::Directory,
    clock: &Clock,
    base: &[Box<[u8]>],
) -> Result<SourceAnswer, String> {
    let limits = limits();
    let programs = Programs {
        directory: directory.programs().as_os_str().as_encoded_bytes().into(),
        claude: program.as_os_str().as_encoded_bytes().into(),
    };
    let source_name = SourceName::new(source.as_bytes()).ok_or("invalid program source name")?;
    let configuration = smith_protocol_program::configuration_directory(&programs, source_name.as_bytes())
        .map_err(|_| "invalid program source directory")?;
    smith_protocol_program::environment(base, &configuration).map_err(|_| "invalid program source environment")?;
    let operations = skein_io::operations(&limits).and_then(|n| n.checked_add(1)).ok_or("program operation bound")?;
    let mut kernel =
        skein_shell::Kernel::open(skein_shell::Config { operations }).map_err(|_| "program kernel unavailable")?;
    let root = open_root(&directory.programs())?;
    let mut requests = Queue::with_capacity(QUEUE);
    let source = ProgramSource::new(
        SOURCE_OWNER,
        ask,
        &programs,
        source_name,
        base,
        root,
        b".".as_slice().into(),
        clock.now().now.saturating_add(smith_agent_service::profile::standard().policy.check_timeout),
        DETAIL_BYTES,
        &mut requests,
    )
    .map_err(|_| "invalid program source environment")?;
    let mut runner = Runner {
        io: skein_io::Io::new(&limits),
        limits,
        source,
        answer: None,
        root,
        closing: false,
        closed: false,
        events: Queue::with_capacity(QUEUE),
        requests,
        completions: Queue::with_capacity(QUEUE),
        submissions: Queue::with_capacity(SUBMISSIONS),
        operations,
    };
    let _exit = skein_shell::drive(&mut kernel, clock, &mut runner);
    runner.answer.ok_or_else(|| "program source returned no answer".into())
}

pub(super) const ROOT_CLOSE: Token = Token::new(u64::MAX);

// One source command needs child pipes and bounded I/O deliveries.
const fn limits() -> skein_io::Limits {
    skein_io::Limits {
        sockets: 3,
        names: 0,
        refusals: 4,
        intake: 4096,
        receive: 4096,
        output: 1,
        sends: 1,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(1),
        retry: Duration::from_millis(10),
    }
}

// Keep the source owner distinct from the root-close owner.
const SOURCE_OWNER: Token = Token::new(100);

// Each turn can queue both child-pipe demands and their terminals.
const QUEUE: u32 = 32;

// Submissions allow the I/O queue plus its cancellation burst.
const SUBMISSIONS: u32 = 64;

// SourceAnswer retains only this many bytes at the stderr tail.
pub(crate) const DETAIL_BYTES: u32 = 1024;

// Escape runner output before retaining it in a report or sending it to a terminal.
pub(super) fn detail(bytes: &[u8]) -> String {
    bytes.iter().flat_map(|byte| byte.escape_ascii()).map(char::from).collect()
}

pub(crate) fn open_root(path: &std::path::Path) -> Result<skein_io::kernel::Fd, String> {
    skein_shell::open_root(path).map_err(|_| "programs directory is unreadable".into())
}
