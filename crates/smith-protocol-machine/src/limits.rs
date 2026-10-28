//! Bounded machine ownership (programming-model.md, section 6.3;
//! protocol/agent.md, section 2).

use skein_json::tokenizer;
use skein_lib::Duration;

/// File translation capacity, checked before effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum simultaneous file requests.
    pub operations: u32,
    /// Maximum admitted workspace roots.
    pub roots: u32,
    /// Maximum relative path bytes.
    pub path_bytes: u32,
    /// Maximum content bytes for load or store.
    pub file_bytes: u32,
    /// Maximum combined command, pattern or filter argument bytes.
    pub tool_payload: u32,
    /// Maximum entries retained from a tree listing.
    pub entries: u32,
    /// Maximum retained entry cells and names.
    pub entry_bytes: u64,
    /// Maximum simultaneous child processes.
    pub processes: u32,
    /// Upper bound for either command output window.
    pub output_bytes: u32,
    /// Upper bound for retained search hits.
    pub search_hits: u32,
    /// Upper bound for retained search path and text bytes.
    pub search_bytes: u32,
    /// Maximum one rg JSON or NUL-delimited path record accepted for parsing.
    pub search_line_bytes: u32,
    /// Maximum configured command environment bytes.
    pub env_bytes: u32,
    /// Grace between a polite signal and a kill.
    pub group_stop: Duration,
}

/// The most records one step can emit into each queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxOut {
    /// Domain terminals.
    pub to_domain: u32,
    /// IO requests.
    pub below: u32,
}

/// A terminal per process and room for its stop followed by close.
#[must_use]
pub const fn max_out(limits: &Limits) -> MaxOut {
    let stopping = limits.processes.checked_mul(2).expect("process emission count fits");
    let below = if stopping > 3 { stopping } else { 3 };
    MaxOut { to_domain: if limits.processes == 0 { 1 } else { limits.processes }, below }
}

/// Whether the machine's capacities and search edge fit (programming-model.md, 6.3).
#[must_use]
pub fn valid(limits: &Limits) -> bool {
    limits.processes.checked_mul(2).is_some()
        && tokenizer::worst_case(&tokenizer::Limits {
            depth: 8,
            string: limits.search_line_bytes.max(1),
            number: 20,
            chunk: 64,
            length: limits.search_line_bytes.max(1),
        })
        .is_some()
}

/// Numeric declarations needed by this layer's transitional derivation.
/// The service supplies only owned numbers and this domain's own child limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Derivation {
    pub operations: u32,
    pub processes: u32,
    pub roots: u32,
    pub path_bytes: u32,
    pub file_bytes: u32,
    pub tool_payload: u32,
    pub entries: u32,
    pub entry_bytes: u64,
    pub shell_head: u32,
    pub shell_tail: u32,
    pub search_hits: u32,
    pub search_bytes: u32,
    pub environment_bytes: u32,
    pub group_stop: Duration,
}

/// The first field this layer cannot derive, returned to its parent before effects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    /// A JSON search line, derived from retained bytes and escaping.
    SearchLineBytes,
}

impl Field {
    /// The field name in the effective limits, without its layer.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::SearchLineBytes => "search_line_bytes",
        }
    }
}

/// Derive this layer from declarations, refusing checked arithmetic overflow.
/// Contract: protocol/limits.md, sections 2 and 3.
pub fn derive(inputs: &Derivation) -> Result<Limits, Field> {
    Ok(Limits {
        operations: inputs.operations,
        roots: inputs.roots,
        path_bytes: inputs.path_bytes,
        file_bytes: inputs.file_bytes,
        tool_payload: inputs.tool_payload,
        entries: inputs.entries,
        entry_bytes: inputs.entry_bytes,
        processes: inputs.processes,
        output_bytes: inputs.shell_head.max(inputs.shell_tail).max(inputs.search_bytes),
        search_hits: inputs.search_hits,
        search_bytes: inputs.search_bytes,
        // JSON escapes one text/path byte into at most six bytes. The fixed
        // envelope includes match offsets, line number and routing keys.
        search_line_bytes: inputs
            .search_bytes
            .checked_add(inputs.path_bytes)
            .ok_or(Field::SearchLineBytes)?
            .checked_mul(6)
            .ok_or(Field::SearchLineBytes)?
            .checked_add(256)
            .ok_or(Field::SearchLineBytes)?,
        env_bytes: inputs.environment_bytes,
        group_stop: inputs.group_stop,
    })
}

#[cfg(test)]
mod memory {
    use super::Limits;
    use crate::{Below, BelowEvent, Component, Environment, FromDomain, ToDomain};
    use skein_io::{file, kernel};
    use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
    use skein_world::domain::heap::{Counting, Meter};
    use smith_domain::{run, tools};

    #[global_allocator]
    static HEAP: Counting = Counting;

    fn caps() -> Limits {
        Limits {
            operations: 0,
            roots: 1,
            path_bytes: 1,
            file_bytes: 65_536,
            tool_payload: 65_536,
            entries: 0,
            entry_bytes: 0,
            processes: 8,
            output_bytes: 0,
            search_hits: 0,
            search_bytes: 0,
            search_line_bytes: 1,
            env_bytes: 0,
            group_stop: Duration::from_secs(1),
        }
    }

    #[test]
    fn invalid_limits_are_refused() {
        let limits = caps();
        assert!(super::valid(&limits));
        assert!(!super::valid(&Limits { processes: u32::MAX, ..limits }));
        assert!(!super::valid(&Limits { search_line_bytes: u32::MAX, ..limits }));
    }

    fn workspace() -> run::Workspace {
        run::Workspace {
            directories: Box::new([run::Directory {
                host: b"/host".as_slice().into(),
                name: b"r".as_slice().into(),
                root: Token::new(1),
                writable: false,
                git: false,
                conflicts: Box::new([]),
            }]),
        }
    }

    #[test]
    fn every_pending_listing_keeps_its_largest_filter_within_the_bound() {
        // Peak with eight pending listings holding their largest filters.
        // Measured 598_592 B on 2026-10-28; a quarter of headroom, rounded up.
        const FILTERS_CEILING: u64 = 750_000;

        let limits = caps();
        let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
        let workspace = workspace();
        let mut terminals = Queue::<ToDomain>::with_capacity(1);
        let mut below = Queue::<Below>::with_capacity(1);
        let meter = Meter::new();
        meter.start();
        let mut component = Component::new(&limits, Environment::encode(&[], &limits).expect("empty environment"));
        component.workspace(&workspace);
        meter.check(meter.end(), FILTERS_CEILING, &"component and root");
        for index in 0..limits.processes {
            meter.start();
            component.from_domain(&env, listing(index), &mut terminals, &mut below);
            let measured = meter.end();
            assert!(terminals.is_empty());
            assert_eq!(below.pop(), Some(directory(index)));
            meter.check(measured, FILTERS_CEILING, &index);
        }
        assert_eq!(component.next_deadline(), Some(Time::from_nanos(100)));
        for index in 0..limits.processes {
            finish_listing(&mut component, &env, index, &mut terminals, &mut below);
        }
        assert_eq!(component.next_deadline(), None);
        component.close(&mut below);
        assert!(component.closed());
    }

    fn directory(index: u32) -> Below {
        Below::OpenDirectory {
            owner: Token::new(u64::from(index) + 10),
            root: Token::new(1),
            path: b".".as_slice().into(),
            deadline: Time::from_nanos(100),
        }
    }

    fn finish_listing(
        component: &mut Component,
        env: &Env<Limits>,
        index: u32,
        terminals: &mut Queue<ToDomain>,
        below: &mut Queue<Below>,
    ) {
        let owner = Token::new(u64::from(index) + 10);
        component.from_below(
            env,
            BelowEvent::File(file::Event::Failed {
                owner,
                error: kernel::Error::NotFound,
                committed: false,
                residue: None,
            }),
            terminals,
            below,
        );
        assert_eq!(terminals.pop(), Some(ToDomain::Done { owner, done: tools::Done::Missing }));
    }

    fn listing(index: u32) -> FromDomain {
        let mut filter = skein_lib::bytes::zeroed(65_536);
        filter.fill(b'x');
        FromDomain::Op {
            owner: Token::new(u64::from(index) + 10),
            op: tools::Op::List {
                at: tools::Place { root: Token::new(1), path: Box::new([]) },
                depth: tools::Depth::Whole,
                glob: Some(filter),
                max: 0,
                max_bytes: 0,
            },
            deadline: Time::from_nanos(100),
        }
    }

    #[test]
    fn saturated_listings_keep_their_pathname_copies_within_the_bound() {
        // Ceiling on held listing paths: a quarter above 10-28's figure, moved by hand when a real change passes it.
        const LISTINGS_HELD_CEILING: u64 = 500_000;
        // Ceiling on listing path peak: a quarter above 10-28's figure, moved by hand when a real change passes it.
        const LISTINGS_PEAK_CEILING: u64 = 540_000;
        let meter = Meter::new();
        meter.start();
        let mut listings: [Option<crate::Listing>; 8] = [const { None }; 8];
        for listing in &mut listings {
            *listing = Some(crate::Listing::new(0, 0, tools::Depth::Whole, 16_384));
        }
        meter.check(meter.end(), LISTINGS_PEAK_CEILING, &"eight record buffers");
        let mut peak = 0;
        for listing in &mut listings {
            let listing = listing.as_mut().expect("eight saturated listings");
            for byte in [b'a', b'b'] {
                meter.start();
                for _ in 0_u32..16_384 {
                    listing.push(byte);
                }
                listing.push(0);
                peak = peak.max(meter.check(meter.end(), LISTINGS_PEAK_CEILING, &byte));
            }
        }
        assert!(
            meter.held() <= LISTINGS_HELD_CEILING,
            "held listing paths {} exceed {LISTINGS_HELD_CEILING}",
            meter.held()
        );
        assert!(peak <= LISTINGS_PEAK_CEILING, "listing path peak {peak} exceeds {LISTINGS_PEAK_CEILING}");
    }

    #[test]
    fn saturated_listings_allocate_terminal_cells_only_in_the_finishing_step() {
        // Ceiling on held listing entries: a quarter above 10-28's figure, moved by hand when a real change passes it.
        const LISTING_ENTRIES_HELD_CEILING: u64 = 140_000;
        // Ceiling on finishing listings' peak: a quarter above 10-28's figure, moved by hand when a real change passes it.
        const LISTING_TERMINALS_PEAK_CEILING: u64 = 150_000;
        let meter = Meter::new();
        meter.start();
        let mut listings: [Option<crate::Listing>; 8] = [const { None }; 8];
        for slot in &mut listings {
            let mut listing = crate::Listing::new(512, 13_312, tools::Depth::Whole, 2);
            for index in 0_u16..512 {
                listing.push(b'a' + u8::try_from(index / 26).expect("first letter"));
                listing.push(b'a' + u8::try_from(index % 26).expect("second letter"));
                listing.push(0);
            }
            *slot = Some(listing);
        }
        meter.check(meter.end(), LISTING_TERMINALS_PEAK_CEILING, &"eight full entry lists");
        assert!(
            meter.held() <= LISTING_ENTRIES_HELD_CEILING,
            "held listing entries {} exceed {LISTING_ENTRIES_HELD_CEILING}",
            meter.held()
        );
        let mut terminals: [Option<tools::Done>; 8] = [const { None }; 8];
        let mut peak = 0;
        for (listing, terminal) in listings.iter_mut().zip(&mut terminals) {
            meter.start();
            *terminal = Some(listing.take().expect("full listing").finish(Some(kernel::Exit::Code(0)), false));
            peak = peak.max(meter.check(meter.end(), LISTING_TERMINALS_PEAK_CEILING, &"one terminal list"));
        }
        assert!(
            peak <= LISTING_TERMINALS_PEAK_CEILING,
            "listing terminal peak {peak} exceeds {LISTING_TERMINALS_PEAK_CEILING}"
        );
    }
}
