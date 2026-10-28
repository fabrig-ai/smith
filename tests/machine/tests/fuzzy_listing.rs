//! Selected-file streams against an independent whole-tree set and checked heap bound.

use skein_lib::Rng;
use skein_world::domain::heap::{Counting, Meter};
use smith_domain::tools;
use smith_protocol_machine::Listing;
use std::collections::{BTreeMap, BTreeSet};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn seeded_selected_paths_keep_the_exact_tree_prefix_and_omitted_count_within_memory() {
    // Peak of the 128 seeded selected-prefix and omitted-count scenarios.
    // Measured 1_201 B on 2026-10-28; a quarter of headroom, rounded up.
    const SELECTED_PATHS_CEILING: u64 = 1_600;
    const BRANCHES: u64 = 4;
    const CHILDREN: u64 = 3;
    for seed in 0..128 {
        let mut rng = Rng::new(seed);
        let depth = match rng.below(4) {
            0 => tools::Depth::Whole,
            value => tools::Depth::Levels(u32::try_from(value).expect("small depth")),
        };
        let maximum = u32::try_from(rng.below(17)).expect("small count");
        let bytes = rng.below(2049);
        let mut selected = BTreeSet::new();
        selected.insert(b".Git/config".to_vec());
        for index in 0..24 {
            let branch = rng.below(BRANCHES);
            let child = rng.below(CHILDREN);
            selected.insert(format!("a{branch}/deep{child}/file{index:02}").into_bytes());
            selected.insert(format!("a{branch}/deep{child}.rs").into_bytes());
            selected.insert(format!("a{branch}-b/file{index:02}").into_bytes());
            selected.insert(format!("a{branch}.rs").into_bytes());
        }
        let mut selected: Vec<_> = selected.into_iter().collect();
        selected.sort_by_key(|path| path.split(|byte| *byte == b'/').map(Vec::from).collect::<Vec<_>>());
        let mut all = BTreeMap::new();
        let levels = match depth {
            tools::Depth::Whole => u32::MAX,
            tools::Depth::Levels(levels) => levels,
        };
        let mut output = Vec::new();
        for file in &selected {
            output.extend_from_slice(file);
            output.push(0);
            if file.starts_with(b".Git/") {
                continue;
            }
            let components: Vec<_> = file.split(|byte| *byte == b'/').collect();
            let mut name = Vec::new();
            for (index, component) in components.iter().enumerate() {
                if u32::try_from(index + 1).expect("finite depth") > levels {
                    break;
                }
                if index > 0 {
                    name.push(b'/');
                }
                name.extend_from_slice(component);
                let kind = if index + 1 == components.len() { tools::Kind::File } else { tools::Kind::Directory };
                all.insert(name.clone(), kind);
            }
        }
        let mut expected = Vec::new();
        let mut used = 0_u64;
        for (name, kind) in &all {
            let next = used + u64::try_from(core::mem::size_of::<tools::Entry>() + name.len()).expect("finite entry");
            if expected.len() == usize::try_from(maximum).expect("finite count") || next > bytes {
                break;
            }
            expected.push(tools::Entry { name: name.clone().into(), kind: *kind });
            used = next;
        }
        let more = u64::try_from(all.len() - expected.len()).expect("finite remainder");
        let meter = Meter::new();
        meter.start();
        let mut listing = Listing::new(maximum, bytes, depth, 256);
        for byte in &output {
            listing.push(*byte);
        }
        let actual = listing.finish(Some(skein_io::kernel::Exit::Code(0)), false);
        let peak = meter.check(meter.end(), SELECTED_PATHS_CEILING, &seed);
        assert!(peak > 0);
        assert_eq!(actual, tools::Done::Listed { entries: expected.into(), more }, "seed {seed}");
    }
}
