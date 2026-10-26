//! Kept turns, their pieces word and their patch set (transcript.md, 12).

use std::{collections::BTreeSet, fs};

use skein_codec::{
    patch::{Halt, Site},
    schema::{self, DeclIx, FieldIx, Held, Shape, Type},
};
use smith_transcript_patches::patches as fixture;

#[path = "support/patches.rs"]
mod patches;

const TRANSCRIPT: &str = include_str!("../../../crates/smith-transcript/schema/transcript-v6.schema");

#[test]
fn each_version_one_golden_patches_to_its_version_two_golden() {
    let output =
        skein_codegen::generate(&skein_codegen::parse(fixture::kept::fixture_turn::TEXTS[0]).expect("v1 schema"));
    let directory = skein_scratch::paths::workspace().join("tests/transcript-patches/patches/golden/0001/fixture-turn");
    let files: BTreeSet<_> = fs::read_dir(&directory)
        .expect("patch goldens")
        .map(|entry| entry.expect("golden entry").file_name())
        .collect();
    let mut checked = BTreeSet::new();
    let mut patcher = patches::fixture_patcher();
    for golden in output.goldens {
        let bytes =
            patches::patch_to_current(&mut patcher, golden.bytes.into_boxed_slice()).expect("v1 golden patches");
        assert_eq!(bytes.as_ref(), fs::read(directory.join(&golden.name)).expect("v2 golden"), "{}", golden.name);
        checked.insert(std::ffi::OsString::from(golden.name));
    }
    assert_eq!(checked, files, "every patch golden checked exactly once");
}

#[test]
fn a_version_two_turn_runs_no_plan() {
    assert_eq!(patches::fixture_patcher().plan_from(2), None);
}

#[test]
fn the_fixture_patch_is_marked_fresh_windows_and_an_unmarked_one_is_not() {
    const UNMARKED: &str =
        "patch 1\nowner fixture-smith 1\n\nfamily fixture-turn 1 to 2\nadd Turn.tokens u32 0\ndrop Turn.replay\n";
    for (text, marked) in [(fixture::PATCHES[0], true), (UNMARKED, false)] {
        let header = skein_codec::patch::header(text.as_bytes()).expect("patch header");
        assert_eq!(header.marked(text.as_bytes(), smith_transcript::FRESH_WINDOWS.as_bytes()), marked);
    }
    let unmarked_lines: Vec<_> = UNMARKED.lines().collect();
    let fixture_lines: Vec<_> = fixture::PATCHES[0].lines().filter(|line| !line.starts_with("mark ")).collect();
    assert_eq!(unmarked_lines, fixture_lines, "same patch without its mark");
}

#[test]
fn found_hits_holds_pieces_of_hit_read_from_the_schema() {
    let schema = schema::parse(TRANSCRIPT.as_bytes()).expect("runtime transcript schema");
    let found = schema.find(b"Found").expect("Found declaration");
    let hit = schema.find(b"Hit").expect("Hit declaration");
    let mut held_fields = Vec::new();
    for index in 0..schema.declarations() {
        let declaration = DeclIx::new(index);
        if let Shape::Record { first, count, .. } = schema.declaration(declaration).shape {
            for offset in 0..count {
                let field = schema.field(FieldIx::new(first.index() + offset));
                let mut ty = field.ty;
                loop {
                    match schema.ty(ty) {
                        Type::Bytes { held: Held::Pieces(item), .. } => {
                            held_fields.push((declaration, schema.spelled(field.name), item));
                            break;
                        }
                        Type::Bytes { held: Held::Record { .. }, .. } => panic!("unexpected held record"),
                        Type::List { item, .. } | Type::Option(item) => ty = item,
                        Type::U8
                        | Type::U16
                        | Type::U32
                        | Type::U64
                        | Type::Bool
                        | Type::Duration
                        | Type::Fixed(_)
                        | Type::Secret { .. }
                        | Type::Text(_)
                        | Type::Bytes { .. }
                        | Type::Named(_) => break,
                    }
                }
            }
        }
    }
    assert_eq!(held_fields, [(found, b"hits".as_slice(), hit)]);
}

#[test]
fn the_real_set_names_its_owner_family_root_and_mark() {
    assert_eq!(smith_transcript::OWNER, "smith");
    assert_eq!(smith_transcript::FAMILY, "smith-transcript");
    assert_eq!(smith_transcript::ROOT, "Turn");
    assert_eq!(smith_transcript::FRESH_WINDOWS, "fresh-windows");
    let schema = schema::parse(TRANSCRIPT.as_bytes()).expect("runtime transcript schema");
    assert_eq!(schema.spelled(schema.family()), smith_transcript::FAMILY.as_bytes());
    let root = schema.find(smith_transcript::ROOT.as_bytes()).expect("Turn declaration");
    assert!(matches!(schema.declaration(root).shape, Shape::Record { versioned: true, .. }));
}

#[test]
fn the_real_set_is_empty_before_the_baseline() {
    assert!(smith_transcript::patches::PATCHES.is_empty());
    assert!(smith_transcript::patches::kept::smith_transcript::TEXTS.is_empty());
}

#[test]
fn the_real_written_answers_every_site_no_site() {
    let mut patcher = patches::fixture_patcher();
    let plan = patcher.plan_from(1).expect("v1 plan");
    let root = patcher.schema(1).expect("v1 schema").find(smith_transcript::ROOT.as_bytes()).expect("Turn");
    let output =
        skein_codegen::generate(&skein_codegen::parse(fixture::kept::fixture_turn::TEXTS[0]).expect("v1 schema"));
    patcher.start(plan, root, output.goldens[0].bytes.clone().into_boxed_slice()).expect("start golden");
    let (old, new) = patcher.parts();
    let mut context = smith_transcript::patches::Context::default();
    for site in [Site::new(0, 0), Site::new(1, 0), Site::new(u32::MAX, u32::MAX)] {
        assert_eq!(
            smith_transcript::patches::written(site, old, new, old.root(), &mut context),
            Err(Halt::NoSite(site))
        );
    }
}
