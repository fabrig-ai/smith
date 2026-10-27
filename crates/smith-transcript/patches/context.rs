//! The owner's per-record inputs to written patch functions (codec.md, 11.5): the command wrote it once; it is the owner's.

#[derive(Debug)]
pub struct Context {}

// Default by contract (codec.md, 11.5), and empty: no live state. Written
// out, since production clippy.toml refuses the derive and clippy reports a
// derive's disallowed macro at the crate alone, where an expectation would
// cover every item.
#[expect(clippy::derivable_impls, reason = "the derive is refused crate-wide; see above")]
impl Default for Context {
    fn default() -> Self {
        Self {}
    }
}
