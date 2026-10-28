//! Fresh Open words cross the shipped direct provider boundary.
#[global_allocator]
static HEAP: skein_heap::Counting = skein_heap::Counting;

#[path = "support/direct_opening.rs"]
mod direct_opening;
#[path = "support/opening.rs"]
mod opening;

#[test]
fn open_brief_is_the_native_peers_first_user_message() {
    for anthropic in [false, true] {
        drop(direct_opening::direct(37, anthropic));
    }
}
