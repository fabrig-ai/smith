//! Hosted processes are metered through teardown (protocol/hosts.md, section 3;
//! programming-model.md, section 6.3).

#[cfg(test)]
mod tests {
    use skein_world::domain::heap::Counting;

    #[global_allocator]
    static HEAP: Counting = Counting;

    #[test]
    fn hosted_parent_agent_and_peer_are_metered_at_every_iteration_and_drop() {
        use smith_hosts_world::{Program, World};
        for program in [Program::Service, Program::ErrorTail] {
            let mut world = World::new(7, program);
            world.check_memory();
            world.settle();
            assert!(world.seen().gone.is_some(), "the parent releases its slot");
            drop(world);
        }
    }
}
