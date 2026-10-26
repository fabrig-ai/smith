//! The agent shell keeps its service, startup configuration, inherited roots,
//! trace and error writer. It never knows the driving kernel or neighbours.
//! `Agent` hosts the service and `run` supplies its kernel and clock.
//! The hosted face takes all resources from its caller; native startup is Unix-only.
//! Contract: shell.md, sections 2.2 and 2.5; protocol/agent.md, sections 4–6.

mod agent_shell;

pub mod config;
#[cfg(unix)]
pub mod program;
#[cfg(unix)]
pub mod relay;
pub mod trace;

pub use agent_shell::{Agent, ProgramResources, Resources, given_up_line, identity};

#[cfg(unix)]
pub use agent_shell::run;
