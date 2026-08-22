//! External environment effects remain separate from harness verdict logic.

mod compose;
mod compose_command;
mod compose_execute;
mod compose_observe;
mod compose_provision;
mod compose_support;
mod compose_types;
mod observer;
mod observer_error;
mod observer_record;
mod security;
mod terminal;
mod terminal_capture;

pub use compose::DockerComposeEnvironment;
pub use compose_types::{
    ComposeArtifact, ComposeFailure, ComposeObservation, ComposePhase, ComposeRequest,
};
pub use terminal::{TerminalOutput, TerminalRequest, run_terminal};

#[cfg(test)]
mod compose_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod security_test;
#[cfg(test)]
mod terminal_test;
