//! External environment effects remain separate from harness verdict logic.

mod compose;
mod compose_command;
mod compose_support;
mod compose_types;
mod terminal;
mod terminal_capture;

pub use compose::DockerComposeEnvironment;
pub use compose_types::{ComposeArtifact, ComposeFailure, ComposePhase, ComposeRequest};
pub use terminal::{TerminalOutput, TerminalRequest, run_terminal};

#[cfg(test)]
mod compose_test;
#[cfg(test)]
mod terminal_test;
