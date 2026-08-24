//! External environment effects remain separate from harness verdict logic.

mod compose;
mod compose_command;
mod compose_disruption;
mod compose_execute;
mod compose_feature;
mod compose_observe;
mod compose_partition_leader;
mod compose_ports;
mod compose_provision;
mod compose_readiness;
mod compose_security;
mod compose_support;
mod compose_topic_readiness;
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
mod compose_disruption_test;
#[cfg(test)]
mod compose_partition_leader_test;
#[cfg(test)]
mod compose_ports_test;
#[cfg(test)]
mod compose_provision_test;
#[cfg(test)]
mod compose_test;
#[cfg(test)]
mod compose_test_fixture;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod security_test;
#[cfg(test)]
mod terminal_test;
