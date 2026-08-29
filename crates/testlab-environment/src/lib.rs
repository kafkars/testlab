//! External environment effects remain separate from harness verdict logic.

mod adversary_connection;
mod adversary_frame;
mod adversary_output;
mod adversary_process;
mod adversary_process_evidence;
mod adversary_process_io;
mod adversary_process_types;
mod adversary_state;
mod adversary_worker;
mod broker_policy_command;
mod broker_policy_observation;
mod compose;
mod compose_broker_policy;
mod compose_broker_role;
mod compose_command;
mod compose_construction;
mod compose_disruption;
mod compose_execute;
mod compose_feature;
mod compose_network_proxy;
mod compose_observe;
mod compose_observe_admin;
mod compose_ports;
mod compose_provision;
mod compose_provision_targets;
mod compose_readiness;
mod compose_security;
mod compose_seed;
mod compose_startup;
mod compose_support;
mod compose_topic_readiness;
mod compose_types;
mod kafka_role_wire;
mod network_proxy_output;
mod network_proxy_process;
mod network_proxy_process_finish;
mod network_proxy_process_io;
mod network_proxy_process_types;
mod network_proxy_relay;
mod network_proxy_state;
mod network_proxy_worker;
mod observer;
mod observer_admin;
mod observer_admin_batch_topic_target;
mod observer_admin_classic_group;
mod observer_admin_config;
mod observer_admin_config_target;
mod observer_admin_group;
mod observer_admin_group_target;
mod observer_admin_metadata;
mod observer_admin_partition_offsets_target;
mod observer_admin_plural_group_target;
mod observer_admin_target;
mod observer_admin_topic_target;
mod observer_error;
mod observer_group_offset;
mod observer_group_offsets;
mod observer_partition_offsets;
mod observer_record;
mod security;
mod terminal;
mod terminal_capture;

pub use adversary_process_types::{AdversaryProcessRequest, RunningAdversary};
pub use adversary_worker::run_adversary_worker;
pub use compose::DockerComposeEnvironment;
pub use compose_types::{
    ComposeArtifact, ComposeFailure, ComposeObservation, ComposePhase, ComposeRequest,
};
pub use network_proxy_process_types::{
    NetworkProxyFinish, NetworkProxyProcessRequest, RunningNetworkProxy,
};
pub use network_proxy_worker::run_network_proxy_worker;
pub use terminal::{TerminalOutput, TerminalRequest, run_terminal};

#[cfg(test)]
mod adversary_frame_test;
#[cfg(test)]
mod adversary_state_test;
#[cfg(test)]
mod broker_policy_test;
#[cfg(test)]
mod compose_broker_role_test;
#[cfg(test)]
mod compose_concurrent_provision_test;
#[cfg(test)]
mod compose_disruption_test;
#[cfg(test)]
mod compose_observe_admin_test;
#[cfg(test)]
mod compose_plural_group_provision_test;
#[cfg(test)]
mod compose_ports_test;
#[cfg(test)]
mod compose_provision_test;
#[cfg(test)]
mod compose_startup_test;
#[cfg(test)]
mod compose_test;
#[cfg(test)]
mod compose_test_fixture;
#[cfg(test)]
mod compose_validate_only_test;
#[cfg(test)]
mod kafka_role_wire_test;
#[cfg(test)]
mod network_proxy_process_test;
#[cfg(test)]
mod network_proxy_relay_test;
#[cfg(test)]
mod network_proxy_state_test;
#[cfg(test)]
mod observer_admin_batch_topic_target_test;
#[cfg(test)]
mod observer_admin_classic_group_test;
#[cfg(test)]
mod observer_admin_config_test;
#[cfg(test)]
mod observer_admin_group_target_test;
#[cfg(test)]
mod observer_admin_group_test;
#[cfg(test)]
mod observer_admin_plural_group_target_test;
#[cfg(test)]
mod observer_admin_target_test;
#[cfg(test)]
mod observer_admin_validate_only_test;
#[cfg(test)]
mod observer_group_offset_test;
#[cfg(test)]
mod observer_group_offsets_test;
#[cfg(test)]
mod observer_test;
#[cfg(test)]
mod security_test;
#[cfg(test)]
mod terminal_test;
