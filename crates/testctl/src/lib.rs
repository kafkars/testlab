//! Testctl owns scenario execution, subject supervision, and sealed evidence.

mod app;
mod candidate;
mod candidate_manifest;
mod candidate_provenance;
mod catalog;
mod catalog_io;
mod evidence;
mod evidence_io;
mod identity;
mod issued_operations;
mod process;
mod process_io;
mod protocol_session;
mod qualification;
mod qualification_evidence;
mod qualification_merge;
mod qualification_shard;
mod recorder;
mod run_error;
mod runner;
mod runner_adversary;
mod runner_environment;
mod runner_protocol;
mod runner_protocol_admin;
mod runner_protocol_admin_config;
mod runner_protocol_admin_group_batch;
mod runner_protocol_cancel;
mod runner_protocol_concurrent;
mod runner_protocol_event;
mod runner_protocol_family;
mod runner_protocol_identity;
mod runner_protocol_share;
mod session;
mod session_command;
mod session_command_admin;
mod session_command_admin_batch;
mod session_command_admin_config;
mod session_command_admin_group_batch;
mod session_command_admin_records;
mod session_command_concurrent;
mod session_command_consumer;
mod session_environment_control;
mod session_share;
mod time;

pub use app::run_cli;
pub use run_error::AppError;

#[cfg(test)]
mod candidate_provenance_test;
#[cfg(test)]
mod candidate_test;
#[cfg(test)]
mod catalog_assigned_consumer_controls_test;
#[cfg(test)]
mod catalog_client_metrics_test;
#[cfg(test)]
mod catalog_group_configuration_test;
#[cfg(test)]
mod catalog_group_controls_test;
#[cfg(test)]
mod catalog_group_shutdown_test;
#[cfg(test)]
mod catalog_kafkars_contract_test;
#[cfg(test)]
mod catalog_lifecycle_test;
#[cfg(test)]
mod catalog_producer_cancellation_test;
#[cfg(test)]
mod catalog_producer_configuration_test;
#[cfg(test)]
mod catalog_share_batch_test;
#[cfg(test)]
mod catalog_share_configuration_test;
#[cfg(test)]
mod catalog_test;
#[cfg(test)]
mod catalog_transaction_offsets_test;
#[cfg(test)]
mod issued_concurrent_operations_test;
#[cfg(test)]
mod issued_operations_test;
#[cfg(test)]
mod qualification_shard_test;
#[cfg(test)]
mod qualification_test;
#[cfg(test)]
mod recorder_test;
#[cfg(test)]
mod runner_docker_test;
#[cfg(test)]
mod runner_protocol_admin_batch_test;
#[cfg(test)]
mod runner_protocol_admin_group_batch_test;
#[cfg(test)]
mod runner_protocol_admin_test;
#[cfg(test)]
mod runner_protocol_concurrent_test;
#[cfg(test)]
mod runner_protocol_producer_cancellation_test;
#[cfg(test)]
mod runner_protocol_share_test;
#[cfg(test)]
mod runner_protocol_test;
#[cfg(test)]
mod runner_protocol_transaction_offsets_test;
#[cfg(test)]
mod runner_test;
#[cfg(test)]
mod session_command_admin_batch_test;
#[cfg(test)]
mod session_command_admin_config_test;
#[cfg(test)]
mod session_command_admin_group_batch_test;
#[cfg(test)]
mod session_command_admin_test;
#[cfg(test)]
mod session_command_admin_validate_test;
#[cfg(test)]
mod session_command_client_metrics_test;
#[cfg(test)]
mod session_command_concurrent_test;
#[cfg(test)]
mod session_command_consumer_test;
#[cfg(test)]
mod session_command_policy_test;
#[cfg(test)]
mod session_command_producer_cancellation_test;
#[cfg(test)]
mod session_command_producer_configuration_test;
#[cfg(test)]
mod session_command_transaction_test;
#[cfg(test)]
mod session_share_test;
#[cfg(test)]
mod session_test;
