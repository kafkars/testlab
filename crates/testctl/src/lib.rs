//! Testctl owns scenario execution, subject supervision, and sealed evidence.

#[cfg(test)]
mod admin_leader_election_protocol_test;
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
mod runner_protocol_admin_acl;
mod runner_protocol_admin_client_quota;
mod runner_protocol_admin_config;
mod runner_protocol_admin_family;
mod runner_protocol_admin_group_batch;
mod runner_protocol_admin_producers;
mod runner_protocol_admin_share_group;
mod runner_protocol_admin_transactions;
mod runner_protocol_admin_user_scram;
mod runner_protocol_cancel;
mod runner_protocol_concurrent;
mod runner_protocol_event;
mod runner_protocol_expected;
mod runner_protocol_family;
mod runner_protocol_identity;
mod runner_protocol_share;
mod runner_protocol_transaction;
mod session;
mod session_command;
mod session_command_admin;
mod session_command_admin_acl;
mod session_command_admin_batch;
mod session_command_admin_broker_unregistration;
mod session_command_admin_client_quota;
mod session_command_admin_config;
mod session_command_admin_delegation_token;
mod session_command_admin_group_batch;
mod session_command_admin_leader_election;
mod session_command_admin_partition_reassignments;
mod session_command_admin_producers;
mod session_command_admin_records;
mod session_command_admin_replica_log_dirs;
mod session_command_admin_share_group;
mod session_command_admin_streams_group;
mod session_command_admin_transactions;
mod session_command_admin_user_scram;
mod session_command_concurrent;
mod session_command_consumer;
mod session_environment_control;
mod session_share;
mod time;

pub use app::run_cli;
pub use run_error::AppError;

#[cfg(test)]
mod action_contract_test;
#[cfg(test)]
mod admin_acl_protocol_test;
#[cfg(test)]
mod admin_client_quota_protocol_test;
#[cfg(test)]
mod admin_config_resources_protocol_test;
#[cfg(test)]
mod admin_consumer_group_members_removal_protocol_test;
#[cfg(test)]
mod admin_consumer_groups_deletion_protocol_test;
#[cfg(test)]
mod admin_delete_records_batch_protocol_test;
#[cfg(test)]
mod admin_log_dirs_protocol_test;
#[cfg(test)]
mod admin_metadata_quorum_protocol_test;
#[cfg(test)]
mod admin_partition_reassignments_protocol_test;
#[cfg(test)]
mod admin_producers_protocol_test;
#[cfg(test)]
mod admin_replica_log_dirs_protocol_test;
#[cfg(test)]
mod admin_share_group_lifecycle_protocol_test;
#[cfg(test)]
mod admin_share_group_offset_deletion_protocol_test;
#[cfg(test)]
mod admin_share_group_offset_mutation_protocol_test;
#[cfg(test)]
mod admin_share_group_protocol_test;
#[cfg(test)]
mod admin_share_groups_description_protocol_test;
#[cfg(test)]
mod admin_share_groups_offsets_protocol_test;
#[cfg(test)]
mod admin_topic_configs_protocol_test;
#[cfg(test)]
mod admin_topics_deletion_protocol_test;
#[cfg(test)]
mod admin_topics_description_protocol_test;
#[cfg(test)]
mod admin_transactions_protocol_test;
#[cfg(test)]
mod admin_user_scram_protocol_test;
#[cfg(test)]
mod candidate_provenance_test;
#[cfg(test)]
mod candidate_test;
#[cfg(test)]
mod catalog_admin_discovery_test;
#[cfg(test)]
mod catalog_assigned_consumer_configuration_test;
#[cfg(test)]
mod catalog_assigned_consumer_controls_test;
#[cfg(test)]
mod catalog_client_metrics_test;
#[cfg(test)]
mod catalog_feature_updates_test;
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
mod catalog_replica_log_dirs_test;
#[cfg(test)]
mod catalog_security_matrix_test;
#[cfg(test)]
mod catalog_share_batch_test;
#[cfg(test)]
mod catalog_share_configuration_test;
#[cfg(test)]
mod catalog_share_group_admin_test;
#[cfg(test)]
mod catalog_streams_group_admin_test;
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
mod session_command_admin_group_listing_test;
#[cfg(test)]
mod session_command_admin_manual_partitions_test;
#[cfg(test)]
mod session_command_admin_manual_topic_test;
#[cfg(test)]
mod session_command_admin_test;
#[cfg(test)]
mod session_command_admin_validate_test;
#[cfg(test)]
mod session_command_assigned_consumer_configuration_test;
#[cfg(test)]
mod session_command_assigned_consumer_event_test;
#[cfg(test)]
mod session_command_client_identity_test;
#[cfg(test)]
mod session_command_client_metrics_test;
#[cfg(test)]
mod session_command_concurrent_test;
#[cfg(test)]
mod session_command_consumer_test;
#[cfg(test)]
mod session_command_handle_ownership_test;
#[cfg(test)]
mod session_command_policy_test;
#[cfg(test)]
mod session_command_producer_cancellation_test;
#[cfg(test)]
mod session_command_producer_configuration_test;
#[cfg(test)]
mod session_command_share_method_test;
#[cfg(test)]
mod session_command_transaction_test;
#[cfg(test)]
mod session_command_transfer_test;
#[cfg(test)]
mod session_share_test;
#[cfg(test)]
mod session_test;
