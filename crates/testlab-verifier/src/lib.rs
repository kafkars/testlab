//! Pure deterministic verification maps public history and observations to contracts.

mod admin;
mod admin_acl;
mod admin_batch;
mod admin_broker_unregistration;
mod admin_classic_groups;
mod admin_client_quota;
mod admin_cluster;
mod admin_config;
mod admin_config_batch;
mod admin_config_resources;
mod admin_consumer_group_member_removal;
mod admin_consumer_groups_deletion;
mod admin_consumer_groups_description;
mod admin_delegation_token;
mod admin_discovery;
mod admin_failure;
mod admin_feature_updates;
mod admin_features;
mod admin_group;
mod admin_group_baseline;
mod admin_group_batch;
mod admin_group_batch_mutation;
mod admin_group_evidence;
mod admin_group_mutation;
mod admin_leader_election;
mod admin_log_dirs;
mod admin_metadata_quorum;
mod admin_offset_batch;
mod admin_operation;
mod admin_partition_reassignments;
mod admin_producer_fencing;
mod admin_producers;
mod admin_records;
mod admin_records_batch;
mod admin_replica_log_dirs;
mod admin_share_group;
mod admin_share_group_offset_deletion;
mod admin_share_group_offset_mutation;
mod admin_share_groups_deletion;
mod admin_share_groups_description;
mod admin_share_groups_offsets;
mod admin_streams_group;
mod admin_topic;
mod admin_topics_deletion;
mod admin_topics_description;
mod admin_transactions;
mod admin_user_scram;
mod admin_validate_only;
mod admin_validate_only_evidence;
mod adversary;
mod assigned_consumer_controls;
mod assigned_consumer_events;
mod assigned_consumer_receive_method;
mod broker_policy;
mod broker_policy_acl;
mod broker_policy_assigned_consumer;
mod broker_policy_control;
mod broker_policy_recovery;
mod broker_role_recovery;
mod client_failure;
mod client_metrics;
mod concurrent;
mod concurrent_support;
mod consumer;
mod contracts;
mod group_consumer_controls;
mod group_consumer_receive_method;
mod group_consumer_shutdown;
mod group_ownership;
mod group_partial_checkpoint;
mod group_processing_acknowledgement;
mod group_receive_failures;
mod group_recovery;
mod group_redistribution;
mod index;
mod lifecycle;
mod lifecycle_commands;
mod network_proxy;
mod network_proxy_progress;
mod observations;
mod producer_cancellation;
mod producer_error;
mod producer_partitioning;
mod producer_records;
mod producer_send_method;
mod producer_timestamp;
mod protocol;
mod record_consumers;
mod record_offsets;
mod share;
mod share_receive;
mod support;
mod transaction;
mod transaction_admin_abort;
mod transaction_boundaries;
mod transaction_offsets;
mod transaction_records;
mod transaction_send_method;
mod verify;
mod verify_index;

pub use contracts::known_contract_ids;
pub use verify::verify;

#[cfg(test)]
mod admin_acl_test;
#[cfg(test)]
mod admin_batch_test;
#[cfg(test)]
mod admin_broker_unregistration_test;
#[cfg(test)]
mod admin_classic_groups_test;
#[cfg(test)]
mod admin_client_quota_test;
#[cfg(test)]
mod admin_config_batch_mutation_test;
#[cfg(test)]
mod admin_config_batch_test;
#[cfg(test)]
mod admin_config_resources_test;
#[cfg(test)]
mod admin_config_test;
#[cfg(test)]
mod admin_consumer_group_member_removal_test;
#[cfg(test)]
mod admin_consumer_groups_deletion_test;
#[cfg(test)]
mod admin_consumer_groups_description_test;
#[cfg(test)]
mod admin_discovery_test;
#[cfg(test)]
mod admin_earliest_offset_test;
#[cfg(test)]
mod admin_failure_test;
#[cfg(test)]
mod admin_feature_updates_test;
#[cfg(test)]
mod admin_features_test;
#[cfg(test)]
mod admin_group_batch_test;
#[cfg(test)]
mod admin_group_delete_batch_test;
#[cfg(test)]
mod admin_group_lifecycle_test;
#[cfg(test)]
mod admin_group_multi_test;
#[cfg(test)]
mod admin_group_mutation_test;
#[cfg(test)]
mod admin_group_test;
#[cfg(test)]
mod admin_group_verdict_test;
#[cfg(test)]
mod admin_leader_election_test;
#[cfg(test)]
mod admin_log_dirs_test;
#[cfg(test)]
mod admin_metadata_quorum_test;
#[cfg(test)]
mod admin_offset_batch_test;
#[cfg(test)]
mod admin_partition_reassignments_test;
#[cfg(test)]
mod admin_producer_fencing_test;
#[cfg(test)]
mod admin_producers_test;
#[cfg(test)]
mod admin_records_batch_test;
#[cfg(test)]
mod admin_records_test;
#[cfg(test)]
mod admin_replica_log_dirs_alteration_test;
#[cfg(test)]
mod admin_replica_log_dirs_test;
#[cfg(test)]
mod admin_share_group_offset_deletion_test;
#[cfg(test)]
mod admin_share_group_offset_mutation_test;
#[cfg(test)]
mod admin_share_group_offset_test;
#[cfg(test)]
mod admin_share_group_test;
#[cfg(test)]
mod admin_share_groups_deletion_test;
#[cfg(test)]
mod admin_share_groups_description_test;
#[cfg(test)]
mod admin_share_groups_offsets_test;
#[cfg(test)]
mod admin_test;
#[cfg(test)]
mod admin_topic_cluster_test;
#[cfg(test)]
mod admin_topic_failure_test;
#[cfg(test)]
mod admin_topic_ids_test;
#[cfg(test)]
mod admin_topics_deletion_test;
#[cfg(test)]
mod admin_topics_description_test;
#[cfg(test)]
mod admin_transactions_test;
#[cfg(test)]
mod admin_user_scram_test;
#[cfg(test)]
mod admin_validate_only_test;
#[cfg(test)]
mod adversary_test;
#[cfg(test)]
mod assigned_consumer_controls_test;
#[cfg(test)]
mod assigned_consumer_events_test;
#[cfg(test)]
mod assigned_consumer_receive_method_test;
#[cfg(test)]
mod assigned_cursor_test;
#[cfg(test)]
mod broker_policy_assigned_consumer_test;
#[cfg(test)]
mod broker_policy_test;
#[cfg(test)]
mod broker_role_recovery_terminal_test;
#[cfg(test)]
mod broker_role_recovery_test;
#[cfg(test)]
mod client_metrics_test;
#[cfg(test)]
mod concurrent_fixture_history_test;
#[cfg(test)]
mod concurrent_fixture_lifecycle_test;
#[cfg(test)]
mod concurrent_fixture_test;
#[cfg(test)]
mod concurrent_test;
#[cfg(test)]
mod contract_test;
#[cfg(test)]
mod group_consumer_controls_test;
#[cfg(test)]
mod group_consumer_receive_method_test;
#[cfg(test)]
mod group_consumer_shutdown_test;
#[cfg(test)]
mod group_ownership_test;
#[cfg(test)]
mod group_partial_checkpoint_test;
#[cfg(test)]
mod group_processing_acknowledgement_test;
#[cfg(test)]
mod group_receive_failures_test;
#[cfg(test)]
mod group_recovery_share_test;
#[cfg(test)]
mod group_recovery_test;
#[cfg(test)]
mod group_redistribution_test;
#[cfg(test)]
mod lifecycle_commands_test;
#[cfg(test)]
mod network_proxy_modes_test;
#[cfg(test)]
mod network_proxy_test;
#[cfg(test)]
mod producer_cancellation_test;
#[cfg(test)]
mod producer_partitioning_test;
#[cfg(test)]
mod producer_send_method_test;
#[cfg(test)]
mod producer_timestamp_test;
#[cfg(test)]
mod record_consumers_test;
#[cfg(test)]
mod record_offsets_test;
#[cfg(test)]
mod share_test;
#[cfg(test)]
mod transaction_admin_abort_test;
#[cfg(test)]
mod transaction_boundaries_test;
#[cfg(test)]
mod transaction_fence_test;
#[cfg(test)]
mod transaction_offsets_test;
#[cfg(test)]
mod transaction_records_test;
#[cfg(test)]
mod transaction_send_method_test;
#[cfg(test)]
mod transaction_test;
#[cfg(test)]
mod verify_fixture;
#[cfg(test)]
mod verify_integrity_test;
#[cfg(test)]
mod verify_test;
