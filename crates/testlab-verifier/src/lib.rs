//! Pure deterministic verification maps public history and observations to contracts.

mod admin;
mod admin_batch;
mod admin_classic_groups;
mod admin_cluster;
mod admin_config;
mod admin_discovery;
mod admin_failure;
mod admin_group;
mod admin_group_baseline;
mod admin_group_batch;
mod admin_group_batch_mutation;
mod admin_group_evidence;
mod admin_group_mutation;
mod admin_records;
mod admin_topic;
mod admin_validate_only;
mod admin_validate_only_evidence;
mod adversary;
mod assigned_consumer_controls;
mod broker_policy;
mod broker_policy_acl;
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
mod group_consumer_shutdown;
mod group_ownership;
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
mod producer_records;
mod protocol;
mod record_consumers;
mod record_offsets;
mod share;
mod share_receive;
mod support;
mod transaction;
mod transaction_boundaries;
mod transaction_offsets;
mod transaction_records;
mod verify;
mod verify_index;

pub use contracts::known_contract_ids;
pub use verify::verify;

#[cfg(test)]
mod admin_batch_test;
#[cfg(test)]
mod admin_classic_groups_test;
#[cfg(test)]
mod admin_config_test;
#[cfg(test)]
mod admin_discovery_test;
#[cfg(test)]
mod admin_earliest_offset_test;
#[cfg(test)]
mod admin_failure_test;
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
mod admin_records_test;
#[cfg(test)]
mod admin_test;
#[cfg(test)]
mod admin_topic_cluster_test;
#[cfg(test)]
mod admin_topic_failure_test;
#[cfg(test)]
mod admin_validate_only_test;
#[cfg(test)]
mod adversary_test;
#[cfg(test)]
mod assigned_consumer_controls_test;
#[cfg(test)]
mod assigned_cursor_test;
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
mod group_consumer_shutdown_test;
#[cfg(test)]
mod group_ownership_test;
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
mod record_consumers_test;
#[cfg(test)]
mod record_offsets_test;
#[cfg(test)]
mod share_test;
#[cfg(test)]
mod transaction_boundaries_test;
#[cfg(test)]
mod transaction_fence_test;
#[cfg(test)]
mod transaction_offsets_test;
#[cfg(test)]
mod transaction_records_test;
#[cfg(test)]
mod transaction_test;
#[cfg(test)]
mod verify_fixture;
#[cfg(test)]
mod verify_integrity_test;
#[cfg(test)]
mod verify_test;
