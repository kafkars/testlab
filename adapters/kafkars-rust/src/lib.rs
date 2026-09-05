//! Kafkars adapter translates only the packaged public client surface.

mod adapter_error;
mod admission_retry;
mod assigned_consumer_positions;
#[cfg(test)]
mod assigned_consumer_positions_test;
mod assigned_consumers;
mod client_metrics;
mod connection_security;
mod group_assignment_observe;
#[cfg(test)]
mod group_assignment_observe_test;
mod group_consumer_shutdown;
mod group_consumers;
#[cfg(test)]
mod group_consumers_test;
mod group_receive_events;
#[cfg(test)]
mod group_receive_events_test;
mod group_receive_set;
mod kafkars_api;
mod normalize;
mod producer_configuration;
mod protocol;
mod protocol_admin;
mod protocol_admin_classic_group;
mod protocol_admin_cluster;
mod protocol_admin_config;
mod protocol_admin_create_topics_batch;
mod protocol_admin_group;
mod protocol_admin_group_offset_batch;
mod protocol_admin_group_offset_batch_mutation;
mod protocol_admin_group_offset_mutation;
mod protocol_admin_plural_result;
mod protocol_admin_read;
mod protocol_admin_result;
mod protocol_admin_validation_event;
mod protocol_admin_write;
mod protocol_cancel;
mod protocol_client;
mod protocol_concurrent;
mod protocol_consumer;
mod protocol_descriptor;
mod protocol_group;
mod protocol_lifecycle;
mod protocol_send;
#[cfg(kafkars_share_candidate)]
mod protocol_share;
#[cfg(kafkars_share_candidate)]
mod share_consumers;
#[cfg(kafkars_share_candidate)]
mod share_consumers_acknowledge;
#[cfg(kafkars_share_candidate)]
mod share_consumers_close;
#[cfg(kafkars_share_candidate)]
mod share_consumers_receive;
#[cfg(all(test, kafkars_share_candidate))]
mod share_consumers_test;
mod state;
mod state_client;
mod state_consumer;
mod state_error;
mod state_share;
mod transaction_execute;
mod transaction_fence;
mod transaction_transform;
mod transactional_producers;

pub use adapter_error::AdapterError;
pub use protocol::run_stdio;

#[cfg(test)]
mod admission_retry_test;
#[cfg(test)]
mod client_metrics_test;
#[cfg(test)]
mod connection_security_test;
#[cfg(test)]
mod normalize_test;
#[cfg(test)]
mod producer_configuration_test;
#[cfg(test)]
mod protocol_admin_config_test;
#[cfg(test)]
mod protocol_admin_create_topics_batch_test;
#[cfg(test)]
mod protocol_admin_delete_records_test;
#[cfg(test)]
mod protocol_admin_discovery_test;
#[cfg(test)]
mod protocol_admin_group_offset_batch_test;
#[cfg(test)]
mod protocol_admin_group_offset_test;
#[cfg(test)]
mod protocol_admin_group_test;
#[cfg(test)]
mod protocol_admin_offset_test;
#[cfg(test)]
mod protocol_admin_plural_result_test;
#[cfg(test)]
mod protocol_admin_test;
#[cfg(test)]
mod protocol_admin_validation_event_test;
#[cfg(test)]
mod protocol_cancel_test;
#[cfg(test)]
mod protocol_concurrent_test;
#[cfg(test)]
mod protocol_test;
#[cfg(test)]
mod state_test;
