//! ACL policy command matching retains exact public request identities.

use testlab_schema::{AdapterCommand, ScenarioAction};

pub(super) fn matches(action: &ScenarioAction, command: &AdapterCommand) -> bool {
    match (action, command) {
        (
            ScenarioAction::Send {
                producer_id,
                operation_id,
                method,
                partitioning,
                record,
                ..
            },
            AdapterCommand::Send {
                producer_id: actual_producer,
                operation_id: actual_operation,
                method: actual_method,
                partitioning: actual_partitioning,
                record: actual_record,
                ..
            },
        ) => {
            producer_id == actual_producer
                && operation_id == actual_operation
                && method == actual_method
                && partitioning == actual_partitioning
                && record == actual_record
        }
        (
            ScenarioAction::GroupReceive {
                consumer_id,
                method,
                checkpoint_method,
                receive_id,
                processing_acknowledgement_delay_ms,
                processed_record_count,
                timeout_ms,
                ..
            },
            AdapterCommand::GroupReceive {
                consumer_id: actual_consumer,
                method: actual_method,
                checkpoint_method: actual_checkpoint_method,
                receive_id: actual_receive,
                processing_acknowledgement_delay_ms: actual_delay,
                processed_record_count: actual_processed_count,
                timeout_ms: actual_timeout,
            },
        ) => {
            consumer_id == actual_consumer
                && method == actual_method
                && checkpoint_method == actual_checkpoint_method
                && receive_id == actual_receive
                && processing_acknowledgement_delay_ms == actual_delay
                && processed_record_count == actual_processed_count
                && timeout_ms == actual_timeout
        }
        (ScenarioAction::CreateTopic(action), AdapterCommand::CreateTopic(command)) => {
            action.client_id == command.client_id
                && action.operation_id == command.operation_id
                && action.topic == command.topic
                && action.partitions == command.partitions
                && action.replication_factor == command.replication_factor
                && action.replica_assignments == command.replica_assignments
                && action.validate_only == command.validate_only
                && action.timeout_ms == command.timeout_ms
        }
        (
            ScenarioAction::CreateTransactionalProducer {
                client_id,
                producer_id,
                transactional_id,
                transaction_timeout_ms,
                initialization_timeout_ms,
                ..
            },
            AdapterCommand::CreateTransactionalProducer {
                client_id: actual_client,
                producer_id: actual_producer,
                transactional_id: actual_transactional,
                transaction_timeout_ms: actual_transaction_timeout,
                initialization_timeout_ms: actual_initialization_timeout,
            },
        ) => {
            client_id == actual_client
                && producer_id == actual_producer
                && transactional_id == actual_transactional
                && transaction_timeout_ms == actual_transaction_timeout
                && initialization_timeout_ms == actual_initialization_timeout
        }
        _ => false,
    }
}
