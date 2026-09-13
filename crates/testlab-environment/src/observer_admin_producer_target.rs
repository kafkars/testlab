//! Active-producer targets preserve exact action and command identities.

use testlab_schema::{
    AdapterCommand, DescribeProducersCommand, OperationId, ScenarioAction, TransactionDisposition,
};

use crate::observer_admin_target::{AdminTarget, TargetMatch, invalid};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProducerTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
}

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(match action {
        ScenarioAction::DescribeProducers(action) => Some((
            AdapterCommand::DescribeProducers(DescribeProducersCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                broker_id: action.broker_id,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::Producers(ProducerTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
            }),
        )),
        ScenarioAction::ExecuteTransaction {
            producer_id,
            transaction_id,
            operations,
            method,
            disposition: TransactionDisposition::AdminPartitionAbort,
            topic_identity_operation_id,
            timeout_ms,
        } => {
            let [operation] = operations.as_slice() else {
                return Err(invalid(
                    transaction_id,
                    "requires exactly one staged record for independent observation",
                ));
            };
            Some((
                AdapterCommand::ExecuteTransaction {
                    producer_id: producer_id.clone(),
                    transaction_id: transaction_id.clone(),
                    operations: operations.clone(),
                    method: *method,
                    disposition: TransactionDisposition::AdminPartitionAbort,
                    validate_topic_uuids: topic_identity_operation_id.is_some(),
                    timeout_ms: *timeout_ms,
                },
                AdminTarget::Producers(ProducerTarget {
                    operation_id: transaction_id.clone(),
                    topic: operation.record.topic.clone(),
                    partition: operation.record.partition,
                }),
            ))
        }
        _ => None,
    })
}

#[cfg(test)]
#[path = "observer_admin_producer_target_test.rs"]
mod tests;
