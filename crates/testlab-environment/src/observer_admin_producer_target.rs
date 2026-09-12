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
mod tests {
    use super::*;
    use testlab_schema::{BatchRecord, ProducerId, RecordSpec};

    #[test]
    fn admin_abort_preserves_exact_command_and_observation_target() {
        let action = action(vec![record("record-1")]);
        let (command, target) = match_action(&action)
            .unwrap_or_else(|error| panic!("target: {error}"))
            .unwrap_or_else(|| panic!("admin abort target"));
        assert!(matches!(
            command,
            AdapterCommand::ExecuteTransaction {
                disposition: TransactionDisposition::AdminPartitionAbort,
                timeout_ms: 1_000,
                ..
            }
        ));
        let AdminTarget::Producers(target) = target else {
            panic!("producer target");
        };
        assert_eq!(target.operation_id, operation("transaction-1"));
        assert_eq!(target.topic, "orders");
        assert_eq!(target.partition, 2);
    }

    #[test]
    fn admin_abort_rejects_a_non_singleton_target() {
        let action = action(vec![record("record-1"), record("record-2")]);
        assert!(match_action(&action).is_err());
    }

    fn action(operations: Vec<BatchRecord>) -> ScenarioAction {
        ScenarioAction::ExecuteTransaction {
            producer_id: ProducerId::new("transactional-1")
                .unwrap_or_else(|error| panic!("producer ID: {error}")),
            transaction_id: operation("transaction-1"),
            operations,
            method: Default::default(),
            disposition: TransactionDisposition::AdminPartitionAbort,
            timeout_ms: 1_000,
        }
    }

    fn record(id: &str) -> BatchRecord {
        BatchRecord {
            operation_id: operation(id),
            record: RecordSpec {
                topic: "orders".to_owned(),
                partition: 2,
                sequence: 1,
                timestamp_millis: None,
                key: None,
                value: None,
                headers: Vec::new(),
            },
        }
    }

    fn operation(value: &str) -> OperationId {
        OperationId::new(value).unwrap_or_else(|error| panic!("operation ID: {error}"))
    }
}
