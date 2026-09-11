//! Log-directory targets preserve exact action and command identities.

use testlab_schema::{
    AdapterCommand, DescribeLogDirsCommand, DescribeReplicaLogDirsCommand, OperationId,
    ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, TargetMatch};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogDirsTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
}

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(match action {
        ScenarioAction::DescribeLogDirs(action) => Some((
            AdapterCommand::DescribeLogDirs(DescribeLogDirsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::LogDirs(target(
                &action.operation_id,
                &action.topic,
                action.partition,
            )),
        )),
        ScenarioAction::DescribeReplicaLogDirs(action) => Some((
            AdapterCommand::DescribeReplicaLogDirs(DescribeReplicaLogDirsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::ReplicaLogDirs(target(
                &action.operation_id,
                &action.topic,
                action.partition,
            )),
        )),
        _ => None,
    })
}

fn target(operation_id: &OperationId, topic: &str, partition: i32) -> LogDirsTarget {
    LogDirsTarget {
        operation_id: operation_id.clone(),
        topic: topic.to_owned(),
        partition,
    }
}
