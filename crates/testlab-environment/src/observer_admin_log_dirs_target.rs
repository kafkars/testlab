//! Log-directory targets preserve exact action and command identities.

use testlab_schema::{
    AdapterCommand, AlterReplicaLogDirsCommand, DescribeLogDirsCommand,
    DescribeReplicaLogDirsCommand, OperationId, ReplicaLogDirAssignmentSpec, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, TargetMatch};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogDirsTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplicaLogDirsAlterationTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) assignments: Vec<ReplicaLogDirAssignmentSpec>,
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
        ScenarioAction::AlterReplicaLogDirs(action) => {
            let Some(first) = action.assignments.first() else {
                return Err(crate::observer_admin_target::invalid(
                    &action.operation_id,
                    "alteration has no replica assignments",
                ));
            };
            Some((
                AdapterCommand::AlterReplicaLogDirs(AlterReplicaLogDirsCommand {
                    client_id: action.client_id.clone(),
                    operation_id: action.operation_id.clone(),
                    assignments: action.assignments.clone(),
                    timeout_ms: action.timeout_ms,
                }),
                AdminTarget::ReplicaLogDirsAlteration(ReplicaLogDirsAlterationTarget {
                    operation_id: action.operation_id.clone(),
                    topic: first.topic.clone(),
                    partition: first.partition,
                    assignments: action.assignments.clone(),
                }),
            ))
        }
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
