//! Replica log-directory actions translate without leaking verifier expectations.

use testlab_schema::{
    AdapterCommand, AlterReplicaLogDirsCommand, DescribeReplicaLogDirsCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::AlterReplicaLogDirs(action) => (
            AdapterCommand::AlterReplicaLogDirs(AlterReplicaLogDirsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                assignments: action.assignments.clone(),
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ReplicaLogDirsAltered(action.operation_id.clone()),
        ),
        ScenarioAction::DescribeReplicaLogDirs(action) => (
            AdapterCommand::DescribeReplicaLogDirs(DescribeReplicaLogDirsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                timeout_ms: action.timeout_ms,
            }),
            ExpectedEvent::ReplicaLogDirsDescribed(
                action.operation_id.clone(),
                action.topic.clone(),
                action.partition,
            ),
        ),
        _ => return None,
    })
}
