//! Streams-group lifecycle targets retain exact final-deletion order.

use testlab_schema::{AdapterCommand, ExerciseStreamsGroupAdminLifecycleCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct StreamsGroupTarget {
    pub(super) operation_id: testlab_schema::OperationId,
    pub(super) group_ids: Vec<String>,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    let ScenarioAction::ExerciseStreamsGroupAdminLifecycle(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::ExerciseStreamsGroupAdminLifecycle(
            ExerciseStreamsGroupAdminLifecycleCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                primary_group_id: action.primary_group_id.clone(),
                secondary_group_id: action.secondary_group_id.clone(),
                input_topic: action.input_topic.clone(),
                altered_offset: action.altered_offset,
                include_authorized_operations: action.include_authorized_operations,
                include_topology_description: action.include_topology_description,
                require_stable: action.require_stable,
                timeout_ms: action.timeout_ms,
            },
        ),
        AdminTarget::StreamsGroupsLifecycle(StreamsGroupTarget {
            operation_id: action.operation_id.clone(),
            group_ids: vec![
                action.secondary_group_id.clone(),
                action.primary_group_id.clone(),
            ],
        }),
    ))
}
