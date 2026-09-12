//! Streams-group lifecycle intent maps to one exact composite Admin command.

use testlab_schema::{AdapterCommand, ExerciseStreamsGroupAdminLifecycleCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
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
                timeout_ms: action.timeout_ms,
            },
        ),
        ExpectedEvent::StreamsGroupAdminLifecycleExercised(action.operation_id.clone()),
    ))
}
