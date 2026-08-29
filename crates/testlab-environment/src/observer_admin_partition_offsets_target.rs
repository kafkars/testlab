//! Offset-admin actions produce exact independent watermark targets.

use testlab_schema::{
    AdapterCommand, AdminOffsetPosition, DeleteRecordsCommand, ListOffsetsCommand, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, PartitionOffsetsTarget, TargetMatch};

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    Some(match action {
        ScenarioAction::ListOffsets(action) if action.expected_error_code.is_none() => (
            AdapterCommand::ListOffsets(ListOffsetsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                position: action.position,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::PartitionOffsets(PartitionOffsetsTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                expected_low: (action.position == AdminOffsetPosition::Earliest)
                    .then_some(action.expected_offset)
                    .flatten(),
                expected_high: (action.position == AdminOffsetPosition::Latest)
                    .then_some(action.expected_offset)
                    .flatten(),
                poll_expected: false,
            }),
        ),
        ScenarioAction::DeleteRecords(action) => (
            AdapterCommand::DeleteRecords(DeleteRecordsCommand {
                client_id: action.client_id.clone(),
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                before_offset: action.before_offset,
                timeout_ms: action.timeout_ms,
            }),
            AdminTarget::PartitionOffsets(PartitionOffsetsTarget {
                operation_id: action.operation_id.clone(),
                topic: action.topic.clone(),
                partition: action.partition,
                expected_low: Some(action.before_offset),
                expected_high: Some(action.expected_high_watermark),
                poll_expected: true,
            }),
        ),
        _ => return None,
    })
}
