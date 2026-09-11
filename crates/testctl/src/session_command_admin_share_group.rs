//! Share-group read actions translate without leaking verifier expectations.

use testlab_schema::{
    AdapterCommand, DescribeShareGroupCommand, ListShareGroupOffsetsCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::DescribeShareGroup(value) => (
            AdapterCommand::DescribeShareGroup(DescribeShareGroupCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupDescribed {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
            },
        ),
        ScenarioAction::ListShareGroupOffsets(value) => (
            AdapterCommand::ListShareGroupOffsets(ListShareGroupOffsetsCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ShareGroupOffsetsListed {
                operation_id: value.operation_id.clone(),
                group_id: value.group_id.clone(),
                topic: value.topic.clone(),
                partition: value.partition,
            },
        ),
        _ => return None,
    })
}
