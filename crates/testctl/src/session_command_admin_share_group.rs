//! Share-group description actions translate without leaking verifier expectations.

use testlab_schema::{AdapterCommand, DescribeShareGroupCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    let ScenarioAction::DescribeShareGroup(value) = action else {
        return None;
    };
    Some((
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
    ))
}
