//! Client-quota actions translate without leaking verifier-owned expected values.

use testlab_schema::{AdapterCommand, DescribeClientQuotaCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::AlterClientQuota(value) => (
            AdapterCommand::AlterClientQuota(value.clone()),
            ExpectedEvent::ClientQuotaAltered(value.operation_id.clone()),
        ),
        ScenarioAction::DescribeClientQuota(value) => (
            AdapterCommand::DescribeClientQuota(DescribeClientQuotaCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::ClientQuotaDescribed(value.operation_id.clone()),
        ),
        _ => return None,
    })
}
