//! Client-quota actions translate without leaking verifier-owned expected values.

use testlab_schema::{
    AdapterCommand, AlterClientQuotaCommand, DescribeClientQuotaCommand, ScenarioAction,
};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::AlterClientQuota(value) => {
            let command = AlterClientQuotaCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
                bytes_per_second: value.bytes_per_second,
                validate_only: value.validate_only,
                timeout_ms: value.timeout_ms,
            };
            let expected = if value.validate_only {
                ExpectedEvent::ClientQuotaAlterationValidated(value.operation_id.clone())
            } else {
                ExpectedEvent::ClientQuotaAltered(value.operation_id.clone())
            };
            (AdapterCommand::AlterClientQuota(command), expected)
        }
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
