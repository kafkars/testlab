//! Client-quota actions become exact independent Kafka CLI observation targets.

use testlab_schema::{
    AdapterCommand, AlterClientQuotaCommand, DescribeClientQuotaCommand, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ClientQuotaTarget, TargetMatch};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(Some(match action {
        ScenarioAction::AlterClientQuota(value) => (
            AdapterCommand::AlterClientQuota(AlterClientQuotaCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
                bytes_per_second: value.bytes_per_second,
                validate_only: value.validate_only,
                timeout_ms: value.timeout_ms,
            }),
            AdminTarget::ClientQuota(ClientQuotaTarget {
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
            }),
        ),
        ScenarioAction::DescribeClientQuota(value) => (
            AdapterCommand::DescribeClientQuota(DescribeClientQuotaCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
                timeout_ms: value.timeout_ms,
            }),
            AdminTarget::ClientQuota(ClientQuotaTarget {
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                direction: value.direction,
            }),
        ),
        _ => return Ok(None),
    }))
}
