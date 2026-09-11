//! Log-directory targets preserve exact action and command identities.

use testlab_schema::{AdapterCommand, DescribeLogDirsCommand, OperationId, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LogDirsTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
}

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::DescribeLogDirs(action) = action else {
        return Ok(None);
    };
    Ok(Some((
        AdapterCommand::DescribeLogDirs(DescribeLogDirsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::LogDirs(LogDirsTarget {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
        }),
    )))
}
