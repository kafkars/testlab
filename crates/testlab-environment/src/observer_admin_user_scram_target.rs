//! User SCRAM actions become exact independent Kafka CLI observation targets.

use testlab_schema::{AdapterCommand, DescribeUserScramCredentialCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch, UserScramCredentialTarget};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    Ok(Some(match action {
        ScenarioAction::AlterUserScramCredential(value) => (
            AdapterCommand::AlterUserScramCredential(value.clone()),
            AdminTarget::UserScramCredential(UserScramCredentialTarget {
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                mechanism: value.mechanism,
            }),
        ),
        ScenarioAction::DescribeUserScramCredential(value) => (
            AdapterCommand::DescribeUserScramCredential(DescribeUserScramCredentialCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                mechanism: value.mechanism,
                timeout_ms: value.timeout_ms,
            }),
            AdminTarget::UserScramCredential(UserScramCredentialTarget {
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                mechanism: value.mechanism,
            }),
        ),
        _ => return Ok(None),
    }))
}
