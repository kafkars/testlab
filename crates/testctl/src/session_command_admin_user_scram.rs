//! User SCRAM actions translate without leaking expectations or password bytes.

use testlab_schema::{AdapterCommand, DescribeUserScramCredentialCommand, ScenarioAction};

use crate::runner_protocol::ExpectedEvent;

pub(crate) fn translate(action: &ScenarioAction) -> Option<(AdapterCommand, ExpectedEvent)> {
    Some(match action {
        ScenarioAction::AlterUserScramCredential(value) => (
            AdapterCommand::AlterUserScramCredential(value.clone()),
            ExpectedEvent::UserScramCredentialAltered(value.operation_id.clone()),
        ),
        ScenarioAction::DescribeUserScramCredential(value) => (
            AdapterCommand::DescribeUserScramCredential(DescribeUserScramCredentialCommand {
                client_id: value.client_id.clone(),
                operation_id: value.operation_id.clone(),
                user: value.user.clone(),
                mechanism: value.mechanism,
                timeout_ms: value.timeout_ms,
            }),
            ExpectedEvent::UserScramCredentialDescribed(value.operation_id.clone()),
        ),
        _ => return None,
    })
}
