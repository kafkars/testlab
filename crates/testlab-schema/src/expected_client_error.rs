//! Expected public failures remain scenario facts and never enter adapter commands.

use crate::ScenarioAction;

/// Returns the exact normalized error declared for one public action.
pub fn expected_client_error(action: &ScenarioAction) -> Option<&str> {
    crate::expected_admin_error(action)
        .map(|(_, code)| code)
        .or(match action {
            ScenarioAction::GroupReceive {
                expected_error_code,
                ..
            }
            | ScenarioAction::CreateTransactionalProducer {
                expected_error_code,
                ..
            } => expected_error_code.as_deref(),
            _ => None,
        })
}
