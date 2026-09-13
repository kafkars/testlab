//! Broker-unregistration targets retain only independent postcondition expectations.

use testlab_schema::{AdapterCommand, ScenarioAction, UnregisterBrokerCommand};

use crate::observer_admin_target::{AdminTarget, TargetMatch};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BrokerUnregistrationTarget {
    pub(super) operation_id: testlab_schema::OperationId,
    pub(super) expected_remaining_broker_ids: Vec<i32>,
}

pub(super) fn match_action(action: &ScenarioAction) -> Option<TargetMatch> {
    let ScenarioAction::UnregisterBroker(action) = action else {
        return None;
    };
    Some((
        AdapterCommand::UnregisterBroker(UnregisterBrokerCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            broker_id: action.broker_id,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::BrokerUnregistration(BrokerUnregistrationTarget {
            operation_id: action.operation_id.clone(),
            expected_remaining_broker_ids: action.expected_remaining_broker_ids.clone(),
        }),
    ))
}

#[cfg(test)]
#[path = "observer_admin_broker_unregistration_target_test.rs"]
mod tests;
