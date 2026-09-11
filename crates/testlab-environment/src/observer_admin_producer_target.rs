//! Active-producer targets preserve exact action and command identities.

use testlab_schema::{AdapterCommand, DescribeProducersCommand, OperationId, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, TargetMatch};
use crate::observer_error::ObserverError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ProducerTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
}

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::DescribeProducers(action) = action else {
        return Ok(None);
    };
    Ok(Some((
        AdapterCommand::DescribeProducers(DescribeProducersCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::Producers(ProducerTarget {
            operation_id: action.operation_id.clone(),
            topic: action.topic.clone(),
            partition: action.partition,
        }),
    )))
}
