//! Batched topic creation maps one exact wire call to ordered metadata targets.

use testlab_schema::{
    AdapterCommand, CreateTopicBatchCommandItem, CreateTopicsBatchCommand, ScenarioAction,
};

use crate::observer_admin_target::{AdminTarget, ListTarget, TargetMatch, unique};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::CreateTopicsBatch(action) = action else {
        return Ok(None);
    };
    let names = action
        .topics
        .iter()
        .map(|item| item.topic.clone())
        .collect::<Vec<_>>();
    unique(&names, &action.operation_id, "topics")?;
    let topics = action
        .topics
        .iter()
        .map(|item| CreateTopicBatchCommandItem {
            topic: item.topic.clone(),
            partitions: item.partitions,
            replication_factor: item.replication_factor,
        })
        .collect();
    Ok(Some((
        AdapterCommand::CreateTopicsBatch(CreateTopicsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            topics,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::Topics(ListTarget {
            operation_id: action.operation_id.clone(),
            names,
        }),
    )))
}
