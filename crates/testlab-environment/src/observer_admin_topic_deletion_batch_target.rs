//! Plural topic deletion maps exact expectation-free commands to ordered absence targets.

use testlab_schema::{AdapterCommand, DeleteTopicsCommand, ScenarioAction};

use crate::observer_admin_target::{AdminTarget, ListTarget, TargetMatch, unique};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::DeleteTopics(action) = action else {
        return Ok(None);
    };
    let topics = action
        .topics
        .iter()
        .map(|topic| topic.topic.clone())
        .collect::<Vec<_>>();
    unique(&topics, &action.operation_id, "topics")?;
    Ok(Some((
        AdapterCommand::DeleteTopics(DeleteTopicsCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            selection: action.selection,
            topics: topics.clone(),
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::TopicDeletions(ListTarget {
            operation_id: action.operation_id.clone(),
            names: topics,
        }),
    )))
}
