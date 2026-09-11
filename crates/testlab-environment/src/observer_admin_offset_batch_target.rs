//! Batch offset actions become exact ordered independent watermark targets.

use testlab_schema::{
    AdapterCommand, ListOffsetsBatchCommand, OffsetListingSelection, ScenarioAction,
};

use crate::observer_admin_target::{
    AdminTarget, PartitionOffsetsBatchTarget, PartitionOffsetsTarget, TargetMatch, unique,
};
use crate::observer_error::ObserverError;

pub(super) fn match_action(action: &ScenarioAction) -> Result<Option<TargetMatch>, ObserverError> {
    let ScenarioAction::ListOffsetsBatch(action) = action else {
        return Ok(None);
    };
    let identities = action
        .queries
        .iter()
        .map(|query| (query.topic.clone(), query.partition))
        .collect::<Vec<_>>();
    unique(
        &identities,
        &action.operation_id,
        "batch offset topic-partitions",
    )?;
    let queries = action
        .queries
        .iter()
        .map(|query| OffsetListingSelection {
            topic: query.topic.clone(),
            partition: query.partition,
            position: query.position,
        })
        .collect();
    let offsets = action
        .queries
        .iter()
        .map(|query| PartitionOffsetsTarget {
            operation_id: action.operation_id.clone(),
            topic: query.topic.clone(),
            partition: query.partition,
            expected_low: (query.position == testlab_schema::AdminOffsetPosition::Earliest)
                .then_some(query.expected_offset),
            expected_high: (query.position == testlab_schema::AdminOffsetPosition::Latest)
                .then_some(query.expected_offset),
            poll_expected: false,
        })
        .collect();
    Ok(Some((
        AdapterCommand::ListOffsetsBatch(ListOffsetsBatchCommand {
            client_id: action.client_id.clone(),
            operation_id: action.operation_id.clone(),
            queries,
            timeout_ms: action.timeout_ms,
        }),
        AdminTarget::PartitionOffsetsBatch(PartitionOffsetsBatchTarget {
            operation_id: action.operation_id.clone(),
            offsets,
        }),
    )))
}
