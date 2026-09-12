//! Record-producing actions contribute externally provisioned topic topology.

use std::collections::{BTreeMap, BTreeSet};

use testlab_schema::{RecordSpec, ScenarioAction};

use crate::compose_provision_targets::require_topic;

pub(super) fn record(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    action: &ScenarioAction,
) {
    match action {
        ScenarioAction::Send {
            partitioning,
            record,
            ..
        } => {
            let partitions = partitioning
                .partition_count()
                .and_then(|count| i32::try_from(count).ok())
                .unwrap_or_else(|| record.partition.saturating_add(1));
            require_topic(topics, subject_created, &record.topic, partitions);
        }
        ScenarioAction::SendBatch { operations, .. }
        | ScenarioAction::ExecuteTransaction { operations, .. } => {
            for operation in operations {
                record_topic(topics, subject_created, &operation.record);
            }
        }
        ScenarioAction::ExecuteTransactionalTransform(action) => {
            for operation in &action.operations {
                record_topic(topics, subject_created, &operation.record);
            }
        }
        ScenarioAction::FenceTransaction { operation, .. } => {
            record_topic(topics, subject_created, &operation.record);
        }
        ScenarioAction::TransferAssignedRecord(action) => require_topic(
            topics,
            subject_created,
            &action.target_topic,
            action.target_partition.saturating_add(1),
        ),
        ScenarioAction::StartConcurrentActors(action) => {
            for actor in &action.actors {
                if let testlab_schema::ConcurrentActor::ProducerSend { record, .. } = actor {
                    record_topic(topics, subject_created, record);
                }
            }
        }
        _ => {}
    }
}

fn record_topic(
    topics: &mut BTreeMap<String, i32>,
    subject_created: &BTreeSet<String>,
    record: &RecordSpec,
) {
    require_topic(
        topics,
        subject_created,
        &record.topic,
        record.partition.saturating_add(1),
    );
}
