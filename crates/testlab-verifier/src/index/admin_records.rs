//! Record-deletion indexing retains every public outcome at its shared event position.

use testlab_schema::AdapterEvent;

use super::{HistoryIndex, IndexedRecordsDeleted};

pub(super) fn record(index: &mut HistoryIndex, event: &AdapterEvent, sequence: u64) -> bool {
    match event {
        AdapterEvent::RecordsDeleted(value) => index
            .records_deleted
            .entry(value.operation_id.clone())
            .or_default()
            .push(IndexedRecordsDeleted {
                history_sequence: sequence,
                topic: value.topic.clone(),
                partition: value.partition,
                low_watermark: Some(value.low_watermark),
                error_code: None,
            }),
        AdapterEvent::RecordsBatchDeleted(value) => index
            .records_deleted
            .entry(value.operation_id.clone())
            .or_default()
            .extend(value.outcomes.iter().map(|outcome| IndexedRecordsDeleted {
                history_sequence: sequence,
                topic: outcome.topic.clone(),
                partition: outcome.partition,
                low_watermark: outcome.low_watermark,
                error_code: outcome.error_code.clone(),
            })),
        _ => return false,
    }
    true
}
