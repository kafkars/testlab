//! Share recovery requires one exact accepted batch while each broker is offline.

use testlab_schema::{
    AdapterEvent, ConsumerId, HistoryEntry, HistoryPayload, OperationId, ShareConsumedRecord,
    ShareDisposition, TerminalStatus,
};

use crate::group_recovery::verify;
use crate::group_recovery_test::{recovery_history, recovery_scenario};
use crate::index::HistoryIndex;
use crate::verify_fixture::event;

#[test]
fn three_share_acquisitions_and_certain_accepts_prove_recovery() {
    assert!(violations(&share_history()).is_empty());
}

#[test]
fn missing_uncertain_unaccepted_empty_duplicate_or_late_share_progress_fails() {
    for mutation in 0..10 {
        let mut entries = share_history();
        match mutation {
            0 => {
                entries.remove(10);
            }
            1 => {
                if let AdapterEvent::ShareAcknowledgementCompleted { success, .. } =
                    adapter(&mut entries[10])
                {
                    *success = false;
                }
            }
            2 => {
                if let AdapterEvent::ShareAcknowledgementCompleted { delivery, .. } =
                    adapter(&mut entries[10])
                {
                    *delivery = Some(TerminalStatus::PossiblySent);
                }
            }
            3 => {
                if let AdapterEvent::ShareAcknowledgementCompleted { dispositions, .. } =
                    adapter(&mut entries[10])
                {
                    *dispositions = vec![ShareDisposition::Release];
                }
            }
            4 => {
                if let AdapterEvent::ShareReceiveCompleted { records, .. } =
                    adapter(&mut entries[9])
                {
                    records.clear();
                }
            }
            5 => {
                let duplicate = entries[10].clone();
                entries.insert(11, duplicate);
            }
            6 => entries.swap(10, 11),
            7 => {
                if let AdapterEvent::ShareAcknowledgementCompleted { receive_id, .. } =
                    adapter(&mut entries[10])
                {
                    *receive_id = id("missing");
                }
            }
            8 => entries.swap(8, 9),
            _ => {
                if let AdapterEvent::ShareAcknowledgementCompleted { code, .. } =
                    adapter(&mut entries[10])
                {
                    *code = Some("transport".to_owned());
                }
            }
        }
        resequence(&mut entries);
        assert!(!violations(&entries).is_empty(), "mutation {mutation}");
    }
}

fn share_history() -> Vec<HistoryEntry> {
    let mut entries = Vec::new();
    for entry in recovery_history(true) {
        if let HistoryPayload::AdapterEvent { event: envelope } = &entry.payload
            && let AdapterEvent::GroupReceiveCompleted {
                receive_id,
                records,
                ..
            } = &envelope.event
        {
            entries.push(event(
                0,
                AdapterEvent::ShareReceiveCompleted {
                    consumer_id: ConsumerId::new("share-1")
                        .unwrap_or_else(|error| panic!("consumer: {error}")),
                    receive_id: receive_id.clone(),
                    records: records
                        .iter()
                        .cloned()
                        .map(|record| ShareConsumedRecord {
                            record,
                            delivery_count: 1,
                        })
                        .collect(),
                    acquisition_count: records.len(),
                    member_epoch: Some(1),
                    assignment_epoch: Some(1),
                },
            ));
            entries.push(event(
                0,
                AdapterEvent::ShareAcknowledgementCompleted {
                    acknowledgement_id: id(&format!("ack-{receive_id}")),
                    receive_id: receive_id.clone(),
                    dispositions: vec![ShareDisposition::Accept],
                    success: true,
                    delivery: None,
                    code: None,
                },
            ));
        } else {
            entries.push(entry);
        }
    }
    resequence(&mut entries);
    entries
}

fn adapter(entry: &mut HistoryEntry) -> &mut AdapterEvent {
    let HistoryPayload::AdapterEvent { event } = &mut entry.payload else {
        panic!("adapter fixture");
    };
    &mut event.event
}

fn resequence(entries: &mut [HistoryEntry]) {
    for (sequence, entry) in entries.iter_mut().enumerate() {
        entry.sequence = sequence as u64;
        entry.observed_unix_ms = sequence as u64;
    }
}

fn violations(entries: &[HistoryEntry]) -> Vec<testlab_schema::Violation> {
    let mut violations = Vec::new();
    verify(
        &recovery_scenario(),
        &HistoryIndex::build(entries),
        &mut violations,
    );
    violations
}

fn id(value: &str) -> OperationId {
    OperationId::new(value).unwrap_or_else(|error| panic!("operation: {error}"))
}
