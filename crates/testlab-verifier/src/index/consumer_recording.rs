//! Consumer event recording keeps ownership evidence separate from generic history flow.

use testlab_schema::AdapterEvent;

use super::{
    HistoryIndex, IndexedAssignedConsumerControl, IndexedGroupAssignments,
    IndexedGroupConsumerControl, IndexedGroupReceiveSet, IndexedReceive, push,
};

impl HistoryIndex {
    pub(super) fn record_consumer_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::AssignedConsumerCreated { consumer_id } => {
                push(&mut self.consumers_created, consumer_id.clone(), sequence);
            }
            AdapterEvent::AssignmentCompleted { consumer_id } => {
                push(&mut self.assignments, consumer_id.clone(), sequence);
            }
            AdapterEvent::AssignedConsumerControlCompleted(completion) => self
                .assigned_controls
                .entry(completion.operation_id.clone())
                .or_default()
                .push(IndexedAssignedConsumerControl {
                    history_sequence: sequence,
                    completion: completion.clone(),
                }),
            AdapterEvent::ReceiveCompleted {
                receive_id,
                records,
            } => self.record_receive(receive_id, records, None, None, sequence),
            AdapterEvent::AssignedConsumerClosed { consumer_id } => {
                push(&mut self.consumers_closed, consumer_id.clone(), sequence);
            }
            AdapterEvent::GroupConsumerCreated { consumer_id } => push(
                &mut self.group_consumers_created,
                consumer_id.clone(),
                sequence,
            ),
            AdapterEvent::GroupReceiveCompleted {
                receive_id,
                records,
                committed,
                group_epoch,
            } => self.record_receive(
                receive_id,
                records,
                Some(*committed),
                *group_epoch,
                sequence,
            ),
            AdapterEvent::GroupAssignmentsObserved(observation) => self
                .group_assignments
                .entry(observation.operation_id.clone())
                .or_default()
                .push(IndexedGroupAssignments {
                    history_sequence: sequence,
                    observation: observation.clone(),
                }),
            AdapterEvent::GroupReceiveSetCompleted(completion) => self
                .group_receive_sets
                .entry(completion.receive_id.clone())
                .or_default()
                .push(IndexedGroupReceiveSet {
                    history_sequence: sequence,
                    completion: completion.clone(),
                }),
            AdapterEvent::GroupConsumerControlCompleted(completion) => self
                .group_controls
                .entry(completion.operation_id.clone())
                .or_default()
                .push(IndexedGroupConsumerControl {
                    history_sequence: sequence,
                    completion: completion.clone(),
                }),
            AdapterEvent::GroupConsumerShutdownCompleted(_) => {}
            AdapterEvent::GroupConsumerClosed { consumer_id } => push(
                &mut self.group_consumers_closed,
                consumer_id.clone(),
                sequence,
            ),
            _ => return false,
        }
        true
    }

    fn record_receive(
        &mut self,
        receive_id: &testlab_schema::OperationId,
        records: &[testlab_schema::ConsumedRecord],
        committed: Option<bool>,
        group_epoch: Option<testlab_schema::GroupMembershipEpoch>,
        sequence: u64,
    ) {
        self.receives
            .entry(receive_id.clone())
            .or_default()
            .push(IndexedReceive {
                history_sequence: sequence,
                records: records.to_vec(),
                committed,
                group_epoch,
            });
    }
}
