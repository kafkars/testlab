//! Share history indexing retains acquisition, disposition, assignment, and close certainty.

use testlab_schema::{AdapterCommand, AdapterEvent, ScenarioAction};

use super::{
    HistoryIndex, IndexedShareAcknowledgement, IndexedShareClose, IndexedShareReceive, push,
};

impl HistoryIndex {
    pub(super) fn record_share_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::ShareConsumerCreated { consumer_id } => {
                push(
                    &mut self.share_consumers_created,
                    consumer_id.clone(),
                    sequence,
                );
            }
            AdapterEvent::ShareReceiveCompleted {
                consumer_id,
                receive_id,
                records,
                member_epoch,
                assignment_epoch,
            } => self
                .share_receives
                .entry(receive_id.clone())
                .or_default()
                .push(IndexedShareReceive {
                    history_sequence: sequence,
                    consumer_id: consumer_id.clone(),
                    records: records.clone(),
                    member_epoch: *member_epoch,
                    assignment_epoch: *assignment_epoch,
                }),
            AdapterEvent::ShareAcknowledgementCompleted {
                acknowledgement_id,
                receive_id,
                disposition,
                success,
                delivery,
                code,
            } => self
                .share_acknowledgements
                .entry(acknowledgement_id.clone())
                .or_default()
                .push(IndexedShareAcknowledgement {
                    history_sequence: sequence,
                    receive_id: receive_id.clone(),
                    disposition: *disposition,
                    success: *success,
                    delivery: *delivery,
                    code: code.clone(),
                }),
            AdapterEvent::ShareBatchDropped { receive_id } => {
                push(
                    &mut self.share_batches_dropped,
                    receive_id.clone(),
                    sequence,
                );
            }
            AdapterEvent::ShareConsumerClosed {
                consumer_id,
                success,
                delivery,
                code,
            } => self
                .share_consumers_closed
                .entry(consumer_id.clone())
                .or_default()
                .push(IndexedShareClose {
                    history_sequence: sequence,
                    success: *success,
                    delivery: *delivery,
                    code: code.clone(),
                }),
            _ => return false,
        }
        true
    }

    pub(super) fn record_share_command(&mut self, command: &AdapterCommand) -> bool {
        match command {
            AdapterCommand::CreateShareConsumer { consumer_id, .. } => {
                self.share_consumers_create_issued
                    .insert(consumer_id.clone());
            }
            AdapterCommand::ShareReceive { receive_id, .. } => {
                self.share_receives_issued.insert(receive_id.clone());
            }
            AdapterCommand::ShareAcknowledge {
                acknowledgement_id, ..
            } => {
                self.share_acknowledgements_issued
                    .insert(acknowledgement_id.clone());
            }
            AdapterCommand::DropShareBatch { receive_id, .. } => {
                self.share_batches_drop_issued.insert(receive_id.clone());
            }
            AdapterCommand::CloseShareConsumer { consumer_id } => {
                self.share_consumers_close_issued
                    .insert(consumer_id.clone());
            }
            _ => return false,
        }
        true
    }

    pub(super) fn share_action_issued(&self, action: &ScenarioAction) -> Option<bool> {
        Some(match action {
            ScenarioAction::CreateShareConsumer { consumer_id, .. } => {
                self.share_consumers_create_issued.contains(consumer_id)
            }
            ScenarioAction::ShareReceive { receive_id, .. } => {
                self.share_receives_issued.contains(receive_id)
            }
            ScenarioAction::ShareAcknowledge {
                acknowledgement_id, ..
            } => self
                .share_acknowledgements_issued
                .contains(acknowledgement_id),
            ScenarioAction::DropShareBatch { receive_id, .. } => {
                self.share_batches_drop_issued.contains(receive_id)
            }
            ScenarioAction::CloseShareConsumer { consumer_id, .. } => {
                self.share_consumers_close_issued.contains(consumer_id)
            }
            _ => return None,
        })
    }
}
