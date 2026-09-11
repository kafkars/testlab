//! Share-group indexing separates public results from Kafka CLI state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterEvent, AdminShareGroupDescription, AdminShareGroupOffsetAlteration,
    AdminShareGroupOffsetListing, BrokerShareGroupOffset, BrokerShareGroupState,
    BrokerStateObservation, OperationId,
};

pub(crate) use super::admin_client_quota::Indexed;

#[derive(Debug, Default)]
pub(crate) struct AdminShareGroupIndex {
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminShareGroupDescription>>>,
    pub(crate) offsets_listed: BTreeMap<OperationId, Vec<Indexed<AdminShareGroupOffsetListing>>>,
    pub(crate) offsets_altered:
        BTreeMap<OperationId, Vec<Indexed<AdminShareGroupOffsetAlteration>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerShareGroupState>>>,
    pub(crate) offsets_observed: BTreeMap<OperationId, Vec<Indexed<BrokerShareGroupOffset>>>,
}

impl AdminShareGroupIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::ShareGroupDescribed(value) => self
                .described
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::ShareGroupOffsetsListed(value) => self
                .offsets_listed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::ShareGroupOffsetsAltered(value) => self
                .offsets_altered
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            _ => return false,
        }
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        match observation {
            BrokerStateObservation::ShareGroup(value) => self
                .observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            BrokerStateObservation::ShareGroupOffset(value) => self
                .offsets_observed
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            _ => return false,
        }
        true
    }
}
