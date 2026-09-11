//! Share-group indexing separates the public description from Kafka CLI state.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterEvent, AdminShareGroupDescription, BrokerShareGroupState, BrokerStateObservation,
    OperationId,
};

pub(crate) use super::admin_client_quota::Indexed;

#[derive(Debug, Default)]
pub(crate) struct AdminShareGroupIndex {
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminShareGroupDescription>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerShareGroupState>>>,
}

impl AdminShareGroupIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let AdapterEvent::ShareGroupDescribed(value) = event else {
            return false;
        };
        self.described
            .entry(value.operation_id.clone())
            .or_default()
            .push(Indexed {
                history_sequence: sequence,
                value: value.clone(),
            });
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        let BrokerStateObservation::ShareGroup(value) = observation else {
            return false;
        };
        self.observed
            .entry(value.operation_id.clone())
            .or_default()
            .push(Indexed {
                history_sequence: sequence,
                value: value.clone(),
            });
        true
    }
}
