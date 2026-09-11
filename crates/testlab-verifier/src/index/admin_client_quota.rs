//! Client-quota indexing separates public results from independent Kafka CLI facts.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterEvent, AdminClientQuotaAlteration, AdminClientQuotaDescription, BrokerClientQuotaState,
    BrokerStateObservation, OperationId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Indexed<T> {
    pub(crate) history_sequence: u64,
    pub(crate) value: T,
}

#[derive(Debug, Default)]
pub(crate) struct AdminClientQuotaIndex {
    pub(crate) altered: BTreeMap<OperationId, Vec<Indexed<AdminClientQuotaAlteration>>>,
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminClientQuotaDescription>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerClientQuotaState>>>,
}

impl AdminClientQuotaIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::ClientQuotaAltered(value) => self
                .altered
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::ClientQuotaDescribed(value) => self
                .described
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
        let BrokerStateObservation::ClientQuota(value) = observation else {
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
