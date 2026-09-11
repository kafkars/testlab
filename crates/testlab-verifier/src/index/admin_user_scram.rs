//! User SCRAM indexing separates public results from independent Kafka CLI facts.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterEvent, AdminUserScramCredentialAlteration, AdminUserScramCredentialDescription,
    BrokerStateObservation, BrokerUserScramCredentialState, OperationId,
};

pub(crate) use super::admin_client_quota::Indexed;

#[derive(Debug, Default)]
pub(crate) struct AdminUserScramIndex {
    pub(crate) altered: BTreeMap<OperationId, Vec<Indexed<AdminUserScramCredentialAlteration>>>,
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminUserScramCredentialDescription>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerUserScramCredentialState>>>,
}

impl AdminUserScramIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        match event {
            AdapterEvent::UserScramCredentialAltered(value) => self
                .altered
                .entry(value.operation_id.clone())
                .or_default()
                .push(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            AdapterEvent::UserScramCredentialDescribed(value) => self
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
        let BrokerStateObservation::UserScramCredential(value) = observation else {
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
