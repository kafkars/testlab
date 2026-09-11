//! ACL indexing keeps public results and independent broker facts in separate ordered maps.

use std::collections::BTreeMap;

use testlab_schema::{
    AdapterEvent, AdminAclsCreation, AdminAclsDeletion, AdminAclsDescription, BrokerAclState,
    BrokerStateObservation, OperationId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Indexed<T> {
    pub(crate) history_sequence: u64,
    pub(crate) value: T,
}

#[derive(Debug, Default)]
pub(crate) struct AdminAclIndex {
    pub(crate) created: BTreeMap<OperationId, Vec<Indexed<AdminAclsCreation>>>,
    pub(crate) described: BTreeMap<OperationId, Vec<Indexed<AdminAclsDescription>>>,
    pub(crate) deleted: BTreeMap<OperationId, Vec<Indexed<AdminAclsDeletion>>>,
    pub(crate) observed: BTreeMap<OperationId, Vec<Indexed<BrokerAclState>>>,
}

impl AdminAclIndex {
    pub(crate) fn record_event(&mut self, event: &AdapterEvent, sequence: u64) -> bool {
        let (operation_id, recorded) = match event {
            AdapterEvent::AclsCreated(value) => (
                value.operation_id.clone(),
                Recorded::Created(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            ),
            AdapterEvent::AclsDescribed(value) => (
                value.operation_id.clone(),
                Recorded::Described(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            ),
            AdapterEvent::AclsDeleted(value) => (
                value.operation_id.clone(),
                Recorded::Deleted(Indexed {
                    history_sequence: sequence,
                    value: value.clone(),
                }),
            ),
            _ => return false,
        };
        match recorded {
            Recorded::Created(value) => self.created.entry(operation_id).or_default().push(value),
            Recorded::Described(value) => {
                self.described.entry(operation_id).or_default().push(value);
            }
            Recorded::Deleted(value) => self.deleted.entry(operation_id).or_default().push(value),
        }
        true
    }

    pub(crate) fn record_state(
        &mut self,
        observation: &BrokerStateObservation,
        sequence: u64,
    ) -> bool {
        let BrokerStateObservation::Acl(value) = observation else {
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

enum Recorded {
    Created(Indexed<AdminAclsCreation>),
    Described(Indexed<AdminAclsDescription>),
    Deleted(Indexed<AdminAclsDeletion>),
}
