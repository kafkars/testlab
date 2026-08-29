//! Scenario action state owns live handles and stable operation identities.

use std::collections::{BTreeMap, BTreeSet};

use crate::consumer_action_validation::ConsumerStates;
use crate::transaction_action_validation::{TransactionSends, TransactionStates};
use crate::{ActorId, ClientId, ConcurrencyId, OperationId, ProducerId};

pub(crate) type ClientStates = BTreeMap<ClientId, bool>;
pub(crate) type ProducerStates = BTreeMap<ProducerId, (ClientId, bool)>;

#[derive(Default)]
pub(crate) struct ActionStates {
    pub(crate) clients: ClientStates,
    pub(crate) producers: ProducerStates,
    pub(crate) consumers: ConsumerStates,
    pub(crate) transactions: TransactionStates,
    pub(crate) operation_ids: BTreeSet<OperationId>,
    pub(crate) actor_ids: BTreeSet<ActorId>,
    pub(crate) concurrency_ids: BTreeSet<ConcurrencyId>,
    pub(crate) active_concurrency: Option<ConcurrencyId>,
    pub(crate) sends: BTreeSet<OperationId>,
    pub(crate) transaction_sends: TransactionSends,
    pub(crate) share_batches: crate::share_action_validation::ShareBatchStates,
    pub(crate) role_disruptions: BTreeSet<crate::BrokerRoleTarget>,
    pub(crate) stopped_brokers: BTreeSet<u16>,
    pub(crate) environment_operations: BTreeSet<crate::EnvironmentOperationId>,
    pub(crate) broker_policies: BTreeSet<crate::BrokerPolicy>,
    pub(crate) network_faults: BTreeMap<u16, crate::NetworkFault>,
}
