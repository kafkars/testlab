//! Adapter descriptor reports the exact packaged public surface and protocol.

use std::collections::BTreeSet;

use testlab_schema::{AdapterDescriptor, AdapterId, Capability, PROTOCOL_VERSION};

use crate::AdapterError;

pub(crate) fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    let capabilities = BTreeSet::from([
        Capability::Producer,
        Capability::ProducerCancellation,
        Capability::ProducerConfiguration,
        Capability::ProducerBatch,
        Capability::ConcurrentActors,
        Capability::Lifecycle,
        Capability::ClientReadiness,
        Capability::ClientMetrics,
        Capability::AssignedConsumer,
        Capability::AssignedConsumerControls,
        Capability::ConsumerGroups,
        Capability::ConsumerProtocolGroups,
        Capability::GroupConsumerControls,
        Capability::GroupConsumerConfiguration,
        Capability::GroupConsumerShutdown,
        Capability::Admin,
        Capability::Transactions,
    ]);
    #[cfg(kafkars_share_candidate)]
    let capabilities = {
        let mut capabilities = capabilities;
        capabilities.insert(Capability::ShareConsumer);
        capabilities.insert(Capability::ShareConsumerConfiguration);
        capabilities
    };
    Ok(AdapterDescriptor {
        id: AdapterId::new("kafkars-rust")?,
        implementation: "packaged kafkars Rust client".to_owned(),
        version: "0.0.2-rc.1".to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities,
    })
}
