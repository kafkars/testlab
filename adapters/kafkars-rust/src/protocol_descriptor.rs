//! Adapter descriptor reports the exact packaged public surface and protocol.

use std::collections::BTreeSet;

use testlab_schema::{AdapterDescriptor, AdapterId, Capability, PROTOCOL_VERSION};

use crate::AdapterError;

pub(crate) fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    let capabilities = BTreeSet::from([
        Capability::Producer,
        Capability::ProducerWaitingSend,
        Capability::ProducerCancellation,
        Capability::ProducerConfiguration,
        Capability::ProducerBatch,
        Capability::ConcurrentActors,
        Capability::Lifecycle,
        Capability::ClientReadiness,
        Capability::ExpectedClusterIdentity,
        Capability::ClientMetrics,
        Capability::AssignedConsumer,
        Capability::AssignedConsumerImmediateBatch,
        Capability::AssignedConsumerEvents,
        Capability::AssignedConsumerConfiguration,
        Capability::AssignedConsumerControls,
        Capability::ConsumerGroups,
        Capability::ConsumerProtocolGroups,
        Capability::GroupConsumerImmediateBatch,
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
    #[cfg(kafkars_independent_handles_candidate)]
    let capabilities = {
        let mut capabilities = capabilities;
        capabilities.insert(Capability::IndependentHandles);
        capabilities
    };
    #[cfg(any(kafkars_independent_handles_candidate, kafkars_share_candidate))]
    let version = env!("CARGO_PKG_VERSION");
    #[cfg(not(any(kafkars_independent_handles_candidate, kafkars_share_candidate)))]
    let version = "0.0.2-rc.1";
    Ok(AdapterDescriptor {
        id: AdapterId::new("kafkars-rust")?,
        implementation: "packaged kafkars Rust client".to_owned(),
        version: version.to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities,
    })
}
