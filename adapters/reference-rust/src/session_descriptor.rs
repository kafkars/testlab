//! Reference descriptor exposes only the model-backed public capabilities.

use std::collections::BTreeSet;

use testlab_schema::{AdapterDescriptor, AdapterId, Capability, PROTOCOL_VERSION};

use crate::AdapterError;

pub(crate) fn descriptor() -> Result<AdapterDescriptor, AdapterError> {
    Ok(AdapterDescriptor {
        id: AdapterId::new("reference-rust")?,
        implementation: "testlab reference Rust adapter".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol_version: PROTOCOL_VERSION,
        capabilities: BTreeSet::from([
            Capability::Producer,
            Capability::ProducerBatch,
            Capability::Lifecycle,
            Capability::ClientReadiness,
            Capability::ModelBroker,
        ]),
    })
}
