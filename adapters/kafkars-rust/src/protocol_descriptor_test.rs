//! Candidate descriptor tests pin packaged-client version identity.

use crate::protocol_descriptor;
use testlab_schema::Capability;

#[test]
fn descriptor_reports_the_packaged_client_version() {
    let descriptor = protocol_descriptor::descriptor()
        .unwrap_or_else(|error| panic!("descriptor should be valid: {error}"));

    assert_eq!(descriptor.version, "0.0.2-rc.1");
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::ExpectedClusterIdentity)
    );
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::ProducerWaitingSend)
    );
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::AssignedConsumerImmediateBatch)
    );
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::AssignedConsumerEvents)
    );
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::GroupConsumerImmediateBatch)
    );
    assert!(
        descriptor
            .capabilities
            .contains(&Capability::TransactionBatchSend)
    );
}
