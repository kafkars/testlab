//! User SCRAM adapter tests pin mechanisms and strict public result selection.

use testlab_schema::{OperationId, ScramCredentialMechanism};

use crate::kafkars_api::ScramMechanism;
use crate::protocol_admin_user_scram::{described_credential, public_mechanism};

#[test]
fn portable_mechanisms_map_to_exact_public_constants() {
    assert_eq!(
        public_mechanism(ScramCredentialMechanism::Sha256),
        ScramMechanism::SHA_256
    );
    assert_eq!(
        public_mechanism(ScramCredentialMechanism::Sha512),
        ScramMechanism::SHA_512
    );
}

#[test]
fn missing_or_foreign_public_users_are_rejected() {
    assert!(described_credential(Vec::new(), &operation(), "testlab-user", mechanism()).is_err());
    assert!(
        described_credential(
            vec![("other-user".to_owned(), Ok(Vec::new()))],
            &operation(),
            "testlab-user",
            mechanism(),
        )
        .is_err()
    );
}

fn mechanism() -> ScramCredentialMechanism {
    ScramCredentialMechanism::Sha256
}

fn operation() -> OperationId {
    OperationId::new("user-scram").unwrap_or_else(|error| panic!("operation: {error}"))
}
