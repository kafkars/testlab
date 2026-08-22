//! Subject validation tests pin packaged artifact provenance.

use std::collections::BTreeMap;

use crate::{SubjectArtifact, SubjectError, SubjectId, SubjectManifest};

#[test]
fn packaged_artifact_digest_is_required() {
    let mut subject = fixture();
    subject.artifacts[0].sha256 = "ABC".to_owned();

    assert!(matches!(
        subject.validate(),
        Err(SubjectError::ArtifactDigestInvalid(_))
    ));
}

#[test]
fn exact_cargo_package_is_accepted() {
    assert_eq!(fixture().validate(), Ok(()));
}

fn fixture() -> SubjectManifest {
    SubjectManifest {
        schema_version: 2,
        id: SubjectId::new("kafkars-candidate")
            .unwrap_or_else(|error| panic!("subject id: {error}")),
        display_name: "packaged Kafkars candidate".to_owned(),
        artifacts: vec![SubjectArtifact {
            name: "kafkars".to_owned(),
            version: "0.0.1".to_owned(),
            sha256: "a".repeat(64),
        }],
        command: "/tmp/testlab-kafkars-adapter".to_owned(),
        args: Vec::new(),
        environment: BTreeMap::new(),
        pass_environment: Vec::new(),
        working_directory: Some(".".to_owned()),
    }
}
