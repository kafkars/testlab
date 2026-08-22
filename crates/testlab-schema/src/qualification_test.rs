//! Qualification manifest validation evidence.

use super::{
    CellId, QualificationCell, QualificationError, QualificationId, QualificationManifest,
};

#[test]
fn complete_qualification_round_trips_toml() {
    let expected = qualification();
    let encoded = toml::to_string(&expected)
        .unwrap_or_else(|error| panic!("serialize qualification fixture: {error}"));
    let actual = toml::from_str::<QualificationManifest>(&encoded)
        .unwrap_or_else(|error| panic!("parse qualification fixture: {error}"));

    assert_eq!(actual, expected);
    assert!(actual.validate().is_ok());
}

#[test]
fn duplicate_environment_pack_pairing_is_rejected() {
    let mut manifest = qualification();
    let mut duplicate = manifest.cells[0].clone();
    duplicate.id = cell_id("duplicate-cell");
    manifest.cells.push(duplicate);

    assert!(matches!(
        manifest.validate(),
        Err(QualificationError::DuplicatePairing { .. })
    ));
}

#[test]
fn environment_path_must_reference_cluster_catalog() {
    let mut manifest = qualification();
    manifest.cells[0].environment = "subjects/not-an-environment.toml".to_owned();

    assert!(matches!(
        manifest.validate(),
        Err(QualificationError::CatalogPathInvalid { root, .. }) if root == "clusters"
    ));
}

#[test]
fn qualification_requires_a_gating_cell() {
    let mut manifest = qualification();
    manifest.cells[0].gating = false;

    assert_eq!(manifest.validate(), Err(QualificationError::NoGatingCells));
}

#[test]
fn qualification_rejects_zero_attempts() {
    let mut manifest = qualification();
    manifest.cells[0].attempts = 0;

    assert!(matches!(
        manifest.validate(),
        Err(QualificationError::AttemptsOutOfRange { attempts: 0, .. })
    ));
}

fn qualification() -> QualificationManifest {
    QualificationManifest {
        schema_version: 2,
        id: QualificationId::new("kafkars-pr")
            .unwrap_or_else(|error| panic!("fixture qualification id: {error}")),
        title: "Kafkars pull-request qualification".to_owned(),
        cells: vec![QualificationCell {
            id: cell_id("kafka-4.3.1-plaintext"),
            environment: "clusters/apache-kafka/4.3.1/three-plaintext.toml".to_owned(),
            pack: "packs/kafka-full.toml".to_owned(),
            attempts: 2,
            gating: true,
        }],
    }
}

fn cell_id(value: &str) -> CellId {
    CellId::new(value).unwrap_or_else(|error| panic!("fixture cell id: {error}"))
}
