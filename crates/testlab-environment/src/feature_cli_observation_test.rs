//! Tests for the feature cli observation contract.

use super::*;

const OUTPUT: &str = "\
Feature: eligible.leader.replicas.version SupportedMinVersion: 0 SupportedMaxVersion: 1 FinalizedVersionLevel: 0 Epoch: 7\n\
Feature: metadata.version SupportedMinVersion: 3.3-IV3 SupportedMaxVersion: 4.3-IV0 FinalizedVersionLevel: 4.3-IV0 Epoch: 7\n";

#[test]
fn symbolic_metadata_levels_are_mapped_and_numeric_rows_remain_exact() {
    let observed =
        normalize(9, &operation(), OUTPUT.as_bytes()).unwrap_or_else(|error| panic!("{error}"));
    let BrokerStateObservation::Features(observed) = observed else {
        panic!("feature observation kind");
    };
    assert_eq!(observed.observation, 9);
    assert_eq!(observed.finalized_features_epoch, Some(7));
    assert_eq!(observed.features.len(), 2);
    assert_eq!(observed.features[0].supported_max_version_level, 1);
    assert_eq!(observed.features[1].supported_min_version_level, 7);
    assert_eq!(observed.features[1].finalized_version_level, 30);
}

#[test]
fn pinned_metadata_map_is_contiguous_across_the_supported_matrix() {
    for (level, label) in (1_i16..).zip([
        "3.0-IV1", "3.1-IV0", "3.2-IV0", "3.3-IV0", "3.3-IV1", "3.3-IV2", "3.3-IV3", "3.4-IV0",
        "3.5-IV0", "3.5-IV1", "3.5-IV2", "3.6-IV0", "3.6-IV1", "3.6-IV2", "3.7-IV0", "3.7-IV1",
        "3.7-IV2", "3.7-IV3", "3.7-IV4", "3.8-IV0", "3.9-IV0", "4.0-IV0", "4.0-IV1", "4.0-IV2",
        "4.0-IV3", "4.1-IV0", "4.1-IV1", "4.2-IV0", "4.2-IV1", "4.3-IV0",
    ]) {
        assert_eq!(metadata_level(label), Some(level));
    }
    assert_eq!(metadata_level("4.3-IV1"), None);
}

#[test]
fn malformed_order_epoch_and_unmapped_levels_fail_closed() {
    let reversed = "\
Feature: metadata.version SupportedMinVersion: 3.3-IV3 SupportedMaxVersion: 4.3-IV0 FinalizedVersionLevel: 4.3-IV0 Epoch: 7\n\
Feature: alpha.version SupportedMinVersion: 0 SupportedMaxVersion: 1 FinalizedVersionLevel: 0 Epoch: 7\n";
    assert!(normalize(1, &operation(), reversed.as_bytes()).is_err());

    let epoch_mismatch = OUTPUT.replacen("Epoch: 7", "Epoch: 8", 1);
    assert!(normalize(1, &operation(), epoch_mismatch.as_bytes()).is_err());
    let unmapped = OUTPUT.replace("4.3-IV0", "4.3-IV1");
    assert!(normalize(1, &operation(), unmapped.as_bytes()).is_err());
}

fn operation() -> OperationId {
    OperationId::new("admin-features").unwrap_or_else(|error| panic!("operation id: {error}"))
}
