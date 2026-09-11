//! Batched record-deletion targets pin independent ordered post-call watermarks.

use std::collections::BTreeSet;

use testlab_schema::{AdapterCommand, Scenario, ScenarioAction};

use crate::compose_provision_targets::{SeedTarget, seed_targets, topics};
use crate::observer_admin_target::AdminTarget;

#[test]
fn batch_maps_to_exact_wire_command_and_ordered_watermark_targets() {
    let scenario = scenario();
    let action = scenario
        .steps
        .iter()
        .find_map(|step| match &step.action {
            ScenarioAction::DeleteRecordsBatch(action) => Some(action),
            _ => None,
        })
        .unwrap_or_else(|| panic!("batched record-deletion action"));
    let scenario_action = ScenarioAction::DeleteRecordsBatch(action.clone());
    let (command, expected) =
        crate::observer_admin_partition_offsets_target::match_action(&scenario_action)
            .unwrap_or_else(|| panic!("batched record-deletion target"));
    let AdapterCommand::DeleteRecordsBatch(wire) = &command else {
        panic!("batched record-deletion command");
    };
    assert_eq!(wire.targets[0].partition, 1);
    assert_eq!(wire.targets[1].partition, 0);
    assert!(AdminTarget::from_exact(&scenario_action, &command).is_ok());
    let AdminTarget::PartitionOffsetsBatch(expected) = expected else {
        panic!("batched partition-offset target");
    };
    assert_eq!(expected.offsets.len(), 2);
    assert_eq!(expected.offsets[0].partition, 1);
    assert_eq!(expected.offsets[0].expected_low, Some(2));
    assert_eq!(expected.offsets[0].expected_high, Some(3));
    assert_eq!(expected.offsets[1].partition, 0);
    assert_eq!(expected.offsets[1].expected_low, Some(2));
    assert_eq!(expected.offsets[1].expected_high, Some(2));
    assert!(expected.offsets.iter().all(|target| target.poll_expected));
}

#[test]
fn provisioning_seeds_every_target_and_sizes_the_topic() {
    let scenario = scenario();
    assert_eq!(
        seed_targets(&scenario),
        BTreeSet::from([
            SeedTarget {
                topic: "testlab-kafkars-admin-delete-records-batch".to_owned(),
                partition: 0,
                record_count: 2,
            },
            SeedTarget {
                topic: "testlab-kafkars-admin-delete-records-batch".to_owned(),
                partition: 1,
                record_count: 3,
            },
        ])
    );
    assert_eq!(
        topics(&scenario).get("testlab-kafkars-admin-delete-records-batch"),
        Some(&2)
    );
}

fn scenario() -> Scenario {
    toml::from_str(include_str!(
        "../../../scenarios/kafka/admin-delete-records-batch.toml"
    ))
    .unwrap_or_else(|error| panic!("parse batched record-deletion scenario: {error}"))
}
