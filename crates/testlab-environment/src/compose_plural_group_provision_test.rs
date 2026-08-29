//! Plural group-admin provisioning includes every selected topic-partition.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn every_plural_group_offset_selection_contributes_its_partition() {
    let scenario: Scenario = toml::from_str(
        r#"
schema_version = 37
id = "admin.plural-group-provisioning"
title = "plural group provisioning"
description = "plural group provisioning fixture"
timeout_ms = 1000
requires = ["admin", "lifecycle"]
assertions = []

[[steps]]
id = "list-one-group"
kind = "list_consumer_group_offsets_batch"
client_id = "client-1"
operation_id = "list-one-group"
group_id = "group-a"
require_stable = true
partitions = [
  { topic = "orders", partition = 2, expected_offset = 4 },
  { topic = "audit", partition = 0, expected_offset = 1 },
]
timeout_ms = 500

[[steps]]
id = "list-many-groups"
kind = "list_consumer_groups_offsets"
client_id = "client-1"
operation_id = "list-many-groups"
require_stable = false
groups = [
  { group_id = "group-a", partitions = [
    { topic = "metrics", partition = 3, expected_offset = 2 },
  ] },
  { group_id = "group-b", partitions = [
    { topic = "orders", partition = 4, expected_offset = 6 },
  ] },
]
timeout_ms = 500

[[steps]]
id = "alter-many"
kind = "alter_consumer_group_offsets"
client_id = "client-1"
operation_id = "alter-many"
group_id = "group-a"
offsets = [
  { topic = "audit", partition = 2, offset = 7 },
]
timeout_ms = 500

[[steps]]
id = "delete-many"
kind = "delete_consumer_group_offsets"
client_id = "client-1"
operation_id = "delete-many"
group_id = "group-a"
partitions = [
  { topic = "archive", partition = 5 },
]
timeout_ms = 500
"#,
    )
    .unwrap_or_else(|error| panic!("parse scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([
            ("archive".to_owned(), 6),
            ("audit".to_owned(), 3),
            ("metrics".to_owned(), 4),
            ("orders".to_owned(), 5),
        ])
    );
}
