//! Direct-consumer relative-position translation tests.

use testlab_schema::{AssignedPartitionPosition, AssignedStartPosition};

use crate::assigned_consumer_positions::{offset_spec, positioned_partition, start_position};
use crate::kafkars_api::{OffsetSpec, StartPosition};

#[test]
fn relative_positions_select_matching_list_offsets_queries() {
    assert_eq!(
        offset_spec(AssignedStartPosition::Beginning),
        Some(OffsetSpec::earliest())
    );
    assert_eq!(
        offset_spec(AssignedStartPosition::End),
        Some(OffsetSpec::latest())
    );
    assert_eq!(
        offset_spec(AssignedStartPosition::Offset { offset: 7 }),
        None
    );
}

#[test]
fn resolved_offsets_remain_exact_at_public_assignment() {
    let entry = AssignedPartitionPosition {
        topic: "orders".to_owned(),
        partition: 2,
        position: AssignedStartPosition::Offset { offset: 11 },
    };
    let partition = positioned_partition(&entry);
    assert_eq!(partition.topic(), "orders");
    assert_eq!(partition.partition(), 2);
    assert_eq!(partition.start_position(), Some(StartPosition::Offset(11)));
    assert_eq!(
        start_position(AssignedStartPosition::End),
        StartPosition::End
    );
}
