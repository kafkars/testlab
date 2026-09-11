//! Admin offset targets share exact resource and expected-state selections.

use testlab_schema::OperationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetTarget {
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) expected_offset: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetsSelectionTarget {
    pub(super) group_id: String,
    pub(super) offsets: Vec<GroupOffsetTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) group_id: String,
    pub(super) offsets: Vec<GroupOffsetTarget>,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct GroupsOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) groups: Vec<GroupOffsetsSelectionTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) partition: i32,
    pub(super) expected_low: Option<i64>,
    pub(super) expected_high: Option<i64>,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PartitionOffsetsBatchTarget {
    pub(super) operation_id: OperationId,
    pub(super) offsets: Vec<PartitionOffsetsTarget>,
}
