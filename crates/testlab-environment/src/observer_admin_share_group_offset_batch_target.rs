//! Plural Share offset targets retain nested caller order and expected broker facts.

use testlab_schema::OperationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupOffsetSelectionTarget {
    pub(super) topic: String,
    pub(super) partition: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupOffsetsSelectionTarget {
    pub(super) group_id: String,
    pub(super) offsets: Vec<ShareGroupOffsetSelectionTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ShareGroupsOffsetsTarget {
    pub(super) operation_id: OperationId,
    pub(super) groups: Vec<ShareGroupOffsetsSelectionTarget>,
}
