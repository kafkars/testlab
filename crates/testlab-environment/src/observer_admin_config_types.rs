//! Topic-configuration observer targets retain scenario-only expected values.

use testlab_schema::OperationId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ConfigTarget {
    pub(super) operation_id: OperationId,
    pub(super) topic: String,
    pub(super) config_name: String,
    pub(super) expected_value: String,
    pub(super) poll_expected: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ConfigBatchTarget {
    pub(super) operation_id: OperationId,
    pub(super) configs: Vec<ConfigTarget>,
}
