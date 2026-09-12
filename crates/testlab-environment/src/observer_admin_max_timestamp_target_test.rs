//! Max-timestamp admin targets retain exact commands and independent bounds.

use testlab_schema::{
    AdminOffsetSelector, ClientId, ListOffsetsAction, OperationId, ScenarioAction,
};

use crate::observer_admin_target::AdminTarget;

#[test]
fn max_timestamp_maps_to_an_exact_command_and_immediate_watermarks() {
    let action = ScenarioAction::ListOffsets(ListOffsetsAction {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}")),
        operation_id: OperationId::new("max-timestamp-offset")
            .unwrap_or_else(|error| panic!("operation id: {error}")),
        topic: "orders".to_owned(),
        partition: 0,
        position: AdminOffsetSelector::MaxTimestamp,
        timestamp_millis: None,
        expected_offset: Some(0),
        expected_error_code: None,
        timeout_ms: 500,
    });
    let (command, target) = crate::observer_admin_partition_offsets_target::match_action(&action)
        .unwrap_or_else(|| panic!("missing max-timestamp target"));
    let AdminTarget::PartitionOffsets(target) = target else {
        panic!("max-timestamp target kind");
    };

    assert!(AdminTarget::from_exact(&action, &command).is_ok());
    assert_eq!(target.expected_low, None);
    assert_eq!(target.expected_high, None);
    assert!(!target.poll_expected);
}
