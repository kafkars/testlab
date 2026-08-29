//! Share protocol tests require exact batch and acknowledgement identities.

use testlab_schema::{AdapterEvent, ConsumerId, OperationId, ShareDisposition, TerminalStatus};

use crate::runner_protocol::{EventDisposition, ExpectedEvent};

#[test]
fn share_events_complete_only_the_exact_expected_identity() {
    let receive = id(OperationId::new("share-receive-1"));
    let acknowledgement = id(OperationId::new("share-ack-1"));
    let consumer = id(ConsumerId::new("share-consumer-1"));

    assert_eq!(
        ExpectedEvent::ShareReceiveCompleted(receive.clone())
            .classify(&AdapterEvent::ShareReceiveCompleted {
                consumer_id: consumer.clone(),
                receive_id: receive,
                records: Vec::new(),
                acquisition_count: 0,
                member_epoch: Some(1),
                assignment_epoch: Some(1),
            })
            .unwrap_or_else(|error| panic!("share receive event: {error}")),
        EventDisposition::Complete
    );
    assert_eq!(
        ExpectedEvent::ShareAcknowledgementCompleted(acknowledgement.clone())
            .classify(&AdapterEvent::ShareAcknowledgementCompleted {
                acknowledgement_id: acknowledgement,
                receive_id: id(OperationId::new("share-receive-1")),
                dispositions: vec![ShareDisposition::Release],
                success: false,
                delivery: Some(TerminalStatus::PossiblySent),
                code: Some("transport".to_owned()),
            })
            .unwrap_or_else(|error| panic!("share acknowledgement event: {error}")),
        EventDisposition::Complete
    );
    assert!(
        ExpectedEvent::ShareConsumerClosed(consumer)
            .classify(&AdapterEvent::ShareBatchDropped {
                receive_id: id(OperationId::new("share-receive-2")),
            })
            .is_err()
    );
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
