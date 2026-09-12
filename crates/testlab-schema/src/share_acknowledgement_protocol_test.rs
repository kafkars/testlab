//! Share acknowledgement protocol tests retain the exact public conversion.

use crate::{
    AdapterCommand, ConsumerId, OperationId, ShareAcknowledgementMethod, ShareDisposition,
};

#[test]
fn accept_all_round_trips_across_the_adapter_boundary() {
    let command = AdapterCommand::ShareAcknowledge {
        consumer_id: id(ConsumerId::new("share-1")),
        receive_id: id(OperationId::new("receive-1")),
        acknowledgement_id: id(OperationId::new("ack-1")),
        method: ShareAcknowledgementMethod::AcceptAll,
        dispositions: vec![ShareDisposition::Accept, ShareDisposition::Accept],
        timeout_ms: 30_000,
    };

    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode share acknowledgement: {error}"));
    let decoded: AdapterCommand = serde_json::from_str(&encoded)
        .unwrap_or_else(|error| panic!("decode share acknowledgement: {error}"));

    assert_eq!(decoded, command);
    assert!(encoded.contains("\"method\":\"accept_all\""));
}

fn id<T, E>(result: Result<T, E>) -> T
where
    E: std::fmt::Display,
{
    result.unwrap_or_else(|error| panic!("fixture id: {error}"))
}
