//! Adversary frame tests cover fixed request identity and valid response framing.

use testlab_schema::KafkaApi;

use crate::adversary_frame::{RequestIdentity, parse_request, response};

#[test]
fn request_parser_preserves_supported_fixed_header_identity() {
    let frame = [0, 3, 0, 8, 0, 0, 0, 42];
    assert_eq!(
        parse_request(&frame),
        Ok(RequestIdentity {
            api: KafkaApi::Metadata,
            version: 8,
            correlation_id: 42,
        })
    );
    assert!(parse_request(&frame[..7]).is_err());
    let unsupported = [0, 61, 0, 0, 0, 0, 0, 1];
    assert!(parse_request(&unsupported).is_err());
}

#[test]
fn every_baseline_response_has_exact_frame_and_correlation_identity() {
    for (api, version) in [
        (KafkaApi::ApiVersions, 0),
        (KafkaApi::Metadata, 8),
        (KafkaApi::DescribeCluster, 2),
        (KafkaApi::InitProducerId, 1),
        (KafkaApi::Produce, 8),
    ] {
        let frame = response(
            RequestIdentity {
                api,
                version,
                correlation_id: 17,
            },
            "127.0.0.1:9092",
            "orders",
            23,
        )
        .unwrap_or_else(|error| panic!("encode {api:?}: {error}"));
        let declared = i32::from_be_bytes(
            frame[..4]
                .try_into()
                .unwrap_or_else(|error| panic!("length prefix: {error}")),
        );
        assert_eq!(usize::try_from(declared).ok(), Some(frame.len() - 4));
        assert_eq!(
            i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]),
            23
        );
    }
}

#[test]
fn malformed_endpoint_cannot_manufacture_a_valid_broker_reply() {
    let identity = RequestIdentity {
        api: KafkaApi::Metadata,
        version: 8,
        correlation_id: 1,
    };
    assert!(response(identity, "missing-port", "orders", 1).is_err());
}
