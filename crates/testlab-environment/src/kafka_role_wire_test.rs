//! Kafka role wire tests require exact correlation and complete bounded decoding.

use bytes::BytesMut;
use kafka_wire::{
    ConsumerGroupDescribeResponse, FindCoordinatorRequest, FindCoordinatorResponse, KafkaRequest,
    MetadataRequest, MetadataResponse, ResponseHeader,
    consumer_group_describe_response::{DescribedGroup, Member},
    response_header_version_for,
};
use kafka_wire_core::{ApiVersion, Encoder, KafkaEncode};

#[test]
fn metadata_controller_response_decodes_exactly() {
    let version = ApiVersion::new(8);
    let mut response = MetadataResponse::default();
    response.controller_id = 2;
    let frame = response_frame::<MetadataRequest, _>(&response, 1, version);

    let decoded = crate::kafka_role_wire::decode_response::<MetadataRequest>(&frame, version)
        .unwrap_or_else(|error| panic!("decode metadata role response: {error}"));

    assert_eq!(decoded.controller_id, 2);
}

#[test]
fn coordinator_response_decodes_exactly() {
    let version = ApiVersion::new(2);
    let mut response = FindCoordinatorResponse::default();
    response.error_code = 0;
    response.node_id = 3;
    let frame = response_frame::<FindCoordinatorRequest, _>(&response, 1, version);

    let decoded =
        crate::kafka_role_wire::decode_response::<FindCoordinatorRequest>(&frame, version)
            .unwrap_or_else(|error| panic!("decode coordinator response: {error}"));

    assert_eq!(decoded.node_id, 3);
}

#[test]
fn wrong_correlation_is_rejected() {
    let version = ApiVersion::new(8);
    let frame = response_frame::<MetadataRequest, _>(&MetadataResponse::default(), 9, version);

    let error = match crate::kafka_role_wire::decode_response::<MetadataRequest>(&frame, version) {
        Ok(_) => panic!("wrong correlation must fail"),
        Err(error) => error,
    };

    assert!(error.contains("correlation was 9, expected 1"));
}

#[test]
fn modern_group_description_reports_exact_members_or_fallback() {
    let mut described = DescribedGroup::default();
    described.group_id = "workers".into();
    described.members = vec![Member::default(), Member::default()];
    let mut response = ConsumerGroupDescribeResponse::default();
    response.groups = vec![described];
    assert_eq!(
        crate::kafka_role_wire::modern_group_member_count("workers", &response),
        Ok(Some(2))
    );

    let mut missing = DescribedGroup::default();
    missing.group_id = "classic-workers".into();
    missing.error_code = 69;
    let mut response = ConsumerGroupDescribeResponse::default();
    response.groups = vec![missing];
    assert_eq!(
        crate::kafka_role_wire::modern_group_member_count("classic-workers", &response),
        Ok(None)
    );
}

fn response_frame<R, S>(response: &S, correlation_id: i32, version: ApiVersion) -> Vec<u8>
where
    R: KafkaRequest,
    S: KafkaEncode,
{
    let header_version = response_header_version_for::<R>(version).map_or_else(
        |error| panic!("response header version: {error}"),
        ApiVersion::new,
    );
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    let mut bytes = BytesMut::new();
    let mut encoder = Encoder::new(&mut bytes);
    header
        .encode(&mut encoder, header_version)
        .and_then(|()| response.encode(&mut encoder, version))
        .unwrap_or_else(|error| panic!("encode response: {error}"));
    bytes.to_vec()
}
