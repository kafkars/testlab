//! Kafka role wire tests require exact correlation and complete bounded decoding.

use bytes::BytesMut;
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, ConsumerGroupDescribeResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, KafkaRequest, MetadataRequest, MetadataResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
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

#[test]
fn modern_group_probe_requires_an_advertised_supported_version() {
    use crate::kafka_role_wire::supports_modern_group_description;
    let mut response = ApiVersionsResponse::default();
    assert_eq!(supports_modern_group_description(&response), Ok(false));
    let mut api = AdvertisedApi::default();
    api.api_key = 69;
    for (minimum, maximum, expected) in [(0, 0, false), (0, 1, true), (1, 2, true), (2, 2, false)] {
        api.min_version = minimum;
        api.max_version = maximum;
        response.api_keys = vec![api.clone()];
        assert_eq!(supports_modern_group_description(&response), Ok(expected));
    }
}

#[test]
fn malformed_or_failed_version_discovery_is_not_classic_fallback() {
    use crate::kafka_role_wire::supports_modern_group_description;
    let mut response = ApiVersionsResponse::default();
    response.error_code = 35;
    assert!(supports_modern_group_description(&response).is_err());
    response.error_code = 0;
    let mut api = AdvertisedApi::default();
    api.api_key = 69;
    api.max_version = 1;
    response.api_keys = vec![api.clone(), api.clone()];
    assert!(supports_modern_group_description(&response).is_err());
    api.min_version = 2;
    response.api_keys = vec![api];
    assert!(supports_modern_group_description(&response).is_err());
}

#[test]
fn unsupported_modern_api_is_discovered_without_sending_that_request() {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::time::Duration;

    let listener =
        TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("listen: {error}"));
    let endpoint = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("address: {error}"));
    let worker = std::thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .unwrap_or_else(|error| panic!("accept: {error}"));
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap_or_else(|error| panic!("timeout: {error}"));
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .unwrap_or_else(|error| panic!("prefix: {error}"));
        let length = usize::try_from(i32::from_be_bytes(prefix))
            .unwrap_or_else(|error| panic!("length: {error}"));
        assert!(length < 1024);
        let mut body = vec![0; length];
        stream
            .read_exact(&mut body)
            .unwrap_or_else(|error| panic!("request: {error}"));
        assert_eq!(&body[..4], &[0, 18, 0, 0]);
        let response = response_frame::<ApiVersionsRequest, _>(
            &ApiVersionsResponse::default(),
            1,
            ApiVersion::new(0),
        );
        let length =
            i32::try_from(response.len()).unwrap_or_else(|error| panic!("response size: {error}"));
        stream
            .write_all(&length.to_be_bytes())
            .and_then(|()| stream.write_all(&response))
            .unwrap_or_else(|error| panic!("respond: {error}"));
        // Closing this sole listener makes any unadvertised follow-up request fail.
    });
    assert_eq!(
        crate::kafka_role_wire::consumer_group_member_count(
            &endpoint.to_string(),
            "classic-workers",
            Duration::from_secs(2),
        ),
        Ok(None)
    );
    worker
        .join()
        .unwrap_or_else(|error| panic!("worker: {error:?}"));
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
