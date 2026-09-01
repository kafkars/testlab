//! Bounded plaintext Kafka requests independently discover coordinators and controllers.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kafka_wire::{
    ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, KafkaRequest, MetadataRequest, MetadataResponse, OutboundFrameLimits,
    RequestResponsePair, ResponseHeader, encode_request, response_header_version_for,
};
use kafka_wire_core::{ApiVersion, DecodeLimits, Decoder, KafkaDecode, KafkaEncode, StrBytes};

const CORRELATION_ID: i32 = 1;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const METADATA_VERSION: ApiVersion = ApiVersion::new(8);
const COORDINATOR_VERSION: ApiVersion = ApiVersion::new(2);
const CONSUMER_GROUP_DESCRIBE_VERSION: ApiVersion = ApiVersion::new(1);
const UNSUPPORTED_VERSION: i16 = 35;
const GROUP_ID_NOT_FOUND: i16 = 69;

pub(super) fn controller(endpoint: &str, timeout: Duration) -> Result<i32, String> {
    let response: MetadataResponse = exchange(
        endpoint,
        &MetadataRequest::default(),
        METADATA_VERSION,
        timeout,
    )?;
    valid_node(response.controller_id, "metadata controller")
}

pub(super) fn coordinator(
    endpoint: &str,
    key: &str,
    key_type: i8,
    timeout: Duration,
) -> Result<i32, String> {
    let mut request = FindCoordinatorRequest::default();
    request.key = StrBytes::from(key);
    request.key_type = key_type;
    let response: FindCoordinatorResponse =
        exchange(endpoint, &request, COORDINATOR_VERSION, timeout)?;
    if response.error_code != 0 {
        return Err(format!(
            "FindCoordinator returned Kafka error {}",
            response.error_code
        ));
    }
    valid_node(response.node_id, "coordinator")
}

pub(super) fn consumer_group_member_count(
    endpoint: &str,
    group_id: &str,
    timeout: Duration,
) -> Result<Option<u32>, String> {
    let mut request = ConsumerGroupDescribeRequest::default();
    request.group_ids = vec![group_id.into()];
    let response = exchange(endpoint, &request, CONSUMER_GROUP_DESCRIBE_VERSION, timeout)?;
    modern_group_member_count(group_id, &response)
}

pub(super) fn modern_group_member_count(
    group_id: &str,
    response: &ConsumerGroupDescribeResponse,
) -> Result<Option<u32>, String> {
    let [group] = response.groups.as_slice() else {
        return Err("ConsumerGroupDescribe did not return exactly one group".to_owned());
    };
    if group.group_id.as_str() != group_id {
        return Err(format!(
            "ConsumerGroupDescribe returned group {}, expected {group_id}",
            group.group_id
        ));
    }
    match group.error_code {
        0 => u32::try_from(group.members.len())
            .map(Some)
            .map_err(|_| "ConsumerGroupDescribe member count overflowed".to_owned()),
        UNSUPPORTED_VERSION | GROUP_ID_NOT_FOUND => Ok(None),
        code => Err(format!("ConsumerGroupDescribe returned Kafka error {code}")),
    }
}

fn exchange<R>(
    endpoint: &str,
    request: &R,
    version: ApiVersion,
    timeout: Duration,
) -> Result<R::Response, String>
where
    R: KafkaRequest + RequestResponsePair + KafkaEncode,
    R::Response: KafkaDecode,
{
    let address = endpoint
        .parse::<SocketAddr>()
        .map_err(|error| format!("invalid broker endpoint {endpoint}: {error}"))?;
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .map_err(|error| format!("connect to {endpoint}: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .and_then(|()| stream.set_write_timeout(Some(timeout)))
        .map_err(|error| format!("configure {endpoint} timeout: {error}"))?;
    let mut frame = BytesMut::new();
    encode_request(
        &mut frame,
        CORRELATION_ID,
        Some(StrBytes::from("testlab-role-observer")),
        request,
        version,
        OutboundFrameLimits::new(MAX_FRAME_BYTES),
    )
    .map_err(|error| format!("encode Kafka role request: {error}"))?;
    stream
        .write_all(&frame)
        .map_err(|error| format!("write Kafka role request: {error}"))?;
    decode_response::<R>(&read_frame(&mut stream)?, version)
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, String> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|error| format!("read Kafka role frame length: {error}"))?;
    let length = usize::try_from(i32::from_be_bytes(prefix))
        .map_err(|_| "Kafka role response declared a negative length".to_owned())?;
    if length > MAX_FRAME_BYTES {
        return Err(format!(
            "Kafka role response exceeded {MAX_FRAME_BYTES} bytes"
        ));
    }
    let mut frame = vec![0_u8; length];
    stream
        .read_exact(&mut frame)
        .map_err(|error| format!("read Kafka role response: {error}"))?;
    Ok(frame)
}

pub(super) fn decode_response<R>(frame: &[u8], version: ApiVersion) -> Result<R::Response, String>
where
    R: KafkaRequest + RequestResponsePair,
    R::Response: KafkaDecode,
{
    let mut decoder = Decoder::new(Bytes::copy_from_slice(frame), DecodeLimits::default())
        .map_err(|error| format!("open Kafka role response: {error}"))?;
    let header_version = response_header_version_for::<R>(version)
        .map(ApiVersion::new)
        .map_err(|error| format!("resolve Kafka role response header: {error}"))?;
    let header = ResponseHeader::decode(&mut decoder, header_version)
        .map_err(|error| format!("decode Kafka role response header: {error}"))?;
    if header.correlation_id != CORRELATION_ID {
        return Err(format!(
            "Kafka role response correlation was {}, expected {CORRELATION_ID}",
            header.correlation_id
        ));
    }
    let response = R::Response::decode(&mut decoder, version)
        .map_err(|error| format!("decode Kafka role response body: {error}"))?;
    decoder
        .finish()
        .map_err(|error| format!("finish Kafka role response: {error}"))?;
    Ok(response)
}

fn valid_node(node: i32, label: &str) -> Result<i32, String> {
    (node > 0)
        .then_some(node)
        .ok_or_else(|| format!("{label} node {node} was not a positive broker ID"))
}
