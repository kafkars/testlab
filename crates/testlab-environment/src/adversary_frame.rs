//! Minimal valid Kafka replies isolate response mutation from socket effects.

use bytes::BytesMut;
use kafka_wire::{
    ApiVersionsRequest, ApiVersionsResponse, DescribeClusterRequest, DescribeClusterResponse,
    InitProducerIdRequest, InitProducerIdResponse, KafkaMessage, MetadataRequest, MetadataResponse,
    ProduceRequest, ProduceResponse, ResponseHeader,
    api_versions_response::ApiVersion as AdvertisedApi,
    describe_cluster_response::DescribeClusterBroker,
    metadata_response::{MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic},
    produce_response::{PartitionProduceResponse, TopicProduceResponse},
    response_header_version,
};
use kafka_wire_core::{ApiKey, ApiVersion, Encoder, KafkaEncode, StrBytes};
use testlab_schema::KafkaApi;

const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RequestIdentity {
    pub(crate) api: KafkaApi,
    pub(crate) version: i16,
    pub(crate) correlation_id: i32,
}

pub(crate) fn parse_request(frame: &[u8]) -> Result<RequestIdentity, String> {
    if frame.len() < 8 {
        return Err("Kafka request body is shorter than its fixed header fields".to_owned());
    }
    let api_key = i16::from_be_bytes([frame[0], frame[1]]);
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let api = KafkaApi::from_key(api_key)
        .ok_or_else(|| format!("unsupported Kafka API key {api_key}"))?;
    Ok(RequestIdentity {
        api,
        version,
        correlation_id,
    })
}

pub(crate) fn response(
    identity: RequestIdentity,
    endpoint: &str,
    topic: &str,
    correlation_id: i32,
) -> Result<Vec<u8>, String> {
    match identity.api {
        KafkaApi::ApiVersions => encode(
            &api_versions(),
            identity,
            correlation_id,
            ApiVersionsRequest::is_flexible(ApiVersion::new(identity.version)),
        ),
        KafkaApi::Metadata => encode(
            &metadata(endpoint, topic)?,
            identity,
            correlation_id,
            MetadataRequest::is_flexible(ApiVersion::new(identity.version)),
        ),
        KafkaApi::InitProducerId => encode(
            &init_producer_id(),
            identity,
            correlation_id,
            InitProducerIdRequest::is_flexible(ApiVersion::new(identity.version)),
        ),
        KafkaApi::Produce => encode(
            &produce(topic),
            identity,
            correlation_id,
            ProduceRequest::is_flexible(ApiVersion::new(identity.version)),
        ),
        KafkaApi::DescribeCluster => encode(
            &describe_cluster(endpoint)?,
            identity,
            correlation_id,
            DescribeClusterRequest::is_flexible(ApiVersion::new(identity.version)),
        ),
    }
}

fn encode<T: KafkaEncode>(
    body: &T,
    identity: RequestIdentity,
    correlation_id: i32,
    request_flexible: bool,
) -> Result<Vec<u8>, String> {
    let version = ApiVersion::new(identity.version);
    let header_version = ApiVersion::new(response_header_version(
        ApiKey::new(identity.api.key()),
        version,
        request_flexible,
    ));
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;
    let body_bytes = header
        .encoded_len(header_version)
        .and_then(|header_len| {
            body.encoded_len(version)
                .map(|body_len| header_len + body_len)
        })
        .map_err(|error| format!("response size failed: {error}"))?;
    if body_bytes > MAX_FRAME_BYTES {
        return Err("adversary response exceeded its frame bound".to_owned());
    }
    let length = i32::try_from(body_bytes)
        .map_err(|_| "adversary response exceeded Kafka's frame prefix".to_owned())?;
    let mut bytes = BytesMut::with_capacity(body_bytes + 4);
    bytes.extend_from_slice(&length.to_be_bytes());
    let mut encoder = Encoder::new(&mut bytes);
    header
        .encode(&mut encoder, header_version)
        .and_then(|()| body.encode(&mut encoder, version))
        .map_err(|error| format!("response encode failed: {error}"))?;
    Ok(bytes.to_vec())
}

fn api_versions() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.api_keys = [(0, 3, 8), (3, 4, 8), (18, 0, 0), (22, 0, 1), (60, 0, 2)]
        .into_iter()
        .map(|(api_key, min_version, max_version)| {
            let mut api = AdvertisedApi::default();
            api.api_key = api_key;
            api.min_version = min_version;
            api.max_version = max_version;
            api
        })
        .collect();
    response
}

fn describe_cluster(endpoint: &str) -> Result<DescribeClusterResponse, String> {
    let (host, port) = split_endpoint(endpoint)?;
    let mut broker = DescribeClusterBroker::default();
    broker.broker_id = 0;
    broker.host = StrBytes::from(host);
    broker.port = port;
    let mut response = DescribeClusterResponse::default();
    response.endpoint_type = 1;
    response.cluster_id = StrBytes::from("testlab-adversary");
    response.controller_id = 0;
    response.brokers = vec![broker];
    Ok(response)
}

fn metadata(endpoint: &str, topic: &str) -> Result<MetadataResponse, String> {
    let (host, port) = split_endpoint(endpoint)?;
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 0;
    broker.host = StrBytes::from(host);
    broker.port = port;
    let mut partition = MetadataResponsePartition::default();
    partition.leader_id = 0;
    partition.replica_nodes = vec![0];
    partition.isr_nodes = vec![0];
    let mut topic_response = MetadataResponseTopic::default();
    topic_response.name = Some(StrBytes::from(topic));
    topic_response.partitions = vec![partition];
    let mut response = MetadataResponse::default();
    response.brokers = vec![broker];
    response.controller_id = 0;
    response.cluster_id = Some(StrBytes::from("testlab-adversary"));
    response.topics = vec![topic_response];
    Ok(response)
}

fn split_endpoint(endpoint: &str) -> Result<(&str, i32), String> {
    let (host, port) = endpoint
        .rsplit_once(':')
        .ok_or_else(|| "adversary endpoint did not contain a port".to_owned())?;
    let port = port
        .parse::<i32>()
        .map_err(|error| format!("adversary endpoint port was invalid: {error}"))?;
    Ok((host, port))
}

fn init_producer_id() -> InitProducerIdResponse {
    let mut response = InitProducerIdResponse::default();
    response.producer_id = 1;
    response
}

fn produce(topic: &str) -> ProduceResponse {
    let mut partition = PartitionProduceResponse::default();
    partition.index = 0;
    let mut topic_response = TopicProduceResponse::default();
    topic_response.name = StrBytes::from(topic);
    topic_response.partition_responses = vec![partition];
    let mut response = ProduceResponse::default();
    response.responses = vec![topic_response];
    response
}
