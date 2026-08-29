//! Versioned adversary controls and observations describe hostile Kafka wire behavior.

use serde::{Deserialize, Serialize};

use crate::EnvironmentOperationId;

/// Current JSON Lines control protocol used by the adversary child process.
pub const ADVERSARY_PROTOCOL_VERSION: u16 = 1;

/// One scenario-owned fault armed against the next matching Kafka requests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolFaultAction {
    /// Stable identity for this environment control.
    pub operation_id: EnvironmentOperationId,
    /// Kafka API whose next requests are selected.
    pub api: KafkaApi,
    /// Number of matching requests that receive the fault.
    pub applications: u16,
    /// Exact wire behavior to apply.
    pub fault: ProtocolFault,
}

impl ProtocolFaultAction {
    /// Validates deterministic bounds before acquiring environment effects.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=32).contains(&self.applications) {
            return Err("protocol fault applications must be between 1 and 32".to_owned());
        }
        match self.fault {
            ProtocolFault::PartialFrame { bytes } if !(1..=1_048_576).contains(&bytes) => {
                Err("partial frame bytes must be between 1 and 1048576".to_owned())
            }
            ProtocolFault::WrongCorrelationId { delta: 0 } => {
                Err("wrong correlation id delta must be nonzero".to_owned())
            }
            ProtocolFault::Stall { duration_ms } if !(10..=30_000).contains(&duration_ms) => {
                Err("protocol stall duration_ms must be between 10 and 30000".to_owned())
            }
            _ => Ok(()),
        }
    }
}

/// Kafka request classes supported by the minimized adversary peer.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KafkaApi {
    /// Produce API key 0.
    Produce,
    /// Metadata API key 3.
    Metadata,
    /// `ApiVersions` API key 18.
    ApiVersions,
    /// `InitProducerId` API key 22.
    InitProducerId,
    /// `DescribeCluster` API key 60.
    DescribeCluster,
}

impl KafkaApi {
    /// Returns the Kafka protocol API key.
    pub const fn key(self) -> i16 {
        match self {
            Self::Produce => 0,
            Self::Metadata => 3,
            Self::ApiVersions => 18,
            Self::InitProducerId => 22,
            Self::DescribeCluster => 60,
        }
    }

    /// Maps one supported Kafka protocol API key.
    pub const fn from_key(key: i16) -> Option<Self> {
        match key {
            0 => Some(Self::Produce),
            3 => Some(Self::Metadata),
            18 => Some(Self::ApiVersions),
            22 => Some(Self::InitProducerId),
            60 => Some(Self::DescribeCluster),
            _ => None,
        }
    }
}

/// One deliberate hostile response behavior.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProtocolFault {
    /// Writes only the selected response prefix and closes the connection.
    PartialFrame {
        /// Maximum complete-frame bytes written before disconnect.
        bytes: u32,
    },
    /// Replaces the response correlation ID by an exact nonzero delta.
    WrongCorrelationId {
        /// Signed delta applied to the request correlation ID.
        delta: i32,
    },
    /// Replays a previous complete response from a different API.
    StaleResponse,
    /// Delays the otherwise valid response for a bounded duration.
    Stall {
        /// Exact delay before the response write.
        duration_ms: u64,
    },
    /// Closes at one selected response-side point.
    Disconnect {
        /// Exact disconnect boundary.
        point: DisconnectPoint,
    },
}

/// Selected connection boundaries at which the adversary can disconnect.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DisconnectPoint {
    /// Close immediately after the complete request frame is read.
    AfterRequest,
    /// Build the response but close before writing any response bytes.
    BeforeResponse,
    /// Write the complete response and then close.
    AfterResponse,
}

/// One control line sent to the external adversary process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdversaryControlEnvelope {
    /// Exact adversary protocol version.
    pub protocol_version: u16,
    /// Fault to arm.
    pub control: ProtocolFaultAction,
}

/// One protocol-only stdout line emitted by the adversary process.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdversaryEvent {
    /// Listener is bound and ready for the packaged client.
    Ready {
        /// Exact adversary protocol version.
        protocol_version: u16,
        /// Loopback Kafka endpoint.
        endpoint: String,
    },
    /// One exact control is installed before acknowledgement.
    Armed {
        /// Exact adversary protocol version.
        protocol_version: u16,
        /// Stable acknowledged control identity.
        operation_id: EnvironmentOperationId,
    },
    /// Independent request and response-side observation.
    Observation {
        /// Exact adversary protocol version.
        protocol_version: u16,
        /// Typed observation.
        observation: ProtocolAdversaryObservation,
    },
    /// Adversary integrity or support failure.
    Fatal {
        /// Exact adversary protocol version.
        protocol_version: u16,
        /// Stable failure code.
        code: String,
        /// Bounded diagnostic.
        diagnostic: String,
    },
}

/// One independently observed Kafka request and its selected response behavior.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolAdversaryObservation {
    /// Global monotonic adversary observation ordinal.
    pub observation: u64,
    /// Stable accepted-connection ordinal.
    pub connection: u64,
    /// Stable request ordinal within the connection.
    pub request: u64,
    /// Parsed supported Kafka API.
    pub api: KafkaApi,
    /// Exact request API version.
    pub api_version: i16,
    /// Exact request correlation ID.
    pub correlation_id: i32,
    /// Complete request-frame byte count including its prefix.
    pub request_bytes: u32,
    /// Response bytes actually written before return or disconnect.
    pub response_bytes: u32,
    /// Correlated scenario control when a fault was selected.
    pub control_id: Option<EnvironmentOperationId>,
    /// Exact observed response behavior.
    pub outcome: AdversaryOutcome,
}

/// Environment-observed result of one Kafka request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AdversaryOutcome {
    /// A complete valid baseline response was written.
    Baseline,
    /// The selected fault was applied.
    FaultApplied {
        /// Exact applied fault.
        fault: ProtocolFault,
    },
}
