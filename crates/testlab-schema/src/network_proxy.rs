//! Network-proxy controls and observations preserve transport-fault provenance.

use serde::{Deserialize, Serialize};

use crate::EnvironmentOperationId;

/// Current JSON Lines protocol used by the external network proxy.
pub const NETWORK_PROXY_PROTOCOL_VERSION: u16 = 1;

/// One persistent transport fault applied to a broker route.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "fault_kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkFault {
    /// Stops forwarding in both directions without closing the connection.
    Blackhole,
    /// Delays every forwarded chunk in one exact direction.
    Delay {
        /// Direction whose chunks are delayed.
        direction: NetworkDirection,
        /// Delay applied before each matching write.
        delay_ms: u64,
    },
}

/// Direction across one adapter-to-broker proxy route.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkDirection {
    /// Bytes written by the packaged client toward Kafka.
    ClientToBroker,
    /// Bytes written by Kafka toward the packaged client.
    BrokerToClient,
}

/// Requested state for one persistent network fault.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NetworkFaultState {
    /// Establish the exact fault.
    Present,
    /// Remove the exact previously established fault.
    Absent,
}

/// One paired persistent network-fault transition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkFaultAction {
    /// Stable identity for this environment control.
    pub operation_id: EnvironmentOperationId,
    /// One-based broker route ordinal.
    pub broker_ordinal: u16,
    /// Exact fault established or removed.
    pub fault: NetworkFault,
    /// Requested fault state.
    pub state: NetworkFaultState,
    /// Complete acknowledgement bound.
    pub timeout_ms: u64,
}

/// One request to close every connection currently active on a broker route.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConnectionCutAction {
    /// Stable identity for this environment control.
    pub operation_id: EnvironmentOperationId,
    /// One-based broker route ordinal.
    pub broker_ordinal: u16,
    /// Complete cut acknowledgement bound.
    pub timeout_ms: u64,
}

/// One control line sent to the external network proxy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "control", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkProxyControl {
    /// Establishes or removes one persistent fault.
    AlterFault(NetworkFaultAction),
    /// Closes the route's currently active connections once.
    CutConnections(NetworkConnectionCutAction),
}

impl NetworkProxyControl {
    /// Returns the stable control identity.
    pub fn operation_id(&self) -> &EnvironmentOperationId {
        match self {
            Self::AlterFault(action) => &action.operation_id,
            Self::CutConnections(action) => &action.operation_id,
        }
    }
}

/// One versioned control envelope written to proxy stdin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkProxyControlEnvelope {
    /// Exact proxy protocol version.
    pub protocol_version: u16,
    /// Exact validated control.
    pub control: NetworkProxyControl,
}

/// One externally owned proxy route.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkProxyRoute {
    /// One-based broker route ordinal.
    pub broker_ordinal: u16,
    /// Adapter-facing loopback endpoint.
    pub listen_endpoint: String,
    /// Hidden broker-facing loopback endpoint.
    pub upstream_endpoint: String,
}

/// One independently observed completed fault effect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(
    tag = "observation_kind",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum NetworkProxyObservation {
    /// Aggregate counters for one exact applied and removed fault window.
    FaultWindow(NetworkFaultWindowObservation),
    /// Exact active-connection set closed by one cut control.
    ConnectionsCut(NetworkConnectionsCutObservation),
}

impl NetworkProxyObservation {
    /// Returns the global monotonic proxy observation ordinal.
    pub const fn observation(&self) -> u64 {
        match self {
            Self::FaultWindow(value) => value.observation,
            Self::ConnectionsCut(value) => value.observation,
        }
    }
}

/// Aggregate proxy counters across one persistent fault window.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkFaultWindowObservation {
    /// Global monotonic proxy observation ordinal.
    pub observation: u64,
    /// Stable fault-application identity.
    pub apply_operation_id: EnvironmentOperationId,
    /// Stable fault-removal identity.
    pub remove_operation_id: EnvironmentOperationId,
    /// One-based broker route ordinal.
    pub broker_ordinal: u16,
    /// Exact fault active in the window.
    pub fault: NetworkFault,
    /// Worker-observed application time.
    pub started_unix_ms: u64,
    /// Worker-observed removal time.
    pub completed_unix_ms: u64,
    /// Connections active when the fault was applied.
    pub connections_at_start: u64,
    /// Connections accepted while the fault was active.
    pub connections_accepted: u64,
    /// Client-to-broker bytes forwarded while the fault was active.
    pub client_to_broker_bytes: u64,
    /// Broker-to-client bytes forwarded while the fault was active.
    pub broker_to_client_bytes: u64,
    /// Client-to-broker bytes deliberately delayed in the window.
    pub delayed_client_to_broker_bytes: u64,
    /// Broker-to-client bytes deliberately delayed in the window.
    pub delayed_broker_to_client_bytes: u64,
    /// Relay checks held by an active blackhole.
    pub blocked_intervals: u64,
}

/// One exact active connection cut.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkConnectionsCutObservation {
    /// Global monotonic proxy observation ordinal.
    pub observation: u64,
    /// Stable cut control identity.
    pub operation_id: EnvironmentOperationId,
    /// One-based broker route ordinal.
    pub broker_ordinal: u16,
    /// Connections proven closed by this control.
    pub connections_cut: u64,
    /// Worker-observed completion time.
    pub completed_unix_ms: u64,
}

/// Protocol-only stdout event emitted by the external proxy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum NetworkProxyEvent {
    /// Every declared listener is bound.
    Ready {
        /// Exact proxy protocol version.
        protocol_version: u16,
        /// Exact bound routes in broker order.
        routes: Vec<NetworkProxyRoute>,
    },
    /// One persistent fault was installed.
    FaultApplied {
        /// Exact proxy protocol version.
        protocol_version: u16,
        /// Stable acknowledged control identity.
        operation_id: EnvironmentOperationId,
    },
    /// One persistent fault was removed and summarized.
    FaultRemoved {
        /// Exact proxy protocol version.
        protocol_version: u16,
        /// Independent fault-window observation.
        observation: NetworkProxyObservation,
    },
    /// One active connection set was closed and summarized.
    ConnectionsCut {
        /// Exact proxy protocol version.
        protocol_version: u16,
        /// Independent cut observation.
        observation: NetworkProxyObservation,
    },
    /// Proxy integrity or support failure.
    Fatal {
        /// Exact proxy protocol version.
        protocol_version: u16,
        /// Stable failure code.
        code: String,
        /// Bounded diagnostic.
        diagnostic: String,
    },
}
