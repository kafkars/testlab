//! Public client construction observations preserve the returned configuration surface.

use serde::{Deserialize, Serialize};

use crate::ClientId;

/// Exact values read back from one successfully constructed public client.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfigurationObservation {
    /// Scenario-local identity carried by the creation command.
    pub client_id: ClientId,
    /// Public `Client::client_id` result.
    pub observed_client_id: Option<String>,
    /// Public `Client::bootstrap_servers` result in configured order.
    pub observed_bootstrap_servers: Vec<String>,
    /// Public `Client::expected_cluster_id` result.
    pub observed_expected_cluster_id: Option<String>,
}
