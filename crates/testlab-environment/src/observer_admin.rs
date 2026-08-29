//! In-session admin observation dispatches exact targets to independent Kafka clients.

use std::time::Instant;

use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use testlab_schema::{BrokerStateObservation, RunId};

use crate::observer_admin_classic_group;
use crate::observer_admin_config;
use crate::observer_admin_group;
use crate::observer_admin_metadata;
use crate::observer_admin_target::AdminTarget;
use crate::observer_error::ObserverError;
use crate::observer_group_offset;
use crate::observer_group_offsets;
use crate::observer_partition_offsets;
use crate::security::ClientSecurity;

#[derive(Clone, Copy, Debug)]
pub(super) struct AdminObserverRequest<'a> {
    pub(super) endpoint: &'a str,
    pub(super) run_id: &'a RunId,
    pub(super) deadline: Instant,
    pub(super) security: &'a ClientSecurity,
    pub(super) cluster_size: u16,
    pub(super) first_observation: u64,
}

pub(super) fn capture(
    request: AdminObserverRequest<'_>,
    target: &AdminTarget,
) -> Result<Vec<BrokerStateObservation>, ObserverError> {
    match target {
        AdminTarget::Topic(target) => Ok(vec![observer_admin_metadata::capture_topic(
            request, target,
        )?]),
        AdminTarget::Topics(target) => observer_admin_metadata::capture_topics(request, target),
        AdminTarget::Cluster(operation_id) => Ok(vec![observer_admin_metadata::capture_cluster(
            request,
            operation_id,
        )?]),
        AdminTarget::ConsumerGroups(target) => {
            observer_admin_group::capture_groups(request, target)
        }
        AdminTarget::ConsumerGroup(target) => {
            Ok(vec![observer_admin_group::capture_group(request, target)?])
        }
        AdminTarget::ConsumerGroupOffset(target) => {
            Ok(vec![observer_group_offset::capture_admin_target(
                request, target,
            )?])
        }
        AdminTarget::ConsumerGroupOffsets(target) => {
            observer_group_offsets::capture_group(request, target)
        }
        AdminTarget::ConsumerGroupsOffsets(target) => {
            observer_group_offsets::capture_groups(request, target)
        }
        AdminTarget::ClassicGroups(target) => {
            observer_admin_classic_group::capture(request, target)
        }
        AdminTarget::TopicConfig(target) => {
            Ok(vec![observer_admin_config::capture(request, target)?])
        }
        AdminTarget::PartitionOffsets(target) => {
            Ok(vec![observer_partition_offsets::capture(request, target)?])
        }
    }
}

pub(super) fn client(
    request: AdminObserverRequest<'_>,
    purpose: &str,
) -> Result<AdminClient<DefaultClientContext>, ObserverError> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", request.endpoint).set(
        "client.id",
        format!("testlab-admin-observer-{}-{purpose}", request.run_id),
    );
    request.security.configure(&mut config);
    config.create().map_err(ObserverError::Kafka)
}
