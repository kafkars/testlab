//! In-session admin observation dispatches exact targets to independent Kafka clients.

use std::time::Instant;

use rdkafka::ClientConfig;
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use testlab_schema::{BrokerStateObservation, RunId};

use crate::observer_admin_classic_group;
use crate::observer_admin_config;
use crate::observer_admin_consumer_group_deletion_batch;
use crate::observer_admin_group;
use crate::observer_admin_leader_election;
use crate::observer_admin_metadata;
use crate::observer_admin_partition_reassignment;
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
        AdminTarget::Acls(_) => Err(ObserverError::InvalidTarget(
            "ACL target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ClientQuota(_) => Err(ObserverError::InvalidTarget(
            "client-quota target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::UserScramCredential(_) => Err(ObserverError::InvalidTarget(
            "user SCRAM target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ShareGroup(_) => Err(ObserverError::InvalidTarget(
            "Share-group target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ShareGroupDescriptions(_) => Err(ObserverError::InvalidTarget(
            "Share-group descriptions require the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ShareGroups(_) => Err(ObserverError::InvalidTarget(
            "Share-groups target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ShareGroupOffset(_) => Err(ObserverError::InvalidTarget(
            "Share-group offset target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::Features(_) => Err(ObserverError::InvalidTarget(
            "feature target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::MetadataQuorum(_) => Err(ObserverError::InvalidTarget(
            "metadata-quorum target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::Producers(_) => Err(ObserverError::InvalidTarget(
            "active-producer target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::LogDirs(_) => Err(ObserverError::InvalidTarget(
            "log-directory target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ReplicaLogDirs(_) => Err(ObserverError::InvalidTarget(
            "replica log-directory target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::Transactions(_) => Err(ObserverError::InvalidTarget(
            "transaction target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::PartitionReassignments(_) => Err(ObserverError::InvalidTarget(
            "partition-reassignment listing requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::ShareGroupsOffsets(_) => Err(ObserverError::InvalidTarget(
            "Share-group offsets target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::Topic(target) => Ok(vec![observer_admin_metadata::capture_topic(
            request, target,
        )?]),
        AdminTarget::TopicIdentities(_) => Err(ObserverError::InvalidTarget(
            "topic-identity target requires the pinned Kafka CLI observer".to_owned(),
        )),
        AdminTarget::Topics(target) => observer_admin_metadata::capture_topics(request, target),
        AdminTarget::TopicDeletions(target) => {
            observer_admin_metadata::capture_absent_topics(request, target)
        }
        AdminTarget::Cluster(operation_id) => Ok(vec![observer_admin_metadata::capture_cluster(
            request,
            operation_id,
        )?]),
        AdminTarget::ConsumerGroups(target) => {
            observer_admin_group::capture_groups(request, target)
        }
        AdminTarget::ConsumerGroupDescriptions(target) => {
            observer_admin_classic_group::capture_consumer_groups(request, target)
        }
        AdminTarget::ConsumerGroupDeletions(target) => {
            observer_admin_consumer_group_deletion_batch::capture(request, target)
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
        AdminTarget::TopicConfigs(target) => observer_admin_config::capture_batch(request, target),
        AdminTarget::PartitionOffsets(target) => {
            Ok(vec![observer_partition_offsets::capture(request, target)?])
        }
        AdminTarget::PartitionOffsetsBatch(target) => {
            observer_partition_offsets::capture_batch(request, target)
        }
        AdminTarget::LeaderElection(target) => Ok(vec![observer_admin_leader_election::capture(
            request, target,
        )?]),
        AdminTarget::PartitionAssignments(target) => {
            Ok(vec![observer_admin_partition_reassignment::capture(
                request, target,
            )?])
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
