//! Manual topic-creation adapter tests pin exact replica assignments.

use kafkars::admin::NewTopicPlacement;
use testlab_schema::{
    ClientId, CreateTopicCommand, OperationId, TopicCreationConfig, TopicReplicaAssignmentSpec,
};

#[test]
fn manual_assignments_select_the_public_manual_topic_constructor() {
    let command = command(Some(vec![assignment(0, &[2, 1]), assignment(1, &[3, 2])]));

    let topic = crate::protocol_admin_write::new_topic(&command);

    let NewTopicPlacement::Manual { assignments } = topic.placement() else {
        panic!("manual placement kind");
    };
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].partition_index(), 0);
    assert_eq!(assignments[0].broker_ids(), &[2, 1]);
    assert_eq!(assignments[1].partition_index(), 1);
    assert_eq!(assignments[1].broker_ids(), &[3, 2]);
}

#[test]
fn absent_assignments_retain_automatic_topic_placement() {
    let command = command(None);

    let topic = crate::protocol_admin_write::new_topic(&command);

    assert_eq!(topic.partitions(), 2);
    assert_eq!(topic.requested_replication_factor(), 2);
    assert!(matches!(
        topic.placement(),
        NewTopicPlacement::Automatic {
            partitions: 2,
            replication_factor: 2
        }
    ));
}

#[test]
fn creation_configs_retain_caller_order() {
    let mut command = command(None);
    command.configs = vec![
        TopicCreationConfig {
            name: "cleanup.policy".to_owned(),
            value: "compact".to_owned(),
        },
        TopicCreationConfig {
            name: "min.insync.replicas".to_owned(),
            value: "1".to_owned(),
        },
    ];

    let topic = crate::protocol_admin_write::new_topic(&command);

    assert_eq!(
        topic,
        kafkars::admin::NewTopic::new("orders", 2)
            .replication_factor(2)
            .config("cleanup.policy", "compact")
            .config("min.insync.replicas", "1")
    );
}

fn command(replica_assignments: Option<Vec<TopicReplicaAssignmentSpec>>) -> CreateTopicCommand {
    CreateTopicCommand {
        client_id: ClientId::new("client-1").unwrap_or_else(|error| panic!("client ID: {error}")),
        operation_id: OperationId::new("manual-create")
            .unwrap_or_else(|error| panic!("operation ID: {error}")),
        topic: "orders".to_owned(),
        partitions: 2,
        replication_factor: 2,
        replica_assignments,
        configs: Vec::new(),
        validate_only: false,
        timeout_ms: 1_000,
    }
}

fn assignment(partition_index: i32, broker_ids: &[i32]) -> TopicReplicaAssignmentSpec {
    TopicReplicaAssignmentSpec {
        partition_index,
        broker_ids: broker_ids.to_vec(),
    }
}
