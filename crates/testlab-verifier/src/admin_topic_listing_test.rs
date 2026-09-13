//! Topic-list verifier controls distinguish names-only evidence from complete public detail.

use super::*;
use testlab_schema::{
    AdminListedTopicOutcome, AdminTopicDescriptionValue, AdminTopicsListing, ListTopicsAction,
    TopicListingExpectation,
};

#[test]
fn detailed_topic_list_matches_required_metadata_and_authorization() {
    let operation_id = operation("topics-1");
    let scenario = listing_scenario(operation_id.clone(), true);
    let history = good_history(operation_id);

    assert!(admin_violations(&scenario, &history, &[]).is_empty());
}

#[test]
fn topic_list_requires_independent_metadata_and_one_completion() {
    let operation_id = operation("topics-1");
    let scenario = listing_scenario(operation_id.clone(), true);
    let history = good_history(operation_id.clone());
    assert_contract(
        &admin_violations(&scenario, &history[..1], &[]),
        "ADMIN-004",
    );

    let mut duplicate = history.clone();
    duplicate.insert(1, duplicate[0].clone());
    duplicate[1].sequence = 2;
    duplicate[1].observed_unix_ms = 2;
    duplicate[2].sequence = 3;
    duplicate[2].observed_unix_ms = 3;
    assert_contract(&admin_violations(&scenario, &duplicate, &[]), "ADMIN-004");
}

#[test]
fn topic_list_rejects_unsorted_detail_or_wrong_required_topology() {
    let operation_id = operation("topics-1");
    let scenario = listing_scenario(operation_id.clone(), true);
    let mut unsorted = good_history(operation_id.clone());
    listing(&mut unsorted).outcomes.swap(0, 1);
    assert_contract(&admin_violations(&scenario, &unsorted, &[]), "ADMIN-004");

    let mut wrong_topology = good_history(operation_id);
    listing(&mut wrong_topology).outcomes[1]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("marker description"))
        .partitions[0]
        .partition = 1;
    assert_contract(
        &admin_violations(&scenario, &wrong_topology, &[]),
        "ADMIN-004",
    );
}

#[test]
fn topic_list_rejects_missing_authorization_or_changed_wire_option() {
    let operation_id = operation("topics-1");
    let scenario = listing_scenario(operation_id.clone(), true);
    let mut missing = good_history(operation_id.clone());
    listing(&mut missing).outcomes[1]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("marker description"))
        .authorized_operations = None;
    assert_contract(&admin_violations(&scenario, &missing, &[]), "ADMIN-004");

    let mut issued = vec![command(
        0,
        AdapterCommand::ListTopics(ListTopicsCommand {
            client_id: client(),
            operation_id: operation_id.clone(),
            include_internal: false,
            include_authorized_operations: false,
            timeout_ms: 1_000,
        }),
    )];
    issued.extend(good_history(operation_id));
    let index = HistoryIndex::build(&issued);
    let mut violations = Vec::new();
    verify_admin(&scenario, &index, &[], &mut violations);
    assert_contract(&violations, "ADMIN-004");
}

#[test]
fn topic_list_without_authorization_rejects_unrequested_metadata() {
    let operation_id = operation("topics-1");
    let scenario = listing_scenario(operation_id.clone(), false);
    let mut history = good_history(operation_id);
    for outcome in &mut listing(&mut history).outcomes {
        outcome
            .description
            .as_mut()
            .unwrap_or_else(|| panic!("listed description"))
            .authorized_operations = None;
    }
    assert!(admin_violations(&scenario, &history, &[]).is_empty());

    listing(&mut history).outcomes[1]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("marker description"))
        .authorized_operations = Some(7);
    assert_contract(&admin_violations(&scenario, &history, &[]), "ADMIN-004");
}

#[test]
fn internal_topic_is_excluded_then_included_with_exact_public_marker() {
    let excluded_operation = operation("topics-internal-excluded");
    let excluded = internal_scenario(excluded_operation.clone(), false);
    let excluded_history = vec![
        event(
            1,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: excluded_operation.clone(),
                outcomes: vec![outcome("marker", Some(7), vec![0])],
            }),
        ),
        topic_state(2, excluded_operation.clone(), "marker", vec![0]),
        topic_state(3, excluded_operation, "__consumer_offsets", vec![0, 1]),
    ];
    assert!(admin_violations(&excluded, &excluded_history, &[]).is_empty());

    let mut leaked = excluded_history.clone();
    let mut unexpected_internal = outcome("__consumer_offsets", Some(7), vec![0, 1]);
    unexpected_internal
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("internal description"))
        .internal = true;
    listing(&mut leaked).outcomes.insert(0, unexpected_internal);
    assert_contract(&admin_violations(&excluded, &leaked, &[]), "ADMIN-004");

    let included_operation = operation("topics-internal-included");
    let included = internal_scenario(included_operation.clone(), true);
    let mut internal = outcome("__consumer_offsets", Some(7), vec![0, 1]);
    internal
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("internal description"))
        .internal = true;
    let included_history = vec![
        event(
            1,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: included_operation.clone(),
                outcomes: vec![internal, outcome("marker", Some(7), vec![0])],
            }),
        ),
        topic_state(2, included_operation.clone(), "marker", vec![0]),
        topic_state(3, included_operation, "__consumer_offsets", vec![0, 1]),
    ];
    assert!(admin_violations(&included, &included_history, &[]).is_empty());

    let mut wrong_marker = included_history;
    listing(&mut wrong_marker).outcomes[0]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("internal description"))
        .internal = false;
    assert_contract(
        &admin_violations(&included, &wrong_marker, &[]),
        "ADMIN-004",
    );
}

fn listing_scenario(
    operation_id: OperationId,
    include_authorization: bool,
) -> testlab_schema::Scenario {
    admin_scenario(ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id,
        include_internal: false,
        include_authorized_operations: include_authorization,
        expected_topics: vec![expected("marker", false, true)],
        timeout_ms: 1_000,
    }))
}

fn internal_scenario(
    operation_id: OperationId,
    include_internal: bool,
) -> testlab_schema::Scenario {
    admin_scenario(ScenarioAction::ListTopics(ListTopicsAction {
        client_id: client(),
        operation_id,
        include_internal,
        include_authorized_operations: true,
        expected_topics: vec![
            expected("marker", false, true),
            expected("__consumer_offsets", true, include_internal),
        ],
        timeout_ms: 1_000,
    }))
}

fn expected(topic: &str, internal: bool, included: bool) -> TopicListingExpectation {
    TopicListingExpectation {
        topic: topic.to_owned(),
        internal,
        included,
    }
}

fn good_history(operation_id: OperationId) -> Vec<HistoryEntry> {
    vec![
        event(
            1,
            AdapterEvent::TopicsListed(AdminTopicsListing {
                operation_id: operation_id.clone(),
                outcomes: vec![
                    outcome("another", Some(7), vec![0]),
                    outcome("marker", Some(7), vec![0]),
                ],
            }),
        ),
        topic_state(2, operation_id, "marker", vec![0]),
    ]
}

fn outcome(
    topic: &str,
    authorization: Option<i32>,
    partitions: Vec<i32>,
) -> AdminListedTopicOutcome {
    AdminListedTopicOutcome {
        topic: topic.to_owned(),
        description: Some(AdminTopicDescriptionValue {
            topic_id: Some([1; 16]),
            internal: false,
            authorized_operations: authorization,
            partitions: partitions
                .into_iter()
                .map(crate::admin_topic::topology::partition)
                .collect(),
        }),
        error_code: None,
    }
}

fn listing(history: &mut [HistoryEntry]) -> &mut AdminTopicsListing {
    let HistoryPayload::AdapterEvent { event, .. } = &mut history[0].payload else {
        panic!("listing event")
    };
    let AdapterEvent::TopicsListed(listing) = event else {
        panic!("topics listed")
    };
    listing
}
