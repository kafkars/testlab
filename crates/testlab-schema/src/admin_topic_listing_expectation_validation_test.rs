//! Topic-list expectation validation rejects ambiguous inclusion claims.

use std::collections::{BTreeMap, BTreeSet};

use super::ScenarioAction;

#[test]
fn list_topics_rejects_invalid_or_incoherent_expectations() {
    let client_id = super::client("client-1");
    let clients = BTreeMap::from([(client_id.clone(), false)]);
    let mut operation_ids = BTreeSet::new();
    let mut problems = Vec::new();

    for (suffix, expected_topics) in [
        ("empty", Vec::new()),
        (
            "large",
            (0..33).map(|index| format!("topic-{index}")).collect(),
        ),
        (
            "invalid",
            vec![String::new(), "records".to_owned(), "records".to_owned()],
        ),
    ] {
        super::validate(
            &super::list_topics(
                client_id.clone(),
                super::operation(&format!("admin-topics-{suffix}")),
                expected_topics,
            ),
            &clients,
            &mut operation_ids,
            &mut problems,
        );
    }

    let mut incoherent = super::list_topics(
        client_id,
        super::operation("admin-topics-incoherent"),
        vec!["__consumer_offsets".to_owned()],
    );
    let ScenarioAction::ListTopics(action) = &mut incoherent else {
        unreachable!("list topics fixture")
    };
    action.expected_topics[0].internal = true;
    super::validate(&incoherent, &clients, &mut operation_ids, &mut problems);

    super::assert_problem(&problems, "expected_topics must contain 1 to 32 entries");
    super::assert_problem(
        &problems,
        "expected_topics must contain unique valid topics",
    );
    super::assert_problem(&problems, "expected_topics inclusion must match");
}
