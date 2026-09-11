//! Mixed group-description tests require detailed ordered public and independent facts.

#[path = "admin_consumer_groups_description_test_fixture.rs"]
mod fixture;
#[path = "admin_consumer_groups_description_test_values.rs"]
mod values;

use testlab_schema::GroupMembershipEpoch;

use fixture::{
    assert_contract, description, group_observation, history, receive_epoch, violations,
};

#[test]
fn exact_mixed_group_descriptions_pass() {
    assert!(violations(&history()).is_empty());
}

#[test]
fn mixed_descriptions_reject_reordering_or_wrong_broker_count() {
    let mut reordered = history();
    description(&mut reordered).outcomes.swap(0, 1);
    assert_contract(&violations(&reordered));

    let mut wrong_count = history();
    group_observation(&mut wrong_count[5]).member_count = Some(2);
    assert_contract(&violations(&wrong_count));
}

#[test]
fn mixed_descriptions_reject_wrong_protocol_details_or_receive_epoch() {
    let mut wrong_group_epoch = history();
    description(&mut wrong_group_epoch).outcomes[0]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("description"))
        .group_epoch = Some(0);
    assert_contract(&violations(&wrong_group_epoch));

    let mut wrong_receive_epoch = history();
    receive_epoch(
        &mut wrong_receive_epoch[0],
        GroupMembershipEpoch::Classic { generation_id: 2 },
    );
    assert_contract(&violations(&wrong_receive_epoch));
}

#[test]
fn mixed_descriptions_reject_missing_typed_assignment_or_classic_payload() {
    let mut missing_assignment = history();
    description(&mut missing_assignment).outcomes[0]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("description"))
        .members[0]
        .assignment
        .clear();
    assert_contract(&violations(&missing_assignment));

    let mut missing_classic = history();
    description(&mut missing_classic).outcomes[1]
        .description
        .as_mut()
        .unwrap_or_else(|| panic!("description"))
        .members[0]
        .classic_assignment
        .clear();
    assert_contract(&violations(&missing_classic));
}
