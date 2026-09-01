//! Transaction provisioning tests retain every declared transform output target.

use std::collections::BTreeMap;

use testlab_schema::Scenario;

use crate::compose_provision_targets::topics;

#[test]
fn transactional_transform_provisions_input_and_output_topics() {
    let scenario: Scenario = toml::from_str(include_str!(
        "../../../scenarios/kafka/transactional-offset-classic.toml"
    ))
    .unwrap_or_else(|error| panic!("parse transactional transform scenario: {error}"));

    assert_eq!(
        topics(&scenario),
        BTreeMap::from([
            ("testlab-kafkars-classic-transform-input".to_owned(), 1,),
            ("testlab-kafkars-classic-transform-output".to_owned(), 1,),
        ])
    );
}
