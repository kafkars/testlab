//! Client-quota adapter tests pin exact entities and lossless whole-number normalization.

use testlab_schema::{BrokerQuotaDirection, OperationId};

use crate::protocol_admin_client_quota::{described_rate, public_entity, whole_rate};

#[test]
fn named_user_entity_maps_to_one_exact_public_component() {
    let entity = public_entity("testlab-user");
    let [component] = entity.components() else {
        panic!("one public quota component");
    };
    assert_eq!(component.entity_type(), "user");
    assert_eq!(component.entity_name(), Some("testlab-user"));
}

#[test]
fn whole_rate_rejects_fractional_nonfinite_and_out_of_range_values() {
    assert_eq!(
        whole_rate(65_536.0, &operation()).unwrap_or_else(|error| panic!("whole quota: {error}")),
        65_536
    );
    assert_eq!(
        whole_rate(f64::from(u32::MAX), &operation())
            .unwrap_or_else(|error| panic!("maximum quota: {error}")),
        u64::from(u32::MAX)
    );
    for invalid in [0.0, 1.5, f64::NAN, f64::INFINITY, f64::from(u32::MAX) + 1.0] {
        assert!(whole_rate(invalid, &operation()).is_err(), "{invalid:?}");
    }
}

#[test]
fn absent_public_quota_entity_is_not_inferred_as_a_value() {
    assert!(
        described_rate(
            Vec::new(),
            &operation(),
            "testlab-user",
            BrokerQuotaDirection::Producer,
        )
        .is_err()
    );
}

fn operation() -> OperationId {
    OperationId::new("client-quota").unwrap_or_else(|error| panic!("operation: {error}"))
}
