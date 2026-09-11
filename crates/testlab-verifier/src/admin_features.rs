//! Cluster feature verification compares public ranges with pinned Kafka CLI rows.

use testlab_schema::{
    AdminFeaturesDescription, BrokerFeatureState, BrokerFeaturesState, FeatureVersionRange,
    ScenarioAction, Violation,
};

use crate::admin::{AdminCommandWindow, immediate_after_public, public_after_command};
use crate::index::HistoryIndex;
use crate::index::admin_features::Indexed;
use crate::support::violation;

pub(crate) fn verify_features_action(
    action: &ScenarioAction,
    index: &HistoryIndex,
    violations: &mut Vec<Violation>,
) -> bool {
    if matches!(action, ScenarioAction::ValidateFeatureUpdates(_)) {
        return crate::admin_feature_updates::verify(action, index, violations);
    }
    let command_window = index.admin_command_window(action);
    let ScenarioAction::DescribeFeatures(action) = action else {
        return false;
    };
    let public = index.admin_features.described.get(&action.operation_id);
    let independent = index.admin_features.observed.get(&action.operation_id);
    if exact_match(
        public,
        independent,
        command_window,
        action.expected_zk_migration_ready,
    ) {
        return true;
    }
    violations.push(violation(
        "ADMIN-050",
        format!(
            "admin operation {} expected canonical public feature metadata matching one immediate Kafka CLI snapshot",
            action.operation_id
        ),
        Some(action.operation_id.clone()),
        evidence(public, independent),
    ));
    true
}

fn exact_match(
    public: Option<&Vec<Indexed<AdminFeaturesDescription>>>,
    independent: Option<&Vec<Indexed<BrokerFeaturesState>>>,
    window: Option<AdminCommandWindow>,
    expected_zk_migration_ready: bool,
) -> bool {
    let (Some([public]), Some([independent])) =
        (public.map(Vec::as_slice), independent.map(Vec::as_slice))
    else {
        return false;
    };
    public.value.zk_migration_ready == expected_zk_migration_ready
        && public.value.finalized_features_epoch == independent.value.finalized_features_epoch
        && supported_match(
            &public.value.supported_features,
            public.value.supported_features_complete,
            &independent.value.features,
        )
        && finalized_match(
            &public.value.finalized_features,
            &independent.value.features,
        )
        && public_after_command(window, public.history_sequence)
        && immediate_after_public(
            window,
            public.history_sequence,
            independent.history_sequence,
        )
}

fn supported_match(
    public: &[FeatureVersionRange],
    complete: bool,
    independent: &[BrokerFeatureState],
) -> bool {
    if !canonical_ranges(public, false) || !canonical_independent(independent) {
        return false;
    }
    let all_public_match = public.iter().all(|range| {
        independent.iter().any(|feature| {
            feature.name == range.name
                && feature.supported_min_version_level == range.min_version_level
                && feature.supported_max_version_level == range.max_version_level
        })
    });
    let omitted_are_legacy_zero = independent.iter().all(|feature| {
        public.iter().any(|range| range.name == feature.name)
            || (!complete && feature.supported_min_version_level == 0)
    });
    all_public_match && omitted_are_legacy_zero && (!complete || public.len() == independent.len())
}

fn finalized_match(public: &[FeatureVersionRange], independent: &[BrokerFeatureState]) -> bool {
    canonical_ranges(public, true)
        && public.iter().all(|range| {
            independent.iter().any(|feature| {
                feature.name == range.name
                    && feature.finalized_version_level == range.max_version_level
            })
        })
        && independent.iter().all(|feature| {
            feature.finalized_version_level == 0
                || public.iter().any(|range| {
                    range.name == feature.name
                        && range.max_version_level == feature.finalized_version_level
                })
        })
}

fn canonical_ranges(ranges: &[FeatureVersionRange], empty_allowed: bool) -> bool {
    (empty_allowed || !ranges.is_empty())
        && ranges.iter().all(|range| {
            !range.name.is_empty()
                && range.min_version_level >= 0
                && range.max_version_level >= range.min_version_level
        })
        && ranges
            .windows(2)
            .all(|pair| pair[0].name.as_bytes() < pair[1].name.as_bytes())
}

fn canonical_independent(features: &[BrokerFeatureState]) -> bool {
    !features.is_empty()
        && features.iter().all(|feature| {
            !feature.name.is_empty()
                && feature.supported_min_version_level >= 0
                && feature.supported_max_version_level >= feature.supported_min_version_level
                && feature.finalized_version_level >= 0
                && (feature.finalized_version_level == 0
                    || (feature.finalized_version_level >= feature.supported_min_version_level
                        && feature.finalized_version_level <= feature.supported_max_version_level))
        })
        && features
            .windows(2)
            .all(|pair| pair[0].name.as_bytes() < pair[1].name.as_bytes())
}

fn evidence(
    public: Option<&Vec<Indexed<AdminFeaturesDescription>>>,
    independent: Option<&Vec<Indexed<BrokerFeaturesState>>>,
) -> Vec<String> {
    public
        .into_iter()
        .flatten()
        .map(|value| format!("history:{}", value.history_sequence))
        .chain(
            independent
                .into_iter()
                .flatten()
                .map(|value| format!("broker-state-observation:{}", value.value.observation)),
        )
        .collect()
}
