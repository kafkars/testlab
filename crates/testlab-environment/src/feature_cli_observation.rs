//! Pinned Kafka feature CLI output becomes exact numeric broker feature state.

use std::str;

use testlab_schema::{
    BrokerFeatureState, BrokerFeaturesState, BrokerStateObservation, OperationId,
};

use crate::observer_error::ObserverError;

const METADATA_FEATURE: &str = "metadata.version";

#[derive(Clone, Debug, Eq, PartialEq)]
struct RawFeature {
    name: String,
    supported_min: String,
    supported_max: String,
    finalized: String,
    epoch: Option<i64>,
}

pub(crate) fn normalize(
    observation: u64,
    operation_id: &OperationId,
    stdout: &[u8],
) -> Result<BrokerStateObservation, ObserverError> {
    let rows = parse_rows(stdout)?;
    let epoch = rows.first().and_then(|row| row.epoch);
    if rows.iter().any(|row| row.epoch != epoch) {
        return Err(invalid("feature rows did not share one finalized epoch"));
    }
    let features = rows
        .into_iter()
        .map(normalize_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BrokerStateObservation::Features(BrokerFeaturesState {
        observation,
        operation_id: operation_id.clone(),
        features,
        finalized_features_epoch: epoch,
    }))
}

fn parse_rows(stdout: &[u8]) -> Result<Vec<RawFeature>, ObserverError> {
    let text = str::from_utf8(stdout)
        .map_err(|_| invalid("feature description output was not valid UTF-8"))?;
    let rows = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(parse_row)
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Err(invalid("feature description returned no rows"));
    }
    if rows
        .windows(2)
        .any(|pair| pair[0].name.as_bytes() >= pair[1].name.as_bytes())
    {
        return Err(invalid(
            "feature description rows were not unique canonical name order",
        ));
    }
    Ok(rows)
}

fn parse_row(line: &str) -> Result<RawFeature, ObserverError> {
    let line = line
        .trim()
        .strip_prefix("Feature:")
        .ok_or_else(|| invalid("feature description row had no Feature marker"))?;
    let (name, line) = field(line, "SupportedMinVersion:")?;
    let (supported_min, line) = field(line, "SupportedMaxVersion:")?;
    let (supported_max, line) = field(line, "FinalizedVersionLevel:")?;
    let (finalized, epoch) = field(line, "Epoch:")?;
    let epoch = match epoch.trim() {
        "-" => None,
        value => Some(parse_nonnegative_i64(value, "finalized feature epoch")?),
    };
    Ok(RawFeature {
        name: nonempty(name, "feature name")?,
        supported_min: nonempty(supported_min, "supported minimum")?,
        supported_max: nonempty(supported_max, "supported maximum")?,
        finalized: nonempty(finalized, "finalized level")?,
        epoch,
    })
}

fn field<'a>(input: &'a str, next: &str) -> Result<(&'a str, &'a str), ObserverError> {
    input
        .split_once(next)
        .ok_or_else(|| invalid(format!("feature description row omitted {next}")))
}

fn nonempty(value: &str, field: &str) -> Result<String, ObserverError> {
    let value = value.trim();
    if value.is_empty() {
        Err(invalid(format!("feature description had an empty {field}")))
    } else {
        Ok(value.to_owned())
    }
}

fn normalize_row(row: RawFeature) -> Result<BrokerFeatureState, ObserverError> {
    let minimum = level(&row.name, &row.supported_min)?;
    let maximum = level(&row.name, &row.supported_max)?;
    let finalized = level(&row.name, &row.finalized)?;
    if maximum < minimum || finalized > maximum {
        return Err(invalid(format!(
            "feature {} exposed incoherent version levels",
            row.name
        )));
    }
    Ok(BrokerFeatureState {
        name: row.name,
        supported_min_version_level: minimum,
        supported_max_version_level: maximum,
        finalized_version_level: finalized,
    })
}

fn level(feature: &str, value: &str) -> Result<i16, ObserverError> {
    numeric_level(value)
        .or_else(|| {
            if feature == METADATA_FEATURE {
                metadata_level(value)
            } else {
                None
            }
        })
        .ok_or_else(|| {
            invalid(format!(
                "feature {feature} level {value:?} was not numeric or in the pinned metadata map"
            ))
        })
}

fn metadata_level(value: &str) -> Option<i16> {
    Some(match value {
        "3.3-IV3" => 7,
        "3.4-IV0" => 8,
        "3.5-IV0" => 9,
        "3.5-IV1" => 10,
        "3.5-IV2" => 11,
        "3.6-IV0" => 12,
        "3.6-IV1" => 13,
        "3.6-IV2" => 14,
        "3.7-IV0" => 15,
        "3.7-IV1" => 16,
        "3.7-IV2" => 17,
        "3.7-IV3" => 18,
        "3.7-IV4" => 19,
        "3.8-IV0" => 20,
        "3.9-IV0" => 21,
        "4.0-IV0" => 22,
        "4.0-IV1" => 23,
        "4.0-IV2" => 24,
        "4.0-IV3" => 25,
        "4.1-IV0" => 26,
        "4.1-IV1" => 27,
        "4.2-IV0" => 28,
        "4.2-IV1" => 29,
        "4.3-IV0" => 30,
        _ => return None,
    })
}

fn numeric_level(value: &str) -> Option<i16> {
    value
        .parse::<i16>()
        .ok()
        .or_else(|| value.strip_prefix("UNKNOWN ")?.parse::<i16>().ok())
        .filter(|level| *level >= 0)
}

fn parse_nonnegative_i64(value: &str, field: &str) -> Result<i64, ObserverError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .ok_or_else(|| invalid(format!("{field} was not a nonnegative i64")))
}

fn invalid(message: impl Into<String>) -> ObserverError {
    ObserverError::InvalidBrokerState(format!("Kafka CLI feature snapshot: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OUTPUT: &str = "\
Feature: eligible.leader.replicas.version SupportedMinVersion: 0 SupportedMaxVersion: 1 FinalizedVersionLevel: 0 Epoch: 7\n\
Feature: metadata.version SupportedMinVersion: 3.3-IV3 SupportedMaxVersion: 4.3-IV0 FinalizedVersionLevel: 4.3-IV0 Epoch: 7\n";

    #[test]
    fn symbolic_metadata_levels_are_mapped_and_numeric_rows_remain_exact() {
        let observed =
            normalize(9, &operation(), OUTPUT.as_bytes()).unwrap_or_else(|error| panic!("{error}"));
        let BrokerStateObservation::Features(observed) = observed else {
            panic!("feature observation kind");
        };
        assert_eq!(observed.observation, 9);
        assert_eq!(observed.finalized_features_epoch, Some(7));
        assert_eq!(observed.features.len(), 2);
        assert_eq!(observed.features[0].supported_max_version_level, 1);
        assert_eq!(observed.features[1].supported_min_version_level, 7);
        assert_eq!(observed.features[1].finalized_version_level, 30);
    }

    #[test]
    fn pinned_metadata_map_is_contiguous_across_the_supported_matrix() {
        for (level, label) in (7_i16..).zip([
            "3.3-IV3", "3.4-IV0", "3.5-IV0", "3.5-IV1", "3.5-IV2", "3.6-IV0", "3.6-IV1", "3.6-IV2",
            "3.7-IV0", "3.7-IV1", "3.7-IV2", "3.7-IV3", "3.7-IV4", "3.8-IV0", "3.9-IV0", "4.0-IV0",
            "4.0-IV1", "4.0-IV2", "4.0-IV3", "4.1-IV0", "4.1-IV1", "4.2-IV0", "4.2-IV1", "4.3-IV0",
        ]) {
            assert_eq!(metadata_level(label), Some(level));
        }
        assert_eq!(metadata_level("4.3-IV1"), None);
    }

    #[test]
    fn malformed_order_epoch_and_unmapped_levels_fail_closed() {
        let reversed = "\
Feature: metadata.version SupportedMinVersion: 3.3-IV3 SupportedMaxVersion: 4.3-IV0 FinalizedVersionLevel: 4.3-IV0 Epoch: 7\n\
Feature: alpha.version SupportedMinVersion: 0 SupportedMaxVersion: 1 FinalizedVersionLevel: 0 Epoch: 7\n";
        assert!(normalize(1, &operation(), reversed.as_bytes()).is_err());

        let epoch_mismatch = OUTPUT.replacen("Epoch: 7", "Epoch: 8", 1);
        assert!(normalize(1, &operation(), epoch_mismatch.as_bytes()).is_err());
        let unmapped = OUTPUT.replace("4.3-IV0", "4.3-IV1");
        assert!(normalize(1, &operation(), unmapped.as_bytes()).is_err());
    }

    fn operation() -> OperationId {
        OperationId::new("admin-features").unwrap_or_else(|error| panic!("operation id: {error}"))
    }
}
