//! ACL result normalization preserves caller positions and nested deletion matches.

use testlab_schema::{
    AdminAclCreationOutcome, AdminAclDeleteMatch, AdminAclDeletionOutcome, LiteralAclBinding,
    OperationId,
};

use crate::AdapterError;
use crate::kafkars_api::{
    CreateAclOutcome, CreateAclResult, DeleteAclFilterOutcome, DeleteAclFilterResult,
    DeleteAclMatchResult,
};
use crate::protocol_admin_acl_mapping::{invalid_result, schema_binding, schema_filter};

pub(crate) fn creation_outcomes(
    outcomes: Vec<CreateAclOutcome>,
    expected: &[LiteralAclBinding],
    operation_id: &OperationId,
) -> Result<Vec<AdminAclCreationOutcome>, AdapterError> {
    if outcomes.len() != expected.len() {
        return Err(invalid_result(
            operation_id,
            "returned the wrong ACL creation count",
        ));
    }
    outcomes
        .into_iter()
        .zip(expected)
        .map(|(outcome, expected)| {
            let (binding, result) = outcome.into_parts();
            let binding = schema_binding(&binding, operation_id)?;
            if &binding != expected {
                return Err(invalid_result(
                    operation_id,
                    "reordered ACL creation outcomes",
                ));
            }
            let error_code = match result {
                CreateAclResult::Created => None,
                CreateAclResult::BrokerFailed(error) => Some(broker_code(error.code())),
            };
            Ok(AdminAclCreationOutcome {
                binding,
                error_code,
            })
        })
        .collect()
}

pub(crate) fn deletion_outcomes(
    outcomes: Vec<DeleteAclFilterOutcome>,
    expected: &[LiteralAclBinding],
    operation_id: &OperationId,
) -> Result<Vec<AdminAclDeletionOutcome>, AdapterError> {
    if outcomes.len() != expected.len() {
        return Err(invalid_result(
            operation_id,
            "returned the wrong ACL deletion count",
        ));
    }
    outcomes
        .into_iter()
        .zip(expected)
        .map(|(outcome, expected)| deletion_outcome(outcome, expected, operation_id))
        .collect()
}

fn deletion_outcome(
    outcome: DeleteAclFilterOutcome,
    expected: &LiteralAclBinding,
    operation_id: &OperationId,
) -> Result<AdminAclDeletionOutcome, AdapterError> {
    let (filter, result) = outcome.into_parts();
    let filter = schema_filter(&filter, operation_id)?;
    if &filter != expected {
        return Err(invalid_result(
            operation_id,
            "reordered ACL deletion filters",
        ));
    }
    let (error_code, matches) = match result {
        DeleteAclFilterResult::BrokerFailed(error) => (Some(broker_code(error.code())), Vec::new()),
        DeleteAclFilterResult::Matched(matches) => (
            None,
            matches
                .into_iter()
                .map(|value| {
                    let (binding, result) = value.into_parts();
                    Ok(AdminAclDeleteMatch {
                        binding: schema_binding(&binding, operation_id)?,
                        error_code: match result {
                            DeleteAclMatchResult::Deleted => None,
                            DeleteAclMatchResult::BrokerFailed(error) => {
                                Some(broker_code(error.code()))
                            }
                        },
                    })
                })
                .collect::<Result<Vec<_>, AdapterError>>()?,
        ),
    };
    Ok(AdminAclDeletionOutcome {
        filter,
        error_code,
        matches,
    })
}

fn broker_code(code: i16) -> String {
    format!("broker:broker_{code}")
}
