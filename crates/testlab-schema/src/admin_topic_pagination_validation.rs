//! Topic-description pagination validation keeps page intent explicit and bounded.

use crate::{DescribeTopicAction, TopicDescriptionApi};

const MAX_PAGE_PARTITIONS: u32 = 10_000;
const MAX_EXPECTED_PAGES: usize = 10_000;

pub(super) fn validate(action: &DescribeTopicAction, problems: &mut Vec<String>) {
    match action.api {
        TopicDescriptionApi::Metadata => validate_metadata(action, problems),
        TopicDescriptionApi::DescribeTopicPartitions => validate_pages(action, problems),
    }
}

fn validate_metadata(action: &DescribeTopicAction, problems: &mut Vec<String>) {
    if action.pagination.is_some() || action.expected_page_partitions.is_some() {
        problems.push(format!(
            "admin operation {} metadata description cannot declare topic-partition pagination",
            action.operation_id
        ));
    }
}

fn validate_pages(action: &DescribeTopicAction, problems: &mut Vec<String>) {
    let Some(pagination) = action.pagination else {
        problems.push(format!(
            "admin operation {} DescribeTopicPartitions requires pagination controls",
            action.operation_id
        ));
        return;
    };
    if !(1..=MAX_PAGE_PARTITIONS).contains(&pagination.response_partition_limit) {
        problems.push(format!(
            "admin operation {} response_partition_limit must be between 1 and {MAX_PAGE_PARTITIONS}",
            action.operation_id
        ));
    }
    let expected = action.expected_partitions.as_deref();
    let pages = action.expected_page_partitions.as_deref();
    match (expected, pages) {
        (Some(expected), Some(pages)) => {
            validate_expected_pages(action, pagination, expected, pages, problems);
        }
        (Some(_), None) => problems.push(format!(
            "admin operation {} DescribeTopicPartitions success requires expected_page_partitions",
            action.operation_id
        )),
        (None, Some(_)) => problems.push(format!(
            "admin operation {} failed description cannot declare expected_page_partitions",
            action.operation_id
        )),
        (None, None) => {}
    }
}

fn validate_expected_pages(
    action: &DescribeTopicAction,
    pagination: crate::TopicDescriptionPagination,
    expected: &[i32],
    pages: &[Vec<i32>],
    problems: &mut Vec<String>,
) {
    let limit = usize::try_from(pagination.response_partition_limit).unwrap_or(usize::MAX);
    if pages.is_empty() || pages.len() > MAX_EXPECTED_PAGES {
        problems.push(format!(
            "admin operation {} expected_page_partitions must contain 1 to {MAX_PAGE_PARTITIONS} pages",
            action.operation_id
        ));
    }
    if pages.iter().any(|page| {
        page.is_empty()
            || page.len() > limit
            || page.iter().any(|partition| *partition < 0)
            || page.windows(2).any(|pair| pair[0] >= pair[1])
    }) {
        problems.push(format!(
            "admin operation {} expected pages must be nonempty sorted unique nonnegative partitions within response_partition_limit",
            action.operation_id
        ));
    }
    let flattened = pages.iter().flatten().copied().collect::<Vec<_>>();
    if flattened != expected {
        problems.push(format!(
            "admin operation {} expected page partitions must exactly flatten to expected_partitions",
            action.operation_id
        ));
    }
    match (pagination.follow_cursors, pages.len()) {
        (true, 0 | 1) => problems.push(format!(
            "admin operation {} follow_cursors requires at least two expected pages",
            action.operation_id
        )),
        (false, count) if count != 1 => problems.push(format!(
            "admin operation {} without follow_cursors requires exactly one expected page",
            action.operation_id
        )),
        _ => {}
    }
    if pagination.follow_cursors
        && pages
            .iter()
            .take(pages.len().saturating_sub(1))
            .any(|page| page.len() != limit)
    {
        problems.push(format!(
            "admin operation {} each continued page must fill response_partition_limit",
            action.operation_id
        ));
    }
}
