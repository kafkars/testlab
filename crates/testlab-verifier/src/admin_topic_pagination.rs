//! Exact page and cursor matching distinguishes explicit pagination from aggregation.

use testlab_schema::{AdminTopicDescriptionPage, DescribeTopicAction, TopicDescriptionApi};

pub(super) fn matches(action: &DescribeTopicAction, actual: &[AdminTopicDescriptionPage]) -> bool {
    match action.api {
        TopicDescriptionApi::Metadata => actual.is_empty(),
        TopicDescriptionApi::DescribeTopicPartitions => explicit_pages_match(action, actual),
    }
}

fn explicit_pages_match(
    action: &DescribeTopicAction,
    actual: &[AdminTopicDescriptionPage],
) -> bool {
    let (Some(pagination), Some(expected)) = (
        action.pagination,
        action.expected_page_partitions.as_deref(),
    ) else {
        return false;
    };
    actual.len() == expected.len()
        && actual.iter().enumerate().all(|(index, page)| {
            page.partitions == expected[index]
                && page.partitions.len()
                    <= usize::try_from(pagination.response_partition_limit).unwrap_or(usize::MAX)
                && cursor_matches(action, expected, index, page)
        })
}

fn cursor_matches(
    action: &DescribeTopicAction,
    expected: &[Vec<i32>],
    index: usize,
    page: &AdminTopicDescriptionPage,
) -> bool {
    let expected_partition = action
        .pagination
        .filter(|pagination| pagination.follow_cursors)
        .and_then(|_| expected.get(index + 1))
        .and_then(|partitions| partitions.first())
        .copied();
    match (page.next_cursor.as_ref(), expected_partition) {
        (None, None) => true,
        (Some(cursor), Some(partition)) => {
            cursor.topic_name == action.topic && cursor.partition_index == partition
        }
        _ => false,
    }
}
