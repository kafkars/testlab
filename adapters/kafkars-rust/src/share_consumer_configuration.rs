//! Share consumer policy maps back from the exact public builder used for registration.

use std::time::Duration;

use testlab_schema::{ShareConsumerBuilderSelection, ShareConsumerFetchConfiguration};

use crate::kafkars_api::{ShareConsumerBuilder, ShareConsumerFetchConfig};
use crate::state::StateError;

pub(crate) fn selected(
    builder: &ShareConsumerBuilder,
    configured_fetch: bool,
) -> Result<ShareConsumerBuilderSelection, StateError> {
    Ok(ShareConsumerBuilderSelection {
        rack: builder.selected_rack().map(str::to_owned),
        fetch: configured_fetch
            .then(|| selected_fetch(builder.selected_fetch_config()))
            .transpose()?,
        membership_start_timeout_ns: selected_nanos(
            builder.selected_membership_start_timeout(),
            "membership_start_timeout",
        )?,
        close_timeout_ms: whole_millis(builder.selected_close_timeout(), "close_timeout")?,
    })
}

fn selected_fetch(
    selected: ShareConsumerFetchConfig,
) -> Result<ShareConsumerFetchConfiguration, StateError> {
    Ok(ShareConsumerFetchConfiguration {
        max_wait_ms: whole_millis(selected.max_wait(), "fetch.max_wait")?,
        min_bytes: selected_u64(selected.min_bytes(), "fetch.min_bytes")?,
        max_bytes: selected_u64(selected.max_bytes(), "fetch.max_bytes")?,
        max_records: selected_u32(selected.max_records(), "fetch.max_records")?,
        batch_size: selected_u32(selected.batch_size(), "fetch.batch_size")?,
        attempt_timeout_ms: whole_millis(selected.attempt_timeout(), "fetch.attempt_timeout")?,
    })
}

fn whole_millis(value: Duration, field: &str) -> Result<u64, StateError> {
    let millis = u64::try_from(value.as_millis()).map_err(|_| invalid(field))?;
    if Duration::from_millis(millis) != value {
        return Err(invalid(field));
    }
    Ok(millis)
}

fn selected_nanos(value: Duration, field: &str) -> Result<u64, StateError> {
    u64::try_from(value.as_nanos()).map_err(|_| invalid(field))
}

fn selected_u64(value: usize, field: &str) -> Result<u64, StateError> {
    u64::try_from(value).map_err(|_| invalid(field))
}

fn selected_u32(value: usize, field: &str) -> Result<u32, StateError> {
    u32::try_from(value).map_err(|_| invalid(field))
}

fn invalid(field: &str) -> StateError {
    StateError::ShareSurface(format!(
        "selected {field} was not representable in the portable protocol"
    ))
}
