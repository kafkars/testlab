//! Protocol-adversary environment validation keeps the exposed topic portable.

use crate::EnvironmentError;

pub(crate) fn validate_topic(topic: &str) -> Result<(), EnvironmentError> {
    let valid = !topic.is_empty()
        && topic.len() <= 249
        && topic.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-')
        });
    if valid {
        Ok(())
    } else {
        Err(EnvironmentError::AdversaryTopicInvalid(topic.to_owned()))
    }
}
