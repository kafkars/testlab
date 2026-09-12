//! Configuration-mutation contracts retain exact method and public surface identity.

use testlab_schema::{AlterTopicConfigsAction, TopicConfigMutationMethod};

pub(super) fn alteration(action: &AlterTopicConfigsAction) -> &'static str {
    if action
        .topics
        .iter()
        .any(|topic| topic.method == TopicConfigMutationMethod::RestoreDefault)
    {
        return "ADMIN-079";
    }
    if action
        .topics
        .iter()
        .any(|topic| topic.method != TopicConfigMutationMethod::Set)
    {
        return "ADMIN-080";
    }
    match action.api {
        testlab_schema::TopicConfigMutationApi::Topic => "ADMIN-049",
        testlab_schema::TopicConfigMutationApi::Resource => "ADMIN-065",
        testlab_schema::TopicConfigMutationApi::LegacyTopic => "ADMIN-066",
        testlab_schema::TopicConfigMutationApi::LegacyResource => "ADMIN-067",
    }
}
