//! Client creation translation preserves identity policy and withholds its oracle.

use testlab_schema::{AdapterCommand, ClientId, ScenarioAction};

#[test]
fn expected_cluster_identity_crosses_the_boundary_without_the_expected_error() {
    let client_id = ClientId::new("client-1").unwrap_or_else(|error| panic!("client id: {error}"));
    let action = ScenarioAction::CreateClient(testlab_schema::CreateClientAction {
        client_id: client_id.clone(),
        expected_cluster_id: Some("cluster-a".to_owned()),
        expected_error_code: Some("identity".to_owned()),
    });

    let Some((command, _)) = crate::session_command::translate(&action) else {
        panic!("client identity action must translate");
    };

    assert_eq!(
        command,
        AdapterCommand::CreateClient(testlab_schema::CreateClientCommand {
            client_id,
            expected_cluster_id: Some("cluster-a".to_owned()),
        })
    );
    let encoded = serde_json::to_string(&command)
        .unwrap_or_else(|error| panic!("encode client command: {error}"));
    assert!(!encoded.contains("expected_error_code"));
}
