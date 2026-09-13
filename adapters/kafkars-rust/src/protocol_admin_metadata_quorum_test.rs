//! Metadata-quorum adapter tests pin Kafka UUID encoding.

use crate::protocol_admin_metadata_quorum::kafka_uuid;

#[test]
fn kafka_uuid_uses_unpadded_url_safe_base64() {
    assert_eq!(
        kafka_uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
        "AAECAwQFBgcICQoLDA0ODw"
    );
}
