pub const IDX_MAGIC: &[u8; 4] = b"SIDX";
pub const IDX_VERSION: u8 = 1;

pub const DIM_PROGRAM_ID: &str = "program_id";
pub const DIM_ACCOUNT_KEY: &str = "account_key";
pub const DIM_SIGNATURE: &str = "signature";
pub const DIM_VOTE_FLAG: &str = "vote_flag";
pub const DIM_FAILED_FLAG: &str = "failed_flag";
pub const DIM_ACCOUNT_PUBKEY: &str = "account_pubkey";
pub const DIM_OWNER_PROGRAM: &str = "owner_program";
pub const DIM_SLOT: &str = "slot";
pub const DIM_PARENT_SLOT: &str = "parent_slot";

#[derive(
    Debug, PartialEq, Eq, Hash, Clone, serde::Serialize, serde::Deserialize, PartialOrd, Ord,
)]
#[serde(untagged)]
pub enum DimValue {
    Bytes(Vec<u8>),
    U64(u64),
    Bool(bool),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DimValueType {
    Pubkey32,
    Signature64,
    U64,
    Bool,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DimEntryHeader {
    pub value: DimValue,
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DimensionHeader {
    pub name: String,
    pub value_type: DimValueType,
    pub entries: Vec<DimEntryHeader>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct IdxHeader {
    pub stream: String,
    pub start_slot: u64,
    pub end_slot: u64,
    pub message_count: u64,
    pub dimensions: Vec<DimensionHeader>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idx_header_msgpack_roundtrip() {
        let header = IdxHeader {
            stream: "tx".to_string(),
            start_slot: 100,
            end_slot: 200,
            message_count: 42,
            dimensions: vec![
                DimensionHeader {
                    name: "program_id".to_string(),
                    value_type: DimValueType::Pubkey32,
                    entries: vec![DimEntryHeader {
                        value: DimValue::Bytes(vec![1, 2, 3, 4]),
                        offset: 0,
                        length: 10,
                    }],
                },
                DimensionHeader {
                    name: "slot".to_string(),
                    value_type: DimValueType::U64,
                    entries: vec![DimEntryHeader {
                        value: DimValue::U64(150),
                        offset: 10,
                        length: 5,
                    }],
                },
                DimensionHeader {
                    name: "vote_flag".to_string(),
                    value_type: DimValueType::Bool,
                    entries: vec![DimEntryHeader {
                        value: DimValue::Bool(true),
                        offset: 15,
                        length: 1,
                    }],
                },
            ],
        };

        let serialized = rmp_serde::to_vec_named(&header).expect("serialize");
        let deserialized: IdxHeader = rmp_serde::from_slice(&serialized).expect("deserialize");

        assert_eq!(header.stream, deserialized.stream);
        assert_eq!(header.start_slot, deserialized.start_slot);
        assert_eq!(header.end_slot, deserialized.end_slot);
        assert_eq!(header.message_count, deserialized.message_count);
        assert_eq!(header.dimensions.len(), deserialized.dimensions.len());

        for (orig_dim, de_dim) in header.dimensions.iter().zip(&deserialized.dimensions) {
            assert_eq!(orig_dim.name, de_dim.name);
            assert_eq!(orig_dim.value_type, de_dim.value_type);
            assert_eq!(orig_dim.entries.len(), de_dim.entries.len());

            for (orig_entry, de_entry) in orig_dim.entries.iter().zip(&de_dim.entries) {
                assert_eq!(orig_entry.value, de_entry.value);
                assert_eq!(orig_entry.offset, de_entry.offset);
                assert_eq!(orig_entry.length, de_entry.length);
            }
        }
    }
}
