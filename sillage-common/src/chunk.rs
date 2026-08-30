use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Schema version for the on-disk `.meta.json` format.
///
/// Bumped when fields are added, removed, or reordered.
/// The writer writes this value; the reader validates it.
pub const SCHEMA_VERSION: u32 = 1;

/// On-disk chunk metadata, serialized as pretty JSON into `.meta.json`.
///
/// This is the single source of truth for the meta schema.
/// The writer constructs it; the reader deserializes it.
/// Field names and types must stay byte-compatible with existing data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkMeta {
    pub schema_version: u32,
    pub stream: String,
    pub start_slot: u64,
    pub end_slot_exclusive: u64,
    pub first_message_slot: Option<u64>,
    pub last_message_slot: Option<u64>,
    pub message_count: u64,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
    pub recv_ns_first: Option<u64>,
    pub recv_ns_last: Option<u64>,
    pub sealed_reason: String,
    pub index_dimensions: Vec<String>,
}

/// Errors that can arise when iterating length-prefixed frames.
#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("truncated length prefix: fewer than 4 bytes remain")]
    TruncatedLengthPrefix,
    #[error("declared length {declared} exceeds remaining buffer ({remaining})")]
    LengthPastBuffer { declared: usize, remaining: usize },
}

/// Append a length-prefixed frame to `buf`.
///
/// Wire format: `[u32 LE length][payload bytes]`.
pub fn write_len_prefixed(buf: &mut Vec<u8>, payload: &[u8]) {
    let len = payload.len() as u32;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(payload);
}

/// Iterator over length-prefixed frames in a decompressed byte buffer.
///
/// Each call to `next()` reads a `[u32 LE length]` header then yields
/// `&buf[4..4+len]`. On malformed input (truncated header or length
/// past the buffer end) it yields `Err` and stops.
pub struct FrameIter<'a> {
    data: &'a [u8],
}

impl<'a> FrameIter<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> Iterator for FrameIter<'a> {
    type Item = Result<&'a [u8], ChunkError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.is_empty() {
            return None;
        }
        if self.data.len() < 4 {
            self.data = &[];
            return Some(Err(ChunkError::TruncatedLengthPrefix));
        }
        let len =
            u32::from_le_bytes([self.data[0], self.data[1], self.data[2], self.data[3]]) as usize;
        let remaining = self.data.len() - 4;
        if len > remaining {
            let err = Err(ChunkError::LengthPastBuffer {
                declared: len,
                remaining,
            });
            self.data = &[];
            return Some(err);
        }
        let frame = &self.data[4..4 + len];
        self.data = &self.data[4 + len..];
        Some(Ok(frame))
    }
}

/// Convenience: return a `FrameIter` over the given decompressed buffer.
pub fn iter_frames(decompressed: &[u8]) -> FrameIter<'_> {
    FrameIter::new(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_meta_round_trips() {
        let meta = ChunkMeta {
            schema_version: SCHEMA_VERSION,
            stream: "tx".to_string(),
            start_slot: 0,
            end_slot_exclusive: 1000,
            first_message_slot: Some(0),
            last_message_slot: Some(999),
            message_count: 100,
            uncompressed_bytes: 4096,
            compressed_bytes: 1024,
            recv_ns_first: Some(1_000_000),
            recv_ns_last: Some(2_000_000),
            sealed_reason: "watermark".to_string(),
            index_dimensions: vec![
                "program_id".to_string(),
                "account_key".to_string(),
                "signature".to_string(),
                "vote_flag".to_string(),
                "failed_flag".to_string(),
            ],
        };
        let json = serde_json::to_string(&meta).unwrap();
        let back: ChunkMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(meta, back);
    }

    #[test]
    fn chunk_meta_deserializes_writer_pretty_json() {
        let pretty = r#"{
  "schema_version": 1,
  "stream": "tx",
  "start_slot": 0,
  "end_slot_exclusive": 1000,
  "first_message_slot": 0,
  "last_message_slot": 99,
  "message_count": 100,
  "uncompressed_bytes": 12345,
  "compressed_bytes": 6789,
  "recv_ns_first": 1000000,
  "recv_ns_last": 2000000,
  "sealed_reason": "watermark",
  "index_dimensions": [
    "program_id",
    "account_key",
    "signature",
    "vote_flag",
    "failed_flag"
  ]
}"#;
        let meta: ChunkMeta = serde_json::from_str(pretty).unwrap();
        assert_eq!(meta.schema_version, 1);
        assert_eq!(meta.stream, "tx");
        assert_eq!(meta.start_slot, 0);
        assert_eq!(meta.end_slot_exclusive, 1000);
        assert_eq!(meta.first_message_slot, Some(0));
        assert_eq!(meta.last_message_slot, Some(99));
        assert_eq!(meta.message_count, 100);
        assert_eq!(meta.uncompressed_bytes, 12345);
        assert_eq!(meta.compressed_bytes, 6789);
        assert_eq!(meta.recv_ns_first, Some(1_000_000));
        assert_eq!(meta.recv_ns_last, Some(2_000_000));
        assert_eq!(meta.sealed_reason, "watermark");
        assert_eq!(
            meta.index_dimensions,
            vec![
                "program_id",
                "account_key",
                "signature",
                "vote_flag",
                "failed_flag"
            ]
        );
    }

    #[test]
    fn write_len_prefixed_then_iter_round_trips() {
        let payloads: &[&[u8]] = &[b"hello", b"world", b"foo"];
        let mut buf = Vec::new();
        for p in payloads {
            write_len_prefixed(&mut buf, p);
        }
        let frames: Vec<&[u8]> = iter_frames(&buf).map(|r| r.unwrap()).collect();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0], b"hello");
        assert_eq!(frames[1], b"world");
        assert_eq!(frames[2], b"foo");
    }

    #[test]
    fn iter_frames_errs_on_truncated_length_prefix() {
        let buf = [0u8, 0];
        let mut iter = iter_frames(&buf);
        let result = iter.next();
        assert!(matches!(
            result,
            Some(Err(ChunkError::TruncatedLengthPrefix))
        ));
        // iterator stops after error
        assert!(iter.next().is_none());
    }

    #[test]
    fn iter_frames_errs_on_length_past_buffer() {
        // len prefix says 100, only 10 bytes follow
        let len_bytes = 100u32.to_le_bytes();
        let mut buf = Vec::with_capacity(4 + 10);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(&[0u8; 10]);
        let mut iter = iter_frames(&buf);
        let result = iter.next();
        match result {
            Some(Err(ChunkError::LengthPastBuffer {
                declared,
                remaining,
            })) => {
                assert_eq!(declared, 100);
                assert_eq!(remaining, 10);
            }
            other => panic!("expected LengthPastBuffer, got {other:?}"),
        }
        assert!(iter.next().is_none());
    }

    #[test]
    fn iter_frames_empty_buffer_yields_nothing() {
        let buf: &[u8] = b"";
        let mut iter = iter_frames(buf);
        assert!(iter.next().is_none());
    }
}
