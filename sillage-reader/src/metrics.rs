use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

// Replay metrics
pub const MESSAGES_SENT_TOTAL: &str = "reader_messages_sent_total";
pub const BYTES_SENT_TOTAL: &str = "reader_bytes_sent_total";
pub const REPLAY_LAG_SECONDS: &str = "reader_replay_lag_seconds";
pub const REPLAY_DROPPED_TOTAL: &str = "reader_replay_dropped_total";

// Connection metrics
pub const ACTIVE_CONNECTIONS: &str = "reader_active_connections";
pub const CONNECTIONS_TOTAL: &str = "reader_connections_total";
pub const CONNECTIONS_REJECTED_TOTAL: &str = "reader_connections_rejected_total";

// Chunk cache metrics
pub const CHUNK_CACHE_HITS_TOTAL: &str = "reader_chunk_cache_hits_total";
pub const CHUNK_CACHE_MISSES_TOTAL: &str = "reader_chunk_cache_misses_total";
pub const CHUNK_DECODE_SECONDS: &str = "reader_chunk_decode_seconds";

// Index cache metrics
pub const INDEX_CACHE_HITS_TOTAL: &str = "reader_index_cache_hits_total";
pub const INDEX_CACHE_MISSES_TOTAL: &str = "reader_index_cache_misses_total";

// R2 sync metrics
pub const R2_CHUNKS_FETCHED_TOTAL: &str = "reader_r2_chunks_fetched_total";
pub const R2_BYTES_DOWNLOADED_TOTAL: &str = "reader_r2_bytes_downloaded_total";
pub const R2_FETCH_SECONDS: &str = "reader_r2_fetch_seconds";

// Readiness metric
pub const READER_READY: &str = "reader_ready";

pub fn describe() {
    describe_counter!(
        MESSAGES_SENT_TOTAL,
        Unit::Count,
        "Total number of messages successfully emitted to subscribers"
    );
    describe_counter!(
        BYTES_SENT_TOTAL,
        Unit::Bytes,
        "Total number of bytes successfully emitted to subscribers"
    );
    describe_histogram!(
        REPLAY_LAG_SECONDS,
        Unit::Seconds,
        "Replay lag per message (time between target emit time and actual emit time)"
    );
    describe_counter!(
        REPLAY_DROPPED_TOTAL,
        Unit::Count,
        "Total number of messages dropped due to lag or send timeout"
    );

    describe_gauge!(
        ACTIVE_CONNECTIONS,
        Unit::Count,
        "Number of currently active subscriber connections"
    );
    describe_counter!(
        CONNECTIONS_TOTAL,
        Unit::Count,
        "Total number of subscriber connections accepted"
    );
    describe_counter!(
        CONNECTIONS_REJECTED_TOTAL,
        Unit::Count,
        "Subscriber connections refused for exceeding a concurrency limit"
    );

    describe_counter!(
        CHUNK_CACHE_HITS_TOTAL,
        Unit::Count,
        "Total number of chunk cache hits"
    );
    describe_counter!(
        CHUNK_CACHE_MISSES_TOTAL,
        Unit::Count,
        "Total number of chunk cache misses"
    );
    describe_histogram!(
        CHUNK_DECODE_SECONDS,
        Unit::Seconds,
        "Time spent decoding a chunk from zstd"
    );

    describe_counter!(
        INDEX_CACHE_HITS_TOTAL,
        Unit::Count,
        "Total number of index cache hits"
    );
    describe_counter!(
        INDEX_CACHE_MISSES_TOTAL,
        Unit::Count,
        "Total number of index cache misses"
    );

    describe_counter!(
        R2_CHUNKS_FETCHED_TOTAL,
        Unit::Count,
        "Total number of chunks fetched from R2"
    );
    describe_counter!(
        R2_BYTES_DOWNLOADED_TOTAL,
        Unit::Bytes,
        "Total number of bytes downloaded from R2"
    );
    describe_histogram!(
        R2_FETCH_SECONDS,
        Unit::Seconds,
        "Time spent fetching a file from R2"
    );

    describe_gauge!(
        READER_READY,
        Unit::Count,
        "Reader readiness flag (1 = ready, 0 = not ready)"
    );
}
