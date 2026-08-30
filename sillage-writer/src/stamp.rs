use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy)]
pub(crate) struct RecvStamp {
    /// Monotonic time since process start. Read only by stream-ordering tests
    /// today; reserved for Phase 7 wall-clock pacing in the reader.
    #[allow(dead_code)]
    pub mono: Instant,
    /// Wall-clock UNIX nanoseconds. Use for meta.json + replay pacing.
    pub wall_ns: u64,
}

impl RecvStamp {
    pub fn now() -> Self {
        Self {
            mono: Instant::now(),
            wall_ns: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before UNIX epoch")
                .as_nanos() as u64,
        }
    }
}

#[derive(Debug)]
pub(crate) struct Stamped<T> {
    pub recv: RecvStamp,
    pub inner: T,
}

impl<T> Stamped<T> {
    pub fn new(inner: T) -> Self {
        Self {
            recv: RecvStamp::now(),
            inner,
        }
    }
}
