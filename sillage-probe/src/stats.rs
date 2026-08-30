use std::collections::BTreeMap;
use std::time::Instant;

use crate::decode::UpdateKind;

pub struct Stats {
    pub started: Instant,
    pub by_kind: BTreeMap<UpdateKind, u64>,
    pub total_msgs: u64,
    pub total_bytes: u64,
    pub first_slot: Option<u64>,
    pub last_slot: Option<u64>,
    pub max_seen_slot: u64,
    pub slot_regressions: u64,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            by_kind: BTreeMap::new(),
            total_msgs: 0,
            total_bytes: 0,
            first_slot: None,
            last_slot: None,
            max_seen_slot: 0,
            slot_regressions: 0,
        }
    }

    pub fn observe(&mut self, kind: UpdateKind, bytes: u64, slot: Option<u64>) {
        self.total_msgs += 1;
        self.total_bytes += bytes;
        *self.by_kind.entry(kind).or_insert(0) += 1;

        if let Some(s) = slot {
            if self.first_slot.is_none() {
                self.first_slot = Some(s);
            }
            self.last_slot = Some(s);

            if s < self.max_seen_slot {
                self.slot_regressions += 1;
            } else if s > self.max_seen_slot {
                self.max_seen_slot = s;
            }
        }
    }

    pub fn render(&self, speed: Option<f64>) -> String {
        let elapsed = self.started.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let msgs_per_sec = if elapsed_secs > 0.0 {
            self.total_msgs as f64 / elapsed_secs
        } else {
            0.0
        };
        let mb_per_sec = if elapsed_secs > 0.0 {
            (self.total_bytes as f64 / (1024.0 * 1024.0)) / elapsed_secs
        } else {
            0.0
        };

        let mut lines = Vec::new();
        lines.push(format!(
            "--- stats (elapsed: {}.{:03}s) ---",
            elapsed.as_secs(),
            elapsed.subsec_millis()
        ));
        lines.push(format!(
            "messages: {} total ({:.1} msg/s)",
            self.total_msgs, msgs_per_sec
        ));
        lines.push(format!(
            "bytes: {} total ({:.2} MB/s)",
            human_bytes(self.total_bytes),
            mb_per_sec
        ));

        let mut kind_parts = Vec::new();
        for (kind, count) in &self.by_kind {
            kind_parts.push(format!("{}={}", kind, count));
        }
        if !kind_parts.is_empty() {
            lines.push(format!("by_kind: {}", kind_parts.join(" ")));
        }

        if let (Some(first), Some(last)) = (self.first_slot, self.last_slot) {
            let span = last.saturating_sub(first);
            lines.push(format!(
                "slots: first={} last={} span={}",
                first, last, span
            ));
            lines.push(format!(
                "max_seen_slot={} regressions={}",
                self.max_seen_slot, self.slot_regressions
            ));

            if let Some(speed_val) = speed {
                let expected_wall = (span as f64 * 0.4) / speed_val;
                lines.push(format!(
                    "pacing (approx): speed={:.1}x, span={} slots, expected_wall={:.1}s",
                    speed_val, span, expected_wall
                ));
            }
        }

        lines.join("\n")
    }
}

fn human_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}
