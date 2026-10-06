use std::time::Duration;

use sillage_common::config::PacingConfig;
use tokio::time::Instant;

/// Upper bound on `spin_us`. The spin blocks a runtime worker, so a typo in
/// config must not be able to park one for seconds at a time.
const MAX_SPIN_US: u64 = 10_000;

/// Wall-clock pacer for a single customer's replay stream.
///
/// The first message with a resolvable timestamp sets the anchor. All later
/// targets are `anchor_real + (msg_ts - anchor_source) / speed_multiplier`.
/// Messages with no resolvable timestamp emit immediately and do not set the
/// anchor.
pub struct Pacer {
    enabled: bool,
    speed_multiplier: f64,
    lag_warn: Duration,
    lag_drop: Duration,
    spin: Duration,
    anchor: Option<(u64, Instant)>,
    last_warn: Option<Instant>,
}

impl Pacer {
    pub fn from_config(cfg: &PacingConfig) -> Self {
        Self {
            enabled: cfg.enabled,
            speed_multiplier: cfg.speed_multiplier,
            lag_warn: Duration::from_millis(cfg.lag_warn_ms),
            lag_drop: Duration::from_millis(cfg.lag_drop_ms),
            spin: Duration::from_micros(cfg.spin_us.min(MAX_SPIN_US)),
            anchor: None,
            last_warn: None,
        }
    }

    /// Compute the `Instant` at which the message at `source_ns` should be
    /// emitted. Returns `Instant::now()` when the pacer is disabled or
    /// `source_ns` is `None`. The first call with `Some(_)` establishes the
    /// anchor.
    pub fn target(&mut self, source_ns: Option<u64>) -> Instant {
        if !self.enabled {
            return Instant::now();
        }
        let Some(ts) = source_ns else {
            return Instant::now();
        };
        match self.anchor {
            None => {
                let now = Instant::now();
                self.anchor = Some((ts, now));
                now
            }
            Some((anchor_ts, anchor_real)) => {
                let delta_ns = ts.saturating_sub(anchor_ts);
                let scaled_ns = (delta_ns as f64 / self.speed_multiplier) as u64;
                anchor_real + Duration::from_nanos(scaled_ns)
            }
        }
    }

    /// Wait until `target`. With `spin_us` unset this is a plain timer sleep,
    /// accurate to about a millisecond. With it set, the timer covers all but
    /// the last `spin_us` and a busy-wait covers the rest, which lands within
    /// tens of microseconds at the cost of a spinning worker.
    pub async fn wait_until(&self, target: Instant) {
        if self.spin.is_zero() {
            tokio::time::sleep_until(target).await;
            return;
        }
        if let Some(coarse) = target.checked_sub(self.spin) {
            tokio::time::sleep_until(coarse).await;
        }
        // Spin on the OS clock: tokio's clock does not advance without an
        // await when paused, and a spin never awaits.
        let remaining = target.saturating_duration_since(Instant::now());
        let deadline = std::time::Instant::now() + remaining;
        while std::time::Instant::now() < deadline {
            std::hint::spin_loop();
        }
    }

    pub fn lag_drop(&self) -> Duration {
        self.lag_drop
    }

    /// Observe the actual emit time relative to `target` and decide what to do.
    pub fn observe_emit(&mut self, target: Instant) -> LagAction {
        if !self.enabled {
            return LagAction::Ok;
        }
        let now = Instant::now();
        if now <= target {
            return LagAction::Ok;
        }
        let lag = now - target;
        if lag >= self.lag_drop {
            return LagAction::Drop { lag };
        }
        if lag >= self.lag_warn {
            let should_warn = self
                .last_warn
                .is_none_or(|t| now.duration_since(t) > Duration::from_secs(10));
            if should_warn {
                self.last_warn = Some(now);
                return LagAction::Warn { lag };
            }
        }
        LagAction::Ok
    }
}

#[derive(Debug, PartialEq)]
pub enum LagAction {
    Ok,
    Warn { lag: Duration },
    Drop { lag: Duration },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn first_message_sets_anchor() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let now = Instant::now();
        let t0 = pacer.target(Some(1_000_000_000));
        assert_eq!(t0, now, "first message should emit immediately (anchor)");

        let t1 = pacer.target(Some(1_000_500_000));
        assert_eq!(
            t1,
            now + Duration::from_nanos(500_000),
            "second message 500ns later should be 500ns after anchor"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn out_of_order_timestamps_clamp_to_anchor() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let anchor_real = Instant::now();
        pacer.target(Some(2_000_000_000));

        let t = pacer.target(Some(1_000_000_000));
        assert_eq!(
            t, anchor_real,
            "out-of-order timestamp should clamp to anchor_real"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn speed_multiplier_compresses_intervals() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 2.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let anchor_real = Instant::now();
        pacer.target(Some(0));

        let t = pacer.target(Some(1_000_000_000));
        assert_eq!(
            t,
            anchor_real + Duration::from_nanos(500_000_000),
            "speed=2.0 should halve a 1s gap to 500ms"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn none_timestamp_emits_immediately_without_setting_anchor() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        pacer.target(None);
        assert!(pacer.anchor.is_none(), "None should not establish anchor");

        let anchor_real = Instant::now();
        let t = pacer.target(Some(1_000_000_000));
        assert_eq!(t, anchor_real, "first Some() sets anchor");
    }

    #[tokio::test(start_paused = true)]
    async fn observe_emit_ok_when_on_time() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 5_000,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let target = Instant::now() + Duration::from_secs(1);
        assert_eq!(pacer.observe_emit(target), LagAction::Ok);
    }

    #[tokio::test(start_paused = true)]
    async fn observe_emit_warns_after_threshold() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 100,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let target = Instant::now();
        tokio::time::advance(Duration::from_millis(200)).await;

        let action = pacer.observe_emit(target);
        assert!(
            matches!(action, LagAction::Warn { .. }),
            "expected Warn, got {action:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn observe_emit_drops_after_threshold() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 100,
            lag_drop_ms: 500,
            spin_us: 0,
        });

        let target = Instant::now();
        tokio::time::advance(Duration::from_secs(1)).await;

        let action = pacer.observe_emit(target);
        assert!(
            matches!(action, LagAction::Drop { .. }),
            "expected Drop, got {action:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn observe_emit_warn_is_rate_limited() {
        let mut pacer = Pacer::from_config(&PacingConfig {
            enabled: true,
            speed_multiplier: 1.0,
            lag_warn_ms: 100,
            lag_drop_ms: 30_000,
            spin_us: 0,
        });

        let target = Instant::now();
        tokio::time::advance(Duration::from_millis(200)).await;

        let first = pacer.observe_emit(target);
        assert!(
            matches!(first, LagAction::Warn { .. }),
            "expected Warn on first, got {first:?}"
        );

        let target2 = Instant::now();
        tokio::time::advance(Duration::from_millis(200)).await;
        let second = pacer.observe_emit(target2);
        assert_eq!(
            second,
            LagAction::Ok,
            "second warn within 10s should be suppressed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_until_without_spin_reaches_target() {
        let pacer = Pacer::from_config(&PacingConfig::default());
        let target = Instant::now() + Duration::from_millis(5);
        pacer.wait_until(target).await;
        assert!(Instant::now() >= target);
    }

    #[tokio::test]
    async fn wait_until_with_spin_is_not_early() {
        let pacer = Pacer::from_config(&PacingConfig {
            spin_us: 2_000,
            ..PacingConfig::default()
        });
        let target = Instant::now() + Duration::from_millis(5);
        pacer.wait_until(target).await;
        assert!(Instant::now() >= target);
    }

    #[tokio::test]
    async fn wait_until_with_spin_returns_for_past_target() {
        let pacer = Pacer::from_config(&PacingConfig {
            spin_us: 2_000,
            ..PacingConfig::default()
        });
        let target = Instant::now();
        tokio::time::sleep(Duration::from_millis(1)).await;
        pacer.wait_until(target).await;
    }

    #[test]
    fn spin_is_capped() {
        let pacer = Pacer::from_config(&PacingConfig {
            spin_us: u64::MAX,
            ..PacingConfig::default()
        });
        assert_eq!(pacer.spin, Duration::from_micros(MAX_SPIN_US));
    }
}
