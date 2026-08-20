//! Real-time pacing for replay.
//!
//! `speed = 0` means "as fast as possible" (no sleeping). `speed = 1` means
//! real-time: the wall-clock delay between events matches the exchange event
//! times. `speed > 1` accelerates.

use std::time::{Duration, Instant};

pub struct ReplayTiming {
    pub speed: f64,
    first_event: Option<Instant>,
    last_ts_ms: Option<u64>,
    last_sleep: Option<Instant>,
}

impl ReplayTiming {
    pub fn new(speed: f64) -> Self {
        Self {
            speed,
            first_event: None,
            last_ts_ms: None,
            last_sleep: None,
        }
    }

    pub fn is_realtime(&self) -> bool {
        self.speed > 0.0
    }

    /// Sleep until the next event should be processed given its exchange time.
    pub async fn pace(&mut self, event_time_ms: u64) {
        if self.speed <= 0.0 {
            return;
        }
        if self.first_event.is_none() {
            self.first_event = Some(Instant::now());
            self.last_ts_ms = Some(event_time_ms);
            return;
        }
        let Some(last_ts) = self.last_ts_ms else {
            return;
        };
        let now = Instant::now();
        let last_sleep = self.last_sleep.unwrap_or(now);

        let real_elapsed = now.duration_since(last_sleep);
        let target_elapsed = Duration::from_secs_f64(
            (event_time_ms.saturating_sub(last_ts)) as f64 / 1000.0 / self.speed,
        );
        self.last_ts_ms = Some(event_time_ms);

        let mut wait = target_elapsed.saturating_sub(real_elapsed);
        if wait.is_zero() {
            return;
        }
        // Never over-sleep the pacing of a burst by more than a bounded amount.
        let max_wait = Duration::from_secs_f64(5.0 / self.speed);
        if wait > max_wait {
            wait = max_wait;
        }
        tokio::time::sleep(wait).await;
        self.last_sleep = Some(Instant::now());
    }
}
