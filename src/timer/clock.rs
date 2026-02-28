use std::time::Duration;
use tokio::time::Instant;
use tokio::time::{Interval, interval_at};

/// Internal clock that provides periodic time intervals.
///
/// This is a thin wrapper around tokio's Interval that generates
/// a tick every second.
pub(crate) struct Clock {
    inner: Interval,
}

impl Clock {
    /// Creates a new Clock that ticks every second.
    pub(crate) fn new() -> Self {
        let inner = interval_at(Instant::now(), Duration::from_secs(1));
        Self { inner }
    }

    /// Waits for the next tick (approximately 1 second).
    ///
    /// This is an async method that returns when the next interval occurs.
    pub(crate) async fn tick(&mut self) {
        self.inner.tick().await;
    }
}
