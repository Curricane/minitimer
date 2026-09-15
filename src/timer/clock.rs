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
    ///
    /// The first tick is due one period from now, not immediately: the wheel
    /// hand is expected to advance in step with elapsed time, and tokio fires
    /// the first tick of an interval started at `Instant::now()` right away,
    /// which used to place every task one second early.
    pub(crate) fn new() -> Self {
        let inner = interval_at(
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        Self { inner }
    }

    /// Waits for the next tick (approximately 1 second).
    ///
    /// This is an async method that returns when the next interval occurs.
    pub(crate) async fn tick(&mut self) {
        self.inner.tick().await;
    }
}
