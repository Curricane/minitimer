use std::time::Duration;
use tokio::time::Instant;
use tokio::time::{Interval, interval_at};
pub(crate) struct Clock {
    inner: Interval,
}

impl Clock {
    pub(crate) fn new() -> Self {
        let inner = interval_at(Instant::now(), Duration::from_secs(1));
        Self { inner }
    }

    pub(crate) async fn tick(&mut self) {
        self.inner.tick().await;
    }
}
