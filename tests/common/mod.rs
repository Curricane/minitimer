//! Common test utilities and helper structures for integration tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use minitimer::task::TaskRunner;

/// A simple test task that increments a counter when executed.
pub struct CounterTask {
    counter: Arc<AtomicU64>,
}

impl CounterTask {
    pub fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl TaskRunner for CounterTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A slow test task that takes time to execute.
pub struct SlowTask {
    counter: Arc<AtomicU64>,
    delay_ms: u64,
}

impl SlowTask {
    pub fn new(counter: Arc<AtomicU64>, delay_ms: u64) -> Self {
        Self { counter, delay_ms }
    }
}

#[async_trait]
impl TaskRunner for SlowTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// A task that always fails.
pub struct FailingTask {
    counter: Arc<AtomicU64>,
}

impl FailingTask {
    pub fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl TaskRunner for FailingTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Err("Task failed intentionally".into())
    }
}
