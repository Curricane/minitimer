//! Common test utilities and helper structures for integration tests.

#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use std::future::Future;
use std::task::{Context, Waker};

use async_trait::async_trait;
use minitimer::MiniTimer;
use minitimer::task::TaskRunner;

/// The number of tasks the current runtime is still running.
///
/// A test that drops a timer uses this to see whether the tick source and the
/// event loop survived the timer, instead of guessing from timing alone.
pub fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

/// Waits for `counter` to reach at least `target`, and panics if it never does.
pub async fn wait_for_count(counter: &Arc<AtomicU64>, target: u64) {
    for _ in 0..120 {
        if counter.load(Ordering::SeqCst) >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    panic!(
        "the task ran {} times, expected at least {target}",
        counter.load(Ordering::SeqCst)
    );
}

/// Waits for the runtime to shed the tasks a timer left behind, and reports how
/// many are still alive so the caller can assert on the number.
pub async fn wait_for_alive_tasks(baseline: usize) -> usize {
    let mut alive = alive_tasks();

    for _ in 0..100 {
        if alive <= baseline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        alive = alive_tasks();
    }

    alive
}

/// Leaves a tick in the timer's event channel without waiting for it.
///
/// The future is polled once and then dropped. The channel is empty when this
/// is called, so that first poll is where the event is queued; the wait for the
/// tick to be applied is what keeps the poll pending.
pub fn send_tick_without_waiting(timer: &MiniTimer) {
    let mut tick = std::pin::pin!(timer.tick());
    let mut context = Context::from_waker(Waker::noop());

    assert!(
        tick.as_mut().poll(&mut context).is_pending(),
        "the wait for its own tick has to keep the first poll pending"
    );
}

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

/// A task that reports how many executions overlap in time.
pub struct ConcurrencyProbe {
    active: Arc<AtomicU64>,
    peak: Arc<AtomicU64>,
    delay_ms: u64,
}

impl ConcurrencyProbe {
    pub fn new(active: Arc<AtomicU64>, peak: Arc<AtomicU64>, delay_ms: u64) -> Self {
        Self {
            active,
            peak,
            delay_ms,
        }
    }

    /// The highest number of executions that were running at the same time.
    pub fn peak(peak: &Arc<AtomicU64>) -> u64 {
        peak.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl TaskRunner for ConcurrencyProbe {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let running = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(running, Ordering::SeqCst);

        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;

        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(())
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
