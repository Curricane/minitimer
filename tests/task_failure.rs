//! Integration tests for task failure handling.
//!
//! These tests verify that failing tasks don't crash the timer
//! and are handled gracefully.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::FailingTask;

/// Captures the log records the library emits so tests can assert on them.
struct CapturingLogger;

static LOG_RECORDS: Mutex<Vec<String>> = Mutex::new(Vec::new());

impl log::Log for CapturingLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if let Ok(mut records) = LOG_RECORDS.lock() {
            records.push(record.args().to_string());
        }
    }

    fn flush(&self) {}
}

fn capture_logs() {
    static LOGGER: CapturingLogger = CapturingLogger;
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        let _ = log::set_logger(&LOGGER);
        log::set_max_level(log::LevelFilter::Warn);
    });
}

/// Test that a failing task is reported to the log.
#[tokio::test]
async fn test_failing_task_is_logged() {
    capture_logs();

    let counter = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new();

    let task = TaskBuilder::new(4242)
        .with_frequency_once_by_seconds(1)
        .spawn_async(FailingTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(counter.load(Ordering::SeqCst), 1, "Task should have run");

    let records = LOG_RECORDS.lock().unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.contains("4242") && record.contains("failed")),
        "The task failure should have been logged, records: {records:?}"
    );
}

/// Test that failing tasks don't crash the timer.
#[tokio::test]
async fn test_failing_task_does_not_crash_timer() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(FailingTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to execute (and fail) multiple times
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Verify timer is still running
    assert!(
        timer.is_running(),
        "Timer should still be running after task failures"
    );

    // Verify task was executed (even though it failed)
    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Failing task should still be executed, executed {} times",
        count
    );
}
