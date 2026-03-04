//! Integration tests for task failure handling.
//!
//! These tests verify that failing tasks don't crash the timer
//! and are handled gracefully.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::FailingTask;

/// Test that failing tasks don't crash the timer.
#[tokio::test]
async fn test_failing_task_does_not_crash_timer() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(FailingTask::new(counter.clone()))
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
