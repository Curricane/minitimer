//! Integration tests for concurrency control and boundary conditions.
//!
//! These tests verify max concurrency limits, boundary values,
/// and very long interval task handling.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::{CounterTask, SlowTask};

/// Test max concurrency limit is respected.
#[tokio::test]
async fn test_max_concurrency_respected() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Create a task with max_concurrency = 1
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .with_max_concurrency(1)
        .spawn_async(SlowTask::new(counter.clone(), 500))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to start executing
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Get running tasks - should have at most 1
    let running = timer.get_running_tasks();
    assert!(
        running.len() <= 1,
        "Should have at most 1 running task due to max_concurrency = 1, found {}",
        running.len()
    );

    // Cleanup
    timer.remove_task(1);
}

/// Test task with 1 second interval (minimum practical interval).
#[tokio::test]
async fn test_one_second_interval() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task with 1s interval should execute at least once, executed {} times",
        count
    );
}

/// Test countdown with 1 execution.
#[tokio::test]
async fn test_countdown_one_execution() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_count_down_by_seconds(1, 1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Countdown task with 1 execution should execute at least once, executed {} times",
        count
    );
}

/// Test very long interval task (more than 24 hours).
#[tokio::test]
async fn test_very_long_interval_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // 100000 seconds = ~27.8 hours
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(100000)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Verify task is in pending state
    let status = timer.task_status(1);
    assert!(
        status.is_some() && status.as_ref().unwrap().running_records.is_empty(),
        "Long interval task should be in Pending state"
    );

    // Sleep a short time and verify task doesn't execute
    tokio::time::sleep(Duration::from_secs(2)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Very long interval task should NOT execute within 2 seconds, executed {} times",
        count
    );
}
