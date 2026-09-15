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
use common::{ConcurrencyProbe, CounterTask, SlowTask};

/// Test max concurrency limit is respected.
///
/// Counts the executions that overlap in time rather than sampling the running
/// task list, so a task that never starts cannot make the assertion pass.
#[tokio::test]
async fn test_max_concurrency_respected() {
    let active = Arc::new(AtomicU64::new(0));
    let peak = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Task runs for 1.5s every second, so a 1 second limit is saturated.
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .with_max_concurrency(1)
        .spawn_async(ConcurrencyProbe::new(active.clone(), peak.clone(), 1500))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(6)).await;

    let peak = ConcurrencyProbe::peak(&peak);
    assert!(
        peak <= 1,
        "max_concurrency = 1 must not allow overlapping executions, peak was {}",
        peak
    );
    assert!(
        active.load(Ordering::SeqCst) <= 1,
        "At most one execution may be in flight"
    );
}

/// Test that sampling the running task list reflects an in-flight execution.
#[tokio::test]
async fn test_running_tasks_are_reported() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(SlowTask::new(counter.clone(), 2000))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(
        timer.get_running_tasks(),
        vec![1],
        "A task whose execution is in flight should be reported as running"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        timer.get_running_tasks().is_empty(),
        "No execution should be in flight once the runner has finished"
    );
    assert!(
        !timer.contains_task(1),
        "A finished task should have left the scheduler"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

/// Test that a concurrency limit of zero still lets the task run.
#[tokio::test]
async fn test_zero_concurrency_limit_is_raised() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .with_max_concurrency(0)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "A task with a zero concurrency limit should still execute"
    );
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
