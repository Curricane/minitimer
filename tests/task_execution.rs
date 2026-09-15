//! Integration tests for task execution with different frequencies.
//!
//! These tests verify that tasks execute correctly with various scheduling
//! frequencies: once, repeated, and countdown.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test that a task scheduled with Once frequency executes exactly once.
#[tokio::test]
async fn test_task_executes_once() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(4)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 1,
        "Once task should execute exactly once, executed {} times",
        count
    );
    assert!(
        !timer.contains_task(1),
        "Once task should be removed after its single execution"
    );
    assert_eq!(timer.task_count(), 0, "No task should be left scheduled");
}

/// Test that a task is not fired before its delay has elapsed.
///
/// The wheel hand must advance in step with elapsed time: the first tick of
/// the internal clock used to be delivered immediately, which made every task
/// fire one second early.
#[tokio::test]
async fn test_delay_is_not_shortened() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(2500)).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "Task scheduled for 3 seconds must not run after 2.5 seconds"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        counter.load(Ordering::SeqCst) >= 1,
        "Task scheduled for 3 seconds should have run after 4.5 seconds"
    );
}

/// Test repeated task execution.
#[tokio::test]
async fn test_repeated_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 2,
        "Repeated task should execute multiple times, executed {} times",
        count
    );
}

/// Test countdown task execution.
#[tokio::test]
async fn test_countdown_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_count_down_by_seconds(2, 1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(4)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 2,
        "Countdown task should execute exactly 2 times, executed {} times",
        count
    );
    assert!(
        !timer.contains_task(1),
        "Countdown task should be removed once it has run its executions"
    );
}

/// Test that countdown executions are spaced by the configured interval.
#[tokio::test]
async fn test_countdown_uses_configured_interval() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // 2 executions, 2 seconds apart
    let task = TaskBuilder::new(1)
        .with_frequency_count_down_by_seconds(2, 2)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Only the first execution is due after 2.5 seconds
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "Countdown with a 2 second interval should have run once after 2.5 seconds"
    );

    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "Both countdown executions should have run after 4.5 seconds"
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
    assert_eq!(
        count, 1,
        "Countdown task with 1 execution should execute exactly once, executed {} times",
        count
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

/// Test multiple tasks with different frequencies.
#[tokio::test]
async fn test_multiple_tasks_different_frequencies() {
    let counter1 = Arc::new(AtomicU64::new(0));
    let counter2 = Arc::new(AtomicU64::new(0));
    let counter3 = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task1 = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter1.clone()))
        .unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(2)
        .spawn_async(CounterTask::new(counter2.clone()))
        .unwrap();

    let task3 = TaskBuilder::new(3)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter3.clone()))
        .unwrap();

    timer.add_task(task1).unwrap();
    timer.add_task(task2).unwrap();
    timer.add_task(task3).unwrap();

    assert_eq!(timer.task_count(), 3, "Should have 3 tasks");

    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(
        counter1.load(Ordering::SeqCst) >= 2,
        "Task 1 (1s interval) should run at least 2 times"
    );
    assert!(
        counter2.load(Ordering::SeqCst) >= 1,
        "Task 2 (2s interval) should run at least 1 time"
    );
    assert!(
        counter3.load(Ordering::SeqCst) >= 1,
        "Task 3 (once) should run at least 1 time"
    );
}
