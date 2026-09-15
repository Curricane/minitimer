//! Integration tests for error handling scenarios.
//!
//! These tests verify proper error handling for duplicate tasks,
//! non-existent tasks, and invalid operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test adding a task with an existing ID replaces the previous schedule.
#[tokio::test]
async fn test_add_duplicate_task() {
    let replaced_counter = Arc::new(AtomicU64::new(0));
    let replacement_counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task1 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(2)
        .spawn_async(CounterTask::new(replaced_counter.clone()))
        .unwrap();

    let task2 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(5)
        .spawn_async(CounterTask::new(replacement_counter.clone()))
        .unwrap();

    timer.add_task(task1).unwrap();

    // Adding task with same ID should succeed (current implementation allows replacement)
    let result = timer.add_task(task2);
    assert!(
        result.is_ok(),
        "Adding task with duplicate ID should be allowed (replaces existing)"
    );

    // Verify only one task exists
    assert_eq!(
        timer.task_count(),
        1,
        "Should have only 1 task after replacement"
    );

    // The replaced schedule must not fire
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        replaced_counter.load(Ordering::SeqCst),
        0,
        "The replaced task should have been cancelled"
    );

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        replacement_counter.load(Ordering::SeqCst),
        1,
        "The replacement should have run once"
    );
}

/// Test that a task whose first alarm already passed is still schedulable.
///
/// `spawn_async` computes the first alarm from the time the builder is called,
/// so handing a task to the timer later than its delay must not panic.
#[tokio::test]
async fn test_add_task_with_past_alarm_timestamp() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    // Let the task's first alarm timestamp fall into the past before adding it.
    tokio::time::sleep(Duration::from_secs(2)).await;

    timer
        .add_task(task)
        .expect("A task with a passed alarm time should still be accepted");

    assert!(timer.contains_task(1), "Task should be scheduled");

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(
        counter.load(Ordering::SeqCst) >= 1,
        "Stale task should run on the next tick, executed {} times",
        counter.load(Ordering::SeqCst)
    );
}

/// Test removing a non-existent task returns None.
#[tokio::test]
async fn test_remove_nonexistent_task() {
    let timer = MiniTimer::new();

    let removed = timer.remove_task(999);
    assert!(
        removed.is_none(),
        "Removing non-existent task should return None"
    );
}

/// Test querying state of non-existent task returns None.
#[tokio::test]
async fn test_query_nonexistent_task_state() {
    let timer = MiniTimer::new();

    let status = timer.task_status(999);
    assert!(
        status.is_none(),
        "Querying non-existent task status should return None"
    );
}
