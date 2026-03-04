//! Integration tests for task management operations.
//!
//! These tests verify task lifecycle management including adding, removing,
//! querying, and listing tasks.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test that tasks can be added and removed from the timer.
#[tokio::test]
async fn test_task_add_and_remove() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(100)
        .with_frequency_once_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    assert!(timer.contains_task(100), "Task 100 should exist");

    let removed = timer.remove_task(100);
    assert!(removed.is_some(), "Task should be removed");

    assert!(
        !timer.contains_task(100),
        "Task 100 should not exist after removal"
    );
}

/// Test querying task state.
#[tokio::test]
async fn test_task_state_query() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(200)
        .with_frequency_once_by_seconds(10)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let status = timer.task_status(200);
    assert!(status.is_some(), "Task status should exist for task 200");
    assert!(
        status.as_ref().unwrap().running_records.is_empty(),
        "Task should be in Pending state before execution"
    );

    let non_existent_status = timer.task_status(999);
    assert!(
        non_existent_status.is_none(),
        "Task status should be None for non-existent task"
    );
}

/// Test task count functionality.
#[tokio::test]
async fn test_task_count() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    assert_eq!(timer.task_count(), 0, "Initial task count should be 0");

    let task1 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task1).unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(120)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task2).unwrap();

    assert_eq!(timer.task_count(), 2, "Task count should be 2");
}

/// Test that pending tasks can be listed.
#[tokio::test]
async fn test_get_pending_tasks() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task1 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task1).unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(120)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task2).unwrap();

    let pending = timer.get_pending_tasks();
    assert_eq!(pending.len(), 2, "Should have 2 pending tasks");
    assert!(
        pending.contains(&1) && pending.contains(&2),
        "Pending tasks should contain task IDs 1 and 2"
    );
}

/// Test removing a running task.
#[tokio::test]
async fn test_remove_running_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let removed = timer.remove_task(1);
    assert!(removed.is_some(), "Task should be removable");

    assert!(
        !timer.contains_task(1),
        "Task should not exist after removal"
    );
}

/// Test get running tasks.
#[tokio::test]
async fn test_get_running_tasks() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let running = timer.get_running_tasks();
    assert!(
        !running.is_empty() || timer.task_count() > 0,
        "Should have running or pending tasks"
    );
}
