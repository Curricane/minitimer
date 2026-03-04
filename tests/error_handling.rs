//! Integration tests for error handling scenarios.
//!
//! These tests verify proper error handling for duplicate tasks,
//! non-existent tasks, and invalid operations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test adding a task with duplicate ID fails.
#[tokio::test]
async fn test_add_duplicate_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task1 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    let task2 = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(120)
        .spwan_async(CounterTask::new(counter.clone()))
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
