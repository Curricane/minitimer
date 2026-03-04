//! Integration tests for performance scenarios.
//!
//! These tests verify system behavior under load with large numbers
//! of tasks and rapid operations.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test handling a large number of tasks.
#[tokio::test]
async fn test_large_number_of_tasks() {
    let timer = MiniTimer::new();
    let num_tasks: usize = 1000;

    // Add many tasks
    for i in 0..num_tasks {
        let counter = Arc::new(AtomicU64::new(0));
        let task = TaskBuilder::new(i as u64)
            .with_frequency_once_by_seconds(60)
            .spawn_async(CounterTask::new(counter))
            .unwrap();
        timer.add_task(task).unwrap();
    }

    assert_eq!(
        timer.task_count(),
        num_tasks,
        "Should have {} tasks",
        num_tasks
    );

    // Verify we can query pending tasks
    let pending = timer.get_pending_tasks();
    assert_eq!(
        pending.len(),
        num_tasks,
        "Should have {} pending tasks",
        num_tasks
    );
}

/// Test rapid task add and remove operations.
#[tokio::test]
async fn test_rapid_add_remove_operations() {
    let timer = MiniTimer::new();
    let counter = Arc::new(AtomicU64::new(0));

    // Rapidly add and remove tasks
    for i in 0..100 {
        let task = TaskBuilder::new(i)
            .with_frequency_once_by_seconds(60)
            .spawn_async(CounterTask::new(counter.clone()))
            .unwrap();
        timer.add_task(task).unwrap();

        if i % 2 == 0 {
            timer.remove_task(i);
        }
    }

    // Should have approximately 50 tasks remaining
    let count = timer.task_count();
    assert!(
        (50..=100).contains(&count),
        "Should have between 50-100 tasks after rapid add/remove, found {}",
        count
    );
}
