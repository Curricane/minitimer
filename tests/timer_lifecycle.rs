//! Integration tests for timer lifecycle management.
//!
//! These tests verify timer start/stop functionality, cloning behavior,
//! and state sharing between cloned timers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test that timer start and stop work correctly.
#[tokio::test]
async fn test_timer_start_stop() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    assert!(timer.is_running(), "Timer should be running after new()");

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    timer.stop().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        !timer.is_running(),
        "Timer should not be running after stop"
    );
}

/// Test timer stop functionality.
#[tokio::test]
async fn test_timer_stop_functionality() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Add a task before stopping
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to execute a few times
    tokio::time::sleep(Duration::from_secs(3)).await;

    let count_before_stop = counter.load(Ordering::SeqCst);
    assert!(
        count_before_stop >= 1,
        "Task should execute before stopping, executed {} times",
        count_before_stop
    );

    // Stop the timer
    timer.stop().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify timer is stopped
    assert!(
        !timer.is_running(),
        "Timer should not be running after stop"
    );
}

/// Test that timer can be cloned and used across different async contexts.
#[tokio::test]
async fn test_timer_clone() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let timer_clone = timer.clone();

    assert_eq!(
        timer.task_count(),
        timer_clone.task_count(),
        "Cloned timer should have same task count"
    );
}

/// Test timer clone shares state correctly.
#[tokio::test]
async fn test_timer_clone_shares_state() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer1 = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer1.add_task(task).unwrap();

    let timer2 = timer1.clone();

    // Both timers should see the same task
    assert!(timer1.contains_task(1), "Timer1 should see the task");
    assert!(timer2.contains_task(1), "Timer2 should see the task");

    assert_eq!(
        timer1.task_count(),
        timer2.task_count(),
        "Both timers should have same task count"
    );

    // Remove from one timer, should be removed from both
    timer2.remove_task(1);

    assert!(
        !timer1.contains_task(1),
        "Task should be removed from timer1"
    );
    assert!(
        !timer2.contains_task(1),
        "Task should be removed from timer2"
    );
}
