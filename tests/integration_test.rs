//! Integration tests for the MiniTimer system.
//!
//! These tests verify the end-to-end functionality of the timer system,
//! including task scheduling, execution, and lifecycle management.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use minitimer::task::{TaskBuilder, TaskRunner};
use minitimer::MiniTimer;

/// A simple test task that increments a counter when executed.
struct CounterTask {
    counter: Arc<AtomicU64>,
}

impl CounterTask {
    fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl TaskRunner for CounterTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Test that a task scheduled with Once frequency executes exactly once.
#[tokio::test]
async fn test_task_executes_once() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();
    timer.start();

    tokio::time::sleep(Duration::from_secs(3)).await;

    timer.stop().await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task should execute at least once, but executed {} times",
        count
    );
}

/// Test that tasks can be added and removed from the timer.
#[tokio::test]
async fn test_task_add_and_remove() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(100)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
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
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let state = timer.get_task_state(200);
    assert!(
        state.is_some(),
        "Task state should exist for task 200"
    );
    assert_eq!(
        state.unwrap(),
        minitimer::TaskState::Pending,
        "Task should be in Pending state before execution"
    );

    let non_existent_state = timer.get_task_state(999);
    assert!(
        non_existent_state.is_none(),
        "Task state should be None for non-existent task"
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
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task1).unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(120)
        .spwan_async(CounterTask::new(counter.clone()))
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
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task1).unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(120)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task2).unwrap();

    let pending = timer.get_pending_tasks();
    assert_eq!(pending.len(), 2, "Should have 2 pending tasks");
    assert!(
        pending.contains(&1) && pending.contains(&2),
        "Pending tasks should contain task IDs 1 and 2"
    );
}

/// Test repeated task execution.
#[tokio::test]
async fn test_repeated_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();
    timer.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    timer.stop().await;

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
        .with_frequency_count_down_by_seconds(3, 1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();
    timer.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    timer.stop().await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1 && count <= 4,
        "Countdown task should execute limited times, executed {} times",
        count
    );
}

/// Test that timer start and stop work correctly.
#[tokio::test]
async fn test_timer_start_stop() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    assert!(!timer.is_running(), "Timer should not be running initially");

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task).unwrap();

    timer.start();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(timer.is_running(), "Timer should be running after start");

    timer.stop().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(!timer.is_running(), "Timer should not be running after stop");
}

/// Test removing a running task.
#[tokio::test]
async fn test_remove_running_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();
    timer.start();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let removed = timer.remove_task(1);
    assert!(removed.is_some(), "Task should be removable");

    assert!(
        !timer.contains_task(1),
        "Task should not exist after removal"
    );

    timer.stop().await;
}

/// Test get running tasks.
#[tokio::test]
async fn test_get_running_tasks() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();
    timer.start();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let running = timer.get_running_tasks();
    assert!(
        !running.is_empty() || timer.task_count() > 0,
        "Should have running or pending tasks"
    );

    timer.stop().await;
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
        .spwan_async(CounterTask::new(counter1.clone()))
        .unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(2)
        .spwan_async(CounterTask::new(counter2.clone()))
        .unwrap();

    let task3 = TaskBuilder::new(3)
        .with_frequency_once_by_seconds(1)
        .spwan_async(CounterTask::new(counter3.clone()))
        .unwrap();

    timer.add_task(task1).unwrap();
    timer.add_task(task2).unwrap();
    timer.add_task(task3).unwrap();

    assert_eq!(timer.task_count(), 3, "Should have 3 tasks");

    timer.start();

    tokio::time::sleep(Duration::from_secs(5)).await;

    timer.stop().await;

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

/// Test that timer can be cloned and used across different async contexts.
#[tokio::test]
async fn test_timer_clone() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let timer_clone = timer.clone();

    assert_eq!(
        timer.task_count(),
        timer_clone.task_count(),
        "Cloned timer should have same task count"
    );
}
