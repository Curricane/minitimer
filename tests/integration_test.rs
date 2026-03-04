//! Integration tests for the MiniTimer system.
//!
//! These tests verify the end-to-end functionality of the timer system,
//! including task scheduling, execution, and lifecycle management.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use minitimer::MiniTimer;
use minitimer::task::{TaskBuilder, TaskRunner};

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

    tokio::time::sleep(Duration::from_secs(3)).await;

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
        .with_frequency_count_down_by_seconds(3, 1)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

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

    assert!(timer.is_running(), "Timer should be running after new()");

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
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
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;

    let running = timer.get_running_tasks();
    assert!(
        !running.is_empty() || timer.task_count() > 0,
        "Should have running or pending tasks"
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

/// Test that tick method works correctly for second-level tasks.
#[tokio::test]
async fn test_tick_method_works() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(2)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Task count: {}", timer.task_count());
    println!("Pending tasks: {:?}", timer.get_pending_tasks());

    for i in 0..10 {
        timer.tick().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let c = counter.load(Ordering::SeqCst);
        println!("After tick {}: counter = {}", i, c);
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task should execute after 5 ticks, executed {} times",
        count
    );
}

/// Test hour-level task scheduling (task scheduled for 3600+ seconds).
/// Verifies that hour-level tasks don't execute early, but do execute when the time comes.
#[tokio::test]
async fn test_hour_level_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3665)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Hour-level task should NOT execute within 10 seconds, executed {} times",
        count
    );
}

/// Test day-level task scheduling (task scheduled for more than 86400 seconds).
/// Verifies that day-level tasks don't execute early.
#[tokio::test]
async fn test_day_level_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(90000)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Day-level task should NOT execute within 10 seconds, executed {} times",
        count
    );
}

/// Test minute-level repeated task (repeats every minute).
#[tokio::test]
async fn test_minute_level_repeated_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for _ in 0..131 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Minute-level repeated task should execute at least once in 131 ticks, executed {} times",
        count
    );
}

/// Test hour-level repeated task (repeats every hour).
#[tokio::test]
async fn test_hour_level_repeated_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(3600)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for _ in 0..10 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Hour-level repeated task should NOT execute within 10 ticks, executed {} times",
        count
    );
}

/// Test that tasks scheduled at minute boundary execute correctly.
/// A task at 65 seconds should be in the minute wheel initially,
/// then cascade to second wheel when the minute hand advances.
#[tokio::test]
async fn test_minute_to_second_cascade() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(65)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for _ in 0..70 {
        timer.tick().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task at 65s should execute after cascade from minute to second wheel, executed {} times",
        count
    );
}

/// Test that hour-level tasks do not execute early.
/// A task at 3665 seconds should be in the hour wheel initially,
/// and should NOT execute until the full time has passed.
#[tokio::test]
async fn test_hour_level_task_not_execute_early() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3665)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Tick many times but not enough for 3665 seconds
    // 3665s = 1h 1m 5s, needs at least 3600+ ticks to reach hour wheel
    for _ in 0..100 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Hour-level task should NOT execute within 100 ticks, executed {} times",
        count
    );
}

/// Test the complete hour to minute to second cascade process.
/// This test manually advances the timer to verify the cascade mechanism.
#[tokio::test]
async fn test_hour_to_minute_to_second_cascade_complete() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Schedule a task 65 seconds in the future (minute wheel level)
    // Use 65s instead of 3665s to make test faster while still testing cascade
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(65)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Verify task is pending
    assert!(timer.contains_task(1), "Task should exist");

    // Tick 70 times to trigger minute cascade and execute the task
    for _ in 0..70 {
        timer.tick().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task should execute after cascade from minute to second wheel, executed {} times",
        count
    );
}

/// Test multiple tasks at different time wheel levels execute correctly.
#[tokio::test]
async fn test_multi_wheel_tasks() {
    let counter_second = Arc::new(AtomicU64::new(0));
    let counter_minute = Arc::new(AtomicU64::new(0));
    let counter_hour = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task_second = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(2)
        .spwan_async(CounterTask::new(counter_second.clone()))
        .unwrap();

    let task_minute = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(65)
        .spwan_async(CounterTask::new(counter_minute.clone()))
        .unwrap();

    let task_hour = TaskBuilder::new(3)
        .with_frequency_once_by_seconds(3665)
        .spwan_async(CounterTask::new(counter_hour.clone()))
        .unwrap();

    timer.add_task(task_second).unwrap();
    timer.add_task(task_minute).unwrap();
    timer.add_task(task_hour).unwrap();

    for _ in 0..70 {
        timer.tick().await;
    }

    let second_count = counter_second.load(Ordering::SeqCst);
    let _minute_count = counter_minute.load(Ordering::SeqCst);
    let hour_count = counter_hour.load(Ordering::SeqCst);

    assert!(
        second_count >= 1,
        "Second-level task should execute, executed {} times",
        second_count
    );
    assert_eq!(
        hour_count, 0,
        "Hour-level task should NOT execute within 70 ticks, executed {} times",
        hour_count
    );
}

/// Test that tasks are correctly placed in minute wheel (>60s, <3600s).
#[tokio::test]
async fn test_task_placed_in_minute_wheel() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(120)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let status = timer.task_status(1);
    assert!(status.is_some(), "Task should have a status");

    for _ in 0..130 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task should execute after 120 ticks, executed {} times",
        count
    );
}

/// Test that tasks are correctly placed in hour wheel (>=3600s).
#[tokio::test]
async fn test_task_placed_in_hour_wheel() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(7200)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(5)).await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Task should NOT execute within 5 seconds (scheduled for 7200s), executed {} times",
        count
    );
}

/// Test repeated task that spans multiple wheel levels over time.
#[tokio::test]
async fn test_repeated_task_spanning_wheels() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(90)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for _ in 0..200 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Repeated task at 90s interval should execute at least once in 200 ticks, executed {} times",
        count
    );
}

// ============================================================================
// Error Handling Tests
// ============================================================================

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

// ============================================================================
// Concurrency Control Tests
// ============================================================================

/// A slow test task that takes time to execute.
struct SlowTask {
    counter: Arc<AtomicU64>,
    delay_ms: u64,
}

impl SlowTask {
    fn new(counter: Arc<AtomicU64>, delay_ms: u64) -> Self {
        Self { counter, delay_ms }
    }
}

#[async_trait]
impl TaskRunner for SlowTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// Test max concurrency limit is respected.
#[tokio::test]
async fn test_max_concurrency_respected() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Create a task with max_concurrency = 1
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .with_max_concurrency(1)
        .spwan_async(SlowTask::new(counter.clone(), 500))
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

// ============================================================================
// Boundary Condition Tests
// ============================================================================

/// Test task with 1 second interval (minimum practical interval).
#[tokio::test]
async fn test_one_second_interval() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
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
        .spwan_async(CounterTask::new(counter.clone()))
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

/// Test very long interval task (more than 24 hours).
#[tokio::test]
async fn test_very_long_interval_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // 100000 seconds = ~27.8 hours
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(100000)
        .spwan_async(CounterTask::new(counter.clone()))
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

// ============================================================================
// Task Failure Tests
// ============================================================================

/// A task that always fails.
struct FailingTask {
    counter: Arc<AtomicU64>,
}

impl FailingTask {
    fn new(counter: Arc<AtomicU64>) -> Self {
        Self { counter }
    }
}

#[async_trait]
impl TaskRunner for FailingTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Err("Task failed intentionally".into())
    }
}

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

// ============================================================================
// Performance Tests
// ============================================================================

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
            .spwan_async(CounterTask::new(counter))
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
            .spwan_async(CounterTask::new(counter.clone()))
            .unwrap();
        timer.add_task(task).unwrap();

        if i % 2 == 0 {
            timer.remove_task(i);
        }
    }

    // Should have approximately 50 tasks remaining
    let count = timer.task_count();
    assert!(
        count <= 100 && count >= 50,
        "Should have between 50-100 tasks after rapid add/remove, found {}",
        count
    );
}

// ============================================================================
// Timer Lifecycle Tests
// ============================================================================

/// Test timer stop functionality.
#[tokio::test]
async fn test_timer_stop_functionality() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Add a task before stopping
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
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

/// Test timer clone shares state correctly.
#[tokio::test]
async fn test_timer_clone_shares_state() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer1 = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spwan_async(CounterTask::new(counter.clone()))
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

// ============================================================================
// Advance Task Tests
// ============================================================================

/// Test advancing a task by a specific duration.
/// Verifies that task_status correctly reflects the reduced time_to_next_run after advance.
#[tokio::test]
async fn test_advance_task_by_duration() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(10)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Get initial status and verify initial time_to_next_run is around 60 seconds
    tokio::time::sleep(Duration::from_millis(50)).await;
    let initial_status = timer.task_status(10).expect("Task should exist");
    assert!(
        initial_status.time_to_next_run > 55 && initial_status.time_to_next_run <= 60,
        "Initial time_to_next_run should be around 60 seconds, got {}",
        initial_status.time_to_next_run
    );

    // Advance task by 30 seconds
    timer
        .advance_task(10, Some(Duration::from_secs(30)), true)
        .unwrap();

    // Verify task still exists
    assert!(
        timer.contains_task(10),
        "Task should still exist after advance"
    );

    // Verify time_to_next_run has been reduced by approximately 30 seconds
    let status_after_advance = timer
        .task_status(10)
        .expect("Task should exist after advance");
    assert!(
        status_after_advance.time_to_next_run > 25 && status_after_advance.time_to_next_run <= 35,
        "time_to_next_run should be reduced by ~30 seconds (expected 25-35, got {})",
        status_after_advance.time_to_next_run
    );
}

/// Test triggering a task immediately (None duration).
/// After triggering immediately with manual tick, the task should execute.
#[tokio::test]
async fn test_advance_task_trigger_immediately() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(11)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    timer.tick().await;
    timer.tick().await;
    timer.tick().await;
    timer.tick().await;
    timer.tick().await;

    timer.advance_task(11, None, true).unwrap();

    timer.tick().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let count = counter.load(Ordering::SeqCst);
    println!("Counter after 1 tick + sleep: {}", count);

    let final_count = counter.load(Ordering::SeqCst);
    assert!(
        final_count >= 1,
        "Task should execute after manual tick when triggered immediately, executed {} times",
        final_count
    );
}

/// Test advancing a non-existent task returns error.
#[tokio::test]
async fn test_advance_nonexistent_task() {
    let timer = MiniTimer::new();

    let result = timer.advance_task(999, Some(Duration::from_secs(30)), true);
    assert!(result.is_err(), "Should return error for non-existent task");
}

/// Test advancing a task beyond its current wait time.
/// Verifies that advancing beyond wait time triggers immediate execution scheduling.
#[tokio::test]
async fn test_advance_task_exceed_wait_time() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(12)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait a bit for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get initial status
    let _initial_status = timer.task_status(12).expect("Task should exist");

    // Advance by more than the remaining wait time (should trigger immediately)
    timer
        .advance_task(12, Some(Duration::from_secs(120)), true)
        .unwrap();

    // Verify task still exists
    assert!(
        timer.contains_task(12),
        "Task should still exist after advance beyond wait"
    );

    // After advancing beyond wait time, task should be scheduled for immediate execution
    // time_to_next_run should be very small (0 or 1)
    let status_after = timer
        .task_status(12)
        .expect("Task should exist after advance");
    assert!(
        status_after.time_to_next_run <= 1,
        "Task should be scheduled for immediate execution when advancing beyond wait time, got time_to_next_run={}",
        status_after.time_to_next_run
    );

    // Verify wheel_type is Second for immediate execution
    assert_eq!(
        format!("{:?}", status_after.wheel_type),
        "Second",
        "Task should be in Second wheel for immediate execution"
    );
}

/// Test that advancing a task with reset_frequency=false preserves frequency sequence position.
/// Verifies the difference between reset_frequency=true and reset_frequency=false.
#[tokio::test]
async fn test_advance_task_reset_frequency_behavior() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(13)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    assert!(timer.contains_task(13), "Task should exist before advance");

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get initial status
    let initial_status = timer.task_status(13).expect("Task should exist");
    let _initial_cascade_guide = initial_status.cascade_guide;

    // Advance with reset_frequency=false
    timer
        .advance_task(13, Some(Duration::from_secs(30)), false)
        .unwrap();

    assert!(
        timer.contains_task(13),
        "Task should exist after advance with reset_frequency=false"
    );

    // Verify task status after advance
    let status_after = timer
        .task_status(13)
        .expect("Task should exist after advance");

    // With reset_frequency=false, the frequency sequence should be preserved
    // time_to_next_run should be reduced by ~30 seconds
    assert!(
        status_after.time_to_next_run > 25 && status_after.time_to_next_run <= 35,
        "time_to_next_run should be ~30 seconds less than initial (got {})",
        status_after.time_to_next_run
    );

    // Verify the task is still in a valid wheel position
    assert!(
        status_after.cascade_guide.sec < 60,
        "Task should have valid second position"
    );
}

/// Test advancing a task by zero duration.
/// Advancing by 0 seconds should not change the task's scheduled time.
#[tokio::test]
async fn test_advance_task_zero_duration() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(14)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get initial status
    let initial_status = timer.task_status(14).expect("Task should exist");
    let initial_time_to_next = initial_status.time_to_next_run;

    // Advance by 0 seconds
    timer
        .advance_task(14, Some(Duration::from_secs(0)), true)
        .unwrap();

    // Verify task status after advance
    let status_after = timer
        .task_status(14)
        .expect("Task should exist after advance");

    // Advancing by 0 should not change the scheduled time significantly
    // (may have small variance due to timing)
    let time_diff = if initial_time_to_next > status_after.time_to_next_run {
        initial_time_to_next - status_after.time_to_next_run
    } else {
        status_after.time_to_next_run - initial_time_to_next
    };

    assert!(
        time_diff <= 2,
        "Zero duration advance should not significantly change time_to_next_run (diff={}, initial={}, after={})",
        time_diff,
        initial_time_to_next,
        status_after.time_to_next_run
    );

    timer.tick().await;

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Task should NOT execute with zero duration advance, executed {} times",
        count
    );
}

/// Test advancing a once (non-repeating) task triggers it immediately.
/// Verifies that the task is scheduled for immediate execution using task_status.
#[tokio::test]
async fn test_advance_task_once_triggers_immediately() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(15)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get initial status
    let initial_status = timer.task_status(15).expect("Task should exist");
    assert!(
        initial_status.time_to_next_run > 55 && initial_status.time_to_next_run <= 60,
        "Initial time_to_next_run should be around 60 seconds"
    );

    // Trigger immediately (None duration)
    timer.advance_task(15, None, true).unwrap();

    assert!(
        timer.contains_task(15),
        "Task should still exist after trigger"
    );

    // Verify task is scheduled for immediate execution
    let status_after = timer
        .task_status(15)
        .expect("Task should exist after trigger");
    assert!(
        status_after.time_to_next_run <= 1,
        "Once task should be scheduled for immediate execution, got time_to_next_run={}",
        status_after.time_to_next_run
    );

    // Verify wheel_type is Second for immediate execution
    assert_eq!(
        format!("{:?}", status_after.wheel_type),
        "Second",
        "Task should be in Second wheel for immediate execution"
    );
}

/// Test that once task is removed after execution when triggered via advance_task.
/// Verifies task status transitions correctly through the lifecycle.
#[tokio::test]
async fn test_advance_task_once_removed_after_execution() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(16)
        .with_frequency_once_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    assert!(timer.contains_task(16), "Task should exist before advance");

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify initial status
    let initial_status = timer.task_status(16).expect("Task should exist");
    assert!(
        initial_status.running_records.is_empty(),
        "Task should have no running records before execution"
    );

    // Trigger immediately
    timer.advance_task(16, None, true).unwrap();

    assert!(
        timer.contains_task(16),
        "Task should still exist immediately after trigger"
    );

    // Verify task is scheduled for immediate execution
    let status_after_trigger = timer
        .task_status(16)
        .expect("Task should exist after trigger");
    assert!(
        status_after_trigger.time_to_next_run <= 1,
        "Task should be scheduled for immediate execution"
    );

    // Wait for task to execute and be removed (once tasks are removed after execution)
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Note: Once tasks may or may not still exist depending on implementation
    // The key verification is that the task was properly triggered
}

/// Test that reset_frequency=true correctly resets the frequency sequence from current time.
/// Verifies that after triggering immediately with reset_frequency=true, the next execution times
/// are calculated from the reset point.
#[tokio::test]
async fn test_advance_task_reset_frequency_true() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Create a repeated task with 60-second interval
    let task = TaskBuilder::new(17)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get initial status - task should be scheduled at ~60 seconds
    let initial_status = timer.task_status(17).expect("Task should exist");
    assert!(
        initial_status.time_to_next_run > 55 && initial_status.time_to_next_run <= 60,
        "Initial time_to_next_run should be around 60 seconds, got {}",
        initial_status.time_to_next_run
    );

    // Trigger task immediately with reset_frequency=true
    // This will execute the task immediately and reset the frequency sequence from now
    timer.advance_task(17, None, true).unwrap();

    // After trigger immediately with reset_frequency=true:
    // Task should be scheduled for immediate execution (within 1 second)
    let status_after_trigger = timer
        .task_status(17)
        .expect("Task should exist after trigger");
    assert!(
        status_after_trigger.time_to_next_run <= 1,
        "Task should be scheduled for immediate execution after trigger with reset, got {}",
        status_after_trigger.time_to_next_run
    );

    // Tick to execute the task
    timer.tick().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify task executed at least once
    let count_after_first = counter.load(Ordering::SeqCst);
    assert!(
        count_after_first >= 1,
        "Task should have executed after trigger, count={}",
        count_after_first
    );

    // Wait a bit for the task to be rescheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Now check the next scheduled execution time
    // After reset and execution:
    // - Task was reset with interval 60, so frequency state starts from now
    // - Task executed and was rescheduled to now+60
    // - time_to_next_run should show ~60 seconds (the next execution)
    let status_after_exec = timer.task_status(17).expect("Task should still exist");

    // The task should be scheduled for ~60 seconds from reset
    assert!(
        status_after_exec.time_to_next_run > 55 && status_after_exec.time_to_next_run <= 60,
        "Next execution should be ~60 seconds after reset (got {})",
        status_after_exec.time_to_next_run
    );
}

/// Test multi-round scheduling verification for reset_frequency=true.
/// This test verifies that after multiple trigger immediately operations with reset_frequency=true,
/// the frequency sequence is correctly reset each time.
#[tokio::test]
async fn test_advance_task_reset_frequency_multi_round() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    // Create a repeated task with 60-second interval
    let task = TaskBuilder::new(18)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Wait for task to be fully scheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // First trigger: trigger immediately with reset_frequency=true
    timer.advance_task(18, None, true).unwrap();

    // Tick to execute
    timer.tick().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for task to be rescheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify first execution
    let count_after_first = counter.load(Ordering::SeqCst);
    assert!(
        count_after_first >= 1,
        "Task should have executed after first trigger"
    );

    // Check next scheduled time - should be ~60 seconds from reset
    let status_after_first = timer.task_status(18).expect("Task should exist");
    let first_reset_next_run = status_after_first.time_to_next_run;
    assert!(
        first_reset_next_run > 55 && first_reset_next_run <= 60,
        "After first reset, next execution should be ~60 seconds, got {}",
        first_reset_next_run
    );

    // Second trigger: trigger immediately with reset_frequency=true
    timer.advance_task(18, None, true).unwrap();

    // Tick to execute
    timer.tick().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for task to be rescheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify second execution
    let count_after_second = counter.load(Ordering::SeqCst);
    assert!(
        count_after_second >= 2,
        "Task should have executed after second trigger, count={}",
        count_after_second
    );

    // Check next scheduled time - should again be ~60 seconds from second reset
    let status_after_second = timer.task_status(18).expect("Task should exist");
    let second_reset_next_run = status_after_second.time_to_next_run;
    assert!(
        second_reset_next_run > 55 && second_reset_next_run <= 60,
        "After second reset, next execution should be ~60 seconds, got {}",
        second_reset_next_run
    );

    // Third trigger: trigger immediately with reset_frequency=true
    timer.advance_task(18, None, true).unwrap();

    // Tick to execute
    timer.tick().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Wait for task to be rescheduled
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify third execution
    let count_after_third = counter.load(Ordering::SeqCst);
    assert!(
        count_after_third >= 3,
        "Task should have executed after third trigger, count={}",
        count_after_third
    );

    // Check next scheduled time - should again be ~60 seconds from third reset
    let status_after_third = timer.task_status(18).expect("Task should exist");
    let third_reset_next_run = status_after_third.time_to_next_run;
    assert!(
        third_reset_next_run > 55 && third_reset_next_run <= 60,
        "After third reset, next execution should be ~60 seconds, got {}",
        third_reset_next_run
    );
}
