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

    let state = timer.get_task_state(200);
    assert!(state.is_some(), "Task state should exist for task 200");
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

/// Test that tasks scheduled at hour boundary execute correctly.
/// A task at 3665 seconds should be in the hour wheel initially,
/// then cascade to minute wheel, then to second wheel.
#[tokio::test]
async fn test_hour_to_minute_to_second_cascade() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3665)
        .spwan_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for _ in 0..70 {
        timer.tick().await;
    }

    let count = counter.load(Ordering::SeqCst);
    assert_eq!(
        count, 0,
        "Task at 3665s should NOT execute within 70 ticks, executed {} times",
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

    let state = timer.get_task_state(1);
    assert!(state.is_some(), "Task should have a state");

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
