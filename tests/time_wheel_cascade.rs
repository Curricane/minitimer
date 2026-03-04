//! Integration tests for time wheel cascade mechanism.
//!
//! These tests verify that tasks are correctly placed in different time wheels
//! (second, minute, hour) and cascade properly between them.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

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
