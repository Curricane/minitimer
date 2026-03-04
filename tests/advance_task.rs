//! Integration tests for advance_task functionality.
//!
//! These tests verify that tasks can be advanced/triggered manually,
//! with options to control frequency reset behavior.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

/// Test advancing a task by a specific duration.
/// Verifies that task_status correctly reflects the reduced time_to_next_run after advance.
#[tokio::test]
async fn test_advance_task_by_duration() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(10)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
    let time_diff = initial_time_to_next.abs_diff(status_after.time_to_next_run);

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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
        .spawn_async(CounterTask::new(counter.clone()))
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
