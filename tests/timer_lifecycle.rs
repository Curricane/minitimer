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
use common::{CounterTask, alive_tasks, wait_for_alive_tasks, wait_for_count};

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

/// Test that a stopped timer no longer consumes timer events.
#[tokio::test]
async fn test_timer_stop_ends_the_event_loop() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;

    let before_stop = counter.load(Ordering::SeqCst);
    assert!(
        before_stop >= 1,
        "Task should execute before stopping, executed {} times",
        before_stop
    );

    timer.stop().await;
    assert!(!timer.is_running(), "Timer should be stopped");

    // The event loop is gone, so manual ticks are ignored
    for _ in 0..5 {
        timer.tick().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        counter.load(Ordering::SeqCst),
        before_stop,
        "A stopped timer must not process any further event"
    );

    // Stopping again is harmless
    timer.stop().await;
}

/// Test that a timer does not outlive its last handle.
///
/// A timer that is dropped instead of stopped used to keep its tick source and
/// its event loop alive for good, and kept running the tasks it had scheduled.
#[tokio::test]
async fn test_drop_without_stop_shuts_the_timer_down() {
    let baseline = alive_tasks();
    let counter = Arc::new(AtomicU64::new(0));

    {
        let timer = MiniTimer::new();

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(1)
            .spawn_async(CounterTask::new(counter.clone()))
            .unwrap();
        timer.add_task(task).unwrap();

        wait_for_count(&counter, 1).await;
    }

    let alive = wait_for_alive_tasks(baseline).await;
    assert!(
        alive <= baseline,
        "dropping the timer left {} task(s) running",
        alive.saturating_sub(baseline)
    );

    let runs_at_drop = counter.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(2000)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        runs_at_drop,
        "a dropped timer must not keep executing its tasks"
    );
}

/// Test that every clone of a timer has to be dropped.
#[tokio::test]
async fn test_drop_of_every_clone_shuts_the_timer_down() {
    let baseline = alive_tasks();
    let counter = Arc::new(AtomicU64::new(0));

    {
        let timer = MiniTimer::new();

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(1)
            .spawn_async(CounterTask::new(counter.clone()))
            .unwrap();
        timer.add_task(task).unwrap();

        let second_handle = timer.clone();
        wait_for_count(&counter, 1).await;

        // One handle going away must not shut the timer down
        drop(timer);
        wait_for_count(&counter, 2).await;

        drop(second_handle);
    }

    let alive = wait_for_alive_tasks(baseline).await;
    assert!(
        alive <= baseline,
        "dropping every handle left {} task(s) running",
        alive.saturating_sub(baseline)
    );
}

/// Test that a manual timer is released as well.
///
/// A manual timer has no tick source, so this guards the other half of the
/// ownership rule: no spawned task may hold the `Arc<Inner>` itself, or the
/// last handle could not release the event loop.
#[tokio::test]
async fn test_drop_without_stop_shuts_a_manual_timer_down() {
    let baseline = alive_tasks();

    let timer = MiniTimer::new_manual();
    let counter = Arc::new(AtomicU64::new(0));

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task).unwrap();

    drop(timer);

    let alive = wait_for_alive_tasks(baseline).await;
    assert!(
        alive <= baseline,
        "dropping a manual timer left {} task(s) running",
        alive.saturating_sub(baseline)
    );
    assert_eq!(counter.load(Ordering::SeqCst), 0);
}

/// Test that stopping a timer before dropping it stays leak free.
///
/// Stopping already releases the event loop on its own, so this guards the
/// combination rather than the fix itself.
#[tokio::test]
async fn test_stop_then_drop_shuts_the_timer_down() {
    let baseline = alive_tasks();
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();
    timer.add_task(task).unwrap();

    wait_for_count(&counter, 1).await;
    timer.stop().await;
    drop(timer);

    let alive = wait_for_alive_tasks(baseline).await;
    assert!(
        alive <= baseline,
        "stopping and dropping the timer left {} task(s) running",
        alive.saturating_sub(baseline)
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
