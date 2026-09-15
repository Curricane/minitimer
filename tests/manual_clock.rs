//! Integration tests for driving the timer by hand.
//!
//! A manual timer never ticks on its own, so task scheduling can be tested
//! without waiting for the wall clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::{CounterTask, SlowTask};

/// Test that a manual timer only moves when it is told to.
#[tokio::test]
async fn test_manual_timer_waits_for_ticks() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(5)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    // Real time passes, the timer does not
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "A manual timer must not tick on its own"
    );

    timer.elapse(Duration::from_secs(4)).await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        0,
        "The task is due after five ticks"
    );

    timer.tick().await;
    timer.wait_for_idle().await;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "The tick that reaches the alarm should run the task"
    );
}

/// Test that elapsing moves a repeated task forward one interval at a time.
#[tokio::test]
async fn test_manual_timer_repeats_with_each_interval() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(3)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    for expected in 1..=3 {
        timer.elapse(Duration::from_secs(3)).await;
        timer.wait_for_idle().await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            expected,
            "The task should have run {expected} times"
        );
    }
}

/// Test that a manual timer reports the status of a task without any waiting.
#[tokio::test]
async fn test_manual_timer_status_is_exact() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(90)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    let status = timer.task_status(1).expect("Task should exist");
    assert_eq!(status.time_to_next_run, 90);

    timer.elapse(Duration::from_secs(30)).await;

    let status = timer.task_status(1).expect("Task should exist");
    assert_eq!(status.time_to_next_run, 60);
}

/// Test waiting for the executions a timer has started.
#[tokio::test]
async fn test_wait_for_idle_waits_for_running_tasks() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(SlowTask::new(counter.clone(), 300))
        .unwrap();

    timer.add_task(task).unwrap();

    timer.elapse(Duration::from_secs(1)).await;

    assert_eq!(
        timer.get_running_tasks(),
        vec![1],
        "The task should be reported as running"
    );

    timer.wait_for_idle().await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "Waiting for idle should mean the execution has finished"
    );
    assert!(timer.get_running_tasks().is_empty());
}
