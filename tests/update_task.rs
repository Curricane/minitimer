//! Integration tests for update_task functionality.
//!
//! These tests verify that tasks can be updated with new configurations.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

mod common;
use common::CounterTask;

#[tokio::test]
async fn test_update_task_basic() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let initial_status = timer.task_status(1).expect("Task should exist");
    assert_eq!(initial_status.max_concurrency, 1);
    assert_eq!(
        initial_status.frequency_config,
        minitimer::FrequencySeconds::Repeated(60)
    );

    let new_task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(30)
        .with_max_concurrency(5)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(1, new_task).unwrap();

    let updated_status = timer
        .task_status(1)
        .expect("Task should exist after update");
    assert_eq!(updated_status.max_concurrency, 5);
    assert_eq!(
        updated_status.frequency_config,
        minitimer::FrequencySeconds::Repeated(30)
    );
}

#[tokio::test]
async fn test_update_task_not_found() {
    let timer = MiniTimer::new();

    let new_task = TaskBuilder::new(999)
        .with_frequency_repeated_by_seconds(30)
        .spawn_async(CounterTask::new(Arc::new(AtomicU64::new(0))))
        .unwrap();

    let result = timer.update_task(999, new_task);
    assert!(result.is_err(), "Should return error for non-existent task");
}

#[tokio::test]
async fn test_update_task_preserves_running_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(30)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(2, new_task).unwrap();

    assert!(
        timer.contains_task(2),
        "Task should still exist after update"
    );
}

#[tokio::test]
async fn test_update_task_change_frequency_type() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(3)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(3)
        .with_frequency_once_by_seconds(120)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(3, new_task).unwrap();

    let updated_status = timer
        .task_status(3)
        .expect("Task should exist after update");
    assert_eq!(
        updated_status.frequency_config,
        minitimer::FrequencySeconds::Once(120)
    );
}

#[tokio::test]
async fn test_update_task_to_countdown() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(4)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(4)
        .with_frequency_count_down_by_seconds(3, 10)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(4, new_task).unwrap();

    let updated_status = timer
        .task_status(4)
        .expect("Task should exist after update");
    assert_eq!(
        updated_status.frequency_config,
        minitimer::FrequencySeconds::CountDown(3, 10)
    );
}

#[tokio::test]
async fn test_update_task_wheel_placement() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(5)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(5)
        .with_frequency_repeated_by_seconds(10)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(5, new_task).unwrap();

    let updated_status = timer
        .task_status(5)
        .expect("Task should exist after update");
    assert_eq!(format!("{:?}", updated_status.wheel_type), "Second");
}

#[tokio::test]
async fn test_update_task_task_executes_with_new_frequency() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(6)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(6)
        .with_frequency_repeated_by_seconds(2)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(6, new_task).unwrap();

    timer.tick().await;
    timer.tick().await;
    timer.tick().await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let count = counter.load(Ordering::SeqCst);
    assert!(
        count >= 1,
        "Task should execute with new 2-second frequency, got {}",
        count
    );
}

#[tokio::test]
async fn test_update_task_different_task_id_in_new_task() {
    let counter = Arc::new(AtomicU64::new(0));

    let timer = MiniTimer::new();

    let task = TaskBuilder::new(7)
        .with_frequency_repeated_by_seconds(60)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(Duration::from_millis(50)).await;

    let new_task = TaskBuilder::new(8)
        .with_frequency_repeated_by_seconds(30)
        .spawn_async(CounterTask::new(counter.clone()))
        .unwrap();

    timer.update_task(7, new_task).unwrap();

    assert!(timer.contains_task(7), "Task with id 7 should exist");
    assert!(!timer.contains_task(8), "Task with id 8 should not exist");

    let updated_status = timer
        .task_status(7)
        .expect("Task 7 should exist after update");
    assert_eq!(
        updated_status.frequency_config,
        minitimer::FrequencySeconds::Repeated(30)
    );
}
