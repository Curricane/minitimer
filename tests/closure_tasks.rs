//! Integration tests for defining tasks inline with closures.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use minitimer::MiniTimer;
use minitimer::task::TaskBuilder;

/// Test that a closure can be used as a repeating task.
#[tokio::test]
async fn test_closure_task_repeats() {
    let runs = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let counter = runs.clone();
    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(2)
        .spawn_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

    timer.add_task(task).unwrap();

    timer.elapse(Duration::from_secs(2)).await;
    timer.wait_for_idle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    timer.elapse(Duration::from_secs(4)).await;
    timer.wait_for_idle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 3);
}

/// Test that a closure can be used for a one shot task.
#[tokio::test]
async fn test_closure_once_task() {
    let runs = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let counter = runs.clone();
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3)
        .spawn_async(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

    timer.add_task(task).unwrap();

    timer.elapse(Duration::from_secs(3)).await;
    timer.wait_for_idle().await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(
        !timer.contains_task(1),
        "A one shot closure task should be removed after it ran"
    );
}

/// Test that a closure can await before finishing.
#[tokio::test]
async fn test_closure_task_can_be_async() {
    let runs = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let counter = runs.clone();
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(1)
        .spawn_async(move || {
            let counter = counter.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                counter.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();

    timer.add_task(task).unwrap();

    timer.elapse(Duration::from_secs(1)).await;

    assert_eq!(
        timer.get_running_tasks(),
        vec![1],
        "The execution should still be in flight"
    );

    timer.wait_for_idle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
