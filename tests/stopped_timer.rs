//! Integration tests for what a stopped timer does.
//!
//! `stop` is final: the timer applies no further tick, and it refuses the work
//! it cannot carry out.

use std::time::Duration;

use minitimer::task::TaskBuilder;
use minitimer::{MiniTimer, TaskError};

mod common;
use common::send_tick_without_waiting;

/// Test that a stopped timer refuses the work it cannot carry out.
#[tokio::test]
async fn test_a_stopped_timer_refuses_new_work() {
    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(|| async {})
        .unwrap();
    timer.add_task(task).unwrap();

    timer.stop().await;

    let another = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(1)
        .spawn_async(|| async {})
        .unwrap();
    assert_eq!(timer.add_task(another), Err(TaskError::TimerStopped));
    assert!(
        !timer.contains_task(2),
        "a task that was refused must not be scheduled"
    );

    let replacement = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(2)
        .spawn_async(|| async {})
        .unwrap();
    assert_eq!(
        timer.update_task(1, replacement),
        Err(TaskError::TimerStopped)
    );
    assert_eq!(
        timer.advance_task(1, None, true),
        Err(TaskError::TimerStopped)
    );

    // Inspecting and removing a task stays available, so that a caller can
    // still clean up.
    assert!(timer.remove_task(1).is_some());
}

/// Test that a tick which was still queued when the timer was stopped is not
/// applied afterwards.
#[tokio::test]
async fn test_stop_does_not_apply_a_queued_tick() {
    for round in 0..10 {
        let timer = MiniTimer::new_manual();

        let task = TaskBuilder::new(1)
            .with_frequency_once_by_seconds(10)
            .spawn_async(|| async {})
            .unwrap();
        timer.add_task(task).unwrap();

        send_tick_without_waiting(&timer);
        timer.stop().await;

        // Give the event loop the chance to pick up what was left in the
        // channel, which is where a stopped timer has to leave it.
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert_eq!(
            timer
                .task_status(1)
                .expect("a stopped timer stays queryable")
                .time_to_next_run,
            10,
            "round {round}: a stopped timer applied a tick that was still queued"
        );
    }
}

/// Test that a task operation racing with `stop` stays within the contract.
///
/// The window between the state check and the write is narrow, so the test
/// checks the part it can observe: the two run at the same time without
/// getting stuck, and nothing is accepted once the stop has been observed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_task_operations_race_with_stop() {
    for round in 0..20 {
        let timer = MiniTimer::new_manual();
        let stopper = timer.clone();

        let stopping = tokio::spawn(async move {
            // Let the adding get going, so that the two really do overlap.
            tokio::task::yield_now().await;
            stopper.stop().await;
        });

        for id in 0..20 {
            let task = TaskBuilder::new(id)
                .with_frequency_once_by_seconds(60)
                .spawn_async(|| async {})
                .unwrap();

            if timer.add_task(task) == Err(TaskError::TimerStopped) {
                break;
            }
        }

        stopping.await.unwrap();

        let late = TaskBuilder::new(99)
            .with_frequency_once_by_seconds(60)
            .spawn_async(|| async {})
            .unwrap();
        assert_eq!(
            timer.add_task(late),
            Err(TaskError::TimerStopped),
            "round {round}: a stopped timer accepted a task"
        );
    }
}
