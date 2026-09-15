//! Scheduling a task without defining a type.
//!
//! A closure can stand in for a `TaskRunner`, which keeps short tasks to a few
//! lines. The timer is driven by hand so the example is deterministic.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example closure_task
//! ```

use minitimer::{MiniTimer, TaskBuilder};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let runs = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let counter = runs.clone();
    let heartbeat = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(5)
        .spawn_async(move || {
            let counter = counter.clone();
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                println!("  heartbeat #{n}");
            }
        })
        .unwrap();

    let shutdown = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(20)
        .spawn_async(|| async {
            println!("  shutting down");
        })
        .unwrap();

    timer.add_task(heartbeat).unwrap();
    timer.add_task(shutdown).unwrap();

    println!("Running 20 seconds worth of ticks:");
    for _ in 0..2 {
        timer.elapse(Duration::from_secs(10)).await;
        timer.wait_for_idle().await;
    }

    println!("Heartbeats: {}", runs.load(Ordering::SeqCst));
    println!("Tasks left: {}", timer.task_count());
    timer.stop().await;
}
