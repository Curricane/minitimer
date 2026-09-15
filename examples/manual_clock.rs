//! Driving the timer by hand.
//!
//! A manual timer never ticks on its own, so the whole schedule can be
//! exercised without waiting for the wall clock. This is how the library's own
//! tests cover timing sensitive behaviour.
//!
//! Run with:
//!
//! ```bash
//! cargo run --example manual_clock
//! ```

use async_trait::async_trait;
use minitimer::{MiniTimer, TaskBuilder, TaskRunner};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct Report {
    label: &'static str,
    elapsed: Arc<AtomicU64>,
}

#[async_trait]
impl TaskRunner for Report {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let at = self.elapsed.fetch_add(1, Ordering::SeqCst) + 1;
        println!("  [{}] run #{}", self.label, at);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let elapsed = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(10)
        .spawn_async(Report {
            label: "every 10s",
            elapsed: elapsed.clone(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Nothing happens on its own, even though real time passes:");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    println!("  runs so far: {}", elapsed.load(Ordering::SeqCst));

    println!("\nElapsing 25 seconds, one tick per second:");
    timer.elapse(Duration::from_secs(25)).await;
    timer.wait_for_idle().await;
    println!("  runs so far: {}", elapsed.load(Ordering::SeqCst));

    let status = timer.task_status(1).expect("task should exist");
    println!(
        "  next run in {}s, on the {:?} wheel",
        status.time_to_next_run, status.wheel_type
    );

    println!("\nElapsing another 25 seconds:");
    timer.elapse(Duration::from_secs(25)).await;
    timer.wait_for_idle().await;
    println!("  runs so far: {}", elapsed.load(Ordering::SeqCst));

    timer.stop().await;
    println!("\nTimer stopped.");
}
