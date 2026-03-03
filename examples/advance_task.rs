use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct CounterTask {
    id: u64,
    counter: Arc<AtomicU64>,
}

#[async_trait]
impl TaskRunner for CounterTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let count = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        println!("Task {} executed! (count: {})", self.id, count);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let counter = Arc::new(AtomicU64::new(0));
    let timer = minitimer::MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask {
            id: 1,
            counter: counter.clone(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Task 1 added with 60-second interval.");
    println!("Initial task count: {}", timer.task_count());

    println!("\n--- Test 1: Advance task by 30 seconds ---");
    println!("Task was scheduled for 60 seconds from now.");
    println!("Advancing by 30 seconds...");

    timer
        .advance_task(1, Some(Duration::from_secs(30)), true)
        .unwrap();

    println!(
        "Task 1 still exists after advance: {}",
        timer.contains_task(1)
    );

    println!("\n--- Test 2: Trigger task immediately ---");
    println!("Triggering task 1 immediately (None duration)...");

    timer.advance_task(1, None, true).unwrap();

    println!(
        "Task 1 still exists after trigger: {}",
        timer.contains_task(1)
    );

    println!("\n--- Test 3: Advance beyond wait time ---");
    let task2 = TaskBuilder::new(2)
        .with_frequency_repeated_by_seconds(60)
        .spwan_async(CounterTask {
            id: 2,
            counter: counter.clone(),
        })
        .unwrap();

    timer.add_task(task2).unwrap();

    println!("Task 2 scheduled for 60 seconds, advancing by 120 seconds (beyond wait)...");

    timer
        .advance_task(2, Some(Duration::from_secs(120)), true)
        .unwrap();

    println!("Task 2 still exists: {}", timer.contains_task(2));

    println!("\n--- Test 4: Advance non-existent task (error case) ---");

    let result = timer.advance_task(999, Some(Duration::from_secs(30)), true);

    match result {
        Ok(_) => println!("Unexpected success!"),
        Err(e) => println!("Expected error: {}", e),
    }

    println!("\n--- Final task count: {} ---", timer.task_count());

    println!("\nExample completed. The advance_task API allows:");
    println!("  - advance_task(id, None)        -> Trigger immediately, schedule next run");
    println!("  - advance_task(id, Some(duration)) -> Advance by specific duration");
    println!("  - If duration > remaining wait, triggers immediately and schedules next run");
    println!("\nNote: For repeating tasks, after acceleration the frequency sequence");
    println!("is reset from the current time, ensuring consistent intervals for");
    println!("subsequent executions.");

    timer.stop().await;
}
