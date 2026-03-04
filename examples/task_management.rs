use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};

struct SimpleTask {
    id: u64,
}

#[async_trait]
impl TaskRunner for SimpleTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        println!("Task {} executed!", self.id);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let timer = minitimer::MiniTimer::new();

    let task1 = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(2)
        .spawn_async(SimpleTask { id: 1 })
        .unwrap();

    let task2 = TaskBuilder::new(2)
        .with_frequency_once_by_seconds(5)
        .spawn_async(SimpleTask { id: 2 })
        .unwrap();

    let task3 = TaskBuilder::new(3)
        .with_frequency_count_down_by_seconds(3, 1)
        .spawn_async(SimpleTask { id: 3 })
        .unwrap();

    timer.add_task(task1).unwrap();
    timer.add_task(task2).unwrap();
    timer.add_task(task3).unwrap();

    println!("Added 3 tasks. Initial task count: {}", timer.task_count());

    println!("\n--- Pending tasks: {:?}", timer.get_pending_tasks());
    println!("--- Running tasks: {:?}", timer.get_running_tasks());

    if let Some(status) = timer.task_status(1) {
        println!("\nTask 1 status: {:?}", status);
    }

    println!("\n--- Removing task 2 ---");
    let removed = timer.remove_task(2);
    if removed.is_some() {
        println!("Task 2 removed successfully!");
    }

    println!("Task count after removal: {}", timer.task_count());
    println!("Contains task 1: {}", timer.contains_task(1));
    println!("Contains task 2: {}", timer.contains_task(2));

    println!("\nWaiting 4 seconds to let tasks run...");
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    println!("\n--- After 4 seconds ---");
    println!("Task 1 status: {:?}", timer.task_status(1));
    println!("Task 3 status: {:?}", timer.task_status(3));
    println!("Running tasks: {:?}", timer.get_running_tasks());

    println!("\nExample completed.");
}
