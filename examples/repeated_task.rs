use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};

struct PeriodicTask {
    counter: std::sync::Arc<std::sync::Mutex<u32>>,
}

#[async_trait]
impl TaskRunner for PeriodicTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        println!("Periodic task executed! Count: {}", *counter);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));

    let timer = minitimer::MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_repeated_by_seconds(1)
        .spwan_async(PeriodicTask {
            counter: counter.clone(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Timer started. Running periodic task every 1 second for 5 seconds...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let final_count = *counter.lock().unwrap();
    println!("Example completed. Final count: {}", final_count);
}
