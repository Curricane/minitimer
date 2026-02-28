use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};

struct DelayedPrintTask {
    message: String,
}

#[async_trait]
impl TaskRunner for DelayedPrintTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        println!("Task executed: {}", self.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let timer = minitimer::MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3)
        .spwan_async(DelayedPrintTask {
            message: "Hello from minitimer!".to_string(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Timer started. Waiting 5 seconds...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    println!("Example completed.");
}
