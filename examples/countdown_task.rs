use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};

struct CountDownTask {
    counter: std::sync::Arc<std::sync::Mutex<u32>>,
}

#[async_trait]
impl TaskRunner for CountDownTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let mut counter = self.counter.lock().unwrap();
        *counter += 1;
        println!("CountDown task executed! Execution #{}", *counter);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let counter = std::sync::Arc::new(std::sync::Mutex::new(0));

    let timer = minitimer::MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_count_down_by_seconds(3, 1)
        .spwan_async(CountDownTask {
            counter: counter.clone(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    println!("Timer started. Task will execute 3 times with 1 second interval...");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let final_count = *counter.lock().unwrap();
    println!("Example completed. Total executions: {}", final_count);
}
