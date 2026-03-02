use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::{Duration, sleep};

struct ConcurrentTask {
    task_id: u64,
    active_count: Arc<AtomicUsize>,
}

#[async_trait]
impl TaskRunner for ConcurrentTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        let current = self.active_count.fetch_add(1, Ordering::SeqCst);
        println!(
            "Task {} started. Active count: {}",
            self.task_id,
            current + 1
        );

        sleep(Duration::from_millis(500)).await;

        let remaining = self.active_count.fetch_sub(1, Ordering::SeqCst);
        println!(
            "Task {} completed. Active count: {}",
            self.task_id,
            remaining - 1
        );

        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let active_count = Arc::new(AtomicUsize::new(0));
    let max_concurrency = 3;

    let timer = minitimer::MiniTimer::new();

    for i in 1..=5 {
        let task = TaskBuilder::new(i)
            .with_frequency_once_by_seconds(1)
            .with_max_concurrency(max_concurrency)
            .spwan_async(ConcurrentTask {
                task_id: i,
                active_count: active_count.clone(),
            })
            .unwrap();

        timer.add_task(task).unwrap();
    }

    println!(
        "Timer started. Added 5 tasks with max concurrency of {}. Waiting 3 seconds...",
        max_concurrency
    );
    sleep(Duration::from_secs(3)).await;

    println!("Example completed.");
}
