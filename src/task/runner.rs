/// Trait for implementing custom task runners.
///
/// Users implement this trait to define what their scheduled task does.
/// The task runner is executed when the scheduled time arrives.
///
/// The scheduler runs tasks through `TaskBuilder::spawn_async`, which accepts
/// runners producing `()`. A failure is logged with the `log` crate and does
/// not affect the timer or the task's remaining executions.
///
/// A closure can be used instead of implementing the trait, which is handy for
/// short tasks:
///
/// ```no_run
/// # use minitimer::{MiniTimer, TaskBuilder};
/// # use std::sync::Arc;
/// # use std::sync::atomic::{AtomicU64, Ordering};
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let runs = Arc::new(AtomicU64::new(0));
/// let timer = MiniTimer::new();
///
/// let counter = runs.clone();
/// let task = TaskBuilder::new(1)
///     .with_frequency_repeated_by_seconds(30)
///     .spawn_async(move || {
///         let counter = counter.clone();
///         async move {
///             counter.fetch_add(1, Ordering::SeqCst);
///         }
///     })?;
///
/// timer.add_task(task)?;
/// # Ok(())
/// # }
/// ```
///
/// A closure is called again for every execution, so it has to be callable
/// repeatedly and the future it returns has to own everything it uses.
#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync + 'static {
    /// The output type produced by the task runner.
    type Output: Send + 'static;

    /// Executes the task.
    ///
    /// # Returns
    /// * `Ok(Self::Output)` - On successful execution
    /// * `Err(Box<dyn std::error::Error + Send + Sync>)` - On failure
    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>>;
}

#[async_trait::async_trait]
impl<F, Fut> TaskRunner for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        (self)().await;
        Ok(())
    }
}
