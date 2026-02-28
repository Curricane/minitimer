/// Trait for implementing custom task runners.
///
/// Users implement this trait to define what their scheduled task does.
/// The task runner is executed when the scheduled time arrives.
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
