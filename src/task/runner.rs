#[async_trait::async_trait]
pub trait TaskRunner: Send + Sync + 'static {
    type Output: Send + 'static;

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>>;
}
