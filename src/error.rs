use thiserror::Error;

/// Errors that can occur in the task system.
#[derive(Error, Debug)]
pub enum TaskError {
    /// The frequency configuration is invalid.
    #[error("invalid frequency: {0}")]
    InvalidFrequency(String),

    /// The task was not found.
    #[error("task not found: {0}")]
    TaskNotFound(u64),
}
