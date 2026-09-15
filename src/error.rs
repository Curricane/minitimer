use thiserror::Error;

/// Errors that can occur in the task system.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum TaskError {
    /// The frequency configuration is invalid.
    #[error("invalid frequency: {0}")]
    InvalidFrequency(String),

    /// The task was not found.
    #[error("task not found: {0}")]
    TaskNotFound(u64),

    /// The timer has been stopped, so the task would never run.
    #[error("the timer has been stopped")]
    TimerStopped,
}
