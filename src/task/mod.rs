#![allow(clippy::module_inception)]
pub mod frequency;
pub mod runner;
pub(crate) mod task;

/// Unique identifier for a task.
pub type TaskId = u64;

/// Unique identifier for a task execution record.
pub type RecordId = i64;

/// Represents the current state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Task is scheduled but not yet executing.
    Pending,
    /// Task is currently executing.
    Running,
    /// Task has completed execution.
    Completed,
    /// Task has been removed from the scheduler.
    Removed,
}

/// Record of a task execution instance.
#[derive(Debug, Clone)]
pub struct RunningRecord {
    /// The unique identifier of the task.
    pub task_id: TaskId,
    /// The unique identifier of this execution record.
    pub record_id: RecordId,
    /// The current state of this execution.
    pub state: TaskState,
}

pub use runner::TaskRunner;

pub use task::{Task, TaskBuilder};
