pub mod frequency;
pub mod runner;
pub mod task;

pub type TaskId = u64;
pub type RecordId = i64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Removed,
}

pub use runner::TaskRunner;

pub use task::{Task, TaskBuilder, TaskContext};
