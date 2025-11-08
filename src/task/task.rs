use std::sync::Arc;

use crate::task::{TaskId, TaskRunner, frequency::FrequencyState};

pub struct Task {
    /// The unique identifier for the task.
    pub task_id: TaskId,
    /// The actual task runner that will be executed.
    pub(crate) runner: Arc<dyn TaskRunner<Output = ()> + Send + Sync>,
    /// The round number when the task is scheduled.
    round: u64,

    /// The frequency state of the task.
    pub(crate) frequency: FrequencyState,
}

impl Task {
    pub fn is_arrived(&self) -> bool {
        self.round == 0
    }
}
