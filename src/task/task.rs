use std::sync::Arc;

use async_channel::Sender;

use crate::{
    error::TaskError,
    task::{
        RecordId, TaskId, TaskRunner,
        frequency::{FrequencySeconds, FrequencyState},
    },
    timer::TimerEvent,
    utils,
};

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

    pub(crate) fn sub_round(&mut self) {
        self.round = self.round.saturating_sub(1);
    }
}

#[derive(Default)]
pub struct TaskBuilder {
    task_id: TaskId,
    frequency: FrequencySeconds,
}

impl TaskBuilder {
    pub fn new(task_id: u64) -> Self {
        Self {
            task_id: task_id,
            ..Default::default()
        }
    }

    pub fn with_frequency_once_by_seconds(&mut self, seconds: u64) -> &mut Self {
        self.frequency = FrequencySeconds::Once(seconds);
        self
    }

    pub fn with_frequency_repeated_by_seconds(&mut self, seconds: u64) -> &mut Self {
        self.frequency = FrequencySeconds::Repeated(seconds);
        self
    }

    pub fn with_frequency_count_down_by_seconds(
        &mut self,
        count_down: u64,
        seconds: u64,
    ) -> &mut Self {
        self.frequency = FrequencySeconds::CountDown(count_down, seconds);
        self
    }

    pub fn with_frequency_once_by_timestamp_seconds(
        &mut self,
        timestamp: u64,
    ) -> Result<&mut Self, TaskError> {
        let now = utils::timestamp();
        let gap = timestamp.checked_sub(now).filter(|&gap| gap > 0).ok_or(
            TaskError::InvalidFrequency(format!(
                "Once timestamp({timestamp} need greater than current timestamp({now})"
            )),
        )?;
        self.frequency = FrequencySeconds::Once(gap);
        Ok(self)
    }

    pub fn spwan_async<T: TaskRunner<Output = ()> + Send + Sync>(
        self,
        task_runner: T,
    ) -> Result<Task, TaskError> {
        let frequency = self.frequency.into();
        Ok(Task {
            task_id: self.task_id,
            runner: Arc::new(task_runner),
            round: 0,
            frequency,
        })
    }
}

pub struct TaskContext {
    /// The id of Task.
    pub task_id: TaskId,
    /// The id of the task running instance.
    pub record_id: RecordId,

    pub(crate) timer_event_sender: Option<Sender<TimerEvent>>,
}
