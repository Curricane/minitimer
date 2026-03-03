use std::sync::Arc;

use async_channel::Sender;

use crate::{
    error::TaskError,
    task::{
        RecordId, TaskId, TaskRunner,
        frequency::{FrequencySeconds, FrequencyState},
    },
    timer::{TimerEvent, wheel::WheelCascadeGuide},
    utils,
};

/// A scheduled task that can be executed by the timer.
///
/// The Task contains the execution logic (via TaskRunner) and scheduling
/// information (frequency, wheel position).
#[derive(Clone)]
pub struct Task {
    /// The unique identifier for the task.
    pub task_id: TaskId,
    /// The actual task runner that will be executed.
    pub(crate) runner: Arc<dyn TaskRunner<Output = ()> + Send + Sync>,
    /// The round number when the task is scheduled.
    pub(crate) cascade_guide: WheelCascadeGuide,

    /// The frequency state of the task.
    pub(crate) frequency: FrequencyState,

    /// The original frequency configuration for resetting after acceleration.
    pub(crate) frequency_config: FrequencySeconds,

    /// Maximum concurrent executions for this task.
    pub max_concurrency: usize,
}

impl Task {
    /// Checks if the task has arrived at its scheduled time.
    ///
    /// # Arguments
    /// * `current_sec` - Current second (0-59)
    /// * `current_min` - Current minute (0-59)
    /// * `current_hour` - Current hour (0-23)
    ///
    /// # Returns
    /// `true` if the current time matches the task's scheduled time, `false` otherwise.
    pub fn is_arrived(&self, current_sec: u64, current_min: u64, current_hour: u64) -> bool {
        self.cascade_guide
            .is_arrived(current_sec, current_min, current_hour)
    }

    /// Gets the next alarm timestamp and advances the frequency state.
    ///
    /// # Returns
    /// The next timestamp when the task should execute, or None if no more executions.
    pub fn next_alarm_timestamp(&mut self) -> Option<u64> {
        self.frequency.next_alarm_timestamp()
    }

    pub(crate) fn set_wheel_position(&mut self, wheel_position: WheelCascadeGuide) {
        self.cascade_guide = wheel_position;
    }
}

/// Builder for creating Task instances.
///
/// Allows configuration of task frequency, concurrency limits, and other properties
/// before building a Task.
#[derive(Default, Clone, Copy)]
pub struct TaskBuilder {
    task_id: TaskId,
    frequency: FrequencySeconds,
    max_concurrency: usize,
}

impl TaskBuilder {
    /// Creates a new TaskBuilder with the given task ID.
    ///
    /// # Arguments
    /// * `task_id` - Unique identifier for the task
    pub fn new(task_id: u64) -> Self {
        Self {
            task_id,
            frequency: FrequencySeconds::default(),
            max_concurrency: 1,
        }
    }

    /// Sets the task to execute once after the specified number of seconds.
    ///
    /// # Arguments
    /// * `seconds` - Number of seconds to wait before execution
    pub fn with_frequency_once_by_seconds(&mut self, seconds: u64) -> &mut Self {
        self.frequency = FrequencySeconds::Once(seconds);
        self
    }

    /// Sets the task to execute repeatedly at the specified interval.
    ///
    /// # Arguments
    /// * `seconds` - Interval in seconds between executions
    pub fn with_frequency_repeated_by_seconds(&mut self, seconds: u64) -> &mut Self {
        self.frequency = FrequencySeconds::Repeated(seconds);
        self
    }

    /// Sets the maximum number of concurrent executions for this task.
    ///
    /// # Arguments
    /// * `max` - Maximum number of concurrent executions
    pub fn with_max_concurrency(&mut self, max: usize) -> &mut Self {
        self.max_concurrency = max;
        self
    }

    /// Sets the task to execute a specific number of times at the specified interval.
    ///
    /// # Arguments
    /// * `count_down` - Number of times the task will execute
    /// * `seconds` - Interval in seconds between executions
    pub fn with_frequency_count_down_by_seconds(
        &mut self,
        count_down: u64,
        seconds: u64,
    ) -> &mut Self {
        self.frequency = FrequencySeconds::CountDown(count_down, seconds);
        self
    }

    /// Sets the task to execute once at the specified Unix timestamp.
    ///
    /// # Arguments
    /// * `timestamp` - Unix timestamp (seconds since epoch) when the task should execute
    ///
    /// # Returns
    /// * `Ok(&mut Self)` - If the timestamp is in the future
    /// * `Err(TaskError)` - If the timestamp is in the past or now
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

    /// Builds and returns a Task with the configured settings.
    ///
    /// # Arguments
    /// * `task_runner` - The runner that defines what the task does
    ///
    /// # Returns
    /// * `Ok(Task)` - If the task was successfully built
    /// * `Err(TaskError)` - If there was an error building the task
    pub fn spwan_async<T: TaskRunner<Output = ()> + Send + Sync>(
        self,
        task_runner: T,
    ) -> Result<Task, TaskError> {
        let frequency = self.frequency.into();
        Ok(Task {
            task_id: self.task_id,
            runner: Arc::new(task_runner),
            cascade_guide: WheelCascadeGuide::default(),
            frequency,
            frequency_config: self.frequency,
            max_concurrency: self.max_concurrency,
        })
    }
}

/// Context provided to a task during execution.
///
/// Contains information about the task and execution instance.
#[allow(dead_code)]
pub(crate) struct TaskContext {
    pub task_id: TaskId,
    pub record_id: RecordId,

    #[allow(dead_code)]
    pub(crate) timer_event_sender: Option<Sender<TimerEvent>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_with_frequency_once_by_timestamp_seconds_valid() {
        let now = utils::timestamp();
        let future = now + 100;
        let mut builder = TaskBuilder::new(1);
        let result = builder.with_frequency_once_by_timestamp_seconds(future);
        assert!(result.is_ok());
    }

    #[test]
    fn test_with_frequency_once_by_timestamp_seconds_past() {
        let now = utils::timestamp();
        let past = now - 10;
        let mut builder = TaskBuilder::new(1);
        let result = builder.with_frequency_once_by_timestamp_seconds(past);
        assert!(result.is_err());
    }

    #[test]
    fn test_with_frequency_once_by_timestamp_seconds_now() {
        let now = utils::timestamp();
        let mut builder = TaskBuilder::new(1);
        let result = builder.with_frequency_once_by_timestamp_seconds(now);
        assert!(result.is_err());
    }
}
