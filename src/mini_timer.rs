use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_channel::{Receiver, Sender, bounded};

use crate::error::TaskError;
use crate::task::{Task, TaskId};
use crate::timer::wheel::{MulitWheel, TaskStatus};
use crate::timer::{Timer, TimerEvent};

/// Main timer system for scheduling and executing tasks.
///
/// MiniTimer is the primary interface for users to interact with the timer system.
/// It handles task scheduling, execution, and concurrency control.
pub struct MiniTimer {
    wheel: Arc<MulitWheel>,
    event_sender: Sender<TimerEvent>,
    timer: Timer,
    is_running: Arc<AtomicBool>,
}

impl MiniTimer {
    /// Creates a new MiniTimer instance.
    ///
    /// Requires a Tokio runtime, since the tick source and the event loop run
    /// as spawned tasks.
    ///
    /// # Returns
    /// A new MiniTimer with initialized components.
    pub fn new() -> Self {
        let (event_sender, event_receiver) = bounded(16);

        let wheel = Arc::new(MulitWheel::new());
        let timer = Timer::new(event_sender.clone());
        let is_running = Arc::new(AtomicBool::new(true));

        let mut ticker = timer.clone();
        tokio::spawn(async move {
            ticker.run().await;
        });

        let loop_wheel = wheel.clone();
        let loop_running = is_running.clone();
        tokio::spawn(async move {
            Self::event_loop(loop_wheel, event_receiver, loop_running).await;
        });

        Self {
            wheel,
            event_sender,
            timer,
            is_running,
        }
    }

    /// Consumes timer events until the timer is stopped.
    ///
    /// This runs as a spawned task that owns only the state it needs: holding a
    /// `MiniTimer` (and with it a sender) would keep the channel open and leave
    /// the task running forever after `stop()`.
    async fn event_loop(
        wheel: Arc<MulitWheel>,
        event_receiver: Receiver<TimerEvent>,
        is_running: Arc<AtomicBool>,
    ) {
        while let Ok(event) = event_receiver.recv().await {
            match event {
                TimerEvent::Tick => {
                    wheel.tick();

                    let arrived_tasks = wheel.execute_arrived_tasks();
                    for task in arrived_tasks {
                        wheel.process_arrived_task(task);
                    }
                }
                TimerEvent::StopTimer => break,
            }
        }

        is_running.store(false, Ordering::Relaxed);
    }

    /// Advances the timer by one tick (one second).
    ///
    /// This is useful for testing purposes to simulate time progression
    /// without waiting for the real clock.
    /// Note: This requires start() to be called first to start the event loop.
    pub async fn tick(&self) {
        let _ = self.event_sender.send(TimerEvent::Tick).await;
    }

    /// Adds a task to the timer system.
    ///
    /// # Arguments
    /// * `task` - The task to add
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully added
    /// * `Err(TaskError)` - If there was an error adding the task
    pub fn add_task(&self, task: Task) -> Result<(), TaskError> {
        self.wheel.add_task(task)
    }

    /// Removes a task from the timer system.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to remove
    ///
    /// # Returns
    /// The removed task if it existed, None otherwise.
    pub fn remove_task(&self, task_id: TaskId) -> Option<Task> {
        self.wheel.remove_task(task_id)
    }

    /// Checks if a task exists in the timer system.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to check
    ///
    /// # Returns
    /// `true` if the task exists, `false` otherwise.
    pub fn contains_task(&self, task_id: TaskId) -> bool {
        self.wheel.task_tracking_info(task_id).is_some()
    }

    /// Gets the total number of tasks in the timer system.
    ///
    /// # Returns
    /// The number of tasks currently scheduled.
    pub fn task_count(&self) -> usize {
        self.wheel.task_tracker_map.len()
    }

    /// Gets a list of all pending tasks.
    ///
    /// # Returns
    /// A vector of task IDs that are currently pending execution.
    pub fn get_pending_tasks(&self) -> Vec<TaskId> {
        self.wheel.get_all_pending_tasks()
    }

    /// Gets a list of all running tasks.
    ///
    /// # Returns
    /// A vector of task IDs that are currently running.
    pub fn get_running_tasks(&self) -> Vec<TaskId> {
        self.wheel.get_running_tasks()
    }

    /// Gets the current status of a task.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to check
    ///
    /// # Returns
    /// * `Some(TaskStatus)` - If the task exists, containing all tracking information
    /// * `None` - If the task doesn't exist
    pub fn task_status(&self, task_id: TaskId) -> Option<TaskStatus> {
        self.wheel.task_status(task_id)
    }

    /// Advances a task's scheduled execution time.
    ///
    /// - If `duration` is `None`: triggers the task immediately and schedules the next run
    /// - If `duration` is `Some(duration)`: advances the task by the specified duration
    ///
    /// For repeating tasks, the `reset_frequency` parameter controls whether to reset
    /// the frequency sequence from the current time:
    /// - If `true` (default): resets the frequency sequence, ensuring consistent intervals
    /// - If `false`: preserves the current frequency sequence position
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to advance
    /// * `duration` - Optional duration to advance by. `None` means trigger immediately.
    /// * `reset_frequency` - Whether to reset the frequency sequence for repeating tasks (default: true)
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully advanced
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn advance_task(
        &self,
        task_id: TaskId,
        duration: Option<std::time::Duration>,
        reset_frequency: bool,
    ) -> Result<(), TaskError> {
        let duration_secs = duration.map(|d| d.as_secs());
        self.wheel
            .accelerate_task(task_id, duration_secs, reset_frequency)
    }

    /// Updates an existing task with a new Task.
    ///
    /// This replaces the existing task with a new one, preserving the task_id
    /// specified by the `task_id` parameter. The `task_id` field in `new_task`
    /// is ignored and will be overwritten by the `task_id` parameter.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to update
    /// * `new_task` - The new task to replace the existing one (its task_id field will be ignored)
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully updated
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn update_task(&self, task_id: TaskId, new_task: Task) -> Result<(), TaskError> {
        self.wheel.update_task(task_id, new_task)
    }

    /// Stops the timer system.
    ///
    /// Stops the tick source and shuts the event loop down, so the timer stops
    /// executing tasks and the loop task does not outlive it. Calling `stop`
    /// more than once has no further effect.
    pub async fn stop(&self) {
        if !self.is_running.swap(false, Ordering::Relaxed) {
            return;
        }

        self.timer.stop();

        // Wake the event loop so it can wind down instead of waiting forever.
        let _ = self.event_sender.send(TimerEvent::StopTimer).await;
    }

    /// Checks if the timer system is currently running.
    ///
    /// # Returns
    /// `true` if the timer is running, `false` otherwise.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }
}

impl Default for MiniTimer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MiniTimer {
    fn clone(&self) -> Self {
        Self {
            wheel: self.wheel.clone(),
            event_sender: self.event_sender.clone(),
            timer: self.timer.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Guards the auto-trait derivation of the shared timer types: if a field
    /// ever stops being `Send`/`Sync` this fails to compile instead of being
    /// silently papered over by a manual `unsafe impl`.
    #[test]
    fn timer_types_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<MiniTimer>();
        assert_send_sync::<Timer>();
    }
}
