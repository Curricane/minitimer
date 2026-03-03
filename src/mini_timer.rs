use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_channel::{Receiver, Sender, bounded};

use crate::error::TaskError;
use crate::task::{Task, TaskId, TaskState};
use crate::timer::wheel::MulitWheel;
use crate::timer::{Timer, TimerEvent};

/// Main timer system for scheduling and executing tasks.
///
/// MiniTimer is the primary interface for users to interact with the timer system.
/// It handles task scheduling, execution, and concurrency control.
pub struct MiniTimer {
    wheel: Arc<MulitWheel>,
    event_receiver: Receiver<TimerEvent>,
    event_sender: Sender<TimerEvent>,
    timer: Timer,
    is_running: Arc<AtomicBool>,
}

impl MiniTimer {
    /// Creates a new MiniTimer instance.
    ///
    /// # Returns
    /// A new MiniTimer with initialized components.
    pub fn new() -> Self {
        let (event_sender, event_receiver) = bounded(16);

        let wheel = Arc::new(MulitWheel::new());
        let timer = Timer::new(event_sender.clone());
        let is_running = Arc::new(AtomicBool::new(true));

        let mini_timer = Self {
            wheel,
            event_receiver,
            event_sender,
            timer,
            is_running: is_running.clone(),
        };

        let mut timer_clone = mini_timer.clone();
        tokio::spawn(async move {
            timer_clone.run().await;
        });

        mini_timer
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
        self.wheel.get_task_tracking_info(task_id).is_some()
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

    /// Gets the current state of a task.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to check
    ///
    /// # Returns
    /// * `Some(TaskState::Running)` - If the task is currently running
    /// * `Some(TaskState::Pending)` - If the task is scheduled but not running
    /// * `None` - If the task doesn't exist
    pub fn get_task_state(&self, task_id: TaskId) -> Option<TaskState> {
        if let Some(tracker) = self.wheel.task_tracker_map.get(&task_id) {
            if !tracker.running_records.is_empty() {
                Some(TaskState::Running)
            } else {
                Some(TaskState::Pending)
            }
        } else {
            None
        }
    }

    /// Advances a task's scheduled execution time.
    ///
    /// - If `duration` is `None`: triggers the task immediately and schedules the next run
    /// - If `duration` is `Some(duration)`: advances the task by the specified duration
    ///
    /// For repeating tasks, after acceleration the frequency sequence is reset
    /// from the current time, ensuring consistent intervals for subsequent executions.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to advance
    /// * `duration` - Optional duration to advance by. `None` means trigger immediately.
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully advanced
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn advance_task(
        &self,
        task_id: TaskId,
        duration: Option<std::time::Duration>,
    ) -> Result<(), TaskError> {
        let duration_secs = duration.map(|d| d.as_secs());
        self.wheel.accelerate_task(task_id, duration_secs)
    }

    /// Stops the timer system.
    ///
    /// This stops the internal timer and sets the running flag to false.
    pub async fn stop(&self) {
        if !self.is_running.load(Ordering::Relaxed) {
            return;
        }

        self.timer.stop();
        self.is_running.store(false, Ordering::Relaxed);
    }

    /// Runs the timer event loop.
    ///
    /// This method starts the internal timer and processes events in a loop:
    /// 1. Handles TimerEvent::Tick by executing arrived tasks
    /// 2. Handles TimerEvent::StopTimer by breaking the loop
    /// 3. Stops on any error
    pub async fn run(&mut self) {
        self.is_running.store(true, Ordering::Relaxed);

        let mut timer = self.timer.clone();

        tokio::spawn(async move {
            timer.run().await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        loop {
            match self.event_receiver.recv().await {
                Ok(TimerEvent::Tick) => {
                    self.wheel.tick();

                    let arrived_tasks = self.wheel.execute_arrived_tasks();
                    for task in arrived_tasks {
                        MulitWheel::process_arrived_task(self.wheel.clone(), task);
                    }
                }
                Ok(TimerEvent::StopTimer) => {
                    break;
                }
                Err(_) => {
                    break;
                }
            }
        }

        self.is_running.store(false, Ordering::Relaxed);
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
            event_receiver: self.event_receiver.clone(),
            event_sender: self.event_sender.clone(),
            timer: self.timer.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

unsafe impl Send for MiniTimer {}

unsafe impl Sync for MiniTimer {}
