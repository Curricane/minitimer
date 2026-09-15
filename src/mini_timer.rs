use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_channel::{Receiver, Sender, bounded, unbounded};
use tokio::sync::Notify;

use crate::error::TaskError;
use crate::task::{Task, TaskId};
use crate::timer::wheel::{MulitWheel, TaskStatus};
use crate::timer::{Timer, TimerEvent};

/// Main timer system for scheduling and executing tasks.
///
/// MiniTimer is the primary interface for users to interact with the timer system.
/// It handles task scheduling, execution, and concurrency control.
///
/// A running timer owns two spawned tasks — a tick source and the event loop
/// that applies the ticks — and stops both when [`MiniTimer::stop`] is called
/// or when the last handle to it is dropped.
pub struct MiniTimer {
    inner: Arc<Inner>,
}

/// The state every handle of a timer shares.
///
/// Keeping it behind one `Arc` is what gives a timer an owner: the spawned
/// tasks hold the pieces they need — the wheel, the event channel, the tick
/// source — but never an `Arc<Inner>`, so the last handle going away drops
/// `Inner`, and its `Drop` winds the timer down.
struct Inner {
    wheel: Arc<MulitWheel>,
    event_sender: Sender<TimerEvent>,
    /// One notification per applied tick, so `tick` can wait for its effect.
    tick_applied: Receiver<()>,
    timer: Timer,
    is_running: Arc<AtomicBool>,
    /// Wakes the event loop so it can wind down. `Drop` cannot await a channel
    /// send, and the event channel can be full.
    shutdown: Arc<Notify>,
}

impl Inner {
    /// Spawns the tick source, which sends a tick every second until it is
    /// stopped. Requires a Tokio runtime.
    fn spawn_tick_source(&self) {
        let mut ticker = self.timer.clone();
        tokio::spawn(async move {
            ticker.run().await;
        });
    }

    /// Stops the tick source and wakes the event loop, which then exits and
    /// releases the wheel.
    fn shut_down(&self) {
        self.timer.stop();
        let _ = self.event_sender.try_send(TimerEvent::StopTimer);
        self.shutdown.notify_one();
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // A timer that is dropped instead of stopped would otherwise leave the
        // tick source and the event loop alive for good, still executing the
        // tasks of a timer that nobody holds any more.
        self.shut_down();
    }
}

impl MiniTimer {
    /// Creates a new MiniTimer that follows the wall clock, ticking once per
    /// second.
    ///
    /// Requires a Tokio runtime, since the tick source and the event loop run
    /// as spawned tasks.
    ///
    /// # Returns
    /// A new MiniTimer with initialized components.
    pub fn new() -> Self {
        Self::build(true)
    }

    /// Creates a new MiniTimer whose time only moves when it is told to.
    ///
    /// The timer never ticks on its own: time advances through [`MiniTimer::tick`]
    /// and [`MiniTimer::elapse`]. Together with [`MiniTimer::wait_for_idle`] this
    /// makes task scheduling testable without waiting for the clock.
    ///
    /// Requires a Tokio runtime for the event loop.
    ///
    /// # Returns
    /// A new MiniTimer that is driven by hand.
    pub fn new_manual() -> Self {
        Self::build(false)
    }

    fn build(follow_wall_clock: bool) -> Self {
        let (event_sender, event_receiver) = bounded(16);
        let (tick_applied_sender, tick_applied) = unbounded();

        let wheel = Arc::new(MulitWheel::new());
        let timer = Timer::new(event_sender.clone());
        let is_running = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(Notify::new());

        let inner = Arc::new(Inner {
            wheel: wheel.clone(),
            event_sender,
            tick_applied,
            timer,
            is_running,
            shutdown: shutdown.clone(),
        });

        if follow_wall_clock {
            inner.spawn_tick_source();
        }

        tokio::spawn(async move {
            Self::event_loop(wheel, event_receiver, tick_applied_sender, shutdown).await;
        });

        Self { inner }
    }

    /// Consumes timer events until the timer is stopped.
    ///
    /// This runs as a spawned task that owns only the state it needs: holding a
    /// `MiniTimer` (and with it a sender) would keep the channel open and leave
    /// the task running forever after `stop()`.
    async fn event_loop(
        wheel: Arc<MulitWheel>,
        event_receiver: Receiver<TimerEvent>,
        tick_applied: Sender<()>,
        shutdown: Arc<Notify>,
    ) {
        loop {
            let event = tokio::select! {
                // Shutting down outranks the events that are still queued, so
                // that a stopped timer does not apply a tick that was already
                // waiting in the channel.
                biased;

                _ = shutdown.notified() => break,

                event = event_receiver.recv() => match event {
                    Ok(event) => event,
                    // Every sender is gone, so no event can arrive any more.
                    Err(_) => break,
                },
            };

            match event {
                TimerEvent::Tick => {
                    wheel.tick();

                    let arrived_tasks = wheel.execute_arrived_tasks();
                    for task in arrived_tasks {
                        wheel.process_arrived_task(task);
                    }

                    let _ = tick_applied.try_send(());
                }
                TimerEvent::StopTimer => break,
            }
        }
    }

    /// Advances the timer by one tick (one second).
    ///
    /// Resolves once the tick has been applied, so the effect of the elapsed
    /// second — including any task that became due — has been scheduled when
    /// this returns. Use [`MiniTimer::new_manual`] to keep a timer from ticking
    /// on its own, or [`MiniTimer::elapse`] to move it by more than a second.
    pub async fn tick(&self) {
        if self
            .inner
            .event_sender
            .send(TimerEvent::Tick)
            .await
            .is_err()
        {
            // The event loop is gone, so there is nothing to wait for.
            return;
        }

        let _ = self.inner.tick_applied.recv().await;
    }

    /// Advances the timer by the given duration, one tick per second.
    ///
    /// The wheel moves in whole seconds, so a sub-second part of `duration` is
    /// ignored.
    ///
    /// # Arguments
    /// * `duration` - How far to move the timer
    pub async fn elapse(&self, duration: Duration) {
        for _ in 0..duration.as_secs() {
            self.tick().await;
        }
    }

    /// Waits until no task execution is in flight.
    ///
    /// Useful in tests to observe the result of the tasks a timer started, and
    /// to wait for running tasks after [`MiniTimer::stop`]. Note that a
    /// repeating task keeps starting new executions as time moves on, so this
    /// is normally called with a timer that has been stopped.
    pub async fn wait_for_idle(&self) {
        loop {
            let idle = self.inner.wheel.idle_notified();
            tokio::pin!(idle);
            // Register interest first: a completion between the check below and
            // the await would otherwise be missed.
            idle.as_mut().enable();

            if self.inner.wheel.get_running_tasks().is_empty() {
                return;
            }

            idle.await;
        }
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
        self.inner.wheel.add_task(task)
    }

    /// Removes a task from the timer system.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to remove
    ///
    /// # Returns
    /// The removed task if it existed, None otherwise.
    pub fn remove_task(&self, task_id: TaskId) -> Option<Task> {
        self.inner.wheel.remove_task(task_id)
    }

    /// Checks if a task exists in the timer system.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to check
    ///
    /// # Returns
    /// `true` if the task exists, `false` otherwise.
    pub fn contains_task(&self, task_id: TaskId) -> bool {
        self.inner.wheel.task_tracking_info(task_id).is_some()
    }

    /// Gets the total number of tasks in the timer system.
    ///
    /// # Returns
    /// The number of tasks currently scheduled.
    pub fn task_count(&self) -> usize {
        self.inner.wheel.task_tracker_map.len()
    }

    /// Gets a list of all pending tasks.
    ///
    /// A task is pending while it waits for its next execution; a task with an
    /// execution in flight is not pending.
    ///
    /// # Returns
    /// A vector of task IDs that are currently pending execution.
    pub fn get_pending_tasks(&self) -> Vec<TaskId> {
        self.inner.wheel.get_all_pending_tasks()
    }

    /// Gets a list of all running tasks.
    ///
    /// # Returns
    /// A vector of task IDs that are currently running.
    pub fn get_running_tasks(&self) -> Vec<TaskId> {
        self.inner.wheel.get_running_tasks()
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
        self.inner.wheel.task_status(task_id)
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
        self.inner
            .wheel
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
        self.inner.wheel.update_task(task_id, new_task)
    }

    /// Stops the timer system.
    ///
    /// Stops the tick source and shuts the event loop down, so the timer stops
    /// executing tasks and the loop task does not outlive it. This is final: a
    /// stopped timer cannot be started again. Calling `stop` more than once has
    /// no further effect, and dropping the last handle does the same thing.
    ///
    /// The call signals the shutdown rather than waiting for it: a tick that
    /// the event loop is already applying can still land, and the loop task
    /// takes a moment to exit. Use [`MiniTimer::wait_for_idle`] to wait for the
    /// executions a tick started.
    pub async fn stop(&self) {
        if !self.inner.is_running.swap(false, Ordering::AcqRel) {
            return;
        }

        self.inner.shut_down();
    }

    /// Checks if the timer system is currently running.
    ///
    /// # Returns
    /// `true` if the timer is running, `false` otherwise.
    pub fn is_running(&self) -> bool {
        self.inner.is_running.load(Ordering::Acquire)
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
            inner: self.inner.clone(),
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
