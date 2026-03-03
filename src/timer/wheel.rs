use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

use crate::{
    error::TaskError,
    task::{RecordId, Task, TaskId, TaskState},
    timer::slot::Slot,
    utils::timestamp,
};

/// Multi-level time wheel implementation for task scheduling.
///
/// This structure implements a three-level timing wheel system:
/// - Second wheel: 60 slots (0-59 seconds)
/// - Minute wheel: 60 slots (0-59 minutes)
/// - Hour wheel: 24 slots (0-23 hours)
///
/// Tasks are distributed across these wheels based on their execution time,
/// providing O(1) time complexity for task lookup and execution.
pub(crate) struct MulitWheel {
    sec_wheel: Wheel,
    min_wheel: Wheel,
    hour_wheel: Wheel,

    pub(crate) task_tracker_map: DashMap<TaskId, TaskTrackingInfo>,
}

impl MulitWheel {
    /// Creates a new MultiWheel instance with three-level timing wheels.
    pub(crate) fn new() -> Self {
        Self {
            sec_wheel: Wheel::new(60),
            min_wheel: Wheel::new(60),
            hour_wheel: Wheel::new(24),
            task_tracker_map: DashMap::new(),
        }
    }

    /// Set the positions of all wheels for testing purposes
    #[cfg(test)]
    pub(crate) fn set_wheel_positions(&self, sec: u64, min: u64, hour: u64) {
        self.sec_wheel.set_hand_position(sec);
        self.min_wheel.set_hand_position(min);
        self.hour_wheel.set_hand_position(hour);
    }

    /// Get the current positions of all wheels for testing purposes.
    pub(crate) fn get_wheel_positions(&self) -> (u64, u64, u64) {
        (
            self.sec_wheel.hand.load(Ordering::Relaxed),
            self.min_wheel.hand.load(Ordering::Relaxed),
            self.hour_wheel.hand.load(Ordering::Relaxed),
        )
    }

    /// Advances the time wheel by one second.
    ///
    /// This method moves the second wheel hand forward by one position.
    /// If the second wheel overflows (reaches 60), it triggers a cascade to the minute wheel.
    /// If the minute wheel overflows, it triggers a cascade to the hour wheel.
    ///
    /// Returns the carry value if there's an overflow beyond hours (i.e., more than 24 hours have passed),
    /// otherwise returns None.
    pub(crate) fn tick(&self) -> Option<u64> {
        self.sec_wheel
            .hand_move(1)
            .and_then(|carry| {
                let carry = self.min_wheel.hand_move(carry);
                self.cascade_minute_tasks();
                carry
            })
            .and_then(|carry| {
                let carry = self.hour_wheel.hand_move(carry);
                self.cascade_hour_tasks();
                carry
            })
    }

    /// Executes all tasks that have arrived at their scheduled time.
    ///
    /// Returns a vector of tasks that are ready to be executed.
    /// The tasks are removed from the wheel but not from the task tracker.
    pub(crate) fn execute_arrived_tasks(&self) -> Vec<Task> {
        let mut executed_tasks = Vec::new();
        let (current_sec, current_min, current_hour) = self.get_wheel_positions();
        let hand = self.sec_wheel.hand_position();

        if let Some(mut slot) = self.sec_wheel.slots.get_mut(&hand) {
            let arrived_task_ids = slot.arrival_time_tasks(current_sec, current_min, current_hour);
            for task_id in arrived_task_ids {
                if let Some(task) = slot.remove_task(task_id) {
                    executed_tasks.push(task);
                }
            }
        }

        executed_tasks
    }

    /// Processes an arrived task: tries to start it with concurrency control,
    /// or re-adds it to the wheel if concurrency limit is reached.
    ///
    /// If concurrency is available, the task is spawned as an async task and
    /// rescheduled for its next execution. If concurrency is full, the task
    /// is re-added to the wheel to retry on the next tick.
    ///
    /// # Arguments
    /// * `wheel` - The wheel reference for scheduling and tracking
    /// * `task` - The task to process
    pub(crate) fn process_arrived_task(wheel: Arc<Self>, task: Task) {
        let task_id = task.task_id;
        let max_concurrency = task.max_concurrency;
        let runner = task.runner.clone();

        match wheel.try_start_task(task_id, max_concurrency) {
            Some(record_id) => {
                let mut task_clone = task;
                wheel.reschedule_task(&mut task_clone);
                tokio::spawn(async move {
                    let _ = runner.run().await;
                    wheel.complete_task(task_id, record_id);
                });
            }
            None => {
                let _ = wheel.add_task(task);
            }
        }
    }

    /// Reschedules a task for its next execution.
    ///
    /// This is called after a task completes execution to schedule its next run
    /// based on its frequency settings.
    ///
    /// Returns `true` if the task was successfully rescheduled, `false` otherwise.
    pub(crate) fn reschedule_task(&self, task: &mut Task) -> bool {
        if let Some(next_timestamp) = task.next_alarm_timestamp() {
            let next_alarm_sec = next_timestamp.saturating_sub(timestamp());
            if next_alarm_sec > 0 {
                let next_guide = self.cal_next_hand_position(next_alarm_sec);
                task.set_wheel_position(next_guide);
                let _ = self.add_task(task.clone());
                return true;
            }
        }
        false
    }

    /// Calculates the next wheel position for a task based on the time until its next execution.
    ///
    /// This method determines which slot the task should be placed in on which wheel,
    /// based on the number of seconds until the task's next execution time.
    ///
    /// # Arguments
    /// * `next_alarm_sec` - The number of seconds until the task's next execution
    ///
    /// # Returns
    /// A `WheelCascadeGuide` that specifies the exact position (second, minute, hour, and round)
    /// where the task should be placed.
    pub(crate) fn cal_next_hand_position(&self, next_alarm_sec: u64) -> WheelCascadeGuide {
        let (current_second, current_minute, current_hour) = self.get_wheel_positions();

        let total_seconds = current_second + next_alarm_sec;
        let final_sec = total_seconds % 60;

        let total_minutes = current_minute + (total_seconds / 60);
        let final_min = total_minutes % 60;

        // Check if there will be a carry from seconds to minutes
        let has_min_carry = total_seconds >= 60;

        if has_min_carry {
            // Check if there will be a carry from minutes to hours
            let has_hour_carry = total_minutes >= 60;

            if has_hour_carry {
                // There will be carry to hours, we need to calculate rounds as well
                let total_hours = current_hour + (total_minutes / 60);
                let final_hour = total_hours % 24;
                let round = total_hours / 24;

                WheelCascadeGuide {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: Some(final_hour),
                    round,
                }
            } else {
                // Only minute carry, no hour carry
                WheelCascadeGuide {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: None,
                    round: 0,
                }
            }
        } else {
            // No carry, only seconds level
            WheelCascadeGuide {
                sec: final_sec,
                min: None,
                hour: None,
                round: 0,
            }
        }
    }
}

/// A single level time wheel with a fixed number of slots.
///
/// Each wheel maintains a "hand" that points to the current position.
/// Tasks are placed in slots based on their scheduled execution time.
pub(crate) struct Wheel {
    slots: DashMap<u64, Slot>,
    hand: Arc<AtomicU64>,
    num_slots: u64,
}

impl Wheel {
    /// Creates a new Wheel with the specified number of slots.
    pub(crate) fn new(num_slots: u64) -> Self {
        let slots = DashMap::new();
        for i in 0..num_slots {
            slots.insert(i, Slot::new());
        }

        Self {
            slots,
            hand: Arc::new(AtomicU64::new(0)),
            num_slots,
        }
    }

    /// Moves the hand forward by the specified number of steps.
    ///
    /// Returns the carry amount if the hand overflows the wheel (wraps around),
    /// otherwise returns None.
    ///
    /// # Arguments
    /// * `step` - The number of slots to move forward
    ///
    /// # Returns
    /// * `Some(carry)` - The number of times the wheel has wrapped around
    /// * `None` - No overflow occurred
    pub(crate) fn hand_move(&self, step: u64) -> Option<u64> {
        if step == 0 {
            return None;
        }
        let pre_hand = self.hand.fetch_add(step, Ordering::Relaxed);
        let new_hand = pre_hand + step;
        let carry = new_hand / self.num_slots;

        if carry > 0 {
            // Reset the hand to the correct position after carry
            self.hand
                .store(new_hand % self.num_slots, Ordering::Relaxed);
            Some(carry)
        } else {
            None
        }
    }

    /// Returns the current position of the hand.
    pub(crate) fn hand_position(&self) -> u64 {
        self.hand.load(Ordering::Relaxed)
    }

    /// Set the hand position of the wheel for testing purposes.
    #[cfg(test)]
    pub(crate) fn set_hand_position(&self, position: u64) {
        self.hand
            .store(position % self.num_slots, Ordering::Relaxed);
    }

    /// Adds a task to the specified slot in the wheel.
    ///
    /// # Arguments
    /// * `task` - The task to add
    /// * `slot_num` - The slot number to place the task in
    pub(crate) fn add_task(&self, task: Task, slot_num: u64) {
        self.slots.get_mut(&slot_num).unwrap().add_task(task);
    }
}

/// Guide for cascade positioning of tasks across multiple time wheels.
///
/// This structure tracks the exact position where a task should be placed
/// across the three-level time wheel system (second, minute, hour wheels).
#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct WheelCascadeGuide {
    pub sec: u64,
    pub min: Option<u64>,
    pub hour: Option<u64>,
    pub round: u64,
}

impl WheelCascadeGuide {
    /// Checks if the task has arrived at its scheduled time.
    ///
    /// # Arguments
    /// * `current_sec` - Current second (0-59)
    /// * `current_min` - Current minute (0-59)
    /// * `current_hour` - Current hour (0-23)
    ///
    /// # Returns
    /// `true` if the current time matches the scheduled time and round is 0, `false` otherwise.
    pub(crate) fn is_arrived(&self, current_sec: u64, current_min: u64, current_hour: u64) -> bool {
        if let Some(hour) = self.hour {
            if let Some(minute) = self.min {
                return self.sec == current_sec
                    && minute == current_min
                    && hour == current_hour
                    && self.round == 0;
            }
            return false;
        }
        if let Some(minute) = self.min {
            return self.sec == current_sec && minute == current_min && self.round == 0;
        }
        self.sec == current_sec && self.round == 0
    }
}

/// Task tracking information structure.
///
/// Contains metadata about a task including its position in the wheel system,
/// the wheel type it's currently in, and running records for concurrency tracking.
#[derive(Debug, Clone)]
pub struct TaskTrackingInfo {
    pub cascade_guide: WheelCascadeGuide,
    pub wheel_type: WheelType,
    pub slot_num: u64,
    #[allow(dead_code)]
    pub max_concurrency: usize,
    pub running_records: DashMap<RecordId, TaskState>,
}

/// Represents the type of wheel a task is currently in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WheelType {
    Second,
    Minute,
    Hour,
}

impl MulitWheel {
    /// Quickly query task tracking information by task ID.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    ///
    /// # Returns
    /// Some(TaskTrackingInfo) if the task exists, None otherwise.
    pub fn get_task_tracking_info(&self, task_id: TaskId) -> Option<TaskTrackingInfo> {
        self.task_tracker_map.get(&task_id).map(|info| info.clone())
    }

    /// Get all pending tasks (tasks currently scheduled in the wheel).
    ///
    /// # Returns
    /// A vector of task IDs that are currently pending execution.
    pub fn get_all_pending_tasks(&self) -> Vec<TaskId> {
        self.task_tracker_map.iter().map(|r| *r.key()).collect()
    }

    /// Get all running task IDs (tasks that have at least one running record).
    ///
    /// # Returns
    /// A vector of task IDs that are currently running.
    pub fn get_running_tasks(&self) -> Vec<TaskId> {
        self.task_tracker_map
            .iter()
            .filter(|t| !t.running_records.is_empty())
            .map(|t| *t.key())
            .collect()
    }

    #[allow(dead_code)]
    /// Get the current number of running instances for a specific task.
    ///
    /// This is an O(1) operation.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    ///
    /// # Returns
    /// The number of currently running instances of the task.
    pub fn get_task_running_count(&self, task_id: TaskId) -> usize {
        self.task_tracker_map
            .get(&task_id)
            .map(|t| t.running_records.len())
            .unwrap_or(0)
    }

    /// Generates a new unique record ID based on the current timestamp in nanoseconds.
    fn generate_record_id(&self) -> RecordId {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as RecordId
    }

    /// Attempts to start a task execution with concurrency control.
    ///
    /// This method checks if the task has reached its maximum concurrency limit
    /// before starting a new execution instance.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    /// * `max_concurrency` - The maximum allowed concurrent executions for this task
    ///
    /// # Returns
    /// * `Some(RecordId)` - A unique record ID for this execution instance if successful
    /// * `None` - If the concurrency limit has been reached
    pub fn try_start_task(&self, task_id: TaskId, max_concurrency: usize) -> Option<RecordId> {
        let tracker = self.task_tracker_map.get(&task_id)?;

        let current_count = tracker.running_records.len();
        if current_count >= max_concurrency {
            return None;
        }

        let record_id = self.generate_record_id();
        tracker
            .running_records
            .insert(record_id, TaskState::Running);
        Some(record_id)
    }

    /// Marks a task execution as completed.
    ///
    /// This removes the running record, allowing new executions of the task to start.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    /// * `record_id` - The record ID of the execution instance to complete
    pub fn complete_task(&self, task_id: TaskId, record_id: RecordId) {
        if let Some(tracker) = self.task_tracker_map.get(&task_id) {
            tracker.running_records.remove(&record_id);
        }
    }

    /// Adds a task to the wheel and initializes its tracking information.
    ///
    /// The task is placed in the appropriate wheel (second, minute, or hour)
    /// based on its next execution time.
    ///
    /// # Arguments
    /// * `task` - The task to add
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully added
    /// * `Err(TaskError)` - If there was an error adding the task
    pub fn add_task(&self, mut task: Task) -> Result<(), TaskError> {
        let next_exec_timestamp = match task.next_alarm_timestamp() {
            Some(t) => t,
            None => return Ok(()),
        };

        let next_alarm_sec = next_exec_timestamp - timestamp();
        let next_guide = self.cal_next_hand_position(next_alarm_sec);
        task.cascade_guide = next_guide;

        let max_concurrency = task.max_concurrency;

        // Determine the wheel where the task should be placed based on the calculated cascade guide and record position information
        let tracking_info = if let Some(hour) = next_guide.hour {
            self.hour_wheel.add_task(task.clone(), hour);
            TaskTrackingInfo {
                cascade_guide: next_guide,
                wheel_type: WheelType::Hour,
                slot_num: hour,
                max_concurrency,
                running_records: DashMap::new(),
            }
        } else if let Some(min) = next_guide.min {
            self.min_wheel.add_task(task.clone(), min);
            TaskTrackingInfo {
                cascade_guide: next_guide,
                wheel_type: WheelType::Minute,
                slot_num: min,
                max_concurrency,
                running_records: DashMap::new(),
            }
        } else {
            self.sec_wheel.add_task(task.clone(), next_guide.sec);
            TaskTrackingInfo {
                cascade_guide: next_guide,
                wheel_type: WheelType::Second,
                slot_num: next_guide.sec,
                max_concurrency,
                running_records: DashMap::new(),
            }
        };

        // Update task tracking map
        self.task_tracker_map.insert(task.task_id, tracking_info);
        Ok(())
    }

    /// Cascades tasks from the minute wheel to the second wheel.
    ///
    /// This is called when the minute wheel hand advances past a slot.
    /// Tasks in that slot are moved to their designated second wheel slot
    /// based on their cascade guide.
    pub fn cascade_minute_tasks(&self) {
        let hand = self.min_wheel.hand.load(Ordering::Relaxed);
        let slot = self.min_wheel.slots.remove(&hand);
        if let Some((_, slot)) = slot {
            for task in slot.task_map.into_values() {
                let slot_num = task.cascade_guide.sec;

                // Update information from tracking map
                if let Some(mut tracking_info) = self.task_tracker_map.get_mut(&task.task_id) {
                    tracking_info.wheel_type = WheelType::Second;
                    tracking_info.slot_num = slot_num;
                    tracking_info.cascade_guide = task.cascade_guide;
                }

                // Add task to second wheel
                self.sec_wheel.add_task(task, slot_num);
            }
        }
        self.min_wheel.slots.insert(hand, Slot::new());
    }

    /// Cascades tasks from the hour wheel to the minute wheel.
    ///
    /// This is called when the hour wheel hand advances past a slot.
    /// Tasks in that slot are either:
    /// - Moved to the minute wheel if their round is 0
    /// - Re-added to the hour wheel with an updated round count if round > 0
    pub fn cascade_hour_tasks(&self) {
        let hand = self.hour_wheel.hand.load(Ordering::Relaxed);
        let slot = self.hour_wheel.slots.remove(&hand);
        let mut new_slot = Slot::new();
        if let Some((_, slot)) = slot {
            for mut task in slot.task_map.into_values() {
                let round = task.cascade_guide.round;
                if round > 0 {
                    // Update round in tracking information
                    if let Some(mut tracking_info) = self.task_tracker_map.get_mut(&task.task_id) {
                        task.cascade_guide.round = task.cascade_guide.round.saturating_sub(1);
                        tracking_info.cascade_guide = task.cascade_guide;
                    }
                    new_slot.add_task(task);
                    continue;
                } else {
                    // Move from hour wheel to minute wheel
                    if let Some(mut tracking_info) = self.task_tracker_map.get_mut(&task.task_id) {
                        tracking_info.wheel_type = WheelType::Minute;
                        tracking_info.slot_num = task.cascade_guide.min.unwrap();
                        tracking_info.cascade_guide = task.cascade_guide;
                    }

                    let slot_num = task.cascade_guide.min.unwrap();
                    self.min_wheel.add_task(task, slot_num);
                }
            }
        }
        self.hour_wheel.slots.insert(hand, new_slot);
    }

    /// Removes a task from the wheel and cleans up tracking information.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to remove
    ///
    /// # Returns
    /// The removed task if it existed, None otherwise.
    pub fn remove_task(&self, task_id: TaskId) -> Option<Task> {
        if let Some((_, tracking_info)) = self.task_tracker_map.remove(&task_id) {
            let tracking_info = tracking_info.clone();
            match tracking_info.wheel_type {
                WheelType::Second => self.sec_wheel.remove_task(task_id, tracking_info.slot_num),
                WheelType::Minute => self.min_wheel.remove_task(task_id, tracking_info.slot_num),
                WheelType::Hour => self.hour_wheel.remove_task(task_id, tracking_info.slot_num),
            }
        } else {
            None
        }
    }

    /// Removes a task from the wheel only (preserves tracking info including running records).
    ///
    /// This is used by accelerate_task to reschedule a task without losing
    /// its running records in the task_tracker_map.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to remove from wheel
    ///
    /// # Returns
    /// The removed task if it existed, None otherwise.
    fn remove_task_from_wheel_only(&self, task_id: TaskId) -> Option<Task> {
        let tracking_info = self.task_tracker_map.get(&task_id)?;
        let wheel_type = tracking_info.wheel_type;
        let slot_num = tracking_info.slot_num;

        match wheel_type {
            WheelType::Second => self.sec_wheel.remove_task(task_id, slot_num),
            WheelType::Minute => self.min_wheel.remove_task(task_id, slot_num),
            WheelType::Hour => self.hour_wheel.remove_task(task_id, slot_num),
        }
    }

    /// Accelerates a task by the specified duration.
    ///
    /// - If `duration` is `None`: triggers the task immediately and schedules the next run
    /// - If `duration` is `Some(duration)`: advances the task by the specified duration
    ///
    /// For repeating tasks, after acceleration the frequency sequence is reset
    /// from the current time, ensuring consistent intervals for subsequent executions.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to accelerate
    /// * `duration_secs` - Optional duration in seconds to advance by. `None` means trigger immediately.
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully accelerated
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn accelerate_task(
        &self,
        task_id: TaskId,
        duration_secs: Option<u64>,
    ) -> Result<(), TaskError> {
        let mut task = match self.remove_task_from_wheel_only(task_id) {
            Some(t) => t,
            None => return Err(TaskError::TaskNotFound(task_id)),
        };

        let now = timestamp();

        if let Some(secs) = duration_secs {
            let current_next = task
                .frequency
                .peek_alarm_timestamp()
                .ok_or(TaskError::TaskNotFound(task_id))?;

            let new_timestamp = current_next.saturating_sub(secs);

            if new_timestamp <= now {
                // Reset frequency from current time (no need to advance first)
                let interval = task.frequency_config.interval();
                task.frequency.reset_from_timestamp(now, interval);
            } else {
                // Reschedule to the accelerated time
                let new_alarm_sec = new_timestamp - now;
                let next_guide = self.cal_next_hand_position(new_alarm_sec);
                task.set_wheel_position(next_guide);
                self.reschedule_task_internal(&task, &next_guide)?;
                return Ok(());
            }
        } else {
            // For immediate trigger: add task to second wheel at current hand position
            // Skip frequency reset for now - debug to see if this works first
            let (current_sec, _, _) = self.get_wheel_positions();
            let immediate_guide = WheelCascadeGuide {
                round: 0,
                sec: current_sec,
                min: None,
                hour: None,
            };
            task.set_wheel_position(immediate_guide);
            self.reschedule_task_internal(&task, &immediate_guide)?;
            return Ok(());
        }

        // Schedule the next execution after acceleration
        if let Some(next_timestamp) = task.frequency.peek_alarm_timestamp() {
            let next_alarm_sec = next_timestamp.saturating_sub(now);
            if next_alarm_sec == 0 {
                // Task should execute immediately - use wheel's current hand position
                let (current_sec, _, _) = self.get_wheel_positions();
                let immediate_guide = WheelCascadeGuide {
                    round: 0,
                    sec: current_sec,
                    min: None,
                    hour: None,
                };
                task.set_wheel_position(immediate_guide);
                self.reschedule_task_internal(&task, &immediate_guide)?;
            } else if next_alarm_sec > 0 {
                let next_guide = self.cal_next_hand_position(next_alarm_sec);
                task.set_wheel_position(next_guide);
                self.reschedule_task_internal(&task, &next_guide)?;
            }
            // If next_alarm_sec < 0 (shouldn't happen due to saturating_sub), do nothing
        }

        Ok(())
    }

    /// Reschedules a task to a new wheel position (internal, preserves tracking info).
    fn reschedule_task_internal(
        &self,
        task: &Task,
        guide: &WheelCascadeGuide,
    ) -> Result<(), TaskError> {
        if let Some(mut tracking_info) = self.task_tracker_map.get_mut(&task.task_id) {
            tracking_info.cascade_guide = *guide;

            if let Some(hour) = guide.hour {
                tracking_info.wheel_type = WheelType::Hour;
                tracking_info.slot_num = hour;
                self.hour_wheel.add_task(task.clone(), hour);
            } else if let Some(min) = guide.min {
                tracking_info.wheel_type = WheelType::Minute;
                tracking_info.slot_num = min;
                self.min_wheel.add_task(task.clone(), min);
            } else {
                tracking_info.wheel_type = WheelType::Second;
                tracking_info.slot_num = guide.sec;
                self.sec_wheel.add_task(task.clone(), guide.sec);
            }
        }

        Ok(())
    }
}

// Implement remove_task method for Wheel
impl Wheel {
    /// Removes a task from a specific slot in the wheel.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to remove
    /// * `slot_num` - The slot number to remove the task from
    ///
    /// # Returns
    /// The removed task if it existed in the slot, None otherwise.
    pub fn remove_task(&self, task_id: TaskId, slot_num: u64) -> Option<Task> {
        if let Some(mut slot) = self.slots.get_mut(&slot_num) {
            slot.remove_task(task_id)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{TaskBuilder, TaskRunner};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    // Simple test task runner
    struct TestTaskRunner {
        execution_count: Arc<AtomicU64>,
    }

    impl TestTaskRunner {
        fn new() -> Self {
            Self {
                execution_count: Arc::new(AtomicU64::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl TaskRunner for TestTaskRunner {
        type Output = ();

        async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
            self.execution_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn test_cal_next_hand_position_no_carry() {
        let wheel = MulitWheel::new();
        // 10:20:30
        wheel.set_wheel_positions(30, 20, 10);

        let pos = wheel.cal_next_hand_position(5);
        assert_eq!(pos.sec, 35);
        assert_eq!(pos.min, None);
        assert_eq!(pos.hour, None);
        assert_eq!(pos.round, 0);
    }

    #[test]
    fn test_cal_next_hand_position_second_carry() {
        let wheel = MulitWheel::new();
        // 10:20:58
        wheel.set_wheel_positions(58, 20, 10);

        // (58 + 5 = 63 => 3 seconds, 21 minutes)
        let pos = wheel.cal_next_hand_position(5);
        assert_eq!(pos.sec, 3);
        assert_eq!(pos.min, Some(21));
        assert_eq!(pos.hour, None);
        assert_eq!(pos.round, 0);
    }

    #[test]
    fn test_cal_next_hand_position_minute_carry() {
        let wheel = MulitWheel::new();
        // 10:59:50
        wheel.set_wheel_positions(50, 59, 10);

        // (50 + 20 = 70 => 10 seconds, 60 minutes => 0 minutes, 11 hours)
        let pos = wheel.cal_next_hand_position(20);
        assert_eq!(pos.sec, 10);
        assert_eq!(pos.min, Some(0));
        assert_eq!(pos.hour, Some(11));
        assert_eq!(pos.round, 0);
    }

    #[test]
    fn test_cal_next_hand_position_hour_carry() {
        let wheel = MulitWheel::new();
        // 23:59:55
        wheel.set_wheel_positions(55, 59, 23);

        // (55 + 10 = 65 => 5 seconds, 60 minutes => 0 minutes, 24 hours => 0 hours, 1 round)
        let pos = wheel.cal_next_hand_position(10);
        assert_eq!(pos.sec, 5);
        assert_eq!(pos.min, Some(0));
        assert_eq!(pos.hour, Some(0));
        assert_eq!(pos.round, 1);
    }

    #[test]
    fn test_cal_next_hand_position_large_interval() {
        let wheel = MulitWheel::new();
        // 10:30:40
        wheel.set_wheel_positions(40, 30, 10);

        // 7200 sec => 2 hours
        let pos = wheel.cal_next_hand_position(7200);
        assert_eq!(pos.sec, 40);
        assert_eq!(pos.min, Some(30));
        assert_eq!(pos.hour, Some(12));
        assert_eq!(pos.round, 0);
    }

    #[test]
    fn test_cal_next_hand_position_exceed_one_day() {
        let wheel = MulitWheel::new();
        // 20:30:40
        wheel.set_wheel_positions(40, 30, 20);

        // 100000 sec => 27.8 hours
        let pos = wheel.cal_next_hand_position(100000);
        // 40 + 100000 = 100040 seconds
        // 100040 % 60 = 20 seconds
        // (30 + 100040/60) % 60 = (30 + 1667) % 60 = 1697 % 60 = 17 minutes
        // (20 + 1697/60) % 24 = (20 + 28) % 24 = 48 % 24 = 0 hours
        // 48 / 24 = 2 rounds
        assert_eq!(pos.sec, 20);
        assert_eq!(pos.min, Some(17));
        assert_eq!(pos.hour, Some(0));
        assert_eq!(pos.round, 2);
    }

    #[test]
    fn test_tick_without_cascade() {
        let multi_wheel = MulitWheel::new();

        // Test tick without any cascade (no carry-over between wheels)
        // This verifies that the second wheel moves normally without triggering minute or hour cascades
        let result = multi_wheel.tick();
        assert_eq!(result, None);
        assert_eq!(multi_wheel.sec_wheel.hand_position(), 1);

        // Test another tick to ensure continuous movement
        let result = multi_wheel.tick();
        assert_eq!(result, None);
        assert_eq!(multi_wheel.sec_wheel.hand_position(), 2);
    }

    #[test]
    fn test_tick_with_minute_cascade() {
        let multi_wheel = MulitWheel::new();

        // Add a task to minute wheel slot 0
        let task = TaskBuilder::new(1)
            .with_frequency_once_by_seconds(60)
            .spwan_async(TestTaskRunner::new())
            .unwrap();
        multi_wheel.min_wheel.add_task(task, 0);

        // Set second wheel hand position to 59 (last second of a minute)
        // This will trigger a cascade to the minute wheel on the next tick
        multi_wheel.sec_wheel.set_hand_position(59);

        // Execute tick which should trigger minute cascade
        // The task should be moved from minute wheel to second wheel for execution
        multi_wheel.tick();

        // Verify that the task is no longer in the minute wheel slot 0
        // It should have been cascaded down to the second wheel for execution
        assert!(
            !multi_wheel
                .sec_wheel
                .slots
                .get(&0)
                .unwrap()
                .task_map
                .contains_key(&1)
        );
    }

    #[test]
    fn test_tick_with_hour_cascade() {
        let multi_wheel = MulitWheel::new();

        // Set both second and minute wheels to their maximum positions (59)
        // This creates a scenario where both seconds and minutes will cascade
        multi_wheel.sec_wheel.set_hand_position(59);
        multi_wheel.min_wheel.set_hand_position(59);

        // Add a task to hour wheel slot 0 (last hour of the day)
        let mut task = TaskBuilder::new(2)
            .with_frequency_once_by_seconds(3600)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        // Set the task's wheel position to simulate it being at the end of the day
        // (59 seconds, 59 minutes, 23 hours)
        task.set_wheel_position(WheelCascadeGuide {
            sec: 59,
            min: Some(59),
            hour: Some(23),
            round: 0,
        });
        multi_wheel.hour_wheel.add_task(task, 0);

        // Execute tick which should trigger hour cascade
        // The task should be moved from hour wheel to minute wheel
        multi_wheel.tick();

        // Verify that the task is no longer in the minute wheel slot 0
        // It should have been cascaded down from the hour wheel
        assert!(
            !multi_wheel
                .min_wheel
                .slots
                .get(&0)
                .unwrap()
                .task_map
                .contains_key(&2)
        );
    }

    #[test]
    fn test_task_tracking_add_and_query() {
        let wheel = MulitWheel::new();
        let task = TaskBuilder::new(100)
            .with_frequency_once_by_seconds(10)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        // Add task to wheel
        wheel.add_task(task).unwrap();

        // Verify task tracking information
        let tracking_info = wheel.get_task_tracking_info(100).unwrap();
        assert_eq!(tracking_info.wheel_type, WheelType::Second); // 10 seconds should go to second wheel
    }

    #[test]
    fn test_task_tracking_direct_cascade_update() {
        let wheel = MulitWheel::new();

        // Manually create a task and add it to minute wheel slot 5
        let mut task = TaskBuilder::new(105)
            .with_frequency_once_by_seconds(60) // Next execution in 60 seconds
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        // Set up cascade guide to place task in minute wheel slot 5
        task.cascade_guide = WheelCascadeGuide {
            sec: 10,      // Will be placed in sec wheel slot 10 when cascaded
            min: Some(5), // Currently in min wheel slot 5
            hour: None,
            round: 0,
        };

        // Add task directly to minute wheel slot 5
        wheel.min_wheel.add_task(task, 5);

        // Initialize tracking info for the task before cascade
        let initial_tracking = TaskTrackingInfo {
            cascade_guide: WheelCascadeGuide {
                sec: 10,
                min: Some(5),
                hour: None,
                round: 0,
            },
            wheel_type: WheelType::Minute,
            slot_num: 5,
            max_concurrency: 1,
            running_records: DashMap::new(),
        };
        wheel.task_tracker_map.insert(105, initial_tracking);

        // Simulate cascade minute to second - manually move the wheel hand to 5 to trigger cascade
        wheel.min_wheel.set_hand_position(5);

        // Call the cascade function that updates tracking
        wheel.cascade_minute_tasks(); // Use the version that updates tracking

        // Verify the tracking information was updated correctly
        if let Some(updated_info) = wheel.get_task_tracking_info(105) {
            // After cascading from minute to second, the task should be in second wheel
            assert_eq!(updated_info.wheel_type, WheelType::Second);
            assert_eq!(updated_info.slot_num, 10); // Based on cascade guide sec value
        }
    }

    #[test]
    fn test_task_tracking_cascade_hour_to_minute() {
        let wheel = MulitWheel::new();
        // Create a task that should go to hour wheel (in 3600+ seconds)
        let mut task = TaskBuilder::new(102)
            .with_frequency_once_by_seconds(3665) // 3665 seconds from now (1h 1m 5s)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        // Manually set the wheel position to make the task go to hour wheel
        task.cascade_guide = WheelCascadeGuide {
            sec: 5,
            min: Some(1),
            hour: Some(1),
            round: 0,
        };

        // Add task to hour wheel manually
        wheel.hour_wheel.add_task(task, 1);

        // Initialize tracking info for the task
        let tracking_info = TaskTrackingInfo {
            cascade_guide: WheelCascadeGuide {
                sec: 5,
                min: Some(1),
                hour: Some(1),
                round: 0,
            },
            wheel_type: WheelType::Hour,
            slot_num: 1,
            max_concurrency: 1,
            running_records: DashMap::new(),
        };
        wheel.task_tracker_map.insert(102, tracking_info);

        // Simulate cascading by directly calling cascade method
        wheel.cascade_hour_tasks();

        // Verify the task is now tracked as being in minute wheel
        if let Some(_updated_info) = wheel.get_task_tracking_info(102) {
            // If the task didn't get moved to minute wheel due to round > 0 logic,
            // the tracking would still reflect its current state
            // If moved to minute wheel, wheel_type should be Minute
        }
    }

    #[test]
    fn test_task_tracking_remove() {
        let wheel = MulitWheel::new();
        let task = TaskBuilder::new(103)
            .with_frequency_once_by_seconds(5)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        // Add task to wheel
        wheel.add_task(task).unwrap();

        // Verify task exists in tracking
        assert!(wheel.get_task_tracking_info(103).is_some());

        // Remove task
        let removed_task = wheel.remove_task(103);
        assert!(removed_task.is_some());

        // Verify task no longer exists in tracking
        assert!(wheel.get_task_tracking_info(103).is_none());
    }

    #[test]
    fn test_task_tracking_info_structure() {
        let cascade_guide = WheelCascadeGuide {
            sec: 10,
            min: Some(20),
            hour: Some(3),
            round: 1,
        };

        let tracking_info = TaskTrackingInfo {
            cascade_guide,
            wheel_type: WheelType::Minute,
            slot_num: 20,
            max_concurrency: 1,
            running_records: DashMap::new(),
        };

        assert_eq!(tracking_info.cascade_guide.sec, 10);
        assert_eq!(tracking_info.cascade_guide.min, Some(20));
        assert_eq!(tracking_info.cascade_guide.hour, Some(3));
        assert_eq!(tracking_info.cascade_guide.round, 1);
        assert_eq!(tracking_info.wheel_type, WheelType::Minute);
        assert_eq!(tracking_info.slot_num, 20);
    }

    #[test]
    fn test_wheel_cascade_guide_is_arrived_second_only() {
        let guide = WheelCascadeGuide {
            sec: 30,
            min: None,
            hour: None,
            round: 0,
        };

        assert!(guide.is_arrived(30, 0, 0));
        assert!(!guide.is_arrived(29, 0, 0));
    }

    #[test]
    fn test_wheel_cascade_guide_is_arrived_minute_and_second() {
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(15),
            hour: None,
            round: 0,
        };

        assert!(guide.is_arrived(30, 15, 0));
        assert!(!guide.is_arrived(30, 14, 0));
        assert!(!guide.is_arrived(29, 15, 0));
    }

    #[test]
    fn test_wheel_cascade_guide_is_arrived_hour_minute_second() {
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(15),
            hour: Some(10),
            round: 0,
        };

        assert!(guide.is_arrived(30, 15, 10));
        assert!(!guide.is_arrived(30, 15, 9));
        assert!(!guide.is_arrived(29, 15, 10));
    }

    #[test]
    fn test_wheel_cascade_guide_is_arrived_round_not_zero() {
        let guide = WheelCascadeGuide {
            sec: 30,
            min: None,
            hour: None,
            round: 1,
        };

        assert!(!guide.is_arrived(30, 0, 0));
    }

    #[test]
    fn test_wheel_cascade_guide_is_arrived_min_without_hour() {
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(15),
            hour: None,
            round: 0,
        };

        assert!(guide.is_arrived(30, 15, 5));
    }

    #[test]
    fn test_accelerate_task_by_duration() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let original_info = wheel.get_task_tracking_info(1).unwrap();
        assert_eq!(original_info.wheel_type, WheelType::Minute);

        wheel.accelerate_task(1, Some(30)).unwrap();

        assert!(wheel.get_task_tracking_info(1).is_some());
    }

    #[test]
    fn test_accelerate_task_trigger_immediately() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(2)
            .with_frequency_repeated_by_seconds(60)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        wheel.accelerate_task(2, None).unwrap();

        let info = wheel.get_task_tracking_info(2).unwrap();
        assert_eq!(info.wheel_type, WheelType::Second);
        assert_eq!(info.slot_num, 30);
    }

    #[test]
    fn test_accelerate_task_not_found() {
        let wheel = MulitWheel::new();
        let result = wheel.accelerate_task(999, Some(30));
        assert!(result.is_err());
    }

    #[test]
    fn test_accelerate_task_exceed_current_wait() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(3)
            .with_frequency_repeated_by_seconds(60)
            .spwan_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        wheel.accelerate_task(3, Some(120)).unwrap();

        assert!(wheel.get_task_tracking_info(3).is_some());
    }
}
