use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use log::warn;

use crate::{
    error::TaskError,
    task::{RecordId, Task, TaskId, TaskState, frequency::FrequencySeconds},
    timer::slot::Slot,
    utils::timestamp,
};

const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
const SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;

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

    pub(crate) task_tracker_map: Arc<DashMap<TaskId, TaskTrackingInfo>>,
}

impl MulitWheel {
    /// Creates a new MultiWheel instance with three-level timing wheels.
    pub(crate) fn new() -> Self {
        Self {
            sec_wheel: Wheel::new(60),
            min_wheel: Wheel::new(60),
            hour_wheel: Wheel::new(24),
            task_tracker_map: Arc::new(DashMap::new()),
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
    /// A task whose frequency has no execution left is removed from the
    /// scheduler once it has been started.
    ///
    /// # Arguments
    /// * `task` - The task to process
    pub(crate) fn process_arrived_task(&self, task: Task) {
        let task_id = task.task_id;
        let max_concurrency = task.max_concurrency;
        let runner = task.runner.clone();

        match self.try_start_task(task_id, max_concurrency) {
            Some(record_id) => {
                let mut task_clone = task;
                // A task with no execution left is dropped from the scheduler
                // once this last execution finishes.
                let is_last_execution = !self.reschedule_task(&mut task_clone);

                let wheel = self.clone();
                tokio::spawn(async move {
                    let _ = runner.run().await;
                    wheel.complete_task(task_id, record_id);
                    if is_last_execution {
                        let _ = wheel.remove_task(task_id);
                    }
                });
            }
            None => {
                if task.frequency.peek_alarm_timestamp().is_some() {
                    let _ = self.add_task(task);
                } else {
                    // The only remaining execution was skipped, so the task is done.
                    let _ = self.remove_task(task_id);
                }
            }
        }
    }

    /// Reschedules a task for its next execution.
    ///
    /// This is called after a task completes execution to schedule its next run
    /// based on its frequency settings.
    ///
    /// Returns `true` if the task was successfully rescheduled, `false` if it
    /// has no execution left.
    pub(crate) fn reschedule_task(&self, task: &mut Task) -> bool {
        if task.frequency.peek_alarm_timestamp().is_none() {
            return false;
        }

        self.add_task(task.clone()).is_ok()
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
        let min_carry = total_seconds / 60;

        if min_carry == 0 {
            return WheelCascadeGuide {
                sec: final_sec,
                min: None,
                hour: None,
                round: 0,
            };
        }

        let total_minutes = current_minute + min_carry;
        let final_min = total_minutes % 60;
        let hour_carry = total_minutes / 60;

        if hour_carry == 0 {
            return WheelCascadeGuide {
                sec: final_sec,
                min: Some(final_min),
                hour: None,
                round: 0,
            };
        }

        let total_hours = current_hour + hour_carry;
        let final_hour = total_hours % 24;

        // The hand reaches the task's slot for the first time within the delay
        // itself; `round` counts the further rotations the task has to survive
        // before it is released, one rotation per hour-wheel lap.
        let round = (hour_carry - 1) / 24;

        WheelCascadeGuide {
            sec: final_sec,
            min: Some(final_min),
            hour: Some(final_hour),
            round,
        }
    }
}

/// A single level time wheel with a fixed number of slots.
///
/// Each wheel maintains a "hand" that points to the current position.
/// Tasks are placed in slots based on their scheduled execution time.
pub(crate) struct Wheel {
    slots: Arc<DashMap<u64, Slot>>,
    hand: Arc<AtomicU64>,
    num_slots: u64,
}

impl Wheel {
    /// Creates a new Wheel with the specified number of slots.
    pub(crate) fn new(num_slots: u64) -> Self {
        let slots = Arc::new(DashMap::new());
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

impl Clone for Wheel {
    fn clone(&self) -> Self {
        Self {
            slots: self.slots.clone(),
            hand: self.hand.clone(),
            num_slots: self.num_slots,
        }
    }
}

/// Guide for cascade positioning of tasks across multiple time wheels.
///
/// This structure tracks the exact position where a task should be placed
/// across the three-level time wheel system (second, minute, hour wheels).
#[derive(Debug, Default, Copy, Clone)]
pub struct WheelCascadeGuide {
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

/// Task status information structure.
///
/// Contains metadata about a task including its position in the wheel system,
/// the wheel type it's currently in, and running records for concurrency tracking.
#[derive(Debug, Clone)]
pub struct TaskStatus {
    pub cascade_guide: WheelCascadeGuide,
    pub wheel_type: WheelType,
    pub slot_num: u64,
    pub max_concurrency: usize,
    pub running_records: Vec<RecordId>,
    pub frequency_config: FrequencySeconds,
    /// The number of seconds remaining until the next execution.
    pub time_to_next_run: u64,
}

/// Represents the type of wheel a task is currently in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WheelType {
    Second,
    Minute,
    Hour,
}

impl Clone for MulitWheel {
    fn clone(&self) -> Self {
        Self {
            sec_wheel: self.sec_wheel.clone(),
            min_wheel: self.min_wheel.clone(),
            hour_wheel: self.hour_wheel.clone(),
            task_tracker_map: self.task_tracker_map.clone(),
        }
    }
}

impl MulitWheel {
    /// Quickly query task tracking information by task ID.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    ///
    /// # Returns
    /// Some(TaskTrackingInfo) if the task exists, None otherwise.
    pub(crate) fn task_tracking_info(&self, task_id: TaskId) -> Option<TaskTrackingInfo> {
        self.task_tracker_map.get(&task_id).map(|info| info.clone())
    }

    /// Gets the task status including frequency configuration.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task
    ///
    /// # Returns
    /// Some(TaskStatus) if the task exists, None otherwise.
    pub(crate) fn task_status(&self, task_id: TaskId) -> Option<TaskStatus> {
        let (task, tracking_info) = self.task(task_id)?;

        let frequency_config = task.frequency_config;
        let time_to_next_run = self.calculate_next_run_seconds(&tracking_info.cascade_guide);

        Some(TaskStatus {
            cascade_guide: tracking_info.cascade_guide,
            wheel_type: tracking_info.wheel_type,
            slot_num: tracking_info.slot_num,
            max_concurrency: tracking_info.max_concurrency,
            running_records: tracking_info
                .running_records
                .iter()
                .map(|r| *r.key())
                .collect(),
            frequency_config,
            time_to_next_run,
        })
    }

    /// Calculates the number of seconds until the next execution.
    ///
    /// A task is positioned by as many coordinates as the wheel it sits on
    /// needs: hour, minute and second on the hour wheel, minute and second on
    /// the minute wheel, second only on the second wheel. The coarser
    /// coordinates must not take part in the calculation.
    ///
    /// # Arguments
    /// * `guide` - The wheel cascade guide containing task position info
    ///
    /// # Returns
    /// The number of seconds remaining until the task's next execution.
    fn calculate_next_run_seconds(&self, guide: &WheelCascadeGuide) -> u64 {
        let (current_sec, current_min, current_hour) = self.get_wheel_positions();

        match (guide.hour, guide.min) {
            (Some(hour), Some(min)) => {
                let target = hour * SECONDS_PER_HOUR + min * SECONDS_PER_MINUTE + guide.sec;
                let current = current_hour * SECONDS_PER_HOUR
                    + current_min * SECONDS_PER_MINUTE
                    + current_sec;
                let until_slot = (target + SECONDS_PER_DAY - current) % SECONDS_PER_DAY;

                // Landing on the current slot means the hand has just passed
                // it, so the next visit takes a full day.
                let until_slot = if until_slot == 0 {
                    SECONDS_PER_DAY
                } else {
                    until_slot
                };

                until_slot + guide.round * SECONDS_PER_DAY
            }
            (None, Some(min)) => {
                let target = min * SECONDS_PER_MINUTE + guide.sec;
                let current = current_min * SECONDS_PER_MINUTE + current_sec;
                let until_slot = (target + SECONDS_PER_HOUR - current) % SECONDS_PER_HOUR;

                if until_slot == 0 {
                    SECONDS_PER_HOUR
                } else {
                    until_slot
                }
            }
            // A task without a minute position is placed by second only.
            _ => (guide.sec + SECONDS_PER_MINUTE - current_sec) % SECONDS_PER_MINUTE,
        }
    }

    pub(crate) fn task(&self, task_id: TaskId) -> Option<(Task, TaskTrackingInfo)> {
        let tracking_info = self.task_tracker_map.get(&task_id)?.clone();
        let task = match tracking_info.wheel_type {
            WheelType::Second => self
                .sec_wheel
                .slots
                .get(&tracking_info.slot_num)
                .and_then(|slot| slot.task_map.get(&task_id).cloned()),
            WheelType::Minute => self
                .min_wheel
                .slots
                .get(&tracking_info.slot_num)
                .and_then(|slot| slot.task_map.get(&task_id).cloned()),
            WheelType::Hour => self
                .hour_wheel
                .slots
                .get(&tracking_info.slot_num)
                .and_then(|slot| slot.task_map.get(&task_id).cloned()),
        };

        if task.is_none() {
            warn!("task not found in wheel but tracking info exists");
        }

        Some((task?, tracking_info))
    }

    /// Get all pending tasks (tasks scheduled and not currently running).
    ///
    /// # Returns
    /// A vector of task IDs that are waiting for their next execution.
    pub fn get_all_pending_tasks(&self) -> Vec<TaskId> {
        self.task_tracker_map
            .iter()
            .filter(|t| t.running_records.is_empty())
            .map(|r| *r.key())
            .collect()
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
        // A task id must not be scheduled twice: drop the previous placement so
        // replacing a task cannot leave the old schedule running.
        let _ = self.remove_task_from_wheel_only(task.task_id);

        let next_exec_timestamp = match task.next_alarm_timestamp() {
            Some(t) => t,
            None => return Ok(()),
        };

        // A task is built (and its first alarm computed) before it reaches the
        // timer, so the alarm can already have passed. Schedule it for the next
        // tick in that case instead of underflowing.
        let next_alarm_sec = next_exec_timestamp.saturating_sub(timestamp()).max(1);
        let next_guide = self.cal_next_hand_position(next_alarm_sec);
        task.cascade_guide = next_guide;

        let max_concurrency = task.max_concurrency;

        // Determine the wheel where the task should be placed based on the calculated cascade guide and record position information
        let (wheel_type, slot_num) = if let Some(hour) = next_guide.hour {
            self.hour_wheel.add_task(task.clone(), hour);
            (WheelType::Hour, hour)
        } else if let Some(min) = next_guide.min {
            self.min_wheel.add_task(task.clone(), min);
            (WheelType::Minute, min)
        } else {
            self.sec_wheel.add_task(task.clone(), next_guide.sec);
            (WheelType::Second, next_guide.sec)
        };

        // Update the tracking entry in place. Replacing it would hand out a
        // fresh, empty set of running records and drop the executions that are
        // still in flight, which disables the concurrency limit.
        self.task_tracker_map
            .entry(task.task_id)
            .and_modify(|info| {
                info.cascade_guide = next_guide;
                info.wheel_type = wheel_type;
                info.slot_num = slot_num;
                info.max_concurrency = max_concurrency;
            })
            .or_insert_with(|| TaskTrackingInfo {
                cascade_guide: next_guide,
                wheel_type,
                slot_num,
                max_concurrency,
                running_records: DashMap::new(),
            });

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
            for mut task in slot.task_map.into_values() {
                let slot_num = task.cascade_guide.sec;

                // The task is now scheduled by the second it holds, so the
                // coarser positions must not take part in arrival checks.
                task.cascade_guide.min = None;
                task.cascade_guide.hour = None;

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
                if task.cascade_guide.round > 0 {
                    task.cascade_guide.round = task.cascade_guide.round.saturating_sub(1);
                    // Update round in tracking information
                    if let Some(mut tracking_info) = self.task_tracker_map.get_mut(&task.task_id) {
                        tracking_info.cascade_guide = task.cascade_guide;
                    }
                    new_slot.add_task(task);
                    continue;
                }

                // The hour position has served its purpose; the task is placed
                // by minute and second from here on.
                task.cascade_guide.hour = None;

                match task.cascade_guide.min {
                    // A zero minute offset means the task is due within the
                    // minute that has just been filled, so it goes to the
                    // second wheel instead of waiting a full minute rotation.
                    Some(0) => {
                        task.cascade_guide.min = None;
                        let slot_num = task.cascade_guide.sec;
                        if let Some(mut tracking_info) =
                            self.task_tracker_map.get_mut(&task.task_id)
                        {
                            tracking_info.wheel_type = WheelType::Second;
                            tracking_info.slot_num = slot_num;
                            tracking_info.cascade_guide = task.cascade_guide;
                        }
                        self.sec_wheel.add_task(task, slot_num);
                    }
                    Some(slot_num) => {
                        if let Some(mut tracking_info) =
                            self.task_tracker_map.get_mut(&task.task_id)
                        {
                            tracking_info.wheel_type = WheelType::Minute;
                            tracking_info.slot_num = slot_num;
                            tracking_info.cascade_guide = task.cascade_guide;
                        }
                        self.min_wheel.add_task(task, slot_num);
                    }
                    None => {
                        let slot_num = task.cascade_guide.sec;
                        self.sec_wheel.add_task(task, slot_num);
                    }
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
    /// For repeating tasks, the `reset_frequency` parameter controls whether to reset
    /// the frequency sequence from the current time:
    /// - If `true`: resets the frequency sequence, ensuring consistent intervals for subsequent executions
    /// - If `false`: preserves the current frequency sequence position
    ///
    /// When the task is ready to execute immediately, it is processed via `process_arrived_task`,
    /// which handles concurrency control and rescheduling.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to accelerate
    /// * `duration_secs` - Optional duration in seconds to advance by. `None` means trigger immediately.
    /// * `reset_frequency` - Whether to reset the frequency sequence for repeating tasks
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully accelerated
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn accelerate_task(
        &self,
        task_id: TaskId,
        duration_secs: Option<u64>,
        reset_frequency: bool,
    ) -> Result<(), TaskError> {
        let mut task = match self.remove_task_from_wheel_only(task_id) {
            Some(t) => t,
            None => return Err(TaskError::TaskNotFound(task_id)),
        };

        let now = timestamp();

        match duration_secs {
            Some(secs) => self.accelerate_by_duration(&mut task, secs, now, reset_frequency),
            None => self.trigger_immediately(&mut task, now, reset_frequency),
        }
    }

    fn accelerate_by_duration(
        &self,
        task: &mut Task,
        secs: u64,
        now: u64,
        reset_frequency: bool,
    ) -> Result<(), TaskError> {
        // Calculate remaining wait time based on the task's current wheel position
        // instead of using frequency.peek_alarm_timestamp() which returns the NEXT
        // execution time after the current one (frequency state was already advanced
        // when the task was added to the wheel).
        let remaining_wait = self.calculate_next_run_seconds(&task.cascade_guide);

        if secs >= remaining_wait {
            self.trigger_immediately(task, now, reset_frequency)?;
        } else {
            let new_alarm_sec = remaining_wait - secs;
            let next_guide = self.cal_next_hand_position(new_alarm_sec);
            task.set_wheel_position(next_guide);
            self.reschedule_task_internal(task, &next_guide)?;
        }

        Ok(())
    }

    fn trigger_immediately(
        &self,
        task: &mut Task,
        now: u64,
        reset_frequency: bool,
    ) -> Result<(), TaskError> {
        if reset_frequency {
            let interval = task.frequency_config.interval();
            task.frequency.reset_from_timestamp(now, interval);
        }
        self.schedule_for_immediate_execution(task);
        Ok(())
    }

    fn schedule_for_immediate_execution(&self, task: &mut Task) {
        let (current_sec, _, _) = self.get_wheel_positions();
        let next_sec = (current_sec + 1) % 60;
        let immediate_guide = WheelCascadeGuide {
            round: 0,
            sec: next_sec,
            min: None,
            hour: None,
        };
        task.set_wheel_position(immediate_guide);
        let _ = self.reschedule_task_internal(task, &immediate_guide);
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

    /// Updates an existing task with a new Task.
    ///
    /// This replaces the existing task with a new one, preserving the task_id
    /// but using the new task's frequency, concurrency, and runner settings.
    /// The previously scheduled execution is cancelled, so the old runner and
    /// frequency cannot fire again.
    ///
    /// # Arguments
    /// * `task_id` - The unique identifier of the task to update
    /// * `new_task` - The new task to replace the existing one
    ///
    /// # Returns
    /// * `Ok(())` - If the task was successfully updated
    /// * `Err(TaskError)` - If the task doesn't exist
    pub fn update_task(&self, task_id: TaskId, new_task: Task) -> Result<(), TaskError> {
        if !self.task_tracker_map.contains_key(&task_id) {
            return Err(TaskError::TaskNotFound(task_id));
        }

        let mut new_task = new_task;
        new_task.task_id = task_id;

        if new_task.frequency.peek_alarm_timestamp().is_none() {
            let _ = self.remove_task(task_id);
            return Ok(());
        }

        // `add_task` drops the old placement of this id before scheduling the
        // new one and keeps the tracking entry (and its running records) alive.
        self.add_task(new_task)
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

        // (55 + 10 = 65 => 5 seconds, 60 minutes => 0 minutes, 24 hours => 0 hours)
        // The hour hand reaches the target slot on its next move, five seconds
        // away, so no extra rotation is needed.
        let pos = wheel.cal_next_hand_position(10);
        assert_eq!(pos.sec, 5);
        assert_eq!(pos.min, Some(0));
        assert_eq!(pos.hour, Some(0));
        assert_eq!(pos.round, 0);
    }

    #[test]
    fn test_cal_next_hand_position_day_rounds() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(0, 0, 0);

        // 24 hours: the target slot is reached exactly when the wheel comes
        // back around, with no further rotation to survive.
        let pos = wheel.cal_next_hand_position(24 * 3600);
        assert_eq!(pos.hour, Some(0));
        assert_eq!(pos.round, 0);

        // 25 hours: one extra lap after reaching the target slot
        let pos = wheel.cal_next_hand_position(25 * 3600);
        assert_eq!(pos.hour, Some(1));
        assert_eq!(pos.round, 1);

        // 48 hours: one extra lap, with the second one releasing the task
        let pos = wheel.cal_next_hand_position(48 * 3600);
        assert_eq!(pos.round, 1);

        // 49 hours
        let pos = wheel.cal_next_hand_position(49 * 3600);
        assert_eq!(pos.round, 2);
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
        // 28 hour-wheel moves => one extra lap
        assert_eq!(pos.sec, 20);
        assert_eq!(pos.min, Some(17));
        assert_eq!(pos.hour, Some(0));
        assert_eq!(pos.round, 1);
    }

    #[test]
    fn test_calculate_next_run_seconds_per_wheel() {
        let wheel = MulitWheel::new();
        // 10:20:30
        wheel.set_wheel_positions(30, 20, 10);

        // Second wheel task: only the second counts
        let guide = WheelCascadeGuide {
            sec: 35,
            min: None,
            hour: None,
            round: 0,
        };
        assert_eq!(wheel.calculate_next_run_seconds(&guide), 5);

        // Minute wheel task: a task 60 seconds away sits on the next minute
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(21),
            hour: None,
            round: 0,
        };
        assert_eq!(wheel.calculate_next_run_seconds(&guide), 60);

        // Hour wheel task: 10 seconds past the next hour
        let guide = WheelCascadeGuide {
            sec: 5,
            min: Some(0),
            hour: Some(11),
            round: 0,
        };
        assert_eq!(wheel.calculate_next_run_seconds(&guide), 39 * 60 + 35);

        // The same task one day out
        let guide = WheelCascadeGuide {
            sec: 5,
            min: Some(0),
            hour: Some(11),
            round: 1,
        };
        assert_eq!(
            wheel.calculate_next_run_seconds(&guide),
            SECONDS_PER_DAY + 39 * 60 + 35
        );
    }

    #[test]
    fn test_calculate_next_run_seconds_on_the_current_slot() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(0, 0, 0);

        // A task due in exactly 24 hours lands on the slot the hand is on
        let guide = WheelCascadeGuide {
            sec: 0,
            min: Some(0),
            hour: Some(0),
            round: 0,
        };
        assert_eq!(wheel.calculate_next_run_seconds(&guide), SECONDS_PER_DAY);

        // A task due in exactly one hour lands on the current minute
        let guide = WheelCascadeGuide {
            sec: 0,
            min: Some(0),
            hour: None,
            round: 0,
        };
        assert_eq!(wheel.calculate_next_run_seconds(&guide), SECONDS_PER_HOUR);
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
            .spawn_async(TestTaskRunner::new())
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
            .spawn_async(TestTaskRunner::new())
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
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        // Add task to wheel
        wheel.add_task(task).unwrap();

        // Verify task tracking information
        let tracking_info = wheel.task_tracking_info(100).unwrap();
        assert_eq!(tracking_info.wheel_type, WheelType::Second); // 10 seconds should go to second wheel
    }

    #[test]
    fn test_task_tracking_direct_cascade_update() {
        let wheel = MulitWheel::new();

        // Manually create a task and add it to minute wheel slot 5
        let mut task = TaskBuilder::new(105)
            .with_frequency_once_by_seconds(60) // Next execution in 60 seconds
            .spawn_async(TestTaskRunner::new())
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
        if let Some(updated_info) = wheel.task_tracking_info(105) {
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
            .spawn_async(TestTaskRunner::new())
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
        if let Some(_updated_info) = wheel.task_tracking_info(102) {
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
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        // Add task to wheel
        wheel.add_task(task).unwrap();

        // Verify task exists in tracking
        assert!(wheel.task_tracking_info(103).is_some());

        // Remove task
        let removed_task = wheel.remove_task(103);
        assert!(removed_task.is_some());

        // Verify task no longer exists in tracking
        assert!(wheel.task_tracking_info(103).is_none());
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
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let original_info = wheel.task_tracking_info(1).unwrap();
        assert_eq!(original_info.wheel_type, WheelType::Minute);

        wheel.accelerate_task(1, Some(30), true).unwrap();

        assert!(wheel.task_tracking_info(1).is_some());
    }

    #[test]
    fn test_accelerate_task_trigger_immediately() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(2)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        wheel.accelerate_task(2, None, true).unwrap();

        let info = wheel.task_tracking_info(2).unwrap();
        assert_eq!(info.wheel_type, WheelType::Second);
        assert_eq!(info.slot_num, 31);
    }

    #[test]
    fn test_accelerate_task_not_found() {
        let wheel = MulitWheel::new();
        let result = wheel.accelerate_task(999, Some(30), true);
        assert!(result.is_err());
    }

    #[test]
    fn test_accelerate_task_exceed_current_wait() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(3)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        wheel.accelerate_task(3, Some(120), true).unwrap();

        assert!(wheel.task_tracking_info(3).is_some());
    }

    #[test]
    fn test_calculate_next_run_seconds_on_its_own_slot() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:45
        wheel.set_wheel_positions(45, 30, 10);

        // A task sitting on the slot the hour hand is on has just missed it,
        // so it waits for the next visit of that slot.
        let guide = WheelCascadeGuide {
            sec: 45,
            min: Some(30),
            hour: Some(10),
            round: 0,
        };

        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, SECONDS_PER_DAY);
    }

    #[test]
    fn test_calculate_next_run_seconds_later_same_day() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:45
        wheel.set_wheel_positions(45, 30, 10);

        // Target time is 10:35:30 (4 minutes 45 seconds later)
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(35),
            hour: Some(10),
            round: 0,
        };

        // Expected: (10*3600 + 35*60 + 30) - (10*3600 + 30*60 + 45) = 285 seconds
        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 285);
    }

    #[test]
    fn test_calculate_next_run_seconds_next_day() {
        let wheel = MulitWheel::new();
        // Set current time to 23:59:50
        wheel.set_wheel_positions(50, 59, 23);

        // Target time is 00:00:10 (next day, 20 seconds later)
        let guide = WheelCascadeGuide {
            sec: 10,
            min: Some(0),
            hour: Some(0),
            round: 0,
        };

        // Expected: (24*3600 - 86390 + 10) = 20 seconds
        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 20);
    }

    #[test]
    fn test_calculate_next_run_seconds_with_round() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:45
        wheel.set_wheel_positions(45, 30, 10);

        // Same slot, so the task waits out the visit that follows the one it
        // just missed (24 hours) plus the extra lap it carries (1 round)
        let guide = WheelCascadeGuide {
            sec: 45,
            min: Some(30),
            hour: Some(10),
            round: 1,
        };

        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 2 * SECONDS_PER_DAY);
    }

    #[test]
    fn test_calculate_next_run_seconds_with_multiple_rounds() {
        let wheel = MulitWheel::new();
        // Set current time to 12:00:00
        wheel.set_wheel_positions(0, 0, 12);

        // Target time with 2 rounds (2 days later)
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(15),
            hour: Some(14),
            round: 2,
        };

        // Expected: (14*3600 + 15*60 + 30 - 12*3600) + 2 * 86400
        // = (51330 - 43200) + 172800 = 8130 + 172800 = 180930 seconds
        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 180930);
    }

    #[test]
    fn test_calculate_next_run_seconds_second_only() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:45
        wheel.set_wheel_positions(45, 30, 10);

        // A task on the second wheel is placed by its second slot only
        let guide = WheelCascadeGuide {
            sec: 50,
            min: None,
            hour: None,
            round: 0,
        };

        // The second hand reaches slot 50 five seconds from now
        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 5);
    }

    #[test]
    fn test_calculate_next_run_seconds_minute_only() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:45
        wheel.set_wheel_positions(45, 30, 10);

        // A task on the minute wheel ignores the hour
        let guide = WheelCascadeGuide {
            sec: 30,
            min: Some(35),
            hour: None,
            round: 0,
        };

        // 10:30:45 -> 10:35:30 is 4 minutes 45 seconds
        let result = wheel.calculate_next_run_seconds(&guide);
        assert_eq!(result, 4 * 60 + 45);
    }

    #[test]
    fn test_task_status_time_to_next_run() {
        let wheel = MulitWheel::new();
        // Set current time to 10:30:00
        wheel.set_wheel_positions(0, 30, 10);

        // Create a task that runs in 5 minutes (300 seconds)
        let task = TaskBuilder::new(200)
            .with_frequency_once_by_seconds(300)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        // Get task status and verify time_to_next_run
        let status = wheel.task_status(200).unwrap();

        // Verify the cascade_guide has expected values
        // 300 seconds from 10:30:00 should be 35 minutes (no hour since it's within same hour)
        assert_eq!(status.cascade_guide.sec, 0);
        assert_eq!(status.cascade_guide.min, Some(35));
        assert_eq!(status.cascade_guide.hour, None); // No hour since it's within the same hour
        assert_eq!(status.cascade_guide.round, 0);

        // time_to_next_run should be the 300 seconds the task was configured with
        assert_eq!(
            status.time_to_next_run, 300,
            "Expected time_to_next_run to be 300 seconds, got {}",
            status.time_to_next_run
        );
    }

    #[test]
    fn test_update_task_with_task() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let original_status = wheel.task_status(1).unwrap();
        assert_eq!(original_status.max_concurrency, 1);

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(30)
            .with_max_concurrency(5)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(
            updated_status.frequency_config,
            FrequencySeconds::Repeated(30)
        );
        assert_eq!(updated_status.max_concurrency, 5);
    }

    #[test]
    fn test_update_task_not_found() {
        let wheel = MulitWheel::new();

        let new_task = TaskBuilder::new(999)
            .with_frequency_repeated_by_seconds(30)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        let result = wheel.update_task(999, new_task);
        assert!(result.is_err());
    }

    #[test]
    fn test_update_task_different_frequency() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_once_by_seconds(120)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(updated_status.frequency_config, FrequencySeconds::Once(120));
    }

    #[test]
    fn test_update_task_preserves_task_in_wheel() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        assert!(wheel.task_tracking_info(1).is_some());

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(30)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        assert!(
            wheel.task_tracking_info(1).is_some(),
            "Task should still exist after update"
        );
    }

    #[test]
    fn test_update_task_with_countdown_frequency() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_count_down_by_seconds(3, 10)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(
            updated_status.frequency_config,
            FrequencySeconds::CountDown(3, 10)
        );
    }

    #[test]
    fn test_update_task_long_interval() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(7200)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(
            updated_status.frequency_config,
            FrequencySeconds::Repeated(7200)
        );
        assert_eq!(updated_status.wheel_type, WheelType::Hour);
    }

    #[test]
    fn test_update_task_preserves_running_records() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let record_id = wheel.try_start_task(1, 10).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(30)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert!(updated_status.running_records.contains(&record_id));
    }

    #[test]
    fn test_update_task_different_task_id_in_new_task() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(2)
            .with_frequency_repeated_by_seconds(30)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(
            updated_status.frequency_config,
            FrequencySeconds::Repeated(30)
        );
    }

    #[test]
    fn test_update_task_second_wheel_placement() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(10)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(updated_status.wheel_type, WheelType::Second);
    }

    #[test]
    fn test_update_task_minute_wheel_placement() {
        let wheel = MulitWheel::new();
        wheel.set_wheel_positions(30, 0, 0);

        let task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(60)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.add_task(task).unwrap();

        let new_task = TaskBuilder::new(1)
            .with_frequency_repeated_by_seconds(120)
            .spawn_async(TestTaskRunner::new())
            .unwrap();

        wheel.update_task(1, new_task).unwrap();

        let updated_status = wheel.task_status(1).unwrap();
        assert_eq!(updated_status.wheel_type, WheelType::Minute);
    }
}
