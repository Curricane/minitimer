use std::{collections::HashMap, mem::swap};

use crate::task::{TaskId, task::Task};

/// A slot in the time wheel that holds tasks scheduled for a specific time position.
///
/// Each slot contains a map of task IDs to tasks that are scheduled
/// to execute at that particular time position.
#[derive(Clone)]
pub(crate) struct Slot {
    pub task_map: HashMap<TaskId, Task>,
}

impl Slot {
    /// Creates a new empty slot.
    pub(crate) fn new() -> Self {
        Slot {
            task_map: HashMap::new(),
        }
    }

    /// Adds a task to the slot.
    ///
    /// # Arguments
    /// * `task` - The task to add
    ///
    /// # Returns
    /// The previous task with the same ID if one existed.
    pub(crate) fn add_task(&mut self, task: Task) -> Option<Task> {
        self.task_map.insert(task.task_id, task)
    }

    #[allow(dead_code)]
    /// Updates a task in the slot if it exists, otherwise adds it.
    ///
    /// # Arguments
    /// * `task` - The task to update or add
    ///
    /// # Returns
    /// The previous task if one existed with the same ID.
    pub(crate) fn update_task(&mut self, mut task: Task) -> Option<Task> {
        match self.task_map.get_mut(&task.task_id) {
            Some(t) => {
                swap(t, &mut task);
                Some(task)
            }

            None => self.task_map.insert(task.task_id, task),
        }
    }

    /// Removes a task from the slot.
    ///
    /// # Arguments
    /// * `task_id` - The ID of the task to remove
    ///
    /// # Returns
    /// The removed task if it existed.
    pub(crate) fn remove_task(&mut self, task_id: TaskId) -> Option<Task> {
        self.task_map.remove(&task_id)
    }

    /// Returns a list of task IDs that have arrived at their scheduled time.
    ///
    /// # Arguments
    /// * `current_sec` - Current second (0-59)
    /// * `current_min` - Current minute (0-59)
    /// * `current_hour` - Current hour (0-23)
    ///
    /// # Returns
    /// A vector of task IDs that are ready to execute.
    pub(crate) fn arrival_time_tasks(
        &self,
        current_sec: u64,
        current_min: u64,
        current_hour: u64,
    ) -> Vec<TaskId> {
        let mut task_id_vec = vec![];

        for (_, task) in self.task_map.iter() {
            if task.is_arrived(current_sec, current_min, current_hour) {
                task_id_vec.push(task.task_id);
            }
        }

        task_id_vec
    }

    #[allow(dead_code)]
    pub(crate) fn shrink(&mut self) {
        self.task_map.shrink_to(128);
    }
}
