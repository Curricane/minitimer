use std::{collections::HashMap, mem::swap};

use crate::task::{TaskId, task::Task};

pub(crate) struct Slot {
    pub task_map: HashMap<TaskId, Task>,
}

impl Slot {
    pub(crate) fn new() -> Self {
        Slot {
            task_map: HashMap::new(),
        }
    }

    pub(crate) fn add_task(&mut self, task: Task) -> Option<Task> {
        self.task_map.insert(task.task_id, task)
    }

    #[allow(dead_code)]
    pub(crate) fn update_task(&mut self, mut task: Task) -> Option<Task> {
        match self.task_map.get_mut(&task.task_id) {
            Some(t) => {
                swap(t, &mut task);
                Some(task)
            }

            None => self.task_map.insert(task.task_id, task),
        }
    }

    pub(crate) fn remove_task(&mut self, task_id: TaskId) -> Option<Task> {
        self.task_map.remove(&task_id)
    }

    pub(crate) fn arrival_time_tasks(
        &mut self,
        current_sec: u64,
        current_min: u64,
        current_hour: u64,
    ) -> Vec<TaskId> {
        let mut task_id_vec = vec![];

        for (_, task) in self.task_map.iter_mut() {
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
