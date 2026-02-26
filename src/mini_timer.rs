use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_channel::{Receiver, bounded};

use crate::error::TaskError;
use crate::task::{Task, TaskId};
use crate::timer::wheel::MulitWheel;
use crate::timer::{Timer, TimerEvent};

pub struct MiniTimer {
    wheel: Arc<MulitWheel>,
    event_receiver: Receiver<TimerEvent>,
    timer: Timer,
    is_running: Arc<AtomicBool>,
}

impl MiniTimer {
    pub fn new() -> Self {
        let (event_sender, event_receiver) = bounded(16);

        let wheel = Arc::new(MulitWheel::new());
        let timer = Timer::new(event_sender);

        Self {
            wheel,
            event_receiver,
            timer,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn add_task(&self, task: Task) -> Result<(), TaskError> {
        self.wheel.add_task(task)
    }

    pub fn remove_task(&self, task_id: TaskId) -> Option<Task> {
        self.wheel.remove_task(task_id)
    }

    pub fn contains_task(&self, task_id: TaskId) -> bool {
        self.wheel.get_task_tracking_info(task_id).is_some()
    }

    pub fn task_count(&self) -> usize {
        self.wheel.task_tracker_map.len()
    }

    pub async fn stop(&self) {
        if !self.is_running.load(Ordering::Relaxed) {
            return;
        }

        self.timer.stop();
        self.is_running.store(false, Ordering::Relaxed);
    }

    pub async fn run(&mut self) {
        self.is_running.store(true, Ordering::Relaxed);

        let mut timer = self.timer.clone();

        tokio::spawn(async move {
            timer.run().await;
        });

        loop {
            match self.event_receiver.recv().await {
                Ok(TimerEvent::Tick) => {
                    self.wheel.tick();

                    let arrived_tasks = self.wheel.execute_arrived_tasks();
                    for task in arrived_tasks {
                        let wheel = self.wheel.clone();
                        let task_id = task.task_id;
                        let runner = task.runner.clone();
                        let cascade_guide = task.cascade_guide;
                        let frequency = task.frequency;

                        tokio::spawn(async move {
                            let _ = runner.run().await;

                            let mut task_clone = Task {
                                task_id,
                                runner,
                                cascade_guide,
                                frequency,
                            };
                            let _ = wheel.reschedule_task(&mut task_clone);
                        });
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

    pub fn start(&self) {
        let mut timer = self.clone();
        tokio::spawn(async move {
            timer.run().await;
        });
    }

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
            timer: self.timer.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

unsafe impl Send for MiniTimer {}

unsafe impl Sync for MiniTimer {}
