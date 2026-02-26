use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_channel::Sender;

use crate::timer::{Clock, TimerEvent};

pub struct Timer {
    clock: Clock,
    event_sender: Sender<TimerEvent>,
    is_running: Arc<AtomicBool>,
}

impl Timer {
    pub fn new(event_sender: Sender<TimerEvent>) -> Self {
        Self {
            clock: Clock::new(),
            event_sender,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    pub async fn run(&mut self) {
        self.is_running.store(true, Ordering::Relaxed);

        while self.is_running.load(Ordering::Relaxed) {
            self.clock.tick().await;

            if self.is_running.load(Ordering::Relaxed) {
                let _ = self.event_sender.send(TimerEvent::Tick).await;
            }
        }
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
    }
}

impl Clone for Timer {
    fn clone(&self) -> Self {
        Self {
            clock: Clock::new(),
            event_sender: self.event_sender.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

unsafe impl Send for Timer {}
unsafe impl Sync for Timer {}
