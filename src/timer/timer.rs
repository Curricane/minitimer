use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_channel::Sender;

use crate::timer::{Clock, TimerEvent};

/// Timer that generates periodic tick events at 1-second intervals.
///
/// The Timer is responsible for generating time-based events that drive
/// the execution of scheduled tasks. It uses a Clock to generate ticks
/// and sends TimerEvent::Tick events through a channel.
pub struct Timer {
    clock: Clock,
    event_sender: Sender<TimerEvent>,
    is_running: Arc<AtomicBool>,
}

impl Timer {
    /// Creates a new Timer instance.
    ///
    /// # Arguments
    /// * `event_sender` - The channel sender for sending timer events
    pub fn new(event_sender: Sender<TimerEvent>) -> Self {
        Self {
            clock: Clock::new(),
            event_sender,
            is_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Checks if the timer is currently running.
    ///
    /// # Returns
    /// `true` if the timer is running, `false` otherwise.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// Starts the timer and runs the event loop.
    ///
    /// This method runs an asynchronous loop that:
    /// 1. Waits for one second (via clock.tick())
    /// 2. Sends a TimerEvent::Tick event
    ///
    /// The loop continues until stop() is called.
    pub async fn run(&mut self) {
        self.is_running.store(true, Ordering::Relaxed);

        while self.is_running.load(Ordering::Relaxed) {
            self.clock.tick().await;

            if self.is_running.load(Ordering::Relaxed) {
                let _ = self.event_sender.send(TimerEvent::Tick).await;
            }
        }
    }

    /// Stops the timer.
    ///
    /// This sets the internal running flag to false, which will cause
    /// the run() method to exit on its next iteration.
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
