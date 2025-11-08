use async_channel::Sender;

use crate::timer::{Clock, TimerEvent};

pub struct Timer {
    clock: Clock,
    event_sender: Sender<TimerEvent>,
}
