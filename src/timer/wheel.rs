use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

use crate::{error::TaskError, task::Task, timer::slot::Slot, utils::timestamp};

pub(crate) struct MulitWheel {
    sec_wheel: Wheel,
    min_wheel: Wheel,
    hour_wheel: Wheel,
}

impl MulitWheel {
    pub(crate) fn new() -> Self {
        Self {
            sec_wheel: Wheel::new(60),
            min_wheel: Wheel::new(60),
            hour_wheel: Wheel::new(24),
        }
    }

    pub(crate) fn move_second_hand(&self) {
        let need_move = self.sec_wheel.hand_move();
        if need_move {
            let need_move = self.min_wheel.hand_move();
            if need_move {
                self.hour_wheel.hand_move();
            }
        }
    }

    pub(crate) fn cal_next_hand_position(&self, next_alarm_sec: u64) -> MultiWheelPosition {
        let current_second = self.sec_wheel.hand.load(Ordering::Relaxed);
        let current_minute = self.min_wheel.hand.load(Ordering::Relaxed);
        let current_hour = self.hour_wheel.hand.load(Ordering::Relaxed);

        if next_alarm_sec < 60 {
            MultiWheelPosition {
                sec: (current_second + next_alarm_sec) % 60,
                min: None,
                hour: None,
                round: 0,
            }
        } else {
            let total_seconds = current_second + next_alarm_sec;
            let final_sec = total_seconds % 60;

            let total_minutes = current_minute + (total_seconds / 60);
            let final_min = total_minutes % 60;

            if next_alarm_sec < 3600 && total_minutes < 60 {
                // one hour to one day, and no minute carry to hour
                MultiWheelPosition {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: None,
                    round: 0,
                }
            } else {
                // one day to more day, or minute carry to hour
                let total_hours = current_hour + (total_minutes / 60);
                let final_hour = total_hours % 24;
                let round = total_hours / 24;

                MultiWheelPosition {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: Some(final_hour),
                    round,
                }
            }
        }
    }

    pub(crate) fn add_task(&self, mut task: Task) -> Result<(), TaskError> {
        let next_exec_timestamp = match task.get_next_alarm_timestamp() {
            Some(t) => t,
            None => return Ok(()),
        };

        let next_alarm_sec = next_exec_timestamp - timestamp();

        let next_pos = self.cal_next_hand_position(next_alarm_sec);
        task.wheel_position = next_pos;

        if let Some(hand) = next_pos.hour {
            self.hour_wheel.add_task(task, hand);
        } else if let Some(hand) = next_pos.min {
            self.min_wheel.add_task(task, hand);
        } else {
            self.sec_wheel.add_task(task, next_pos.sec);
        }

        Ok(())
    }
}

pub(crate) struct Wheel {
    slots: DashMap<u64, Slot>,
    hand: Arc<AtomicU64>,
    num_slots: u64,
}

impl Wheel {
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

    /// Move the hand to the next slot.
    /// Returns true if the hand moves to the beginning of the wheel.
    pub(crate) fn hand_move(&self) -> bool {
        let pre_hand = self.hand.fetch_add(1, Ordering::Relaxed);
        if pre_hand == (self.num_slots - 1) {
            self.hand.store(0, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub(crate) fn add_task(&self, task: Task, slot_num: u64) {
        self.slots.get_mut(&slot_num).unwrap().add_task(task);
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub(crate) struct MultiWheelPosition {
    pub sec: u64,
    pub min: Option<u64>,
    pub hour: Option<u64>,
    pub round: u64,
}

impl MultiWheelPosition {
    pub(crate) fn is_arrived(&self) -> bool {
        todo!()
    }
}
