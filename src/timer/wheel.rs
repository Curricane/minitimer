use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
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

    /// Set the positions of all wheels for testing purposes
    #[cfg(test)]
    pub(crate) fn set_wheel_positions(&self, sec: u64, min: u64, hour: u64) {
        self.sec_wheel.set_hand_position(sec);
        self.min_wheel.set_hand_position(min);
        self.hour_wheel.set_hand_position(hour);
    }

    /// Get the current positions of all wheels for testing purposes
    pub(crate) fn get_wheel_positions(&self) -> (u64, u64, u64) {
        (
            self.sec_wheel.hand.load(Ordering::Relaxed),
            self.min_wheel.hand.load(Ordering::Relaxed),
            self.hour_wheel.hand.load(Ordering::Relaxed),
        )
    }

    fn cascade_minute_tasks(&self) {
        let hand = self.min_wheel.hand.load(Ordering::Relaxed);
        let slot = self.min_wheel.slots.remove(&hand);
        if let Some((_, slot)) = slot {
            for task in slot.task_map.into_values() {
                let slot_num = task.wheel_position.sec;
                self.sec_wheel.add_task(task, slot_num);
            }
        }
        self.min_wheel.slots.insert(hand, Slot::new());
    }

    fn cascade_hour_tasks(&self) {
        let hand = self.hour_wheel.hand.load(Ordering::Relaxed);
        let slot = self.hour_wheel.slots.remove(&hand);
        let mut new_slot = Slot::new();
        if let Some((_, slot)) = slot {
            for mut task in slot.task_map.into_values() {
                let round = task.wheel_position.round;
                if round > 0 {
                    task.wheel_position.round.saturating_sub(1);
                    new_slot.add_task(task);
                    continue;
                } else {
                    task.wheel_position.round -= 1;
                    let slot_num = task.wheel_position.min.unwrap();
                    self.min_wheel.add_task(task, slot_num);
                }
            }
        }
        self.hour_wheel.slots.insert(hand, new_slot);
    }

    pub(crate) fn tick(&self) -> Option<u64> {
        self.sec_wheel
            .hand_move(1)
            .and_then(|carry| self.min_wheel.hand_move(carry))
            .and_then(|carry| self.hour_wheel.hand_move(carry))
    }

    pub(crate) fn cal_next_hand_position(&self, next_alarm_sec: u64) -> MultiWheelPosition {
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

                MultiWheelPosition {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: Some(final_hour),
                    round,
                }
            } else {
                // Only minute carry, no hour carry
                MultiWheelPosition {
                    sec: final_sec,
                    min: Some(final_min),
                    hour: None,
                    round: 0,
                }
            }
        } else {
            // No carry, only seconds level
            MultiWheelPosition {
                sec: final_sec,
                min: None,
                hour: None,
                round: 0,
            }
        }
    }

    pub(crate) fn add_task(&self, mut task: Task) -> Result<(), TaskError> {
        let next_exec_timestamp = match task.next_alarm_timestamp() {
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
    /// Returns the carry amount.
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

    /// Set the hand position of the wheel for testing purposes
    #[cfg(test)]
    pub(crate) fn set_hand_position(&self, position: u64) {
        self.hand
            .store(position % self.num_slots, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
