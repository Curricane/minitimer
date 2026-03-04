use std::{
    iter::{Peekable, StepBy},
    ops::RangeFrom,
};

use crate::utils::timestamp;

pub(crate) type SecondsState = Peekable<StepBy<RangeFrom<u64>>>;
const ONE_MINUTE: u64 = 60;

/// Frequency specification for task execution timing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrequencySeconds {
    /// Execute once after the specified number of seconds.
    Once(u64),
    /// Execute repeatedly at the specified interval (in seconds).
    Repeated(u64),
    /// Execute a specific number of times at the specified interval.
    CountDown(u64, u64),
}

impl FrequencySeconds {
    /// Returns the interval in seconds for this frequency.
    ///
    /// # Returns
    /// The interval between executions in seconds.
    pub(crate) fn interval(&self) -> u64 {
        match self {
            Self::Once(seconds) => *seconds,
            Self::Repeated(seconds) => *seconds,
            Self::CountDown(_, seconds) => *seconds,
        }
    }
}

impl Default for FrequencySeconds {
    fn default() -> FrequencySeconds {
        FrequencySeconds::Once(ONE_MINUTE)
    }
}

/// Internal state representation of task frequency.
///
/// This is used to track the next execution time for tasks.
#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum FrequencyState {
    /// Repeated execution at a fixed interval.
    SecondsRepeated(SecondsState),
    /// Countdown execution with a limited number of repetitions.
    SecondsCountDown(u64, SecondsState),
}

impl From<FrequencySeconds> for FrequencyState {
    fn from(frequency: FrequencySeconds) -> Self {
        match frequency {
            FrequencySeconds::Once(seconds) => {
                assert!(seconds > 0, "once frequency must be greater than 0");
                let state: SecondsState = ((timestamp() + seconds)..)
                    .step_by(seconds as usize)
                    .peekable();
                FrequencyState::SecondsRepeated(state)
            }
            FrequencySeconds::Repeated(seconds) => {
                assert!(seconds > 0, "repeated frequency must be greater than 0");
                let state: SecondsState = ((timestamp() + seconds)..)
                    .step_by(seconds as usize)
                    .peekable();
                FrequencyState::SecondsRepeated(state)
            }
            FrequencySeconds::CountDown(count_down, seconds) => {
                assert!(seconds > 0, "countdown initial must be greater than 0");
                let state: SecondsState = (timestamp() + seconds..)
                    .step_by(count_down as usize)
                    .peekable();
                FrequencyState::SecondsCountDown(count_down, state)
            }
        }
    }
}

impl FrequencyState {
    #[allow(dead_code)]
    /// Peeks at the next alarm timestamp without advancing the state.
    ///
    /// # Returns
    /// The next timestamp when the task should execute, or None if no more executions.
    pub(crate) fn peek_alarm_timestamp(&mut self) -> Option<u64> {
        match self {
            Self::SecondsRepeated(state) => state.peek().copied(),
            Self::SecondsCountDown(_, state) => state.peek().copied(),
        }
    }

    /// Gets the next alarm timestamp and advances the state.
    ///
    /// WARNING: This method advances the frequency state, meaning subsequent calls
    /// will return the next timestamp in the sequence. If you need to peek at the
    /// next timestamp without advancing, use `peek_alarm_timestamp()` instead.
    ///
    /// # Returns
    /// The next timestamp when the task should execute, or None if no more executions.
    pub(crate) fn next_alarm_timestamp(&mut self) -> Option<u64> {
        match self {
            Self::SecondsRepeated(state) => state.next(),
            Self::SecondsCountDown(_, state) => state.next(),
        }
    }

    #[allow(dead_code)]
    /// Decrements the countdown for CountDown frequency types.
    pub(crate) fn down_count(&mut self) {
        if let Self::SecondsCountDown(count, _) = self {
            *count = count.saturating_sub(1);
        }
    }

    /// Resets the frequency state from a given timestamp.
    ///
    /// This is used when accelerating a task to restart the frequency
    /// sequence from a new base time.
    ///
    /// # Arguments
    /// * `base_timestamp` - The new base timestamp to start the sequence from
    /// * `interval` - The interval in seconds between executions
    pub(crate) fn reset_from_timestamp(&mut self, base_timestamp: u64, interval: u64) {
        let new_state: SecondsState = ((base_timestamp + interval)..)
            .step_by(interval as usize)
            .peekable();

        match self {
            Self::SecondsRepeated(_) => {
                *self = Self::SecondsRepeated(new_state);
            }
            Self::SecondsCountDown(count, _) => {
                *self = Self::SecondsCountDown(*count, new_state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frequency_state_from_once() {
        let freq = FrequencySeconds::Once(10);
        let mut state = FrequencyState::from(freq);

        // For Once, we should get a timestamp in the future
        let now = crate::utils::timestamp();
        let alarm = state.peek_alarm_timestamp().unwrap();
        assert!(alarm >= now + 10);

        // Next call should give the same timestamp (peek doesn't advance)
        let alarm2 = state.peek_alarm_timestamp().unwrap();
        assert_eq!(alarm, alarm2);

        // next_alarm_timestamp should advance the state
        let alarm3 = state.next_alarm_timestamp().unwrap();
        assert_eq!(alarm, alarm3);
    }

    #[test]
    fn test_frequency_state_from_repeated() {
        let freq = FrequencySeconds::Repeated(5);
        let mut state = FrequencyState::from(freq);

        let now = crate::utils::timestamp();
        // For Repeated, we should get a sequence starting from 0 with step 5
        let alarm1 = state.next_alarm_timestamp().unwrap();
        assert_eq!(alarm1, now + 5);

        let alarm2 = state.next_alarm_timestamp().unwrap();
        assert_eq!(alarm2, now + 10);

        let alarm3 = state.next_alarm_timestamp().unwrap();
        assert_eq!(alarm3, now + 15);
    }

    #[test]
    fn test_frequency_state_from_countdown() {
        // Note: CountDown implementation creates a sequence starting from 'seconds'
        // with step 'count_down', and the count is handled separately
        let freq = FrequencySeconds::CountDown(2, 5); // count_down=2, seconds=5
        let state = FrequencyState::from(freq);

        // Check that it's the CountDown variant with correct count
        match state {
            FrequencyState::SecondsCountDown(count, _) => assert_eq!(count, 2),
            _ => panic!("Expected SecondsCountDown variant"),
        }
    }

    #[test]
    fn test_peek_alarm_timestamp() {
        let freq = FrequencySeconds::Repeated(10);
        let mut state = FrequencyState::from(freq);

        // Peek should not advance the state
        let peek1 = state.peek_alarm_timestamp().unwrap();
        let peek2 = state.peek_alarm_timestamp().unwrap();
        assert_eq!(peek1, peek2);

        // But next should advance
        let next1 = state.next_alarm_timestamp().unwrap();
        assert_eq!(peek1, next1);

        let peek3 = state.peek_alarm_timestamp().unwrap();
        assert_ne!(peek1, peek3);
    }

    #[test]
    fn test_reset_from_timestamp_repeated() {
        let freq = FrequencySeconds::Repeated(10);
        let mut state = FrequencyState::from(freq);

        // Advance the state a few times
        let _ = state.next_alarm_timestamp().unwrap();
        let _ = state.next_alarm_timestamp().unwrap();

        // Reset from a specific timestamp
        let reset_base = 1000;
        state.reset_from_timestamp(reset_base, 10);

        // After reset, the next alarm should be at reset_base + interval
        let next = state.peek_alarm_timestamp().unwrap();
        assert_eq!(next, reset_base + 10);

        // Subsequent alarms should follow the new interval
        let next2 = state.next_alarm_timestamp().unwrap();
        assert_eq!(next2, reset_base + 10);

        let next3 = state.next_alarm_timestamp().unwrap();
        assert_eq!(next3, reset_base + 20);
    }

    #[test]
    fn test_reset_from_timestamp_countdown() {
        let freq = FrequencySeconds::CountDown(5, 10);
        let mut state = FrequencyState::from(freq);

        // Reset from a specific timestamp
        let reset_base = 2000;
        state.reset_from_timestamp(reset_base, 10);

        // After reset, the next alarm should be at reset_base + interval
        let next = state.peek_alarm_timestamp().unwrap();
        assert_eq!(next, reset_base + 10);

        // Count should be preserved
        match state {
            FrequencyState::SecondsCountDown(count, _) => assert_eq!(count, 5),
            _ => panic!("Expected SecondsCountDown variant"),
        }
    }

    #[test]
    fn test_frequency_seconds_interval() {
        assert_eq!(FrequencySeconds::Once(30).interval(), 30);
        assert_eq!(FrequencySeconds::Repeated(60).interval(), 60);
        assert_eq!(FrequencySeconds::CountDown(3, 15).interval(), 15);
    }
}
