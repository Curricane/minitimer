use std::{
    iter::{Peekable, StepBy},
    ops::RangeFrom,
};

use crate::utils::timestamp;

pub(crate) type SecondsState = Peekable<StepBy<RangeFrom<u64>>>;
const ONE_MINUTE: u64 = 60;
pub enum FrequencySeconds {
    Once(u64),
    Repeated(u64),
    CountDown(u64, u64),
}

impl Default for FrequencySeconds {
    fn default() -> FrequencySeconds {
        FrequencySeconds::Once(ONE_MINUTE)
    }
}

pub(crate) enum FrequencyState {
    SecondsRepeated(SecondsState),
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
    pub(crate) fn peek_alarm_timestamp(&mut self) -> Option<u64> {
        match self {
            Self::SecondsRepeated(state) => state.peek().map(|t| *t),
            Self::SecondsCountDown(_, state) => state.peek().map(|t| *t),
        }
    }

    pub(crate) fn next_alarm_timestamp(&mut self) -> Option<u64> {
        match self {
            Self::SecondsRepeated(state) => state.next(),
            Self::SecondsCountDown(_, state) => state.next(),
        }
    }

    pub(crate) fn down_count(&mut self) {
        if let Self::SecondsCountDown(count, _) = self {
            *count = count.saturating_sub(1);
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
}
