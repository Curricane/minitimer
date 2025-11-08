use std::{iter::StepBy, ops::RangeFrom};

use crate::utils::timestamp;

pub(crate) type SecondsState = StepBy<RangeFrom<u64>>;
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
                let state: SecondsState = ((timestamp() + seconds)..).step_by(seconds as usize);
                FrequencyState::SecondsRepeated(state)
            }
            FrequencySeconds::Repeated(seconds) => {
                assert!(seconds > 0, "repeated frequency must be greater than 0");
                let state: SecondsState = (0..).step_by(seconds as usize);
                FrequencyState::SecondsRepeated(state)
            }
            FrequencySeconds::CountDown(count_down, seconds) => {
                assert!(seconds > 0, "countdown initial must be greater than 0");
                let state: SecondsState = (seconds..).step_by(count_down as usize);
                FrequencyState::SecondsCountDown(count_down, state)
            }
        }
    }
}
