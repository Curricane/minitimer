use crate::utils::timestamp;

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

    /// Validates that the frequency describes a schedulable task.
    ///
    /// # Returns
    /// * `Ok(())` - If the interval and execution count are usable
    /// * `Err(String)` - A description of the invalid configuration
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.interval() == 0 {
            return Err(format!("interval must be greater than 0 seconds: {self:?}"));
        }

        if let Self::CountDown(count_down, _) = self
            && *count_down == 0
        {
            return Err(format!(
                "countdown execution count must be greater than 0: {self:?}"
            ));
        }

        Ok(())
    }
}

impl Default for FrequencySeconds {
    fn default() -> FrequencySeconds {
        FrequencySeconds::Once(ONE_MINUTE)
    }
}

/// Cursor over the executions a task still has ahead of it.
///
/// A task is scheduled by placing its next alarm on the wheel and consuming
/// that alarm, so `next_alarm` is the execution after the one being placed.
/// Once the last execution has been handed out the cursor is exhausted and
/// every further call returns `None`, which is what ends a finite task.
#[derive(Clone)]
pub(crate) struct FrequencyState {
    /// Timestamp of the next execution, `None` when the task has no runs left.
    next_alarm: Option<u64>,
    /// Seconds between executions.
    interval: u64,
    /// Executions still to come, `None` when the task repeats without end.
    executions_left: Option<u64>,
}

impl FrequencyState {
    fn new(first_alarm: u64, interval: u64, executions_left: Option<u64>) -> Self {
        Self {
            next_alarm: (executions_left != Some(0)).then_some(first_alarm),
            interval,
            executions_left,
        }
    }

    /// Peeks at the next alarm timestamp without consuming it.
    ///
    /// # Returns
    /// The next timestamp when the task should execute, or `None` if no more executions.
    pub(crate) fn peek_alarm_timestamp(&self) -> Option<u64> {
        self.next_alarm
    }

    /// Consumes the next alarm timestamp.
    ///
    /// Subsequent calls return the following timestamp in the sequence.
    /// Use `peek_alarm_timestamp()` when the cursor must not advance.
    ///
    /// # Returns
    /// The next timestamp when the task should execute, or `None` if no more executions.
    pub(crate) fn next_alarm_timestamp(&mut self) -> Option<u64> {
        let current = self.next_alarm?;

        self.executions_left = self.executions_left.map(|left| left - 1);
        self.next_alarm = match self.executions_left {
            Some(0) => None,
            _ => Some(current + self.interval),
        };

        Some(current)
    }

    /// Restarts the sequence from a given timestamp.
    ///
    /// This is used when accelerating a task to restart the frequency
    /// sequence from a new base time. The executions left are preserved, so an
    /// exhausted task stays exhausted.
    ///
    /// # Arguments
    /// * `base_timestamp` - The new base timestamp to start the sequence from
    /// * `interval` - The interval in seconds between executions
    pub(crate) fn reset_from_timestamp(&mut self, base_timestamp: u64, interval: u64) {
        self.interval = interval;
        self.next_alarm = match self.executions_left {
            Some(0) => None,
            _ => Some(base_timestamp + interval),
        };
    }
}

impl From<FrequencySeconds> for FrequencyState {
    fn from(frequency: FrequencySeconds) -> Self {
        let now = timestamp();
        match frequency {
            FrequencySeconds::Once(seconds) => Self::new(now + seconds, seconds, Some(1)),
            FrequencySeconds::Repeated(seconds) => Self::new(now + seconds, seconds, None),
            FrequencySeconds::CountDown(count_down, seconds) => {
                Self::new(now + seconds, seconds, Some(count_down))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frequency_state_from_once_exhausts_after_one_alarm() {
        let freq = FrequencySeconds::Once(10);
        let mut state = FrequencyState::from(freq);

        let now = crate::utils::timestamp();
        let alarm = state.peek_alarm_timestamp().unwrap();
        assert!(alarm >= now + 10);

        // Peeking does not advance the cursor
        assert_eq!(state.peek_alarm_timestamp(), Some(alarm));

        // Consuming the alarm exhausts a Once task
        assert_eq!(state.next_alarm_timestamp(), Some(alarm));
        assert_eq!(state.peek_alarm_timestamp(), None);
        assert_eq!(state.next_alarm_timestamp(), None);
    }

    #[test]
    fn test_frequency_state_from_repeated_never_exhausts() {
        let freq = FrequencySeconds::Repeated(5);
        let mut state = FrequencyState::from(freq);

        let now = crate::utils::timestamp();
        assert_eq!(state.next_alarm_timestamp(), Some(now + 5));
        assert_eq!(state.next_alarm_timestamp(), Some(now + 10));
        assert_eq!(state.next_alarm_timestamp(), Some(now + 15));
    }

    #[test]
    fn test_frequency_state_from_countdown_is_bounded() {
        let freq = FrequencySeconds::CountDown(3, 5);
        let mut state = FrequencyState::from(freq);

        let now = crate::utils::timestamp();
        let first = state.next_alarm_timestamp().unwrap();
        assert!(first >= now + 5, "first execution waits the initial delay");

        // Exactly three executions are configured
        assert!(state.next_alarm_timestamp().is_some());
        assert!(state.next_alarm_timestamp().is_some());
        assert_eq!(state.peek_alarm_timestamp(), None);
        assert_eq!(state.next_alarm_timestamp(), None);
    }

    #[test]
    fn test_frequency_state_from_countdown_uses_the_configured_interval() {
        let freq = FrequencySeconds::CountDown(3, 5);
        let mut state = FrequencyState::from(freq);

        let now = crate::utils::timestamp();
        assert_eq!(state.next_alarm_timestamp(), Some(now + 5));
        assert_eq!(state.next_alarm_timestamp(), Some(now + 10));
        assert_eq!(state.next_alarm_timestamp(), Some(now + 15));
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
    fn test_reset_from_timestamp_countdown_keeps_executions_left() {
        let freq = FrequencySeconds::CountDown(2, 10);
        let mut state = FrequencyState::from(freq);

        let reset_base = 2000;
        state.reset_from_timestamp(reset_base, 10);
        assert_eq!(state.peek_alarm_timestamp(), Some(reset_base + 10));

        // Two executions were configured, so the task ends after the second one
        assert_eq!(state.next_alarm_timestamp(), Some(reset_base + 10));
        assert_eq!(state.peek_alarm_timestamp(), Some(reset_base + 20));
        assert_eq!(state.next_alarm_timestamp(), Some(reset_base + 20));
        assert_eq!(state.peek_alarm_timestamp(), None);
    }

    #[test]
    fn test_reset_from_timestamp_does_not_revive_exhausted_task() {
        let freq = FrequencySeconds::Once(10);
        let mut state = FrequencyState::from(freq);
        let _ = state.next_alarm_timestamp();

        state.reset_from_timestamp(3000, 10);
        assert_eq!(state.peek_alarm_timestamp(), None);
    }

    #[test]
    fn test_frequency_seconds_interval() {
        assert_eq!(FrequencySeconds::Once(30).interval(), 30);
        assert_eq!(FrequencySeconds::Repeated(60).interval(), 60);
        assert_eq!(FrequencySeconds::CountDown(3, 15).interval(), 15);
    }
}
