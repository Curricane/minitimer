use std::time::SystemTime;

/// Returns the current UNIX timestamp in seconds.
///
/// # Returns
/// The number of seconds since the UNIX epoch (1970-01-01 00:00:00 UTC).
///
/// # Panics
/// Panics if the system time is before the UNIX epoch.
pub fn timestamp() -> u64 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}
