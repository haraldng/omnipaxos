//! Clock simulation for clock-assisted consensus.
//!
//! Currently returns hardware time in microseconds with zero uncertainty.
//! Drift and uncertainty will be added in later steps.


use std::time::{SystemTime, UNIX_EPOCH};

/// Simulated clock API (step 1): returns hardware time in microseconds.
/// Uncertainty is 0 for now (wiring step).
pub struct Clock;

impl Clock {
    /// Creates a new clock instance.

    pub fn new() -> Self {
        Clock
    }

    /// Hardware time in microseconds (u64)
    pub fn now_us(&self) -> u64 {
        let d = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        d.as_micros() as u64
    }

    /// For now: no uncertainty
    pub fn uncertainty_us(&self) -> u64 {
        0
    }
}