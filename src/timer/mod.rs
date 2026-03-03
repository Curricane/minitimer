#![allow(clippy::module_inception)]
mod clock;
pub(crate) mod event;
pub(crate) mod slot;
pub(crate) mod timer;
pub(crate) mod wheel;

pub(crate) use clock::Clock;
pub use event::TimerEvent;
pub use timer::Timer;
pub use wheel::{TaskStatus, WheelCascadeGuide, WheelType};
