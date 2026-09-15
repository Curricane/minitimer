//! A lightweight timer for delayed and recurring asynchronous tasks, built on
//! the Tokio runtime.
//!
//! Tasks are described with [`TaskBuilder`], which takes either a type
//! implementing [`TaskRunner`] or a closure, and handed to a [`MiniTimer`].
//! Scheduling uses a three-level timing wheel (seconds, minutes, hours), so
//! placing and looking up a task costs the same whether the wheel holds one
//! task or many.
//!
//! ```no_run
//! use minitimer::{MiniTimer, TaskBuilder};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let timer = MiniTimer::new();
//!
//! let task = TaskBuilder::new(1)
//!     .with_frequency_once_by_seconds(3)
//!     .spawn_async(|| async { println!("three seconds later"); })?;
//!
//! timer.add_task(task)?;
//! # Ok(())
//! # }
//! ```
//!
//! A timer that follows the wall clock needs a runtime to be created in. For
//! tests, [`MiniTimer::new_manual`] builds one whose time only moves when the
//! test says so:
//!
//! ```
//! use minitimer::{MiniTimer, TaskBuilder};
//! use std::time::Duration;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let timer = MiniTimer::new_manual();
//! let task = TaskBuilder::new(1)
//!     .with_frequency_once_by_seconds(1)
//!     .spawn_async(|| async { println!("ran"); })?;
//! timer.add_task(task)?;
//!
//! timer.elapse(Duration::from_secs(1)).await;
//! timer.wait_for_idle().await;
//! # Ok(())
//! # }
//! ```
//!
//! See the [README](https://github.com/Curricane/minitimer) for a full guide.
//!
//! A timer owns two spawned tasks — a tick source and the event loop that
//! applies its ticks. [`MiniTimer::stop`] and dropping the last handle to the
//! timer both shut them down; the event loop exits at once and the tick source
//! at the latest when its second is up. Executions that are already in flight
//! are not cancelled, which is what [`MiniTimer::wait_for_idle`] is for.

pub mod error;
pub(crate) mod mini_timer;
pub mod task;
pub mod timer;
pub mod utils;

pub use error::TaskError;
pub use mini_timer::MiniTimer;
pub use task::{
    FrequencySeconds, RecordId, RunningRecord, TaskBuilder, TaskId, TaskRunner, TaskState,
};
pub use timer::{TaskStatus, TimerEvent, WheelCascadeGuide, WheelType};
