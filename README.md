# MiniTimer

[![Crates.io](https://img.shields.io/crates/v/minitimer)](https://crates.io/crates/minitimer)
[![License](https://img.shields.io/crates/l/minitimer)](https://crates.io/crates/minitimer)
[![Rust](https://github.com/Curricane/minitimer/actions/workflows/rust.yml/badge.svg)](https://github.com/Curricane/minitimer/actions)

MiniTimer is a lightweight timer library built on the Tokio runtime, designed for scheduling and executing delayed tasks. It uses a three-level timing wheel (second wheel, minute wheel, hour wheel) algorithm to place and look up tasks in constant time.

## Features

- **High-performance timing wheel algorithm**: Three-level timing wheel design, with constant time task placement and lookup
- **Multiple task execution modes**:
  - One-time delayed tasks, which are removed once they have run
  - Repeated tasks
  - Countdown tasks, executing a fixed number of times at the configured interval
- **Task concurrency control**: Supports setting maximum concurrency for each task
- **Dynamic task management**: Supports dynamically adding, canceling, removing and replacing tasks at runtime
- **Testable scheduling**: Tasks can be driven by hand, tick by tick, without waiting for the clock
- **Fully async**: Built on Tokio runtime with async/await support

## Installation

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
minitimer = "0.1"
tokio = { version = "1", features = ["full"] }
async-trait = "0.1"
```

## Quick Start

```rust
use async_trait::async_trait;
use minitimer::{TaskBuilder, TaskRunner};

struct MyTask {
    message: String,
}

#[async_trait]
impl TaskRunner for MyTask {
    type Output = ();

    async fn run(&self) -> Result<Self::Output, Box<dyn std::error::Error + Send + Sync>> {
        println!("Task executed: {}", self.message);
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let timer = minitimer::MiniTimer::new();

    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(3)
        .spawn_async(MyTask {
            message: "Hello from MiniTimer!".to_string(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}
```

Short tasks do not need a type. Any closure returning a future can be scheduled:

```rust
use minitimer::TaskBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

let runs = Arc::new(AtomicU64::new(0));
let counter = runs.clone();

let task = TaskBuilder::new(1)
    .with_frequency_repeated_by_seconds(30)
    .spawn_async(move || {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    })
    .unwrap();
```

A closure is called once per execution, so it has to be callable repeatedly and the future it returns has to own what it uses.

## Task Execution Modes

### One-time Delayed Task

Runs exactly once and is then removed from the timer.

```rust
let task = TaskBuilder::new(1)
    .with_frequency_once_by_seconds(60)
    .spawn_async(MyTask { ... })
    .unwrap();
```

### Repeated Task

Runs every `n` seconds until it is removed.

```rust
let task = TaskBuilder::new(1)
    .with_frequency_repeated_by_seconds(10)
    .spawn_async(MyTask { ... })
    .unwrap();
```

### Countdown Task

Runs `count` times, `interval` seconds apart.

```rust
// 3 executions, one second apart
let task = TaskBuilder::new(1)
    .with_frequency_count_down_by_seconds(3, 1)
    .spawn_async(MyTask { ... })
    .unwrap();
```

### Timestamp-based Task

Runs once at a Unix timestamp; a timestamp that is not in the future is rejected with a `TaskError`.

```rust
let target_timestamp = 1700000000;
let task = TaskBuilder::new(1)
    .with_frequency_once_by_timestamp_seconds(target_timestamp)
    .spawn_async(MyTask { ... })
    .unwrap();
```

### Concurrency Control

Limits how many executions of a task may overlap. An occurrence that arrives while the limit is reached is skipped and the task waits for its next one.

```rust
let task = TaskBuilder::new(1)
    .with_frequency_repeated_by_seconds(1)
    .with_max_concurrency(3)
    .spawn_async(MyTask { ... })
    .unwrap();
```

## Task Management API

```rust
let timer = minitimer::MiniTimer::new();

// Add a task
timer.add_task(task).unwrap();

// Replace a task: the previous schedule stops, the task id is kept
timer.update_task(task_id, new_task).unwrap();

// Trigger a task now, or move it closer (None triggers immediately)
timer.advance_task(task_id, Some(Duration::from_secs(5)), true).unwrap();

// Remove a task
let removed = timer.remove_task(task_id);

// Check if a task exists
if timer.contains_task(task_id) {
    println!("Task exists");
}

// Get task state
if let Some(status) = timer.task_status(task_id) {
    println!("Runs in {}s on the {:?} wheel", status.time_to_next_run, status.wheel_type);
}

// Tasks waiting for their next execution, and tasks running right now
let pending = timer.get_pending_tasks();
let running = timer.get_running_tasks();

// Number of scheduled tasks
let count = timer.task_count();

// Stop the timer: the tick source and the event loop are shut down
timer.stop().await;
```

## Testing

Timing-sensitive behaviour does not have to be tested by sleeping. `MiniTimer::new_manual` builds a timer that never ticks on its own, so time only moves when the test says so:

```rust
use minitimer::{MiniTimer, TaskBuilder};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[tokio::test]
async fn task_runs_on_the_fifth_second() {
    let runs = Arc::new(AtomicU64::new(0));
    let timer = MiniTimer::new_manual();

    let counter = runs.clone();
    let task = TaskBuilder::new(1)
        .with_frequency_once_by_seconds(5)
        .spawn_async(move || {
            let counter = counter.clone();
            async move { counter.fetch_add(1, Ordering::SeqCst); }
        })
        .unwrap();
    timer.add_task(task).unwrap();

    timer.elapse(Duration::from_secs(4)).await;
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    timer.tick().await;
    timer.wait_for_idle().await;
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}
```

- `tick()` resolves once the tick has been applied, so the effect of the elapsed second is visible when it returns
- `elapse(duration)` moves the timer by a whole duration, one tick per second
- `wait_for_idle()` waits until no execution is in flight, which is also useful after `stop()` on a wall clock timer

See `examples/manual_clock.rs` for a runnable version.

## Timing Wheel Algorithm

MiniTimer uses a three-level timing wheel for efficient task scheduling:

- **Second wheel**: 60 slots (0-59 seconds)
- **Minute wheel**: 60 slots (0-59 minutes)
- **Hour wheel**: 24 slots (0-23 hours)

Tasks are distributed across these wheels based on their execution time. As the wheel rotates, tasks cascade down (from hour wheel to minute wheel, from minute wheel to second wheel) until they reach the second wheel for execution. A task that is more than a day away carries the number of extra laps of the hour wheel it has to survive.

## Notes and Limits

- The timer requires a Tokio runtime and has to be constructed inside one
- Time advances in whole seconds. Delays are resolved to the second, so a wall clock timer may run a task up to a second later than requested
- The wheel moves with ticks. A runtime that is not polled (a suspended process, a blocked executor) delays tasks until it is polled again
- A task failure is logged through the `log` crate; it does not stop the timer or the task's remaining executions
- A wall clock timer can be combined with `tick()`, but the drift-free timing guarantees only apply to `MiniTimer::new_manual`

## Examples

Runnable examples are available in the [examples](./examples) directory:

- `once_delayed_task.rs` - One-time delayed task
- `repeated_task.rs` - Repeated task
- `countdown_task.rs` - Countdown task
- `concurrency_control.rs` - Concurrency control
- `task_management.rs` - Task management
- `advance_task.rs` - Triggering and advancing tasks
- `closure_task.rs` - Scheduling inline closures
- `manual_clock.rs` - Driving a timer by hand

Run an example:

```bash
cargo run --example once_delayed_task
cargo run --example repeated_task
cargo run --example countdown_task
cargo run --example concurrency_control
cargo run --example task_management
```

## License

Apache-2.0
