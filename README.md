# MiniTimer

[![Crates.io](https://img.shields.io/crates/v/minitimer)](https://crates.io/crates/minitimer)
[![License](https://img.shields.io/crates/l/minitimer)](https://crates.io/crates/minitimer)
[![Rust](https://github.com/Curricane/minitimer/actions/workflows/rust.yml/badge.svg)](https://github.com/Curricane/minitimer/actions)

MiniTimer is a lightweight timer library built on the Tokio runtime, designed for scheduling and executing delayed tasks. It uses a three-level timing wheel (second wheel, minute wheel, hour wheel) algorithm to achieve O(1) time complexity for task lookup and execution.

## Features

- **High-performance timing wheel algorithm**: Three-level timing wheel design with O(1) time complexity for task lookup and execution
- **Multiple task execution modes**:
  - One-time delayed tasks
  - Repeated tasks
  - Countdown tasks (execute a fixed number of times)
- **Task concurrency control**: Supports setting maximum concurrency for each task
- **Dynamic task management**: Supports dynamically adding, canceling, and removing tasks at runtime
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
        .spwan_async(MyTask {
            message: "Hello from MiniTimer!".to_string(),
        })
        .unwrap();

    timer.add_task(task).unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}
```

## Task Execution Modes

### One-time Delayed Task

```rust
let task = TaskBuilder::new(1)
    .with_frequency_once_by_seconds(60)
    .spwan_async(MyTask { ... })
    .unwrap();
```

### Repeated Task

```rust
let task = TaskBuilder::new(1)
    .with_frequency_repeated_by_seconds(10)
    .spwan_async(MyTask { ... })
    .unwrap();
```

### Countdown Task

```rust
let task = TaskBuilder::new(1)
    .with_frequency_count_down_by_seconds(3, 1)
    .spwan_async(MyTask { ... })
    .unwrap();
```

### Timestamp-based Task

```rust
let target_timestamp = 1700000000;
let task = TaskBuilder::new(1)
    .with_frequency_once_by_timestamp_seconds(target_timestamp)
    .spwan_async(MyTask { ... })
    .unwrap();
```

### Concurrency Control

```rust
let task = TaskBuilder::new(1)
    .with_frequency_repeated_by_seconds(1)
    .with_max_concurrency(3)
    .spwan_async(MyTask { ... })
    .unwrap();
```

## Task Management API

```rust
let timer = minitimer::MiniTimer::new();

// Add a task
timer.add_task(task).unwrap();

// Remove a task
let removed = timer.remove_task(task_id);

// Check if a task exists
if timer.contains_task(task_id) {
    println!("Task exists");
}

// Get task state
if let Some(state) = timer.get_task_state(task_id) {
    println!("Task state: {:?}", state);
}

// Get pending tasks
let pending = timer.get_pending_tasks();

// Get running tasks
let running = timer.get_running_tasks();

// Get task count
let count = timer.task_count();

// Stop the timer
timer.stop().await;
```

## Timing Wheel Algorithm

MiniTimer uses a three-level timing wheel for efficient task scheduling:

- **Second wheel**: 60 slots (0-59 seconds)
- **Minute wheel**: 60 slots (0-59 minutes)
- **Hour wheel**: 24 slots (0-23 hours)

Tasks are distributed across these wheels based on their execution time. As the wheel rotates, tasks cascade down (from hour wheel to minute wheel, from minute wheel to second wheel) until they reach the second wheel for execution. This design ensures O(1) time complexity for task lookup and execution.

## Examples

More examples are available in the [examples](./examples) directory:

- `once_delayed_task.rs` - One-time delayed task
- `repeated_task.rs` - Repeated task
- `countdown_task.rs` - Countdown task
- `concurrency_control.rs` - Concurrency control
- `task_management.rs` - Task management

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
