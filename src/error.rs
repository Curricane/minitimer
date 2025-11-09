use thiserror::Error;

#[derive(Error, Debug)]
pub enum TaskError {
    #[error("invalid frequency: {0}")]
    InvalidFrequency(String),
}
