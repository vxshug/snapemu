mod payload;
mod gateway;

pub use payload::DownloadData;
pub use payload::UpData;
pub use gateway::*;

#[derive(Debug)]
pub enum CustomError {
    Key,
    Format(String),
    MIC
}