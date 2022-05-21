use std::error::Error;
use std::fmt;
use log::info;
use crate::ParsedMessage;

#[derive(Debug)]
pub struct ProcessingError(String);
impl fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChannelError: {}", self.0)
    }
}
impl Error for ProcessingError {}


pub trait BatchProcessor {
    fn process_batch(&self, batch:Vec<ParsedMessage>) -> Result<(),ProcessingError>;
}

pub struct DummyBatchProcessor {}

impl BatchProcessor for DummyBatchProcessor {
    fn process_batch(&self, batch: Vec<ParsedMessage>) -> Result<(), ProcessingError> {
        for m in batch {
            info!("PARSED_MESSAGE: {:?}", m);
        };
        Ok(())
    }
}

