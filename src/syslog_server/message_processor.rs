use crate::{LogParseError, ParsedMessage};
use log::{error, info};
use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub struct ProcessingError {
    desc: String,
}

impl ProcessingError {
    // pub fn new(desc: String)-> Self {
    //     Self {
    //         desc: desc,
    //     }
    // }

    pub fn get_desc(&self) -> &String {
        &self.desc
    }
}

impl fmt::Display for ProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection error: {}", self.get_desc(),)
    }
}

impl Error for ProcessingError {}

pub trait MessageProcessor {
    fn process_message(&mut self, pm: ParsedMessage) -> Result<(), ProcessingError>;
    fn process_error(&mut self, err: LogParseError) -> Result<(), ProcessingError>;
    fn flush(&mut self) -> Result<(), ProcessingError>;
}

pub struct MessageBatcher {
    buf: Vec<ParsedMessage>,
    batch_size: usize,
}

impl MessageBatcher {
    pub fn new(batch_size: usize) -> Self {
        Self {
            buf: Vec::with_capacity(batch_size),
            batch_size,
        }
    }

    fn my_process_message(&mut self, pm: ParsedMessage) -> Option<Vec<ParsedMessage>> {
        self.buf.push(pm);
        if self.buf.len() >= self.batch_size {
            let batch = self.buf.drain(0..).collect::<Vec<_>>();
            return Some(batch);
        }
        return None;
    }

    fn process_batch(&self, batch: Vec<ParsedMessage>) {
        info!("Processing batch with size: {}", batch.len())
    }
}

impl MessageProcessor for MessageBatcher {
    fn process_message(&mut self, pm: ParsedMessage) -> Result<(), ProcessingError> {
        let batch = self.my_process_message(pm);
        if batch.is_some() {
            self.process_batch(batch.unwrap());
        }
        Ok(())
    }

    fn process_error(&mut self, err: LogParseError) -> Result<(), ProcessingError> {
        error!("PARSE ERROR: {:?}", err);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), ProcessingError> {
        let batch = self.buf.drain(0..).collect::<Vec<_>>();
        self.process_batch(batch);
        Ok(())
    }
}
