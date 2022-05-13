use std::error::Error;
use crate::query_processor::QlRow;

pub trait OutputSink {
    fn output_header(&mut self) -> Result<(),Box<dyn Error>>;
    fn output_row(&mut self, row: QlRow) -> Result<(),Box<dyn Error>>;
    fn flush(&mut self) -> Result<(),Box<dyn Error>>;
}


