use crate::query_processor::QlRow;
use std::error::Error;

pub trait OutputSink {
    fn output_header(&mut self) -> Result<(), Box<dyn Error>>;
    fn output_row(&mut self, row: QlRow) -> Result<(), Box<dyn Error>>;
    fn flush(&mut self) -> Result<(), Box<dyn Error>>;
}
