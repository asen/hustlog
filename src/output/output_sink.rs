use crate::ql_processor::QlRow;
use crate::DynError;

pub trait OutputSink {
    fn output_header(&mut self) -> Result<(), DynError>;

    fn output_batch(&mut self, batch: Vec<QlRow>) -> Result<(), DynError>;

    fn flush(&mut self) -> Result<(), DynError>;
    fn shutdown(&mut self) -> Result<(), DynError>;
}
