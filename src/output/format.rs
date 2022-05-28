use crate::query_processor::QlRow;
use crate::DynError;

pub trait OutputSink {
    fn output_header(&mut self) -> Result<(), DynError>;
    fn output_row(&mut self, row: QlRow) -> Result<(), DynError>;
    fn flush(&mut self) -> Result<(), DynError>;

    fn output_batch(&mut self, batch: Vec<QlRow>) -> Result<(), DynError> {
        for r in batch {
            self.output_row(r)?;
        }
        Ok(())
    }
}
