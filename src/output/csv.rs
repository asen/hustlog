use crate::output::format::OutputSink;
use crate::ql_processor::{QlRow, QlSchema};
use std::sync::Arc;
use crate::{DynBoxWrite, DynError};
use crate::parser::ParserSchema;

pub struct CsvOutput {
    schema: Arc<QlSchema>,
    wr: csv::Writer<DynBoxWrite>,
    add_header: bool,
}

impl CsvOutput {
    pub fn new(schema: Arc<QlSchema>, outp: DynBoxWrite, add_header: bool) -> Self {
        Self {
            schema,
            wr: csv::Writer::from_writer(outp),
            add_header,
        }
    }
}

impl OutputSink for CsvOutput {
    fn output_header(&mut self) -> Result<(), DynError> {
        if !self.add_header {
            return Ok(());
        }
        let o = self
            .schema
            .col_defs()
            .iter()
            .map(|&x| x.name().as_bytes())
            .collect::<Vec<_>>();
        let ret = self.wr.write_record(o);
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }

    fn output_row(&mut self, row: QlRow) -> Result<(), DynError> {
        let rc_row = row.data_as_strs();
        let o = rc_row.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        let ret = self.wr.write_record(o);
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }

    fn flush(&mut self) -> Result<(), DynError> {
        let ret = self.wr.flush();
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }
}
