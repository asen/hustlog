use std::error::Error;
use std::io::Write;
use crate::output::format::OutputSink;
use crate::{ParserSchema, QlSchema};
use crate::query_processor::QlRow;

pub struct CsvOutput {
    schema: QlSchema,
    wr: csv::Writer<Box<dyn Write>>,
    add_header: bool,
}

impl CsvOutput {
    pub fn new(schema: QlSchema, outp: Box<dyn Write>, add_header: bool) -> Self {
        Self {
            schema,
            wr: csv::Writer::from_writer(outp),
            add_header,
        }
    }

}

impl OutputSink for CsvOutput {
    fn output_header(&mut self) -> Result<(), Box<dyn Error>> {
        if !self.add_header {
            return Ok(())
        }
        let o = self.schema.col_defs().iter().map(|&x|{
            x.name().as_bytes()
        }).collect::<Vec<_>>();
        let ret = self.wr.write_record(o);
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }

    fn output_row(&mut self, row: QlRow) -> Result<(), Box<dyn Error>> {
        let rc_row = row.data_as_strs();
        let o = rc_row.iter().map(|x| x.as_ref()).collect::<Vec<_>>();
        let ret = self.wr.write_record(o);
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        let ret = self.wr.flush();
        if ret.is_ok() {
            Ok(())
        } else {
            Err(Box::new(ret.err().unwrap()))
        }
    }
}
