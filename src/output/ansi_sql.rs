use crate::output::format::OutputSink;
use crate::query_processor::QlRow;
use crate::sqlgen::BatchedInserts;
use crate::{QlSchema, SqlCreateSchema};
use std::error::Error;
use std::io::Write;

pub struct AnsiSqlOutput {
    ddl: Option<SqlCreateSchema>,
    inserts: BatchedInserts,
    // outp: Box<dyn Write>,
}

impl AnsiSqlOutput {
    pub fn new(schema: QlSchema, add_ddl: bool, batch_size: usize, outp: Box<dyn Write>) -> Self {
        let ddl = if add_ddl {
            Some(SqlCreateSchema::from_ql_schema(&schema))
        } else {
            None
        };
        let inserts = BatchedInserts::new(Box::new(schema), batch_size, outp);
        Self { ddl, inserts }
    }
}

impl OutputSink for AnsiSqlOutput {
    fn output_header(&mut self) -> Result<(), Box<dyn Error>> {
        if self.ddl.is_none() {
            return Ok(());
        }
        let ddl_ref = self.ddl.as_ref().unwrap();
        self.inserts
            .print_header_str(ddl_ref.get_create_sql().as_str())?;
        Ok(())
    }

    fn output_row(&mut self, row: QlRow) -> Result<(), Box<dyn Error>> {
        self.inserts.add_to_batch(row)
    }

    fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        self.inserts.flush()
    }
}
