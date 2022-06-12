use crate::output::output_sink::OutputSink;
use crate::ql_processor::{QlRow, QlSchema};
use crate::sqlgen::{BatchedInserts, SqlCreateSchema};
use crate::{DynBoxWrite, DynError};
use std::sync::Arc;

pub struct AnsiSqlOutput {
    ddl: Option<SqlCreateSchema>,
    inserts: BatchedInserts,
    // outp: Box<dyn Write>,
}

impl AnsiSqlOutput {
    pub fn new(schema: Arc<QlSchema>, add_ddl: bool, batch_size: usize, outp: DynBoxWrite, pre_name_opts: &Arc<str>, table_opts:&Arc<str>) -> Self {
        let ddl = if add_ddl {
            Some(SqlCreateSchema::from_ql_schema(&schema, Arc::clone(pre_name_opts), Arc::clone(table_opts)))
        } else {
            None
        };
        let inserts = BatchedInserts::new(schema, batch_size, outp);
        Self { ddl, inserts }
    }

    fn output_row(&mut self, row: QlRow) -> Result<(), DynError> {
        self.inserts.add_to_batch(row)
    }
}

impl OutputSink for AnsiSqlOutput {
    fn output_header(&mut self) -> Result<(), DynError> {
        if self.ddl.is_none() {
            return Ok(());
        }
        let ddl_ref = self.ddl.as_ref().unwrap();
        self.inserts
            .print_header_str(ddl_ref.get_create_sql().as_str())?;
        Ok(())
    }


    fn flush(&mut self) -> Result<(), DynError> {
        self.inserts.flush()
    }

    fn output_batch(&mut self, batch: Vec<QlRow>) -> Result<(), DynError> {
        for r in batch {
            self.output_row(r)?
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), DynError> {
        Ok(())
    }
}
