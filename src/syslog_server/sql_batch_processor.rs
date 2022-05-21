use log::info;
use sqlparser::ast::{Expr, Value};
use std::error::Error;

use crate::query_processor::{
    eval_query, get_group_by_exprs, get_limit, get_offset, get_order_by_exprs, get_res_cols,
    LazyContext, QlSelectCols, SqlSelectQuery,
};
use crate::syslog_server::batch_processor::BatchProcessor;
use crate::{GrokSchema, ParsedMessage, QlMemTable, QlSchema};

pub struct SqlBatchProcessor {
    query: SqlSelectQuery,
    select_cols: QlSelectCols,
    input_schema: QlSchema,
    output_schema: QlSchema,
    limit: Option<usize>,
    offset: i64,
    group_by_exprs: Vec<usize>,
    order_by_exprs: Vec<(usize, bool)>, //outp sink
}

impl SqlBatchProcessor {
    pub fn new(query: &str, schema: &GrokSchema) -> Result<Self, Box<dyn Error>> {
        let query = SqlSelectQuery::new(query)?;
        let result_cols = get_res_cols(&schema, &query);
        let select_cols = QlSelectCols::new(result_cols);
        let input_schema = QlSchema::from(&schema);
        let output_schema = select_cols.to_out_schema(&input_schema)?;
        let mut empty_lazy_context = LazyContext::empty();
        let limit = get_limit(&query, &mut empty_lazy_context)?;
        let offset = get_offset(&query, &mut empty_lazy_context)?;
        let group_by_exprs = get_group_by_exprs(&query, &mut empty_lazy_context)?;
        let order_by_exprs = get_order_by_exprs(&query, &mut empty_lazy_context)?;

        Ok(Self {
            query,
            select_cols,
            input_schema,
            output_schema,
            limit,
            offset,
            group_by_exprs,
            order_by_exprs,
        })
    }
}

const TRUE_EXPRESSION: Expr = Expr::Value(Value::Boolean(true));

impl BatchProcessor for SqlBatchProcessor {
    fn process_batch(&self, batch: Vec<ParsedMessage>) -> Result<(), Box<dyn Error>> {
        let where_c: &Expr = &self
            .query
            .get_select()
            .selection
            .as_ref()
            .unwrap_or(&TRUE_EXPRESSION);
        let mut input_tabe = QlMemTable::from_parsed_messages_vec(self.input_schema.clone(), batch);
        let mut output_table = QlMemTable::new(&self.output_schema);
        eval_query(
            &self.select_cols,
            where_c,
            self.limit,
            self.offset,
            &self.group_by_exprs,
            &self.order_by_exprs,
            &mut Box::new(&mut input_tabe),
            &mut Box::new(&mut output_table),
        )?;
        // TODO print output table
        for r in output_table.get_rows() {
            info!("SqlBatchProcessor RESULT ROW: {:?}", r);
        }
        Ok(())
    }
}
