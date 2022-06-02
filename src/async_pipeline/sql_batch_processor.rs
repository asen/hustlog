use log::{error, info};
use sqlparser::ast::{Expr, Value};
use std::sync::Arc;

use crate::async_pipeline::message_queue::{
    ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage,
};
use crate::parser::GrokSchema;
use crate::ql_processor::{
    eval_query, get_group_by_exprs, get_limit, get_offset, get_order_by_exprs, get_res_cols,
    LazyContext, QlMemTable, QlRowBatch, QlSchema, QlSelectCols, SqlSelectQuery,
};
use crate::DynError;

const TRUE_EXPRESSION: Expr = Expr::Value(Value::Boolean(true));

pub struct SqlBatchProcessor {
    tx: ChannelSender<QueueMessage<QlRowBatch>>,
    rx: ChannelReceiver<QueueMessage<QlRowBatch>>,
    //query: Arc<SqlSelectQuery>,
    select_cols: Arc<QlSelectCols>,
    where_c: Arc<Expr>,
    input_schema: Arc<QlSchema>,
    output_schema: Arc<QlSchema>,
    limit: Option<usize>,
    offset: i64,
    group_by_exprs: Arc<Vec<usize>>,
    order_by_exprs: Arc<Vec<(usize, bool)>>,
    output_sender: Option<MessageSender<QlRowBatch>>,
}

impl SqlBatchProcessor {
    pub fn new(
        query: &str,
        schema: &GrokSchema,
        //output_sender: MessageSender<QlRowBatch>,
        channel_size: usize,
    ) -> Result<Self, DynError> {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
        let query = Arc::new(SqlSelectQuery::new(query)?);
        let result_cols = get_res_cols(&schema, &query);
        let select_cols = Arc::new(QlSelectCols::new(result_cols));
        let input_schema = QlSchema::from(&schema);
        let output_schema = select_cols.to_out_schema(&input_schema)?;
        let mut empty_lazy_context = LazyContext::empty();
        let limit = get_limit(&query, &mut empty_lazy_context)?;
        let offset = get_offset(&query, &mut empty_lazy_context)?;
        let group_by_exprs = Arc::new(get_group_by_exprs(&query, &mut empty_lazy_context)?);
        let order_by_exprs = Arc::new(get_order_by_exprs(&query, &mut empty_lazy_context)?);
        let where_c: Arc<Expr> = Arc::from(
            query
                .get_select()
                .selection
                .as_ref()
                .unwrap_or(&TRUE_EXPRESSION)
                .clone(),
        );
        Ok(Self {
            tx,
            rx,
            //query,
            select_cols,
            where_c,
            input_schema: Arc::new(input_schema),
            output_schema: Arc::new(output_schema),
            limit,
            offset,
            group_by_exprs,
            order_by_exprs,
            output_sender: None,
        })
    }

    pub fn wrap_sender(
        mut self,
        output_sender: MessageSender<QlRowBatch>,
    ) -> Result<(MessageSender<QlRowBatch>, QueueJoinHandle), DynError> {
        self.output_sender = Some(output_sender);
        let ret = self.clone_sender();
        let jh = self.consume_queue_async();
        Ok((ret, jh))
    }

    pub fn get_output_schema(&self) -> &Arc<QlSchema> {
        &self.output_schema
    }

    async fn execute_query_async(&self, batch: QlRowBatch) -> Result<(), DynError> {
        let mut input_tabe = QlMemTable::from_rows_batch(self.input_schema.clone(), batch);
        let select_cols = Arc::clone(&self.select_cols);
        let where_c = Arc::clone(&self.where_c);
        let limit = self.limit;
        let offset = self.offset;
        let group_by_exprs = Arc::clone(&self.group_by_exprs);
        let order_by_exprs = Arc::clone(&self.order_by_exprs);
        let cloned_schema = self.output_schema.clone();
        let table_res: Result<QlMemTable, DynError> = tokio_rayon::spawn_fifo(move || {
            let mut output_table = QlMemTable::new(cloned_schema);
            eval_query(
                select_cols,
                where_c,
                limit,
                offset,
                group_by_exprs,
                order_by_exprs,
                &mut Box::new(&mut input_tabe),
                &mut Box::new(&mut output_table),
            )?;
            Ok(output_table)
        })
        .await;
        self.output_sender
            .as_ref()
            .unwrap()
            .send(table_res?.consume_rows())
            .await?;
        Ok(())
    }

    fn clone_sender(&self) -> MessageSender<QlRowBatch> {
        MessageSender::new(self.tx.clone())
    }

    fn consume_queue_async(mut self) -> QueueJoinHandle {
        assert!(self.output_sender.is_some());
        let jh = tokio::spawn(async move {
            info!("Consuming SQL queue ...");
            self.consume_queue().await;
            info!("Done consuming SQL queue.");
            Ok(())
        });
        QueueJoinHandle::new("sql_batch", jh)
    }

    async fn consume_queue(&mut self) {
        assert!(self.output_sender.is_some());
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(rb) => {
                    if let Err(err) = self.execute_query_async(rb).await {
                        error!("Failed to execute_query_async: {:?}", err);
                    }
                }
                QueueMessage::Flush => {
                    if let Err(err) = self.output_sender.as_ref().unwrap().flush().await {
                        error!("Failed to flush output sink, aborting: {:?}", err);
                        break;
                    }
                }
                QueueMessage::Shutdown => {
                    info!("Shutdown message received");
                    if let Err(err) = self.output_sender.as_ref().unwrap().shutdown().await {
                        error!("Failed to flush output sink, aborting: {:?}", err);
                        break;
                    }
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::async_pipeline::message_queue::tests::{init_test_rayon_pool, TestMessageQueue};
    use crate::async_pipeline::sql_batch_processor::SqlBatchProcessor;
    use crate::async_pipeline::LinesBuffer;
    use crate::parser::{test_dummy_data, test_dummy_schema, GrokParser, LogParser};
    use crate::ql_processor::{QlRow, QlSchema};
    use bytes::BufMut;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sql_batch_processor1() {
        init_test_rayon_pool();
        let (test_queue_sender, test_queue_jh) = TestMessageQueue::create(2);
        let schema = test_dummy_schema();
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let bp = SqlBatchProcessor::new("select * from DUMMY", &schema, 2).unwrap();
        let (sender, bjh) = bp.wrap_sender(test_queue_sender).unwrap();
        let parser = GrokParser::new(schema).unwrap();
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(test_dummy_data(100).as_bytes());
        let mut ql_rows = Vec::new();
        for raw in lb.flush() {
            let parsed = parser.parse(raw).unwrap();
            ql_rows.push(QlRow::from_parsed_message(parsed, ql_schema.as_ref()));
        }
        sender.send(ql_rows).await.unwrap();
        sender.flush().await.unwrap();
        sender.shutdown().await.unwrap();
        bjh.join().await;
        let test_queue_res = test_queue_jh.await.unwrap();
        let test_queue = test_queue_res.unwrap();
        assert_eq!(test_queue.received, 1);
        assert_eq!(test_queue.flushed, 1);
        assert_eq!(test_queue.shutdown, 1);
    }
}
