use crate::async_pipeline::async_parser::AsyncParser;
use crate::async_pipeline::batching_queue::BatchingQueue;
use crate::async_pipeline::message_queue::{MessageSender, QueueJoinHandle};
use crate::async_pipeline::output_processor::{DynOutputSink, OutputProcessor};
use crate::async_pipeline::sql_batch_processor::SqlBatchProcessor;
use crate::output::{AnsiSqlOutput, CsvOutput, OdbcSink};
use crate::parser::RawMessage;
use crate::ql_processor::QlSchema;
use crate::{DynError, HustlogConfig, OutputFormat};
use log::debug;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Create and wire the processing pipeline
/// return a tuple consisting of the raw message sender and a vector of JoinHandles
/// to be awaited on shutdown, or return an error on failure
pub async fn create_processing_pipeline(
    hcrc: &Arc<HustlogConfig>,
) -> Result<(MessageSender<Vec<RawMessage>>, Vec<QueueJoinHandle>), DynError> {
    hcrc.init_rayon_pool()?;
    let schema = hcrc.get_grok_schema();
    let ql_input_schema = Arc::new(QlSchema::from(&schema));
    let mut sql_processor: Option<SqlBatchProcessor> = None;
    let ql_output_schema = if hcrc.query().is_some() {
        sql_processor = Some(SqlBatchProcessor::new(
            hcrc.query().as_ref().unwrap().as_str(),
            schema,
            hcrc.get_async_channel_size(),
        )?);
        sql_processor.as_ref().unwrap().get_output_schema().clone()
    } else {
        ql_input_schema.clone()
    };
    let sink: DynOutputSink = match hcrc.output_format() {
        OutputFormat::DEFAULT => {
            debug!("Using default (CSV) output");
            Arc::new(Mutex::new(CsvOutput::new(
                ql_output_schema.clone(),
                hcrc.get_output_writer()?,
                hcrc.output_add_ddl(),
            )))
        }
        OutputFormat::SQL => {
            debug!("Using SQL output");
            Arc::new(Mutex::new(AnsiSqlOutput::new(
                ql_output_schema.clone(),
                hcrc.output_add_ddl(),
                hcrc.output_batch_size(),
                hcrc.get_output_writer()?,
                hcrc.get_ddl_pre_name_opts(),
                hcrc.get_ddl_table_opts(),
            )))
        }
        OutputFormat::ODBC => {
            debug!("Using ODBC output");
            Arc::new(Mutex::new(OdbcSink::new(
                ql_output_schema.clone(),
                hcrc.get_output(),
            )?))
        }

    };
    let mut join_handles = Vec::new();
    let (mut output_sender, jh) =
        OutputProcessor::wrap_sink(sink, hcrc.get_async_channel_size(), hcrc.output_add_ddl());
    join_handles.push(jh);
    if sql_processor.is_some() {
        let (new_sender, jh) = sql_processor.unwrap().wrap_sender(output_sender)?;
        output_sender = new_sender;
        join_handles.push(jh)
    }
    let (batch_sender, jh) = AsyncParser::wrap_parsed_sender(
        output_sender,
        schema.clone(),
        hcrc.get_async_channel_size(),
    )?;
    join_handles.push(jh);
    let (raw_sender, jh) = BatchingQueue::wrap_output(
        hcrc.output_batch_size(),
        hcrc.get_async_channel_size(),
        batch_sender,
    );
    join_handles.push(jh);
    join_handles.reverse(); //we want to shut these down in reverse order later
    Ok((raw_sender, join_handles))
}
