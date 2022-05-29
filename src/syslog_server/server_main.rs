use crate::syslog_server::batching_queue::BatchingQueue;
use crate::syslog_server::message_queue::{MessageSender, QueueJoinHandle};
use crate::syslog_server::async_parser::AsyncParser;
use crate::syslog_server::sql_batch_processor::SqlBatchProcessor;
use crate::syslog_server::tcp_server::{ConnectionError, TcpServerConnection};
use crate::syslog_server::udp_server::UdpServerState;
use crate::{AnsiSqlOutput, CsvOutput, DynError, HustlogConfig, OutputFormat, QlSchema, RawMessage};
use log::{debug, info};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::syslog_server::output_processor::{DynOutputSink, OutputProcessor};

/// Create and wire the processing pipeline
/// return a tuple consisting o fthe raw message sender and a vector of JoinHandles to be awaited
/// or error.
fn create_processing_pipeline(
    hcrc: &Arc<HustlogConfig>,
) -> Result<(MessageSender<Vec<RawMessage>>, Vec<QueueJoinHandle>), DynError> {
    let schema = hcrc.get_grok_schema();
    let ql_input_schema = Arc::new(QlSchema::from(&schema));
    let mut sql_processor: Option<SqlBatchProcessor> = None;
    let ql_output_schema = if hcrc.query().is_some() {
        sql_processor = Some(
            SqlBatchProcessor::new(
                hcrc.query().as_ref().unwrap().as_str(),
                schema,
            )?
        );
        sql_processor.as_ref().unwrap().get_output_schema().clone()
    } else {
        ql_input_schema.clone()
    };
    let outp_wr = hcrc.get_outp()?;
    let sink: DynOutputSink = match hcrc.output_format() {
        OutputFormat::DEFAULT => {
            debug!("Using default (CSV) output");
            Arc::new(Mutex::new(CsvOutput::new(
                ql_output_schema.clone(), outp_wr, hcrc.output_add_ddl()
            )))
        }
        OutputFormat::SQL => {
            debug!("Using SQL output");
            Arc::new(Mutex::new(AnsiSqlOutput::new(
                ql_output_schema.clone(), hcrc.output_add_ddl(), hcrc.output_batch_size(), outp_wr
            )))
        }
    };
    let mut join_handles = Vec::new();
    let (mut output_sender, jh) = OutputProcessor::wrap_sink(sink);
    join_handles.push(jh);
    if sql_processor.is_some() {
        let (new_sender, jh) = sql_processor.unwrap().wrap_sender(output_sender)?;
        output_sender = new_sender;
        join_handles.push(jh)
    }
    let (parsed_sender, jh) = BatchingQueue::wrap_output(ql_input_schema, hcrc.output_batch_size(), output_sender);
    join_handles.push(jh);
    let (raw_sender, jh) = AsyncParser::wrap_parsed_sender(parsed_sender, schema.clone())?;
    join_handles.push(jh);
    join_handles.reverse(); //we want to shut these down in reverse order later
    Ok((raw_sender, join_handles))
}

pub async fn server_main(hc: &HustlogConfig) -> Result<(), DynError> {
    let sc = match hc.get_syslog_server_config() {
        Ok(x) => Ok(x),
        Err(ce) => Err(Box::new(ce)),
    }?;
    let host_port = sc.get_host_port();
    let hcrc = Arc::new(hc.clone());
    hcrc.init_rayon_pool()?;
    let (raw_sender, join_handles) = create_processing_pipeline(&hcrc)?;
    match sc.proto.as_str() {
        "tcp" => TcpServerConnection::tcp_server_main(raw_sender, hcrc, &host_port).await?,
        "udp" => UdpServerState::udp_server_main(raw_sender, hcrc, &host_port).await?,
        x => {
            return Err(Box::new(ConnectionError::new(
                format!(
                    "Invalid protocol (only udp and tcp are currently supported): {}",
                    x
                )
                .into(),
            )))
        }
    }
    for jh in join_handles {
        jh.join().await;
    }
    info!("Server shut down");
    Ok(())
}
