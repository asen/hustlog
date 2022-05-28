use crate::syslog_server::batching_queue::BatchingQueue;
use crate::syslog_server::message_queue::MessageSender;
use crate::syslog_server::async_parser::AsyncParser;
use crate::syslog_server::sql_batch_processor::SqlBatchProcessor;
use crate::syslog_server::tcp_server::{ConnectionError, TcpServerConnection};
use crate::syslog_server::udp_server::UdpServerState;
use crate::{AnsiSqlOutput, CsvOutput, DynError, GrokParser, HustlogConfig, OutputFormat, QlSchema, RawMessage};
use log::{debug, info};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::syslog_server::output_processor::{DynOutputSink, OutputProcessor};

// Create and wire the processing pipeline
fn create_batch_processor(
    hcrc: &Arc<HustlogConfig>,
) -> Result<MessageSender<Vec<RawMessage>>, DynError> {
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
    let mut output_sender = OutputProcessor::wrap_sink(sink);
    if sql_processor.is_some() {
        output_sender = sql_processor.unwrap().wrap_sender(output_sender)?;
    }
    let parsed_sender = BatchingQueue::wrap_output(ql_output_schema, hcrc.output_batch_size(), output_sender);
    let grok_parser = GrokParser::new(schema.clone())?;
    let async_parser = AsyncParser::new(parsed_sender, Arc::from(grok_parser));
    let raw_sender = async_parser.clone_sender();
    async_parser.consume_parser_queue_async();
    Ok(raw_sender)
}

pub async fn server_main(hc: &HustlogConfig) -> Result<(), DynError> {
    let sc = match hc.get_syslog_server_config() {
        Ok(x) => Ok(x),
        Err(ce) => Err(Box::new(ce)),
    }?;
    let host_port = sc.get_host_port();
    let hcrc = Arc::new(hc.clone());
    hcrc.init_rayon_pool()?;
    let raw_sender = create_batch_processor(&hcrc)?;
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
    info!("Server shut down");
    Ok(())
}
