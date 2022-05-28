use crate::syslog_server::batch_processor::{BatchProcessor, DummyBatchProcessor};
use crate::syslog_server::batching_queue::BatchingQueue;
use crate::syslog_server::message_queue::{MessageQueue, MessageSender};
use crate::syslog_server::async_parser::AsyncParser;
use crate::syslog_server::sql_batch_processor::SqlBatchProcessor;
use crate::syslog_server::tcp_server::{ConnectionError, TcpServerConnection};
use crate::syslog_server::udp_server::UdpServerState;
use crate::{GrokParser, HustlogConfig, RawMessage};
use log::info;
use std::error::Error;
use std::sync::Arc;

// Create and wire the processing pipeline
fn create_batch_processor(
    hcrc: &Arc<HustlogConfig>,
) -> Result<MessageSender<Vec<RawMessage>>, Box<dyn Error>> {
    let schema = hcrc.get_grok_schema();
    // TODO use proper processor, depending on "output" config
    //let batch_processor: Arc<Mutex<dyn BatchProcessor + Send>> = Arc::new(Mutex::new(DummyBatchProcessor{}));
    let batch_processor: Arc<dyn BatchProcessor + Send + Sync> = if hcrc.query().is_some() {
        Arc::new(SqlBatchProcessor::new(
            hcrc.query().as_ref().unwrap().as_str(),
            schema,
        )?)
    } else {
        Arc::new(DummyBatchProcessor {})
    };
    let batching_queue = BatchingQueue::new(hcrc.output_batch_size(), batch_processor);
    let parsed_sender = batching_queue.clone_sender();
    batching_queue.consume_batching_queue_async();
    let grok_parser = GrokParser::new(schema.clone())?;
    let async_parser = AsyncParser::new(parsed_sender, Arc::from(grok_parser));
    let raw_sender = async_parser.get_sender();
    async_parser.consume_parser_queue_async();
    Ok(raw_sender)
}

pub async fn server_main(hc: &HustlogConfig) -> Result<(), Box<dyn Error>> {
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
