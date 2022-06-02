use crate::async_pipeline::message_queue::{
    ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage,
};
use crate::parser::{GrokParser, GrokSchema, LogParser, RawMessage};
use crate::ql_processor::{QlRow, QlRowBatch, QlSchema};
use crate::DynError;
use log::{error, info};
use std::sync::Arc;

pub struct AsyncParser {
    parsed_tx: MessageSender<QlRowBatch>,
    tx: ChannelSender<QueueMessage<Vec<RawMessage>>>,
    rx: ChannelReceiver<QueueMessage<Vec<RawMessage>>>,
    ql_schema: Arc<QlSchema>,
    log_parser: Arc<GrokParser>,
}

impl AsyncParser {
    pub fn wrap_parsed_sender(
        parsed_sender: MessageSender<QlRowBatch>,
        schema: GrokSchema,
        channel_size: usize,
    ) -> Result<(MessageSender<Vec<RawMessage>>, QueueJoinHandle), DynError> {
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let grok_parser = GrokParser::new(schema)?;
        let async_parser = AsyncParser::new(
            parsed_sender,
            ql_schema,
            Arc::from(grok_parser),
            channel_size,
        );
        let raw_sender = async_parser.clone_sender();
        let jh = async_parser.consume_parser_queue_async();
        Ok((raw_sender, jh))
    }

    fn new(
        parsed_tx: MessageSender<QlRowBatch>,
        ql_schema: Arc<QlSchema>,
        log_parser: Arc<GrokParser>,
        channel_size: usize,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
        Self {
            parsed_tx,
            tx,
            rx,
            log_parser,
            ql_schema,
        }
    }

    fn consume_parser_queue_async(mut self) -> QueueJoinHandle {
        let jh = tokio::spawn(async move {
            info!("Consuming Raw messages queue ...");
            self.consume_queue().await;
            info!("Done consuming Raw messages queue.");
            Ok(())
        });
        QueueJoinHandle::new("parser", jh)
    }

    async fn consume_queue(&mut self) {
        info!("ASEN: Consuming Raw messages queue ...");
        while let Some(msg) = self.rx.recv().await {
            //let parsed_tx = self.parsed_tx.clone();
            match msg {
                QueueMessage::Data(batch) => {
                    let parsed = self.parse_batch(batch).await;
                    if !parsed.is_empty() {
                        if let Err(err) = self.parsed_tx.send(parsed).await {
                            error!(
                                "Failed to send parsed message batch downstream, aborting: {}",
                                err
                            );
                            break;
                        };
                    }
                }
                QueueMessage::Flush => {
                    if let Err(err) = self.parsed_tx.flush().await {
                        error!("Failed to send flush message downstream, aborting: {}", err);
                        break;
                    }
                }
                QueueMessage::Shutdown => {
                    if let Err(err) = self.parsed_tx.shutdown().await {
                        error!("Failed to send shutdown message downstream: {}", err);
                    }
                    break;
                }
            }
        }
    }

    async fn parse_batch(&self, raw_vec: Vec<RawMessage>) -> QlRowBatch {
        let parser_ref = Arc::clone(&self.log_parser);
        let ql_schema_ref = Arc::clone(&self.ql_schema);
        tokio_rayon::spawn_fifo(move || {
            let mut ret_buf = Vec::with_capacity(raw_vec.len());
            for raw in raw_vec {
                let parse_res = parser_ref.parse(raw);
                match parse_res {
                    Ok(parsed) => {
                        let ql_row = QlRow::from_parsed_message(parsed, ql_schema_ref.as_ref());
                        ret_buf.push(ql_row)
                    }
                    Err(err) => {
                        // TODO add send_error to MessageSender ?
                        error!("Error parsing message: {}", err);
                    }
                }
            }
            ret_buf
        })
        .await
    }

    pub fn clone_sender(&self) -> MessageSender<Vec<RawMessage>> {
        MessageSender::new(self.tx.clone())
    }
}
