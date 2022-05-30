use crate::syslog_server::message_queue::{ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage};
use crate::{DynError, GrokParser, GrokSchema, LogParser, ParsedMessage, RawMessage};
use log::{error, info};
use std::sync::Arc;

pub struct AsyncParser {
    parsed_tx: MessageSender<ParsedMessage>,
    tx: ChannelSender<QueueMessage<Vec<RawMessage>>>,
    rx: ChannelReceiver<QueueMessage<Vec<RawMessage>>>,
    log_parser: Arc<GrokParser>,
}

impl AsyncParser {
    pub fn wrap_parsed_sender(
        parsed_sender: MessageSender<ParsedMessage>,
        schema: GrokSchema,
        queue_size: usize,
    ) -> Result<(MessageSender<Vec<RawMessage>>,QueueJoinHandle), DynError> {
        let grok_parser = GrokParser::new(schema)?;
        let async_parser = AsyncParser::new(parsed_sender, Arc::from(grok_parser), queue_size);
        let raw_sender = async_parser.clone_sender();
        let jh = async_parser.consume_parser_queue_async();
        Ok((raw_sender, jh))
    }

    fn new(parsed_tx: MessageSender<ParsedMessage>, log_parser: Arc<GrokParser>, queue_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(queue_size);
        Self {
            parsed_tx,
            tx,
            rx,
            log_parser,
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
        while let Some(msg) = self.rx.recv().await {
            //let parsed_tx = self.parsed_tx.clone();
            match msg {
                QueueMessage::Data(batch) => self.parse_batch(batch).await,
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

    async fn parse_batch(&self, raw_vec: Vec<RawMessage>) {
        let parser_ref = Arc::clone(&self.log_parser);
        let sender_ref = self.parsed_tx.clone_sender();
        let parse_res = tokio_rayon::spawn_fifo(move || {
            let mut ret_buf = Vec::with_capacity(raw_vec.len());
            for raw in raw_vec {
                let parse_res = parser_ref.parse(raw);
                match parse_res {
                    Ok(parsed) => {
                        ret_buf.push(parsed)
                    }
                    Err(err) => {
                        // TODO add send_error to MessageSender ?
                        error!("Error parsing message: {}", err);
                    }
                }
            }
            ret_buf
        }).await;
        for parsed in parse_res {
            if let Err(err) = sender_ref.send(parsed).await {
                error!(
                                "Error sending parsed message downstream - aborting: {:?}",
                                err
                            );
                break;
            };
        }
    }

    pub fn clone_sender(&self) -> MessageSender<Vec<RawMessage>> {
        MessageSender::new(self.tx.clone())
    }
}
