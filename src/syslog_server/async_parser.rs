use crate::syslog_server::message_queue::{MessageSender, QueueMessage};
use crate::{GrokParser, LogParser, ParsedMessage, RawMessage};
use log::{error, info};
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct AsyncParser {
    parsed_tx: MessageSender<ParsedMessage>,
    tx: UnboundedSender<QueueMessage<Vec<RawMessage>>>,
    rx: UnboundedReceiver<QueueMessage<Vec<RawMessage>>>,
    log_parser: Arc<GrokParser>,
}

impl AsyncParser {
    pub fn new(parsed_tx: MessageSender<ParsedMessage>, log_parser: Arc<GrokParser>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            parsed_tx,
            tx,
            rx,
            log_parser,
        }
    }

    pub fn consume_parser_queue_async(mut self) {
        tokio::spawn(async move {
            info!("Consuming Raw messages queue ...");
            self.consume_queue().await;
            info!("Done consuming Raw messages queue.");
        });
    }

    async fn consume_queue(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            //let parsed_tx = self.parsed_tx.clone();
            match msg {
                QueueMessage::Data(batch) => self.parse_batch(batch).await,
                QueueMessage::Flush => {
                    if let Err(err) = self.parsed_tx.flush() {
                        error!("Failed to send flush message downstream, aborting: {}", err);
                        break;
                    }
                }
                QueueMessage::Shutdown => {
                    if let Err(err) = self.parsed_tx.shutdown() {
                        error!("Failed to send shutdown message downstream: {}", err);
                    }
                    break;
                }
            }
        }
    }

    async fn parse_batch(&self, raw_vec: Vec<RawMessage>) {
        let parser_ref = Arc::clone(&self.log_parser);
        let sender_ref = self.parsed_tx.clone();
        tokio_rayon::spawn_fifo(move || {
            for raw in raw_vec {
                let parse_res = parser_ref.parse(raw);
                match parse_res {
                    Ok(parsed) => {
                        if let Err(err) = sender_ref.send(parsed) {
                            error!(
                                "Error sending parsed message downstream - aborting: {:?}",
                                err
                            );
                            break;
                        };
                    }
                    Err(err) => {
                        // TODO add send_error to MessageSender ?
                        error!("Error parsing message: {}", err);
                    }
                }
            }
        })
        .await;
    }

    pub fn clone_sender(&self) -> MessageSender<Vec<RawMessage>> {
        MessageSender::new(self.tx.clone())
    }
}
