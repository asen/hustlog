use crate::ql_processor::{QlRow, QlRowBatch, QlSchema};
use crate::async_pipeline::message_queue::{ChannelReceiver, ChannelSender, MessageQueue, MessageSender, QueueJoinHandle, QueueMessage};
use log::{error, info};
use std::sync::Arc;
use crate::parser::ParsedMessage;

pub struct BatchingQueue {
    tx: ChannelSender<QueueMessage<ParsedMessage>>,
    rx: ChannelReceiver<QueueMessage<ParsedMessage>>,
    buf: Vec<ParsedMessage>,
    schema: Arc<QlSchema>,
    batch_size: usize,
    batch_sender: MessageSender<QlRowBatch>,
    batch_processed: bool, // keeping track whether a batch was processed between flushes
}

impl BatchingQueue {
    fn new(
        schema: Arc<QlSchema>,
        batch_size: usize,
        queue_size: usize,
        batch_sender: MessageSender<QlRowBatch>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(queue_size);
        let buf = Vec::with_capacity(batch_size);
        Self {
            tx,
            rx,
            buf,
            schema,
            batch_size,
            batch_sender,
            batch_processed: false,
        }
    }

    pub fn wrap_output(
        schema: Arc<QlSchema>,
        batch_size: usize,
        queue_size: usize,
        batch_sender: MessageSender<QlRowBatch>,
    ) -> (MessageSender<ParsedMessage>, QueueJoinHandle) {
        let batching_queue = BatchingQueue::new(schema, batch_size, queue_size, batch_sender);
        let parsed_sender = batching_queue.clone_sender();
        let jh = batching_queue.consume_batching_queue_async();
        (parsed_sender, jh)
    }

    fn batch_message(&mut self, pm: ParsedMessage) -> Option<Vec<ParsedMessage>> {
        self.buf.push(pm);
        if self.buf.len() >= self.batch_size {
            let batch = self.flush();
            return Some(batch);
        }
        None
    }

    fn flush(&mut self) -> Vec<ParsedMessage> {
        self.buf.drain(0..).collect::<Vec<_>>()
    }

    async fn process_batch(&mut self, batch: Vec<ParsedMessage>) {
        if batch.is_empty() {
            return;
        }
        let my_sender = self.batch_sender.clone_sender();
        let my_schema = Arc::clone(&self.schema);
        let res = tokio_rayon::spawn_fifo(move || {
            let mut to_send = Vec::with_capacity(batch.len());
            for pm in batch {
                to_send.push(QlRow::from_parsed_message(pm, my_schema.as_ref()))
            }
            to_send
        })
        .await;
        if let Err(err) = my_sender.send(res).await {
            error!("Error sending batch downstream: {}", err);
        }
    }

    async fn consume_queue(&mut self) {
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(pm) => {
                    if let Some(batch) = self.batch_message(pm) {
                        self.process_batch(batch).await;
                        self.batch_processed = true; // this prevents Flush from actually flushing next time
                    }
                }
                QueueMessage::Flush => {
                    // only flush if no batches were processed since last flush
                    if !self.batch_processed {
                        let batch = self.flush();
                        self.process_batch(batch).await;
                    }
                    self.batch_processed = false;
                }
                QueueMessage::Shutdown => {
                    info!("Shutdown message received");
                    let batch = self.flush();
                    self.process_batch(batch).await;
                    if let Err(err) = self.batch_sender.shutdown().await {
                        error!("Failed to send shutdown message to batch_sender: {:?}", err)
                    }
                    break;
                }
            }
        }
    }

    fn consume_batching_queue_async(mut self) -> QueueJoinHandle {
        let jh = tokio::spawn(async move {
            info!("Consuming parsed messages queue ...");
            self.consume_queue().await;
            info!("Done consuming parsed messages queue.");
            Ok(())
        });
        QueueJoinHandle::new("batching", jh)
    }
}

impl MessageQueue<ParsedMessage> for BatchingQueue {
    fn clone_sender(&self) -> MessageSender<ParsedMessage> {
        MessageSender::new(self.tx.clone())
    }
}
