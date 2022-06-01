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
                        if let Err(err) = self.batch_sender.flush().await {
                            error!("Failed to send flush message to batch_sender (aborting): {:?}", err);
                            break;
                        }
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

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bytes::BufMut;
    use crate::async_pipeline::batching_queue::BatchingQueue;
    use crate::async_pipeline::LinesBuffer;
    use crate::async_pipeline::message_queue::tests::TestMessageQueue;
    use crate::async_pipeline::output_processor::OutputProcessor;
    use crate::async_pipeline::output_processor::test::create_test_sink;
    use crate::parser::{GrokParser, LogParser, test_dummy_data, test_dummy_schema};
    use crate::ql_processor::QlSchema;

    #[tokio::test]
    async fn test_batching_queue1() {
        let test_sink = create_test_sink();
        let (sender, ojh) = OutputProcessor::wrap_sink(test_sink.clone(), 10, false);
        let schema = test_dummy_schema();
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let (sender, bjh) = BatchingQueue::wrap_output(ql_schema, 10, 2, sender);
        let parser = GrokParser::new(schema).unwrap();
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(test_dummy_data(99).as_bytes());
        let raw_vec = lb.flush();
        for raw in raw_vec {
            let parsed = parser.parse(raw).unwrap();
            sender.send(parsed).await.unwrap();
        }
        sender.shutdown().await.unwrap();
        bjh.join().await;
        ojh.join().await;
        let test_sink = test_sink.lock().await;
        assert_eq!(test_sink.output_header_called, 0);
        assert_eq!(test_sink.output_row_called, 99);
        assert_eq!(test_sink.flush_called, 1); // shutdown flushes too
    }

    #[tokio::test]
    async fn test_batching_queue2() {
        let (test_queue_sender, test_queue_jh) = TestMessageQueue::create(2);
        let schema = test_dummy_schema();
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let (sender, bjh) =
            BatchingQueue::wrap_output(ql_schema, 10, 2, test_queue_sender);
        let parser = GrokParser::new(schema).unwrap();
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(test_dummy_data(99).as_bytes());
        let raw_vec = lb.flush();
        for raw in raw_vec {
            let parsed = parser.parse(raw).unwrap();
            sender.send(parsed).await.unwrap();
        }
        sender.flush().await.unwrap(); // first flush after a batch is processed is ignored
        sender.flush().await.unwrap();
        sender.shutdown().await.unwrap();
        bjh.join().await;
        let test_queue_res = test_queue_jh.await.unwrap();
        let test_queue = test_queue_res.unwrap();
        assert_eq!(test_queue.received, 10);
        assert_eq!(test_queue.flushed, 1);
        assert_eq!(test_queue.shutdown, 1);
    }

}