use crate::async_pipeline::message_queue::{
    ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage,
};
use crate::parser::RawMessage;
use log::{error, info};

pub struct BatchingQueue {
    tx: ChannelSender<QueueMessage<Vec<RawMessage>>>,
    rx: ChannelReceiver<QueueMessage<Vec<RawMessage>>>,
    buf: Vec<RawMessage>,
    batch_size: usize,
    batch_sender: MessageSender<Vec<RawMessage>>,
    batch_processed: bool, // keeping track whether a batch was processed between flushes
}

impl BatchingQueue {
    fn new(
        batch_size: usize,
        channel_size: usize,
        batch_sender: MessageSender<Vec<RawMessage>>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
        let buf = Vec::with_capacity(batch_size);
        Self {
            tx,
            rx,
            buf,
            batch_size,
            batch_sender,
            batch_processed: false,
        }
    }

    pub fn wrap_output(
        batch_size: usize,
        channel_size: usize,
        batch_sender: MessageSender<Vec<RawMessage>>,
    ) -> (MessageSender<Vec<RawMessage>>, QueueJoinHandle) {
        let batching_queue = BatchingQueue::new(batch_size, channel_size, batch_sender);
        let parsed_sender = batching_queue.clone_sender();
        let jh = batching_queue.consume_batching_queue_async();
        (parsed_sender, jh)
    }

    fn batch_messages(&mut self, mut rm: Vec<RawMessage>) -> Option<Vec<Vec<RawMessage>>> {
        self.buf.append(&mut rm);
        if (self.batch_size > 0) && (self.buf.len() >= self.batch_size) {
            let reminder = if self.buf.len() % self.batch_size == 0 {
                0
            } else {
                1
            };
            let res_size = self.buf.len() / self.batch_size + reminder;
            let mut ret = Vec::with_capacity(res_size);
            while self.buf.len() >= self.batch_size {
                let batch = self.buf.drain(0..self.batch_size).collect::<Vec<_>>();
                ret.push(batch);
            }
            return Some(ret);
        }
        None
    }

    fn flush(&mut self) -> Vec<RawMessage> {
        self.buf.drain(0..).collect::<Vec<_>>()
    }

    async fn consume_queue(&mut self) {
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(rm) => {
                    if let Some(batches) = self.batch_messages(rm) {
                        for batch in batches {
                            if let Err(err) = self.batch_sender.send(batch).await {
                                error!("Error sending batch downstream (aborting): {:?}", err);
                                break;
                            }
                        }
                        self.batch_processed = true; // this prevents Flush from actually flushing next time
                    }
                }
                QueueMessage::Flush => {
                    // only flush if no batches were processed since last flush
                    if !self.batch_processed {
                        let batch = self.flush();
                        if !batch.is_empty() {
                            if let Err(err) = self.batch_sender.send(batch).await {
                                error!(
                                    "Error sending flush batch downstream (aborting): {:?}",
                                    err
                                );
                                break;
                            }
                        }
                        if let Err(err) = self.batch_sender.flush().await {
                            error!(
                                "Failed to send flush message to batch_sender (aborting): {:?}",
                                err
                            );
                            break;
                        }
                    }
                    self.batch_processed = false;
                }
                QueueMessage::Shutdown => {
                    let batch = self.flush();
                    if !batch.is_empty() {
                        if let Err(err) = self.batch_sender.send(batch).await {
                            error!("Error sending shutdown batch downstream: {:?}", err);
                        }
                    }
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
            info!("Consuming raw messages queue ...");
            self.consume_queue().await;
            info!("Done consuming raw messages queue.");
            Ok(())
        });
        QueueJoinHandle::new("batching", jh)
    }

    pub fn clone_sender(&self) -> MessageSender<Vec<RawMessage>> {
        MessageSender::new(self.tx.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::async_pipeline::batching_queue::BatchingQueue;
    use crate::async_pipeline::message_queue::tests::TestMessageQueue;
    use crate::async_pipeline::LinesBuffer;
    use crate::parser::test_dummy_data;
    use bytes::BufMut;

    #[tokio::test]
    async fn test_batching_queue1() {
        let (test_queue_sender, test_queue_jh) = TestMessageQueue::create(2, false, false);
        let (sender, bjh) = BatchingQueue::wrap_output(10, 2, test_queue_sender);
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(test_dummy_data(99).as_bytes());
        let raw_vec = lb.flush();
        sender.send(raw_vec).await.unwrap();
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

    #[tokio::test]
    async fn test_batching_queue_zero_batch_size2() {
        let (test_queue_sender, test_queue_jh) = TestMessageQueue::create(2, true, false);
        let (sender, bjh) = BatchingQueue::wrap_output(0, 2, test_queue_sender);
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(test_dummy_data(99).as_bytes());
        let raw_vec = lb.flush();
        sender.send(raw_vec).await.unwrap();
        sender.flush().await.unwrap();
        sender.flush().await.unwrap(); // buf should be empty so this flush should do nothing other than increase flushed
        sender.shutdown().await.unwrap();
        bjh.join().await;
        let test_queue_res = test_queue_jh.await.unwrap();
        let test_queue = test_queue_res.unwrap();
        assert_eq!(test_queue.buf[0].len(), 99);
        assert_eq!(test_queue.received, 1);
        assert_eq!(test_queue.flushed, 2);
        assert_eq!(test_queue.shutdown, 1);
    }
}
