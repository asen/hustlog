use crate::syslog_server::batch_processor::BatchProcessor;
use crate::ParsedMessage;
use log::{error, info};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct QueueError(String);
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChannelError: {}", self.0)
    }
}
impl Error for QueueError {}

impl From<SendError<QueueMessage>> for QueueError {
    fn from(err: SendError<QueueMessage>) -> Self {
        Self(err.to_string())
    }
}

enum QueueMessage {
    Data(ParsedMessage),
    Flush,
    Shutdown,
}

pub struct MessageSender {
    channel_sender: UnboundedSender<QueueMessage>,
}

//TODO maybe convert to a trait if unbounded_queue needs to be bounded/configurable
impl MessageSender {
    //NOT pub
    fn new(channel_sender: UnboundedSender<QueueMessage>) -> Self {
        Self { channel_sender }
    }

    pub fn send(&self, value: ParsedMessage) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Data(value))
            .map_err(|e| e.into())
    }

    pub fn shutdown(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Shutdown)
            .map_err(|e| e.into())
    }

    pub fn flush(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Flush)
            .map_err(|e| e.into())
    }

    pub fn clone(&self) -> Self {
        Self {
            channel_sender: self.channel_sender.clone(),
        }
    }
}

pub trait MessageQueue {
    fn clone_sender(&self) -> MessageSender;
}

pub struct BatchingQueue {
    tx: UnboundedSender<QueueMessage>,
    rx: UnboundedReceiver<QueueMessage>,
    buf: Vec<ParsedMessage>,
    batch_size: usize,
    batch_processor: Arc<dyn BatchProcessor + Send + Sync>,
}

impl BatchingQueue {
    pub fn new(batch_size: usize, batch_processor: Arc<dyn BatchProcessor + Send + Sync>) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let buf = Vec::with_capacity(batch_size);
        Self {
            tx,
            rx,
            buf,
            batch_size,
            batch_processor,
        }
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
        let my_bp = Arc::clone(&self.batch_processor);
        tokio_rayon::spawn_fifo(move || {
            //if let Err(err) = my_bp.lock().unwrap().process_batch(batch)
            if let Err(err) = my_bp.process_batch(batch) {
                error!("Error processing batch: {}", err);
            }
        })
        .await
    }

    pub async fn consume_queue(&mut self) {
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(pm) => {
                    if let Some(batch) = self.batch_message(pm) {
                        self.process_batch(batch).await
                    }
                }
                QueueMessage::Flush => {
                    let batch = self.flush();
                    self.process_batch(batch).await;
                }
                QueueMessage::Shutdown => {
                    info!("Shutdown message received");
                    let batch = self.flush();
                    self.process_batch(batch).await;
                    break;
                }
            }
        }
    }
}

impl MessageQueue for BatchingQueue {
    fn clone_sender(&self) -> MessageSender {
        MessageSender::new(self.tx.clone())
    }
}
