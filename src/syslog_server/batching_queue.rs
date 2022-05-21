use crate::ParsedMessage;
use std::error::Error;
use std::fmt;
use log::info;
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
            channel_sender: self.channel_sender.clone()
        }
    }
}

pub struct BatchingQueue {
    tx: UnboundedSender<QueueMessage>,
    rx: UnboundedReceiver<QueueMessage>,
    buf: Vec<ParsedMessage>,
    batch_size: usize,
}

impl BatchingQueue {
    pub fn new(batch_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let buf = Vec::with_capacity(batch_size);
        Self {
            tx,
            rx,
            buf,
            batch_size,
        }
    }

    pub fn get_sender(&self) -> MessageSender {
        MessageSender::new(self.tx.clone())
    }

    fn batch_message(&mut self, pm: ParsedMessage) -> Option<Vec<ParsedMessage>> {
        self.buf.push(pm);
        if self.buf.len() >= self.batch_size {
            let batch = self.flush();
            return Some(batch)
        }
        None
    }

    fn flush(&mut self) -> Vec<ParsedMessage> {
        self.buf.drain(0..).collect::<Vec<_>>()
    }

    fn process_batch(&mut self, batch: Vec<ParsedMessage>) {
        info!("TODO process batch with size {}", batch.len())
    }

    pub async fn consume_queue(&mut self) {
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(pm) => {
                    if let Some(batch) = self.batch_message(pm) {
                        self.process_batch(batch)
                    }
                }
                QueueMessage::Flush => {
                    let batch = self.flush();
                    self.process_batch(batch);
                }
                QueueMessage::Shutdown => {
                    info!("Shutdown message received");
                    let batch = self.flush();
                    self.process_batch(batch);
                    break
                }
            }
        }
    }
}
