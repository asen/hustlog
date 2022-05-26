use std::error::Error;
use std::fmt;
use tokio::sync::mpsc::UnboundedSender;

pub enum QueueMessage<T> {
    Data(T),
    Flush,
    Shutdown,
}

#[derive(Debug)]
pub struct QueueError(String);
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChannelError: {}", self.0)
    }
}
impl Error for QueueError {}

pub struct MessageSender<T> {
    channel_sender: UnboundedSender<QueueMessage<T>>,
}

//TODO maybe convert to a trait if unbounded_queue needs to be bounded/configurable
impl<T> MessageSender<T> {
    pub fn new(channel_sender: UnboundedSender<QueueMessage<T>>) -> Self {
        Self { channel_sender }
    }

    pub fn send(&self, value: T) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Data(value))
            .map_err(|e| QueueError(e.to_string()))
    }

    pub fn shutdown(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Shutdown)
            .map_err(|e| QueueError(e.to_string()))
    }

    pub fn flush(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Flush)
            .map_err(|e| QueueError(e.to_string()))
    }

    pub fn clone(&self) -> Self {
        Self {
            channel_sender: self.channel_sender.clone(),
        }
    }
}

pub trait MessageQueue<T> {
    fn clone_sender(&self) -> MessageSender<T>;
}
