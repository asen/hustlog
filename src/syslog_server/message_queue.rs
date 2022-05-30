use std::error::Error;
use std::fmt;
use log::{debug, error};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;
use crate::DynError;

pub struct QueueJoinHandle {
    name: &'static str,
    handle: JoinHandle<Result<(),DynError>>,
}

impl QueueJoinHandle {
    pub fn new(
        name: &'static str,
        handle: JoinHandle<Result<(),DynError>>,
    ) -> Self {
        Self {
            name,
            handle,
        }
    }

    pub async fn join(self) -> () {
        let Self { name, handle } = self;
        match handle.await {
            Ok(join_ok) => {
                if let Err(err) = join_ok {
                    error!("QueueJoinHandle({}) consumeing queue returned error {:?}", name, err)
                } else {
                    debug!("QueueJoinHandle({}) completed with success", name)
                }
            }
            Err(join_err) => {
                error!("QueueJoinHandle({}) join returned error {:?}", name, join_err)
            }
        };
    }
}

pub enum QueueMessage<T> {
    Data(T),
    Flush,
    Shutdown,
}

#[derive(Debug)]
pub struct QueueError(String);
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QueueError: {}", self.0)
    }
}
impl Error for QueueError {}


pub type ChannelSender<T> = Sender<T>;
pub type ChannelReceiver<T> = Receiver<T>;

pub struct MessageSender<T> {
    channel_sender: ChannelSender<QueueMessage<T>>,
}

//TODO maybe convert to a trait if unbounded_queue needs to be bounded/configurable
impl<T> MessageSender<T> {
    pub fn new(channel_sender: ChannelSender<QueueMessage<T>>) -> Self {
        Self { channel_sender }
    }

    pub async fn send(&self, value: T) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Data(value))
            .await
            .map_err(|e| QueueError(e.to_string()))
    }

    pub async fn shutdown(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Shutdown)
            .await
            .map_err(|e| QueueError(e.to_string()))
    }

    pub async fn flush(&self) -> Result<(), QueueError> {
        self.channel_sender
            .send(QueueMessage::Flush)
            .await
            .map_err(|e| QueueError(e.to_string()))
    }

    pub fn clone_sender(&self) -> Self {
        Self {
            channel_sender: self.channel_sender.clone(),
        }
    }
}

pub trait MessageQueue<T> {
    fn clone_sender(&self) -> MessageSender<T>;
}
