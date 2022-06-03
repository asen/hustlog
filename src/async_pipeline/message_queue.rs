use crate::DynError;
use log::{debug, error};
use std::error::Error;
use std::fmt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

pub struct QueueJoinHandle {
    name: &'static str,
    handle: JoinHandle<Result<(), DynError>>,
}

impl QueueJoinHandle {
    pub fn new(name: &'static str, handle: JoinHandle<Result<(), DynError>>) -> Self {
        Self { name, handle }
    }

    pub async fn join(self) -> () {
        let Self { name, handle } = self;
        match handle.await {
            Ok(join_ok) => {
                if let Err(err) = join_ok {
                    error!(
                        "QueueJoinHandle({}) consumeing queue returned error {:?}",
                        name, err
                    )
                } else {
                    debug!("QueueJoinHandle({}) completed with success", name)
                }
            }
            Err(join_err) => {
                error!(
                    "QueueJoinHandle({}) join returned error {:?}",
                    name, join_err
                )
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

#[cfg(test)]
pub mod tests {
    use crate::async_pipeline::message_queue::{ChannelReceiver, MessageSender, QueueMessage};
    use crate::DynError;
    use log::info;
    use tokio::task::JoinHandle;
    use tokio_rayon::rayon::ThreadPoolBuilder;

    pub fn init_test_rayon_pool() {
        // ignoring errors ...
        let _ = ThreadPoolBuilder::new().num_threads(2).build_global();
    }

    pub struct TestMessageQueue<T> {
        rx: ChannelReceiver<QueueMessage<T>>,
        pub received: usize,
        pub flushed: usize,
        pub shutdown: usize,
        pub keep_buf: bool,
        pub truncate_on_flush: bool,
        pub buf: Vec<T>,
    }

    impl<T> TestMessageQueue<T>
    where
        T: Send + Sync + 'static,
    {
        pub fn create(
            channel_size: usize,
            keep_buf: bool,
            truncate_on_flush: bool,
        ) -> (
            MessageSender<T>,
            JoinHandle<Result<TestMessageQueue<T>, DynError>>,
        ) {
            let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
            let mq = Self {
                rx,
                received: 0,
                flushed: 0,
                shutdown: 0,
                keep_buf,
                truncate_on_flush,
                buf: Vec::new(),
            };
            let ret = MessageSender::new(tx);
            let jh = mq.consume_queue_async();
            (ret, jh)
        }

        pub fn consume_queue_async(mut self) -> JoinHandle<Result<TestMessageQueue<T>, DynError>> {
            tokio::spawn(async move {
                info!("Consuming test queue ...");
                self.consume_queue().await;
                info!("Done consuming test queue.");
                Ok(self)
            })
        }

        async fn consume_queue(&mut self) {
            while let Some(cmsg) = self.rx.recv().await {
                match cmsg {
                    QueueMessage::Data(d) => {
                        self.received += 1;
                        if self.keep_buf {
                            self.buf.push(d);
                        }
                    }
                    QueueMessage::Flush => {
                        self.flushed += 1;
                        if self.truncate_on_flush {
                            self.buf.truncate(0);
                        }
                    }
                    QueueMessage::Shutdown => {
                        self.shutdown += 1;
                        break;
                    }
                }
            }
        }
    }
}
