use std::sync::Arc;
use log::{error, info};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use crate::OutputSink;
use crate::query_processor::QlRowBatch;
use crate::syslog_server::message_queue::{MessageSender, QueueMessage};

pub type DynOutputSink = Arc<Mutex<dyn OutputSink + Send + Sync>>;

pub struct OutputProcessor {
    rx: UnboundedReceiver<QueueMessage<QlRowBatch>>,
    tx: UnboundedSender<QueueMessage<QlRowBatch>>,
    output_sink: DynOutputSink,
}

impl OutputProcessor {

    fn new(output_sink: DynOutputSink) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            rx,
            tx,
            output_sink,
        }
    }

    pub fn wrap_sink(output_sink: DynOutputSink) -> MessageSender<QlRowBatch> {
        let op = Self::new(output_sink);
        let ret = op.clone_sender();
        op.consume_queue_async();
        ret
    }

    fn clone_sender(&self) -> MessageSender<QlRowBatch> {
        MessageSender::new(self.tx.clone())
    }

    pub fn consume_queue_async(mut self) -> () {
        tokio::spawn(async move {
            info!("Consuming output queue ...");
            self.consume_queue().await;
            info!("Done consuming output queue.");
        });
    }

    async fn consume_queue(&mut self) {
        while let Some(cmsg) = self.rx.recv().await {
            match cmsg {
                QueueMessage::Data(rb) => {
                    let mut sink = self.output_sink.lock().await;
                    if let Err(err) = sink.output_batch(rb) {
                        error!("Failed to output row, aborting: {:?}", err);
                        break;
                    }
                }
                QueueMessage::Flush => {
                    let mut sink = self.output_sink.lock().await;
                    if let Err(err) = sink.flush() {
                        error!("Failed to flush output sink, aborting: {:?}", err);
                        break;
                    }
                }
                QueueMessage::Shutdown => {
                    info!("Shutdown message received");
                    let mut sink = self.output_sink.lock().await;
                    if let Err(err) = sink.flush() {
                        error!("Failed to flush output sink during shutdown: {:?}", err);
                    }
                    break;
                }
            }
        }
    }
}
