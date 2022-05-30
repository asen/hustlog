use crate::query_processor::QlRowBatch;
use crate::syslog_server::message_queue::{
    ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage,
};
use crate::OutputSink;
use log::{error, info};
use std::sync::Arc;
use tokio::sync::Mutex;

pub type DynOutputSink = Arc<Mutex<dyn OutputSink + Send + Sync>>;

pub struct OutputProcessor {
    rx: ChannelReceiver<QueueMessage<QlRowBatch>>,
    tx: ChannelSender<QueueMessage<QlRowBatch>>,
    output_sink: DynOutputSink,
    //join_handle: Option<JoinHandle<()>>,
    add_ddl: bool,
}

impl OutputProcessor {
    fn new(output_sink: DynOutputSink, queue_size: usize, add_ddl: bool) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(queue_size);
        Self {
            rx,
            tx,
            output_sink,
            //join_handle: None,
            add_ddl,
        }
    }

    pub fn wrap_sink(
        output_sink: DynOutputSink,
        queue_size: usize,
        add_ddl: bool,
    ) -> (MessageSender<QlRowBatch>, QueueJoinHandle) {
        let op = Self::new(output_sink, queue_size, add_ddl);
        let ret = op.clone_sender();
        let jh = op.consume_queue_async();
        (ret, jh)
    }

    fn clone_sender(&self) -> MessageSender<QlRowBatch> {
        MessageSender::new(self.tx.clone())
    }

    pub fn consume_queue_async(mut self) -> QueueJoinHandle {
        let jh = tokio::spawn(async move {
            info!("Consuming output queue ...");
            self.consume_queue().await;
            info!("Done consuming output queue.");
            Ok(())
        });
        QueueJoinHandle::new("output", jh)
    }

    async fn consume_queue(&mut self) {
        if self.add_ddl {
            if let Err(err) = self.output_sink.lock().await.output_header() {
                error!("Failed to output header, aborting: {:?}", err);
                return;
            };
        }
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

#[cfg(test)]
mod test {
    use crate::query_processor::QlRow;
    use crate::syslog_server::output_processor::{DynOutputSink, OutputProcessor};
    use crate::{DynError, OutputSink, ParsedValue};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub fn create_test_sink() -> DynOutputSink {
        Arc::new(Mutex::new(TestOutputSink::new()))
    }

    pub struct TestOutputSink {
        pub output_header_called: u32,
        pub output_row_called: u32,
        pub flush_called: u32,
    }

    impl TestOutputSink {
        pub fn new() -> Self {
            Self {
                output_header_called: 0,
                output_row_called: 0,
                flush_called: 0,
            }
        }
    }

    impl OutputSink for TestOutputSink {
        fn output_header(&mut self) -> Result<(), DynError> {
            self.output_header_called += 1;
            println!(
                "TestOutputSink.output_header invoked ({})",
                &self.output_header_called
            );
            Ok(())
        }

        fn output_row(&mut self, row: QlRow) -> Result<(), DynError> {
            self.output_row_called += 1;
            println!(
                "TestOutputSink.output_row invoked: ROW ({}): {:?}",
                &self.output_row_called, row
            );
            Ok(())
        }

        fn flush(&mut self) -> Result<(), DynError> {
            self.flush_called += 1;
            println!(
                "TestOutputSink.flush_called invoked ({})",
                &self.flush_called
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_async_paser() {
        let test_sink: DynOutputSink = create_test_sink();
        let (sender, jh) = OutputProcessor::wrap_sink(test_sink.clone(), 10, false);
        sender
            .send(vec![QlRow::new(
                None,
                vec![
                    (Arc::from("blah1"), ParsedValue::LongVal(42)),
                    (
                        Arc::from("blah2"),
                        ParsedValue::StrVal(Arc::new("blah 2 value".to_string())),
                    ),
                ],
            )])
            .await
            .unwrap();
        sender.flush().await.unwrap();
        sender.shutdown().await.unwrap();
        jh.join().await;
    }
}
