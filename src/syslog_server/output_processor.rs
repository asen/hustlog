use std::sync::Arc;
use log::{error, info};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use crate::OutputSink;
use crate::query_processor::QlRowBatch;
use crate::syslog_server::message_queue::{MessageSender, QueueJoinHandle, QueueMessage};

pub type DynOutputSink = Arc<Mutex<dyn OutputSink + Send + Sync>>;

pub struct OutputProcessor {
    rx: UnboundedReceiver<QueueMessage<QlRowBatch>>,
    tx: UnboundedSender<QueueMessage<QlRowBatch>>,
    output_sink: DynOutputSink,
    //join_handle: Option<JoinHandle<()>>,
}

impl OutputProcessor {

    fn new(output_sink: DynOutputSink) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            rx,
            tx,
            output_sink,
            //join_handle: None,
        }
    }

    pub fn wrap_sink(output_sink: DynOutputSink) -> (MessageSender<QlRowBatch>, QueueJoinHandle) {
        let op = Self::new(output_sink);
        let ret = op.clone_sender();
        let jh = op.consume_queue_async();
        (ret,jh)
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
    use std::sync::Arc;
    use tokio::sync::Mutex;
    use crate::{DynError, OutputSink, ParsedValue};
    use crate::query_processor::QlRow;
    use crate::syslog_server::output_processor::{DynOutputSink, OutputProcessor};

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
                flush_called: 0
            }
        }
    }

    impl OutputSink for TestOutputSink {
        fn output_header(&mut self) -> Result<(), DynError> {
            self.output_header_called += 1;
            println!("TestOutputSink.output_header invoked ({})", &self.output_header_called);
            Ok(())
        }

        fn output_row(&mut self, row: QlRow) -> Result<(), DynError> {
            self.output_row_called += 1;
            println!("TestOutputSink.output_row invoked: ROW ({}): {:?}", &self.output_row_called, row);
            Ok(())
        }

        fn flush(&mut self) -> Result<(), DynError> {
            self.flush_called += 1;
            println!("TestOutputSink.flush_called invoked ({})", &self.flush_called);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_async_paser() {
        let test_sink: DynOutputSink = create_test_sink();
        let (sender, jh) = OutputProcessor::wrap_sink(test_sink.clone());
        sender.send(
            vec![
                QlRow::new(None, vec![
                    (Arc::from("blah1"), ParsedValue::LongVal(42)),
                    (Arc::from("blah2"), ParsedValue::StrVal(Arc::new("blah 2 value".to_string())))
                ])
            ]
        ).unwrap();
        sender.flush().unwrap();
        sender.shutdown().unwrap();
        jh.join().await;
    }

}
