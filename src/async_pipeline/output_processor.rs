use crate::async_pipeline::message_queue::{
    ChannelReceiver, ChannelSender, MessageSender, QueueJoinHandle, QueueMessage,
};
use crate::output::OutputSink;
use crate::ql_processor::QlRowBatch;
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
    fn new(output_sink: DynOutputSink, channel_size: usize, add_ddl: bool) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_size);
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
        channel_size: usize,
        add_ddl: bool,
    ) -> (MessageSender<QlRowBatch>, QueueJoinHandle) {
        let op = Self::new(output_sink, channel_size, add_ddl);
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
                    if let Err(err) = sink.shutdown() {
                        error!("Failed to shutdown output sinks: {:?}", err);
                    }
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use crate::async_pipeline::output_processor::OutputProcessor;
    use crate::output::OutputSink;
    use crate::parser::ParsedValue;
    use crate::ql_processor::QlRow;
    use crate::DynError;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    pub fn create_test_sink() -> Arc<Mutex<TestOutputSink>> {
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

        fn output_row(&mut self, _row: QlRow) -> Result<(), DynError> {
            self.output_row_called += 1;
            // println!(
            //     "TestOutputSink.output_row invoked: ROW ({}): {:?}",
            //     &self.output_row_called, row
            // );
            Ok(())
        }
    }

    impl OutputSink for TestOutputSink {
        fn output_header(&mut self) -> Result<(), DynError> {
            self.output_header_called += 1;
            // println!(
            //     "TestOutputSink.output_header invoked ({})",
            //     &self.output_header_called
            // );
            Ok(())
        }

        fn flush(&mut self) -> Result<(), DynError> {
            self.flush_called += 1;
            // println!(
            //     "TestOutputSink.flush_called invoked ({})",
            //     &self.flush_called
            // );
            Ok(())
        }

        fn output_batch(&mut self, batch: Vec<QlRow>) -> Result<(), DynError> {
            for r in batch {
                self.output_row(r)?
            }
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), DynError> {
            Ok(())
        }
    }

    pub fn test_ql_row() -> QlRow {
        QlRow::new(
            None,
            vec![
                (Arc::from("blah1"), Arc::new(ParsedValue::LongVal(42))),
                (
                    Arc::from("blah2"),
                    Arc::new(ParsedValue::StrVal(Arc::new("blah 2 value".to_string()))),
                ),
            ],
        )
    }

    pub fn test_ql_rows(num: usize) -> Vec<QlRow> {
        let mut ret = Vec::with_capacity(num);
        for _ in 0..num {
            ret.push(test_ql_row())
        }
        ret
    }

    #[tokio::test]
    async fn test_async_output_processor1() {
        let test_sink = create_test_sink();
        let (sender, jh) = OutputProcessor::wrap_sink(test_sink.clone(), 10, false);
        sender.send(test_ql_rows(1)).await.unwrap();
        sender.flush().await.unwrap();
        sender.shutdown().await.unwrap();
        jh.join().await;
        let test_sink = test_sink.lock().await;
        assert_eq!(test_sink.output_header_called, 0);
        assert_eq!(test_sink.output_row_called, 1);
        assert_eq!(test_sink.flush_called, 2); // shutdown flushes too
    }

    #[tokio::test]
    async fn test_async_output_processor2() {
        let test_sink = create_test_sink();
        let (sender, jh) = OutputProcessor::wrap_sink(test_sink.clone(), 10, true);
        sender.send(test_ql_rows(1)).await.unwrap();
        sender.send(test_ql_rows(100)).await.unwrap();
        sender.send(test_ql_rows(10)).await.unwrap();
        //sender.flush().await.unwrap();
        sender.shutdown().await.unwrap();
        jh.join().await;
        let test_sink = test_sink.lock().await;
        assert_eq!(test_sink.output_header_called, 1);
        assert_eq!(test_sink.output_row_called, 111);
        assert_eq!(test_sink.flush_called, 1); // shutdown flushes too
    }
}
