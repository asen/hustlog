use crate::syslog_server::lines_buffer::LinesBuffer;
use crate::syslog_server::message_queue::{MessageSender, QueueMessage};
use crate::RawMessage;
use bytes::BufMut;
use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

fn system_time_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

pub struct UdpStream {
    last_data_rcvd: u64,
    remote_addr: Arc<str>,
    buffer: LinesBuffer,
}

impl UdpStream {
    pub fn new(remote_addr: Arc<str>, use_line_merger: bool) -> Self {
        Self {
            last_data_rcvd: system_time_now(),
            remote_addr,
            buffer: LinesBuffer::new(2048, use_line_merger), // TODO
        }
    }

    pub fn get_buffer(&mut self) -> &mut LinesBuffer {
        &mut self.buffer
    }

    pub fn touch(&mut self) {
        self.last_data_rcvd = system_time_now();
    }

    pub fn is_expired(&self, min_ttl: u64, now: u64) -> bool {
        self.last_data_rcvd + min_ttl < now
    }

    pub fn get_age_secs(&self) -> u64 {
        system_time_now() - self.last_data_rcvd
    }

    pub fn get_remote_addr(&self) -> &Arc<str> {
        &self.remote_addr
    }
}

#[derive(Debug)]
pub struct UdpData {
    sender: Arc<str>,
    data: Vec<u8>,
}

impl UdpData {
    pub fn new(sender: Arc<str>, data: Vec<u8>) -> Self {
        Self { sender, data }
    }
}

pub struct UdpServerState {
    parser_tx: MessageSender<Vec<RawMessage>>,
    tx: UnboundedSender<QueueMessage<UdpData>>,
    rx: UnboundedReceiver<QueueMessage<UdpData>>,
    streams: HashMap<Arc<str>, UdpStream>,
    min_idle_ttl: u64,
    use_line_merger: bool,
}

impl UdpServerState {
    pub fn new(
        parser_tx: MessageSender<Vec<RawMessage>>,
        min_idle_ttl: u64,
        use_line_merger: bool,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            parser_tx,
            tx,
            rx,
            streams: HashMap::new(),
            min_idle_ttl,
            use_line_merger,
        }
    }

    async fn flush(&mut self, now: u64) -> usize {
        let min_ttl = self.min_idle_ttl;
        let expired = self
            .streams
            .iter()
            .filter(|&(_, v)| v.is_expired(min_ttl, now))
            .map(|(k, _)| k.clone())
            .collect::<Vec<Arc<str>>>();
        let mut total_drained = 0;
        for k in expired {
            if let Some(mut removed) = self.streams.remove(&k) {
                let drained = self.drain_stream(removed.get_buffer()).await;
                info!(
                    "Closed udp stream: remote_addr={} age={} drained={}",
                    removed.get_remote_addr(),
                    removed.get_age_secs(),
                    drained
                );
                total_drained += drained;
            };
        }
        if let Err(err) = self.parser_tx.flush() {
            error!("Failed to send flush message to parser: {}", err)
        }
        total_drained
    }

    async fn drain_stream(&mut self, buf: &mut LinesBuffer) -> usize {
        //let buf = stream.get_buffer();
        let msgs = buf.read_messages_from_buf();
        let ret = msgs.len();
        if let Err(err) = self.parser_tx.send(msgs) {
            error!("Error sending parsed message downstream: {:?}", err);
        }
        ret
    }

    async fn consume_queue(&mut self) {
        while let Some(usmsg) = self.rx.recv().await {
            match usmsg {
                QueueMessage::Data(ud) => {
                    // info!("Data message received: {:?}", ud);
                    let UdpData {
                        sender: remote_addr,
                        data,
                    } = ud;
                    let stream = self
                        .streams
                        .entry(remote_addr.clone())
                        .or_insert(UdpStream::new(remote_addr, self.use_line_merger));
                    stream.touch();
                    let lines_buf = stream.get_buffer();
                    lines_buf.get_buf().put(data.as_slice());
                    let msgs = lines_buf.read_messages_from_buf();
                    if let Err(err) = self.parser_tx.send(msgs) {
                        error!(
                            "Error sending parsed message downstream - aborting: {:?}",
                            err
                        );
                        break;
                    };
                }
                QueueMessage::Flush => {
                    let flushed = self.flush(system_time_now()).await;
                    debug!(
                        "UdpServerState: Flush message received: flushed={}",
                        flushed
                    );
                }
                QueueMessage::Shutdown => {
                    let flushed = self.flush(0).await; //everything is expired when shutting down
                    info!("Shutdown message received: flushed={}", flushed);
                    break;
                }
            }
        }
    }

    pub fn consume_udp_server_state_queue_async(mut self) {
        tokio::spawn(async move {
            info!("Consuming Udp Server State messages queue ...");
            self.consume_queue().await;
            info!("Done consuming Udp Server State messages queue.");
        });
    }

    pub fn get_sender(&self) -> MessageSender<UdpData> {
        MessageSender::new(self.tx.clone())
    }
}
