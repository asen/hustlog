use crate::syslog_server::lines_buffer::LinesBuffer;
use crate::async_pipeline::message_queue::{ChannelReceiver, ChannelSender, MessageSender, QueueMessage};
use crate::{DynError, HustlogConfig};
use bytes::BufMut;
use log::{debug, error, info, Level, log_enabled, trace};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::signal;
use tokio::time::interval;
use crate::parser::RawMessage;

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
            buffer: LinesBuffer::new(use_line_merger),
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
    tx: ChannelSender<QueueMessage<UdpData>>,
    rx: ChannelReceiver<QueueMessage<UdpData>>,
    streams: HashMap<Arc<str>, UdpStream>,
    min_idle_ttl: u64,
    use_line_merger: bool,
}

impl UdpServerState {
    pub fn new(
        parser_tx: MessageSender<Vec<RawMessage>>,
        min_idle_ttl: u64,
        use_line_merger: bool,
        queue_size: usize,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(queue_size);
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
        if let Err(err) = self.parser_tx.flush().await {
            error!("Failed to send flush message to parser: {}", err)
        }
        total_drained
    }

    async fn drain_stream(&mut self, buf: &mut LinesBuffer) -> usize {
        //let buf = stream.get_buffer();
        let msgs = buf.read_messages_from_buf();
        let ret = msgs.len();
        if let Err(err) = self.parser_tx.send(msgs).await {
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
                    if let Err(err) = self.parser_tx.send(msgs).await {
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
                    if let Err(err) = self.parser_tx.shutdown().await {
                        error!("Faailed to send shutdown message to parser: {:?}", err)
                    };
                    break;
                }
            }
        }
    }

    pub fn consume_udp_data_queue_async(mut self) {
        tokio::spawn(async move {
            info!("Consuming Udp Server State messages queue ...");
            self.consume_queue().await;
            info!("Done consuming Udp Server State messages queue.");
        });
    }

    pub fn clone_sender(&self) -> MessageSender<UdpData> {
        MessageSender::new(self.tx.clone())
    }

    pub async fn udp_server_main(
        raw_sender: MessageSender<Vec<RawMessage>>,
        hcrc: Arc<HustlogConfig>,
        host_port: &String,
    ) -> Result<(), DynError> {
        let socket = UdpSocket::bind(host_port).await?;
        info!(
            "Starting Hustlog UDP server listening on {} with config: {:?}",
            &host_port, hcrc
        );
        let mut buf = vec![0; 64 * 1024]; //max UDP packet is 64K
        let server_state =
            UdpServerState::new(
                raw_sender,
                hcrc.get_idle_timeout(),
                hcrc.merge_multi_line(),
                hcrc.get_channel_size(),
            );
        let udp_data_sender = server_state.clone_sender();
        server_state.consume_udp_data_queue_async();

        let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));
        loop {
            let udp_data_sender = udp_data_sender.clone_sender();
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("SIGTERM received, flushing buffers ...");
                    udp_data_sender.shutdown().await?; //this does flush internally
                    break
                }
                _tick = intvl.tick() => {
                    if log_enabled!(Level::Trace) {
                        trace!("TICK");
                    }
                    udp_data_sender.flush().await?;
                }
                res = socket.recv_from(&mut buf) => {
                    match res {
                        Ok(ok_res) => {
                            let (rcvd, rcvd_from) = ok_res;
                            let data = Vec::from(&buf[0..rcvd]);
                            for x in &mut buf[0..rcvd] {
                                *x = 0
                            }
                            let rcvd_from = rcvd_from.to_string();
                            udp_data_sender.send(UdpData::new(Arc::from(rcvd_from.as_str()), data)).await?;
                        },
                        Err(err_res) => {
                            error!("socket.recv_from returned error: {:?}", err_res);
                            return Err(Box::new(err_res))
                        }
                    }

                }
            }
        }
        Ok(())
    }
}
