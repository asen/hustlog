use crate::async_pipeline::lines_buffer::LinesBuffer;
use crate::async_pipeline::message_queue::MessageSender;
use crate::parser::RawMessage;
use crate::{DynError, HustlogConfig};
use log::{debug, error, info, log_enabled, trace, Level};
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::time::interval;

#[derive(Debug)]
pub struct ConnectionError {
    desc: String,
}

impl ConnectionError {
    pub fn new(desc: String) -> Self {
        Self { desc: desc }
    }

    pub fn get_desc(&self) -> &String {
        &self.desc
    }
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Connection error: {}", self.get_desc(),)
    }
}

impl Error for ConnectionError {}

impl From<std::io::Error> for ConnectionError {
    fn from(io_err: std::io::Error) -> Self {
        ConnectionError::new(io_err.to_string())
    }
}

pub struct TcpServerConnection {
    raw_sender: MessageSender<Vec<RawMessage>>,
    socket: TcpStream,
    remote_addr: String,
    buffer: LinesBuffer,
    is_closed: bool,
    is_error: bool,
}

impl TcpServerConnection {
    pub fn new(
        raw_sender: MessageSender<Vec<RawMessage>>,
        socket: TcpStream,
        remote_addr: String,
        use_line_merger: bool,
    ) -> Self {
        Self {
            raw_sender,
            socket,
            remote_addr: remote_addr,
            buffer: LinesBuffer::new(use_line_merger),
            is_closed: false,
            is_error: false,
        }
    }

    async fn receive_messsages(&mut self) -> Result<Vec<RawMessage>, ConnectionError> {
        loop {
            if self.is_closed {
                if self.is_error {
                    return Err(ConnectionError::new(format!(
                        "Connection reset by peer: {}",
                        self.remote_addr
                    )));
                } else {
                    return Ok(Vec::new());
                }
            }
            let msgs = self.buffer.read_messages_from_buf();
            if !msgs.is_empty() {
                return Ok(msgs);
            }
            let bytes_read = self.socket.read_buf(&mut self.buffer.get_buf()).await?;
            if bytes_read == 0 {
                //connection closed
                self.is_error = !self.buffer.is_empty();
                self.is_closed = true;
                let msgs = self.buffer.flush();
                return Ok(msgs);
            }
        }
    }

    pub async fn process_socket(&mut self) -> Result<(), DynError> {
        loop {
            let batch = self.receive_messsages().await?;
            if batch.is_empty() {
                break;
            }
            if log_enabled!(Level::Trace) {
                trace!(
                    "RECEIVED MESSAGES BATCH: ({}) first={:?}",
                    batch.len(),
                    batch.first()
                )
            }
            self.raw_sender.send(batch).await?
        }
        Ok(())
    }

    pub fn process_connection_async(
        raw_sender: MessageSender<Vec<RawMessage>>,
        socket: TcpStream,
        remote_addr: String,
        merge_multi_line: bool,
    ) {
        tokio::spawn(async move {
            let mut conn =
                TcpServerConnection::new(raw_sender, socket, remote_addr, merge_multi_line);
            let conn_result = conn.process_socket().await;
            //process_socket(socket, &remote_addr, hc, sender).await;
            if let Err(err) = conn_result {
                error!(
                    "Connection from {} resulted in error: {}",
                    &conn.remote_addr, err
                );
            } else {
                debug!("Connection from {} closed", &conn.remote_addr)
            }
        });
    }

    pub async fn tcp_server_main(
        raw_sender: MessageSender<Vec<RawMessage>>,
        hcrc: Arc<HustlogConfig>,
        host_port: &String,
    ) -> Result<(), DynError> {
        let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));

        let listener = TcpListener::bind(&host_port).await?;
        info!(
            "Starting Hustlog TCP server listening on {} with config: {:?}",
            &host_port, hcrc
        );
        let hcrc = Arc::clone(&hcrc);
        loop {
            // accept connections or process events, in a loop
            let raw_sender = raw_sender.clone_sender();
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("SIGTERM received, flushing buffers ...");
                    raw_sender.shutdown().await?; //this does flush internally
                    break
                }
                _tick = intvl.tick() => {
                    if log_enabled!(Level::Trace) {
                        trace!("TICK");
                    }
                    raw_sender.flush().await?;
                }
                accept_res = listener.accept() => {
                    let (socket, remote_addr) = accept_res?;
                    let remote_addr_str: String = remote_addr.to_string();
                    info!("Accepted connection from {}", remote_addr_str.as_str());
                    TcpServerConnection::process_connection_async(
                        raw_sender,
                        socket,
                        remote_addr_str,
                        hcrc.merge_multi_line(),
                    );
                }
            }
        }
        Ok(())
    }
}
