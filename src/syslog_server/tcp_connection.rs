use crate::syslog_server::lines_buffer::LinesBuffer;
use crate::RawMessage;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

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
    socket: TcpStream,
    remote_addr: Arc<str>,
    buffer: LinesBuffer,
    is_closed: bool,
    is_error: bool,
}

impl TcpServerConnection {
    pub fn new(socket: TcpStream, remote_addr: &Arc<str>, use_line_merger: bool) -> Self {
        Self {
            socket,
            remote_addr: Arc::clone(remote_addr),
            buffer: LinesBuffer::new(16 * 1024, use_line_merger),
            is_closed: false,
            is_error: false,
        }
    }

    pub async fn receive_messsages(&mut self) -> Result<Vec<RawMessage>, ConnectionError> {
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
}
