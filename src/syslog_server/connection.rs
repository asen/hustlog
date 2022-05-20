use crate::{LineMerger, RawMessage, SpaceLineMerger};
use bytes::{Buf, BytesMut};
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

pub struct ServerConnection {
    socket: TcpStream,
    remote_addr: Arc<str>,
    buffer: BytesMut,
    line_merger: Option<SpaceLineMerger>,
}

impl ServerConnection {
    pub fn new(socket: TcpStream, remote_addr: &Arc<str>, use_line_merger: bool) -> Self {
        let line_merger = if use_line_merger {
            Some(SpaceLineMerger::new())
        } else {
            None
        };
        Self {
            socket,
            remote_addr: Arc::clone(remote_addr),
            buffer: BytesMut::with_capacity(64 * 1024),
            line_merger,
        }
    }

    pub async fn receive_messsage(&mut self) -> Result<Option<RawMessage>, ConnectionError> {
        loop {
            if let Some(msg) = self.read_message_from_buf() {
                return Ok(Some(msg));
            }
            let bytes_read = self.socket.read_buf(&mut self.buffer).await?;
            //println!("DEBUG: bytes_read={}", bytes_read);
            if 0 == bytes_read {
                if self.line_merger.is_some() {
                    let buffered = self.line_merger.as_mut().unwrap().flush();
                    if buffered.is_some() {
                        return Ok(buffered);
                    } // else nothing left in line merger
                }
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no incomplete lines in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending an incomplete line.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    let err_msg = format!("connection reset by peer: {:?}", self.remote_addr);
                    return Err(ConnectionError::new(err_msg));
                }
            }
        }
    }

    // drop leading \r or \n s in buffer
    fn drop_leading_newlines(&mut self) {
        loop {
            let f = self.buffer.first();
            if f.is_none() {
                break;
            }
            let f = f.unwrap();
            if *f == '\r' as u8 || *f == '\n' as u8 {
                self.buffer.advance(1)
            } else {
                break;
            }
        }
    }

    fn read_line_from_buf(&mut self) -> Option<String> {
        let pos_of_nl = self
            .buffer
            .iter()
            .position(|x| *x == '\r' as u8 || *x == '\n' as u8);
        if pos_of_nl.is_none() {
            None
        } else {
            let line = self.buffer.split_to(pos_of_nl.unwrap());
            let utf8_str = String::from_utf8_lossy(line.as_ref()).to_string();
            self.drop_leading_newlines();
            Some(utf8_str)
        }
    }

    fn read_message_from_buf(&mut self) -> Option<RawMessage> {
        let has_line_meger = self.line_merger.is_some();
        if has_line_meger {
            let mut ret: Option<RawMessage> = None;
            while let Some(line) = self.read_line_from_buf() {
                let lm = self.line_merger.as_mut().unwrap();
                ret = lm.add_line(line);
                if ret.is_some() {
                    break;
                }
            }
            ret
        } else {
            self.read_line_from_buf().map(|ln| RawMessage::new(ln))
        }
    }
}
