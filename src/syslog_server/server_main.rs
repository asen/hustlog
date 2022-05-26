use crate::syslog_server::batch_processor::{BatchProcessor, DummyBatchProcessor};
use crate::syslog_server::batching_queue::MessageQueue;
use crate::syslog_server::batching_queue::{BatchingQueue, MessageSender};
use crate::syslog_server::connection::{ConnectionError, TcpServerConnection};
use crate::syslog_server::server_parser::ServerParser;
use crate::syslog_server::sql_batch_processor::SqlBatchProcessor;
use crate::{GrokParser, HustlogConfig, RawMessage};
use log::{debug, error, info, log_enabled, trace, Level};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::signal;
use tokio::time::interval;

async fn process_socket(
    socket: TcpStream,
    remote_addr: &Arc<str>,
    hc: Arc<HustlogConfig>,
    server_parser: Arc<ServerParser>,
    sender: MessageSender,
) -> Result<(), Box<dyn Error>> {
    let mut conn = TcpServerConnection::new(socket, remote_addr, hc.merge_multi_line());
    while let Some(msg) = conn.receive_messsage().await? {
        if log_enabled!(Level::Trace) {
            trace!("RECEIVED MESSAGE: {:?}", msg)
        }
        let pr = server_parser.parse_raw(msg).await;
        match pr {
            Ok(parsed) => {
                sender.send(parsed)?;
            }
            Err(err) => {
                // TODO add send_error to MessageSender ?
                error!("Error parsing message: {}", err);
            }
        }
    }
    Ok(())
}

fn process_connection_async(
    socket: TcpStream,
    remote_addr_str: String,
    hc: Arc<HustlogConfig>,
    server_parser: Arc<ServerParser>,
    sender: MessageSender,
) {
    tokio::spawn(async move {
        let remote_addr = Arc::from(remote_addr_str.as_str());
        let conn_result = process_socket(socket, &remote_addr, hc, server_parser, sender).await;
        if let Err(err) = conn_result {
            error!("Connection from {} resulted in error: {}", remote_addr, err);
        } else {
            debug!("Connection from {} closed", remote_addr)
        }
    });
}

fn consume_batching_queue_async(mut batching_queue: BatchingQueue) {
    tokio::spawn(async move {
        info!("Consuming parsed messages queue ...");
        batching_queue.consume_queue().await;
        info!("Done consuming parsed messages queue.");
    });
}

fn create_batch_processor(hcrc: &Arc<HustlogConfig>) -> Result<(MessageSender, Arc<ServerParser>),Box<dyn Error>> {
    let schema = hcrc.get_grok_schema();
    let grok_parser = GrokParser::new(schema.clone())?;
    let server_parser = Arc::new(ServerParser::new(Arc::new(grok_parser)));
    // TODO use proper processor, depending on "output" config
    //let batch_processor: Arc<Mutex<dyn BatchProcessor + Send>> = Arc::new(Mutex::new(DummyBatchProcessor{}));
    let batch_processor: Arc<dyn BatchProcessor + Send + Sync> = if hcrc.query().is_some() {
        Arc::new(SqlBatchProcessor::new(
            hcrc.query().as_ref().unwrap().as_str(),
            schema,
        )?)
    } else {
        Arc::new(DummyBatchProcessor {})
    };
    let batching_queue = BatchingQueue::new(hcrc.output_batch_size(), batch_processor);
    let sender = batching_queue.clone_sender();
    consume_batching_queue_async(batching_queue);
    Ok((sender, server_parser))
}

async fn tcp_server_main(hcrc: Arc<HustlogConfig>, host_port: &String) -> Result<(), Box<dyn Error>> {
    let (sender, server_parser) = create_batch_processor(&hcrc)?;
    let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));

    let listener = TcpListener::bind(&host_port).await?;
    info!(
        "Starting Hustlog TCP server listening on {} with config: {:?}",
        &host_port, hcrc
    );
    let hcrc = Arc::clone(&hcrc);
    loop {
        // accept connections or process events, in a loop
        let sender = sender.clone();
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("SIGTERM received, flushing buffers ...");
                sender.shutdown()?; //this does flush internally
                break
            }
            _tick = intvl.tick() => {
                if log_enabled!(Level::Trace) {
                    trace!("TICK");
                }
                sender.flush()?;
            }
            accept_res = listener.accept() => {
                let (socket, remote_addr) = accept_res?;
                let remote_addr_str: String = remote_addr.to_string();
                info!("Accepted connection from {}", remote_addr_str.as_str());
                process_connection_async(socket,
                    remote_addr_str,
                    Arc::clone(&hcrc),
                    Arc::clone(&server_parser),
                    sender,
                );
            }
        }
    }
    Ok(())
}

async fn udp_server_main(hcrc: Arc<HustlogConfig>, host_port: &String) -> Result<(), Box<dyn Error>> {
    let (sender, server_parser) = create_batch_processor(&hcrc)?;
    let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));

    let socket = UdpSocket::bind(host_port).await?;
    info!(
        "Starting Hustlog UDP server listening on {} with config: {:?}",
        &host_port, hcrc
    );
    let mut buf = vec![0; 65535];
    loop {
        let sender = sender.clone();
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("SIGTERM received, flushing buffers ...");
                sender.shutdown()?; //this does flush internally
                break
            }
            _tick = intvl.tick() => {
                if log_enabled!(Level::Trace) {
                    trace!("TICK");
                }
                sender.flush()?;
            }
            res = socket.recv_from(&mut buf) => {
                match res {
                    Ok(ok_res) => {
                        let (rcvd, _rcvd_from) = ok_res;
                        //let remote_addr_str: String = rcvd_from.to_string();
                        // TODO move from_utf8_lossy to rayon?
                        let utf8_str = String::from_utf8_lossy(&buf[0..rcvd]).to_string();
                        for x in &mut buf[0..rcvd] {
                            *x = 0
                        }
                        //info!("Accepted message from {}", remote_addr_str.as_str());
                        //info!("MESSAGE: size={} {}", rcvd, utf8_str);
                        let async_res = server_parser.parse_raw(RawMessage::new(utf8_str)).await;
                        match async_res {
                            Ok(parsed) => sender.send(parsed)?,
                            Err(parse_error) => {
                                error!("PARSE ERROR: {} RAW: {}", parse_error.get_desc(), parse_error.get_raw().as_str());
                            }
                        }
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

pub async fn server_main(hc: &HustlogConfig) -> Result<(), Box<dyn Error>> {
    let sc = match hc.get_syslog_server_config() {
        Ok(x) => Ok(x),
        Err(ce) => Err(Box::new(ce)),
    }?;
    let host_port = sc.get_host_port();
    let hcrc = Arc::new(hc.clone());
    hcrc.init_rayon_pool()?;
    match sc.proto.as_str() {
        "tcp" => tcp_server_main(hcrc, &host_port).await?,
        "udp" => udp_server_main(hcrc, &host_port).await?,
        x => return Err(Box::new(ConnectionError::new(
            format!("Invalid protocol (only udp and tcp are currently supported): {}", x).into())))
    }
    info!("Server shut down");
    Ok(())
}
