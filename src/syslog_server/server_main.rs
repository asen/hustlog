use crate::syslog_server::batch_processor::{BatchProcessor, DummyBatchProcessor};
use crate::syslog_server::batching_queue::BatchingQueue;
use crate::syslog_server::message_queue::{MessageQueue, MessageSender};
use crate::syslog_server::server_parser::ServerParser;
use crate::syslog_server::sql_batch_processor::SqlBatchProcessor;
use crate::syslog_server::tcp_connection::{ConnectionError, TcpServerConnection};
use crate::syslog_server::udp_stream::{UdpData, UdpServerState};
use crate::{GrokParser, HustlogConfig, ParsedMessage};
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
    sender: MessageSender<ParsedMessage>,
) -> Result<(), Box<dyn Error>> {
    let mut conn = TcpServerConnection::new(socket, remote_addr, hc.merge_multi_line());
    loop {
        let batch = conn.receive_messsages().await?;
        if batch.is_empty() {
            break;
        }
        if log_enabled!(Level::Trace) {
            trace!("RECEIVED MESSAGES BATCH: {:?}", batch)
        }
        let parse_result = server_parser.parse_batch(batch).await;
        for pr in parse_result {
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
    }
    Ok(())
}

fn process_connection_async(
    socket: TcpStream,
    remote_addr_str: String,
    hc: Arc<HustlogConfig>,
    server_parser: Arc<ServerParser>,
    sender: MessageSender<ParsedMessage>,
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

fn consume_udp_server_state_queue_async(mut udp_server_state: UdpServerState) {
    tokio::spawn(async move {
        info!("Consuming Udp Server State messages queue ...");
        udp_server_state.consume_queue().await;
        info!("Done consuming Udp Server State messages queue.");
    });
}

fn create_batch_processor(
    hcrc: &Arc<HustlogConfig>,
) -> Result<(MessageSender<ParsedMessage>, Arc<ServerParser>), Box<dyn Error>> {
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

async fn tcp_server_main(
    hcrc: Arc<HustlogConfig>,
    host_port: &String,
) -> Result<(), Box<dyn Error>> {
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

async fn udp_server_main(
    hcrc: Arc<HustlogConfig>,
    host_port: &String,
) -> Result<(), Box<dyn Error>> {
    let (parsed_sender, server_parser) = create_batch_processor(&hcrc)?;
    let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));

    let socket = UdpSocket::bind(host_port).await?;
    info!(
        "Starting Hustlog UDP server listening on {} with config: {:?}",
        &host_port, hcrc
    );
    let mut buf = vec![0; 65535];
    let server_state = UdpServerState::new(
        server_parser,
        parsed_sender,
        hcrc.get_idle_timeout(),
        hcrc.merge_multi_line(),
    );
    let sender = server_state.get_sender();
    consume_udp_server_state_queue_async(server_state);
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
                        let (rcvd, rcvd_from) = ok_res;
                        let data = Vec::from(&buf[0..rcvd]);
                        for x in &mut buf[0..rcvd] {
                            *x = 0
                        }
                        let rcvd_from = rcvd_from.to_string();
                        sender.send(UdpData::new(Arc::from(rcvd_from.as_str()), data))?;
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
        x => {
            return Err(Box::new(ConnectionError::new(
                format!(
                    "Invalid protocol (only udp and tcp are currently supported): {}",
                    x
                )
                .into(),
            )))
        }
    }
    info!("Server shut down");
    Ok(())
}
