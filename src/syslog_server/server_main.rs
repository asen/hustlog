use crate::syslog_server::connection::ServerConnection;
use crate::syslog_server::server_parser::ServerParser;
use crate::{GrokParser, HustlogConfig};
use log::{debug, error, info, log_enabled, trace, Level};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::time::interval;
use crate::syslog_server::batching_queue::{BatchingQueue, MessageSender};

async fn process_socket(
    socket: TcpStream,
    remote_addr: &Arc<str>,
    hc: Arc<HustlogConfig>,
    server_parser: Arc<ServerParser>,
    sender: MessageSender,
) -> Result<(), Box<dyn Error>> {
    let mut conn = ServerConnection::new(socket, remote_addr, hc.merge_multi_line());
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
            error!(
                "Connection from {} resulted in error: {}",
                remote_addr, err
            );
        } else {
            debug!("Connection from {} closed", remote_addr)
        }
    });
}

fn consume_queue_async(mut batching_queue: BatchingQueue) {
    tokio::spawn(async move {
        info!("Consuming parsed messages queue ...");
        batching_queue.consume_queue().await;
        info!("Done consuming parsed messages queue.");
    });
}

pub async fn server_main(hc: &HustlogConfig) -> Result<(), Box<dyn Error>> {
    let sc = match hc.get_syslog_server_config() {
        Ok(x) => Ok(x),
        Err(ce) => Err(Box::new(ce)),
    }?;
    // TODO support udp
    let host_port = sc.get_host_port();
    let listener = TcpListener::bind(&host_port).await?;
    hc.init_rayon_pool()?;
    let hcrc = Arc::new(hc.clone());
    info!(
        "Starting Hustlog server listening on {} with config: {:?}",
        &host_port, hcrc
    );
    let schema = hcrc.get_grok_schema();
    let grok_parser = GrokParser::new(schema.clone())?;
    let server_parser = Arc::new(ServerParser::new(Arc::new(grok_parser)));
    let batching_queue = BatchingQueue::new(hcrc.output_batch_size()); // TODO
    let sender = batching_queue.get_sender();
    let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));
    consume_queue_async(batching_queue);
    loop {
        // accept connections or process events, in a loop
        let sender = sender.clone();
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("SIGTERM received, flushing buffers ...");
                sender.shutdown()?;
                break
            }
            _tick = intvl.tick() => {
                //debug!("TICK");
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
    info!("Server shut down");
    Ok(())
}
