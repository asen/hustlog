use crate::syslog_server::connection::ServerConnection;
use crate::syslog_server::message_processor::{MessageBatcher, MessageProcessor};
use crate::syslog_server::server_parser::ServerParser;
use crate::{GrokParser, HustlogConfig};
use log::{debug, error, info, log_enabled, trace, Level};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::time::interval;

async fn process_socket(
    socket: TcpStream,
    remote_addr: &Arc<str>,
    hc: Arc<HustlogConfig>,
    server_parser: Arc<ServerParser>,
    processor: Arc<Mutex<dyn MessageProcessor + Send>>,
) -> Result<(), Box<dyn Error>> {
    let mut conn = ServerConnection::new(socket, remote_addr, hc.merge_multi_line());
    while let Some(msg) = conn.receive_messsage().await? {
        if log_enabled!(Level::Trace) {
            trace!("RECEIVED MESSAGE: {:?}", msg)
        }
        let pr = server_parser.parse_raw(msg).await;
        match pr {
            Ok(parsed) => {
                let process_res = processor.lock().unwrap().process_message(parsed);
                if let Err(process_err) = process_res {
                    error!("Error processing message: {}", process_err)
                }
            }
            Err(err) => {
                let process_res = processor.lock().unwrap().process_error(err);
                if let Err(process_err) = process_res {
                    error!("Error processing parser failure: {}", process_err)
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
    let batch_processor = Arc::new(Mutex::new(MessageBatcher::new(hcrc.output_batch_size()))); // TODO
    let mut intvl = interval(Duration::from_secs(hcrc.get_tick_interval()));
    loop {
        // accept connections or process tick events, in a loop
        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("SIGTERM received, flusing buffers ...");
                batch_processor.lock().unwrap().flush()?;
                break
            }
            _tick = intvl.tick() => {
                debug!("TICK");
                batch_processor.lock().unwrap().flush()?;
            }
            accept_res = listener.accept() => {
                let (socket, remote_addr) = accept_res?;
                let remote_addr_str: String = remote_addr.to_string();
                info!("Accepted connection from {}", remote_addr_str.as_str());
                let hc_arc = Arc::clone(&hcrc);
                let sp_arc = Arc::clone(&server_parser);
                let mb_arc = Arc::clone(&batch_processor);
                tokio::spawn(async move {
                    let remote_conn_info = Arc::from(remote_addr_str.as_str());
                    let conn_result =
                        process_socket(socket, &remote_conn_info, hc_arc, sp_arc, mb_arc).await;
                    if let Err(err) = conn_result {
                        error!(
                            "Connection from {} resulted in error: {}",
                            remote_conn_info, err
                        );
                    } else {
                        debug!("Connection from {} closed", remote_conn_info)
                    }
                });
            }
        }
    }
    info!("Server shut down");
    Ok(())
}
