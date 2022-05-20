use crate::syslog_server::connection::ServerConnection;
use crate::{GrokParser, HustlogConfig, LogParser};
use std::error::Error;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use log::{error, log_enabled, Level, trace, debug, info};

async fn process_socket(
    socket: TcpStream,
    remote_addr: &Arc<str>,
    hc: Arc<HustlogConfig>,
) -> Result<(), Box<dyn Error>> {
    let mut conn = ServerConnection::new(socket, remote_addr, hc.merge_multi_line());
    let parser = GrokParser::new(hc.get_grok_schema().clone())?;
    while let Some(msg) = conn.receive_messsage().await? {
        if log_enabled!(Level::Trace) {
            trace!("RECEIVED MESSAGE: {:?}", msg)
        }
        // TODO send to parser thread pool
        let pr = parser.parse(msg);
        match pr {
            Ok(parsed) => {
                debug!("MESSAGE: {:?}", parsed)
            }
            Err(err) => {
                error!("PARSE ERROR: {:?}", err)
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
    let host_port = format!("{}:{}", &sc.listen_host, &sc.port);
    let listener = TcpListener::bind(host_port).await?;
    let hcrc = Arc::new(hc.clone());
    info!("Starting Hustlog server with config: {:?}", hcrc);
    loop {
        // The second item contains the IP and port of the new connection.
        let (socket, remote_addr) = listener.accept().await?;
        let hc_arc = Arc::clone(&hcrc);
        tokio::spawn(async move {
            let remote_conn_info = Arc::from(remote_addr.to_string().as_str());
            let conn_result = process_socket(socket, &remote_conn_info, hc_arc).await;
            if let Err(err) = conn_result {
                error!("Connection resulted in error {}", err);
            } else {
                debug!("Connection from {} closed", remote_conn_info)
            }
        });
    }
    //Ok(())
}
