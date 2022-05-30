
use log::info;
use std::sync::Arc;
use crate::async_pipeline::create_processing_pipeline;
use crate::{DynError, HustlogConfig};
use crate::syslog_server::tcp_server::{ConnectionError, TcpServerConnection};
use crate::syslog_server::udp_server::UdpServerState;

pub async fn server_main(hc: &HustlogConfig) -> Result<(), DynError> {
    let sc = match hc.get_syslog_server_config() {
        Ok(x) => Ok(x),
        Err(ce) => Err(Box::new(ce)),
    }?;
    let host_port = sc.get_host_port();
    let hcrc = Arc::new(hc.clone());
    hcrc.init_rayon_pool()?;
    let (raw_sender, join_handles) = create_processing_pipeline(&hcrc)?;
    match sc.proto.as_str() {
        "tcp" => TcpServerConnection::tcp_server_main(raw_sender, hcrc, &host_port).await?,
        "udp" => UdpServerState::udp_server_main(raw_sender, hcrc, &host_port).await?,
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
    for jh in join_handles {
        jh.join().await;
    }
    info!("Server shut down");
    Ok(())
}
