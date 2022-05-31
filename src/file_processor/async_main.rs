use std::sync::Arc;
use log::error;
use tokio::io::AsyncReadExt;
use crate::{DynError, HustlogConfig};
use crate::async_pipeline::{create_processing_pipeline, LinesBuffer};
use crate::async_pipeline::message_queue::MessageSender;
use crate::parser::RawMessage;

async fn process_input(hcrc: Arc<HustlogConfig>, raw_sender: MessageSender<Vec<RawMessage>>) -> Result<(),DynError> {
    let mut async_read = hcrc.get_async_read().await?;
    let mut lines_buffer = LinesBuffer::new(hcrc.merge_multi_line());
    loop {
        let read_res = async_read.as_mut().read_buf(lines_buffer.get_buf()).await;
        match read_res {
            Ok(rd) => {
                if rd == 0 { // nothing left to read
                    break
                }
                let msgs = lines_buffer.read_messages_from_buf();
                if let Err(err) = raw_sender.send(msgs).await {
                    error!("Error sending raw message downstream, aborting: {:?}", err);
                    break;
                }
            }
            Err(err) => {
                error!("Error reading from input, aborting: {:?}", err);
                break;
            }
        }
    }
    let msgs = lines_buffer.flush();
    raw_sender.send(msgs).await?;
    raw_sender.shutdown().await?;
    Ok(())
}


pub async fn file_process_main(hc: &HustlogConfig) -> Result<(), DynError> {
    let hcrc = Arc::new(hc.clone());
    let (raw_sender, join_handles) = create_processing_pipeline(&hcrc).await?;
    let process_input_res = process_input(hcrc, raw_sender).await;
    let err = if let Err(e) = process_input_res {
        error!("Error from the input processing: {:?}", e);
        Some(e)
    } else {
        None
    };
    for jh in join_handles {
        jh.join().await;
    }
    if err.is_some() {
        Err(err.unwrap())
    } else {
        Ok(())
    }
}
