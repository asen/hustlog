use std::io::BufRead;
use std::sync::Arc;
use log::error;
use crate::{DynError, HustlogConfig};
use crate::async_pipeline::create_processing_pipeline;
use crate::async_pipeline::message_queue::MessageSender;
use crate::parser::{LineMerger, RawMessage, SpaceLineMerger};

async fn process_input(hcrc: Arc<HustlogConfig>, raw_sender: MessageSender<Vec<RawMessage>>) -> Result<(),DynError> {
    let buf_read = hcrc.get_buf_read()?;
    let mut line_merger = if hcrc.merge_multi_line() {
        Some(SpaceLineMerger::new())
    } else {
        None
    };
    for line in buf_read.lines() {
        let line = line?;
        let raw_msg = if line_merger.is_some() {
            line_merger.as_mut().unwrap().add_line(line)
        } else {
            Some(RawMessage::new(line))
        };
        if raw_msg.is_some() {
            raw_sender.send(vec![raw_msg.unwrap()]).await?
        }
    }
    if line_merger.is_some() {
        let raw_msg = line_merger.unwrap().flush();
        if raw_msg.is_some() {
            raw_sender.send(vec![raw_msg.unwrap()]).await?
        }
    }
    Ok(())
}


pub async fn file_process_main(hc: &HustlogConfig) -> Result<(), DynError> {
    let hcrc = Arc::new(hc.clone());
    let (raw_sender, join_handles) = create_processing_pipeline(&hcrc)?;
    let cloned_sender = raw_sender.clone_sender();
    let main_res = tokio_rayon::spawn_fifo(move || async {
        process_input(hcrc, cloned_sender).await
    }).await.await;//.join();
    let mut err: Option<DynError> = None;
    if let Err(e) = main_res {
        error!("Error from the input thread: {:?}", e);
        err = Some(e);
    }
    raw_sender.shutdown().await?;
    for jh in join_handles {
        jh.join().await;
    }
    if err.is_some() {
        Err(err.unwrap())
    } else {
        Ok(())
    }
}
