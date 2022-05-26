use crate::{GrokParser, LogParseError, LogParser, ParsedMessage, RawMessage};
use std::sync::Arc;

pub struct ServerParser {
    log_parser: Arc<GrokParser>,
}

impl ServerParser {
    pub fn new(log_parser: Arc<GrokParser>) -> Self {
        Self { log_parser }
    }

    // pub async fn parse_raw(&self, raw: RawMessage) -> Result<ParsedMessage, LogParseError> {
    //     let parser_ref = Arc::clone(&self.log_parser);
    //     tokio_rayon::spawn_fifo(move || parser_ref.parse(raw)).await
    // }

    pub async fn parse_batch(
        &self,
        raw_vec: Vec<RawMessage>,
    ) -> Vec<Result<ParsedMessage, LogParseError>> {
        let parser_ref = Arc::clone(&self.log_parser);
        tokio_rayon::spawn_fifo(move || {
            let mut ret = Vec::with_capacity(raw_vec.len());
            for raw in raw_vec {
                ret.push(parser_ref.parse(raw))
            }
            ret
        })
        .await
    }
}
