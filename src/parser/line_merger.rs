use crate::RawMessage;

pub trait LineMerger {
    fn add_line(&mut self, line: String) -> Option<RawMessage>;
    fn flush(&mut self) -> Option<RawMessage>;
}

pub struct SpaceLineMerger {
    buf: Vec<String>,
    join_str: String,
}

impl SpaceLineMerger {
    pub fn new() -> SpaceLineMerger {
        // TODO configure capcity?
        // TODO: make join str configurable
        Self {
            buf: Vec::with_capacity(10),
            join_str: " ".to_string(),
        }
    }
}

impl LineMerger for SpaceLineMerger {
    fn add_line(&mut self, line: String) -> Option<RawMessage> {
        if self.buf.is_empty() {
            self.buf.push(line);
            return None;
        }
        if line.starts_with(" ") || line.starts_with("\t") {
            // line continuation
            self.buf.push(line);
            return None;
        }
        let ret = Some(RawMessage::new(self.buf.join(&self.join_str)));
        self.buf.clear();
        self.buf.push(line);
        ret
    }

    fn flush(&mut self) -> Option<RawMessage> {
        if self.buf.is_empty() {
            None
        } else {
            let ret = Some(RawMessage::new(self.buf.join(&self.join_str)));
            self.buf.clear();
            ret
        }
    }
}
