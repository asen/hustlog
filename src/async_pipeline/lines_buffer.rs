use bytes::{Buf, BytesMut};
use crate::parser::{LineMerger, RawMessage, SpaceLineMerger};
use bstr::ByteSlice;

const LINE_ENDING_CHARS: [u8; 2] = ['\n' as u8, '\r' as u8];

const DECIMAL_DIGIT_CHARS: [u8; 10] = [
    '0' as u8, '1' as u8, '2' as u8, '3' as u8, '4' as u8,
    '5' as u8, '6' as u8, '7' as u8, '8' as u8, '9' as u8,
];

const SYSLOG_PRI_OPEN_TAG: u8 = '<' as u8;
const SYSLOG_PRI_CLOSE_TAG: u8 = '>' as u8;

pub struct LinesBuffer {
    buf: BytesMut,
    line_merger: Option<SpaceLineMerger>,
}

impl LinesBuffer {
    pub fn new(
        //capacity: usize,
        use_line_merger: bool
    ) -> Self {
        let line_merger = if use_line_merger {
            Some(SpaceLineMerger::new())
        } else {
            None
        };
        Self {
            buf: BytesMut::with_capacity(
                //capacity,
                64 * 1024, //TODO make configurable?
            ),
            line_merger,
        }
    }

    // drop leading \r or \n s in buffer
    fn drop_leading_newlines(&mut self) {
        loop {
            let f = self.buf.first();
            if f.is_none() {
                break;
            }
            if LINE_ENDING_CHARS.contains(f.unwrap()) {
                self.buf.advance(1)
            } else {
                break;
            }
        }
    }

    fn drop_syslog_priority(&mut self) {
        let first_c = self.buf.first();
        if let Some(&SYSLOG_PRI_OPEN_TAG) = first_c  {
            let mut iter = self.buf.iter();
            let mut to_advance = 1;
            let _fc = iter.next().unwrap(); // skip the '<'
            while let Some(c) = iter.next() {
                if DECIMAL_DIGIT_CHARS.contains(c) {
                    to_advance += 1;
                } else if SYSLOG_PRI_CLOSE_TAG == *c {
                    self.buf.advance(to_advance + 1);
                    break;
                } else {
                    break; // not a digit, nor a close tag - not a priority prefix
                }
            }
        }
    }

    fn read_line_from_buf(&mut self) -> Option<String> {
        self.drop_syslog_priority(); // TODO make this call optional?
        let pos_of_nl = self.buf.find_byteset(LINE_ENDING_CHARS);
        if pos_of_nl.is_none() {
            None
        } else {
            let line = self.buf.split_to(pos_of_nl.unwrap());
            let utf8_str = String::from_utf8_lossy(line.as_ref()).to_string();
            self.drop_leading_newlines();
            Some(utf8_str)
        }
    }

    pub fn read_message_from_buf(&mut self) -> Option<RawMessage> {
        let has_line_merger = self.line_merger.is_some();
        if has_line_merger {
            let mut ret: Option<RawMessage> = None;
            while let Some(line) = self.read_line_from_buf() {
                let lm = self.line_merger.as_mut().unwrap();
                ret = lm.add_line(line);
                if ret.is_some() {
                    break;
                }
            }
            ret
        } else {
            self.read_line_from_buf().map(|ln| RawMessage::new(ln))
        }
    }

    pub fn read_messages_from_buf(&mut self) -> Vec<RawMessage> {
        let has_line_meger = self.line_merger.is_some();
        let mut ret = Vec::new();
        if has_line_meger {
            while let Some(line) = self.read_line_from_buf() {
                let lm = self.line_merger.as_mut().unwrap();
                let line_ret = lm.add_line(line);
                if line_ret.is_some() {
                    ret.push(line_ret.unwrap());
                }
            }
        } else {
            while let Some(ln) = self.read_line_from_buf() {
                ret.push(RawMessage::new(ln));
            }
        }
        ret
    }

    pub fn get_buf(&mut self) -> &mut BytesMut {
        &mut self.buf
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn flush(&mut self) -> Vec<RawMessage> {
        let mut ret = Vec::new();
        while let Some(msg) = self.read_message_from_buf() {
            ret.push(msg)
        }
        let last_line = if self.buf.is_empty() {
            None
        } else {
            Some(String::from_utf8_lossy(self.buf.as_ref()).to_string())
        };
        if self.line_merger.is_some() {
            let lm = self.line_merger.as_mut().unwrap();
            if last_line.is_some() {
                if let Some(msg) = lm.add_line(last_line.unwrap()) {
                    ret.push(msg)
                }
            }
            if let Some(last_msg) = self.line_merger.as_mut().unwrap().flush() {
                ret.push(last_msg)
            }
        } else {
            if last_line.is_some() {
                ret.push(RawMessage::new(last_line.unwrap()))
            }
        }
        ret
    }
}
