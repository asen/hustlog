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

#[cfg(test)]
mod test {
    use bytes::{BufMut, BytesMut};
    use crate::async_pipeline::LinesBuffer;

    const TEST_TEXT: &'static str = r#"<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.authd" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.eventmonitor" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.mail" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.performance" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.iokit.power" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.contacts.ContactsAutocomplete" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.mkb" sharing output destination "/private/var/log/keybagd.log" with ASL Module "com.apple.mkb.internal".
	Output parameters from ASL Module "com.apple.mkb.internal" override any specified in ASL Module "com.apple.mkb".
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.mkb" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:30:05 actek-mac syslogd[106]: Configuration Notice:
	ASL Module "com.apple.MessageTracer" claims selected messages.
	Those messages may not appear in standard system log files or in the ASL database.
<191>May 25 00:34:32 actek-mac syslogd[106]: ASL Sender Statistics
<191>May 25 00:46:43 actek-mac syslogd[106]: ASL Sender Statistics
<191>May 25 01:01:44 actek-mac syslogd[106]: ASL Sender Statistics
<191>May 25 01:20:27 actek-mac syslogd[106]: ASL Sender Statistics
<191>May 25 01:36:32 actek-mac syslogd[106]: ASL Sender Statistics"#;

    fn fill_buf(buf: &mut BytesMut, add_nl: bool) {
        buf.put_slice(TEST_TEXT.as_bytes());
        if add_nl {
            buf.put("\n".as_bytes());
        }
    }


    #[test]
    fn test_line_buffer_with_lm1() {
        let mut lb = LinesBuffer::new(true);
        fill_buf(lb.get_buf(), false);
        let mut lines = lb.read_messages_from_buf();
        let mut flush_lines = lb.flush();
        lines.append(&mut flush_lines);
        assert_eq!(14, lines.len());
        // println!("LINES_COUNT: {}", lines.len());
        // for ln in lines {
        //     println!("LINE: {}", ln.as_str())
        // }
    }

    #[test]
    fn test_line_buffer_with_lm2() {
        let mut lb = LinesBuffer::new(true);
        for _ in 0..64 {
            fill_buf(lb.get_buf(), true);
        }
        let mut lines = lb.read_messages_from_buf();
        let mut flush_lines = lb.flush();
        lines.append(&mut flush_lines);
        assert_eq!(14 * 64, lines.len());
        // println!("LINES_COUNT: {}", lines.len());
        // for ln in lines {
        //     println!("LINE: {}", ln.as_str())
        // }
    }


    #[test]
    fn test_line_buffer_no_lm1() {
        let mut lb = LinesBuffer::new(false);
        fill_buf(lb.get_buf(), false);
        let mut lines = lb.read_messages_from_buf();
        let mut flush_lines = lb.flush();
        lines.append(&mut flush_lines);
        assert_eq!(32, lines.len());
        // println!("LINES_COUNT: {}", lines.len());
        // for ln in lines {
        //     println!("LINE: {}", ln.as_str())
        // }
    }

    #[test]
    fn test_line_buffer_no_lm2() {
        let mut lb = LinesBuffer::new(false);
        for _ in 0..64 {
            fill_buf(lb.get_buf(), true);
        }
        let mut lines = lb.read_messages_from_buf();
        let mut flush_lines = lb.flush();
        lines.append(&mut flush_lines);
        assert_eq!(32 * 64, lines.len());
        //println!("buf.capacity: {}", lb.get_buf().capacity());
        // println!("LINES_COUNT: {}", lines.len());
        // for ln in lines {
        //     println!("LINE: {}", ln.as_str())
        // }
    }

    #[test]
    fn test_line_buffer_no_lm3() {
        let mut lb = LinesBuffer::new(false);
        let mut lines = lb.read_messages_from_buf();
        for _ in 0..64 {
            fill_buf(lb.get_buf(), true);
            let mut ret_lines = lb.read_messages_from_buf();
            lines.append(&mut ret_lines)
        }
        let mut flush_lines = lb.flush();
        lines.append(&mut flush_lines);
        assert_eq!(32 * 64, lines.len());
        //println!("buf.capacity: {}", lb.get_buf().capacity());
        // println!("LINES_COUNT: {}", lines.len());
        // for ln in lines {
        //     println!("LINE: {}", ln.as_str())
        // }
    }

}
