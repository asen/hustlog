// Copyright 2022 Asen Lazarov

use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::rc::Rc;

use chrono::Datelike;
use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Offset, TimeZone};

#[derive(Debug)]
pub struct RawMessage(String);

impl RawMessage {
    pub fn new(s: String) -> RawMessage {
        RawMessage(s)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug)]
pub struct LogParseError {
    desc: String,
    raw_msg: RawMessage,
}

impl LogParseError {
    pub fn new(desc: &str, raw: RawMessage) -> LogParseError {
        LogParseError {
            desc: desc.to_string(),
            raw_msg: raw,
        }
    }

    pub fn get_raw(&self) -> &RawMessage {
        &self.raw_msg
    }

    pub fn get_desc(&self) -> &String {
        &self.desc
    }
}

impl fmt::Display for LogParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Log parse error: {}", self.desc)
    }
}

impl Error for LogParseError {}

#[derive(Debug, Clone)]
pub enum ParsedValue {
    NullVal,
    BoolVal(bool),
    LongVal(i64),
    DoubleVal(f64),
    TimeVal(DateTime<FixedOffset>),
    StrVal(Rc<String>),
}

impl Eq for ParsedValue {}

impl Hash for ParsedValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let ParsedValue::DoubleVal(d) = self {
            if d.is_nan() {
                f64::NAN.to_bits().hash(state)
            } else {
                d.to_bits().hash(state)
            }
        } else {
            self.hash(state)
        }
    }
}

impl PartialEq for ParsedValue {
    fn eq(&self, other: &Self) -> bool {
        match self {
            ParsedValue::NullVal => other == &ParsedValue::NullVal,
            ParsedValue::BoolVal(b) => {
                if let ParsedValue::BoolVal(x) = other {
                    x == b
                } else {
                    false
                }
            }
            ParsedValue::LongVal(l) => {
                if let ParsedValue::LongVal(x) = other {
                    x == l
                } else if let ParsedValue::DoubleVal(x) = other {
                    let lx = *l as f64;
                    x.partial_cmp(&lx) == Some(Ordering::Equal)
                } else {
                    false
                }
            }
            ParsedValue::DoubleVal(d) => {
                if let ParsedValue::DoubleVal(x) = other {
                    x == d
                } else if let ParsedValue::LongVal(x) = other {
                    *x as f64 == *d
                } else {
                    false
                }
            }
            ParsedValue::TimeVal(t) => {
                if let ParsedValue::TimeVal(x) = other {
                    x.timestamp_nanos() == t.timestamp_nanos()
                } else {
                    false
                }
            }
            ParsedValue::StrVal(s) => {
                if let ParsedValue::StrVal(x) = other {
                    s.as_str().eq(x.as_str())
                } else {
                    s.as_str().eq(other.to_rc_str().as_str())
                }
            }
        }
    }
}

impl PartialOrd for ParsedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            ParsedValue::NullVal => {
                match other {
                    ParsedValue::NullVal => Some(Ordering::Equal),
                    _ => Some(Ordering::Less), // NULL is lesser than anything???
                }
            }
            ParsedValue::BoolVal(b) => match other {
                ParsedValue::NullVal => Some(Ordering::Greater),
                ParsedValue::BoolVal(x) => b.partial_cmp(x),
                _ => Some(Ordering::Less),
            },
            ParsedValue::LongVal(l) => match other {
                ParsedValue::NullVal => Some(Ordering::Greater),
                ParsedValue::BoolVal(_) => Some(Ordering::Greater),
                ParsedValue::LongVal(x) => l.partial_cmp(x),
                ParsedValue::DoubleVal(x) => (*l as f64).partial_cmp(x),
                ParsedValue::TimeVal(x) => l.partial_cmp(&x.timestamp_millis()),
                ParsedValue::StrVal(_) => Some(Ordering::Less),
            },
            ParsedValue::DoubleVal(d) => match other {
                ParsedValue::NullVal => Some(Ordering::Greater),
                ParsedValue::BoolVal(_) => Some(Ordering::Greater),
                ParsedValue::LongVal(x) => d.partial_cmp(&(*x as f64)),
                ParsedValue::DoubleVal(x) => d.partial_cmp(x),
                ParsedValue::TimeVal(x) => d.partial_cmp(&(x.timestamp_millis() as f64)),
                ParsedValue::StrVal(_) => Some(Ordering::Less),
            },
            ParsedValue::TimeVal(t) => match other {
                ParsedValue::NullVal => Some(Ordering::Greater),
                ParsedValue::BoolVal(_) => Some(Ordering::Greater),
                ParsedValue::LongVal(x) => t.timestamp_millis().partial_cmp(x),
                ParsedValue::DoubleVal(x) => (t.timestamp_millis() as f64).partial_cmp(x),
                ParsedValue::TimeVal(x) => t.timestamp_nanos().partial_cmp(&x.timestamp_nanos()),
                ParsedValue::StrVal(_) => Some(Ordering::Less),
            },
            ParsedValue::StrVal(s) => {
                match other {
                    ParsedValue::StrVal(x) => s.as_str().partial_cmp(x.as_str()),
                    _ => {
                        // String is greater than all others
                        Some(Ordering::Greater)
                    }
                }
            }
        }
    }
}

impl ParsedValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ParsedValue::NullVal => Some(false),
            ParsedValue::BoolVal(x) => Some(*x),
            ParsedValue::StrVal(_) => None,
            ParsedValue::LongVal(x) => Some(*x != 0),
            ParsedValue::DoubleVal(x) => Some(*x != 0.0),
            ParsedValue::TimeVal(_) => None,
        }
    }

    pub fn to_rc_str(&self) -> Rc<String> {
        match self {
            ParsedValue::NullVal => Rc::new("NULL".to_string()),
            ParsedValue::BoolVal(x) => Rc::new(x.to_string()),
            ParsedValue::StrVal(x) => x.clone(),
            ParsedValue::LongVal(x) => Rc::new(x.to_string()),
            ParsedValue::DoubleVal(x) => Rc::new(x.to_string()),
            ParsedValue::TimeVal(x) => Rc::new(x.to_string()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimeTypeFormat {
    format_specifier: Box<String>,
    needs_year: bool,
    needs_tz: bool,
    local_tz_offset: i32,
}

fn local_timezone_offset() -> i32 {
    Local.timestamp(0, 0).offset().fix().local_minus_utc()
}

// https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html#specifiers
const YEAR_SPECIFIERS: [&str; 11] = [
    "%y", "%Y", "%G", "%g", "%D", "%x", "%f", "%v", "%c", "%+", "%s",
];
const TZ_SPECIFIERS: [&str; 6] = ["%Z", "%z", "%:z", "%#z", "%+", "%s"];

impl TimeTypeFormat {
    pub fn new(fmt: &str) -> TimeTypeFormat {
        let (specifier, need_year) = if YEAR_SPECIFIERS.iter().any(|&ys| fmt.contains(ys)) {
            (fmt.to_string(), false)
        } else {
            (format!("{} %Y", fmt), true)
        };
        let has_tz = TZ_SPECIFIERS.iter().any(|&zs| fmt.contains(zs));
        TimeTypeFormat {
            format_specifier: Box::new(specifier),
            needs_year: need_year,
            needs_tz: !has_tz,
            local_tz_offset: local_timezone_offset(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum ParsedValueType {
    NullType,
    BoolType,
    LongType,
    DoubleType,
    TimeType(TimeTypeFormat), // format specifier
    StrType,
}

// impl ParsedValueType {
//     pub fn clone(&self) -> ParsedValueType {
//         match self {
//             case &ParsedValueType::TimeType(b) => ParsedValueType::TimeType(Box::new(*b.clone()))
//         }
//     }
// }

fn parse_ts(s: &str, fmt: &TimeTypeFormat) -> Option<DateTime<FixedOffset>> {
    let needs_year_str: String;
    let to_parse = if fmt.needs_year {
        let current_date = chrono::Utc::now(); // TODO this is broken
        let year = current_date.year();
        needs_year_str = format!("{} {}", s, year);
        //println!("{} - {}", needs_year_str, &fmt.format_specifier);
        needs_year_str.as_str()
    } else {
        s
    };
    if fmt.needs_tz {
        //println!("{} - {}", fmt.needs_tz, &fmt.local_tz_offset);
        NaiveDateTime::parse_from_str(to_parse, &fmt.format_specifier)
            .ok()
            .and_then(|nd| {
                FixedOffset::east(fmt.local_tz_offset)
                    .from_local_datetime(&nd)
                    .single()
            })
    } else {
        DateTime::parse_from_str(to_parse, &fmt.format_specifier).ok()
    }
}

pub fn str2val(s: &str, ctype: &ParsedValueType) -> Option<ParsedValue> {
    match ctype {
        ParsedValueType::StrType => Some(ParsedValue::StrVal(Rc::new(String::from(s)))),
        ParsedValueType::LongType => s.parse::<i64>().ok().map(|v| ParsedValue::LongVal(v)),
        ParsedValueType::DoubleType => s.parse::<f64>().ok().map(|v| ParsedValue::DoubleVal(v)),
        ParsedValueType::TimeType(fmt) => parse_ts(s, fmt).map(|v| ParsedValue::TimeVal(v)),
        ParsedValueType::NullType => Some(ParsedValue::NullVal),
        ParsedValueType::BoolType => {
            if s.eq_ignore_ascii_case("true") {
                Some(ParsedValue::BoolVal(true))
            } else if s.eq_ignore_ascii_case("false") {
                Some(ParsedValue::BoolVal(false))
            } else {
                None
            }
        }
    }
}

pub fn str2type(s: &str) -> Option<ParsedValueType> {
    match s {
        "str" | "" => Some(ParsedValueType::StrType),
        "int" | "long" => Some(ParsedValueType::LongType),
        "float" | "double" => Some(ParsedValueType::DoubleType),
        "bool" => Some(ParsedValueType::BoolType),
        "null" => Some(ParsedValueType::NullType),
        x => {
            let ts_prefix = "ts:";
            if x.starts_with(ts_prefix) {
                Some(ParsedValueType::TimeType(TimeTypeFormat::new(
                    x.strip_prefix(ts_prefix).unwrap(),
                )))
            } else {
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct ParsedData(HashMap<Rc<str>, ParsedValue>);

impl ParsedData {
    pub fn new(hm: HashMap<Rc<str>, ParsedValue>) -> ParsedData {
        ParsedData(hm)
    }

    pub fn get_value(&self, key: &str) -> Option<&ParsedValue> {
        self.0.get(key)
    }
}

#[derive(Debug)]
pub struct ParsedMessage {
    raw: RawMessage,
    parsed: ParsedData,
}

impl ParsedMessage {
    pub fn new(raw: RawMessage, parsed: ParsedData) -> ParsedMessage {
        ParsedMessage { raw, parsed }
    }
    pub fn get_raw(&self) -> &RawMessage {
        &self.raw
    }
    pub fn get_parsed(&self) -> &ParsedData {
        &self.parsed
    }
}

pub trait LogParser {
    fn parse(&self, msg: RawMessage) -> Result<ParsedMessage, LogParseError>;
}

pub struct ParseErrorProcessor {
    stderr: Box<dyn Write>,
}

impl ParseErrorProcessor {
    pub fn new(stderr: Box<dyn Write>) -> ParseErrorProcessor {
        ParseErrorProcessor { stderr: stderr }
    }

    pub fn process(&mut self, msg: RawMessage, err: &str) -> () {
        let s = ["PARSE ERROR: ", err, " RAW: ", msg.as_str(), "\n"].join("");
        self.stderr.write(s.as_bytes()).unwrap();
    }
}

pub struct SpaceLineMerger {
    buf: Vec<String>,
}

impl SpaceLineMerger {
    pub fn new() -> SpaceLineMerger {
        Self {
            buf: Vec::with_capacity(10),
        } // TODO configure capcity?
    }
}

impl LineMerger for SpaceLineMerger {
    fn add_line(&mut self, line: String) -> Option<RawMessage> {
        if self.buf.is_empty() {
            self.buf.push(line);
            return None;
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            // line continuation
            self.buf.push(line);
            return None;
        }
        let ret = Some(RawMessage::new(self.buf.join("\n")));
        self.buf.clear();
        self.buf.push(line);
        ret
    }

    fn flush(&mut self) -> Option<RawMessage> {
        if self.buf.is_empty() {
            None
        } else {
            let ret = Some(RawMessage::new(self.buf.join("\n")));
            self.buf.clear();
            ret
        }
    }
}

pub trait LineMerger {
    fn add_line(&mut self, line: String) -> Option<RawMessage>;
    fn flush(&mut self) -> Option<RawMessage>;
}

pub struct ParserIterator {
    parser: Box<dyn LogParser>,
    line_merger: Option<Box<dyn LineMerger>>,
    input_iterator: Box<dyn Iterator<Item = io::Result<String>>>,
    error_processor: ParseErrorProcessor,
}

impl ParserIterator {
    pub fn new(
        parser: Box<dyn LogParser>,
        line_merger: Option<Box<dyn LineMerger>>,
        input_iterator: Box<dyn Iterator<Item = io::Result<String>>>,
        error_processor: ParseErrorProcessor,
    ) -> ParserIterator {
        Self {
            parser,
            line_merger,
            input_iterator,
            error_processor,
        }
    }

    fn next_raw(&mut self) -> Option<RawMessage> {
        let inp_next = self.input_iterator.next();
        match inp_next {
            None => {
                if self.line_merger.is_some() {
                    self.line_merger.as_mut().unwrap().flush()
                } else {
                    None
                }
            }
            Some(x) => {
                match x {
                    Ok(s) => {
                        if self.line_merger.is_some() {
                            let mut ret = self.line_merger.as_mut().unwrap().add_line(s);
                            while ret.is_none() {
                                if let Some(res_str) = self.input_iterator.next() {
                                    if let Ok(ss) = res_str {
                                        ret = self.line_merger.as_mut().unwrap().add_line(ss)
                                    } else {
                                        break;
                                    }
                                } else {
                                    ret = self.line_merger.as_mut().unwrap().flush();
                                    break;
                                }
                            }
                            ret
                        } else {
                            // no line merger
                            Some(RawMessage::new(s))
                        }
                    }
                    Err(_) => {
                        None // TODO log?
                    }
                }
            }
        }
    }
}

impl Iterator for ParserIterator {
    type Item = ParsedMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(raw) = self.next_raw() {
            let mut parsed = self.parser.parse(raw);
            while parsed.is_err() {
                let LogParseError { raw_msg, desc } = parsed.err().unwrap();
                self.error_processor.process(raw_msg, &desc);
                let nraw = self.next_raw();
                if nraw.is_none() {
                    return None;
                }
                parsed = self.parser.parse(nraw.unwrap());
            }
            parsed.ok()
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::BufWriter;

    use chrono::TimeZone;

    use crate::parser::*;

    use super::*;

    #[test]
    fn chrono_parse_works() {
        let ret =
            DateTime::parse_from_str("2022-04-20 21:12:55+0300", "%Y-%m-%d %H:%M:%S%z").unwrap();
        println!("{:?}", ret)
    }

    #[test]
    fn str2val_works() {
        assert_eq!(
            str2val("4", &ParsedValueType::LongType).unwrap(),
            ParsedValue::LongVal(4i64)
        );
        assert_eq!(
            str2val("4", &ParsedValueType::DoubleType).unwrap(),
            ParsedValue::DoubleVal(4.0f64)
        );
        assert_eq!(
            str2val(
                "2022-04-20 21:12:55.999+0200",
                &ParsedValueType::TimeType(TimeTypeFormat::new("%Y-%m-%d %H:%M:%S.%3f%z"))
            )
            .unwrap(),
            ParsedValue::TimeVal(
                FixedOffset::east(7200)
                    .ymd(2022, 4, 20)
                    .and_hms_micro(21, 12, 55, 999000)
            )
        );
        assert_eq!(
            str2val("blah", &ParsedValueType::StrType).unwrap(),
            ParsedValue::StrVal(Rc::new(String::from("blah")))
        );
    }

    #[test]
    fn test_parse_date_syslog() {
        assert_eq!(
            str2val(
                "Apr 22 02:34:54",
                &ParsedValueType::TimeType(TimeTypeFormat::new("%b %e %H:%M:%S"))
            )
            .unwrap(),
            ParsedValue::TimeVal(
                FixedOffset::east(local_timezone_offset())
                    .ymd(2022, 4, 22)
                    .and_hms(2, 34, 54)
            )
        );
    }

    #[test]
    fn test_parse() {
        let parsed = NaiveDateTime::parse_from_str("Apr 22 02:34:54 2022", "%b %e %H:%M:%S %Y")
            .ok()
            .unwrap();
        let parsed = FixedOffset::east(-3600)
            .from_local_datetime(&parsed)
            .unwrap();
        println!("{}", parsed)
    }

    #[test]
    fn test_parse2() {
        let parsed = NaiveDateTime::parse_from_str("2022-04-22 02:34:54", "%Y-%m-%d %H:%M:%S")
            .ok()
            .unwrap();
        let parsed = FixedOffset::east(3600)
            .from_local_datetime(&parsed)
            .unwrap();
        println!("{}", parsed)
    }

    #[test]
    fn test_parser_iterator() {
        let schema = GrokSchema::new(
            String::from("SYSLOGLINE"),
            vec![
                GrokColumnDef::new(
                    Rc::new("timestamp".to_string()),
                    ParsedValueType::TimeType(TimeTypeFormat::new("%b %e %H:%M:%S")),
                    vec![Rc::new(String::from("timestamp"))],
                    true,
                ),
                GrokColumnDef::new(
                    Rc::new("message".to_string()),
                    ParsedValueType::StrType,
                    vec![Rc::new(String::from("message"))],
                    true,
                ),
            ],
            true,
            vec![],
            false,
        );
        let lines = vec![
            "Apr 22 02:34:54 actek-mac login[49532]: USER_PROCESS: 49532 ttys000",
            "   =========",
            "Apr 22 04:42:04 actek-mac syslogd[104]: ASL Sender Statistics",
            "Apr 22 04:42:05 actek-mac syslogd[104]: ASL Sender Statistics",
            "   =========",
        ];

        {
            let lines: Vec<io::Result<String>> =
                lines.clone().iter().map(|&x| Ok(x.to_string())).collect();
            let parser = GrokParser::new(schema.clone()).unwrap();
            let mut pit = ParserIterator::new(
                Box::new(parser),
                None,
                Box::new(lines.into_iter()),
                ParseErrorProcessor::new(Box::new(BufWriter::new(io::stderr()))),
            );
            println!("No line merger");
            while let Some(x) = pit.next() {
                println!("{:?}", x)
            }
        }

        {
            let lines: Vec<io::Result<String>> =
                lines.clone().iter().map(|&x| Ok(x.to_string())).collect();
            let parser = GrokParser::new(schema.clone()).unwrap();
            let mut pit = ParserIterator::new(
                Box::new(parser),
                Some(Box::new(SpaceLineMerger::new())),
                Box::new(lines.into_iter()),
                ParseErrorProcessor::new(Box::new(BufWriter::new(io::stderr()))),
            );
            println!("With line merger");
            while let Some(x) = pit.next() {
                println!("{:?}", x)
            }
        }
    }
}
