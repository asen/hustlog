// Copyright 2022 Asen Lazarov

use chrono::{DateTime, FixedOffset, Local, NaiveDateTime, Offset, TimeZone};
use chrono::Datelike;
use std::collections::HashMap;

#[derive(PartialEq, Debug)]
pub enum ParsedValue {
    StrVal(Box<String>),
    IntVal(i32),
    LongVal(i64),
    ULongVal(u64),
    FloatVal(f32),
    DoubleVal(f64),
    TimeVal(DateTime<FixedOffset>),
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
const YEAR_SPECIFIERS: [&str;11] = ["%y", "%Y", "%G", "%g", "%D", "%x", "%f", "%v", "%c", "%+", "%s"];
const TZ_SPECIFIERS: [&str;6] = ["%Z", "%z", "%:z", "%#z", "%+", "%s"];

impl TimeTypeFormat {
    pub fn new(fmt: &str) -> TimeTypeFormat {
        let (specifier, need_year) = if YEAR_SPECIFIERS.iter().any(|&ys| {
            fmt.contains(ys)
        }) {
            (fmt.to_string(), false)
        } else {
            (format!("{} %Y", fmt), true)
        };
        let has_tz= TZ_SPECIFIERS.iter().any(|&zs| {
            fmt.contains(zs)
        });
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
    StrType,
    IntType,
    LongType,
    ULongType,
    FloatType,
    DoubleType,
    TimeType(TimeTypeFormat), // format specifier
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
        NaiveDateTime::parse_from_str( to_parse,&fmt.format_specifier)
            .ok()
            .and_then(|nd| {
                FixedOffset::east(fmt.local_tz_offset).from_local_datetime(&nd).single()
            })
    } else {
        DateTime::parse_from_str(
            to_parse, &fmt.format_specifier
        ).ok()
    }
}

pub fn str2val(s: &str, ctype: &ParsedValueType) -> Option<ParsedValue> {
    match ctype {
        ParsedValueType::StrType => Some(ParsedValue::StrVal(Box::new(String::from(s)))),
        ParsedValueType::IntType => s.parse::<i32>().ok().map(|v| ParsedValue::IntVal(v)),
        ParsedValueType::LongType => s.parse::<i64>().ok().map(|v| ParsedValue::LongVal(v)),
        ParsedValueType::ULongType => s.parse::<u64>().ok().map(|v| ParsedValue::ULongVal(v)),
        ParsedValueType::FloatType => s.parse::<f32>().ok().map(|v| ParsedValue::FloatVal(v)),
        ParsedValueType::DoubleType => s.parse::<f64>().ok().map(|v| ParsedValue::DoubleVal(v)),
        ParsedValueType::TimeType(fmt) => parse_ts(s,fmt)
            .map(|v| ParsedValue::TimeVal(v)),
    }
}

pub fn str2type(s: &str) -> Option<ParsedValueType> {
    match s {
        "str" | "" => Some(ParsedValueType::StrType),
        "int" => Some(ParsedValueType::IntType),
        "long" => Some(ParsedValueType::LongType),
        "ulong" => Some(ParsedValueType::ULongType),
        "float" => Some(ParsedValueType::FloatType),
        "double" => Some(ParsedValueType::DoubleType),
        x => {
            let ts_prefix = "ts:";
            if x.starts_with(ts_prefix) {
                Some(ParsedValueType::TimeType(TimeTypeFormat::new(x.strip_prefix(ts_prefix).unwrap())))
            } else {
                None
            }
        }
    }
}

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
pub struct ParsedData<'a>(HashMap<&'a String, ParsedValue>);

impl ParsedData<'_> {
    pub fn new(hm: HashMap<&String, ParsedValue>) -> ParsedData {
        ParsedData(hm)
    }
}

#[derive(Debug)]
pub struct ParsedMessage<'a> {
    raw: RawMessage,
    parsed: ParsedData<'a>,
}

impl ParsedMessage<'_> {
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
    fn parse(&self, msg: RawMessage) -> Result<ParsedMessage, RawMessage>;
}

pub struct SpaceLineMerger {
    buf: Vec<String>,
}

impl SpaceLineMerger {
    pub fn new() -> SpaceLineMerger {
        Self { buf: Vec::with_capacity(10 ) } // TODO configure capcity?
    }
}

impl LineMerger for SpaceLineMerger {
    fn add_line(&mut self, line: String) -> Option<RawMessage> {
        if self.buf.is_empty() {
            self.buf.push(line);
            return None
        }
        if line.starts_with(' ') || line.starts_with('\t') {
            // line continuation
            self.buf.push(line);
            return None
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

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
            str2val("4", &ParsedValueType::IntType).unwrap(),
            ParsedValue::IntVal(4)
        );
        assert_eq!(
            str2val("4", &ParsedValueType::LongType).unwrap(),
            ParsedValue::LongVal(4i64)
        );
        assert_eq!(
            str2val("4", &ParsedValueType::ULongType).unwrap(),
            ParsedValue::ULongVal(4u64)
        );
        assert_eq!(
            str2val("4", &ParsedValueType::FloatType).unwrap(),
            ParsedValue::FloatVal(4.0f32)
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
            ParsedValue::StrVal(Box::new(String::from("blah")))
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
        let parsed = NaiveDateTime::parse_from_str(
            "Apr 22 02:34:54 2022", "%b %e %H:%M:%S %Y"
        ).ok().unwrap();
        let parsed =
            FixedOffset::east(-3600).from_local_datetime(&parsed).unwrap();
        println!("{}", parsed)
    }

    #[test]
    fn test_parse2() {
        let parsed = NaiveDateTime::parse_from_str(
            "2022-04-22 02:34:54", "%Y-%m-%d %H:%M:%S"
        ).ok().unwrap();
        let parsed =
            FixedOffset::east(3600).from_local_datetime(&parsed).unwrap();
        println!("{}", parsed)
    }
}
