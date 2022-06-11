// Copyright 2022 Asen Lazarov

extern crate grok;

use std::collections::HashMap;
use std::sync::Arc;

use crate::DynError;
use grok::{patterns, Grok, Pattern};

use crate::parser::parser::*;
use crate::parser::schema::{ParserColDef, ParserSchema};

#[derive(Debug, Clone)]
pub struct GrokColumnDef {
    pcd: ParserColDef,
    lookup_names: Vec<Arc<String>>,
    required: bool,
}

impl GrokColumnDef {
    pub fn new(
        col_name: Arc<str>,
        col_type: ParsedValueType,
        lookup_names: Vec<Arc<String>>,
        required: bool,
    ) -> GrokColumnDef {
        let pcd = ParserColDef::new(col_name.as_ref(), &col_type, required);
        Self {
            pcd,
            lookup_names,
            required,
        }
    }

    pub fn clone(&self) -> GrokColumnDef {
        GrokColumnDef {
            pcd: self.pcd.clone(),
            lookup_names: self.lookup_names.iter().map(|s| s.clone()).collect(),
            required: self.required,
        }
    }

    pub fn col_name(&self) -> &Arc<str> {
        &self.pcd.name()
    }

    pub fn col_type(&self) -> &ParsedValueType {
        &self.pcd.pv_type()
    }

    pub fn required(&self) -> bool { self.required }
}

#[derive(Debug, Clone)]
pub struct GrokSchema {
    pattern: String,
    columns: Vec<GrokColumnDef>,
    load_default: bool,
    extra_patterns: Vec<(String, String)>,
    grok_with_alias_only: bool,
}

impl GrokSchema {
    pub fn new(
        pattern: String,
        columns: Vec<GrokColumnDef>,
        load_default: bool,
        extra_patterns: Vec<(String, String)>,
        grok_with_alias_only: bool,
    ) -> GrokSchema {
        Self {
            pattern,
            columns,
            load_default,
            extra_patterns,
            grok_with_alias_only,
        }
    }

    pub fn columns(&self) -> &Vec<GrokColumnDef> {
        &self.columns
    }
}

impl ParserSchema for GrokSchema {
    fn name(&self) -> &str {
        &self.pattern
    }

    fn col_defs(&self) -> Vec<&ParserColDef> {
        self.columns.iter().map(|x| &x.pcd).collect::<Vec<_>>()
    }
}

pub struct GrokParser {
    schema: GrokSchema,
    pattern: Pattern,
}

impl GrokParser {
    pub fn new(schema: GrokSchema) -> Result<GrokParser, DynError> {
        let mut grok = if schema.load_default {
            Grok::with_patterns()
        } else {
            Grok::empty()
        };
        for (n, p) in &schema.extra_patterns {
            grok.insert_definition(n, p)
        }
        let pattern = grok.compile(
            format!("%{{{}}}", schema.pattern.as_str()).as_str(),
            schema.grok_with_alias_only,
        )?;
        Result::Ok(GrokParser { schema, pattern })
    }

    pub fn default_patterns() -> Vec<(String, String)> {
        patterns()
            .iter()
            .map(|&p| (p.0.to_string(), p.1.to_string()))
            .collect()
    }

    // pub fn get_schema(&self) -> &GrokSchema {
    //     &self.schema
    // }
}

impl LogParser for GrokParser {
    fn parse(&self, msg: RawMessage) -> Result<ParsedMessage, LogParseError> {
        let mopt = self.pattern.match_against(msg.as_str());
        if mopt.is_some() {
            let m = mopt.unwrap();
            let mut hm: HashMap<Arc<str>, Arc<ParsedValue>> = HashMap::new();
            for c in &self.schema.columns {
                let mut found = false;
                for lnm in &c.lookup_names {
                    let mm = m.get(lnm.as_str());
                    if mm.is_some() {
                        let opv: Option<Arc<ParsedValue>> = str2val(mm.unwrap(), c.col_type());
                        if opv.is_some() {
                            hm.insert(Arc::from(lnm.as_str()), opv.unwrap());
                            found = true;
                            break;
                        }
                    }
                }
                if c.required && !found {
                    return Err(LogParseError::from_string(
                        format!(
                            "Required field not found: {} RAW: {}",
                            c.pcd.name(),
                            msg.as_str()
                        ),
                        msg,
                    ));
                }
            }
            Ok(ParsedMessage::new(msg, ParsedData::new(hm)))
        } else {
            Err(LogParseError::new("GROK pattern did not match", msg))
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    pub fn test_syslog_schema() -> GrokSchema {
        GrokSchema {
            pattern: String::from("SYSLOGLINE"),
            load_default: true,
            columns: vec![
                GrokColumnDef::new(
                    Arc::from("timestamp"),
                    ParsedValueType::TimeType(TimeTypeFormat::new("%b %e %H:%M:%S")),
                    vec![Arc::new(String::from("timestamp"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("message"),
                    ParsedValueType::StrType(65535),
                    vec![Arc::new(String::from("message"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("logsource"),
                    ParsedValueType::StrType(256),
                    vec![Arc::new(String::from("logsource"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("program"),
                    ParsedValueType::StrType(256),
                    vec![Arc::new(String::from("program"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("pid"),
                    ParsedValueType::LongType,
                    vec![Arc::new(String::from("pid"))],
                    true,
                ),
            ],
            grok_with_alias_only: false,
            extra_patterns: vec![],
        }
    }

    pub fn test_dummy_schema() -> GrokSchema {
        GrokSchema {
            pattern: String::from("DUMMY"),
            load_default: true,
            columns: vec![
                // GrokColumnDef::new(
                //     Arc::from("timestamp"),
                //     ParsedValueType::TimeType(TimeTypeFormat::new("%b %e %H:%M:%S")),
                //     vec![Arc::new(String::from("timestamp"))],
                //     true,
                // ),
                GrokColumnDef::new(
                    Arc::from("num"),
                    ParsedValueType::LongType,
                    vec![Arc::new(String::from("num"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("message"),
                    ParsedValueType::StrType(65535),
                    vec![Arc::new(String::from("message"))],
                    true,
                ),
            ],
            grok_with_alias_only: false,
            extra_patterns: vec![(
                "DUMMY".to_string(),
                "%{NUMBER:num} +%{GREEDYDATA:message}".to_string(),
            )],
        }
    }

    pub fn test_dummy_data(num_lines: usize) -> String {
        (0..num_lines)
            .map(|i| format!("{} line number {}\n", i, i))
            .collect::<Vec<_>>()
            .join("")
    }

    #[test]
    fn parse_works() {
        let schema = GrokSchema {
            pattern: String::from("test_pat"),
            load_default: false,
            columns: vec![
                GrokColumnDef::new(
                    Arc::from("logts"),
                    ParsedValueType::TimeType(TimeTypeFormat::new("%Y-%m-%dT%H:%M:%S.%3f%z")),
                    vec![Arc::new(String::from("logts"))],
                    true,
                ),
                GrokColumnDef::new(
                    Arc::from("msg"),
                    ParsedValueType::StrType(65536),
                    vec![Arc::new(String::from("msg"))],
                    true,
                ),
            ],
            grok_with_alias_only: false,
            extra_patterns: vec![
                (String::from("NOSPACES"), String::from("[^ ]+")),
                (String::from("MESSAGE"), String::from(".*")),
                (
                    String::from("test_pat"),
                    String::from("%{NOSPACES:logts} %{MESSAGE:msg}"),
                ),
            ],
        };
        let parser = GrokParser::new(schema).unwrap();
        let lines = vec![
            "2022-04-20T21:12:55.999+0300 msg0 blah",
            "2022-04-20T21:12:56.057+0300 msg1 blahblah",
            "2022-04-20T21:12:56.998+0300 msg2 blah ala bala",
        ];
        for ln in lines {
            let parsed = parser.parse(RawMessage::new(String::from(ln))).unwrap();
            println!("{:?}", parsed.get_raw());
            println!("{:?}", parsed.get_parsed());
            println!("{:?}", parsed)
        }
    }

    #[test]
    fn parse_partial_date_works() {
        let schema = test_syslog_schema();
        let parser = GrokParser::new(schema).unwrap();
        let lines = vec![
            "Apr 22 02:34:54 actek-mac login[49532]: USER_PROCESS: 49532 ttys000",
            "Apr 22 04:42:04 actek-mac syslogd[104]: ASL Sender Statistics",
        ];
        for ln in lines {
            let parsed = parser.parse(RawMessage::new(String::from(ln))).unwrap();
            //println!("{:?}", parsed.get_raw());
            //println!("{:?}", parsed.get_parsed());
            println!("{:?}", parsed)
        }
        let line_no_msg = "Apr 22 02:34:54 actek-mac";
        let raw_msg = RawMessage::new(String::from(line_no_msg));
        let parsed = parser.parse(raw_msg);
        assert_eq!(parsed.as_ref().err().is_some(), true);
        println!("{:?}", parsed)
    }
}
