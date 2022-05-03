// Copyright 2022 Asen Lazarov

extern crate grok;

use std::collections::HashMap;
use std::error::Error;
use std::rc::Rc;

use grok::{patterns, Grok, Pattern};

use crate::parser::parser::*;

#[derive(Debug, Clone)]
pub struct GrokColumnDef {
    col_name: Rc<String>,
    col_type: ParsedValueType,
    lookup_names: Vec<Rc<String>>,
    required: bool,
}

impl GrokColumnDef {
    pub fn new(
        col_name: Rc<String>,
        col_type: ParsedValueType,
        lookup_names: Vec<Rc<String>>,
        required: bool,
    ) -> GrokColumnDef {
        Self {
            col_name,
            col_type,
            lookup_names,
            required,
        }
    }

    // pub fn simple(col_type: ParsedValueType, lookup_name: String) -> GrokColumnDef {
    //     Self {
    //         col_type,
    //         lookup_names: vec![lookup_name]
    //     }
    // }

    pub fn clone(&self) -> GrokColumnDef {
        GrokColumnDef {
            col_name: self.col_name.clone(),
            col_type: self.col_type.clone(),
            lookup_names: self.lookup_names.iter().map(|s| s.clone()).collect(),
            required: self.required,
        }
    }

    pub fn col_name(&self) -> &String {
        &self.col_name
    }

    pub fn col_type(&self) -> &ParsedValueType {
        &self.col_type
    }
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

pub struct GrokParser {
    schema: GrokSchema,
    pattern: Pattern,
}

impl GrokParser {
    pub fn new(schema: GrokSchema) -> Result<GrokParser, Box<dyn Error>> {
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
            let mut hm: HashMap<Rc<str>, ParsedValue> = HashMap::new();
            for c in &self.schema.columns {
                let mut found = false;
                for lnm in &c.lookup_names {
                    let mm = m.get(lnm.as_str());
                    if mm.is_some() {
                        let opv: Option<ParsedValue> = str2val(mm.unwrap(), c.col_type());
                        if opv.is_some() {
                            hm.insert(Rc::from(lnm.as_str()), opv.unwrap());
                            found = true;
                            break;
                        }
                    }
                }
                if c.required && !found {
                    return Err(LogParseError::new("required field not found - TODO", msg));
                }
            }
            Ok(ParsedMessage::new(msg, ParsedData::new(hm)))
        } else {
            Err(LogParseError::new("GROK pattern did not match", msg))
        }
    }
}

#[cfg(test)]
pub fn test_syslog_schema() -> GrokSchema {
    GrokSchema {
        pattern: String::from("SYSLOGLINE"),
        load_default: true,
        columns: vec![
            GrokColumnDef::new(
                Rc::new("timestamp".to_string()),
                ParsedValueType::TimeType(TimeTypeFormat::new("%b %e %H:%M:%S")),
                vec![Rc::new(String::from("timestamp"))],
                true,
            ),
            GrokColumnDef {
                col_name: Rc::new("message".to_string()),
                col_type: ParsedValueType::StrType,
                lookup_names: vec![Rc::new(String::from("message"))],
                required: true,
            },
            GrokColumnDef {
                col_name: Rc::new("logsource".to_string()),
                col_type: ParsedValueType::StrType,
                lookup_names: vec![Rc::new(String::from("logsource"))],
                required: true,
            },
            GrokColumnDef {
                col_name: Rc::new("program".to_string()),
                col_type: ParsedValueType::StrType,
                lookup_names: vec![Rc::new(String::from("program"))],
                required: true,
            },
            GrokColumnDef {
                col_name: Rc::new("pid".to_string()),
                col_type: ParsedValueType::LongType,
                lookup_names: vec![Rc::new(String::from("pid"))],
                required: true,
            },
        ],
        grok_with_alias_only: false,
        extra_patterns: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_works() {
        let schema = GrokSchema {
            pattern: String::from("test_pat"),
            load_default: false,
            columns: vec![
                GrokColumnDef::new(
                    Rc::new("logts".to_string()),
                    ParsedValueType::TimeType(TimeTypeFormat::new("%Y-%m-%dT%H:%M:%S.%3f%z")),
                    vec![Rc::new(String::from("logts"))],
                    true,
                ),
                GrokColumnDef {
                    col_name: Rc::new("msg".to_string()),
                    col_type: ParsedValueType::StrType,
                    lookup_names: vec![Rc::new(String::from("msg"))],
                    required: true,
                },
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
