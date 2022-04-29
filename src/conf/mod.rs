// Copyright 2022 Asen Lazarov

use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::rc::Rc;

use clap::Parser;

use crate::parser::*;

#[derive(Debug, Clone)]
pub struct ConfigError(String);

impl ConfigError {
    pub fn new(s: &str) -> ConfigError {
        ConfigError(s.to_string())
    }
}
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Configuration error: {}", self.0)
    }
}

impl Error for ConfigError {}

#[derive(Parser, Debug)]
#[clap(name = "hustlog")]
#[clap(author = "Asen Lazarov <asen.lazarov@gmail.com>")]
#[clap(version = "0.1")]
#[clap(about = "A tool to mess with logs", long_about = None)]
pub struct MyArgs {
    ///Input source
    #[clap(short, long)]
    input: Option<String>,

    ///Output destination
    #[clap(short, long)]
    output: Option<String>,

    /// TODO
    #[clap(short, long)]
    conf: Option<String>,

    /// Grok Pattern name to use
    #[clap(short = 'p', long)]
    grok_pattern: Option<String>,

    /// Grok Patterns file to use
    #[clap(short = 't', long)]
    grok_patterns_file: Option<String>,

    /// Extra Grok Patterns, can be multiple. E.g. -e "NOT_SPACE [^ ]+"
    #[clap(short = 'e', long)]
    grok_extra_pattern: Vec<String>,

    /// SQL query
    #[clap(short, long)]
    query: Option<String>,

    /// print the defoult patterns to the output stream and exit
    #[clap(long)]
    grok_list_default_patterns: bool,

    #[clap(long)]
    grok_with_alias_only: bool,

    #[clap(long)]
    grok_ignore_default_patterns: bool,

    /// Grok schema columns, can be multiple.
    /// E.g. -s "+timestamp:ts:%Y-%m-%d %H:%M:%S.%3f%z" -s message:str -s another_str
    #[clap(short = 's', long)]
    grok_schema_column: Vec<String>,

    /// Whether to merge lines starting with whitespace with the previous ones
    /// My have small performance impact.
    #[clap(short, long)]
    merge_multi_line: bool,
}

impl MyArgs {
    fn parse_col_defs(schema_columns: &Vec<String>) -> Result<Vec<GrokColumnDef>, Box<dyn Error>> {
        let grok_schema_cols: Vec<_> = schema_columns
            .iter()
            .map(|x| {
                let mut my_iter = x.splitn(2, ":").into_iter();
                let lookup_names_csv = my_iter.next().unwrap();
                let (lookup_names_csv, required) = if lookup_names_csv.starts_with('+') {
                    (lookup_names_csv.strip_prefix('+').unwrap(), true)
                } else {
                    (lookup_names_csv, false)
                };
                let lookup_names: Vec<Rc<String>> = lookup_names_csv
                    .split(',')
                    .into_iter()
                    .filter(|&x| !x.is_empty())
                    .map(|x| Rc::new(x.to_string()))
                    .collect();
                if lookup_names.is_empty() {
                    Err(Box::new(ConfigError("Empty lookup names".into())))
                } else {
                    let col_type = my_iter.next().unwrap_or("str");
                    let col_type = str2type(col_type);
                    if col_type.is_none() {
                        Err(Box::new(ConfigError("Invalid column type".into())))
                    } else {
                        let col_name = lookup_names.first().unwrap().clone();
                        Ok(GrokColumnDef::new(
                            col_name,
                            col_type.unwrap(),
                            lookup_names,
                            required,
                        ))
                    }
                }
            })
            .collect();
        let first_err = grok_schema_cols.iter().find(|&x| x.is_err());
        if first_err.is_some() {
            return Err(Box::new(first_err.unwrap().as_ref().err().unwrap().clone()));
        }
        let grok_schema_cols: Vec<GrokColumnDef> = grok_schema_cols
            .iter()
            .map(|x| x.as_ref().ok().unwrap().clone())
            .collect();
        Ok(grok_schema_cols)
    }

    fn get_grok_col_defs(&self) -> Result<Vec<GrokColumnDef>, Box<dyn Error>> {
        MyArgs::parse_col_defs(&self.grok_schema_column)
    }

    pub fn get_grok_schema(&self) -> Result<Option<GrokSchema>, Box<dyn Error>> {
        if self.grok_pattern.is_none() {
            return Ok(None);
        }
        if self.grok_schema_column.is_empty() {
            return Err(Box::new(ConfigError(
                "At least one grok schema column is required when pattern is specified".into(),
            )));
        }
        let pattern = self.grok_pattern.as_ref().unwrap().clone();

        let grok_schema_cols: Vec<GrokColumnDef> = self.get_grok_col_defs()?;
        let extra_patterns: Vec<(String, String)> = self
            .grok_extra_pattern
            .iter()
            .map(|x| {
                let mut spliter = x.splitn(2, " ").into_iter();
                let first: String = spliter.next().unwrap().to_string();
                let second: String = spliter.next().unwrap_or("").to_string();
                (first, second)
            })
            .collect();
        Ok(Some(GrokSchema::new(
            pattern,
            grok_schema_cols,
            !self.grok_ignore_default_patterns,
            extra_patterns,
            self.grok_with_alias_only,
        )))
    }

    pub fn get_buf_read(&self) -> Result<Box<dyn BufRead>, Box<dyn Error>> {
        let reader: Box<dyn BufRead> = match &self.input {
            None => Box::new(BufReader::new(io::stdin())),
            Some(filename) => Box::new(BufReader::new(fs::File::open(filename)?)),
        };
        Ok(reader)
    }

    pub fn get_outp(&self) -> Result<Box<dyn Write>, Box<dyn Error>> {
        let writer: Box<dyn Write> = match &self.output {
            None => Box::new(BufWriter::new(io::stdout())),
            Some(filename) => Box::new(BufWriter::new(fs::File::create(filename)?)),
        };
        Ok(writer)
    }

    pub fn get_logger(&self) -> Box<dyn Write> {
        Box::new(BufWriter::new(io::stderr()))
    }

    pub fn grok_list_default_patterns(&self) -> bool {
        self.grok_list_default_patterns
    }

    pub fn merge_multi_line(&self) -> bool {
        self.merge_multi_line
    }

    pub fn query(&self) -> &Option<String> {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_col_defs_works() {
        let parsed = MyArgs::parse_col_defs(&vec![
            "timestamp:ts:%b %e %H:%M:%S".to_string(),
            "message".to_string(),
        ])
        .unwrap();
        println!("{:?}", parsed)
    }
}
