use std::error::Error;
use std::fs;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::rc::Rc;
use crate::{ConfigError, GrokColumnDef, GrokSchema, MyArgs, str2type};
use crate::conf::external::ExternalConfig;

macro_rules! args_or_external_vec{
    ($a:expr,$b:expr, $prop:ident, $err: expr) => {
        if $a.$prop.is_empty() {
            if $b.$prop.is_some() {
                let ret_ref = $b.$prop.as_ref().unwrap();
                if ret_ref.is_empty() {
                    let my_err: Box<dyn Error> = Box::new(ConfigError::new($err));
                    Err(my_err)
                } else {
                    Ok(ret_ref)
                }

            } else {
                let my_err: Box<dyn Error> = Box::new(ConfigError::new($err));
                Err(my_err)
            }
        } else {
            Ok(&$a.$prop)
        }
    }
}

macro_rules! args_or_external_opt{
    ($a:expr,$b:expr, $prop:ident, $err: expr) => {
        if $a.$prop.is_some() {
            Ok($a.$prop.as_ref().unwrap())
        } else {
            if ($b.$prop.is_some()) {
                Ok($b.$prop.as_ref().unwrap())
            } else {
                let my_err: Box<dyn Error> = Box::new(ConfigError::new($err));
                Err(my_err)
            }
        }
    }
}

macro_rules! args_or_external_vec_default{
    ($a:expr,$b:expr, $prop:ident, $def: expr) => {
        if $a.$prop.is_empty() {
            if $b.$prop.is_some() {
                let ret_ref = $b.$prop.as_ref().unwrap();
                if ret_ref.is_empty() {
                    $def
                } else {
                    ret_ref
                }
            } else {
                $def
            }
        } else {
            &$a.$prop
        }
    }
}

macro_rules! args_or_external_opt_default{
    ($a:expr,$b:expr, $prop:ident, $def: expr) => {
        if $a.$prop.is_some() {
            $a.$prop.as_ref().unwrap()
        } else {
            if ($b.$prop.is_some()) {
                $b.$prop.as_ref().unwrap()
            } else {
                $def
            }
        }
    }
}

macro_rules! args_or_external_bool_default{
    ($a:expr,$b:expr, $prop:ident, $def: expr) => {
        if $a.$prop {
            $a.$prop
        } else {
            if ($b.$prop.is_some()) {
                $b.$prop.unwrap()
            } else {
                $def
            }
        }
    }
}

pub enum OutputFormat {
    DEFAULT,
    SQL,
}

pub struct HustlogConfig {
    input: String,
    merge_multi_line: bool,

    grok_schema: GrokSchema,
    query: Option<String>,

    output: String,
    output_format: String, //TODO
    output_batch_size: usize,
    output_add_ddl: bool,
}

impl HustlogConfig {
    pub fn new(args: MyArgs) -> Result<HustlogConfig,Box<dyn Error>> {
        let external_conf = args.get_external_conf()?;
        let schema = Self::parse_grok_schema(&args, &external_conf)?;
        let input = args_or_external_opt_default!(&args, &external_conf, input, "-");
        let merge_multi_line = args_or_external_bool_default!(&args, &external_conf, merge_multi_line, false);
        let query_str_ref = args_or_external_opt_default!(&args, &external_conf, query, "");
        let query_str: Option<String> = if query_str_ref  == "" {
            None
        } else {
            Some(query_str_ref.to_string())
        };
        let output = args_or_external_opt_default!(&args, &external_conf, output, "-");
        let output_format = args_or_external_opt_default!(&args, &external_conf, output, "csv");
        let output_batch_size = args_or_external_opt_default!(&args, &external_conf, output_batch_size, &1000);
        let output_add_ddl = args_or_external_bool_default!(&args, &external_conf, output_add_ddl, false);
        Ok(Self{
            input: input.to_string(),
            merge_multi_line: merge_multi_line,
            grok_schema: schema,
            query: query_str,
            output: output.to_string(),
            output_format: output_format.to_string(),
            output_batch_size: *output_batch_size,
            output_add_ddl: output_add_ddl,
        })
    }

    fn parse_col_defs(args: &MyArgs, external_conf: &ExternalConfig) -> Result<Vec<GrokColumnDef>, Box<dyn Error>> {
        let schema_columns = args_or_external_vec!(&args, &external_conf,
            grok_schema_columns, "At least one grok schema column is required")?;
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
                    Err(Box::new(ConfigError::new("Empty lookup names")))
                } else {
                    let col_type = my_iter.next().unwrap_or("str");
                    let col_type = str2type(col_type);
                    if col_type.is_none() {
                        Err(Box::new(ConfigError::new("Invalid column type")))
                    } else {
                        let col_name = lookup_names.first().unwrap().clone();
                        Ok(GrokColumnDef::new(
                            Rc::from(col_name.as_str()),
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

    fn parse_grok_schema(args: &MyArgs, external_conf: &ExternalConfig) -> Result<GrokSchema, Box<dyn Error>> {
        let pattern = args_or_external_opt!(&args, &external_conf, grok_pattern,
            "GROK pattern is required")?;
        let grok_schema_cols: Vec<GrokColumnDef> = Self::parse_col_defs(&args, &external_conf)?;
        let empty_vec = Vec::new();
        let grok_extra_patterns = args_or_external_vec_default!(&args, &external_conf,
            grok_extra_patterns, &empty_vec);
        let extra_patterns: Vec<(String, String)> = grok_extra_patterns
            .iter()
            .map(|x| {
                let mut spliter = x.splitn(2, " ").into_iter();
                let first: String = spliter.next().unwrap().to_string();
                let second: String = spliter.next().unwrap_or("").to_string();
                (first, second)
            })
            .collect();
        let grok_ignore_default_patterns = args_or_external_bool_default!(&args, &external_conf, grok_ignore_default_patterns,
            false);
        let grok_with_alias_only = args_or_external_bool_default!(&args, &external_conf, grok_with_alias_only,
            false);
        Ok(GrokSchema::new(
            pattern.clone(),
            grok_schema_cols,
            !grok_ignore_default_patterns,
            extra_patterns,
            grok_with_alias_only,
        ))
    }

    pub fn get_buf_read(&self) -> Result<Box<dyn BufRead>, Box<dyn Error>> {
        let reader: Box<dyn BufRead> = if &self.input == "-" {
            Box::new(BufReader::new(io::stdin()))
        } else {
            Box::new(BufReader::new(fs::File::open(&self.input)?))
        };
        Ok(reader)
    }

    pub fn get_outp(&self) -> Result<Box<dyn Write>, Box<dyn Error>> {
        let writer: Box<dyn Write> = if &self.output == "-" {
            Box::new(BufWriter::new(io::stdout()))
        } else {
            Box::new(BufWriter::new(fs::File::create(&self.output)?))
        };
        Ok(writer)
    }

    pub fn get_logger(&self) -> Box<dyn Write> {
        Box::new(BufWriter::new(io::stderr()))
    }

    pub fn get_grok_schema(&self) -> &GrokSchema {
        &self.grok_schema
    }

    pub fn merge_multi_line(&self) -> bool {
        self.merge_multi_line
    }

    pub fn query(&self) -> &Option<String> {
        &self.query
    }

    pub fn output_format(&self) -> OutputFormat {
        match self.output_format.as_str() {
            "sql" => OutputFormat::SQL,
            _ => OutputFormat::DEFAULT,
        }
    }

    pub fn output_add_ddl(&self) -> bool {
        self.output_add_ddl
    }

    pub fn output_batch_size(&self) -> usize {
        self.output_batch_size
    }
}


// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn parse_col_defs_works() {
//         let parsed = HustlogConfig::parse_col_defs(&vec![
//             "timestamp:ts:%b %e %H:%M:%S".to_string(),
//             "message".to_string(),
//         ])
//             .unwrap();
//         println!("{:?}", parsed)
//     }
// }
