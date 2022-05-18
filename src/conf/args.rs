use std::error::Error;
use std::fs;
use std::io::{self, BufWriter, Write};

use clap::Parser;
use crate::conf::external::ExternalConfig;


#[derive(Parser, Debug)]
#[clap(name = "hustlog")]
#[clap(author = "Asen Lazarov <asen.lazarov@gmail.com>")]
#[clap(version = "0.1")]
#[clap(about = "A tool to mess with logs", long_about = None)]
pub struct MyArgs {
    ///Input source
    #[clap(short, long)]
    pub input: Option<String>,

    ///Output destination
    #[clap(short, long)]
    pub output: Option<String>,

    ///Output format. One of:
    ///     sql
    ///     csv (default)
    #[clap(short = 'f', long)]
    pub output_format: Option<String>,

    // INSERT batch size when generating SQL
    #[clap(short = 'b', long)]
    pub output_batch_size: Option<usize>,

    ///Add a create table statement before the inserts
    #[clap(long)]
    pub output_add_ddl: bool,

    /// Yaml config file to use for default values
    /// command line options still override conf values
    #[clap(short, long)]
    conf: Option<String>,

    /// Grok Pattern name to use
    #[clap(short = 'p', long)]
    pub grok_pattern: Option<String>,

    /// Grok Patterns file to use
    #[clap(short = 't', long)]
    pub grok_patterns_file: Option<String>,

    /// Extra Grok Patterns, can be multiple. E.g. -e "NOT_SPACE [^ ]+"
    #[clap(short = 'e', long)]
    pub grok_extra_patterns: Vec<String>,

    /// SQL query
    #[clap(short, long)]
    pub query: Option<String>,

    /// print the defoult patterns to the output stream and exit
    #[clap(long)]
    pub grok_list_default_patterns: bool,

    #[clap(long)]
    pub grok_with_alias_only: bool,

    #[clap(long)]
    pub grok_ignore_default_patterns: bool,

    /// Grok schema columns, can be multiple.
    /// E.g. -s "+timestamp:ts:%Y-%m-%d %H:%M:%S.%3f%z" -s message:str -s another_str
    #[clap(short = 's', long)]
    pub grok_schema_columns: Vec<String>,

    /// Whether to merge lines starting with whitespace with the previous ones
    #[clap(short, long)]
    pub merge_multi_line: bool,
}

impl MyArgs {

    pub fn get_external_conf(&self) -> Result<ExternalConfig, Box<dyn Error>> {
        if self.conf.is_some() {
            let pc = ExternalConfig::from_yaml_file(self.conf.as_ref().unwrap().as_str())?;
            Ok(pc)
        } else {
            Ok(ExternalConfig::empty())
        }
    }

    pub fn get_outp(&self) -> Result<Box<dyn Write>, Box<dyn Error>> {
        let writer: Box<dyn Write> = match &self.output {
            None => Box::new(BufWriter::new(io::stdout())),
            Some(filename) => {
                if filename == "-" {
                    Box::new(BufWriter::new(io::stdout()))
                } else {
                    Box::new(BufWriter::new(fs::File::create(filename)?))
                }
            },
        };
        Ok(writer)
    }


    pub fn grok_list_default_patterns(&self) -> bool {
        self.grok_list_default_patterns
    }
}
