use std::fs;
use std::io::{self, BufWriter};

use crate::conf::external::ExternalConfig;
use clap::Parser;
use crate::{DynBoxWrite, DynError};

#[derive(Parser, Debug)]
#[clap(name = "hustlog")]
#[clap(author = "Asen Lazarov <asen.lazarov@gmail.com>")]
#[clap(version = "0.1")]
#[clap(about = "A tool to mess with logs", long_about = None)]
pub struct MyArgs {
    /// print the defoult patterns to the output stream and exit
    /// not available in config
    #[clap(long)]
    pub grok_list_default_patterns: bool,

    /// Yaml config file to use for default values
    /// command line options still override conf values
    #[clap(short, long)]
    pub conf: Option<String>,

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

    /// Grok Pattern name to use
    #[clap(short = 'g', long)]
    pub grok_pattern: Option<String>,

    /// Grok Patterns file to use
    #[clap(short = 'p', long)]
    pub grok_patterns_file: Option<String>,

    /// Extra Grok Patterns, can be multiple. E.g. -e "NOT_SPACE [^ ]+"
    #[clap(short = 'e', long)]
    pub grok_extra_patterns: Vec<String>,

    /// SQL query
    #[clap(short, long)]
    pub query: Option<String>,

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

    /// How many threads to use in the "CPU-intensive work" (Rayon) thread pool.
    /// Default is 2
    #[clap(short, long)]
    pub rayon_threads: Option<usize>,

    /// How often the server "tick" event should be emitted, in seconds.
    /// Normally buffers are flushed at that time so that ends up being the maximum delay
    /// between a message entering the system and being (batch) processed.
    /// Default is 30 seconds.
    #[clap(long)]
    pub tick_interval: Option<u64>,

    /// Idle UDP streams are closed after being idle for that long
    /// Default is 30 seconds.
    #[clap(long)]
    pub idle_timeout: Option<u64>,

    /// Internal async queues channel size. Backpressure is applied when a channel gets full.
    /// Default is 1000
    #[clap(long)]
    pub channel_size: Option<usize>,

    /// Potentially temporary option - to test sync (single-threaded) bs async file I/O performance
    /// set to true to use the async pipeline, may change the default too (currently default is false).
    #[clap(long)]
    pub async_file_processing: bool,

}

impl MyArgs {
    pub fn get_external_conf(&self) -> Result<ExternalConfig, DynError> {
        if self.conf.is_some() {
            let pc = ExternalConfig::from_yaml_file(self.conf.as_ref().unwrap().as_str())?;
            Ok(pc)
        } else {
            Ok(ExternalConfig::empty())
        }
    }

    pub fn get_outp(&self) -> Result<DynBoxWrite, DynError> {
        let writer: DynBoxWrite = match &self.output {
            None => Box::new(BufWriter::new(io::stdout())),
            Some(filename) => {
                if filename == "-" {
                    Box::new(BufWriter::new(io::stdout()))
                } else {
                    Box::new(BufWriter::new(fs::File::create(filename)?))
                }
            }
        };
        Ok(writer)
    }

    pub fn grok_list_default_patterns(&self) -> bool {
        self.grok_list_default_patterns
    }
}
