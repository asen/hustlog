use crate::conf::external::ExternalConfig;
use crate::DynError;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(name = "hustlog")]
#[clap(author = "Asen Lazarov <asen.lazarov@gmail.com>")]
#[clap(version = "0.1")]
#[clap(about = "A tool to mess with logs", long_about = None)]
pub struct MyArgs {
    /// Print the defoult patterns to the output stream and exit.
    /// Not available in config
    #[clap(long)]
    pub grok_list_default_patterns: bool,

    /// Yaml config file to use for default values.
    /// Command line options still override conf values
    #[clap(short, long)]
    pub conf: Option<String>,

    /// Input source
    /// Can be "-" for stdin, a path to file, or a syslog server defined as
    /// syslog-<tcp|udp>:<listen_address>:<listen_port>, Examples:
    /// -i -
    /// -i /var/log/system.log
    /// -i syslog-tcp:localhost:10514
    /// -i syslog-udp:localhost:10514
    #[clap(short, long)]
    pub input: Option<String>,

    /// Output destination
    /// Currently only file + stdout output is supported
    /// TODO: odbc and (forwarding) syslog client
    #[clap(short, long)]
    pub output: Option<String>,

    ///Output format. One of:
    ///     sql
    ///     csv (default)
    #[clap(short = 'f', long)]
    pub output_format: Option<String>,

    /// All of the hustlog processing happens over batches of data which
    /// are normally processed (possibly - in parallel) in a dedicated
    /// rayon thread pool. This option determines the batch size.
    ///
    /// This also determines the "window" in the incoming data stream on top
    /// of which SQL transformations are applied.
    ///
    /// Setting this to 0 implies unlimited batch size and batches will be
    /// processed based on time interval (as set by the --tick-interval option).
    /// One should consider the amount of data which needs to be processed
    /// and the available memory before setting that to 0.
    #[clap(short = 'b', long)]
    pub output_batch_size: Option<usize>,

    /// When outputting CSV rows - add a header row before the CSV output
    /// WHen outputting SQL inserts - add a DDL (create statement) before the INSERT batches
    #[clap(long)]
    pub output_add_ddl: bool,

    /// Grok Pattern name to use. This is required for any parsing to happen
    /// and must be the name of a pre-defined or custom grok pattern.
    /// Use the special --grok-list-default-patterns option to list the
    /// available default patterns or use --grok-patterns-file and/or
    /// --grok-extra-patterns to define custom ones.
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
    /// At least one column def is required
    /// A column def is a string starting with an optional "+" (indicating that the
    /// field is mandatory) followed by the column (grok lookup) name and then a column (":")
    /// followed by the type: \[+\]<column_name>\[:type_spec\]
    ///
    /// Type spec can be one of:
    ///   - str (default, can be omitted)
    ///   - int - integer value
    ///   - float - floating point value
    ///   - bool - a "true" or "false" value.
    ///   - ts:<ts_format> - timestamp type which includes the format string to be used to
    ///         parse timestamps out of the parser input strings
    ///
    /// E.g.
    ///     -s "+timestamp:ts:%Y-%m-%d %H:%M:%S.%3f%z"
    ///     -s message:str
    ///     -s pid:int
    ///     -s resp_time:float
    //      -s another_str
    #[clap(short = 's', long)]
    pub grok_schema_columns: Vec<String>,

    /// Whether to merge lines starting with whitespace with the previous ones.
    /// Useful if there are multi-line messages in the input.
    /// Note that since the grok parsing is line-based, the new lines are replaced with space.
    #[clap(short, long)]
    pub merge_multi_line: bool,

    /// How many threads to use in the "CPU-intensive work" (Rayon) thread pool.
    /// Default is 2
    #[clap(short, long)]
    pub rayon_threads: Option<usize>,

    /// How often the server "tick" event should be emitted, in seconds.
    /// Normally buffers are flushed at that time so that ends up determining the
    /// maximum delay between a message entering the system and being (batch) processed.
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
    pub async_channel_size: Option<usize>,
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

    // pub fn get_outp(&self) -> Result<DynBoxWrite, DynError> {
    //     let writer: DynBoxWrite = match &self.output {
    //         None => Box::new(BufWriter::new(io::stdout())),
    //         Some(filename) => {
    //             if filename == "-" {
    //                 Box::new(BufWriter::new(io::stdout()))
    //             } else {
    //                 Box::new(BufWriter::new(fs::File::create(filename)?))
    //             }
    //         }
    //     };
    //     Ok(writer)
    // }

    pub fn grok_list_default_patterns(&self) -> bool {
        self.grok_list_default_patterns
    }
}
