// Copyright 2022 Asen Lazarov

use clap::Parser;

use crate::file_processor::file_process_main;
use crate::parser::GrokParser;
use crate::syslog_server::server_main;
use conf::*;

mod async_pipeline;
mod conf;
mod file_processor;
mod output;
mod parser;
mod ql_processor;
mod sqlgen;
mod syslog_server;

fn tokio_server_main(hc: HustlogConfig) -> Result<(), DynError> {
    // let mut rt = tokio::runtime::Runtime::new().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { server_main(hc).await })
}

fn tokio_file_process_main(hc: HustlogConfig) -> Result<(), DynError> {
    // let mut rt = tokio::runtime::Runtime::new().unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { file_process_main(hc).await })
}

fn main_print_default_patterns() -> Result<(), DynError> {
    for (p, s) in GrokParser::default_patterns() {
        println!("{} {}", p, s);
    }
    return Ok(());
}

fn main() -> Result<(), DynError> {
    let args: MyArgs = MyArgs::parse();
    //println!("ARGS: {:?}", args);
    if args.grok_list_default_patterns() {
        return main_print_default_patterns();
    }
    // no conf/schema before this point, no args after it.
    let conf = HustlogConfig::new(args)?;
    env_logger::init(); // TODO use conf?

    let is_syslog_server = conf.input_is_syslog_server();
    if is_syslog_server {
        return tokio_server_main(conf);
    }
    tokio_file_process_main(conf)
}

#[test]
fn verify_app() {
    use clap::CommandFactory;
    MyArgs::command().debug_assert()
}
