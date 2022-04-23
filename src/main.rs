// Copyright 2022 Asen Lazarov

mod conf;
mod parser;
mod filter;

use clap::Parser;
use conf::*;
use parser::*;
use std::error::Error;
use std::io::{BufRead, Write};


fn my_parse(raw: RawMessage, parser: &GrokParser,
            outp: &mut Box<dyn Write>, log: &mut Box<dyn Write>) -> Result<(), Box<dyn Error>>{
    let parsed: Result<ParsedMessage, RawMessage> = parser.parse(raw);
    if parsed.is_ok() {
        let ok: ParsedMessage = parsed.unwrap();
        outp.write(
            format!("PARSED: {:?} RAW: {:?}\n", &ok.get_parsed(), &ok.get_raw()).as_bytes(),
        )?;
    } else {
        log.write(format!("ERROR: RAW: {:?}\n", parsed.err()).as_bytes())?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: MyArgs = MyArgs::parse();
    println!("ARGS: {:?}", args);
    let mut log = args.get_logger();
    let rdr = args.get_buf_read()?;
    let mut outp: Box<dyn Write> = args.get_outp()?;
    if args.grok_list_default_patterns() {
        for (p,s) in GrokParser::default_patterns() {
            outp.write(p.as_bytes())?;
            outp.write(" ".as_bytes())?;
            outp.write(s.as_bytes())?;
            outp.write("\n".as_bytes())?;
        }
        return Ok(())
    }
    //println!("{:?}", args);
    let schema = args.get_grok_schema()?;
    //println!("{:?}", schema);
    if schema.is_some() {
        let parser = GrokParser::new(schema.unwrap())?;
        let mut line_merger = if args.merge_multi_line() {
            Some(SpaceLineMerger::new())
        } else {
            None
        };
        for ln in rdr.lines() {
            let s = ln?;
            let raw = if line_merger.is_some() {
                line_merger.as_mut().unwrap().add_line(s)
            } else {
                Some(RawMessage::new(s))
            };
            if raw.is_some() {
                my_parse(raw.unwrap(), &parser, &mut outp, &mut log)?;
            }
        }
        if line_merger.is_some() {
            let raw = line_merger.unwrap().flush();
            if raw.is_some() {
                my_parse(raw.unwrap(), &parser, &mut outp, &mut log)?;
            }
        }
        Ok(())
    } else {
        Err(Box::new(ConfigError::new(
            "Missing grok pattern or column defs",
        )))
    }
}

#[test]
fn verify_app() {
    use clap::CommandFactory;
    MyArgs::command().debug_assert()
}
