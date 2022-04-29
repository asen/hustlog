// Copyright 2022 Asen Lazarov

extern crate core;

use std::error::Error;
use std::io::{BufRead, Write};

use clap::Parser;

use conf::*;
use parser::*;

use crate::query_processor::{process_sql_one_shot, ResultTable};

mod conf;
mod parser;
mod query;
mod query_processor;

fn my_parse(
    raw: RawMessage,
    parser: &GrokParser,
    outp: &mut Box<dyn Write>,
    log: &mut Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    let parsed: Result<ParsedMessage, LogParseError> = parser.parse(raw);
    if parsed.is_ok() {
        let ok: ParsedMessage = parsed.unwrap();
        outp.write(
            format!("PARSED: {:?} RAW: {:?}\n", &ok.get_parsed(), &ok.get_raw()).as_bytes(),
        )?;
    } else {
        let err: LogParseError = parsed.err().unwrap();
        log.write(
            format!(
                "ERROR:: {} RAW: {}\n",
                err.get_desc(),
                err.get_raw().as_str()
            )
            .as_bytes(),
        )?;
    }
    Ok(())
}

fn main_test(
    rdr: Box<dyn BufRead>,
    schema: &GrokSchema,
    use_line_merger: bool,
    outp: &mut Box<dyn Write>,
    log: &mut Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    let parser = GrokParser::new(schema.clone())?;
    let mut line_merger = if use_line_merger {
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
            my_parse(raw.unwrap(), &parser, outp, log)?;
        }
    }
    if line_merger.is_some() {
        let raw = line_merger.unwrap().flush();
        if raw.is_some() {
            my_parse(raw.unwrap(), &parser, outp, log)?;
        }
    }
    Ok(())
}

fn print_result_table(
    rt: &ResultTable,
    out: &mut Box<dyn Write>,
    sep: &str,
) -> Result<(), Box<dyn Error>> {
    for (i, r) in rt.get_rows().iter().enumerate() {
        let istr = format!("({}) ", i);
        out.write(istr.as_bytes())?;
        out.write("COMPUTED:".as_bytes())?;
        for (cn, cv) in r.get_computed() {
            out.write(sep.as_bytes())?;
            out.write(cn.as_bytes())?;
            out.write("=".as_bytes())?;
            out.write(cv.to_rc_str().as_bytes())?;
        }
        out.write("\nRAW: ".as_bytes())?;
        out.write(r.get_raw().as_str().as_bytes())?;
        out.write("\n".as_bytes())?;
    }
    Ok(())
}

fn main_sql(
    rdr: Box<dyn BufRead>,
    schema: &GrokSchema,
    use_line_merger: bool,
    query: &str,
    outp: Box<dyn Write>,
    log: Box<dyn Write>,
) -> Result<(), Box<dyn Error>> {
    let res = process_sql_one_shot(rdr, schema, use_line_merger, query, log)?;
    print_result_table(&res, &mut Box::new(outp), ", ")
}

fn main_print_default_patterns(mut outp: Box<dyn Write>) -> Result<(), Box<dyn Error>> {
    for (p, s) in GrokParser::default_patterns() {
        outp.write(p.as_bytes())?;
        outp.write(" ".as_bytes())?;
        outp.write(s.as_bytes())?;
        outp.write("\n".as_bytes())?;
    }
    return Ok(());
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: MyArgs = MyArgs::parse();
    //println!("ARGS: {:?}", args);
    let mut log = args.get_logger();
    let rdr = args.get_buf_read()?;
    let mut outp: Box<dyn Write> = args.get_outp()?;
    if args.grok_list_default_patterns() {
        return main_print_default_patterns(outp);
    }
    //println!("{:?}", args);
    let schema = args.get_grok_schema()?;
    //println!("{:?}", schema);
    if schema.is_some() {
        if let Some(q) = args.query() {
            main_sql(
                rdr,
                &schema.unwrap(),
                args.merge_multi_line(),
                q,
                Box::new(outp),
                Box::new(log),
            )
        } else {
            main_test(
                rdr,
                &schema.unwrap(),
                args.merge_multi_line(),
                &mut outp,
                &mut log,
            )
        }
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
