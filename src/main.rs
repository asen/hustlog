// Copyright 2022 Asen Lazarov

extern crate core;

use std::error::Error;
use std::io::Write;

use clap::Parser;

use conf::*;
use parser::*;

use crate::query_processor::{ParserIteratorInputTable, process_sql, QlInputTable, QlMemTable, QlSchema};
use crate::sqlgen::{ql_table_to_sql, SqlCreateSchema};

mod conf;
mod parser;
mod query_processor;
mod sqlgen;

fn print_result_table(
    mut rt: Box<dyn QlInputTable>,
    out: &mut Box<dyn Write>,
    sep: &str,
) -> Result<(), Box<dyn Error>> {
    let mut i = 0;
    while let Some(r) = rt.read_row()? {
        i += 1;
        let istr = format!("({}) ", i);
        out.write(istr.as_bytes())?;
        out.write("COMPUTED:".as_bytes())?;
        for (cn, cv) in r.data() {
            out.write(sep.as_bytes())?;
            out.write(cn.as_bytes())?;
            out.write("=".as_bytes())?;
            out.write(cv.to_rc_str().as_bytes())?;
        }

        out.write("\nRAW: ".as_bytes())?;
        if r.raw().is_some() {
            out.write(r.raw().as_ref().unwrap().as_str().as_bytes())?;
        };
        out.write("\n".as_bytes())?;
    }
    Ok(())
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


fn main_process_pit(
    schema: &GrokSchema,
    pit: ParserIterator,
    sql: Option<&String>,
    outp_format: OutputFormat,
    outp_batch_size: usize,
    add_ddl: bool,
    mut outp: Box<dyn Write>,
)  -> Result<(), Box<dyn Error>> {
    // consume the parser iterator
    // if sql is provided -> apply it
    let mut query_output =  if sql.is_some() {
        let ss: &str = &sql.unwrap().as_ref();
        let mut sql_res = QlMemTable::new(&QlSchema::from(&schema));
        process_sql(schema, pit, ss, Box::new(&mut sql_res))?;
        Box::new(sql_res ) as Box<dyn QlInputTable>
    } else {
        // just use the iterator as input table
        let itbl = ParserIteratorInputTable::new(
            pit,
            QlSchema::from(schema)
        );
        Box::new(itbl ) as Box<dyn QlInputTable>
    };

    match outp_format {
        OutputFormat::DEFAULT => {
            print_result_table(query_output, &mut outp, ",")?;
        }
        OutputFormat::SQL => {
            if add_ddl {
                let ddl = SqlCreateSchema::from_grok_schema(&schema);
                let s = ddl.get_create_sql();
                outp.write(s.as_bytes())?;
                outp.write("\n".as_bytes())?;
            }
            ql_table_to_sql(&mut query_output, outp, outp_batch_size)?;
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: MyArgs = MyArgs::parse();
    //println!("ARGS: {:?}", args);
    let outp: Box<dyn Write> = args.get_outp()?;
    if args.grok_list_default_patterns() {
        return main_print_default_patterns(outp);
    }
    let log = args.get_logger();
    let rdr = args.get_buf_read()?;
    //println!("{:?}", args);
    let schema_opt = args.get_grok_schema()?;
    //println!("{:?}", schema);
    if schema_opt.is_some() {
        let schema = schema_opt.unwrap();
        let pit = schema
            .create_parser_iterator(rdr, args.merge_multi_line(), log)?;
        main_process_pit(
            &schema,
            pit,
            args.query().as_ref(),
            args.output_format().unwrap_or(OutputFormat::DEFAULT),
            args.output_batch_size(),
            args.output_add_ddl(),
            outp
        )
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
