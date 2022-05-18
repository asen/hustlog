// Copyright 2022 Asen Lazarov

extern crate core;

use std::error::Error;
use std::io::Write;

use clap::Parser;

use conf::*;
use output::*;
use parser::*;

use crate::query_processor::{ParserIteratorInputTable, process_sql, QlInputTable, QlMemTable, QlSchema};
use crate::sqlgen::SqlCreateSchema;

mod conf;
mod parser;
mod query_processor;
mod sqlgen;
mod output;

// fn print_result_table(
//     mut rt: Box<dyn QlInputTable>,
//     out: &mut Box<dyn Write>,
//     sep: &str,
// ) -> Result<(), Box<dyn Error>> {
//     let mut i = 0;
//     while let Some(r) = rt.read_row()? {
//         i += 1;
//         let istr = format!("({}) ", i);
//         out.write(istr.as_bytes())?;
//         out.write("COMPUTED:".as_bytes())?;
//         for (cn, cv) in r.data() {
//             out.write(sep.as_bytes())?;
//             out.write(cn.as_bytes())?;
//             out.write("=".as_bytes())?;
//             out.write(cv.to_rc_str().as_bytes())?;
//         }
//
//         out.write("\nRAW: ".as_bytes())?;
//         if r.raw().is_some() {
//             out.write(r.raw().as_ref().unwrap().as_str().as_bytes())?;
//         };
//         out.write("\n".as_bytes())?;
//     }
//     Ok(())
// }

fn main_print_default_patterns(mut outp: Box<dyn Write>) -> Result<(), Box<dyn Error>> {
    for (p, s) in GrokParser::default_patterns() {
        outp.write(p.as_bytes())?;
        outp.write(" ".as_bytes())?;
        outp.write(s.as_bytes())?;
        outp.write("\n".as_bytes())?;
    }
    return Ok(());
}

fn get_output_sink(ofrmt: OutputFormat,
                   add_ddl: bool,
                   outp_batch_size: usize,
                   ql_schema: QlSchema,
                   outp: Box<dyn Write>) -> Box<dyn OutputSink> {
    match ofrmt {
        OutputFormat::DEFAULT => {
            Box::new(CsvOutput::new(ql_schema, outp, add_ddl))
        }
        OutputFormat::SQL => {
            Box::new( AnsiSqlOutput::new(
                ql_schema,
                add_ddl,
                outp_batch_size,
                outp,
            ) )
        }
    }
}

fn main_process_pit(
    schema: &GrokSchema,
    pit: ParserIterator,
    sql: Option<&String>,
    outp_format: OutputFormat,
    outp_batch_size: usize,
    add_ddl: bool,
    outp: Box<dyn Write>,
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

    let mut out_sink = get_output_sink(
        outp_format,
        add_ddl,
        outp_batch_size,
        query_output.ql_schema().clone(),
        outp,
    );
    out_sink.output_header()?;
    while let Some(r) = query_output.read_row()? {
        out_sink.output_row(r)?;
    }
    out_sink.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: MyArgs = MyArgs::parse();
    //println!("ARGS: {:?}", args);
    if args.grok_list_default_patterns() {
        let outp: Box<dyn Write> = args.get_outp()?;
        return main_print_default_patterns(outp);
    }
    // no conf/schema before this point, no args after it.
    let conf = HustlogConfig::new(args)?;
    let outp: Box<dyn Write> = conf.get_outp()?;
    let log = conf.get_logger();
    let rdr = conf.get_buf_read()?;
    //println!("{:?}", args);
    let schema = conf.get_grok_schema();
    //println!("{:?}", schema);
    let pit = schema
        .create_parser_iterator(rdr, conf.merge_multi_line(), log)?;
    main_process_pit(
        &schema,
        pit,
        conf.query().as_ref(),
        conf.output_format(),
        conf.output_batch_size(),
        conf.output_add_ddl(),
        outp
    )
}

#[test]
fn verify_app() {
    use clap::CommandFactory;
    MyArgs::command().debug_assert()
}
