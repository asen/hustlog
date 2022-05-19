use crate::query_processor::QlRow;
use crate::{ParsedValue, ParserSchema};
use std::error::Error;
use std::io::Write;

fn output_value_for_sql(pv: &ParsedValue, outp: &mut Box<dyn Write>) -> Result<(), Box<dyn Error>> {
    match pv {
        ParsedValue::NullVal => {
            outp.write("NULL".as_bytes())?;
        }
        ParsedValue::BoolVal(b) => {
            if *b {
                outp.write("TRUE".as_bytes())?;
            } else {
                outp.write("FALSE".as_bytes())?;
            }
        }
        ParsedValue::LongVal(n) => {
            outp.write(n.to_string().as_bytes())?;
        }
        ParsedValue::DoubleVal(d) => {
            outp.write(d.to_string().as_bytes())?;
        }
        ParsedValue::TimeVal(t) => {
            outp.write("'".as_bytes())?;
            outp.write(t.to_string().as_bytes())?;
            outp.write("'".as_bytes())?;
        }
        ParsedValue::StrVal(s) => {
            outp.write("'".as_bytes())?;
            // escape quotes - replace all single quotes with two single quotes
            outp.write(s.replace("'", "''").as_bytes())?;
            // if s.contains('\'') {
            //     for x in s.split('\'') {
            //         outp.write(x.as_bytes())?;
            //         outp.write("''".as_bytes())?;
            //     }
            // } else {
            //     outp.write(s.as_bytes())?;
            // }
            outp.write("'".as_bytes())?;
        }
    }
    Ok(())
}

pub struct BatchedInserts {
    schema: Box<dyn ParserSchema>,
    batch_size: usize,
    buf: Vec<QlRow>,
    outp: Box<dyn Write>,
}

impl BatchedInserts {
    pub fn new(schema: Box<dyn ParserSchema>, batch_size: usize, outp: Box<dyn Write>) -> Self {
        Self {
            schema,
            batch_size,
            buf: Vec::new(),
            outp,
        }
    }

    pub fn print_header_str(&mut self, s: &str) -> Result<(), Box<dyn Error>> {
        self.outp.write(s.as_bytes())?;
        self.outp.write("\n".as_bytes())?;
        Ok(())
    }

    pub fn add_to_batch(&mut self, row: QlRow) -> Result<(), Box<dyn Error>> {
        self.buf.push(row);
        if self.buf.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), Box<dyn Error>> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.outp.write("INSERT INTO ".as_bytes())?;
        self.outp.write(self.schema.name().as_bytes())?;
        self.outp.write(" (".as_bytes())?;
        let cn = self
            .schema
            .col_defs()
            .iter()
            .map(|&x| x.name().as_ref())
            .collect::<Vec<_>>()
            .join(",");
        self.outp.write(cn.as_bytes())?;
        self.outp.write(")\n VALUES \n".as_bytes())?;
        //let cd_vec = self.schema.col_defs();
        let mut rit = self.buf.iter().peekable();
        while let Some(row) = rit.next() {
            self.outp.write("(".as_bytes())?;
            let mut cit = row.data().iter().peekable();
            while let Some((_, pv)) = cit.next() {
                output_value_for_sql(pv, &mut self.outp)?;
                if cit.peek().is_some() {
                    self.outp.write(",".as_bytes())?;
                }
            }
            self.outp.write(")".as_bytes())?;
            if rit.peek().is_some() {
                self.outp.write(",".as_bytes())?;
            } else {
                self.outp.write(";".as_bytes())?;
            }
            self.outp.write("\n".as_bytes())?;
        }
        self.buf.clear();
        Ok(())
    }
}

// pub fn raw_to_sql(
//     mut itable: Box<dyn QlInputTable>,
//     outp: Box<dyn Write>,
// ) -> Result<(), Box<dyn Error>> {
//     let ql_schema = itable.ql_schema().clone();
//     let bs: Box<&dyn ParserSchema> = Box::new(&ql_schema as &dyn ParserSchema);
//     let mut sql_inserts = BatchedInserts::new(&bs, 100, outp);
//     while let Some(pm) = itable.read_row()? {
//         sql_inserts.add_to_batch(pm)?;
//     }
//     sql_inserts.flush()?;
//     Ok(())
// }

#[cfg(test)]
mod test {
    use crate::sqlgen::sql_create::SqlCreateSchema;
    use crate::sqlgen::BatchedInserts;
    use crate::{
        test_syslog_schema, ParserIteratorInputTable, ParserSchema, QlInputTable, QlSchema,
    };
    use std::error::Error;
    use std::io;
    use std::io::{BufRead, BufReader, BufWriter, Write};

    pub fn ql_table_to_sql(
        inp: &mut Box<dyn QlInputTable>,
        outp: Box<dyn Write>,
        batch_size: usize,
    ) -> Result<(), Box<dyn Error>> {
        let ql_schema: QlSchema = inp.ql_schema().clone();
        let ql_schema: Box<dyn ParserSchema> = Box::new(ql_schema);
        let mut sql_inserts = BatchedInserts::new(ql_schema, batch_size, outp);
        while let Some(row) = inp.read_row()? {
            sql_inserts.add_to_batch(row)?;
        }
        sql_inserts.flush()?;
        Ok(())
    }

    const LINES1: &str = "Apr 22 02:34:54 actek-mac login[49532]: USER_PROCESS: 49532 ttys000\n\
        Apr 22 04:42:04 actek-mac syslogd[103]: ASL Sender Statistics\n\
        Apr 22 04:43:04 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:43:34 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:48:50 actek-mac login[49531]: USER_PROCESS: 49532 ttys000\n\
        ";

    fn get_logger() -> Box<dyn Write> {
        Box::new(BufWriter::new(std::io::stderr()))
    }
    fn get_stdout() -> Box<dyn Write> {
        Box::new(BufWriter::new(io::stdout()))
    }

    #[test]
    fn test_sql_gen1() {
        let schema = test_syslog_schema();
        let ql_schema = QlSchema::from(&schema);
        let ddl = SqlCreateSchema::from_ql_schema(&ql_schema);
        let s = ddl.get_create_sql();
        let mut out = get_stdout();
        out.write(s.as_bytes()).unwrap();
        out.write("\n".as_bytes()).unwrap();
        let rdr: Box<dyn BufRead> = Box::new(BufReader::new(LINES1.as_bytes()));
        let pit = schema
            .create_parser_iterator(rdr, false, get_logger())
            .unwrap();
        let itbl = ParserIteratorInputTable::new(pit, QlSchema::from(&schema));
        let mut itbl_box = Box::new(itbl) as Box<dyn QlInputTable>;
        ql_table_to_sql(&mut itbl_box, out, 2).unwrap();
    }
}
