use crate::parser::{DynParserSchema, ParsedValue};
use crate::ql_processor::QlRow;
use crate::{DynBoxWrite, DynError};
use std::io::Write;

const SQL_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

fn output_value_for_sql(pv: &ParsedValue, outp: &mut DynBoxWrite) -> Result<(), DynError> {
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
            outp.write(t.format(SQL_TIME_FORMAT).to_string().as_bytes())?;
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
    schema: DynParserSchema,
    batch_size: usize,
    buf: Vec<QlRow>,
    outp: DynBoxWrite,
}

impl BatchedInserts {
    pub fn new(schema: DynParserSchema, batch_size: usize, outp: DynBoxWrite) -> Self {
        Self {
            schema,
            batch_size,
            buf: Vec::new(),
            outp,
        }
    }

    pub fn print_header_str(&mut self, s: &str) -> Result<(), DynError> {
        self.outp.write(s.as_bytes())?;
        self.outp.write("\n".as_bytes())?;
        Ok(())
    }

    pub fn add_to_batch(&mut self, row: QlRow) -> Result<(), DynError> {
        self.buf.push(row);
        if self.buf.len() >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), DynError> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.outp.write("INSERT INTO ".as_bytes())?;
        self.outp.write(self.schema.output_name().as_bytes())?;
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

#[cfg(test)]
mod tests {
    use crate::parser::{test_syslog_schema, DynParserSchema};
    use crate::ql_processor::tests::input_to_table_test;
    use crate::ql_processor::{QlInputTable, QlSchema};
    use crate::sqlgen::sql_create::SqlCreateSchema;
    use crate::sqlgen::BatchedInserts;
    use crate::{DynBoxWrite, DynError};
    use log::info;
    use std::io;
    use std::io::{BufWriter, Write};
    use std::sync::Arc;

    fn ql_table_to_sql_test(
        inp: &mut Box<dyn QlInputTable>,
        outp: DynBoxWrite,
        batch_size: usize,
    ) -> Result<(), DynError> {
        let ql_schema: DynParserSchema = inp.ql_schema().clone();
        // let ql_schema: DynParserSchema = Arc::from();
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
        Apr 22 04:48:50 actek-mac login[49531]: USER_PROCESS: 49532 ttys000\n";

    pub struct DummyWrite {
        pub written: usize,
        pub flushed: usize,
    }

    impl DummyWrite {
        pub fn new() -> Self {
            Self {
                written: 0,
                flushed: 0,
            }
        }
    }

    impl Write for DummyWrite {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let len = buf.len();
            self.written += len;
            info!("DummyWrite: ({}): {}", len, String::from_utf8_lossy(buf));
            Ok(len)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushed += 1;
            info!(
                "DummyWrite: flushed: {} written: {}",
                self.flushed, self.written
            );
            Ok(())
        }
    }

    fn get_dummy_outp() -> DynBoxWrite {
        Box::new(BufWriter::new(DummyWrite::new()))
    }

    #[test]
    fn test_sql_gen1() {
        let schema = test_syslog_schema();
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let ddl = SqlCreateSchema::from_ql_schema(&ql_schema, Arc::from(""), Arc::from(""));
        let s = ddl.get_create_sql();
        let mut out = get_dummy_outp();
        out.write(s.as_bytes()).unwrap();
        out.write("\n".as_bytes()).unwrap();
        let itbl = input_to_table_test(LINES1, schema);
        let mut itbl_box = Box::new(itbl) as Box<dyn QlInputTable>;
        ql_table_to_sql_test(&mut itbl_box, out, 2).unwrap();
    }
}
