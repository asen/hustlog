use crate::output::OutputSink;
use crate::parser::{ParsedValue, ParsedValueType, ParserSchema};
use crate::ql_processor::{QlRow, QlSchema};
use crate::DynError;
use chrono::{DateTime, Datelike, FixedOffset, Timelike};
use odbc_api::buffers::{AnyColumnSliceMut, BufferDescription, BufferKind};
use odbc_api::sys::{SmallInt, Timestamp, UInteger, USmallInt};
use odbc_api::Environment;
use std::sync::Arc;

pub struct OdbcSink {
    env: Environment,
    connection_str: Arc<str>,
    schema: Arc<QlSchema>,
}

fn pv_type2buffer_kind(pv_type: &ParsedValueType) -> BufferKind {
    match pv_type {
        ParsedValueType::NullType => {
            // should never happen
            panic!("BUG: Can not convert ParsedValueType::NullType to ODBC BufferKind");
        }
        ParsedValueType::BoolType => {
            BufferKind::U8 // TODO use bit instead?
        }
        ParsedValueType::LongType => BufferKind::I64,
        ParsedValueType::DoubleType => BufferKind::F64,
        ParsedValueType::TimeType(_) => BufferKind::Timestamp,
        ParsedValueType::StrType(sz) => BufferKind::Text { max_str_len: *sz },
    }
}

fn hustlog_ts2_odbc_ts(tv: &DateTime<FixedOffset>) -> Timestamp {
    Timestamp {
        year: tv.year() as SmallInt,
        month: tv.month() as USmallInt,
        day: tv.day() as USmallInt,
        hour: tv.hour() as USmallInt,
        minute: tv.minute() as USmallInt,
        second: tv.second() as USmallInt,
        fraction: (tv.timestamp_millis() % 1000) as UInteger,
    }
}

impl OdbcSink {
    pub fn new(schema: Arc<QlSchema>, connection_str: &str) -> Result<Self, DynError> {
        let env = Environment::new()?;
        Ok(Self {
            env,
            connection_str: Arc::from(connection_str),
            schema,
        })
    }

    fn write_batch(&mut self, batch: &Vec<QlRow>) -> Result<(), DynError> {
        let conn = self
            .env
            .connect_with_connection_string(self.connection_str.as_ref())?;
        let col_names = self
            .schema
            .col_defs()
            .iter()
            .map(|&cd| Arc::clone(cd.name()))
            .collect::<Vec<_>>()
            .join(",");
        let values_str = self
            .schema
            .col_defs()
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");
        let qry = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.schema.name(),
            col_names,
            values_str
        );
        let prepared = conn.prepare(qry.as_str())?;

        let buffer_description = self
            .schema
            .col_defs()
            .iter()
            .map(|&cd| BufferDescription {
                nullable: !cd.required(),
                kind: pv_type2buffer_kind(cd.pv_type()),
            })
            .collect::<Vec<_>>();

        // // Create a columnar buffer which fits the input parameters.

        // The capacity must be able to hold at least the largest batch. We do everything in one go, so
        // we set it to the length of the input parameters.
        let capacity = batch.len();
        // Allocate memory for the array column parameters and bind it to the statement.
        let mut prebound = prepared.into_any_column_inserter(capacity, buffer_description)?;
        // Length of this batch
        prebound.set_num_rows(capacity);

        let mut row_index = 0;
        for row in batch {
            for n in 0..self.schema.col_defs().len() {
                let v = &row
                    .data()
                    .get(n)
                    .expect("BUG: Incompatible row size - sholud not happen")
                    .1;
                let col = prebound.column_mut(n);
                match col {
                    AnyColumnSliceMut::Text(mut x) => {
                        if let ParsedValue::NullVal = v.as_ref() {
                            x.set_cell(row_index, None);
                        } else if let ParsedValue::StrVal(sv) = v.as_ref() {
                            x.set_cell(row_index, Some(sv.as_bytes()))
                        } else {
                            panic!("BUG: Incompatible ODBC Text value: {:?}", v)
                        }
                    }
                    // AnyColumnSliceMut::WText(_) => {}
                    // AnyColumnSliceMut::Binary(_) => {}
                    // AnyColumnSliceMut::Date(_) => {}
                    // AnyColumnSliceMut::Time(_) => {}
                    AnyColumnSliceMut::Timestamp(x) => {
                        if let ParsedValue::TimeVal(tv) = v.as_ref() {
                            x[row_index] = hustlog_ts2_odbc_ts(tv);
                        } else {
                            panic!("BUG: Incompatible ODBC Timestamp value: {:?}", v)
                        }
                    }
                    AnyColumnSliceMut::F64(x) => {
                        if let ParsedValue::DoubleVal(d) = v.as_ref() {
                            x[row_index] = *d;
                        } else {
                            panic!("BUG: Incompatible ODBC Text value: {:?}", v)
                        }
                    }
                    // AnyColumnSliceMut::F32(_) => {}
                    // AnyColumnSliceMut::I8(_) => {}
                    // AnyColumnSliceMut::I16(_) => {}
                    // AnyColumnSliceMut::I32(_) => {}
                    AnyColumnSliceMut::I64(x) => {
                        if let ParsedValue::LongVal(i) = v.as_ref() {
                            x[row_index] = *i;
                        } else {
                            panic!("BUG: Incompatible ODBC I64 value: {:?}", v)
                        }
                    }
                    AnyColumnSliceMut::U8(x) => {
                        if let ParsedValue::BoolVal(b) = v.as_ref() {
                            x[row_index] = if *b { 1 } else { 0 };
                        } else {
                            panic!("BUG: Incompatible ODBC U8 (bool) value: {:?}", v)
                        }
                    }
                    // AnyColumnSliceMut::Bit(_) => {}
                    // AnyColumnSliceMut::NullableDate(_) => {}
                    // AnyColumnSliceMut::NullableTime(_) => {}
                    AnyColumnSliceMut::NullableTimestamp(mut x) => {
                        let (values, _is) = x.raw_values();
                        if let ParsedValue::TimeVal(tv) = v.as_ref() {
                            values[row_index] = hustlog_ts2_odbc_ts(tv);
                        }
                    }
                    AnyColumnSliceMut::NullableF64(mut x) => {
                        let (values, _is) = x.raw_values();
                        if let ParsedValue::DoubleVal(d) = v.as_ref() {
                            values[row_index] = *d;
                        }
                    }
                    // AnyColumnSliceMut::NullableF32(_) => {}
                    // AnyColumnSliceMut::NullableI8(_) => {}
                    // AnyColumnSliceMut::NullableI16(_) => {}
                    // AnyColumnSliceMut::NullableI32(_) => {}
                    AnyColumnSliceMut::NullableI64(mut x) => {
                        let (values, _is) = x.raw_values();
                        if let ParsedValue::LongVal(d) = v.as_ref() {
                            values[row_index] = *d;
                        }
                    }
                    AnyColumnSliceMut::NullableU8(mut x) => {
                        let (values, _is) = x.raw_values();
                        if let ParsedValue::BoolVal(d) = v.as_ref() {
                            values[row_index] = if *d { 1 } else { 0 };
                        }
                    }
                    // AnyColumnSliceMut::NullableBit(_) => {}
                    _ => panic!("BUG: Unsupported AnyColumnSliceMut variant"),
                }
            }
            row_index += 1;
        }
        prebound.execute()?;
        Ok(())
    }
}

impl OutputSink for OdbcSink {
    fn output_header(&mut self) -> Result<(), DynError> {
        todo!()
    }

    fn flush(&mut self) -> Result<(), DynError> {
        //TODO ???
        Ok(())
    }

    fn output_batch(&mut self, batch: Vec<QlRow>) -> Result<(), DynError> {
        // if let Err(err) = self.write_batch(&batch) {
        //     // TODO local retry queue ?
        //     return Err(err)
        // }
        // Ok(())
        self.write_batch(&batch)
    }

    fn shutdown(&mut self) -> Result<(), DynError> {
        // TODO close any open connections?
        Ok(())
    }
}
