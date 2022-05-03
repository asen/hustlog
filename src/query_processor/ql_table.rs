use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io::{BufRead, Write};
use std::rc::Rc;
use sqlparser::ast::{Expr, Value};
use crate::{GrokParser, GrokSchema, LineMerger, ParsedValue, ParseErrorProcessor, ParserIterator, RawMessage, SpaceLineMerger};
use crate::query::SqlSelectQuery;
use crate::query_processor::ql_agg_expr::AggExpr;
use crate::query_processor::ql_eval_expr::{eval_expr, eval_integer_expr, LazyContext};
use crate::query_processor::ql_schema::{get_res_cols, QlRow, QlRowContext, QlSchema, QlSelectCols, QlSelectItem};
use crate::query_processor::QueryError;

pub trait QlInputTable {
    fn read_row(&mut self) -> Result<Option<QlRow>,Box<dyn Error>>;
}

pub trait QlOutputTable {
    fn write_row(&mut self, row: QlRow) -> Result<(),Box<dyn Error>>;
    fn num_written(&self) -> usize;
}


pub struct QlMemTable {
    //schema: QlSchema,
    buf: VecDeque<QlRow>,
    written: usize,
}

impl QlMemTable {
    pub fn new(
        //schema: &QlSchema
    ) -> Self {
        Self {
            //schema: schema.clone(),
            buf: VecDeque::new(),
            written: 0,
        }
    }

    pub fn get_rows(&self) -> &VecDeque<QlRow> {
        &self.buf
    }

    // pub fn get_schema(&self) -> &QlSchema {
    //     &self.schema
    // }
}

impl QlOutputTable for QlMemTable {
    fn write_row(&mut self, row: QlRow) -> Result<(), Box<dyn Error>> {
        self.buf.push_back(row);
        self.written += 1;
        Ok(())
    }

    fn num_written(&self) -> usize {
        self.written
    }
}

impl QlInputTable for QlMemTable {
    fn read_row(&mut self) -> Result<Option<QlRow>, Box<dyn Error>> {
        Ok(self.buf.pop_front())
    }
}

struct QlParserIteratorInputTable<'a> {
    schema: &'a QlSchema,
    pit: ParserIterator,
}

impl QlInputTable for QlParserIteratorInputTable<'_> {
    fn read_row(&mut self) -> Result<Option<QlRow>, Box<dyn Error>> {
        let pit_next = self.pit.next();
        if pit_next.is_none() {
            return Ok(None)
        }
        let pm = pit_next.unwrap();

        let ret = QlRow::from_parsed_message(pm, self.schema);
        Ok(Some(ret))
    }
}

#[derive(Eq, Hash, PartialEq)]
struct QlGroupByKey(Vec<(Rc<str>,ParsedValue)>);

struct QlGroupByContext {
    //gb_key_ixes: Vec<usize>,
    by_gb_key: HashMap<Rc<QlGroupByKey>, Vec<Box<dyn AggExpr>>>,
    keys_ordered: Vec<Rc<QlGroupByKey>>,
}

impl QlGroupByContext {
    pub fn new(
        //gb_key_ixes: Vec<usize>
        ) -> Self {
        Self {
            //gb_key_ixes,
            by_gb_key: HashMap::new(),
            keys_ordered: Vec::new(),
        }
    }

    pub fn add_row(&mut self,
                   gb_key: QlGroupByKey,
                   empty_agg_exprs: Vec<Box<dyn AggExpr>>,
                   ctx: &QlRowContext,
                   dctx: &mut LazyContext,
                   gb_ixes: &Vec<usize>
    ) -> Result<(), QueryError> {
        let gb_key_ref = Rc::new(gb_key);
        if !&self.by_gb_key.contains_key(&gb_key_ref) {
            let _ = &self.keys_ordered.push(gb_key_ref.clone());
            let _ = &self.by_gb_key.insert(gb_key_ref.clone(), empty_agg_exprs);
        }
        let agg_exprs: &mut Vec<Box<dyn AggExpr>> = self.by_gb_key.get_mut(&gb_key_ref).unwrap();
        for ae in agg_exprs.iter_mut() {
            ae.add_context(ctx, dctx, gb_ixes)?;
        }
        Ok(())
    }

    fn output_to_table(&self, sel_cols: &QlSelectCols,
                       outp: &mut Box<&mut dyn QlOutputTable>) -> Result<(), Box<dyn Error>> {
        for gb_key_ref in &self.keys_ordered {
            let agg_exprs = self.by_gb_key.get(gb_key_ref).unwrap();
            let mut outp_row = Vec::with_capacity(sel_cols.cols().len());

            let mut lazy_ex_iter = gb_key_ref.0.iter();
            let mut agg_exprs_iter = agg_exprs.iter();
            for sc in sel_cols.cols() {
                match sc {
                    QlSelectItem::RawMessage => {
                        return Err(Box::new(QueryError::unexpected(
                            "Can not use aggregate functions combined with wildcard/raw message specifier")))
                    }
                    QlSelectItem::LazyExpr(_) => {
                        let pv = lazy_ex_iter.next().unwrap().clone();
                        outp_row.push(pv)
                    }
                    QlSelectItem::AggExpr(_) => {
                        let ae = agg_exprs_iter.next().unwrap();
                        let pv = ae.eval()?;
                        outp_row.push((ae.name(), pv))
                    }
                }
            };
            outp.write_row(QlRow::new(None, outp_row))?;
        }
        Ok(())
    }
}

// impl QlOutputTable for QlGroupByContext {
//     fn output_row(&mut self, row: QlRow) -> Result<(), Box<dyn Error>> {
//         let gb_key = Vec::new();
//         for ix in self.gb_key_ixes {
//             let pd = row.data()
//                 .get(ix)
//                 .map(|(_,v)| { v.clone() })
//                 .ok_or(Box::new(QueryError::unexpected("gb_key_ixes has an invalid index")));
//         }
//         Ok(())
//     }
// }


fn get_limit(
    qry: &SqlSelectQuery,
    empty_lazy_context: &mut LazyContext,
) -> Result<Option<usize>, QueryError> {
    if qry.get_limit().is_some() {
        let num = eval_integer_expr(
            qry.get_limit().unwrap(),
            &QlRowContext::empty(),
            empty_lazy_context,
            "limit",
        )?;
        Ok(Some(num as usize))
    } else {
        Ok(None)
    }
}

fn get_offset(
    qry: &SqlSelectQuery,
    empty_lazy_context: &mut LazyContext,
) -> Result<i64, QueryError> {
    if qry.get_offset().is_some() {
        let num = eval_integer_expr(
            qry.get_offset().unwrap(),
            &QlRowContext::empty(),
            empty_lazy_context,
            "offset",
        )?;
        Ok(num)
    } else {
        Ok(0)
    }
}


pub fn eval_query(
    select_c: &QlSelectCols,
    where_c: &Expr,
    limit: Option<usize>,
    offset: i64,
    group_by_exprs: &Vec<usize>,
    inp: &mut Box<&mut dyn QlInputTable>,
    outp: &mut Box<&mut dyn QlOutputTable>
) -> Result<(),Box<dyn Error>> {
    let has_agg = select_c.validate_group_by_cols(group_by_exprs)?;
    let needs_raw = select_c.has_raw_message();
    let mut gb_context = QlGroupByContext::new();
    let mut my_offset = offset;
    while let Some(irow) = inp.read_row()? {
        let raw: Option<RawMessage> = if needs_raw { irow.raw().clone() } else { None };
        let static_ctx = QlRowContext::from_row(&irow);
        let mut lazy_ctx = select_c.lazy_context();
        let where_result = eval_expr(where_c, &static_ctx, &mut lazy_ctx)?
            .as_bool().unwrap_or(false);
        if where_result {
            //row matches
            // eval our lazy contexts
            let mut outp_vals: Vec<(Rc<str>, ParsedValue)> = Vec::new();
            let lazy_exp_vec = select_c.lazy_exprs();
            for le in lazy_exp_vec {
                let pv = lazy_ctx
                    .get_value(le.name(), &static_ctx)?.unwrap_or(ParsedValue::NullVal);
                outp_vals.push((le.name().clone(),pv));
            }
            if has_agg {
                // handle group by stuff
                let agg_exprs = select_c.agg_exprs();
                gb_context.add_row(
                    QlGroupByKey(outp_vals), agg_exprs,
                    &static_ctx,
                    &mut lazy_ctx,
                    group_by_exprs
                )?;
            } else {
                //generate the output row
                if my_offset > 0 {
                    my_offset -= 1
                } else {
                    let orow = QlRow::new(raw, outp_vals);
                    outp.write_row(orow)?;
                    if limit.is_some() && outp.num_written() >= limit.unwrap() {
                        break;
                    }
                }
            }
        }
    }
    if has_agg {
        // TODO handle limit/offset
        gb_context.output_to_table(&select_c, outp)?;
    }
    Ok(())
}


pub fn process_sql(
    rdr: Box<dyn BufRead>,
    schema: &GrokSchema,
    use_line_merger: bool,
    query: &str,
    log: Box<dyn Write>,
) -> Result<Box<QlMemTable>, Box<dyn Error>> {
    let qry = SqlSelectQuery::new(query)?;
    let parser = GrokParser::new(schema.clone())?;
    let line_merger: Option<Box<dyn LineMerger>> = if use_line_merger {
        Some(Box::new(SpaceLineMerger::new()))
    } else {
        None
    };
    let eror_processor = ParseErrorProcessor::new(log);
    let pit = ParserIterator::new(
        Box::new(parser),
        line_merger,
        Box::new(rdr.lines().into_iter()),
        eror_processor,
    );
    let ql_schema = QlSchema::from(schema);
    let mut in_table = QlParserIteratorInputTable {
        schema: &ql_schema,
        pit
    };
    let mut in_table_ref: Box<&mut dyn QlInputTable> = Box::new(&mut in_table);
    let mut out_table = QlMemTable::new(
        //&ql_schema
    );
    let mut out_table_ref: Box<&mut dyn QlOutputTable> = Box::new(&mut out_table);
    let res_cols = get_res_cols(schema, &qry);
    let select_c = QlSelectCols::new(res_cols);
    let true_expr = Expr::Value(Value::Boolean(true));
    let where_c: &Expr = &qry.get_select().selection.as_ref().unwrap_or(&true_expr);
    let mut empty_lazy_context = LazyContext::empty();
    let limit = get_limit(&qry, &mut empty_lazy_context)?;
    let offset = get_offset(&qry, &mut empty_lazy_context)?;

    let group_by_exprs = Vec::new(); // TODO
    eval_query(
        &select_c,
        where_c,
        limit,
        offset,
        &group_by_exprs,
        &mut in_table_ref,
        &mut out_table_ref
    )?;
    // match res {
    //     Ok(r) => Ok(r),
    //     Err(e) => Err(Box::new(e)),
    // }
    Ok(Box::new(out_table))
}


#[cfg(test)]
mod test {
    // use std::collections::HashMap;
    use std::error::Error;
    use std::io::{BufRead, BufReader, BufWriter, Write};
    // use std::rc::Rc;

    // use sqlparser::ast::Expr::*;
    // use sqlparser::ast::Value::*;
    // use sqlparser::ast::{BinaryOperator::*, Ident};

    use crate::parser::test_syslog_schema;
    use crate::QlMemTable;
    // use crate::query_processor::ql_schema::{QlRow, QlRowContext, QlSchema};
    //use crate::ResultTable;

    use super::process_sql;

    fn get_logger() -> Box<dyn Write> {
        Box::new(BufWriter::new(std::io::stderr()))
    }

    // #[test]
    // fn test_eval_expr() {
    //     let expr = Box::new(BinaryOp {
    //         left: Box::new(BinaryOp {
    //             left: Box::new(Identifier(Ident {
    //                 value: "a".to_string(),
    //                 quote_style: None,
    //             })),
    //             op: Gt,
    //             right: Box::new(Identifier(Ident {
    //                 value: "b".to_string(),
    //                 quote_style: None,
    //             })),
    //         }),
    //         op: And,
    //         right: Box::new(BinaryOp {
    //             left: Box::new(Identifier(Ident {
    //                 value: "b".to_string(),
    //                 quote_style: None,
    //             })),
    //             op: Lt,
    //             right: Box::new(Value(Number("100".to_string(), false))),
    //         }),
    //     });
    //     let hm = HashMap::from([
    //         (Rc::from("a"), ParsedValue::LongVal(150)),
    //         (Rc::from("b"), ParsedValue::LongVal(50)),
    //     ]);
    //
    //     let pd1 = QlRow::from_parsed_message() ParsedData::new(hm);
    //     let ret = eval_expr(
    //         &expr,
    //         &QlRowContext::from_row(Some(&pd1)),
    //         &mut LazyContext::empty(),
    //     )
    //     .unwrap();
    //     println!("RESULT: {:?}", ret);
    // }

    fn test_query(query: &str, input: &'static str) -> Result<Box<QlMemTable>, Box<dyn Error>> {
        let schema = test_syslog_schema();
        let log = get_logger();
        let rdr: Box<dyn BufRead> = Box::new(BufReader::new(input.as_bytes()));
        let rrt = process_sql(rdr, &schema, false, query, log);
        match &rrt {
            Ok(rt) => {
                for r in rt.get_rows() {
                    println!("COMPUTED: {:?}", r)
                }
            }
            Err(err) => {
                println!("ERROR: {:?}", err)
            }
        }
        rrt
    }

    const LINES1: &str = "Apr 22 02:34:54 actek-mac login[49532]: USER_PROCESS: 49532 ttys000\n\
        Apr 22 04:42:04 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:43:04 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:43:34 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:48:50 actek-mac login[49532]: USER_PROCESS: 49532 ttys000\n\
        ";

    #[test]
    fn test_process_sql_one_shot1() {
        let query = "select * from SYSLOGLINE where \
            message=\"ASL Sender Statistics\" and \
            timestamp > DATE(\"%b %e %H:%M:%S\", \"Apr 22 03:00:00\") and \
            timestamp < DATE(\"%b %e %H:%M:%S\", \"Apr 22 05:00:00\") \
            limit 3 offset 1;";
        let rt = test_query(query, LINES1).unwrap();
        assert_eq!(rt.get_rows().len(), 2)
    }

    #[test]
    fn test_process_sql_one_shot2() {
        let query = "select timestamp as ts, program as prog, 2+2 as four, 3/2.0 \
            from SYSLOGLINE where \
            message=\"ASL Sender Statistics\" and \
            timestamp > DATE(\"%b %e %H:%M:%S\", \"Apr 22 03:00:00\") and \
            timestamp < DATE(\"%b %e %H:%M:%S\", \"Apr 22 05:00:00\") \
            limit 3 offset 1;";
        let rt = test_query(query, LINES1).unwrap();
        assert_eq!(rt.get_rows().len(), 2)
    }

    #[test]
    fn test_process_sql_one_shot3() {
        let query = "select timestamp as ts, ((program || ':') || pid) as prog \
            from SYSLOGLINE where \
            message=\"ASL Sender Statistics\" and \
            timestamp > DATE(\"%b %e %H:%M:%S\", \"Apr 22 03:00:00\") and \
            timestamp < DATE(\"%b %e %H:%M:%S\", \"Apr 22 05:00:00\") \
            limit 3 offset 1;";
        let rt = test_query(query, LINES1).unwrap();
        assert_eq!(rt.get_rows().len(), 2)
    }

    #[test]
    fn test_process_sql_one_shot4_div_by_zero() {
        let query = "select timestamp, 2/0 \
            from SYSLOGLINE where \
            message=\"ASL Sender Statistics\" and \
            timestamp > DATE(\"%b %e %H:%M:%S\", \"Apr 22 03:00:00\") and \
            timestamp < DATE(\"%b %e %H:%M:%S\", \"Apr 22 05:00:00\") \
            limit 3 offset 1;";
        let rt = test_query(query, LINES1);
        assert!(rt.is_err())
    }

    #[test]
    fn test_process_sql_one_shot5() {
        let query = "select timestamp as ts, ((program || ':') || pid) as prog \
            from SYSLOGLINE";
        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() >= 5)
    }

    // #[test]
    // fn test_process_sql_one_shot6() {
    //     let query = "select count(*) \
    //         from SYSLOGLINE";
    //     let rt = test_query(query, LINES1).unwrap();
    //     assert!(rt.get_rows().len() >= 5)
    // }
}
