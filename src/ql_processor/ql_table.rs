use crate::parser::{arc_null_pv, ParsedValue, RawMessage};
use crate::ql_processor::ql_agg_expr::AggExpr;
use crate::ql_processor::ql_eval_expr::{eval_expr, eval_integer_expr, LazyContext};
use crate::ql_processor::ql_schema::{QlRow, QlRowContext, QlSchema, QlSelectCols, QlSelectItem};
use crate::ql_processor::SqlSelectQuery;
use crate::ql_processor::{QlRowBatch, QueryError};
use crate::DynError;
use sqlparser::ast::Expr;
use std::cmp::{min, Ordering};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

pub trait QlInputTable {
    fn read_row(&mut self) -> Result<Option<QlRow>, DynError>;
    fn ql_schema(&self) -> &Arc<QlSchema>;
}

pub trait QlOutputTable {
    fn write_row(&mut self, row: QlRow) -> Result<(), DynError>;
    fn num_written(&self) -> usize;
    fn ordered_slice(
        &mut self,
        limit: Option<usize>,
        offset: i64,
        order_by_exprs: &Vec<(usize, bool)>,
    ) -> Box<&dyn QlInputTable>;
    fn set_schema(&mut self, new_schema: Arc<QlSchema>);
}

pub struct QlMemTable {
    schema: Arc<QlSchema>,
    buf: Vec<QlRow>,
    written: usize,
    read: usize,
}

impl QlMemTable {
    pub fn new(schema: Arc<QlSchema>) -> Self {
        Self {
            schema: schema,
            buf: Vec::new(),
            written: 0,
            read: 0,
        }
    }

    pub fn from_rows_batch(schema: Arc<QlSchema>, batch: QlRowBatch) -> Self {
        Self {
            schema,
            written: batch.len(),
            buf: batch,
            read: 0,
        }
    }

    pub fn consume_rows(self) -> QlRowBatch {
        self.buf
    }

    #[cfg(test)]
    pub fn get_rows(&self) -> &Vec<QlRow> {
        &self.buf
    }
}

impl QlOutputTable for QlMemTable {
    fn write_row(&mut self, row: QlRow) -> Result<(), DynError> {
        self.buf.push(row);
        self.written += 1;
        Ok(())
    }

    fn num_written(&self) -> usize {
        self.written
    }

    fn set_schema(&mut self, new_schema: Arc<QlSchema>) {
        self.schema = new_schema;
    }

    fn ordered_slice(
        &mut self,
        limit: Option<usize>,
        offset: i64,
        order_by_exprs: &Vec<(usize, bool)>,
    ) -> Box<&dyn QlInputTable> {
        let arc_null_pv = Arc::new(ParsedValue::NullVal);
        self.buf.sort_by(|x, y| {
            let x_sk: Vec<(Arc<ParsedValue>, bool)> = order_by_exprs
                .iter()
                .map(|(pos, asc)| {
                    let pv: &Arc<ParsedValue> =
                        x.data().get(*pos).map(|(_rc, v)| v).unwrap_or(&arc_null_pv);
                    (Arc::clone(pv), *asc)
                })
                .collect::<Vec<_>>();
            let y_sk: Vec<(Arc<ParsedValue>, bool)> = order_by_exprs
                .iter()
                .map(|(pos, asc)| {
                    let pv: &Arc<ParsedValue> =
                        y.data().get(*pos).map(|(_rc, v)| v).unwrap_or(&arc_null_pv);
                    (Arc::clone(pv), *asc)
                })
                .collect::<Vec<_>>();

            let mut ord = None;
            for (lh, rh) in x_sk.iter().zip(y_sk) {
                if lh.0 != rh.0 {
                    if lh.1 {
                        ord = Some(lh.0.partial_cmp(&rh.0).unwrap_or(Ordering::Less))
                    } else {
                        //desc
                        ord = Some(lh.0.partial_cmp(&rh.0).unwrap_or(Ordering::Less).reverse())
                    }
                    break;
                }
            }
            ord.unwrap_or(Ordering::Equal)
        });

        if offset > 0 {
            let uoffset = offset as usize;
            let to_drain = min(uoffset, self.buf.len());
            self.buf.drain(0..to_drain);
        }
        if limit.is_some() {
            let limit_u = limit.unwrap();
            if limit_u < self.buf.len() {
                self.buf.drain(limit.unwrap()..);
            }
        }
        let sz = self.buf.len();
        self.written = sz;
        self.read = 0;
        Box::new(self)
    }
}

impl QlInputTable for QlMemTable {
    fn read_row(&mut self) -> Result<Option<QlRow>, DynError> {
        let ret = self.buf.get(self.read);
        if ret.is_none() {
            return Ok(None);
        }
        self.read += 1;
        let ret: QlRow = ret.unwrap().clone();
        Ok(Some(ret))
    }

    fn ql_schema(&self) -> &Arc<QlSchema> {
        &self.schema
    }
}

#[derive(Eq, Hash, PartialEq, Debug)]
struct QlGroupByKey(Vec<(Arc<str>, Arc<ParsedValue>)>);

struct QlGroupByContext {
    //gb_key_ixes: Vec<usize>,
    by_gb_key: HashMap<Arc<QlGroupByKey>, Vec<Box<dyn AggExpr>>>,
    keys_ordered: Vec<Arc<QlGroupByKey>>,
}

impl QlGroupByContext {
    pub fn new() -> Self {
        Self {
            //gb_key_ixes,
            by_gb_key: HashMap::new(),
            keys_ordered: Vec::new(),
        }
    }

    pub fn add_row(
        &mut self,
        gb_key: QlGroupByKey,
        empty_agg_exprs: Vec<Box<dyn AggExpr>>,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        let gb_key_ref = Arc::new(gb_key);
        let en = self.by_gb_key.entry(gb_key_ref.clone());
        let mut exists = true;
        let agg_exprs: &mut Vec<Box<dyn AggExpr>> = match en {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                exists = false;
                e.insert(empty_agg_exprs)
            }
        };
        if !exists {
            self.keys_ordered.push(gb_key_ref.clone());
        }
        for ae in agg_exprs.iter_mut() {
            ae.add_context(ctx, dctx)?;
        }
        Ok(())
    }

    fn output_to_table(
        &self,
        sel_cols: &QlSelectCols,
        outp: &mut Box<&mut dyn QlOutputTable>,
        limit: Option<usize>,
        offset: i64,
        order_by: &Vec<(usize, bool)>,
    ) -> Result<(), DynError> {
        let mut my_offset = offset;
        let has_order_by = !order_by.is_empty();
        for gb_key_ref in &self.keys_ordered {
            if !has_order_by && my_offset > 0 {
                my_offset -= 1;
                continue;
            }
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
                        let pv = lazy_ex_iter.next().unwrap(); //.clone();
                        let pvc = (Arc::clone(&pv.0), Arc::clone(&pv.1));
                        outp_row.push(pvc)
                    }
                    QlSelectItem::AggExpr(_) => {
                        let ae = agg_exprs_iter.next().unwrap();
                        let pv = ae.eval()?;
                        outp_row.push((ae.name().clone(), pv))
                    }
                }
            }
            outp.write_row(QlRow::new(None, outp_row))?;
            if !has_order_by && limit.is_some() && outp.num_written() >= limit.unwrap() {
                break;
            }
        }
        outp.ordered_slice(limit, offset, order_by);
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

pub fn get_limit(
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

pub fn get_offset(
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

fn eval_lazy_ctxs(
    select_c: &QlSelectCols,
    static_ctx: &QlRowContext,
    lazy_ctx: &mut LazyContext,
) -> Result<Vec<(Arc<str>, Arc<ParsedValue>)>, QueryError> {
    let mut outp_vals: Vec<(Arc<str>, Arc<ParsedValue>)> = Vec::new();
    let lazy_exp_vec = select_c.lazy_exprs();
    for le in lazy_exp_vec {
        let pv = lazy_ctx
            .get_value(le.name(), &static_ctx)?
            .unwrap_or(arc_null_pv());
        outp_vals.push((le.name().clone(), pv));
    }
    Ok(outp_vals)
}

pub fn eval_query(
    select_c: Arc<QlSelectCols>,
    where_c: Arc<Expr>,
    limit: Option<usize>,
    offset: i64,
    group_by_exprs: Arc<Vec<usize>>,
    order_by_exprs: Arc<Vec<(usize, bool)>>,
    inp: &mut Box<&mut dyn QlInputTable>,
    outp: &mut Box<&mut dyn QlOutputTable>,
) -> Result<(), DynError> {
    let has_agg = select_c.validate_group_by_cols(group_by_exprs.as_ref())?;
    let has_order_by = !order_by_exprs.is_empty();
    let needs_raw = select_c.has_raw_message();
    let mut gb_context = QlGroupByContext::new();
    let mut my_offset = offset;
    let mut out_schema_set = false;
    while let Some(irow) = inp.read_row()? {
        let raw: Option<RawMessage> = if needs_raw { irow.raw().clone() } else { None };
        let static_ctx = QlRowContext::from_row(&irow);
        let mut lazy_ctx = select_c.lazy_context();
        let where_result = eval_expr(where_c.as_ref(), &static_ctx, &mut lazy_ctx)?
            .as_bool()
            .unwrap_or(false);
        if where_result {
            //row matches
            // eval our lazy contexts
            let outp_vals = eval_lazy_ctxs(select_c.as_ref(), &static_ctx, &mut lazy_ctx)?;
            if has_agg {
                // handle group by stuff
                let agg_exprs = select_c.agg_exprs();
                gb_context.add_row(
                    QlGroupByKey(outp_vals),
                    agg_exprs,
                    &static_ctx,
                    &mut lazy_ctx,
                )?;
            } else {
                //generate the output row
                // limit and offset can be applied only if there is no order by
                if !has_order_by && my_offset > 0 {
                    my_offset -= 1
                } else {
                    let orow = QlRow::new(raw, outp_vals);
                    if !out_schema_set {
                        out_schema_set = true
                    }
                    outp.write_row(orow)?;
                    if !has_order_by && limit.is_some() && outp.num_written() >= limit.unwrap() {
                        break;
                    }
                }
            }
        }
    }
    if has_agg {
        gb_context.output_to_table(&select_c, outp, limit, offset, order_by_exprs.as_ref())?;
    } else if has_order_by {
        // TODO apply order
        // then offset and limit
        outp.ordered_slice(limit, offset, order_by_exprs.as_ref());
    }
    Ok(())
}

pub fn get_group_by_exprs(
    qry: &SqlSelectQuery,
    mut empty_lazy_context: &mut LazyContext,
) -> Result<Vec<usize>, DynError> {
    let mut group_by_exprs = Vec::new(); // TODO
    for (ix, gbe) in qry.get_select().group_by.iter().enumerate() {
        let num = eval_integer_expr(
            gbe,
            &QlRowContext::empty(),
            &mut empty_lazy_context,
            ix.to_string().as_str(),
        )?;
        if num > 0 {
            group_by_exprs.push(num as usize - 1);
        } else {
            return Err(Box::new(QueryError::new(
                "GROUP BY columns are 1-based column indexes",
            )));
        }
    }
    Ok(group_by_exprs)
}

pub fn get_order_by_exprs(
    qry: &SqlSelectQuery,
    mut empty_lazy_context: &mut LazyContext,
) -> Result<Vec<(usize, bool)>, DynError> {
    let mut order_by_exprs = Vec::new(); // TODO
    for (ix, obe) in qry.get_order_by().iter().enumerate() {
        let ex = &obe.expr;
        if obe.nulls_first.is_some() {
            return Err(Box::new(QueryError::not_supported(
                "NULLS FIRST or NULLS LAST are not supported, nulls are always first for now",
            )));
        }
        let num = eval_integer_expr(
            ex,
            &QlRowContext::empty(),
            &mut empty_lazy_context,
            ix.to_string().as_str(),
        )?;
        if num > 0 {
            order_by_exprs.push((num as usize - 1, obe.asc.unwrap_or(true)));
        } else {
            return Err(Box::new(QueryError::new(
                "ORDER BY columns are 1-based column indexes",
            )));
        }
    }
    Ok(order_by_exprs)
}

#[cfg(test)]
pub mod tests {
    use crate::async_pipeline::LinesBuffer;
    use bytes::BufMut;
    use sqlparser::ast::Value;
    use std::sync::Arc;

    use crate::parser::{test_syslog_schema, GrokSchema, LogParser};
    use crate::ql_processor::{get_res_cols, QlMemTable, QlSchema};
    use crate::{DynError, GrokParser};

    use super::*;

    pub fn process_sql_test(
        query: &str,
        mut in_table: Box<&mut dyn QlInputTable>,
        mut out_table: Box<&mut dyn QlOutputTable>,
    ) -> Result<(), DynError> {
        //println!("process_sql: {}", schema.columns().len());
        let qry = Arc::new(SqlSelectQuery::new(query)?);
        let ql_schema = in_table.ql_schema().clone();
        let res_cols = get_res_cols(&qry);
        let select_c = Arc::new(QlSelectCols::new(res_cols));
        out_table.set_schema(Arc::new(select_c.to_out_schema(ql_schema.as_ref())?));
        let true_expr = Expr::Value(Value::Boolean(true));
        let where_c: Arc<Expr> = Arc::from(
            qry.get_select()
                .selection
                .as_ref()
                .unwrap_or(&true_expr)
                .clone(),
        );
        let mut empty_lazy_context = LazyContext::empty();
        let limit = get_limit(&qry, &mut empty_lazy_context)?;
        let offset = get_offset(&qry, &mut empty_lazy_context)?;

        //println!("process_sql: {}", select_c.cols().len());
        let group_by_exprs = Arc::new(get_group_by_exprs(&qry, &mut empty_lazy_context)?);
        let order_by_exprs = Arc::new(get_order_by_exprs(&qry, &mut empty_lazy_context)?);
        eval_query(
            select_c,
            where_c,
            limit,
            offset,
            group_by_exprs,
            order_by_exprs,
            &mut in_table,
            &mut out_table,
        )?;
        // match res {
        //     Ok(r) => Ok(r),
        //     Err(e) => Err(Box::new(e)),
        // }
        Ok(())
    }

    pub fn input_to_table_test(input: &'static str, schema: GrokSchema) -> QlMemTable {
        let ql_schema = Arc::new(QlSchema::from(&schema));
        let mut ret = QlMemTable::new(ql_schema.clone());
        let mut lb = LinesBuffer::new(false);
        lb.get_buf().put(input.as_bytes());
        let parser = GrokParser::new(schema).unwrap();
        for ln in lb.flush() {
            if let Ok(parsed) = parser.parse(ln) {
                ret.write_row(QlRow::from_parsed_message(parsed, ql_schema.as_ref()))
                    .unwrap();
            }
        }
        ret
    }

    fn test_query(query: &str, input: &'static str) -> Result<Box<QlMemTable>, DynError> {
        let schema = test_syslog_schema();
        let mut in_table = input_to_table_test(input, schema);
        let mut rrt = QlMemTable::new(in_table.ql_schema().clone());
        let res = process_sql_test(query, Box::new(&mut in_table), Box::new(&mut rrt));
        if res.is_ok() {
            for r in rrt.get_rows() {
                println!("RESULT: {:?}", r)
            }
        } else {
            println!("ERROR: {:?}", res);
            return Err(res.err().unwrap());
        }
        Ok(Box::new(rrt))
    }

    const LINES1: &str = "Apr 22 02:34:54 actek-mac login[49532]: USER_PROCESS: 49532 ttys000\n\
        Apr 22 04:42:04 actek-mac syslogd[103]: ASL Sender Statistics\n\
        Apr 22 04:43:04 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:43:34 actek-mac syslogd[104]: ASL Sender Statistics\n\
        Apr 22 04:48:50 actek-mac login[49531]: USER_PROCESS: 49532 ttys000\n\
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

    #[test]
    fn test_process_sql_one_shot6() {
        let query = "select count(*) as cnt \
            from SYSLOGLINE";
        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 1)
    }

    #[test]
    fn test_process_sql_one_shot7() {
        let query = "select program, count(*) as cnt \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }

    #[test]
    fn test_process_sql_one_shot8() {
        let query = "select program, min(pid) as min_pid \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }

    #[test]
    fn test_process_sql_one_shot9() {
        let query = "select program, max(pid) as max_pid \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }

    #[test]
    fn test_process_sql_one_shot10() {
        let query = "select program, sum(pid) \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }

    #[test]
    fn test_process_sql_one_shot11() {
        let query = "select program, avg(pid) \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }

    #[test]
    fn test_process_sql_one_shot12() {
        let query = "select program, avg(pid) as avg, max(pid) as max, \
            min(pid) as min, count() as cnt, count(distinct(pid)) as dcnt \
            from SYSLOGLINE group by 1";
        // println!("test_process_sql_one_shot7: {:?}", ParsedValue::NullVal);

        let rt = test_query(query, LINES1).unwrap();
        assert!(rt.get_rows().len() == 2)
    }
}
