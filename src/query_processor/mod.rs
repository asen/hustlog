use std::collections::HashMap;
use std::error::Error;
use std::io::{BufRead, Write};
use std::rc::Rc;

use sqlparser::ast::{Expr, Ident, SelectItem, Value};

use crate::parser::*;
use crate::query::*;
use crate::query_processor::eval::*;
pub use crate::query_processor::query_error::*;
pub use crate::query_processor::result::*;

mod eval;
mod query_error;
mod result;

fn eval_integer_expr(
    expr: &Expr,
    ctx: &StaticCtx,
    dctx: &mut LazyContext,
    name: &str,
) -> Result<i64, QueryError> {
    match eval_expr(expr, ctx, dctx)? {
        ParsedValue::LongVal(x) => Ok(x),
        x => Err(QueryError::new(&format!(
            "Expression did not evauluate to an integer number ({}): {:?} , expr: {:?}",
            name, x, expr
        ))),
    }
}

pub fn process_query_one_shot(
    schema: &GrokSchema,
    qry: &SqlSelectQuery,
    input: impl Iterator<Item = ParsedMessage>,
) -> Result<ResultTable, QueryError> {
    let mut ret = ResultTable::new();
    let true_expr = Expr::Value(Value::Boolean(true));
    let wh: &Expr = qry.get_select().selection.as_ref().unwrap_or(&true_expr);
    let mut empty_lazy_context = LazyContext::empty();
    let limit = if qry.get_limit().is_some() {
        let num = eval_integer_expr(
            qry.get_limit().unwrap(),
            &EMPTY_CTX,
            &mut empty_lazy_context,
            "limit",
        )?;
        Some(num as usize)
    } else {
        None
    };
    let mut offset = if qry.get_offset().is_some() {
        let num = eval_integer_expr(
            qry.get_offset().unwrap(),
            &EMPTY_CTX,
            &mut empty_lazy_context,
            "offset",
        )?;
        num
    } else {
        0
    };
    //let ord = qry.get_order_by();
    //println!("DEBUG QRY: {:?}", &wh);

    let selection: &Vec<SelectItem> = &qry.get_select().projection;
    let res_cols: Vec<(Rc<str>, LazyExpr)> = selection
        .iter()
        .enumerate()
        .flat_map(|(i, x)| match x {
            SelectItem::UnnamedExpr(expr) => {
                let my_name = i.to_string();
                vec![(Rc::from(my_name.as_str()), LazyExpr::new(expr))]
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                vec![(Rc::from(alias.value.as_str()), LazyExpr::new(expr))]
            }
            SelectItem::QualifiedWildcard(wc) => {
                vec![(
                    Rc::from(object_name_to_string(wc).as_str()),
                    LazyExpr::err(QueryError::not_supported("SelectItem::QualifiedWildcard")),
                )]
            }
            SelectItem::Wildcard => {
                let vec = schema
                    .columns()
                    .iter()
                    .map(|cd| {
                        let col_name = cd.col_name().as_str();
                        (
                            Rc::from(col_name),
                            LazyExpr::new(&Expr::Identifier(Ident::new(col_name))),
                        )
                    })
                    .collect::<Vec<_>>();
                vec
            }
        })
        .collect::<Vec<_>>();

    for pm in input {
        let static_ctx = StaticCtx {
            pd: Some(pm.get_parsed()),
        };
        let mut hm = HashMap::new();
        for (k, v) in &res_cols {
            hm.insert(k.clone(), v.clone());
        }
        let mut dctx = LazyContext::new(hm);
        let where_result = eval_expr(wh, &static_ctx, &mut dctx)?;
        //println!("DEBUG EVAL RESULT: {:?} {:?}", &result, pm.get_parsed());
        if where_result.as_bool().unwrap_or(false) {
            if offset <= 0 {
                let mut computed = Vec::new();
                for (nm, _) in &res_cols {
                    let pv = dctx
                        .get_value(nm, &static_ctx)?
                        .unwrap_or(ParsedValue::NullVal);
                    computed.push((nm.clone(), pv))
                }
                ret.add_row(pm, computed);
                if let Some(lmt) = limit {
                    if ret.get_rows().len() >= lmt {
                        break;
                    }
                }
            } else {
                offset -= 1
            }
        }
    }
    //let sort_by = &qry.get_select().sort_by;

    Ok(ret)
}

// pub fn process_query(
//     schema: &GrokSchema,
//     qry: &SqlSelectQuery,
//     input: impl Iterator<Item = ParsedMessage>,
// ) -> Result<ResultTable, QueryError> {
//     Err(QueryError::not_impl("process_query"))
// }

pub fn process_sql_one_shot(
    rdr: Box<dyn BufRead>,
    schema: &GrokSchema,
    use_line_merger: bool,
    query: &str,
    log: Box<dyn Write>,
) -> Result<ResultTable, Box<dyn Error>> {
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
    let res = process_query_one_shot(schema, &qry, pit);
    match res {
        Ok(r) => Ok(r),
        Err(e) => Err(Box::new(e)),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::error::Error;
    use std::io::{BufRead, BufReader, BufWriter, Write};
    use std::rc::Rc;

    use sqlparser::ast::Expr::*;
    use sqlparser::ast::Value::*;
    use sqlparser::ast::{BinaryOperator::*, Ident};

    use crate::parser::test_syslog_schema;
    use crate::query_processor::{eval_expr, LazyContext, StaticCtx};
    use crate::{ParsedData, ParsedValue, ResultTable};

    use super::process_sql_one_shot;

    fn get_logger() -> Box<dyn Write> {
        Box::new(BufWriter::new(std::io::stderr()))
    }

    #[test]
    fn test_eval_expr() {
        let expr = Box::new(BinaryOp {
            left: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident {
                    value: "a".to_string(),
                    quote_style: None,
                })),
                op: Gt,
                right: Box::new(Identifier(Ident {
                    value: "b".to_string(),
                    quote_style: None,
                })),
            }),
            op: And,
            right: Box::new(BinaryOp {
                left: Box::new(Identifier(Ident {
                    value: "b".to_string(),
                    quote_style: None,
                })),
                op: Lt,
                right: Box::new(Value(Number("100".to_string(), false))),
            }),
        });
        let hm = HashMap::from([
            (Rc::from("a"), ParsedValue::LongVal(150)),
            (Rc::from("b"), ParsedValue::LongVal(50)),
        ]);
        let pd1 = ParsedData::new(hm);
        let ret = eval_expr(
            &expr,
            &StaticCtx { pd: Some(&pd1) },
            &mut LazyContext::empty(),
        )
        .unwrap();
        println!("RESULT: {:?}", ret);
    }

    fn test_query(query: &str, input: &'static str) -> Result<ResultTable, Box<dyn Error>> {
        let schema = test_syslog_schema();
        let log = get_logger();
        let rdr: Box<dyn BufRead> = Box::new(BufReader::new(input.as_bytes()));
        let rrt = process_sql_one_shot(rdr, &schema, false, query, log);
        match &rrt {
            Ok(rt) => {
                for r in rt.get_rows() {
                    println!("COMPUTED: {:?}", r.get_computed())
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
