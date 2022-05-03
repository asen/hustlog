use super::*;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};
use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::rc::Rc;
use crate::query_processor::ql_schema::QlRowContext;

pub trait AggExpr {
    fn add_context(&mut self,
                   ctx: &QlRowContext,
                   dctx: &mut LazyContext,
                   gb_ixes: &Vec<usize>,
    ) -> Result<(), QueryError>;

    fn eval(&self) -> Result<ParsedValue, QueryError>;

    fn clone_expr(&self) -> Box<dyn AggExpr>;

    fn name(&self) -> Rc<str>;
}

struct CountExpr {
    name: Rc<str>,
    cnt: i64,
}

impl AggExpr for CountExpr {
    fn add_context(&mut self, _ctx: &QlRowContext, _dctx: &mut LazyContext,
                   _gb_ixes: &Vec<usize>,) -> Result<(), QueryError> {
        self.cnt += 1;
        Ok(())
    }

    fn eval(&self) -> Result<ParsedValue, QueryError> {
        Ok(ParsedValue::LongVal(self.cnt))
    }

    fn clone_expr(&self) -> Box<dyn AggExpr> {
        Box::new(Self{ name: self.name.clone(), cnt:  self.cnt })
    }

    fn name(&self) -> Rc<str> {
        self.name.clone()
    }
}

struct CountDistinctExpr {
    name: Rc<str>,
    distinct_expr: Expr,
    distinct_vs: HashSet<ParsedValue>,
}

impl AggExpr for CountDistinctExpr {
    fn add_context(&mut self, ctx: &QlRowContext, dctx: &mut LazyContext,
                   _gb_ixes: &Vec<usize>,) -> Result<(), QueryError> {
        let v = eval_expr(&self.distinct_expr, ctx, dctx)?;
        self.distinct_vs.borrow_mut().insert(v);
        Ok(())
    }

    fn eval(&self) -> Result<ParsedValue, QueryError> {
        Ok(ParsedValue::LongVal(self.distinct_vs.len() as i64))
    }

    fn clone_expr(&self) -> Box<dyn AggExpr> {
        Box::new(Self{
            name: self.name.clone(),
            distinct_expr:  self.distinct_expr.clone(),
            distinct_vs: self.distinct_vs.clone(),
        })
    }

    fn name(&self) -> Rc<str> {
        self.name.clone()
    }
}

fn get_func_arg_expr(farg: &FunctionArg) -> Result<&Expr, QueryError> {
    match farg {
        FunctionArg::Named { name: _, arg } => {
            match arg {
                FunctionArgExpr::Expr(ex) => {
                    Ok(ex)
                }
                FunctionArgExpr::QualifiedWildcard(_) => {
                    Err(QueryError::new("Can not use qualified wildcard in this context"))
                }
                FunctionArgExpr::Wildcard => {
                    Err(QueryError::new("Can not use wildcard in this context"))
                }
            }
        }
        FunctionArg::Unnamed(arg) => {
            match arg {
                FunctionArgExpr::Expr(ex) => {
                    Ok(ex)
                }
                FunctionArgExpr::QualifiedWildcard(_) => {
                    Err(QueryError::new("Can not use qualified wildcard in this context"))
                }
                FunctionArgExpr::Wildcard => {
                    Err(QueryError::new("Can not use wildcard in this context"))
                }
            }
        }
    }
}

pub fn get_agg_expr(col_name: &Rc<str>, from: &Expr) -> Result<Option<Box<dyn AggExpr>>, QueryError> {
    let ret: Option<Result<Box<dyn AggExpr>, QueryError>> = if let Expr::Function(fun) = from {
        let name = object_name_to_string(&fun.name);
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => if !fun.distinct {
                Some(Ok(Box::new(CountExpr {
                    name: col_name.clone(),
                    cnt: 0
                })))
            } else {
                let args: &Vec<FunctionArg> = &fun.args;
                if args.len() != 1 {
                    Some(Err(QueryError::new("COUNT DISTINCT requires exactly one expression")))
                } else {
                    let first_arg = args.first().unwrap();
                    let first_arg_expr = get_func_arg_expr(first_arg);
                    match first_arg_expr {
                        Ok(e) => {
                            Some(Ok(Box::new(CountDistinctExpr {
                                name: col_name.clone(),
                                distinct_expr: e.clone(),
                                distinct_vs: HashSet::new()
                            })))
                        },
                        Err(x) => { Some(Err(x)) }
                    }
                }

            },
            _ => None
        }
    } else {
        None
    };
    match ret {
        None => { Ok(None )}
        Some(r) => {
            match r {
                Ok(b) => { Ok(Some(b)) }
                Err(e) => { Err(e) }
            }
        }
    }
}