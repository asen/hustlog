use super::*;
use crate::ql_processor::ql_schema::QlRowContext;
use sqlparser::ast::{Expr, FunctionArg, FunctionArgExpr};
use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub type DynAggExpr = Box<dyn AggExpr + Send + Sync>;

pub trait AggExpr {
    fn add_context(&mut self, ctx: &QlRowContext, dctx: &mut LazyContext)
        -> Result<(), QueryError>;

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError>;

    fn clone_expr(&self) -> DynAggExpr;

    fn name(&self) -> &Arc<str>;

    fn result_type(
        &self,
        ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError>;
}

struct CountExpr {
    name: Arc<str>,
    cnt: i64,
}

impl AggExpr for CountExpr {
    fn add_context(
        &mut self,
        _ctx: &QlRowContext,
        _dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        self.cnt += 1;
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        Ok(Arc::new(ParsedValue::LongVal(self.cnt)))
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            name: self.name.clone(),
            cnt: self.cnt,
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn result_type(
        &self,
        _ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        Ok(ParsedValueType::LongType)
    }
}

struct CountDistinctExpr {
    name: Arc<str>,
    distinct_expr: Expr,
    distinct_vs: HashSet<Arc<ParsedValue>>,
}

impl AggExpr for CountDistinctExpr {
    fn add_context(
        &mut self,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        let v = eval_expr(&self.distinct_expr, ctx, dctx)?;
        self.distinct_vs.borrow_mut().insert(v);
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        Ok(Arc::new(
            ParsedValue::LongVal(self.distinct_vs.len() as i64),
        ))
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            name: self.name.clone(),
            distinct_expr: self.distinct_expr.clone(),
            distinct_vs: self.distinct_vs.clone(),
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn result_type(
        &self,
        _ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        Ok(ParsedValueType::LongType)
    }
}

struct MinExpr {
    name: Arc<str>,
    expr: Expr,
    curv: Option<Arc<ParsedValue>>,
}

impl AggExpr for MinExpr {
    fn add_context(
        &mut self,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        let calc = eval_expr(&self.expr, ctx, dctx)?;
        if self.curv.is_none() {
            self.curv = Some(calc)
        } else {
            if self.curv.as_ref().unwrap() > &calc {
                self.curv = Some(calc)
            }
        }
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        Ok(self.curv.as_ref().unwrap_or(&arc_null_pv()).clone())
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            name: self.name.clone(),
            expr: self.expr.clone(),
            curv: self.curv.clone(),
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn result_type(
        &self,
        ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        eval_expr_type(&self.expr, ctx)
    }
}

struct MaxExpr {
    name: Arc<str>,
    expr: Expr,
    curv: Option<Arc<ParsedValue>>,
}

impl AggExpr for MaxExpr {
    fn add_context(
        &mut self,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        let calc = eval_expr(&self.expr, ctx, dctx)?;
        if self.curv.is_none() {
            self.curv = Some(calc)
        } else {
            if self.curv.as_ref().unwrap() < &calc {
                self.curv = Some(calc)
            }
        }
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        Ok(self.curv.as_ref().unwrap_or(&arc_null_pv()).clone())
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            name: self.name.clone(),
            expr: self.expr.clone(),
            curv: self.curv.clone(),
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn result_type(
        &self,
        ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        eval_expr_type(&self.expr, ctx)
    }
}

struct SumExpr {
    name: Arc<str>,
    expr: Expr,
    curv: Option<Arc<ParsedValue>>,
}

fn add_parsed_values(
    v1: &Arc<ParsedValue>,
    v2: &Arc<ParsedValue>,
) -> Result<Arc<ParsedValue>, QueryError> {
    match v1.as_ref() {
        ParsedValue::NullVal => Ok(v2.clone()),
        ParsedValue::LongVal(x1) => match v2.as_ref() {
            ParsedValue::NullVal => Ok(v1.clone()),
            ParsedValue::LongVal(x2) => Ok(Arc::new(ParsedValue::LongVal(*x1 + *x2))),
            ParsedValue::DoubleVal(x2) => Ok(Arc::new(ParsedValue::DoubleVal(*x2 + *x1 as f64))),
            _ => Err(QueryError::new(
                "Addition is only supported for numeric values",
            )),
        },
        ParsedValue::DoubleVal(x1) => match v2.as_ref() {
            ParsedValue::NullVal => Ok(v1.clone()),
            ParsedValue::LongVal(x2) => Ok(Arc::new(ParsedValue::DoubleVal(*x2 as f64 + *x1))),
            ParsedValue::DoubleVal(x2) => Ok(Arc::new(ParsedValue::DoubleVal(*x2 + *x1))),
            _ => Err(QueryError::new(
                "Addition is only supported for numeric values",
            )),
        },
        _ => Err(QueryError::new(
            "Addition is only supported for numeric values",
        )), // ParsedValue::BoolVal(_) => {}
            // ParsedValue::TimeVal(_) => {}
            // ParsedValue::StrVal(_) => {}
    }
}

impl AggExpr for SumExpr {
    fn add_context(
        &mut self,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        let zero_long = Arc::new(ParsedValue::LongVal(0));
        let calc = eval_expr(&self.expr, ctx, dctx)?;
        let &lval = &self.curv.as_ref().unwrap_or(&zero_long);
        self.curv = Some(add_parsed_values(lval, &calc)?);
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        Ok(self.curv.as_ref().unwrap_or(&arc_null_pv()).clone())
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            name: self.name.clone(),
            expr: self.expr.clone(),
            curv: self.curv.clone(),
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn result_type(
        &self,
        ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        eval_expr_type(&self.expr, ctx)
    }
}

struct AvgExpr {
    sum_expr: SumExpr,
    cnt: usize,
}

impl AggExpr for AvgExpr {
    fn add_context(
        &mut self,
        ctx: &QlRowContext,
        dctx: &mut LazyContext,
    ) -> Result<(), QueryError> {
        self.sum_expr.add_context(ctx, dctx)?;
        self.cnt += 1;
        Ok(())
    }

    fn eval(&self) -> Result<Arc<ParsedValue>, QueryError> {
        if self.cnt == 0 {
            return Ok(arc_null_pv());
        }
        let sum_val = self.sum_expr.eval()?;
        match sum_val.as_ref() {
            ParsedValue::NullVal => Ok(arc_null_pv()),
            ParsedValue::LongVal(x) => Ok(Arc::new(ParsedValue::DoubleVal(
                *x as f64 / self.cnt as f64,
            ))),
            ParsedValue::DoubleVal(x) => Ok(Arc::new(ParsedValue::DoubleVal(*x / self.cnt as f64))),
            _ => Err(QueryError::new(
                "Averaging is only supported for numeric values",
            )),
            // ParsedValue::BoolVal(_) => {}
            // ParsedValue::TimeVal(_) => {}
            // ParsedValue::StrVal(_) => {}
        }
    }

    fn clone_expr(&self) -> DynAggExpr {
        Box::new(Self {
            sum_expr: SumExpr {
                name: self.sum_expr.name.clone(),
                expr: self.sum_expr.expr.clone(),
                curv: self.sum_expr.curv.clone(),
            },
            cnt: self.cnt,
        })
    }

    fn name(&self) -> &Arc<str> {
        &self.sum_expr.name()
    }

    fn result_type(
        &self,
        _ctx: &HashMap<Arc<str>, ParsedValueType>,
    ) -> Result<ParsedValueType, QueryError> {
        Ok(ParsedValueType::DoubleType)
    }
}

fn get_func_arg_expr(farg: &FunctionArg) -> Result<&Expr, QueryError> {
    match farg {
        FunctionArg::Named { name: _, arg } => match arg {
            FunctionArgExpr::Expr(ex) => Ok(ex),
            FunctionArgExpr::QualifiedWildcard(_) => Err(QueryError::new(
                "Can not use qualified wildcard in this context",
            )),
            FunctionArgExpr::Wildcard => {
                Err(QueryError::new("Can not use wildcard in this context"))
            }
        },
        FunctionArg::Unnamed(arg) => match arg {
            FunctionArgExpr::Expr(ex) => Ok(ex),
            FunctionArgExpr::QualifiedWildcard(_) => Err(QueryError::new(
                "Can not use qualified wildcard in this context",
            )),
            FunctionArgExpr::Wildcard => {
                Err(QueryError::new("Can not use wildcard in this context"))
            }
        },
    }
}

pub fn get_agg_expr(col_name: &Arc<str>, from: &Expr) -> Result<Option<DynAggExpr>, QueryError> {
    let ret: Option<Result<DynAggExpr, QueryError>> = if let Expr::Function(fun) = from {
        let name = object_name_to_string(&fun.name);
        // TODO too much copy/pasting across the match arms
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => {
                if !fun.distinct {
                    Some(Ok(Box::new(CountExpr {
                        name: col_name.clone(),
                        cnt: 0,
                    })))
                } else {
                    let args: &Vec<FunctionArg> = &fun.args;
                    if args.len() != 1 {
                        Some(Err(QueryError::new(
                            "COUNT DISTINCT requires exactly one expression",
                        )))
                    } else {
                        let first_arg = args.first().unwrap();
                        let first_arg_expr = get_func_arg_expr(first_arg);
                        match first_arg_expr {
                            Ok(e) => Some(Ok(Box::new(CountDistinctExpr {
                                name: col_name.clone(),
                                distinct_expr: e.clone(),
                                distinct_vs: HashSet::new(),
                            }))),
                            Err(x) => Some(Err(x)),
                        }
                    }
                }
            }
            "SUM" => {
                let args: &Vec<FunctionArg> = &fun.args;
                if args.len() != 1 {
                    Some(Err(QueryError::new("SUM requires exactly one expression")))
                } else {
                    let first_arg = args.first().unwrap();
                    let first_arg_expr = get_func_arg_expr(first_arg);
                    match first_arg_expr {
                        Ok(e) => Some(Ok(Box::new(SumExpr {
                            name: col_name.clone(),
                            expr: e.clone(),
                            curv: None,
                        }))),
                        Err(x) => Some(Err(x)),
                    }
                }
            }
            "AVG" => {
                let args: &Vec<FunctionArg> = &fun.args;
                if args.len() != 1 {
                    Some(Err(QueryError::new("AVG requires exactly one expression")))
                } else {
                    let first_arg = args.first().unwrap();
                    let first_arg_expr = get_func_arg_expr(first_arg);
                    match first_arg_expr {
                        Ok(e) => Some(Ok(Box::new(AvgExpr {
                            sum_expr: SumExpr {
                                name: col_name.clone(),
                                expr: e.clone(),
                                curv: None,
                            },
                            cnt: 0,
                        }))),
                        Err(x) => Some(Err(x)),
                    }
                }
            }
            "MIN" => {
                let args: &Vec<FunctionArg> = &fun.args;
                if args.len() != 1 {
                    Some(Err(QueryError::new("MIN requires exactly one expression")))
                } else {
                    let first_arg = args.first().unwrap();
                    let first_arg_expr = get_func_arg_expr(first_arg);
                    match first_arg_expr {
                        Ok(e) => Some(Ok(Box::new(MinExpr {
                            name: col_name.clone(),
                            expr: e.clone(),
                            curv: None,
                        }))),
                        Err(x) => Some(Err(x)),
                    }
                }
            }
            "MAX" => {
                let args: &Vec<FunctionArg> = &fun.args;
                if args.len() != 1 {
                    Some(Err(QueryError::new("MAX requires exactly one expression")))
                } else {
                    let first_arg = args.first().unwrap();
                    let first_arg_expr = get_func_arg_expr(first_arg);
                    match first_arg_expr {
                        Ok(e) => Some(Ok(Box::new(MaxExpr {
                            name: col_name.clone(),
                            expr: e.clone(),
                            curv: None,
                        }))),
                        Err(x) => Some(Err(x)),
                    }
                }
            }

            _ => None,
        }
    } else {
        None
    };
    match ret {
        None => Ok(None),
        Some(r) => match r {
            Ok(b) => Ok(Some(b)),
            Err(e) => Err(e),
        },
    }
}
