use std::collections::HashMap;
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::rc::Rc;

use crate::query_processor::QueryError;
use crate::{str2val, ParsedData, ParsedValue, ParsedValueType, TimeTypeFormat};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, ObjectName, UnaryOperator, Value,
};

pub struct StaticCtx<'a> {
    pub pd: Option<&'a ParsedData>,
}

pub const EMPTY_CTX: StaticCtx = StaticCtx { pd: None };

impl StaticCtx<'_> {
    pub fn get_value(&self, key: &str) -> Option<ParsedValue> {
        match self.pd {
            Some(pdd) => pdd.get_value(key).map(|x| x.clone()),
            None => None,
        }
    }

    pub fn is_none(&self) -> bool {
        self.pd.is_none()
    }
}

pub struct LazyExpr {
    expr: Expr,
    res: Option<Result<Option<ParsedValue>, QueryError>>,
}

impl LazyExpr {
    pub fn new(expr: &Expr) -> LazyExpr {
        Self {
            expr: expr.clone(),
            res: None,
        }
    }

    pub fn err(qerr: QueryError) -> LazyExpr {
        Self {
            expr: Expr::Value(Value::Null),
            res: Some(Err(qerr)),
        }
    }

    pub fn clone(&self) -> LazyExpr {
        Self {
            expr: self.expr.clone(),
            res: self.res.clone(),
        }
    }
}

pub struct LazyContext {
    hm: HashMap<Rc<str>, LazyExpr>,
}

impl LazyContext {
    pub fn new(hm: HashMap<Rc<str>, LazyExpr>) -> Self {
        Self { hm }
    }

    pub fn empty() -> LazyContext {
        Self { hm: HashMap::new() }
    }

    pub fn get_value(
        &mut self,
        key: &str,
        ctx: &StaticCtx,
    ) -> Result<Option<ParsedValue>, QueryError> {
        // XXX this is to make rust borrow checker happy -
        // it is easier to temporarily remove the LazyExpr from the hash map
        // so that it can be safely evaluated with the rest of the hash map as a lazy context
        // of course this wouldn't be thread-safe if it ever has to be
        let lex_opt = self.hm.remove(key);
        if lex_opt.is_none() {
            return Ok(None);
        }
        let mut lex = lex_opt.unwrap();
        if lex.res.is_some() {
            let ret = lex.res.as_ref().unwrap().clone();
            self.hm.insert(Rc::from(key), lex);
            return ret;
        }
        let res = eval_expr(&lex.expr, ctx, self);
        let ret = match res {
            Ok(pv) => Ok(Some(pv)),
            Err(x) => Err(x),
        };
        lex.res = Some(ret.clone());
        self.hm.insert(Rc::from(key), lex);
        return ret;
    }
}

fn eval_aritmethic_op<T>(lval: T, rval: T, op: &BinaryOperator) -> Result<T, QueryError>
where
    T: Add<Output = T>
        + Mul<Output = T>
        + Sub<Output = T>
        + Div<Output = T>
        + Rem<Output = T>
        + std::cmp::PartialEq
        + std::default::Default,
{
    match op {
        BinaryOperator::Plus => Ok(lval + rval),
        BinaryOperator::Minus => Ok(lval - rval),
        BinaryOperator::Multiply => Ok(lval * rval),
        BinaryOperator::Divide => {
            if rval == Default::default() {
                Err(QueryError::new("Attempt to divide by zero"))
            } else {
                Ok(lval / rval)
            }
        }
        BinaryOperator::Modulo => {
            if rval == Default::default() {
                Err(QueryError::new(
                    "Attempt to extract mod from dividing by zero",
                ))
            } else {
                Ok(lval % rval)
            }
        }
        // BinaryOperator::BitwiseOr => {}
        // BinaryOperator::BitwiseAnd => {}
        // BinaryOperator::BitwiseXor => {}
        // BinaryOperator::PGBitwiseShiftLeft => {}
        // BinaryOperator::PGBitwiseShiftRight => {}
        _ => Err(QueryError::unexpected("Invalid arithmetic op")),
    }
}

pub fn object_name_to_string(onm: &ObjectName) -> String {
    onm.0
        .iter()
        .map(|x| x.value.as_str())
        .collect::<Vec<&str>>()
        .join(",")
}

fn func_arg_to_pv(
    arg: &FunctionArg,
    ctx: &StaticCtx,
    dctx: &mut LazyContext,
) -> Result<ParsedValue, QueryError> {
    match arg {
        FunctionArg::Named { .. } => Err(QueryError::not_supported(
            "Named function arguments are not supported yet",
        )),
        FunctionArg::Unnamed(fax) => match fax {
            FunctionArgExpr::Expr(xp) => eval_expr(xp, ctx, dctx),
            FunctionArgExpr::QualifiedWildcard(_) => Err(QueryError::not_supported(
                "FunctionArgExpr::QualifiedWildcard",
            )),
            FunctionArgExpr::Wildcard => Ok(ParsedValue::StrVal(Rc::new("*".to_string()))),
        },
    }
}

fn eval_function_date(
    fun: &Function,
    ctx: &StaticCtx,
    dctx: &mut LazyContext,
) -> Result<ParsedValue, QueryError> {
    let mut args_iter = fun.args.iter();
    let cur_arg: &FunctionArg = args_iter
        .next()
        .ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx, dctx)?;
    let tformat = if let ParsedValue::StrVal(rs) = curv {
        Ok(TimeTypeFormat::new(rs.as_str()))
    } else {
        Err(QueryError::new(
            "DATE function first argument must be a time format string",
        ))
    }?;
    let cur_arg: &FunctionArg = args_iter
        .next()
        .ok_or(QueryError::new("DATE function requires arguments"))?;
    let curv = func_arg_to_pv(cur_arg, ctx, dctx)?;
    match curv {
        ParsedValue::TimeVal(d) => Ok(ParsedValue::TimeVal(d)),
        ParsedValue::StrVal(s) => str2val(s.as_str(), &ParsedValueType::TimeType(tformat)).ok_or(
            QueryError::new(&format!("Failed to parse date from string {}", s.as_str())),
        ),
        x => Err(QueryError::not_impl(&format!(
            "Unsupported argument type for DATE function {:?}",
            x
        ))),
    }
}

fn eval_function(
    fun: &Function,
    ctx: &StaticCtx,
    dctx: &mut LazyContext,
) -> Result<ParsedValue, QueryError> {
    let fun_name = object_name_to_string(&fun.name);
    match fun_name.as_str() {
        "DATE" => eval_function_date(fun, ctx, dctx),
        _ => Err(QueryError::not_supported(&format!("Function: {:?}", fun))),
    }
}

pub fn eval_expr(
    expr: &Expr,
    ctx: &StaticCtx,
    dctx: &mut LazyContext,
) -> Result<ParsedValue, QueryError> {
    match expr {
        Expr::Identifier(x) => {
            if x.quote_style.is_some() || ctx.is_none() {
                Ok(ParsedValue::StrVal(Rc::new(x.value.clone())))
            } else {
                if let Some(lazyv) = dctx.get_value(&x.value, ctx)? {
                    Ok(lazyv)
                } else if let Some(val) = ctx.get_value(&x.value) {
                    Ok(val.clone())
                } else {
                    Ok(ParsedValue::NullVal)
                }
            }
        }
        Expr::CompoundIdentifier(_) => Err(QueryError::not_impl("Expr::CompoundIdentifier")),
        Expr::IsNull(x) => {
            let res = eval_expr(x, ctx, dctx)?;
            Ok(ParsedValue::BoolVal(res == ParsedValue::NullVal))
        }
        Expr::IsNotNull(x) => {
            let res = eval_expr(x, ctx, dctx)?;
            Ok(ParsedValue::BoolVal(res != ParsedValue::NullVal))
        }
        Expr::IsDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsDistinctFrom")),
        Expr::IsNotDistinctFrom(_, _) => Err(QueryError::not_impl("Expr::IsNotDistinctFrom")),
        Expr::InList { .. } => Err(QueryError::not_impl("Expr::InList")),
        Expr::InSubquery { .. } => Err(QueryError::not_impl("Expr::InSubquery")),
        Expr::InUnnest { .. } => Err(QueryError::not_impl("Expr::InUnnest")),
        Expr::Between { .. } => Err(QueryError::not_impl("Expr::Between")),
        Expr::BinaryOp { left, op, right } => {
            let lval = eval_expr(left, ctx, dctx)?;
            let mut rval = || {
                // using a closure for lazy evaluation
                let rv = eval_expr(right, ctx, dctx)?;
                Ok(rv)
            };
            // another closure for lazy eval
            let mut arithmetic_op = |op: &BinaryOperator| {
                let rvalv: ParsedValue = rval()?;
                match (&lval, &rvalv) {
                    (ParsedValue::LongVal(lv), ParsedValue::LongVal(rv)) => {
                        Ok(ParsedValue::LongVal(eval_aritmethic_op(*lv, *rv, &op)?))
                    }
                    (ParsedValue::DoubleVal(lv), ParsedValue::LongVal(rv)) => {
                        let rvd = *rv as f64;
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(*lv, rvd, &op)?))
                    }
                    (ParsedValue::LongVal(lv), ParsedValue::DoubleVal(rv)) => {
                        let lvd = *lv as f64;
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(lvd, *rv, &op)?))
                    }
                    (ParsedValue::DoubleVal(lv), ParsedValue::DoubleVal(rv)) => {
                        Ok(ParsedValue::DoubleVal(eval_aritmethic_op(*lv, *rv, &op)?))
                    }
                    _ => Err(QueryError::incompatible_types(&lval, &rvalv, &op)),
                }
            };
            match op {
                BinaryOperator::Plus => arithmetic_op(&op),
                BinaryOperator::Minus => arithmetic_op(&op),
                BinaryOperator::Multiply => arithmetic_op(&op),
                BinaryOperator::Divide => arithmetic_op(&op),
                BinaryOperator::Modulo => arithmetic_op(&op),
                BinaryOperator::StringConcat => {
                    let lstr = lval.to_rc_str();
                    let rstr = rval()?.to_rc_str();
                    Ok(ParsedValue::StrVal(Rc::new(
                        [lstr.as_str(), rstr.as_str()].join(""),
                    )))
                }
                BinaryOperator::Gt => Ok(ParsedValue::BoolVal(lval > rval()?)),
                BinaryOperator::Lt => Ok(ParsedValue::BoolVal(lval < rval()?)),
                BinaryOperator::GtEq => Ok(ParsedValue::BoolVal(lval >= rval()?)),
                BinaryOperator::LtEq => Ok(ParsedValue::BoolVal(lval > rval()?)),
                BinaryOperator::Spaceship => Err(QueryError::not_impl("BinaryOperator::Spaceship")),
                BinaryOperator::Eq => Ok(ParsedValue::BoolVal(lval == rval()?)),
                BinaryOperator::NotEq => Ok(ParsedValue::BoolVal(lval != rval()?)),
                BinaryOperator::And => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) && rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Or => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) || rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Xor => Ok(ParsedValue::BoolVal(
                    lval.as_bool().unwrap_or(false) != rval()?.as_bool().unwrap_or(false),
                )),
                BinaryOperator::Like => Err(QueryError::not_impl("BinaryOperator::Like")),
                BinaryOperator::NotLike => Err(QueryError::not_impl("BinaryOperator::NotLike")),
                BinaryOperator::ILike => Err(QueryError::not_impl("BinaryOperator::ILike")),
                BinaryOperator::NotILike => Err(QueryError::not_impl("BinaryOperator::NotILike")),

                BinaryOperator::BitwiseOr => Err(QueryError::not_impl("BinaryOperator::BitwiseOr")),
                BinaryOperator::BitwiseAnd => {
                    Err(QueryError::not_impl("BinaryOperator::BitwiseAnd"))
                }
                BinaryOperator::BitwiseXor => {
                    Err(QueryError::not_impl("BinaryOperator::BitwiseXor"))
                }
                BinaryOperator::PGBitwiseXor => {
                    Err(QueryError::not_impl("BinaryOperator::PGBitwiseXor"))
                }
                BinaryOperator::PGBitwiseShiftLeft => {
                    Err(QueryError::not_impl("BinaryOperator::PGBitwiseShiftLeft"))
                }
                BinaryOperator::PGBitwiseShiftRight => {
                    Err(QueryError::not_impl("BinaryOperator::PGBitwiseShiftRight"))
                }

                BinaryOperator::PGRegexMatch => {
                    Err(QueryError::not_impl("BinaryOperator::PGRegexMatch"))
                }
                BinaryOperator::PGRegexIMatch => {
                    Err(QueryError::not_impl("BinaryOperator::PGRegexIMatch"))
                }
                BinaryOperator::PGRegexNotMatch => {
                    Err(QueryError::not_impl("BinaryOperator::PGRegexNotMatch"))
                }
                BinaryOperator::PGRegexNotIMatch => {
                    Err(QueryError::not_impl("BinaryOperator::PGRegexNotIMatch"))
                }
            }
        }
        Expr::UnaryOp { op, expr } => {
            let val = eval_expr(expr, ctx, dctx)?;
            match op {
                UnaryOperator::Plus => Err(QueryError::not_impl("UnaryOperator::Plus")),
                UnaryOperator::Minus => Err(QueryError::not_impl("UnaryOperator::Minus")),
                UnaryOperator::Not => Ok(ParsedValue::BoolVal(!val.as_bool().unwrap_or(false))),
                UnaryOperator::PGBitwiseNot => {
                    Err(QueryError::not_impl("UnaryOperator::PGBitwiseNot"))
                }
                UnaryOperator::PGSquareRoot => {
                    Err(QueryError::not_impl("UnaryOperator::PGSquareRoot"))
                }
                UnaryOperator::PGCubeRoot => Err(QueryError::not_impl("UnaryOperator::PGCubeRoot")),
                UnaryOperator::PGPostfixFactorial => {
                    Err(QueryError::not_impl("UnaryOperator::PGPostfixFactorial"))
                }
                UnaryOperator::PGPrefixFactorial => {
                    Err(QueryError::not_impl("UnaryOperator::PGPrefixFactorial"))
                }
                UnaryOperator::PGAbs => Err(QueryError::not_impl("UnaryOperator::PGAbs")),
            }
        }
        Expr::Cast { .. } => Err(QueryError::not_impl("Expr::Cast")),
        Expr::TryCast { .. } => Err(QueryError::not_impl("Expr::TryCast")),
        Expr::Extract { .. } => Err(QueryError::not_impl("Expr::Extract")),
        Expr::Substring { .. } => Err(QueryError::not_impl("Expr::Substring")),
        Expr::Trim { .. } => Err(QueryError::not_impl("Expr::Trim")),
        Expr::Collate { .. } => Err(QueryError::not_impl("Expr::Collate")),
        Expr::Nested(be) => eval_expr(be, ctx, dctx),
        Expr::Value(v) => {
            match v {
                Value::Number(x, _) => {
                    let s: &String = x;
                    if s.find('.').is_some() {
                        str2val(s, &ParsedValueType::DoubleType).ok_or(QueryError::unexpected(
                            &["Failed to parse double value: ", s].join(""),
                        ))
                    } else {
                        str2val(s, &ParsedValueType::LongType).ok_or(QueryError::unexpected(
                            &["Failed to parse long value: ", s].join(""),
                        ))
                    }
                }
                //Value::Number(x, _) => {}
                Value::SingleQuotedString(x) => {
                    let s: &String = x;
                    Ok(ParsedValue::StrVal(Rc::new(s.clone())))
                }
                Value::NationalStringLiteral(_) => {
                    Err(QueryError::not_impl("Value::NationalStringLiteral"))
                }
                Value::HexStringLiteral(_) => Err(QueryError::not_impl("Value::HexStringLiteral")),
                Value::DoubleQuotedString(x) => {
                    let s: &String = x;
                    Ok(ParsedValue::StrVal(Rc::new(s.clone())))
                }
                Value::Boolean(b) => Ok(ParsedValue::BoolVal(*b)),
                Value::Interval { .. } => Err(QueryError::not_impl("Value::Interval")),
                Value::Null => Ok(ParsedValue::NullVal),
                Value::Placeholder(_) => Err(QueryError::not_impl("Value::Placeholder")),
                #[allow(unreachable_patterns)]
                // XXX the IntelliJ IDE does not see this as unreachable
                _ => Err(QueryError::not_supported("Impossible")),
            }
        }
        Expr::TypedString { .. } => Err(QueryError::not_impl("Expr::TypedString")),
        Expr::MapAccess { .. } => Err(QueryError::not_impl("Expr::MapAccess")),
        Expr::Function(f) => eval_function(f, ctx, dctx),
        Expr::Case { .. } => Err(QueryError::not_impl("Expr::Case")),
        Expr::Exists(_) => Err(QueryError::not_impl("Expr::Exists")),
        Expr::Subquery(_) => Err(QueryError::not_impl("Expr::Subquery")),
        Expr::ListAgg(_) => Err(QueryError::not_impl("Expr::ListAgg")),
        Expr::GroupingSets(_) => Err(QueryError::not_impl("Expr::GroupingSets")),
        Expr::Cube(_) => Err(QueryError::not_impl("Expr::Cube")),
        Expr::Rollup(_) => Err(QueryError::not_impl("Expr::Rollup")),
        Expr::Tuple(_) => Err(QueryError::not_impl("Expr::Tuple")),
        Expr::ArrayIndex { .. } => Err(QueryError::not_impl("Expr::ArrayIndex")),
        Expr::Array(_) => Err(QueryError::not_impl("Expr::Array")),
    }
}
